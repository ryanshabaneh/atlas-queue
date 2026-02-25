package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yourorg/atlas/internal/domain"
	"github.com/yourorg/atlas/internal/registry"
)

type Worker struct {
	ID            uuid.UUID
	Hostname      string
	Queues        []string
	Pool          *pgxpool.Pool
	Registry      *registry.Registry
	Logger        *slog.Logger
	LeaseSeconds  int
	startDone     chan struct{}
	startDoneOnce sync.Once
}

func New(
	id uuid.UUID,
	hostname string,
	queues []string,
	pool *pgxpool.Pool,
	reg *registry.Registry,
	logger *slog.Logger,
	leaseSeconds int,
) *Worker {
	if leaseSeconds <= 0 {
		leaseSeconds = 30
	}
	return &Worker{
		ID:           id,
		Hostname:     hostname,
		Queues:       queues,
		Pool:         pool,
		Registry:     reg,
		Logger:       logger,
		LeaseSeconds: leaseSeconds,
		startDone:    make(chan struct{}),
	}
}

// Start runs the poll loop until ctx is canceled. Each job is executed
// synchronously; per-job goroutines for lease extension and cooperative
// cancellation live only for the duration of that job.
func (w *Worker) Start(ctx context.Context) {
	defer w.startDoneOnce.Do(func() { close(w.startDone) })

	w.Logger.Info("worker starting",
		"worker_id", w.ID,
		"queues", w.Queues,
		"handlers", w.Registry.Names())

	for {
		if ctx.Err() != nil {
			return
		}

		execID := uuid.New()
		job, err := ClaimJob(ctx, w.Pool, w.Queues, w.ID.String(), execID, w.LeaseSeconds)
		if err != nil {
			w.Logger.Error("claim error", "err", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if job == nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		w.runJob(ctx, job, execID)
	}
}

// DrainAndWait blocks until the poll loop exits (usually after ctx cancellation)
// or until the caller's timeout/cancelation is reached.
func (w *Worker) DrainAndWait(ctx context.Context) error {
	select {
	case <-w.startDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Worker) runJob(ctx context.Context, job *domain.Job, execID uuid.UUID) {
	traceID := uuid.New().String()
	log := w.Logger.With(
		"job_id", job.ID,
		"queue", job.Queue,
		"handler", job.HandlerName,
		"attempt", job.RetryCount,
		"trace_id", traceID,
		"exec_id", execID,
	)

	if err := writeExecLogStart(ctx, w.Pool, execID, job, w.ID, w.Hostname, traceID); err != nil {
		log.Error("failed to write exec log start", "err", err)
		_, _ = markRetry(ctx, w.Pool, job, execID, fmt.Errorf("execution log write failed: %w", err))
		return
	}

	log.Info("job started")

	handler, err := w.Registry.Lookup(job.HandlerName)
	if err != nil {
		log.Error("unknown handler — marking dead", "err", err)
		updated, markErr := markDead(ctx, w.Pool, job.ID, execID, err)
		if markErr != nil {
			log.Error("failed to mark dead", "err", markErr)
			return
		}
		if !updated {
			log.Warn("stale dead transition ignored", "job_id", job.ID, "exec_id", execID)
			return
		}
		writeExecLogFinish(ctx, w.Pool, execID, "failed", err, log)
		return
	}

	result := executeJob(ctx, w.Pool, job, execID, handler, w.ID.String(), w.LeaseSeconds, log)

	switch result.outcome {
	case "completed":
		updated, err := markCompleted(ctx, w.Pool, job.ID, execID)
		if err != nil {
			log.Error("failed to mark completed", "err", err)
			return
		}
		if !updated {
			log.Warn("stale completion ignored", "job_id", job.ID, "exec_id", execID)
			return
		}
		log.Info("job completed")
		writeExecLogFinish(ctx, w.Pool, execID, "completed", nil, log)

	case "canceled":
		updated, err := markCanceled(ctx, w.Pool, job.ID, execID)
		if err != nil {
			log.Error("failed to mark canceled", "err", err)
			return
		}
		if !updated {
			log.Warn("stale cancel transition ignored", "job_id", job.ID, "exec_id", execID)
			return
		}
		log.Info("job canceled")
		writeExecLogFinish(ctx, w.Pool, execID, "canceled", nil, log)

	case "abandoned":
		log.Info("job execution abandoned due to worker shutdown; leaving state unchanged")
		return

	case "failed":
		var fatalErr *registry.FatalError
		isFatal := errors.As(result.err, &fatalErr)
		if isFatal || job.RetryCount >= job.MaxRetries {
			updated, err := markDead(ctx, w.Pool, job.ID, execID, result.err)
			if err != nil {
				log.Error("failed to mark dead", "err", err)
				return
			}
			if !updated {
				log.Warn("stale dead transition ignored", "job_id", job.ID, "exec_id", execID)
				return
			}
			log.Warn("job dead", "err", result.err, "is_fatal", isFatal)
		} else {
			updated, err := markRetry(ctx, w.Pool, job, execID, result.err)
			if err != nil {
				log.Error("failed to mark retry", "err", err)
				return
			}
			if !updated {
				log.Warn("stale retry transition ignored", "job_id", job.ID, "exec_id", execID)
				return
			}
			log.Warn("job failed — will retry",
				"err", result.err,
				"retry_count", job.RetryCount,
				"max_retries", job.MaxRetries)
		}
		writeExecLogFinish(ctx, w.Pool, execID, "failed", result.err, log)
	}
}
