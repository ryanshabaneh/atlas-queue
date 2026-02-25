package worker

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yourorg/atlas/internal/domain"
	"github.com/yourorg/atlas/internal/registry"
)

type executeResult struct {
	outcome string // "completed" | "failed" | "canceled" | "abandoned"
	err     error
}

// executeJob runs the handler inside a cancelable context. Two background
// goroutines run for the lifetime of the call:
//   - extendLease: refreshes lock_expires_at every leaseSeconds/3
//   - watchCancellation: polls canceled_at and cancels the handler context
//
// Both goroutines are stopped via their stop channel before executeJob returns.
func executeJob(
	ctx context.Context,
	pool *pgxpool.Pool,
	job *domain.Job,
	execID uuid.UUID,
	handler registry.Handler,
	workerID string,
	leaseSeconds int,
	logger *slog.Logger,
) executeResult {
	execCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	leaseStop := make(chan struct{})
	go extendLease(execCtx, pool, job.ID, workerID, execID, leaseSeconds, leaseStop, logger)
	defer close(leaseStop)

	cancelStop := make(chan struct{})
	var userCanceled atomic.Bool
	go watchCancellation(execCtx, pool, job.ID, &userCanceled, cancel, cancelStop, logger)
	defer close(cancelStop)

	handlerErr := handler(execCtx, job.Payload)

	if ctx.Err() != nil {
		return executeResult{outcome: "abandoned"}
	}
	if execCtx.Err() != nil {
		if userCanceled.Load() {
			return executeResult{outcome: "canceled"}
		}
		if ctx.Err() != nil {
			return executeResult{outcome: "abandoned"}
		}
		return executeResult{outcome: "abandoned"}
	}
	if handlerErr != nil {
		return executeResult{outcome: "failed", err: handlerErr}
	}
	return executeResult{outcome: "completed"}
}

// extendLease periodically refreshes lock_expires_at so the reaper does not
// reclaim a job that is still actively running. The ticker fires at
// leaseSeconds/3, giving two extension opportunities before expiry.
func extendLease(
	ctx context.Context,
	pool *pgxpool.Pool,
	jobID uuid.UUID,
	workerID string,
	execID uuid.UUID,
	leaseSeconds int,
	stop <-chan struct{},
	logger *slog.Logger,
) {
	interval := time.Duration(leaseSeconds) * time.Second / 3
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				return
			}
			result, err := pool.Exec(ctx, `
				UPDATE jobs
				SET lock_expires_at = NOW() + ($1 * interval '1 second')
				WHERE id = $2
				  AND locked_by = $3
				  AND state = 'running'
				  AND lock_expires_at > NOW()
				  AND current_execution_id = $4`,
				leaseSeconds, jobID, workerID, execID)
			if err != nil {
				logger.Warn("lease extension failed",
					"job_id", jobID, "err", err)
				continue
			}
			if result.RowsAffected() == 0 {
				logger.Warn("lease extension fenced; stopping extender",
					"job_id", jobID,
					"exec_id", execID)
				return
			}
		}
	}
}

// watchCancellation polls canceled_at every 3 seconds. When it is set the
// handler's context is canceled so the handler can exit cooperatively.
func watchCancellation(
	ctx context.Context,
	pool *pgxpool.Pool,
	jobID uuid.UUID,
	userCanceled *atomic.Bool,
	cancel context.CancelFunc,
	stop <-chan struct{},
	logger *slog.Logger,
) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			var canceledAt *time.Time
			err := pool.QueryRow(ctx,
				`SELECT canceled_at FROM jobs WHERE id = $1`, jobID,
			).Scan(&canceledAt)
			if err != nil {
				logger.Warn("cancellation check failed",
					"job_id", jobID, "err", err)
				continue
			}
			if canceledAt != nil {
				userCanceled.Store(true)
				cancel()
				return
			}
		}
	}
}
