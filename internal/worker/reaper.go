package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/yourorg/atlas/internal/ratelimit"
)

// reaperLockKey is the PostgreSQL advisory lock key used for reaper election.
// Only one reaper wins the lock across all workers in the cluster.
const reaperLockKey = int64(0x47415445)

// RunReaper competes for the advisory lock and runs the reaper loop on the
// winner. The lock is held on a dedicated connection so it auto-releases if
// the process crashes. Non-winners sleep and retry every 10 seconds.
func RunReaper(ctx context.Context, pool *pgxpool.Pool,
	rc *redis.Client, logger *slog.Logger) {
	for {
		if ctx.Err() != nil {
			return
		}

		conn, err := pool.Acquire(ctx)
		if err != nil {
			logger.Error("reaper: acquire failed", "err", err)
			time.Sleep(5 * time.Second)
			continue
		}

		var won bool
		err = conn.QueryRow(ctx,
			`SELECT pg_try_advisory_lock($1)`, reaperLockKey).Scan(&won)
		if err != nil || !won {
			conn.Release()
			time.Sleep(10 * time.Second)
			continue
		}

		logger.Info("reaper: won election")
		runReaperLoop(ctx, conn.Conn(), pool, rc, logger)
		conn.Release()
	}
}

// runReaperLoop ticks every 30 seconds and runs both orphan recovery and dead
// worker detection. It exits when ctx is canceled or when the underlying
// connection is lost (which releases the advisory lock, allowing another
// worker to win the election).
func runReaperLoop(ctx context.Context, _ *pgx.Conn,
	pool *pgxpool.Pool, rc *redis.Client, logger *slog.Logger) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := reapOrphanedJobs(ctx, pool, rc, logger); err != nil {
				logger.Error("reaper: orphan reap failed", "err", err)
				return
			}
			reapDeadWorkers(ctx, pool, logger)
		}
	}
}

// reapOrphanedJobs finds running jobs whose lease has expired, resets them to
// pending so another worker can claim them, and marks their execution_log rows
// as orphaned. It also releases their entries from the Redis inflight SET so
// the AIMD controller does not count them.
//
// The CTE captures current_execution_id before the UPDATE nulls it — without
// the CTE, RETURNING would give NULL for those columns.
//
// FOR UPDATE SKIP LOCKED ensures the reaper never blocks on a row that is
// being concurrently updated by a worker extending its lease. LIMIT 500 bounds
// work per cycle.
func reapOrphanedJobs(ctx context.Context, pool *pgxpool.Pool,
	rc *redis.Client, logger *slog.Logger) error {
	rows, err := pool.Query(ctx, `
		WITH orphans AS (
			SELECT id, current_execution_id, queue
			FROM jobs
			WHERE state = 'running' AND lock_expires_at < NOW()
			ORDER BY lock_expires_at ASC
			LIMIT 500
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs SET
			state                = 'pending',
			scheduled_at         = clock_timestamp() + interval '1 second',
			locked_by            = NULL,
			locked_at            = NULL,
			lock_expires_at      = NULL,
			current_execution_id = NULL,
			state_version        = state_version + 1,
			updated_at           = NOW()
		FROM orphans
		WHERE jobs.id = orphans.id
		RETURNING orphans.id, orphans.current_execution_id, orphans.queue`)
	if err != nil {
		return err
	}

	type orphan struct {
		jobID  uuid.UUID
		execID *uuid.UUID
		queue  string
	}
	var reclaimed []orphan

	for rows.Next() {
		var jobID uuid.UUID
		var execID *uuid.UUID
		var queue string
		if err := rows.Scan(&jobID, &execID, &queue); err != nil {
			continue
		}
		reclaimed = append(reclaimed, orphan{jobID: jobID, execID: execID, queue: queue})
	}

	// Close the result set before side effects so the UPDATE ... RETURNING
	// statement commits promptly and the pending row is observable.
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, o := range reclaimed {
		if o.execID != nil {
			ratelimit.ReleaseInflight(ctx, rc, o.queue, o.execID.String()) //nolint:errcheck
			_, err := pool.Exec(ctx, `
				UPDATE execution_log
				SET finished_at = NOW(), outcome = 'orphaned'
				WHERE id = $1 AND finished_at IS NULL`, o.execID)
			if err != nil {
				logger.Warn("reaper: exec log update failed",
					"exec_id", o.execID, "err", err)
			}
		}

		logger.Info("reaper: requeued orphan",
			"job_id", o.jobID,
			"exec_id", o.execID,
			"queue", o.queue)
	}
	return nil
}

// reapDeadWorkers marks workers whose heartbeat has not been updated in the
// last 30 seconds as dead. This is informational — job recovery is handled
// by the lease expiry mechanism in reapOrphanedJobs, not by worker status.
func reapDeadWorkers(ctx context.Context, pool *pgxpool.Pool,
	logger *slog.Logger) {
	result, err := pool.Exec(ctx, `
		UPDATE workers SET status = 'dead'
		WHERE status = 'active'
		  AND last_heartbeat < NOW() - interval '30 seconds'`)
	if err != nil {
		logger.Error("reaper: dead worker reap failed", "err", err)
		return
	}
	if result.RowsAffected() > 0 {
		logger.Info("reaper: marked workers dead",
			"count", result.RowsAffected())
	}
}
