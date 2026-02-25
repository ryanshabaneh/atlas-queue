package worker

import (
	"context"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yourorg/atlas/internal/domain"
)

func markCompleted(ctx context.Context, pool *pgxpool.Pool, jobID, execID uuid.UUID) (bool, error) {
	if ctx.Err() != nil {
		return false, nil
	}
	result, err := pool.Exec(ctx, `
		UPDATE jobs SET
			state                = 'completed',
			completed_at         = NOW(),
			locked_by            = NULL,
			locked_at            = NULL,
			lock_expires_at      = NULL,
			current_execution_id = NULL,
			state_version        = state_version + 1,
			updated_at           = NOW()
		WHERE id = $1
		  AND state = 'running'
		  AND current_execution_id = $2
		  AND lock_expires_at > NOW()`, jobID, execID)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() == 1, nil
}

// markDead moves a job to the terminal dead state. Used when retry_count has
// reached max_retries or the handler returned a FatalError.
func markDead(
	ctx context.Context, pool *pgxpool.Pool,
	jobID uuid.UUID, execID uuid.UUID, handlerErr error,
) (bool, error) {
	if ctx.Err() != nil {
		return false, nil
	}
	result, err := pool.Exec(ctx, `
		UPDATE jobs SET
			state                = 'dead',
			last_error           = $1,
			last_error_at        = NOW(),
			locked_by            = NULL,
			locked_at            = NULL,
			lock_expires_at      = NULL,
			current_execution_id = NULL,
			state_version        = state_version + 1,
			updated_at           = NOW()
		WHERE id = $2
		  AND state = 'running'
		  AND current_execution_id = $3
		  AND lock_expires_at > NOW()`, handlerErr.Error(), jobID, execID)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() == 1, nil
}

func markCanceled(ctx context.Context, pool *pgxpool.Pool, jobID, execID uuid.UUID) (bool, error) {
	if ctx.Err() != nil {
		return false, nil
	}
	result, err := pool.Exec(ctx, `
		UPDATE jobs SET
			state                = 'canceled',
			canceled_at          = NOW(),
			locked_by            = NULL,
			locked_at            = NULL,
			lock_expires_at      = NULL,
			current_execution_id = NULL,
			state_version        = state_version + 1,
			updated_at           = NOW()
		WHERE id = $1
		  AND state = 'running'
		  AND current_execution_id = $2
		  AND lock_expires_at > NOW()`, jobID, execID)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() == 1, nil
}

// markRetry re-queues a failed job for a future attempt. It increments
// retry_count, clears lock fields, and sets scheduled_at via computeBackoff
// so the poller will not pick it up until the backoff window has elapsed.
func markRetry(
	ctx context.Context, pool *pgxpool.Pool,
	job *domain.Job, execID uuid.UUID, handlerErr error,
) (bool, error) {
	if ctx.Err() != nil {
		return false, nil
	}
	backoff := computeBackoff(job.RetryCount)
	result, err := pool.Exec(ctx, `
		UPDATE jobs SET
			state                = 'pending',
			scheduled_at         = NOW() + ($1 * interval '1 millisecond'),
			retry_count          = retry_count + 1,
			last_error           = $2,
			last_error_at        = NOW(),
			locked_by            = NULL,
			locked_at            = NULL,
			lock_expires_at      = NULL,
			current_execution_id = NULL,
			state_version        = state_version + 1,
			updated_at           = NOW()
		WHERE id = $3
		  AND state = 'running'
		  AND current_execution_id = $4
		  AND lock_expires_at > NOW()`,
		backoff.Milliseconds(), handlerErr.Error(), job.ID, execID)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() == 1, nil
}

// computeBackoff returns an exponentially increasing delay with ±25% jitter.
// Base = 5s, max = 1h, exponent capped at 20 to prevent integer overflow.
func computeBackoff(attempt int) time.Duration {
	base := 5 * time.Second
	maxDelay := 1 * time.Hour
	shift := attempt
	if shift > 20 {
		shift = 20
	}
	d := base * time.Duration(1<<shift)
	if d > maxDelay {
		d = maxDelay
	}
	jitter := time.Duration(rand.Int63n(int64(d/2))) - d/4
	return d + jitter
}
