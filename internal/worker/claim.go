package worker

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yourorg/atlas/internal/domain"
)

// claimSQL atomically selects and locks a single pending job across all queues.
//
// FOR UPDATE SKIP LOCKED prevents contention: workers that lose the race move
// on immediately rather than blocking. queue in ORDER BY ensures fairness so a
// high-volume queue cannot starve low-volume ones. Lease duration is injected
// via $3 (seconds) so claim and extend stay in sync.
const claimSQL = `
WITH candidate AS (
    SELECT id FROM jobs
    WHERE queue        = ANY($1::text[])
      AND state        = 'pending'
      AND scheduled_at <= NOW()
    ORDER BY
        priority DESC,
        queue,
        created_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs
SET
    state                = 'running',
    locked_by            = $2,
    locked_at            = NOW(),
    lock_expires_at      = NOW() + ($3 * interval '1 second'),
    current_execution_id = $4,
    state_version        = state_version + 1,
    updated_at           = NOW()
FROM candidate
WHERE jobs.id = candidate.id
RETURNING
    jobs.id, jobs.queue, jobs.handler_name, jobs.payload, jobs.payload_hash,
    jobs.state, jobs.priority, jobs.scheduled_at, jobs.created_at,
    jobs.updated_at, jobs.retry_count, jobs.max_retries,
    jobs.idempotency_key, jobs.locked_by, jobs.locked_at,
    jobs.lock_expires_at, jobs.current_execution_id, jobs.state_version`

// ClaimJob attempts to claim one pending job from queues for workerID.
// Returns nil, nil when no job is available (normal idle state).
func ClaimJob(
	ctx context.Context,
	pool *pgxpool.Pool,
	queues []string,
	workerID string,
	execID uuid.UUID,
	leaseSecs int,
) (*domain.Job, error) {
	row := pool.QueryRow(ctx, claimSQL, queues, workerID, leaseSecs, execID)
	job := &domain.Job{}
	if err := scanJob(row, job); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return job, nil
}

// scanJob populates a Job from the columns returned by claimSQL.
// The column order must match the RETURNING clause exactly.
func scanJob(row pgx.Row, job *domain.Job) error {
	var state string
	err := row.Scan(
		&job.ID,
		&job.Queue,
		&job.HandlerName,
		&job.Payload,
		&job.PayloadHash,
		&state,
		&job.Priority,
		&job.ScheduledAt,
		&job.CreatedAt,
		&job.UpdatedAt,
		&job.RetryCount,
		&job.MaxRetries,
		&job.IdempotencyKey,
		&job.LockedBy,
		&job.LockedAt,
		&job.LockExpiresAt,
		&job.CurrentExecutionID,
		&job.StateVersion,
	)
	if err != nil {
		return err
	}
	job.State = domain.JobState(state)
	return nil
}
