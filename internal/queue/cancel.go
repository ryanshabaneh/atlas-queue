package queue

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CancelResult reports whether the job was found and whether cancellation
// happened immediately (pending→canceled) or cooperatively (running→canceled_at set).
type CancelResult struct {
	Found     bool
	Immediate bool
}

// CancelJob attempts to cancel a job by ID.
//
// Pending jobs are moved to 'canceled' immediately (Immediate=true).
// Running jobs have canceled_at set so that watchCancellation stops the
// handler cooperatively within ~3 s (Immediate=false). state_version is
// bumped in both paths so that any NOTIFY trigger fires.
// Jobs already in a terminal state (completed, dead, canceled) are not
// modified; Found=false is returned.
func CancelJob(ctx context.Context, pool *pgxpool.Pool, jobID uuid.UUID) (CancelResult, error) {
	// Pending → canceled immediately.
	tag, err := pool.Exec(ctx, `
		UPDATE jobs SET
			state         = 'canceled',
			canceled_at   = NOW(),
			state_version = state_version + 1,
			updated_at    = NOW()
		WHERE id = $1 AND state = 'pending'`, jobID)
	if err != nil {
		return CancelResult{}, err
	}
	if tag.RowsAffected() > 0 {
		return CancelResult{Found: true, Immediate: true}, nil
	}

	// Running → set canceled_at only; watchCancellation stops the handler.
	tag, err = pool.Exec(ctx, `
		UPDATE jobs SET
			canceled_at   = NOW(),
			state_version = state_version + 1,
			updated_at    = NOW()
		WHERE id = $1 AND state = 'running'
		  AND canceled_at IS NULL`, jobID)
	if err != nil {
		return CancelResult{}, err
	}
	return CancelResult{
		Found:     tag.RowsAffected() > 0,
		Immediate: false,
	}, nil
}
