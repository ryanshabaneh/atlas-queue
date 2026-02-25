package queue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yourorg/atlas/internal/domain"
)

// EnqueueOptions configures a single job submission.
type EnqueueOptions struct {
	Queue          string
	HandlerName    string
	Payload        []byte
	IdempotencyKey string
	Priority       int
	MaxRetries     int
	RunAt          *time.Time
	Delay          *time.Duration
}

// EnqueueResult is returned by Enqueue.
type EnqueueResult struct {
	JobID    uuid.UUID
	State    domain.JobState
	Inserted bool // false when the idempotency key already existed
}

const insertSQL = `
INSERT INTO jobs
    (queue, handler_name, payload, payload_hash,
     idempotency_key, priority, max_retries, scheduled_at,
     state, state_version)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending', 0)
ON CONFLICT (queue, idempotency_key) DO NOTHING
RETURNING id, state`

// Enqueue submits a job. If the (queue, idempotency_key) pair already exists
// the existing row is returned with Inserted=false.
func Enqueue(ctx context.Context, pool *pgxpool.Pool, opts EnqueueOptions) (EnqueueResult, error) {
	if opts.Queue == "" {
		return EnqueueResult{}, fmt.Errorf("queue name is required")
	}
	if opts.HandlerName == "" {
		return EnqueueResult{}, fmt.Errorf("handler name is required")
	}
	if opts.IdempotencyKey == "" {
		return EnqueueResult{}, fmt.Errorf("idempotency key is required")
	}
	if opts.MaxRetries < 0 {
		return EnqueueResult{}, fmt.Errorf("max_retries must be >= 0")
	}

	hash := sha256.Sum256(opts.Payload)
	payloadHash := hex.EncodeToString(hash[:])

	scheduledAt := time.Now()
	if opts.RunAt != nil {
		scheduledAt = *opts.RunAt
	} else if opts.Delay != nil {
		scheduledAt = time.Now().Add(*opts.Delay)
	}

	var res EnqueueResult
	err := pool.QueryRow(ctx, insertSQL,
		opts.Queue, opts.HandlerName, opts.Payload, payloadHash,
		opts.IdempotencyKey, opts.Priority, opts.MaxRetries, scheduledAt,
	).Scan(&res.JobID, &res.State)

	if errors.Is(err, pgx.ErrNoRows) {
		// Conflict: row already exists — fetch it.
		err = pool.QueryRow(ctx,
			`SELECT id, state FROM jobs WHERE queue=$1 AND idempotency_key=$2`,
			opts.Queue, opts.IdempotencyKey,
		).Scan(&res.JobID, &res.State)
		res.Inserted = false
		return res, err
	}

	res.Inserted = true
	return res, err
}
