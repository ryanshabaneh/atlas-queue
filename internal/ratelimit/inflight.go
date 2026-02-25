package ratelimit

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// ClaimInflight records an execution ID in the per-queue inflight SET.
// Using a SET (not a counter) means SREM is idempotent — a crashed worker
// or double-release can never push the count negative.
func ClaimInflight(ctx context.Context, rc *redis.Client,
	queue, execID string) error {
	return rc.SAdd(ctx, InflightSetKey(queue), execID).Err()
}

// ReleaseInflight removes an execution ID from the per-queue inflight SET.
// Safe to call multiple times; SREM on a missing member is a no-op.
func ReleaseInflight(ctx context.Context, rc *redis.Client,
	queue, execID string) error {
	return rc.SRem(ctx, InflightSetKey(queue), execID).Err()
}

// InflightCount returns the number of currently active executions for queue.
func InflightCount(ctx context.Context, rc *redis.Client,
	queue string) (int64, error) {
	return rc.SCard(ctx, InflightSetKey(queue)).Result()
}

// ConcurrencyLimit returns the current AIMD-controlled limit for queue.
// Returns the default of 10 when no limit has been set yet.
func ConcurrencyLimit(ctx context.Context, rc *redis.Client,
	queue string) (int64, error) {
	v, err := rc.Get(ctx, ConcurrencyLimitKey(queue)).Int64()
	if err == redis.Nil {
		return 10, nil
	}
	return v, err
}

// CanClaim returns true when the queue has capacity for one more execution.
// There is a documented TOCTOU window between this check and the SADD in
// ClaimInflight. Overshoot is bounded by worker count and is acceptable —
// a Lua script would add complexity for marginal gain.
func CanClaim(ctx context.Context, rc *redis.Client,
	queue string) (bool, error) {
	limit, err := ConcurrencyLimit(ctx, rc, queue)
	if err != nil {
		return false, err
	}
	inflight, err := InflightCount(ctx, rc, queue)
	if err != nil {
		return false, err
	}
	return inflight < limit, nil
}
