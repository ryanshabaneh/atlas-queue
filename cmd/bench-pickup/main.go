// cmd/bench-pickup measures idle-worker pickup latency.
//
// With workers already running and idle, it enqueues N noop_handler jobs
// spaced apart by a jittered gap (so every job arrives at an idle worker),
// waits for them to finish, and reports percentiles of
//
//	MIN(execution_log.started_at) - jobs.created_at
//
// Both timestamps come from the database clock, so there is no client/worker
// clock skew. locked_at cannot be used because completion clears it.
//
// Usually driven by scripts/bench_pickup.sh.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yourorg/atlas/internal/db"
	"github.com/yourorg/atlas/internal/queue"
)

func main() {
	var (
		n           = flag.Int("n", 300, "number of jobs to enqueue")
		minGap      = flag.Duration("min-gap", 50*time.Millisecond, "minimum gap between submissions")
		maxGap      = flag.Duration("max-gap", 100*time.Millisecond, "maximum gap between submissions")
		queueName   = flag.String("queue", "default", "queue to submit to")
		minWorkers  = flag.Int("min-workers", 1, "wait for at least this many active workers before starting")
		seed        = flag.Int64("seed", 1, "jitter seed; fixed by default so every mode sees the same arrival pattern")
		label       = flag.String("label", "", "label printed on the RESULT line (e.g. the pickup mode)")
		startupWait = flag.Duration("startup-timeout", 3*time.Minute, "how long to wait for workers to be active and idle")
		finishWait  = flag.Duration("finish-timeout", 2*time.Minute, "how long to wait for submitted jobs to finish")
	)
	flag.Parse()
	if *n <= 0 || *minGap < 0 || *maxGap < *minGap {
		log.Fatalf("invalid flags: need n > 0 and 0 <= min-gap <= max-gap")
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://atlas:atlas@localhost:5432/atlas"
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	pool, err := db.Connect(ctx, databaseURL)
	if err != nil {
		log.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	log.Printf("waiting for >= %d active, idle worker(s)", *minWorkers)
	if err := waitForIdleWorkers(ctx, pool, *queueName, *minWorkers, *startupWait); err != nil {
		log.Fatal(err)
	}

	// Unique per run so repeated runs against the same database never collide
	// with (or get deduplicated against) an earlier run's jobs.
	runID := fmt.Sprintf("%d", time.Now().UnixNano())
	keyPrefix := "bench-pickup-" + runID + "-"
	rng := rand.New(rand.NewSource(*seed))
	payload := []byte(`{"bench":"pickup"}`)

	log.Printf("run %s: enqueuing %d jobs to %q, gap %s-%s", runID, *n, *queueName, *minGap, *maxGap)
	for i := 0; i < *n; i++ {
		if _, err := queue.Enqueue(ctx, pool, queue.EnqueueOptions{
			Queue:          *queueName,
			HandlerName:    "noop_handler",
			Payload:        payload,
			IdempotencyKey: fmt.Sprintf("%s%05d", keyPrefix, i),
		}); err != nil {
			log.Fatalf("enqueue job %d: %v", i, err)
		}
		gap := *minGap
		if span := int64(*maxGap - *minGap); span > 0 {
			gap += time.Duration(rng.Int63n(span + 1))
		}
		select {
		case <-ctx.Done():
			log.Fatal("interrupted")
		case <-time.After(gap):
		}
	}

	completed, err := waitForFinish(ctx, pool, *queueName, keyPrefix, *n, *finishWait)
	if err != nil {
		log.Fatal(err)
	}
	incomplete := *n - completed

	// Refuse to report percentiles over a partial run: missing jobs are the
	// slow tail, so dropping them would flatter the numbers.
	if incomplete > 0 {
		fmt.Printf("RESULT label=%s n=%d completed=%d incomplete=%d status=INVALID\n",
			*label, *n, completed, incomplete)
		os.Exit(1)
	}

	var p50, p95, p99, maxMs, avgMs float64
	err = pool.QueryRow(ctx, `
		WITH lat AS (
			SELECT EXTRACT(EPOCH FROM MIN(e.started_at) - j.created_at) * 1000 AS ms
			FROM jobs j
			JOIN execution_log e ON e.job_id = j.id
			WHERE j.queue = $1
			  AND j.idempotency_key LIKE $2 || '%'
			GROUP BY j.id, j.created_at
		)
		SELECT
			percentile_cont(0.50) WITHIN GROUP (ORDER BY ms),
			percentile_cont(0.95) WITHIN GROUP (ORDER BY ms),
			percentile_cont(0.99) WITHIN GROUP (ORDER BY ms),
			MAX(ms),
			AVG(ms)
		FROM lat`, *queueName, keyPrefix,
	).Scan(&p50, &p95, &p99, &maxMs, &avgMs)
	if err != nil {
		log.Fatalf("compute latency: %v", err)
	}

	fmt.Printf("pickup latency over %d jobs (ms): p50=%.1f p95=%.1f p99=%.1f max=%.1f avg=%.1f\n",
		*n, p50, p95, p99, maxMs, avgMs)
	fmt.Printf("RESULT label=%s n=%d completed=%d incomplete=0 p50_ms=%.1f p95_ms=%.1f p99_ms=%.1f max_ms=%.1f avg_ms=%.1f status=OK\n",
		*label, *n, completed, p50, p95, p99, maxMs, avgMs)
}

// waitForIdleWorkers blocks until enough workers are heartbeating and the
// queue has nothing pending or running (e.g. the worker's startup smoke job),
// so the first measured job lands on an idle worker.
func waitForIdleWorkers(ctx context.Context, pool *pgxpool.Pool, queueName string, minWorkers int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		var active, busy int
		err := pool.QueryRow(ctx, `
			SELECT
				(SELECT COUNT(*) FROM workers
				 WHERE status = 'active' AND last_heartbeat > NOW() - interval '15 seconds'),
				(SELECT COUNT(*) FROM jobs
				 WHERE queue = $1 AND state IN ('pending', 'running'))`, queueName,
		).Scan(&active, &busy)
		if err != nil {
			return fmt.Errorf("check workers: %w", err)
		}
		if active >= minWorkers && busy == 0 {
			// Let workers fall back into their idle wait after any startup work.
			time.Sleep(2 * time.Second)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for workers: active=%d (want >= %d), busy jobs=%d",
				active, minWorkers, busy)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// waitForFinish waits until every submitted job is terminal or the timeout
// passes, and returns how many completed.
func waitForFinish(ctx context.Context, pool *pgxpool.Pool, queueName, keyPrefix string, n int, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for {
		var terminal, completed int
		err := pool.QueryRow(ctx, `
			SELECT
				COUNT(*) FILTER (WHERE state IN ('completed', 'dead', 'canceled')),
				COUNT(*) FILTER (WHERE state = 'completed')
			FROM jobs
			WHERE queue = $1 AND idempotency_key LIKE $2 || '%'`, queueName, keyPrefix,
		).Scan(&terminal, &completed)
		if err != nil {
			return 0, fmt.Errorf("check progress: %w", err)
		}
		if terminal >= n || time.Now().After(deadline) {
			return completed, nil
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}
