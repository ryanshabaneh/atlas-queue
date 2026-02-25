package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/yourorg/atlas/internal/db"
	"github.com/yourorg/atlas/internal/migrate"
	"github.com/yourorg/atlas/internal/queue"
	"github.com/yourorg/atlas/internal/registry"
	"github.com/yourorg/atlas/internal/worker"
)

var failCount int32

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://atlas:atlas@localhost:5432/atlas"
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	if err := worker.EnableParentDeathSignal(); err != nil {
		logger.Warn("failed to enable parent-death signal", "err", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	// Connect to PostgreSQL.
	logger.Info("connecting to database", "url", databaseURL)
	pool, err := db.Connect(ctx, databaseURL)
	if err != nil {
		logger.Error("connect to database failed", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	logger.Info("database connected")

	if err := migrate.Run(ctx, pool); err != nil {
		logger.Error("run migrations failed", "err", err)
		os.Exit(1)
	}

	// Connect to Redis.
	redisOpts, err := redis.ParseURL(redisURL)
	if err != nil {
		logger.Error("parse redis URL failed", "err", err, "url", redisURL)
		os.Exit(1)
	}
	rc := redis.NewClient(redisOpts)
	defer rc.Close()

	logger.Info("connecting to redis", "url", redisURL)
	if err := rc.Ping(ctx).Err(); err != nil {
		logger.Error("redis ping failed", "err", err)
		os.Exit(1)
	}
	logger.Info("redis connected")

	reg := registry.New()

	// noop_handler: completes immediately. Used for throughput benchmarks.
	reg.Register("noop_handler", func(ctx context.Context, payload []byte) error {
		return nil
	})

	// slow_handler: sleeps 30 s, respecting context cancellation.
	// Used to observe lease extension and cooperative cancellation.
	reg.Register("slow_handler", func(ctx context.Context, payload []byte) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(30 * time.Second):
			return nil
		}
	})

	// fail_handler: always returns an error. Phase 3 marks the job dead.
	reg.Register("fail_handler", func(ctx context.Context, payload []byte) error {
		return fmt.Errorf("simulated failure")
	})

	// fail_then_succeed: fails the first 2 attempts, succeeds on attempt 3.
	// Phase 4 retry validation handler.
	reg.Register("fail_then_succeed", func(ctx context.Context, payload []byte) error {
		n := atomic.AddInt32(&failCount, 1)
		if n <= 2 {
			return fmt.Errorf("transient failure attempt %d", n)
		}
		return nil
	})

	// fatal_handler: returns FatalError — must go dead on first attempt.
	reg.Register("fatal_handler", func(ctx context.Context, payload []byte) error {
		return &registry.FatalError{Cause: fmt.Errorf("fatal problem")}
	})

	hostname, _ := os.Hostname()
	workerID := uuid.New()
	queues := []string{"default", "emails", "notifications"}

	w := worker.New(workerID, hostname, queues, pool, reg, logger, 30)

	logger.Info("registering worker", "worker_id", workerID, "hostname", hostname, "queues", queues)
	if err := worker.RegisterWorker(ctx, pool, w); err != nil {
		logger.Error("register worker failed", "err", err)
		os.Exit(1)
	}
	logger.Info("worker registered", "worker_id", workerID)

	logger.Info("worker ready",
		"worker_id", workerID,
		"hostname", hostname,
		"queues", queues,
		"handlers", reg.Names())

	// Heartbeat: update last_heartbeat every 5s so the reaper can distinguish
	// live workers from crashed ones.
	logger.Info("starting heartbeat goroutine", "worker_id", workerID)
	go w.RunHeartbeat(ctx)

	// Reaper: competes for advisory lock; the winner reclaims orphaned jobs
	// and marks dead workers every 15 seconds.
	go worker.RunReaper(ctx, pool, rc, logger)

	// Smoke-test: enqueue one noop job on startup. Idempotency key prevents
	// duplicate rows on restart.
	res, err := queue.Enqueue(ctx, pool, queue.EnqueueOptions{
		Queue:          "default",
		HandlerName:    "noop_handler",
		Payload:        []byte(`{"smoke":"test"}`),
		IdempotencyKey: "smoke-test-phase5",
	})
	if err != nil {
		logger.Warn("smoke-test enqueue failed", "err", err)
	} else {
		logger.Info("smoke-test job",
			"job_id", res.JobID,
			"state", res.State,
			"inserted", res.Inserted)
	}

	go w.Start(ctx)

	<-ctx.Done()

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer drainCancel()
	if err := w.DrainAndWait(drainCtx); err != nil {
		logger.Warn("shutdown drain timeout; orphaned jobs will be reaped", "err", err)
	}

	logger.Info("shutdown complete")
}
