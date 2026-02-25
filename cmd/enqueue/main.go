// cmd/enqueue is a minimal bootstrap for testing Phase 2.
// It connects to Postgres, runs migrations, registers two handlers,
// then enqueues a job (demonstrating idempotency on the second call).
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/yourorg/atlas/internal/db"
	"github.com/yourorg/atlas/internal/migrate"
	"github.com/yourorg/atlas/internal/queue"
	"github.com/yourorg/atlas/internal/registry"
)

func main() {
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

	if err := migrate.Run(ctx, pool); err != nil {
		log.Fatalf("run migrations: %v", err)
	}

	// --- Handler registry (Phase 2) ---
	reg := registry.New()
	reg.Register("send_email", func(ctx context.Context, payload []byte) error {
		fmt.Printf("[send_email] payload: %s\n", payload)
		return nil
	})
	reg.Register("noop", func(ctx context.Context, payload []byte) error {
		return nil
	})
	log.Printf("registered handlers: %v", reg.Names())

	// --- Enqueue (Phase 2) ---
	res, err := queue.Enqueue(ctx, pool, queue.EnqueueOptions{
		Queue:          "emails",
		HandlerName:    "send_email",
		Payload:        []byte(`{"to":"alice@example.com","subject":"Hello"}`),
		IdempotencyKey: "welcome-alice-v1",
		Priority:       5,
		MaxRetries:     3,
	})
	if err != nil {
		log.Fatalf("enqueue: %v", err)
	}
	log.Printf("enqueue result: job_id=%s state=%s inserted=%v",
		res.JobID, res.State, res.Inserted)

	// Second call with the same key — must not insert a duplicate.
	res2, err := queue.Enqueue(ctx, pool, queue.EnqueueOptions{
		Queue:          "emails",
		HandlerName:    "send_email",
		Payload:        []byte(`{"to":"alice@example.com","subject":"Hello"}`),
		IdempotencyKey: "welcome-alice-v1",
	})
	if err != nil {
		log.Fatalf("enqueue (duplicate): %v", err)
	}
	log.Printf("idempotency check: job_id=%s state=%s inserted=%v (want inserted=false)",
		res2.JobID, res2.State, res2.Inserted)

	if res.JobID != res2.JobID {
		log.Fatalf("idempotency broken: got different job IDs %s vs %s",
			res.JobID, res2.JobID)
	}
	log.Println("idempotency OK — same job_id returned on duplicate submission")
}
