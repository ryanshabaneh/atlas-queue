# Atlas Queue

Atlas Queue is a distributed job processing system designed to provide
crash-safe execution, idempotent submission, and deterministic recovery
without relying on external brokers like Kafka or SQS.

It uses PostgreSQL row-level leasing (`FOR UPDATE SKIP LOCKED`) and a
Redis-backed reaper election mechanism to guarantee zero duplicate
completion under worker failure.

**Stack:** Go, PostgreSQL, Redis, gRPC, Docker

## Guarantees

- At-most-once completion (no duplicate executions)
- Crash-safe job recovery
- Idempotent job submission
- Deterministic execution logging
- Lease fencing against stale workers

Validated under concurrent workers with forced SIGKILL, lease expiration, and orphan reclamation scenarios.

## Architecture

```
  grpcurl / client
        │
        ▼
  ┌─────────────┐        ┌──────────────┐
  │ gRPC Server │───────▶│  PostgreSQL  │◀──── Workers
  │  :50051     │        │  (jobs +     │      (claim, execute,
  └─────────────┘        │  exec_log)   │       heartbeat)
                         └──────────────┘
                                │
                         ┌──────────────┐
                         │    Redis     │
                         │ (reaper lock)│
                         └──────────────┘
```

State machine: `pending → running → completed | canceled | dead`

Retries reuse the `pending` state. Orphaned jobs (worker crash) are returned to
`pending` by the reaper and re-executed on a healthy worker.

## Run

```bash
docker compose up --build
```

This starts PostgreSQL, Redis, one worker, and the gRPC server. Migrations run
automatically on startup.

## Submit a job via grpcurl

```bash
# Submit
grpcurl -plaintext localhost:50051 atlas.AtlasQueue/SubmitJob \
  '{"queue":"default","handler_name":"noop_handler","idempotency_key":"my-job-1","max_retries":3}'

# Check status  (paste job_id from above)
grpcurl -plaintext localhost:50051 atlas.AtlasQueue/GetJobStatus \
  '{"job_id":"<job_id>"}'

# Cancel a job
grpcurl -plaintext localhost:50051 atlas.AtlasQueue/CancelJob \
  '{"job_id":"<job_id>"}'

# Stream state-change events (Ctrl-C to stop)
grpcurl -plaintext localhost:50051 atlas.AtlasQueue/StreamJobUpdates \
  '{"job_id":"<job_id>"}'

# Execution history
grpcurl -plaintext localhost:50051 atlas.AtlasQueue/GetJobHistory \
  '{"job_id":"<job_id>"}'
```

Available handlers: `noop_handler`, `slow_handler` (30 s), `fail_handler`,
`fail_then_succeed`, `fatal_handler`.

Available queues: `default`, `emails`, `notifications`.

## Failure recovery

**Worker crash (SIGKILL / OOM)**
The worker holds a 30-second lease on any running job. If the lease is not
renewed — because the worker is dead — the reaper marks the execution row
`orphaned`, returns the job to `pending`, and a healthy worker picks it up.
The job is never lost and never completed twice.

**Worker shutdown (SIGTERM)**
The worker finishes its current job, then exits. The lease expires naturally
and the job is reclaimed by the reaper. The execution is marked `orphaned`,
not `canceled`, so retry budget is preserved.

**Cooperative cancellation**
`CancelJob` on a pending job transitions it to `canceled` immediately.
`CancelJob` on a running job sets `canceled_at`; the worker's cancellation
watcher detects this within ~3 s and stops the handler cleanly.

**Lease fencing**
Each execution has a unique `execution_id`. A stale worker that wakes up
after being considered dead cannot extend the lease or write a completion
unless its `execution_id` still matches the job row. Stale writes are silently
dropped.

**Retries**
A failed job is retried up to `max_retries` times with the attempt counter
incremented. A `FatalError` or exhausted retries moves the job to `dead`.

## Test

```bash
bash tests/phase5_test.sh
```
