# Atlas Queue

Atlas Queue is a distributed job processing system designed to provide
crash-safe execution, idempotent submission, and deterministic recovery
without relying on external brokers like Kafka or SQS.

It uses PostgreSQL row-level leasing (`FOR UPDATE SKIP LOCKED`), execution-ID fencing,
and advisory-lock reaper election to guarantee zero duplicate completion under worker failure.

**Stack:** Go, PostgreSQL, gRPC, Docker

## Guarantees

- Exactly-once completion; at-least-once execution (handlers should be idempotent)
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
  ┌─────────────┐        ┌──────────────┐  claim / heartbeat  ┌──────────────┐
  │ gRPC Server │───────▶│  PostgreSQL  │◀────────────────────│   Workers    │
  │  :50051     │        │  jobs        │                     │  (one holds  │
  └─────────────┘        │  exec_log    │────────────────────▶│  the reaper  │
                         │  workers     │  NOTIFY job_ready   │  advisory    │
                         └──────────────┘                     │  lock)       │
                                                              └──────────────┘
```

PostgreSQL is the only coordination point: it is the queue, the lease
store, the wakeup channel (LISTEN/NOTIFY) and the reaper election (advisory
lock). Redis runs alongside for the per-queue concurrency limiter, which is
not yet enforced.

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

## Job pickup

Idle workers block on PostgreSQL `LISTEN job_ready` instead of polling.
Triggers (migration `005`) call `pg_notify('job_ready', queue)` whenever a
job becomes claimable: on insert as `pending`, and when a retry or reaper
reclaim moves a job back to `pending`.

Each worker loop:

1. Drains: claims and runs jobs until `ClaimJob` returns nothing.
2. Waits on its dedicated LISTEN connection until a notification for one of
   its queues arrives, or a timeout of
   `min(5s fallback, time until the next pending scheduled_at)` elapses.

The `scheduled_at`-aware timeout keeps delayed jobs and retry backoffs prompt
without fast polling. The 5 s fallback covers any missed notification. If the
LISTEN connection drops, the worker polls every 500 ms while it reconnects
(backoff 500 ms → 10 s). Several workers waking on one notification is fine:
`FOR UPDATE SKIP LOCKED` hands the job to exactly one of them.

`ATLAS_PICKUP_MODE=poll` restores the original 500 ms polling loop
(default: `listen`).

### Benchmark

```bash
scripts/bench_pickup.sh              # WORKERS=1 N=300 by default
WORKERS=4 scripts/bench_pickup.sh
```

For each mode the script starts a fresh stack and enqueues `N` `noop_handler`
jobs 50–100 ms apart so they arrive at idle workers. It reports pickup latency
as `execution_log.started_at - jobs.created_at`, with both timestamps taken
from the database clock. A run with any incomplete job is marked `INVALID` and
reports no percentiles.

Pickup latency in ms (workers=1, n=300, seed=1; all 300 jobs completed in
both modes; Docker Desktop on an arm64 Mac):

| | poll | listen |
|---|---|---|
| p50 | 263.2 | 4.4 |
| p95 | 478.4 | 31.8 |
| p99 | 498.0 | 33.7 |
| max | 521.0 | 111.6 |
| avg | 257.6 | 10.2 |

## Failure recovery

**Worker crash (SIGKILL / OOM)**
The worker holds a 30-second lease on any running job. If the lease is not
renewed because the worker is dead the reaper marks the execution row
`orphaned`, returns the job to `pending`, and a healthy worker picks it up.
The job is never lost and never completed twice.

**Worker shutdown (SIGTERM)**
SIGTERM cancels the running handler's context. The job keeps its lease and is
reclaimed by the reaper once the lease expires. The execution is marked
`orphaned`, not `canceled`, so retry budget is preserved.

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
