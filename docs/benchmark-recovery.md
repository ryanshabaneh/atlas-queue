# Crash Recovery Benchmark

How long does a crashed worker's in-flight job take to get picked up again by a
healthy worker?

## Method

`tests/bench_recovery.sh` runs the same trial set against two timing configs on
a 3-worker cluster:

1. Insert a job bound to `slow_handler` (30s), guaranteeing it is still running
   when the worker dies.
2. Wait for a worker to claim it; map that worker's UUID to its container.
3. Read `clock_timestamp()` from Postgres, then `docker kill -s KILL` that
   worker. No SIGTERM, no cleanup — the lease is left dangling.
4. Poll until the job is `running` again under a *different* `locked_by`.
5. Recovery latency = that row's `locked_at` minus the kill timestamp.

Both timestamps come from the Postgres clock, so there is no host/container
clock skew in the measurement.

The fleet is restored to 3 workers between trials. 6 trials per config.

## Results

5 usable trials per config (one per config was skipped when the script could
not map the claiming worker's UUID to its container).

| config                     | p50   | p90   | max   | min   |
| -------------------------- | ----- | ----- | ----- | ----- |
| lease 30s, reaper poll 30s | 54.9s | 54.9s | 56.5s | 34.6s |
| lease 15s, reaper poll 5s  | 19.6s | 19.9s | 20.1s | 17.6s |

**2.8x faster at p50.**

## Why the numbers land where they do

Recovery cannot begin until two things happen in sequence:

1. **The lease expires.** A running worker refreshes `lock_expires_at` every
   `leaseSeconds/3`, so at the moment of the kill the lease has between
   `2/3 * leaseSeconds` and `leaseSeconds` left on it.
2. **The reaper notices.** The elected reaper sweeps for expired leases on a
   fixed tick, so up to one full interval passes before the job is reset to
   `pending`.

That puts recovery in `[2/3 * lease, lease + tick]`:

- 30s/30s config → 20–60s. Observed 34.6–56.5s.
- 15s/5s config → 10–20s. Observed 17.6–20.1s.

The wide `before` spread (34.6s vs 56.5s) is the reaper tick phase: a job whose
lease expires just before a sweep recovers fast, one that expires just after
waits a full 30s. Shrinking the tick to 5s collapses that variance, which is
why the `after` numbers cluster tightly.

## Tuning knobs

Both are environment variables on the worker:

- `LEASE_SECONDS` (default 15) — how long a claim is held before it is
  considered abandoned.
- `REAPER_INTERVAL_SECONDS` (default 5) — how often the elected reaper sweeps
  for expired leases.

The tradeoff is query load against recovery time: a 5s sweep runs 6x more
reclaim queries per minute than a 30s sweep. The sweep is bounded at 500 rows
per cycle and uses `FOR UPDATE SKIP LOCKED`, so it never blocks a worker that
is extending its own lease.

Setting the lease too low is the real risk — if it drops near the handler's
heartbeat interval, a slow-but-healthy worker can have its job reclaimed out
from under it. Lease fencing means the original worker's completion is then
rejected, so the job runs twice rather than being lost.

## Reproducing

```bash
tests/bench_recovery.sh 6
```

Requires Docker. Takes roughly 10–12 minutes, most of it spent waiting out the
`before` config's own recovery latency.

## Caveats

- 5 samples per config. Enough to separate 55s from 20s; not enough for a
  meaningful p99.
- Run on a single machine with all workers, Postgres, and Redis local, so
  network latency is absent.
- The Dockerfile cross-compiles to `GOARCH=amd64`; on an arm64 host the
  worker binaries run emulated. Recovery latency is timer-bound rather than
  CPU-bound, so this should not shift the numbers materially.
