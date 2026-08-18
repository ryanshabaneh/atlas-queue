# Throughput and Chaos Benchmark

Two questions: how fast does the queue drain, and does it stay correct when
workers die mid-job?

Both are run by `tests/bench_throughput.sh`. Every count is scoped to the jobs
a given run inserted — workers seed a `smoke-test-phase5` job on boot, so
unscoped counts are off by one.

## Throughput and scaling

10,000 `noop_handler` jobs, drained from a cold queue.

| workers | drain time | throughput    | vs 1 worker |
| ------- | ---------- | ------------- | ----------- |
| 1       | 21.9s      | 456 jobs/sec  | 1.00x       |
| 3       | 15.0s      | 668 jobs/sec  | 1.46x       |
| 6       | 8.3s       | 1206 jobs/sec | 2.64x       |

Scaling is real but sublinear: 6x the workers buys 2.6x the throughput.

The bottleneck is Postgres round trips, not claim contention. Every job costs
roughly four sequential database operations — `ClaimJob`, the `execution_log`
start row, the completion update, plus a Redis call to release the in-flight
slot — and the worker loop runs them one job at a time before claiming the
next. At 456 jobs/sec a single worker is spending about 2.2ms per job, which
is round-trip latency, not work.

`FOR UPDATE SKIP LOCKED` is doing its job here: if workers were contending for
the same rows, throughput would flatten or degrade as workers were added
rather than continuing to climb. Batched claims and a pipelined completion
path are the obvious next lever.

Note the concurrency limiter defaults to 10 in-flight per queue, which would
cap the 6-worker run. The benchmark raises it so that it measures claim
throughput rather than the limiter.

## Chaos: correctness under repeated SIGKILL

10,000 `bench_handler` jobs (50ms each) across 3 workers, with one worker
SIGKILLed every 15 seconds and the fleet restored after each kill.

`bench_handler` sleeps rather than returning immediately on purpose. Against
`noop_handler` the same test killed 5 workers and orphaned **zero**
executions — a job is only held for ~2ms, so every kill landed between jobs
and nothing ever needed reclaiming. The test passed while proving nothing. A
50ms handler is ~25x the claim/complete cycle, so kills land inside a running
handler.

| measure                      | result       |
| ---------------------------- | ------------ |
| workers SIGKILLed            | 8            |
| executions orphaned by kills | 6            |
| completed                    | 10000 /10000 |
| jobs with >1 completion      | 0            |
| jobs not completed           | 0            |

Six executions were interrupted mid-handler, reclaimed by the reaper, and
re-run on a different worker. Every one of the 10,000 jobs still has exactly
one `completed` row in the append-only `execution_log`.

That is the interesting result. Nothing was lost, and nothing ran to
completion twice, even though six jobs genuinely executed more than once —
the killed attempt is recorded as `orphaned`, and lease fencing means a
resurrected worker cannot mark a job complete after its lease has been
reclaimed.

## Reproducing

```bash
tests/bench_throughput.sh 10000          # scaling passes, then chaos
tests/bench_throughput.sh 10000 chaos    # chaos only
```

Requires Docker. The full run takes roughly 8 minutes; chaos alone about 4.

## Caveats

- Single machine: workers, Postgres, and Redis are all local containers, so
  these numbers carry no network latency. Real throughput over a network will
  be lower and the round-trip bottleneck more pronounced.
- The Dockerfile cross-compiles to `GOARCH=amd64`; on an arm64 host the worker
  binaries run emulated, which depresses the absolute throughput numbers. The
  scaling *ratios* are unaffected.
- Throughput was measured once per worker count, not averaged over repeats.
