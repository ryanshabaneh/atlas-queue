#!/usr/bin/env bash
# Two benchmarks over the same job set:
#
#   1. Throughput / scaling — drain N jobs with 1, 3, and 6 workers. Shows
#      whether FOR UPDATE SKIP LOCKED actually lets workers scale, or whether
#      they just contend on the same rows.
#   2. Chaos — drain N jobs with 3 workers while SIGKILLing one every 10s,
#      then verify no job was lost and none completed twice.
#
# Usage: tests/bench_throughput.sh [jobs]
set -euo pipefail

JOBS="${1:-10000}"
MODE="${2:-all}"          # "all" or "chaos" to skip the scaling passes
KILL_EVERY=15             # seconds between SIGKILLs during the chaos run
MAX_KILLS=8
PROJECT_NAME="atlasq-thr"
PG_PORT=55434
QUEUE="default"

# Workers seed a 'smoke-test-phase5' job on boot, so every count below is
# scoped to jobs this run inserted rather than to the whole table.
RUN_TAG="thr-$$-"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/atlasq_thr.XXXXXX")"
OVERRIDE="$TMP_DIR/docker-compose.thr.yml"

cleanup() {
  set +e
  docker compose -p "$PROJECT_NAME" -f docker-compose.yml -f "$OVERRIDE" \
    down -v --remove-orphans >/dev/null 2>&1
  rm -rf "$TMP_DIR" >/dev/null 2>&1
}
trap cleanup EXIT

compose() {
  docker compose -p "$PROJECT_NAME" -f docker-compose.yml -f "$OVERRIDE" "$@"
}

sql() {
  compose exec -T postgres psql -U atlas -d atlas -X -qAt -v ON_ERROR_STOP=1 -c "$1" \
    2>/dev/null | head -n 1 | tr -d '\r[:space:]'
}

write_override() {
  cat >"$OVERRIDE" <<YAML
services:
  postgres:
    ports:
      - "${PG_PORT}:5432"
    command:
      - postgres
      - -c
      - max_connections=200
  redis:
    ports: []
  worker:
    ports: []
    environment:
      DATABASE_URL: postgres://atlas:atlas@postgres:5432/atlas
      REDIS_URL: redis://redis:6379
      LEASE_SECONDS: "15"
      REAPER_INTERVAL_SECONDS: "5"
YAML
}

boot() {
  local workers="$1"
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  compose up -d --build --scale worker="$workers" postgres redis worker >/dev/null

  local deadline=$((SECONDS + 120))
  until [ "$(sql 'SELECT 1')" = "1" ]; do
    [ "$SECONDS" -ge "$deadline" ] && { echo "postgres never came up" >&2; exit 1; }
    sleep 2
  done
  # Workers create their tables/rows on boot; give claim loops a moment.
  sleep 5
}

# The concurrency limiter defaults to 10 in-flight per queue, which would cap
# the 6-worker run below its real ceiling. Raise it so the benchmark measures
# claim throughput rather than the limiter.
raise_concurrency_limit() {
  compose exec -T redis redis-cli SET "atlas:queue:${QUEUE}:concurrency_limit" 100 >/dev/null 2>&1 || true
}

enqueue() {
  local handler="${1:-noop_handler}"
  sql "INSERT INTO jobs (queue, handler_name, payload, payload_hash,
         idempotency_key, max_retries)
       SELECT '${QUEUE}', '${handler}', '{}', 'bench',
              '${RUN_TAG}' || g, 10
       FROM generate_series(1, ${JOBS}) g" >/dev/null
}

completed_count() {
  sql "SELECT COUNT(*) FROM jobs
       WHERE state='completed' AND idempotency_key LIKE '${RUN_TAG}%'"
}

# Blocks until every job this run inserted reaches a terminal state.
drain() {
  local deadline=$((SECONDS + 1800)) done_n=0
  while :; do
    done_n="$(sql "SELECT COUNT(*) FROM jobs
                   WHERE state IN ('completed','dead','canceled')
                     AND idempotency_key LIKE '${RUN_TAG}%'")"
    [ -n "$done_n" ] && [ "$done_n" -ge "$JOBS" ] && return 0
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "  TIMEOUT draining (last=${done_n}/${JOBS})" >&2
      return 1
    fi
    sleep 1
  done
}

run_throughput() {
  local workers="$1"
  write_override
  boot "$workers"
  raise_concurrency_limit

  enqueue
  local start end elapsed rate
  start="$(sql 'SELECT EXTRACT(EPOCH FROM clock_timestamp())')"
  drain || true
  end="$(sql 'SELECT EXTRACT(EPOCH FROM clock_timestamp())')"

  local completed
  completed="$(completed_count)"
  elapsed="$(awk -v a="$end" -v b="$start" 'BEGIN{printf "%.2f", a-b}')"
  rate="$(awk -v n="$completed" -v t="$elapsed" 'BEGIN{printf "%.0f", (t>0)? n/t : 0}')"
  printf "  %d worker(s): %s jobs in %ss = %s jobs/sec\n" \
    "$workers" "$completed" "$elapsed" "$rate"
  echo "$workers $rate" >>"$TMP_DIR/rates"
}

# Runs as a background job alongside the drain; the
# kill count is written to a file because it lives in a subshell.
kill_loop() {
  local workers="$1" n=0
  echo 0 >"$TMP_DIR/kills"
  while [ "$n" -lt "$MAX_KILLS" ]; do
    sleep "$KILL_EVERY"
    local cid
    cid="$(compose ps -q --status running worker | head -n 1)"
    [ -z "$cid" ] && continue
    if docker kill -s KILL "$cid" >/dev/null 2>&1; then
      n=$((n + 1))
      echo "$n" >"$TMP_DIR/kills"
    fi
    compose up -d --scale worker="$workers" worker >/dev/null 2>&1
  done
}

run_chaos() {
  local workers=3
  write_override
  boot "$workers"
  raise_concurrency_limit

  # bench_handler, not noop_handler: a job must be in flight long enough for a
  # kill to interrupt it, otherwise nothing ever needs reclaiming.
  enqueue bench_handler
  echo "  draining ${JOBS} jobs across ${workers} workers," \
       "SIGKILLing one every ${KILL_EVERY}s (${MAX_KILLS}x)..."

  # Kills run alongside the drain. The drain outlasts them because every job
  # held by a killed worker must wait out lease expiry plus a reaper sweep
  # before another worker can pick it up.
  kill_loop "$workers" &
  local killer_pid=$!
  drain || true
  kill "$killer_pid" 2>/dev/null || true
  wait "$killer_pid" 2>/dev/null || true

  local completed dupes lost kills orphaned
  kills="$(cat "$TMP_DIR/kills" 2>/dev/null || echo 0)"
  # Executions cut short by a kill and reclaimed by the reaper. A nonzero
  # count is what proves the kills actually interrupted in-flight work.
  orphaned="$(sql "SELECT COUNT(*) FROM execution_log e
                   JOIN jobs j ON j.id = e.job_id
                   WHERE e.outcome='orphaned'
                     AND j.idempotency_key LIKE '${RUN_TAG}%'")"
  completed="$(completed_count)"
  # A job that ran twice would have two 'completed' rows in the append-only log.
  dupes="$(sql "SELECT COUNT(*) FROM (
                  SELECT e.job_id FROM execution_log e
                  JOIN jobs j ON j.id = e.job_id
                  WHERE e.outcome='completed'
                    AND j.idempotency_key LIKE '${RUN_TAG}%'
                  GROUP BY e.job_id HAVING COUNT(*) > 1) d")"
  lost="$(sql "SELECT COUNT(*) FROM jobs
               WHERE state <> 'completed' AND idempotency_key LIKE '${RUN_TAG}%'")"

  echo "  workers SIGKILLed during run:  ${kills}"
  echo "  executions orphaned by kills:  ${orphaned}"
  echo "  completed:                     ${completed}/${JOBS}"
  echo "  jobs with >1 completion:       ${dupes}"
  echo "  jobs not completed:            ${lost}"
}

if [ "$MODE" != "chaos" ]; then
  echo "=== Throughput / scaling (${JOBS} noop jobs) ==="
  for w in 1 3 6; do
    run_throughput "$w"
  done
  echo
fi

echo "=== Chaos (${JOBS} jobs, 3 workers, repeated SIGKILL) ==="
run_chaos

echo
echo "======== SCALING ========"
awk '{r[$1]=$2} END {
  base = r[1]
  for (w in r) order[++n] = w
  printf "  1 worker:  %6d jobs/sec  (1.00x)\n", r[1]
  if (3 in r) printf "  3 workers: %6d jobs/sec  (%.2fx)\n", r[3], r[3]/base
  if (6 in r) printf "  6 workers: %6d jobs/sec  (%.2fx)\n", r[6], r[6]/base
}' "$TMP_DIR/rates"
