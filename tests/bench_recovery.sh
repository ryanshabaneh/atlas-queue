#!/usr/bin/env bash
# Measures how long a crashed worker's in-flight job takes to get picked up
# again by a healthy worker. Runs the same trial set against two timing
# configs so the improvement is measured, not asserted.
#
# Usage: tests/bench_recovery.sh [trials]
set -euo pipefail

TRIALS="${1:-6}"
WORKERS=3
PROJECT_NAME="atlasq-bench"
PG_PORT=55433

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/atlasq_bench.XXXXXX")"
OVERRIDE="$TMP_DIR/docker-compose.bench.yml"
RESULTS="$TMP_DIR/results"

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
  local lease="$1" reaper="$2"
  cat >"$OVERRIDE" <<YAML
services:
  postgres:
    ports:
      - "${PG_PORT}:5432"
  redis:
    ports: []
  worker:
    ports: []
    environment:
      DATABASE_URL: postgres://atlas:atlas@postgres:5432/atlas
      REDIS_URL: redis://redis:6379
      LEASE_SECONDS: "${lease}"
      REAPER_INTERVAL_SECONDS: "${reaper}"
YAML
}

wait_for_pg() {
  local deadline=$((SECONDS + 90))
  until [ "$(sql 'SELECT 1')" = "1" ]; do
    [ "$SECONDS" -ge "$deadline" ] && { echo "postgres never came up" >&2; exit 1; }
    sleep 2
  done
}

# Waits for all worker containers to be running and polling.
wait_for_workers() {
  local deadline=$((SECONDS + 60))
  until [ "$(compose ps -q worker | wc -l | tr -d ' ')" = "$WORKERS" ] &&
        ! compose ps --status exited -q worker | grep -q .; do
    [ "$SECONDS" -ge "$deadline" ] && break
    sleep 1
  done
  sleep 3   # let claim loops settle
}

# Finds the container whose logs announce the given worker UUID.
container_for_worker() {
  local wid="$1" cid
  for cid in $(compose ps -q worker); do
    if docker logs "$cid" 2>&1 | grep -q "$wid"; then
      echo "$cid"
      return 0
    fi
  done
  return 1
}

run_config() {
  local label="$1" lease="$2" reaper="$3"
  write_override "$lease" "$reaper"

  echo
  echo "=== $label (lease=${lease}s, reaper poll=${reaper}s, ${WORKERS} workers) ==="
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  compose up -d --build --scale worker="$WORKERS" postgres redis worker >/dev/null
  wait_for_pg
  wait_for_workers

  local n=0
  while [ "$n" -lt "$TRIALS" ]; do
    n=$((n + 1))
    local key="bench-${label}-$$-${n}"

    # A 30s handler guarantees the job is still in flight when we kill.
    sql "INSERT INTO jobs (queue, handler_name, payload, payload_hash,
           idempotency_key, max_retries)
         VALUES ('default','slow_handler','{}','bench','${key}',5)" >/dev/null

    # Wait for some worker to claim it.
    local wid="" deadline=$((SECONDS + 30))
    until [ -n "$wid" ]; do
      wid="$(sql "SELECT COALESCE(locked_by,'') FROM jobs
                  WHERE idempotency_key='${key}' AND state='running'")"
      [ "$SECONDS" -ge "$deadline" ] && break
      [ -z "$wid" ] && sleep 0.5
    done
    if [ -z "$wid" ]; then
      echo "  trial ${n}: never claimed, skipping" >&2
      continue
    fi

    local cid
    cid="$(container_for_worker "$wid")" || {
      echo "  trial ${n}: could not map worker to container, skipping" >&2
      continue
    }

    # Kill timestamp comes from the DB clock so it shares a clock with locked_at.
    local kill_ts
    kill_ts="$(sql 'SELECT EXTRACT(EPOCH FROM clock_timestamp())')"
    docker kill -s KILL "$cid" >/dev/null

    # Recovery = a *different* worker has the job running again.
    local recovered="" rdeadline=$((SECONDS + 120))
    until [ -n "$recovered" ]; do
      recovered="$(sql "SELECT COALESCE(EXTRACT(EPOCH FROM locked_at)::text,'')
                        FROM jobs
                        WHERE idempotency_key='${key}'
                          AND state='running' AND locked_by <> '${wid}'")"
      [ "$SECONDS" -ge "$rdeadline" ] && break
      [ -z "$recovered" ] && sleep 0.25
    done

    if [ -z "$recovered" ]; then
      echo "  trial ${n}: no recovery within 120s" >&2
    else
      local secs
      secs="$(awk -v a="$recovered" -v b="$kill_ts" 'BEGIN{printf "%.2f", a-b}')"
      echo "  trial ${n}: ${secs}s"
      echo "$secs" >>"$RESULTS.$label"
    fi

    # Clear the job so it stops occupying a worker, then restore the fleet.
    sql "UPDATE jobs SET state='canceled', canceled_at=NOW(), locked_by=NULL,
         lock_expires_at=NULL WHERE idempotency_key='${key}'" >/dev/null
    compose up -d --scale worker="$WORKERS" worker >/dev/null 2>&1
    wait_for_workers
  done
}

percentiles() {
  local label="$1"
  local f="$RESULTS.$label"
  [ -f "$f" ] || { echo "$label: no samples"; return; }
  sort -n "$f" | awk -v label="$label" '
    {v[NR]=$1}
    END {
      if (NR==0) { print label": no samples"; exit }
      p50=v[int((NR-1)*0.5)+1]; p90=v[int((NR-1)*0.9)+1]
      printf "%-10s n=%d  p50=%.1fs  p90=%.1fs  max=%.1fs\n", label, NR, p50, p90, v[NR]
    }'
}

run_config "before" 30 30
run_config "after"  15 5

echo
echo "======== RESULTS (${TRIALS} SIGKILL trials per config) ========"
percentiles "before"
percentiles "after"
