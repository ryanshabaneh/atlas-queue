#!/usr/bin/env bash
set -euo pipefail

DB="postgres://atlas:atlas@localhost:55432/atlas"

FAILURES=0
RUN_TOKEN="$(date +%s)-$$"
PROJECT_NAME="atlasq-phase5-test"
SMOKE_KEY="smoke-test-phase5"
TOTAL_TEST5_JOBS=100

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/atlasq_phase5.XXXXXX")"
COMPOSE_OVERRIDE="$TMP_DIR/docker-compose.worker.override.yml"

cleanup() {
  set +e
  if [ -f "$COMPOSE_OVERRIDE" ]; then
    docker compose \
      -p "$PROJECT_NAME" \
      -f docker-compose.yml \
      -f "$COMPOSE_OVERRIDE" \
      down -v --remove-orphans >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR" >/dev/null 2>&1 || true
}
trap cleanup EXIT

pass() {
  echo "PASS: $*"
}

fail() {
  echo "FAIL: $*"
  FAILURES=$((FAILURES + 1))
}

header() {
  echo
  echo "========================================"
  echo "$*"
  echo "========================================"
}

compose() {
  docker compose -p "$PROJECT_NAME" -f docker-compose.yml -f "$COMPOSE_OVERRIDE" "$@"
}

sql() {
  compose exec -T postgres \
    psql -U atlas -d atlas -X -qAt -v ON_ERROR_STOP=1 "$@"
}

sql_val() {
  sql -c "$1" | head -n 1 | tr -d '\r'
}

sql_num() {
  local v
  v="$(sql_val "$1")"
  v="${v//$'\n'/}"
  v="${v//$'\r'/}"
  v="${v//[[:space:]]/}"
  printf '%s\n' "$v"
}

assert_eq() {
  local got="$1"
  local want="$2"
  local msg="$3"
  if [ "$got" != "$want" ]; then
    echo "ASSERT FAIL: $msg (got=$got want=$want)" >&2
    return 1
  fi
}

assert_ne() {
  local got="$1"
  local not_want="$2"
  local msg="$3"
  if [ "$got" = "$not_want" ]; then
    echo "ASSERT FAIL: $msg (unexpected=$got)" >&2
    return 1
  fi
}

assert_nonempty() {
  local got="$1"
  local msg="$2"
  if [ -z "$got" ]; then
    echo "ASSERT FAIL: $msg (empty)" >&2
    return 1
  fi
}

wait_until() {
  local timeout_secs="$1"
  local interval_secs="$2"
  local desc="$3"
  shift 3

  local deadline=$((SECONDS + timeout_secs))
  while true; do
    if "$@"; then
      return 0
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "TIMEOUT: $desc" >&2
      return 1
    fi
    sleep "$interval_secs"
  done
}

wait_sql_eq() {
  local timeout_secs="$1"
  local interval_secs="$2"
  local query="$3"
  local expected="$4"
  local desc="$5"
  local last=""
  local deadline=$((SECONDS + timeout_secs))

  while true; do
    last="$(sql_val "$query")"
    if [ "$last" = "$expected" ]; then
      return 0
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "TIMEOUT: $desc (last=$last expected=$expected)" >&2
      return 1
    fi
    sleep "$interval_secs"
  done
}

wait_sql_num_ge() {
  local timeout_secs="$1"
  local interval_secs="$2"
  local query="$3"
  local min="$4"
  local desc="$5"
  local last="0"
  local deadline=$((SECONDS + timeout_secs))

  while true; do
    last="$(sql_num "$query")"
    if [ -n "$last" ] && [ "$last" -ge "$min" ]; then
      return 0
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "TIMEOUT: $desc (last=$last min=$min)" >&2
      return 1
    fi
    sleep "$interval_secs"
  done
}

write_worker_override() {
  cat >"$COMPOSE_OVERRIDE" <<'YAML'
services:
  postgres:
    ports:
      - "55432:5432"

  worker:
    image: golang:1.22-bookworm
    entrypoint: []
    working_dir: /workspace
    command: ["sh", "-c", "exec go run ./cmd/worker"]
    environment:
      DATABASE_URL: postgres://atlas:atlas@postgres:5432/atlas
      REDIS_URL: redis://redis:6379
      GOMODCACHE: /go/pkg/mod
      GOCACHE: /root/.cache/go-build
    volumes:
      - .:/workspace
      - go_mod_cache:/go/pkg/mod
      - go_build_cache:/root/.cache/go-build
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy

volumes:
  go_mod_cache:
  go_build_cache:
YAML
}

wait_for_postgres() {
  wait_until 60 2 "postgres to accept psql connections" \
    bash -lc "docker compose -p '$PROJECT_NAME' -f docker-compose.yml -f '$COMPOSE_OVERRIDE' exec -T postgres psql -U atlas -d atlas -X -qAt -v ON_ERROR_STOP=1 -c 'SELECT 1' >/dev/null 2>&1"
}

wait_for_redis() {
  wait_until 60 2 "redis to be reachable" \
    compose exec -T redis redis-cli ping >/dev/null
}

bootstrap_stack() {
  write_worker_override
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  compose up -d postgres redis >/dev/null
  wait_for_postgres
  wait_for_redis
  docker run --rm \
    --network "${PROJECT_NAME}_default" \
    -v "$REPO_ROOT:/workspace" \
    -w /workspace \
    -e DATABASE_URL=postgres://atlas:atlas@postgres:5432/atlas \
    golang:1.22-bookworm \
    sh -c "go run ./cmd/migrate"
}

stop_workers() {
  compose stop worker >/dev/null 2>&1 || true
  compose rm -fsv worker >/dev/null 2>&1 || true
}

reset_db_and_redis() {
  sql -c "TRUNCATE TABLE execution_log, jobs, workers RESTART IDENTITY CASCADE;"
  compose exec -T redis redis-cli FLUSHALL >/dev/null
}

wait_active_workers() {
  local expected="$1"
  wait_sql_eq 120 2 \
    "SELECT COUNT(*) FROM workers WHERE status='active';" \
    "$expected" \
    "active workers == $expected"
}

warm_and_start_workers() {
  local n="$1"
  if [ "$n" -le 0 ]; then
    return 0
  fi

  # Start one worker first to warm the Go module/build caches deterministically.
  compose up -d --scale worker=1 worker >/dev/null
  if ! wait_active_workers 1; then
    return 1
  fi

  if [ "$n" -gt 1 ]; then
    compose up -d --scale worker="$n" worker >/dev/null
    if ! wait_active_workers "$n"; then
      return 1
    fi
  fi

  # Allow the startup smoke job to settle. Assertions always filter by test key,
  # but letting the worker become idle avoids noise during the first test action.
  sleep 2
  return 0
}

reset_environment() {
  local worker_count="$1"
  stop_workers
  reset_db_and_redis
  warm_and_start_workers "$worker_count"
}

enqueue_job() {
  local key="$1"
  local handler="$2"
  local queue="${3:-default}"
  local payload="${4}"
  [[ -z "$payload" ]] && payload='{"test":true}'
  local max_retries="${5:-3}"
  local priority="${6:-0}"

  sql_val "
    INSERT INTO jobs
      (queue, handler_name, payload, payload_hash,
       idempotency_key, priority, max_retries, scheduled_at,
       state, state_version)
    VALUES
      ('$queue', '$handler', '$payload'::jsonb,
       'testhash',
       '$key', $priority, $max_retries, NOW(), 'pending', 0)
    RETURNING id;"
}

job_state() {
  local job_id="$1"
  sql_val "SELECT state FROM jobs WHERE id = '$job_id';"
}

job_current_exec() {
  local job_id="$1"
  sql_val "SELECT COALESCE(current_execution_id::text, '') FROM jobs WHERE id = '$job_id';"
}

job_locked_by() {
  local job_id="$1"
  sql_val "SELECT COALESCE(locked_by, '') FROM jobs WHERE id = '$job_id';"
}

job_worker_hostname() {
  local job_id="$1"
  sql_val "
    SELECT w.hostname
    FROM jobs j
    JOIN workers w ON w.id::text = j.locked_by
    WHERE j.id = '$job_id';"
}

wait_job_running() {
  local job_id="$1"
  wait_sql_eq 60 1 \
    "SELECT state FROM jobs WHERE id = '$job_id';" \
    "running" \
    "job $job_id to enter running"
}

wait_job_completed() {
  local job_id="$1"
  wait_sql_eq 240 2 \
    "SELECT state FROM jobs WHERE id = '$job_id';" \
    "completed" \
    "job $job_id to complete"
}

wait_exec_outcome() {
  local exec_id="$1"
  local outcome="$2"
  wait_sql_eq 240 2 \
    "SELECT COALESCE(outcome, '') FROM execution_log WHERE id = '$exec_id';" \
    "$outcome" \
    "execution $exec_id outcome=$outcome"
}

container_for_hostname() {
  local hostname="$1"
  local cid=""
  local h=""
  for cid in $(compose ps -q worker); do
    h="$(docker inspect -f '{{.Config.Hostname}}' "$cid" 2>/dev/null || true)"
    if [ "$h" = "$hostname" ]; then
      printf '%s\n' "$cid"
      return 0
    fi
  done
  return 1
}

kill_worker_for_job() {
  local job_id="$1"
  local signal="$2"
  local hostname cid

  hostname="$(job_worker_hostname "$job_id")"
  if ! assert_nonempty "$hostname" "worker hostname for job $job_id"; then
    return 1
  fi
  cid="$(container_for_hostname "$hostname" || true)"
  if ! assert_nonempty "$cid" "container for hostname $hostname"; then
    return 1
  fi

  docker kill --signal "$signal" "$cid" >/dev/null
  printf '%s\n' "$hostname"
}

stop_one_active_worker() {
  local row worker_id hostname cid
  row="$(sql_val "SELECT id::text || '|' || hostname FROM workers WHERE status='active' ORDER BY last_heartbeat ASC, id ASC LIMIT 1;")"
  if ! assert_nonempty "$row" "active worker row"; then
    return 1
  fi
  worker_id="${row%%|*}"
  hostname="${row#*|}"
  cid="$(container_for_hostname "$hostname" || true)"
  if ! assert_nonempty "$cid" "container for hostname $hostname"; then
    return 1
  fi
  docker stop -t 2 "$cid" >/dev/null
  printf '%s\n' "$worker_id"
}

record_orphan_snapshot() {
  local exec_id="$1"
  local snapshot_file="$2"
  local row
  row="$(sql_val "SELECT id::text || '|' || COALESCE(outcome,'') || '|' || COALESCE(to_char(finished_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.USOF'), '') FROM execution_log WHERE id = '$exec_id';")"
  if ! assert_nonempty "$row" "orphan snapshot row for exec $exec_id"; then
    return 1
  fi
  printf '%s\n' "$row" >>"$snapshot_file"
}

test1_worker_heartbeat() {
  if ! reset_environment 2; then
    return 1
  fi

  local before_count before_max after_recent updated_count
  before_count="$(sql_num "SELECT COUNT(*) FROM workers WHERE status='active';")"
  if ! assert_eq "$before_count" "2" "two workers registered"; then
    return 1
  fi

  before_max="$(sql_val "SELECT to_char(MAX(last_heartbeat), 'YYYY-MM-DD\"T\"HH24:MI:SS.USOF') FROM workers WHERE status='active';")"
  sleep 6

  after_recent="$(sql_num "SELECT COUNT(*) FROM workers WHERE status='active' AND last_heartbeat > NOW() - interval '10 seconds';")"
  if ! assert_eq "$after_recent" "2" "active workers heartbeat updated within 10s"; then
    return 1
  fi

  updated_count="$(sql_num "
    SELECT COUNT(*)
    FROM workers
    WHERE status='active'
      AND to_char(last_heartbeat, 'YYYY-MM-DD\"T\"HH24:MI:SS.USOF') > '$before_max';")"
  if [ "$updated_count" -lt 1 ]; then
    echo "ASSERT FAIL: expected at least one heartbeat tick after 6s" >&2
    return 1
  fi

  return 0
}

test2_sigterm_does_not_cancel_job() {
  if ! reset_environment 2; then
    return 1
  fi

  local key job_id old_exec killed_host state canceled_rows completed_rows orphaned_rows
  key="t2-${RUN_TOKEN}-sigterm"
  # Repo slow_handler sleeps 30s (long enough to force reclaim/orphan behavior).
  job_id="$(enqueue_job "$key" "slow_handler" "default" '{"test":"sigterm"}' 3 0)"
  if ! assert_nonempty "$job_id" "test2 job id"; then
    return 1
  fi

  if ! wait_job_running "$job_id"; then
    return 1
  fi

  old_exec="$(job_current_exec "$job_id")"
  if ! assert_nonempty "$old_exec" "test2 current_execution_id"; then
    return 1
  fi

  killed_host="$(kill_worker_for_job "$job_id" "TERM" || true)"
  if ! assert_nonempty "$killed_host" "killed worker hostname"; then
    return 1
  fi

  # Keep two workers available while preserving the SIGTERM event on the in-flight one.
  compose up -d --scale worker=2 worker >/dev/null

  state="$(job_state "$job_id")"
  if [ "$state" = "canceled" ]; then
    echo "ASSERT FAIL: job was marked canceled after SIGTERM" >&2
    return 1
  fi

  if ! wait_exec_outcome "$old_exec" "orphaned"; then
    return 1
  fi

  if ! wait_job_completed "$job_id"; then
    return 1
  fi

  canceled_rows="$(sql_num "SELECT COUNT(*) FROM execution_log WHERE job_id = '$job_id' AND outcome = 'canceled';")"
  orphaned_rows="$(sql_num "SELECT COUNT(*) FROM execution_log WHERE job_id = '$job_id' AND outcome = 'orphaned';")"
  completed_rows="$(sql_num "SELECT COUNT(*) FROM execution_log WHERE job_id = '$job_id' AND outcome = 'completed';")"

  if ! assert_eq "$canceled_rows" "0" "no execution_log canceled row after SIGTERM shutdown"; then
    return 1
  fi
  if ! assert_eq "$orphaned_rows" "1" "exactly one orphaned execution after SIGTERM"; then
    return 1
  fi
  if ! assert_eq "$completed_rows" "1" "exactly one completed execution after reclaim"; then
    return 1
  fi

  state="$(job_state "$job_id")"
  if ! assert_eq "$state" "completed" "job eventually completed after SIGTERM"; then
    return 1
  fi

  state="$(sql_val "SELECT outcome FROM execution_log WHERE id = '$old_exec';")"
  if ! assert_eq "$state" "orphaned" "original execution remains orphaned"; then
    return 1
  fi

  return 0
}

test3_dead_worker_marking() {
  if ! reset_environment 2; then
    return 1
  fi

  local dead_worker_id status
  dead_worker_id="$(stop_one_active_worker || true)"
  if ! assert_nonempty "$dead_worker_id" "worker id to stop"; then
    return 1
  fi

  if ! wait_sql_eq 75 2 \
    "SELECT status FROM workers WHERE id = '$dead_worker_id';" \
    "dead" \
    "worker $dead_worker_id to be marked dead"; then
    return 1
  fi

  status="$(sql_val "SELECT status FROM workers WHERE id = '$dead_worker_id';")"
  if ! assert_eq "$status" "dead" "dead worker marking"; then
    return 1
  fi

  return 0
}

test4_orphan_requeue() {
  if ! reset_environment 2; then
    return 1
  fi

  local key job_id old_exec state saw_pending exec_rows orphaned_rows new_started
  key="t4-${RUN_TOKEN}-orphan"
  job_id="$(enqueue_job "$key" "slow_handler" "default" '{"test":"orphan_requeue"}' 3 0)"
  if ! assert_nonempty "$job_id" "test4 job id"; then
    return 1
  fi

  if ! wait_job_running "$job_id"; then
    return 1
  fi
  old_exec="$(job_current_exec "$job_id")"
  if ! assert_nonempty "$old_exec" "test4 old exec id"; then
    return 1
  fi

  if ! kill_worker_for_job "$job_id" "KILL" >/dev/null; then
    return 1
  fi
  compose up -d --scale worker=2 worker >/dev/null

  saw_pending=0
  new_started=0
  local deadline=$((SECONDS + 120))
  while [ "$SECONDS" -lt "$deadline" ]; do
    state="$(job_state "$job_id")"
    if [ "$state" = "pending" ]; then
      saw_pending=1
    fi

    exec_rows="$(sql_num "SELECT COUNT(*) FROM execution_log WHERE job_id = '$job_id';")"
    if [ "$exec_rows" -ge 2 ]; then
      new_started=1
    fi

    orphaned_rows="$(sql_num "SELECT COUNT(*) FROM execution_log WHERE job_id = '$job_id' AND outcome = 'orphaned';")"
    if [ "$new_started" -eq 1 ] && [ "$orphaned_rows" -ge 1 ]; then
      break
    fi
    sleep 0.25
  done

  if [ "$saw_pending" -ne 1 ]; then
    echo "ASSERT FAIL: did not observe transient pending state during orphan reclaim" >&2
    return 1
  fi

  if ! wait_exec_outcome "$old_exec" "orphaned"; then
    return 1
  fi

  if ! wait_sql_num_ge 180 2 \
    "SELECT COUNT(*) FROM execution_log WHERE job_id = '$job_id';" \
    2 \
    "new execution row for orphaned job"; then
    return 1
  fi

  if ! wait_job_completed "$job_id"; then
    return 1
  fi

  orphaned_rows="$(sql_num "SELECT COUNT(*) FROM execution_log WHERE job_id = '$job_id' AND outcome = 'orphaned';")"
  if ! assert_eq "$orphaned_rows" "1" "old execution marked orphaned exactly once"; then
    return 1
  fi

  exec_rows="$(sql_num "SELECT COUNT(DISTINCT id) FROM execution_log WHERE job_id = '$job_id';")"
  if [ "$exec_rows" -lt 2 ]; then
    echo "ASSERT FAIL: expected at least two executions for orphan requeue (got $exec_rows)" >&2
    return 1
  fi

  return 0
}

chaos_kill_loop() {
  local prefix="$1"
  local target_workers="$2"
  local max_runtime_secs="$3"
  local end_time=$((SECONDS + max_runtime_secs))
  local ids picked idx i

  RANDOM=12345
  while [ "$SECONDS" -lt "$end_time" ]; do
    if [ "$(sql_num "SELECT COUNT(*) FROM jobs WHERE idempotency_key LIKE '${prefix}-%' AND state = 'completed';")" -ge "$TOTAL_TEST5_JOBS" ]; then
      return 0
    fi

    ids="$(compose ps -q worker || true)"
    if [ -z "$ids" ]; then
      compose up -d --scale worker="$target_workers" worker >/dev/null || true
      sleep 2
      continue
    fi

    set -- $ids
    if [ "$#" -eq 0 ]; then
      sleep 1
      continue
    fi

    idx=$((RANDOM % $# + 1))
    picked=""
    i=1
    for ids in "$@"; do
      if [ "$i" -eq "$idx" ]; then
        picked="$ids"
        break
      fi
      i=$((i + 1))
    done

    if [ -n "$picked" ]; then
      docker kill --signal KILL "$picked" >/dev/null 2>&1 || true
    fi

    sleep 2
    compose up -d --scale worker="$target_workers" worker >/dev/null || true
    sleep 3
  done
}

test5_no_duplicate_completion() {
  if ! reset_environment 4; then
    return 1
  fi

  local prefix i handler payload chaos_pid completed_count total_count dup_count terminal_bad
  prefix="t5-${RUN_TOKEN}"

  for i in $(seq 1 "$TOTAL_TEST5_JOBS"); do
    handler="noop_handler"
    payload='{"test":"bulk_noop"}'
    if [ $((i % 10)) -eq 0 ]; then
      handler="slow_handler"
      payload='{"test":"bulk_slow"}'
    fi
    enqueue_job "${prefix}-${i}" "$handler" "default" "$payload" 3 0 >/dev/null
  done

  chaos_pid=""
  chaos_kill_loop "$prefix" 4 300 &
  chaos_pid=$!

  if ! wait_sql_eq 420 3 \
    "SELECT COUNT(*) FROM jobs WHERE idempotency_key LIKE '${prefix}-%' AND state = 'completed';" \
    "$TOTAL_TEST5_JOBS" \
    "all test5 jobs completed"; then
    kill "$chaos_pid" >/dev/null 2>&1 || true
    wait "$chaos_pid" >/dev/null 2>&1 || true
    return 1
  fi

  kill "$chaos_pid" >/dev/null 2>&1 || true
  wait "$chaos_pid" >/dev/null 2>&1 || true

  completed_count="$(sql_num "SELECT COUNT(*) FROM jobs WHERE idempotency_key LIKE '${prefix}-%' AND state = 'completed';")"
  total_count="$(sql_num "SELECT COUNT(*) FROM jobs WHERE idempotency_key LIKE '${prefix}-%';")"
  terminal_bad="$(sql_num "SELECT COUNT(*) FROM jobs WHERE idempotency_key LIKE '${prefix}-%' AND state IN ('dead','canceled');")"
  dup_count="$(sql_num "
    SELECT COUNT(*) FROM (
      SELECT el.job_id
      FROM execution_log el
      JOIN jobs j ON j.id = el.job_id
      WHERE el.outcome = 'completed'
        AND j.idempotency_key LIKE '${prefix}-%'
      GROUP BY el.job_id
      HAVING COUNT(*) > 1
    ) t;")"

  if ! assert_eq "$total_count" "$TOTAL_TEST5_JOBS" "all test5 jobs inserted"; then
    return 1
  fi
  if ! assert_eq "$completed_count" "$TOTAL_TEST5_JOBS" "all test5 jobs completed"; then
    return 1
  fi
  if ! assert_eq "$terminal_bad" "0" "no test5 jobs dead/canceled"; then
    return 1
  fi
  if ! assert_eq "$dup_count" "0" "no duplicate completions"; then
    return 1
  fi

  return 0
}

test6_lease_fencing() {
  if ! reset_environment 2; then
    return 1
  fi

  local key job_id old_exec fake_exec old_lock_state_after12 old_outcome completed_exec completed_count
  key="t6-${RUN_TOKEN}-lease-fence"
  job_id="$(enqueue_job "$key" "slow_handler" "default" '{"test":"lease_fence"}' 3 0)"
  if ! assert_nonempty "$job_id" "test6 job id"; then
    return 1
  fi

  if ! wait_job_running "$job_id"; then
    return 1
  fi
  old_exec="$(job_current_exec "$job_id")"
  if ! assert_nonempty "$old_exec" "test6 old exec id"; then
    return 1
  fi

  fake_exec="$(python3 -c 'import uuid; print(uuid.uuid4())' 2>/dev/null || python -c 'import uuid; print(uuid.uuid4())')"
  if ! assert_nonempty "$fake_exec" "test6 fake exec id"; then
    return 1
  fi

  # Fault-injection for lease fencing:
  # Keep the same worker lock owner, but swap current_execution_id and shorten
  # the lease to 5s. Without current_execution_id fencing, the stale extendLease
  # loop would still extend this row because locked_by matches.
  if ! assert_eq "$(sql_num "
    WITH bumped AS (
      UPDATE jobs
      SET current_execution_id = '$fake_exec'::uuid,
          lock_expires_at      = NOW() + interval '5 seconds',
          state_version        = state_version + 1,
          updated_at           = NOW()
      WHERE id = '$job_id'
        AND state = 'running'
        AND current_execution_id = '$old_exec'::uuid
      RETURNING 1
    )
    SELECT COUNT(*) FROM bumped;")" "1" "inject stale execution id and 5s lease"; then
    return 1
  fi

  sleep 12

  old_lock_state_after12="$(sql_val "
    SELECT CASE WHEN lock_expires_at < NOW() THEN 'expired' ELSE 'extended' END
    FROM jobs
    WHERE id = '$job_id';")"
  if ! assert_eq "$old_lock_state_after12" "expired" "stale worker could not extend lease after execID mismatch"; then
    return 1
  fi

  # Deterministically emulate reaper actions for the original execution so we
  # can assert stale markCompleted/writeExecLogFinish behavior after the old
  # handler returns.
  if ! assert_eq "$(sql_num "
    WITH x AS (
      UPDATE execution_log
      SET finished_at = NOW(),
          outcome     = 'orphaned'
      WHERE id = '$old_exec'::uuid
        AND finished_at IS NULL
      RETURNING 1
    )
    SELECT COUNT(*) FROM x;")" "1" "mark original exec orphaned"; then
    return 1
  fi

  if ! assert_eq "$(sql_num "
    WITH x AS (
      UPDATE jobs
      SET state                = 'pending',
          locked_by            = NULL,
          locked_at            = NULL,
          lock_expires_at      = NULL,
          current_execution_id = NULL,
          state_version        = state_version + 1,
          updated_at           = NOW()
      WHERE id = '$job_id'
        AND state = 'running'
      RETURNING 1
    )
    SELECT COUNT(*) FROM x;")" "1" "requeue job for new execution"; then
    return 1
  fi

  if ! wait_sql_num_ge 60 1 \
    "SELECT COUNT(*) FROM execution_log WHERE job_id = '$job_id';" \
    2 \
    "new execution started after manual requeue"; then
    return 1
  fi

  if ! wait_job_completed "$job_id"; then
    return 1
  fi

  completed_count="$(sql_num "SELECT COUNT(*) FROM execution_log WHERE job_id = '$job_id' AND outcome = 'completed';")"
  if ! assert_eq "$completed_count" "1" "only one completed execution for test6 job"; then
    return 1
  fi

  completed_exec="$(sql_val "
    SELECT id::text
    FROM execution_log
    WHERE job_id = '$job_id'
      AND outcome = 'completed'
    ORDER BY finished_at DESC
    LIMIT 1;")"
  if ! assert_nonempty "$completed_exec" "completed exec id for test6"; then
    return 1
  fi
  if ! assert_ne "$completed_exec" "$old_exec" "job must complete under a new execID"; then
    return 1
  fi

  old_outcome="$(sql_val "SELECT outcome FROM execution_log WHERE id = '$old_exec';")"
  if ! assert_eq "$old_outcome" "orphaned" "original exec row not overwritten after stale completion attempt"; then
    return 1
  fi

  return 0
}

test7_execution_log_integrity() {
  if ! reset_environment 2; then
    return 1
  fi

  local key job_id old_exec orphan_finished_at_1 orphan_finished_at_2
  local orphan_count completed_count null_finished_after_complete overwritten_count
  key="t7-${RUN_TOKEN}-execlog"
  job_id="$(enqueue_job "$key" "slow_handler" "default" '{"test":"execlog_integrity"}' 3 0)"
  if ! assert_nonempty "$job_id" "test7 job id"; then
    return 1
  fi

  if ! wait_job_running "$job_id"; then
    return 1
  fi
  old_exec="$(job_current_exec "$job_id")"
  if ! assert_nonempty "$old_exec" "test7 old exec id"; then
    return 1
  fi

  if ! kill_worker_for_job "$job_id" "TERM" >/dev/null; then
    return 1
  fi
  compose up -d --scale worker=2 worker >/dev/null

  if ! wait_exec_outcome "$old_exec" "orphaned"; then
    return 1
  fi

  orphan_finished_at_1="$(sql_val "
    SELECT to_char(finished_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.USOF')
    FROM execution_log
    WHERE id = '$old_exec';")"
  if ! assert_nonempty "$orphan_finished_at_1" "orphan finished_at present"; then
    return 1
  fi

  if ! wait_job_completed "$job_id"; then
    return 1
  fi

  sleep 2

  orphan_finished_at_2="$(sql_val "
    SELECT to_char(finished_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.USOF')
    FROM execution_log
    WHERE id = '$old_exec';")"
  if ! assert_eq "$orphan_finished_at_2" "$orphan_finished_at_1" "orphan finished_at not overwritten"; then
    return 1
  fi

  orphan_count="$(sql_num "SELECT COUNT(*) FROM execution_log WHERE job_id = '$job_id' AND outcome = 'orphaned';")"
  completed_count="$(sql_num "SELECT COUNT(*) FROM execution_log WHERE job_id = '$job_id' AND outcome = 'completed';")"
  overwritten_count="$(sql_num "SELECT COUNT(*) FROM execution_log WHERE id = '$old_exec' AND outcome = 'completed';")"
  null_finished_after_complete="$(sql_num "
    SELECT COUNT(*)
    FROM execution_log e
    JOIN jobs j ON j.id = e.job_id
    WHERE j.id = '$job_id'
      AND j.state = 'completed'
      AND e.finished_at IS NULL;")"

  if ! assert_eq "$orphan_count" "1" "exactly one orphaned row retained"; then
    return 1
  fi
  if ! assert_eq "$completed_count" "1" "exactly one completed row retained"; then
    return 1
  fi
  if ! assert_eq "$overwritten_count" "0" "orphaned row outcome not changed to completed"; then
    return 1
  fi
  if ! assert_eq "$null_finished_after_complete" "0" "no unfinished execution_log rows after job completion"; then
    return 1
  fi

  return 0
}

run_test() {
  local label="$1"
  local fn="$2"

  header "$label"
  if "$fn"; then
    pass "$label"
  else
    fail "$label"
  fi
}

main() {
  bootstrap_stack

  run_test "TEST 1 — Worker Heartbeat" test1_worker_heartbeat
  run_test "TEST 2 — SIGTERM Does NOT Cancel Job" test2_sigterm_does_not_cancel_job
  run_test "TEST 3 — Dead Worker Marking" test3_dead_worker_marking
  run_test "TEST 4 — Orphan Requeue" test4_orphan_requeue
  run_test "TEST 5 — No Duplicate Completion" test5_no_duplicate_completion
  run_test "TEST 6 — Lease Fencing" test6_lease_fencing
  run_test "TEST 7 — Execution Log Integrity" test7_execution_log_integrity

  echo
  if [ "$FAILURES" -eq 0 ]; then
    echo "=== ALL PHASE 5 TESTS PASSED ==="
    exit 0
  fi

  echo "=== $FAILURES TESTS FAILED ==="
  exit 1
}

main "$@"
