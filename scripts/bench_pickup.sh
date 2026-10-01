#!/usr/bin/env bash
# Benchmarks idle-worker pickup latency for ATLAS_PICKUP_MODE=poll vs listen.
#
# For each mode: fresh postgres + redis, migrations, WORKERS workers, then
# cmd/bench-pickup enqueues N noop jobs and reports p50/p95/p99/max of
# execution_log.started_at - jobs.created_at. Results are printed side by side.
#
# Workers run natively via `go run` in golang:1.22 (same approach as
# tests/phase5_test.sh) rather than the amd64 Dockerfile image, so on arm64
# hosts nothing runs under emulation. Both modes use the identical setup and
# the same jitter seed.
#
# Usage: scripts/bench_pickup.sh
# Env:   WORKERS (default 1)  N (default 300)  MODES (default "poll listen")
#        PG_PORT (default 55433)  SEED (default 1)
set -euo pipefail

WORKERS="${WORKERS:-1}"
N="${N:-300}"
MODES="${MODES:-poll listen}"
PG_PORT="${PG_PORT:-55433}"
SEED="${SEED:-1}"

PROJECT_NAME="atlasq-bench-pickup"
DB_URL="postgres://atlas:atlas@localhost:${PG_PORT}/atlas"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/atlasq_bench.XXXXXX")"
COMPOSE_OVERRIDE="$TMP_DIR/docker-compose.bench.override.yml"

# Go caches live in volumes outside the compose project so `down -v` between
# modes does not force a full recompile (which would only slow startup; the
# bench waits for idle workers either way).
GOMOD_VOLUME="atlasq-bench-gomod"
GOBUILD_VOLUME="atlasq-bench-gobuild"

compose() {
  docker compose -p "$PROJECT_NAME" -f docker-compose.yml -f "$COMPOSE_OVERRIDE" "$@"
}

cleanup() {
  set +e
  compose down -v --remove-orphans >/dev/null 2>&1
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

cat >"$COMPOSE_OVERRIDE" <<YAML
services:
  postgres:
    ports:
      - "${PG_PORT}:5432"

  worker:
    image: golang:1.22-bookworm
    entrypoint: []
    working_dir: /workspace
    command: ["sh", "-c", "exec go run ./cmd/worker"]
    environment:
      DATABASE_URL: postgres://atlas:atlas@postgres:5432/atlas
      REDIS_URL: redis://redis:6379
      ATLAS_PICKUP_MODE: \${ATLAS_PICKUP_MODE}
      GOMODCACHE: /go/pkg/mod
      GOCACHE: /root/.cache/go-build
    volumes:
      - .:/workspace
      - ${GOMOD_VOLUME}:/go/pkg/mod
      - ${GOBUILD_VOLUME}:/root/.cache/go-build

volumes:
  ${GOMOD_VOLUME}:
    external: true
  ${GOBUILD_VOLUME}:
    external: true
YAML

docker volume create "$GOMOD_VOLUME" >/dev/null
docker volume create "$GOBUILD_VOLUME" >/dev/null

# Build the host-side tools once, up front.
go build -o "$TMP_DIR/bench-pickup" ./cmd/bench-pickup
go build -o "$TMP_DIR/migrate" ./cmd/migrate

wait_for_postgres() {
  local deadline=$((SECONDS + 60))
  until compose exec -T postgres pg_isready -U atlas -d atlas >/dev/null 2>&1; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "timed out waiting for postgres" >&2
      return 1
    fi
    sleep 1
  done
}

run_mode() {
  local mode="$1"
  local out="$TMP_DIR/$mode.out"
  export ATLAS_PICKUP_MODE="$mode"

  echo
  echo "=== mode=$mode workers=$WORKERS n=$N ==="
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  compose up -d postgres redis >/dev/null 2>&1
  wait_for_postgres

  # Migrate once before starting workers so concurrent workers do not race
  # on schema creation.
  DATABASE_URL="$DB_URL" "$TMP_DIR/migrate" >/dev/null

  compose up -d --scale worker="$WORKERS" worker >/dev/null 2>&1

  # -startup-timeout covers the first `go run` compile in a cold cache.
  DATABASE_URL="$DB_URL" "$TMP_DIR/bench-pickup" \
    -n "$N" -min-workers "$WORKERS" -seed "$SEED" -label "$mode" \
    -startup-timeout 5m | tee "$out" || true

  # Confirm every worker actually ran in the requested mode, so a broken env
  # plumb-through can't silently benchmark the default.
  local in_mode
  in_mode="$(compose logs worker 2>/dev/null | grep -c "\"pickup_mode\":\"$mode\"" || true)"
  if [ "$in_mode" -lt "$WORKERS" ]; then
    echo "ERROR: only $in_mode/$WORKERS worker(s) logged pickup_mode=$mode" >&2
    echo "RESULT label=$mode status=WRONG_MODE" >>"$out"
  fi

  compose down -v --remove-orphans >/dev/null 2>&1 || true
}

for mode in $MODES; do
  run_mode "$mode"
done

# result_field <mode> <key> → value of key=value on that mode's RESULT line.
result_field() {
  local line
  line="$(grep '^RESULT ' "$TMP_DIR/$1.out" 2>/dev/null | tail -n 1)"
  local v
  v="$(printf '%s\n' "$line" | tr ' ' '\n' | sed -n "s/^$2=//p")"
  printf '%s' "${v:--}"
}

echo
echo "=== pickup latency: workers=$WORKERS n=$N seed=$SEED ==="
printf '%-12s' "metric"
for mode in $MODES; do printf '%12s' "$mode"; done
echo
for key in status completed incomplete p50_ms p95_ms p99_ms max_ms avg_ms; do
  printf '%-12s' "$key"
  for mode in $MODES; do printf '%12s' "$(result_field "$mode" "$key")"; done
  echo
done

status=0
for mode in $MODES; do
  [ "$(result_field "$mode" status)" = "OK" ] || status=1
done
exit "$status"
