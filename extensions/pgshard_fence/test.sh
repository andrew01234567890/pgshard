#!/usr/bin/env bash
set -Eeuo pipefail

readonly image="${PGSHARD_AGENT_RUNTIME_TEST_IMAGE:?PGSHARD_AGENT_RUNTIME_TEST_IMAGE is required}"
readonly suffix="${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-$$"
readonly container="pgshard-target-fence-${suffix}"
readonly data_volume="pgshard-target-fence-data-${suffix}"
readonly socket_volume="pgshard-target-fence-socket-${suffix}"
readonly test_image="pgshard/target-fence-test:${suffix}"
fixture_dir="$(mktemp -d "${RUNNER_TEMP:-/tmp}/pgshard-target-fence.XXXXXX")"
readonly fixture_dir
readonly hba_file="$fixture_dir/pg_hba.conf"

cleanup() {
  docker rm --force "$container" >/dev/null 2>&1 || true
  docker volume rm --force "$data_volume" "$socket_volume" >/dev/null 2>&1 || true
  docker image rm --force "$test_image" >/dev/null 2>&1 || true
  rm -f "$hba_file"
  rmdir "$fixture_dir" 2>/dev/null || true
}
trap cleanup EXIT

printf '%s\n' \
  'local postgres postgres peer' \
  'local all all reject' \
  'local replication all reject' \
  'host replication postgres 127.0.0.1/32 trust' \
  'host all all 127.0.0.1/32 trust' \
  'host all all ::1/128 trust' >"$hba_file"
chmod 0444 "$hba_file"

docker volume create "$data_volume" >/dev/null
docker volume create "$socket_volume" >/dev/null
docker run --rm --user 0:0 \
  --volume "$data_volume:/pgdata" \
  --volume "$socket_volume:/socket" \
  --entrypoint /bin/sh "$image" -ceu '
    chown 999:999 /pgdata /socket
    chmod 0700 /socket
  '
docker run --rm --user 999:999 \
  --volume "$data_volume:/pgdata" \
  --entrypoint /usr/lib/postgresql/18/bin/initdb "$image" \
  --pgdata=/pgdata --username=postgres --auth-local=peer --auth-host=trust \
  --no-instructions >/dev/null

start_postgres() {
  local synchronous_standby_names="${1:-}"
  docker run --detach --name "$container" --user 999:999 \
    --volume "$data_volume:/pgdata" \
    --volume "$socket_volume:/socket" \
    --mount "type=bind,src=$hba_file,dst=/tmp/pgshard-target-fence-hba.conf,readonly" \
    --entrypoint /usr/lib/postgresql/18/bin/postgres "$image" \
    -D /pgdata \
    -c listen_addresses=127.0.0.1 \
    -c hba_file=/tmp/pgshard-target-fence-hba.conf \
    -c unix_socket_directories=/socket \
    -c unix_socket_permissions=0700 \
    -c shared_preload_libraries=pgshard_fence \
    -c "synchronous_standby_names=$synchronous_standby_names" \
    -c synchronous_commit=on >/dev/null

  for _ in $(seq 1 120); do
    if docker exec --user 999:999 "$container" psql -X --no-password \
      --host=/socket --username=postgres --dbname=postgres \
      --tuples-only --no-align --command='SELECT 1' >/dev/null 2>&1; then
      return 0
    fi
    if [[ "$(docker inspect --format '{{.State.Running}}' "$container")" != true ]]; then
      docker logs "$container" >&2
      return 1
    fi
    sleep 0.25
  done
  docker logs "$container" >&2
  return 1
}

stop_postgres() {
  docker stop --time 10 "$container" >/dev/null
  docker rm "$container" >/dev/null
}

control_sql() {
  docker exec --user 999:999 "$container" psql -X --no-password \
    --host=/socket --username=postgres --dbname=postgres \
    --set=ON_ERROR_STOP=1 --tuples-only --no-align --command="$1"
}

ordinary_sql() {
  docker exec --user 999:999 "$container" psql -X --no-password \
    --host=127.0.0.1 --username=postgres --dbname=postgres \
    --set=ON_ERROR_STOP=1 --tuples-only --no-align --command="$1"
}

remote_identify_system() {
  docker exec --user 999:999 "$container" psql -X --no-password \
    --dbname='host=127.0.0.1 port=5432 user=postgres replication=true sslmode=disable' \
    --set=ON_ERROR_STOP=1 --tuples-only --no-align \
    --command='IDENTIFY_SYSTEM'
}

expect_ordinary_failure() {
  if ordinary_sql 'SELECT 1' >/dev/null 2>&1; then
    echo "ordinary session unexpectedly passed the target fence: $1" >&2
    return 1
  fi
}

start_postgres 'ANY 1 (pgshard_member_0001)'
expect_ordinary_failure disarmed
if remote_identify_system >/dev/null 2>&1; then
  echo "remote replication unexpectedly passed the disarmed target fence" >&2
  exit 1
fi
docker build \
  --file deploy/images/rust.Dockerfile \
  --target postgres-fence-test-runner \
  --build-arg PGSHARD_BUILD_VERSION=0.0.0-dev+target-fence-test \
  --build-arg PGSHARD_GIT_SHA="$(git rev-parse HEAD)" \
  --tag "$test_image" . >/dev/null
docker run --rm --user 999:999 \
  --volume "$socket_volume:/socket" \
  --env PGSHARD_TARGET_FENCE_TEST_SOCKET=/socket \
  "$test_image" \
  postgres_fence::tests::live_postgres18_installs_renews_and_detects_control_session_loss \
  --ignored --exact
remote_identity="$(remote_identify_system)"
if [[ ! "$remote_identity" =~ ^[1-9][0-9]*\|[1-9][0-9]*\|[0-9A-F]+/[0-9A-F]+\|$ ]]; then
  echo "remote replication returned an invalid post-install identity" >&2
  exit 1
fi

stop_postgres
start_postgres
expect_ordinary_failure rust_session_restart
control_sql 'ALTER FUNCTION pg_catalog.pgshard_fence_install(bytea, bytea) SECURITY DEFINER' >/dev/null
docker run --rm --user 999:999 \
  --volume "$socket_volume:/socket" \
  --env PGSHARD_TARGET_FENCE_TEST_SOCKET=/socket \
  "$test_image" \
  postgres_fence::tests::live_postgres18_rejects_incompatible_extension_before_installation \
  --ignored --exact
expect_ordinary_failure incompatible_catalog
if remote_identify_system >/dev/null 2>&1; then
  echo "remote replication passed after incompatible target rejection" >&2
  exit 1
fi
control_sql 'ALTER FUNCTION pg_catalog.pgshard_fence_install(bytea, bytea) SECURITY INVOKER' >/dev/null
control_sql 'CREATE EXTENSION IF NOT EXISTS pgshard_fence WITH SCHEMA pg_catalog' >/dev/null

boottime_ns="$(docker exec "$container" awk '{printf "%.0f\n", $1 * 1000000000}' /proc/uptime)"
readonly boottime_ns
deadline_ns="$((boottime_ns + 10000000000))"
regressive_deadline_ns="$((boottime_ns + 5000000000))"
printf -v deadline_hex '%016x' "$deadline_ns"
printf -v regressive_deadline_hex '%016x' "$regressive_deadline_ns"
readonly deadline_hex regressive_deadline_hex

ack="$(control_sql "
  SELECT encode(installed_identity, 'hex') || '|' ||
         encode(installed_deadline_boottime_ns, 'hex')
  FROM pg_catalog.pgshard_fence_install(
    decode('010203', 'hex'), decode('$deadline_hex', 'hex'))
")"
test "$ack" = "010203|$deadline_hex"
test "$(ordinary_sql 'SELECT 1')" = "1"

if control_sql "
  SELECT * FROM pg_catalog.pgshard_fence_install(
    decode('010204', 'hex'), decode('$deadline_hex', 'hex'))
" >/dev/null 2>&1; then
  echo "target fence accepted an identity change" >&2
  exit 1
fi
if control_sql "
  SELECT * FROM pg_catalog.pgshard_fence_install(
    decode('010203', 'hex'), decode('$regressive_deadline_hex', 'hex'))
" >/dev/null 2>&1; then
  echo "target fence accepted a deadline regression" >&2
  exit 1
fi

extended_deadline_ns="$((deadline_ns + 2000000000))"
printf -v extended_deadline_hex '%016x' "$extended_deadline_ns"
extended_ack="$(control_sql "
  SELECT encode(installed_identity, 'hex') || '|' ||
         encode(installed_deadline_boottime_ns, 'hex')
  FROM pg_catalog.pgshard_fence_install(
    decode('010203', 'hex'), decode('$extended_deadline_hex', 'hex'))
")"
test "$extended_ack" = "010203|$extended_deadline_hex"

sleep 13
expect_ordinary_failure expired
renewed_deadline_ns="$((extended_deadline_ns + 10000000000))"
printf -v renewed_deadline_hex '%016x' "$renewed_deadline_ns"
if control_sql "
  SELECT * FROM pg_catalog.pgshard_fence_install(
    decode('010203', 'hex'), decode('$renewed_deadline_hex', 'hex'))
" >/dev/null 2>&1; then
  echo "target fence rearmed after authority expiry" >&2
  exit 1
fi

stop_postgres
start_postgres
expect_ordinary_failure restart
