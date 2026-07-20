#!/usr/bin/env bash
set -Eeuo pipefail

readonly image="${PGSHARD_AGENT_RUNTIME_TEST_IMAGE:?PGSHARD_AGENT_RUNTIME_TEST_IMAGE is required}"
readonly suffix="${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-$$"
readonly container="pgshard-target-fence-${suffix}"
readonly data_volume="pgshard-target-fence-data-${suffix}"
readonly socket_volume="pgshard-target-fence-socket-${suffix}"
readonly vanilla_container="pgshard-target-fence-vanilla-${suffix}"
readonly vanilla_data_volume="pgshard-target-fence-vanilla-data-${suffix}"
readonly asset_container="pgshard-target-fence-assets-${suffix}"
readonly vanilla_image='docker.io/library/postgres:18@sha256:3a82e1f56c8f0f5616a11103ac3d47e632c3938698946a7ad26da0df1334744a'
readonly test_image="pgshard/target-fence-test:${suffix}"
fixture_dir="$(mktemp -d "${RUNNER_TEMP:-/tmp}/pgshard-target-fence.XXXXXX")"
readonly fixture_dir
readonly hba_file="$fixture_dir/pg_hba.conf"
readonly control_fifo="$fixture_dir/control.fifo"
readonly control_output="$fixture_dir/control.out"
readonly control_error="$fixture_dir/control.err"
readonly idle_fifo="$fixture_dir/idle.fifo"
readonly idle_output="$fixture_dir/idle.out"
readonly idle_error="$fixture_dir/idle.err"
readonly active_output="$fixture_dir/active.out"
readonly active_error="$fixture_dir/active.err"
readonly walsender_output="$fixture_dir/walsender.out"
readonly walsender_error="$fixture_dir/walsender.err"
control_client_pid=""
idle_client_pid=""
active_client_pid=""
walsender_client_pid=""
background_worker_pid=""
barrier_worker_pid=""

cleanup() {
  docker rm --force "$container" >/dev/null 2>&1 || true
  docker rm --force "$vanilla_container" "$asset_container" >/dev/null 2>&1 || true
  for pid in "$control_client_pid" "$idle_client_pid" "$active_client_pid" "$walsender_client_pid"; do
    if [[ -n "$pid" ]]; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
    fi
  done
  docker volume rm --force "$data_volume" "$socket_volume" \
    "$vanilla_data_volume" >/dev/null 2>&1 || true
  docker image rm --force "$test_image" >/dev/null 2>&1 || true
  rm -f "$hba_file" "$control_fifo" "$control_output" "$control_error" \
    "$idle_fifo" "$idle_output" "$idle_error" \
    "$active_output" "$active_error" "$walsender_output" "$walsender_error" \
    "$fixture_dir/pgshard_fence.so"
  rmdir "$fixture_dir" 2>/dev/null || true
}
trap cleanup EXIT

diagnose_failure() {
  if docker inspect "$container" >/dev/null 2>&1; then
    docker logs "$container" >&2 || true
  fi
  test ! -s "$control_error" || cat "$control_error" >&2
  test ! -s "$idle_error" || cat "$idle_error" >&2
  test ! -s "$active_error" || cat "$active_error" >&2
  test ! -s "$walsender_error" || cat "$walsender_error" >&2
}
trap diagnose_failure ERR

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
  local autovacuum="${2:-on}"
  local log_min_messages="${3:-warning}"
  local post_auth_delay="${4:-0}"

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
    -c autovacuum="$autovacuum" \
    -c log_min_messages="$log_min_messages" \
    -c post_auth_delay="$post_auth_delay" \
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
  docker kill --signal=QUIT "$container" >/dev/null
  for _ in $(seq 1 120); do
    if [[ "$(docker inspect --format '{{.State.Running}}' "$container")" != true ]]; then
      break
    fi
    sleep 0.05
  done
  test "$(docker inspect --format '{{.State.Running}}' "$container")" != true
  docker rm "$container" >/dev/null
  if [[ -n "$control_client_pid" ]]; then
    exec 3>&-
    wait "$control_client_pid" >/dev/null 2>&1 || true
    control_client_pid=""
  fi
}

prove_expired_shutdown_wal_is_fenced() {
  local exit_code
  local postgres_logs

  docker kill --signal=TERM "$container" >/dev/null
  for _ in $(seq 1 200); do
    if [[ "$(docker inspect --format '{{.State.Running}}' "$container")" != true ]]; then
      break
    fi
    sleep 0.05
  done
  if [[ "$(docker inspect --format '{{.State.Running}}' "$container")" == true ]]; then
    echo 'PostgreSQL did not fail closed after the fenced shutdown checkpoint' >&2
    docker logs "$container" >&2
    return 1
  fi
  exit_code="$(docker inspect --format '{{.State.ExitCode}}' "$container")"
  if (( exit_code == 0 )); then
    echo 'fenced shutdown checkpoint exited successfully instead of fail-closed' >&2
    docker logs "$container" >&2
    return 1
  fi
  postgres_logs="$(docker logs "$container" 2>&1)"
  if ! grep --extended-regexp \
    'PANIC: +pgshard writable authority does not permit WAL insertion' \
    <<<"$postgres_logs" >/dev/null; then
    echo 'expired shutdown did not reach the primary WAL fence:' >&2
    printf '%s\n' "$postgres_logs" >&2
    return 1
  fi
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

ordinary_sql_with_timeout() {
  docker exec --user 999:999 \
    --env 'PGOPTIONS=-c statement_timeout=3000' "$container" \
    psql -X --no-password \
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

prove_core_abi_mismatch_fails_closed() {
  local vanilla_logs

  docker create --name "$asset_container" "$image" >/dev/null
  docker cp \
    "$asset_container:/usr/lib/postgresql/18/lib/pgshard_fence.so" \
    "$fixture_dir/pgshard_fence.so"
  docker rm "$asset_container" >/dev/null

  docker volume create "$vanilla_data_volume" >/dev/null
  docker run --rm --user 0:0 \
    --volume "$vanilla_data_volume:/pgdata" \
    --entrypoint /bin/sh "$vanilla_image" -ceu \
    'chown 999:999 /pgdata' >/dev/null
  docker run --rm --user 999:999 \
    --volume "$vanilla_data_volume:/pgdata" \
    --entrypoint /usr/lib/postgresql/18/bin/initdb "$vanilla_image" \
    --pgdata=/pgdata --username=postgres --auth=trust \
    --no-instructions >/dev/null
  if docker run --name "$vanilla_container" --user 999:999 \
    --volume "$vanilla_data_volume:/pgdata" \
    --mount "type=bind,src=$fixture_dir/pgshard_fence.so,dst=/usr/lib/postgresql/18/lib/pgshard_fence.so,readonly" \
    --entrypoint /usr/lib/postgresql/18/bin/postgres "$vanilla_image" \
    -D /pgdata -c shared_preload_libraries=pgshard_fence >/dev/null 2>&1; then
    echo "patched fence extension loaded into an unpatched PostgreSQL core" >&2
    return 1
  fi
  vanilla_logs="$(docker logs "$vanilla_container" 2>&1)"
  if ! grep --fixed-strings 'undefined symbol: PgshardFence' \
    <<<"$vanilla_logs" >/dev/null; then
    echo "vanilla PostgreSQL failed for an unexpected reason:" >&2
    printf '%s\n' "$vanilla_logs" >&2
    return 1
  fi
  docker rm "$vanilla_container" >/dev/null
  docker volume rm "$vanilla_data_volume" >/dev/null
}

wait_for_file_line() {
  local file="$1"
  local expected="$2"
  local description="$3"

  for _ in $(seq 1 200); do
    if grep --fixed-strings --line-regexp "$expected" "$file" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.05
  done
  echo "timed out waiting for $description" >&2
  test ! -s "$control_error" || cat "$control_error" >&2
  return 1
}

wait_for_sql_value() {
  local query="$1"
  local expected="$2"
  local description="$3"
  local actual=""

  for _ in $(seq 1 200); do
    actual="$(control_sql "$query")"
    if [[ "$actual" == "$expected" ]]; then
      return 0
    fi
    sleep 0.05
  done
  echo "timed out waiting for $description; last value: $actual" >&2
  return 1
}

wait_until_boottime_after() {
  local threshold_ns="$1"
  local current_ns=""

  for _ in $(seq 1 500); do
    current_ns="$(docker exec "$container" \
      awk '{printf "%.0f\n", $1 * 1000000000}' /proc/uptime)"
    if (( current_ns > threshold_ns )); then
      return 0
    fi
    sleep 0.05
  done
  echo "timed out waiting to pass CLOCK_BOOTTIME deadline $threshold_ns" >&2
  return 1
}

wait_for_container_log() {
  local expected="$1"
  local description="$2"

  for _ in $(seq 1 200); do
    if docker logs "$container" 2>&1 | grep --fixed-strings "$expected" >/dev/null; then
      return 0
    fi
    sleep 0.05
  done
  echo "timed out waiting for $description" >&2
  docker logs "$container" >&2
  return 1
}

control_write() {
  printf '%s\n' "$1" >&3
}

start_control() {
  local owner_identity_hex="$1"
  local owner_deadline_hex="$2"

  rm -f "$control_fifo"
  : >"$control_output"
  : >"$control_error"
  mkfifo "$control_fifo"
  exec 3<>"$control_fifo"
  docker exec -i --user 999:999 "$container" \
    stdbuf --output=L psql -X --no-password --quiet \
    --host=/socket --username=postgres --dbname=postgres \
    --set=ON_ERROR_STOP=1 --tuples-only --no-align \
    <"$control_fifo" >"$control_output" 2>"$control_error" &
  control_client_pid=$!

  control_write "
    CREATE EXTENSION IF NOT EXISTS pgshard_fence WITH SCHEMA pg_catalog;
    SELECT 'OWNER|' || pg_catalog.pg_backend_pid() || '|' ||
           encode(installed_identity, 'hex') || '|' ||
           encode(installed_deadline_boottime_ns, 'hex')
    FROM pg_catalog.pgshard_fence_install(
      decode('$owner_identity_hex', 'hex'), decode('$owner_deadline_hex', 'hex'));
  "
  for _ in $(seq 1 200); do
    if grep --extended-regexp --line-regexp \
      "OWNER\\|[0-9]+\\|${owner_identity_hex}\\|${owner_deadline_hex}" \
      "$control_output" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$control_client_pid" >/dev/null 2>&1; then
      cat "$control_error" >&2
      return 1
    fi
    sleep 0.05
  done
  echo "timed out waiting for retained control-session ACK" >&2
  cat "$control_error" >&2
  return 1
}

renew_control() {
  local renewal_identity_hex="$1"
  local renewal_deadline_hex="$2"
  local marker="RENEWED|${renewal_identity_hex}|${renewal_deadline_hex}"

  control_write "
    SELECT 'RENEWED|' || encode(installed_identity, 'hex') || '|' ||
           encode(installed_deadline_boottime_ns, 'hex')
    FROM pg_catalog.pgshard_fence_install(
      decode('$renewal_identity_hex', 'hex'), decode('$renewal_deadline_hex', 'hex'));
  "
  wait_for_file_line "$control_output" "$marker" "retained control-session renewal ACK"
}

expect_owner_rejection() {
  local expression="$1"
  local sqlstate="$2"
  local marker="$3"

  control_write "
    DO \$pgshard\$
    BEGIN
      BEGIN
        PERFORM $expression;
        RAISE EXCEPTION 'pgshard fence unexpectedly accepted invalid renewal'
          USING ERRCODE = 'P0001';
      EXCEPTION WHEN SQLSTATE '$sqlstate' THEN
        NULL;
      END;
    END
    \$pgshard\$;
    \\echo $marker
  "
  wait_for_file_line "$control_output" "$marker" "$marker"
}

expect_non_owner_rejection() {
  local owner_identity_hex="$1"
  local owner_deadline_hex="$2"

  control_sql "
    DO \$pgshard\$
    BEGIN
      BEGIN
        PERFORM pg_catalog.pgshard_fence_install(
          decode('$owner_identity_hex', 'hex'),
          decode('$owner_deadline_hex', 'hex'));
        RAISE EXCEPTION 'second control PID unexpectedly renewed the fence'
          USING ERRCODE = 'P0001';
      EXCEPTION WHEN SQLSTATE '55000' THEN
        NULL;
      END;
    END
    \$pgshard\$;
  " >/dev/null
}

close_control() {
  control_write '\q'
  exec 3>&-
  wait "$control_client_pid"
  control_client_pid=""
}

start_fenced_background_worker() {
  local marker=""

  control_write "
    CREATE OR REPLACE FUNCTION pg_temp.pgshard_test_start_background_worker()
    RETURNS integer
    AS '\$libdir/pgshard_fence', 'pgshard_fence_test_start_background_worker'
    LANGUAGE C;
    SELECT 'BACKGROUND_WORKER|' ||
           pg_temp.pgshard_test_start_background_worker();
  "
  for _ in $(seq 1 200); do
    marker="$(grep --extended-regexp --line-regexp \
      'BACKGROUND_WORKER\|[1-9][0-9]*' "$control_output" | tail -1 || true)"
    if [[ -n "$marker" ]]; then
      background_worker_pid="${marker#BACKGROUND_WORKER|}"
      return 0
    fi
    if ! kill -0 "$control_client_pid" >/dev/null 2>&1; then
      cat "$control_error" >&2
      return 1
    fi
    sleep 0.05
  done
  echo 'timed out starting dynamic no-database background worker' >&2
  cat "$control_error" >&2
  return 1
}

start_barrier_worker() {
  local marker=""

  control_write "
    CREATE OR REPLACE FUNCTION pg_temp.pgshard_test_start_barrier_worker()
    RETURNS integer
    AS '\$libdir/pgshard_fence', 'pgshard_fence_test_start_barrier_worker'
    LANGUAGE C;
    SELECT 'BARRIER_WORKER|' ||
           pg_temp.pgshard_test_start_barrier_worker();
  "
  for _ in $(seq 1 200); do
    marker="$(grep --extended-regexp --line-regexp \
      'BARRIER_WORKER\|[1-9][0-9]*' "$control_output" | tail -1 || true)"
    if [[ -n "$marker" ]]; then
      barrier_worker_pid="${marker#BARRIER_WORKER|}"
      return 0
    fi
    if ! kill -0 "$control_client_pid" >/dev/null 2>&1; then
      cat "$control_error" >&2
      return 1
    fi
    sleep 0.05
  done
  echo 'timed out starting non-barrier background worker' >&2
  cat "$control_error" >&2
  return 1
}

stop_barrier_worker() {
  control_write "
    CREATE OR REPLACE FUNCTION pg_temp.pgshard_test_stop_barrier_worker()
    RETURNS void
    AS '\$libdir/pgshard_fence', 'pgshard_fence_test_stop_barrier_worker'
    LANGUAGE C;
    SELECT pg_temp.pgshard_test_stop_barrier_worker();
    \echo BARRIER_WORKER_STOPPED
  "
  wait_for_file_line "$control_output" 'BARRIER_WORKER_STOPPED' \
    'non-barrier background-worker shutdown'
}

start_fenced_workloads() {
  rm -f "$idle_fifo"
  : >"$idle_output"
  : >"$idle_error"
  : >"$active_output"
  : >"$active_error"
  : >"$walsender_output"
  : >"$walsender_error"
  mkfifo "$idle_fifo"
  exec 4<>"$idle_fifo"

  docker exec -i --user 999:999 --env PGAPPNAME=pgshard-fence-idle "$container" \
    stdbuf --output=L psql -X --no-password --quiet \
    --host=127.0.0.1 --username=postgres --dbname=postgres \
    --set=ON_ERROR_STOP=1 --tuples-only --no-align \
    <"$idle_fifo" >"$idle_output" 2>"$idle_error" &
  idle_client_pid=$!
  printf '%s\n' "BEGIN; SELECT 'IDLE|' || pg_catalog.pg_backend_pid();" >&4

  docker exec --user 999:999 --env PGAPPNAME=pgshard-fence-active "$container" \
    psql -X --no-password --quiet \
    --host=127.0.0.1 --username=postgres --dbname=postgres \
    --set=ON_ERROR_STOP=1 --command='SELECT pg_sleep(30)' \
    >"$active_output" 2>"$active_error" &
  active_client_pid=$!

  docker exec --user 999:999 --env PGAPPNAME=pgshard-fence-walsender "$container" \
    sh -ceu '
      receive_dir="/tmp/pgshard-receivewal"
      mkdir "$receive_dir"
      exec pg_receivewal --no-loop --verbose \
        --host=127.0.0.1 --username=postgres --directory="$receive_dir"
    ' >"$walsender_output" 2>"$walsender_error" &
  walsender_client_pid=$!
  start_fenced_background_worker

  wait_for_sql_value "
    SELECT count(*) FROM pg_catalog.pg_stat_activity
    WHERE application_name = 'pgshard-fence-idle'
      AND state = 'idle in transaction'
  " "1" "idle transaction"
  wait_for_sql_value "
    SELECT count(*) FROM pg_catalog.pg_stat_activity
    WHERE application_name = 'pgshard-fence-active' AND state = 'active'
  " "1" "already-running statement"
  wait_for_sql_value "
    SELECT count(*) FROM pg_catalog.pg_stat_activity
    WHERE application_name = 'pgshard-fence-walsender' AND backend_type = 'walsender'
  " "1" "physical walsender"
}

wait_for_process_exit() {
  local pid="$1"
  local description="$2"

  for _ in $(seq 1 300); do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      wait "$pid" >/dev/null 2>&1 || true
      return 0
    fi
    sleep 0.05
  done
  echo "$description survived the closed target fence" >&2
  docker logs "$container" >&2
  return 1
}

wait_for_container_pid_exit() {
  local pid="$1"
  local description="$2"

  for _ in $(seq 1 300); do
    if ! docker exec "$container" kill -0 "$pid" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.05
  done
  echo "$description survived the closed target fence" >&2
  docker logs "$container" >&2
  return 1
}

expect_fenced_workloads_stopped() {
  wait_for_process_exit "$active_client_pid" "already-running statement"
  wait_for_process_exit "$walsender_client_pid" "physical walsender"
  wait_for_container_pid_exit "$background_worker_pid" \
    "dynamic no-database background worker"
  wait_for_sql_value "
    SELECT count(*) FROM pg_catalog.pg_stat_activity
    WHERE application_name = 'pgshard-fence-idle'
  " "0" "terminated idle-transaction backend"
  wait_for_sql_value "
    SELECT count(*) FROM pg_catalog.pg_stat_activity
    WHERE backend_type IN ('autovacuum launcher', 'logical replication launcher')
  " "0" "stopped core maintenance launchers"
  printf '%s\n' 'SELECT 1;' >&4 || true
  exec 4>&-
  wait_for_process_exit "$idle_client_pid" "idle transaction client"
  grep --fixed-strings \
    'pgshard writable authority is not installed or has expired' \
    "$active_error" >/dev/null
  grep --fixed-strings \
    'pgshard writable authority is not installed or has expired' \
    "$idle_error" >/dev/null
  active_client_pid=""
  idle_client_pid=""
  walsender_client_pid=""
  background_worker_pid=""
}

docker build \
  --file deploy/images/rust.Dockerfile \
  --target postgres-fence-test-runner \
  --build-arg PGSHARD_BUILD_VERSION=0.0.0-dev+target-fence-test \
  --build-arg PGSHARD_GIT_SHA="$(git rev-parse HEAD)" \
  --tag "$test_image" . >/dev/null

prove_core_abi_mismatch_fails_closed
start_postgres 'ANY 1 (pgshard_member_0001)'
expect_ordinary_failure disarmed
if remote_identify_system >/dev/null 2>&1; then
  echo "remote replication unexpectedly passed the disarmed target fence" >&2
  exit 1
fi
docker run --rm --user 999:999 \
  --volume "$socket_volume:/socket" \
  --env PGSHARD_TARGET_FENCE_TEST_SOCKET=/socket \
  "$test_image" \
  postgres_fence::tests::live_postgres18_installs_renews_and_detects_control_session_loss \
  --ignored --exact
if remote_identify_system >/dev/null 2>&1; then
  echo "remote replication passed after target control-session loss" >&2
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

boottime_ns="$(docker exec "$container" awk '{printf "%.0f\n", $1 * 1000000000}' /proc/uptime)"
readonly boottime_ns
deadline_ns="$((boottime_ns + 15000000000))"
regressive_deadline_ns="$((boottime_ns + 5000000000))"
printf -v deadline_hex '%016x' "$deadline_ns"
printf -v regressive_deadline_hex '%016x' "$regressive_deadline_ns"
readonly deadline_hex regressive_deadline_hex

start_control 010203 "$deadline_hex"
expect_non_owner_rejection 010203 "$deadline_hex"
remote_identity="$(remote_identify_system)"
if [[ ! "$remote_identity" =~ ^[1-9][0-9]*\|[1-9][0-9]*\|[0-9A-F]+/[0-9A-F]+\|$ ]]; then
  echo "remote replication returned an invalid post-install identity" >&2
  exit 1
fi
test "$(ordinary_sql 'SELECT 1')" = "1"
ordinary_sql \
  'CREATE TABLE public.pgshard_fence_wal_probe (value integer NOT NULL)' \
  >/dev/null
ordinary_sql \
  'INSERT INTO public.pgshard_fence_wal_probe (value) VALUES (1)' \
  >/dev/null
test "$(ordinary_sql 'SELECT count(*) FROM public.pgshard_fence_wal_probe')" = "1"

start_barrier_worker
ordinary_sql 'CREATE DATABASE pgshard_barrier_probe' >/dev/null
ordinary_sql_with_timeout 'DROP DATABASE pgshard_barrier_probe' >/dev/null
stop_barrier_worker
wait_for_container_pid_exit "$barrier_worker_pid" \
  'non-barrier background worker after termination'
barrier_worker_pid=""

expect_owner_rejection "
  pg_catalog.pgshard_fence_install(
    decode('010204', 'hex'), decode('$deadline_hex', 'hex'))
" 22023 IDENTITY_REJECTED
expect_owner_rejection "
  pg_catalog.pgshard_fence_install(
    decode('010203', 'hex'), decode('$regressive_deadline_hex', 'hex'))
" 22023 DEADLINE_REGRESSION_REJECTED

start_fenced_workloads
extended_deadline_ns="$((deadline_ns + 5000000000))"
printf -v extended_deadline_hex '%016x' "$extended_deadline_ns"
renew_control 010203 "$extended_deadline_hex"
wait_until_boottime_after "$((deadline_ns + 250000000))"
for pid in "$active_client_pid" "$idle_client_pid" "$walsender_client_pid"; do
  kill -0 "$pid"
done
docker exec "$container" kill -0 "$background_worker_pid"
wait_for_sql_value "
  SELECT count(*) FROM pg_catalog.pg_stat_activity
  WHERE application_name IN (
    'pgshard-fence-idle', 'pgshard-fence-active', 'pgshard-fence-walsender'
  )
" "3" "workloads surviving their replaced deadline"
expect_fenced_workloads_stopped
expect_ordinary_failure expired

close_control
prove_expired_shutdown_wal_is_fenced
start_postgres
expect_ordinary_failure restart

connection_loss_boottime_ns="$(docker exec "$container" awk '{printf "%.0f\n", $1 * 1000000000}' /proc/uptime)"
connection_loss_deadline_ns="$((connection_loss_boottime_ns + 30000000000))"
printf -v connection_loss_deadline_hex '%016x' "$connection_loss_deadline_ns"
start_control 0a0b0c "$connection_loss_deadline_hex"
start_fenced_workloads
for pid in "$active_client_pid" "$idle_client_pid" "$walsender_client_pid"; do
  kill -0 "$pid"
done
close_control
expect_fenced_workloads_stopped
expect_ordinary_failure control_session_loss

stop_postgres
start_postgres
expect_ordinary_failure final_restart

stop_postgres
start_postgres '' off debug1
expect_ordinary_failure emergency_autovacuum_disarmed
control_sql "
  CREATE FUNCTION pg_temp.pgshard_test_request_autovacuum() RETURNS void
  AS '\$libdir/pgshard_fence', 'pgshard_fence_test_request_autovacuum'
  LANGUAGE C;
  SELECT pg_temp.pgshard_test_request_autovacuum();
" >/dev/null
sleep 1
if docker logs "$container" 2>&1 |
    grep --fixed-strings 'autovacuum launcher started' >/dev/null; then
  echo 'emergency autovacuum launcher bypassed the disarmed core gate' >&2
  exit 1
fi
emergency_boottime_ns="$(docker exec "$container" \
  awk '{printf "%.0f\n", $1 * 1000000000}' /proc/uptime)"
emergency_deadline_ns="$((emergency_boottime_ns + 30000000000))"
printf -v emergency_deadline_hex '%016x' "$emergency_deadline_ns"
start_control 0d0e0f "$emergency_deadline_hex"
wait_for_container_log 'autovacuum launcher started' \
  'authorized emergency autovacuum launcher'
stop_postgres

start_postgres '' off warning 3
race_boottime_ns="$(docker exec "$container" \
  awk '{printf "%.0f\n", $1 * 1000000000}' /proc/uptime)"
race_deadline_ns="$((race_boottime_ns + 5000000000))"
printf -v race_deadline_hex '%016x' "$race_deadline_ns"
start_control 0f1011 "$race_deadline_hex"
start_fenced_background_worker
wait_until_boottime_after "$((race_deadline_ns + 250000000))"
wait_for_container_pid_exit "$background_worker_pid" \
  'dynamic background worker crossing the launch deadline'
wait_for_container_log \
  'pgshard writable authority is not installed or has expired' \
  'background-worker launch-race fence'
if docker logs "$container" 2>&1 |
    grep --fixed-strings \
      'pgshard fence test background worker entered extension code' >/dev/null; then
  echo 'expired dynamic worker entered extension code after its launch delay' >&2
  exit 1
fi
background_worker_pid=""
close_control
stop_postgres
