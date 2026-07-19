#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$repo_root"

readonly image="${PGSHARD_AGENT_POSTGRES_TEST_IMAGE:-docker.io/library/postgres:18@sha256:32ca0af8e77bfb8c6610c488e4691f83f972a3e9e64d3b02facf3ab111ad5500}"
readonly suffix="${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-$$"
readonly network="pgshard-generation-${suffix}"
readonly primary="pgshard-generation-primary-${suffix}"
readonly standby="pgshard-generation-standby-${suffix}"
readonly primary_data="pgshard-generation-primary-data-${suffix}"
readonly standby_data="pgshard-generation-standby-data-${suffix}"
readonly primary_socket="pgshard-generation-primary-socket-${suffix}"
readonly standby_socket="pgshard-generation-standby-socket-${suffix}"
readonly replication_password="pgshard_generation_replication_test"
fixture_dir="$(mktemp -d "${RUNNER_TEMP:-/tmp}/pgshard-generation.XXXXXX")"
readonly fixture_dir
readonly primary_hba="$fixture_dir/primary.pg_hba.conf"
readonly build_messages="$fixture_dir/cargo-messages.json"

if [[ ! "$image" =~ @sha256:[0-9a-f]{64}$ ]]; then
  echo "PostgreSQL 18 test image must be pinned by sha256 digest: $image" >&2
  exit 1
fi

cleanup() {
  docker rm --force "$standby" "$primary" >/dev/null 2>&1 || true
  docker network rm "$network" >/dev/null 2>&1 || true
  docker volume rm --force \
    "$primary_data" "$standby_data" "$primary_socket" "$standby_socket" \
    >/dev/null 2>&1 || true
  rm -f "$primary_hba" "$build_messages"
  rmdir "$fixture_dir" 2>/dev/null || true
}
trap cleanup EXIT

pulled=false
for attempt in 1 2 3 4; do
  if docker pull "$image"; then
    pulled=true
    break
  fi
  if (( attempt < 4 )); then
    sleep $((attempt * 5))
  fi
done
if [[ "$pulled" != true ]]; then
  echo "failed to pull pinned PostgreSQL 18 image after 4 attempts: $image" >&2
  exit 1
fi

printf '%s\n' \
  'local postgres postgres peer' \
  'local all all reject' \
  'local replication all reject' \
  'host replication replicator all scram-sha-256' \
  'host all all all reject' >"$primary_hba"
chmod 0444 "$primary_hba"

cargo test --locked -p pgshard-agent --lib --no-run --message-format=json \
  >"$build_messages"
test_binary="$(
  jq --raw-output \
    'select(.reason == "compiler-artifact" and .target.name == "pgshard_agent" and .profile.test == true) | .executable // empty' \
    "$build_messages" | tail -n 1
)"
readonly test_binary
test -n "$test_binary"
test -x "$test_binary"

docker network create "$network" >/dev/null
docker volume create "$primary_data" >/dev/null
docker volume create "$standby_data" >/dev/null
docker volume create "$primary_socket" >/dev/null
docker volume create "$standby_socket" >/dev/null

docker run --detach --name "$primary" \
  --network "$network" --network-alias primary \
  --volume "$primary_data:/var/lib/postgresql/18/docker" \
  --volume "$primary_socket:/var/run/postgresql" \
  --mount "type=bind,src=$primary_hba,dst=/etc/pgshard-generation-primary-hba.conf,readonly" \
  --env PGDATA=/var/lib/postgresql/18/docker \
  --env POSTGRES_PASSWORD=disposable-primary-password \
  "$image" \
  -c listen_addresses='*' \
  -c hba_file=/etc/pgshard-generation-primary-hba.conf \
  -c wal_level=replica \
  -c max_wal_senders=4 \
  -c hot_standby=on \
  -c event_triggers=off >/dev/null

wait_ready() {
  local container="$1"
  for _ in $(seq 1 120); do
    if docker exec "$container" /bin/sh -ceu \
      'test "$(cat /proc/1/comm)" = postgres' \
      >/dev/null 2>&1 && \
      docker exec "$container" pg_isready \
      --host=/var/run/postgresql --username=postgres --dbname=postgres \
      >/dev/null 2>&1; then
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

wait_ready "$primary"
docker exec --user 999:999 "$primary" psql -X --no-password \
  --host=/var/run/postgresql --username=postgres --dbname=postgres \
  --set=ON_ERROR_STOP=1 --command="
    CREATE ROLE replicator WITH REPLICATION LOGIN
      PASSWORD '${replication_password}';
  " >/dev/null

docker run --rm --user 0:0 --volume "$standby_data:/standby" \
  --entrypoint /bin/chown "$image" -R 999:999 /standby
docker run --rm --user 999:999 --network "$network" \
  --volume "$standby_data:/standby" \
  --env PGPASSWORD="$replication_password" \
  --entrypoint /usr/lib/postgresql/18/bin/pg_basebackup \
  "$image" \
  --host=primary --port=5432 --username=replicator \
  --pgdata=/standby --wal-method=stream --checkpoint=fast \
  --write-recovery-conf --no-password

docker run --detach --name "$standby" \
  --network "$network" \
  --volume "$standby_data:/var/lib/postgresql/18/docker" \
  --volume "$standby_socket:/var/run/postgresql" \
  --mount "type=bind,src=$repo_root/deploy/images/quarantine.pg_hba.conf,dst=/etc/pgshard-generation-standby-hba.conf,readonly" \
  --env PGDATA=/var/lib/postgresql/18/docker \
  --env POSTGRES_PASSWORD=disposable-standby-password \
  "$image" \
  -c listen_addresses= \
  -c hba_file=/etc/pgshard-generation-standby-hba.conf \
  -c max_wal_senders=4 \
  -c hot_standby=on \
  -c event_triggers=off >/dev/null
wait_ready "$standby"

for _ in $(seq 1 120); do
  if [[ "$(docker exec --user 999:999 "$primary" psql -X --no-password \
      --host=/var/run/postgresql --username=postgres --dbname=postgres \
      --tuples-only --no-align \
      --command="SELECT count(*) FROM pg_catalog.pg_stat_replication \
                 WHERE application_name = 'walreceiver' AND state = 'streaming'")" = 1 ]]; then
    break
  fi
  sleep 0.25
done
test "$(docker exec --user 999:999 "$primary" psql -X --no-password \
  --host=/var/run/postgresql --username=postgres --dbname=postgres \
  --tuples-only --no-align \
  --command="SELECT count(*) FROM pg_catalog.pg_stat_replication \
             WHERE application_name = 'walreceiver' AND state = 'streaming'")" = 1

docker run --rm --user 999:999 --network none --read-only \
  --cap-drop ALL --security-opt no-new-privileges \
  --volume "$primary_socket:/primary-socket" \
  --volume "$standby_socket:/standby-socket" \
  --mount "type=bind,src=$test_binary,dst=/test/pgshard-agent-test,readonly" \
  --env PGSHARD_AGENT_TEST_SOCKET_DIR=/primary-socket \
  --env PGSHARD_AGENT_TEST_STANDBY_SOCKET_DIR=/standby-socket \
  --entrypoint /test/pgshard-agent-test \
  "$image" \
  --ignored --exact \
  postgres_generation::tests::live_postgres18_replicates_exact_generation_across_flush_barriers \
  --nocapture
