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
readonly standby_credentials="pgshard-generation-standby-credentials-${suffix}"
readonly fence_primary="pgshard-generation-fence-${suffix}"
readonly fence_socket="pgshard-generation-fence-socket-${suffix}"
readonly fence_hba="pgshard-generation-fence-hba-${suffix}"
readonly identity_primary="pgshard-generation-identity-${suffix}"
readonly identity_socket="pgshard-generation-identity-socket-${suffix}"
readonly identity_hba="pgshard-generation-identity-hba-${suffix}"
readonly identity_log="pgshard-generation-identity-log-${suffix}"
readonly replication_password="pgshard_generation_replication_test"
readonly standby_application_name="pgshard_member_0001"
readonly synchronous_standby_names="pgshard_member_0001, pgshard_member_0002"
readonly replication_role="pgshard_replication"
readonly standby_passfile="/run/pgshard/credentials/primary.pass"
readonly ambient_password_canary="pgshard_ambient_password_canary"
readonly runtime_image="${PGSHARD_AGENT_RUNTIME_TEST_IMAGE:-}"
printf -v final_generation_identity '%s\n' \
  'format=1' \
  'cluster_name=cluster-1' \
  'cluster_uid=cluster-1-uid' \
  'shard=0' \
  'lease_namespace=database' \
  'lease_name=cluster-1-lease' \
  'lease_uid=cluster-1-lease-uid' \
  'holder=holder-d' \
  'term=4'
readonly final_generation_identity
# The local records the agent installs for every runtime role. Seeded here so a
# postmaster has a policy before any test can reload one, and asserted against
# the product's own `hba_policy_contents` by every live test that starts from
# it: a fixture that is more permissive than production answers questions
# production would refuse, which is how a policy that rejects the agent's own
# catalog writer read as green.
printf -v agent_local_hba_policy '%s\n' \
  'local postgres,shardschema postgres peer' \
  'local shardschema pgshard_pooler_catalog scram-sha-256' \
  'local shardschema pgshard_orchestrator_catalog scram-sha-256' \
  'local all all reject' \
  'local replication all reject'
readonly agent_local_hba_policy
fixture_dir="$(mktemp -d "${RUNNER_TEMP:-/tmp}/pgshard-generation.XXXXXX")"
readonly fixture_dir
readonly primary_hba="$fixture_dir/primary.pg_hba.conf"
readonly build_messages="$fixture_dir/cargo-messages.json"

if [[ ! "$image" =~ @sha256:[0-9a-f]{64}$ ]]; then
  echo "PostgreSQL 18 test image must be pinned by sha256 digest: $image" >&2
  exit 1
fi

cleanup() {
  docker rm --force "$standby" "$primary" "$fence_primary" "$identity_primary" \
    >/dev/null 2>&1 || true
  docker network rm "$network" >/dev/null 2>&1 || true
  docker volume rm --force \
    "$primary_data" "$standby_data" "$primary_socket" "$standby_socket" \
    "$standby_credentials" "$fence_socket" "$fence_hba" \
    "$identity_socket" "$identity_hba" "$identity_log" \
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

{
  printf '%s' "$agent_local_hba_policy"
  printf '%s\n' \
    'host replication pgshard_replication all scram-sha-256' \
    'host all all all reject'
} >"$primary_hba"
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
docker volume create "$standby_credentials" >/dev/null

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

# The agent retries a connection it cannot yet establish, and its materializer
# retries a catalog it cannot yet reconcile, so a test given a server it can
# never satisfy waits rather than failing. Unbounded, that consumes the whole
# job timeout and reports nothing: the runner kills the step mid-`docker` and
# the log simply stops, which says neither which test was running nor that it
# was waiting at all.
#
# Every test invocation is bounded here instead, and names itself when its bound
# expires. SIGINT rather than SIGTERM because it reaches the test process
# through `docker run`, and the kill-after is the backstop for a container that
# ignores it.
readonly TEST_BOUND_SECONDS=300
bounded() {
  local what="$1"
  shift
  local status=0
  timeout --signal=INT --kill-after=30s "$TEST_BOUND_SECONDS" "$@" || status=$?
  if (( status == 124 || status == 130 )); then
    echo "timed out after ${TEST_BOUND_SECONDS}s waiting for ${what}" >&2
    return 1
  fi
  return "$status"
}

fail_container() {
  local container_name="$1"
  shift
  echo "$*" >&2
  docker inspect --format \
    'container={{.Name}} running={{.State.Running}} exit_code={{.State.ExitCode}} error={{.State.Error}}' \
    "$container_name" >&2 || true
  docker logs "$container_name" >&2 || true
  exit 1
}

require_container_exit_code() {
  local container_name="$1"
  local expected="$2"
  local purpose="$3"
  local actual
  actual="$(docker inspect --format '{{.State.ExitCode}}' "$container_name")"
  if [[ "$actual" != "$expected" ]]; then
    fail_container "$container_name" \
      "$purpose: expected container exit code $expected, observed $actual"
  fi
}

normalize_standby_socket() {
  docker run --rm --user 0:0 --volume "$standby_socket:/socket" \
    --entrypoint /bin/sh "$image" -ceu '
      find /socket -mindepth 1 -maxdepth 1 -delete
      chown 999:999 /socket
      chmod 0700 /socket
    '
}

wait_ready "$primary"
docker exec --user 999:999 "$primary" psql -X --no-password \
  --host=/var/run/postgresql --username=postgres --dbname=postgres \
  --set=ON_ERROR_STOP=1 --command="
    CREATE ROLE ${replication_role} WITH REPLICATION LOGIN
      PASSWORD '${replication_password}';
  " >/dev/null

docker run --rm --user 0:0 \
  --volume "$standby_credentials:/run/pgshard/credentials" \
  --env REPLICATION_PASSWORD="$replication_password" \
  --entrypoint /bin/sh "$image" -ceu '
    printf "primary:5432:*:pgshard_replication:%s\n" "$REPLICATION_PASSWORD" \
      > /run/pgshard/credentials/primary.pass
    chown 999:999 /run/pgshard/credentials/primary.pass
    chmod 0400 /run/pgshard/credentials/primary.pass
  '

docker run --rm --user 0:0 --volume "$standby_data:/standby" \
  --entrypoint /bin/chown "$image" -R 999:999 /standby
docker run --rm --user 999:999 --network "$network" \
  --volume "$standby_data:/standby" \
  --env PGPASSWORD="$replication_password" \
  --entrypoint /usr/lib/postgresql/18/bin/pg_basebackup \
  "$image" \
  --dbname="host=primary port=5432 user=${replication_role} application_name=${standby_application_name}" \
  --pgdata=/standby --wal-method=stream --checkpoint=fast \
  --slot="$standby_application_name" --create-slot \
  --no-password

docker run --rm --user 999:999 --volume "$standby_data:/standby" \
  --entrypoint /bin/sh "$image" -ceu '
    : > /standby/standby.signal
    chmod 0600 /standby/standby.signal
  '

docker run --detach --name "$standby" \
  --network "$network" \
  --volume "$standby_data:/var/lib/postgresql/18/docker" \
  --volume "$standby_socket:/var/run/postgresql" \
  --volume "$standby_credentials:/run/pgshard/credentials:ro" \
  --mount "type=bind,src=$repo_root/deploy/images/quarantine.pg_hba.conf,dst=/etc/pgshard-generation-standby-hba.conf,readonly" \
  --env PGDATA=/var/lib/postgresql/18/docker \
  --env POSTGRES_PASSWORD=disposable-standby-password \
  "$image" \
  -c listen_addresses= \
  -c hba_file=/etc/pgshard-generation-standby-hba.conf \
  -c "primary_conninfo=host=primary port=5432 user=${replication_role} application_name=${standby_application_name} passfile=${standby_passfile} sslmode=disable" \
  -c "primary_slot_name=${standby_application_name}" \
  -c max_wal_senders=4 \
  -c hot_standby=on \
  -c event_triggers=off >/dev/null
wait_ready "$standby"
docker stop --time 10 "$standby" >/dev/null
docker rm "$standby" >/dev/null
normalize_standby_socket

if [[ -n "$runtime_image" ]]; then
  test "$(docker inspect --format '{{.Config.User}}' "$runtime_image")" = "999:999"
  # The Pod gives the agent a memory-backed runtime volume mounted read-write at
  # /run/pgshard, with the socket and credential mounts nested inside it. A
  # read-only root filesystem carrying only those nested mounts is a different
  # deployment: the agent owns that directory and materializes the policy and
  # configuration it is judged against into it before every spawn.
  start_agent_standby() {
    docker run --detach --name "$standby" \
      --network "$network" --read-only --cap-drop ALL \
      --security-opt no-new-privileges \
      --tmpfs /run/pgshard:rw,uid=999,gid=999,mode=0700 \
      --volume "$standby_data:/var/lib/postgresql/18/docker" \
      --volume "$standby_socket:/run/pgshard/postgres" \
      --volume "$standby_credentials:/run/pgshard/credentials:ro" \
      --env PGDATA=/var/lib/postgresql/18/docker \
      --env PGSHARD_HTTP_BIND=0.0.0.0:8080 \
      --env PGSHARD_CLUSTER_ID=cluster-1 \
      --env PGSHARD_SHARD_ID=0 \
      --env PGSHARD_INSTANCE_ID=cluster-1-shard-0-1 \
      --env PGSHARD_POSTGRES_MODE=replication-standby \
      --env PGSHARD_POSTGRES_SOCKET_DIR=/run/pgshard/postgres \
      --env PGSHARD_POSTGRES_HBA_FILE=/run/pgshard/hba/pg_hba.conf \
      --env PGSHARD_POSTGRES_PRIMARY_HOST=primary \
      --env PGSHARD_POSTGRES_PRIMARY_PORT=5432 \
      --env PGSHARD_POSTGRES_PRIMARY_SLOT_NAME="$standby_application_name" \
      --env PGSHARD_POSTGRES_PRIMARY_PASSFILE="$standby_passfile" \
      --env PGPASSWORD="$ambient_password_canary" \
      "$runtime_image" >/dev/null
  }
  standby_status() {
    docker exec "$primary" /bin/bash -ceu '
      exec 3<>"/dev/tcp/$1/8080"
      printf "GET /status HTTP/1.0\r\nHost: %s\r\n\r\n" "$1" >&3
      sed -n "/^{/p" <&3
    ' -- "$standby"
  }
  wait_agent_standby_running() {
    local status
    for _ in $(seq 1 120); do
      status="$(standby_status 2>/dev/null || true)"
      if jq --arg generation "$final_generation_identity" \
          --arg member_slot "$standby_application_name" \
          --exit-status '
            .postgres_process == "running_replication_standby" and
            .replication_evidence.role == "standby" and
            .replication_evidence.generation_identity == $generation and
            .replication_evidence.member_slot_name == $member_slot and
            .replication_evidence.in_recovery == true
          ' <<<"$status" \
          >/dev/null 2>&1 && \
        [[ "$(docker exec --user 999:999 "$standby" psql -X --no-password \
          --host=/run/pgshard/postgres --username=postgres --dbname=postgres \
          --tuples-only --no-align \
          --command='SELECT pg_catalog.pg_is_in_recovery()')" = t ]]; then
        return 0
      fi
      if [[ "$(docker inspect --format '{{.State.Running}}' "$standby")" != true ]]; then
        docker logs "$standby" >&2
        return 1
      fi
      sleep 0.25
    done
    status="$(standby_status 2>/dev/null || true)"
    echo "last supervised standby status: ${status:-<unavailable>}" >&2
    fail_container "$standby" \
      "supervised standby did not report exact final-generation replication evidence before the deadline"
  }
fi

# Keep the generation-mutating test isolated from the fail-closed evidence
# monitor. The test mutates evidence invariants and advances generations; a raw
# PostgreSQL standby keeps those operations separate while preserving the final
# exact generation for the later supervised lifecycle assertions.
docker run --detach --name "$standby" \
  --network "$network" \
  --volume "$standby_data:/var/lib/postgresql/18/docker" \
  --volume "$standby_socket:/var/run/postgresql" \
  --volume "$standby_credentials:/run/pgshard/credentials:ro" \
  --mount "type=bind,src=$repo_root/deploy/images/quarantine.pg_hba.conf,dst=/etc/pgshard-generation-standby-hba.conf,readonly" \
  --env PGDATA=/var/lib/postgresql/18/docker \
  --env POSTGRES_PASSWORD=disposable-standby-password \
  "$image" \
  -c listen_addresses= \
  -c hba_file=/etc/pgshard-generation-standby-hba.conf \
  -c "primary_conninfo=host=primary port=5432 user=${replication_role} application_name=${standby_application_name} passfile=${standby_passfile} sslmode=disable" \
  -c "primary_slot_name=${standby_application_name}" \
  -c max_wal_senders=4 \
  -c hot_standby=on \
  -c event_triggers=off >/dev/null
wait_ready "$standby"

for _ in $(seq 1 120); do
  if [[ "$(docker exec --user 999:999 "$primary" psql -X --no-password \
      --host=/var/run/postgresql --username=postgres --dbname=postgres \
      --tuples-only --no-align \
      --command="SELECT count(*) FROM pg_catalog.pg_stat_replication \
                 WHERE application_name = '${standby_application_name}' \
                   AND state = 'streaming'")" = 1 ]]; then
    break
  fi
  sleep 0.25
done
test "$(docker exec --user 999:999 "$primary" psql -X --no-password \
  --host=/var/run/postgresql --username=postgres --dbname=postgres \
  --tuples-only --no-align \
  --command="SELECT count(*) FROM pg_catalog.pg_stat_replication \
             WHERE application_name = '${standby_application_name}' \
               AND state = 'streaming'")" = 1

docker exec --user 999:999 "$primary" psql -X --no-password \
  --host=/var/run/postgresql --username=postgres --dbname=postgres \
  --set=ON_ERROR_STOP=1 \
  --command="ALTER SYSTEM SET synchronous_standby_names = \
             'ANY 1 (${synchronous_standby_names})'" >/dev/null
docker exec --user 999:999 "$primary" psql -X --no-password \
  --host=/var/run/postgresql --username=postgres --dbname=postgres \
  --set=ON_ERROR_STOP=1 --command="SELECT pg_catalog.pg_reload_conf()" >/dev/null
for _ in $(seq 1 120); do
  if [[ "$(docker exec --user 999:999 "$primary" psql -X --no-password \
      --host=/var/run/postgresql --username=postgres --dbname=postgres \
      --tuples-only --no-align \
      --command="SELECT count(*) FROM pg_catalog.pg_stat_replication \
                 WHERE application_name = '${standby_application_name}' \
                   AND state = 'streaming' AND sync_state = 'quorum'")" = 1 ]]; then
    break
  fi
  sleep 0.25
done
test "$(docker exec --user 999:999 "$primary" psql -X --no-password \
  --host=/var/run/postgresql --username=postgres --dbname=postgres \
  --tuples-only --no-align \
  --command="SELECT count(*) FROM pg_catalog.pg_stat_replication \
             WHERE application_name = '${standby_application_name}' \
               AND state = 'streaming' AND sync_state = 'quorum'")" = 1

wait_primary_streaming_quorum() {
  local count
  for _ in $(seq 1 120); do
    count="$(docker exec --user 999:999 "$primary" psql -X --no-password \
      --host=/var/run/postgresql --username=postgres --dbname=postgres \
      --tuples-only --no-align \
      --command="SELECT count(*) FROM pg_catalog.pg_stat_replication \
                 WHERE application_name = '${standby_application_name}' \
                   AND state = 'streaming' AND sync_state = 'quorum'")"
    if [[ "$count" = 1 ]]; then
      return 0
    fi
    if [[ "$(docker inspect --format '{{.State.Running}}' "$standby")" != true ]]; then
      fail_container "$standby" \
        "supervised standby exited before primary streaming quorum was restored"
    fi
    sleep 0.25
  done
  fail_container "$standby" \
    "primary did not report the supervised standby as streaming quorum before the deadline"
}

bounded postgres_generation::tests::live_postgres18_proves_any_one_synchronous_generation_replay \
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
    postgres_generation::tests::live_postgres18_proves_any_one_synchronous_generation_replay \
    --nocapture

# The cross-database fence can only be established against a running server:
# a read-only fence transaction cannot take the row lock, an idle holder is
# terminated by the evidence timeouts, and the lock wait itself is the property
# under test. None of that is reachable from a unit test.
#
# It runs against its own disposable server rather than the shared primary,
# because it publishes generations of its own and the steps that follow assert
# on the primary's final generation.
docker volume create "$fence_socket" >/dev/null
docker volume create "$fence_hba" >/dev/null

# Seeded and owned by the runtime user before the server exists: the postmaster
# refuses to start without an HBA file it can read, and the tests below reload
# this one in place.
#
# Running this server under the stock image default instead is what let the
# materializer's own sessions go unexamined. That default is `trust`, which
# admits every role on every database, so no session the materializer opens was
# ever put in front of a record that could refuse one -- while every policy the
# product deploys refused its catalog writer outright.
docker run --rm --user 0:0 --volume "$fence_hba:/hba" \
  --entrypoint /bin/sh "$image" -ceu "
    printf '%s' '$agent_local_hba_policy' > /hba/pg_hba.conf
    chown -R 999:999 /hba
    chmod 0600 /hba/pg_hba.conf
  "

docker run --detach --name "$fence_primary" \
  --network "$network" \
  --volume "$fence_socket:/var/run/postgresql" \
  --volume "$fence_hba:/hba" \
  --env POSTGRES_PASSWORD=disposable-fence-password \
  "$image" \
  -c hba_file=/hba/pg_hba.conf >/dev/null
wait_ready "$fence_primary"

# Every session the materializer opens, in front of the policy the agent itself
# installs, taken from `hba_policy_contents` rather than copied. The refusals
# the same policy must still produce are asserted alongside them.
#
# Named exactly, like every other test here. A substring also selects the
# catalog identity test of the same name, whose fixture stages the two catalog
# login roles and leaves them behind -- and the materializer's own reset drops
# only the four roles the migration owns, so the migration below would then
# refuse a server carrying roles it did not create.
for fence_test in \
  postgres_generation::tests::live_postgres18_the_installed_policy_admits_every_materializer_session \
  postgres_generation::tests::live_postgres18_the_installed_policy_still_refuses_an_identity_it_never_named
do
  bounded "$fence_test" \
    docker run --rm --user 999:999 \
    --network "$network" \
    --volume "$fence_socket:/fence-socket" \
    --volume "$fence_hba:/fence-hba" \
    --mount "type=bind,src=$test_binary,dst=/test/pgshard-agent-test,readonly" \
    --env PGSHARD_AGENT_TEST_SOCKET_DIR=/fence-socket \
    --env PGSHARD_AGENT_TEST_HBA_FILE=/fence-hba/pg_hba.conf \
    --entrypoint /test/pgshard-agent-test \
    "$image" \
    --ignored --exact "$fence_test" --nocapture
done

bounded postgres_generation::tests::live_postgres18_materializer_fences_publication \
  docker run --rm --user 999:999 \
    --network "$network" \
    --volume "$fence_socket:/fence-socket" \
    --mount "type=bind,src=$test_binary,dst=/test/pgshard-agent-test,readonly" \
    --env PGSHARD_AGENT_TEST_SOCKET_DIR=/fence-socket \
    --entrypoint /test/pgshard-agent-test \
    "$image" \
  --ignored --exact \
  postgres_generation::tests::live_postgres18_materializer_fences_publication \
  --nocapture

# Drives the real compiled program and revokes authority in the window before
# the commit checks, which is the only way to observe that a revoked attempt
# leaves nothing behind and that a later one materializes completely.
bounded catalog_materializer::tests::live_postgres18_revoked_authority_does_not_materialize \
  docker run --rm --user 999:999 \
    --network "$network" \
    --volume "$fence_socket:/fence-socket" \
    --mount "type=bind,src=$test_binary,dst=/test/pgshard-agent-test,readonly" \
    --env PGSHARD_AGENT_TEST_SOCKET_DIR=/fence-socket \
    --entrypoint /test/pgshard-agent-test \
    "$image" \
  --ignored --exact \
  catalog_materializer::tests::live_postgres18_revoked_authority_does_not_materialize \
  --nocapture

# Verification has to reject a catalog that satisfies the fragments' own
# postconditions but disagrees with the declared inventory, which only a real
# catalog carrying an undeclared shard can demonstrate.
bounded catalog_materializer::tests::live_postgres18_verification_rejects_an_unexpected_active_shard \
  docker run --rm --user 999:999 \
    --network "$network" \
    --volume "$fence_socket:/fence-socket" \
    --mount "type=bind,src=$test_binary,dst=/test/pgshard-agent-test,readonly" \
    --env PGSHARD_AGENT_TEST_SOCKET_DIR=/fence-socket \
    --entrypoint /test/pgshard-agent-test \
    "$image" \
  --ignored --exact \
  catalog_materializer::tests::live_postgres18_verification_rejects_an_unexpected_active_shard \
  --nocapture

# Reconciliation has to settle the committed prefix an ambiguous commit or an
# interrupted attempt leaves behind.
bounded catalog_materializer::tests::live_postgres18_reconciles_a_partially_materialized_catalog \
  docker run --rm --user 999:999 \
    --network "$network" \
    --volume "$fence_socket:/fence-socket" \
    --mount "type=bind,src=$test_binary,dst=/test/pgshard-agent-test,readonly" \
    --env PGSHARD_AGENT_TEST_SOCKET_DIR=/fence-socket \
    --entrypoint /test/pgshard-agent-test \
    "$image" \
  --ignored --exact \
  catalog_materializer::tests::live_postgres18_reconciles_a_partially_materialized_catalog \
  --nocapture

# Each remaining invariant has to be load-bearing, the predicates have to be one
# locked observation, and a divergence the program cannot undo has to end the
# attempt rather than hold the fence against publication.
bounded catalog_materializer::tests::live_postgres18_verification_rejects_a_catalog_that_breaks_an_invariant \
  docker run --rm --user 999:999 \
    --network "$network" \
    --volume "$fence_socket:/fence-socket" \
    --mount "type=bind,src=$test_binary,dst=/test/pgshard-agent-test,readonly" \
    --env PGSHARD_AGENT_TEST_SOCKET_DIR=/fence-socket \
    --entrypoint /test/pgshard-agent-test \
    "$image" \
  --ignored --exact \
  catalog_materializer::tests::live_postgres18_verification_rejects_a_catalog_that_breaks_an_invariant \
  --nocapture

bounded catalog_materializer::tests::live_postgres18_verification_serializes_on_the_cluster_state_row \
  docker run --rm --user 999:999 \
    --network "$network" \
    --volume "$fence_socket:/fence-socket" \
    --mount "type=bind,src=$test_binary,dst=/test/pgshard-agent-test,readonly" \
    --env PGSHARD_AGENT_TEST_SOCKET_DIR=/fence-socket \
    --entrypoint /test/pgshard-agent-test \
    "$image" \
  --ignored --exact \
  catalog_materializer::tests::live_postgres18_verification_serializes_on_the_cluster_state_row \
  --nocapture

bounded catalog_materializer::tests::live_postgres18_bounded_retry_gives_up_on_a_catalog_it_cannot_reconcile \
  docker run --rm --user 999:999 \
    --network "$network" \
    --volume "$fence_socket:/fence-socket" \
    --mount "type=bind,src=$test_binary,dst=/test/pgshard-agent-test,readonly" \
    --env PGSHARD_AGENT_TEST_SOCKET_DIR=/fence-socket \
    --entrypoint /test/pgshard-agent-test \
    "$image" \
  --ignored --exact \
  catalog_materializer::tests::live_postgres18_bounded_retry_gives_up_on_a_catalog_it_cannot_reconcile \
  --nocapture

docker rm --force "$fence_primary" >/dev/null
docker volume rm "$fence_socket" "$fence_hba" >/dev/null

# The catalog identity checks decide a role's exact shape, its role-wide and
# database-wide defaults, whether an installed credential really authenticates,
# and whether the session it authenticates into is the one a production client
# gets. Only a real server answers any of those, and three of them need a
# server the test may reconfigure while it runs: an HBA file it rewrites and
# reloads, a statement log it reads back, and a catalog database it drops and
# rebuilds. That is a disposable server of its own rather than the shared
# primary, whose final generation the steps above and below assert on.
docker volume create "$identity_socket" >/dev/null
docker volume create "$identity_hba" >/dev/null
docker volume create "$identity_log" >/dev/null

# The postmaster refuses to start without an HBA file it can read, and the
# tests rewrite this one in place, so it is seeded and owned by the runtime
# user before the server exists.
docker run --rm --user 0:0 \
  --volume "$identity_hba:/hba" --volume "$identity_log:/log" \
  --entrypoint /bin/sh "$image" -ceu "
    printf '%s' '$agent_local_hba_policy' > /hba/pg_hba.conf
    chown -R 999:999 /hba /log
    chmod 0600 /hba/pg_hba.conf
    chmod 0700 /log
  "

# log_statement=all is what makes the credential-log test able to conclude
# anything: it proves the log was recording the installing statement before it
# concludes the verifier is absent from it. The collector is what turns that
# log into a file the test container can read.
docker run --detach --name "$identity_primary" \
  --network "$network" \
  --volume "$identity_socket:/var/run/postgresql" \
  --volume "$identity_hba:/hba" \
  --volume "$identity_log:/log" \
  --env POSTGRES_PASSWORD=disposable-identity-password \
  "$image" \
  -c listen_addresses= \
  -c hba_file=/hba/pg_hba.conf \
  -c log_statement=all \
  -c logging_collector=on \
  -c log_destination=stderr \
  -c log_directory=/log \
  -c log_filename=server.log \
  -c log_file_mode=0600 \
  -c log_rotation_age=0 \
  -c log_rotation_size=0 \
  -c log_truncate_on_rotation=off \
  -c event_triggers=off >/dev/null
wait_ready "$identity_primary"

# Two of these tests leave their own policy in force, and one of them leaves a
# policy that admits no catalog identity at all. Each starts from the same
# known policy rather than from whatever the previous one left behind.
reset_identity_policy() {
  docker run --rm --user 999:999 --volume "$identity_hba:/hba" \
    --entrypoint /bin/sh "$image" -ceu \
    "printf '%s' '$agent_local_hba_policy' > /hba/pg_hba.conf"
  docker exec --user 999:999 "$identity_primary" psql -X --no-password \
    --host=/var/run/postgresql --username=postgres --dbname=postgres \
    --set=ON_ERROR_STOP=1 --command="SELECT pg_catalog.pg_reload_conf()" >/dev/null
}

run_identity_test() {
  local identity_test="$1"
  reset_identity_policy
  bounded "$identity_test" \
    docker run --rm --user 999:999 --network none --read-only \
      --cap-drop ALL --security-opt no-new-privileges \
      --volume "$identity_socket:/identity-socket" \
      --volume "$identity_hba:/identity-hba" \
      --volume "$identity_log:/identity-log:ro" \
      --mount "type=bind,src=$test_binary,dst=/test/pgshard-agent-test,readonly" \
      --env PGSHARD_AGENT_TEST_SOCKET_DIR=/identity-socket \
      --env PGSHARD_AGENT_TEST_HBA_FILE=/identity-hba/pg_hba.conf \
      --env PGSHARD_AGENT_TEST_SERVER_LOG=/identity-log/server.log \
      --entrypoint /test/pgshard-agent-test \
      "$image" \
      --ignored --exact "$identity_test" --nocapture
}

# The four states a role passes through and the shape it must never be adopted
# from, each divergence one at a time.
run_identity_test \
  catalog_identity::tests::live_postgres18_classifies_the_operator_owned_role_states
run_identity_test \
  catalog_identity::tests::live_postgres18_refuses_to_adopt_a_role_the_operator_does_not_own

# The update's own WHERE clause cannot see the role-wide defaults that are part
# of the operation writer's shape, so only the framed post-condition can refuse
# an installation that would leave a role the operator does not own.
run_identity_test \
  catalog_identity::tests::live_postgres18_refuses_an_installation_that_leaves_a_role_it_does_not_own

# PostgreSQL stores role-wide defaults in application order and the state query
# compares them with an ordered array literal. Only the server settles which
# order that is.
run_identity_test \
  catalog_identity::tests::live_postgres18_role_defaults_store_exactly_the_array_the_state_query_compares

# Binding the verifier keeps it out of the statement text; the DETAIL and
# auto_explain channels are what the installation has to close for itself, on a
# session that demonstrably logs bound parameters both before and after.
run_identity_test \
  catalog_identity::tests::live_postgres18_a_credential_never_reaches_the_statement_log

# A connection that succeeded under trust or peer proves nothing about the
# credential, and the agent's own pre-serving policy uses peer. Both halves of
# that — a path that admits without asking, and a policy that admits nothing —
# need real HBA records the server reloads.
run_identity_test \
  catalog_identity::tests::live_postgres18_a_credential_proof_needs_an_hba_record_that_admits_it

# And the policy the agent installs has to be a policy that admits both catalog
# identities on the catalog database, while still refusing the identity it names
# nowhere and the database it did not name one on.
run_identity_test \
  catalog_identity::tests::live_postgres18_the_installed_policy_admits_both_catalog_identities
run_identity_test \
  catalog_identity::tests::live_postgres18_authenticating_is_not_the_same_as_a_canonical_session

docker rm --force "$identity_primary" >/dev/null
docker volume rm "$identity_socket" "$identity_hba" "$identity_log" >/dev/null

if [[ -n "$runtime_image" ]]; then
  if ! docker stop --time 10 "$standby" >/dev/null; then
    fail_container "$standby" \
      "raw standby did not stop after the generation-mutating test"
  fi
  require_container_exit_code "$standby" 0 \
    "raw standby clean-stop assertion failed after the generation-mutating test"
  docker rm "$standby" >/dev/null
  normalize_standby_socket

  start_agent_standby
  wait_agent_standby_running
  wait_primary_streaming_quorum
  postmaster_environment="$(docker exec --user 999:999 "$standby" /bin/sh -ceu '
    postmaster_pid="$(cat /run/pgshard/postgres/postmaster.external.pid)"
    tr "\000" "\n" < "/proc/$postmaster_pid/environ"
  ')"
  if [[ "$postmaster_environment" == *PGPASSWORD=* ]] || \
      [[ "$postmaster_environment" == *"$ambient_password_canary"* ]]; then
    fail_container "$standby" \
      "supervised standby postmaster inherited an ambient password"
  fi

  # Kill the container before the agent's 5-second smart-stop deadline. A
  # monitor connection left open would block smart shutdown and fail this
  # clean-exit assertion instead of being masked by the agent's fast fallback.
  if ! docker stop --time 4 "$standby" >/dev/null; then
    fail_container "$standby" "supervised standby did not stop on SIGTERM"
  fi
  require_container_exit_code "$standby" 0 \
    "supervised standby clean-stop assertion failed"
  clean_control_data="$(docker run --rm --user 999:999 \
    --volume "$standby_data:/standby:ro" \
    --entrypoint /usr/lib/postgresql/18/bin/pg_controldata \
    "$image" /standby)"
  if [[ "$clean_control_data" != \
      *'Database cluster state:               shut down in recovery'* ]]; then
    fail_container "$standby" \
      "supervised standby control data did not record a clean recovery shutdown"
  fi
  docker rm "$standby" >/dev/null
  normalize_standby_socket
  start_agent_standby
  wait_agent_standby_running
  wait_primary_streaming_quorum
  docker exec --user 999:999 "$standby" \
    pg_ctl -D /var/lib/postgresql/18/docker promote -W || true
  for _ in $(seq 1 120); do
    if [[ "$(docker inspect --format '{{.State.Running}}' "$standby")" = false ]]; then
      break
    fi
    sleep 0.25
  done
  if [[ "$(docker inspect --format '{{.State.Running}}' "$standby")" != false ]]; then
    fail_container "$standby" \
      "promoted supervised standby remained running instead of fencing"
  fi
  if [[ "$(docker inspect --format '{{.State.ExitCode}}' "$standby")" = 0 ]]; then
    fail_container "$standby" \
      "promoted supervised standby exited successfully instead of reporting a fence"
  fi
  standby_logs="$(docker logs "$standby" 2>&1)"
  if [[ "$standby_logs" != *'received promote request'* ]] ||
      [[ "$standby_logs" != *'archive recovery complete'* ]]; then
    fail_container "$standby" \
      "promoted supervised standby logs omitted PostgreSQL promotion evidence"
  fi
  if [[ "$standby_logs" != *'StandbyRecovery { source: RecoveryEnded }'* ]] &&
      [[ "$standby_logs" != \
        *'ReplicationEvidence { source: Observation { source: InvalidReplicationEvidence } }'* ]]; then
    fail_container "$standby" \
      "promoted supervised standby logs omitted an exact fail-closed fence"
  fi
fi
