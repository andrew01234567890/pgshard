// Package resources produces the Kubernetes resources owned by a PgShardCluster.
// Planning is deliberately pure: the controller can test and diff a complete,
// deterministic desired state before it writes anything to the API server.
package resources

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	distreference "github.com/distribution/reference"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ManagedByLabel = "app.kubernetes.io/managed-by"
	InstanceLabel  = "app.kubernetes.io/instance"
	ComponentLabel = "app.kubernetes.io/component"
	ClusterLabel   = "pgshard.io/cluster"
	ShardLabel     = "pgshard.io/shard"
	RoleLabel      = "pgshard.io/role"
	MemberLabel    = "pgshard.io/member"

	ManagedByValue = "pgshard-operator"
	// ClusterResourceFinalizer protects operator-owned resources and marks a
	// PgShardCluster lifecycle that crossed the fencing handshake barrier.
	ClusterResourceFinalizer = "pgshard.io/owned-resources"

	PostgreSQLConfigSuffix   = "-postgresql-config"
	PostgreSQLPasswordKey    = "superuser-password"
	CatalogServiceSuffix     = "-shardschema"
	CatalogPasswordKey       = "catalog-password"
	CatalogCACertificateKey  = "ca.crt"
	CatalogTLSCertificateKey = "tls.crt"
	CatalogTLSPrivateKeyKey  = "tls.key"
	TopologyConfigSuffix     = "-topology"
	EtcdSuffix               = "-etcd"
	OrchestratorSuffix       = "-orchestrator"
	PoolerSuffix             = "-pooler"

	PostgreSQLPort int32 = 5432
	PoolerRWPort   int32 = 5432
	PoolerROPort   int32 = 5433
	PoolerRPort    int32 = 5434
	EtcdClientPort int32 = 2379
	EtcdPeerPort   int32 = 2380
	HTTPPort       int32 = 8080

	etcdExecutable                      = "/usr/local/bin/etcd"
	etcdDataMigrationExecutable         = "/usr/local/bin/pgshard-etcd-data-migrate"
	defaultEtcdImage                    = "registry.k8s.io/etcd:3.6.5-0@sha256:042ef9c02799eb9303abf1aa99b09f09d94b8ee3ba0c2dd3f42dc4e1d3dce534"
	defaultPostgreSQLImage              = "docker.io/library/postgres@sha256:311136771dca6826c3b6e691ebf8cb6e896e165074bc57a728f9619f25f0c4c7"
	developmentPostgreSQLBootstrapImage = "pgshard/postgres-agent:dev"

	ConfigHashAnnotation                    = "pgshard.io/config-hash"
	ApplyOwnershipAnnotation                = "pgshard.io/apply-ownership"
	ApplyOwnershipVersion                   = "v1"
	RetainedFromAnnotation                  = "pgshard.io/retained-from"
	PostgreSQLBootstrapClusterUIDAnnotation = "pgshard.io/bootstrap-cluster-uid"
	CatalogAccessClusterUIDAnnotation       = "pgshard.io/catalog-access-cluster-uid"
	PostgreSQLDataClusterUIDAnnotation      = "pgshard.io/data-cluster-uid"
	PostgreSQLDataProtectionFinalizer       = "pgshard.io/postgresql-data-protection"
	PostgreSQLPodClusterUIDAnnotation       = "pgshard.io/postgresql-cluster-uid"
	PostgreSQLNodeUIDAnnotation             = "pgshard.io/postgresql-node-uid"
	PostgreSQLNodeBootIDAnnotation          = "pgshard.io/postgresql-node-boot-id"
	PostgreSQLPodTerminationFinalizer       = "pgshard.io/postgresql-termination"
	postgresqlBootstrapMarker               = ".pgshard-bootstrap-complete"
	shardschemaMigrationPath                = "/usr/share/pgshard/migrations/0001_shardschema.sql"
	shardschemaMigrationSHA256              = "690aaf875666c5735e1fc0fc649d5746edcd9831e47a5ef155053c6e9d681333"
	shardschemaMigrationHashAnnotation      = "pgshard.io/shardschema-migration-sha256"
)

const postgresqlBootstrapScript = `set -Eeuo pipefail
: "${PGSHARD_NODE_UID:?binding-time node UID is required}"
: "${PGSHARD_NODE_BOOT_ID:?binding-time node boot ID is required}"
: "${PGSHARD_POSTGRESQL_MAJOR:?expected PostgreSQL major is required}"

if [[ "$PGSHARD_POSTGRESQL_MAJOR" != "18" ]]; then
  echo "operator release has an unsupported PostgreSQL major" >&2
  exit 1
fi
postgres_version="$(postgres --version)"
case "$postgres_version" in
  "postgres (PostgreSQL) $PGSHARD_POSTGRESQL_MAJOR."*) ;;
  *)
    echo "bootstrap image does not provide the operator's PostgreSQL major" >&2
    exit 1
    ;;
esac

parent=/var/lib/postgresql/18
volume_root="${parent%/*}"
final="$parent/docker"
staging="$parent/.pgshard-init"
marker="$final/.pgshard-bootstrap-complete"
catalog_genesis_intent="$final/.pgshard-catalog-genesis-intent"
expected="$(mktemp /tmp/pgshard-bootstrap-identity.XXXXXX)"
cleanup_expected() {
  rm -f -- "$expected"
}
trap cleanup_expected EXIT

umask 077
printf 'cluster_uid=%s\nshard=%s\n' "$PGSHARD_CLUSTER_UID" "$PGSHARD_SHARD_ID" > "$expected"

if [[ "$PGSHARD_BOOTSTRAP_SHARDSCHEMA" == "true" ]]; then
  if [[ ! "$PGSHARD_SHARD_COUNT" =~ ^[1-9][0-9]*$ ]] \
    || [[ ! "$PGSHARD_MAXIMUM_SHARDS" =~ ^[1-9][0-9]*$ ]] \
    || (( PGSHARD_SHARD_COUNT > PGSHARD_MAXIMUM_SHARDS )); then
    echo "refusing invalid shardschema shard count" >&2
    exit 1
  fi
  if [[ ! -f "$PGSHARD_SHARDSCHEMA_MIGRATION" ]]; then
    echo "shardschema migration is missing from the bootstrap image" >&2
    exit 1
  fi
  read -r observed_migration_sha _ < <(sha256sum -- "$PGSHARD_SHARDSCHEMA_MIGRATION")
  if [[ "$observed_migration_sha" != "$PGSHARD_SHARDSCHEMA_MIGRATION_SHA256" ]]; then
    echo "shardschema migration does not match the operator release" >&2
    exit 1
  fi
  if [[ ! "$PGSHARD_CATALOG_CLIENT_SHA256" =~ ^[0-9a-f]{64}$ ]] \
    || [[ ! "$PGSHARD_CATALOG_SERVER_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
    echo "refusing invalid checkpointed shardschema material digests" >&2
    exit 1
  fi
  catalog_password="$(</etc/pgshard/catalog-auth/catalog-password)"
  if [[ ! "$catalog_password" =~ ^[0-9a-f]{64}$ ]]; then
    echo "refusing an invalid shardschema reader credential" >&2
    exit 1
  fi
  observed_catalog_client_sha="$(
    pgshard-catalog-material-digest client \
      /etc/pgshard/catalog-auth/catalog-password \
      /etc/pgshard/catalog-auth/ca.crt
  )"
  observed_catalog_server_sha="$(
    pgshard-catalog-material-digest server \
      /etc/pgshard/catalog-tls/tls.key \
      /etc/pgshard/catalog-tls/tls.crt
  )"
  if [[ "$observed_catalog_client_sha" != "$PGSHARD_CATALOG_CLIENT_SHA256" ]] \
    || [[ "$observed_catalog_server_sha" != "$PGSHARD_CATALOG_SERVER_SHA256" ]]; then
    echo "refusing shardschema material that differs from the checkpointed creation result" >&2
    exit 1
  fi
fi

if [[ -f "$final/PG_VERSION" ]]; then
  read -r durable_postgres_major < "$final/PG_VERSION"
  if [[ "$durable_postgres_major" != "$PGSHARD_POSTGRESQL_MAJOR" ]]; then
    echo "refusing a PostgreSQL data directory from another major version" >&2
    exit 1
  fi
fi

if [[ -f "$marker" && -f "$final/PG_VERSION" ]]; then
  if ! cmp -s -- "$marker" "$expected"; then
    echo "refusing PostgreSQL data directory owned by another cluster or shard" >&2
    exit 1
  fi
  rm -rf -- "$staging"
  sync "$final" "$parent" "$volume_root"
elif [[ -e "$final" ]]; then
  echo "refusing to replace an incomplete or unmarked PostgreSQL data directory" >&2
  exit 1
else
  rm -rf -- "$staging"
  mkdir -p -- "$staging"
  chmod 0700 "$staging"
  initdb \
    --pgdata="$staging" \
    --username=postgres \
    --pwfile=/etc/pgshard/bootstrap/superuser-password \
    --auth-local=trust \
    --auth-host=scram-sha-256 \
    --data-checksums \
    --encoding=UTF8 \
    --locale=C
  read -r initialized_postgres_major < "$staging/PG_VERSION"
  if [[ "$initialized_postgres_major" != "$PGSHARD_POSTGRESQL_MAJOR" ]]; then
    echo "initialized PostgreSQL data does not match the operator major" >&2
    exit 1
  fi
  printf '\nhost all all all scram-sha-256\n' >> "$staging/pg_hba.conf"
  cp -- "$expected" "$staging/.pgshard-bootstrap-complete"
  chmod 0600 "$staging/.pgshard-bootstrap-complete"
  if [[ "$PGSHARD_BOOTSTRAP_SHARDSCHEMA" == "true" ]]; then
    printf 'version=1\ncluster_uid=%s\nshard_count=%s\nmigration_sha256=%s\n' \
      "$PGSHARD_CLUSTER_UID" \
      "$PGSHARD_SHARD_COUNT" \
      "$PGSHARD_SHARDSCHEMA_MIGRATION_SHA256" > "$staging/.pgshard-catalog-genesis-intent"
    chmod 0600 "$staging/.pgshard-catalog-genesis-intent"
    sync "$staging/.pgshard-catalog-genesis-intent"
  fi
  # initdb has already persisted the new cluster. Flush only the files and
  # directory entries that this script added so another mounted filesystem
  # cannot delay PostgreSQL bootstrap or Pod termination.
  sync "$staging/pg_hba.conf" "$staging/.pgshard-bootstrap-complete" "$staging"
  mv -- "$staging" "$final"
  sync "$final" "$parent" "$volume_root"
fi

catalog_genesis_pending=false
if [[ "$PGSHARD_BOOTSTRAP_SHARDSCHEMA" == "true" ]]; then
  printf 'version=1\ncluster_uid=%s\nshard_count=%s\nmigration_sha256=%s\n' \
    "$PGSHARD_CLUSTER_UID" \
    "$PGSHARD_SHARD_COUNT" \
    "$PGSHARD_SHARDSCHEMA_MIGRATION_SHA256" > "$expected"
  if [[ -e "$catalog_genesis_intent" || -L "$catalog_genesis_intent" ]]; then
    if [[ ! -f "$catalog_genesis_intent" || -L "$catalog_genesis_intent" ]] \
      || ! cmp -s -- "$catalog_genesis_intent" "$expected"; then
      echo "refusing an invalid or foreign shardschema genesis intent" >&2
      exit 1
    fi
    catalog_genesis_pending=true
  fi
fi

cleanup_expected
trap - EXIT

if [[ ! -f "$final/postgresql.auto.conf" || -L "$final/postgresql.auto.conf" ]]; then
  echo "refusing an unsafe restored postgresql.auto.conf" >&2
  exit 1
fi
if grep -Eq '^[[:space:]]*[^#[:space:]]' "$final/postgresql.auto.conf"; then
  echo "refusing active settings in restored postgresql.auto.conf" >&2
  exit 1
else
  inspect_status=$?
  if (( inspect_status != 1 )); then
    echo "refusing postgresql.auto.conf that cannot be inspected safely" >&2
    exit 1
  fi
fi
for recovery_state in standby.signal recovery.signal; do
  if [[ -e "$final/$recovery_state" || -L "$final/$recovery_state" ]]; then
    echo "refusing PostgreSQL recovery state during primary bootstrap ($recovery_state)" >&2
    exit 1
  fi
done
if [[ -L "$final/pg_wal" || ! -d "$final/pg_wal" ]]; then
  echo "refusing PostgreSQL WAL outside the managed PGDATA directory" >&2
  exit 1
fi
if [[ -L "$final/pg_tblspc" || ! -d "$final/pg_tblspc" ]]; then
  echo "refusing an unsafe PostgreSQL tablespace directory" >&2
  exit 1
fi
if ! tablespace_entry="$(
  find "$final/pg_tblspc" -mindepth 1 -maxdepth 1 -print -quit
)"; then
  echo "refusing a PostgreSQL tablespace directory that cannot be inspected safely" >&2
  exit 1
fi
if [[ -n "$tablespace_entry" ]]; then
  echo "refusing PostgreSQL tablespaces outside the managed PGDATA directory" >&2
  exit 1
fi

# pg_hba.conf is operator-owned state. Non-catalog shards have no restored
# topology to preflight, so publish their canonical rules now. Shard zero must
# defer this durable write until the complete catalog and credential checks
# succeed; RestoreTopologyMismatch cannot alter catalog contents or publish a
# new serving HBA. Starting PostgreSQL to inspect a physical backup can still
# advance internal PGDATA state, so full restore no-mutation belongs to the
# signed-manifest controller preflight before a target is provisioned.
if [[ "$PGSHARD_BOOTSTRAP_SHARDSCHEMA" != "true" ]]; then
  hba_staging="$final/.pgshard-pg_hba.conf.next"
  rm -f -- "$hba_staging"
  printf '%s\n' \
    'local all all trust' \
    'host all all all scram-sha-256' > "$hba_staging"
  chmod 0600 "$hba_staging"
  sync "$hba_staging" "$final"
  mv -- "$hba_staging" "$final/pg_hba.conf"
  sync "$final/pg_hba.conf" "$final"
  exit 0
fi

socket=/tmp/pgshard-catalog-bootstrap
rm -rf -- "$socket"
mkdir -m 0700 -- "$socket"
quarantine_hba="$(mktemp /tmp/pgshard-catalog-bootstrap-hba.XXXXXX)"
chmod 0600 "$quarantine_hba"
printf '%s\n' \
  'local all postgres trust' \
  'local all all reject' \
  'host all all 0.0.0.0/0 reject' \
  'host all all ::0/0 reject' > "$quarantine_hba"
export PGOPTIONS='-c lock_timeout=5s -c statement_timeout=30s -c transaction_timeout=120s -c idle_in_transaction_session_timeout=30s -c search_path=pg_catalog -c quote_all_identifiers=off -c event_triggers=off -c session_replication_role=origin -c session_preload_libraries= -c local_preload_libraries= -c jit=off -c default_tablespace= -c temp_tablespaces= -c default_table_access_method=heap -c default_transaction_read_only=off -c row_security=off -c synchronous_commit=on -c zero_damaged_pages=off -c ignore_checksum_failure=off -c password_encryption=scram-sha-256 -c scram_iterations=4096 -c log_statement=none -c log_min_error_statement=panic -c log_min_duration_statement=-1 -c log_min_duration_sample=-1 -c log_statement_sample_rate=0 -c log_transaction_sample_rate=0 -c log_duration=off -c log_parameter_max_length=0 -c log_parameter_max_length_on_error=0 -c log_min_messages=warning -c debug_print_parse=off -c debug_print_rewritten=off -c debug_print_plan=off -c log_parser_stats=off -c log_planner_stats=off -c log_executor_stats=off -c log_statement_stats=off'
cleanup_bootstrap_runtime() {
  rm -f -- "$quarantine_hba"
  rm -rf -- "$socket"
}
stop_temporary_postgres() {
  result=$?
  trap - EXIT
  if pg_ctl -D "$final" status >/dev/null 2>&1; then
    if ! pg_ctl -D "$final" -w -t 45 stop -m fast; then
      result=1
    fi
  fi
  cleanup_bootstrap_runtime
  exit "$result"
}
trap stop_temporary_postgres EXIT

# Role and database defaults remain untrusted even after active restored
# postgresql.auto.conf settings have been rejected. Mirror the agent's
# quarantine launch boundary: command-line values override inherited settings,
# callbacks and preload libraries are disabled, durability checks stay strict,
# and only this container's private Unix socket can authenticate.
pg_ctl -D "$final" -w -t 45 start \
  -o "-c config_file=/etc/pgshard/postgresql/primary-0000.conf -c data_directory='$final' -c hba_file='$quarantine_hba' -c external_pid_file=/tmp/pgshard-catalog-bootstrap.pid -c listen_addresses='' -c unix_socket_directories='$socket' -c unix_socket_permissions=0700 -c unix_socket_group= -c port=5432 -c ssl=off -c restart_after_crash=off -c primary_conninfo= -c primary_slot_name= -c restore_command= -c archive_cleanup_command= -c recovery_end_command= -c archive_mode=on -c archive_command= -c archive_library= -c max_wal_senders=0 -c max_logical_replication_workers=0 -c sync_replication_slots=off -c wal_receiver_create_temp_slot=off -c idle_replication_slot_timeout=0 -c max_slot_wal_keep_size=-1 -c shared_preload_libraries= -c session_preload_libraries= -c local_preload_libraries= -c event_triggers=off -c jit=off -c fsync=on -c full_page_writes=on -c synchronous_commit=on -c ignore_invalid_pages=off -c data_sync_retry=off -c ignore_checksum_failure=off -c zero_damaged_pages=off -c password_encryption=scram-sha-256 -c scram_iterations=4096 -c logging_collector=off -c log_statement=none -c log_min_error_statement=panic -c log_min_duration_statement=-1 -c log_min_duration_sample=-1 -c log_statement_sample_rate=0 -c log_transaction_sample_rate=0 -c log_duration=off -c log_parameter_max_length=0 -c log_parameter_max_length_on_error=0"

validate_bootstrap_session_policy() {
  local database="$1"
  session_policy="$(
    psql -X --no-password --host="$socket" --username=postgres --dbname="$database" \
      --set=ON_ERROR_STOP=1 --no-align --tuples-only --command="
        SELECT CASE WHEN
          current_setting('search_path') = 'pg_catalog'
          AND current_setting('quote_all_identifiers') = 'off'
          AND current_setting('event_triggers') = 'off'
          AND current_setting('session_replication_role') = 'origin'
          AND current_setting('default_transaction_read_only') = 'off'
          AND current_setting('row_security') = 'off'
          AND current_setting('synchronous_commit') = 'on'
          AND current_setting('zero_damaged_pages') = 'off'
          AND current_setting('ignore_checksum_failure') = 'off'
          AND current_setting('password_encryption') = 'scram-sha-256'
          AND current_setting('scram_iterations') = '4096'
          AND current_setting('log_statement') = 'none'
          AND current_setting('log_min_error_statement') = 'panic'
          AND current_setting('log_min_duration_statement') = '-1'
          AND current_setting('log_min_duration_sample') = '-1'
          AND current_setting('log_statement_sample_rate')::numeric = 0
          AND current_setting('log_transaction_sample_rate')::numeric = 0
          AND current_setting('log_duration') = 'off'
          AND current_setting('log_parameter_max_length') = '0'
          AND current_setting('log_parameter_max_length_on_error') = '0'
          AND current_setting('debug_print_parse') = 'off'
          AND current_setting('debug_print_rewritten') = 'off'
          AND current_setting('debug_print_plan') = 'off'
          AND current_setting('log_parser_stats') = 'off'
          AND current_setting('log_planner_stats') = 'off'
          AND current_setting('log_executor_stats') = 'off'
          AND current_setting('log_statement_stats') = 'off'
        THEN 1 ELSE 0 END"
  )"
  if [[ "$session_policy" != "1" ]]; then
    echo "refusing to inspect shardschema without the enforced bootstrap session policy" >&2
    return 1
  fi
}

# Verify command-line and PGOPTIONS precedence before the first restored
# catalog lookup. Database- and role-scoped settings are untrusted input.
validate_bootstrap_session_policy postgres

database_exists="$(
  psql -X --no-password --host="$socket" --username=postgres --dbname=postgres --no-align --tuples-only \
    --command="SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'shardschema'"
)"
case "$database_exists" in
  1) ;;
  "")
    if [[ "$catalog_genesis_pending" != "true" ]]; then
      echo "refusing pre-existing PGDATA without durable shardschema topology evidence" >&2
      exit 1
    fi
    createdb --no-password --host="$socket" --username=postgres --template=template0 --encoding=UTF8 shardschema
    ;;
  *)
    echo "refusing ambiguous shardschema database lookup" >&2
    exit 1
    ;;
esac

validate_bootstrap_session_policy shardschema

validate_cluster_configuration() {
  configuration_state="$(
    psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
      --set=ON_ERROR_STOP=1 --no-align --tuples-only --command="
        SELECT pg_catalog.count(*)
          FROM pgshard_catalog.cluster_configuration AS configuration
         WHERE configuration.singleton
           AND configuration.home_shard_id::text = 'shard-0000'"
  )"
  if [[ "$configuration_state" != "1" ]]; then
    echo "refusing shardschema home-shard identity that conflicts with shard-0000" >&2
    return 1
  fi
}

count_missing_shards() {
  psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
    --set=ON_ERROR_STOP=1 \
    --no-align --tuples-only --command="
      SELECT pg_catalog.count(*)
        FROM pg_catalog.generate_series(0, $PGSHARD_SHARD_COUNT::bigint - 1) AS expected(shard_number)
        LEFT JOIN pgshard_catalog.shards AS shards
          ON shards.shard_id::text = 'shard-' || pg_catalog.lpad(expected.shard_number::text, 4, '0')
         AND shards.shard_number = expected.shard_number
       WHERE shards.shard_id IS NULL"
}

validate_shard_inventory() {
  local allow_missing="${1:-false}"
  invalid_shards="$(
    psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
      --set=ON_ERROR_STOP=1 --no-align --tuples-only --command="
        SELECT pg_catalog.count(*)
          FROM pgshard_catalog.shards AS shards
         WHERE NOT (
           shards.shard_id::text = 'shard-' || pg_catalog.lpad(
             shards.shard_number::text,
             CASE
               WHEN pg_catalog.length(shards.shard_number::text) < 4 THEN 4
               ELSE pg_catalog.length(shards.shard_number::text)
             END,
             '0'
           )
           AND (
             (shards.shard_number < $PGSHARD_SHARD_COUNT::bigint AND shards.state = 'active')
             OR (shards.shard_number >= $PGSHARD_SHARD_COUNT::bigint AND shards.state = 'retired')
           )
         )"
  )"
  if [[ "$invalid_shards" != "0" ]]; then
    echo "RestoreTopologyMismatch: shardschema inventory conflicts with the configured immutable shard topology" >&2
    return 1
  fi
  if [[ "$allow_missing" != "true" ]] && [[ "$(count_missing_shards)" != "0" ]]; then
    echo "RestoreTopologyMismatch: shardschema inventory conflicts with the configured immutable shard topology" >&2
    return 1
  fi
}

validate_genesis_inventory_reachable() {
  genesis_inventory_state="$(
    psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
      --set=ON_ERROR_STOP=1 --no-align --tuples-only --command="
        SELECT CASE WHEN
          (
            pg_catalog.count(*) = 1
            AND pg_catalog.count(*) FILTER (
                  WHERE shards.shard_id::text = 'shard-0000'
                    AND shards.shard_number = 0
                    AND shards.state = 'active'
                ) = 1
          ) OR (
            pg_catalog.count(*) = $PGSHARD_SHARD_COUNT::bigint
            AND pg_catalog.count(*) FILTER (
                  WHERE shards.state = 'active'
                    AND shards.shard_number >= 0
                    AND shards.shard_number < $PGSHARD_SHARD_COUNT::bigint
                    AND shards.shard_id::text = 'shard-' || pg_catalog.lpad(
                          shards.shard_number::text,
                          4,
                          '0'
                        )
                ) = $PGSHARD_SHARD_COUNT::bigint
          )
          THEN 1 ELSE 0 END
          FROM pgshard_catalog.shards AS shards"
  )"
  if [[ "$genesis_inventory_state" != "1" ]]; then
    echo "RestoreTopologyMismatch: shardschema inventory is not a reachable genesis state for the configured immutable shard topology" >&2
    return 1
  fi
}

validate_restore_lineage() {
  invalid_lineage="$(
    psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
      --set=ON_ERROR_STOP=1 --no-align --tuples-only --command="
        SELECT (
                 SELECT pg_catalog.count(*)
                   FROM pgshard_catalog.shards AS shards
                  WHERE NOT EXISTS (
                          SELECT
                            FROM pgshard_catalog.shard_restore_incarnations AS history
                           WHERE history.shard_id = shards.shard_id
                        )
                     OR (shards.state = 'active') IS DISTINCT FROM EXISTS (
                          SELECT
                            FROM pgshard_catalog.shard_restore_incarnations AS incarnations
                           WHERE incarnations.shard_id = shards.shard_id
                             AND incarnations.state = 'active'
                        )
               ) + (
                 SELECT pg_catalog.count(*)
                   FROM pgshard_catalog.shard_restore_incarnations AS incarnations
                   LEFT JOIN pgshard_catalog.shards AS shards
                     ON shards.shard_id = incarnations.shard_id
                  WHERE shards.shard_id IS NULL
               )"
  )"
  if [[ "$invalid_lineage" != "0" ]]; then
    echo "refusing shardschema restore lineage that conflicts with shard state" >&2
    return 1
  fi
}

validate_catalog_sequence_progress() {
  unsafe_sequences="$(
    psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
      --set=ON_ERROR_STOP=1 --no-align --tuples-only --command="
        SELECT pg_catalog.count(*)
          FROM (
            SELECT
              CASE
                WHEN sequence_state.is_called
                  THEN sequence_state.last_value::numeric + 1
                ELSE sequence_state.last_value::numeric
              END AS next_value,
              COALESCE(
                (SELECT pg_catalog.max(routing_epoch)::numeric
                   FROM pgshard_catalog.routing_epochs),
                0
              ) AS maximum_value,
              (SELECT sequences.seqmax::numeric
                 FROM pg_catalog.pg_sequence AS sequences
                WHERE sequences.seqrelid =
                      'pgshard_catalog.routing_epochs_routing_epoch_seq'::pg_catalog.regclass
              ) AS maximum_generated_value
              FROM pgshard_catalog.routing_epochs_routing_epoch_seq AS sequence_state
            UNION ALL
            SELECT
              CASE
                WHEN sequence_state.is_called
                  THEN sequence_state.last_value::numeric + 1
                ELSE sequence_state.last_value::numeric
              END,
              COALESCE(
                (SELECT pg_catalog.max(registered_table_id)::numeric
                   FROM pgshard_catalog.registered_tables),
                0
              ),
              (SELECT sequences.seqmax::numeric
                 FROM pg_catalog.pg_sequence AS sequences
                WHERE sequences.seqrelid =
                      'pgshard_catalog.registered_tables_registered_table_id_seq'::pg_catalog.regclass
              )
              FROM pgshard_catalog.registered_tables_registered_table_id_seq AS sequence_state
          ) AS sequence_progress
         WHERE sequence_progress.next_value <= sequence_progress.maximum_value
            OR sequence_progress.next_value > sequence_progress.maximum_generated_value"
  )"
  if [[ "$unsafe_sequences" != "0" ]]; then
    echo "refusing shardschema identity sequence progress that conflicts with catalog rows" >&2
    return 1
  fi
}

validate_catalog_inventory() {
  local allow_missing="${1:-false}"
  validate_cluster_configuration
  validate_shard_inventory "$allow_missing"
  if [[ "$allow_missing" == "true" ]]; then
    validate_genesis_inventory_reachable
  fi
  validate_restore_lineage
  validate_catalog_sequence_progress
}

# CREATE TABLE IF NOT EXISTS cannot repair a damaged pre-existing relation.
# Fingerprint every namespaced object plus the behavior-bearing relation,
# sequence, column, constraint, index, type, routine-signature, rule, trigger,
# and policy metadata. Only the released v0.49 shape and the current shape
# reached by a fresh install or v0.49 upgrade are valid inputs. Function bodies
# are replaced by the migration; their exact signatures and execution
# attributes still participate in the shape.
catalog_schema_fingerprint="$(
  psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
    --set=ON_ERROR_STOP=1 --quiet --no-align --tuples-only --command="
      SET SESSION search_path = pg_catalog;
      SET SESSION quote_all_identifiers = off;
      WITH catalog_namespace AS MATERIALIZED (
        SELECT namespaces.oid
          FROM pg_catalog.pg_namespace AS namespaces
         WHERE namespaces.nspname = 'pgshard_catalog'
      ), catalog_objects AS (
        SELECT pg_catalog.format(
                   'namespace|%s',
                   pg_catalog.pg_describe_object(
                     dependencies.classid,
                     dependencies.objid,
                     dependencies.objsubid
                   )
               ) AS object
          FROM pg_catalog.pg_depend AS dependencies
         WHERE dependencies.refclassid = 'pg_catalog.pg_namespace'::pg_catalog.regclass
           AND dependencies.refobjid = (SELECT oid FROM catalog_namespace)
           AND dependencies.refobjsubid = 0
           AND dependencies.deptype = 'n'
        UNION ALL
        SELECT pg_catalog.format(
                   'relation|%s|%s|%s|%s|%s|%s|%s|%s|%s',
                   relations.relname,
                   relations.relkind,
                   relations.relpersistence,
                   relations.relreplident,
                   relations.relrowsecurity,
                   relations.relforcerowsecurity,
                   relations.relispartition,
                   COALESCE(access_methods.amname, ''),
                   COALESCE(pg_catalog.array_to_string(relations.reloptions, ','), '')
               )
          FROM pg_catalog.pg_class AS relations
          LEFT JOIN pg_catalog.pg_am AS access_methods
            ON access_methods.oid = relations.relam
         WHERE relations.relnamespace = (SELECT oid FROM catalog_namespace)
           AND relations.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
        UNION ALL
        SELECT pg_catalog.format(
                   'sequence|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s',
                   sequences.relname,
                   pg_catalog.format_type(sequence_metadata.seqtypid, NULL),
                   sequence_metadata.seqstart,
                   sequence_metadata.seqincrement,
                   sequence_metadata.seqmax,
                   sequence_metadata.seqmin,
                   sequence_metadata.seqcache,
                   sequence_metadata.seqcycle,
                   COALESCE(owned_relations.relname, ''),
                   COALESCE(owned_attributes.attname, '')
               )
          FROM pg_catalog.pg_sequence AS sequence_metadata
          JOIN pg_catalog.pg_class AS sequences
            ON sequences.oid = sequence_metadata.seqrelid
          LEFT JOIN pg_catalog.pg_depend AS ownership
            ON ownership.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
           AND ownership.objid = sequences.oid
           AND ownership.objsubid = 0
           AND ownership.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass
           AND ownership.refobjsubid > 0
           AND ownership.deptype IN ('a', 'i')
          LEFT JOIN pg_catalog.pg_class AS owned_relations
            ON owned_relations.oid = ownership.refobjid
          LEFT JOIN pg_catalog.pg_attribute AS owned_attributes
            ON owned_attributes.attrelid = ownership.refobjid
           AND owned_attributes.attnum = ownership.refobjsubid
         WHERE sequences.relnamespace = (SELECT oid FROM catalog_namespace)
        UNION ALL
        SELECT pg_catalog.format(
                   'inherits|%s|%s|%s',
                   children.relname,
                   parent_namespaces.nspname,
                   parents.relname
               )
          FROM pg_catalog.pg_inherits AS inheritance
          JOIN pg_catalog.pg_class AS children ON children.oid = inheritance.inhrelid
          JOIN pg_catalog.pg_class AS parents ON parents.oid = inheritance.inhparent
          JOIN pg_catalog.pg_namespace AS parent_namespaces
            ON parent_namespaces.oid = parents.relnamespace
         WHERE children.relnamespace = (SELECT oid FROM catalog_namespace)
        UNION ALL
        SELECT pg_catalog.format(
                   'column|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s',
                   relations.relname,
                   attributes.attnum,
                   attributes.attname,
                   pg_catalog.format_type(attributes.atttypid, attributes.atttypmod),
                   attributes.attnotnull,
                   attributes.attidentity,
                   attributes.attgenerated,
                   attributes.attstorage,
                   attributes.attcompression,
                   CASE
                     WHEN attributes.attcollation = 0 THEN ''
                     ELSE pg_catalog.format(
                       '%I.%I',
                       collation_namespaces.nspname,
                       collations.collname
                     )
                   END,
                   COALESCE(
                     pg_catalog.pg_get_expr(defaults.adbin, defaults.adrelid, true),
                     ''
                   )
               )
          FROM pg_catalog.pg_attribute AS attributes
          JOIN pg_catalog.pg_class AS relations ON relations.oid = attributes.attrelid
          LEFT JOIN pg_catalog.pg_attrdef AS defaults
            ON defaults.adrelid = attributes.attrelid
           AND defaults.adnum = attributes.attnum
          LEFT JOIN pg_catalog.pg_collation AS collations
            ON collations.oid = attributes.attcollation
          LEFT JOIN pg_catalog.pg_namespace AS collation_namespaces
            ON collation_namespaces.oid = collations.collnamespace
         WHERE relations.relnamespace = (SELECT oid FROM catalog_namespace)
           AND relations.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
           AND attributes.attnum > 0
           AND NOT attributes.attisdropped
        UNION ALL
        SELECT pg_catalog.format(
                   'dropped-column|%s|%s',
                   relations.relname,
                   attributes.attnum
               )
          FROM pg_catalog.pg_attribute AS attributes
          JOIN pg_catalog.pg_class AS relations ON relations.oid = attributes.attrelid
         WHERE relations.relnamespace = (SELECT oid FROM catalog_namespace)
           AND attributes.attnum > 0
           AND attributes.attisdropped
        UNION ALL
        SELECT pg_catalog.format(
                   'constraint|%s|%s|%s|%s|%s|%s|%s',
                   COALESCE(relations.relname, ''),
                   constraints.conname,
                   constraints.contype,
                   constraints.condeferrable,
                   constraints.condeferred,
                   constraints.convalidated,
                   pg_catalog.pg_get_constraintdef(constraints.oid, true)
               )
          FROM pg_catalog.pg_constraint AS constraints
          LEFT JOIN pg_catalog.pg_class AS relations ON relations.oid = constraints.conrelid
         WHERE constraints.connamespace = (SELECT oid FROM catalog_namespace)
        UNION ALL
        SELECT pg_catalog.format(
                   'index|%s|%s|%s|%s|%s|%s|%s|%s|%s',
                   tables.relname,
                   indexes.relname,
                   index_metadata.indisvalid,
                   index_metadata.indisready,
                   index_metadata.indisunique,
                   index_metadata.indisprimary,
                   index_metadata.indisexclusion,
                   index_metadata.indisreplident,
                   pg_catalog.pg_get_indexdef(indexes.oid)
               )
          FROM pg_catalog.pg_index AS index_metadata
          JOIN pg_catalog.pg_class AS indexes ON indexes.oid = index_metadata.indexrelid
          JOIN pg_catalog.pg_class AS tables ON tables.oid = index_metadata.indrelid
         WHERE tables.relnamespace = (SELECT oid FROM catalog_namespace)
        UNION ALL
        SELECT pg_catalog.format(
                   'type|%s|%s|%s|%s|%s|%s',
                   types.typname,
                   types.typtype,
                   pg_catalog.format_type(types.typbasetype, types.typtypmod),
                   types.typnotnull,
                   CASE
                     WHEN types.typcollation = 0 THEN ''
                     ELSE pg_catalog.format(
                       '%I.%I',
                       collation_namespaces.nspname,
                       collations.collname
                     )
                   END,
                   COALESCE(
                     pg_catalog.pg_get_expr(types.typdefaultbin, 0, true),
                     types.typdefault,
                     ''
                   )
               )
          FROM pg_catalog.pg_type AS types
          LEFT JOIN pg_catalog.pg_collation AS collations ON collations.oid = types.typcollation
          LEFT JOIN pg_catalog.pg_namespace AS collation_namespaces
            ON collation_namespaces.oid = collations.collnamespace
         WHERE types.typnamespace = (SELECT oid FROM catalog_namespace)
           AND types.typtype IN ('d', 'e')
        UNION ALL
        SELECT pg_catalog.format(
                   'enum|%s|%s|%s',
                   types.typname,
                   enum_values.enumsortorder,
                   enum_values.enumlabel
               )
          FROM pg_catalog.pg_enum AS enum_values
          JOIN pg_catalog.pg_type AS types ON types.oid = enum_values.enumtypid
         WHERE types.typnamespace = (SELECT oid FROM catalog_namespace)
        UNION ALL
        SELECT pg_catalog.format(
                   'routine|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s',
                   routines.proname,
                   pg_catalog.pg_get_function_identity_arguments(routines.oid),
                   pg_catalog.format_type(routines.prorettype, NULL),
                   routines.prokind,
                   routines.provolatile,
                   routines.prosecdef,
                   routines.proleakproof,
                   routines.proparallel,
                   routines.proisstrict,
                   COALESCE(pg_catalog.array_to_string(routines.proconfig, ','), '')
               )
         FROM pg_catalog.pg_proc AS routines
         WHERE routines.pronamespace = (SELECT oid FROM catalog_namespace)
        UNION ALL
        SELECT pg_catalog.format(
                   'rule|%s|%s|%s|%s|%s|%s',
                   relations.relname,
                   rewrite_rules.rulename,
                   rewrite_rules.ev_type,
                   rewrite_rules.ev_enabled,
                   rewrite_rules.is_instead,
                   pg_catalog.pg_get_ruledef(rewrite_rules.oid, true)
               )
          FROM pg_catalog.pg_rewrite AS rewrite_rules
          JOIN pg_catalog.pg_class AS relations ON relations.oid = rewrite_rules.ev_class
         WHERE relations.relnamespace = (SELECT oid FROM catalog_namespace)
        UNION ALL
        SELECT pg_catalog.format(
                   'trigger|%s|%s|%s|%s',
                   relations.relname,
                   triggers.tgname,
                   triggers.tgenabled,
                   pg_catalog.pg_get_triggerdef(triggers.oid, true)
               )
          FROM pg_catalog.pg_trigger AS triggers
          JOIN pg_catalog.pg_class AS relations ON relations.oid = triggers.tgrelid
         WHERE relations.relnamespace = (SELECT oid FROM catalog_namespace)
           AND NOT triggers.tgisinternal
        UNION ALL
        SELECT pg_catalog.format(
                   'internal-trigger|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s',
                   relations.relname,
                   constraints.conname,
                   routines.proname,
                   triggers.tgtype,
                   triggers.tgenabled,
                   triggers.tgdeferrable,
                   triggers.tginitdeferred,
                   triggers.tgnargs,
                   pg_catalog.octet_length(triggers.tgargs),
                   triggers.tgattr,
                   triggers.tgqual IS NULL AND triggers.tgparentid = 0
               )
          FROM pg_catalog.pg_trigger AS triggers
          JOIN pg_catalog.pg_class AS relations ON relations.oid = triggers.tgrelid
          JOIN pg_catalog.pg_constraint AS constraints ON constraints.oid = triggers.tgconstraint
          JOIN pg_catalog.pg_proc AS routines ON routines.oid = triggers.tgfoid
         WHERE relations.relnamespace = (SELECT oid FROM catalog_namespace)
           AND triggers.tgisinternal
        UNION ALL
        SELECT pg_catalog.format(
                   'policy|%s|%s|%s|%s|%s|%s|%s',
                   relations.relname,
                   policies.polname,
                   policies.polcmd,
                   policies.polpermissive,
                   COALESCE(pg_catalog.array_to_string(policies.polroles, ','), ''),
                   COALESCE(
                     pg_catalog.pg_get_expr(policies.polqual, policies.polrelid, true),
                     ''
                   ),
                   COALESCE(
                     pg_catalog.pg_get_expr(policies.polwithcheck, policies.polrelid, true),
                     ''
                   )
               )
          FROM pg_catalog.pg_policy AS policies
          JOIN pg_catalog.pg_class AS relations ON relations.oid = policies.polrelid
         WHERE relations.relnamespace = (SELECT oid FROM catalog_namespace)
      )
      SELECT object
        FROM catalog_objects
       ORDER BY object COLLATE \"C\"" |
    sha256sum
)"
catalog_schema_fingerprint="${catalog_schema_fingerprint%% *}"

catalog_core_tables="$(
  psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
    --set=ON_ERROR_STOP=1 --no-align --tuples-only --command="
      SELECT
        pg_catalog.to_regclass('pgshard_catalog.cluster_configuration') IS NOT NULL,
        pg_catalog.to_regclass('pgshard_catalog.shards') IS NOT NULL,
        pg_catalog.to_regclass('pgshard_catalog.shard_restore_incarnations') IS NOT NULL"
)"
case "$catalog_core_tables" in
  "f|f|f")
    if [[ "$catalog_schema_fingerprint" != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" ]]; then
      echo "refusing a non-empty pre-existing pgshard_catalog schema" >&2
      exit 1
    fi
    if [[ "$catalog_genesis_pending" != "true" ]]; then
      echo "refusing an empty pre-existing shardschema without durable genesis evidence" >&2
      exit 1
    fi
    catalog_requires_initial_inventory=true
    ;;
  "t|t|t")
    case "$catalog_schema_fingerprint" in
      "ee17a64c8eec5e2e9a44f29d4764edac90680980f61df35bdb2284c01b57c4d9"|\
      "2720fa78d0bc96c21311b1656eeaabbb3e745ea65fa9d1ea701ffb67cde1b1d9"|\
      "ceec4ff5d633d28afacf1e93fbc2547591017e57f172dc3a8072814bb6d3867a") ;;
      *)
        echo "refusing an unsupported or malformed pre-existing shardschema catalog ($catalog_schema_fingerprint)" >&2
        exit 1
        ;;
    esac
    if [[ "$catalog_genesis_pending" == "true" ]]; then
      validate_catalog_inventory true
      catalog_requires_initial_inventory=true
    else
      validate_catalog_inventory false
      catalog_requires_initial_inventory=false
    fi
    ;;
  *)
    echo "refusing a partial pre-existing shardschema catalog" >&2
    exit 1
    ;;
esac

psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
  --set=ON_ERROR_STOP=1 --file="$PGSHARD_SHARDSCHEMA_MIGRATION"

if [[ "$catalog_requires_initial_inventory" == "true" ]]; then
  # A hard crash can leave either migration-created shard zero or the complete
  # atomic inventory. No other subset can be produced by this bootstrap.
  validate_catalog_inventory true
fi

missing_shards="$(count_missing_shards)"
if [[ "$missing_shards" != "0" ]]; then
  if [[ "$catalog_requires_initial_inventory" != "true" ]]; then
    echo "RestoreTopologyMismatch: shardschema inventory conflicts with the configured immutable shard topology" >&2
    exit 1
  fi
  psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
    --set=ON_ERROR_STOP=1 <<PGSHARD_SHARD_INVENTORY
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
SET LOCAL session_replication_role = origin;
INSERT INTO pgshard_catalog.shards(shard_id, shard_number, state)
SELECT (
         'shard-' || pg_catalog.lpad(expected.shard_number::text, 4, '0')
       )::pgshard_catalog.resource_name,
       expected.shard_number,
       'active'
  FROM pg_catalog.generate_series(0, $PGSHARD_SHARD_COUNT::bigint - 1) AS expected(shard_number)
  LEFT JOIN pgshard_catalog.shards AS shards
    ON shards.shard_id::text = 'shard-' || pg_catalog.lpad(expected.shard_number::text, 4, '0')
   AND shards.shard_number = expected.shard_number
 WHERE shards.shard_id IS NULL;
DO \$pgshard_inventory_postcondition\$
BEGIN
  IF EXISTS (
      SELECT
        FROM pg_catalog.generate_series(
               0,
               $PGSHARD_SHARD_COUNT::bigint - 1
             ) AS expected(shard_number)
        LEFT JOIN pgshard_catalog.shards AS shards
          ON shards.shard_id::text = 'shard-' || pg_catalog.lpad(
               expected.shard_number::text,
               4,
               '0'
             )
         AND shards.shard_number = expected.shard_number
       WHERE shards.shard_id IS NULL
          OR shards.state <> 'active'
          OR NOT EXISTS (
               SELECT
                 FROM pgshard_catalog.shard_restore_incarnations AS incarnations
                WHERE incarnations.shard_id = shards.shard_id
                  AND incarnations.state = 'active'
             )
  ) THEN
    RAISE EXCEPTION USING
      ERRCODE = '55000',
      MESSAGE = 'initial shardschema inventory failed its transactional postcondition';
  END IF;
END
\$pgshard_inventory_postcondition\$;
COMMIT;
PGSHARD_SHARD_INVENTORY
fi

validate_catalog_inventory false

if [[ "$(count_missing_shards)" != "0" ]]; then
  echo "refusing shardschema inventory with missing configured shards" >&2
  exit 1
fi

validate_bootstrap_session_policy shardschema

read_catalog_role_state() {
  psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
    --set=ON_ERROR_STOP=1 --no-align --tuples-only --command="
      SELECT COALESCE((
        SELECT CASE WHEN
        NOT roles.rolsuper
        AND roles.rolinherit
        AND NOT roles.rolcreaterole
        AND NOT roles.rolcreatedb
        AND NOT roles.rolreplication
        AND NOT roles.rolbypassrls
        AND roles.rolconnlimit = -1
        AND roles.rolvaliduntil IS NULL
        AND (
          NOT EXISTS (
            SELECT
              FROM pg_catalog.pg_db_role_setting AS settings
             WHERE settings.setrole = roles.oid
          )
          OR EXISTS (
            SELECT
              FROM pg_catalog.pg_db_role_setting AS settings
              JOIN pg_catalog.pg_database AS databases
                ON databases.oid = settings.setdatabase
             WHERE settings.setrole = roles.oid
               AND databases.datname = 'shardschema'
               AND settings.setconfig = ARRAY[
                     'search_path=pg_catalog',
                     'statement_timeout=30s',
                     'lock_timeout=5s',
                     'transaction_timeout=120s',
                     'idle_in_transaction_session_timeout=30s',
                     'default_transaction_read_only=off',
                     'row_security=off',
                     'synchronous_commit=on',
                     'zero_damaged_pages=off',
                     'ignore_checksum_failure=off',
                     'jit=off'
                   ]::text[]
               AND NOT EXISTS (
                     SELECT
                       FROM pg_catalog.pg_db_role_setting AS other_settings
                      WHERE other_settings.setrole = roles.oid
                        AND (other_settings.setdatabase, other_settings.setrole)
                            IS DISTINCT FROM (settings.setdatabase, settings.setrole)
                   )
          )
        )
        AND (
          SELECT pg_catalog.count(*)
            FROM pg_catalog.pg_auth_members AS memberships
           WHERE memberships.member = roles.oid
        ) = 1
        AND EXISTS (
          SELECT
            FROM pg_catalog.pg_auth_members AS memberships
           WHERE memberships.member = roles.oid
             AND memberships.roleid = 'pgshard_catalog_reader'::pg_catalog.regrole
             AND memberships.grantor = (
               SELECT principals.oid
                 FROM pg_catalog.pg_roles AS principals
                WHERE principals.rolname = current_user
             )
             AND NOT memberships.admin_option
             AND memberships.inherit_option
             AND NOT memberships.set_option
        )
        AND NOT EXISTS (
          SELECT
            FROM pg_catalog.pg_auth_members AS memberships
           WHERE memberships.roleid = roles.oid
        )
        AND NOT EXISTS (
          SELECT
            FROM pg_catalog.pg_database AS databases
           WHERE databases.datdba = roles.oid
        )
        AND NOT EXISTS (
          SELECT
            FROM pg_catalog.pg_tablespace AS tablespaces
           WHERE tablespaces.spcowner = roles.oid
        )
        THEN CASE
          WHEN roles.rolcanlogin
            AND roles.rolpassword LIKE 'SCRAM-SHA-256\$4096:%'
            THEN 'safe'
          WHEN NOT roles.rolcanlogin
            AND roles.rolpassword IS NULL
            THEN 'staging'
          ELSE 'unsafe'
        END
        ELSE 'unsafe'
      END
        FROM pg_catalog.pg_authid AS roles
       WHERE roles.rolname = 'pgshard_pooler_catalog'
      ), 'absent')"
}

catalog_role_state="$(read_catalog_role_state)"
case "$catalog_role_state" in
  absent)
    # Establish a harmless, crash-recoverable role shape first. It cannot log
    # in until the verifier is installed by the parameterized catalog update.
    psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
      --set=ON_ERROR_STOP=1 <<'PGSHARD_CATALOG_LOGIN_STAGING'
BEGIN;
CREATE ROLE pgshard_pooler_catalog
  NOLOGIN NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION
  NOBYPASSRLS CONNECTION LIMIT -1;
GRANT pgshard_catalog_reader TO pgshard_pooler_catalog
  WITH ADMIN FALSE, INHERIT TRUE, SET FALSE;
COMMIT;
PGSHARD_CATALOG_LOGIN_STAGING
    catalog_role_state=staging
    ;;
  staging|safe) ;;
  *)
    echo "refusing an unsafe shardschema reader role" >&2
    exit 1
    ;;
esac

if [[ "$catalog_role_state" == "staging" ]]; then
  # Generate the SCRAM verifier client-side. During verifier installation, the
  # plaintext password is never part of SQL, argv, or environment, and the
  # verifier is a bind value with error-parameter logging disabled rather than
  # loggable query text.
  catalog_scram_verifier="$(
    pgshard-scram-verifier < /etc/pgshard/catalog-auth/catalog-password
  )"
  case "$catalog_scram_verifier" in
    'SCRAM-SHA-256$4096:'*) ;;
    *)
      echo "refusing an invalid client-generated SCRAM verifier" >&2
      exit 1
      ;;
  esac
  catalog_login_update="$(
    {
      printf '%s\n' \
        'UPDATE pg_catalog.pg_authid SET rolpassword = $1, rolcanlogin = true WHERE rolname = '\''pgshard_pooler_catalog'\'' AND NOT rolcanlogin AND rolpassword IS NULL AND NOT rolsuper AND rolinherit AND NOT rolcreaterole AND NOT rolcreatedb AND NOT rolreplication AND NOT rolbypassrls AND rolconnlimit = -1 AND rolvaliduntil IS NULL RETURNING 1'
      printf '%s %s\n' '\bind' "'$catalog_scram_verifier'"
      printf '%s\n' '\g'
    } | psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
      --set=ON_ERROR_STOP=1 --quiet --no-align --tuples-only
  )"
  unset catalog_scram_verifier
  if [[ "$catalog_login_update" != "1" ]]; then
    echo "refusing a shardschema reader role that changed during credential installation" >&2
    exit 1
  fi
fi

catalog_role_state="$(read_catalog_role_state)"
if [[ "$catalog_role_state" != "safe" ]]; then
  echo "refusing an unsafe shardschema reader role" >&2
  exit 1
fi

# The shardschema database and its fixed login are operator-owned. Only a login
# with no per-role defaults or the exact canonical defaults reaches here. After
# exact topology and catalog validation, remove database-wide defaults and
# re-establish the fixed login defaults. Noncanonical per-role defaults fail
# closed before this mutation. Otherwise settings such as
# zero_damaged_pages, ignore_checksum_failure, or session_preload_libraries
# can be applied by PostgreSQL at trusted database/role startup precedence and
# silently survive into every production pooler session.
psql -X --no-password --host="$socket" --username=postgres --dbname=postgres \
  --set=ON_ERROR_STOP=1 <<'PGSHARD_CATALOG_SESSION_DEFAULTS'
BEGIN;
ALTER DATABASE shardschema RESET ALL;
ALTER ROLE pgshard_pooler_catalog RESET ALL;
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema RESET ALL;
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema SET search_path = pg_catalog;
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema SET statement_timeout = '30s';
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema SET lock_timeout = '5s';
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema SET transaction_timeout = '120s';
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema SET default_transaction_read_only = off;
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema SET row_security = off;
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema SET synchronous_commit = on;
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema SET zero_damaged_pages = off;
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema SET ignore_checksum_failure = off;
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema SET jit = off;
COMMIT;
PGSHARD_CATALOG_SESSION_DEFAULTS

# Prove that the immutable Secret still matches PostgreSQL's SCRAM verifier
# without ALTER ROLE churn. The temporary server remains Unix-socket-only.
printf '%s\n' \
  'local all postgres trust' \
  'local shardschema pgshard_pooler_catalog scram-sha-256' \
  'local all all reject' \
  'host all all 0.0.0.0/0 reject' \
  'host all all ::0/0 reject' > "$quarantine_hba"
pg_ctl -D "$final" reload
catalog_authentication="$(
  # The catalog role is intentionally not a superuser, so it cannot inherit
  # the bootstrap superuser's PGOPTIONS (for example event_triggers=off). This
  # connection deliberately has no startup options: it proves the exact
  # database-and-role defaults that production pooler sessions will inherit.
  # The credential stays out of SQL and argv; the private temporary server and
  # already-validated event-trigger set remain the boundary.
  PGPASSWORD="$catalog_password" env -u PGOPTIONS \
    psql -X --no-password --host="$socket" --username=pgshard_pooler_catalog --dbname=shardschema \
    --set=ON_ERROR_STOP=1 --no-align --tuples-only --command="
      SELECT CASE WHEN current_user = 'pgshard_pooler_catalog'
                    AND current_setting('search_path') = 'pg_catalog'
                    AND current_setting('statement_timeout')::interval = interval '30 seconds'
                    AND current_setting('lock_timeout')::interval = interval '5 seconds'
                    AND current_setting('transaction_timeout')::interval = interval '120 seconds'
                    AND current_setting('idle_in_transaction_session_timeout')::interval = interval '30 seconds'
                    AND current_setting('default_transaction_read_only') = 'off'
                    AND current_setting('row_security') = 'off'
                    AND current_setting('synchronous_commit') = 'on'
                    AND current_setting('jit') = 'off'
                    AND pg_catalog.pg_has_role(
                          current_user,
                          'pgshard_catalog_reader',
                          'USAGE'
                        )
                    AND (SELECT pg_catalog.count(*) FROM pgshard_catalog.shards) >= 1
                  THEN 1 ELSE 0 END"
)"
unset catalog_password
if [[ "$catalog_authentication" != "1" ]]; then
  echo "refusing a shardschema reader credential that does not authenticate" >&2
  exit 1
fi

# Publish the serving HBA only after every restored-catalog, role, and
# credential check has succeeded. The catalog login has no Unix-socket or
# non-catalog-database escape hatch; its sole serving path is TLS to
# shardschema with SCRAM channel binding enforced by the pooler.
hba_staging="$final/.pgshard-pg_hba.conf.next"
rm -f -- "$hba_staging"
printf '%s\n' \
  'local all postgres trust' \
  'local all pgshard_pooler_catalog reject' \
  'local all all trust' \
  'hostnossl shardschema all all reject' \
  'hostssl shardschema pgshard_pooler_catalog all scram-sha-256' \
  'hostssl shardschema all all reject' \
  'host all pgshard_pooler_catalog all reject' \
  'host all all all scram-sha-256' > "$hba_staging"
chmod 0600 "$hba_staging"
sync "$hba_staging" "$final"
mv -- "$hba_staging" "$final/pg_hba.conf"
sync "$final/pg_hba.conf" "$final"

# The genesis intent is the crash-recovery authority until PostgreSQL has
# cleanly stopped with synchronous commit enforced. If shutdown fails or the
# container is killed, the durable intent remains and the next attempt accepts
# only the two states this bootstrap can actually produce.
pg_ctl -D "$final" -w -t 45 stop -m fast
if [[ "$catalog_genesis_pending" == "true" ]]; then
  rm -- "$catalog_genesis_intent"
  sync "$final"
fi
trap - EXIT
cleanup_bootstrap_runtime
sync "$final" "$parent" "$volume_root"
`

// Images contains the deployable images used by the supporting workloads.
// Image references are controller configuration, not part of the cluster API,
// so changing a controller release does not mutate the user's database spec.
type Images struct {
	Etcd                string
	Orchestrator        string
	Pooler              string
	PostgreSQL          string
	PostgreSQLBootstrap string
}

// DefaultImages are safe supporting-runtime defaults. The privileged
// PostgreSQL bootstrap image intentionally has no remote default: a deployment
// must select an immutable digest, or the exact never-pulled local development
// image used by the repository manifests.
func DefaultImages() Images {
	return Images{
		Etcd:         defaultEtcdImage,
		Orchestrator: "ghcr.io/andrew01234567890/pgshard-orch:main",
		Pooler:       "ghcr.io/andrew01234567890/pgshard-pooler:main",
		PostgreSQL:   defaultPostgreSQLImage,
	}
}

// DevelopmentImages selects the exact local bootstrap tag used by the
// repository manifests. Its Pod pull policy is Never, so Kubernetes cannot
// resolve that privileged image from a registry.
func DevelopmentImages() Images {
	images := DefaultImages()
	images.PostgreSQLBootstrap = developmentPostgreSQLBootstrapImage
	return images
}

func validatePostgreSQLBootstrapImage(image string) error {
	if image == developmentPostgreSQLBootstrapImage {
		return nil
	}
	named, err := distreference.ParseNormalizedNamed(image)
	if err != nil || strings.TrimSpace(image) != image {
		return fmt.Errorf("PostgreSQL bootstrap image must use an immutable sha256 digest (the exact local %q image is development-only)", developmentPostgreSQLBootstrapImage)
	}
	canonical, ok := named.(distreference.Canonical)
	if !ok || canonical.Digest().Algorithm().String() != "sha256" {
		return fmt.Errorf("PostgreSQL bootstrap image must use an immutable sha256 digest (the exact local %q image is development-only)", developmentPostgreSQLBootstrapImage)
	}
	return nil
}

// ValidateImagesForCluster checks image configuration before reconciliation
// creates durable bootstrap credentials or data volumes. Plan repeats this
// validation as defense in depth.
func ValidateImagesForCluster(cluster *pgshardv1alpha1.PgShardCluster, images Images) error {
	if cluster == nil {
		return fmt.Errorf("cluster is nil")
	}
	if strings.TrimSpace(images.Etcd) == "" || strings.TrimSpace(images.Orchestrator) == "" || strings.TrimSpace(images.Pooler) == "" || strings.TrimSpace(images.PostgreSQL) == "" {
		return fmt.Errorf("etcd, orchestrator, pooler, and PostgreSQL images must all be configured")
	}
	if cluster.Spec.MembersPerShard == 1 {
		if err := validatePostgreSQLBootstrapImage(images.PostgreSQLBootstrap); err != nil {
			return err
		}
	}
	return nil
}

// Plan returns the complete set of safe-to-create resources for cluster.
// Single-member asynchronous shards receive one PostgreSQL 18 primary. The
// multi-member path stays fail closed until physical replication, fencing,
// promotion, and recovery are implemented together.
func Plan(cluster *pgshardv1alpha1.PgShardCluster, images Images) ([]client.Object, error) {
	if cluster == nil {
		return nil, fmt.Errorf("cluster is nil")
	}
	if messages := validation.IsDNS1123Label(cluster.Name); len(messages) != 0 {
		return nil, fmt.Errorf("cluster name %q cannot be used for owned Services: %s", cluster.Name, messages[0])
	}
	if len(cluster.Name) > pgshardv1alpha1.MaximumClusterNameLength {
		return nil, fmt.Errorf("cluster name %q is too long: at most %d characters are supported", cluster.Name, pgshardv1alpha1.MaximumClusterNameLength)
	}
	if cluster.Namespace == "" {
		return nil, fmt.Errorf("cluster namespace is empty")
	}
	if cluster.UID == "" {
		return nil, fmt.Errorf("cluster UID is empty")
	}
	if cluster.Spec.Shards < 1 || cluster.Spec.Shards > pgshardv1alpha1.MaximumShards {
		return nil, fmt.Errorf("shards must be between 1 and %d", pgshardv1alpha1.MaximumShards)
	}
	if err := pgshardv1alpha1.ValidateClusterForReconciliation(cluster); err != nil {
		return nil, fmt.Errorf("cluster fails safety validation: %w", err)
	}
	if err := ValidateImagesForCluster(cluster, images); err != nil {
		return nil, err
	}
	if endpoint := cluster.Spec.Observability.OpenTelemetryEndpoint; endpoint != "" {
		if err := pgshardv1alpha1.ValidateOpenTelemetryEndpoint(endpoint); err != nil {
			return nil, fmt.Errorf("invalid OpenTelemetry endpoint: %w", err)
		}
	}
	if repository := cluster.Spec.Backup.Repository; repository.S3 != nil {
		if err := pgshardv1alpha1.ValidateObjectReferenceName(repository.S3.CredentialsSecretRef.Name); err != nil {
			return nil, fmt.Errorf("invalid S3 credential Secret reference: %w", err)
		}
		if repository.S3.Endpoint != "" {
			if err := pgshardv1alpha1.ValidateCredentialFreeHTTPSEndpoint(repository.S3.Endpoint); err != nil {
				return nil, fmt.Errorf("invalid S3 endpoint: %w", err)
			}
		}
	}
	if repository := cluster.Spec.Backup.Repository; repository.Filesystem != nil {
		if err := pgshardv1alpha1.ValidateObjectReferenceName(repository.Filesystem.PersistentVolumeClaimName); err != nil {
			return nil, fmt.Errorf("invalid backup PVC reference: %w", err)
		}
	}

	postgresql, err := cluster.ResolvedPostgreSQLConfiguration()
	if err != nil {
		return nil, fmt.Errorf("resolve PostgreSQL settings: %w", err)
	}
	postgresqlConfig := renderPostgreSQLConfiguration(postgresql)
	postgresqlHash := configMapDataHash(postgresqlConfig)
	postgresqlConfigName := PostgreSQLConfigMapName(cluster.Name, postgresqlHash)
	topologyConfig, err := renderTopology(cluster)
	if err != nil {
		return nil, err
	}
	topologyHash := configHash(topologyConfig)
	bootstraps, err := postgresqlBootstraps(cluster)
	if err != nil {
		return nil, err
	}
	var catalogAccess *pgshardv1alpha1.CatalogAccessStatus
	if cluster.Spec.MembersPerShard == 1 {
		catalogAccess = cluster.Status.CatalogAccess
		if catalogAccess == nil || catalogAccess.SecretUID == "" ||
			!CatalogAccessSecretNameIsValid(cluster.Name, catalogAccess.SecretName) ||
			!validCatalogMaterialSHA256(catalogAccess.ClientSHA256) ||
			!validCatalogMaterialSHA256(catalogAccess.ServerSHA256) {
			return nil, fmt.Errorf("catalog access creation result is missing or invalid")
		}
	}

	objects := make([]client.Object, 0, 16+3*cluster.Spec.Shards)
	objects = append(objects,
		immutableConfigMap(cluster, postgresqlConfigName, postgresqlConfig),
		configMap(cluster, cluster.Name+TopologyConfigSuffix, map[string]string{"cluster.json": topologyConfig}),
		applicationService(cluster, "rw", cluster.Spec.Services.ReadWrite, PoolerRWPort),
		applicationService(cluster, "ro", cluster.Spec.Services.ReadOnly, PoolerROPort),
		applicationService(cluster, "r", cluster.Spec.Services.Read, PoolerRPort),
		etcdService(cluster),
		orchestratorService(cluster),
		poolerService(cluster),
		etcdNetworkPolicy(cluster),
	)
	if cluster.Spec.MembersPerShard == 1 {
		objects = append(objects, catalogService(cluster))
	}
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		objects = append(objects, shardService(cluster, shard), postgresqlNetworkPolicy(cluster, shard))
		if cluster.Spec.MembersPerShard == 1 {
			bootstrap := bootstraps[shard]
			objects = append(objects,
				postgresqlShardStatefulSet(cluster, shard, images.PostgreSQL, images.PostgreSQLBootstrap, bootstrap.SecretName, bootstrap.PVCName, postgresqlConfigName, postgresqlHash, catalogAccess),
				postgresqlPrimaryDisruptionBudget(cluster, shard),
			)
		}
	}

	objects = append(objects,
		etcdStatefulSet(cluster, images.Etcd, images.Orchestrator),
		orchestratorDeployment(cluster, images.Orchestrator, topologyHash),
		poolerDeployment(cluster, images.Pooler, topologyHash, catalogAccess),
		podDisruptionBudget(cluster, "etcd", 1),
		podDisruptionBudget(cluster, "orchestrator", 1),
		podDisruptionBudget(cluster, "pooler", 1),
	)
	if cluster.Spec.Pooler.Scaling.Mode == pgshardv1alpha1.ScalingHPA {
		objects = append(objects, poolerHPA(cluster))
	}
	return objects, nil
}

func postgresqlBootstraps(cluster *pgshardv1alpha1.PgShardCluster) (map[int32]pgshardv1alpha1.PostgreSQLBootstrapStatus, error) {
	if cluster.Spec.MembersPerShard != 1 {
		return nil, nil
	}
	bootstraps := make(map[int32]pgshardv1alpha1.PostgreSQLBootstrapStatus, len(cluster.Status.PostgreSQLBootstraps))
	for _, bootstrap := range cluster.Status.PostgreSQLBootstraps {
		if bootstrap.Shard < 0 || bootstrap.Shard >= cluster.Spec.Shards {
			return nil, fmt.Errorf("PostgreSQL bootstrap references invalid shard %d", bootstrap.Shard)
		}
		if bootstrap.SecretName == "" || bootstrap.SecretUID == "" || !bootstrap.PVCFenceDetached || bootstrap.PVCName == "" || bootstrap.PVCUID == "" || bootstrap.PVCStorageClassName == nil {
			return nil, fmt.Errorf("PostgreSQL bootstrap for shard %d is incomplete (credential name=%t UID=%t, PVC fence detached=%t, PVC name=%t UID=%t, storage class=%t)", bootstrap.Shard, bootstrap.SecretName != "", bootstrap.SecretUID != "", bootstrap.PVCFenceDetached, bootstrap.PVCName != "", bootstrap.PVCUID != "", bootstrap.PVCStorageClassName != nil)
		}
		if _, duplicate := bootstraps[bootstrap.Shard]; duplicate {
			return nil, fmt.Errorf("PostgreSQL bootstrap for shard %d is duplicated", bootstrap.Shard)
		}
		bootstraps[bootstrap.Shard] = bootstrap
	}
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		if _, ok := bootstraps[shard]; !ok {
			return nil, fmt.Errorf("PostgreSQL bootstrap for shard %d is missing", shard)
		}
	}
	return bootstraps, nil
}

func renderPostgreSQLConfiguration(configuration pgshardv1alpha1.ResolvedPostgreSQLConfiguration) map[string]string {
	data := make(map[string]string, 1+len(configuration.Primaries)+len(configuration.Standbys))
	data["postgresql.conf"] = renderPostgreSQLConfig(configuration.Common)
	for _, primary := range configuration.Primaries {
		data[fmt.Sprintf("primary-%04d.conf", primary.Ordinal)] = renderPostgreSQLRoleConfig(primary.Settings)
	}
	for _, standby := range configuration.Standbys {
		data[fmt.Sprintf("standby-%04d.conf", standby.Ordinal)] = renderPostgreSQLRoleConfig(standby.Settings)
	}
	return data
}

func renderPostgreSQLRoleConfig(settings map[string]string) string {
	return "include = '/etc/pgshard/postgresql/postgresql.conf'\n" + renderPostgreSQLConfig(settings)
}

func renderPostgreSQLConfig(settings map[string]string) string {
	keys := make([]string, 0, len(settings))
	for key := range settings {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var output strings.Builder
	output.WriteString("# Generated by pgshard-operator. Manual edits are overwritten.\n")
	for _, key := range keys {
		fmt.Fprintf(&output, "%s = %s\n", key, settings[key])
	}
	return output.String()
}

type topologyDocument struct {
	Cluster         string                `json:"cluster"`
	Namespace       string                `json:"namespace"`
	Durability      string                `json:"durability"`
	MembersPerShard int32                 `json:"membersPerShard"`
	Listeners       []topologyListener    `json:"listeners"`
	Shards          []topologyShard       `json:"shards"`
	Databases       []string              `json:"databases,omitempty"`
	Backup          topologyBackup        `json:"backup"`
	Observability   topologyObservability `json:"observability"`
}

type topologyListener struct {
	Mode       string `json:"mode"`
	Service    string `json:"service"`
	TargetPort int32  `json:"targetPort"`
}

type topologyShard struct {
	ID      int32  `json:"id"`
	Service string `json:"service"`
}

type topologyBackup struct {
	Type                  string `json:"type"`
	Bucket                string `json:"bucket,omitempty"`
	Endpoint              string `json:"endpoint,omitempty"`
	Region                string `json:"region,omitempty"`
	Prefix                string `json:"prefix,omitempty"`
	CredentialsSecret     string `json:"credentialsSecret,omitempty"`
	PersistentVolumeClaim string `json:"persistentVolumeClaim,omitempty"`
}

type topologyObservability struct {
	Prometheus            bool   `json:"prometheus"`
	ServiceMonitor        bool   `json:"serviceMonitorRequested"`
	OpenTelemetryEndpoint string `json:"openTelemetryEndpoint,omitempty"`
}

func renderTopology(cluster *pgshardv1alpha1.PgShardCluster) (string, error) {
	document := topologyDocument{
		Cluster:         cluster.Name,
		Namespace:       cluster.Namespace,
		Durability:      string(cluster.Spec.Durability),
		MembersPerShard: cluster.Spec.MembersPerShard,
		Listeners: []topologyListener{
			{Mode: "rw", Service: cluster.Name + "-rw", TargetPort: PoolerRWPort},
			{Mode: "ro", Service: cluster.Name + "-ro", TargetPort: PoolerROPort},
			{Mode: "r", Service: cluster.Name + "-r", TargetPort: PoolerRPort},
		},
		Shards: make([]topologyShard, 0, cluster.Spec.Shards),
		Backup: topologyBackup{Type: string(cluster.Spec.Backup.Repository.Type)},
		Observability: topologyObservability{
			Prometheus:            cluster.Spec.Observability.Prometheus != nil && *cluster.Spec.Observability.Prometheus,
			ServiceMonitor:        cluster.Spec.Observability.ServiceMonitor,
			OpenTelemetryEndpoint: cluster.Spec.Observability.OpenTelemetryEndpoint,
		},
	}
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		document.Shards = append(document.Shards, topologyShard{ID: shard, Service: shardName(cluster.Name, shard)})
	}
	for _, database := range cluster.Spec.Databases {
		document.Databases = append(document.Databases, database.Name)
	}
	sort.Strings(document.Databases)
	if repository := cluster.Spec.Backup.Repository; repository.S3 != nil {
		document.Backup.Bucket = repository.S3.Bucket
		document.Backup.Endpoint = repository.S3.Endpoint
		document.Backup.Region = repository.S3.Region
		document.Backup.Prefix = repository.S3.Prefix
		document.Backup.CredentialsSecret = repository.S3.CredentialsSecretRef.Name
	}
	if repository := cluster.Spec.Backup.Repository; repository.Filesystem != nil {
		document.Backup.PersistentVolumeClaim = repository.Filesystem.PersistentVolumeClaimName
	}
	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return "", fmt.Errorf("render topology: %w", err)
	}
	return string(encoded) + "\n", nil
}

func configHash(configs ...string) string {
	hash := sha256.New()
	for _, config := range configs {
		hash.Write([]byte(config))
		hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func configMapDataHash(data map[string]string) string {
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	hash := sha256.New()
	for _, key := range keys {
		hash.Write([]byte(key))
		hash.Write([]byte{0})
		hash.Write([]byte(data[key]))
		hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// PostgreSQLConfigMapName returns the content-addressed name for one generated
// PostgreSQL configuration. A workload keeps using its old immutable object
// until the same apply publishes both its new resource limit and new reference.
func PostgreSQLConfigMapName(cluster, hash string) string {
	return cluster + PostgreSQLConfigSuffix + "-" + hash
}

func configMap(cluster *pgshardv1alpha1.PgShardCluster, name string, data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: ownedMeta(cluster, name, "configuration", nil),
		Data:       data,
	}
}

func immutableConfigMap(cluster *pgshardv1alpha1.PgShardCluster, name string, data map[string]string) *corev1.ConfigMap {
	configuration := configMap(cluster, name, data)
	configuration.Immutable = ptr(true)
	return configuration
}

func applicationService(cluster *pgshardv1alpha1.PgShardCluster, mode string, template pgshardv1alpha1.ServiceTemplate, targetPort int32) *corev1.Service {
	appProtocol := "postgresql"
	var selector map[string]string
	if mode == "rw" {
		selector = componentSelector(cluster, "pooler")
	}
	return &corev1.Service{
		ObjectMeta: ownedMeta(cluster, cluster.Name+"-"+mode, "pooler", template.Annotations),
		Spec: corev1.ServiceSpec{
			Type:     template.Type,
			Selector: selector,
			Ports: []corev1.ServicePort{{
				Name:        "postgresql",
				Protocol:    corev1.ProtocolTCP,
				AppProtocol: &appProtocol,
				Port:        PostgreSQLPort,
				TargetPort:  intstr.FromString("pooler-" + mode),
			}},
		},
	}
}

func shardService(cluster *pgshardv1alpha1.PgShardCluster, shard int32) *corev1.Service {
	selector := componentSelector(cluster, "postgresql")
	selector[ShardLabel] = shardLabel(shard)
	return &corev1.Service{
		ObjectMeta: ownedMeta(cluster, shardName(cluster.Name, shard), "postgresql", nil),
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 selector,
			Ports: []corev1.ServicePort{
				{Name: "postgresql", Protocol: corev1.ProtocolTCP, Port: PostgreSQLPort, TargetPort: intstr.FromString("postgresql")},
				{Name: "agent-http", Protocol: corev1.ProtocolTCP, Port: HTTPPort, TargetPort: intstr.FromString("agent-http")},
			},
		},
	}
}

// CatalogServiceName returns the ready-only service that exposes the
// authoritative shardschema database on shard zero.
func CatalogServiceName(cluster string) string { return cluster + CatalogServiceSuffix }

// CatalogAccessSecretPrefix returns a bounded, cluster-specific prefix for an
// unpredictable client-generated Secret name. The controller appends 128 bits
// of randomness, checkpoints a non-consumable creation intent, and then records
// the resulting API identity before planning workloads.
func CatalogAccessSecretPrefix(cluster string) string {
	const maximumPrefixLength = 31 // leaves 32 hexadecimal characters in a DNS label
	literal := cluster + "-catalog-"
	if len(literal) <= maximumPrefixLength {
		return literal
	}
	digest := sha256.Sum256([]byte(cluster))
	encoded := hex.EncodeToString(digest[:6])
	prefixLength := maximumPrefixLength - len("-cat-") - len(encoded) - 1
	return cluster[:prefixLength] + "-cat-" + encoded + "-"
}

// CatalogAccessSecretNameIsValid verifies the checkpointed 128-bit random
// suffix and bounded cluster-specific prefix.
func CatalogAccessSecretNameIsValid(cluster, name string) bool {
	prefix := CatalogAccessSecretPrefix(cluster)
	if !strings.HasPrefix(name, prefix) || len(name) != len(prefix)+32 {
		return false
	}
	suffix := name[len(prefix):]
	decoded, err := hex.DecodeString(suffix)
	return err == nil && len(decoded) == 16 && hex.EncodeToString(decoded) == suffix
}

// CatalogClientMaterialSHA256 binds the exact password and CA projection used
// by the pooler and shard-zero bootstrap init container.
func CatalogClientMaterialSHA256(password, caCertificate []byte) string {
	return catalogMaterialSHA256("pgshard-catalog-client-v1", password, caCertificate)
}

// CatalogServerMaterialSHA256 binds the exact PostgreSQL serving certificate
// and private-key projection validated before the postmaster starts.
func CatalogServerMaterialSHA256(serverCertificate, serverPrivateKey []byte) string {
	return catalogMaterialSHA256("pgshard-catalog-server-v1", serverPrivateKey, serverCertificate)
}

func catalogMaterialSHA256(domain string, key []byte, values ...[]byte) string {
	hash := hmac.New(sha256.New, key)
	for _, value := range append([][]byte{[]byte(domain)}, values...) {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write(value)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func validCatalogMaterialSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
}

// CatalogTLSDNSNames returns the complete exact hostname set accepted by the
// catalog server certificate.
func CatalogTLSDNSNames(cluster, namespace string) []string {
	service := CatalogServiceName(cluster)
	return []string{
		service,
		service + "." + namespace,
		service + "." + namespace + ".svc",
		service + "." + namespace + ".svc.cluster.local",
	}
}

func catalogService(cluster *pgshardv1alpha1.PgShardCluster) *corev1.Service {
	selector := componentSelector(cluster, "postgresql")
	selector[ShardLabel] = shardLabel(0)
	selector[RoleLabel] = "primary"
	return &corev1.Service{
		ObjectMeta: ownedMeta(cluster, CatalogServiceName(cluster.Name), "shardschema", nil),
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: selector,
			Ports: []corev1.ServicePort{{
				Name:       "postgresql",
				Protocol:   corev1.ProtocolTCP,
				Port:       PostgreSQLPort,
				TargetPort: intstr.FromString("postgresql"),
			}},
		},
	}
}

// PostgreSQLAuthSecretPrefix is the readable portion of a randomly named
// credential. The controller appends cryptographic randomness and records the
// resulting name and API UID before any workload can reference it.
func PostgreSQLAuthSecretPrefix(cluster string, shard int32) string {
	return shardName(cluster, shard) + "-auth-"
}

// PostgreSQLShardStatefulSetName returns the deterministic, role-neutral
// PostgreSQL workload name for one shard. The StatefulSet ordinal is the stable
// member identity; primary and replica roles belong in mutable labels.
func PostgreSQLShardStatefulSetName(cluster string, shard int32) string {
	return fmt.Sprintf("%s-shard-%04d", boundedPostgreSQLWorkloadPrefix(cluster), shard)
}

// LegacyPostgreSQLPrimaryStatefulSetName identifies the pre-role-neutral
// singleton workload during the one-way controller migration. It must not be
// used when planning new resources.
func LegacyPostgreSQLPrimaryStatefulSetName(cluster string, shard int32) string {
	return fmt.Sprintf("%s-shard-%04d-primary", boundedPostgreSQLWorkloadPrefix(cluster), shard)
}

// StatefulSet Pod names append an ordinal and must remain DNS labels. Preserve
// the legacy cluster-name API limit by bounding only the new PostgreSQL
// workload prefix, with a deterministic digest preventing truncation aliases.
func boundedPostgreSQLWorkloadPrefix(cluster string) string {
	const (
		maximumPrefixLength = 42
		digestBytes         = 12
	)
	// Hash names at the boundary too: otherwise a longer name's bounded output
	// could alias an accepted, literal 42-character cluster name.
	if len(cluster) < maximumPrefixLength {
		return cluster
	}
	digest := sha256.Sum256([]byte(cluster))
	suffix := hex.EncodeToString(digest[:digestBytes])
	return cluster[:maximumPrefixLength-len(suffix)-1] + "-" + suffix
}

// PostgreSQLDataPVCPrefix is the readable, role-neutral portion of a randomly
// named, pre-created data volume. Workloads only reference a name and UID
// checkpointed in PgShardCluster status.
func PostgreSQLDataPVCPrefix(cluster string, shard int32) string {
	return shardName(cluster, shard) + "-data-"
}

// PostgreSQLAuthSecret returns one immutable shard bootstrap Secret. It starts
// cluster-owned so a late create cannot outlive a failed bootstrap. The
// controller checkpoints its API UID and detaches it before using that exact
// Secret as the durable owner of outcome-unknown PVC creates. After the exact
// PVC UID is checkpointed, ownership is inverted: the live PVC is protected
// independently and the Secret becomes its dependent tombstone.
func PostgreSQLAuthSecret(cluster *pgshardv1alpha1.PgShardCluster, shard int32, name string, password []byte) *corev1.Secret {
	metadata := ownedMeta(cluster, name, "postgresql", nil)
	metadata.Labels[ShardLabel] = shardLabel(shard)
	metadata.Annotations[PostgreSQLBootstrapClusterUIDAnnotation] = string(cluster.UID)
	delete(metadata.Annotations, ApplyOwnershipAnnotation)
	return &corev1.Secret{
		ObjectMeta: metadata,
		Immutable:  ptr(true),
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{PostgreSQLPasswordKey: append([]byte(nil), password...)},
	}
}

// CatalogAccessIntentSecret is the stable, non-consumable identity created
// before any catalog credential or private key exists. Once its API UID is
// checkpointed, the controller updates this exact resource once with immutable
// material. A delayed Create can therefore commit only an empty Secret. Serving
// processes receive split projections; shard-zero bootstrap temporarily
// receives both projections to validate retained material before PGDATA access.
func CatalogAccessIntentSecret(cluster *pgshardv1alpha1.PgShardCluster, name string) *corev1.Secret {
	metadata := ownedMeta(cluster, name, "shardschema", nil)
	metadata.Annotations[CatalogAccessClusterUIDAnnotation] = string(cluster.UID)
	return &corev1.Secret{
		ObjectMeta: metadata,
		Type:       corev1.SecretTypeOpaque,
	}
}

// PostgreSQLDataPVC returns the standalone data volume for the stable member
// currently bootstrapped for a shard. Size and storage class come from the
// checkpointed provisioning contract. Every create is controlled by the exact
// detached credential Secret UID. The controller adds its data-protection
// finalizer only after the API UID is checkpointed, then detaches the live PVC
// and anchors the Secret tombstone to it. Delayed create requests retain this
// initial owner and no finalizer, so Kubernetes can garbage-collect them after
// the tombstone is deleted.
func PostgreSQLDataPVC(cluster *pgshardv1alpha1.PgShardCluster, shard int32, name string, storageSize resource.Quantity, storageClassName *string, fenceName string, fenceUID types.UID) *corev1.PersistentVolumeClaim {
	metadata := ownedMeta(cluster, name, "postgresql", nil)
	controller := true
	blockDeletion := true
	metadata.OwnerReferences = []metav1.OwnerReference{{
		APIVersion:         corev1.SchemeGroupVersion.String(),
		Kind:               "Secret",
		Name:               fenceName,
		UID:                fenceUID,
		Controller:         &controller,
		BlockOwnerDeletion: &blockDeletion,
	}}
	metadata.Annotations[PostgreSQLDataClusterUIDAnnotation] = string(cluster.UID)
	metadata.Labels[ShardLabel] = shardLabel(shard)
	metadata.Labels[RoleLabel] = "primary"
	metadata.Labels[MemberLabel] = "0000"
	delete(metadata.Annotations, ApplyOwnershipAnnotation)
	var selectedStorageClass *string
	if storageClassName != nil {
		selectedStorageClass = ptr(*storageClassName)
	}
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metadata,
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: selectedStorageClass,
			Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: storageSize.DeepCopy()}},
		},
	}
}

func postgresqlShardStatefulSet(cluster *pgshardv1alpha1.PgShardCluster, shard int32, image, bootstrapImage, secretName, pvcName, configurationName, configurationHash string, catalogAccess *pgshardv1alpha1.CatalogAccessStatus) *appsv1.StatefulSet {
	const (
		postgresUID = int64(999)
		replicas    = int32(1)
	)
	name := PostgreSQLShardStatefulSetName(cluster.Name, shard)
	selector := componentSelector(cluster, "postgresql")
	selector[ShardLabel] = shardLabel(shard)
	selector[MemberLabel] = "0000"
	podLabels := maps.Clone(selector)
	podLabels[RoleLabel] = "primary"
	podLabels[ManagedByLabel] = ManagedByValue
	allowPrivilegeEscalation := false
	readOnlyRootFilesystem := true
	runAsNonRoot := true
	seccomp := &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}
	fsGroupChangePolicy := corev1.FSGroupChangeOnRootMismatch
	postgresSecurity := &corev1.SecurityContext{
		AllowPrivilegeEscalation: &allowPrivilegeEscalation,
		ReadOnlyRootFilesystem:   &readOnlyRootFilesystem,
		RunAsNonRoot:             &runAsNonRoot,
		RunAsUser:                ptr(postgresUID),
		RunAsGroup:               ptr(postgresUID),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
	readinessProbeCommand := []string{"pg_isready", "--quiet", "--host=127.0.0.1", "--port=5432", "--username=postgres"}
	bootstrapPullPolicy := imagePullPolicy(bootstrapImage)
	if bootstrapImage == developmentPostgreSQLBootstrapImage {
		bootstrapPullPolicy = corev1.PullNever
	}
	postgres := corev1.Container{
		Name:            "postgresql",
		Image:           image,
		ImagePullPolicy: imagePullPolicy(image),
		Args:            []string{"-c", "config_file=/etc/pgshard/postgresql/primary-0000.conf", "-c", "allow_alter_system=off"},
		Env:             []corev1.EnvVar{{Name: "PGDATA", Value: "/var/lib/postgresql/18/docker"}},
		Ports:           []corev1.ContainerPort{{Name: "postgresql", ContainerPort: PostgreSQLPort, Protocol: corev1.ProtocolTCP}},
		Resources:       cluster.Spec.PostgreSQL.Resources,
		SecurityContext: postgresSecurity,
		ReadinessProbe: &corev1.Probe{
			ProbeHandler:     corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: readinessProbeCommand}},
			PeriodSeconds:    2,
			TimeoutSeconds:   2,
			FailureThreshold: 2,
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "data", MountPath: "/var/lib/postgresql"},
			{Name: "runtime", MountPath: "/var/run/postgresql"},
			{Name: "tmp", MountPath: "/tmp"},
			{Name: "postgresql-config", MountPath: "/etc/pgshard/postgresql", ReadOnly: true},
		},
	}
	if shard == 0 {
		postgres.Args = append(postgres.Args,
			"-c", "ssl=on",
			"-c", "ssl_cert_file=/etc/pgshard/catalog-tls/tls.crt",
			"-c", "ssl_key_file=/etc/pgshard/catalog-tls/tls.key",
			"-c", "ssl_min_protocol_version=TLSv1.3",
			"-c", "ssl_max_protocol_version=TLSv1.3",
		)
		postgres.VolumeMounts = append(postgres.VolumeMounts,
			corev1.VolumeMount{Name: "catalog-server-tls", MountPath: "/etc/pgshard/catalog-tls", ReadOnly: true},
		)
	}
	bootstrap := corev1.Container{
		Name:            "bootstrap-postgresql",
		Image:           bootstrapImage,
		ImagePullPolicy: bootstrapPullPolicy,
		Command:         []string{"bash", "-ceu", postgresqlBootstrapScript},
		Env: []corev1.EnvVar{
			{Name: "PGSHARD_CLUSTER_UID", Value: string(cluster.UID)},
			{Name: "PGSHARD_SHARD_ID", Value: shardLabel(shard)},
			{Name: "PGSHARD_POSTGRESQL_MAJOR", Value: pgshardv1alpha1.PostgreSQLMajor18},
			{Name: "PGSHARD_SHARD_COUNT", Value: fmt.Sprintf("%d", cluster.Spec.Shards)},
			{Name: "PGSHARD_MAXIMUM_SHARDS", Value: fmt.Sprintf("%d", pgshardv1alpha1.MaximumShards)},
			{Name: "PGSHARD_BOOTSTRAP_SHARDSCHEMA", Value: fmt.Sprintf("%t", shard == 0)},
			{Name: "PGSHARD_SHARDSCHEMA_MIGRATION", Value: shardschemaMigrationPath},
			{Name: "PGSHARD_SHARDSCHEMA_MIGRATION_SHA256", Value: shardschemaMigrationSHA256},
			{Name: "PGSHARD_NODE_UID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: fmt.Sprintf("metadata.annotations['%s']", PostgreSQLNodeUIDAnnotation)}}},
			{Name: "PGSHARD_NODE_BOOT_ID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: fmt.Sprintf("metadata.annotations['%s']", PostgreSQLNodeBootIDAnnotation)}}},
		},
		Resources:       cluster.Spec.PostgreSQL.Resources,
		SecurityContext: postgresSecurity.DeepCopy(),
		VolumeMounts: []corev1.VolumeMount{
			{Name: "data", MountPath: "/var/lib/postgresql"},
			{Name: "tmp", MountPath: "/tmp"},
			{Name: "postgresql-config", MountPath: "/etc/pgshard/postgresql", ReadOnly: true},
			{Name: "bootstrap-secret", MountPath: "/etc/pgshard/bootstrap", ReadOnly: true},
		},
	}
	if shard == 0 {
		if catalogAccess == nil {
			panic("validated single-member plan has no catalog access checkpoint")
		}
		bootstrap.VolumeMounts = append(bootstrap.VolumeMounts,
			corev1.VolumeMount{Name: "catalog-bootstrap-auth", MountPath: "/etc/pgshard/catalog-auth", ReadOnly: true},
			corev1.VolumeMount{Name: "catalog-server-tls", MountPath: "/etc/pgshard/catalog-tls", ReadOnly: true},
		)
		bootstrap.Env = append(bootstrap.Env,
			corev1.EnvVar{Name: "PGSHARD_CATALOG_CLIENT_SHA256", Value: catalogAccess.ClientSHA256},
			corev1.EnvVar{Name: "PGSHARD_CATALOG_SERVER_SHA256", Value: catalogAccess.ServerSHA256},
		)
	}
	automount := false
	enableServiceLinks := false
	podAnnotations := map[string]string{
		ConfigHashAnnotation:              configurationHash,
		PostgreSQLPodClusterUIDAnnotation: string(cluster.UID),
	}
	if shard == 0 {
		podAnnotations[shardschemaMigrationHashAnnotation] = shardschemaMigrationSHA256
	}
	volumes := []corev1.Volume{
		{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName}}},
		{Name: "runtime", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: ptr(resource.MustParse("64Mi"))}}},
		{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: ptr(resource.MustParse("64Mi"))}}},
		{Name: "postgresql-config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: configurationName}}}},
		{Name: "bootstrap-secret", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: secretName, DefaultMode: ptr(int32(0o440))}}},
	}
	if shard == 0 {
		catalogSecret := catalogAccess.SecretName
		volumes = append(volumes,
			corev1.Volume{
				Name: "catalog-server-tls",
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName:  catalogSecret,
					DefaultMode: ptr(int32(0o440)),
					Items: []corev1.KeyToPath{
						{Key: CatalogTLSCertificateKey, Path: "tls.crt"},
						{Key: CatalogTLSPrivateKeyKey, Path: "tls.key"},
					},
				}},
			},
			corev1.Volume{
				Name: "catalog-bootstrap-auth",
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName:  catalogSecret,
					DefaultMode: ptr(int32(0o440)),
					Items: []corev1.KeyToPath{
						{Key: CatalogPasswordKey, Path: "catalog-password"},
						{Key: CatalogCACertificateKey, Path: "ca.crt"},
					},
				}},
			},
		)
	}
	return &appsv1.StatefulSet{
		ObjectMeta: ownedMeta(cluster, name, "postgresql", nil),
		Spec: appsv1.StatefulSetSpec{
			Replicas:            ptr(replicas),
			ServiceName:         shardName(cluster.Name, shard),
			PodManagementPolicy: appsv1.OrderedReadyPodManagement,
			MinReadySeconds:     5,
			// Singleton primaries cannot be rolled concurrently without taking every
			// shard down. Keep template updates inert until the future staged upgrade
			// coordinator deletes one safely selected Pod at a time.
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType},
			Selector:       &metav1.LabelSelector{MatchLabels: selector},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      podLabels,
					Annotations: podAnnotations,
					Finalizers:  []string{PostgreSQLPodTerminationFinalizer},
				},
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken:  &automount,
					EnableServiceLinks:            &enableServiceLinks,
					TerminationGracePeriodSeconds: ptr(int64(60)),
					NodeSelector:                  map[string]string{corev1.LabelOSStable: "linux"},
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot:        &runAsNonRoot,
						RunAsUser:           ptr(postgresUID),
						RunAsGroup:          ptr(postgresUID),
						FSGroup:             ptr(postgresUID),
						FSGroupChangePolicy: &fsGroupChangePolicy,
						SeccompProfile:      seccomp,
					},
					InitContainers: []corev1.Container{bootstrap},
					Containers:     []corev1.Container{postgres},
					Volumes:        volumes,
				},
			},
		},
	}
}

func postgresqlPrimaryDisruptionBudget(cluster *pgshardv1alpha1.PgShardCluster, shard int32) *policyv1.PodDisruptionBudget {
	minimum := intstr.FromInt32(1)
	selector := componentSelector(cluster, "postgresql")
	selector[ShardLabel] = shardLabel(shard)
	selector[RoleLabel] = "primary"
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: ownedMeta(cluster, PostgreSQLShardStatefulSetName(cluster.Name, shard), "postgresql", nil),
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable:               &minimum,
			Selector:                   &metav1.LabelSelector{MatchLabels: selector},
			UnhealthyPodEvictionPolicy: unhealthyEvictionPolicyPtr(policyv1.AlwaysAllow),
		},
	}
}

func etcdService(cluster *pgshardv1alpha1.PgShardCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: ownedMeta(cluster, cluster.Name+EtcdSuffix, "etcd", nil),
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 componentSelector(cluster, "etcd"),
			Ports: []corev1.ServicePort{
				{Name: "client", Protocol: corev1.ProtocolTCP, Port: EtcdClientPort, TargetPort: intstr.FromString("client")},
				{Name: "peer", Protocol: corev1.ProtocolTCP, Port: EtcdPeerPort, TargetPort: intstr.FromString("peer")},
			},
		},
	}
}

func orchestratorService(cluster *pgshardv1alpha1.PgShardCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: ownedMeta(cluster, cluster.Name+OrchestratorSuffix, "orchestrator", nil),
		Spec: corev1.ServiceSpec{
			Selector: componentSelector(cluster, "orchestrator"),
			Ports:    []corev1.ServicePort{{Name: "http", Protocol: corev1.ProtocolTCP, Port: HTTPPort, TargetPort: intstr.FromString("http")}},
		},
	}
}

func poolerService(cluster *pgshardv1alpha1.PgShardCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: ownedMeta(cluster, cluster.Name+PoolerSuffix, "pooler", nil),
		Spec: corev1.ServiceSpec{
			Type:                     corev1.ServiceTypeClusterIP,
			PublishNotReadyAddresses: true,
			Selector:                 componentSelector(cluster, "pooler"),
			Ports:                    []corev1.ServicePort{{Name: "http", Protocol: corev1.ProtocolTCP, Port: HTTPPort, TargetPort: intstr.FromString("http")}},
		},
	}
}

func etcdNetworkPolicy(cluster *pgshardv1alpha1.PgShardCluster) *networkingv1.NetworkPolicy {
	tcp := corev1.ProtocolTCP
	clientPort := intstr.FromInt32(EtcdClientPort)
	peerPort := intstr.FromInt32(EtcdPeerPort)
	return &networkingv1.NetworkPolicy{
		ObjectMeta: ownedMeta(cluster, cluster.Name+EtcdSuffix, "etcd", nil),
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: componentSelector(cluster, "etcd")},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{
				{
					From: []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{ClusterLabel: cluster.Name},
						MatchExpressions: []metav1.LabelSelectorRequirement{{
							Key: ComponentLabel, Operator: metav1.LabelSelectorOpIn, Values: []string{"orchestrator", "pooler", "postgresql"},
						}},
					}}},
					Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: &clientPort}},
				},
				{
					From:  []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{MatchLabels: componentSelector(cluster, "etcd")}}},
					Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: &peerPort}},
				},
			},
		},
	}
}

func postgresqlNetworkPolicy(cluster *pgshardv1alpha1.PgShardCluster, shard int32) *networkingv1.NetworkPolicy {
	tcp := corev1.ProtocolTCP
	postgresqlPort := intstr.FromInt32(PostgreSQLPort)
	selector := componentSelector(cluster, "postgresql")
	selector[ShardLabel] = shardLabel(shard)
	postgresqlPeer := maps.Clone(selector)
	controlPeer := map[string]string{ClusterLabel: cluster.Name}
	return &networkingv1.NetworkPolicy{
		ObjectMeta: ownedMeta(cluster, shardName(cluster.Name, shard)+"-ingress", "postgresql", nil),
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: selector},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{
				{
					From: []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{
						MatchLabels: controlPeer,
						MatchExpressions: []metav1.LabelSelectorRequirement{{
							Key: ComponentLabel, Operator: metav1.LabelSelectorOpIn, Values: []string{"orchestrator", "pooler"},
						}},
					}}},
					Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: &postgresqlPort}},
				},
				{
					From:  []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{MatchLabels: postgresqlPeer}}},
					Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: &postgresqlPort}},
				},
			},
		},
	}
}

func etcdStatefulSet(cluster *pgshardv1alpha1.PgShardCluster, image, migrationImage string) *appsv1.StatefulSet {
	const replicas int32 = 3
	var storageClassName *string
	if cluster.Spec.Storage.StorageClassName != nil {
		storageClassName = ptr(*cluster.Spec.Storage.StorageClassName)
	}
	name := cluster.Name + EtcdSuffix
	selector := componentSelector(cluster, "etcd")
	claimMetadata := ownedMeta(cluster, "data", "etcd", nil)
	// The namespace is assigned when the StatefulSet creates each claim. A
	// direct CR controller reference lets our finalizer UID-safely wait for PVC
	// deletion instead of racing a same-name cluster replacement.
	claimMetadata.Namespace = ""
	initialCluster := make([]string, 0, replicas)
	for ordinal := int32(0); ordinal < replicas; ordinal++ {
		pod := fmt.Sprintf("%s-%d", name, ordinal)
		initialCluster = append(initialCluster, fmt.Sprintf("%s=http://%s.%s.%s.svc:%d", pod, pod, name, cluster.Namespace, EtcdPeerPort))
	}
	podSpec := securePodSpec(selector, []corev1.Container{{
		Name:            "etcd",
		Image:           image,
		ImagePullPolicy: imagePullPolicy(image),
		Command:         []string{etcdExecutable},
		Args: []string{
			"--name=$(POD_NAME)",
			"--data-dir=/var/lib/etcd/data",
			"--listen-client-urls=http://0.0.0.0:2379",
			"--advertise-client-urls=http://$(POD_NAME)." + name + "." + cluster.Namespace + ".svc:2379",
			"--listen-peer-urls=http://0.0.0.0:2380",
			"--initial-advertise-peer-urls=http://$(POD_NAME)." + name + "." + cluster.Namespace + ".svc:2380",
			"--initial-cluster=" + strings.Join(initialCluster, ","),
			"--initial-cluster-state=new",
			"--initial-cluster-token=" + cluster.Name,
			"--quota-backend-bytes=805306368",
			"--max-wals=2",
			"--max-snapshots=2",
			"--auto-compaction-mode=periodic",
			"--auto-compaction-retention=1h",
			"--enable-grpc-gateway=true",
		},
		Env: []corev1.EnvVar{{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}}},
		Ports: []corev1.ContainerPort{
			{Name: "client", ContainerPort: EtcdClientPort, Protocol: corev1.ProtocolTCP},
			{Name: "peer", ContainerPort: EtcdPeerPort, Protocol: corev1.ProtocolTCP},
		},
		Resources:      resources("100m", "128Mi", "1", "512Mi"),
		ReadinessProbe: httpReadinessProbe("/readyz", "client"),
		LivenessProbe:  httpLivenessProbe("/livez", "client"),
		VolumeMounts:   []corev1.VolumeMount{{Name: "data", MountPath: "/var/lib/etcd"}},
	}})
	podSpec.InitContainers = []corev1.Container{{
		Name:            "prepare-data",
		Image:           migrationImage,
		ImagePullPolicy: imagePullPolicy(migrationImage),
		Command:         []string{etcdDataMigrationExecutable},
		Resources:       resources("10m", "16Mi", "100m", "64Mi"),
		SecurityContext: secureContainerSecurityContext(),
		VolumeMounts:    []corev1.VolumeMount{{Name: "data", MountPath: "/var/lib/etcd"}},
	}}
	return &appsv1.StatefulSet{
		ObjectMeta: ownedMeta(cluster, name, "etcd", nil),
		Spec: appsv1.StatefulSetSpec{
			Replicas:            ptr(replicas),
			ServiceName:         name,
			PodManagementPolicy: appsv1.ParallelPodManagement,
			UpdateStrategy:      appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType},
			Selector:            &metav1.LabelSelector{MatchLabels: selector},
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: selector},
				Spec:       podSpec,
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: claimMetadata,
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					StorageClassName: storageClassName,
					Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("2Gi")}},
				},
			}},
		},
	}
}

func orchestratorDeployment(cluster *pgshardv1alpha1.PgShardCluster, image, hash string) *appsv1.Deployment {
	const replicas int32 = 3
	selector := componentSelector(cluster, "orchestrator")
	env := []corev1.EnvVar{
		{Name: "PGSHARD_CLUSTER_ID", Value: cluster.Name},
		{Name: "PGSHARD_CLUSTER_UID", Value: string(cluster.UID)},
		{Name: "PGSHARD_ORCH_ID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}},
		{Name: "PGSHARD_ETCD_ENDPOINTS", Value: etcdEndpoints(cluster)},
	}
	if cluster.Spec.Observability.OpenTelemetryEndpoint != "" {
		env = append(env, corev1.EnvVar{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: cluster.Spec.Observability.OpenTelemetryEndpoint})
	}
	podSpec := securePodSpec(selector, []corev1.Container{{
		Name:            "orchestrator",
		Image:           image,
		ImagePullPolicy: imagePullPolicy(image),
		Env:             env,
		Ports:           []corev1.ContainerPort{{Name: "http", ContainerPort: HTTPPort, Protocol: corev1.ProtocolTCP}},
		Resources:       resources("100m", "128Mi", "1", "512Mi"),
		ReadinessProbe:  httpReadinessProbe("/readyz", "http"),
		LivenessProbe:   httpLivenessProbe("/healthz", "http"),
		VolumeMounts:    []corev1.VolumeMount{{Name: "topology", MountPath: "/etc/pgshard", ReadOnly: true}},
	}})
	// The process clears readiness before a cancellation-aware, ten-second
	// shutdown drain. Keep the kubelet's hard deadline explicit and larger.
	podSpec.TerminationGracePeriodSeconds = ptr(int64(30))
	deployment := &appsv1.Deployment{
		ObjectMeta: ownedMeta(cluster, cluster.Name+OrchestratorSuffix, "orchestrator", nil),
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: selector},
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RollingUpdateDeploymentStrategyType, RollingUpdate: &appsv1.RollingUpdateDeployment{MaxUnavailable: intOrStringPtr(intstr.FromInt32(1)), MaxSurge: intOrStringPtr(intstr.FromInt32(1))}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: selector, Annotations: map[string]string{ConfigHashAnnotation: hash}},
				Spec:       podSpec,
			},
		},
	}
	deployment.Spec.Template.Spec.Volumes = []corev1.Volume{{
		Name: "topology",
		VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Name + TopologyConfigSuffix},
		}},
	}}
	return deployment
}

func poolerDeployment(cluster *pgshardv1alpha1.PgShardCluster, image, hash string, catalogAccess *pgshardv1alpha1.CatalogAccessStatus) *appsv1.Deployment {
	replicas := poolerReplicas(cluster)
	var desiredReplicas *int32
	if cluster.Spec.Pooler.Scaling.Mode == pgshardv1alpha1.ScalingFixed {
		desiredReplicas = ptr(replicas)
	}
	selector := componentSelector(cluster, "pooler")
	catalogMode := "bootstrap-unavailable"
	if cluster.Spec.MembersPerShard == 1 {
		catalogMode = "operator-tls"
	}
	env := []corev1.EnvVar{
		{Name: "PGSHARD_CLUSTER_ID", Value: cluster.Name},
		{Name: "PGSHARD_TOPOLOGY_FILE", Value: "/etc/pgshard/topology/cluster.json"},
		{Name: "PGSHARD_HTTP_BIND", Value: "0.0.0.0:8080"},
		{Name: "PGSHARD_RW_BIND", Value: "0.0.0.0:5432"},
		{Name: "PGSHARD_RO_BIND", Value: "0.0.0.0:5433"},
		{Name: "PGSHARD_R_BIND", Value: "0.0.0.0:5434"},
		{Name: "PGSHARD_CATALOG_MODE", Value: catalogMode},
		{Name: "PGSHARD_ETCD_ENDPOINTS", Value: etcdEndpoints(cluster)},
	}
	volumeMounts := []corev1.VolumeMount{{Name: "topology", MountPath: "/etc/pgshard/topology", ReadOnly: true}}
	volumes := []corev1.Volume{{Name: "topology", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Name + TopologyConfigSuffix}}}}}
	if cluster.Spec.MembersPerShard == 1 {
		if catalogAccess == nil {
			panic("validated single-member plan has no catalog access checkpoint")
		}
		env = append(env,
			corev1.EnvVar{Name: "PGSHARD_SHARDSCHEMA_HOST", Value: fmt.Sprintf("%s.%s.svc", CatalogServiceName(cluster.Name), cluster.Namespace)},
			corev1.EnvVar{Name: "PGSHARD_SHARDSCHEMA_PASSWORD_FILE", Value: "/etc/pgshard/catalog/catalog-password"},
			corev1.EnvVar{Name: "PGSHARD_SHARDSCHEMA_CA_FILE", Value: "/etc/pgshard/catalog/ca.crt"},
			corev1.EnvVar{Name: "PGSHARD_SHARDSCHEMA_CLIENT_SHA256", Value: catalogAccess.ClientSHA256},
			corev1.EnvVar{Name: "PGSHARD_RW_BACKEND_HOST", Value: fmt.Sprintf("%s.%s.svc", CatalogServiceName(cluster.Name), cluster.Namespace)},
		)
		volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: "catalog-client", MountPath: "/etc/pgshard/catalog", ReadOnly: true})
		volumes = append(volumes, corev1.Volume{
			Name: "catalog-client",
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName:  catalogAccess.SecretName,
				DefaultMode: ptr(int32(0o440)),
				Items: []corev1.KeyToPath{
					{Key: CatalogPasswordKey, Path: "catalog-password"},
					{Key: CatalogCACertificateKey, Path: "ca.crt"},
				},
			}},
		})
	}
	if cluster.Spec.Observability.OpenTelemetryEndpoint != "" {
		env = append(env, corev1.EnvVar{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: cluster.Spec.Observability.OpenTelemetryEndpoint})
	}
	podSpec := securePodSpec(selector, []corev1.Container{{
		Name:            "pooler",
		Image:           image,
		ImagePullPolicy: imagePullPolicy(image),
		Env:             env,
		Ports: []corev1.ContainerPort{
			{Name: "pooler-rw", ContainerPort: PoolerRWPort, Protocol: corev1.ProtocolTCP},
			{Name: "pooler-ro", ContainerPort: PoolerROPort, Protocol: corev1.ProtocolTCP},
			{Name: "pooler-r", ContainerPort: PoolerRPort, Protocol: corev1.ProtocolTCP},
			{Name: "http", ContainerPort: HTTPPort, Protocol: corev1.ProtocolTCP},
		},
		Resources:      resources("250m", "256Mi", "2", "1Gi"),
		ReadinessProbe: httpReadinessProbe("/readyz", "http"),
		LivenessProbe:  httpLivenessProbe("/healthz", "http"),
		Lifecycle:      &corev1.Lifecycle{PreStop: &corev1.LifecycleHandler{Sleep: &corev1.SleepAction{Seconds: 10}}},
		VolumeMounts:   volumeMounts,
	}})
	podSpec.TerminationGracePeriodSeconds = ptr(int64(60))
	podSpec.Volumes = volumes
	return &appsv1.Deployment{
		ObjectMeta: ownedMeta(cluster, cluster.Name+PoolerSuffix, "pooler", nil),
		Spec: appsv1.DeploymentSpec{
			Replicas: desiredReplicas,
			Selector: &metav1.LabelSelector{MatchLabels: selector},
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RollingUpdateDeploymentStrategyType, RollingUpdate: &appsv1.RollingUpdateDeployment{MaxUnavailable: intOrStringPtr(intstr.FromInt32(1)), MaxSurge: intOrStringPtr(intstr.FromInt32(1))}},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: selector, Annotations: map[string]string{ConfigHashAnnotation: hash}}, Spec: podSpec},
		},
	}
}

func poolerHPA(cluster *pgshardv1alpha1.PgShardCluster) *autoscalingv2.HorizontalPodAutoscaler {
	hpa := cluster.Spec.Pooler.Scaling.HPA
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: ownedMeta(cluster, cluster.Name+PoolerSuffix, "pooler", nil),
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: cluster.Name + PoolerSuffix},
			MinReplicas:    ptr(hpa.MinReplicas),
			MaxReplicas:    hpa.MaxReplicas,
			Metrics: []autoscalingv2.MetricSpec{{
				Type: autoscalingv2.ResourceMetricSourceType,
				Resource: &autoscalingv2.ResourceMetricSource{
					Name:   corev1.ResourceCPU,
					Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: ptr(hpa.TargetCPUUtilizationPercentage)},
				},
			}},
			Behavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{
				ScaleUp:   &autoscalingv2.HPAScalingRules{StabilizationWindowSeconds: ptr(int32(30)), SelectPolicy: scalingPolicyPtr(autoscalingv2.MaxChangePolicySelect), Policies: []autoscalingv2.HPAScalingPolicy{{Type: autoscalingv2.PercentScalingPolicy, Value: 100, PeriodSeconds: 60}}},
				ScaleDown: &autoscalingv2.HPAScalingRules{StabilizationWindowSeconds: ptr(int32(300)), SelectPolicy: scalingPolicyPtr(autoscalingv2.MaxChangePolicySelect), Policies: []autoscalingv2.HPAScalingPolicy{{Type: autoscalingv2.PercentScalingPolicy, Value: 25, PeriodSeconds: 60}}},
			},
		},
	}
}

func podDisruptionBudget(cluster *pgshardv1alpha1.PgShardCluster, component string, maxUnavailable int32) *policyv1.PodDisruptionBudget {
	value := intstr.FromInt32(maxUnavailable)
	budget := &policyv1.PodDisruptionBudget{
		ObjectMeta: ownedMeta(cluster, cluster.Name+"-"+component, component, nil),
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable:             &value,
			Selector:                   &metav1.LabelSelector{MatchLabels: componentSelector(cluster, component)},
			UnhealthyPodEvictionPolicy: unhealthyEvictionPolicyPtr(policyv1.AlwaysAllow),
		},
	}
	if component == "pooler" {
		budget.Spec.MaxUnavailable = nil
		budget.Spec.MinAvailable = &value
	}
	return budget
}

func securePodSpec(selector map[string]string, containers []corev1.Container) corev1.PodSpec {
	runAsNonRoot := true
	runAsUser := int64(10001)
	runAsGroup := int64(10001)
	fsGroup := int64(10001)
	fsGroupChangePolicy := corev1.FSGroupChangeOnRootMismatch
	seccomp := corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}
	for index := range containers {
		containers[index].SecurityContext = secureContainerSecurityContext()
	}
	automount := false
	enableServiceLinks := false
	return corev1.PodSpec{
		AutomountServiceAccountToken: &automount,
		EnableServiceLinks:           &enableServiceLinks,
		SecurityContext: &corev1.PodSecurityContext{
			RunAsNonRoot:        &runAsNonRoot,
			RunAsUser:           &runAsUser,
			RunAsGroup:          &runAsGroup,
			FSGroup:             &fsGroup,
			FSGroupChangePolicy: &fsGroupChangePolicy,
			SeccompProfile:      &seccomp,
		},
		Containers: containers,
		TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
			{MaxSkew: 1, TopologyKey: corev1.LabelHostname, WhenUnsatisfiable: corev1.ScheduleAnyway, LabelSelector: &metav1.LabelSelector{MatchLabels: selector}},
			{MaxSkew: 1, TopologyKey: corev1.LabelTopologyZone, WhenUnsatisfiable: corev1.ScheduleAnyway, LabelSelector: &metav1.LabelSelector{MatchLabels: selector}},
		},
	}
}

func secureContainerSecurityContext() *corev1.SecurityContext {
	runAsNonRoot := true
	runAsUser := int64(10001)
	runAsGroup := int64(10001)
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: ptr(false),
		ReadOnlyRootFilesystem:   ptr(true),
		RunAsNonRoot:             &runAsNonRoot,
		RunAsUser:                &runAsUser,
		RunAsGroup:               &runAsGroup,
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
}

func ownedMeta(cluster *pgshardv1alpha1.PgShardCluster, name, component string, annotations map[string]string) metav1.ObjectMeta {
	controller := true
	blockDeletion := true
	ownedAnnotations := cloneMap(annotations)
	if ownedAnnotations == nil {
		ownedAnnotations = make(map[string]string, 1)
	}
	ownedAnnotations[ApplyOwnershipAnnotation] = ApplyOwnershipVersion
	return metav1.ObjectMeta{
		Name:        name,
		Namespace:   cluster.Namespace,
		Labels:      labels(cluster, component),
		Annotations: ownedAnnotations,
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion:         pgshardv1alpha1.GroupVersion.String(),
			Kind:               "PgShardCluster",
			Name:               cluster.Name,
			UID:                cluster.UID,
			Controller:         &controller,
			BlockOwnerDeletion: &blockDeletion,
		}},
	}
}

func labels(cluster *pgshardv1alpha1.PgShardCluster, component string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name": "pgshard",
		ManagedByLabel:           ManagedByValue,
		InstanceLabel:            cluster.Name,
		ComponentLabel:           component,
		ClusterLabel:             cluster.Name,
	}
}

func componentSelector(cluster *pgshardv1alpha1.PgShardCluster, component string) map[string]string {
	return map[string]string{ClusterLabel: cluster.Name, ComponentLabel: component}
}

func shardName(cluster string, shard int32) string {
	return fmt.Sprintf("%s-shard-%04d", cluster, shard)
}

func shardLabel(shard int32) string { return fmt.Sprintf("%04d", shard) }

func etcdEndpoints(cluster *pgshardv1alpha1.PgShardCluster) string {
	name := cluster.Name + EtcdSuffix
	endpoints := make([]string, 0, 3)
	for ordinal := 0; ordinal < 3; ordinal++ {
		endpoints = append(endpoints, fmt.Sprintf("http://%s-%d.%s.%s.svc:%d", name, ordinal, name, cluster.Namespace, EtcdClientPort))
	}
	return strings.Join(endpoints, ",")
}

func poolerReplicas(cluster *pgshardv1alpha1.PgShardCluster) int32 {
	if cluster.Spec.Pooler.Scaling.Mode == pgshardv1alpha1.ScalingFixed {
		return cluster.Spec.Pooler.Scaling.Fixed.Replicas
	}
	return cluster.Spec.Pooler.Scaling.HPA.MinReplicas
}

func resources(requestCPU, requestMemory, limitCPU, limitMemory string) corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(requestCPU), corev1.ResourceMemory: resource.MustParse(requestMemory)},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(limitCPU), corev1.ResourceMemory: resource.MustParse(limitMemory)},
	}
}

func httpReadinessProbe(path, port string) *corev1.Probe {
	return httpProbe(path, port, 1)
}

func httpLivenessProbe(path, port string) *corev1.Probe {
	return httpProbe(path, port, 3)
}

func httpProbe(path, port string, failureThreshold int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstr.FromString(port), Scheme: corev1.URISchemeHTTP}},
		InitialDelaySeconds: 5,
		PeriodSeconds:       10,
		TimeoutSeconds:      3,
		FailureThreshold:    failureThreshold,
	}
}

func imagePullPolicy(image string) corev1.PullPolicy {
	if strings.Contains(image, "@") {
		return corev1.PullIfNotPresent
	}
	lastComponent := image[strings.LastIndex(image, "/")+1:]
	if !strings.Contains(lastComponent, ":") || strings.HasSuffix(lastComponent, ":latest") || strings.HasSuffix(lastComponent, ":main") {
		return corev1.PullAlways
	}
	return corev1.PullIfNotPresent
}

func cloneMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func ptr[T any](value T) *T { return &value }

func intOrStringPtr(value intstr.IntOrString) *intstr.IntOrString { return &value }

func scalingPolicyPtr(value autoscalingv2.ScalingPolicySelect) *autoscalingv2.ScalingPolicySelect {
	return &value
}

func unhealthyEvictionPolicyPtr(value policyv1.UnhealthyPodEvictionPolicyType) *policyv1.UnhealthyPodEvictionPolicyType {
	return &value
}

// Key identifies an object independently of its in-memory concrete pointer.
func Key(object client.Object) string {
	return fmt.Sprintf("%T/%s/%s", object, object.GetNamespace(), object.GetName())
}
