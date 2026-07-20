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
	"strconv"
	"strings"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	distreference "github.com/distribution/reference"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ManagedByLabel                           = "app.kubernetes.io/managed-by"
	InstanceLabel                            = "app.kubernetes.io/instance"
	ComponentLabel                           = "app.kubernetes.io/component"
	ClusterLabel                             = "pgshard.io/cluster"
	ShardLabel                               = "pgshard.io/shard"
	RoleLabel                                = "pgshard.io/role"
	MemberLabel                              = "pgshard.io/member"
	PostgreSQLRuntimeAnnotation              = "pgshard.io/postgresql-runtime"
	PostgreSQLGenerationDurabilityAnnotation = "pgshard.io/postgresql-generation-durability"
	PostgreSQLSynchronousStandbysAnnotation  = "pgshard.io/postgresql-synchronous-standbys"

	ManagedByValue = "pgshard-operator"
	// ClusterResourceFinalizer protects operator-owned resources and marks a
	// PgShardCluster lifecycle that crossed the fencing handshake barrier.
	ClusterResourceFinalizer = "pgshard.io/owned-resources"

	PostgreSQLConfigSuffix           = "-postgresql-config"
	PostgreSQLPasswordKey            = "superuser-password"
	PostgreSQLReplicationPasswordKey = "replication-password"
	CatalogServiceSuffix             = "-shardschema"
	CatalogPasswordKey               = "catalog-password"
	CatalogCACertificateKey          = "ca.crt"
	CatalogTLSCertificateKey         = "tls.crt"
	CatalogTLSPrivateKeyKey          = "tls.key"
	TopologyConfigSuffix             = "-topology"
	OrchestratorSuffix               = "-orchestrator"
	OrchestratorLeaseSuffix          = "-orch-lease"
	PoolerSuffix                     = "-pooler"

	PostgreSQLPort int32 = 5432
	PoolerRWPort   int32 = 5432
	PoolerROPort   int32 = 5433
	PoolerRPort    int32 = 5434
	HTTPPort       int32 = 8080

	defaultPostgreSQLImage              = "docker.io/library/postgres:18@sha256:32ca0af8e77bfb8c6610c488e4691f83f972a3e9e64d3b02facf3ab111ad5500"
	developmentPostgreSQLBootstrapImage = "pgshard/postgres-agent:dev"

	ConfigHashAnnotation                      = "pgshard.io/config-hash"
	ApplyOwnershipAnnotation                  = "pgshard.io/apply-ownership"
	ApplyOwnershipVersion                     = "v1"
	RetainedFromAnnotation                    = "pgshard.io/retained-from"
	PostgreSQLBootstrapClusterUIDAnnotation   = "pgshard.io/bootstrap-cluster-uid"
	PostgreSQLReplicationClusterUIDAnnotation = "pgshard.io/replication-cluster-uid"
	CatalogAccessClusterUIDAnnotation         = "pgshard.io/catalog-access-cluster-uid"
	PostgreSQLDataClusterUIDAnnotation        = "pgshard.io/data-cluster-uid"
	PostgreSQLDataProtectionFinalizer         = "pgshard.io/postgresql-data-protection"
	PostgreSQLPodClusterUIDAnnotation         = "pgshard.io/postgresql-cluster-uid"
	PostgreSQLNodeUIDAnnotation               = "pgshard.io/postgresql-node-uid"
	PostgreSQLNodeBootIDAnnotation            = "pgshard.io/postgresql-node-boot-id"
	PostgreSQLPodTerminationFinalizer         = "pgshard.io/postgresql-termination"
	postgresqlBootstrapMarker                 = ".pgshard-bootstrap-complete"
	shardschemaMigrationPath                  = "/usr/share/pgshard/migrations/0001_shardschema.sql"
	databaseGenesisKey                        = "database-genesis.sql"
	databaseGenesisPath                       = "/etc/pgshard/postgresql/database-genesis.sql"
	databaseTopologyPreflightKey              = "database-topology-preflight.sql"
	databaseTopologyPreflightPath             = "/etc/pgshard/postgresql/database-topology-preflight.sql"
	shardschemaMigrationSHA256                = "5c4d0fee9d069580ae90b6c71d78db5f160f6f01fa7fc5150f797693f88ff50a"
	shardschemaMigrationHashAnnotation        = "pgshard.io/shardschema-migration-sha256"
)

const postgresqlBootstrapScript = `set -Eeuo pipefail
: "${PGSHARD_NODE_UID:?binding-time node UID is required}"
: "${PGSHARD_NODE_BOOT_ID:?binding-time node boot ID is required}"
: "${PGSHARD_POSTGRESQL_MAJOR:?expected PostgreSQL major is required}"
: "${PGSHARD_POSTGRESQL_CONFIG_SHA256:?expected PostgreSQL configuration digest is required}"

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

if [[ ! "$PGSHARD_POSTGRESQL_CONFIG_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
  echo "refusing invalid PostgreSQL configuration digest" >&2
  exit 1
fi
bootstrap_hba_mode="${PGSHARD_BOOTSTRAP_HBA_MODE:-serving}"
case "$bootstrap_hba_mode" in
  serving|replication-bootstrap-primary) ;;
  *)
    echo "refusing invalid PostgreSQL bootstrap HBA mode" >&2
    exit 1
    ;;
esac
if [[ "$PGSHARD_BOOTSTRAP_SHARDSCHEMA" == "true" ]] && [[ "$bootstrap_hba_mode" != "serving" ]]; then
  echo "shardschema bootstrap requires the serving HBA mode" >&2
  exit 1
fi
if [[ "$bootstrap_hba_mode" == "replication-bootstrap-primary" ]]; then
  if [[ "$PGSHARD_MEMBERS_PER_SHARD" != "3" && "$PGSHARD_MEMBERS_PER_SHARD" != "5" ]]; then
    echo "replication bootstrap requires three or five members per shard" >&2
    exit 1
  fi
  if [[ ! "$PGSHARD_REPLICATION_MATERIAL_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
    echo "refusing an invalid checkpointed replication material digest" >&2
    exit 1
  fi
  replication_password="$(</etc/pgshard/replication/replication-password)"
  if [[ ! "$replication_password" =~ ^[0-9a-f]{64}$ ]]; then
    echo "refusing an invalid PostgreSQL replication credential" >&2
    exit 1
  fi
  unset replication_password
  observed_replication_sha="$(
    pgshard-catalog-material-digest replication \
      /etc/pgshard/replication/replication-password
  )"
  if [[ "$observed_replication_sha" != "$PGSHARD_REPLICATION_MATERIAL_SHA256" ]]; then
    echo "refusing replication material that differs from the checkpointed creation result" >&2
    exit 1
  fi
  unset observed_replication_sha
fi
configuration_source="${PGSHARD_POSTGRESQL_CONFIG_SOURCE:-/etc/pgshard/postgresql-source}"
configuration_target="${PGSHARD_POSTGRESQL_CONFIG_TARGET:-/etc/pgshard/postgresql}"
source_configuration_hash() {
  local path key
  {
    while IFS= read -r -d '' path; do
      if [[ ! -f "$path" ]]; then
        echo "PostgreSQL configuration source contains a non-file entry" >&2
        return 1
      fi
      key="${path##*/}"
      printf '%s\0' "$key"
      cat -- "$path"
      printf '\0'
    done < <(find "$configuration_source" -mindepth 1 -maxdepth 1 \( -type f -o -type l \) ! -name '..data' -print0 | LC_ALL=C sort -z)
  } | sha256sum | cut -d ' ' -f 1
}
target_configuration_hash() {
  local path key
  {
    while IFS= read -r -d '' path; do
      key="${path##*/}"
      printf '%s\0' "$key"
      cat -- "$path"
      printf '\0'
    done < <(find "$configuration_target" -mindepth 1 -maxdepth 1 -type f -print0 | LC_ALL=C sort -z)
  } | sha256sum | cut -d ' ' -f 1
}
observed_configuration_hash="$(source_configuration_hash)"
if [[ "$observed_configuration_hash" != "$PGSHARD_POSTGRESQL_CONFIG_SHA256" ]]; then
  echo "PostgreSQL configuration does not match the controller-owned Pod contract" >&2
  exit 1
fi
find "$configuration_target" -mindepth 1 -maxdepth 1 -delete
while IFS= read -r -d '' path; do
  install -m 0444 -- "$path" "$configuration_target/${path##*/}"
done < <(find "$configuration_source" -mindepth 1 -maxdepth 1 \( -type f -o -type l \) ! -name '..data' -print0 | LC_ALL=C sort -z)
if [[ "$(target_configuration_hash)" != "$PGSHARD_POSTGRESQL_CONFIG_SHA256" ]]; then
  echo "copied PostgreSQL configuration does not match the controller-owned Pod contract" >&2
  exit 1
fi

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
if [[ "$PGSHARD_BOOTSTRAP_SHARDSCHEMA" != "true" ]] \
  && [[ "$bootstrap_hba_mode" != "replication-bootstrap-primary" ]]; then
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

if [[ "$bootstrap_hba_mode" == "replication-bootstrap-primary" ]]; then
  socket=/tmp/pgshard-replication-bootstrap
  rm -rf -- "$socket"
  mkdir -m 0700 -- "$socket"
  quarantine_hba="$(mktemp /tmp/pgshard-replication-bootstrap-hba.XXXXXX)"
  chmod 0600 "$quarantine_hba"
  printf '%s\n' \
    'local all postgres trust' \
    'local replication pgshard_replication scram-sha-256' \
    'local replication all reject' \
    'local all all reject' \
    'host all all 0.0.0.0/0 reject' \
    'host all all ::0/0 reject' > "$quarantine_hba"
  export PGOPTIONS='-c lock_timeout=5s -c statement_timeout=30s -c transaction_timeout=120s -c idle_in_transaction_session_timeout=30s -c search_path=pg_catalog -c quote_all_identifiers=off -c event_triggers=off -c session_replication_role=origin -c session_preload_libraries= -c local_preload_libraries= -c jit=off -c default_tablespace= -c temp_tablespaces= -c default_table_access_method=heap -c default_transaction_read_only=off -c row_security=off -c synchronous_commit=on -c zero_damaged_pages=off -c ignore_checksum_failure=off -c password_encryption=scram-sha-256 -c scram_iterations=4096 -c log_statement=none -c log_min_error_statement=panic -c log_min_duration_statement=-1 -c log_min_duration_sample=-1 -c log_statement_sample_rate=0 -c log_transaction_sample_rate=0 -c log_duration=off -c log_parameter_max_length=0 -c log_parameter_max_length_on_error=0 -c log_min_messages=warning -c debug_print_parse=off -c debug_print_rewritten=off -c debug_print_plan=off -c log_parser_stats=off -c log_planner_stats=off -c log_executor_stats=off -c log_statement_stats=off'
  cleanup_replication_bootstrap_runtime() {
    rm -f -- "$quarantine_hba"
    rm -rf -- "$socket"
  }
  stop_replication_bootstrap_postgres() {
    result=$?
    trap - EXIT
    if pg_ctl -D "$final" status >/dev/null 2>&1; then
      if ! pg_ctl -D "$final" -w -t 45 stop -m fast; then
        result=1
      fi
    fi
    cleanup_replication_bootstrap_runtime
    exit "$result"
  }
  trap stop_replication_bootstrap_postgres EXIT

  # Start only on the private Unix socket with every inherited extension,
  # recovery, network, logging, and unsafe-durability path pinned closed.
  pg_ctl -D "$final" -w -t 45 start \
    -o "-c config_file=/etc/pgshard/postgresql/primary-0000.conf -c data_directory='$final' -c hba_file='$quarantine_hba' -c external_pid_file=/tmp/pgshard-replication-bootstrap.pid -c listen_addresses='' -c unix_socket_directories='$socket' -c unix_socket_permissions=0700 -c unix_socket_group= -c port=5432 -c ssl=off -c restart_after_crash=off -c primary_conninfo= -c primary_slot_name= -c restore_command= -c archive_cleanup_command= -c recovery_end_command= -c archive_mode=on -c archive_command= -c archive_library= -c max_wal_senders=1 -c max_logical_replication_workers=0 -c sync_replication_slots=off -c wal_receiver_create_temp_slot=off -c idle_replication_slot_timeout=0 -c max_slot_wal_keep_size=-1 -c synchronous_standby_names='' -c synchronized_standby_slots='' -c shared_preload_libraries= -c session_preload_libraries= -c local_preload_libraries= -c event_triggers=off -c jit=off -c fsync=on -c full_page_writes=on -c synchronous_commit=on -c ignore_invalid_pages=off -c data_sync_retry=off -c ignore_checksum_failure=off -c zero_damaged_pages=off -c password_encryption=scram-sha-256 -c scram_iterations=4096 -c logging_collector=off -c log_statement=none -c log_min_error_statement=panic -c log_min_duration_statement=-1 -c log_min_duration_sample=-1 -c log_statement_sample_rate=0 -c log_transaction_sample_rate=0 -c log_duration=off -c log_parameter_max_length=0 -c log_parameter_max_length_on_error=0"

  replication_session_policy="$(
    psql -X --no-password --host="$socket" --username=postgres --dbname=postgres \
      --set=ON_ERROR_STOP=1 --no-align --tuples-only --command="
        SELECT CASE WHEN
          current_setting('search_path') = 'pg_catalog'
          AND current_setting('event_triggers') = 'off'
          AND current_setting('session_replication_role') = 'origin'
          AND current_setting('default_transaction_read_only') = 'off'
          AND current_setting('row_security') = 'off'
          AND current_setting('synchronous_commit') = 'on'
          AND current_setting('synchronous_standby_names') = ''
          AND current_setting('synchronized_standby_slots') = ''
          AND current_setting('zero_damaged_pages') = 'off'
          AND current_setting('ignore_checksum_failure') = 'off'
          AND current_setting('password_encryption') = 'scram-sha-256'
          AND current_setting('scram_iterations') = '4096'
          AND current_setting('log_statement') = 'none'
          AND current_setting('log_min_error_statement') = 'panic'
          AND current_setting('log_parameter_max_length') = '0'
          AND current_setting('log_parameter_max_length_on_error') = '0'
        THEN 1 ELSE 0 END"
  )"
  if [[ "$replication_session_policy" != "1" ]]; then
    echo "refusing to materialize replication state without the enforced bootstrap session policy" >&2
    exit 1
  fi

  read_replication_role_state() {
    psql -X --no-password --host="$socket" --username=postgres --dbname=postgres \
      --set=ON_ERROR_STOP=1 --no-align --tuples-only --command="
        SELECT COALESCE((
          SELECT CASE WHEN
            NOT roles.rolsuper
            AND NOT roles.rolinherit
            AND NOT roles.rolcreaterole
            AND NOT roles.rolcreatedb
            AND NOT roles.rolbypassrls
            AND roles.rolconnlimit = -1
            AND roles.rolvaliduntil IS NULL
            AND NOT EXISTS (
              SELECT FROM pg_catalog.pg_auth_members AS memberships
               WHERE memberships.member = roles.oid OR memberships.roleid = roles.oid
            )
            AND NOT EXISTS (
              SELECT FROM pg_catalog.pg_database AS databases
               WHERE databases.datdba = roles.oid
            )
            AND NOT EXISTS (
              SELECT FROM pg_catalog.pg_tablespace AS tablespaces
               WHERE tablespaces.spcowner = roles.oid
            )
            AND NOT EXISTS (
              SELECT FROM pg_catalog.pg_db_role_setting AS settings
               WHERE settings.setrole = roles.oid
            )
            AND NOT EXISTS (
              SELECT FROM pg_catalog.pg_shdepend AS dependencies
               WHERE dependencies.refclassid = 'pg_catalog.pg_authid'::pg_catalog.regclass
                 AND dependencies.refobjid = roles.oid
            )
          THEN CASE
            WHEN roles.rolcanlogin
              AND roles.rolreplication
              AND roles.rolpassword LIKE 'SCRAM-SHA-256\$4096:%'
              THEN 'safe'
            WHEN NOT roles.rolcanlogin
              AND NOT roles.rolreplication
              AND roles.rolpassword IS NULL
              THEN 'staging'
            ELSE 'unsafe'
          END
          ELSE 'unsafe'
          END
            FROM pg_catalog.pg_authid AS roles
           WHERE roles.rolname = 'pgshard_replication'
        ), 'absent')"
  }

  declare -a expected_replication_slots=()
  declare -A expected_replication_slot_set=()
  for (( member = 1; member < PGSHARD_MEMBERS_PER_SHARD; member++ )); do
    printf -v slot_name 'pgshard_member_%04d' "$member"
    expected_replication_slots+=("$slot_name")
    expected_replication_slot_set["$slot_name"]=1
  done

  # Preflight the complete reserved namespace before any role or slot write.
  # A same-name object with an unexpected shape is never adopted or deleted.
  if ! replication_slot_preflight="$(
    psql -X --no-password --host="$socket" --username=postgres --dbname=postgres \
      --set=ON_ERROR_STOP=1 --no-align --tuples-only --field-separator='|' --command="
        SELECT slot_name,
               CASE WHEN slot_type = 'physical'
                          AND database IS NULL
                          AND plugin IS NULL
                          AND NOT temporary
                          AND NOT active
                          AND active_pid IS NULL
                          AND restart_lsn IS NOT NULL
                          AND wal_status IN ('reserved', 'extended')
                          AND invalidation_reason IS NULL
                          AND NOT two_phase
                          AND NOT failover
                          AND NOT synced
                    THEN 'safe' ELSE 'unsafe' END
          FROM pg_catalog.pg_replication_slots
         WHERE pg_catalog.left(slot_name, pg_catalog.length('pgshard_member_')) = 'pgshard_member_'
         ORDER BY slot_name"
  )"; then
    echo "refusing replication state whose managed slot namespace cannot be inspected" >&2
    exit 1
  fi
  if [[ -n "$replication_slot_preflight" ]]; then
    while IFS='|' read -r slot_name slot_state extra; do
      if [[ -n "$extra" || -z "${expected_replication_slot_set[$slot_name]:-}" || "$slot_state" != "safe" ]]; then
        echo "refusing an unsafe or foreign managed physical replication slot" >&2
        exit 1
      fi
    done <<< "$replication_slot_preflight"
  fi
  unset replication_slot_preflight

  replication_role_state="$(read_replication_role_state)"
  case "$replication_role_state" in
    absent)
      psql -X --no-password --host="$socket" --username=postgres --dbname=postgres \
        --set=ON_ERROR_STOP=1 <<'PGSHARD_REPLICATION_LOGIN_STAGING'
BEGIN;
CREATE ROLE pgshard_replication
  NOLOGIN NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE NOREPLICATION
  NOBYPASSRLS CONNECTION LIMIT -1;
COMMIT;
PGSHARD_REPLICATION_LOGIN_STAGING
      replication_role_state=staging
      ;;
    staging|safe) ;;
    *)
      echo "refusing an unsafe PostgreSQL replication role" >&2
      exit 1
      ;;
  esac

  if [[ "$replication_role_state" == "staging" ]]; then
    replication_scram_verifier="$(
      pgshard-scram-verifier < /etc/pgshard/replication/replication-password
    )"
    case "$replication_scram_verifier" in
      'SCRAM-SHA-256$4096:'*) ;;
      *)
        echo "refusing an invalid client-generated replication SCRAM verifier" >&2
        exit 1
        ;;
    esac
    replication_login_update="$(
      {
        printf '%s\n' \
          'UPDATE pg_catalog.pg_authid SET rolpassword = $1, rolcanlogin = true, rolreplication = true WHERE rolname = '\''pgshard_replication'\'' AND NOT rolcanlogin AND rolpassword IS NULL AND NOT rolsuper AND NOT rolinherit AND NOT rolcreaterole AND NOT rolcreatedb AND NOT rolreplication AND NOT rolbypassrls AND rolconnlimit = -1 AND rolvaliduntil IS NULL RETURNING 1'
        printf '%s %s\n' '\bind' "'$replication_scram_verifier'"
        printf '%s\n' '\g'
      } | psql -X --no-password --host="$socket" --username=postgres --dbname=postgres \
        --set=ON_ERROR_STOP=1 --quiet --no-align --tuples-only
    )"
    if [[ "$replication_login_update" != "1" ]]; then
      echo "refusing a replication role that changed during credential installation" >&2
      exit 1
    fi
    replication_verifier_matches="$(
      {
        printf '%s\n' \
          'SELECT 1 FROM pg_catalog.pg_authid WHERE rolname = '\''pgshard_replication'\'' AND rolpassword = $1'
        printf '%s %s\n' '\bind' "'$replication_scram_verifier'"
        printf '%s\n' '\g'
      } | psql -X --no-password --host="$socket" --username=postgres --dbname=postgres \
        --set=ON_ERROR_STOP=1 --quiet --no-align --tuples-only
    )"
    unset replication_scram_verifier
    if [[ "$replication_verifier_matches" != "1" ]]; then
      echo "refusing a replication role that changed during credential installation" >&2
      exit 1
    fi
  fi

  if [[ "$(read_replication_role_state)" != "safe" ]]; then
    echo "refusing an unsafe PostgreSQL replication role" >&2
    exit 1
  fi

  # Prove the exact password before creating any slot. The plaintext remains
  # in bootstrap-shell memory and the child libpq environment; it never enters
  # SQL, argv, or PostgreSQL logs. Physical replication mode is required for
  # the special replication HBA database token.
  replication_password="$(</etc/pgshard/replication/replication-password)"
  if ! PGPASSWORD="$replication_password" env -u PGOPTIONS \
    timeout --signal=TERM --kill-after=2s 10s psql -X --no-password \
      --dbname="host=$socket port=5432 user=pgshard_replication replication=true connect_timeout=5" \
      --set=ON_ERROR_STOP=1 --quiet --command='IDENTIFY_SYSTEM' >/dev/null; then
    echo "refusing a PostgreSQL replication credential that does not authenticate" >&2
    exit 1
  fi
  unset replication_password

  for slot_name in "${expected_replication_slots[@]}"; do
    existing_slot="$(
      {
        printf '%s\n' 'SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1'
        printf '%s %s\n' '\bind' "'$slot_name'"
        printf '%s\n' '\g'
      } | psql -X --no-password --host="$socket" --username=postgres --dbname=postgres \
        --set=ON_ERROR_STOP=1 --quiet --no-align --tuples-only
    )"
    case "$existing_slot" in
      1) ;;
      "")
        created_slot="$(
          {
            printf '%s\n' 'SELECT slot_name FROM pg_catalog.pg_create_physical_replication_slot($1, true, false)'
            printf '%s %s\n' '\bind' "'$slot_name'"
            printf '%s\n' '\g'
          } | psql -X --no-password --host="$socket" --username=postgres --dbname=postgres \
            --set=ON_ERROR_STOP=1 --quiet --no-align --tuples-only
        )"
        if [[ "$created_slot" != "$slot_name" ]]; then
          echo "physical replication slot creation returned an unexpected identity" >&2
          exit 1
        fi
        ;;
      *)
        echo "physical replication slot lookup returned an unexpected result" >&2
        exit 1
        ;;
    esac
  done

  safe_replication_slot_count="$(
    psql -X --no-password --host="$socket" --username=postgres --dbname=postgres \
      --set=ON_ERROR_STOP=1 --no-align --tuples-only --command="
        SELECT count(*)
          FROM pg_catalog.pg_replication_slots
         WHERE pg_catalog.left(slot_name, pg_catalog.length('pgshard_member_')) = 'pgshard_member_'
           AND slot_type = 'physical'
           AND database IS NULL
           AND plugin IS NULL
           AND NOT temporary
           AND NOT active
           AND active_pid IS NULL
           AND restart_lsn IS NOT NULL
           AND wal_status IN ('reserved', 'extended')
           AND invalidation_reason IS NULL
           AND NOT two_phase
           AND NOT failover
           AND NOT synced"
  )"
  if [[ "$safe_replication_slot_count" != "$((PGSHARD_MEMBERS_PER_SHARD - 1))" ]]; then
    echo "refusing an incomplete or unsafe managed physical replication slot set" >&2
    exit 1
  fi

  pg_ctl -D "$final" -w -t 45 stop -m fast
  trap - EXIT
  cleanup_replication_bootstrap_runtime

  hba_staging="$final/.pgshard-pg_hba.conf.next"
  rm -f -- "$hba_staging"
  install -m 0600 -- /etc/pgshard/replication-bootstrap-primary.pg_hba.conf "$hba_staging"
  sync "$hba_staging" "$final"
  mv -- "$hba_staging" "$final/pg_hba.conf"
  sync "$final/pg_hba.conf" "$final" "$parent" "$volume_root"
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
            OR parents.relnamespace = (SELECT oid FROM catalog_namespace)
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
      "ceec4ff5d633d28afacf1e93fbc2547591017e57f172dc3a8072814bb6d3867a"|\
      "3189b8a08cf2dedb5542cdf1dd58dec2f173f848ae67612aa4263751c404ea7a"|\
      "8bb87bb746ed463bf744b7b809477e9d36ad95d7cca06e25980085bba1ae4659"|\
      "2a20ec8e1bec9f660d6656484ebbebab0b694788e5f0bda657eb33816bf884a6"|\
      "06d2271274d6dfdeda51aba8293056b6fb23f451f1806f3dcd41763c595ee1de") ;;
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

database_genesis=/etc/pgshard/postgresql/database-genesis.sql
database_topology_preflight=/etc/pgshard/postgresql/database-topology-preflight.sql
if [[ ! -f "$database_genesis" || -L "$database_genesis" ]]; then
  echo "database genesis topology is missing or not a regular file" >&2
  exit 1
fi
if [[ ! -f "$database_topology_preflight" || -L "$database_topology_preflight" ]]; then
  echo "database topology preflight is missing or not a regular file" >&2
  exit 1
fi
if [[ "$catalog_core_tables" == "t|t|t" ]]; then
  psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
    --set=ON_ERROR_STOP=1 \
    --set=PGSHARD_ALLOW_EMPTY_DATABASE_TOPOLOGY="$catalog_genesis_pending" \
    --file="$database_topology_preflight" >/dev/null
fi

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

psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
  --set=ON_ERROR_STOP=1 \
  --set=PGSHARD_ALLOW_EMPTY_DATABASE_TOPOLOGY="$catalog_requires_initial_inventory" \
  --file="$database_genesis" >/dev/null

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

const postgresqlStandbyBootstrapScript = `set -Eeuo pipefail
: "${PGSHARD_CLUSTER_UID:?cluster UID is required}"
: "${PGSHARD_SHARD_ID:?shard identity is required}"
: "${PGSHARD_MEMBER_ID:?member identity is required}"
: "${PGSHARD_SOURCE_HOST:?source Pod DNS is required}"
: "${PGSHARD_PRIMARY_SLOT_NAME:?physical replication slot is required}"
: "${PGSHARD_REPLICATION_MATERIAL_SHA256:?replication material digest is required}"
: "${PGSHARD_TARGET_PVC_UID:?target PVC UID is required}"
: "${PGSHARD_TARGET_SECRET_UID:?target creation-fence Secret UID is required}"
: "${PGSHARD_SOURCE_PVC_UID:?source PVC UID is required}"
: "${PGSHARD_REPLICATION_SECRET_UID:?replication Secret UID is required}"
: "${PGSHARD_NODE_UID:?binding-time node UID is required}"
: "${PGSHARD_NODE_BOOT_ID:?binding-time node boot ID is required}"

if [[ ! "$PGSHARD_SHARD_ID" =~ ^[0-9]{4}$ ]] \
  || [[ ! "$PGSHARD_MEMBER_ID" =~ ^[0-9]{4}$ ]] \
  || [[ "$PGSHARD_MEMBER_ID" == "0000" ]]; then
  echo "refusing a non-canonical physical standby identity" >&2
  exit 1
fi
if [[ "$PGSHARD_PRIMARY_SLOT_NAME" != "pgshard_member_$PGSHARD_MEMBER_ID" ]]; then
  echo "refusing a physical slot that differs from the member identity" >&2
  exit 1
fi
if [[ ! "$PGSHARD_SOURCE_HOST" =~ ^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*$ ]] \
  || (( ${#PGSHARD_SOURCE_HOST} > 253 )); then
  echo "refusing invalid source Pod DNS" >&2
  exit 1
fi
if [[ ! "$PGSHARD_REPLICATION_MATERIAL_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
  echo "refusing an invalid checkpointed replication material digest" >&2
  exit 1
fi
for checkpoint_uid in \
  "$PGSHARD_TARGET_PVC_UID" \
  "$PGSHARD_TARGET_SECRET_UID" \
  "$PGSHARD_SOURCE_PVC_UID" \
  "$PGSHARD_REPLICATION_SECRET_UID"; do
  if [[ ! "$checkpoint_uid" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]; then
    echo "refusing an invalid checkpointed Kubernetes UID" >&2
    exit 1
  fi
done

replication_password_path=/etc/pgshard/replication/replication-password
observed_replication_sha="$(
  pgshard-catalog-material-digest replication "$replication_password_path"
)"
if [[ "$observed_replication_sha" != "$PGSHARD_REPLICATION_MATERIAL_SHA256" ]]; then
  echo "refusing replication material that differs from the checkpointed creation result" >&2
  exit 1
fi
unset observed_replication_sha

replication_password="$(<"$replication_password_path")"
if [[ "$(wc -c < "$replication_password_path")" != "64" ]] \
  || [[ ! "$replication_password" =~ ^[0-9a-f]{64}$ ]]; then
  echo "refusing an invalid PostgreSQL replication credential" >&2
  exit 1
fi
passfile_directory=/run/pgshard/standby-auth
passfile="$passfile_directory/passfile"
passfile_staging="$passfile_directory/.passfile.next"
umask 077
rm -f -- "$passfile_staging"
printf '%s:%s:*:pgshard_replication:%s\n' \
  "$PGSHARD_SOURCE_HOST" 5432 "$replication_password" > "$passfile_staging"
unset replication_password
chmod 0400 "$passfile_staging"
mv -- "$passfile_staging" "$passfile"

parent=/var/lib/postgresql/18
volume_root="${parent%/*}"
final="$parent/docker"
staging="$parent/.pgshard-standby-init"
clone_marker_name=.pgshard-standby-clone-complete
source_identity_name=.pgshard-bootstrap-complete
expected_source_identity="$(mktemp /tmp/pgshard-source-identity.XXXXXX)"
expected_clone_identity="$(mktemp /tmp/pgshard-standby-identity.XXXXXX)"
socket="$(mktemp -d /tmp/pgshard-standby-socket.XXXXXX)"
started=false
source_system_identifier=
runtime_uid="$(id -u)"
cleanup_standby_bootstrap() {
  result=$?
  trap - EXIT
  if [[ "$started" == "true" ]] || pg_ctl -D "$staging" status >/dev/null 2>&1; then
    if ! pg_ctl -D "$staging" -w -t 45 stop -m immediate >/dev/null 2>&1; then
      result=1
    fi
  fi
  rm -f -- "$expected_source_identity" "$expected_clone_identity"
  rm -rf -- "$socket"
  exit "$result"
}
trap cleanup_standby_bootstrap EXIT
printf 'cluster_uid=%s\nshard=%s\n' \
  "$PGSHARD_CLUSTER_UID" "$PGSHARD_SHARD_ID" > "$expected_source_identity"

validate_standby_data() {
  local candidate="$1"
  local control_report system_identifier tablespace_entry symlink_entry
  if [[ ! -d "$candidate" || -L "$candidate" ]]; then
    echo "refusing a missing or unsafe PostgreSQL standby data directory" >&2
    return 1
  fi
  if [[ ! -f "$candidate/PG_VERSION" || -L "$candidate/PG_VERSION" ]]; then
    echo "refusing a missing or unsafe PostgreSQL standby version" >&2
    return 1
  fi
  if [[ "$(<"$candidate/PG_VERSION")" != "18" ]]; then
    echo "refusing a PostgreSQL standby from another major version" >&2
    return 1
  fi
  if [[ ! -f "$candidate/global/pg_control" || -L "$candidate/global/pg_control" ]]; then
    echo "refusing a missing or unsafe PostgreSQL standby control file" >&2
    return 1
  fi
  control_report="$(LC_ALL=C pg_controldata "$candidate")"
  system_identifier="$(
    awk -F: '$1 == "Database system identifier" { value=$2; gsub(/^[[:space:]]+|[[:space:]]+$/, "", value); print value }' \
      <<<"$control_report"
  )"
  if [[ ! "$system_identifier" =~ ^[1-9][0-9]*$ ]]; then
    echo "refusing an invalid or ambiguous PostgreSQL system identifier" >&2
    return 1
  fi
  if [[ -n "$source_system_identifier" ]] \
    && [[ "$system_identifier" != "$source_system_identifier" ]]; then
    echo "refusing a physical clone with a different PostgreSQL system identifier" >&2
    return 1
  fi
  if [[ ! -d "$candidate/pg_wal" || -L "$candidate/pg_wal" ]]; then
    echo "refusing PostgreSQL WAL outside the managed standby PGDATA" >&2
    return 1
  fi
  if [[ ! -d "$candidate/pg_tblspc" || -L "$candidate/pg_tblspc" ]]; then
    echo "refusing an unsafe PostgreSQL standby tablespace directory" >&2
    return 1
  fi
  tablespace_entry="$(find "$candidate/pg_tblspc" -mindepth 1 -maxdepth 1 -print -quit)"
  if [[ -n "$tablespace_entry" ]]; then
    echo "refusing PostgreSQL standby tablespaces outside the managed PGDATA" >&2
    return 1
  fi
  symlink_entry="$(find "$candidate" -xdev -type l -print -quit)"
  if [[ -n "$symlink_entry" ]]; then
    echo "refusing a symlink in PostgreSQL standby storage" >&2
    return 1
  fi
  if [[ ! -f "$candidate/$source_identity_name" || -L "$candidate/$source_identity_name" ]] \
    || [[ "$(stat -c '%a:%u' "$candidate/$source_identity_name")" != "600:$runtime_uid" ]] \
    || ! cmp -s -- "$candidate/$source_identity_name" "$expected_source_identity"; then
    echo "refusing a physical backup from another cluster or shard" >&2
    return 1
  fi
  if [[ ! -f "$candidate/standby.signal" || -L "$candidate/standby.signal" ]] \
    || [[ -s "$candidate/standby.signal" ]] \
    || [[ "$(stat -c '%a:%u' "$candidate/standby.signal")" != "600:$runtime_uid" ]]; then
    echo "refusing a missing or unsafe physical standby signal" >&2
    return 1
  fi
  for recovery_state in recovery.signal backup_label tablespace_map; do
    if [[ -e "$candidate/$recovery_state" || -L "$candidate/$recovery_state" ]]; then
      echo "refusing incomplete PostgreSQL standby recovery state ($recovery_state)" >&2
      return 1
    fi
  done
  if [[ ! -f "$candidate/postgresql.auto.conf" || -L "$candidate/postgresql.auto.conf" ]]; then
    echo "refusing an unsafe standby postgresql.auto.conf" >&2
    return 1
  fi
  if grep -Eq '^[[:space:]]*[^#[:space:]]' "$candidate/postgresql.auto.conf"; then
    echo "refusing active settings in standby postgresql.auto.conf" >&2
    return 1
  else
    inspect_status=$?
    if (( inspect_status != 1 )); then
      echo "refusing standby postgresql.auto.conf that cannot be inspected safely" >&2
      return 1
    fi
  fi
  printf 'version=1\ncluster_uid=%s\nshard=%s\nmember=%s\nsource=%s\nslot=%s\ntarget_pvc_uid=%s\ntarget_secret_uid=%s\nsource_pvc_uid=%s\nreplication_secret_uid=%s\nreplication_material_sha256=%s\nsystem_identifier=%s\n' \
    "$PGSHARD_CLUSTER_UID" \
    "$PGSHARD_SHARD_ID" \
    "$PGSHARD_MEMBER_ID" \
    "$PGSHARD_SOURCE_HOST" \
    "$PGSHARD_PRIMARY_SLOT_NAME" \
    "$PGSHARD_TARGET_PVC_UID" \
    "$PGSHARD_TARGET_SECRET_UID" \
    "$PGSHARD_SOURCE_PVC_UID" \
    "$PGSHARD_REPLICATION_SECRET_UID" \
    "$PGSHARD_REPLICATION_MATERIAL_SHA256" \
    "$system_identifier" > "$expected_clone_identity"
}

if [[ -e "$final" || -L "$final" ]]; then
  if [[ ! -d "$final" || -L "$final" ]]; then
    echo "refusing to replace foreign PostgreSQL standby state" >&2
    exit 1
  fi
  validate_standby_data "$final"
  if [[ ! -f "$final/$clone_marker_name" || -L "$final/$clone_marker_name" ]] \
    || [[ "$(stat -c '%a:%u' "$final/$clone_marker_name")" != "600:$runtime_uid" ]] \
    || ! cmp -s -- "$final/$clone_marker_name" "$expected_clone_identity"; then
    echo "refusing incomplete or foreign PostgreSQL standby state" >&2
    exit 1
  fi
  rm -rf -- "$staging"
  sync "$final" "$parent" "$volume_root"
  trap - EXIT
  rm -f -- "$expected_source_identity" "$expected_clone_identity"
  rm -rf -- "$socket"
  exit 0
fi

rm -rf -- "$staging"
export PGPASSFILE="$passfile"
source_system_record="$(
  timeout --signal=TERM --kill-after=2s 10s \
    psql -X --no-password \
      --dbname="host=$PGSHARD_SOURCE_HOST port=5432 user=pgshard_replication passfile=$passfile replication=true sslmode=disable" \
      --no-align --tuples-only --field-separator='|' \
      --command='IDENTIFY_SYSTEM'
)"
if [[ ! "$source_system_record" =~ ^([1-9][0-9]*)\|([1-9][0-9]*)\|[0-9A-F]+/[0-9A-F]+\|$ ]]; then
  echo "refusing an invalid or ambiguous source IDENTIFY_SYSTEM response" >&2
  exit 1
fi
source_system_identifier="${BASH_REMATCH[1]}"
timeout --signal=TERM --kill-after=10s 15m \
  pg_basebackup \
    --pgdata="$staging" \
    --host="$PGSHARD_SOURCE_HOST" \
    --port=5432 \
    --username=pgshard_replication \
    --slot="$PGSHARD_PRIMARY_SLOT_NAME" \
    --wal-method=stream \
    --checkpoint=fast \
    --no-password

if [[ -e "$staging/standby.signal" || -L "$staging/standby.signal" ]]; then
  echo "refusing unexpected recovery state in a new physical base backup" >&2
  exit 1
fi
install -m 0600 /dev/null "$staging/standby.signal"
rm -f -- \
  "$staging/.pgshard-writable-generation" \
  "$staging/.pgshard-writable-generation.next"

primary_conninfo="host=$PGSHARD_SOURCE_HOST port=5432 user=pgshard_replication application_name=$PGSHARD_PRIMARY_SLOT_NAME passfile=$passfile sslmode=disable"
pg_ctl -D "$staging" -w -t 60 start \
  -o "-c listen_addresses='' -c unix_socket_directories='$socket' -c unix_socket_permissions=0700 -c hba_file=/etc/pgshard/quarantine.pg_hba.conf -c external_pid_file=/tmp/pgshard-standby-bootstrap.pid -c ssl=off -c restart_after_crash=off -c primary_conninfo='$primary_conninfo' -c primary_slot_name='$PGSHARD_PRIMARY_SLOT_NAME' -c recovery_target_timeline=latest -c recovery_target_action=shutdown -c restore_command= -c archive_cleanup_command= -c recovery_end_command= -c shared_preload_libraries= -c synchronous_standby_names='' -c synchronous_commit=local"
started=true
if [[ "$(timeout --signal=TERM --kill-after=2s 10s psql -X --no-password --host="$socket" --username=postgres --dbname=postgres --no-align --tuples-only --command='SELECT pg_catalog.pg_is_in_recovery()')" != "t" ]]; then
  echo "refusing a physical clone that did not enter standby recovery" >&2
  exit 1
fi
pg_ctl -D "$staging" -w -t 45 stop -m fast
started=false

validate_standby_data "$staging"
if [[ -e "$staging/$clone_marker_name" || -L "$staging/$clone_marker_name" ]]; then
  echo "refusing a pre-existing physical standby clone marker" >&2
  exit 1
fi
install -m 0600 -- "$expected_clone_identity" "$staging/$clone_marker_name"
sync \
  "$staging/standby.signal" \
  "$staging/$clone_marker_name" \
  "$staging"
mv -- "$staging" "$final"
sync "$final" "$parent" "$volume_root"
trap - EXIT
rm -f -- "$expected_source_identity" "$expected_clone_identity"
rm -rf -- "$socket"
`

// Images contains controller-owned workload composition inputs. Image
// references and the PostgreSQL runtime mode are controller configuration, not
// part of the cluster API, so changing a controller release does not mutate the
// user's database spec.
type Images struct {
	Orchestrator        string
	Pooler              string
	PostgreSQL          string
	PostgreSQLBootstrap string
	PostgreSQLRuntime   PostgreSQLRuntime
}

// PostgreSQLRuntime selects the controller-owned process composition. Direct
// preserves the currently serving singleton foundation. AgentQuarantine is an
// explicit non-serving integration boundary used to validate projected API
// identity, writable-term Lease coordination, and target-side process fencing
// before serving activation is implemented.
type PostgreSQLRuntime string

const (
	PostgreSQLRuntimeDirect          PostgreSQLRuntime = "direct"
	PostgreSQLRuntimeAgentQuarantine PostgreSQLRuntime = "agent-quarantine"
)

// String implements flag.Value without turning an empty zero value into an
// invalid default.
func (runtime PostgreSQLRuntime) String() string {
	if runtime == "" {
		return string(PostgreSQLRuntimeDirect)
	}
	return string(runtime)
}

// Set validates one explicit operator process-composition mode.
func (runtime *PostgreSQLRuntime) Set(value string) error {
	requested := PostgreSQLRuntime(value)
	if requested != PostgreSQLRuntimeDirect && requested != PostgreSQLRuntimeAgentQuarantine {
		return fmt.Errorf("PostgreSQL runtime must be %q or %q", PostgreSQLRuntimeDirect, PostgreSQLRuntimeAgentQuarantine)
	}
	*runtime = requested
	return nil
}

func (runtime PostgreSQLRuntime) agentQuarantine() bool {
	return runtime == PostgreSQLRuntimeAgentQuarantine
}

// ObservePostgreSQLRuntime classifies the process composition carried by an
// existing StatefulSet template or Pod. Missing annotations are accepted only
// for the legacy direct shape, so an operator upgrade can retain an existing
// direct singleton without silently adopting an agent runtime.
func ObservePostgreSQLRuntime(annotations map[string]string, spec corev1.PodSpec) (PostgreSQLRuntime, error) {
	var postgres *corev1.Container
	for index := range spec.Containers {
		if spec.Containers[index].Name != "postgresql" {
			continue
		}
		if postgres != nil {
			return "", fmt.Errorf("PostgreSQL runtime has duplicate postgresql containers")
		}
		postgres = &spec.Containers[index]
	}
	if postgres == nil {
		return "", fmt.Errorf("PostgreSQL runtime has no postgresql container")
	}

	annotated := PostgreSQLRuntime(annotations[PostgreSQLRuntimeAnnotation])
	agentShape := postgresqlAgentShape(annotations, spec, *postgres)
	switch annotated {
	case "", PostgreSQLRuntimeDirect:
		if agentShape {
			return "", fmt.Errorf("direct PostgreSQL runtime carries agent-quarantine process composition")
		}
		return PostgreSQLRuntimeDirect, nil
	case PostgreSQLRuntimeAgentQuarantine:
		if !agentShape {
			return "", fmt.Errorf("agent-quarantine PostgreSQL runtime annotation does not match its process composition")
		}
		return PostgreSQLRuntimeAgentQuarantine, nil
	default:
		return "", fmt.Errorf("PostgreSQL runtime annotation %q is invalid", annotated)
	}
}

// ObservePostgreSQLRuntimeForCluster additionally binds a structurally valid
// replication-bootstrap source to the PgShardCluster's immutable topology and
// durability. Pod annotations describe shape; they are not an independent
// authority that may downgrade or shrink the source's generation contract.
func ObservePostgreSQLRuntimeForCluster(cluster *pgshardv1alpha1.PgShardCluster, annotations map[string]string, spec corev1.PodSpec) (PostgreSQLRuntime, error) {
	if cluster == nil {
		return "", fmt.Errorf("cluster is nil")
	}
	observed, err := ObservePostgreSQLRuntime(annotations, spec)
	if err != nil || observed != PostgreSQLRuntimeAgentQuarantine {
		return observed, err
	}
	for _, container := range spec.Containers {
		if container.Name != "postgresql" {
			continue
		}
		mode, modeOK := containerUniqueLiteralEnvironment(container, "PGSHARD_POSTGRES_MODE")
		if !modeOK || mode != "replication-bootstrap-primary" {
			return observed, nil
		}
		if postgresqlLegacyBootstrapGenerationShape(annotations, container) {
			return observed, nil
		}
		wantDurability, wantCandidates := postgresqlGenerationDurability(cluster)
		gotDurability, durabilityOK := containerUniqueLiteralEnvironment(container, "PGSHARD_POSTGRES_GENERATION_DURABILITY")
		gotCandidates, candidatesOK := containerUniqueLiteralEnvironment(container, "PGSHARD_POSTGRES_SYNCHRONOUS_STANDBY_NAMES")
		annotatedCandidates, candidatesAnnotated := annotations[PostgreSQLSynchronousStandbysAnnotation]
		if !durabilityOK || gotDurability != wantDurability || annotations[PostgreSQLGenerationDurabilityAnnotation] != wantDurability ||
			(wantCandidates == "" && (candidatesOK || candidatesAnnotated)) ||
			(wantCandidates != "" && (!candidatesOK || gotCandidates != wantCandidates || !candidatesAnnotated || annotatedCandidates != wantCandidates)) {
			return "", fmt.Errorf("replication-bootstrap source generation durability does not match immutable cluster topology")
		}
		return observed, nil
	}
	return "", fmt.Errorf("PostgreSQL runtime has no postgresql container")
}

// IsPostgreSQLReplicationBootstrapSourcePod recognizes only the deterministic,
// role-neutral member-zero Pod composed while a multi-member shard is being
// bootstrapped. The absent role label is deliberate: this source may seed
// standbys, but it is not authorized to serve application traffic.
func IsPostgreSQLReplicationBootstrapSourcePod(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	if _, hasRole := pod.Labels[RoleLabel]; hasRole || pod.Labels[MemberLabel] != memberLabel(0) {
		return false
	}
	shardText := pod.Labels[ShardLabel]
	shard, err := strconv.ParseInt(shardText, 10, 32)
	if err != nil || shard < 0 || shardText != shardLabel(int32(shard)) {
		return false
	}
	cluster := pod.Labels[ClusterLabel]
	if cluster == "" || pod.Name != PostgreSQLMemberStatefulSetName(cluster, int32(shard), 0)+"-0" ||
		pod.Spec.ServiceAccountName != PostgreSQLAgentServiceAccountName(cluster, int32(shard)) {
		return false
	}
	runtime, err := ObservePostgreSQLRuntime(pod.Annotations, pod.Spec)
	if err != nil || runtime != PostgreSQLRuntimeAgentQuarantine {
		return false
	}
	for index := range pod.Spec.Containers {
		container := pod.Spec.Containers[index]
		if container.Name != "postgresql" {
			continue
		}
		mode, modeOK := containerUniqueLiteralEnvironment(container, "PGSHARD_POSTGRES_MODE")
		hbaFile, hbaFileOK := containerUniqueLiteralEnvironment(container, "PGSHARD_POSTGRES_HBA_FILE")
		return modeOK && hbaFileOK && mode == "replication-bootstrap-primary" &&
			hbaFile == "/etc/pgshard/replication-bootstrap-primary.pg_hba.conf"
	}
	return false
}

// IsCurrentPostgreSQLReplicationBootstrapSourcePod excludes the complete
// pre-generation-durability source shape retained by
// IsPostgreSQLReplicationBootstrapSourcePod only for existing Pod lifecycle
// fencing. New Pod bindings must carry the current generation contract.
func IsCurrentPostgreSQLReplicationBootstrapSourcePod(pod *corev1.Pod) bool {
	if !IsPostgreSQLReplicationBootstrapSourcePod(pod) {
		return false
	}
	for index := range pod.Spec.Containers {
		container := pod.Spec.Containers[index]
		if container.Name == "postgresql" {
			return !postgresqlLegacyBootstrapGenerationShape(pod.Annotations, container)
		}
	}
	return false
}

// IsPostgreSQLReplicationStandbyPod recognizes only one deterministic,
// role-neutral nonzero member composed as a TCP-closed physical standby. The
// classifier deliberately includes its upstream Pod DNS, pre-created slot,
// private passfile, and lack of writable-term or Kubernetes API authority.
func IsPostgreSQLReplicationStandbyPod(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	if _, hasRole := pod.Labels[RoleLabel]; hasRole {
		return false
	}
	shardText := pod.Labels[ShardLabel]
	shard, err := strconv.ParseInt(shardText, 10, 32)
	if err != nil || shard < 0 || shardText != shardLabel(int32(shard)) {
		return false
	}
	memberText := pod.Labels[MemberLabel]
	member, err := strconv.ParseInt(memberText, 10, 32)
	if err != nil || member <= 0 || memberText != memberLabel(int32(member)) {
		return false
	}
	cluster := pod.Labels[ClusterLabel]
	if cluster == "" || pod.Namespace == "" ||
		pod.Name != PostgreSQLMemberStatefulSetName(cluster, int32(shard), int32(member))+"-0" ||
		pod.Spec.ServiceAccountName != PostgreSQLStandbyServiceAccountName(cluster, int32(shard)) {
		return false
	}
	runtime, err := ObservePostgreSQLRuntime(pod.Annotations, pod.Spec)
	if err != nil || runtime != PostgreSQLRuntimeAgentQuarantine {
		return false
	}
	expectedSource := postgresqlMemberPodDNS(cluster, int32(shard), 0, pod.Namespace)
	expectedSlot := "pgshard_member_" + memberText
	for index := range pod.Spec.Containers {
		container := pod.Spec.Containers[index]
		if container.Name != "postgresql" {
			continue
		}
		mode, modeOK := containerUniqueLiteralEnvironment(container, "PGSHARD_POSTGRES_MODE")
		hbaFile, hbaFileOK := containerUniqueLiteralEnvironment(container, "PGSHARD_POSTGRES_HBA_FILE")
		source, sourceOK := containerUniqueLiteralEnvironment(container, "PGSHARD_POSTGRES_PRIMARY_HOST")
		slot, slotOK := containerUniqueLiteralEnvironment(container, "PGSHARD_POSTGRES_PRIMARY_SLOT_NAME")
		passfile, passfileOK := containerUniqueLiteralEnvironment(container, "PGSHARD_POSTGRES_PRIMARY_PASSFILE")
		return modeOK && hbaFileOK && sourceOK && slotOK && passfileOK &&
			mode == "replication-standby" && hbaFile == "/etc/pgshard/quarantine.pg_hba.conf" &&
			source == expectedSource && slot == expectedSlot &&
			passfile == "/run/pgshard/standby-auth/passfile" &&
			containerHasReadOnlyMount(container, "standby-passfile", "/run/pgshard/standby-auth") &&
			!containerHasMount(container, "replication-credential", "/etc/pgshard/replication")
	}
	return false
}

func postgresqlAgentShape(annotations map[string]string, spec corev1.PodSpec, postgres corev1.Container) bool {
	if spec.ServiceAccountName == "" || spec.AutomountServiceAccountToken == nil || *spec.AutomountServiceAccountToken || len(postgres.Command) != 0 || len(postgres.Args) != 0 {
		return false
	}
	mode, modeOK := containerUniqueLiteralEnvironment(postgres, "PGSHARD_POSTGRES_MODE")
	hbaFile, hbaFileOK := containerUniqueLiteralEnvironment(postgres, "PGSHARD_POSTGRES_HBA_FILE")
	quarantine := modeOK && hbaFileOK && mode == "quarantine" && hbaFile == "/etc/pgshard/quarantine.pg_hba.conf"
	bootstrapSource := modeOK && hbaFileOK && mode == "replication-bootstrap-primary" && hbaFile == "/etc/pgshard/replication-bootstrap-primary.pg_hba.conf"
	standby := modeOK && hbaFileOK && mode == "replication-standby" && hbaFile == "/etc/pgshard/quarantine.pg_hba.conf"
	if bootstrapSource {
		bootstrapSource = postgresqlBootstrapGenerationShape(annotations, postgres)
	} else if annotations[PostgreSQLGenerationDurabilityAnnotation] != "" ||
		annotations[PostgreSQLSynchronousStandbysAnnotation] != "" ||
		containerHasEnvironment(postgres, "PGSHARD_POSTGRES_GENERATION_DURABILITY") ||
		containerHasEnvironment(postgres, "PGSHARD_POSTGRES_SYNCHRONOUS_STANDBY_NAMES") {
		return false
	}
	if standby {
		source, sourceOK := containerUniqueLiteralEnvironment(postgres, "PGSHARD_POSTGRES_PRIMARY_HOST")
		port, portOK := containerUniqueLiteralEnvironment(postgres, "PGSHARD_POSTGRES_PRIMARY_PORT")
		slot, slotOK := containerUniqueLiteralEnvironment(postgres, "PGSHARD_POSTGRES_PRIMARY_SLOT_NAME")
		passfile, passfileOK := containerUniqueLiteralEnvironment(postgres, "PGSHARD_POSTGRES_PRIMARY_PASSFILE")
		standby = sourceOK && source != "" && portOK && port == "5432" &&
			slotOK && canonicalPostgreSQLMemberSlot(slot) && passfileOK &&
			passfile == "/run/pgshard/standby-auth/passfile"
	}
	if (!quarantine && !bootstrapSource && !standby) ||
		!containerHasPort(postgres, "agent-http", HTTPPort) ||
		!containerHasMount(postgres, "runtime", "/run/pgshard") {
		return false
	}
	if standby {
		if !containerHasReadOnlyMount(postgres, "standby-passfile", "/run/pgshard/standby-auth") ||
			!podHasMemoryEmptyDirVolume(spec, "standby-passfile") ||
			containerMountsSecretOrServiceAccountToken(spec, postgres) ||
			containerHasMount(postgres, "kubernetes-api", "/var/run/secrets/kubernetes.io/serviceaccount") ||
			containerHasVolumeMount(postgres, "replication-credential") ||
			podHasServiceAccountTokenProjection(spec) {
			return false
		}
		for _, name := range []string{
			"PGSHARD_CLUSTER_UID",
			"PGSHARD_POD_UID",
			"PGSHARD_LEASE_NAMESPACE",
			"PGSHARD_WRITABLE_LEASE_NAME",
			"PGSHARD_WRITABLE_LEASE_UID",
			"PGSHARD_MAX_LEASE_TTL_MS",
			"PGSHARD_WRITABLE_LEASE_DURATION_SECONDS",
			"PGSHARD_WRITABLE_LEASE_RENEW_DEADLINE_SECONDS",
			"PGSHARD_WRITABLE_LEASE_RETRY_MS",
			"PGSHARD_KUBERNETES_REQUEST_TIMEOUT_MS",
		} {
			if containerHasEnvironment(postgres, name) {
				return false
			}
		}
	} else if !containerHasMount(postgres, "kubernetes-api", "/var/run/secrets/kubernetes.io/serviceaccount") ||
		!podHasProjectedVolume(spec, "kubernetes-api") {
		return false
	}
	return probeHTTPPath(postgres.StartupProbe) == "/healthz" &&
		probeHTTPPath(postgres.LivenessProbe) == "/healthz" &&
		probeHTTPPath(postgres.ReadinessProbe) == "/readyz"
}

func postgresqlBootstrapGenerationShape(annotations map[string]string, postgres corev1.Container) bool {
	if postgresqlLegacyBootstrapGenerationShape(annotations, postgres) {
		return true
	}
	durability, durabilityOK := containerUniqueLiteralEnvironment(postgres, "PGSHARD_POSTGRES_GENERATION_DURABILITY")
	if !durabilityOK || annotations[PostgreSQLGenerationDurabilityAnnotation] != durability {
		return false
	}

	annotatedCandidates, candidatesAnnotated := annotations[PostgreSQLSynchronousStandbysAnnotation]
	switch durability {
	case "local":
		return !candidatesAnnotated && !containerHasEnvironment(postgres, "PGSHARD_POSTGRES_SYNCHRONOUS_STANDBY_NAMES")
	case "remote-apply-any-one":
		candidates, candidatesOK := containerUniqueLiteralEnvironment(postgres, "PGSHARD_POSTGRES_SYNCHRONOUS_STANDBY_NAMES")
		return candidatesOK && candidatesAnnotated && candidates == annotatedCandidates && canonicalPostgreSQLSynchronousStandbySet(candidates)
	default:
		return false
	}
}

// postgresqlLegacyBootstrapGenerationShape recognizes only the complete
// pre-generation-durability source shape shipped by v0.73. This keeps its
// already-finalized Pod inside lifecycle fencing while an OnDelete template is
// upgraded. Any partial setting is a conflicting shape, not a legacy one.
func postgresqlLegacyBootstrapGenerationShape(annotations map[string]string, postgres corev1.Container) bool {
	_, durabilityAnnotated := annotations[PostgreSQLGenerationDurabilityAnnotation]
	_, candidatesAnnotated := annotations[PostgreSQLSynchronousStandbysAnnotation]
	return !durabilityAnnotated && !candidatesAnnotated &&
		!containerHasEnvironment(postgres, "PGSHARD_POSTGRES_GENERATION_DURABILITY") &&
		!containerHasEnvironment(postgres, "PGSHARD_POSTGRES_SYNCHRONOUS_STANDBY_NAMES")
}

func canonicalPostgreSQLSynchronousStandbySet(candidates string) bool {
	return candidates == "pgshard_member_0001,pgshard_member_0002" ||
		candidates == "pgshard_member_0001,pgshard_member_0002,pgshard_member_0003,pgshard_member_0004"
}

func canonicalPostgreSQLMemberSlot(slot string) bool {
	member, ok := strings.CutPrefix(slot, "pgshard_member_")
	return ok && len(member) == 4 && strings.IndexFunc(member, func(character rune) bool {
		return character < '0' || character > '9'
	}) == -1
}

func containerUniqueLiteralEnvironment(container corev1.Container, name string) (string, bool) {
	var value string
	found := false
	for _, environment := range container.Env {
		if environment.Name != name {
			continue
		}
		if found || environment.ValueFrom != nil {
			return "", false
		}
		value = environment.Value
		found = true
	}
	return value, found
}

func containerHasLiteralEnvironment(container corev1.Container, name, value string) bool {
	for _, environment := range container.Env {
		if environment.Name == name && environment.ValueFrom == nil && environment.Value == value {
			return true
		}
	}
	return false
}

func containerHasEnvironment(container corev1.Container, name string) bool {
	for _, environment := range container.Env {
		if environment.Name == name {
			return true
		}
	}
	return false
}

func containerHasPort(container corev1.Container, name string, port int32) bool {
	for _, candidate := range container.Ports {
		if candidate.Name == name && candidate.ContainerPort == port && candidate.Protocol == corev1.ProtocolTCP {
			return true
		}
	}
	return false
}

func containerHasMount(container corev1.Container, name, path string) bool {
	for _, mount := range container.VolumeMounts {
		if mount.Name == name && mount.MountPath == path {
			return true
		}
	}
	return false
}

func containerHasVolumeMount(container corev1.Container, name string) bool {
	for _, mount := range container.VolumeMounts {
		if mount.Name == name {
			return true
		}
	}
	return false
}

func containerHasReadOnlyMount(container corev1.Container, name, path string) bool {
	for _, mount := range container.VolumeMounts {
		if mount.Name == name && mount.MountPath == path && mount.ReadOnly {
			return true
		}
	}
	return false
}

func podHasProjectedVolume(spec corev1.PodSpec, name string) bool {
	for _, volume := range spec.Volumes {
		if volume.Name == name && volume.Projected != nil {
			return true
		}
	}
	return false
}

func podHasServiceAccountTokenProjection(spec corev1.PodSpec) bool {
	for _, volume := range spec.Volumes {
		if volume.Projected == nil {
			continue
		}
		for _, source := range volume.Projected.Sources {
			if source.ServiceAccountToken != nil {
				return true
			}
		}
	}
	return false
}

func podHasMemoryEmptyDirVolume(spec corev1.PodSpec, name string) bool {
	found := false
	for _, volume := range spec.Volumes {
		if volume.Name != name {
			continue
		}
		if found || volume.EmptyDir == nil || volume.EmptyDir.Medium != corev1.StorageMediumMemory ||
			volume.Secret != nil || volume.Projected != nil || volume.PersistentVolumeClaim != nil {
			return false
		}
		found = true
	}
	return found
}

func containerMountsSecretOrServiceAccountToken(spec corev1.PodSpec, container corev1.Container) bool {
	for _, mount := range container.VolumeMounts {
		matched := false
		for _, volume := range spec.Volumes {
			if volume.Name != mount.Name {
				continue
			}
			if matched {
				return true
			}
			matched = true
			if volume.Secret != nil {
				return true
			}
			if volume.Projected != nil {
				for _, source := range volume.Projected.Sources {
					if source.Secret != nil || source.ServiceAccountToken != nil {
						return true
					}
				}
			}
		}
		if !matched {
			return true
		}
	}
	return false
}

func probeHTTPPath(probe *corev1.Probe) string {
	if probe == nil || probe.HTTPGet == nil {
		return ""
	}
	return probe.HTTPGet.Path
}

// DefaultImages are safe supporting-runtime defaults. The privileged
// PostgreSQL bootstrap image intentionally has no remote default: a deployment
// must select an immutable digest, or the exact never-pulled local development
// image used by the repository manifests.
func DefaultImages() Images {
	return Images{
		Orchestrator:      "ghcr.io/andrew01234567890/pgshard-orch:main",
		Pooler:            "ghcr.io/andrew01234567890/pgshard-pooler:main",
		PostgreSQL:        defaultPostgreSQLImage,
		PostgreSQLRuntime: PostgreSQLRuntimeDirect,
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
	if strings.TrimSpace(images.Orchestrator) == "" || strings.TrimSpace(images.Pooler) == "" || strings.TrimSpace(images.PostgreSQL) == "" {
		return fmt.Errorf("orchestrator, pooler, and PostgreSQL images must all be configured")
	}
	if images.PostgreSQLRuntime != "" && images.PostgreSQLRuntime != PostgreSQLRuntimeDirect && images.PostgreSQLRuntime != PostgreSQLRuntimeAgentQuarantine {
		return fmt.Errorf("PostgreSQL runtime must be %q or %q", PostgreSQLRuntimeDirect, PostgreSQLRuntimeAgentQuarantine)
	}
	multiMemberSourceStorage := (cluster.Spec.MembersPerShard == 3 || cluster.Spec.MembersPerShard == 5) && images.PostgreSQLRuntime.agentQuarantine()
	if cluster.Spec.MembersPerShard == 1 || multiMemberSourceStorage {
		if err := validatePostgreSQLBootstrapImage(images.PostgreSQLBootstrap); err != nil {
			return err
		}
	}
	return nil
}

// Plan returns the complete set of safe-to-create resources for cluster.
// Single-member asynchronous shards receive one PostgreSQL 18 primary. An
// explicit multi-member agent runtime receives one non-serving member-zero
// replication-bootstrap source and one TCP-closed physical standby per other
// member. Serving activation, promotion, and recovery remain fail closed.
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
	postgresqlConfig[databaseGenesisKey] = renderDatabaseGenesisSQL(cluster)
	postgresqlConfig[databaseTopologyPreflightKey] = renderDatabaseTopologyPreflightSQL(cluster)
	postgresqlHash := configMapDataHash(postgresqlConfig)
	postgresqlConfigName := PostgreSQLConfigMapName(cluster.Name, postgresqlHash)
	topologyConfig, err := renderTopology(cluster)
	if err != nil {
		return nil, err
	}
	topologyHash := configHash(topologyConfig)
	var bootstraps map[postgresqlBootstrapKey]pgshardv1alpha1.PostgreSQLBootstrapStatus
	if cluster.Spec.MembersPerShard == 1 || images.PostgreSQLRuntime.agentQuarantine() {
		bootstraps, err = postgresqlBootstraps(cluster)
		if err != nil {
			return nil, err
		}
	}
	var writableLeases map[int32]pgshardv1alpha1.PostgreSQLWritableLeaseStatus
	if images.PostgreSQLRuntime.agentQuarantine() {
		writableLeases, err = postgresqlWritableLeases(cluster)
		if err != nil {
			return nil, err
		}
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
	var replicationCredentials map[int32]pgshardv1alpha1.PostgreSQLReplicationCredentialStatus
	if cluster.Spec.MembersPerShard > 1 && images.PostgreSQLRuntime.agentQuarantine() {
		replicationCredentials, err = postgresqlReplicationCredentials(cluster)
		if err != nil {
			return nil, err
		}
	}

	objects := make([]client.Object, 0, 18+(7+cluster.Spec.MembersPerShard)*cluster.Spec.Shards)
	objects = append(objects,
		immutableConfigMap(cluster, postgresqlConfigName, postgresqlConfig),
		configMap(cluster, cluster.Name+TopologyConfigSuffix, map[string]string{"cluster.json": topologyConfig}),
		applicationService(cluster, "rw", cluster.Spec.Services.ReadWrite, PoolerRWPort),
		applicationService(cluster, "ro", cluster.Spec.Services.ReadOnly, PoolerROPort),
		applicationService(cluster, "r", cluster.Spec.Services.Read, PoolerRPort),
		orchestratorService(cluster),
		poolerService(cluster),
		orchestratorServiceAccount(cluster),
		orchestratorLeaseRole(cluster),
		orchestratorLeaseRoleBinding(cluster),
		orchestratorLease(cluster),
	)
	if cluster.Spec.MembersPerShard == 1 {
		objects = append(objects, catalogService(cluster))
	}
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		objects = append(objects,
			shardService(cluster, shard),
			postgresqlAgentServiceAccount(cluster, shard),
			postgresqlAgentLeaseRole(cluster, shard),
			postgresqlAgentLeaseRoleBinding(cluster, shard),
			PostgreSQLWritableLease(cluster, shard),
			postgresqlNetworkPolicy(cluster, shard),
		)
		if cluster.Spec.MembersPerShard == 1 {
			bootstrap := bootstraps[postgresqlBootstrapKey{shard: shard, member: 0}]
			writableLease := writableLeases[shard]
			objects = append(objects,
				postgresqlShardStatefulSet(cluster, shard, images, bootstrap.SecretName, bootstrap.PVCName, postgresqlConfigName, postgresqlHash, catalogAccess, writableLease),
				postgresqlPrimaryDisruptionBudget(cluster, shard),
			)
		} else if images.PostgreSQLRuntime.agentQuarantine() {
			bootstrap := bootstraps[postgresqlBootstrapKey{shard: shard, member: 0}]
			writableLease := writableLeases[shard]
			replicationCredential := replicationCredentials[shard]
			objects = append(objects,
				postgresqlStandbyServiceAccount(cluster, shard),
				postgresqlReplicationBootstrapSourceStatefulSet(cluster, shard, images, bootstrap, postgresqlConfigName, postgresqlHash, writableLease, replicationCredential),
			)
			for member := int32(1); member < cluster.Spec.MembersPerShard; member++ {
				objects = append(objects, postgresqlReplicationStandbyStatefulSet(
					cluster,
					shard,
					member,
					images,
					bootstraps[postgresqlBootstrapKey{shard: shard, member: member}],
					bootstrap,
					replicationCredential,
				))
			}
		}
	}

	objects = append(objects,
		orchestratorDeployment(cluster, images.Orchestrator, topologyHash),
		poolerDeployment(cluster, images.Pooler, topologyHash, catalogAccess),
		podDisruptionBudget(cluster, "orchestrator", 1),
		podDisruptionBudget(cluster, "pooler", 1),
	)
	if cluster.Spec.Pooler.Scaling.Mode == pgshardv1alpha1.ScalingHPA {
		objects = append(objects, poolerHPA(cluster))
	}
	return objects, nil
}

type postgresqlBootstrapKey struct {
	shard  int32
	member int32
}

func postgresqlBootstraps(cluster *pgshardv1alpha1.PgShardCluster) (map[postgresqlBootstrapKey]pgshardv1alpha1.PostgreSQLBootstrapStatus, error) {
	bootstraps := make(map[postgresqlBootstrapKey]pgshardv1alpha1.PostgreSQLBootstrapStatus, len(cluster.Status.PostgreSQLBootstraps))
	for _, bootstrap := range cluster.Status.PostgreSQLBootstraps {
		if bootstrap.Shard < 0 || bootstrap.Shard >= cluster.Spec.Shards || bootstrap.Member < 0 || bootstrap.Member >= cluster.Spec.MembersPerShard {
			return nil, fmt.Errorf("PostgreSQL bootstrap references invalid shard %d member %d", bootstrap.Shard, bootstrap.Member)
		}
		if bootstrap.SecretName == "" || bootstrap.SecretUID == "" || !bootstrap.PVCFenceDetached || bootstrap.PVCName == "" || bootstrap.PVCUID == "" || bootstrap.PVCStorageClassName == nil {
			return nil, fmt.Errorf("PostgreSQL bootstrap for shard %d member %d is incomplete (credential name=%t UID=%t, PVC fence detached=%t, PVC name=%t UID=%t, storage class=%t)", bootstrap.Shard, bootstrap.Member, bootstrap.SecretName != "", bootstrap.SecretUID != "", bootstrap.PVCFenceDetached, bootstrap.PVCName != "", bootstrap.PVCUID != "", bootstrap.PVCStorageClassName != nil)
		}
		key := postgresqlBootstrapKey{shard: bootstrap.Shard, member: bootstrap.Member}
		if _, duplicate := bootstraps[key]; duplicate {
			return nil, fmt.Errorf("PostgreSQL bootstrap for shard %d member %d is duplicated", bootstrap.Shard, bootstrap.Member)
		}
		bootstraps[key] = bootstrap
	}
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		for member := int32(0); member < cluster.Spec.MembersPerShard; member++ {
			if _, ok := bootstraps[postgresqlBootstrapKey{shard: shard, member: member}]; !ok {
				return nil, fmt.Errorf("PostgreSQL bootstrap for shard %d member %d is missing", shard, member)
			}
		}
	}
	return bootstraps, nil
}

func postgresqlWritableLeases(cluster *pgshardv1alpha1.PgShardCluster) (map[int32]pgshardv1alpha1.PostgreSQLWritableLeaseStatus, error) {
	checkpoints := make(map[int32]pgshardv1alpha1.PostgreSQLWritableLeaseStatus, len(cluster.Status.PostgreSQLWritableLeases))
	uids := make(map[types.UID]struct{}, len(cluster.Status.PostgreSQLWritableLeases))
	for _, checkpoint := range cluster.Status.PostgreSQLWritableLeases {
		if checkpoint.Shard < 0 || checkpoint.Shard >= cluster.Spec.Shards {
			return nil, fmt.Errorf("PostgreSQL writable-term Lease checkpoint for shard %d is invalid", checkpoint.Shard)
		}
		expectedName := PostgreSQLWritableLeaseName(cluster.Name, checkpoint.Shard)
		if checkpoint.LeaseName != expectedName || checkpoint.LeaseUID == "" || len(checkpoint.LeaseUID) > 128 {
			return nil, fmt.Errorf("PostgreSQL writable-term Lease checkpoint for shard %d is invalid", checkpoint.Shard)
		}
		if _, duplicate := checkpoints[checkpoint.Shard]; duplicate {
			return nil, fmt.Errorf("PostgreSQL writable-term Lease checkpoint for shard %d is duplicated", checkpoint.Shard)
		}
		if _, duplicate := uids[checkpoint.LeaseUID]; duplicate {
			return nil, fmt.Errorf("PostgreSQL writable-term Lease UID %s is duplicated", checkpoint.LeaseUID)
		}
		checkpoints[checkpoint.Shard] = checkpoint
		uids[checkpoint.LeaseUID] = struct{}{}
	}
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		if _, ok := checkpoints[shard]; !ok {
			return nil, fmt.Errorf("PostgreSQL writable-term Lease checkpoint for shard %d is missing", shard)
		}
	}
	return checkpoints, nil
}

func postgresqlReplicationCredentials(cluster *pgshardv1alpha1.PgShardCluster) (map[int32]pgshardv1alpha1.PostgreSQLReplicationCredentialStatus, error) {
	credentials := make(map[int32]pgshardv1alpha1.PostgreSQLReplicationCredentialStatus, len(cluster.Status.PostgreSQLReplicationCredentials))
	names := make(map[string]struct{}, len(cluster.Status.PostgreSQLReplicationCredentials))
	uids := make(map[types.UID]struct{}, len(cluster.Status.PostgreSQLReplicationCredentials))
	for _, credential := range cluster.Status.PostgreSQLReplicationCredentials {
		if credential.Shard < 0 || credential.Shard >= cluster.Spec.Shards ||
			!PostgreSQLReplicationSecretNameIsValid(cluster.Name, credential.Shard, credential.SecretName) ||
			credential.SecretUID == "" || !validCatalogMaterialSHA256(credential.MaterialSHA256) {
			return nil, fmt.Errorf("PostgreSQL replication credential checkpoint for shard %d is invalid", credential.Shard)
		}
		if _, duplicate := credentials[credential.Shard]; duplicate {
			return nil, fmt.Errorf("PostgreSQL replication credential checkpoint for shard %d is duplicated", credential.Shard)
		}
		if _, duplicate := names[credential.SecretName]; duplicate {
			return nil, fmt.Errorf("PostgreSQL replication credential Secret name %s is duplicated", credential.SecretName)
		}
		if _, duplicate := uids[credential.SecretUID]; duplicate {
			return nil, fmt.Errorf("PostgreSQL replication credential Secret UID %s is duplicated", credential.SecretUID)
		}
		credentials[credential.Shard] = credential
		names[credential.SecretName] = struct{}{}
		uids[credential.SecretUID] = struct{}{}
	}
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		if _, ok := credentials[shard]; !ok {
			return nil, fmt.Errorf("PostgreSQL replication credential checkpoint for shard %d is missing", shard)
		}
	}
	return credentials, nil
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

func renderDatabaseGenesisSQL(cluster *pgshardv1alpha1.PgShardCluster) string {
	databases := sortedDatabaseTemplates(cluster)

	var output strings.Builder
	output.WriteString("-- Generated by pgshard-operator. Manual edits are overwritten.\n")
	output.WriteString("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;\n")
	output.WriteString("SET LOCAL search_path = pg_catalog;\n")
	output.WriteString("\\i " + databaseTopologyPreflightPath + "\n")
	for _, database := range databases {
		fmt.Fprintf(
			&output,
			"SELECT pg_catalog.count(*) FROM pgshard_catalog.install_database_genesis(%s::pgshard_catalog.sql_identifier, ARRAY[",
			postgresqlStringLiteral(database.Name),
		)
		for ordinal, cell := range database.ResolvedCells(cluster.Spec.Shards) {
			if ordinal != 0 {
				output.WriteByte(',')
			}
			fmt.Fprintf(&output, "%d", cell)
		}
		output.WriteString("]::bigint[]);\n")
	}

	output.WriteString("DO $pgshard_database_genesis_postcondition$\nBEGIN\n")
	output.WriteString("  IF EXISTS (\n")
	output.WriteString("      SELECT FROM pgshard_catalog.logical_databases AS databases\n")
	output.WriteString("       WHERE databases.state <> 'retired'\n")
	output.WriteString("         AND NOT (databases.database_name::text = ANY (ARRAY[")
	for index, database := range databases {
		if index != 0 {
			output.WriteByte(',')
		}
		output.WriteString(postgresqlStringLiteral(database.Name))
	}
	output.WriteString("]::text[]))\n  ) THEN\n")
	output.WriteString("    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'database genesis contains an undeclared active logical database';\n")
	output.WriteString("  END IF;\nEND\n$pgshard_database_genesis_postcondition$;\n")
	output.WriteString("COMMIT;\n")
	return output.String()
}

func renderDatabaseTopologyPreflightSQL(cluster *pgshardv1alpha1.PgShardCluster) string {
	databases := sortedDatabaseTemplates(cluster)
	expectedRangeCount := 0
	for _, database := range databases {
		expectedRangeCount += len(database.ResolvedCells(cluster.Spec.Shards))
	}
	var output strings.Builder
	writeMismatchQuery := func(placements bool) {
		output.WriteString("SELECT EXISTS (\n")
		output.WriteString("  WITH expected_databases(database_name, shard_numbers) AS (\n")
		if len(databases) == 0 {
			output.WriteString("    SELECT NULL::text, NULL::bigint[] WHERE false\n")
		} else {
			output.WriteString("    VALUES\n")
			for index, database := range databases {
				if index != 0 {
					output.WriteString(",\n")
				}
				fmt.Fprintf(&output, "      (%s::text, ARRAY[", postgresqlStringLiteral(database.Name))
				for ordinal, cell := range database.ResolvedCells(cluster.Spec.Shards) {
					if ordinal != 0 {
						output.WriteByte(',')
					}
					fmt.Fprintf(&output, "%d", cell)
				}
				output.WriteString("]::bigint[])")
			}
			output.WriteByte('\n')
		}
		output.WriteString("  ), expected_cells AS (\n")
		output.WriteString("    SELECT databases.database_name, cells.ordinality::bigint AS range_ordinal, cells.shard_number\n")
		output.WriteString("      FROM expected_databases AS databases\n")
		output.WriteString("      CROSS JOIN LATERAL pg_catalog.unnest(databases.shard_numbers) WITH ORDINALITY AS cells(shard_number, ordinality)\n")
		output.WriteString("  ), expected_ranges AS (\n")
		output.WriteString("    SELECT database_name, range_ordinal,\n")
		output.WriteString("           pg_catalog.floor(((range_ordinal - 1)::numeric * 18446744073709551616) / pg_catalog.count(*) OVER (PARTITION BY database_name)) AS range_start,\n")
		output.WriteString("           pg_catalog.floor((range_ordinal::numeric * 18446744073709551616) / pg_catalog.count(*) OVER (PARTITION BY database_name)) AS range_end,\n")
		output.WriteString("           shard_number\n")
		output.WriteString("      FROM expected_cells\n")
		output.WriteString("  ), actual_topology_state AS (\n")
		output.WriteString("    SELECT NOT EXISTS (SELECT FROM ONLY pgshard_catalog.logical_databases)\n")
		output.WriteString("       AND NOT EXISTS (SELECT FROM ONLY pgshard_catalog.routing_epochs)\n")
		output.WriteString("       AND NOT EXISTS (SELECT FROM ONLY pgshard_catalog.routing_ranges)\n")
		if placements {
			output.WriteString("       AND NOT EXISTS (SELECT FROM ONLY pgshard_catalog.database_shards)\n")
			output.WriteString("       AND NOT EXISTS (SELECT FROM ONLY pgshard_catalog.database_shard_placements)\n")
		}
		output.WriteString("           AS is_empty\n")
		output.WriteString("  ), actual_databases AS MATERIALIZED (\n")
		output.WriteString("    SELECT databases.logical_database_id, databases.database_name, databases.state\n")
		output.WriteString("      FROM ONLY pgshard_catalog.logical_databases AS databases\n")
		output.WriteString("     WHERE databases.state <> 'retired'\n")
		fmt.Fprintf(&output, "     LIMIT %d\n", len(databases)+1)
		output.WriteString("  ), active_epoch_counts AS (\n")
		output.WriteString("    SELECT epochs.logical_database_id, pg_catalog.count(*) AS active_epoch_count\n")
		output.WriteString("      FROM ONLY pgshard_catalog.routing_epochs AS epochs\n")
		output.WriteString("      JOIN actual_databases AS databases USING (logical_database_id)\n")
		output.WriteString("     WHERE epochs.state = 'active'\n")
		output.WriteString("     GROUP BY epochs.logical_database_id\n")
		output.WriteString("  ), actual_range_sample AS MATERIALIZED (\n")
		output.WriteString("    SELECT databases.logical_database_id, databases.database_name::text AS database_name,\n")
		output.WriteString("           ranges.range_start, ranges.range_end, shards.shard_number,\n")
		output.WriteString("           databases.state AS database_state, epochs.state AS routing_state,\n")
		output.WriteString("           epochs.logical_database_id = databases.logical_database_id AS routing_is_owned,\n")
		if placements {
			output.WriteString("           database_shards.shard_ordinal AS database_shard_ordinal, database_shards.state AS database_shard_state,\n")
			output.WriteString("           placement_counts.active_placement_count,\n")
		} else {
			output.WriteString("           NULL::bigint AS database_shard_ordinal, 'active'::text AS database_shard_state,\n")
			output.WriteString("           1::bigint AS active_placement_count,\n")
		}
		output.WriteString("           shards.state AS shard_state, COALESCE(active_counts.active_epoch_count, 0) AS active_epoch_count\n")
		output.WriteString("      FROM actual_databases AS databases\n")
		output.WriteString("      LEFT JOIN ONLY pgshard_catalog.active_routing_epochs AS active ON active.logical_database_id = databases.logical_database_id\n")
		output.WriteString("      LEFT JOIN ONLY pgshard_catalog.routing_epochs AS epochs ON epochs.routing_epoch = active.routing_epoch\n")
		if placements {
			output.WriteString("      LEFT JOIN ONLY pgshard_catalog.routing_ranges AS ranges ON ranges.logical_database_id = databases.logical_database_id AND ranges.routing_epoch = active.routing_epoch\n")
			output.WriteString("      LEFT JOIN ONLY pgshard_catalog.database_shards AS database_shards ON database_shards.logical_database_id = ranges.logical_database_id AND database_shards.database_shard_id = ranges.database_shard_id\n")
			output.WriteString("      LEFT JOIN LATERAL (SELECT pg_catalog.count(*) AS active_placement_count FROM ONLY pgshard_catalog.database_shard_placements AS counted WHERE counted.logical_database_id = ranges.logical_database_id AND counted.database_shard_id = ranges.database_shard_id AND counted.state = 'active') AS placement_counts ON true\n")
			output.WriteString("      LEFT JOIN ONLY pgshard_catalog.database_shard_placements AS placements ON placements.logical_database_id = ranges.logical_database_id AND placements.database_shard_id = ranges.database_shard_id AND placements.state = 'active'\n")
			output.WriteString("      LEFT JOIN ONLY pgshard_catalog.shards AS shards ON shards.shard_id = placements.shard_id\n")
		} else {
			output.WriteString("      LEFT JOIN ONLY pgshard_catalog.routing_ranges AS ranges ON ranges.routing_epoch = active.routing_epoch\n")
			output.WriteString("      LEFT JOIN ONLY pgshard_catalog.shards AS shards ON shards.shard_id = ranges.shard_id\n")
		}
		output.WriteString("      LEFT JOIN active_epoch_counts AS active_counts ON active_counts.logical_database_id = databases.logical_database_id\n")
		fmt.Fprintf(&output, "     LIMIT %d\n", expectedRangeCount+1)
		output.WriteString("  ), actual_ranges AS (\n")
		output.WriteString("    SELECT sample.database_name,\n")
		output.WriteString("           pg_catalog.row_number() OVER (PARTITION BY sample.logical_database_id ORDER BY sample.range_start, sample.range_end)::bigint AS range_ordinal,\n")
		output.WriteString("           sample.range_start, sample.range_end, sample.shard_number, sample.database_state,\n")
		output.WriteString("           sample.routing_state, sample.routing_is_owned, sample.database_shard_ordinal,\n")
		output.WriteString("           sample.database_shard_state,\n")
		output.WriteString("           sample.active_placement_count, sample.shard_state, sample.active_epoch_count\n")
		output.WriteString("      FROM actual_range_sample AS sample\n")
		output.WriteString("  ), mismatch AS (\n")
		output.WriteString("    SELECT 1 WHERE (SELECT pg_catalog.count(*) FROM actual_databases) > ")
		fmt.Fprintf(&output, "%d\n", len(databases))
		output.WriteString("    UNION ALL\n")
		output.WriteString("    SELECT 1 WHERE (SELECT pg_catalog.count(*) FROM actual_range_sample) > ")
		fmt.Fprintf(&output, "%d\n", expectedRangeCount)
		output.WriteString("    UNION ALL\n")
		output.WriteString("    SELECT 1 WHERE NOT EXISTS (SELECT FROM expected_databases)\n")
		output.WriteString("                   AND NOT (SELECT is_empty FROM actual_topology_state)\n")
		output.WriteString("    UNION ALL\n")
		output.WriteString("    SELECT 1\n")
		output.WriteString("      FROM expected_ranges\n")
		output.WriteString("      FULL JOIN actual_ranges USING (database_name, range_ordinal)\n")
		output.WriteString("     WHERE (NOT pg_catalog.current_setting('pgshard.bootstrap_allow_empty_database_topology')::boolean\n")
		output.WriteString("            OR NOT (SELECT is_empty FROM actual_topology_state))\n")
		output.WriteString("       AND (expected_ranges.range_start IS DISTINCT FROM actual_ranges.range_start\n")
		output.WriteString("         OR expected_ranges.range_end IS DISTINCT FROM actual_ranges.range_end\n")
		output.WriteString("         OR expected_ranges.shard_number IS DISTINCT FROM actual_ranges.shard_number\n")
		output.WriteString("         OR actual_ranges.database_state IS DISTINCT FROM 'active'\n")
		output.WriteString("         OR actual_ranges.routing_state IS DISTINCT FROM 'active'\n")
		output.WriteString("         OR actual_ranges.routing_is_owned IS DISTINCT FROM true\n")
		if placements {
			output.WriteString("         OR actual_ranges.database_shard_ordinal IS DISTINCT FROM expected_ranges.range_ordinal - 1\n")
		}
		output.WriteString("         OR actual_ranges.database_shard_state IS DISTINCT FROM 'active'\n")
		output.WriteString("         OR actual_ranges.active_placement_count IS DISTINCT FROM 1\n")
		output.WriteString("         OR actual_ranges.shard_state IS DISTINCT FROM 'active'\n")
		output.WriteString("         OR actual_ranges.active_epoch_count IS DISTINCT FROM 1)\n")
		output.WriteString("    UNION ALL\n")
		output.WriteString("    SELECT 1\n")
		output.WriteString("      FROM expected_databases\n")
		output.WriteString("      JOIN ONLY pgshard_catalog.logical_databases AS databases ON databases.database_name::text = expected_databases.database_name\n")
		output.WriteString("     WHERE databases.state = 'retired'\n")
		output.WriteString("  )\n")
		output.WriteString("  SELECT FROM mismatch\n")
		output.WriteString(")")
	}

	output.WriteString("-- Generated by pgshard-operator. Manual edits are overwritten.\n")
	output.WriteString("\\if :{?PGSHARD_ALLOW_EMPTY_DATABASE_TOPOLOGY}\n")
	output.WriteString("\\else\n\\set PGSHARD_ALLOW_EMPTY_DATABASE_TOPOLOGY false\n\\endif\n")
	output.WriteString("SELECT pg_catalog.set_config('pgshard.bootstrap_allow_empty_database_topology', :'PGSHARD_ALLOW_EMPTY_DATABASE_TOPOLOGY', false);\n")
	output.WriteString("DO $pgshard_database_topology_preflight$\nDECLARE\n  topology_mismatch boolean;\nBEGIN\n")
	output.WriteString("  PERFORM state.catalog_epoch FROM pgshard_catalog.cluster_state AS state WHERE state.singleton FOR UPDATE;\n")
	output.WriteString("  IF NOT FOUND THEN\n")
	output.WriteString("    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'shardschema cluster state singleton is missing';\n")
	output.WriteString("  END IF;\n")
	output.WriteString("  IF EXISTS (SELECT FROM pg_catalog.pg_attribute AS attributes WHERE attributes.attrelid = 'pgshard_catalog.routing_ranges'::pg_catalog.regclass AND attributes.attname = 'shard_id' AND attributes.attnum > 0 AND NOT attributes.attisdropped) THEN\n")
	output.WriteString("    EXECUTE $pgshard_legacy_topology$")
	writeMismatchQuery(false)
	output.WriteString("$pgshard_legacy_topology$ INTO topology_mismatch;\n")
	output.WriteString("  ELSE\n")
	output.WriteString("    EXECUTE $pgshard_placement_topology$")
	writeMismatchQuery(true)
	output.WriteString("$pgshard_placement_topology$ INTO topology_mismatch;\n")
	output.WriteString("  END IF;\n")
	output.WriteString("  IF topology_mismatch THEN\n")
	output.WriteString("    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'RestoreTopologyMismatch: shardschema logical database topology conflicts with configured immutable database genesis';\n")
	output.WriteString("  END IF;\nEND\n$pgshard_database_topology_preflight$;\n")
	return output.String()
}

func sortedDatabaseTemplates(cluster *pgshardv1alpha1.PgShardCluster) []pgshardv1alpha1.DatabaseTemplate {
	databases := append([]pgshardv1alpha1.DatabaseTemplate(nil), cluster.Spec.Databases...)
	sort.Slice(databases, func(left, right int) bool {
		return databases[left].Name < databases[right].Name
	})
	return databases
}

func postgresqlStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

type topologyDocument struct {
	Cluster         string                `json:"cluster"`
	Namespace       string                `json:"namespace"`
	Durability      string                `json:"durability"`
	MembersPerShard int32                 `json:"membersPerShard"`
	Listeners       []topologyListener    `json:"listeners"`
	Shards          []topologyShard       `json:"shards"`
	Databases       []topologyDatabase    `json:"databases,omitempty"`
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

type topologyDatabase struct {
	Name   string  `json:"name"`
	Shards int32   `json:"shards"`
	Cells  []int32 `json:"cells"`
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
	for _, database := range sortedDatabaseTemplates(cluster) {
		document.Databases = append(document.Databases, topologyDatabase{
			Name:   database.Name,
			Shards: database.ResolvedShardCount(cluster.Spec.Shards),
			Cells:  database.ResolvedCells(cluster.Spec.Shards),
		})
	}
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

// PostgreSQLReplicationSecretPrefix returns a bounded shard-specific prefix
// for an unpredictable staged replication credential name. The controller
// appends 128 bits of randomness before checkpointing the creation intent.
func PostgreSQLReplicationSecretPrefix(cluster string, shard int32) string {
	const maximumPrefixLength = 31 // leaves 32 hexadecimal characters in a DNS label
	literal := fmt.Sprintf("%s-r%04d-", cluster, shard)
	if len(literal) <= maximumPrefixLength {
		return literal
	}
	digest := sha256.Sum256([]byte(cluster))
	encoded := hex.EncodeToString(digest[:6])
	shardSuffix := fmt.Sprintf("-r%04d-", shard)
	prefixLength := maximumPrefixLength - len(encoded) - len(shardSuffix) - 1
	return cluster[:prefixLength] + "-" + encoded + shardSuffix
}

// PostgreSQLReplicationSecretNameIsValid verifies the checkpointed random
// suffix and exact cluster/shard prefix.
func PostgreSQLReplicationSecretNameIsValid(cluster string, shard int32, name string) bool {
	prefix := PostgreSQLReplicationSecretPrefix(cluster, shard)
	if !strings.HasPrefix(name, prefix) || len(name) != len(prefix)+32 {
		return false
	}
	suffix := name[len(prefix):]
	decoded, err := hex.DecodeString(suffix)
	return err == nil && len(decoded) == 16 && hex.EncodeToString(decoded) == suffix
}

// PostgreSQLReplicationMaterialSHA256 binds the exact password bytes projected
// into primary bootstrap and standby passfile-formatting containers.
func PostgreSQLReplicationMaterialSHA256(password []byte) string {
	return catalogMaterialSHA256("pgshard-postgresql-replication-v1", password)
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

// PostgreSQLMemberAuthSecretPrefix is the readable, role-neutral portion of a
// randomly named member credential. The controller appends cryptographic
// randomness and records the resulting name and API UID before any workload can
// reference it.
func PostgreSQLMemberAuthSecretPrefix(cluster string, shard, member int32) string {
	return postgresqlMemberName(cluster, shard, member) + "-auth-"
}

// PostgreSQLAuthSecretPrefix preserves the member-zero helper used by the
// direct singleton path.
func PostgreSQLAuthSecretPrefix(cluster string, shard int32) string {
	return PostgreSQLMemberAuthSecretPrefix(cluster, shard, 0)
}

// PostgreSQLShardStatefulSetName returns the deterministic, role-neutral
// PostgreSQL workload name for one shard. The StatefulSet ordinal is the stable
// member identity; primary and replica roles belong in mutable labels.
func PostgreSQLShardStatefulSetName(cluster string, shard int32) string {
	return fmt.Sprintf("%s-shard-%04d", boundedPostgreSQLWorkloadPrefix(cluster), shard)
}

// PostgreSQLMemberStatefulSetName returns the singleton workload identity for a
// stable physical member. Member zero retains the existing name so this
// storage-only transition does not replace a running single-member Pod.
func PostgreSQLMemberStatefulSetName(cluster string, shard, member int32) string {
	base := PostgreSQLShardStatefulSetName(cluster, shard)
	if member == 0 {
		return base
	}
	return fmt.Sprintf("%s-m%04d", base, member)
}

func postgresqlMemberPodDNS(cluster string, shard, member int32, namespace string) string {
	return fmt.Sprintf(
		"%s-0.%s.%s.svc",
		PostgreSQLMemberStatefulSetName(cluster, shard, member),
		shardName(cluster, shard),
		namespace,
	)
}

// PostgreSQLWritableLeaseName returns the deterministic, role-neutral Lease
// name for one physical cell. The stable member name and Pod UID belong in the
// runtime holder identity, never in this reusable coordination envelope.
func PostgreSQLWritableLeaseName(cluster string, shard int32) string {
	return PostgreSQLShardStatefulSetName(cluster, shard) + "-term"
}

// PostgreSQLAgentServiceAccountName returns the role-neutral API identity shared
// by candidate agents in one physical cell. Pods must explicitly opt into a
// bounded projected token; the ServiceAccount itself never automounts one.
func PostgreSQLAgentServiceAccountName(cluster string, shard int32) string {
	return PostgreSQLShardStatefulSetName(cluster, shard) + "-agent"
}

func postgresqlAgentServiceAccount(cluster *pgshardv1alpha1.PgShardCluster, shard int32) *corev1.ServiceAccount {
	metadata := ownedMeta(cluster, PostgreSQLAgentServiceAccountName(cluster.Name, shard), "postgresql-agent", nil)
	metadata.Labels[ShardLabel] = shardLabel(shard)
	return &corev1.ServiceAccount{
		ObjectMeta:                   metadata,
		AutomountServiceAccountToken: ptr(false),
	}
}

// PostgreSQLStandbyServiceAccountName returns the unprivileged identity shared
// by physical standbys in one shard. It has no Role or RoleBinding and exists
// only to make the lack of Kubernetes API authority explicit and fenceable.
func PostgreSQLStandbyServiceAccountName(cluster string, shard int32) string {
	return PostgreSQLShardStatefulSetName(cluster, shard) + "-standby"
}

func postgresqlStandbyServiceAccount(cluster *pgshardv1alpha1.PgShardCluster, shard int32) *corev1.ServiceAccount {
	metadata := ownedMeta(cluster, PostgreSQLStandbyServiceAccountName(cluster.Name, shard), "postgresql-standby", nil)
	metadata.Labels[ShardLabel] = shardLabel(shard)
	return &corev1.ServiceAccount{
		ObjectMeta:                   metadata,
		AutomountServiceAccountToken: ptr(false),
	}
}

func postgresqlAgentLeaseRole(cluster *pgshardv1alpha1.PgShardCluster, shard int32) *rbacv1.Role {
	metadata := ownedMeta(cluster, PostgreSQLAgentServiceAccountName(cluster.Name, shard), "postgresql-agent", nil)
	metadata.Labels[ShardLabel] = shardLabel(shard)
	return &rbacv1.Role{
		ObjectMeta: metadata,
		Rules: []rbacv1.PolicyRule{{
			APIGroups:     []string{coordinationv1.GroupName},
			Resources:     []string{"leases"},
			ResourceNames: []string{PostgreSQLWritableLeaseName(cluster.Name, shard)},
			Verbs:         []string{"get", "update"},
		}},
	}
}

func postgresqlAgentLeaseRoleBinding(cluster *pgshardv1alpha1.PgShardCluster, shard int32) *rbacv1.RoleBinding {
	name := PostgreSQLAgentServiceAccountName(cluster.Name, shard)
	metadata := ownedMeta(cluster, name, "postgresql-agent", nil)
	metadata.Labels[ShardLabel] = shardLabel(shard)
	return &rbacv1.RoleBinding{
		ObjectMeta: metadata,
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     name,
		},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      name,
			Namespace: cluster.Namespace,
		}},
	}
}

// PostgreSQLWritableLease creates only the operator-owned coordination
// envelope. Agents own every runtime spec field and update them with
// resource-version compare-and-swap operations after pinning the Lease UID.
func PostgreSQLWritableLease(cluster *pgshardv1alpha1.PgShardCluster, shard int32) *coordinationv1.Lease {
	metadata := ownedMeta(cluster, PostgreSQLWritableLeaseName(cluster.Name, shard), "postgresql", nil)
	metadata.Labels[ShardLabel] = shardLabel(shard)
	return &coordinationv1.Lease{ObjectMeta: metadata}
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

// PostgreSQLMemberDataPVCPrefix is the readable, role-neutral portion of a randomly
// named, pre-created member data volume. Workloads only reference a name and
// UID checkpointed in PgShardCluster status.
func PostgreSQLMemberDataPVCPrefix(cluster string, shard, member int32) string {
	return postgresqlMemberName(cluster, shard, member) + "-data-"
}

// PostgreSQLDataPVCPrefix preserves the member-zero helper used by the direct
// singleton path.
func PostgreSQLDataPVCPrefix(cluster string, shard int32) string {
	return PostgreSQLMemberDataPVCPrefix(cluster, shard, 0)
}

// PostgreSQLMemberAuthSecret returns one immutable member bootstrap Secret. It starts
// cluster-owned so a late create cannot outlive a failed bootstrap. The
// controller checkpoints its API UID and detaches it before using that exact
// Secret as the durable owner of outcome-unknown PVC creates. After the exact
// PVC UID is checkpointed, ownership is inverted: the live PVC is protected
// independently and the Secret becomes its dependent tombstone.
func PostgreSQLMemberAuthSecret(cluster *pgshardv1alpha1.PgShardCluster, shard, member int32, name string, password []byte) *corev1.Secret {
	metadata := ownedMeta(cluster, name, "postgresql", nil)
	metadata.Labels[ShardLabel] = shardLabel(shard)
	metadata.Labels[MemberLabel] = memberLabel(member)
	metadata.Annotations[PostgreSQLBootstrapClusterUIDAnnotation] = string(cluster.UID)
	delete(metadata.Annotations, ApplyOwnershipAnnotation)
	return &corev1.Secret{
		ObjectMeta: metadata,
		Immutable:  ptr(true),
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{PostgreSQLPasswordKey: append([]byte(nil), password...)},
	}
}

// PostgreSQLAuthSecret preserves the member-zero helper used by the direct
// singleton path.
func PostgreSQLAuthSecret(cluster *pgshardv1alpha1.PgShardCluster, shard int32, name string, password []byte) *corev1.Secret {
	return PostgreSQLMemberAuthSecret(cluster, shard, 0, name, password)
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

// PostgreSQLReplicationIntentSecret is the non-consumable empty identity
// checkpointed before replication material exists. The controller may install
// an immutable password only by updating this exact UID and resourceVersion.
func PostgreSQLReplicationIntentSecret(cluster *pgshardv1alpha1.PgShardCluster, shard int32, name string) *corev1.Secret {
	metadata := ownedMeta(cluster, name, "postgresql-replication", nil)
	metadata.Labels[ShardLabel] = shardLabel(shard)
	metadata.Annotations[PostgreSQLReplicationClusterUIDAnnotation] = string(cluster.UID)
	return &corev1.Secret{
		ObjectMeta: metadata,
		Type:       corev1.SecretTypeOpaque,
	}
}

// PostgreSQLMemberDataPVC returns the standalone data volume for one stable physical
// member. A single-member volume is the current primary; multi-member
// agent-quarantine uses the same lifecycle only as non-serving source storage
// and gives it no role label. Size and storage class come from the checkpointed
// provisioning contract. Every create is controlled by the exact detached
// credential Secret UID. The controller adds its data-protection finalizer only
// after the API UID is checkpointed, then detaches the live PVC and anchors the
// Secret tombstone to it. Delayed create requests retain this initial owner and
// no finalizer, so Kubernetes can garbage-collect them after the tombstone is
// deleted.
func PostgreSQLMemberDataPVC(cluster *pgshardv1alpha1.PgShardCluster, shard, member int32, name string, storageSize resource.Quantity, storageClassName *string, fenceName string, fenceUID types.UID) *corev1.PersistentVolumeClaim {
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
	metadata.Labels[MemberLabel] = memberLabel(member)
	if cluster.Spec.MembersPerShard == 1 {
		metadata.Labels[RoleLabel] = "primary"
	}
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

// PostgreSQLDataPVC preserves the member-zero helper used by the direct
// singleton path.
func PostgreSQLDataPVC(cluster *pgshardv1alpha1.PgShardCluster, shard int32, name string, storageSize resource.Quantity, storageClassName *string, fenceName string, fenceUID types.UID) *corev1.PersistentVolumeClaim {
	return PostgreSQLMemberDataPVC(cluster, shard, 0, name, storageSize, storageClassName, fenceName, fenceUID)
}

func postgresqlShardStatefulSet(cluster *pgshardv1alpha1.PgShardCluster, shard int32, images Images, secretName, pvcName, configurationName, configurationHash string, catalogAccess *pgshardv1alpha1.CatalogAccessStatus, writableLease pgshardv1alpha1.PostgreSQLWritableLeaseStatus) *appsv1.StatefulSet {
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
	bootstrapPullPolicy := imagePullPolicy(images.PostgreSQLBootstrap)
	if images.PostgreSQLBootstrap == developmentPostgreSQLBootstrapImage {
		bootstrapPullPolicy = corev1.PullNever
	}
	postgres := corev1.Container{
		Name:            "postgresql",
		Image:           images.PostgreSQL,
		ImagePullPolicy: imagePullPolicy(images.PostgreSQL),
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
			{Name: "postgresql-runtime-config", MountPath: "/etc/pgshard/postgresql", ReadOnly: true},
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
	if images.PostgreSQLRuntime.agentQuarantine() {
		postgres = postgresqlAgentQuarantineContainer(cluster, shard, images.PostgreSQLBootstrap, bootstrapPullPolicy, postgresSecurity, writableLease)
	}
	bootstrap := corev1.Container{
		Name:            "bootstrap-postgresql",
		Image:           images.PostgreSQLBootstrap,
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
			{Name: "PGSHARD_POSTGRESQL_CONFIG_SHA256", Value: configurationHash},
			{Name: "PGSHARD_NODE_UID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: fmt.Sprintf("metadata.annotations['%s']", PostgreSQLNodeUIDAnnotation)}}},
			{Name: "PGSHARD_NODE_BOOT_ID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: fmt.Sprintf("metadata.annotations['%s']", PostgreSQLNodeBootIDAnnotation)}}},
		},
		Resources:       cluster.Spec.PostgreSQL.Resources,
		SecurityContext: postgresSecurity.DeepCopy(),
		VolumeMounts: []corev1.VolumeMount{
			{Name: "data", MountPath: "/var/lib/postgresql"},
			{Name: "tmp", MountPath: "/tmp"},
			{Name: "postgresql-config", MountPath: "/etc/pgshard/postgresql-source", ReadOnly: true},
			{Name: "postgresql-runtime-config", MountPath: "/etc/pgshard/postgresql"},
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
		PostgreSQLRuntimeAnnotation:       images.PostgreSQLRuntime.String(),
	}
	if shard == 0 {
		podAnnotations[shardschemaMigrationHashAnnotation] = shardschemaMigrationSHA256
	}
	volumes := []corev1.Volume{
		{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName}}},
		{Name: "runtime", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: ptr(resource.MustParse("64Mi"))}}},
		{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: ptr(resource.MustParse("64Mi"))}}},
		{Name: "postgresql-config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: configurationName}}}},
		{Name: "postgresql-runtime-config", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: ptr(resource.MustParse("2Mi"))}}},
		{Name: "bootstrap-secret", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: secretName, DefaultMode: ptr(int32(0o440))}}},
	}
	serviceAccountName := ""
	if images.PostgreSQLRuntime.agentQuarantine() {
		serviceAccountName = PostgreSQLAgentServiceAccountName(cluster.Name, shard)
		volumes = append(volumes, postgresqlAgentKubernetesAPIVolume())
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
	statefulSetMetadata := ownedMeta(cluster, name, "postgresql", nil)
	statefulSetMetadata.Annotations[PostgreSQLRuntimeAnnotation] = images.PostgreSQLRuntime.String()
	return &appsv1.StatefulSet{
		ObjectMeta: statefulSetMetadata,
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
					ServiceAccountName:            serviceAccountName,
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

func postgresqlReplicationBootstrapSourceStatefulSet(cluster *pgshardv1alpha1.PgShardCluster, shard int32, images Images, bootstrap pgshardv1alpha1.PostgreSQLBootstrapStatus, configurationName, configurationHash string, writableLease pgshardv1alpha1.PostgreSQLWritableLeaseStatus, replicationCredential pgshardv1alpha1.PostgreSQLReplicationCredentialStatus) *appsv1.StatefulSet {
	const (
		postgresUID = int64(999)
		replicas    = int32(1)
	)
	name := PostgreSQLMemberStatefulSetName(cluster.Name, shard, 0)
	selector := componentSelector(cluster, "postgresql")
	selector[ShardLabel] = shardLabel(shard)
	selector[MemberLabel] = memberLabel(0)
	podLabels := maps.Clone(selector)
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
	bootstrapPullPolicy := imagePullPolicy(images.PostgreSQLBootstrap)
	if images.PostgreSQLBootstrap == developmentPostgreSQLBootstrapImage {
		bootstrapPullPolicy = corev1.PullNever
	}
	agent := postgresqlReplicationBootstrapPrimaryContainer(cluster, shard, images.PostgreSQLBootstrap, bootstrapPullPolicy, postgresSecurity, writableLease)
	generationDurability, synchronousStandbys := postgresqlGenerationDurability(cluster)
	bootstrapContainer := corev1.Container{
		Name:            "bootstrap-postgresql",
		Image:           images.PostgreSQLBootstrap,
		ImagePullPolicy: bootstrapPullPolicy,
		Command:         []string{"bash", "-ceu", postgresqlBootstrapScript},
		Env: []corev1.EnvVar{
			{Name: "PGSHARD_CLUSTER_UID", Value: string(cluster.UID)},
			{Name: "PGSHARD_SHARD_ID", Value: shardLabel(shard)},
			{Name: "PGSHARD_POSTGRESQL_MAJOR", Value: pgshardv1alpha1.PostgreSQLMajor18},
			{Name: "PGSHARD_SHARD_COUNT", Value: fmt.Sprintf("%d", cluster.Spec.Shards)},
			{Name: "PGSHARD_MAXIMUM_SHARDS", Value: fmt.Sprintf("%d", pgshardv1alpha1.MaximumShards)},
			{Name: "PGSHARD_BOOTSTRAP_SHARDSCHEMA", Value: "false"},
			{Name: "PGSHARD_BOOTSTRAP_HBA_MODE", Value: "replication-bootstrap-primary"},
			{Name: "PGSHARD_MEMBERS_PER_SHARD", Value: fmt.Sprintf("%d", cluster.Spec.MembersPerShard)},
			{Name: "PGSHARD_REPLICATION_MATERIAL_SHA256", Value: replicationCredential.MaterialSHA256},
			{Name: "PGSHARD_SHARDSCHEMA_MIGRATION", Value: shardschemaMigrationPath},
			{Name: "PGSHARD_SHARDSCHEMA_MIGRATION_SHA256", Value: shardschemaMigrationSHA256},
			{Name: "PGSHARD_POSTGRESQL_CONFIG_SHA256", Value: configurationHash},
			{Name: "PGSHARD_NODE_UID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: fmt.Sprintf("metadata.annotations['%s']", PostgreSQLNodeUIDAnnotation)}}},
			{Name: "PGSHARD_NODE_BOOT_ID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: fmt.Sprintf("metadata.annotations['%s']", PostgreSQLNodeBootIDAnnotation)}}},
		},
		Resources:       cluster.Spec.PostgreSQL.Resources,
		SecurityContext: postgresSecurity.DeepCopy(),
		VolumeMounts: []corev1.VolumeMount{
			{Name: "data", MountPath: "/var/lib/postgresql"},
			{Name: "tmp", MountPath: "/tmp"},
			{Name: "postgresql-config", MountPath: "/etc/pgshard/postgresql-source", ReadOnly: true},
			{Name: "postgresql-runtime-config", MountPath: "/etc/pgshard/postgresql"},
			{Name: "bootstrap-secret", MountPath: "/etc/pgshard/bootstrap", ReadOnly: true},
			{Name: "replication-credential", MountPath: "/etc/pgshard/replication", ReadOnly: true},
		},
	}
	automount := false
	enableServiceLinks := false
	podAnnotations := map[string]string{
		ConfigHashAnnotation:                     configurationHash,
		PostgreSQLPodClusterUIDAnnotation:        string(cluster.UID),
		PostgreSQLRuntimeAnnotation:              images.PostgreSQLRuntime.String(),
		PostgreSQLGenerationDurabilityAnnotation: generationDurability,
	}
	if synchronousStandbys != "" {
		podAnnotations[PostgreSQLSynchronousStandbysAnnotation] = synchronousStandbys
	}
	volumes := []corev1.Volume{
		{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: bootstrap.PVCName}}},
		{Name: "runtime", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: ptr(resource.MustParse("64Mi"))}}},
		{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: ptr(resource.MustParse("64Mi"))}}},
		{Name: "postgresql-config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: configurationName}}}},
		{Name: "postgresql-runtime-config", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: ptr(resource.MustParse("2Mi"))}}},
		{Name: "bootstrap-secret", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: bootstrap.SecretName, DefaultMode: ptr(int32(0o440))}}},
		{Name: "replication-credential", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName:  replicationCredential.SecretName,
			DefaultMode: ptr(int32(0o440)),
			Items:       []corev1.KeyToPath{{Key: PostgreSQLReplicationPasswordKey, Path: "replication-password", Mode: ptr(int32(0o440))}},
		}}},
		postgresqlAgentKubernetesAPIVolume(),
	}
	statefulSetMetadata := ownedMeta(cluster, name, "postgresql", nil)
	statefulSetMetadata.Labels[ShardLabel] = shardLabel(shard)
	statefulSetMetadata.Labels[MemberLabel] = memberLabel(0)
	statefulSetMetadata.Annotations[PostgreSQLRuntimeAnnotation] = images.PostgreSQLRuntime.String()
	statefulSetMetadata.Annotations[PostgreSQLGenerationDurabilityAnnotation] = generationDurability
	if synchronousStandbys != "" {
		statefulSetMetadata.Annotations[PostgreSQLSynchronousStandbysAnnotation] = synchronousStandbys
	}
	return &appsv1.StatefulSet{
		ObjectMeta: statefulSetMetadata,
		Spec: appsv1.StatefulSetSpec{
			Replicas:            ptr(replicas),
			ServiceName:         shardName(cluster.Name, shard),
			PodManagementPolicy: appsv1.OrderedReadyPodManagement,
			MinReadySeconds:     5,
			UpdateStrategy:      appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType},
			Selector:            &metav1.LabelSelector{MatchLabels: selector},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      podLabels,
					Annotations: podAnnotations,
					Finalizers:  []string{PostgreSQLPodTerminationFinalizer},
				},
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken:  &automount,
					ServiceAccountName:            PostgreSQLAgentServiceAccountName(cluster.Name, shard),
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
					InitContainers: []corev1.Container{bootstrapContainer},
					Containers:     []corev1.Container{agent},
					Volumes:        volumes,
				},
			},
		},
	}
}

func postgresqlReplicationStandbyStatefulSet(cluster *pgshardv1alpha1.PgShardCluster, shard, member int32, images Images, bootstrap, sourceBootstrap pgshardv1alpha1.PostgreSQLBootstrapStatus, replicationCredential pgshardv1alpha1.PostgreSQLReplicationCredentialStatus) *appsv1.StatefulSet {
	const (
		postgresUID = int64(999)
		replicas    = int32(1)
	)
	name := PostgreSQLMemberStatefulSetName(cluster.Name, shard, member)
	selector := componentSelector(cluster, "postgresql")
	selector[ShardLabel] = shardLabel(shard)
	selector[MemberLabel] = memberLabel(member)
	podLabels := maps.Clone(selector)
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
	pullPolicy := imagePullPolicy(images.PostgreSQLBootstrap)
	if images.PostgreSQLBootstrap == developmentPostgreSQLBootstrapImage {
		pullPolicy = corev1.PullNever
	}
	sourceHost := postgresqlMemberPodDNS(cluster.Name, shard, 0, cluster.Namespace)
	slotName := "pgshard_member_" + memberLabel(member)
	bootstrapContainer := corev1.Container{
		Name:            "bootstrap-standby",
		Image:           images.PostgreSQLBootstrap,
		ImagePullPolicy: pullPolicy,
		Command:         []string{"bash", "-ceu", postgresqlStandbyBootstrapScript},
		Env: []corev1.EnvVar{
			{Name: "PGSHARD_CLUSTER_UID", Value: string(cluster.UID)},
			{Name: "PGSHARD_SHARD_ID", Value: shardLabel(shard)},
			{Name: "PGSHARD_MEMBER_ID", Value: memberLabel(member)},
			{Name: "PGSHARD_SOURCE_HOST", Value: sourceHost},
			{Name: "PGSHARD_PRIMARY_SLOT_NAME", Value: slotName},
			{Name: "PGSHARD_REPLICATION_MATERIAL_SHA256", Value: replicationCredential.MaterialSHA256},
			{Name: "PGSHARD_TARGET_PVC_UID", Value: string(bootstrap.PVCUID)},
			{Name: "PGSHARD_TARGET_SECRET_UID", Value: string(bootstrap.SecretUID)},
			{Name: "PGSHARD_SOURCE_PVC_UID", Value: string(sourceBootstrap.PVCUID)},
			{Name: "PGSHARD_REPLICATION_SECRET_UID", Value: string(replicationCredential.SecretUID)},
			{Name: "PGSHARD_NODE_UID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: fmt.Sprintf("metadata.annotations['%s']", PostgreSQLNodeUIDAnnotation)}}},
			{Name: "PGSHARD_NODE_BOOT_ID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: fmt.Sprintf("metadata.annotations['%s']", PostgreSQLNodeBootIDAnnotation)}}},
		},
		Resources:       cluster.Spec.PostgreSQL.Resources,
		SecurityContext: postgresSecurity.DeepCopy(),
		VolumeMounts: []corev1.VolumeMount{
			{Name: "data", MountPath: "/var/lib/postgresql"},
			{Name: "tmp", MountPath: "/tmp"},
			{Name: "replication-credential", MountPath: "/etc/pgshard/replication", ReadOnly: true},
			{Name: "standby-passfile", MountPath: "/run/pgshard/standby-auth"},
		},
	}
	agent := postgresqlReplicationStandbyContainer(
		cluster,
		shard,
		images.PostgreSQLBootstrap,
		pullPolicy,
		postgresSecurity,
		sourceHost,
		slotName,
	)
	automount := false
	enableServiceLinks := false
	podAnnotations := map[string]string{
		PostgreSQLPodClusterUIDAnnotation: string(cluster.UID),
		PostgreSQLRuntimeAnnotation:       images.PostgreSQLRuntime.String(),
	}
	volumes := []corev1.Volume{
		{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: bootstrap.PVCName}}},
		{Name: "runtime", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: ptr(resource.MustParse("64Mi"))}}},
		{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: ptr(resource.MustParse("64Mi"))}}},
		{Name: "replication-credential", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName:  replicationCredential.SecretName,
			DefaultMode: ptr(int32(0o440)),
			Items:       []corev1.KeyToPath{{Key: PostgreSQLReplicationPasswordKey, Path: "replication-password", Mode: ptr(int32(0o440))}},
		}}},
		{Name: "standby-passfile", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{
			Medium:    corev1.StorageMediumMemory,
			SizeLimit: ptr(resource.MustParse("64Ki")),
		}}},
	}
	statefulSetMetadata := ownedMeta(cluster, name, "postgresql", nil)
	statefulSetMetadata.Labels[ShardLabel] = shardLabel(shard)
	statefulSetMetadata.Labels[MemberLabel] = memberLabel(member)
	statefulSetMetadata.Annotations[PostgreSQLRuntimeAnnotation] = images.PostgreSQLRuntime.String()
	return &appsv1.StatefulSet{
		ObjectMeta: statefulSetMetadata,
		Spec: appsv1.StatefulSetSpec{
			Replicas:            ptr(replicas),
			ServiceName:         shardName(cluster.Name, shard),
			PodManagementPolicy: appsv1.OrderedReadyPodManagement,
			MinReadySeconds:     5,
			UpdateStrategy:      appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType},
			Selector:            &metav1.LabelSelector{MatchLabels: selector},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      podLabels,
					Annotations: podAnnotations,
					Finalizers:  []string{PostgreSQLPodTerminationFinalizer},
				},
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken:  &automount,
					ServiceAccountName:            PostgreSQLStandbyServiceAccountName(cluster.Name, shard),
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
					InitContainers: []corev1.Container{bootstrapContainer},
					Containers:     []corev1.Container{agent},
					Volumes:        volumes,
				},
			},
		},
	}
}

func postgresqlAgentQuarantineContainer(cluster *pgshardv1alpha1.PgShardCluster, shard int32, image string, pullPolicy corev1.PullPolicy, security *corev1.SecurityContext, writableLease pgshardv1alpha1.PostgreSQLWritableLeaseStatus) corev1.Container {
	return postgresqlAgentWritableContainer(cluster, shard, image, pullPolicy, security, writableLease, "quarantine", "/etc/pgshard/quarantine.pg_hba.conf")
}

func postgresqlReplicationBootstrapPrimaryContainer(cluster *pgshardv1alpha1.PgShardCluster, shard int32, image string, pullPolicy corev1.PullPolicy, security *corev1.SecurityContext, writableLease pgshardv1alpha1.PostgreSQLWritableLeaseStatus) corev1.Container {
	container := postgresqlAgentWritableContainer(cluster, shard, image, pullPolicy, security, writableLease, "replication-bootstrap-primary", "/etc/pgshard/replication-bootstrap-primary.pg_hba.conf")
	durability, synchronousStandbys := postgresqlGenerationDurability(cluster)
	container.Env = append(container.Env, corev1.EnvVar{Name: "PGSHARD_POSTGRES_GENERATION_DURABILITY", Value: durability})
	if synchronousStandbys != "" {
		container.Env = append(container.Env, corev1.EnvVar{Name: "PGSHARD_POSTGRES_SYNCHRONOUS_STANDBY_NAMES", Value: synchronousStandbys})
	}
	return container
}

// postgresqlGenerationDurability derives the source's startup floor only from
// the immutable topology and durability fields. The exact member identities
// are passed directly to the agent; generated PostgreSQL configuration is not
// an authority for this decision.
func postgresqlGenerationDurability(cluster *pgshardv1alpha1.PgShardCluster) (string, string) {
	if cluster.Spec.Durability == pgshardv1alpha1.DurabilityAsynchronous {
		return "local", ""
	}
	candidates := make([]string, 0, cluster.Spec.MembersPerShard-1)
	for member := int32(1); member < cluster.Spec.MembersPerShard; member++ {
		candidates = append(candidates, "pgshard_member_"+memberLabel(member))
	}
	return "remote-apply-any-one", strings.Join(candidates, ",")
}

func postgresqlReplicationStandbyContainer(cluster *pgshardv1alpha1.PgShardCluster, shard int32, image string, pullPolicy corev1.PullPolicy, security *corev1.SecurityContext, sourceHost, slotName string) corev1.Container {
	environment := []corev1.EnvVar{
		{Name: "PGSHARD_HTTP_BIND", Value: "0.0.0.0:8080"},
		{Name: "PGSHARD_CLUSTER_ID", Value: cluster.Name},
		{Name: "PGSHARD_SHARD_ID", Value: fmt.Sprintf("%d", shard)},
		{Name: "PGSHARD_INSTANCE_ID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "PGSHARD_POSTGRES_MODE", Value: "replication-standby"},
		{Name: "PGDATA", Value: "/var/lib/postgresql/18/docker"},
		{Name: "PGSHARD_POSTGRES_BIN", Value: "/usr/lib/postgresql/18/bin/postgres"},
		{Name: "PGSHARD_POSTGRES_SOCKET_DIR", Value: "/run/pgshard/postgres"},
		{Name: "PGSHARD_POSTGRES_HBA_FILE", Value: "/etc/pgshard/quarantine.pg_hba.conf"},
		{Name: "PGSHARD_POSTGRES_PRIMARY_HOST", Value: sourceHost},
		{Name: "PGSHARD_POSTGRES_PRIMARY_PORT", Value: "5432"},
		{Name: "PGSHARD_POSTGRES_PRIMARY_SLOT_NAME", Value: slotName},
		{Name: "PGSHARD_POSTGRES_PRIMARY_PASSFILE", Value: "/run/pgshard/standby-auth/passfile"},
		{Name: "PGSHARD_POSTGRES_SMART_SHUTDOWN_MS", Value: "5000"},
		{Name: "PGSHARD_POSTGRES_FAST_SHUTDOWN_MS", Value: "44000"},
		{Name: "PGSHARD_POSTGRES_IMMEDIATE_SHUTDOWN_MS", Value: "500"},
	}
	if endpoint := cluster.Spec.Observability.OpenTelemetryEndpoint; endpoint != "" {
		environment = append(environment, corev1.EnvVar{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: endpoint})
	}
	return corev1.Container{
		Name:            "postgresql",
		Image:           image,
		ImagePullPolicy: pullPolicy,
		Env:             environment,
		Ports: []corev1.ContainerPort{
			{Name: "postgresql", ContainerPort: PostgreSQLPort, Protocol: corev1.ProtocolTCP},
			{Name: "agent-http", ContainerPort: HTTPPort, Protocol: corev1.ProtocolTCP},
		},
		Resources:       cluster.Spec.PostgreSQL.Resources,
		SecurityContext: security.DeepCopy(),
		StartupProbe: &corev1.Probe{
			ProbeHandler:     corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("agent-http"), Scheme: corev1.URISchemeHTTP}},
			PeriodSeconds:    1,
			TimeoutSeconds:   1,
			FailureThreshold: 40,
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler:     corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("agent-http"), Scheme: corev1.URISchemeHTTP}},
			PeriodSeconds:    5,
			TimeoutSeconds:   1,
			FailureThreshold: 3,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler:     corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromString("agent-http"), Scheme: corev1.URISchemeHTTP}},
			PeriodSeconds:    2,
			TimeoutSeconds:   1,
			FailureThreshold: 1,
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "data", MountPath: "/var/lib/postgresql"},
			{Name: "runtime", MountPath: "/run/pgshard"},
			{Name: "tmp", MountPath: "/tmp"},
			{Name: "standby-passfile", MountPath: "/run/pgshard/standby-auth", ReadOnly: true},
		},
	}
}

func postgresqlAgentWritableContainer(cluster *pgshardv1alpha1.PgShardCluster, shard int32, image string, pullPolicy corev1.PullPolicy, security *corev1.SecurityContext, writableLease pgshardv1alpha1.PostgreSQLWritableLeaseStatus, mode, hbaFile string) corev1.Container {
	environment := []corev1.EnvVar{
		{Name: "PGSHARD_HTTP_BIND", Value: "0.0.0.0:8080"},
		{Name: "PGSHARD_CLUSTER_ID", Value: cluster.Name},
		{Name: "PGSHARD_CLUSTER_UID", Value: string(cluster.UID)},
		{Name: "PGSHARD_SHARD_ID", Value: fmt.Sprintf("%d", shard)},
		{Name: "PGSHARD_INSTANCE_ID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "PGSHARD_POD_UID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}},
		{Name: "PGSHARD_LEASE_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
		{Name: "PGSHARD_WRITABLE_LEASE_NAME", Value: writableLease.LeaseName},
		{Name: "PGSHARD_WRITABLE_LEASE_UID", Value: string(writableLease.LeaseUID)},
		{Name: "PGSHARD_MAX_LEASE_TTL_MS", Value: "15000"},
		{Name: "PGSHARD_WRITABLE_LEASE_DURATION_SECONDS", Value: "15"},
		{Name: "PGSHARD_WRITABLE_LEASE_RENEW_DEADLINE_SECONDS", Value: "10"},
		{Name: "PGSHARD_WRITABLE_LEASE_RETRY_MS", Value: "2000"},
		{Name: "PGSHARD_KUBERNETES_REQUEST_TIMEOUT_MS", Value: "2000"},
		{Name: "PGSHARD_POSTGRES_MODE", Value: mode},
		{Name: "PGDATA", Value: "/var/lib/postgresql/18/docker"},
		{Name: "PGSHARD_POSTGRES_BIN", Value: "/usr/lib/postgresql/18/bin/postgres"},
		{Name: "PGSHARD_POSTGRES_SOCKET_DIR", Value: "/run/pgshard/postgres"},
		{Name: "PGSHARD_POSTGRES_HBA_FILE", Value: hbaFile},
		{Name: "PGSHARD_POSTGRES_SMART_SHUTDOWN_MS", Value: "5000"},
		{Name: "PGSHARD_POSTGRES_FAST_SHUTDOWN_MS", Value: "44000"},
		{Name: "PGSHARD_POSTGRES_IMMEDIATE_SHUTDOWN_MS", Value: "500"},
	}
	if endpoint := cluster.Spec.Observability.OpenTelemetryEndpoint; endpoint != "" {
		environment = append(environment, corev1.EnvVar{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: endpoint})
	}
	return corev1.Container{
		Name:            "postgresql",
		Image:           image,
		ImagePullPolicy: pullPolicy,
		Env:             environment,
		Ports: []corev1.ContainerPort{
			{Name: "postgresql", ContainerPort: PostgreSQLPort, Protocol: corev1.ProtocolTCP},
			{Name: "agent-http", ContainerPort: HTTPPort, Protocol: corev1.ProtocolTCP},
		},
		Resources:       cluster.Spec.PostgreSQL.Resources,
		SecurityContext: security.DeepCopy(),
		StartupProbe: &corev1.Probe{
			ProbeHandler:     corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("agent-http"), Scheme: corev1.URISchemeHTTP}},
			PeriodSeconds:    1,
			TimeoutSeconds:   1,
			FailureThreshold: 40,
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler:     corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("agent-http"), Scheme: corev1.URISchemeHTTP}},
			PeriodSeconds:    5,
			TimeoutSeconds:   1,
			FailureThreshold: 3,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler:     corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromString("agent-http"), Scheme: corev1.URISchemeHTTP}},
			PeriodSeconds:    2,
			TimeoutSeconds:   1,
			FailureThreshold: 1,
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "data", MountPath: "/var/lib/postgresql"},
			{Name: "runtime", MountPath: "/run/pgshard"},
			{Name: "tmp", MountPath: "/tmp"},
			{Name: "kubernetes-api", MountPath: "/var/run/secrets/kubernetes.io/serviceaccount", ReadOnly: true},
		},
	}
}

func postgresqlAgentKubernetesAPIVolume() corev1.Volume {
	return corev1.Volume{
		Name: "kubernetes-api",
		VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
			DefaultMode: ptr(int32(0o440)),
			Sources: []corev1.VolumeProjection{
				{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token", ExpirationSeconds: ptr(int64(600))}},
				{ConfigMap: &corev1.ConfigMapProjection{
					LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"},
					Items:                []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}},
				}},
				{DownwardAPI: &corev1.DownwardAPIProjection{Items: []corev1.DownwardAPIVolumeFile{{
					Path:     "namespace",
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
				}}}},
			},
		}},
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

func orchestratorServiceAccount(cluster *pgshardv1alpha1.PgShardCluster) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta:                   ownedMeta(cluster, cluster.Name+OrchestratorSuffix, "orchestrator", nil),
		AutomountServiceAccountToken: ptr(false),
	}
}

func orchestratorLeaseRole(cluster *pgshardv1alpha1.PgShardCluster) *rbacv1.Role {
	name := cluster.Name + OrchestratorSuffix
	return &rbacv1.Role{
		ObjectMeta: ownedMeta(cluster, name, "orchestrator", nil),
		Rules: []rbacv1.PolicyRule{{
			APIGroups:     []string{coordinationv1.GroupName},
			Resources:     []string{"leases"},
			ResourceNames: []string{cluster.Name + OrchestratorLeaseSuffix},
			Verbs:         []string{"get", "update"},
		}},
	}
}

func orchestratorLeaseRoleBinding(cluster *pgshardv1alpha1.PgShardCluster) *rbacv1.RoleBinding {
	name := cluster.Name + OrchestratorSuffix
	return &rbacv1.RoleBinding{
		ObjectMeta: ownedMeta(cluster, name, "orchestrator", nil),
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     name,
		},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      name,
			Namespace: cluster.Namespace,
		}},
	}
}

// orchestratorLease creates only the stable identity and ownership envelope.
// The operator intentionally owns no Lease spec fields: orchestrator replicas
// update those leaves with resourceVersion compare-and-swap operations.
func orchestratorLease(cluster *pgshardv1alpha1.PgShardCluster) *coordinationv1.Lease {
	return &coordinationv1.Lease{
		ObjectMeta: ownedMeta(cluster, cluster.Name+OrchestratorLeaseSuffix, "orchestrator", nil),
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

func orchestratorDeployment(cluster *pgshardv1alpha1.PgShardCluster, image, hash string) *appsv1.Deployment {
	const replicas int32 = 3
	selector := componentSelector(cluster, "orchestrator")
	env := []corev1.EnvVar{
		{Name: "PGSHARD_CLUSTER_ID", Value: cluster.Name},
		{Name: "PGSHARD_CLUSTER_UID", Value: string(cluster.UID)},
		{Name: "PGSHARD_ORCH_ID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "PGSHARD_POD_UID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}},
		{Name: "PGSHARD_LEASE_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
		{Name: "PGSHARD_LEASE_NAME", Value: cluster.Name + OrchestratorLeaseSuffix},
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
	automountServiceAccountToken := true
	podSpec.AutomountServiceAccountToken = &automountServiceAccountToken
	podSpec.ServiceAccountName = cluster.Name + OrchestratorSuffix
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

func postgresqlMemberName(cluster string, shard, member int32) string {
	return fmt.Sprintf("%s-member-%04d", shardName(cluster, shard), member)
}

func shardLabel(shard int32) string { return fmt.Sprintf("%04d", shard) }

func memberLabel(member int32) string { return fmt.Sprintf("%04d", member) }

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
