// Package resources produces the Kubernetes resources owned by a PgShardCluster.
// Planning is deliberately pure: the controller can test and diff a complete,
// deterministic desired state before it writes anything to the API server.
package resources

import (
	"crypto/sha256"
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

	PostgreSQLConfigSuffix = "-postgresql-config"
	PostgreSQLPasswordKey  = "superuser-password"
	TopologyConfigSuffix   = "-topology"
	EtcdSuffix             = "-etcd"
	OrchestratorSuffix     = "-orchestrator"
	PoolerSuffix           = "-pooler"

	PostgreSQLPort int32 = 5432
	PoolerRWPort   int32 = 5432
	PoolerROPort   int32 = 5433
	PoolerRPort    int32 = 5434
	EtcdClientPort int32 = 2379
	EtcdPeerPort   int32 = 2380
	HTTPPort       int32 = 8080

	etcdExecutable                      = "/usr/local/bin/etcd"
	defaultEtcdImage                    = "registry.k8s.io/etcd:3.6.5-0@sha256:042ef9c02799eb9303abf1aa99b09f09d94b8ee3ba0c2dd3f42dc4e1d3dce534"
	defaultPostgreSQLImage              = "docker.io/library/postgres@sha256:311136771dca6826c3b6e691ebf8cb6e896e165074bc57a728f9619f25f0c4c7"
	developmentPostgreSQLBootstrapImage = "pgshard/postgres-agent:dev"

	ConfigHashAnnotation                    = "pgshard.io/config-hash"
	ApplyOwnershipAnnotation                = "pgshard.io/apply-ownership"
	ApplyOwnershipVersion                   = "v1"
	RetainedFromAnnotation                  = "pgshard.io/retained-from"
	PostgreSQLBootstrapClusterUIDAnnotation = "pgshard.io/bootstrap-cluster-uid"
	PostgreSQLDataClusterUIDAnnotation      = "pgshard.io/data-cluster-uid"
	PostgreSQLDataProtectionFinalizer       = "pgshard.io/postgresql-data-protection"
	PostgreSQLPodClusterUIDAnnotation       = "pgshard.io/postgresql-cluster-uid"
	PostgreSQLNodeUIDAnnotation             = "pgshard.io/postgresql-node-uid"
	PostgreSQLNodeBootIDAnnotation          = "pgshard.io/postgresql-node-boot-id"
	PostgreSQLPodTerminationFinalizer       = "pgshard.io/postgresql-termination"
	postgresqlBootstrapMarker               = ".pgshard-bootstrap-complete"
	shardschemaMigrationPath                = "/usr/share/pgshard/migrations/0001_shardschema.sql"
	shardschemaMigrationSHA256              = "df8cf333c840add50e584ba7d968648ef6c740d447cc66a108fb82aba1751fb9"
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
  # initdb has already persisted the new cluster. Flush only the files and
  # directory entries that this script added so another mounted filesystem
  # cannot delay PostgreSQL bootstrap or Pod termination.
  sync "$staging/pg_hba.conf" "$staging/.pgshard-bootstrap-complete" "$staging"
  mv -- "$staging" "$final"
  sync "$final" "$parent" "$volume_root"
fi

cleanup_expected
trap - EXIT

if [[ "$PGSHARD_BOOTSTRAP_SHARDSCHEMA" != "true" ]]; then
  exit 0
fi

socket=/tmp/pgshard-catalog-bootstrap
rm -rf -- "$socket"
mkdir -m 0700 -- "$socket"
export PGOPTIONS='-c lock_timeout=5s -c statement_timeout=30s -c transaction_timeout=120s -c idle_in_transaction_session_timeout=30s'
stop_temporary_postgres() {
  result=$?
  trap - EXIT
  if pg_ctl -D "$final" status >/dev/null 2>&1; then
    if ! pg_ctl -D "$final" -w -t 45 stop -m fast; then
      result=1
    fi
  fi
  exit "$result"
}
trap stop_temporary_postgres EXIT

pg_ctl -D "$final" -w -t 45 start \
  -o "-c config_file=/etc/pgshard/postgresql/primary-0000.conf -c listen_addresses='' -c unix_socket_directories='$socket' -c unix_socket_permissions=0700"

database_exists="$(
  psql -X --no-password --host="$socket" --username=postgres --dbname=postgres --no-align --tuples-only \
    --command="SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'shardschema'"
)"
case "$database_exists" in
  1) ;;
  "")
    createdb --no-password --host="$socket" --username=postgres --template=template0 --encoding=UTF8 shardschema
    ;;
  *)
    echo "refusing ambiguous shardschema database lookup" >&2
    exit 1
    ;;
esac

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

validate_shard_inventory() {
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
    echo "refusing shardschema inventory that conflicts with the configured immutable shards" >&2
    return 1
  fi
}

validate_restore_lineage() {
  invalid_lineage="$(
    psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
      --set=ON_ERROR_STOP=1 --no-align --tuples-only --command="
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
               )"
  )"
  if [[ "$invalid_lineage" != "0" ]]; then
    echo "refusing shardschema restore lineage that conflicts with shard state" >&2
    return 1
  fi
}

validate_catalog_inventory() {
  validate_cluster_configuration
  validate_shard_inventory
  validate_restore_lineage
}

catalog_core_tables="$(
  psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
    --set=ON_ERROR_STOP=1 --no-align --tuples-only --command="
      SELECT
        pg_catalog.to_regclass('pgshard_catalog.cluster_configuration') IS NOT NULL,
        pg_catalog.to_regclass('pgshard_catalog.shards') IS NOT NULL,
        pg_catalog.to_regclass('pgshard_catalog.shard_restore_incarnations') IS NOT NULL"
)"
case "$catalog_core_tables" in
  "f|f|f") ;;
  "t|t|t") validate_catalog_inventory ;;
  *)
    echo "refusing a partial pre-existing shardschema catalog" >&2
    exit 1
    ;;
esac

psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
  --set=ON_ERROR_STOP=1 --file="$PGSHARD_SHARDSCHEMA_MIGRATION"

validate_catalog_inventory

missing_shards="$(
  psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
    --set=ON_ERROR_STOP=1 \
    --no-align --tuples-only --command="
      SELECT pg_catalog.count(*)
        FROM pg_catalog.generate_series(0, $PGSHARD_SHARD_COUNT::bigint - 1) AS expected(shard_number)
        LEFT JOIN pgshard_catalog.shards AS shards
          ON shards.shard_id::text = 'shard-' || pg_catalog.lpad(expected.shard_number::text, 4, '0')
         AND shards.shard_number = expected.shard_number
       WHERE shards.shard_id IS NULL"
)"
if [[ "$missing_shards" != "0" ]]; then
  psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
    --set=ON_ERROR_STOP=1 <<PGSHARD_SHARD_INVENTORY
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
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
COMMIT;
PGSHARD_SHARD_INVENTORY
fi

validate_catalog_inventory

pg_ctl -D "$final" -w -t 45 stop -m fast
trap - EXIT
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
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		objects = append(objects, shardService(cluster, shard), postgresqlNetworkPolicy(cluster, shard))
		if cluster.Spec.MembersPerShard == 1 {
			bootstrap := bootstraps[shard]
			objects = append(objects,
				postgresqlPrimaryStatefulSet(cluster, shard, images.PostgreSQL, images.PostgreSQLBootstrap, bootstrap.SecretName, bootstrap.PVCName, postgresqlConfigName, postgresqlHash),
				postgresqlPrimaryDisruptionBudget(cluster, shard),
			)
		}
	}

	objects = append(objects,
		etcdStatefulSet(cluster, images.Etcd),
		orchestratorDeployment(cluster, images.Orchestrator, topologyHash),
		poolerDeployment(cluster, images.Pooler, topologyHash),
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
	return &corev1.Service{
		ObjectMeta: ownedMeta(cluster, cluster.Name+"-"+mode, "pooler", template.Annotations),
		Spec: corev1.ServiceSpec{
			Type:     template.Type,
			Selector: componentSelector(cluster, "pooler"),
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

// PostgreSQLAuthSecretPrefix is the readable portion of a randomly named
// credential. The controller appends cryptographic randomness and records the
// resulting name and API UID before any workload can reference it.
func PostgreSQLAuthSecretPrefix(cluster string, shard int32) string {
	return shardName(cluster, shard) + "-auth-"
}

// PostgreSQLPrimaryStatefulSetName returns the deterministic singleton primary
// workload name for one shard.
func PostgreSQLPrimaryStatefulSetName(cluster string, shard int32) string {
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

// PostgreSQLPrimaryDataPVCPrefix is the readable portion of a randomly named,
// pre-created data volume. Workloads only reference a name and UID checkpointed
// in PgShardCluster status.
func PostgreSQLPrimaryDataPVCPrefix(cluster string, shard int32) string {
	return shardName(cluster, shard) + "-primary-data-"
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

// PostgreSQLPrimaryDataPVC returns the standalone data volume for a singleton
// primary. Size and storage class come from the checkpointed provisioning
// contract. Every create is controlled by the exact detached credential Secret
// UID. The controller adds its data-protection finalizer only after the API UID
// is checkpointed, then detaches the live PVC and anchors the Secret tombstone
// to it. Delayed create requests retain this initial owner and no finalizer, so
// Kubernetes can garbage-collect them after the tombstone is deleted.
func PostgreSQLPrimaryDataPVC(cluster *pgshardv1alpha1.PgShardCluster, shard int32, name string, storageSize resource.Quantity, storageClassName *string, fenceName string, fenceUID types.UID) *corev1.PersistentVolumeClaim {
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

func postgresqlPrimaryStatefulSet(cluster *pgshardv1alpha1.PgShardCluster, shard int32, image, bootstrapImage, secretName, pvcName, configurationName, configurationHash string) *appsv1.StatefulSet {
	const (
		postgresUID = int64(999)
		replicas    = int32(1)
	)
	name := PostgreSQLPrimaryStatefulSetName(cluster.Name, shard)
	selector := componentSelector(cluster, "postgresql")
	selector[ShardLabel] = shardLabel(shard)
	selector[RoleLabel] = "primary"
	selector[MemberLabel] = "0000"
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
	readinessProbeCommand := []string{"pg_isready", "--quiet", "--host=127.0.0.1", "--port=5432", "--username=postgres"}
	bootstrapPullPolicy := imagePullPolicy(bootstrapImage)
	if bootstrapImage == developmentPostgreSQLBootstrapImage {
		bootstrapPullPolicy = corev1.PullNever
	}
	postgres := corev1.Container{
		Name:            "postgresql",
		Image:           image,
		ImagePullPolicy: imagePullPolicy(image),
		Args:            []string{"-c", "config_file=/etc/pgshard/postgresql/primary-0000.conf"},
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
	automount := false
	enableServiceLinks := false
	podAnnotations := map[string]string{
		ConfigHashAnnotation:              configurationHash,
		PostgreSQLPodClusterUIDAnnotation: string(cluster.UID),
	}
	if shard == 0 {
		podAnnotations[shardschemaMigrationHashAnnotation] = shardschemaMigrationSHA256
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
					Volumes: []corev1.Volume{
						{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName}}},
						{Name: "runtime", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: ptr(resource.MustParse("64Mi"))}}},
						{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: ptr(resource.MustParse("64Mi"))}}},
						{Name: "postgresql-config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: configurationName}}}},
						{Name: "bootstrap-secret", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: secretName, DefaultMode: ptr(int32(0o440))}}},
					},
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
		ObjectMeta: ownedMeta(cluster, PostgreSQLPrimaryStatefulSetName(cluster.Name, shard), "postgresql", nil),
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

func etcdStatefulSet(cluster *pgshardv1alpha1.PgShardCluster, image string) *appsv1.StatefulSet {
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
				Spec: securePodSpec(selector, []corev1.Container{{
					Name:            "etcd",
					Image:           image,
					ImagePullPolicy: imagePullPolicy(image),
					Command:         []string{etcdExecutable},
					Args: []string{
						"--name=$(POD_NAME)",
						"--data-dir=/var/lib/etcd",
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
				}}),
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
		{Name: "PGSHARD_ORCH_ID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}},
		{Name: "PGSHARD_ETCD_ENDPOINTS", Value: etcdEndpoints(cluster)},
	}
	if cluster.Spec.Observability.OpenTelemetryEndpoint != "" {
		env = append(env, corev1.EnvVar{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: cluster.Spec.Observability.OpenTelemetryEndpoint})
	}
	deployment := &appsv1.Deployment{
		ObjectMeta: ownedMeta(cluster, cluster.Name+OrchestratorSuffix, "orchestrator", nil),
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: selector},
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RollingUpdateDeploymentStrategyType, RollingUpdate: &appsv1.RollingUpdateDeployment{MaxUnavailable: intOrStringPtr(intstr.FromInt32(1)), MaxSurge: intOrStringPtr(intstr.FromInt32(1))}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: selector, Annotations: map[string]string{ConfigHashAnnotation: hash}},
				Spec: securePodSpec(selector, []corev1.Container{{
					Name:            "orchestrator",
					Image:           image,
					ImagePullPolicy: imagePullPolicy(image),
					Env:             env,
					Ports:           []corev1.ContainerPort{{Name: "http", ContainerPort: HTTPPort, Protocol: corev1.ProtocolTCP}},
					Resources:       resources("100m", "128Mi", "1", "512Mi"),
					ReadinessProbe:  httpReadinessProbe("/readyz", "http"),
					LivenessProbe:   httpLivenessProbe("/healthz", "http"),
					VolumeMounts:    []corev1.VolumeMount{{Name: "topology", MountPath: "/etc/pgshard", ReadOnly: true}},
				}}),
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

func poolerDeployment(cluster *pgshardv1alpha1.PgShardCluster, image, hash string) *appsv1.Deployment {
	replicas := poolerReplicas(cluster)
	var desiredReplicas *int32
	if cluster.Spec.Pooler.Scaling.Mode == pgshardv1alpha1.ScalingFixed {
		desiredReplicas = ptr(replicas)
	}
	selector := componentSelector(cluster, "pooler")
	env := []corev1.EnvVar{
		{Name: "PGSHARD_CLUSTER_ID", Value: cluster.Name},
		{Name: "PGSHARD_TOPOLOGY_FILE", Value: "/etc/pgshard/topology/cluster.json"},
		{Name: "PGSHARD_HTTP_BIND", Value: "0.0.0.0:8080"},
		{Name: "PGSHARD_RW_BIND", Value: "0.0.0.0:5432"},
		{Name: "PGSHARD_RO_BIND", Value: "0.0.0.0:5433"},
		{Name: "PGSHARD_R_BIND", Value: "0.0.0.0:5434"},
		{Name: "PGSHARD_CATALOG_MODE", Value: "bootstrap-unavailable"},
		{Name: "PGSHARD_ETCD_ENDPOINTS", Value: etcdEndpoints(cluster)},
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
		VolumeMounts:   []corev1.VolumeMount{{Name: "topology", MountPath: "/etc/pgshard/topology", ReadOnly: true}},
	}})
	podSpec.TerminationGracePeriodSeconds = ptr(int64(60))
	podSpec.Volumes = []corev1.Volume{{Name: "topology", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Name + TopologyConfigSuffix}}}}}
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
		containers[index].SecurityContext = &corev1.SecurityContext{
			AllowPrivilegeEscalation: ptr(false),
			ReadOnlyRootFilesystem:   ptr(true),
			RunAsNonRoot:             &runAsNonRoot,
			RunAsUser:                &runAsUser,
			RunAsGroup:               &runAsGroup,
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		}
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
