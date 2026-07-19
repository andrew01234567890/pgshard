package resources

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
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

func TestCatalogMaterialSHA256MatchesRustContract(t *testing.T) {
	t.Parallel()
	if got, want := PostgreSQLReplicationMaterialSHA256([]byte("password")), "f28e708e623164f153012f8f21e13d4bbd3ad2de150d3181b69316275bb49f7e"; got != want {
		t.Fatalf("replication material SHA-256 = %q, want shared Rust vector %q", got, want)
	}
	if got, want := CatalogClientMaterialSHA256(nil, []byte("catalog-ca")), "f25d89531a7aa9937005eb56aab838662145cadff1315196229e0cd334ece559"; got != want {
		t.Fatalf("client material SHA-256 = %q, want shared Rust vector %q", got, want)
	}
	if got, want := CatalogServerMaterialSHA256([]byte("catalog-certificate"), nil), "219f722b1a1d47cb6b569c6c6bc6e9dfe5131f6d4e8fc507bcf93c106df8409d"; got != want {
		t.Fatalf("server material SHA-256 = %q, want shared Rust vector %q", got, want)
	}
}

func TestPostgreSQLBootstrapScriptHasValidBashSyntax(t *testing.T) {
	t.Parallel()
	command := exec.Command("bash", "-n")
	command.Stdin = strings.NewReader(postgresqlBootstrapScript)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("PostgreSQL bootstrap script syntax: %v\n%s", err, output)
	}
}

func TestPlanIsDeterministicAndWiresGeneratedConfiguration(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	cluster.Spec.PostgreSQL.Parameters = map[string]string{
		"log_statement":             "ddl",
		"default_statistics_target": "200",
	}

	first, err := Plan(cluster, DefaultImages())
	if err != nil {
		t.Fatal(err)
	}
	second, err := Plan(cluster.DeepCopy(), DefaultImages())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("the same cluster produced different plans")
	}

	postgresConfig := postgresqlConfigMap(t, first, cluster.Name)
	if postgresConfig.Immutable == nil || !*postgresConfig.Immutable {
		t.Fatal("PostgreSQL configuration is not immutable")
	}
	contents := postgresConfig.Data["postgresql.conf"]
	if !strings.Contains(contents, "shared_buffers = 512MB\n") || !strings.Contains(contents, "fsync = on\n") || !strings.Contains(contents, "listen_addresses = '*'\n") || !strings.Contains(contents, "max_replication_slots = 20\n") {
		t.Fatalf("resource-derived settings were not rendered:\n%s", contents)
	}
	if strings.Index(contents, "default_statistics_target") > strings.Index(contents, "log_statement") {
		t.Fatal("PostgreSQL parameters are not sorted")
	}
	if len(postgresConfig.Data) != 9 {
		t.Fatalf("PostgreSQL configuration documents = %#v", postgresConfig.Data)
	}
	databaseGenesis := postgresConfig.Data[databaseGenesisKey]
	analytics := "install_database_genesis('analytics'::pgshard_catalog.sql_identifier, ARRAY[0,1]::bigint[])"
	app := "install_database_genesis('app'::pgshard_catalog.sql_identifier, ARRAY[0,1]::bigint[])"
	if !strings.Contains(databaseGenesis, analytics) || !strings.Contains(databaseGenesis, app) || strings.Index(databaseGenesis, analytics) > strings.Index(databaseGenesis, app) {
		t.Fatalf("database genesis is not canonical:\n%s", databaseGenesis)
	}
	if !strings.Contains(databaseGenesis, "\\i "+databaseTopologyPreflightPath) {
		t.Fatalf("database genesis does not repeat topology preflight under its transaction lock:\n%s", databaseGenesis)
	}
	databasePreflight := postgresConfig.Data[databaseTopologyPreflightKey]
	analyticsPreflight := "('analytics'::text, ARRAY[0,1]::bigint[])"
	appPreflight := "('app'::text, ARRAY[0,1]::bigint[])"
	if !strings.Contains(databasePreflight, analyticsPreflight) || !strings.Contains(databasePreflight, appPreflight) || strings.Index(databasePreflight, analyticsPreflight) > strings.Index(databasePreflight, appPreflight) {
		t.Fatalf("database topology preflight is not canonical:\n%s", databasePreflight)
	}
	if !strings.Contains(databasePreflight, "actual_databases AS MATERIALIZED") ||
		!strings.Contains(databasePreflight, "PGSHARD_ALLOW_EMPTY_DATABASE_TOPOLOGY") ||
		!strings.Contains(databasePreflight, "NOT pg_catalog.current_setting('pgshard.bootstrap_allow_empty_database_topology')::boolean") ||
		!strings.Contains(databasePreflight, "WHERE databases.state <> 'retired'\n     LIMIT 3") ||
		!strings.Contains(databasePreflight, "actual_range_sample AS MATERIALIZED") ||
		!strings.Contains(databasePreflight, "LEFT JOIN active_epoch_counts AS active_counts ON active_counts.logical_database_id = databases.logical_database_id\n     LIMIT 5") ||
		!strings.Contains(databasePreflight, "$pgshard_legacy_topology$") ||
		!strings.Contains(databasePreflight, "$pgshard_placement_topology$") {
		t.Fatalf("database topology preflight is not bounded by declared topology:\n%s", databasePreflight)
	}
	primary := postgresConfig.Data["primary-0000.conf"]
	if !strings.Contains(primary, "synchronized_standby_slots = 'pgshard_member_0001,pgshard_member_0002'\n") || !strings.Contains(primary, "synchronous_standby_names = 'ANY 1 (pgshard_member_0001,pgshard_member_0002)'\n") {
		t.Fatalf("primary role settings were not rendered:\n%s", primary)
	}
	promotedPrimary := postgresConfig.Data["primary-0001.conf"]
	if !strings.Contains(promotedPrimary, "synchronized_standby_slots = 'pgshard_member_0000,pgshard_member_0002'\n") || strings.Contains(promotedPrimary, "pgshard_member_0001") {
		t.Fatalf("promoted primary did not exclude itself:\n%s", promotedPrimary)
	}
	standby := postgresConfig.Data["standby-0001.conf"]
	for _, expected := range []string{
		"hot_standby_feedback = on\n",
		"primary_slot_name = 'pgshard_member_0001'\n",
		"sync_replication_slots = on\n",
		"wal_receiver_status_interval = 1s\n",
	} {
		if !strings.Contains(standby, expected) {
			t.Fatalf("standby role setting %q was not rendered:\n%s", expected, standby)
		}
	}

	pooler := object[*appsv1.Deployment](t, first, "demo-pooler")
	if len(pooler.Spec.Template.Spec.Volumes) != 1 || pooler.Spec.Template.Spec.Volumes[0].Name != "topology" {
		t.Fatalf("pooler volumes = %#v", pooler.Spec.Template.Spec.Volumes)
	}
	if pooler.Spec.Template.Annotations[ConfigHashAnnotation] == "" {
		t.Fatal("pooler does not roll when topology configuration changes")
	}
	for _, suffix := range []string{"rw", "ro", "r"} {
		service := object[*corev1.Service](t, first, "demo-"+suffix)
		if service.Spec.Ports[0].Port != PostgreSQLPort || service.Spec.Ports[0].TargetPort.StrVal != "pooler-"+suffix {
			t.Fatalf("%s service port = %#v", suffix, service.Spec.Ports[0])
		}
		if suffix == "rw" && !reflect.DeepEqual(service.Spec.Selector, componentSelector(cluster, "pooler")) {
			t.Fatalf("read-write Service selector = %#v", service.Spec.Selector)
		}
		if suffix != "rw" && service.Spec.Selector != nil {
			t.Fatalf("unsupported %s Service unexpectedly selects ready poolers: %#v", suffix, service.Spec.Selector)
		}
		assertOwned(t, service, cluster)
	}
	poolerControl := object[*corev1.Service](t, first, "demo-pooler")
	if poolerControl.Spec.Type != corev1.ServiceTypeClusterIP || !poolerControl.Spec.PublishNotReadyAddresses || poolerControl.Spec.Ports[0].Port != HTTPPort || poolerControl.Spec.Ports[0].TargetPort.StrVal != "http" {
		t.Fatalf("pooler control service = %#v", poolerControl.Spec)
	}

	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		service := object[*corev1.Service](t, first, shardName(cluster.Name, shard))
		if service.Spec.ClusterIP != corev1.ClusterIPNone || !service.Spec.PublishNotReadyAddresses {
			t.Fatalf("shard service is not headless: %#v", service.Spec)
		}
	}
	for _, item := range first {
		if statefulSet, ok := item.(*appsv1.StatefulSet); ok && statefulSet.Labels[ComponentLabel] == "postgresql" {
			t.Fatal("planner must not create PostgreSQL Pods before safe lifecycle and HA exist")
		}
		assertOwned(t, item, cluster)
	}
}

func TestMaximumValidClusterFitsKubernetesConfigMaps(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	cluster.Name = strings.Repeat("c", pgshardv1alpha1.MaximumClusterNameLength)
	cluster.Namespace = strings.Repeat("n", 63)
	cluster.Spec.Shards = pgshardv1alpha1.MaximumShards
	cluster.Spec.Databases = make([]pgshardv1alpha1.DatabaseTemplate, pgshardv1alpha1.MaximumDatabases)
	for index := range cluster.Spec.Databases {
		cluster.Spec.Databases[index] = pgshardv1alpha1.DatabaseTemplate{
			Name:   fmt.Sprintf("db-%04d-%s", index, strings.Repeat("x", 55)),
			Shards: pgshardv1alpha1.MaximumShards,
		}
	}
	maximumEndpoint := func(host string) string {
		prefix := "https://" + host + "/"
		return prefix + strings.Repeat("x", pgshardv1alpha1.MaximumEndpointLength-len(prefix))
	}
	cluster.Spec.Backup.Repository = pgshardv1alpha1.BackupRepository{
		Type: pgshardv1alpha1.RepositoryS3,
		S3: &pgshardv1alpha1.S3Repository{
			Bucket:   strings.Repeat("b", pgshardv1alpha1.MaximumS3BucketLength),
			Endpoint: maximumEndpoint("minio.example.com"),
			Region:   strings.Repeat("r", pgshardv1alpha1.MaximumS3RegionLength),
			Prefix:   strings.Repeat("p", pgshardv1alpha1.MaximumS3PrefixLength),
			CredentialsSecretRef: corev1.LocalObjectReference{
				Name: strings.Repeat("s", 63) + "." + strings.Repeat("s", 63) + "." + strings.Repeat("s", 63) + "." + strings.Repeat("s", 61),
			},
		},
	}
	cluster.Spec.Observability.OpenTelemetryEndpoint = maximumEndpoint("collector.example.com")
	if err := pgshardv1alpha1.ValidateClusterForReconciliation(cluster); err != nil {
		t.Fatalf("maximum bounded cluster is not valid: %v", err)
	}

	plan, err := Plan(cluster, DefaultImages())
	if err != nil {
		t.Fatal(err)
	}
	objects := []*corev1.ConfigMap{
		postgresqlConfigMap(t, plan, cluster.Name),
		object[*corev1.ConfigMap](t, plan, cluster.Name+TopologyConfigSuffix),
	}
	for _, object := range objects {
		encoded, err := json.Marshal(object)
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) >= 1024*1024 {
			t.Fatalf("maximum valid ConfigMap %s serializes to %d bytes", object.Name, len(encoded))
		}
	}
}

func TestTopologyDocumentKeepsIndependentDatabasePlacements(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	cluster.Spec.Shards = 8
	cluster.Spec.Databases = []pgshardv1alpha1.DatabaseTemplate{
		{Name: "b-dedicated", Shards: 3, Cells: []int32{5, 6, 7}},
		{Name: "a", Shards: 5, Cells: []int32{0, 1, 2, 3, 4}},
		{Name: "b-shared", Shards: 3, Cells: []int32{0, 1, 2}},
	}
	plan, err := Plan(cluster, DefaultImages())
	if err != nil {
		t.Fatal(err)
	}
	topology := object[*corev1.ConfigMap](t, plan, cluster.Name+TopologyConfigSuffix)
	var document topologyDocument
	if err := json.Unmarshal([]byte(topology.Data["cluster.json"]), &document); err != nil {
		t.Fatal(err)
	}
	want := []topologyDatabase{
		{Name: "a", Shards: 5, Cells: []int32{0, 1, 2, 3, 4}},
		{Name: "b-dedicated", Shards: 3, Cells: []int32{5, 6, 7}},
		{Name: "b-shared", Shards: 3, Cells: []int32{0, 1, 2}},
	}
	if !reflect.DeepEqual(document.Databases, want) {
		t.Fatalf("database topology document = %#v, want %#v", document.Databases, want)
	}
}

func TestDatabaseGenesisSQLQuotesIdentifiersAsData(t *testing.T) {
	t.Parallel()
	if got, want := postgresqlStringLiteral("customer's-db"), "'customer''s-db'"; got != want {
		t.Fatalf("PostgreSQL string literal = %q, want %q", got, want)
	}
}

func TestConfigMapDataHashCoversNamesAndContentsDeterministically(t *testing.T) {
	t.Parallel()
	first := map[string]string{
		"postgresql.conf":   "wal_level = logical\n",
		"standby-0001.conf": "hot_standby_feedback = on\n",
	}
	second := map[string]string{
		"standby-0001.conf": "hot_standby_feedback = on\n",
		"postgresql.conf":   "wal_level = logical\n",
	}
	if configMapDataHash(first) != configMapDataHash(second) {
		t.Fatal("configuration hash depends on map insertion order")
	}
	second["standby-0001.conf"] = "hot_standby_feedback = off\n"
	if configMapDataHash(first) == configMapDataHash(second) {
		t.Fatal("configuration hash ignored role-profile content")
	}
	delete(second, "standby-0001.conf")
	second["standby-0002.conf"] = "hot_standby_feedback = on\n"
	if configMapDataHash(first) == configMapDataHash(second) {
		t.Fatal("configuration hash ignored role-profile name")
	}
}

func TestShardschemaMigrationHashMatchesCanonicalSource(t *testing.T) {
	t.Parallel()
	contents, err := os.ReadFile(filepath.Join("..", "..", "..", "crates", "pgshard-catalog", "migrations", "0001_shardschema.sql"))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(contents)
	if got := hex.EncodeToString(digest[:]); got != shardschemaMigrationSHA256 {
		t.Fatalf("shardschema migration digest = %s, want %s", got, shardschemaMigrationSHA256)
	}
}

func TestPostgreSQLConfigurationAndResourceLimitRollTogether(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	cluster.Status.PostgreSQLBootstraps = testPostgreSQLBootstraps(cluster)
	before, err := Plan(cluster, singleMemberImages())
	if err != nil {
		t.Fatal(err)
	}
	beforeConfiguration := postgresqlConfigMap(t, before, cluster.Name)
	beforeStatefulSet := object[*appsv1.StatefulSet](t, before, PostgreSQLShardStatefulSetName(cluster.Name, 0))
	beforePooler := object[*appsv1.Deployment](t, before, cluster.Name+PoolerSuffix)

	cluster.Spec.PostgreSQL.Resources.Requests[corev1.ResourceMemory] = resource.MustParse("3Gi")
	cluster.Spec.PostgreSQL.Resources.Limits[corev1.ResourceMemory] = resource.MustParse("6Gi")
	after, err := Plan(cluster, singleMemberImages())
	if err != nil {
		t.Fatal(err)
	}
	afterConfiguration := postgresqlConfigMap(t, after, cluster.Name)
	afterStatefulSet := object[*appsv1.StatefulSet](t, after, PostgreSQLShardStatefulSetName(cluster.Name, 0))
	afterPooler := object[*appsv1.Deployment](t, after, cluster.Name+PoolerSuffix)
	if beforeConfiguration.Name == afterConfiguration.Name {
		t.Fatal("resource-derived PostgreSQL configuration name did not change")
	}
	if got := configMapVolumeName(t, beforeStatefulSet.Spec.Template.Spec.Volumes, "postgresql-config"); got != beforeConfiguration.Name {
		t.Fatalf("old StatefulSet configuration = %q, want %q", got, beforeConfiguration.Name)
	}
	if got := configMapVolumeName(t, afterStatefulSet.Spec.Template.Spec.Volumes, "postgresql-config"); got != afterConfiguration.Name {
		t.Fatalf("new StatefulSet configuration = %q, want %q", got, afterConfiguration.Name)
	}
	if got := afterStatefulSet.Spec.Template.Spec.Containers[0].Resources.Limits.Memory(); got == nil || got.Cmp(resource.MustParse("6Gi")) != 0 {
		t.Fatalf("new StatefulSet memory limit = %v", got)
	}
	if !reflect.DeepEqual(beforePooler.Spec, afterPooler.Spec) {
		t.Fatal("PostgreSQL-only configuration change rolled the pooler")
	}
}

func TestSingleMemberPlanCreatesPostgreSQL18Primaries(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	cluster.Status.PostgreSQLBootstraps = testPostgreSQLBootstraps(cluster)
	plan, err := Plan(cluster, singleMemberImages())
	if err != nil {
		t.Fatal(err)
	}

	configuration := postgresqlConfigMap(t, plan, cluster.Name)
	configurationHash := configMapDataHash(configuration.Data)
	primaryConfiguration := configuration.Data["primary-0000.conf"]
	if !strings.HasPrefix(primaryConfiguration, "include = '/etc/pgshard/postgresql/postgresql.conf'\n") ||
		!strings.Contains(primaryConfiguration, "synchronized_standby_slots = ''\n") ||
		!strings.Contains(primaryConfiguration, "synchronous_standby_names = ''\n") {
		t.Fatalf("single-member primary configuration = %q", primaryConfiguration)
	}
	catalogService := object[*corev1.Service](t, plan, CatalogServiceName(cluster.Name))
	_, selectsFixedMember := catalogService.Spec.Selector[MemberLabel]
	if catalogService.Spec.PublishNotReadyAddresses || catalogService.Spec.Selector[ShardLabel] != "0000" || catalogService.Spec.Selector[RoleLabel] != "primary" || selectsFixedMember || len(catalogService.Spec.Ports) != 1 || catalogService.Spec.Ports[0].Port != PostgreSQLPort {
		t.Fatalf("ready-only shardschema Service = %#v", catalogService.Spec)
	}
	pooler := object[*appsv1.Deployment](t, plan, cluster.Name+PoolerSuffix)
	poolerContainer := pooler.Spec.Template.Spec.Containers[0]
	if envValue(poolerContainer.Env, "PGSHARD_CATALOG_MODE") != "operator-tls" ||
		envValue(poolerContainer.Env, "PGSHARD_SHARDSCHEMA_HOST") != "demo-shardschema.database.svc" ||
		envValue(poolerContainer.Env, "PGSHARD_SHARDSCHEMA_PASSWORD_FILE") != "/etc/pgshard/catalog/catalog-password" ||
		envValue(poolerContainer.Env, "PGSHARD_SHARDSCHEMA_CA_FILE") != "/etc/pgshard/catalog/ca.crt" ||
		envValue(poolerContainer.Env, "PGSHARD_SHARDSCHEMA_CLIENT_SHA256") != cluster.Status.CatalogAccess.ClientSHA256 ||
		envValue(poolerContainer.Env, "PGSHARD_RW_BACKEND_HOST") != "demo-shardschema.database.svc" {
		t.Fatalf("pooler catalog environment = %#v", poolerContainer.Env)
	}
	poolerCatalogVolume := volumeByName(t, pooler.Spec.Template.Spec.Volumes, "catalog-client")
	if poolerCatalogVolume.Secret == nil || poolerCatalogVolume.Secret.SecretName != cluster.Status.CatalogAccess.SecretName || !reflect.DeepEqual(secretItemKeys(poolerCatalogVolume.Secret.Items), []string{CatalogPasswordKey, CatalogCACertificateKey}) {
		t.Fatalf("pooler catalog Secret projection = %#v", poolerCatalogVolume.Secret)
	}
	if !containsVolumeMount(poolerContainer.VolumeMounts, "catalog-client", true) {
		t.Fatalf("pooler catalog mount = %#v", poolerContainer.VolumeMounts)
	}

	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		name := PostgreSQLShardStatefulSetName(cluster.Name, shard)
		statefulSet := object[*appsv1.StatefulSet](t, plan, name)
		if strings.Contains(statefulSet.Name, "primary") || strings.Contains(statefulSet.Name, "replica") {
			t.Fatalf("PostgreSQL StatefulSet identity contains a mutable role: %q", statefulSet.Name)
		}
		if statefulSet.Spec.Replicas == nil || *statefulSet.Spec.Replicas != 1 || statefulSet.Spec.ServiceName != shardName(cluster.Name, shard) || statefulSet.Spec.UpdateStrategy.Type != appsv1.OnDeleteStatefulSetStrategyType {
			t.Fatalf("PostgreSQL StatefulSet identity = %#v", statefulSet.Spec)
		}
		if _, selectsMutableRole := statefulSet.Spec.Selector.MatchLabels[RoleLabel]; selectsMutableRole || statefulSet.Spec.Template.Labels[RoleLabel] != "primary" || statefulSet.Spec.Selector.MatchLabels[MemberLabel] != "0000" {
			t.Fatalf("PostgreSQL StatefulSet selector is not stable across promotion: selector=%#v labels=%#v", statefulSet.Spec.Selector.MatchLabels, statefulSet.Spec.Template.Labels)
		}
		if statefulSet.Spec.Template.Labels[ManagedByLabel] != ManagedByValue || statefulSet.Spec.Template.Labels[ShardLabel] != shardLabel(shard) || statefulSet.Spec.Template.Labels[RoleLabel] != "primary" || statefulSet.Spec.Template.Labels[MemberLabel] != "0000" {
			t.Fatalf("PostgreSQL labels = %#v", statefulSet.Spec.Template.Labels)
		}
		if statefulSet.Spec.Template.Annotations[PostgreSQLPodClusterUIDAnnotation] != string(cluster.UID) || !reflect.DeepEqual(statefulSet.Spec.Template.Finalizers, []string{PostgreSQLPodTerminationFinalizer}) {
			t.Fatalf("PostgreSQL termination fence = %#v", statefulSet.Spec.Template.ObjectMeta)
		}
		if statefulSet.Annotations[PostgreSQLRuntimeAnnotation] != string(PostgreSQLRuntimeDirect) || statefulSet.Spec.Template.Annotations[PostgreSQLRuntimeAnnotation] != string(PostgreSQLRuntimeDirect) {
			t.Fatalf("direct PostgreSQL runtime contract = StatefulSet %#v Pod %#v", statefulSet.Annotations, statefulSet.Spec.Template.Annotations)
		}
		if observed, err := ObservePostgreSQLRuntime(statefulSet.Spec.Template.Annotations, statefulSet.Spec.Template.Spec); err != nil || observed != PostgreSQLRuntimeDirect {
			t.Fatalf("observe direct PostgreSQL runtime = %q, %v", observed, err)
		}
		if got := statefulSet.Spec.Template.Annotations[shardschemaMigrationHashAnnotation]; (shard == 0 && got != shardschemaMigrationSHA256) || (shard != 0 && got != "") {
			t.Fatalf("shardschema migration annotation for shard %d = %q", shard, got)
		}
		if len(statefulSet.Spec.VolumeClaimTemplates) != 0 {
			t.Fatalf("PostgreSQL data must use a pre-identified standalone PVC: %#v", statefulSet.Spec.VolumeClaimTemplates)
		}
		dataVolume := statefulSet.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim
		if dataVolume == nil || dataVolume.ClaimName != cluster.Status.PostgreSQLBootstraps[shard].PVCName {
			t.Fatalf("PostgreSQL data volume = %#v", dataVolume)
		}
		pod := statefulSet.Spec.Template.Spec
		if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken || pod.ServiceAccountName != "" || hasVolume(pod.Volumes, "kubernetes-api") || pod.NodeSelector[corev1.LabelOSStable] != "linux" || len(pod.InitContainers) != 1 || len(pod.Containers) != 1 {
			t.Fatalf("PostgreSQL Pod boundary = %#v", pod)
		}
		if pod.SecurityContext == nil || pod.SecurityContext.RunAsNonRoot == nil || !*pod.SecurityContext.RunAsNonRoot || pod.SecurityContext.RunAsUser == nil || *pod.SecurityContext.RunAsUser != 999 || pod.SecurityContext.FSGroup == nil || *pod.SecurityContext.FSGroup != 999 || pod.SecurityContext.FSGroupChangePolicy == nil || *pod.SecurityContext.FSGroupChangePolicy != corev1.FSGroupChangeOnRootMismatch {
			t.Fatalf("PostgreSQL Pod security = %#v", pod.SecurityContext)
		}
		postgres := pod.Containers[0]
		if postgres.Image != defaultPostgreSQLImage || postgres.ImagePullPolicy != corev1.PullIfNotPresent || postgres.SecurityContext == nil || postgres.SecurityContext.RunAsUser == nil || *postgres.SecurityContext.RunAsUser != 999 || postgres.SecurityContext.ReadOnlyRootFilesystem == nil || !*postgres.SecurityContext.ReadOnlyRootFilesystem {
			t.Fatalf("PostgreSQL container boundary = %#v", postgres)
		}
		if !containsString(postgres.Args, "config_file=/etc/pgshard/postgresql/primary-0000.conf") || !containsString(postgres.Args, "allow_alter_system=off") || postgres.StartupProbe != nil || postgres.ReadinessProbe == nil || postgres.LivenessProbe != nil {
			t.Fatalf("PostgreSQL startup contract = %#v", postgres)
		}
		for _, setting := range []string{"ssl=on", "ssl_cert_file=/etc/pgshard/catalog-tls/tls.crt", "ssl_key_file=/etc/pgshard/catalog-tls/tls.key", "ssl_min_protocol_version=TLSv1.3", "ssl_max_protocol_version=TLSv1.3"} {
			if containsString(postgres.Args, setting) != (shard == 0) {
				t.Fatalf("PostgreSQL shard %d TLS setting %q in args %#v", shard, setting, postgres.Args)
			}
		}
		readinessProbe := []string{"pg_isready", "--quiet", "--host=127.0.0.1", "--port=5432", "--username=postgres"}
		if !reflect.DeepEqual(postgres.ReadinessProbe.Exec.Command, readinessProbe) {
			t.Fatalf("PostgreSQL readiness probe = %#v", postgres.ReadinessProbe)
		}
		bootstrap := pod.InitContainers[0]
		if bootstrap.Name != "bootstrap-postgresql" || bootstrap.Image != developmentPostgreSQLBootstrapImage || bootstrap.ImagePullPolicy != corev1.PullNever || len(bootstrap.Command) != 3 || !strings.Contains(bootstrap.Command[2], "staging=\"$parent/.pgshard-init\"") || !strings.Contains(bootstrap.Command[2], "host all all all scram-sha-256") || !strings.Contains(bootstrap.Command[2], "cmp -s -- \"$marker\" \"$expected\"") || !strings.Contains(bootstrap.Command[2], "sync \"$staging/pg_hba.conf\" \"$staging/.pgshard-bootstrap-complete\" \"$staging\"") || !strings.Contains(bootstrap.Command[2], "sync \"$final\" \"$parent\" \"$volume_root\"") || !strings.Contains(bootstrap.Command[2], "transaction_timeout=120s") || strings.Contains(bootstrap.Command[2], "\nsync\n") || strings.Contains(bootstrap.Command[2], "sync -f") || !strings.Contains(bootstrap.Command[2], "cp -- \"$expected\" \"$staging/.pgshard-bootstrap-complete\"") || !strings.Contains(bootstrap.Command[2], "mv -- \"$staging\" \"$final\"") || !strings.Contains(bootstrap.Command[2], postgresqlBootstrapMarker) || !strings.Contains(bootstrap.Command[2], "config_file=/etc/pgshard/postgresql/primary-0000.conf") || !strings.Contains(bootstrap.Command[2], "listen_addresses=''") || !strings.Contains(bootstrap.Command[2], "validate_catalog_inventory") || !strings.Contains(bootstrap.Command[2], "INSERT INTO pgshard_catalog.shards") {
			t.Fatalf("PostgreSQL atomic bootstrap contract = %#v", bootstrap)
		}
		if got := strings.Count(bootstrap.Command[2], "sync \"$final\" \"$parent\" \"$volume_root\""); got != 3 {
			t.Fatalf("PostgreSQL final-data publication barriers = %d, want 3", got)
		}
		if envValue(bootstrap.Env, "PGSHARD_POSTGRESQL_CONFIG_SHA256") != configurationHash {
			t.Fatalf("PostgreSQL configuration digest environment = %#v", bootstrap.Env)
		}
		if statefulSet.Spec.Template.Annotations[ConfigHashAnnotation] != configurationHash {
			t.Fatalf("PostgreSQL configuration digest annotation = %#v", statefulSet.Spec.Template.Annotations)
		}
		sourceMounts := 0
		runtimeMounts := 0
		for _, mount := range bootstrap.VolumeMounts {
			switch mount.MountPath {
			case "/etc/pgshard/postgresql-source":
				sourceMounts++
				if mount.Name != "postgresql-config" || !mount.ReadOnly {
					t.Fatalf("PostgreSQL configuration source mount = %#v", mount)
				}
			case "/etc/pgshard/postgresql":
				runtimeMounts++
				if mount.Name != "postgresql-runtime-config" || mount.ReadOnly {
					t.Fatalf("PostgreSQL runtime configuration mount = %#v", mount)
				}
			}
		}
		if sourceMounts != 1 || runtimeMounts != 1 {
			t.Fatalf("PostgreSQL authenticated configuration mounts = source %d, runtime %d, want 1 each", sourceMounts, runtimeMounts)
		}
		if !strings.Contains(bootstrap.Command[2], "database_genesis="+databaseGenesisPath) || !strings.Contains(bootstrap.Command[2], "database_topology_preflight="+databaseTopologyPreflightPath) {
			t.Fatal("PostgreSQL bootstrap does not read copied database topology files")
		}
		if !containsVolumeMount(postgres.VolumeMounts, "postgresql-runtime-config", true) || containsVolumeMount(postgres.VolumeMounts, "postgresql-config", true) {
			t.Fatalf("PostgreSQL runtime configuration mounts = %#v", postgres.VolumeMounts)
		}
		configurationSource := volumeByName(t, pod.Volumes, "postgresql-config")
		if configurationSource.ConfigMap == nil || configurationSource.ConfigMap.Name != configuration.Name {
			t.Fatalf("PostgreSQL configuration source volume = %#v", configurationSource)
		}
		configurationRuntime := volumeByName(t, pod.Volumes, "postgresql-runtime-config")
		if configurationRuntime.EmptyDir == nil || configurationRuntime.EmptyDir.SizeLimit == nil || configurationRuntime.EmptyDir.SizeLimit.Cmp(resource.MustParse("2Mi")) != 0 {
			t.Fatalf("PostgreSQL runtime configuration volume = %#v", configurationRuntime)
		}
		if !strings.Contains(bootstrap.Command[2], "catalog_schema_fingerprint") ||
			!strings.Contains(bootstrap.Command[2], "ee17a64c8eec5e2e9a44f29d4764edac90680980f61df35bdb2284c01b57c4d9") ||
			!strings.Contains(bootstrap.Command[2], "2720fa78d0bc96c21311b1656eeaabbb3e745ea65fa9d1ea701ffb67cde1b1d9") ||
			!strings.Contains(bootstrap.Command[2], "ceec4ff5d633d28afacf1e93fbc2547591017e57f172dc3a8072814bb6d3867a") ||
			!strings.Contains(bootstrap.Command[2], "pg_catalog.pg_sequence") ||
			!strings.Contains(bootstrap.Command[2], "pg_catalog.pg_rewrite") ||
			!strings.Contains(bootstrap.Command[2], "internal-trigger|") ||
			!strings.Contains(bootstrap.Command[2], "SET SESSION search_path = pg_catalog") ||
			!strings.Contains(bootstrap.Command[2], "SET SESSION quote_all_identifiers = off") ||
			!strings.Contains(bootstrap.Command[2], "sequence_state.is_called") ||
			!strings.Contains(bootstrap.Command[2], "shards.shard_id = incarnations.shard_id") ||
			!strings.Contains(bootstrap.Command[2], "catalog_requires_initial_inventory") ||
			!strings.Contains(bootstrap.Command[2], "refusing active settings in restored postgresql.auto.conf") ||
			!strings.Contains(bootstrap.Command[2], "hba_file='$quarantine_hba'") ||
			!strings.Contains(bootstrap.Command[2], "shared_preload_libraries=") ||
			!strings.Contains(bootstrap.Command[2], "event_triggers=off") ||
			!strings.Contains(bootstrap.Command[2], "session_replication_role=origin") ||
			!strings.Contains(bootstrap.Command[2], "default_table_access_method=heap") ||
			!strings.Contains(bootstrap.Command[2], "initial shardschema inventory failed its transactional postcondition") ||
			!strings.Contains(bootstrap.Command[2], "count_missing_shards") ||
			!strings.Contains(bootstrap.Command[2], "validate_genesis_inventory_reachable") ||
			!strings.Contains(bootstrap.Command[2], "refusing shardschema inventory with missing configured shards") ||
			!strings.Contains(bootstrap.Command[2], "--file=\"$database_genesis\"") ||
			!strings.Contains(bootstrap.Command[2], "--file=\"$database_topology_preflight\"") ||
			!strings.Contains(bootstrap.Command[2], "database genesis topology is missing or not a regular file") ||
			!strings.Contains(bootstrap.Command[2], "database topology preflight is missing or not a regular file") ||
			!strings.Contains(bootstrap.Command[2], "CREATE ROLE pgshard_pooler_catalog") ||
			!strings.Contains(bootstrap.Command[2], "WITH ADMIN FALSE, INHERIT TRUE, SET FALSE") ||
			!strings.Contains(bootstrap.Command[2], "roles.rolpassword LIKE 'SCRAM-SHA-256\\$4096:%'") ||
			!strings.Contains(bootstrap.Command[2], "pgshard-scram-verifier") ||
			strings.Count(bootstrap.Command[2], "pgshard-catalog-material-digest client") != 1 ||
			strings.Count(bootstrap.Command[2], "pgshard-catalog-material-digest server") != 1 ||
			strings.Count(bootstrap.Command[2], "pgshard-catalog-material-digest replication") != 1 ||
			!strings.Contains(bootstrap.Command[2], "SET rolpassword = $1, rolcanlogin = true") ||
			strings.Contains(bootstrap.Command[2], "PASSWORD '$catalog_password'") ||
			!strings.Contains(bootstrap.Command[2], "PGPASSWORD=\"$catalog_password\"") ||
			!strings.Contains(bootstrap.Command[2], "hostnossl shardschema all all reject") ||
			!strings.Contains(bootstrap.Command[2], "hostssl shardschema pgshard_pooler_catalog all scram-sha-256") ||
			!strings.Contains(bootstrap.Command[2], "hostssl shardschema all all reject") ||
			!strings.Contains(bootstrap.Command[2], "host all pgshard_pooler_catalog all reject") ||
			!strings.Contains(bootstrap.Command[2], "local all pgshard_pooler_catalog reject") ||
			!strings.Contains(bootstrap.Command[2], "log_min_error_statement=panic") ||
			!strings.Contains(bootstrap.Command[2], "refusing shardschema material that differs from the checkpointed creation result") {
			t.Fatal("PostgreSQL bootstrap does not pin supported catalog shapes")
		}
		stopIndex := strings.LastIndex(bootstrap.Command[2], "pg_ctl -D \"$final\" -w -t 45 stop -m fast")
		intentRemovalIndex := strings.LastIndex(bootstrap.Command[2], "rm -- \"$catalog_genesis_intent\"")
		if stopIndex < 0 || intentRemovalIndex < 0 || stopIndex >= intentRemovalIndex {
			t.Fatal("catalog genesis intent is removed before clean PostgreSQL shutdown")
		}
		expectedHBAOrder := "'local all postgres trust' \\\n" +
			"  'local all pgshard_pooler_catalog reject' \\\n" +
			"  'local all all trust' \\\n" +
			"  'hostnossl shardschema all all reject' \\\n" +
			"  'hostssl shardschema pgshard_pooler_catalog all scram-sha-256' \\\n" +
			"  'hostssl shardschema all all reject' \\\n" +
			"  'host all pgshard_pooler_catalog all reject' \\\n" +
			"  'host all all all scram-sha-256'"
		if !strings.Contains(bootstrap.Command[2], expectedHBAOrder) {
			t.Fatal("catalog HBA rules are not ordered before the generic host grant")
		}
		expectedEnvironmentLength := 11
		if shard == 0 {
			expectedEnvironmentLength = 13
		}
		if len(bootstrap.Env) != expectedEnvironmentLength || bootstrap.Env[0].Name != "PGSHARD_CLUSTER_UID" || bootstrap.Env[0].Value != string(cluster.UID) || bootstrap.Env[1].Name != "PGSHARD_SHARD_ID" || bootstrap.Env[1].Value != shardLabel(shard) ||
			bootstrap.Env[2].Name != "PGSHARD_POSTGRESQL_MAJOR" || bootstrap.Env[2].Value != pgshardv1alpha1.PostgreSQLMajor18 ||
			bootstrap.Env[3].Name != "PGSHARD_SHARD_COUNT" || bootstrap.Env[3].Value != fmt.Sprintf("%d", cluster.Spec.Shards) ||
			bootstrap.Env[4].Name != "PGSHARD_MAXIMUM_SHARDS" || bootstrap.Env[4].Value != fmt.Sprintf("%d", pgshardv1alpha1.MaximumShards) ||
			bootstrap.Env[5].Name != "PGSHARD_BOOTSTRAP_SHARDSCHEMA" || bootstrap.Env[5].Value != fmt.Sprintf("%t", shard == 0) ||
			bootstrap.Env[6].Name != "PGSHARD_SHARDSCHEMA_MIGRATION" || bootstrap.Env[6].Value != shardschemaMigrationPath ||
			bootstrap.Env[7].Name != "PGSHARD_SHARDSCHEMA_MIGRATION_SHA256" || bootstrap.Env[7].Value != shardschemaMigrationSHA256 ||
			bootstrap.Env[8].Name != "PGSHARD_POSTGRESQL_CONFIG_SHA256" || bootstrap.Env[8].Value != configurationHash ||
			bootstrap.Env[9].Name != "PGSHARD_NODE_UID" || bootstrap.Env[9].ValueFrom == nil || bootstrap.Env[9].ValueFrom.FieldRef == nil || bootstrap.Env[9].ValueFrom.FieldRef.FieldPath != "metadata.annotations['pgshard.io/postgresql-node-uid']" ||
			bootstrap.Env[10].Name != "PGSHARD_NODE_BOOT_ID" || bootstrap.Env[10].ValueFrom == nil || bootstrap.Env[10].ValueFrom.FieldRef == nil || bootstrap.Env[10].ValueFrom.FieldRef.FieldPath != "metadata.annotations['pgshard.io/postgresql-node-boot-id']" {
			t.Fatalf("PostgreSQL bootstrap identity = %#v", bootstrap.Env)
		}
		if shard == 0 && (bootstrap.Env[11].Name != "PGSHARD_CATALOG_CLIENT_SHA256" || bootstrap.Env[11].Value != cluster.Status.CatalogAccess.ClientSHA256 || bootstrap.Env[12].Name != "PGSHARD_CATALOG_SERVER_SHA256" || bootstrap.Env[12].Value != cluster.Status.CatalogAccess.ServerSHA256) {
			t.Fatalf("PostgreSQL catalog material checkpoint = %#v", bootstrap.Env)
		}
		if configMapVolumeName(t, pod.Volumes, "postgresql-config") != configuration.Name || !containsVolumeMount(bootstrap.VolumeMounts, "postgresql-config", true) {
			t.Fatalf("PostgreSQL bootstrap configuration mount = %#v", bootstrap.VolumeMounts)
		}
		if bootstrap.SecurityContext == nil || bootstrap.SecurityContext.ReadOnlyRootFilesystem == nil || !*bootstrap.SecurityContext.ReadOnlyRootFilesystem || bootstrap.Resources.Limits.Memory() == nil {
			t.Fatalf("PostgreSQL bootstrap security/resources = %#v", bootstrap)
		}
		passwordReferences := 0
		for _, variable := range postgres.Env {
			if variable.Name == "POSTGRES_PASSWORD" {
				passwordReferences++
			}
			if variable.ValueFrom != nil {
				t.Fatalf("running PostgreSQL received a Secret-backed environment variable: %#v", variable)
			}
		}
		if passwordReferences != 0 || len(postgres.Env) != 1 || postgres.Env[0].Name != "PGDATA" {
			t.Fatalf("PostgreSQL password reference count = %d", passwordReferences)
		}
		for _, mount := range postgres.VolumeMounts {
			if mount.Name == "bootstrap-secret" {
				t.Fatalf("running PostgreSQL mounts the bootstrap Secret: %#v", postgres.VolumeMounts)
			}
		}
		if shard == 0 {
			serverTLS := volumeByName(t, pod.Volumes, "catalog-server-tls")
			if serverTLS.Secret == nil || serverTLS.Secret.SecretName != cluster.Status.CatalogAccess.SecretName || !reflect.DeepEqual(secretItemKeys(serverTLS.Secret.Items), []string{CatalogTLSCertificateKey, CatalogTLSPrivateKeyKey}) {
				t.Fatalf("PostgreSQL catalog TLS projection = %#v", serverTLS.Secret)
			}
			catalogAuth := volumeByName(t, pod.Volumes, "catalog-bootstrap-auth")
			if catalogAuth.Secret == nil || catalogAuth.Secret.SecretName != cluster.Status.CatalogAccess.SecretName || !reflect.DeepEqual(secretItemKeys(catalogAuth.Secret.Items), []string{CatalogPasswordKey, CatalogCACertificateKey}) {
				t.Fatalf("catalog bootstrap password projection = %#v", catalogAuth.Secret)
			}
			if !containsVolumeMount(postgres.VolumeMounts, "catalog-server-tls", true) || containsVolumeMount(postgres.VolumeMounts, "catalog-bootstrap-auth", true) || !containsVolumeMount(bootstrap.VolumeMounts, "catalog-bootstrap-auth", true) || !containsVolumeMount(bootstrap.VolumeMounts, "catalog-server-tls", true) {
				t.Fatalf("catalog least-privilege mounts: PostgreSQL=%#v bootstrap=%#v", postgres.VolumeMounts, bootstrap.VolumeMounts)
			}
		} else {
			for _, name := range []string{"catalog-server-tls", "catalog-bootstrap-auth"} {
				if hasVolume(pod.Volumes, name) || containsVolumeMount(postgres.VolumeMounts, name, true) || containsVolumeMount(bootstrap.VolumeMounts, name, true) {
					t.Fatalf("non-catalog shard %d received catalog material %q", shard, name)
				}
			}
		}
		budget := object[*policyv1.PodDisruptionBudget](t, plan, name)
		if budget.Spec.MinAvailable == nil || budget.Spec.MinAvailable.IntVal != 1 || budget.Spec.Selector.MatchLabels[ShardLabel] != shardLabel(shard) || budget.Spec.Selector.MatchLabels[RoleLabel] != "primary" {
			t.Fatalf("PostgreSQL PDB = %#v", budget.Spec)
		}
	}

	secret := PostgreSQLAuthSecret(cluster, 1, "demo-random-auth", []byte("0123456789abcdef"))
	if secret.Name != "demo-random-auth" || secret.Labels[ShardLabel] != "0001" || secret.Immutable == nil || !*secret.Immutable || string(secret.Data[PostgreSQLPasswordKey]) != "0123456789abcdef" || secret.Annotations[ApplyOwnershipAnnotation] != "" {
		t.Fatalf("PostgreSQL auth Secret = %#v", secret)
	}
	assertOwned(t, secret, cluster)
	catalogName := CatalogAccessSecretPrefix(cluster.Name) + strings.Repeat("a", 32)
	catalogIntent := CatalogAccessIntentSecret(cluster, catalogName)
	if catalogIntent.Name != catalogName || !CatalogAccessSecretNameIsValid(cluster.Name, catalogIntent.Name) || catalogIntent.Immutable != nil || len(catalogIntent.Data) != 0 || catalogIntent.Annotations[CatalogAccessClusterUIDAnnotation] != string(cluster.UID) {
		t.Fatalf("catalog access intent Secret = %#v", catalogIntent)
	}
	assertOwned(t, catalogIntent, cluster)
	replicationName := PostgreSQLReplicationSecretPrefix(cluster.Name, 1) + strings.Repeat("b", 32)
	replicationIntent := PostgreSQLReplicationIntentSecret(cluster, 1, replicationName)
	if replicationIntent.Name != replicationName || !PostgreSQLReplicationSecretNameIsValid(cluster.Name, 1, replicationIntent.Name) || PostgreSQLReplicationSecretNameIsValid(cluster.Name, 0, replicationIntent.Name) || replicationIntent.Immutable != nil || len(replicationIntent.Data) != 0 || replicationIntent.Labels[ShardLabel] != "0001" || replicationIntent.Annotations[PostgreSQLReplicationClusterUIDAnnotation] != string(cluster.UID) {
		t.Fatalf("replication credential intent Secret = %#v", replicationIntent)
	}
	assertOwned(t, replicationIntent, cluster)
	if got := PostgreSQLReplicationMaterialSHA256([]byte("password")); len(got) != 64 || !validCatalogMaterialSHA256(got) {
		t.Fatalf("replication material digest = %q", got)
	}
	if got, want := CatalogTLSDNSNames(cluster.Name, cluster.Namespace), []string{"demo-shardschema", "demo-shardschema.database", "demo-shardschema.database.svc", "demo-shardschema.database.svc.cluster.local"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("catalog TLS DNS names = %#v, want %#v", got, want)
	}
	secret.UID = "demo-random-auth-uid"
	claim := PostgreSQLDataPVC(cluster, 1, "demo-random-data", cluster.Spec.Storage.Size, cluster.Spec.Storage.StorageClassName, secret.Name, secret.UID)
	if claim.Name != "demo-random-data" || claim.Labels[ShardLabel] != "0001" || claim.Spec.Resources.Requests.Storage().Cmp(cluster.Spec.Storage.Size) != 0 || claim.Annotations[ApplyOwnershipAnnotation] != "" {
		t.Fatalf("PostgreSQL data PVC = %#v", claim)
	}
	if claim.Annotations[PostgreSQLDataClusterUIDAnnotation] != string(cluster.UID) {
		t.Fatalf("PostgreSQL data PVC garbage-collection boundary = %#v", claim.ObjectMeta)
	}
	if len(claim.OwnerReferences) != 1 || claim.OwnerReferences[0].Kind != "Secret" || claim.OwnerReferences[0].Name != secret.Name || claim.OwnerReferences[0].UID != secret.UID {
		t.Fatalf("PostgreSQL data PVC creation fence = %#v", claim.OwnerReferences)
	}
	if len(claim.Finalizers) != 0 {
		t.Fatalf("creation-fenced PVC received protection before its API UID checkpoint: %#v", claim.Finalizers)
	}
	if got := PostgreSQLDataPVCPrefix(cluster.Name, 1); got != "demo-shard-0001-member-0000-data-" || strings.Contains(got, "primary") || strings.Contains(got, "replica") {
		t.Fatalf("PostgreSQL data PVC prefix is not role-neutral: %q", got)
	}
}

func TestAgentQuarantinePlanProjectsExactWritableLeaseIdentity(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	cluster.Status.PostgreSQLBootstraps = testPostgreSQLBootstraps(cluster)
	cluster.Status.PostgreSQLWritableLeases = testPostgreSQLWritableLeases(cluster)
	images := singleMemberImages()
	images.PostgreSQLRuntime = PostgreSQLRuntimeAgentQuarantine
	plan, err := Plan(cluster, images)
	if err != nil {
		t.Fatal(err)
	}

	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		statefulSet := object[*appsv1.StatefulSet](t, plan, PostgreSQLShardStatefulSetName(cluster.Name, shard))
		pod := statefulSet.Spec.Template.Spec
		if statefulSet.Annotations[PostgreSQLRuntimeAnnotation] != string(PostgreSQLRuntimeAgentQuarantine) || statefulSet.Spec.Template.Annotations[PostgreSQLRuntimeAnnotation] != string(PostgreSQLRuntimeAgentQuarantine) {
			t.Fatalf("agent PostgreSQL runtime contract for shard %d = StatefulSet %#v Pod %#v", shard, statefulSet.Annotations, statefulSet.Spec.Template.Annotations)
		}
		if observed, err := ObservePostgreSQLRuntime(statefulSet.Spec.Template.Annotations, pod); err != nil || observed != PostgreSQLRuntimeAgentQuarantine {
			t.Fatalf("observe agent PostgreSQL runtime for shard %d = %q, %v", shard, observed, err)
		}
		if pod.ServiceAccountName != PostgreSQLAgentServiceAccountName(cluster.Name, shard) || pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken {
			t.Fatalf("agent Pod identity for shard %d = %#v", shard, pod)
		}
		if len(pod.Containers) != 1 {
			t.Fatalf("agent containers for shard %d = %#v", shard, pod.Containers)
		}
		agent := pod.Containers[0]
		if agent.Name != "postgresql" || agent.Image != developmentPostgreSQLBootstrapImage || agent.ImagePullPolicy != corev1.PullNever || len(agent.Command) != 0 || len(agent.Args) != 0 {
			t.Fatalf("agent runtime image for shard %d = %#v", shard, agent)
		}
		if len(agent.Ports) != 2 || agent.Ports[0].Name != "postgresql" || agent.Ports[0].ContainerPort != PostgreSQLPort || agent.Ports[1].Name != "agent-http" || agent.Ports[1].ContainerPort != HTTPPort {
			t.Fatalf("agent ports for shard %d = %#v", shard, agent.Ports)
		}
		checkpoint := cluster.Status.PostgreSQLWritableLeases[shard]
		wantedValues := map[string]string{
			"PGSHARD_HTTP_BIND":                             "0.0.0.0:8080",
			"PGSHARD_CLUSTER_ID":                            cluster.Name,
			"PGSHARD_CLUSTER_UID":                           string(cluster.UID),
			"PGSHARD_SHARD_ID":                              fmt.Sprintf("%d", shard),
			"PGSHARD_WRITABLE_LEASE_NAME":                   checkpoint.LeaseName,
			"PGSHARD_WRITABLE_LEASE_UID":                    string(checkpoint.LeaseUID),
			"PGSHARD_MAX_LEASE_TTL_MS":                      "15000",
			"PGSHARD_WRITABLE_LEASE_DURATION_SECONDS":       "15",
			"PGSHARD_WRITABLE_LEASE_RENEW_DEADLINE_SECONDS": "10",
			"PGSHARD_WRITABLE_LEASE_RETRY_MS":               "2000",
			"PGSHARD_KUBERNETES_REQUEST_TIMEOUT_MS":         "2000",
			"PGSHARD_POSTGRES_MODE":                         "quarantine",
			"PGDATA":                                        "/var/lib/postgresql/18/docker",
			"PGSHARD_POSTGRES_BIN":                          "/usr/lib/postgresql/18/bin/postgres",
			"PGSHARD_POSTGRES_SOCKET_DIR":                   "/run/pgshard/postgres",
			"PGSHARD_POSTGRES_HBA_FILE":                     "/etc/pgshard/quarantine.pg_hba.conf",
			"PGSHARD_POSTGRES_SMART_SHUTDOWN_MS":            "5000",
			"PGSHARD_POSTGRES_FAST_SHUTDOWN_MS":             "44000",
			"PGSHARD_POSTGRES_IMMEDIATE_SHUTDOWN_MS":        "500",
		}
		wantedFields := map[string]string{
			"PGSHARD_INSTANCE_ID":     "metadata.name",
			"PGSHARD_POD_UID":         "metadata.uid",
			"PGSHARD_LEASE_NAMESPACE": "metadata.namespace",
		}
		if len(agent.Env) != len(wantedValues)+len(wantedFields) {
			t.Fatalf("agent environment length for shard %d = %d: %#v", shard, len(agent.Env), agent.Env)
		}
		for _, variable := range agent.Env {
			if value, ok := wantedValues[variable.Name]; ok {
				if variable.Value != value || variable.ValueFrom != nil {
					t.Fatalf("agent environment %s for shard %d = %#v, want %q", variable.Name, shard, variable, value)
				}
				delete(wantedValues, variable.Name)
				continue
			}
			field, ok := wantedFields[variable.Name]
			if !ok || variable.Value != "" || variable.ValueFrom == nil || variable.ValueFrom.FieldRef == nil || variable.ValueFrom.FieldRef.FieldPath != field {
				t.Fatalf("unexpected agent environment for shard %d: %#v", shard, variable)
			}
			delete(wantedFields, variable.Name)
		}
		if len(wantedValues) != 0 || len(wantedFields) != 0 {
			t.Fatalf("missing agent environment for shard %d: values=%#v fields=%#v", shard, wantedValues, wantedFields)
		}

		if agent.StartupProbe == nil || agent.StartupProbe.HTTPGet == nil || agent.StartupProbe.HTTPGet.Path != "/healthz" || agent.LivenessProbe == nil || agent.LivenessProbe.HTTPGet == nil || agent.LivenessProbe.HTTPGet.Path != "/healthz" || agent.ReadinessProbe == nil || agent.ReadinessProbe.HTTPGet == nil || agent.ReadinessProbe.HTTPGet.Path != "/readyz" {
			t.Fatalf("agent probes for shard %d = startup %#v, liveness %#v, readiness %#v", shard, agent.StartupProbe, agent.LivenessProbe, agent.ReadinessProbe)
		}
		for _, probe := range []*corev1.Probe{agent.StartupProbe, agent.LivenessProbe, agent.ReadinessProbe} {
			if probe.HTTPGet.Port != intstr.FromString("agent-http") || probe.HTTPGet.Scheme != corev1.URISchemeHTTP {
				t.Fatalf("agent probe target for shard %d = %#v", shard, probe.HTTPGet)
			}
		}
		if !containsVolumeMount(agent.VolumeMounts, "data", false) || !containsVolumeMount(agent.VolumeMounts, "runtime", false) || !containsVolumeMount(agent.VolumeMounts, "tmp", false) || !containsVolumeMount(agent.VolumeMounts, "kubernetes-api", true) {
			t.Fatalf("agent mounts for shard %d = %#v", shard, agent.VolumeMounts)
		}
		for _, mount := range agent.VolumeMounts {
			if mount.Name == "runtime" && mount.MountPath != "/run/pgshard" {
				t.Fatalf("agent must create a private child below the runtime mount: %#v", mount)
			}
			if mount.Name == "bootstrap-secret" || mount.Name == "catalog-server-tls" || mount.Name == "catalog-bootstrap-auth" {
				t.Fatalf("agent received bootstrap or catalog credentials: %#v", agent.VolumeMounts)
			}
		}

		apiVolume := volumeByName(t, pod.Volumes, "kubernetes-api").Projected
		if apiVolume == nil || apiVolume.DefaultMode == nil || *apiVolume.DefaultMode != 0o440 || len(apiVolume.Sources) != 3 {
			t.Fatalf("agent API projection for shard %d = %#v", shard, apiVolume)
		}
		token := apiVolume.Sources[0].ServiceAccountToken
		ca := apiVolume.Sources[1].ConfigMap
		namespace := apiVolume.Sources[2].DownwardAPI
		if token == nil || token.Path != "token" || token.Audience != "" || token.ExpirationSeconds == nil || *token.ExpirationSeconds != 600 {
			t.Fatalf("agent token projection for shard %d = %#v", shard, token)
		}
		if ca == nil || ca.Name != "kube-root-ca.crt" || len(ca.Items) != 1 || ca.Items[0].Key != "ca.crt" || ca.Items[0].Path != "ca.crt" {
			t.Fatalf("agent CA projection for shard %d = %#v", shard, ca)
		}
		if namespace == nil || len(namespace.Items) != 1 || namespace.Items[0].Path != "namespace" || namespace.Items[0].FieldRef == nil || namespace.Items[0].FieldRef.FieldPath != "metadata.namespace" {
			t.Fatalf("agent namespace projection for shard %d = %#v", shard, namespace)
		}
	}
}

func TestPostgreSQLRuntimeObservationRejectsAnnotationShapeMismatch(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	cluster.Status.PostgreSQLBootstraps = testPostgreSQLBootstraps(cluster)
	cluster.Status.PostgreSQLWritableLeases = testPostgreSQLWritableLeases(cluster)
	images := singleMemberImages()
	images.PostgreSQLRuntime = PostgreSQLRuntimeAgentQuarantine
	plan, err := Plan(cluster, images)
	if err != nil {
		t.Fatal(err)
	}
	template := object[*appsv1.StatefulSet](t, plan, PostgreSQLShardStatefulSetName(cluster.Name, 0)).Spec.Template.DeepCopy()

	template.Annotations[PostgreSQLRuntimeAnnotation] = string(PostgreSQLRuntimeDirect)
	if _, err := ObservePostgreSQLRuntime(template.Annotations, template.Spec); err == nil || !strings.Contains(err.Error(), "direct PostgreSQL runtime carries agent-quarantine") {
		t.Fatalf("direct annotation over agent shape error = %v", err)
	}
	template.Annotations[PostgreSQLRuntimeAnnotation] = string(PostgreSQLRuntimeAgentQuarantine)
	conflicting := template.DeepCopy()
	conflicting.Spec.Containers[0].Env = append(conflicting.Spec.Containers[0].Env,
		corev1.EnvVar{Name: "PGSHARD_POSTGRES_MODE", Value: "replication-bootstrap-primary"},
		corev1.EnvVar{Name: "PGSHARD_POSTGRES_HBA_FILE", Value: "/etc/pgshard/replication-bootstrap-primary.pg_hba.conf"},
	)
	if _, err := ObservePostgreSQLRuntime(conflicting.Annotations, conflicting.Spec); err == nil || !strings.Contains(err.Error(), "does not match its process composition") {
		t.Fatalf("agent annotation with conflicting runtime environment error = %v", err)
	}
	for index := range template.Spec.Containers[0].Env {
		if template.Spec.Containers[0].Env[index].Name == "PGSHARD_POSTGRES_MODE" {
			template.Spec.Containers[0].Env[index].Value = "direct"
		}
	}
	if _, err := ObservePostgreSQLRuntime(template.Annotations, template.Spec); err == nil || !strings.Contains(err.Error(), "does not match its process composition") {
		t.Fatalf("agent annotation over direct shape error = %v", err)
	}
}

func TestAgentQuarantinePlanRejectsUncheckpointedWritableLeaseIdentity(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*pgshardv1alpha1.PgShardCluster)
		want   string
	}{
		{name: "missing", mutate: func(cluster *pgshardv1alpha1.PgShardCluster) {
			cluster.Status.PostgreSQLWritableLeases = cluster.Status.PostgreSQLWritableLeases[:1]
		}, want: "shard 1 is missing"},
		{name: "out of range shard", mutate: func(cluster *pgshardv1alpha1.PgShardCluster) {
			cluster.Status.PostgreSQLWritableLeases[0].Shard = cluster.Spec.Shards
		}, want: "shard 2 is invalid"},
		{name: "wrong name", mutate: func(cluster *pgshardv1alpha1.PgShardCluster) {
			cluster.Status.PostgreSQLWritableLeases[0].LeaseName = "foreign-term"
		}, want: "shard 0 is invalid"},
		{name: "empty UID", mutate: func(cluster *pgshardv1alpha1.PgShardCluster) {
			cluster.Status.PostgreSQLWritableLeases[0].LeaseUID = ""
		}, want: "shard 0 is invalid"},
		{name: "duplicate shard", mutate: func(cluster *pgshardv1alpha1.PgShardCluster) {
			cluster.Status.PostgreSQLWritableLeases = append(cluster.Status.PostgreSQLWritableLeases, cluster.Status.PostgreSQLWritableLeases[0])
		}, want: "shard 0 is duplicated"},
		{name: "duplicate UID", mutate: func(cluster *pgshardv1alpha1.PgShardCluster) {
			cluster.Status.PostgreSQLWritableLeases[1].LeaseUID = cluster.Status.PostgreSQLWritableLeases[0].LeaseUID
		}, want: "UID test-lease-uid-0000 is duplicated"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cluster := testCluster()
			cluster.Spec.MembersPerShard = 1
			cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
			cluster.Status.PostgreSQLBootstraps = testPostgreSQLBootstraps(cluster)
			cluster.Status.PostgreSQLWritableLeases = testPostgreSQLWritableLeases(cluster)
			test.mutate(cluster)
			images := singleMemberImages()
			images.PostgreSQLRuntime = PostgreSQLRuntimeAgentQuarantine
			if _, err := Plan(cluster, images); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Plan error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestPostgreSQLBootstrapRequiresBindingIdentityBeforeDataAccess(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		env  []string
		want string
	}{
		{name: "node UID", env: []string{"PGSHARD_NODE_BOOT_ID=boot-a"}, want: "binding-time node UID is required"},
		{name: "node boot ID", env: []string{"PGSHARD_NODE_UID=node-a"}, want: "binding-time node boot ID is required"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			parent := t.TempDir()
			script := strings.Replace(postgresqlBootstrapScript, "parent=/var/lib/postgresql/18", fmt.Sprintf("parent=%q", parent), 1)
			command := exec.Command("bash", "-c", script)
			command.Env = []string{"PGSHARD_CLUSTER_UID=cluster-uid", "PGSHARD_SHARD_ID=0000"}
			command.Env = append(command.Env, test.env...)
			output, err := command.CombinedOutput()
			if err == nil || !strings.Contains(string(output), test.want) {
				t.Fatalf("bootstrap without %s error = %v, output = %q", test.name, err, output)
			}
			for _, path := range []string{filepath.Join(parent, ".pgshard-init"), filepath.Join(parent, "docker")} {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("bootstrap touched PGDATA before binding identity validation: %s: %v", path, err)
				}
			}
		})
	}
}

func TestPostgreSQLBootstrapVerifiesMigrationBeforeDataAccess(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	migration := filepath.Join(t.TempDir(), "0001_shardschema.sql")
	if err := os.WriteFile(migration, []byte("SELECT 1;\n"), 0o444); err != nil {
		t.Fatal(err)
	}
	script := strings.Replace(postgresqlBootstrapScript, "parent=/var/lib/postgresql/18", fmt.Sprintf("parent=%q", parent), 1)
	command := exec.Command("bash", "-c", script)
	command.Env = append(bootstrapVersionTestEnvironment(t, pgshardv1alpha1.PostgreSQLMajor18),
		"PGSHARD_CLUSTER_UID=cluster-uid",
		"PGSHARD_SHARD_ID=0000",
		"PGSHARD_POSTGRESQL_MAJOR="+pgshardv1alpha1.PostgreSQLMajor18,
		"PGSHARD_SHARD_COUNT=2",
		fmt.Sprintf("PGSHARD_MAXIMUM_SHARDS=%d", pgshardv1alpha1.MaximumShards),
		"PGSHARD_BOOTSTRAP_SHARDSCHEMA=true",
		"PGSHARD_SHARDSCHEMA_MIGRATION="+migration,
		"PGSHARD_SHARDSCHEMA_MIGRATION_SHA256="+strings.Repeat("0", sha256.Size*2),
		"PGSHARD_NODE_UID=node-a",
		"PGSHARD_NODE_BOOT_ID=boot-a",
	)
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "shardschema migration does not match the operator release") {
		t.Fatalf("bootstrap migration mismatch error = %v, output = %q", err, output)
	}
	for _, path := range []string{filepath.Join(parent, ".pgshard-init"), filepath.Join(parent, "docker")} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("bootstrap touched PGDATA before migration validation: %s: %v", path, err)
		}
	}
}

func TestPostgreSQLBootstrapRefusesMismatchedDurableIdentity(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		marker     string
		clusterUID string
		shard      string
	}{
		{name: "cluster", marker: "cluster_uid=old-cluster\nshard=0000\n", clusterUID: "new-cluster", shard: "0000"},
		{name: "shard", marker: "cluster_uid=cluster-uid\nshard=0000\n", clusterUID: "cluster-uid", shard: "0001"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			parent := t.TempDir()
			final := filepath.Join(parent, "docker")
			if err := os.MkdirAll(final, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(final, "PG_VERSION"), []byte("18\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(final, postgresqlBootstrapMarker), []byte(test.marker), 0o600); err != nil {
				t.Fatal(err)
			}
			script := strings.Replace(postgresqlBootstrapScript, "parent=/var/lib/postgresql/18", fmt.Sprintf("parent=%q", parent), 1)
			command := exec.Command("bash", "-c", script)
			command.Env = append(bootstrapVersionTestEnvironment(t, pgshardv1alpha1.PostgreSQLMajor18), "PGSHARD_CLUSTER_UID="+test.clusterUID, "PGSHARD_SHARD_ID="+test.shard, "PGSHARD_POSTGRESQL_MAJOR="+pgshardv1alpha1.PostgreSQLMajor18, "PGSHARD_BOOTSTRAP_SHARDSCHEMA=false", "PGSHARD_NODE_UID=node-a", "PGSHARD_NODE_BOOT_ID=boot-a")
			output, err := command.CombinedOutput()
			if err == nil {
				t.Fatal("bootstrap accepted a PostgreSQL data directory from a different identity")
			}
			if !strings.Contains(string(output), "refusing PostgreSQL data directory owned by another cluster or shard") {
				t.Fatalf("bootstrap mismatch output = %q", output)
			}
			if _, err := os.Stat(filepath.Join(parent, ".pgshard-init")); !os.IsNotExist(err) {
				t.Fatalf("bootstrap entered initialization after identity mismatch: %v", err)
			}
		})
	}
}

func TestPostgreSQLBootstrapRejectsWrongMajorBeforeDataAccess(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name          string
		binaryMajor   string
		durableMajor  string
		initdbMajor   string
		want          string
		createDurable bool
	}{
		{name: "bootstrap binary", binaryMajor: "17", want: "bootstrap image does not provide the operator's PostgreSQL major"},
		{name: "durable data", binaryMajor: "18", durableMajor: "17", want: "refusing a PostgreSQL data directory from another major version", createDurable: true},
		{name: "initialized staging data", binaryMajor: "18", initdbMajor: "17", want: "initialized PostgreSQL data does not match the operator major"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			parent := t.TempDir()
			if test.createDurable {
				final := filepath.Join(parent, "docker")
				if err := os.MkdirAll(final, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(final, "PG_VERSION"), []byte(test.durableMajor+"\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			script := strings.Replace(postgresqlBootstrapScript, "parent=/var/lib/postgresql/18", fmt.Sprintf("parent=%q", parent), 1)
			command := exec.Command("bash", "-c", script)
			command.Env = append(bootstrapVersionTestEnvironment(t, test.binaryMajor, test.initdbMajor),
				"PGSHARD_CLUSTER_UID=cluster-uid",
				"PGSHARD_SHARD_ID=0000",
				"PGSHARD_POSTGRESQL_MAJOR="+pgshardv1alpha1.PostgreSQLMajor18,
				"PGSHARD_BOOTSTRAP_SHARDSCHEMA=false",
				"PGSHARD_NODE_UID=node-a",
				"PGSHARD_NODE_BOOT_ID=boot-a",
			)
			output, err := command.CombinedOutput()
			if err == nil || !strings.Contains(string(output), test.want) {
				t.Fatalf("wrong-major bootstrap error = %v, output = %q", err, output)
			}
			if _, err := os.Stat(filepath.Join(parent, ".pgshard-init")); test.initdbMajor == "" && !os.IsNotExist(err) {
				t.Fatalf("wrong-major bootstrap touched staging PGDATA: %v", err)
			} else if test.initdbMajor != "" && err != nil {
				t.Fatalf("wrong initdb major was not detected in staging PGDATA: %v", err)
			}
			if !test.createDurable {
				if _, err := os.Stat(filepath.Join(parent, "docker")); !os.IsNotExist(err) {
					t.Fatalf("wrong bootstrap binary published PGDATA: %v", err)
				}
			}
		})
	}
}

func TestPostgreSQLBootstrapDockerRecoveryAndConflict(t *testing.T) {
	if os.Getenv("PGSHARD_POSTGRES_BOOTSTRAP_E2E") != "true" {
		t.Skip("set PGSHARD_POSTGRES_BOOTSTRAP_E2E=true with the local PostgreSQL bootstrap image")
	}
	image := os.Getenv("PGSHARD_POSTGRES_BOOTSTRAP_IMAGE")
	if image == "" {
		t.Fatal("PGSHARD_POSTGRES_BOOTSTRAP_IMAGE is required")
	}
	volume := fmt.Sprintf("pgshard-bootstrap-%d-%d", os.Getpid(), time.Now().UnixNano())
	runDocker := func(arguments ...string) (string, error) {
		t.Helper()
		output, err := exec.Command("docker", arguments...).CombinedOutput()
		return string(output), err
	}
	if output, err := runDocker("volume", "create", volume); err != nil {
		t.Fatalf("create Docker volume: %v\n%s", err, output)
	}
	t.Cleanup(func() {
		if output, err := runDocker("volume", "rm", "--force", volume); err != nil {
			t.Errorf("remove Docker volume: %v\n%s", err, output)
		}
	})
	if output, err := runDocker(
		"run", "--rm", "--user", "0:0",
		"--volume", volume+":/var/lib/postgresql",
		"--entrypoint", "chown", image, "999:999", "/var/lib/postgresql",
	); err != nil {
		t.Fatalf("prepare Docker volume ownership: %v\n%s", err, output)
	}

	newTraversableFixtureDirectory := func(prefix string) string {
		t.Helper()
		directory, err := os.MkdirTemp("", prefix)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(directory, 0o755); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := os.RemoveAll(directory); err != nil {
				t.Errorf("remove Docker fixture directory: %v", err)
			}
		})
		return directory
	}
	secretDirectory := newTraversableFixtureDirectory("pgshard-bootstrap-secret-")
	passwordPath := filepath.Join(secretDirectory, PostgreSQLPasswordKey)
	if err := os.WriteFile(passwordPath, []byte("bootstrap-e2e-only-password\n"), 0o444); err != nil {
		t.Fatal(err)
	}
	replicationDirectory := newTraversableFixtureDirectory("pgshard-replication-auth-")
	const replicationPassword = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
	replicationPasswordPath := filepath.Join(replicationDirectory, PostgreSQLReplicationPasswordKey)
	if err := os.WriteFile(replicationPasswordPath, []byte(replicationPassword), 0o444); err != nil {
		t.Fatal(err)
	}
	catalogAuthDirectory := newTraversableFixtureDirectory("pgshard-catalog-auth-")
	const catalogPassword = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	catalogCA := []byte("bootstrap-e2e-catalog-ca\n")
	if err := os.WriteFile(
		filepath.Join(catalogAuthDirectory, CatalogPasswordKey),
		[]byte(catalogPassword),
		0o444,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(catalogAuthDirectory, CatalogCACertificateKey), catalogCA, 0o444); err != nil {
		t.Fatal(err)
	}
	catalogTLSDirectory := newTraversableFixtureDirectory("pgshard-catalog-tls-")
	catalogServerCertificate := []byte("bootstrap-e2e-server-certificate\n")
	catalogServerPrivateKey := []byte("bootstrap-e2e-server-private-key\n")
	if err := os.WriteFile(filepath.Join(catalogTLSDirectory, CatalogTLSCertificateKey), catalogServerCertificate, 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(catalogTLSDirectory, CatalogTLSPrivateKeyKey), catalogServerPrivateKey, 0o444); err != nil {
		t.Fatal(err)
	}
	catalogClientSHA256 := CatalogClientMaterialSHA256([]byte(catalogPassword), catalogCA)
	catalogServerSHA256 := CatalogServerMaterialSHA256(catalogServerCertificate, catalogServerPrivateKey)
	configurationDirectory := newTraversableFixtureDirectory("pgshard-bootstrap-config-")
	if err := os.WriteFile(filepath.Join(configurationDirectory, "postgresql.conf"), []byte(strings.Join([]string{
		"fsync = on",
		"listen_addresses = '*'",
		"max_prepared_transactions = 8",
		"max_replication_slots = 20",
		"max_wal_senders = 20",
		"wal_level = logical",
		"log_statement = all",
		"log_min_error_statement = error",
		"log_min_duration_statement = 0",
		"",
	}, "\n")), 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(configurationDirectory, "primary-0000.conf"), []byte("include = '/etc/pgshard/postgresql/postgresql.conf'\n"), 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(configurationDirectory, databaseGenesisKey),
		[]byte(renderDatabaseGenesisSQL(&pgshardv1alpha1.PgShardCluster{})),
		0o444,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(configurationDirectory, databaseTopologyPreflightKey),
		[]byte(renderDatabaseTopologyPreflightSQL(&pgshardv1alpha1.PgShardCluster{})),
		0o444,
	); err != nil {
		t.Fatal(err)
	}
	configurationData := make(map[string]string)
	configurationEntries, err := os.ReadDir(configurationDirectory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range configurationEntries {
		contents, err := os.ReadFile(filepath.Join(configurationDirectory, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		configurationData[entry.Name()] = string(contents)
	}
	configurationSHA256 := configMapDataHash(configurationData)
	currentConfigurationSHA256 := func() string {
		t.Helper()
		entries, err := os.ReadDir(configurationDirectory)
		if err != nil {
			t.Fatal(err)
		}
		data := make(map[string]string, len(entries))
		for _, entry := range entries {
			if !entry.Type().IsRegular() {
				continue
			}
			contents, err := os.ReadFile(filepath.Join(configurationDirectory, entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			data[entry.Name()] = string(contents)
		}
		return configMapDataHash(data)
	}
	legacyMigration, err := filepath.Abs(filepath.Join("..", "..", "..", "crates", "pgshard-catalog", "tests", "fixtures", "v0_49_0_shardschema.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(legacyMigration); err != nil {
		t.Fatalf("locate legacy shardschema fixture: %v", err)
	}

	containerArguments := func(dataParent, script string, copyConfiguration bool, environment ...string) []string {
		t.Helper()
		arguments := []string{
			"--user", "999:999", "--network", "none", "--read-only",
			"--volume", volume + ":/var/lib/postgresql",
			"--volume", secretDirectory + ":/etc/pgshard/bootstrap:ro",
			"--volume", replicationDirectory + ":/etc/pgshard/replication:ro",
			"--volume", catalogAuthDirectory + ":/etc/pgshard/catalog-auth:ro",
			"--volume", catalogTLSDirectory + ":/etc/pgshard/catalog-tls:ro",
			"--volume", configurationDirectory + ":/etc/pgshard/postgresql-source:ro",
			"--volume", legacyMigration + ":/tmp/v0_49_0_shardschema.sql:ro",
			"--tmpfs", "/tmp:rw,uid=999,gid=999,mode=0700,size=67108864",
			"--env", "PGDATA=" + dataParent + "/docker",
		}
		if copyConfiguration {
			arguments = append(arguments, "--tmpfs", "/etc/pgshard/postgresql:rw,uid=999,gid=999,mode=0700,size=2097152")
		} else {
			arguments = append(arguments, "--volume", configurationDirectory+":/etc/pgshard/postgresql:ro")
		}
		for _, variable := range environment {
			arguments = append(arguments, "--env", variable)
		}
		arguments = append(arguments, "--entrypoint", "bash", image, "-ceu", script)
		return arguments
	}
	runContainer := func(dataParent, script string, environment ...string) (string, error) {
		t.Helper()
		arguments := append([]string{"run", "--rm"}, containerArguments(dataParent, script, false, environment...)...)
		return runDocker(arguments...)
	}
	runBootstrapContainer := func(dataParent, script string, environment ...string) (string, error) {
		t.Helper()
		arguments := append([]string{"run", "--rm"}, containerArguments(dataParent, script, true, environment...)...)
		return runDocker(arguments...)
	}
	runContainerWithTimeout := func(name, dataParent, script string, timeout time.Duration, environment ...string) (string, error) {
		t.Helper()
		arguments := append([]string{"run", "--rm", "--name", name}, containerArguments(dataParent, script, true, environment...)...)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		output, err := exec.CommandContext(ctx, "docker", arguments...).CombinedOutput()
		if ctx.Err() != nil {
			_, _ = runDocker("rm", "--force", name)
			return string(output), fmt.Errorf("Docker container %s exceeded %s: %w", name, timeout, ctx.Err())
		}
		return string(output), err
	}
	bootstrapEnvironment := func(installCatalog bool, shardCount int) []string {
		return []string{
			"PGSHARD_CLUSTER_UID=bootstrap-e2e-cluster",
			"PGSHARD_SHARD_ID=0000",
			"PGSHARD_POSTGRESQL_MAJOR=" + pgshardv1alpha1.PostgreSQLMajor18,
			fmt.Sprintf("PGSHARD_SHARD_COUNT=%d", shardCount),
			fmt.Sprintf("PGSHARD_MAXIMUM_SHARDS=%d", pgshardv1alpha1.MaximumShards),
			fmt.Sprintf("PGSHARD_BOOTSTRAP_SHARDSCHEMA=%t", installCatalog),
			"PGSHARD_SHARDSCHEMA_MIGRATION=" + shardschemaMigrationPath,
			"PGSHARD_SHARDSCHEMA_MIGRATION_SHA256=" + shardschemaMigrationSHA256,
			"PGSHARD_POSTGRESQL_CONFIG_SHA256=" + currentConfigurationSHA256(),
			"PGSHARD_NODE_UID=bootstrap-e2e-node",
			"PGSHARD_NODE_BOOT_ID=bootstrap-e2e-boot",
			"PGSHARD_CATALOG_CLIENT_SHA256=" + catalogClientSHA256,
			"PGSHARD_CATALOG_SERVER_SHA256=" + catalogServerSHA256,
		}
	}
	replicationBootstrapEnvironment := func(password string, members int) []string {
		return append(bootstrapEnvironment(false, 1),
			"PGSHARD_BOOTSTRAP_HBA_MODE=replication-bootstrap-primary",
			fmt.Sprintf("PGSHARD_MEMBERS_PER_SHARD=%d", members),
			"PGSHARD_REPLICATION_MATERIAL_SHA256="+PostgreSQLReplicationMaterialSHA256([]byte(password)),
		)
	}
	bootstrapScript := func(dataParent string) string {
		if dataParent == "/var/lib/postgresql/18" {
			return postgresqlBootstrapScript
		}
		return strings.Replace(postgresqlBootstrapScript, "parent=/var/lib/postgresql/18", "parent="+dataParent, 1)
	}
	bootstrap := func(dataParent string, installCatalog bool, shardCount int) (string, error) {
		t.Helper()
		output, err := runBootstrapContainer(dataParent, bootstrapScript(dataParent), bootstrapEnvironment(installCatalog, shardCount)...)
		if strings.Contains(output, catalogPassword) {
			t.Fatalf("PostgreSQL bootstrap logged the catalog password:\n%s", output)
		}
		return output, err
	}
	bootstrapReplicationMembers := func(dataParent, password string, members int) (string, error) {
		t.Helper()
		output, err := runBootstrapContainer(dataParent, bootstrapScript(dataParent), replicationBootstrapEnvironment(password, members)...)
		if strings.Contains(output, replicationPassword) || strings.Contains(output, password) {
			t.Fatalf("PostgreSQL replication bootstrap logged a plaintext password")
		}
		return output, err
	}
	bootstrapReplication := func(dataParent, password string) (string, error) {
		t.Helper()
		return bootstrapReplicationMembers(dataParent, password, 3)
	}
	configurationPath := filepath.Join(configurationDirectory, "postgresql.conf")
	originalConfiguration, err := os.ReadFile(configurationPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(configurationPath, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configurationPath, append(originalConfiguration, []byte("archive_command = 'false'\n")...), 0o644); err != nil {
		t.Fatal(err)
	}
	const replacedConfigurationParent = "/var/lib/postgresql/18-replaced-config"
	replacedEnvironment := bootstrapEnvironment(false, 2)
	for index := range replacedEnvironment {
		if strings.HasPrefix(replacedEnvironment[index], "PGSHARD_POSTGRESQL_CONFIG_SHA256=") {
			replacedEnvironment[index] = "PGSHARD_POSTGRESQL_CONFIG_SHA256=" + configurationSHA256
		}
	}
	replacedOutput, replacedErr := runBootstrapContainer(replacedConfigurationParent, bootstrapScript(replacedConfigurationParent), replacedEnvironment...)
	if err := os.WriteFile(configurationPath, originalConfiguration, 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(configurationPath, 0o444); err != nil {
		t.Fatal(err)
	}
	if replacedErr == nil || !strings.Contains(replacedOutput, "PostgreSQL configuration does not match the controller-owned Pod contract") {
		t.Fatalf("bootstrap accepted replaced configuration: %v\n%s", replacedErr, replacedOutput)
	}
	if output, err := runContainer(replacedConfigurationParent, "test ! -e \"$PGDATA\"", bootstrapEnvironment(false, 2)...); err != nil {
		t.Fatalf("replaced configuration touched PGDATA: %v\n%s", err, output)
	}
	const legacyUpgradeDataParent = "/var/lib/postgresql/18"
	if output, err := bootstrap(legacyUpgradeDataParent, false, 2); err != nil {
		t.Fatalf("initialize PGDATA without catalog: %v\n%s", err, output)
	}

	const postgresHarness = `set -Eeuo pipefail
socket=/tmp/pgshard-bootstrap-e2e
mkdir -m 0700 "$socket"
pg_ctl -D "$PGDATA" -w -t 45 start \
  -l /tmp/postgres.log \
  -o "-c config_file=/etc/pgshard/postgresql/primary-0000.conf -c listen_addresses='' -c unix_socket_directories='$socket' -c unix_socket_permissions=0700 -c event_triggers=off -c synchronous_standby_names='' -c synchronized_standby_slots=''" >/dev/null
stop_postgres() {
  result=$?
  trap - EXIT
  pg_ctl -D "$PGDATA" -w -t 45 stop -m fast >/dev/null || result=1
  exit "$result"
}
trap stop_postgres EXIT
`
	writeReplicationPassword := func(password string) {
		t.Helper()
		if err := os.Chmod(replicationPasswordPath, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(replicationPasswordPath, []byte(password), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(replicationPasswordPath, 0o444); err != nil {
			t.Fatal(err)
		}
	}
	replicationSQL := func(dataParent, sql string) (string, error) {
		t.Helper()
		output, err := runContainer(dataParent, postgresHarness+`
psql -X --no-password --host="$socket" --username=postgres --dbname=postgres \
  --set=ON_ERROR_STOP=1 --no-align --tuples-only --command="$PGSHARD_TEST_SQL"
pg_ctl -D "$PGDATA" -w -t 45 stop -m fast >/dev/null
trap - EXIT
`, "PGSHARD_TEST_SQL="+sql)
		return strings.TrimSpace(output), err
	}
	mustReplicationSQL := func(dataParent, sql string) string {
		t.Helper()
		output, err := replicationSQL(dataParent, sql)
		if err != nil {
			t.Fatalf("query replication-bootstrap fixture: %v\n%s", err, output)
		}
		return output
	}
	replicationState := func(dataParent string) string {
		t.Helper()
		return mustReplicationSQL(dataParent, `
SELECT CASE WHEN roles.rolcanlogin
                  AND roles.rolreplication
                  AND NOT roles.rolsuper
                  AND NOT roles.rolinherit
                  AND NOT EXISTS (
                    SELECT FROM pg_catalog.pg_shdepend AS dependencies
                     WHERE dependencies.refclassid = 'pg_catalog.pg_authid'::pg_catalog.regclass
                       AND dependencies.refobjid = roles.oid
                  )
            THEN 'safe' ELSE 'unsafe' END
  FROM pg_catalog.pg_authid AS roles
 WHERE roles.rolname = 'pgshard_replication';
SELECT COALESCE(pg_catalog.string_agg(slot_name, ',' ORDER BY slot_name), '')
  FROM pg_catalog.pg_replication_slots
 WHERE pg_catalog.left(slot_name, pg_catalog.length('pgshard_member_')) = 'pgshard_member_';`)
	}

	const replicationDataParent = "/var/lib/postgresql/18-replication-material"
	replicationConfiguration := append(slices.Clone(originalConfiguration), []byte(
		"synchronous_standby_names = 'ANY 1 (pgshard_member_0001,pgshard_member_0002)'\n"+
			"synchronized_standby_slots = 'pgshard_member_0001,pgshard_member_0002'\n",
	)...)
	if err := os.Chmod(configurationPath, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configurationPath, replicationConfiguration, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(configurationPath, 0o444); err != nil {
		t.Fatal(err)
	}
	if output, err := bootstrapReplication(replicationDataParent, replicationPassword); err != nil {
		t.Fatalf("materialize replication role and slots: %v\n%s", err, output)
	}
	if output, err := bootstrapReplication(replicationDataParent, replicationPassword); err != nil {
		t.Fatalf("replay replication role and slots: %v\n%s", err, output)
	}
	if got := replicationState(replicationDataParent); got != "safe\npgshard_member_0001,pgshard_member_0002" {
		t.Fatalf("replication-bootstrap state = %q", got)
	}
	mustReplicationSQL(replicationDataParent, "SELECT slot_name FROM pg_catalog.pg_create_physical_replication_slot('pgshardxmemberyoutside', true, false)")
	if output, err := bootstrapReplication(replicationDataParent, replicationPassword); err != nil {
		t.Fatalf("unrelated physical slot entered the reserved namespace: %v\n%s", err, output)
	}

	mustReplicationSQL(replicationDataParent, "SELECT pg_catalog.pg_drop_replication_slot('pgshard_member_0002')")
	const wrongReplicationPassword = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
	writeReplicationPassword(wrongReplicationPassword)
	wrongOutput, wrongErr := bootstrapReplication(replicationDataParent, wrongReplicationPassword)
	if wrongErr == nil || !strings.Contains(wrongOutput, "replication credential that does not authenticate") {
		t.Fatalf("wrong replication password did not fail closed before slot repair: %v\n%s", wrongErr, wrongOutput)
	}
	if got := replicationState(replicationDataParent); got != "safe\npgshard_member_0001" {
		t.Fatalf("wrong credential mutated replication state: %q", got)
	}
	writeReplicationPassword(replicationPassword)
	if output, err := bootstrapReplication(replicationDataParent, replicationPassword); err != nil {
		t.Fatalf("repair missing slot after credential proof: %v\n%s", err, output)
	}

	mustReplicationSQL(replicationDataParent, "SELECT pg_catalog.pg_drop_replication_slot('pgshard_member_0002')")
	mustReplicationSQL(replicationDataParent, "SELECT slot_name FROM pg_catalog.pg_create_physical_replication_slot('pgshard_member_0002', false, false)")
	unsafeOutput, unsafeErr := bootstrapReplication(replicationDataParent, replicationPassword)
	if unsafeErr == nil || !strings.Contains(unsafeOutput, "unsafe or foreign managed physical replication slot") {
		t.Fatalf("unsafe expected slot was adopted: %v\n%s", unsafeErr, unsafeOutput)
	}
	mustReplicationSQL(replicationDataParent, "SELECT pg_catalog.pg_drop_replication_slot('pgshard_member_0002')")
	mustReplicationSQL(replicationDataParent, "SELECT slot_name FROM pg_catalog.pg_create_physical_replication_slot('pgshard_member_9999', true, false)")
	foreignOutput, foreignErr := bootstrapReplication(replicationDataParent, replicationPassword)
	if foreignErr == nil || !strings.Contains(foreignOutput, "unsafe or foreign managed physical replication slot") {
		t.Fatalf("foreign reserved slot was adopted: %v\n%s", foreignErr, foreignOutput)
	}
	if got := replicationState(replicationDataParent); got != "safe\npgshard_member_0001,pgshard_member_9999" {
		t.Fatalf("foreign slot rejection mutated replication state: %q", got)
	}
	mustReplicationSQL(replicationDataParent, "SELECT pg_catalog.pg_drop_replication_slot('pgshard_member_9999')")
	mustReplicationSQL(replicationDataParent, "CREATE TABLE replication_dependency_guard (id integer); GRANT SELECT ON replication_dependency_guard TO pgshard_replication")
	dependencyOutput, dependencyErr := bootstrapReplication(replicationDataParent, replicationPassword)
	if dependencyErr == nil || !strings.Contains(dependencyOutput, "unsafe PostgreSQL replication role") {
		t.Fatalf("replication role with direct object dependencies was adopted: %v\n%s", dependencyErr, dependencyOutput)
	}
	if got := replicationState(replicationDataParent); got != "unsafe\npgshard_member_0001" {
		t.Fatalf("unsafe role rejection mutated replication slots: %q", got)
	}
	const fiveMemberReplicationDataParent = "/var/lib/postgresql/18-replication-material-five"
	if output, err := bootstrapReplicationMembers(fiveMemberReplicationDataParent, replicationPassword, 5); err != nil {
		t.Fatalf("materialize five-member replication role and slots: %v\n%s", err, output)
	}
	if got := replicationState(fiveMemberReplicationDataParent); got != "safe\npgshard_member_0001,pgshard_member_0002,pgshard_member_0003,pgshard_member_0004" {
		t.Fatalf("five-member replication-bootstrap state = %q", got)
	}
	if err := os.Chmod(configurationPath, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configurationPath, originalConfiguration, 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(configurationPath, 0o444); err != nil {
		t.Fatal(err)
	}

	prepareLegacyCatalog := postgresHarness + `
createdb --no-password --host="$socket" --username=postgres --template=template0 --encoding=UTF8 shardschema
psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
  --set=ON_ERROR_STOP=1 --file=/tmp/v0_49_0_shardschema.sql >/dev/null
pg_ctl -D "$PGDATA" -w -t 45 stop -m fast >/dev/null
trap - EXIT
`
	if output, err := runContainer(legacyUpgradeDataParent, prepareLegacyCatalog); err != nil {
		t.Fatalf("prepare v0.49.0 catalog database: %v\n%s", err, output)
	}
	if output, err := bootstrap(legacyUpgradeDataParent, true, 1); err != nil {
		t.Fatalf("upgrade v0.49.0 catalog database: %v\n%s", err, output)
	}
	localCatalogLogin := postgresHarness + `
if PGPASSWORD="$(</etc/pgshard/catalog-auth/catalog-password)" \
  psql -X --no-password --host="$socket" --username=pgshard_pooler_catalog --dbname=postgres \
    --set=ON_ERROR_STOP=1 --command='SELECT 1'; then
  echo "catalog login unexpectedly escaped through a local socket" >&2
  exit 1
fi
pg_ctl -D "$PGDATA" -w -t 45 stop -m fast >/dev/null
trap - EXIT
`
	if output, err := runContainer(legacyUpgradeDataParent, localCatalogLogin); err != nil {
		t.Fatalf("prove local-socket catalog login rejection: %v\n%s", err, output)
	}

	catalogSQL := func(dataParent, sql string) string {
		t.Helper()
		output, err := runContainer(dataParent, postgresHarness+`
psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
  --set=ON_ERROR_STOP=1 --no-align --tuples-only --command="$PGSHARD_TEST_SQL"
pg_ctl -D "$PGDATA" -w -t 45 stop -m fast >/dev/null
trap - EXIT
`, "PGSHARD_TEST_SQL="+sql)
		if err != nil {
			t.Fatalf("query catalog fixture: %v\n%s", err, output)
		}
		return strings.TrimSpace(output)
	}
	catalogRoleSQL := func(dataParent, sql string) string {
		t.Helper()
		output, err := runContainer(dataParent, `set -Eeuo pipefail
socket=/tmp/pgshard-catalog-role-e2e
hba=/tmp/pgshard-catalog-role-hba
mkdir -m 0700 "$socket"
printf '%s\n' \
  'local shardschema pgshard_pooler_catalog scram-sha-256' \
  'local all all reject' \
  'host all all all reject' > "$hba"
pg_ctl -D "$PGDATA" -w -t 45 start \
  -l /tmp/postgres.log \
  -o "-c config_file=/etc/pgshard/postgresql/primary-0000.conf -c listen_addresses='' -c unix_socket_directories='$socket' -c unix_socket_permissions=0700 -c hba_file='$hba' -c event_triggers=off" >/dev/null
stop_postgres() {
  result=$?
  trap - EXIT
  pg_ctl -D "$PGDATA" -w -t 45 stop -m fast >/dev/null || result=1
  rm -f -- "$hba"
  exit "$result"
}
trap stop_postgres EXIT
PGPASSWORD="$(</etc/pgshard/catalog-auth/catalog-password)" \
  psql -X --no-password --host="$socket" --username=pgshard_pooler_catalog --dbname=shardschema \
    --set=ON_ERROR_STOP=1 --no-align --tuples-only --command="$PGSHARD_TEST_SQL"
pg_ctl -D "$PGDATA" -w -t 45 stop -m fast >/dev/null
rm -f -- "$hba"
trap - EXIT
`, "PGSHARD_TEST_SQL="+sql)
		if err != nil {
			t.Fatalf("query catalog as production reader role: %v\n%s", err, output)
		}
		return strings.TrimSpace(output)
	}
	fingerprint := func(dataParent string) string {
		t.Helper()
		output, err := runContainer(dataParent, postgresHarness+`
{
  {
  pg_dump --no-password --host="$socket" --username=postgres --dbname=shardschema \
    --schema-only --quote-all-identifiers \
    --restrict-key=pgshardCatalogSnapshot
  pg_dump --no-password --host="$socket" --username=postgres --dbname=shardschema \
    --data-only --quote-all-identifiers \
    --restrict-key=pgshardCatalogSnapshot
  psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
    --set=ON_ERROR_STOP=1 --no-align --tuples-only <<'PGSHARD_FINGERPRINT_SQL'
SELECT pg_catalog.row_to_json(role_state)::text
  FROM (
    SELECT roles.rolname,
           roles.rolsuper,
           roles.rolinherit,
           roles.rolcreaterole,
           roles.rolcreatedb,
           roles.rolcanlogin,
           roles.rolreplication,
	           roles.rolbypassrls,
	           roles.rolconnlimit,
	           roles.rolvaliduntil,
	           roles.rolpassword AS password_verifier
	      FROM pg_catalog.pg_authid AS roles
	     WHERE pg_catalog.left(roles.rolname, 16) = 'pgshard_catalog_'
	        OR roles.rolname = 'pgshard_pooler_catalog'
     ORDER BY roles.rolname
  ) AS role_state;
SELECT pg_catalog.row_to_json(membership_state)::text
  FROM (
    SELECT granted_role.rolname AS granted_role,
           member_role.rolname AS member_role,
           grantor_role.rolname AS grantor_role,
           memberships.admin_option,
           memberships.inherit_option,
           memberships.set_option
      FROM pg_catalog.pg_auth_members AS memberships
      JOIN pg_catalog.pg_roles AS granted_role
        ON granted_role.oid = memberships.roleid
      JOIN pg_catalog.pg_roles AS member_role
        ON member_role.oid = memberships.member
      JOIN pg_catalog.pg_roles AS grantor_role
        ON grantor_role.oid = memberships.grantor
     WHERE pg_catalog.left(granted_role.rolname, 16) = 'pgshard_catalog_'
        OR pg_catalog.left(member_role.rolname, 16) = 'pgshard_catalog_'
     ORDER BY granted_role.rolname, member_role.rolname, grantor_role.rolname
  ) AS membership_state;
SELECT pg_catalog.row_to_json(database_state)::text
  FROM (
    SELECT databases.datname,
           pg_catalog.pg_get_userbyid(databases.datdba) AS owner_name,
           databases.encoding,
           databases.datcollate,
           databases.datctype,
           databases.datlocprovider,
           databases.datlocale,
           databases.daticurules,
           databases.datcollversion,
           databases.datistemplate,
           databases.datallowconn,
           databases.datconnlimit,
           databases.datacl
      FROM pg_catalog.pg_database AS databases
     WHERE databases.datname = 'shardschema'
  ) AS database_state;
SELECT pg_catalog.row_to_json(setting_state)::text
  FROM (
    SELECT COALESCE(databases.datname, '*') AS database_name,
           COALESCE(roles.rolname, '*') AS role_name,
           settings.setconfig
      FROM pg_catalog.pg_db_role_setting AS settings
      LEFT JOIN pg_catalog.pg_database AS databases
        ON databases.oid = settings.setdatabase
      LEFT JOIN pg_catalog.pg_roles AS roles
        ON roles.oid = settings.setrole
     WHERE settings.setdatabase = 0
        OR databases.datname = 'shardschema'
     ORDER BY database_name, role_name
  ) AS setting_state;
SELECT pg_catalog.row_to_json(event_trigger_state)::text
  FROM (
    SELECT triggers.evtname,
           pg_catalog.pg_get_userbyid(triggers.evtowner) AS owner_name,
           triggers.evtevent,
           triggers.evtenabled,
           triggers.evtfoid::pg_catalog.regprocedure::text AS function_name,
           triggers.evttags
      FROM pg_catalog.pg_event_trigger AS triggers
     ORDER BY triggers.evtname
  ) AS event_trigger_state;
PGSHARD_FINGERPRINT_SQL
  } | sha256sum
  sha256sum "$PGDATA/pg_hba.conf"
} | sha256sum
pg_ctl -D "$PGDATA" -w -t 45 stop -m fast >/dev/null
trap - EXIT
`)
		if err != nil {
			t.Fatalf("fingerprint catalog fixture: %v\n%s", err, output)
		}
		fields := strings.Fields(output)
		if len(fields) != 2 || fields[1] != "-" {
			t.Fatalf("catalog and HBA fingerprint output = %q", output)
		}
		return fields[0]
	}
	assertRejectedWithoutCatalogOrHBAMutation := func(dataParent string, shardCount int, want string) {
		t.Helper()
		before := fingerprint(dataParent)
		output, err := bootstrap(dataParent, true, shardCount)
		if err == nil || !strings.Contains(output, want) {
			t.Fatalf("conflicting catalog bootstrap error = %v, want %q\n%s", err, want, output)
		}
		if after := fingerprint(dataParent); after != before {
			t.Fatalf("rejected catalog or serving HBA changed before=%q after=%q", before, after)
		}
	}

	const legacyTopologyMismatchParent = "/var/lib/postgresql/18-legacy-topology-mismatch"
	if output, err := bootstrap(legacyTopologyMismatchParent, false, 2); err != nil {
		t.Fatalf("initialize legacy topology mismatch PGDATA: %v\n%s", err, output)
	}
	if output, err := runContainer(legacyTopologyMismatchParent, prepareLegacyCatalog); err != nil {
		t.Fatalf("prepare legacy topology mismatch catalog: %v\n%s", err, output)
	}
	catalogSQL(legacyTopologyMismatchParent, `
INSERT INTO pgshard_catalog.shards(shard_id, shard_number, state)
VALUES ('shard-0001', 1, 'active');
DO $pgshard_legacy_database_topology$
DECLARE
  database_id uuid;
  routing_generation bigint;
  observed_catalog_epoch bigint;
BEGIN
  INSERT INTO pgshard_catalog.logical_databases(database_name)
  VALUES ('app')
  RETURNING logical_database_id INTO database_id;
  INSERT INTO pgshard_catalog.routing_epochs(logical_database_id)
  VALUES (database_id)
  RETURNING routing_epoch INTO routing_generation;
  INSERT INTO pgshard_catalog.routing_ranges(routing_epoch, range_start, range_end, shard_id)
  VALUES
    (routing_generation, 0, 9223372036854775808, 'shard-0000'),
    (routing_generation, 9223372036854775808, 18446744073709551616, 'shard-0001');
  SELECT catalog_epoch INTO STRICT observed_catalog_epoch
    FROM pgshard_catalog.cluster_state WHERE singleton;
  PERFORM pgshard_catalog.activate_routing_epoch(
    database_id,
    routing_generation,
    NULL,
    observed_catalog_epoch
  );
END
$pgshard_legacy_database_topology$;
`)

	assertRejectedWithoutCatalogOrHBAMutation(legacyUpgradeDataParent, 2, "RestoreTopologyMismatch")
	catalogSQL(legacyUpgradeDataParent, "INSERT INTO pgshard_catalog.shards(shard_id, shard_number, state) VALUES ('shard-0001', 1, 'active')")
	if output, err := bootstrap(legacyUpgradeDataParent, true, 2); err != nil {
		t.Fatalf("replay exact two-shard catalog inventory: %v\n%s", err, output)
	}
	if got := catalogSQL(legacyUpgradeDataParent, "SELECT (SELECT string_agg(shard_id::text || ':' || shard_number::text || ':' || state, ',' ORDER BY shard_number) FROM pgshard_catalog.shards), (SELECT count(*) FROM pgshard_catalog.shard_restore_incarnations WHERE state = 'active'), (SELECT pg_catalog.pg_get_userbyid(nspowner) FROM pg_catalog.pg_namespace WHERE nspname = 'pgshard_catalog')"); got != "shard-0000:0:active,shard-0001:1:active|2|pgshard_catalog_owner" {
		t.Fatalf("recovered catalog inventory = %q", got)
	}
	assertRejectedWithoutCatalogOrHBAMutation(legacyUpgradeDataParent, 1, "RestoreTopologyMismatch")

	genesisCluster := &pgshardv1alpha1.PgShardCluster{Spec: pgshardv1alpha1.PgShardClusterSpec{
		Shards: 2,
		Databases: []pgshardv1alpha1.DatabaseTemplate{
			{Name: "app", Shards: 2, Cells: []int32{0, 1}},
			{Name: "analytics", Shards: 1, Cells: []int32{0}},
		},
	}}
	genesisPath := filepath.Join(configurationDirectory, databaseGenesisKey)
	replaceDatabaseGenesis := func(cluster *pgshardv1alpha1.PgShardCluster) {
		t.Helper()
		files := map[string]string{
			genesisPath: renderDatabaseGenesisSQL(cluster),
			filepath.Join(configurationDirectory, databaseTopologyPreflightKey): renderDatabaseTopologyPreflightSQL(cluster),
		}
		for path, contents := range files {
			if err := os.Chmod(path, 0o644); err != nil {
				t.Fatalf("make database topology fixture writable: %v", err)
			}
			if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
				t.Fatalf("write database topology fixture: %v", err)
			}
			if err := os.Chmod(path, 0o444); err != nil {
				t.Fatalf("make database topology fixture read-only: %v", err)
			}
		}
	}
	conflictingLegacyGenesis := genesisCluster.DeepCopy()
	conflictingLegacyGenesis.Spec.Databases[0].Cells = []int32{1, 0}
	replaceDatabaseGenesis(conflictingLegacyGenesis)
	legacyBefore := fingerprint(legacyTopologyMismatchParent)
	legacyOutput, legacyErr := bootstrap(legacyTopologyMismatchParent, true, 2)
	if legacyErr == nil || !strings.Contains(legacyOutput, "RestoreTopologyMismatch: shardschema logical database topology conflicts") {
		t.Fatalf("legacy topology preflight error = %v\n%s", legacyErr, legacyOutput)
	}
	if legacyAfter := fingerprint(legacyTopologyMismatchParent); legacyAfter != legacyBefore {
		t.Fatalf("legacy topology mismatch mutated catalog before=%q after=%q", legacyBefore, legacyAfter)
	}
	if got := catalogSQL(legacyTopologyMismatchParent, "SELECT pg_catalog.to_regprocedure('pgshard_catalog.install_database_genesis(pgshard_catalog.sql_identifier,bigint[])') IS NULL"); got != "t" {
		t.Fatalf("legacy topology mismatch ran forward migration: %q", got)
	}
	replaceDatabaseGenesis(genesisCluster)
	const primaryDataParent = "/var/lib/postgresql/18-database-topology"
	if output, err := bootstrap(primaryDataParent, true, 2); err != nil {
		t.Fatalf("install declared database genesis: %v\n%s", err, output)
	}
	if got := catalogSQL(primaryDataParent, `
SELECT pg_catalog.string_agg(
         databases.database_name::text || ':' || ranges.range_start::text || ':' || shards.shard_number::text,
         ',' ORDER BY databases.database_name, ranges.range_start
       )
  FROM pgshard_catalog.logical_databases AS databases
  JOIN pgshard_catalog.active_routing_epochs AS active
	ON active.logical_database_id = databases.logical_database_id
  JOIN pgshard_catalog.routing_ranges AS ranges
	ON ranges.logical_database_id = active.logical_database_id
	AND ranges.routing_epoch = active.routing_epoch
  JOIN pgshard_catalog.database_shard_placements AS placements
	ON placements.logical_database_id = ranges.logical_database_id
	AND placements.database_shard_id = ranges.database_shard_id
	AND placements.state = 'active'
  JOIN pgshard_catalog.shards AS shards ON shards.shard_id = placements.shard_id`); got != "analytics:0:0,app:0:0,app:9223372036854775808:1" {
		t.Fatalf("installed database genesis topology = %q", got)
	}
	const emptyTopologyDataParent = "/var/lib/postgresql/18-empty-database-topology"
	if output, err := bootstrap(emptyTopologyDataParent, true, 2); err != nil {
		t.Fatalf("initialize empty-topology rejection fixture: %v\n%s", err, output)
	}
	catalogSQL(emptyTopologyDataParent, `
SET session_replication_role = replica;
DELETE FROM pgshard_catalog.active_routing_epochs;
DELETE FROM pgshard_catalog.routing_ranges;
DELETE FROM pgshard_catalog.routing_epochs;
DELETE FROM pgshard_catalog.database_shard_placements;
DELETE FROM pgshard_catalog.database_shards;
DELETE FROM pgshard_catalog.logical_databases;
SET session_replication_role = origin;
`)
	assertRejectedWithoutCatalogOrHBAMutation(
		emptyTopologyDataParent,
		2,
		"RestoreTopologyMismatch: shardschema logical database topology conflicts",
	)
	genesisEpoch := catalogSQL(primaryDataParent, "SELECT catalog_epoch FROM pgshard_catalog.cluster_state WHERE singleton")
	if output, err := bootstrap(primaryDataParent, true, 2); err != nil {
		t.Fatalf("replay exact database genesis: %v\n%s", err, output)
	}
	if replayedEpoch := catalogSQL(primaryDataParent, "SELECT catalog_epoch FROM pgshard_catalog.cluster_state WHERE singleton"); replayedEpoch != genesisEpoch {
		t.Fatalf("idempotent database genesis changed catalog epoch: before=%q after=%q", genesisEpoch, replayedEpoch)
	}
	catalogSQL(primaryDataParent, `
SET session_replication_role = replica;
UPDATE pgshard_catalog.database_shards
   SET shard_ordinal = 2
 WHERE logical_database_id = (SELECT logical_database_id FROM pgshard_catalog.logical_databases WHERE database_name = 'app')
   AND shard_ordinal = 0;
UPDATE pgshard_catalog.database_shards
   SET shard_ordinal = 0
 WHERE logical_database_id = (SELECT logical_database_id FROM pgshard_catalog.logical_databases WHERE database_name = 'app')
   AND shard_ordinal = 1;
UPDATE pgshard_catalog.database_shards
   SET shard_ordinal = 1
 WHERE logical_database_id = (SELECT logical_database_id FROM pgshard_catalog.logical_databases WHERE database_name = 'app')
   AND shard_ordinal = 2;
SET session_replication_role = origin;
`)
	assertRejectedWithoutCatalogOrHBAMutation(primaryDataParent, 2, "RestoreTopologyMismatch: shardschema logical database topology conflicts")
	catalogSQL(primaryDataParent, `
SET session_replication_role = replica;
UPDATE pgshard_catalog.database_shards
   SET shard_ordinal = 2
 WHERE logical_database_id = (SELECT logical_database_id FROM pgshard_catalog.logical_databases WHERE database_name = 'app')
   AND shard_ordinal = 0;
UPDATE pgshard_catalog.database_shards
   SET shard_ordinal = 0
 WHERE logical_database_id = (SELECT logical_database_id FROM pgshard_catalog.logical_databases WHERE database_name = 'app')
   AND shard_ordinal = 1;
UPDATE pgshard_catalog.database_shards
   SET shard_ordinal = 1
 WHERE logical_database_id = (SELECT logical_database_id FROM pgshard_catalog.logical_databases WHERE database_name = 'app')
   AND shard_ordinal = 2;
SET session_replication_role = origin;
`)
	conflictingGenesis := genesisCluster.DeepCopy()
	conflictingGenesis.Spec.Databases = append(
		conflictingGenesis.Spec.Databases,
		pgshardv1alpha1.DatabaseTemplate{Name: "aardvark", Shards: 1, Cells: []int32{0}},
	)
	conflictingGenesis.Spec.Databases[0].Cells = []int32{1, 0}
	replaceDatabaseGenesis(conflictingGenesis)
	topologySnapshot := func() string {
		t.Helper()
		return catalogSQL(primaryDataParent, `
SELECT state.catalog_epoch,
       (SELECT pg_catalog.string_agg(
                 databases.database_name::text || ':' || active.routing_epoch::text || ':' ||
                 ranges.range_start::text || ':' || ranges.range_end::text || ':' ||
                 ranges.database_shard_id::text || ':' || placements.shard_id::text,
                 ',' ORDER BY databases.database_name, ranges.range_start
               )
          FROM pgshard_catalog.logical_databases AS databases
          JOIN pgshard_catalog.active_routing_epochs AS active
            ON active.logical_database_id = databases.logical_database_id
          JOIN pgshard_catalog.routing_ranges AS ranges
		    ON ranges.logical_database_id = active.logical_database_id
		   AND ranges.routing_epoch = active.routing_epoch
		  JOIN pgshard_catalog.database_shard_placements AS placements
		    ON placements.logical_database_id = ranges.logical_database_id
		   AND placements.database_shard_id = ranges.database_shard_id
		   AND placements.state = 'active'),
		(SELECT pg_catalog.count(*) FROM pgshard_catalog.logical_databases),
		(SELECT pg_catalog.count(*) FROM pgshard_catalog.database_shards),
		(SELECT pg_catalog.count(*) FROM pgshard_catalog.database_shard_placements),
		(SELECT pg_catalog.count(*) FROM pgshard_catalog.routing_epochs),
		(SELECT pg_catalog.count(*) FROM pgshard_catalog.routing_ranges),
		(SELECT sequence_state.last_value::text || ':' || sequence_state.is_called::text
		   FROM pgshard_catalog.routing_epochs_routing_epoch_seq AS sequence_state)
  FROM pgshard_catalog.cluster_state AS state
 WHERE state.singleton`)
	}
	beforeConflict := topologySnapshot()
	conflictOutput, conflictErr := bootstrap(primaryDataParent, true, 2)
	if conflictErr == nil || !strings.Contains(conflictOutput, "RestoreTopologyMismatch: shardschema logical database topology conflicts") {
		t.Fatalf("conflicting multi-database genesis error = %v\n%s", conflictErr, conflictOutput)
	}
	if afterConflict := topologySnapshot(); afterConflict != beforeConflict {
		t.Fatalf("failed multi-database genesis changed catalog topology: before=%q after=%q", beforeConflict, afterConflict)
	}
	if got := catalogSQL(primaryDataParent, "SELECT count(*) FROM pgshard_catalog.logical_databases WHERE database_name = 'aardvark'"); got != "0" {
		t.Fatalf("failed multi-database genesis partially installed an earlier declaration: %q", got)
	}
	replaceDatabaseGenesis(genesisCluster)
	catalogSQL(primaryDataParent, "SELECT pgshard_catalog.install_database_genesis('undeclared'::pgshard_catalog.sql_identifier, ARRAY[0]::bigint[])")
	assertRejectedWithoutCatalogOrHBAMutation(primaryDataParent, 2, "RestoreTopologyMismatch: shardschema logical database topology conflicts")
	catalogSQL(primaryDataParent, `
SET session_replication_role = replica;
DELETE FROM pgshard_catalog.active_routing_epochs
 WHERE logical_database_id = (SELECT logical_database_id FROM pgshard_catalog.logical_databases WHERE database_name = 'undeclared');
DELETE FROM pgshard_catalog.routing_ranges
 WHERE routing_epoch IN (SELECT routing_epoch FROM pgshard_catalog.routing_epochs WHERE logical_database_id = (SELECT logical_database_id FROM pgshard_catalog.logical_databases WHERE database_name = 'undeclared'));
DELETE FROM pgshard_catalog.routing_epochs
 WHERE logical_database_id = (SELECT logical_database_id FROM pgshard_catalog.logical_databases WHERE database_name = 'undeclared');
DELETE FROM pgshard_catalog.logical_databases WHERE database_name = 'undeclared';
SET session_replication_role = origin;
`)
	if output, err := bootstrap(primaryDataParent, true, 2); err != nil {
		t.Fatalf("canonical topology was rejected after undeclared-database fixture cleanup: %v\n%s", err, output)
	}
	catalogSQL(primaryDataParent, "ALTER SEQUENCE pgshard_catalog.routing_epochs_routing_epoch_seq INCREMENT BY 2 CYCLE")
	assertRejectedWithoutCatalogOrHBAMutation(primaryDataParent, 2, "refusing an unsupported or malformed pre-existing shardschema catalog")
	catalogSQL(primaryDataParent, "ALTER SEQUENCE pgshard_catalog.routing_epochs_routing_epoch_seq INCREMENT BY 1 NO CYCLE")
	if output, err := bootstrap(primaryDataParent, true, 2); err != nil {
		t.Fatalf("canonical identity sequence was not restored: %v\n%s", err, output)
	}
	catalogSQL(primaryDataParent, `
INSERT INTO pgshard_catalog.registered_tables(
  logical_database_id,
  schema_name,
  table_name,
  shard_key_column,
  shard_key_type
)
SELECT
  logical_database_id,
  'public',
  'sequence_progress',
  'id',
  'bigint'
FROM pgshard_catalog.logical_databases
WHERE database_name = 'app';
SELECT pg_catalog.setval(
  'pgshard_catalog.routing_epochs_routing_epoch_seq',
  (SELECT pg_catalog.max(routing_epoch) FROM pgshard_catalog.routing_epochs),
  false
);
`)
	assertRejectedWithoutCatalogOrHBAMutation(primaryDataParent, 2, "refusing shardschema identity sequence progress that conflicts with catalog rows")
	catalogSQL(primaryDataParent, `
SELECT pg_catalog.setval(
  'pgshard_catalog.routing_epochs_routing_epoch_seq',
  (SELECT pg_catalog.max(routing_epoch) FROM pgshard_catalog.routing_epochs),
  true
);
SELECT pg_catalog.setval(
  'pgshard_catalog.registered_tables_registered_table_id_seq',
  (SELECT pg_catalog.max(registered_table_id) FROM pgshard_catalog.registered_tables),
  false
);
`)
	assertRejectedWithoutCatalogOrHBAMutation(primaryDataParent, 2, "refusing shardschema identity sequence progress that conflicts with catalog rows")
	catalogSQL(primaryDataParent, `
SELECT pg_catalog.setval(
  'pgshard_catalog.registered_tables_registered_table_id_seq',
  (SELECT pg_catalog.max(registered_table_id) FROM pgshard_catalog.registered_tables),
  true
);
`)
	if output, err := bootstrap(primaryDataParent, true, 2); err != nil {
		t.Fatalf("safe identity sequence progress was rejected: %v\n%s", err, output)
	}
	catalogSQL(primaryDataParent, `
SELECT pg_catalog.setval(
  'pgshard_catalog.routing_epochs_routing_epoch_seq',
  (SELECT sequences.seqmax
     FROM pg_catalog.pg_sequence AS sequences
    WHERE sequences.seqrelid =
          'pgshard_catalog.routing_epochs_routing_epoch_seq'::pg_catalog.regclass),
  true
);
`)
	assertRejectedWithoutCatalogOrHBAMutation(primaryDataParent, 2, "refusing shardschema identity sequence progress that conflicts with catalog rows")
	catalogSQL(primaryDataParent, `
SELECT pg_catalog.setval(
  'pgshard_catalog.routing_epochs_routing_epoch_seq',
  (SELECT pg_catalog.max(routing_epoch) + 1
     FROM pgshard_catalog.routing_epochs),
  false
);
SELECT pg_catalog.setval(
  'pgshard_catalog.registered_tables_registered_table_id_seq',
  (SELECT sequences.seqmax
     FROM pg_catalog.pg_sequence AS sequences
    WHERE sequences.seqrelid =
          'pgshard_catalog.registered_tables_registered_table_id_seq'::pg_catalog.regclass),
  true
);
`)
	assertRejectedWithoutCatalogOrHBAMutation(primaryDataParent, 2, "refusing shardschema identity sequence progress that conflicts with catalog rows")
	catalogSQL(primaryDataParent, `
SELECT pg_catalog.setval(
  'pgshard_catalog.registered_tables_registered_table_id_seq',
  (SELECT pg_catalog.max(registered_table_id) + 1
     FROM pgshard_catalog.registered_tables),
  false
);
`)
	if output, err := bootstrap(primaryDataParent, true, 2); err != nil {
		t.Fatalf("repaired exhausted identity sequences were rejected: %v\n%s", err, output)
	}

	catalogSQL(primaryDataParent, `
CREATE FUNCTION public.pgshard_rejected_event_trigger()
RETURNS event_trigger
LANGUAGE plpgsql
AS $function$
BEGIN
  NULL;
END
$function$;
CREATE EVENT TRIGGER pgshard_rejected_event_trigger
ON ddl_command_start
EXECUTE FUNCTION public.pgshard_rejected_event_trigger();
`)
	assertRejectedWithoutCatalogOrHBAMutation(primaryDataParent, 2, "pre-existing shardschema contains an unsupported event trigger")
	catalogSQL(primaryDataParent, `
DROP EVENT TRIGGER pgshard_rejected_event_trigger;
DROP FUNCTION public.pgshard_rejected_event_trigger();
`)
	if output, err := bootstrap(primaryDataParent, true, 2); err != nil {
		t.Fatalf("canonical database-wide trigger set was rejected: %v\n%s", err, output)
	}
	catalogSQL(primaryDataParent, `
CREATE TABLE public.pgshard_login_observations(observed boolean NOT NULL);
CREATE FUNCTION public.pgshard_rejected_login_trigger()
RETURNS event_trigger
LANGUAGE plpgsql
AS $function$
BEGIN
  INSERT INTO public.pgshard_login_observations VALUES (true);
END
$function$;
CREATE EVENT TRIGGER pgshard_rejected_login_trigger
ON login
EXECUTE FUNCTION public.pgshard_rejected_login_trigger();
`)
	assertRejectedWithoutCatalogOrHBAMutation(primaryDataParent, 2, "pre-existing shardschema contains an unsupported event trigger")
	if got := catalogSQL(primaryDataParent, "SELECT count(*) FROM public.pgshard_login_observations"); got != "0" {
		t.Fatalf("login event trigger ran before catalog rejection: %q", got)
	}
	catalogSQL(primaryDataParent, `
DROP EVENT TRIGGER pgshard_rejected_login_trigger;
DROP FUNCTION public.pgshard_rejected_login_trigger();
DROP TABLE public.pgshard_login_observations;
`)
	if output, err := bootstrap(primaryDataParent, true, 2); err != nil {
		t.Fatalf("canonical login-trigger set was rejected: %v\n%s", err, output)
	}

	catalogSQL(primaryDataParent, "CREATE RULE pgshard_rejected_rule AS ON INSERT TO pgshard_catalog.shards DO INSTEAD NOTHING")
	assertRejectedWithoutCatalogOrHBAMutation(primaryDataParent, 2, "refusing an unsupported or malformed pre-existing shardschema catalog")
	catalogSQL(primaryDataParent, "DROP RULE pgshard_rejected_rule ON pgshard_catalog.shards")
	if output, err := bootstrap(primaryDataParent, true, 2); err != nil {
		t.Fatalf("canonical rewrite-rule set was not restored: %v\n%s", err, output)
	}

	catalogSQL(primaryDataParent, `
DO $pgshard_disable_internal_trigger$
DECLARE
  internal_trigger name;
BEGIN
  SELECT triggers.tgname
    INTO STRICT internal_trigger
    FROM pg_catalog.pg_trigger AS triggers
   WHERE triggers.tgrelid = 'pgshard_catalog.routing_ranges'::pg_catalog.regclass
     AND triggers.tgisinternal
   ORDER BY triggers.oid
   LIMIT 1;
  EXECUTE pg_catalog.format(
    'ALTER TABLE pgshard_catalog.routing_ranges DISABLE TRIGGER %I',
    internal_trigger
  );
END
$pgshard_disable_internal_trigger$;
`)
	assertRejectedWithoutCatalogOrHBAMutation(primaryDataParent, 2, "refusing an unsupported or malformed pre-existing shardschema catalog")
	catalogSQL(primaryDataParent, "ALTER TABLE pgshard_catalog.routing_ranges ENABLE TRIGGER ALL")

	if output, err := bootstrap(primaryDataParent, true, 2); err != nil {
		t.Fatalf("canonical catalog was not restored before GUC coverage: %v\n%s", err, output)
	}
	catalogSQL(primaryDataParent, "ALTER DATABASE shardschema SET search_path TO pgshard_catalog, pg_catalog, public")
	if output, err := bootstrap(primaryDataParent, true, 2); err != nil {
		t.Fatalf("canonical catalog was rejected under a noncanonical database search_path: %v\n%s", err, output)
	}
	catalogSQL(primaryDataParent, "ALTER DATABASE shardschema RESET search_path; ALTER ROLE postgres IN DATABASE shardschema SET quote_all_identifiers TO on")
	if output, err := bootstrap(primaryDataParent, true, 2); err != nil {
		t.Fatalf("canonical catalog was rejected under noncanonical role identifier quoting: %v\n%s", err, output)
	}
	catalogSQL(primaryDataParent, "ALTER ROLE postgres IN DATABASE shardschema RESET quote_all_identifiers")

	catalogSQL(primaryDataParent, `
CREATE OR REPLACE FUNCTION pgshard_catalog.lock_catalog_state()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
BEGIN
  RAISE EXCEPTION 'pre-existing trigger function body executed';
END
$function$;
`)
	if output, err := bootstrap(primaryDataParent, true, 2); err != nil {
		t.Fatalf("bootstrap executed a pre-existing trigger function body: %v\n%s", err, output)
	}
	if got := catalogSQL(primaryDataParent, "SELECT pg_catalog.strpos(pg_catalog.pg_get_functiondef('pgshard_catalog.lock_catalog_state()'::pg_catalog.regprocedure), 'pre-existing trigger function body executed')"); got != "0" {
		t.Fatalf("bootstrap retained the pre-existing trigger function body: %q", got)
	}

	catalogSQL(primaryDataParent, `
ALTER TABLE pgshard_catalog.cluster_configuration DISABLE TRIGGER USER;
UPDATE pgshard_catalog.cluster_configuration SET home_shard_id = 'shard-0001' WHERE singleton;
ALTER TABLE pgshard_catalog.cluster_configuration ENABLE TRIGGER USER;
`)
	assertRejectedWithoutCatalogOrHBAMutation(primaryDataParent, 2, "refusing shardschema home-shard identity")
	catalogSQL(primaryDataParent, `
ALTER TABLE pgshard_catalog.cluster_configuration DISABLE TRIGGER USER;
UPDATE pgshard_catalog.cluster_configuration SET home_shard_id = 'shard-0000' WHERE singleton;
ALTER TABLE pgshard_catalog.cluster_configuration ENABLE TRIGGER USER;
`)

	catalogSQL(primaryDataParent, `
ALTER TABLE pgshard_catalog.shard_restore_incarnations DISABLE TRIGGER ALL;
INSERT INTO pgshard_catalog.shard_restore_incarnations(
  restore_incarnation,
  shard_id,
  state
)
VALUES ('33333333-3333-3333-3333-333333333333', 'ghost-shard', 'active');
ALTER TABLE pgshard_catalog.shard_restore_incarnations ENABLE TRIGGER ALL;
`)
	assertRejectedWithoutCatalogOrHBAMutation(primaryDataParent, 2, "refusing shardschema restore lineage")
	catalogSQL(primaryDataParent, `
ALTER TABLE pgshard_catalog.shard_restore_incarnations DISABLE TRIGGER ALL;
DELETE FROM pgshard_catalog.shard_restore_incarnations
 WHERE restore_incarnation = '33333333-3333-3333-3333-333333333333';
ALTER TABLE pgshard_catalog.shard_restore_incarnations ENABLE TRIGGER ALL;
`)

	catalogSQL(primaryDataParent, `
ALTER TABLE pgshard_catalog.shard_restore_incarnations DISABLE TRIGGER USER;
DELETE FROM pgshard_catalog.shard_restore_incarnations WHERE shard_id = 'shard-0000' AND state = 'active';
ALTER TABLE pgshard_catalog.shard_restore_incarnations ENABLE TRIGGER USER;
`)
	assertRejectedWithoutCatalogOrHBAMutation(primaryDataParent, 2, "refusing shardschema restore lineage")
	catalogSQL(primaryDataParent, `
INSERT INTO pgshard_catalog.shard_restore_incarnations(restore_incarnation, shard_id, state)
VALUES ('11111111-1111-1111-1111-111111111111', 'shard-0000', 'active');
`)

	catalogSQL(primaryDataParent, `
ALTER TABLE pgshard_catalog.shards DISABLE TRIGGER USER;
INSERT INTO pgshard_catalog.shards(shard_id, shard_number, state) VALUES ('shard-10000', 10000, 'retired');
ALTER TABLE pgshard_catalog.shards ENABLE TRIGGER USER;
`)
	assertRejectedWithoutCatalogOrHBAMutation(primaryDataParent, 2, "refusing shardschema restore lineage")
	catalogSQL(primaryDataParent, `
ALTER TABLE pgshard_catalog.shard_restore_incarnations DISABLE TRIGGER USER;
INSERT INTO pgshard_catalog.shard_restore_incarnations(restore_incarnation, shard_id, state, retired_at)
VALUES ('22222222-2222-2222-2222-222222222222', 'shard-10000', 'retired', statement_timestamp());
ALTER TABLE pgshard_catalog.shard_restore_incarnations ENABLE TRIGGER USER;
`)
	if output, err := bootstrap(primaryDataParent, true, 2); err != nil {
		t.Fatalf("canonical five-digit retired shard was rejected: %v\n%s", err, output)
	}
	catalogSQL(primaryDataParent, `
ALTER TABLE pgshard_catalog.shards DISABLE TRIGGER USER;
INSERT INTO pgshard_catalog.shards(shard_id, shard_number, state) VALUES ('shard-1000', 10001, 'retired');
ALTER TABLE pgshard_catalog.shards ENABLE TRIGGER USER;
`)
	assertRejectedWithoutCatalogOrHBAMutation(primaryDataParent, 2, "RestoreTopologyMismatch")
	catalogSQL(primaryDataParent, `
ALTER TABLE pgshard_catalog.shards DISABLE TRIGGER USER;
DELETE FROM pgshard_catalog.shards WHERE shard_number = 10001;
ALTER TABLE pgshard_catalog.shards ENABLE TRIGGER USER;
`)

	catalogSQL(primaryDataParent, `
BEGIN;
LOCK TABLE pgshard_catalog.shards IN ACCESS EXCLUSIVE MODE;
PREPARE TRANSACTION 'pgshard_bootstrap_lock';
`)
	lockContainer := fmt.Sprintf("pgshard-bootstrap-lock-%d-%d", os.Getpid(), time.Now().UnixNano())
	started := time.Now()
	output, err := runContainerWithTimeout(lockContainer, primaryDataParent, bootstrapScript(primaryDataParent), 20*time.Second, bootstrapEnvironment(true, 2)...)
	if err == nil || !strings.Contains(output, "canceling statement due to lock timeout") {
		t.Fatalf("prepared catalog lock was not bounded by lock_timeout after %s: %v\n%s", time.Since(started), err, output)
	}
	if elapsed := time.Since(started); elapsed >= 20*time.Second {
		t.Fatalf("prepared catalog lock exceeded bounded retry window: %s", elapsed)
	}

	crashContainer := fmt.Sprintf("pgshard-bootstrap-crash-%d-%d", os.Getpid(), time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = runDocker("rm", "--force", crashContainer)
	})
	crashBootstrapScript := strings.Replace(bootstrapScript(primaryDataParent), "lock_timeout=5s", "lock_timeout=30s", 1)
	crashArguments := append([]string{"run", "--detach", "--name", crashContainer}, containerArguments(primaryDataParent, crashBootstrapScript, true, bootstrapEnvironment(true, 2)...)...)
	if output, err := runDocker(crashArguments...); err != nil {
		t.Fatalf("start crash-retry bootstrap container: %v\n%s", err, output)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		lockWaiters, err := runDocker(
			"exec", crashContainer,
			"psql", "-X", "--no-password", "--host=/tmp/pgshard-catalog-bootstrap", "--username=postgres", "--dbname=shardschema", "--no-align", "--tuples-only",
			"--command=SELECT pg_catalog.count(*) FROM pg_catalog.pg_stat_activity WHERE datname = 'shardschema' AND wait_event_type = 'Lock'",
		)
		if err == nil && strings.TrimSpace(lockWaiters) == "1" {
			break
		}
		if time.Now().After(deadline) {
			logs, _ := runDocker("logs", crashContainer)
			t.Fatalf("temporary postmaster did not start before crash injection:\n%s", logs)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if output, err := runDocker("kill", "--signal", "KILL", crashContainer); err != nil {
		t.Fatalf("SIGKILL bootstrap container: %v\n%s", err, output)
	}
	if output, err := runDocker("rm", "--force", crashContainer); err != nil {
		t.Fatalf("remove killed bootstrap container: %v\n%s", err, output)
	}
	catalogSQL(primaryDataParent, "ROLLBACK PREPARED 'pgshard_bootstrap_lock'")
	if output, err := bootstrap(primaryDataParent, true, 2); err != nil {
		t.Fatalf("bootstrap did not recover after forced container death: %v\n%s", err, output)
	}
	if got := catalogSQL(primaryDataParent, "SELECT count(*) FILTER (WHERE state = 'active'), (SELECT count(*) FROM pgshard_catalog.shard_restore_incarnations WHERE state = 'active'), count(*) FILTER (WHERE shard_id = 'shard-10000' AND shard_number = 10000 AND state = 'retired') FROM pgshard_catalog.shards"); got != "2|2|1" {
		t.Fatalf("post-recovery catalog inventory = %q", got)
	}

	assertGenesisCrashRetry := func(dataParent, interruptedScript, probeSQL, wantProbe, boundary string, prepareRetry func()) {
		t.Helper()
		containerName := fmt.Sprintf("pgshard-genesis-crash-%d-%d", os.Getpid(), time.Now().UnixNano())
		t.Cleanup(func() {
			_, _ = runDocker("rm", "--force", containerName)
		})
		arguments := append(
			[]string{"run", "--detach", "--name", containerName},
			containerArguments(dataParent, interruptedScript, true, bootstrapEnvironment(true, 2)...)...,
		)
		if output, err := runDocker(arguments...); err != nil {
			t.Fatalf("start %s crash fixture: %v\n%s", boundary, err, output)
		}
		deadline := time.Now().Add(30 * time.Second)
		for {
			observed, err := runDocker(
				"exec", containerName,
				"psql", "-X", "--no-password", "--host=/tmp/pgshard-catalog-bootstrap", "--username=postgres", "--dbname=shardschema", "--no-align", "--tuples-only",
				"--command="+probeSQL,
			)
			if err == nil && strings.TrimSpace(observed) == wantProbe {
				break
			}
			if time.Now().After(deadline) {
				logs, _ := runDocker("logs", containerName)
				t.Fatalf("%s did not become externally durable before crash injection: last probe error=%v output=%q\n%s", boundary, err, observed, logs)
			}
			time.Sleep(100 * time.Millisecond)
		}
		logs, _ := runDocker("logs", containerName)
		if strings.Contains(logs, catalogPassword) {
			t.Fatalf("%s logged the catalog password before forced death:\n%s", boundary, logs)
		}
		if output, err := runDocker("kill", "--signal", "KILL", containerName); err != nil {
			t.Fatalf("SIGKILL %s bootstrap container: %v\n%s", boundary, err, output)
		}
		if output, err := runDocker("rm", "--force", containerName); err != nil {
			t.Fatalf("remove killed %s bootstrap container: %v\n%s", boundary, err, output)
		}
		if output, err := runContainer(dataParent, `set -Eeuo pipefail
test -f "$PGDATA/.pgshard-catalog-genesis-intent"
test ! -L "$PGDATA/.pgshard-catalog-genesis-intent"
`); err != nil {
			t.Fatalf("%s did not preserve the durable genesis intent before retry: %v\n%s", boundary, err, output)
		}
		if prepareRetry != nil {
			prepareRetry()
		}
		if output, err := bootstrap(dataParent, true, 2); err != nil {
			t.Fatalf("catalog genesis did not recover after forced death at %s: %v\n%s", boundary, err, output)
		}
		if got := catalogSQL(dataParent, "SELECT count(*) FILTER (WHERE state = 'active'), (SELECT count(*) FROM pgshard_catalog.shard_restore_incarnations WHERE state = 'active') FROM pgshard_catalog.shards"); got != "2|2" {
			t.Fatalf("recovered genesis inventory = %q", got)
		}
		if got := catalogSQL(dataParent, "SELECT (SELECT count(*) FROM pgshard_catalog.logical_databases WHERE state = 'active'), (SELECT count(*) FROM pgshard_catalog.routing_ranges AS ranges JOIN pgshard_catalog.active_routing_epochs AS active ON active.routing_epoch = ranges.routing_epoch)"); got != "2|3" {
			t.Fatalf("recovered database genesis topology = %q", got)
		}
		if output, err := runContainer(dataParent, `set -Eeuo pipefail
test ! -e "$PGDATA/.pgshard-catalog-genesis-intent"
`); err != nil {
			t.Fatalf("completed genesis retained its intent: %v\n%s", err, output)
		}
	}
	migrationCommand := `psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
  --set=ON_ERROR_STOP=1 --file="$PGSHARD_SHARDSCHEMA_MIGRATION"
`
	const migrationBoundaryDataParent = "/var/lib/postgresql/18-genesis-migration-boundary"
	migrationBoundaryScript := strings.Replace(
		bootstrapScript(migrationBoundaryDataParent),
		migrationCommand,
		migrationCommand+"while :; do sleep 1; done\n",
		1,
	)
	if migrationBoundaryScript == bootstrapScript(migrationBoundaryDataParent) {
		t.Fatal("catalog migration boundary injection did not match the bootstrap script")
	}
	assertGenesisCrashRetry(
		migrationBoundaryDataParent,
		migrationBoundaryScript,
		"SELECT pg_catalog.to_regclass('pgshard_catalog.shards') IS NOT NULL",
		"t",
		"catalog migration commit",
		nil,
	)

	const unreachablePartialDataParent = "/var/lib/postgresql/18-genesis-unreachable-partial"
	unreachablePartialScript := strings.Replace(
		bootstrapScript(unreachablePartialDataParent),
		migrationCommand,
		migrationCommand+"while :; do sleep 1; done\n",
		1,
	)
	if unreachablePartialScript == bootstrapScript(unreachablePartialDataParent) {
		t.Fatal("unreachable partial genesis injection did not match the bootstrap script")
	}
	partialContainer := fmt.Sprintf("pgshard-genesis-partial-%d-%d", os.Getpid(), time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = runDocker("rm", "--force", partialContainer)
	})
	partialArguments := append(
		[]string{"run", "--detach", "--name", partialContainer},
		containerArguments(unreachablePartialDataParent, unreachablePartialScript, true, bootstrapEnvironment(true, 3)...)...,
	)
	if output, err := runDocker(partialArguments...); err != nil {
		t.Fatalf("start unreachable partial genesis fixture: %v\n%s", err, output)
	}
	partialDeadline := time.Now().Add(30 * time.Second)
	for {
		observed, err := runDocker(
			"exec", partialContainer,
			"psql", "-X", "--no-password", "--host=/tmp/pgshard-catalog-bootstrap", "--username=postgres", "--dbname=shardschema", "--no-align", "--tuples-only",
			"--command=SELECT pg_catalog.to_regclass('pgshard_catalog.shards') IS NOT NULL",
		)
		if err == nil && strings.TrimSpace(observed) == "t" {
			break
		}
		if time.Now().After(partialDeadline) {
			logs, _ := runDocker("logs", partialContainer)
			t.Fatalf("unreachable partial fixture did not reach migration commit: last probe error=%v output=%q\n%s", err, observed, logs)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if output, err := runDocker("kill", "--signal", "KILL", partialContainer); err != nil {
		t.Fatalf("SIGKILL unreachable partial fixture: %v\n%s", err, output)
	}
	if output, err := runDocker("rm", "--force", partialContainer); err != nil {
		t.Fatalf("remove unreachable partial fixture: %v\n%s", err, output)
	}
	forgeUnreachablePartial := postgresHarness + `
psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
  --set=ON_ERROR_STOP=1 \
  --command="INSERT INTO pgshard_catalog.shards(shard_id, shard_number, state) VALUES ('shard-0002', 2, 'active')" >/dev/null
pg_ctl -D "$PGDATA" -w -t 45 stop -m fast >/dev/null
trap - EXIT
`
	if output, err := runContainer(unreachablePartialDataParent, forgeUnreachablePartial); err != nil {
		t.Fatalf("forge unreachable two-of-three genesis inventory: %v\n%s", err, output)
	}
	if output, err := bootstrap(unreachablePartialDataParent, true, 3); err == nil || !strings.Contains(output, "RestoreTopologyMismatch: shardschema inventory is not a reachable genesis state") {
		t.Fatalf("unreachable two-of-three genesis error = %v\n%s", err, output)
	}
	if output, err := runContainer(unreachablePartialDataParent, `set -Eeuo pipefail
test -f "$PGDATA/.pgshard-catalog-genesis-intent"
test ! -L "$PGDATA/.pgshard-catalog-genesis-intent"
`); err != nil {
		t.Fatalf("rejected unreachable genesis removed its recovery intent: %v\n%s", err, output)
	}
	catalogSQL(unreachablePartialDataParent, `
INSERT INTO pgshard_catalog.shards(shard_id, shard_number, state)
VALUES ('shard-0001', 1, 'active');
INSERT INTO pgshard_catalog.logical_databases(database_name)
VALUES ('app');
`)
	partialTopologyBefore := fingerprint(unreachablePartialDataParent)
	partialTopologyOutput, partialTopologyErr := bootstrap(unreachablePartialDataParent, true, 3)
	if partialTopologyErr == nil || !strings.Contains(partialTopologyOutput, "RestoreTopologyMismatch: shardschema logical database topology conflicts") {
		t.Fatalf("durable-intent partial database topology error = %v\n%s", partialTopologyErr, partialTopologyOutput)
	}
	if partialTopologyAfter := fingerprint(unreachablePartialDataParent); partialTopologyAfter != partialTopologyBefore {
		t.Fatalf("durable-intent partial database topology mutated catalog before=%q after=%q", partialTopologyBefore, partialTopologyAfter)
	}

	const inventoryTransactionDataParent = "/var/lib/postgresql/18-genesis-inventory-transaction"
	inventoryTransactionScript := strings.Replace(
		bootstrapScript(inventoryTransactionDataParent),
		" WHERE shards.shard_id IS NULL;\nDO \\$pgshard_inventory_postcondition\\$",
		" WHERE shards.shard_id IS NULL;\nSELECT pg_catalog.pg_sleep(600);\nDO \\$pgshard_inventory_postcondition\\$",
		1,
	)
	if inventoryTransactionScript == bootstrapScript(inventoryTransactionDataParent) {
		t.Fatal("catalog inventory transaction boundary injection did not match the bootstrap script")
	}
	assertGenesisCrashRetry(
		inventoryTransactionDataParent,
		inventoryTransactionScript,
		"SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = 'shardschema' AND wait_event = 'PgSleep' AND query = 'SELECT pg_catalog.pg_sleep(600);'",
		"1",
		"open catalog inventory transaction",
		nil,
	)

	const inventoryBoundaryDataParent = "/var/lib/postgresql/18-genesis-inventory-boundary"
	inventoryBoundaryScript := strings.Replace(
		bootstrapScript(inventoryBoundaryDataParent),
		"COMMIT;\nPGSHARD_SHARD_INVENTORY",
		"COMMIT;\nSELECT pg_catalog.pg_sleep(600);\nPGSHARD_SHARD_INVENTORY",
		1,
	)
	if inventoryBoundaryScript == bootstrapScript(inventoryBoundaryDataParent) {
		t.Fatal("catalog inventory boundary injection did not match the bootstrap script")
	}
	assertGenesisCrashRetry(
		inventoryBoundaryDataParent,
		inventoryBoundaryScript,
		"SELECT count(*) FILTER (WHERE state = 'active'), (SELECT count(*) FROM pgshard_catalog.shard_restore_incarnations WHERE state = 'active') FROM pgshard_catalog.shards",
		"2|2",
		"catalog inventory commit",
		nil,
	)

	canonicalDatabaseGenesis := renderDatabaseGenesisSQL(genesisCluster)
	writeDatabaseGenesis := func(contents string) {
		t.Helper()
		if err := os.Chmod(genesisPath, 0o644); err != nil {
			t.Fatalf("make crash-boundary database genesis writable: %v", err)
		}
		if err := os.WriteFile(genesisPath, []byte(contents), 0o644); err != nil {
			t.Fatalf("write crash-boundary database genesis: %v", err)
		}
		if err := os.Chmod(genesisPath, 0o444); err != nil {
			t.Fatalf("make crash-boundary database genesis read-only: %v", err)
		}
	}
	t.Cleanup(func() { writeDatabaseGenesis(canonicalDatabaseGenesis) })
	openDatabaseGenesis := strings.Replace(
		canonicalDatabaseGenesis,
		"DO $pgshard_database_genesis_postcondition$",
		"SELECT pg_catalog.pg_sleep(600);\nDO $pgshard_database_genesis_postcondition$",
		1,
	)
	if openDatabaseGenesis == canonicalDatabaseGenesis {
		t.Fatal("open database genesis transaction injection did not match")
	}
	writeDatabaseGenesis(openDatabaseGenesis)
	const openDatabaseGenesisParent = "/var/lib/postgresql/18-genesis-database-open"
	assertGenesisCrashRetry(
		openDatabaseGenesisParent,
		bootstrapScript(openDatabaseGenesisParent),
		"SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = 'shardschema' AND wait_event = 'PgSleep' AND query = 'SELECT pg_catalog.pg_sleep(600);'",
		"1",
		"open database genesis transaction",
		func() { writeDatabaseGenesis(canonicalDatabaseGenesis) },
	)

	committedDatabaseGenesis := strings.Replace(
		canonicalDatabaseGenesis,
		"COMMIT;\n",
		"COMMIT;\nSELECT pg_catalog.pg_sleep(600);\n",
		1,
	)
	if committedDatabaseGenesis == canonicalDatabaseGenesis {
		t.Fatal("database genesis commit boundary injection did not match")
	}
	writeDatabaseGenesis(committedDatabaseGenesis)
	const committedDatabaseGenesisParent = "/var/lib/postgresql/18-genesis-database-committed"
	assertGenesisCrashRetry(
		committedDatabaseGenesisParent,
		bootstrapScript(committedDatabaseGenesisParent),
		"SELECT (SELECT count(*) FROM pgshard_catalog.logical_databases WHERE state = 'active'), (SELECT count(*) FROM pgshard_catalog.routing_ranges AS ranges JOIN pgshard_catalog.active_routing_epochs AS active ON active.routing_epoch = ranges.routing_epoch)",
		"2|3",
		"database genesis commit",
		func() { writeDatabaseGenesis(canonicalDatabaseGenesis) },
	)

	const emptyDataParent = "/var/lib/postgresql/18-empty"
	if output, err := bootstrap(emptyDataParent, false, 2); err != nil {
		t.Fatalf("initialize malformed-catalog PGDATA: %v\n%s", err, output)
	}
	prepareMalformedCatalog := postgresHarness + `
createdb --no-password --host="$socket" --username=postgres --template=template0 --encoding=UTF8 shardschema
psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
  --set=ON_ERROR_STOP=1 --file=/tmp/v0_49_0_shardschema.sql >/dev/null
psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
  --set=ON_ERROR_STOP=1 --command="ALTER TABLE pgshard_catalog.cluster_configuration DROP COLUMN cluster_id" >/dev/null
pg_ctl -D "$PGDATA" -w -t 45 stop -m fast >/dev/null
trap - EXIT
`
	if output, err := runContainer(emptyDataParent, prepareMalformedCatalog); err != nil {
		t.Fatalf("prepare malformed complete catalog: %v\n%s", err, output)
	}
	if output, err := bootstrap(emptyDataParent, true, 2); err == nil || !strings.Contains(output, "refusing an unsupported or malformed pre-existing shardschema catalog") {
		t.Fatalf("malformed complete catalog error = %v\n%s", err, output)
	}
	recreateEmptyCatalog := postgresHarness + `
dropdb --no-password --host="$socket" --username=postgres shardschema
psql -X --no-password --host="$socket" --username=postgres --dbname=postgres \
  --set=ON_ERROR_STOP=1 --command="DROP ROLE IF EXISTS pgshard_catalog_admin, pgshard_catalog_reader, pgshard_catalog_owner" >/dev/null
createdb --no-password --host="$socket" --username=postgres --template=template0 --encoding=UTF8 shardschema
pg_ctl -D "$PGDATA" -w -t 45 stop -m fast >/dev/null
trap - EXIT
`
	if output, err := runContainer(emptyDataParent, recreateEmptyCatalog); err != nil {
		t.Fatalf("recreate catalog database after malformed shape: %v\n%s", err, output)
	}
	prepareReservedSchema := postgresHarness + `
psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
  --set=ON_ERROR_STOP=1 --command="CREATE SCHEMA pgshard_catalog; CREATE TABLE pgshard_catalog.unrelated(dummy integer)" >/dev/null
pg_ctl -D "$PGDATA" -w -t 45 stop -m fast >/dev/null
trap - EXIT
`
	if output, err := runContainer(emptyDataParent, prepareReservedSchema); err != nil {
		t.Fatalf("prepare occupied reserved schema: %v\n%s", err, output)
	}
	if output, err := bootstrap(emptyDataParent, true, 2); err == nil || !strings.Contains(output, "refusing a non-empty pre-existing pgshard_catalog schema") {
		t.Fatalf("occupied reserved schema error = %v\n%s", err, output)
	}
	if output, err := runContainer(emptyDataParent, recreateEmptyCatalog); err != nil {
		t.Fatalf("recreate catalog database after occupied schema: %v\n%s", err, output)
	}
	preparePartialCatalog := postgresHarness + `
psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
  --set=ON_ERROR_STOP=1 --command="CREATE SCHEMA pgshard_catalog; CREATE TABLE pgshard_catalog.cluster_configuration(dummy integer); CREATE TABLE pgshard_catalog.shards(dummy integer)" >/dev/null
pg_ctl -D "$PGDATA" -w -t 45 stop -m fast >/dev/null
trap - EXIT
`
	if output, err := runContainer(emptyDataParent, preparePartialCatalog); err != nil {
		t.Fatalf("prepare two-of-three partial catalog: %v\n%s", err, output)
	}
	if output, err := bootstrap(emptyDataParent, true, 2); err == nil || !strings.Contains(output, "refusing a partial pre-existing shardschema catalog") {
		t.Fatalf("two-of-three partial catalog error = %v\n%s", err, output)
	}
	if output, err := runContainer(emptyDataParent, recreateEmptyCatalog); err != nil {
		t.Fatalf("recreate empty catalog database: %v\n%s", err, output)
	}
	if output, err := bootstrap(emptyDataParent, true, 2); err == nil || !strings.Contains(output, "refusing an empty pre-existing shardschema without durable genesis evidence") {
		t.Fatalf("empty pre-existing catalog error = %v\n%s", err, output)
	}
	dropCatalogDatabase := postgresHarness + `
dropdb --no-password --host="$socket" --username=postgres shardschema
pg_ctl -D "$PGDATA" -w -t 45 stop -m fast >/dev/null
trap - EXIT
`
	if output, err := runContainer(emptyDataParent, dropCatalogDatabase); err != nil {
		t.Fatalf("drop empty catalog database: %v\n%s", err, output)
	}
	if output, err := bootstrap(emptyDataParent, true, 2); err == nil || !strings.Contains(output, "refusing pre-existing PGDATA without durable shardschema topology evidence") {
		t.Fatalf("absent restored catalog error = %v\n%s", err, output)
	}

	const replicaDefaultDataParent = "/var/lib/postgresql/18-replica-default"
	if output, err := bootstrap(replicaDefaultDataParent, true, 2); err != nil {
		t.Fatalf("initialize inherited-replica-role PGDATA: %v\n%s", err, output)
	}
	prepareReplicaRoleDefault := postgresHarness + `
psql -X --no-password --host="$socket" --username=postgres --dbname=postgres \
  --set=ON_ERROR_STOP=1 \
  --command="
    ALTER DATABASE shardschema SET log_statement = 'all';
    ALTER DATABASE shardschema SET log_min_error_statement = 'error';
    ALTER DATABASE shardschema SET log_min_duration_statement = 0;
    ALTER DATABASE shardschema SET log_min_duration_sample = 0;
    ALTER DATABASE shardschema SET log_statement_sample_rate = 1;
    ALTER DATABASE shardschema SET log_transaction_sample_rate = 1;
    ALTER DATABASE shardschema SET log_duration = on;
    ALTER DATABASE shardschema SET log_parameter_max_length = -1;
    ALTER DATABASE shardschema SET log_parameter_max_length_on_error = -1;
    ALTER DATABASE shardschema SET password_encryption = 'md5';
    ALTER DATABASE shardschema SET scram_iterations = 1024;
    ALTER DATABASE shardschema SET zero_damaged_pages = on;
    ALTER DATABASE shardschema SET ignore_checksum_failure = on;
    ALTER DATABASE shardschema SET session_preload_libraries = 'pgshard_missing_preload';
    ALTER DATABASE shardschema SET local_preload_libraries = 'pgshard_missing_preload';
    ALTER DATABASE shardschema SET default_transaction_read_only = on;
    ALTER ROLE postgres IN DATABASE shardschema SET session_replication_role = replica;
    ALTER ROLE postgres IN DATABASE shardschema SET synchronous_commit = off;
    ALTER ROLE postgres IN DATABASE shardschema SET zero_damaged_pages = on;
    ALTER ROLE postgres IN DATABASE shardschema SET ignore_checksum_failure = on;
    ALTER ROLE postgres IN DATABASE shardschema SET debug_print_parse = on;
    ALTER ROLE postgres IN DATABASE shardschema SET debug_print_rewritten = on;
    ALTER ROLE postgres IN DATABASE shardschema SET debug_print_plan = on;
    ALTER ROLE postgres IN DATABASE shardschema SET log_parser_stats = on;
  " >/dev/null
pg_ctl -D "$PGDATA" -w -t 45 stop -m fast >/dev/null
trap - EXIT
`
	if output, err := runContainer(replicaDefaultDataParent, prepareReplicaRoleDefault); err != nil {
		t.Fatalf("prepare inherited replica session role: %v\n%s", err, output)
	}
	if output, err := bootstrap(replicaDefaultDataParent, true, 2); err != nil {
		t.Fatalf("bootstrap under inherited replica session role: %v\n%s", err, output)
	}
	if got := catalogSQL(replicaDefaultDataParent, "SELECT count(*) FILTER (WHERE state = 'active'), (SELECT count(*) FROM pgshard_catalog.shard_restore_incarnations WHERE state = 'active') FROM pgshard_catalog.shards"); got != "2|2" {
		t.Fatalf("inherited replica role bypassed exact inventory validation: %q", got)
	}
	if got := catalogSQL(replicaDefaultDataParent, "SELECT rolpassword LIKE 'SCRAM-SHA-256$4096:%' FROM pg_catalog.pg_authid WHERE rolname = 'pgshard_pooler_catalog'"); got != "t" {
		t.Fatalf("hostile restored password_encryption created a non-SCRAM catalog login: %q", got)
	}
	if got := catalogRoleSQL(replicaDefaultDataParent, `
SELECT CASE WHEN
  current_setting('search_path') = 'pg_catalog'
  AND current_setting('default_transaction_read_only') = 'off'
  AND current_setting('synchronous_commit') = 'on'
  AND current_setting('row_security') = 'off'
THEN 1 ELSE 0 END`); got != "1" {
		t.Fatalf("production catalog role inherited hostile restored settings: %q", got)
	}
	if got := catalogSQL(replicaDefaultDataParent, `
SELECT
  pg_catalog.count(*) FILTER (
    WHERE databases.datname = 'shardschema'
      AND roles.rolname = 'pgshard_pooler_catalog'
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
  ),
  pg_catalog.count(*) FILTER (
    WHERE (
        (settings.setrole = 0 AND databases.datname = 'shardschema')
        OR roles.rolname = 'pgshard_pooler_catalog'
      )
      AND NOT (
        databases.datname = 'shardschema'
        AND roles.rolname = 'pgshard_pooler_catalog'
      )
  )
FROM pg_catalog.pg_db_role_setting AS settings
LEFT JOIN pg_catalog.pg_database AS databases ON databases.oid = settings.setdatabase
LEFT JOIN pg_catalog.pg_roles AS roles ON roles.oid = settings.setrole`); got != "1|0" {
		t.Fatalf("catalog reader defaults were not canonicalized exactly: %q", got)
	}

	const hostileConfigDataParent = "/var/lib/postgresql/18-hostile-config"
	if output, err := bootstrap(hostileConfigDataParent, true, 2); err != nil {
		t.Fatalf("initialize hostile-config PGDATA: %v\n%s", err, output)
	}
	hostileBefore := catalogSQL(hostileConfigDataParent, "SELECT catalog_epoch, (SELECT count(*) FROM pgshard_catalog.shards), (SELECT count(*) FROM pgshard_catalog.shard_restore_incarnations) FROM pgshard_catalog.cluster_state WHERE singleton")
	prepareHostileAutoConfig := `set -Eeuo pipefail
cp -- "$PGDATA/postgresql.auto.conf" "$PGDATA/postgresql.auto.conf.pgshard-test"
printf '%s\n' \
  "shared_preload_libraries = 'pgshard_missing_preload'" \
  "session_preload_libraries = 'pgshard_missing_preload'" \
  "local_preload_libraries = 'pgshard_missing_preload'" \
  "archive_mode = 'on'" \
  "archive_command = 'touch ` + hostileConfigDataParent + `/docker/pgshard-hostile-command-executed'" \
  "archive_library = 'pgshard_missing_archive_library'" \
  "restore_command = 'touch ` + hostileConfigDataParent + `/docker/pgshard-hostile-command-executed'" \
  "recovery_end_command = 'touch ` + hostileConfigDataParent + `/docker/pgshard-hostile-command-executed'" \
  "data_directory = '/tmp/pgshard-hostile-data'" \
  "hba_file = '/tmp/pgshard-hostile-hba'" \
  "listen_addresses = '*'" \
  "fsync = 'off'" \
  "full_page_writes = 'off'" \
  "zero_damaged_pages = 'on'" \
  >> "$PGDATA/postgresql.auto.conf"
`
	if output, err := runContainer(hostileConfigDataParent, prepareHostileAutoConfig); err != nil {
		t.Fatalf("prepare hostile restored PostgreSQL settings: %v\n%s", err, output)
	}
	if output, err := bootstrap(hostileConfigDataParent, true, 2); err == nil || !strings.Contains(output, "refusing active settings in restored postgresql.auto.conf") {
		t.Fatalf("hostile restored PostgreSQL settings error = %v\n%s", err, output)
	}
	restoreSafeAutoConfig := `set -Eeuo pipefail
test ! -e "$PGDATA/pgshard-hostile-command-executed"
mv -- "$PGDATA/postgresql.auto.conf.pgshard-test" "$PGDATA/postgresql.auto.conf"
`
	if output, err := runContainer(hostileConfigDataParent, restoreSafeAutoConfig); err != nil {
		t.Fatalf("hostile restored settings executed or could not be removed: %v\n%s", err, output)
	}
	assertUnsafeStorageRejected := func(prepare, restore, want string) {
		t.Helper()
		if output, err := runContainer(hostileConfigDataParent, "set -Eeuo pipefail\n"+prepare); err != nil {
			t.Fatalf("prepare unsafe restored storage: %v\n%s", err, output)
		}
		bootstrapOutput, bootstrapErr := bootstrap(hostileConfigDataParent, true, 2)
		if output, err := runContainer(hostileConfigDataParent, "set -Eeuo pipefail\n"+restore); err != nil {
			t.Fatalf("restore safe storage fixture: %v\n%s", err, output)
		}
		if bootstrapErr == nil || !strings.Contains(bootstrapOutput, want) {
			t.Fatalf("unsafe restored storage error = %v, want %q\n%s", bootstrapErr, want, bootstrapOutput)
		}
	}
	assertUnsafeStorageRejected(
		`mv -- "$PGDATA/postgresql.auto.conf" "$PGDATA/postgresql.auto.conf.pgshard-test"
ln -s /tmp/pgshard-missing-auto-conf "$PGDATA/postgresql.auto.conf"
`,
		`rm -- "$PGDATA/postgresql.auto.conf"
mv -- "$PGDATA/postgresql.auto.conf.pgshard-test" "$PGDATA/postgresql.auto.conf"
`,
		"refusing an unsafe restored postgresql.auto.conf",
	)
	assertUnsafeStorageRejected(
		`chmod 000 "$PGDATA/postgresql.auto.conf"
`,
		`chmod 0600 "$PGDATA/postgresql.auto.conf"
`,
		"refusing postgresql.auto.conf that cannot be inspected safely",
	)
	assertUnsafeStorageRejected(
		`ln -s /tmp/pgshard-missing-standby-signal "$PGDATA/standby.signal"
`,
		`rm -- "$PGDATA/standby.signal"
`,
		"refusing PostgreSQL recovery state during primary bootstrap (standby.signal)",
	)
	assertUnsafeStorageRejected(
		`mv -- "$PGDATA/pg_tblspc" "$PGDATA/pg_tblspc.pgshard-test"
ln -s pg_wal "$PGDATA/pg_tblspc"
`,
		`rm -- "$PGDATA/pg_tblspc"
mv -- "$PGDATA/pg_tblspc.pgshard-test" "$PGDATA/pg_tblspc"
`,
		"refusing an unsafe PostgreSQL tablespace directory",
	)
	assertUnsafeStorageRejected(
		`chmod 000 "$PGDATA/pg_tblspc"
`,
		`chmod 0700 "$PGDATA/pg_tblspc"
`,
		"refusing a PostgreSQL tablespace directory that cannot be inspected safely",
	)
	if output, err := bootstrap(hostileConfigDataParent, true, 2); err != nil {
		t.Fatalf("safe storage was rejected after hostile fixtures: %v\n%s", err, output)
	}
	if hostileAfter := catalogSQL(hostileConfigDataParent, "SELECT catalog_epoch, (SELECT count(*) FROM pgshard_catalog.shards), (SELECT count(*) FROM pgshard_catalog.shard_restore_incarnations) FROM pgshard_catalog.cluster_state WHERE singleton"); hostileAfter != hostileBefore {
		t.Fatalf("rejected restored settings changed catalog: before=%q after=%q", hostileBefore, hostileAfter)
	}
}

func TestPlanIncludesSupportingAvailabilityControls(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	storageClass := "fast"
	cluster.Spec.Storage.StorageClassName = &storageClass
	plan, err := Plan(cluster, DefaultImages())
	if err != nil {
		t.Fatal(err)
	}

	serviceAccount := object[*corev1.ServiceAccount](t, plan, "demo-orchestrator")
	if serviceAccount.AutomountServiceAccountToken == nil || *serviceAccount.AutomountServiceAccountToken {
		t.Fatalf("orchestrator ServiceAccount token policy = %#v", serviceAccount.AutomountServiceAccountToken)
	}
	role := object[*rbacv1.Role](t, plan, "demo-orchestrator")
	if len(role.Rules) != 1 || !reflect.DeepEqual(role.Rules[0].APIGroups, []string{"coordination.k8s.io"}) || !reflect.DeepEqual(role.Rules[0].Resources, []string{"leases"}) || !reflect.DeepEqual(role.Rules[0].ResourceNames, []string{"demo-orch-lease"}) || !reflect.DeepEqual(role.Rules[0].Verbs, []string{"get", "update"}) {
		t.Fatalf("orchestrator Lease Role is broader than required: %#v", role.Rules)
	}
	roleBinding := object[*rbacv1.RoleBinding](t, plan, "demo-orchestrator")
	if roleBinding.RoleRef.APIGroup != rbacv1.GroupName || roleBinding.RoleRef.Kind != "Role" || roleBinding.RoleRef.Name != role.Name || len(roleBinding.Subjects) != 1 || roleBinding.Subjects[0].Kind != "ServiceAccount" || roleBinding.Subjects[0].Name != serviceAccount.Name || roleBinding.Subjects[0].Namespace != cluster.Namespace {
		t.Fatalf("orchestrator Lease RoleBinding = %#v", roleBinding)
	}
	lease := object[*coordinationv1.Lease](t, plan, "demo-orch-lease")
	if !metav1.IsControlledBy(lease, cluster) || lease.Spec.HolderIdentity != nil || lease.Spec.RenewTime != nil || lease.Spec.LeaseDurationSeconds != nil {
		t.Fatalf("operator must own only the empty Lease envelope: %#v", lease)
	}
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		name := PostgreSQLWritableLeaseName(cluster.Name, shard)
		writableLease := object[*coordinationv1.Lease](t, plan, name)
		if !metav1.IsControlledBy(writableLease, cluster) ||
			writableLease.Labels[ComponentLabel] != "postgresql" ||
			writableLease.Labels[ShardLabel] != shardLabel(shard) ||
			!reflect.DeepEqual(writableLease.Spec, coordinationv1.LeaseSpec{}) {
			t.Fatalf("PostgreSQL writable-term Lease %s is not an empty cell-bound envelope: %#v", name, writableLease)
		}
		if strings.Contains(name, "primary") || strings.Contains(name, "replica") {
			t.Fatalf("PostgreSQL writable-term Lease name encodes a mutable role: %s", name)
		}

		agentName := PostgreSQLAgentServiceAccountName(cluster.Name, shard)
		agentAccount := object[*corev1.ServiceAccount](t, plan, agentName)
		if agentAccount.AutomountServiceAccountToken == nil || *agentAccount.AutomountServiceAccountToken ||
			agentAccount.Labels[ComponentLabel] != "postgresql-agent" ||
			agentAccount.Labels[ShardLabel] != shardLabel(shard) {
			t.Fatalf("PostgreSQL agent ServiceAccount %s is not fail closed: %#v", agentName, agentAccount)
		}
		agentRole := object[*rbacv1.Role](t, plan, agentName)
		if len(agentRole.Rules) != 1 ||
			!reflect.DeepEqual(agentRole.Rules[0].APIGroups, []string{coordinationv1.GroupName}) ||
			!reflect.DeepEqual(agentRole.Rules[0].Resources, []string{"leases"}) ||
			!reflect.DeepEqual(agentRole.Rules[0].ResourceNames, []string{name}) ||
			!reflect.DeepEqual(agentRole.Rules[0].Verbs, []string{"get", "update"}) {
			t.Fatalf("PostgreSQL agent Role %s is broader than its exact Lease: %#v", agentName, agentRole.Rules)
		}
		agentBinding := object[*rbacv1.RoleBinding](t, plan, agentName)
		if agentBinding.RoleRef != (rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: agentName}) ||
			len(agentBinding.Subjects) != 1 ||
			agentBinding.Subjects[0] != (rbacv1.Subject{Kind: "ServiceAccount", Name: agentName, Namespace: cluster.Namespace}) {
			t.Fatalf("PostgreSQL agent RoleBinding %s crosses its cell identity: %#v", agentName, agentBinding)
		}
	}
	for _, planned := range plan {
		if planned.GetLabels()[ComponentLabel] == "etcd" || strings.Contains(planned.GetName(), "-etcd") {
			t.Fatalf("dedicated etcd resource remains in plan: %T %s", planned, planned.GetName())
		}
	}

	orchestrator := object[*appsv1.Deployment](t, plan, "demo-orchestrator")
	if *orchestrator.Spec.Replicas != 3 || orchestrator.Spec.Template.Spec.Containers[0].ReadinessProbe.HTTPGet.Path != "/readyz" || orchestrator.Spec.Template.Spec.Containers[0].ReadinessProbe.FailureThreshold != 1 {
		t.Fatalf("orchestrator spec = %#v", orchestrator.Spec)
	}
	orchestratorEnv := orchestrator.Spec.Template.Spec.Containers[0].Env
	if orchestratorEnv[1].Name != "PGSHARD_CLUSTER_UID" || orchestratorEnv[1].Value != string(cluster.UID) {
		t.Fatalf("orchestrator cluster incarnation is not UID-bound: %#v", orchestratorEnv[1])
	}
	if orchestrator.Spec.Template.Spec.ServiceAccountName != serviceAccount.Name || orchestrator.Spec.Template.Spec.AutomountServiceAccountToken == nil || !*orchestrator.Spec.Template.Spec.AutomountServiceAccountToken {
		t.Fatalf("orchestrator API identity = %#v", orchestrator.Spec.Template.Spec)
	}
	wantedFields := map[string]string{
		"PGSHARD_ORCH_ID":         "metadata.name",
		"PGSHARD_POD_UID":         "metadata.uid",
		"PGSHARD_LEASE_NAMESPACE": "metadata.namespace",
	}
	for _, variable := range orchestratorEnv {
		if field, wanted := wantedFields[variable.Name]; wanted {
			if variable.ValueFrom == nil || variable.ValueFrom.FieldRef == nil || variable.ValueFrom.FieldRef.FieldPath != field {
				t.Fatalf("orchestrator %s identity = %#v", variable.Name, variable)
			}
			delete(wantedFields, variable.Name)
		}
	}
	if len(wantedFields) != 0 || envValue(orchestratorEnv, "PGSHARD_LEASE_NAME") != lease.Name {
		t.Fatalf("orchestrator Lease environment is incomplete: missing=%#v env=%#v", wantedFields, orchestratorEnv)
	}
	pooler := object[*appsv1.Deployment](t, plan, "demo-pooler")
	poolerContainer := pooler.Spec.Template.Spec.Containers[0]
	if pooler.Spec.Template.Spec.AutomountServiceAccountToken == nil || *pooler.Spec.Template.Spec.AutomountServiceAccountToken {
		t.Fatalf("pooler unexpectedly receives a Kubernetes API token: %#v", pooler.Spec.Template.Spec.AutomountServiceAccountToken)
	}
	if pooler.Spec.Replicas != nil || len(poolerContainer.Ports) != 4 || poolerContainer.ReadinessProbe.HTTPGet.Path != "/readyz" || poolerContainer.ReadinessProbe.FailureThreshold != 1 || poolerContainer.LivenessProbe.HTTPGet.Path != "/healthz" || poolerContainer.LivenessProbe.FailureThreshold != 3 {
		t.Fatalf("pooler spec = %#v", pooler.Spec)
	}
	catalogModeCount := 0
	for _, variable := range poolerContainer.Env {
		switch variable.Name {
		case "PGSHARD_CATALOG_MODE":
			catalogModeCount++
			if variable.Value != "bootstrap-unavailable" {
				t.Fatalf("pooler catalog mode = %q, want bootstrap-unavailable", variable.Value)
			}
		case "PGSHARD_SHARDSCHEMA_DSN_FILE", "PGSHARD_RW_BACKEND_HOST":
			t.Fatalf("bootstrap pooler unexpectedly has %s", variable.Name)
		}
	}
	if catalogModeCount != 1 {
		t.Fatalf("pooler catalog mode count = %d, want 1", catalogModeCount)
	}
	hpa := object[*autoscalingv2.HorizontalPodAutoscaler](t, plan, "demo-pooler")
	if *hpa.Spec.MinReplicas != 2 || hpa.Spec.MaxReplicas != 6 || *hpa.Spec.Metrics[0].Resource.Target.AverageUtilization != 70 {
		t.Fatalf("HPA spec = %#v", hpa.Spec)
	}
	for _, component := range []string{"orchestrator", "pooler"} {
		pdb := object[*policyv1.PodDisruptionBudget](t, plan, "demo-"+component)
		if component == "pooler" {
			if pdb.Spec.MinAvailable == nil || pdb.Spec.MinAvailable.IntVal != 1 {
				t.Fatalf("%s PDB = %#v", component, pdb.Spec)
			}
		} else if pdb.Spec.MaxUnavailable == nil || pdb.Spec.MaxUnavailable.IntVal != 1 {
			t.Fatalf("%s PDB = %#v", component, pdb.Spec)
		}
	}
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		postgresqlPolicy := object[*networkingv1.NetworkPolicy](t, plan, shardName(cluster.Name, shard)+"-ingress")
		if postgresqlPolicy.Spec.PodSelector.MatchLabels[ShardLabel] != shardLabel(shard) || len(postgresqlPolicy.Spec.Ingress) != 2 || postgresqlPolicy.Spec.Ingress[0].Ports[0].Port.IntVal != PostgreSQLPort || postgresqlPolicy.Spec.Ingress[1].Ports[0].Port.IntVal != PostgreSQLPort {
			t.Fatalf("PostgreSQL NetworkPolicy = %#v", postgresqlPolicy.Spec)
		}
		controlPeers := postgresqlPolicy.Spec.Ingress[0].From
		if len(controlPeers) != 1 || controlPeers[0].PodSelector == nil || controlPeers[0].PodSelector.MatchLabels[ClusterLabel] != cluster.Name || len(controlPeers[0].PodSelector.MatchExpressions) != 1 || !reflect.DeepEqual(controlPeers[0].PodSelector.MatchExpressions[0].Values, []string{"orchestrator", "pooler"}) {
			t.Fatalf("PostgreSQL control peers = %#v", controlPeers)
		}
		postgresqlPeers := postgresqlPolicy.Spec.Ingress[1].From
		if len(postgresqlPeers) != 1 || postgresqlPeers[0].PodSelector == nil || postgresqlPeers[0].PodSelector.MatchLabels[ClusterLabel] != cluster.Name || postgresqlPeers[0].PodSelector.MatchLabels[ComponentLabel] != "postgresql" || postgresqlPeers[0].PodSelector.MatchLabels[ShardLabel] != shardLabel(shard) {
			t.Fatalf("PostgreSQL same-shard peers = %#v", postgresqlPeers)
		}
	}
}

func TestFixedPoolerPlanOmitsHPA(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	cluster.Spec.Pooler.Scaling = pgshardv1alpha1.PoolerScaling{
		Mode:  pgshardv1alpha1.ScalingFixed,
		Fixed: &pgshardv1alpha1.FixedScaling{Replicas: 4},
	}
	plan, err := Plan(cluster, DefaultImages())
	if err != nil {
		t.Fatal(err)
	}
	pooler := object[*appsv1.Deployment](t, plan, "demo-pooler")
	if *pooler.Spec.Replicas != 4 {
		t.Fatalf("pooler replicas = %d", *pooler.Spec.Replicas)
	}
	for _, item := range plan {
		if _, ok := item.(*autoscalingv2.HorizontalPodAutoscaler); ok {
			t.Fatal("fixed scaling plan contains an HPA")
		}
	}
}

func TestSingleFixedPoolerPDBProtectsTheOnlyReplica(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	cluster.Spec.Pooler.Scaling = pgshardv1alpha1.PoolerScaling{Mode: pgshardv1alpha1.ScalingFixed, Fixed: &pgshardv1alpha1.FixedScaling{Replicas: 1}}
	plan, err := Plan(cluster, DefaultImages())
	if err != nil {
		t.Fatal(err)
	}
	pdb := object[*policyv1.PodDisruptionBudget](t, plan, "demo-pooler")
	if pdb.Spec.MinAvailable == nil || pdb.Spec.MinAvailable.IntVal != 1 || pdb.Spec.MaxUnavailable != nil {
		t.Fatalf("single-replica PDB = %#v", pdb.Spec)
	}
}

func TestPlanFailsClosedForUnsafeIdentityOrMissingImages(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	cluster.Name = strings.Repeat("a", pgshardv1alpha1.MaximumClusterNameLength+1)
	if _, err := Plan(cluster, DefaultImages()); err == nil || !strings.Contains(err.Error(), "too long") {
		t.Fatalf("expected long-name error, got %v", err)
	}
	cluster = testCluster()
	images := DefaultImages()
	images.Pooler = ""
	if _, err := Plan(cluster, images); err == nil || !strings.Contains(err.Error(), "images") {
		t.Fatalf("expected image error, got %v", err)
	}
	images = DefaultImages()
	images.PostgreSQL = ""
	if _, err := Plan(cluster, images); err == nil || !strings.Contains(err.Error(), "images") {
		t.Fatalf("expected PostgreSQL image error, got %v", err)
	}
	images = DefaultImages()
	cluster = testCluster()
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	cluster.Status.PostgreSQLBootstraps = testPostgreSQLBootstraps(cluster)
	if _, err := Plan(cluster, images); err == nil || !strings.Contains(err.Error(), "bootstrap image") {
		t.Fatalf("expected PostgreSQL bootstrap image error, got %v", err)
	}
	cluster = testCluster()
	cluster.Spec.Observability.OpenTelemetryEndpoint = "file:///tmp/collector"
	if _, err := Plan(cluster, DefaultImages()); err == nil || !strings.Contains(strings.ToLower(err.Error()), "opentelemetry") {
		t.Fatalf("expected OpenTelemetry endpoint error, got %v", err)
	}
	cluster = testCluster()
	cluster.Spec.MembersPerShard = 1
	if _, err := Plan(cluster, DefaultImages()); err == nil || !strings.Contains(err.Error(), "synchronous durability") {
		t.Fatalf("expected defensive full-validation error, got %v", err)
	}
	cluster = testCluster()
	cluster.Spec.Backup.Repository.Filesystem.PersistentVolumeClaimName = "Bad_PVC"
	if _, err := Plan(cluster, DefaultImages()); err == nil || !strings.Contains(err.Error(), "persistentVolumeClaimName") {
		t.Fatalf("expected defensive backup validation error, got %v", err)
	}
	cluster = testCluster()
	invalidStorageClass := "BAD/NAME"
	cluster.Spec.Storage.StorageClassName = &invalidStorageClass
	if _, err := Plan(cluster, DefaultImages()); err == nil || !strings.Contains(err.Error(), "storageClassName") {
		t.Fatalf("expected defensive StorageClass validation error, got %v", err)
	}
}

func TestMultiMemberAgentPlanPublishesOnlyMemberZeroBootstrapSources(t *testing.T) {
	t.Parallel()
	for _, members := range []int32{3, 5} {
		members := members
		t.Run(fmt.Sprintf("members=%d", members), func(t *testing.T) {
			t.Parallel()
			cluster := testCluster()
			cluster.Spec.MembersPerShard = members
			cluster.Status.PostgreSQLBootstraps = testPostgreSQLBootstraps(cluster)
			cluster.Status.CatalogAccess = nil
			cluster.Status.PostgreSQLWritableLeases = testPostgreSQLWritableLeases(cluster)
			cluster.Status.PostgreSQLReplicationCredentials = testPostgreSQLReplicationCredentials(cluster)
			images := DevelopmentImages()
			images.PostgreSQLRuntime = PostgreSQLRuntimeAgentQuarantine

			plan, err := Plan(cluster, images)
			if err != nil {
				t.Fatal(err)
			}
			sources := 0
			for _, item := range plan {
				switch object := item.(type) {
				case *appsv1.StatefulSet:
					if object.Labels[ComponentLabel] != "postgresql" {
						continue
					}
					sources++
					if _, role := object.Spec.Template.Labels[RoleLabel]; role || object.Spec.Template.Labels[MemberLabel] != "0000" {
						t.Fatalf("bootstrap source %s labels = %#v", object.Name, object.Spec.Template.Labels)
					}
					if observed, err := ObservePostgreSQLRuntime(object.Spec.Template.Annotations, object.Spec.Template.Spec); err != nil || observed != PostgreSQLRuntimeAgentQuarantine {
						t.Fatalf("observe bootstrap source %s runtime = %q, %v", object.Name, observed, err)
					}
					agent := object.Spec.Template.Spec.Containers[0]
					if !containerHasLiteralEnvironment(agent, "PGSHARD_POSTGRES_MODE", "replication-bootstrap-primary") || !containerHasLiteralEnvironment(agent, "PGSHARD_POSTGRES_HBA_FILE", "/etc/pgshard/replication-bootstrap-primary.pg_hba.conf") {
						t.Fatalf("bootstrap source %s agent environment = %#v", object.Name, agent.Env)
					}
					if containsNamedVolumeMount(agent.VolumeMounts, "replication-credential") {
						t.Fatalf("bootstrap source %s agent retained the replication credential", object.Name)
					}
					bootstrapContainer := object.Spec.Template.Spec.InitContainers[0]
					if !containsVolumeMount(bootstrapContainer.VolumeMounts, "replication-credential", true) ||
						envValue(bootstrapContainer.Env, "PGSHARD_MEMBERS_PER_SHARD") != fmt.Sprintf("%d", members) {
						t.Fatalf("bootstrap source %s replication initialization = %#v", object.Name, bootstrapContainer)
					}
					var shard int32
					if _, err := fmt.Sscanf(object.Spec.Template.Labels[ShardLabel], "%04d", &shard); err != nil {
						t.Fatalf("bootstrap source %s shard label: %v", object.Name, err)
					}
					credential := cluster.Status.PostgreSQLReplicationCredentials[shard]
					if envValue(bootstrapContainer.Env, "PGSHARD_REPLICATION_MATERIAL_SHA256") != credential.MaterialSHA256 {
						t.Fatalf("bootstrap source %s replication digest environment = %#v", object.Name, bootstrapContainer.Env)
					}
					replicationVolume := volumeByName(t, object.Spec.Template.Spec.Volumes, "replication-credential").Secret
					if replicationVolume == nil || replicationVolume.SecretName != credential.SecretName || replicationVolume.DefaultMode == nil || *replicationVolume.DefaultMode != 0o440 || !reflect.DeepEqual(secretItemKeys(replicationVolume.Items), []string{PostgreSQLReplicationPasswordKey}) {
						t.Fatalf("bootstrap source %s replication projection = %#v", object.Name, replicationVolume)
					}
				case *policyv1.PodDisruptionBudget:
					if object.Labels[ComponentLabel] == "postgresql" {
						t.Fatalf("multi-member source storage published PostgreSQL PDB %s", object.Name)
					}
				case *corev1.Service:
					if object.Name == CatalogServiceName(cluster.Name) {
						t.Fatalf("multi-member source storage published catalog Service %s", object.Name)
					}
				}
			}
			if sources != int(cluster.Spec.Shards) {
				t.Fatalf("bootstrap source StatefulSets = %d, want %d", sources, cluster.Spec.Shards)
			}

			claim := PostgreSQLDataPVC(cluster, 0, "source-data", cluster.Spec.Storage.Size, cluster.Spec.Storage.StorageClassName, "source-fence", "source-fence-uid")
			if claim.Labels[MemberLabel] != "0000" {
				t.Fatalf("source-storage member label = %q", claim.Labels[MemberLabel])
			}
			if role, exists := claim.Labels[RoleLabel]; exists {
				t.Fatalf("non-serving source storage carries authorizing role label %q", role)
			}
			memberClaim := PostgreSQLMemberDataPVC(cluster, 0, members-1, "member-data", cluster.Spec.Storage.Size, cluster.Spec.Storage.StorageClassName, "member-fence", "member-fence-uid")
			memberSecret := PostgreSQLMemberAuthSecret(cluster, 0, members-1, "member-auth", []byte(strings.Repeat("a", 64)))
			wantMember := fmt.Sprintf("%04d", members-1)
			if memberClaim.Labels[MemberLabel] != wantMember || memberSecret.Labels[MemberLabel] != wantMember {
				t.Fatalf("member resource labels = claim %q secret %q, want %q", memberClaim.Labels[MemberLabel], memberSecret.Labels[MemberLabel], wantMember)
			}
			if PostgreSQLMemberDataPVCPrefix(cluster.Name, 0, 0) == PostgreSQLMemberDataPVCPrefix(cluster.Name, 0, members-1) || PostgreSQLMemberAuthSecretPrefix(cluster.Name, 0, 0) == PostgreSQLMemberAuthSecretPrefix(cluster.Name, 0, members-1) {
				t.Fatal("distinct members share a bootstrap resource prefix")
			}
		})
	}
}

func TestReplicationBootstrapSourcePodClassificationIsExact(t *testing.T) {
	t.Parallel()
	pod := testReplicationBootstrapSourcePod(t)
	if !IsPostgreSQLReplicationBootstrapSourcePod(pod) {
		t.Fatalf("planned role-neutral source Pod was not recognized: %#v", pod.ObjectMeta)
	}
	for _, test := range []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{name: "present role", mutate: func(pod *corev1.Pod) { pod.Labels[RoleLabel] = "primary" }},
		{name: "empty role", mutate: func(pod *corev1.Pod) { pod.Labels[RoleLabel] = "" }},
		{name: "different member", mutate: func(pod *corev1.Pod) { pod.Labels[MemberLabel] = "0001" }},
		{name: "different shard", mutate: func(pod *corev1.Pod) { pod.Labels[ShardLabel] = "0001" }},
		{name: "noncanonical shard", mutate: func(pod *corev1.Pod) { pod.Labels[ShardLabel] = "0" }},
		{name: "different name", mutate: func(pod *corev1.Pod) { pod.Name += "-other" }},
		{name: "different service account", mutate: func(pod *corev1.Pod) { pod.Spec.ServiceAccountName += "-other" }},
		{name: "direct runtime", mutate: func(pod *corev1.Pod) { pod.Annotations[PostgreSQLRuntimeAnnotation] = string(PostgreSQLRuntimeDirect) }},
		{name: "quarantine mode", mutate: func(pod *corev1.Pod) {
			for index := range pod.Spec.Containers[0].Env {
				if pod.Spec.Containers[0].Env[index].Name == "PGSHARD_POSTGRES_MODE" {
					pod.Spec.Containers[0].Env[index].Value = "quarantine"
				}
				if pod.Spec.Containers[0].Env[index].Name == "PGSHARD_POSTGRES_HBA_FILE" {
					pod.Spec.Containers[0].Env[index].Value = "/etc/pgshard/quarantine.pg_hba.conf"
				}
			}
		}},
		{name: "duplicate mode", mutate: func(pod *corev1.Pod) {
			pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, corev1.EnvVar{Name: "PGSHARD_POSTGRES_MODE", Value: "replication-bootstrap-primary"})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			changed := pod.DeepCopy()
			test.mutate(changed)
			if IsPostgreSQLReplicationBootstrapSourcePod(changed) {
				t.Fatalf("changed Pod retained bootstrap-source identity: %#v", changed.ObjectMeta)
			}
		})
	}
}

func testReplicationBootstrapSourcePod(t *testing.T) *corev1.Pod {
	t.Helper()
	cluster := testCluster()
	cluster.Spec.MembersPerShard = 3
	cluster.Status.PostgreSQLBootstraps = testPostgreSQLBootstraps(cluster)
	cluster.Status.CatalogAccess = nil
	cluster.Status.PostgreSQLWritableLeases = testPostgreSQLWritableLeases(cluster)
	cluster.Status.PostgreSQLReplicationCredentials = testPostgreSQLReplicationCredentials(cluster)
	images := DevelopmentImages()
	images.PostgreSQLRuntime = PostgreSQLRuntimeAgentQuarantine
	plan, err := Plan(cluster, images)
	if err != nil {
		t.Fatal(err)
	}
	name := PostgreSQLMemberStatefulSetName(cluster.Name, 0, 0)
	for _, item := range plan {
		statefulSet, ok := item.(*appsv1.StatefulSet)
		if !ok || statefulSet.Name != name {
			continue
		}
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name + "-0",
				Namespace:   cluster.Namespace,
				Labels:      maps.Clone(statefulSet.Spec.Template.Labels),
				Annotations: maps.Clone(statefulSet.Spec.Template.Annotations),
				Finalizers:  slices.Clone(statefulSet.Spec.Template.Finalizers),
			},
			Spec: *statefulSet.Spec.Template.Spec.DeepCopy(),
		}
	}
	t.Fatalf("plan has no bootstrap source StatefulSet %s", name)
	return nil
}

func TestMultiMemberAgentSourceStorageRequiresImmutableBootstrapImage(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	cluster.Status.PostgreSQLWritableLeases = testPostgreSQLWritableLeases(cluster)
	images := DefaultImages()
	images.PostgreSQLRuntime = PostgreSQLRuntimeAgentQuarantine
	for _, image := range []string{"", "ghcr.io/andrew01234567890/pgshard-postgres-agent:main"} {
		images.PostgreSQLBootstrap = image
		if _, err := Plan(cluster, images); err == nil || !strings.Contains(err.Error(), "bootstrap image") {
			t.Fatalf("mutable multi-member bootstrap image %q error = %v", image, err)
		}
	}

	// Direct multi-member planning neither creates nor validates source storage.
	if _, err := Plan(cluster, DefaultImages()); err != nil {
		t.Fatalf("direct multi-member plan unexpectedly required a bootstrap image: %v", err)
	}
}

func TestMultiMemberPlanRequiresOneCompleteBootstrapPerMember(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	cluster.Status.PostgreSQLBootstraps = testPostgreSQLBootstraps(cluster)
	cluster.Status.CatalogAccess = nil
	cluster.Status.PostgreSQLWritableLeases = testPostgreSQLWritableLeases(cluster)
	images := DevelopmentImages()
	images.PostgreSQLRuntime = PostgreSQLRuntimeAgentQuarantine

	missing := cluster.DeepCopy()
	missing.Status.PostgreSQLBootstraps = slices.DeleteFunc(missing.Status.PostgreSQLBootstraps, func(bootstrap pgshardv1alpha1.PostgreSQLBootstrapStatus) bool {
		return bootstrap.Shard == 0 && bootstrap.Member == 1
	})
	if _, err := Plan(missing, images); err == nil || !strings.Contains(err.Error(), "shard 0 member 1 is missing") {
		t.Fatalf("missing member bootstrap error = %v", err)
	}

	duplicate := cluster.DeepCopy()
	repeated := duplicate.Status.PostgreSQLBootstraps[0]
	repeated.SecretName += "-duplicate"
	repeated.SecretUID += "-duplicate"
	repeated.PVCName += "-duplicate"
	repeated.PVCUID += "-duplicate"
	duplicate.Status.PostgreSQLBootstraps = append(duplicate.Status.PostgreSQLBootstraps, repeated)
	if _, err := Plan(duplicate, images); err == nil || !strings.Contains(err.Error(), "shard 0 member 0 is duplicated") {
		t.Fatalf("duplicate member bootstrap error = %v", err)
	}
}

func TestMultiMemberPlanRequiresCompleteReplicationCredentialCheckpoints(t *testing.T) {
	t.Parallel()
	base := testCluster()
	base.Status.PostgreSQLBootstraps = testPostgreSQLBootstraps(base)
	base.Status.CatalogAccess = nil
	base.Status.PostgreSQLWritableLeases = testPostgreSQLWritableLeases(base)
	base.Status.PostgreSQLReplicationCredentials = testPostgreSQLReplicationCredentials(base)
	images := DevelopmentImages()
	images.PostgreSQLRuntime = PostgreSQLRuntimeAgentQuarantine
	for _, test := range []struct {
		name   string
		mutate func(*pgshardv1alpha1.PgShardCluster)
		want   string
	}{
		{name: "missing", mutate: func(cluster *pgshardv1alpha1.PgShardCluster) {
			cluster.Status.PostgreSQLReplicationCredentials = cluster.Status.PostgreSQLReplicationCredentials[:1]
		}, want: "shard 1 is missing"},
		{name: "wrong name", mutate: func(cluster *pgshardv1alpha1.PgShardCluster) {
			cluster.Status.PostgreSQLReplicationCredentials[0].SecretName = "foreign-replication"
		}, want: "shard 0 is invalid"},
		{name: "empty UID", mutate: func(cluster *pgshardv1alpha1.PgShardCluster) {
			cluster.Status.PostgreSQLReplicationCredentials[0].SecretUID = ""
		}, want: "shard 0 is invalid"},
		{name: "invalid material digest", mutate: func(cluster *pgshardv1alpha1.PgShardCluster) {
			cluster.Status.PostgreSQLReplicationCredentials[0].MaterialSHA256 = "not-a-digest"
		}, want: "shard 0 is invalid"},
		{name: "duplicate shard", mutate: func(cluster *pgshardv1alpha1.PgShardCluster) {
			cluster.Status.PostgreSQLReplicationCredentials = append(cluster.Status.PostgreSQLReplicationCredentials, cluster.Status.PostgreSQLReplicationCredentials[0])
		}, want: "shard 0 is duplicated"},
		{name: "duplicate UID", mutate: func(cluster *pgshardv1alpha1.PgShardCluster) {
			cluster.Status.PostgreSQLReplicationCredentials[1].SecretUID = cluster.Status.PostgreSQLReplicationCredentials[0].SecretUID
		}, want: "Secret UID test-replication-secret-uid-0000 is duplicated"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cluster := base.DeepCopy()
			test.mutate(cluster)
			if _, err := Plan(cluster, images); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Plan error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestReplicationBootstrapPrimaryHBAImageContract(t *testing.T) {
	t.Parallel()
	contents, err := os.ReadFile(filepath.Join("..", "..", "..", "deploy", "images", "replication-bootstrap-primary.pg_hba.conf"))
	if err != nil {
		t.Fatal(err)
	}
	want := "local postgres postgres peer\n" +
		"local all all reject\n" +
		"local replication all reject\n" +
		"host replication pgshard_replication 0.0.0.0/0 scram-sha-256\n" +
		"host replication pgshard_replication ::0/0 scram-sha-256\n" +
		"host all all 0.0.0.0/0 reject\n" +
		"host all all ::0/0 reject\n"
	if string(contents) != want {
		t.Fatalf("replication bootstrap primary HBA = %q, want %q", contents, want)
	}
}

func TestPostgreSQLBootstrapImageRejectsMutableRemoteReferences(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	cluster.Status.PostgreSQLBootstraps = testPostgreSQLBootstraps(cluster)
	for _, image := range []string{
		"ghcr.io/andrew01234567890/pgshard-postgres-agent:main",
		"ghcr.io/andrew01234567890/pgshard-postgres-agent@sha256:not-a-digest",
		"pgshard/postgres-agent:other-local-tag",
		"registry.example/UPPER/postgres-agent@sha256:" + strings.Repeat("a", 64),
		"registry.example/pgshard//postgres-agent@sha256:" + strings.Repeat("a", 64),
	} {
		images := DefaultImages()
		images.PostgreSQLBootstrap = image
		if _, err := Plan(cluster, images); err == nil || !strings.Contains(err.Error(), "immutable sha256 digest") {
			t.Fatalf("bootstrap image %q error = %v", image, err)
		}
	}
	images := DefaultImages()
	images.PostgreSQLBootstrap = "registry.example/pgshard-postgres-agent:v1@sha256:" + strings.Repeat("a", 64)
	if _, err := Plan(cluster, images); err != nil {
		t.Fatalf("digest-pinned bootstrap image was rejected: %v", err)
	}
}

func TestOrchestratorHasExplicitShutdownBudget(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	plan, err := Plan(cluster, DefaultImages())
	if err != nil {
		t.Fatal(err)
	}
	orchestrator := object[*appsv1.Deployment](t, plan, cluster.Name+OrchestratorSuffix)
	if got := orchestrator.Spec.Template.Spec.TerminationGracePeriodSeconds; got == nil || *got != 30 {
		t.Fatalf("orchestrator termination grace = %v, want 30 seconds", got)
	}
}

func TestMaximumClusterNameUsesBoundedOrchestratorIdentity(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	cluster.Name = strings.Repeat("a", pgshardv1alpha1.MaximumClusterNameLength)
	plan, err := Plan(cluster, DefaultImages())
	if err != nil {
		t.Fatal(err)
	}
	orchestrator := object[*appsv1.Deployment](t, plan, cluster.Name+OrchestratorSuffix)
	identity := orchestrator.Spec.Template.Spec.Containers[0].Env[2]
	if identity.Name != "PGSHARD_ORCH_ID" || identity.ValueFrom == nil || identity.ValueFrom.FieldRef == nil || identity.ValueFrom.FieldRef.FieldPath != "metadata.name" {
		t.Fatalf("orchestrator identity = %#v", identity)
	}
	lease := object[*coordinationv1.Lease](t, plan, cluster.Name+OrchestratorLeaseSuffix)
	if len(lease.Name) > 63 {
		t.Fatalf("maximum cluster name produced invalid Lease name: %q", lease.Name)
	}
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	cluster.Status.PostgreSQLBootstraps = testPostgreSQLBootstraps(cluster)
	plan, err = Plan(cluster, singleMemberImages())
	if err != nil {
		t.Fatal(err)
	}
	statefulSet := object[*appsv1.StatefulSet](t, plan, PostgreSQLShardStatefulSetName(cluster.Name, 0))
	if len(statefulSet.Name) > 63 || len(statefulSet.Name+"-0") > 63 {
		t.Fatalf("maximum cluster name produced invalid StatefulSet or Pod name: %q", statefulSet.Name)
	}
	if strings.Contains(statefulSet.Name, "primary") || strings.Contains(statefulSet.Name, "replica") {
		t.Fatalf("PostgreSQL StatefulSet identity contains a mutable role: %q", statefulSet.Name)
	}
	lastMemberStatefulSet := PostgreSQLMemberStatefulSetName(cluster.Name, pgshardv1alpha1.MaximumShards-1, 4)
	if len(lastMemberStatefulSet) > 63 || len(lastMemberStatefulSet+"-0") > 63 || len(validation.IsDNS1123Label(lastMemberStatefulSet)) != 0 {
		t.Fatalf("maximum member StatefulSet identity is invalid: %q", lastMemberStatefulSet)
	}
	if statefulSet.Spec.ServiceName != shardName(cluster.Name, 0) {
		t.Fatalf("bounded StatefulSet changed the existing shard Service identity: %q", statefulSet.Spec.ServiceName)
	}
	otherName := strings.Repeat("a", pgshardv1alpha1.MaximumClusterNameLength-1) + "b"
	if PostgreSQLShardStatefulSetName(cluster.Name, 0) == PostgreSQLShardStatefulSetName(otherName, 0) {
		t.Fatal("distinct maximum-length cluster names produced the same StatefulSet identity")
	}
	derivedAlias := boundedPostgreSQLWorkloadPrefix(cluster.Name)
	if len(derivedAlias) != 42 {
		t.Fatalf("bounded cluster prefix length = %d, want 42", len(derivedAlias))
	}
	if PostgreSQLShardStatefulSetName(cluster.Name, 0) == PostgreSQLShardStatefulSetName(derivedAlias, 0) {
		t.Fatal("maximum-length cluster name aliased its valid derived 42-character prefix")
	}
}

func TestImagePullPolicyHandlesRegistryPortsAndDigests(t *testing.T) {
	t.Parallel()
	tests := map[string]corev1.PullPolicy{
		"registry.example:5000/pgshard-pooler":          corev1.PullAlways,
		"registry.example:5000/pgshard-pooler:main":     corev1.PullAlways,
		"registry.example:5000/pgshard-pooler:v1.2.3":   corev1.PullIfNotPresent,
		"registry.example/pgshard-pooler@sha256:abcdef": corev1.PullIfNotPresent,
	}
	for image, want := range tests {
		if got := imagePullPolicy(image); got != want {
			t.Errorf("imagePullPolicy(%q) = %q, want %q", image, got, want)
		}
	}
}

func TestPostgreSQLWritableLeaseNameFitsDNSLabelAtMaximumClusterLength(t *testing.T) {
	t.Parallel()
	cluster := strings.Repeat("c", pgshardv1alpha1.MaximumClusterNameLength)
	names := []string{
		PostgreSQLWritableLeaseName(cluster, pgshardv1alpha1.MaximumShards-1),
		PostgreSQLAgentServiceAccountName(cluster, pgshardv1alpha1.MaximumShards-1),
	}
	for _, name := range names {
		if messages := validation.IsDNS1123Label(name); len(messages) != 0 {
			t.Fatalf("writable-term resource name %q is invalid: %s", name, messages[0])
		}
		if len(name) > 63 {
			t.Fatalf("writable-term resource name %q has %d bytes", name, len(name))
		}
	}
}

func testCluster() *pgshardv1alpha1.PgShardCluster {
	prometheus := true
	storageClass := "test-storage"
	return &pgshardv1alpha1.PgShardCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "database", UID: types.UID("cluster-uid"), Generation: 3},
		Spec: pgshardv1alpha1.PgShardClusterSpec{
			Shards:          2,
			MembersPerShard: 3,
			Durability:      pgshardv1alpha1.DurabilitySynchronous,
			PostgreSQL: pgshardv1alpha1.PostgreSQLSpec{
				Version: pgshardv1alpha1.PostgreSQLMajor18,
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("2Gi")},
					Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")},
				},
			},
			Storage: pgshardv1alpha1.StorageSpec{Size: resource.MustParse("10Gi"), StorageClassName: &storageClass, DeletionPolicy: pgshardv1alpha1.DeletionRetain},
			Pooler: pgshardv1alpha1.PoolerSpec{Scaling: pgshardv1alpha1.PoolerScaling{
				Mode: pgshardv1alpha1.ScalingHPA,
				HPA:  &pgshardv1alpha1.HPAScaling{MinReplicas: 2, MaxReplicas: 6, TargetCPUUtilizationPercentage: 70},
			}},
			Services: pgshardv1alpha1.ServiceSet{
				ReadWrite: pgshardv1alpha1.ServiceTemplate{Type: corev1.ServiceTypeClusterIP},
				ReadOnly:  pgshardv1alpha1.ServiceTemplate{Type: corev1.ServiceTypeClusterIP},
				Read:      pgshardv1alpha1.ServiceTemplate{Type: corev1.ServiceTypeClusterIP},
			},
			Backup: pgshardv1alpha1.BackupSpec{Repository: pgshardv1alpha1.BackupRepository{
				Type:       pgshardv1alpha1.RepositoryFilesystem,
				Filesystem: &pgshardv1alpha1.FilesystemRepository{PersistentVolumeClaimName: "backups"},
			}},
			Observability: pgshardv1alpha1.ObservabilitySpec{Prometheus: &prometheus},
			Databases:     []pgshardv1alpha1.DatabaseTemplate{{Name: "app"}, {Name: "analytics"}},
		},
	}
}

func object[T client.Object](t *testing.T, plan []client.Object, name string) T {
	t.Helper()
	var zero T
	for _, item := range plan {
		if candidate, ok := item.(T); ok && candidate.GetName() == name {
			return candidate
		}
	}
	t.Fatalf("%T %q not found", zero, name)
	return zero
}

func postgresqlConfigMap(t *testing.T, plan []client.Object, clusterName string) *corev1.ConfigMap {
	t.Helper()
	prefix := clusterName + PostgreSQLConfigSuffix + "-"
	var configuration *corev1.ConfigMap
	for _, item := range plan {
		candidate, ok := item.(*corev1.ConfigMap)
		if !ok || !strings.HasPrefix(candidate.Name, prefix) {
			continue
		}
		if configuration != nil {
			t.Fatalf("multiple PostgreSQL configurations found: %s and %s", configuration.Name, candidate.Name)
		}
		configuration = candidate
	}
	if configuration == nil {
		t.Fatalf("PostgreSQL configuration with prefix %q not found", prefix)
	}
	return configuration
}

func testPostgreSQLBootstraps(cluster *pgshardv1alpha1.PgShardCluster) []pgshardv1alpha1.PostgreSQLBootstrapStatus {
	cluster.Status.CatalogAccess = &pgshardv1alpha1.CatalogAccessStatus{
		SecretName:   CatalogAccessSecretPrefix(cluster.Name) + strings.Repeat("a", 32),
		SecretUID:    "test-catalog-secret-uid",
		ClientSHA256: strings.Repeat("b", 64),
		ServerSHA256: strings.Repeat("c", 64),
	}
	bootstraps := make([]pgshardv1alpha1.PostgreSQLBootstrapStatus, 0, cluster.Spec.Shards*cluster.Spec.MembersPerShard)
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		for member := int32(0); member < cluster.Spec.MembersPerShard; member++ {
			bootstraps = append(bootstraps, pgshardv1alpha1.PostgreSQLBootstrapStatus{
				Shard: shard, Member: member, SecretName: fmt.Sprintf("test-secret-%04d-%04d", shard, member), SecretUID: types.UID(fmt.Sprintf("test-secret-uid-%04d-%04d", shard, member)),
				PVCFenceDetached: true, PVCName: fmt.Sprintf("test-data-%04d-%04d", shard, member), PVCUID: types.UID(fmt.Sprintf("test-pvc-uid-%04d-%04d", shard, member)), PVCStorageClassName: copyString(cluster.Spec.Storage.StorageClassName),
			})
		}
	}
	return bootstraps
}

func testPostgreSQLWritableLeases(cluster *pgshardv1alpha1.PgShardCluster) []pgshardv1alpha1.PostgreSQLWritableLeaseStatus {
	checkpoints := make([]pgshardv1alpha1.PostgreSQLWritableLeaseStatus, 0, cluster.Spec.Shards)
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		checkpoints = append(checkpoints, pgshardv1alpha1.PostgreSQLWritableLeaseStatus{
			Shard:     shard,
			LeaseName: PostgreSQLWritableLeaseName(cluster.Name, shard),
			LeaseUID:  types.UID(fmt.Sprintf("test-lease-uid-%04d", shard)),
		})
	}
	return checkpoints
}

func testPostgreSQLReplicationCredentials(cluster *pgshardv1alpha1.PgShardCluster) []pgshardv1alpha1.PostgreSQLReplicationCredentialStatus {
	checkpoints := make([]pgshardv1alpha1.PostgreSQLReplicationCredentialStatus, 0, cluster.Spec.Shards)
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		checkpoints = append(checkpoints, pgshardv1alpha1.PostgreSQLReplicationCredentialStatus{
			Shard:          shard,
			SecretName:     PostgreSQLReplicationSecretPrefix(cluster.Name, shard) + strings.Repeat("d", 32),
			SecretUID:      types.UID(fmt.Sprintf("test-replication-secret-uid-%04d", shard)),
			MaterialSHA256: strings.Repeat("e", 64),
		})
	}
	return checkpoints
}

func singleMemberImages() Images {
	return DevelopmentImages()
}

func bootstrapVersionTestEnvironment(t *testing.T, major string, initdbMajor ...string) []string {
	t.Helper()
	directory := t.TempDir()
	configurationSource := t.TempDir()
	configurationTarget := t.TempDir()
	postgres := filepath.Join(directory, "postgres")
	contents := "#!/bin/sh\nprintf '%s\\n' 'postgres (PostgreSQL) " + major + ".0'\n"
	if err := os.WriteFile(postgres, []byte(contents), 0o755); err != nil {
		t.Fatal(err)
	}
	if len(initdbMajor) > 0 && initdbMajor[0] != "" {
		initdb := filepath.Join(directory, "initdb")
		contents := "#!/bin/sh\nset -eu\nfor argument do\n  case \"$argument\" in\n    --pgdata=*) pgdata=${argument#*=} ;;\n  esac\ndone\nmkdir -p \"$pgdata\"\nprintf '%s\\n' '" + initdbMajor[0] + "' > \"$pgdata/PG_VERSION\"\n"
		if err := os.WriteFile(initdb, []byte(contents), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return append(os.Environ(),
		"PATH="+directory+string(os.PathListSeparator)+os.Getenv("PATH"),
		"PGSHARD_POSTGRESQL_CONFIG_SHA256="+configMapDataHash(map[string]string{}),
		"PGSHARD_POSTGRESQL_CONFIG_SOURCE="+configurationSource,
		"PGSHARD_POSTGRESQL_CONFIG_TARGET="+configurationTarget,
	)
}

func copyString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func assertOwned(t *testing.T, object client.Object, cluster *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	if object.GetLabels()[ManagedByLabel] != ManagedByValue || object.GetLabels()[ClusterLabel] != cluster.Name {
		t.Fatalf("%T/%s labels = %#v", object, object.GetName(), object.GetLabels())
	}
	references := object.GetOwnerReferences()
	if len(references) != 1 || references[0].UID != cluster.UID || references[0].Controller == nil || !*references[0].Controller {
		t.Fatalf("%T/%s owner references = %#v", object, object.GetName(), references)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func envValue(variables []corev1.EnvVar, name string) string {
	for _, variable := range variables {
		if variable.Name == name {
			return variable.Value
		}
	}
	return ""
}

func volumeByName(t *testing.T, volumes []corev1.Volume, name string) corev1.VolumeSource {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name == name {
			return volume.VolumeSource
		}
	}
	t.Fatalf("volume %q not found in %#v", name, volumes)
	return corev1.VolumeSource{}
}

func hasVolume(volumes []corev1.Volume, name string) bool {
	for _, volume := range volumes {
		if volume.Name == name {
			return true
		}
	}
	return false
}

func secretItemKeys(items []corev1.KeyToPath) []string {
	keys := make([]string, 0, len(items))
	for _, item := range items {
		keys = append(keys, item.Key)
	}
	return keys
}

func containsVolumeMount(mounts []corev1.VolumeMount, name string, readOnly bool) bool {
	for _, mount := range mounts {
		if mount.Name == name && mount.ReadOnly == readOnly {
			return true
		}
	}
	return false
}

func containsNamedVolumeMount(mounts []corev1.VolumeMount, name string) bool {
	for _, mount := range mounts {
		if mount.Name == name {
			return true
		}
	}
	return false
}

func configMapVolumeName(t *testing.T, volumes []corev1.Volume, name string) string {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name == name && volume.ConfigMap != nil {
			return volume.ConfigMap.Name
		}
	}
	t.Fatalf("ConfigMap volume %q not found: %#v", name, volumes)
	return ""
}
