package resources

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

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
	if len(postgresConfig.Data) != 7 {
		t.Fatalf("PostgreSQL configuration documents = %#v", postgresConfig.Data)
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
	beforeStatefulSet := object[*appsv1.StatefulSet](t, before, PostgreSQLPrimaryStatefulSetName(cluster.Name, 0))
	beforePooler := object[*appsv1.Deployment](t, before, cluster.Name+PoolerSuffix)

	cluster.Spec.PostgreSQL.Resources.Requests[corev1.ResourceMemory] = resource.MustParse("3Gi")
	cluster.Spec.PostgreSQL.Resources.Limits[corev1.ResourceMemory] = resource.MustParse("6Gi")
	after, err := Plan(cluster, singleMemberImages())
	if err != nil {
		t.Fatal(err)
	}
	afterConfiguration := postgresqlConfigMap(t, after, cluster.Name)
	afterStatefulSet := object[*appsv1.StatefulSet](t, after, PostgreSQLPrimaryStatefulSetName(cluster.Name, 0))
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
	primaryConfiguration := configuration.Data["primary-0000.conf"]
	if !strings.HasPrefix(primaryConfiguration, "include = '/etc/pgshard/postgresql/postgresql.conf'\n") ||
		!strings.Contains(primaryConfiguration, "synchronized_standby_slots = ''\n") ||
		!strings.Contains(primaryConfiguration, "synchronous_standby_names = ''\n") {
		t.Fatalf("single-member primary configuration = %q", primaryConfiguration)
	}

	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		name := shardName(cluster.Name, shard) + "-primary"
		statefulSet := object[*appsv1.StatefulSet](t, plan, name)
		if statefulSet.Spec.Replicas == nil || *statefulSet.Spec.Replicas != 1 || statefulSet.Spec.ServiceName != shardName(cluster.Name, shard) || statefulSet.Spec.UpdateStrategy.Type != appsv1.OnDeleteStatefulSetStrategyType {
			t.Fatalf("PostgreSQL StatefulSet identity = %#v", statefulSet.Spec)
		}
		if statefulSet.Spec.Template.Labels[ManagedByLabel] != ManagedByValue || statefulSet.Spec.Template.Labels[ShardLabel] != shardLabel(shard) || statefulSet.Spec.Template.Labels[RoleLabel] != "primary" || statefulSet.Spec.Template.Labels[MemberLabel] != "0000" {
			t.Fatalf("PostgreSQL labels = %#v", statefulSet.Spec.Template.Labels)
		}
		if statefulSet.Spec.Template.Annotations[PostgreSQLPodClusterUIDAnnotation] != string(cluster.UID) || !reflect.DeepEqual(statefulSet.Spec.Template.Finalizers, []string{PostgreSQLPodTerminationFinalizer}) {
			t.Fatalf("PostgreSQL termination fence = %#v", statefulSet.Spec.Template.ObjectMeta)
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
		if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken || pod.NodeSelector[corev1.LabelOSStable] != "linux" || len(pod.InitContainers) != 1 || len(pod.Containers) != 1 {
			t.Fatalf("PostgreSQL Pod boundary = %#v", pod)
		}
		if pod.SecurityContext == nil || pod.SecurityContext.RunAsNonRoot == nil || !*pod.SecurityContext.RunAsNonRoot || pod.SecurityContext.RunAsUser == nil || *pod.SecurityContext.RunAsUser != 999 || pod.SecurityContext.FSGroup == nil || *pod.SecurityContext.FSGroup != 999 || pod.SecurityContext.FSGroupChangePolicy == nil || *pod.SecurityContext.FSGroupChangePolicy != corev1.FSGroupChangeOnRootMismatch {
			t.Fatalf("PostgreSQL Pod security = %#v", pod.SecurityContext)
		}
		postgres := pod.Containers[0]
		if postgres.Image != defaultPostgreSQLImage || postgres.ImagePullPolicy != corev1.PullIfNotPresent || postgres.SecurityContext == nil || postgres.SecurityContext.RunAsUser == nil || *postgres.SecurityContext.RunAsUser != 999 || postgres.SecurityContext.ReadOnlyRootFilesystem == nil || !*postgres.SecurityContext.ReadOnlyRootFilesystem {
			t.Fatalf("PostgreSQL container boundary = %#v", postgres)
		}
		if !containsString(postgres.Args, "config_file=/etc/pgshard/postgresql/primary-0000.conf") || postgres.StartupProbe != nil || postgres.ReadinessProbe == nil || postgres.LivenessProbe != nil {
			t.Fatalf("PostgreSQL startup contract = %#v", postgres)
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
		if !strings.Contains(bootstrap.Command[2], "catalog_schema_fingerprint") ||
			!strings.Contains(bootstrap.Command[2], "ee17a64c8eec5e2e9a44f29d4764edac90680980f61df35bdb2284c01b57c4d9") ||
			!strings.Contains(bootstrap.Command[2], "2720fa78d0bc96c21311b1656eeaabbb3e745ea65fa9d1ea701ffb67cde1b1d9") ||
			!strings.Contains(bootstrap.Command[2], "ceec4ff5d633d28afacf1e93fbc2547591017e57f172dc3a8072814bb6d3867a") ||
			!strings.Contains(bootstrap.Command[2], "pg_catalog.pg_sequence") ||
			!strings.Contains(bootstrap.Command[2], "pg_catalog.pg_rewrite") ||
			!strings.Contains(bootstrap.Command[2], "internal-trigger|") ||
			!strings.Contains(bootstrap.Command[2], "SET SESSION search_path = pg_catalog") ||
			!strings.Contains(bootstrap.Command[2], "SET SESSION quote_all_identifiers = off") ||
			!strings.Contains(bootstrap.Command[2], "count_missing_shards") ||
			!strings.Contains(bootstrap.Command[2], "refusing shardschema inventory with missing configured shards") {
			t.Fatal("PostgreSQL bootstrap does not pin supported catalog shapes")
		}
		if len(bootstrap.Env) != 10 || bootstrap.Env[0].Name != "PGSHARD_CLUSTER_UID" || bootstrap.Env[0].Value != string(cluster.UID) || bootstrap.Env[1].Name != "PGSHARD_SHARD_ID" || bootstrap.Env[1].Value != shardLabel(shard) ||
			bootstrap.Env[2].Name != "PGSHARD_POSTGRESQL_MAJOR" || bootstrap.Env[2].Value != pgshardv1alpha1.PostgreSQLMajor18 ||
			bootstrap.Env[3].Name != "PGSHARD_SHARD_COUNT" || bootstrap.Env[3].Value != fmt.Sprintf("%d", cluster.Spec.Shards) ||
			bootstrap.Env[4].Name != "PGSHARD_MAXIMUM_SHARDS" || bootstrap.Env[4].Value != fmt.Sprintf("%d", pgshardv1alpha1.MaximumShards) ||
			bootstrap.Env[5].Name != "PGSHARD_BOOTSTRAP_SHARDSCHEMA" || bootstrap.Env[5].Value != fmt.Sprintf("%t", shard == 0) ||
			bootstrap.Env[6].Name != "PGSHARD_SHARDSCHEMA_MIGRATION" || bootstrap.Env[6].Value != shardschemaMigrationPath ||
			bootstrap.Env[7].Name != "PGSHARD_SHARDSCHEMA_MIGRATION_SHA256" || bootstrap.Env[7].Value != shardschemaMigrationSHA256 ||
			bootstrap.Env[8].Name != "PGSHARD_NODE_UID" || bootstrap.Env[8].ValueFrom == nil || bootstrap.Env[8].ValueFrom.FieldRef == nil || bootstrap.Env[8].ValueFrom.FieldRef.FieldPath != "metadata.annotations['pgshard.io/postgresql-node-uid']" ||
			bootstrap.Env[9].Name != "PGSHARD_NODE_BOOT_ID" || bootstrap.Env[9].ValueFrom == nil || bootstrap.Env[9].ValueFrom.FieldRef == nil || bootstrap.Env[9].ValueFrom.FieldRef.FieldPath != "metadata.annotations['pgshard.io/postgresql-node-boot-id']" {
			t.Fatalf("PostgreSQL bootstrap identity = %#v", bootstrap.Env)
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
	secret.UID = "demo-random-auth-uid"
	claim := PostgreSQLPrimaryDataPVC(cluster, 1, "demo-random-data", cluster.Spec.Storage.Size, cluster.Spec.Storage.StorageClassName, secret.Name, secret.UID)
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
	configurationDirectory := newTraversableFixtureDirectory("pgshard-bootstrap-config-")
	if err := os.WriteFile(filepath.Join(configurationDirectory, "postgresql.conf"), []byte(strings.Join([]string{
		"fsync = on",
		"listen_addresses = '*'",
		"max_prepared_transactions = 8",
		"max_replication_slots = 20",
		"max_wal_senders = 20",
		"wal_level = logical",
		"",
	}, "\n")), 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(configurationDirectory, "primary-0000.conf"), []byte("include = '/etc/pgshard/postgresql/postgresql.conf'\n"), 0o444); err != nil {
		t.Fatal(err)
	}
	legacyMigration, err := filepath.Abs(filepath.Join("..", "..", "..", "crates", "pgshard-catalog", "tests", "fixtures", "v0_49_0_shardschema.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(legacyMigration); err != nil {
		t.Fatalf("locate legacy shardschema fixture: %v", err)
	}

	containerArguments := func(dataParent, script string, environment ...string) []string {
		t.Helper()
		arguments := []string{
			"--user", "999:999", "--network", "none", "--read-only",
			"--volume", volume + ":/var/lib/postgresql",
			"--volume", secretDirectory + ":/etc/pgshard/bootstrap:ro",
			"--volume", configurationDirectory + ":/etc/pgshard/postgresql:ro",
			"--volume", legacyMigration + ":/tmp/v0_49_0_shardschema.sql:ro",
			"--tmpfs", "/tmp:rw,uid=999,gid=999,mode=0700,size=67108864",
			"--env", "PGDATA=" + dataParent + "/docker",
		}
		for _, variable := range environment {
			arguments = append(arguments, "--env", variable)
		}
		arguments = append(arguments, "--entrypoint", "bash", image, "-ceu", script)
		return arguments
	}
	runContainer := func(dataParent, script string, environment ...string) (string, error) {
		t.Helper()
		arguments := append([]string{"run", "--rm"}, containerArguments(dataParent, script, environment...)...)
		return runDocker(arguments...)
	}
	runContainerWithTimeout := func(name, dataParent, script string, timeout time.Duration, environment ...string) (string, error) {
		t.Helper()
		arguments := append([]string{"run", "--rm", "--name", name}, containerArguments(dataParent, script, environment...)...)
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
			"PGSHARD_NODE_UID=bootstrap-e2e-node",
			"PGSHARD_NODE_BOOT_ID=bootstrap-e2e-boot",
		}
	}
	bootstrapScript := func(dataParent string) string {
		if dataParent == "/var/lib/postgresql/18" {
			return postgresqlBootstrapScript
		}
		return strings.Replace(postgresqlBootstrapScript, "parent=/var/lib/postgresql/18", "parent="+dataParent, 1)
	}
	bootstrap := func(dataParent string, installCatalog bool, shardCount int) (string, error) {
		t.Helper()
		return runContainer(dataParent, bootstrapScript(dataParent), bootstrapEnvironment(installCatalog, shardCount)...)
	}
	const primaryDataParent = "/var/lib/postgresql/18"
	if output, err := bootstrap(primaryDataParent, false, 2); err != nil {
		t.Fatalf("initialize PGDATA without catalog: %v\n%s", err, output)
	}

	const postgresHarness = `set -Eeuo pipefail
socket=/tmp/pgshard-bootstrap-e2e
mkdir -m 0700 "$socket"
pg_ctl -D "$PGDATA" -w -t 45 start \
  -l /tmp/postgres.log \
  -o "-c config_file=/etc/pgshard/postgresql/primary-0000.conf -c listen_addresses='' -c unix_socket_directories='$socket' -c unix_socket_permissions=0700" >/dev/null
stop_postgres() {
  result=$?
  trap - EXIT
  pg_ctl -D "$PGDATA" -w -t 45 stop -m fast >/dev/null || result=1
  exit "$result"
}
trap stop_postgres EXIT
`
	prepareLegacyCatalog := postgresHarness + `
createdb --no-password --host="$socket" --username=postgres --template=template0 --encoding=UTF8 shardschema
psql -X --no-password --host="$socket" --username=postgres --dbname=shardschema \
  --set=ON_ERROR_STOP=1 --file=/tmp/v0_49_0_shardschema.sql >/dev/null
pg_ctl -D "$PGDATA" -w -t 45 stop -m fast >/dev/null
trap - EXIT
`
	if output, err := runContainer(primaryDataParent, prepareLegacyCatalog); err != nil {
		t.Fatalf("prepare v0.49.0 catalog database: %v\n%s", err, output)
	}
	if output, err := bootstrap(primaryDataParent, true, 1); err != nil {
		t.Fatalf("upgrade v0.49.0 catalog database: %v\n%s", err, output)
	}
	if output, err := bootstrap(primaryDataParent, true, 2); err != nil {
		t.Fatalf("recover partial catalog inventory: %v\n%s", err, output)
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
	if got := catalogSQL(primaryDataParent, "SELECT (SELECT string_agg(shard_id::text || ':' || shard_number::text || ':' || state, ',' ORDER BY shard_number) FROM pgshard_catalog.shards), (SELECT count(*) FROM pgshard_catalog.shard_restore_incarnations WHERE state = 'active'), (SELECT pg_catalog.pg_get_userbyid(nspowner) FROM pg_catalog.pg_namespace WHERE nspname = 'pgshard_catalog')"); got != "shard-0000:0:active,shard-0001:1:active|2|pgshard_catalog_owner" {
		t.Fatalf("recovered catalog inventory = %q", got)
	}

	fingerprint := func() string {
		t.Helper()
		return catalogSQL(primaryDataParent, "SELECT (SELECT catalog_epoch FROM pgshard_catalog.cluster_state WHERE singleton), (SELECT string_agg(triggers.oid::text || ':' || triggers.tgenabled::text, ',' ORDER BY triggers.tgname) FROM pg_catalog.pg_trigger AS triggers JOIN pg_catalog.pg_class AS relations ON relations.oid = triggers.tgrelid JOIN pg_catalog.pg_namespace AS namespaces ON namespaces.oid = relations.relnamespace WHERE namespaces.nspname = 'pgshard_catalog'), (SELECT string_agg(sequences.seqrelid::pg_catalog.regclass::text || ':' || sequences.seqincrement::text || ':' || sequences.seqcycle::text, ',' ORDER BY sequences.seqrelid) FROM pg_catalog.pg_sequence AS sequences JOIN pg_catalog.pg_class AS relations ON relations.oid = sequences.seqrelid JOIN pg_catalog.pg_namespace AS namespaces ON namespaces.oid = relations.relnamespace WHERE namespaces.nspname = 'pgshard_catalog'), (SELECT string_agg(rewrite_rules.rulename, ',' ORDER BY rewrite_rules.oid) FROM pg_catalog.pg_rewrite AS rewrite_rules JOIN pg_catalog.pg_class AS relations ON relations.oid = rewrite_rules.ev_class JOIN pg_catalog.pg_namespace AS namespaces ON namespaces.oid = relations.relnamespace WHERE namespaces.nspname = 'pgshard_catalog')")
	}
	assertRejectedWithoutMutation := func(want string) {
		t.Helper()
		before := fingerprint()
		output, err := bootstrap(primaryDataParent, true, 2)
		if err == nil || !strings.Contains(output, want) {
			t.Fatalf("conflicting catalog bootstrap error = %v, want %q\n%s", err, want, output)
		}
		if after := fingerprint(); after != before {
			t.Fatalf("rejected catalog changed before=%q after=%q", before, after)
		}
	}

	catalogSQL(primaryDataParent, "ALTER SEQUENCE pgshard_catalog.routing_epochs_routing_epoch_seq INCREMENT BY 2 CYCLE")
	assertRejectedWithoutMutation("refusing an unsupported or malformed pre-existing shardschema catalog")
	catalogSQL(primaryDataParent, "ALTER SEQUENCE pgshard_catalog.routing_epochs_routing_epoch_seq INCREMENT BY 1 NO CYCLE")
	if output, err := bootstrap(primaryDataParent, true, 2); err != nil {
		t.Fatalf("canonical identity sequence was not restored: %v\n%s", err, output)
	}

	catalogSQL(primaryDataParent, "CREATE RULE pgshard_rejected_rule AS ON INSERT TO pgshard_catalog.shards DO INSTEAD NOTHING")
	assertRejectedWithoutMutation("refusing an unsupported or malformed pre-existing shardschema catalog")
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
	assertRejectedWithoutMutation("refusing an unsupported or malformed pre-existing shardschema catalog")
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
	assertRejectedWithoutMutation("refusing shardschema home-shard identity")
	catalogSQL(primaryDataParent, `
ALTER TABLE pgshard_catalog.cluster_configuration DISABLE TRIGGER USER;
UPDATE pgshard_catalog.cluster_configuration SET home_shard_id = 'shard-0000' WHERE singleton;
ALTER TABLE pgshard_catalog.cluster_configuration ENABLE TRIGGER USER;
`)

	catalogSQL(primaryDataParent, `
ALTER TABLE pgshard_catalog.shard_restore_incarnations DISABLE TRIGGER USER;
DELETE FROM pgshard_catalog.shard_restore_incarnations WHERE shard_id = 'shard-0000' AND state = 'active';
ALTER TABLE pgshard_catalog.shard_restore_incarnations ENABLE TRIGGER USER;
`)
	assertRejectedWithoutMutation("refusing shardschema restore lineage")
	catalogSQL(primaryDataParent, `
INSERT INTO pgshard_catalog.shard_restore_incarnations(restore_incarnation, shard_id, state)
VALUES ('11111111-1111-1111-1111-111111111111', 'shard-0000', 'active');
`)

	catalogSQL(primaryDataParent, `
ALTER TABLE pgshard_catalog.shards DISABLE TRIGGER USER;
INSERT INTO pgshard_catalog.shards(shard_id, shard_number, state) VALUES ('shard-10000', 10000, 'retired');
ALTER TABLE pgshard_catalog.shards ENABLE TRIGGER USER;
`)
	assertRejectedWithoutMutation("refusing shardschema restore lineage")
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
	assertRejectedWithoutMutation("refusing shardschema inventory")
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
	crashArguments := append([]string{"run", "--detach", "--name", crashContainer}, containerArguments(primaryDataParent, crashBootstrapScript, bootstrapEnvironment(true, 2)...)...)
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
	if output, err := bootstrap(emptyDataParent, true, 2); err != nil {
		t.Fatalf("recover empty catalog database: %v\n%s", err, output)
	}
	if output, err := bootstrap(emptyDataParent, true, 2); err != nil {
		t.Fatalf("revalidate freshly installed current catalog: %v\n%s", err, output)
	}
	if got := catalogSQL(emptyDataParent, "SELECT count(*) FILTER (WHERE state = 'active'), (SELECT count(*) FROM pgshard_catalog.shard_restore_incarnations WHERE state = 'active') FROM pgshard_catalog.shards"); got != "2|2" {
		t.Fatalf("empty-database recovery inventory = %q", got)
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

	etcd := object[*appsv1.StatefulSet](t, plan, "demo-etcd")
	if *etcd.Spec.Replicas != 3 || etcd.Spec.ServiceName != "demo-etcd" || len(etcd.Spec.VolumeClaimTemplates) != 1 {
		t.Fatalf("etcd spec = %#v", etcd.Spec)
	}
	if !containsString(etcd.Spec.Template.Spec.Containers[0].Args, "--quota-backend-bytes=805306368") || !containsString(etcd.Spec.Template.Spec.Containers[0].Args, "--max-wals=2") {
		t.Fatalf("etcd quota/retention does not leave storage margin: %#v", etcd.Spec.Template.Spec.Containers[0].Args)
	}
	claim := etcd.Spec.VolumeClaimTemplates[0]
	if claim.Spec.StorageClassName == nil || *claim.Spec.StorageClassName != storageClass || claim.Spec.Resources.Requests.Storage().String() != "2Gi" {
		t.Fatalf("etcd PVC = %#v", claim.Spec)
	}
	if claim.Namespace != "" || !metav1.IsControlledBy(&claim, cluster) {
		t.Fatalf("etcd PVC template is not directly UID-owned by the cluster: %#v", claim.ObjectMeta)
	}
	if etcd.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted != appsv1.RetainPersistentVolumeClaimRetentionPolicyType || etcd.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled != appsv1.RetainPersistentVolumeClaimRetentionPolicyType {
		t.Fatalf("etcd PVC retention is destructive: %#v", etcd.Spec.PersistentVolumeClaimRetentionPolicy)
	}
	if etcd.Spec.Template.Spec.SecurityContext == nil || etcd.Spec.Template.Spec.SecurityContext.SeccompProfile == nil || len(etcd.Spec.Template.Spec.TopologySpreadConstraints) != 2 {
		t.Fatalf("etcd pod hardening/spread is incomplete: %#v", etcd.Spec.Template.Spec)
	}
	etcdContainer := etcd.Spec.Template.Spec.Containers[0]
	if len(etcdContainer.Command) != 1 || etcdContainer.Command[0] != etcdExecutable || etcdContainer.Image != defaultEtcdImage || etcdContainer.ImagePullPolicy != corev1.PullIfNotPresent {
		t.Fatalf("etcd executable/image contract = %#v", etcdContainer)
	}
	if etcdContainer.ReadinessProbe.FailureThreshold != 1 || etcdContainer.LivenessProbe.FailureThreshold != 3 {
		t.Fatalf("etcd probe thresholds = readiness %d, liveness %d", etcdContainer.ReadinessProbe.FailureThreshold, etcdContainer.LivenessProbe.FailureThreshold)
	}

	orchestrator := object[*appsv1.Deployment](t, plan, "demo-orchestrator")
	if *orchestrator.Spec.Replicas != 3 || orchestrator.Spec.Template.Spec.Containers[0].ReadinessProbe.HTTPGet.Path != "/readyz" || orchestrator.Spec.Template.Spec.Containers[0].ReadinessProbe.FailureThreshold != 1 {
		t.Fatalf("orchestrator spec = %#v", orchestrator.Spec)
	}
	if orchestrator.Spec.Template.Spec.Containers[0].Env[1].ValueFrom.FieldRef.FieldPath != "metadata.uid" {
		t.Fatalf("orchestrator identity is not a bounded Pod UID: %#v", orchestrator.Spec.Template.Spec.Containers[0].Env[1])
	}
	pooler := object[*appsv1.Deployment](t, plan, "demo-pooler")
	poolerContainer := pooler.Spec.Template.Spec.Containers[0]
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
		case "PGSHARD_SHARDSCHEMA_DSN_FILE":
			t.Fatal("pooler unexpectedly has a shardschema DSN file")
		}
	}
	if catalogModeCount != 1 {
		t.Fatalf("pooler catalog mode count = %d, want 1", catalogModeCount)
	}
	hpa := object[*autoscalingv2.HorizontalPodAutoscaler](t, plan, "demo-pooler")
	if *hpa.Spec.MinReplicas != 2 || hpa.Spec.MaxReplicas != 6 || *hpa.Spec.Metrics[0].Resource.Target.AverageUtilization != 70 {
		t.Fatalf("HPA spec = %#v", hpa.Spec)
	}
	for _, component := range []string{"etcd", "orchestrator", "pooler"} {
		pdb := object[*policyv1.PodDisruptionBudget](t, plan, "demo-"+component)
		if component == "pooler" {
			if pdb.Spec.MinAvailable == nil || pdb.Spec.MinAvailable.IntVal != 1 {
				t.Fatalf("%s PDB = %#v", component, pdb.Spec)
			}
		} else if pdb.Spec.MaxUnavailable == nil || pdb.Spec.MaxUnavailable.IntVal != 1 {
			t.Fatalf("%s PDB = %#v", component, pdb.Spec)
		}
	}
	policy := object[*networkingv1.NetworkPolicy](t, plan, "demo-etcd")
	if len(policy.Spec.Ingress) != 2 || policy.Spec.Ingress[0].Ports[0].Port.IntVal != EtcdClientPort || policy.Spec.Ingress[1].Ports[0].Port.IntVal != EtcdPeerPort {
		t.Fatalf("etcd NetworkPolicy = %#v", policy.Spec)
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

func TestMaximumClusterNameUsesBoundedOrchestratorIdentity(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	cluster.Name = strings.Repeat("a", pgshardv1alpha1.MaximumClusterNameLength)
	plan, err := Plan(cluster, DefaultImages())
	if err != nil {
		t.Fatal(err)
	}
	orchestrator := object[*appsv1.Deployment](t, plan, cluster.Name+OrchestratorSuffix)
	identity := orchestrator.Spec.Template.Spec.Containers[0].Env[1]
	if identity.Name != "PGSHARD_ORCH_ID" || identity.ValueFrom == nil || identity.ValueFrom.FieldRef == nil || identity.ValueFrom.FieldRef.FieldPath != "metadata.uid" {
		t.Fatalf("orchestrator identity = %#v", identity)
	}
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	cluster.Status.PostgreSQLBootstraps = testPostgreSQLBootstraps(cluster)
	plan, err = Plan(cluster, singleMemberImages())
	if err != nil {
		t.Fatal(err)
	}
	statefulSet := object[*appsv1.StatefulSet](t, plan, PostgreSQLPrimaryStatefulSetName(cluster.Name, 0))
	if len(statefulSet.Name) > 63 || len(statefulSet.Name+"-0") > 63 {
		t.Fatalf("maximum cluster name produced invalid StatefulSet or Pod name: %q", statefulSet.Name)
	}
	if statefulSet.Spec.ServiceName != shardName(cluster.Name, 0) {
		t.Fatalf("bounded StatefulSet changed the existing shard Service identity: %q", statefulSet.Spec.ServiceName)
	}
	otherName := strings.Repeat("a", pgshardv1alpha1.MaximumClusterNameLength-1) + "b"
	if PostgreSQLPrimaryStatefulSetName(cluster.Name, 0) == PostgreSQLPrimaryStatefulSetName(otherName, 0) {
		t.Fatal("distinct maximum-length cluster names produced the same StatefulSet identity")
	}
	derivedAlias := boundedPostgreSQLWorkloadPrefix(cluster.Name)
	if len(derivedAlias) != 42 {
		t.Fatalf("bounded cluster prefix length = %d, want 42", len(derivedAlias))
	}
	if PostgreSQLPrimaryStatefulSetName(cluster.Name, 0) == PostgreSQLPrimaryStatefulSetName(derivedAlias, 0) {
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
	bootstraps := make([]pgshardv1alpha1.PostgreSQLBootstrapStatus, 0, cluster.Spec.Shards)
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		bootstraps = append(bootstraps, pgshardv1alpha1.PostgreSQLBootstrapStatus{
			Shard: shard, SecretName: fmt.Sprintf("test-secret-%04d", shard), SecretUID: types.UID(fmt.Sprintf("test-secret-uid-%04d", shard)),
			PVCFenceDetached: true, PVCName: fmt.Sprintf("test-data-%04d", shard), PVCUID: types.UID(fmt.Sprintf("test-pvc-uid-%04d", shard)), PVCStorageClassName: copyString(cluster.Spec.Storage.StorageClassName),
		})
	}
	return bootstraps
}

func singleMemberImages() Images {
	return DevelopmentImages()
}

func bootstrapVersionTestEnvironment(t *testing.T, major string, initdbMajor ...string) []string {
	t.Helper()
	directory := t.TempDir()
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
	return append(os.Environ(), "PATH="+directory+string(os.PathListSeparator)+os.Getenv("PATH"))
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

func containsVolumeMount(mounts []corev1.VolumeMount, name string, readOnly bool) bool {
	for _, mount := range mounts {
		if mount.Name == name && mount.ReadOnly == readOnly {
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
