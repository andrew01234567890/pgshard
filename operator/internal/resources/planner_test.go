package resources

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

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
	if len(pooler.Spec.Template.Spec.Volumes) != 2 {
		t.Fatalf("pooler volumes = %#v", pooler.Spec.Template.Spec.Volumes)
	}
	if pooler.Spec.Template.Annotations[configHashAnnotation] == "" {
		t.Fatal("pooler does not roll when generated configuration changes")
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

func TestPostgreSQLConfigurationAndResourceLimitRollTogether(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	cluster.Status.PostgreSQLBootstraps = testPostgreSQLBootstraps(cluster)
	before, err := Plan(cluster, DefaultImages())
	if err != nil {
		t.Fatal(err)
	}
	beforeConfiguration := postgresqlConfigMap(t, before, cluster.Name)
	beforeStatefulSet := object[*appsv1.StatefulSet](t, before, PostgreSQLPrimaryStatefulSetName(cluster.Name, 0))

	cluster.Spec.PostgreSQL.Resources.Requests[corev1.ResourceMemory] = resource.MustParse("3Gi")
	cluster.Spec.PostgreSQL.Resources.Limits[corev1.ResourceMemory] = resource.MustParse("6Gi")
	after, err := Plan(cluster, DefaultImages())
	if err != nil {
		t.Fatal(err)
	}
	afterConfiguration := postgresqlConfigMap(t, after, cluster.Name)
	afterStatefulSet := object[*appsv1.StatefulSet](t, after, PostgreSQLPrimaryStatefulSetName(cluster.Name, 0))
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
}

func TestSingleMemberPlanCreatesPostgreSQL18Primaries(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	cluster.Status.PostgreSQLBootstraps = testPostgreSQLBootstraps(cluster)
	plan, err := Plan(cluster, DefaultImages())
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
		if statefulSet.Spec.Replicas == nil || *statefulSet.Spec.Replicas != 1 || statefulSet.Spec.ServiceName != shardName(cluster.Name, shard) {
			t.Fatalf("PostgreSQL StatefulSet identity = %#v", statefulSet.Spec)
		}
		if statefulSet.Spec.Template.Labels[ShardLabel] != shardLabel(shard) || statefulSet.Spec.Template.Labels[RoleLabel] != "primary" || statefulSet.Spec.Template.Labels[MemberLabel] != "0000" {
			t.Fatalf("PostgreSQL labels = %#v", statefulSet.Spec.Template.Labels)
		}
		if statefulSet.Spec.Template.Annotations[PostgreSQLPodClusterUIDAnnotation] != string(cluster.UID) || !reflect.DeepEqual(statefulSet.Spec.Template.Finalizers, []string{PostgreSQLPodTerminationFinalizer}) {
			t.Fatalf("PostgreSQL termination fence = %#v", statefulSet.Spec.Template.ObjectMeta)
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
		if bootstrap.Name != "bootstrap-postgresql" || bootstrap.Image != defaultPostgreSQLImage || len(bootstrap.Command) != 3 || !strings.Contains(bootstrap.Command[2], "staging=\"$parent/.pgshard-init\"") || !strings.Contains(bootstrap.Command[2], "host all all all scram-sha-256") || !strings.Contains(bootstrap.Command[2], "cmp -s -- \"$marker\" \"$expected\"") || !strings.Contains(bootstrap.Command[2], "sync \"$staging/pg_hba.conf\" \"$staging/.pgshard-bootstrap-complete\" \"$staging\"") || !strings.Contains(bootstrap.Command[2], "sync \"$final\" \"$parent\"") || strings.Contains(bootstrap.Command[2], "\nsync\n") || strings.Contains(bootstrap.Command[2], "sync -f") || !strings.Contains(bootstrap.Command[2], "cp -- \"$expected\" \"$staging/.pgshard-bootstrap-complete\"") || !strings.Contains(bootstrap.Command[2], "mv -- \"$staging\" \"$final\"") || !strings.Contains(bootstrap.Command[2], postgresqlBootstrapMarker) {
			t.Fatalf("PostgreSQL atomic bootstrap contract = %#v", bootstrap)
		}
		if len(bootstrap.Env) != 2 || bootstrap.Env[0].Name != "PGSHARD_CLUSTER_UID" || bootstrap.Env[0].Value != string(cluster.UID) || bootstrap.Env[1].Name != "PGSHARD_SHARD_ID" || bootstrap.Env[1].Value != shardLabel(shard) {
			t.Fatalf("PostgreSQL bootstrap identity = %#v", bootstrap.Env)
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
			command.Env = append(os.Environ(), "PGSHARD_CLUSTER_UID="+test.clusterUID, "PGSHARD_SHARD_ID="+test.shard)
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
	plan, err = Plan(cluster, DefaultImages())
	if err != nil {
		t.Fatal(err)
	}
	statefulSet := object[*appsv1.StatefulSet](t, plan, PostgreSQLPrimaryStatefulSetName(cluster.Name, 0))
	if len(statefulSet.Name) > 63 || len(statefulSet.Name+"-0") > 63 {
		t.Fatalf("maximum cluster name produced invalid StatefulSet or Pod name: %q", statefulSet.Name)
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
