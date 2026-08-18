package v1alpha1_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

var k8sClient client.Client

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		fmt.Fprint(os.Stderr, "envtest: KUBEBUILDER_ASSETS is not set; run 'make envtest' to download the control-plane binaries. Skipping API tests.\n")
		os.Exit(0)
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		fmt.Fprint(os.Stderr, "envtest: start: "+err.Error()+"\n")
		os.Exit(1)
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := pgshardv1alpha1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		panic(err)
	}
	code := m.Run()
	_ = env.Stop()
	os.Exit(code)
}

func validCluster(name string) *pgshardv1alpha1.PgShardCluster {
	return &pgshardv1alpha1.PgShardCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: pgshardv1alpha1.PgShardClusterSpec{
			PostgreSQL: pgshardv1alpha1.PostgreSQLSpec{Major: 18},
			Catalog:    pgshardv1alpha1.CatalogSpec{Storage: pgshardv1alpha1.StorageSpec{Size: resource.MustParse("1Gi")}},
			Storage:    pgshardv1alpha1.StorageSpec{Size: resource.MustParse("10Gi")},
		},
	}
}

func create(t *testing.T, obj client.Object) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := k8sClient.Create(ctx, obj)
	if err == nil {
		t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), obj) })
	}
	return err
}

func mustReject(t *testing.T, obj client.Object, want string) {
	t.Helper()
	err := create(t, obj)
	if err == nil {
		t.Fatalf("expected rejection containing %q, object was accepted", want)
	}
	if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), want) {
		t.Fatalf("expected Invalid error containing %q, got: %v", want, err)
	}
}

func TestClusterDefaultsApplied(t *testing.T) {
	c := validCluster("defaults")
	if err := create(t, c); err != nil {
		t.Fatal(err)
	}
	s := c.Spec
	if s.PostgreSQL.Profile != "oltp" {
		t.Errorf("profile = %q, want oltp", s.PostgreSQL.Profile)
	}
	if s.ReplicasPerShard != 3 {
		t.Errorf("replicasPerShard = %d, want 3", s.ReplicasPerShard)
	}
	if s.Catalog.Replicas != 3 {
		t.Errorf("catalog.replicas = %d, want 3", s.Catalog.Replicas)
	}
	if s.Resharding.RetireOldGroupsAfter == nil || s.Resharding.RetireOldGroupsAfter.Duration != 24*time.Hour {
		t.Errorf("retireOldGroupsAfter = %v, want 24h", s.Resharding.RetireOldGroupsAfter)
	}
	if s.Resharding.PauseBefore != "none" {
		t.Errorf("pauseBefore = %q, want none", s.Resharding.PauseBefore)
	}
	if s.Durability.SynchronousCommit != "on" || s.Durability.MinSyncStandbys != 1 {
		t.Errorf("durability = %+v", s.Durability)
	}
	if s.Router.MinReplicas != 2 || s.Router.MaxReplicas != 10 || s.Router.HPA.CPUUtilization != 70 {
		t.Errorf("router = %+v", s.Router)
	}
	if s.Admin.Enabled == nil || !*s.Admin.Enabled {
		t.Errorf("admin.enabled = %v, want true", s.Admin.Enabled)
	}
	if s.Upgrade.Strategy != "online" || s.Upgrade.MaxParallelGroups != 1 {
		t.Errorf("upgrade = %+v", s.Upgrade)
	}
	if s.Shards != nil {
		t.Errorf("shards defaulted to %d, want unset", *s.Shards)
	}
}

func TestClusterStatusSubresourceIgnoredOnCreate(t *testing.T) {
	c := validCluster("status")
	c.Status.ShardMapGeneration = 7
	if err := create(t, c); err != nil {
		t.Fatal(err)
	}
	if c.Status.ShardMapGeneration != 0 {
		t.Fatalf("status persisted on create: %+v", c.Status)
	}
	c.Status.ShardMapGeneration = 7
	c.Status.Conditions = []metav1.Condition{{Type: pgshardv1alpha1.ConditionReady, Status: metav1.ConditionTrue, Reason: "Test", LastTransitionTime: metav1.Now()}}
	if err := k8sClient.Status().Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if c.Status.ShardMapGeneration != 7 || len(c.Status.Conditions) != 1 {
		t.Fatalf("status update lost: %+v", c.Status)
	}
}

func TestClusterReplicasPerShardBelowThreeRejected(t *testing.T) {
	c := validCluster("rps")
	c.Spec.ReplicasPerShard = 2
	mustReject(t, c, "replicasPerShard must be >= 3")
}

func TestClusterCatalogReplicasBelowThreeRejected(t *testing.T) {
	c := validCluster("cat")
	c.Spec.Catalog.Replicas = 1
	mustReject(t, c, "catalog.replicas must be >= 3 for HA")
}

func TestClusterRouterMaxBelowMinRejected(t *testing.T) {
	c := validCluster("router")
	c.Spec.Router = pgshardv1alpha1.RouterSpec{MinReplicas: 5, MaxReplicas: 4}
	mustReject(t, c, "router.maxReplicas must be >= router.minReplicas")
}

func TestClusterRouterMaxEqualMinAccepted(t *testing.T) {
	c := validCluster("router-eq")
	c.Spec.Router = pgshardv1alpha1.RouterSpec{MinReplicas: 4, MaxReplicas: 4}
	if err := create(t, c); err != nil {
		t.Fatal(err)
	}
}

func TestClusterUnsafeParametersRejected(t *testing.T) {
	for _, key := range []string{"fsync", "full_page_writes", "wal_level", "max_prepared_transactions", "ssl", "synchronous_commit"} {
		t.Run(key, func(t *testing.T) {
			c := validCluster("param-" + strings.ReplaceAll(key, "_", "-"))
			c.Spec.PostgreSQL.Parameters = map[string]string{key: "off"}
			mustReject(t, c, "parameters must not set "+key)
		})
	}
}

func TestClusterSafeParametersAccepted(t *testing.T) {
	c := validCluster("param-ok")
	c.Spec.PostgreSQL.Parameters = map[string]string{"work_mem": "64MB", "log_min_duration_statement": "500"}
	if err := create(t, c); err != nil {
		t.Fatal(err)
	}
}

func TestClusterEnumsRejected(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*pgshardv1alpha1.PgShardCluster)
	}{
		{"major", func(c *pgshardv1alpha1.PgShardCluster) { c.Spec.PostgreSQL.Major = 17 }},
		{"profile", func(c *pgshardv1alpha1.PgShardCluster) { c.Spec.PostgreSQL.Profile = "olap" }},
		{"upgrade-strategy", func(c *pgshardv1alpha1.PgShardCluster) { c.Spec.Upgrade.Strategy = "rolling" }},
		{"pause-before", func(c *pgshardv1alpha1.PgShardCluster) { c.Spec.Resharding.PauseBefore = "later" }},
		{"sync-commit", func(c *pgshardv1alpha1.PgShardCluster) { c.Spec.Durability.SynchronousCommit = "off" }},
		{"shards-zero", func(c *pgshardv1alpha1.PgShardCluster) { z := 0; c.Spec.Shards = &z }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validCluster("enum-" + tc.name)
			tc.mut(c)
			if err := create(t, c); err == nil || !apierrors.IsInvalid(err) {
				t.Fatalf("expected Invalid error, got %v", err)
			}
		})
	}
}

func TestClusterMissingRequiredRejected(t *testing.T) {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "pgshard.io/v1alpha1",
		"kind":       "PgShardCluster",
		"metadata":   map[string]any{"name": "noreq", "namespace": "default"},
		"spec": map[string]any{
			"postgresql": map[string]any{"major": 18},
			"catalog":    map[string]any{"storage": map[string]any{"size": "1Gi"}},
			"storage":    map[string]any{},
		},
	}}
	mustReject(t, u, "spec.storage.size")
}

func TestClusterFullSpecAccepted(t *testing.T) {
	c := validCluster("full")
	sc := "fast"
	shards := 4
	c.Spec.Shards = &shards
	c.Spec.PostgreSQL.Major = 19
	c.Spec.PostgreSQL.Profile = "analytics"
	c.Spec.Storage.StorageClassName = &sc
	c.Spec.Resources = corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("8Gi")}}
	c.Spec.Router.TLS.SecretRef = &corev1.LocalObjectReference{Name: "router-tls"}
	c.Spec.Backup.PolicyRef = "nightly"
	if err := create(t, c); err != nil {
		t.Fatal(err)
	}
	if c.Spec.Shards == nil || *c.Spec.Shards != 4 {
		t.Fatalf("shards = %v", c.Spec.Shards)
	}
}

func TestGroupKindEnum(t *testing.T) {
	g := &pgshardv1alpha1.PgShardGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "g1", Namespace: "default"},
		Spec:       pgshardv1alpha1.PgShardGroupSpec{ClusterRef: "c", Kind: "bogus"},
	}
	if err := create(t, g); err == nil || !apierrors.IsInvalid(err) {
		t.Fatalf("expected Invalid, got %v", err)
	}
	g.Spec.Kind = "shard"
	if err := create(t, g); err != nil {
		t.Fatal(err)
	}
}

func TestBackupPolicyStoreTypeEnum(t *testing.T) {
	p := &pgshardv1alpha1.PgShardBackupPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: "default"},
		Spec:       pgshardv1alpha1.PgShardBackupPolicySpec{ObjectStore: pgshardv1alpha1.ObjectStoreSpec{Type: "ftp"}},
	}
	if err := create(t, p); err == nil || !apierrors.IsInvalid(err) {
		t.Fatalf("expected Invalid, got %v", err)
	}
	p.Spec.ObjectStore = pgshardv1alpha1.ObjectStoreSpec{Type: "s3", Bucket: "b", Region: "r", Credentials: pgshardv1alpha1.SecretRefSpec{SecretRef: &corev1.LocalObjectReference{Name: "creds"}}}
	p.Spec.Schedules.Full = "0 2 * * 0"
	p.Spec.Retention.Full = 4
	if err := create(t, p); err != nil {
		t.Fatal(err)
	}
}

func TestBackupTypeDefaultsToFull(t *testing.T) {
	b := &pgshardv1alpha1.PgShardBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "b1", Namespace: "default"},
		Spec:       pgshardv1alpha1.PgShardBackupSpec{ClusterName: "c"},
	}
	if err := create(t, b); err != nil {
		t.Fatal(err)
	}
	if b.Spec.Type != "full" {
		t.Fatalf("type = %q", b.Spec.Type)
	}
}

func TestRestoreTargetMutuallyExclusive(t *testing.T) {
	lsn := "0/1000000"
	r := &pgshardv1alpha1.PgShardRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "r1", Namespace: "default"},
		Spec: pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "c", BackupID: "x", Target: pgshardv1alpha1.RestoreTarget{
			LSN: &lsn, Time: &metav1.Time{Time: time.Now()},
		}},
	}
	mustReject(t, r, "at most one of target.time")
	r.Spec.Target = pgshardv1alpha1.RestoreTarget{LSN: &lsn}
	if err := create(t, r); err != nil {
		t.Fatal(err)
	}
	imm := false
	r2 := &pgshardv1alpha1.PgShardRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "r2", Namespace: "default"},
		Spec:       pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "c", Target: pgshardv1alpha1.RestoreTarget{LSN: &lsn, Immediate: &imm}},
	}
	if err := create(t, r2); err != nil {
		t.Fatalf("immediate=false should not count as a target: %v", err)
	}
}

func TestReshardTargetShardsMinimum(t *testing.T) {
	r := &pgshardv1alpha1.PgShardReshard{
		ObjectMeta: metav1.ObjectMeta{Name: "rs1", Namespace: "default"},
		Spec:       pgshardv1alpha1.PgShardReshardSpec{ClusterName: "c", TargetShards: 0},
	}
	if err := create(t, r); err == nil || !apierrors.IsInvalid(err) {
		t.Fatalf("expected Invalid, got %v", err)
	}
	r.Spec.TargetShards = 8
	if err := create(t, r); err != nil {
		t.Fatal(err)
	}
	r.Status.Phase = "Copying"
	r.Status.JournalIDs = []string{"j1"}
	if err := k8sClient.Status().Update(context.Background(), r); err != nil {
		t.Fatal(err)
	}
}
