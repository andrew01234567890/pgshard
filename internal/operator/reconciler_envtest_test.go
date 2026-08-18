package operator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

var k8sClient client.Client

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		fmt.Fprint(os.Stderr, "envtest: KUBEBUILDER_ASSETS is not set; run 'make envtest'. Skipping reconciler envtests.\n")
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
	scheme, err := NewScheme()
	if err != nil {
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

type fakeProber struct {
	mu        sync.Mutex
	err       error
	streaming map[string]bool
	syncNames map[string]string
	setCalls  []string
	migrated  int
}

func (f *fakeProber) Probe(_ context.Context, dsn string) (PrimaryState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return PrimaryState{}, f.err
	}
	st := PrimaryState{Streaming: map[string]bool{}, SyncStandbyNames: f.syncNames[dsn]}
	for k, v := range f.streaming {
		st.Streaming[k] = v
	}
	return st, nil
}

func (f *fakeProber) SetSyncStandbyNames(_ context.Context, dsn, value string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.syncNames == nil {
		f.syncNames = map[string]string{}
	}
	f.syncNames[dsn] = value
	f.setCalls = append(f.setCalls, value)
	return nil
}

func (f *fakeProber) MigrateCatalog(context.Context, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.migrated++
	return nil
}

func requireEnvtest(t *testing.T) {
	t.Helper()
	if k8sClient == nil {
		t.Skip("KUBEBUILDER_ASSETS not set")
	}
}

func newCluster(name string) *pgshardv1alpha1.PgShardCluster {
	return &pgshardv1alpha1.PgShardCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: pgshardv1alpha1.PgShardClusterSpec{
			PostgreSQL:       pgshardv1alpha1.PostgreSQLSpec{Major: 18, Parameters: map[string]string{"work_mem": "8MB"}},
			Catalog:          pgshardv1alpha1.CatalogSpec{Replicas: 3, Storage: pgshardv1alpha1.StorageSpec{Size: resource.MustParse("1Gi")}},
			ReplicasPerShard: 3,
			Storage:          pgshardv1alpha1.StorageSpec{Size: resource.MustParse("2Gi")},
		},
	}
}

func setup(t *testing.T, name string) (*ClusterReconciler, *fakeProber, *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	requireEnvtest(t)
	ctx := context.Background()
	c := newCluster(name)
	if err := k8sClient.Create(ctx, c); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), c) })
	fp := &fakeProber{err: errors.New("dial: refused")}
	r := &ClusterReconciler{Client: k8sClient, Renderer: Renderer{}, Prober: fp}
	return r, fp, c
}

func reconcile(t *testing.T, r *ClusterReconciler, c *pgshardv1alpha1.PgShardCluster) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(c)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return res
}

func get(t *testing.T, name string, obj client.Object) {
	t.Helper()
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, obj); err != nil {
		t.Fatalf("get %T %s: %v", obj, name, err)
	}
}

func ownedBy(t *testing.T, obj client.Object, c *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	ref := metav1.GetControllerOf(obj)
	if ref == nil || ref.UID != c.UID || ref.Kind != "PgShardCluster" {
		t.Errorf("%T %s is not controlled by the cluster: %+v", obj, obj.GetName(), ref)
	}
}

func condition(t *testing.T, name, condType string) metav1.Condition {
	t.Helper()
	var c pgshardv1alpha1.PgShardCluster
	get(t, name, &c)
	cond := meta.FindStatusCondition(c.Status.Conditions, condType)
	if cond == nil {
		t.Fatalf("condition %s missing; have %+v", condType, c.Status.Conditions)
	}
	if cond.ObservedGeneration != c.Generation {
		t.Errorf("condition %s observedGeneration=%d want %d", condType, cond.ObservedGeneration, c.Generation)
	}
	return *cond
}

func TestReconcileGeneratesGroupObjects(t *testing.T) {
	r, _, c := setup(t, "gen")
	reconcile(t, r, c)

	var sec corev1.Secret
	get(t, "gen-superuser", &sec)
	ownedBy(t, &sec, c)
	if len(sec.Data["password"]) < 32 {
		t.Errorf("generated password too short: %d", len(sec.Data["password"]))
	}
	for _, g := range Groups(c) {
		var pg pgshardv1alpha1.PgShardGroup
		get(t, g.Prefix(), &pg)
		ownedBy(t, &pg, c)
		if pg.Spec.Kind != g.Kind || pg.Spec.ClusterRef != "gen" {
			t.Errorf("group spec wrong: %+v", pg.Spec)
		}
		var cm corev1.ConfigMap
		get(t, g.ConfigMapName(), &cm)
		ownedBy(t, &cm, c)
		for _, want := range []string{"wal_level = replica", "synchronous_commit = on", "ssl = off", "work_mem = '8MB'"} {
			if !strings.Contains(cm.Data["pgshard.conf"], want) {
				t.Errorf("pgshard.conf missing %q:\n%s", want, cm.Data["pgshard.conf"])
			}
		}
		if _, ok := cm.Data["pgshard.override.conf"]; !ok {
			t.Error("override placeholder missing")
		}
		if !strings.Contains(cm.Data["bootstrap.sh"], "pg_basebackup") {
			t.Error("bootstrap script missing")
		}
		for _, svcName := range []string{g.ServiceRW(), g.ServiceRO(), g.ServiceHeadless()} {
			var svc corev1.Service
			get(t, svcName, &svc)
			ownedBy(t, &svc, c)
			switch svcName {
			case g.ServiceRW():
				if svc.Spec.Selector[LabelRole] != RolePrimary {
					t.Errorf("-rw selector: %v", svc.Spec.Selector)
				}
			case g.ServiceRO():
				if svc.Spec.Selector[LabelRole] != RoleReplica {
					t.Errorf("-ro selector: %v", svc.Spec.Selector)
				}
			default:
				if svc.Spec.ClusterIP != corev1.ClusterIPNone || svc.Spec.Selector[LabelRole] != "" {
					t.Errorf("headless service wrong: %+v", svc.Spec)
				}
			}
		}
		var pdb policyv1.PodDisruptionBudget
		get(t, g.PDBPrimary(), &pdb)
		ownedBy(t, &pdb, c)
		if pdb.Spec.MinAvailable.IntValue() != 1 || pdb.Spec.Selector.MatchLabels[LabelRole] != RolePrimary {
			t.Errorf("primary pdb: %+v", pdb.Spec)
		}
		get(t, g.PDBReplicas(), &pdb)
		if pdb.Spec.MinAvailable.IntValue() != 1 || pdb.Spec.Selector.MatchLabels[LabelRole] != RoleReplica {
			t.Errorf("replica pdb for 3 members must be minAvailable=1: %+v", pdb.Spec)
		}
		for i := 0; i < g.Replicas; i++ {
			var pod corev1.Pod
			get(t, g.MemberName(i), &pod)
			ownedBy(t, &pod, c)
			wantRole := RoleReplica
			if i == 0 {
				wantRole = RolePrimary
			}
			if pod.Labels[LabelRole] != wantRole || pod.Labels[LabelOrdinal] != fmt.Sprint(i) {
				t.Errorf("pod %s labels: %v", pod.Name, pod.Labels)
			}
			ctr := pod.Spec.Containers[0]
			if ctr.Image != "ghcr.io/andrew01234567890/pgshard-postgres:18" || strings.Join(ctr.Command, " ") != "bash /etc/pgshard/bootstrap.sh" {
				t.Errorf("pod %s container: image=%s command=%v", pod.Name, ctr.Image, ctr.Command)
			}
			if pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName != pod.Name {
				t.Errorf("pod %s must mount its own PVC: %+v", pod.Name, pod.Spec.Volumes[0])
			}
			var pvc corev1.PersistentVolumeClaim
			get(t, g.MemberName(i), &pvc)
			ownedBy(t, &pvc, c)
			if pvc.Spec.Resources.Requests.Storage().String() != g.Storage.Size.String() {
				t.Errorf("pvc %s size %s want %s", pvc.Name, pvc.Spec.Resources.Requests.Storage(), g.Storage.Size.String())
			}
		}
	}
	if cond := condition(t, "gen", pgshardv1alpha1.ConditionReady); cond.Status != metav1.ConditionFalse || !strings.Contains(cond.Message, "dial: refused") {
		t.Errorf("Ready must be False with the probe error while pods are not running: %+v", cond)
	}
	if cond := condition(t, "gen", ConditionCatalogReady); cond.Status != metav1.ConditionFalse {
		t.Errorf("CatalogReady must be False: %+v", cond)
	}
}

func TestReplicaPDBOmittedBelowThreeMembers(t *testing.T) {
	r, _, c := setup(t, "small")
	// The CRD requires >=3 replicas; exercise the renderer directly for the math.
	g := Group{Cluster: c.Name, Kind: "shard", Replicas: 2, Storage: c.Spec.Storage}
	if pdbs := r.Renderer.PDBs(c, g); len(pdbs) != 1 || pdbs[0].Name != g.PDBPrimary() {
		t.Fatalf("2-member group must only get the primary PDB, got %d", len(pdbs))
	}
	g.Replicas = 5
	pdbs := r.Renderer.PDBs(c, g)
	if len(pdbs) != 2 || pdbs[1].Spec.MinAvailable.IntValue() != 3 {
		t.Fatalf("5-member group replica PDB must be minAvailable=3: %+v", pdbs)
	}
}

func markPodsRunning(t *testing.T, c *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	ctx := context.Background()
	for _, g := range Groups(c) {
		for i := 0; i < g.Replicas; i++ {
			var pod corev1.Pod
			get(t, g.MemberName(i), &pod)
			pod.Status.Phase = corev1.PodRunning
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			if err := k8sClient.Status().Update(ctx, &pod); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestReadinessRequiresProbeAndStreaming(t *testing.T) {
	r, fp, c := setup(t, "ready")
	reconcile(t, r, c)
	markPodsRunning(t, c)

	fp.err = nil
	fp.streaming = map[string]bool{}
	res := reconcile(t, r, c)
	if cond := condition(t, "ready", pgshardv1alpha1.ConditionReady); cond.Status != metav1.ConditionFalse || !strings.Contains(cond.Message, "0/2 replicas streaming") {
		t.Fatalf("Ready must be False without streaming replicas: %+v", cond)
	}
	if res.RequeueAfter != requeueNotReady {
		t.Errorf("not-ready requeue: %v", res.RequeueAfter)
	}
	if fp.migrated != 0 {
		t.Fatal("catalog migration must not run before the catalog group is ready")
	}

	fp.streaming = map[string]bool{
		"ready-catalog-1": true, "ready-catalog-2": true,
		"ready-shard-0-1": true, "ready-shard-0-2": true,
	}
	res = reconcile(t, r, c)
	if cond := condition(t, "ready", pgshardv1alpha1.ConditionReady); cond.Status != metav1.ConditionTrue {
		t.Fatalf("Ready must be True: %+v", cond)
	}
	if cond := condition(t, "ready", ConditionCatalogReady); cond.Status != metav1.ConditionTrue || fp.migrated != 1 {
		t.Fatalf("CatalogReady must be True after one migration, migrated=%d: %+v", fp.migrated, cond)
	}
	if res.RequeueAfter != requeueReady {
		t.Errorf("ready requeue: %v", res.RequeueAfter)
	}
	var pg pgshardv1alpha1.PgShardGroup
	get(t, "ready-shard-0", &pg)
	if pg.Status.Primary != "ready-shard-0-0" || pg.Status.Epoch != 0 || len(pg.Status.Members) != 3 || !pg.Status.Members[2].Ready {
		t.Errorf("group status: %+v", pg.Status)
	}
	var cl pgshardv1alpha1.PgShardCluster
	get(t, "ready", &cl)
	if cl.Status.ObservedGeneration != cl.Generation || len(cl.Status.Shards) != 1 || cl.Status.Shards[0].Primary != "ready-shard-0-0" {
		t.Errorf("cluster status: %+v", cl.Status)
	}

	fp.streaming["ready-shard-0-2"] = false
	reconcile(t, r, c)
	if cond := condition(t, "ready", pgshardv1alpha1.ConditionReady); cond.Status != metav1.ConditionFalse {
		t.Fatalf("Ready must drop to False when a replica stops streaming: %+v", cond)
	}
	if cond := condition(t, "ready", pgshardv1alpha1.ConditionReplicationHealthy); cond.Status != metav1.ConditionFalse {
		t.Errorf("ReplicationHealthy must be False: %+v", cond)
	}
	get(t, "ready-shard-0", &pg)
	if pg.Status.Members[2].Ready {
		t.Errorf("member 2 must be reported not ready when not streaming: %+v", pg.Status.Members)
	}

	fp.streaming["ready-shard-0-2"] = true
	fp.err = errors.New("connection reset")
	reconcile(t, r, c)
	if cond := condition(t, "ready", pgshardv1alpha1.ConditionReady); cond.Status != metav1.ConditionFalse || !strings.Contains(cond.Message, "connection reset") {
		t.Fatalf("Ready must be False when the primary probe fails: %+v", cond)
	}
	if cond := condition(t, "ready", pgshardv1alpha1.ConditionPrimaryHealthy); cond.Status != metav1.ConditionFalse {
		t.Errorf("PrimaryHealthy must be False: %+v", cond)
	}
	get(t, "ready-shard-0", &pg)
	if pg.Status.Primary != "" {
		t.Errorf("group primary must be cleared while the probe fails: %+v", pg.Status)
	}
}

func TestSyncStandbyNamesAppliedHealthyFirst(t *testing.T) {
	r, fp, c := setup(t, "sync")
	fp.err = nil
	fp.streaming = map[string]bool{"sync-catalog-2": true}
	reconcile(t, r, c)
	got := fp.syncNames[DSN("sync-catalog-rw", "default", currentPassword(t, "sync"))]
	if got != `ANY 1 ("sync-catalog-2", "sync-catalog-1")` {
		t.Fatalf("catalog sync names: %q", got)
	}
	if got := fp.syncNames[DSN("sync-shard-0-rw", "default", currentPassword(t, "sync"))]; got != `ANY 1 ("sync-shard-0-1", "sync-shard-0-2")` {
		t.Fatalf("shard sync names: %q", got)
	}
	calls := len(fp.setCalls)
	reconcile(t, r, c)
	if len(fp.setCalls) != calls {
		t.Fatalf("unchanged sync names must not be re-applied: %v", fp.setCalls)
	}
	fp.streaming = map[string]bool{}
	reconcile(t, r, c)
	if got := fp.syncNames[DSN("sync-catalog-rw", "default", currentPassword(t, "sync"))]; got != `ANY 1 ("sync-catalog-1", "sync-catalog-2")` {
		t.Fatalf("losing a standby must keep NumSync and every name: %q", got)
	}
}

func currentPassword(t *testing.T, cluster string) string {
	t.Helper()
	var sec corev1.Secret
	get(t, SecretName(cluster), &sec)
	return string(sec.Data[secretKey])
}

func TestDeletedPodIsRecreatedWithSamePVC(t *testing.T) {
	r, _, c := setup(t, "recreate")
	reconcile(t, r, c)
	var pvc corev1.PersistentVolumeClaim
	get(t, "recreate-shard-0-2", &pvc)
	var pod corev1.Pod
	get(t, "recreate-shard-0-2", &pod)
	if err := k8sClient.Delete(context.Background(), &pod, client.GracePeriodSeconds(0)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(&pod), &corev1.Pod{})
		if err != nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	reconcile(t, r, c)
	var again corev1.Pod
	get(t, "recreate-shard-0-2", &again)
	if again.UID == pod.UID {
		t.Fatal("pod was not recreated")
	}
	var pvcAgain corev1.PersistentVolumeClaim
	get(t, "recreate-shard-0-2", &pvcAgain)
	if pvcAgain.UID != pvc.UID {
		t.Fatal("PVC must survive pod recreation")
	}
	if again.Spec.Volumes[0].PersistentVolumeClaim.ClaimName != pvcAgain.Name {
		t.Fatal("recreated pod does not mount the original PVC")
	}
}

func TestSecretIsReusedAcrossReconciles(t *testing.T) {
	r, _, c := setup(t, "secret")
	reconcile(t, r, c)
	first := currentPassword(t, "secret")
	reconcile(t, r, c)
	if currentPassword(t, "secret") != first {
		t.Fatal("password rotated on a plain reconcile")
	}
}
