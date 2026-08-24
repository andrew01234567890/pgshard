package operator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/placement"
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
	// standbys is keyed by pod IP; missing IPs are unreachable.
	standbys map[string]StandbyState
	// published records shard_status upserts as "shard-<id>:<epoch>:<endpoint>".
	published []string
	// slots records EnsureSlots calls as "<dsn host>:<want>:-<drop>".
	slots      []string
	placements []PlacementWorkflowInfo
	// journal records fence writes and promotions in order.
	journal *[]string
	// settings is the pg_settings view of every member; contexts default to
	// "sighup" for names not listed.
	settings map[string]SettingState
	// shardSets is the fake catalog's pgshard.shard_sets with ranges;
	// endpoints the published primary endpoints keyed by "<set>/<group>";
	// workflows the reshard workflow per shard set.
	shardSets []ShardSetInfo
	endpoints map[string]string
	workflows map[string]WorkflowInfo
	// cutoverSpecs records SetReshardCutoverSpec calls as
	// "<id>:<pause>:<proceed>:<retire>".
	cutoverSpecs []string
}

func (f *fakeProber) SetShardSetMajor(context.Context, string, string, int) error { return nil }

func (f *fakeProber) SetWorkflowRollback(context.Context, string, string) error { return nil }

func (f *fakeProber) SetReshardCutoverSpec(_ context.Context, _ string, id, pause string, proceed []string, retire int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cutoverSpecs = append(f.cutoverSpecs, fmt.Sprintf("%s:%s:%s:%d", id, pause, strings.Join(proceed, "+"), retire))
	return nil
}

func (f *fakeProber) ShardSets(_ context.Context, _ string) ([]ShardSetInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]ShardSetInfo, len(f.shardSets))
	copy(out, f.shardSets)
	return out, nil
}

func (f *fakeProber) MaterializeShardSet(_ context.Context, _ string, name string, generation int64, state string, ranges placement.RangeSet) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, s := range f.shardSets {
		if s.Name == name {
			if len(s.Ranges) > 0 {
				return fmt.Errorf("shard set %s already has ranges", name)
			}
			f.shardSets[i].Ranges = ranges
			f.shardSets[i].State = state
			return nil
		}
	}
	f.shardSets = append(f.shardSets, ShardSetInfo{Name: name, Generation: generation, State: state, Ranges: ranges})
	return nil
}

func (f *fakeProber) DropShardSet(_ context.Context, _ string, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	kept := f.shardSets[:0]
	for _, s := range f.shardSets {
		if s.Name != name {
			kept = append(kept, s)
		}
	}
	f.shardSets = kept
	for k := range f.endpoints {
		if strings.HasPrefix(k, name+"/") {
			delete(f.endpoints, k)
		}
	}
	return nil
}

func (f *fakeProber) PlacementWorkflows(context.Context, string) ([]PlacementWorkflowInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.placements, nil
}

func (f *fakeProber) ReshardWorkflow(_ context.Context, _ string, set string) (WorkflowInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.workflows[set], nil
}

// setShardSetState mimics the controller moving a pending set along.
func (f *fakeProber) setShardSetState(name, state string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.shardSets {
		if f.shardSets[i].Name == name {
			f.shardSets[i].State = state
		}
	}
}

func (f *fakeProber) shardSet(name string) (ShardSetInfo, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range f.shardSets {
		if s.Name == name {
			return s, true
		}
	}
	return ShardSetInfo{}, false
}

func (f *fakeProber) Settings(_ context.Context, _ string, names []string) (map[string]SettingState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]SettingState{}
	for _, n := range names {
		st, ok := f.settings[n]
		if !ok {
			st = SettingState{Context: "sighup"}
		}
		out[n] = st
	}
	return out, nil
}

func (f *fakeProber) ProbeStandby(_ context.Context, dsn string) (StandbyState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for ip, st := range f.standbys {
		if strings.Contains(dsn, "@"+ip+":") {
			return st, nil
		}
	}
	return StandbyState{}, errors.New("unreachable")
}

func (f *fakeProber) PublishShardStatus(_ context.Context, _ string, g Group, epoch int64, endpoint string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published = append(f.published, fmt.Sprintf("%s:%d:%s", g.Name(), epoch, endpoint))
	if f.endpoints == nil {
		f.endpoints = map[string]string{}
	}
	f.endpoints[g.ShardSet()+"/"+g.Name()] = endpoint
	if f.journal != nil {
		*f.journal = append(*f.journal, fmt.Sprintf("publish:%d", epoch))
	}
	return nil
}

func (f *fakeProber) EnsureSlots(_ context.Context, dsn string, want []string, drop string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.slots = append(f.slots, fmt.Sprintf("%s:%s:-%s", dsn[strings.Index(dsn, "@")+1:], strings.Join(want, ","), drop))
	return nil
}

type fakeAgents struct {
	mu       sync.Mutex
	status   map[string]AgentStatus
	errs     map[string]error
	promotes []string
	demotes  []string
	journal  *[]string
	// reloadHash is what Reload reports per addr; reloads records calls.
	reloadHash map[string]string
	reloads    []string
	// syncSlots records the last SetSynchronizedStandbySlots call per addr.
	syncSlots map[string][]string
}

func (f *fakeAgents) SetSynchronizedStandbySlots(_ context.Context, addr string, slots []string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs[addr]; err != nil {
		return nil, err
	}
	if f.syncSlots == nil {
		f.syncSlots = map[string][]string{}
	}
	f.syncSlots[addr] = slots
	return slots, nil
}

func (f *fakeAgents) Reload(_ context.Context, addr string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs[addr]; err != nil {
		return "", err
	}
	f.reloads = append(f.reloads, addr)
	return f.reloadHash[addr], nil
}

func newFakeAgents(journal *[]string) *fakeAgents {
	return &fakeAgents{status: map[string]AgentStatus{}, errs: map[string]error{}, journal: journal, reloadHash: map[string]string{}}
}

func (f *fakeAgents) set(ip string, st AgentStatus, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status[agentAddr(ip)] = st
	if err != nil {
		f.errs[agentAddr(ip)] = err
	} else {
		delete(f.errs, agentAddr(ip))
	}
}

func (f *fakeAgents) Status(_ context.Context, addr string) (AgentStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs[addr]; err != nil {
		return AgentStatus{}, err
	}
	st, ok := f.status[addr]
	if !ok {
		return AgentStatus{}, errors.New("unknown agent " + addr)
	}
	return st, nil
}

func (f *fakeAgents) Promote(_ context.Context, addr string, epoch uint64, holder string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs[addr]; err != nil {
		return err
	}
	st := f.status[addr]
	if epoch <= st.Epoch {
		return fmt.Errorf("stale epoch %d <= %d", epoch, st.Epoch)
	}
	st.Epoch, st.Primary, st.Running = epoch, true, true
	f.status[addr] = st
	f.promotes = append(f.promotes, fmt.Sprintf("%s:%d:%s", addr, epoch, holder))
	if f.journal != nil {
		*f.journal = append(*f.journal, fmt.Sprintf("promote:%d", epoch))
	}
	return nil
}

func (f *fakeAgents) Demote(_ context.Context, addr string, epoch uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	st := f.status[addr]
	if epoch <= st.Epoch {
		return fmt.Errorf("stale epoch %d <= %d", epoch, st.Epoch)
	}
	st.Epoch, st.Primary = epoch, false
	f.status[addr] = st
	f.demotes = append(f.demotes, fmt.Sprintf("%s:%d", addr, epoch))
	return nil
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
			Router:           pgshardv1alpha1.RouterSpec{MinReplicas: 2, MaxReplicas: 5},
		},
	}
}

func setup(t *testing.T, name string) (*ClusterReconciler, *fakeProber, *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	r, fp, _, c := setupWithAgents(t, name)
	return r, fp, c
}

func setupWithAgents(t *testing.T, name string) (*ClusterReconciler, *fakeProber, *fakeAgents, *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	requireEnvtest(t)
	ctx := context.Background()
	c := newCluster(name)
	if err := k8sClient.Create(ctx, c); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), c) })
	journal := &[]string{}
	fp := &fakeProber{err: errors.New("dial: refused"), standbys: map[string]StandbyState{}, journal: journal}
	fa := newFakeAgents(journal)
	r := &ClusterReconciler{Client: k8sClient, Renderer: Renderer{RouterImage: "router:test"}, Prober: fp, Agents: fa, FailoverDelay: time.Nanosecond, PollInterval: time.Millisecond, QuiesceTimeout: 200 * time.Millisecond}
	return r, fp, fa, c
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
		for i := 0; i < g.Replicas; i++ {
			cfg := cm.Data[g.MemberName(i)+".json"]
			wantRole := `"role": "standby"`
			if i == 0 {
				wantRole = `"role": "primary"`
			}
			for _, want := range []string{wantRole, `"member": "` + g.MemberName(i) + `"`, `"work_mem": "8MB"`, `"enabled": true`, `"namespace": "default"`, g.ServiceRW() + ".default.svc"} {
				if !strings.Contains(cfg, want) {
					t.Errorf("agent config for %s missing %q:\n%s", g.MemberName(i), want, cfg)
				}
			}
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
				if len(svc.Spec.Ports) != 2 || svc.Spec.Ports[1].Name != "pooler-grpc" || svc.Spec.Ports[1].Port != 9091 || svc.Spec.Ports[1].TargetPort.IntValue() != 9091 {
					t.Errorf("-rw must expose the pooler: %+v", svc.Spec.Ports)
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
			if ctr.Image != "ghcr.io/andrew01234567890/pgshard-postgres:18" || strings.Join(ctr.Command, " ") != "pgshard-agent" || strings.Join(ctr.Args, " ") != "run --config /etc/pgshard/"+pod.Name+".json" {
				t.Errorf("pod %s container: image=%s command=%v args=%v", pod.Name, ctr.Image, ctr.Command, ctr.Args)
			}
			if ctr.ReadinessProbe.HTTPGet == nil || ctr.ReadinessProbe.HTTPGet.Path != "/readyz" || ctr.StartupProbe.HTTPGet.Path != "/startz" || ctr.LivenessProbe.HTTPGet.Path != "/livez" {
				t.Errorf("pod %s probes must hit the agent endpoints: %+v", pod.Name, ctr)
			}
			if pod.Spec.ServiceAccountName != MemberServiceAccount(c.Name) {
				t.Errorf("pod %s service account %q", pod.Name, pod.Spec.ServiceAccountName)
			}
			if pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName != pod.Name {
				t.Errorf("pod %s must mount its own PVC: %+v", pod.Name, pod.Spec.Volumes[0])
			}
			if len(pod.Spec.Containers) != 2 || pod.Spec.Containers[1].Name != "pooler" || pod.Spec.Containers[1].Image != ctr.Image || pod.Spec.Containers[1].ReadinessProbe.TCPSocket == nil {
				t.Errorf("pod %s must carry the pooler sidecar from the same image: %+v", pod.Name, pod.Spec.Containers)
			}
			var pvc corev1.PersistentVolumeClaim
			get(t, g.MemberName(i), &pvc)
			ownedBy(t, &pvc, c)
			if pvc.Spec.Resources.Requests.Storage().String() != g.Storage.Size.String() {
				t.Errorf("pvc %s size %s want %s", pvc.Name, pvc.Spec.Resources.Requests.Storage(), g.Storage.Size.String())
			}
		}
	}
	if cond := condition(t, "gen", pgshardv1alpha1.ConditionReady); cond.Status != metav1.ConditionFalse || !strings.Contains(cond.Message, "primary unhealthy: pod has no IP yet") {
		t.Errorf("Ready must be False with the probe error while pods are not running: %+v", cond)
	}
	if cond := condition(t, "gen", ConditionCatalogReady); cond.Status != metav1.ConditionFalse {
		t.Errorf("CatalogReady must be False: %+v", cond)
	}
	var dep appsv1.Deployment
	get(t, "gen-router", &dep)
	ownedBy(t, &dep, c)
	if *dep.Spec.Replicas != 2 || dep.Spec.Template.Spec.Containers[0].Image != "router:test" {
		t.Errorf("router deployment: replicas=%d image=%s", *dep.Spec.Replicas, dep.Spec.Template.Spec.Containers[0].Image)
	}
	var hpa autoscalingv2.HorizontalPodAutoscaler
	get(t, "gen-router", &hpa)
	ownedBy(t, &hpa, c)
	if *hpa.Spec.MinReplicas != 2 || hpa.Spec.MaxReplicas != 5 || hpa.Spec.ScaleTargetRef.Name != "gen-router" || *hpa.Spec.Metrics[0].Resource.Target.AverageUtilization != 70 {
		t.Errorf("router hpa: %+v", hpa.Spec)
	}
	var rpdb policyv1.PodDisruptionBudget
	get(t, "gen-router", &rpdb)
	ownedBy(t, &rpdb, c)
	if rpdb.Spec.MinAvailable.IntValue() != 1 || rpdb.Spec.Selector.MatchLabels[LabelComponent] != "router" {
		t.Errorf("router pdb: %+v", rpdb.Spec)
	}
	var rsvc corev1.Service
	get(t, "gen-router", &rsvc)
	ownedBy(t, &rsvc, c)
	if rsvc.Spec.Ports[0].Port != 5432 || rsvc.Spec.Selector[LabelComponent] != "router" {
		t.Errorf("router service: %+v", rsvc.Spec)
	}
	var rsa corev1.ServiceAccount
	get(t, "gen-router", &rsa)
	ownedBy(t, &rsa, c)

	var role rbacv1.Role
	get(t, MemberServiceAccount("gen"), &role)
	ownedBy(t, &role, c)
	if len(role.Rules) != 3 || role.Rules[0].Resources[0] != "leases" {
		t.Errorf("member role rules: %+v", role.Rules)
	}
	if strings.Join(role.Rules[0].Verbs, ",") != "get,list,watch,create" || len(role.Rules[0].ResourceNames) != 0 {
		t.Errorf("unscoped lease rule must not write: %+v", role.Rules[0])
	}
	if strings.Join(role.Rules[1].Verbs, ",") != "get,update,patch" || strings.Join(role.Rules[1].ResourceNames, ",") != strings.Join(ownLeases(c), ",") {
		t.Errorf("scoped lease rule: %+v want %v", role.Rules[1], ownLeases(c))
	}
	var rb rbacv1.RoleBinding
	get(t, MemberServiceAccount("gen"), &rb)
	if rb.RoleRef.Name != role.Name || rb.Subjects[0].Name != MemberServiceAccount("gen") {
		t.Errorf("member role binding: %+v", rb)
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

// podIP is the deterministic fake IP of member i of group index gi.
func podIP(gi, i int) string { return fmt.Sprintf("10.%d.0.%d", gi+1, i+1) }

func markPodRunning(t *testing.T, name, ip string) {
	t.Helper()
	var pod corev1.Pod
	get(t, name, &pod)
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = ip
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if err := k8sClient.Status().Update(context.Background(), &pod); err != nil {
		t.Fatal(err)
	}
}

func markPodsRunning(t *testing.T, c *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	for gi, g := range Groups(c) {
		for i := 0; i < g.Replicas; i++ {
			markPodRunning(t, g.MemberName(i), podIP(gi, i))
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
	if pg.Status.Primary != "ready-shard-0-0" || !pg.Status.Members[1].Ready || pg.Status.Members[2].Ready {
		t.Errorf("designated primary and the last-known sync set (member 1 only) must survive a failed probe: %+v", pg.Status)
	}
}

func TestSyncStandbyNamesAppliedHealthyFirst(t *testing.T) {
	r, fp, fa, c := setupWithAgents(t, "sync")
	reconcile(t, r, c)
	markPodsRunning(t, c)
	fp.err = nil
	fp.streaming = map[string]bool{"sync-catalog-2": true}
	reconcile(t, r, c)
	got := fp.syncNames[DSN("sync-catalog-rw", "default", currentPassword(t, "sync"))]
	if got != `ANY 1 ("sync-catalog-2", "sync-catalog-1")` {
		t.Fatalf("catalog sync names: %q", got)
	}
	if slots := fa.syncSlots[agentAddr(podIP(0, 0))]; !reflect.DeepEqual(slots, []string{SlotName("sync-catalog-2")}) {
		t.Fatalf("synchronized_standby_slots must list the streaming standby's slot only: %v", slots)
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

func ownLeases(c *pgshardv1alpha1.PgShardCluster) []string {
	var out []string
	for _, g := range Groups(c) {
		out = append(out, g.LeaseName())
	}
	return out
}
