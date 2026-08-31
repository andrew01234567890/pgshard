package operator

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/dockertest"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/placement"
)

var k8sClient client.Client

func TestMain(m *testing.M) {
	if !dockertest.EnvtestAvailable() {
		if code := dockertest.EnvtestMissingMain("the reconciler envtests"); code != 0 {
			os.Exit(code)
		}
		// Without the control plane the envtests skip individually; the
		// rest of the package still runs.
		os.Exit(m.Run())
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	// Every test in this package stands its cluster up in the same control
	// plane, and each one takes a handful of Services. The default range
	// holds 254 addresses and nothing gives them back, so the suite ran out
	// as it grew -- "failed to allocate a serviceIP: range is full", in
	// whichever test happened to be last.
	env.ControlPlane.GetAPIServer().Configure().Set("service-cluster-ip-range", "10.0.0.0/16")
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
	// fenced is the catalog write fence the operator reads; paused records
	// the DSNs it made refuse writes, and pausedDSN those already paused.
	fenced    bool
	fenceErr  error
	paused    []string
	pausedDSN map[string]bool
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
	// routerPasswords records every ALTER ROLE the reconcile asked for.
	routerPasswords []string
	// onRelease runs inside ReleaseCatalog, so a test can see what the
	// cluster looked like at that point of the rollback.
	onRelease func()
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
	// serverMajor is what ServerMajor reports (18 when zero); catalogLag a
	// non-empty catch-up lag; the rest record catalog upgrade calls.
	serverMajor             int
	catalogLag              string
	catalogCopies           []string
	catalogCutovers         []string
	catalogReleases         []string
	catalogRollbacks        []string
	catalogRollbackDrops    []string
	catalogRollbackErr      string
	catalogRollbackDisables []string
	// gateWidth, when set, holds every shard group's Probe until that many
	// of them are waiting at once; gateOpened records that they were.
	gateWidth  int
	gateCount  int
	gateOpen   chan struct{}
	gateOpened bool
}

// waitAtGate blocks a shard group's probe until gateWidth groups are
// probing at once, so a test can tell a concurrent pass from a serial one
// without timing it. A serial pass never gets past one waiter and every
// call falls out on the timeout instead.
func (f *fakeProber) waitAtGate(dsn string) {
	f.mu.Lock()
	if f.gateWidth == 0 || !strings.Contains(dsn, "-shard-") {
		f.mu.Unlock()
		return
	}
	if f.gateOpen == nil {
		f.gateOpen = make(chan struct{})
	}
	open := f.gateOpen
	f.gateCount++
	if f.gateCount >= f.gateWidth {
		f.gateOpened = true
		close(open)
	}
	f.mu.Unlock()
	select {
	case <-open:
	case <-time.After(500 * time.Millisecond):
	}
	f.mu.Lock()
	f.gateCount--
	f.mu.Unlock()
}

func (f *fakeProber) SetShardSetMajor(_ context.Context, _ string, name string, major int) error {
	f.setShardSetMajor(name, major)
	return nil
}

func hostOf(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.Host == "" {
		return dsn
	}
	return u.Hostname()
}

func (f *fakeProber) ServerMajor(context.Context, string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.serverMajor != 0 {
		return f.serverMajor, nil
	}
	return 18, nil
}

func (f *fakeProber) EnsureCatalogCopy(_ context.Context, source, target CatalogSide) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.catalogCopies = append(f.catalogCopies, hostOf(source.DSN)+">"+hostOf(target.DSN))
	return nil
}

func (f *fakeProber) CatalogCopyCaughtUp(context.Context, string) (bool, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.catalogLag != "" {
		return false, f.catalogLag, nil
	}
	return true, "", nil
}

func (f *fakeProber) CutoverCatalog(_ context.Context, source, target CatalogSide) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.catalogCutovers = append(f.catalogCutovers, hostOf(source.DSN)+">"+hostOf(target.DSN))
	return nil
}

func (f *fakeProber) RollbackCatalog(_ context.Context, oldDSN, newDSN string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.catalogRollbackErr != "" {
		return errors.New(f.catalogRollbackErr)
	}
	f.catalogRollbacks = append(f.catalogRollbacks, hostOf(newDSN)+">"+hostOf(oldDSN))
	return nil
}

func (f *fakeProber) DropCatalogRollback(_ context.Context, dsn string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.catalogRollbackDrops = append(f.catalogRollbackDrops, hostOf(dsn))
	return nil
}

func (f *fakeProber) DisableCatalogRollback(_ context.Context, dsn string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.catalogRollbackDisables = append(f.catalogRollbackDisables, hostOf(dsn))
	return nil
}

func (f *fakeProber) ReleaseCatalog(_ context.Context, dsn string) error {
	f.mu.Lock()
	onRelease := f.onRelease
	f.catalogReleases = append(f.catalogReleases, hostOf(dsn))
	f.mu.Unlock()
	if onRelease != nil {
		onRelease()
	}
	return nil
}

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

func (f *fakeProber) MaterializeShardSet(_ context.Context, _ string, name string, generation int64, state string, ranges placement.RangeSet, major int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, s := range f.shardSets {
		if s.Name == name {
			if len(s.Ranges) > 0 {
				return fmt.Errorf("shard set %s already has ranges", name)
			}
			f.shardSets[i].Ranges = ranges
			f.shardSets[i].State = state
			if major > 0 {
				f.shardSets[i].PGMajor = major
			}
			return nil
		}
	}
	f.shardSets = append(f.shardSets, ShardSetInfo{Name: name, Generation: generation, State: state, Ranges: ranges, PGMajor: major})
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

func (f *fakeProber) setStandby(ip string, st StandbyState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.standbys[ip] = st
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

func (f *fakeProber) PublishShardStatus(ctx context.Context, dsn string, rows []ShardStatus) error {
	for _, row := range rows {
		if err := f.publishOne(ctx, dsn, row.Group, row.Epoch, row.Endpoint); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeProber) publishOne(_ context.Context, _ string, g Group, epoch int64, endpoint string) error {
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
	f.waitAtGate(dsn)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return PrimaryState{}, f.err
	}
	st := PrimaryState{Streaming: map[string]bool{}, SyncStandbyNames: f.syncNames[dsn], WritesPaused: f.pausedDSN[dsn]}
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

func (f *fakeProber) PauseWrites(_ context.Context, dsn string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pausedDSN == nil {
		f.pausedDSN = map[string]bool{}
	}
	f.pausedDSN[dsn] = true
	f.paused = append(f.paused, dsn)
	return nil
}

func (f *fakeProber) WriteFenced(context.Context, string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fenced, f.fenceErr
}

func (f *fakeProber) MigrateCatalog(context.Context, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.migrated++
	return nil
}

func (f *fakeProber) SetRouterPassword(_ context.Context, dsn, password string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.routerPasswords = append(f.routerPasswords, hostOf(dsn)+"="+password)
	return nil
}

func requireEnvtest(t *testing.T) {
	t.Helper()
	if k8sClient == nil {
		dockertest.EnvtestMissing(t, "this envtest")
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
			InternalTLS:      pgshardv1alpha1.InternalTLSSpec{Insecure: true},
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
	t.Cleanup(func() { deleteCluster(t, c) })
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
			if len(pod.Spec.Containers) != 2 || pod.Spec.Containers[1].Name != "pooler" || pod.Spec.Containers[1].Image != ctr.Image || pod.Spec.Containers[1].ReadinessProbe.HTTPGet == nil {
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

// deleteServicesOf gives a cluster's service IPs back to envtest.
func deleteServicesOf(t *testing.T, c *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	var svcs corev1.ServiceList
	if err := k8sClient.List(context.Background(), &svcs, client.InNamespace(c.Namespace)); err != nil {
		t.Log(err)
		return
	}
	for i := range svcs.Items {
		if strings.HasPrefix(svcs.Items[i].Name, c.Name+"-") {
			_ = k8sClient.Delete(context.Background(), &svcs.Items[i])
		}
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

func TestInternalTLSValidationFailsClosed(t *testing.T) {
	requireEnvtest(t)
	ctx := context.Background()

	missing := newCluster("tls-missing")
	missing.Spec.InternalTLS = pgshardv1alpha1.InternalTLSSpec{}
	if err := k8sClient.Create(ctx, missing); err == nil {
		t.Fatal("cluster with neither secretRef nor insecure must be rejected")
	} else if !strings.Contains(err.Error(), "internalTLS requires secretRef") {
		t.Fatalf("unexpected rejection: %v", err)
	}

	both := newCluster("tls-both")
	both.Spec.InternalTLS = pgshardv1alpha1.InternalTLSSpec{
		SecretRef: &corev1.LocalObjectReference{Name: "internal-tls"}, Insecure: true}
	if err := k8sClient.Create(ctx, both); err == nil {
		t.Fatal("secretRef combined with insecure must be rejected")
	} else if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("unexpected rejection: %v", err)
	}

	secure := newCluster("tls-secure")
	secure.Spec.InternalTLS = pgshardv1alpha1.InternalTLSSpec{SecretRef: &corev1.LocalObjectReference{Name: "internal-tls"}}
	if err := k8sClient.Create(ctx, secure); err != nil {
		t.Fatalf("secretRef alone must be accepted: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), secure) })

	insecure := newCluster("tls-insecure")
	if err := k8sClient.Create(ctx, insecure); err != nil {
		t.Fatalf("explicit insecure opt-in must be accepted: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), insecure) })
}

func TestRouterRollsOnInternalTLSSecretRotation(t *testing.T) {
	requireEnvtest(t)
	ctx := context.Background()
	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "rotate-tls", Namespace: "default"},
		Data: map[string][]byte{"tls.crt": []byte("cert-a"), "tls.key": []byte("key-a"), "ca.crt": []byte("ca-a")}}
	if err := k8sClient.Create(ctx, sec); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), sec) })
	c := newCluster("tls-rotate")
	c.Spec.InternalTLS = pgshardv1alpha1.InternalTLSSpec{SecretRef: &corev1.LocalObjectReference{Name: "rotate-tls"}}
	if err := k8sClient.Create(ctx, c); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), c) })
	r := &ClusterReconciler{Client: k8sClient, Renderer: Renderer{RouterImage: "router:test"}}
	if err := r.reconcileRouter(ctx, c); err != nil {
		t.Fatal(err)
	}
	var dep appsv1.Deployment
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: c.Name + "-router"}, &dep); err != nil {
		t.Fatal(err)
	}
	first := dep.Spec.Template.Annotations[AnnotationInternalTLSChecksum]
	if first == "" {
		t.Fatal("router pod template must carry the internal TLS secret checksum")
	}
	sec.Data["tls.crt"] = []byte("cert-b")
	if err := k8sClient.Update(ctx, sec); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileRouter(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: c.Name + "-router"}, &dep); err != nil {
		t.Fatal(err)
	}
	if got := dep.Spec.Template.Annotations[AnnotationInternalTLSChecksum]; got == first {
		t.Fatal("rotating the internal TLS secret must change the router pod template")
	}
}

// TestPrimaryOutsideTheMemberSetDoesNotPanic: PgShardGroup.status.primary was
// trusted as an oracle, so a primary a failover had promoted to a high
// ordinal -- then scaled out of existence by lowering replicasPerShard --
// left members[state.primary] nil and reconcileGroup dereferenced it. The
// panic aborted the whole cluster's reconcile and retried forever, and the
// only code that could have moved the primary back into range sits
// downstream of the crash, so it could not self-heal.
func TestPrimaryOutsideTheMemberSetDoesNotPanic(t *testing.T) {
	r, _, c := setup(t, "outofrange")
	reconcile(t, r, c)

	g := Groups(c)[1]
	var pg pgshardv1alpha1.PgShardGroup
	get(t, g.Prefix(), &pg)
	base := pg.DeepCopy()
	pg.Status.Primary = g.Prefix() + "-9"
	pg.Status.Epoch = 7
	if err := k8sClient.Status().Patch(context.Background(), &pg, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}

	// Must not panic; controller-runtime would turn a panic into an error
	// that never clears.
	reconcile(t, r, c)

	var after pgshardv1alpha1.PgShardGroup
	get(t, g.Prefix(), &after)
	if after.Status.Primary == g.Prefix()+"-9" {
		t.Fatalf("designated primary %q is still outside the member set", after.Status.Primary)
	}
	if !slices.Contains(g.MemberNames(), after.Status.Primary) {
		t.Fatalf("designated primary %q is not a member of the group", after.Status.Primary)
	}
	// The epoch fences writes against a primary that may still be running,
	// so re-designating must never wind it back.
	if after.Status.Epoch < 7 {
		t.Fatalf("epoch went backwards: %d, was 7", after.Status.Epoch)
	}
}

// TestReconcileWalksGroupsConcurrently pins that a pass costs the slowest
// group rather than the sum of them. Every group used to be reconciled in
// one sequential loop -- Kubernetes reads and writes, an agent RPC and a
// few PostgreSQL round trips each -- so on a large topology a pass ran
// past the requeue interval and a primary failure in the last group was
// not noticed until the walk reached it.
func TestReconcileWalksGroupsConcurrently(t *testing.T) {
	const shards = 8
	r, fp, c := setup(t, "conc")
	// envtest shares one apiserver across the package and never collects
	// the objects a test leaves behind, so a group per shard exhausts its
	// service IP range for every test that follows.
	t.Cleanup(func() { deleteServicesOf(t, c) })
	c.Spec.Shards = ptr.To(shards)
	if err := k8sClient.Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, c)
	markPodsRunning(t, c)
	fp.err = nil
	streaming := map[string]bool{}
	for _, g := range Groups(c) {
		for i := 1; i < g.Replicas; i++ {
			streaming[g.MemberName(i)] = true
		}
	}
	fp.mu.Lock()
	fp.streaming = streaming
	fp.gateWidth = shards
	fp.mu.Unlock()

	reconcile(t, r, c)

	fp.mu.Lock()
	opened := fp.gateOpened
	fp.mu.Unlock()
	if !opened {
		t.Errorf("no two of the %d shard groups were ever reconciled at the same time", shards)
	}
}

func TestNetworkPolicyIsRenderedOnlyWhileItIsEnabled(t *testing.T) {
	r, _, c := setup(t, "netpol")
	ctx := context.Background()
	key := types.NamespacedName{Namespace: "default", Name: MemberNetworkPolicyName("netpol")}

	reconcile(t, r, c)
	var np networkingv1.NetworkPolicy
	if err := k8sClient.Get(ctx, key, &np); !apierrors.IsNotFound(err) {
		t.Fatalf("a policy nobody asked for: %v", err)
	}

	get(t, "netpol", c)
	c.Spec.NetworkPolicy.Enabled = true
	c.Spec.NetworkPolicy.Clients = []networkingv1.NetworkPolicyPeer{{
		PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "pgshard-controller"}},
	}}
	if err := k8sClient.Update(ctx, c); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, c)
	get(t, key.Name, &np)
	ownedBy(t, &np, c)
	if len(np.Spec.Ingress) != 2 || len(np.Spec.Ingress[0].From) != 2 {
		t.Fatalf("policy does not carry the declared client: %+v", np.Spec.Ingress)
	}

	get(t, "netpol", c)
	c.Spec.NetworkPolicy.Enabled = false
	if err := k8sClient.Update(ctx, c); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, c)
	if err := k8sClient.Get(ctx, key, &np); !apierrors.IsNotFound(err) {
		t.Fatalf("a policy that was turned off keeps enforcing: %v", err)
	}
}

// TestRouterCredentialIsGeneratedAndApplied: the router's password is the
// cluster's own, generated once and told to the catalog on every pass, so a
// catalog restored or rebuilt from elsewhere comes back in step with the
// Secret rather than locking the router out.
func TestRouterCredentialIsGeneratedAndApplied(t *testing.T) {
	r, fp, c := setup(t, "rc")
	bringUp(t, r, fp, c)

	var sec corev1.Secret
	get(t, RouterSecretName(c.Name), &sec)
	ownedBy(t, &sec, c)
	pw := string(sec.Data["password"])
	if len(pw) < 32 {
		t.Fatalf("router password is %d characters", len(pw))
	}
	if string(sec.Data["username"]) != catalog.RouterRole {
		t.Errorf("username %q, want %q", sec.Data["username"], catalog.RouterRole)
	}

	var su corev1.Secret
	get(t, SecretName(c.Name), &su)
	if string(su.Data["password"]) == pw {
		t.Error("the router's password must not be the superuser's")
	}

	fp.mu.Lock()
	applied := append([]string(nil), fp.routerPasswords...)
	fp.mu.Unlock()
	if len(applied) == 0 {
		t.Fatal("the catalog was never told the router's password")
	}
	if got := applied[len(applied)-1]; got != "rc-catalog-rw.default.svc="+pw {
		t.Errorf("last ALTER ROLE = %q, want the generated password on the catalog", got)
	}

	reconcile(t, r, c)
	get(t, RouterSecretName(c.Name), &sec)
	if string(sec.Data["password"]) != pw {
		t.Error("the password must survive a reconcile: it was regenerated")
	}
}

// deleteCluster removes the cluster and everything rendered for it.
//
// envtest runs an API server with no controller-manager, so nothing collects
// garbage: an object whose owner is deleted simply stays. A second run of the
// same test then finds the first run's PgShardReshard, or its pods, owned by
// a cluster UID that no longer exists -- which is why
// TestReshardProvisionsNonServingTargets passed under -count=1 and failed
// under -count=3 with "is not controlled by the cluster".
func deleteCluster(t *testing.T, c *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	ctx := context.Background()
	_ = k8sClient.Delete(ctx, c)
	inCluster := client.MatchingLabels{LabelCluster: c.Name}
	ns := client.InNamespace(c.Namespace)
	for _, list := range []client.Object{
		&pgshardv1alpha1.PgShardGroup{},
		&pgshardv1alpha1.PgShardReshard{},
		&corev1.Pod{},
		&corev1.Service{},
		&corev1.Secret{},
		&corev1.ConfigMap{},
	} {
		// A kind the cluster never created deletes nothing, which is why
		// the error is ignored rather than asserted on.
		_ = k8sClient.DeleteAllOf(ctx, list, ns, inCluster, client.GracePeriodSeconds(0))
	}
	// The promotion Leases are made by the agents rather than rendered, so
	// they carry no cluster label and have to be named. A stale one is held
	// by a holder that no longer exists, and nothing expires it without a
	// kubelet.
	for _, g := range append(Groups(c), TargetGroups(c)...) {
		_ = k8sClient.Delete(ctx, &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: g.LeaseName(), Namespace: c.Namespace}})
	}
}
