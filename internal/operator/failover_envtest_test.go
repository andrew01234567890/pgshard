package operator

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

// healthyCluster reconciles a cluster to Ready with fake agents: member 0
// of every group is the running primary, the others running standbys.
func healthyCluster(t *testing.T, name string) (*ClusterReconciler, *fakeProber, *fakeAgents, *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	r, fp, fa, c := setupWithAgents(t, name)
	reconcile(t, r, c)
	markPodsRunning(t, c)
	fp.err = nil
	fp.streaming = map[string]bool{}
	for gi, g := range Groups(c) {
		for i := 0; i < g.Replicas; i++ {
			fa.set(podIP(gi, i), AgentStatus{Running: true, Primary: i == 0}, nil)
			if i > 0 {
				fp.streaming[g.MemberName(i)] = true
			}
		}
	}
	reconcile(t, r, c)
	if cond := condition(t, name, pgshardv1alpha1.ConditionReady); cond.Status != metav1.ConditionTrue {
		t.Fatalf("cluster must be Ready before the scenario: %+v", cond)
	}
	return r, fp, fa, c
}

func deletePod(t *testing.T, name string) {
	t.Helper()
	var pod corev1.Pod
	get(t, name, &pod)
	if err := k8sClient.Delete(context.Background(), &pod, client.GracePeriodSeconds(0)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(&pod), &corev1.Pod{})
		if apierrors.IsNotFound(err) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pod %s not deleted", name)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func groupStatus(t *testing.T, name string) pgshardv1alpha1.PgShardGroupStatus {
	t.Helper()
	var pg pgshardv1alpha1.PgShardGroup
	get(t, name, &pg)
	return pg.Status
}

func podRole(t *testing.T, name string) string {
	t.Helper()
	var pod corev1.Pod
	get(t, name, &pod)
	return pod.Labels[LabelRole]
}

func TestFailoverPromotesHighestFlushedSyncStandbyAndPublishesFenceFirst(t *testing.T) {
	r, fp, fa, c := healthyCluster(t, "fo")
	shard := Groups(c)[1]

	deletePod(t, "fo-shard-0-0")
	fa.set(podIP(1, 0), AgentStatus{}, errors.New("connection refused"))
	fp.standbys[podIP(1, 1)] = StandbyState{InRecovery: true, FlushLSN: 100}
	fp.standbys[podIP(1, 2)] = StandbyState{InRecovery: true, FlushLSN: 200}
	fp.err = errors.New("no primary")
	*fp.journal = nil

	res := reconcile(t, r, c)
	if res.RequeueAfter != requeueFailover {
		t.Errorf("failover pass must requeue quickly, got %v", res.RequeueAfter)
	}
	if len(fa.promotes) != 1 || fa.promotes[0] != agentAddr(podIP(1, 2))+":1:fo-shard-0-2" {
		t.Fatalf("exactly one promotion of the highest-LSN standby at epoch 1 expected, got %v", fa.promotes)
	}
	if got := strings.Join(*fp.journal, ","); got != "publish:1,promote:1" {
		t.Fatalf("the catalog fence must be written before the promotion, journal=%q", got)
	}
	if st := groupStatus(t, "fo-shard-0"); st.Primary != "fo-shard-0-2" || st.Epoch != 1 {
		t.Fatalf("group status after failover: %+v", st)
	}
	var lease coordinationv1.Lease
	get(t, shard.LeaseName(), &lease)
	if ptr.Deref(lease.Spec.HolderIdentity, "") != "fo-shard-0-2" || lease.Annotations[AnnotationPrimaryEpoch] != "1" || lease.Annotations[AnnotationPrimary] != "fo-shard-0-2" {
		t.Fatalf("lease must be handed to the candidate with the epoch published: holder=%q annotations=%v", ptr.Deref(lease.Spec.HolderIdentity, ""), lease.Annotations)
	}
	if got := podRole(t, "fo-shard-0-2"); got != RolePrimary {
		t.Errorf("new primary label %q", got)
	}
	if got := podRole(t, "fo-shard-0-1"); got != RoleReplica {
		t.Errorf("untouched standby label %q", got)
	}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "fo-shard-0-0"}, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("old primary pod must not be recreated before the failover decided: %v", err)
	}
	if got := strings.Join(fp.published, ","); !strings.Contains(got, "shard-0:1:fo-shard-0-2.fo-shard-0-peers.default.svc:9091") {
		t.Errorf("shard_status publication: %q", got)
	}
	if st := groupStatus(t, "fo-catalog"); st.Epoch != 0 || st.Primary != "fo-catalog-0" || len(fa.demotes) != 0 {
		t.Errorf("catalog group must be untouched: %+v demotes=%v", st, fa.demotes)
	}

	reconcile(t, r, c)
	var old corev1.Pod
	get(t, "fo-shard-0-0", &old)
	if old.Labels[LabelRole] != RoleReplica {
		t.Fatalf("old primary must come back as a replica, label %q", old.Labels[LabelRole])
	}
	var cm corev1.ConfigMap
	get(t, shard.ConfigMapName(), &cm)
	if !strings.Contains(cm.Data["fo-shard-0-0.json"], `"role": "standby"`) || !strings.Contains(cm.Data["fo-shard-0-2.json"], `"role": "primary"`) {
		t.Fatalf("agent configs must follow the new primary")
	}
	var cl pgshardv1alpha1.PgShardCluster
	get(t, "fo", &cl)
	if len(cl.Status.Shards) != 1 || cl.Status.Shards[0].Primary != "fo-shard-0-2" || cl.Status.Shards[0].Epoch != 1 {
		t.Errorf("cluster shard status: %+v", cl.Status.Shards)
	}

	markPodRunning(t, "fo-shard-0-0", podIP(1, 0))
	fa.set(podIP(1, 0), AgentStatus{Running: true, Primary: false}, nil)
	fp.err = nil
	fp.streaming = map[string]bool{"fo-catalog-1": true, "fo-catalog-2": true, "fo-shard-0-0": true, "fo-shard-0-1": true}
	reconcile(t, r, c)
	if cond := condition(t, "fo", pgshardv1alpha1.ConditionReady); cond.Status != metav1.ConditionTrue {
		t.Fatalf("cluster must return to Ready once the old primary streams again: %+v", cond)
	}
	if got := fp.syncNames[DSN("fo-shard-0-rw", "default", currentPassword(t, "fo"))]; got != `ANY 1 ("fo-shard-0-0", "fo-shard-0-1")` {
		t.Fatalf("sync names must be recomputed around the new primary: %q", got)
	}
	if len(fa.demotes) != 0 {
		t.Errorf("a member that rejoined as a standby must not be demoted: %v", fa.demotes)
	}
	if last := fp.slots[len(fp.slots)-1]; last != "fo-shard-0-rw.default.svc:5432/postgres?sslmode=disable&connect_timeout=5:pgshard_fo_shard_0_0,pgshard_fo_shard_0_1:-pgshard_fo_shard_0_2" {
		t.Fatalf("the new primary must get slots for the other members and lose its own: %q", last)
	}

	deletePod(t, "fo-shard-0-2")
	fa.set(podIP(1, 2), AgentStatus{}, errors.New("connection refused"))
	fp.standbys[podIP(1, 0)] = StandbyState{InRecovery: true, FlushLSN: 300}
	fp.standbys[podIP(1, 1)] = StandbyState{InRecovery: true, FlushLSN: 300}
	fp.err = errors.New("no primary")
	reconcile(t, r, c)
	if len(fa.promotes) != 2 || fa.promotes[1] != agentAddr(podIP(1, 0))+":2:fo-shard-0-0" {
		t.Fatalf("second failover must promote at epoch 2 (tie broken by name), got %v", fa.promotes)
	}
	if st := groupStatus(t, "fo-shard-0"); st.Epoch != 2 || st.Primary != "fo-shard-0-0" {
		t.Fatalf("epoch must be strictly monotonic: %+v", st)
	}
}

func TestFailoverPromotesHighestFlushedReachableStandby(t *testing.T) {
	r, fp, fa, c := healthyCluster(t, "sync-ex")
	fp.streaming["sync-ex-shard-0-2"] = false
	reconcile(t, r, c)
	if st := groupStatus(t, "sync-ex-shard-0"); st.Members[2].Ready {
		t.Fatalf("member 2 must be recorded as not streaming: %+v", st)
	}

	deletePod(t, "sync-ex-shard-0-0")
	fa.set(podIP(1, 0), AgentStatus{}, errors.New("gone"))
	fp.standbys[podIP(1, 1)] = StandbyState{InRecovery: true, FlushLSN: 10}
	fp.standbys[podIP(1, 2)] = StandbyState{InRecovery: true, FlushLSN: 999}
	fp.err = errors.New("no primary")
	reconcile(t, r, c)
	// Member 2 is not Ready (not streaming at the last observation) but it is
	// listed in synchronous_standby_names and reachable with the highest
	// flushed LSN, so it may hold the only copy of an acknowledged commit and
	// must be the one promoted.
	if len(fa.promotes) != 1 || !strings.HasSuffix(fa.promotes[0], ":1:sync-ex-shard-0-2") {
		t.Fatalf("the reachable standby with the highest flushed LSN must be promoted, got %v", fa.promotes)
	}
}

func TestFailoverWithoutCandidateRecreatesThePrimary(t *testing.T) {
	r, fp, fa, c := healthyCluster(t, "nocand")
	deletePod(t, "nocand-shard-0-0")
	fa.set(podIP(1, 0), AgentStatus{}, errors.New("gone"))
	fp.err = errors.New("no primary")
	reconcile(t, r, c)
	if len(fa.promotes) != 0 {
		t.Fatalf("no standby was reachable, nothing may be promoted: %v", fa.promotes)
	}
	var pod corev1.Pod
	get(t, "nocand-shard-0-0", &pod)
	if pod.Labels[LabelRole] != RolePrimary {
		t.Fatalf("the primary must be recreated as primary, label %q", pod.Labels[LabelRole])
	}
	var lease coordinationv1.Lease
	get(t, Groups(c)[1].LeaseName(), &lease)
	if h := ptr.Deref(lease.Spec.HolderIdentity, "x"); h != "" {
		t.Fatalf("fence must be released so the primary can start, holder %q", h)
	}
	if st := groupStatus(t, "nocand-shard-0"); st.Epoch != 0 || st.Primary != "nocand-shard-0-0" {
		t.Fatalf("epoch must not move without a promotion: %+v", st)
	}
}

func TestFailoverRefusesWhenLeaseHeldByAnotherLiveHolder(t *testing.T) {
	r, fp, fa, c := healthyCluster(t, "held")
	now := metav1.NewMicroTime(time.Now())
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: Groups(c)[1].LeaseName(), Namespace: "default"}}
	lease.Spec.HolderIdentity = ptr.To("intruder")
	lease.Spec.LeaseDurationSeconds = ptr.To(int32(60))
	lease.Spec.RenewTime = &now
	if err := k8sClient.Create(context.Background(), lease); err != nil {
		t.Fatal(err)
	}
	deletePod(t, "held-shard-0-0")
	fa.set(podIP(1, 0), AgentStatus{}, errors.New("gone"))
	fp.standbys[podIP(1, 1)] = StandbyState{InRecovery: true, FlushLSN: 10}
	fp.standbys[podIP(1, 2)] = StandbyState{InRecovery: true, FlushLSN: 10}
	fp.err = errors.New("no primary")
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(c)})
	if !errors.Is(err, ErrLeaseHeldByOther) {
		t.Fatalf("expected ErrLeaseHeldByOther, got %v", err)
	}
	if len(fa.promotes) != 0 {
		t.Fatalf("nothing may be promoted while another holder renews the lease: %v", fa.promotes)
	}
	if st := groupStatus(t, "held-shard-0"); st.Epoch != 0 {
		t.Fatalf("epoch must not move: %+v", st)
	}
}

func TestConvergeRepromotesDesignatedPrimaryWithHigherEpoch(t *testing.T) {
	r, _, fa, c := healthyCluster(t, "conv")
	fa.set(podIP(1, 0), AgentStatus{Running: true, Primary: false, Epoch: 0}, nil)
	reconcile(t, r, c)
	if len(fa.promotes) != 1 || fa.promotes[0] != agentAddr(podIP(1, 0))+":1:conv-shard-0-0" {
		t.Fatalf("a designated primary answering as a standby must be promoted with a strictly greater epoch: %v", fa.promotes)
	}
	if st := groupStatus(t, "conv-shard-0"); st.Epoch != 1 || st.Primary != "conv-shard-0-0" {
		t.Fatalf("group status: %+v", st)
	}
	fa.set(podIP(1, 1), AgentStatus{Running: true, Primary: true, Epoch: 0}, nil)
	reconcile(t, r, c)
	if len(fa.demotes) != 1 || fa.demotes[0] != agentAddr(podIP(1, 1))+":1" {
		t.Fatalf("a non-designated member acting as primary must be demoted at the group epoch: %v", fa.demotes)
	}
	if got := podRole(t, "conv-shard-0-1"); got != RoleUnhealthy {
		t.Fatalf("a rogue primary must leave every Service, label %q", got)
	}
}

func TestSwitchoverPromotesRequestedMemberAndClearsAnnotation(t *testing.T) {
	r, fp, fa, c := healthyCluster(t, "swo")
	fp.standbys[podIP(1, 1)] = StandbyState{InRecovery: true, FlushLSN: 500}
	fp.standbys[podIP(1, 2)] = StandbyState{InRecovery: true, FlushLSN: 500}
	get(t, "swo", c)
	base := c.DeepCopy()
	c.Annotations = map[string]string{AnnotationSwitchover: "swo-shard-0-1"}
	if err := k8sClient.Patch(context.Background(), c, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	fa.set(podIP(1, 0), AgentStatus{}, errors.New("stopped"))
	oldUID := func() string {
		var pod corev1.Pod
		get(t, "swo-shard-0-0", &pod)
		return string(pod.UID)
	}()
	reconcile(t, r, c)
	if len(fa.promotes) != 1 || fa.promotes[0] != agentAddr(podIP(1, 1))+":1:swo-shard-0-1" {
		t.Fatalf("switchover must promote the requested member at epoch 1, got %v", fa.promotes)
	}
	if st := groupStatus(t, "swo-shard-0"); st.Primary != "swo-shard-0-1" || st.Epoch != 1 {
		t.Fatalf("group status: %+v", st)
	}
	get(t, "swo", c)
	if _, ok := c.Annotations[AnnotationSwitchover]; ok {
		t.Fatal("switchover annotation must be removed once done")
	}
	if got := podRole(t, "swo-shard-0-1"); got != RolePrimary {
		t.Errorf("target label %q", got)
	}
	reconcile(t, r, c)
	var pod corev1.Pod
	get(t, "swo-shard-0-0", &pod)
	if string(pod.UID) == oldUID || pod.Labels[LabelRole] != RoleReplica {
		t.Fatalf("old primary must be stopped (pod replaced) and come back as a replica: uid same=%v label=%q", string(pod.UID) == oldUID, pod.Labels[LabelRole])
	}
	if st := groupStatus(t, "swo-catalog"); st.Epoch != 0 {
		t.Errorf("catalog untouched: %+v", st)
	}
}

// TestConvergeRepromotesPendingPrimary: a designated primary whose
// post-promotion setup failed reports PromotionPending while already being a
// primary; converge must re-issue the idempotent Promote at a bumped epoch
// instead of leaving it half-configured forever.
func TestConvergeRepromotesPendingPrimary(t *testing.T) {
	r, _, fa, c := healthyCluster(t, "pp")
	before := len(fa.promotes)
	fa.set(podIP(1, 0), AgentStatus{Running: true, Primary: true, Epoch: 0, PromotionPending: true}, nil)
	reconcile(t, r, c)
	if len(fa.promotes) != before+1 {
		t.Fatalf("expected one re-promotion of the pending primary, got %v", fa.promotes[before:])
	}
	if got := fa.promotes[before]; !strings.HasPrefix(got, agentAddr(podIP(1, 0))+":") || !strings.HasSuffix(got, ":pp-shard-0-0") {
		t.Fatalf("re-promotion must target the designated primary: %s", got)
	}
	// A still-pending primary is not re-promoted again within the rate limit
	// (each attempt bumps the epoch and rewrites the fence)...
	fa.set(podIP(1, 0), AgentStatus{Running: true, Primary: true, Epoch: 1, PromotionPending: true}, nil)
	reconcile(t, r, c)
	if len(fa.promotes) != before+1 {
		t.Fatalf("re-promotion must be rate-limited: %v", fa.promotes[before:])
	}
	// ...but is once the interval has passed.
	base := time.Now()
	r.Now = func() time.Time { return base.Add(repromoteInterval + time.Second) }
	reconcile(t, r, c)
	if len(fa.promotes) != before+2 {
		t.Fatalf("expected a second re-promotion after the interval: %v", fa.promotes[before:])
	}
	// Once the agent reports the setup complete, converge stops re-promoting.
	r.Now = func() time.Time { return base.Add(2*repromoteInterval + time.Second) }
	fa.set(podIP(1, 0), AgentStatus{Running: true, Primary: true, Epoch: 2}, nil)
	reconcile(t, r, c)
	if len(fa.promotes) != before+2 {
		t.Fatalf("converge must not keep re-promoting a completed primary: %v", fa.promotes[before:])
	}
}
