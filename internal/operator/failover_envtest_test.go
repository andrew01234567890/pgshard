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
	r, fp, fa, c := healthyCluster(t, "conv")
	fa.set(podIP(1, 0), AgentStatus{Running: true, Primary: false, Epoch: 0}, nil)
	for i := range 3 {
		fp.setStandby(podIP(1, i), StandbyState{InRecovery: true, Streaming: true, FlushLSN: 500})
	}
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

// TestFailoverRemovesAnUnreachableOldPrimaryBeforePromoting guards the gap
// quiesce cannot close. An agent it cannot reach counts as gone, because
// waiting for one that will never answer would turn every partition into an
// outage — which leaves the old primary possibly alive and still writable
// while a successor is promoted. Its Pod has to go first.
func TestFailoverRemovesAnUnreachableOldPrimaryBeforePromoting(t *testing.T) {
	r, fp, fa, c := healthyCluster(t, "podfence")

	// The old primary's Pod is left in place but stops being ready, and its
	// agent does not answer: the operator cannot know whether PostgreSQL is
	// still running there, which is exactly when assuming it stopped is
	// unsafe.
	var old corev1.Pod
	get(t, "podfence-shard-0-0", &old)
	old.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
	if err := k8sClient.Status().Update(context.Background(), &old); err != nil {
		t.Fatal(err)
	}
	fa.set(podIP(1, 0), AgentStatus{}, errors.New("connection refused"))
	fp.standbys[podIP(1, 1)] = StandbyState{InRecovery: true, FlushLSN: 100}
	fp.standbys[podIP(1, 2)] = StandbyState{InRecovery: true, FlushLSN: 200}
	fp.err = errors.New("no primary")

	reconcile(t, r, c)

	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "podfence-shard-0-0"}, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the old primary's pod must be removed before a successor is promoted: %v", err)
	}
	if len(fa.promotes) != 1 || !strings.HasSuffix(fa.promotes[0], ":1:podfence-shard-0-2") {
		t.Fatalf("expected exactly one promotion after the old primary was fenced, got %v", fa.promotes)
	}
}

// TestConvergeRefusesToPromoteAnUnsafeDesignatedPrimary: converge promoted
// whatever the status named as primary, with none of the checks the
// failover path is built around. A designated primary that is a standby
// behind another member -- reachable through a rolled-back or hand-edited
// group status -- was promoted on the healthy branch of an ordinary
// reconcile, losing every commit it had not replayed.
func TestConvergeRefusesToPromoteAnUnsafeDesignatedPrimary(t *testing.T) {
	t.Run("another member is running as a primary", func(t *testing.T) {
		r, fp, fa, c := healthyCluster(t, "lp")
		before := len(fa.promotes)
		fa.set(podIP(1, 0), AgentStatus{Running: true, Primary: false}, nil)
		fa.set(podIP(1, 1), AgentStatus{Running: true, Primary: true}, nil)
		fp.setStandby(podIP(1, 0), StandbyState{InRecovery: true, Streaming: true, FlushLSN: 100})
		fp.setStandby(podIP(1, 1), StandbyState{FlushLSN: 400})
		fp.setStandby(podIP(1, 2), StandbyState{InRecovery: true, Streaming: true, FlushLSN: 400})
		reconcile(t, r, c)
		if len(fa.promotes) != before {
			t.Fatalf("no promotion may be issued beside a live primary: %v", fa.promotes[before:])
		}
		if cond := condition(t, "lp", pgshardv1alpha1.ConditionPrimaryHealthy); cond.Status == metav1.ConditionTrue {
			t.Fatalf("the refusal must be visible: %+v", cond)
		}
	})

	t.Run("another member holds a higher flushed LSN", func(t *testing.T) {
		r, fp, fa, c := healthyCluster(t, "hl")
		before := len(fa.promotes)
		for i := range 3 {
			fa.set(podIP(1, i), AgentStatus{Running: true, Primary: false}, nil)
		}
		fp.setStandby(podIP(1, 0), StandbyState{InRecovery: true, Streaming: true, FlushLSN: 100})
		fp.setStandby(podIP(1, 1), StandbyState{InRecovery: true, Streaming: true, FlushLSN: 900})
		fp.setStandby(podIP(1, 2), StandbyState{InRecovery: true, Streaming: true, FlushLSN: 100})
		reconcile(t, r, c)
		if len(fa.promotes) != before {
			t.Fatalf("a member behind another must not be promoted: %v", fa.promotes[before:])
		}
		cond := condition(t, "hl", pgshardv1alpha1.ConditionPrimaryHealthy)
		if cond.Status == metav1.ConditionTrue {
			t.Fatalf("the refusal must be visible: %+v", cond)
		}
	})

	t.Run("the designated primary holds the maximum", func(t *testing.T) {
		r, fp, fa, c := healthyCluster(t, "ok")
		before := len(fa.promotes)
		for i := range 3 {
			fa.set(podIP(1, i), AgentStatus{Running: true, Primary: false}, nil)
		}
		fp.setStandby(podIP(1, 0), StandbyState{InRecovery: true, Streaming: true, FlushLSN: 900})
		fp.setStandby(podIP(1, 1), StandbyState{InRecovery: true, Streaming: true, FlushLSN: 900})
		fp.setStandby(podIP(1, 2), StandbyState{InRecovery: true, Streaming: true, FlushLSN: 100})
		reconcile(t, r, c)
		if len(fa.promotes) != before+1 {
			t.Fatalf("the designated primary holds every commit and must be promoted: %v", fa.promotes[before:])
		}
		if got := fa.promotes[before]; !strings.HasSuffix(got, ":ok-shard-0-0") {
			t.Fatalf("promotion must target the designated primary: %s", got)
		}
	})
}

// TestLoadStateReconstructsThePrimaryFromTheFence: the designated primary
// and the fencing epoch were read only from the PgShardGroup status, and a
// blank status designated member 0 at epoch 0 -- whoever actually held the
// data and the fence. An etcd restore, a deleted object or a status-schema
// migration was therefore enough to promote the wrong member and lose
// every commit the two differ by. The group Lease carries the same two
// values, written before any promotion.
func TestLoadStateReconstructsThePrimaryFromTheFence(t *testing.T) {
	r, _, fa, c := healthyCluster(t, "fence")
	g := Groups(c)[1]

	// A failover has moved the primary to member 1 at epoch 7, so that is
	// what the fence says.
	if err := r.fenceLease(context.Background(), c, g, g.MemberName(0), FenceHolder, 7, g.MemberName(1)); err != nil {
		t.Fatal(err)
	}
	// ...and the status is lost.
	var pg pgshardv1alpha1.PgShardGroup
	get(t, g.Prefix(), &pg)
	base := pg.DeepCopy()
	pg.Status.Primary, pg.Status.Epoch = "", 0
	if err := k8sClient.Status().Patch(context.Background(), &pg, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}

	before := len(fa.promotes)
	st, err := r.loadState(context.Background(), c, g)
	if err != nil {
		t.Fatal(err)
	}
	if st.primary != g.MemberName(1) {
		t.Fatalf("designated %s, want the member the fence names (%s)", st.primary, g.MemberName(1))
	}
	if st.epoch < 7 {
		t.Fatalf("epoch %d, want at least the fenced 7", st.epoch)
	}
	if got := groupStatus(t, g.Prefix()); got.Primary != g.MemberName(1) || got.Epoch < 7 {
		t.Fatalf("status not written back: %+v", got)
	}
	if len(fa.promotes) != before {
		t.Fatalf("reading the state must promote nothing: %v", fa.promotes[before:])
	}
}

// TestLoadStateStillBootstrapsMemberZero: with nothing ever promoted there
// is no fence to read, and the first member is the primary.
func TestLoadStateStillBootstrapsMemberZero(t *testing.T) {
	r, _, _, c := healthyCluster(t, "boot")
	g := Groups(c)[1]
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: g.LeaseName(), Namespace: "default"}}
	if err := k8sClient.Delete(context.Background(), lease); client.IgnoreNotFound(err) != nil {
		t.Fatal(err)
	}
	var pg pgshardv1alpha1.PgShardGroup
	get(t, g.Prefix(), &pg)
	base := pg.DeepCopy()
	pg.Status.Primary, pg.Status.Epoch = "", 0
	if err := k8sClient.Status().Patch(context.Background(), &pg, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	st, err := r.loadState(context.Background(), c, g)
	if err != nil {
		t.Fatal(err)
	}
	if st.primary != g.MemberName(0) || st.epoch != 0 {
		t.Fatalf("bootstrap designated %s at epoch %d, want %s at 0", st.primary, st.epoch, g.MemberName(0))
	}
}

// TestAFailoverUnderAWriteFenceHandsOverAPausedPrimary: a barrier stops
// writes with ALTER SYSTEM, which lives in postgresql.auto.conf and is
// rewritten by the agent on promotion, so a member promoted mid-barrier
// used to start serving writes the barrier believed were stopped. The
// catalog write fence is the durable statement of that intent.
func TestAFailoverUnderAWriteFenceHandsOverAPausedPrimary(t *testing.T) {
	r, fp, fa, c := healthyCluster(t, "fp")
	fp.fenced = true

	deletePod(t, "fp-shard-0-0")
	fa.set(podIP(1, 0), AgentStatus{}, errors.New("connection refused"))
	fp.standbys[podIP(1, 1)] = StandbyState{InRecovery: true, FlushLSN: 100}
	fp.standbys[podIP(1, 2)] = StandbyState{InRecovery: true, FlushLSN: 200}
	fp.err = errors.New("no primary")

	reconcile(t, r, c)
	if len(fa.promotes) != 1 {
		t.Fatalf("expected one promotion, got %v", fa.promotes)
	}
	if len(fp.paused) != 1 || !strings.Contains(fp.paused[0], podIP(1, 2)) {
		t.Fatalf("the promoted primary must refuse writes while the fence is raised, paused=%v", fp.paused)
	}
	if got := podRole(t, "fp-shard-0-2"); got != RolePrimary {
		t.Fatalf("new primary label %q", got)
	}
}

// TestAPrimaryThatLostThePauseGetsItBack covers the other way the pause
// evaporates: the primary itself restarts, the agent rewrites
// postgresql.auto.conf on the way up, and nothing else would put the pause
// back.
func TestAPrimaryThatLostThePauseGetsItBack(t *testing.T) {
	r, fp, _, c := healthyCluster(t, "fq")
	reconcile(t, r, c)
	if len(fp.paused) != 0 {
		t.Fatalf("no fence is raised, so nothing may be paused: %v", fp.paused)
	}

	fp.fenced = true
	reconcile(t, r, c)
	if len(fp.paused) == 0 {
		t.Fatal("a raised fence over an unpaused primary must reapply the pause")
	}
	for _, dsn := range fp.paused {
		if strings.Contains(dsn, Groups(c)[0].ServiceRW()) {
			t.Fatalf("the catalog group is never paused by the barrier: %v", fp.paused)
		}
	}
	if !strings.Contains(strings.Join(fp.paused, ","), Groups(c)[1].ServiceRW()) {
		t.Fatalf("the shard primary was not the one paused: %v", fp.paused)
	}

	before := len(fp.paused)
	reconcile(t, r, c)
	if len(fp.paused) != before {
		t.Fatalf("a primary already refusing writes must not be paused again: %v", fp.paused)
	}
}

// TestTheWritePauseNeverStallsAGroup: the fence lives in the catalog
// group, which is itself rebuilt member by member on a storage-class
// change. A group that stopped rolling out, or a failover that refused to
// promote, because the catalog was briefly unreadable would be waiting on
// the very thing it is waiting for -- and an unpaused primary fails the
// barrier's certification rather than spoiling its point.
func TestTheWritePauseNeverStallsAGroup(t *testing.T) {
	r, fp, fa, c := healthyCluster(t, "fr")
	fp.fenceErr = errors.New("catalog unreachable")

	fp.mu.Lock()
	fp.slots = nil
	fp.mu.Unlock()
	reconcile(t, r, c)
	fp.mu.Lock()
	shardSlots := 0
	for _, s := range fp.slots {
		if strings.Contains(s, "fr-shard-0") {
			shardSlots++
		}
	}
	fp.mu.Unlock()
	if shardSlots == 0 {
		t.Fatalf("the shard pass stopped at the fence read, so the group cannot roll out: %v", fp.slots)
	}

	deletePod(t, "fr-shard-0-0")
	fa.set(podIP(1, 0), AgentStatus{}, errors.New("connection refused"))
	fp.standbys[podIP(1, 1)] = StandbyState{InRecovery: true, FlushLSN: 100}
	fp.standbys[podIP(1, 2)] = StandbyState{InRecovery: true, FlushLSN: 200}
	fp.err = errors.New("no primary")
	reconcile(t, r, c)
	if len(fa.promotes) != 1 {
		t.Fatalf("an unreadable fence must not stop a promotion: %v", fa.promotes)
	}
	if got := podRole(t, "fr-shard-0-2"); got != RolePrimary {
		t.Fatalf("the promoted member never took the primary label: %q", got)
	}
}
