package operator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/agent/backup"
	"github.com/andrew01234567890/pgshard/internal/twopc"
)

func TestBarrierTargetIsANameTargetWithABackupSet(t *testing.T) {
	name := "nightly-1"
	o, err := restoreTargetOptions(&pgshardv1alpha1.PgShardRestoreSpec{Target: pgshardv1alpha1.RestoreTarget{Barrier: &name}, BackupID: "b1"})
	if err != nil || o.Type != backup.TargetName || o.Target != "pgshard-nightly-1" || o.BackupID != "b1" {
		t.Fatalf("options %+v err %v", o, err)
	}
	if _, err := restoreTargetOptions(&pgshardv1alpha1.PgShardRestoreSpec{Target: pgshardv1alpha1.RestoreTarget{Barrier: &name}}); err == nil || !strings.Contains(err.Error(), "requires a backup id") {
		t.Fatalf("barrier without a backup set: %v", err)
	}
	bad := "Nightly 1"
	if _, err := restoreTargetOptions(&pgshardv1alpha1.PgShardRestoreSpec{Target: pgshardv1alpha1.RestoreTarget{Barrier: &bad}, BackupID: "b1"}); err == nil || !strings.Contains(err.Error(), "not a barrier name") {
		t.Fatalf("bad barrier name: %v", err)
	}
	lsn := "0/1"
	if _, err := restoreTargetOptions(&pgshardv1alpha1.PgShardRestoreSpec{Target: pgshardv1alpha1.RestoreTarget{Barrier: &name, LSN: &lsn}, BackupID: "b1"}); err == nil || !strings.Contains(err.Error(), "at most one") {
		t.Fatalf("two targets: %v", err)
	}
}

func TestScheduledBarrierNameAndControllerEndpoint(t *testing.T) {
	at := time.Date(2026, 8, 19, 2, 30, 0, 0, time.UTC)
	if got := ScheduledBarrierName("nightly", "demo", at); got != "nightly-demo-20260819-0230" {
		t.Fatalf("name %s", got)
	}
	long := strings.Repeat("p", 50)
	if got := ScheduledBarrierName(long, "demo", at); got != "demo-20260819-0230" {
		t.Fatalf("long policy: %s", got)
	}
	if got := ScheduledBarrierName(long, strings.Repeat("c", 60), at); len(got) != 63 || strings.HasPrefix(got, "-") {
		t.Fatalf("long cluster: %s (%d)", got, len(got))
	}
	if got := ControllerEndpoint("", "demo", "ns"); got != "demo-controller.ns.svc:15500" {
		t.Fatalf("endpoint %s", got)
	}
	if got := ControllerEndpoint("ctl.{namespace}:1", "demo", "ns"); got != "ctl.ns:1" {
		t.Fatalf("endpoint %s", got)
	}
}

// fakeBarriers records CreateBarrier calls per controller address.
type fakeBarriers struct {
	calls []string
	fail  map[string]error
}

func (f *fakeBarriers) CreateBarrier(_ context.Context, addr, name string) error {
	f.calls = append(f.calls, addr+" "+name)
	return f.fail[addr]
}

func TestSchedulerFiresBarriersPerBoundCluster(t *testing.T) {
	pol := newPolicy()
	pol.Spec.BarrierSchedule = "@hourly"
	pol.Spec.ControllerEndpoint = "{cluster}.{namespace}:1"
	other := boundCluster("other")
	other.Spec.Backup.PolicyRef = "weekly"
	cl := fakeClient(t, pol, boundCluster("demo"), boundCluster("beta"), other)
	s := NewBackupScheduler(cl)
	tick := time.Date(2026, 8, 19, 2, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return tick }
	key := client.ObjectKeyFromObject(pol)
	if err := s.FireBarrier(context.Background(), key); err == nil || !strings.Contains(err.Error(), "no barrier client") {
		t.Fatalf("without a client: %v", err)
	}
	fb := &fakeBarriers{fail: map[string]error{"beta.default:1": errors.New("drain: still in flight")}}
	s.Barriers = fb
	err := s.FireBarrier(context.Background(), key)
	if err == nil || !strings.Contains(err.Error(), "cluster beta (beta.default:1): barrier nightly-beta-20260819-0200: drain: still in flight") {
		t.Fatalf("err %v", err)
	}
	if want := []string{"beta.default:1 nightly-beta-20260819-0200", "demo.default:1 nightly-demo-20260819-0200"}; strings.Join(fb.calls, ",") != strings.Join(want, ",") {
		t.Fatalf("calls %v", fb.calls)
	}
	if err := s.FireBarrier(context.Background(), types.NamespacedName{Namespace: "default", Name: "gone"}); err != nil {
		t.Fatal("missing policy must be ignored:", err)
	}
	if err := s.SetBarrier(key, "bad cron"); err == nil || s.BarrierArmed(key) {
		t.Fatalf("bad cron: %v armed=%v", err, s.BarrierArmed(key))
	}
	if err := s.SetBarrier(key, "@hourly"); err != nil || !s.BarrierArmed(key) {
		t.Fatalf("arm: %v", err)
	}
	id := s.barriers[key].id
	if err := s.SetBarrier(key, "@hourly"); err != nil || s.barriers[key].id != id {
		t.Fatal("unchanged schedule must keep its entry")
	}
	if err := s.SetBarrier(key, ""); err != nil || s.BarrierArmed(key) {
		t.Fatal("empty schedule disarms")
	}
	_ = s.SetBarrier(key, "@daily")
	s.Remove(key)
	if s.BarrierArmed(key) {
		t.Fatal("remove must drop the barrier entry")
	}
}

func TestPolicyValidateRejectsBadBarrierSchedule(t *testing.T) {
	pol := newPolicy()
	pol.Spec.BarrierSchedule = "every hour"
	r := &BackupPolicyReconciler{}
	if err := r.validate(pol); err == nil || !strings.Contains(err.Error(), `barrierSchedule "every hour"`) {
		t.Fatalf("err %v", err)
	}
	pol.Spec.BarrierSchedule = "0 * * * *"
	if err := r.validate(pol); err != nil {
		t.Fatal(err)
	}
}

// fakeTwoPC is the two-phase side of the agents: the decision log of the
// catalog and the outcome each shard reports.
type fakeTwoPC struct {
	decisions []twopc.Decision
	outcomes  map[string]twopc.Outcome
	fail      map[string]error
	fenceErr  error
	calls     []string
}

func (f *fakeTwoPC) ListTransactionDecisions(_ context.Context, addr string) ([]twopc.Decision, error) {
	f.calls = append(f.calls, "decisions "+addr)
	return f.decisions, f.fail[addr]
}

func (f *fakeTwoPC) ReconcilePrepared(_ context.Context, addr string, epoch uint64, shard int32, decisions []twopc.Decision) (twopc.Outcome, error) {
	f.calls = append(f.calls, fmt.Sprintf("reconcile %s epoch=%d shard=%d decisions=%d", addr, epoch, shard, len(decisions)))
	return f.outcomes[addr], f.fail[addr]
}

func (f *fakeTwoPC) SetWriteFence(_ context.Context, addr string, epoch uint64, active bool, _ string) error {
	f.calls = append(f.calls, fmt.Sprintf("fence %s epoch=%d active=%v", addr, epoch, active))
	return f.fenceErr
}

// recoveredBarrierRestore builds a restore whose new cluster has every
// primary promoted and Ready, one reconcile away from reconciliation.
func recoveredBarrierRestore(t *testing.T, name string) (*RestoreReconciler, client.Client, *fakeTwoPC) {
	t.Helper()
	source := boundCluster("old")
	two := 2
	source.Spec.Shards = &two
	barrier := "b1"
	rs := newRestore(name, pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "old", NewClusterName: "new", BackupID: "b1", Target: pgshardv1alpha1.RestoreTarget{Barrier: &barrier}})
	bk := completedBackup("b1", "old")
	bk.Status.Groups = append(bk.Status.Groups, pgshardv1alpha1.GroupBackupStatus{Group: "shard-1", Stanza: "old-shard-1-pg18", BackupID: "20260819-100004F"})
	cl := restoreClient(t, source, newPolicy(), bk, rs, superuserSecret("old"))
	agents := newFakeAgents(nil)
	twoPC := &fakeTwoPC{outcomes: map[string]twopc.Outcome{}, fail: map[string]error{}}
	r := &RestoreReconciler{Client: cl, Agents: agents, TwoPC: twoPC, Now: func() time.Time { return time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC) }}
	if _, got := reconcileRestore(t, r, name); got.Status.Phase != pgshardv1alpha1.RestorePhaseRestoring {
		t.Fatalf("after create: %+v", got.Status)
	}
	var created pgshardv1alpha1.PgShardCluster
	if err := cl.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "new"}, &created); err != nil {
		t.Fatal(err)
	}
	if src, _ := RestoreSourceOf(&created); src.Type != backup.TargetName || src.Target != "pgshard-b1" || src.BackupIDs["shard-1"] != "20260819-100004F" {
		t.Fatalf("restore source %+v", src)
	}
	for i, g := range Groups(&created) {
		pg := &pgshardv1alpha1.PgShardGroup{ObjectMeta: metav1.ObjectMeta{Name: g.Prefix(), Namespace: "default"}}
		if err := cl.Create(context.Background(), pg); err != nil {
			t.Fatal(err)
		}
		pg.Status.Primary = g.MemberName(0)
		if err := cl.Status().Update(context.Background(), pg); err != nil {
			t.Fatal(err)
		}
		ip := fmt.Sprintf("10.1.0.%d", i+1)
		if err := cl.Create(context.Background(), readyPod(g.MemberName(0), ip)); err != nil {
			t.Fatal(err)
		}
		agents.set(ip, AgentStatus{Running: true, Primary: true, Timeline: 2, Epoch: uint64(10 + i)}, nil)
	}
	meta.SetStatusCondition(&created.Status.Conditions, metav1.Condition{Type: pgshardv1alpha1.ConditionReady, Status: metav1.ConditionTrue, Reason: "Ready"})
	if err := cl.Status().Update(context.Background(), &created); err != nil {
		t.Fatal(err)
	}
	return r, cl, twoPC
}

func TestBarrierRestoreReconcilesPreparedTransactionsThenUnfences(t *testing.T) {
	r, cl, twoPC := recoveredBarrierRestore(t, "r1")
	twoPC.decisions = []twopc.Decision{{GID: "pgshard-a", State: "commit", Participants: []int32{0, 1}}, {GID: "pgshard-b", State: "abort", Participants: []int32{1}}}
	twoPC.outcomes["10.1.0.2:9090"] = twopc.Outcome{Committed: 1}
	twoPC.outcomes["10.1.0.3:9090"] = twopc.Outcome{Committed: 1, RolledBack: 1}
	res, got := reconcileRestore(t, r, "r1")
	if got.Status.Phase != pgshardv1alpha1.RestorePhaseRecovered || res.RequeueAfter != 0 || got.Status.Error != "" || got.Status.CompletedAt == nil {
		t.Fatalf("status %+v res %v", got.Status, res)
	}
	st := got.Status.Reconciliation
	if st == nil || st.Decisions != 2 || st.Committed != 2 || st.RolledBack != 1 || len(st.Contradictions) != 0 || !st.Unfenced {
		t.Fatalf("reconciliation %+v", st)
	}
	want := []string{
		"decisions 10.1.0.1:9090",
		"reconcile 10.1.0.2:9090 epoch=11 shard=0 decisions=2",
		"reconcile 10.1.0.3:9090 epoch=12 shard=1 decisions=2",
		"fence 10.1.0.1:9090 epoch=10 active=false",
	}
	if strings.Join(twoPC.calls, "\n") != strings.Join(want, "\n") {
		t.Fatalf("calls:\n%s", strings.Join(twoPC.calls, "\n"))
	}
	if cond := meta.FindStatusCondition(got.Status.Conditions, "Progressing"); cond == nil || cond.Status != metav1.ConditionFalse || !strings.Contains(cond.Message, "unfenced: 2 committed, 1 rolled back") {
		t.Fatalf("condition %+v", cond)
	}
	var created pgshardv1alpha1.PgShardCluster
	if err := cl.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "new"}, &created); err != nil {
		t.Fatal(err)
	}
	if _, ok := RestoreSourceOf(&created); ok {
		t.Fatal("recovered barrier restore keeps the restore source annotation")
	}
	// Terminal: another reconcile changes nothing and calls no agent.
	reconcileRestore(t, r, "r1")
	if len(twoPC.calls) != len(want) {
		t.Fatalf("recovered restore touched the agents again: %v", twoPC.calls[len(want):])
	}
}

func TestBarrierRestoreFailsOnContradictionAndStaysFenced(t *testing.T) {
	r, _, twoPC := recoveredBarrierRestore(t, "r2")
	twoPC.decisions = []twopc.Decision{{GID: "pgshard-a", State: "commit", Participants: []int32{0, 1}}}
	twoPC.outcomes["10.1.0.2:9090"] = twopc.Outcome{Committed: 1}
	twoPC.outcomes["10.1.0.3:9090"] = twopc.Outcome{Contradictions: []string{"pgshard-a"}}
	res, got := reconcileRestore(t, r, "r2")
	if got.Status.Phase != pgshardv1alpha1.RestorePhaseFailed || res.RequeueAfter != 0 {
		t.Fatalf("status %+v", got.Status)
	}
	if !strings.Contains(got.Status.Error, "1 contradiction(s), the cluster stays fenced: shard-1: pgshard-a") {
		t.Fatalf("error %q", got.Status.Error)
	}
	if st := got.Status.Reconciliation; st == nil || st.Unfenced || len(st.Contradictions) != 1 || st.Committed != 1 {
		t.Fatalf("reconciliation %+v", st)
	}
	for _, c := range twoPC.calls {
		if strings.HasPrefix(c, "fence") {
			t.Fatalf("fence released despite a contradiction: %v", twoPC.calls)
		}
	}
}

func TestBarrierRestoreRetriesTransientAgentErrors(t *testing.T) {
	r, _, twoPC := recoveredBarrierRestore(t, "r3")
	twoPC.fail["10.1.0.3:9090"] = errors.New("agent 10.1.0.3:9090 unreachable")
	key := client.ObjectKey{Namespace: "default", Name: "r3"}
	_, err := r.Reconcile(context.Background(), reconcileReq(key))
	if err == nil || !strings.Contains(err.Error(), "group shard-1: agent 10.1.0.3:9090 unreachable") {
		t.Fatalf("err %v", err)
	}
	var got pgshardv1alpha1.PgShardRestore
	if err := r.Get(context.Background(), key, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != pgshardv1alpha1.RestorePhaseReconciling || got.Status.CompletedAt != nil {
		t.Fatalf("status %+v", got.Status)
	}
	if cond := meta.FindStatusCondition(got.Status.Conditions, "Progressing"); cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "Reconciling" || !strings.Contains(cond.Message, "reconciling prepared transactions: group shard-1") {
		t.Fatalf("condition %+v", cond)
	}
	delete(twoPC.fail, "10.1.0.3:9090")
	twoPC.fenceErr = errors.New("catalog agent busy")
	if _, err := r.Reconcile(context.Background(), reconcileReq(key)); err == nil || !strings.Contains(err.Error(), "release write fence: catalog agent busy") {
		t.Fatalf("fence err %v", err)
	}
	twoPC.fenceErr = nil
	_, got2 := reconcileRestore(t, r, "r3")
	if got2.Status.Phase != pgshardv1alpha1.RestorePhaseRecovered || !got2.Status.Reconciliation.Unfenced {
		t.Fatalf("status %+v", got2.Status)
	}
	// Reconciliation is idempotent: every retry re-ran the shards.
	n := 0
	for _, c := range twoPC.calls {
		if strings.HasPrefix(c, "reconcile 10.1.0.2") {
			n++
		}
	}
	if n != 3 {
		t.Fatalf("shard-0 reconciled %d times, want 3: %v", n, twoPC.calls)
	}
}

func TestBarrierRestoreWithoutTwoPCClientFailsLoudly(t *testing.T) {
	r, _, _ := recoveredBarrierRestore(t, "r4")
	r.TwoPC = nil
	_, got := reconcileRestore(t, r, "r4")
	if got.Status.Phase != pgshardv1alpha1.RestorePhaseFailed || !strings.Contains(got.Status.Error, "stays fenced") {
		t.Fatalf("status %+v", got.Status)
	}
}

func TestBarrierRestoreNeedsPrimaryPods(t *testing.T) {
	r, cl, twoPC := recoveredBarrierRestore(t, "r5")
	var pod corev1.Pod
	if err := cl.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "new-shard-1-0"}, &pod); err != nil {
		t.Fatal(err)
	}
	pod.Status.PodIP = ""
	if err := cl.Status().Update(context.Background(), &pod); err != nil {
		t.Fatal(err)
	}
	_, got := reconcileRestore(t, r, "r5")
	if got.Status.Phase != pgshardv1alpha1.RestorePhaseRestoring || len(twoPC.calls) != 0 {
		t.Fatalf("status %+v calls %v", got.Status, twoPC.calls)
	}
}

func reconcileReq(key client.ObjectKey) ctrl.Request { return ctrl.Request{NamespacedName: key} }
