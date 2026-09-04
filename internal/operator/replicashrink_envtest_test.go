package operator

import (
	"context"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

// grow brings a group to n members and reports them healthy, so a test can
// then lower it again.
func grow(t *testing.T, r *ClusterReconciler, fp *fakeProber, fa *fakeAgents, c *pgshardv1alpha1.PgShardCluster, n int) {
	t.Helper()
	patchSpec(t, c, func(c *pgshardv1alpha1.PgShardCluster) { c.Spec.ReplicasPerShard = n })
	reconcile(t, r, c)
	markPodsRunning(t, c)
	for i := range n {
		fa.set(podIP(1, i), AgentStatus{Running: true, Primary: i == 0}, nil)
		if i > 0 {
			fp.streaming[Groups(c)[1].MemberName(i)] = true
			fp.standbys[podIP(1, i)] = StandbyState{InRecovery: true, FlushLSN: 900}
		}
	}
	reconcile(t, r, c)
}

// TestLoweringReplicasRetiresMembersOneAtATime is PGS-617. Lowering used to
// be refused outright, because a member the spec stops describing keeps its
// pod, its claim, its slot and its place in synchronous_standby_names and
// nothing takes any of them away.
func TestLoweringReplicasRetiresMembersOneAtATime(t *testing.T) {
	r, fp, fa, c := healthyCluster(t, "shrink")
	grow(t, r, fp, fa, c, 5)
	for _, name := range []string{"shrink-shard-0-3", "shrink-shard-0-4"} {
		if !podExists(t, name) {
			t.Fatalf("%s should exist before the group is lowered", name)
		}
	}

	patchSpec(t, c, func(c *pgshardv1alpha1.PgShardCluster) { c.Spec.ReplicasPerShard = 3 })
	// A step per pass, as everywhere else in the rollout: the first pass
	// after the edit re-renders the group, and the retirement follows once
	// it is settled again.
	for range 4 {
		reconcile(t, r, c)
		if !podExists(t, "shrink-shard-0-4") {
			break
		}
	}

	// Highest ordinal first, and only that one: a group is down a member
	// while a retirement happens, and taking two out at once is what leaves
	// it without a promotable standby.
	if podExists(t, "shrink-shard-0-4") {
		t.Error("the highest extra ordinal should have been retired first")
	}
	if !podExists(t, "shrink-shard-0-3") {
		t.Error("the second extra member must wait until the first is gone")
	}
	if st := groupStatus(t, "shrink-shard-0"); st.Rollout == nil || st.Rollout.Phase != pgshardv1alpha1.RolloutPhaseRetiring {
		t.Fatalf("the step must be recorded as Retiring: %+v", st.Rollout)
	}

	// Its claim is kept: a retired member's volume is the only copy of
	// whatever had not replicated when it left.
	var kept corev1.PersistentVolumeClaimList
	if err := k8sClient.List(context.Background(), &kept, client.InNamespace("default"),
		client.MatchingLabels{LabelMember: "shrink-shard-0-4"}); err != nil {
		t.Fatal(err)
	}
	if len(kept.Items) == 0 {
		t.Error("the retired member's claim must be kept under the default reclaim policy")
	}
	for i := range kept.Items {
		if kept.Items[i].DeletionTimestamp != nil {
			t.Errorf("claim %s is being deleted although the policy is Retain", kept.Items[i].Name)
		}
	}
}

// A member the spec stops describing may still be the primary. Deleting it
// would be a failover the operator did to itself, and to the very member it
// is removing, so stepRetire holds instead.
//
// The check is on the POD's role rather than the designation, and that
// distinction is the point: loadState re-designates a primary that is
// outside the member set (PGS-466's guard), so by the time a retirement
// runs, state.primary already names someone else while PostgreSQL on the
// retired member may still be the primary.
//
// Tested here rather than through a reconcile because the state cannot be
// reached that way: a group whose only primary is outside its member set is
// one the operator treats as having no healthy primary, so it does failover
// work and no rollout work at all. That is the correct ordering -- this
// guard is what stops a retirement acting if it is ever reached anyway.
func TestARetiredMemberStillLabelledPrimaryIsRecognised(t *testing.T) {
	r, _, _, c := healthyCluster(t, "retprim")
	g := Groups(c)[1]

	is, err := r.memberIsPrimary(context.Background(), c, g.MemberName(0))
	if err != nil || !is {
		t.Fatalf("the primary's own pod must be recognised: %v %v", is, err)
	}
	is, err = r.memberIsPrimary(context.Background(), c, g.MemberName(1))
	if err != nil || is {
		t.Fatalf("a replica must not be taken for the primary: %v %v", is, err)
	}
	// A member whose pod is gone is not the primary, and asking must not be
	// an error: that is the ordinary case once a retirement has deleted it.
	is, err = r.memberIsPrimary(context.Background(), c, g.Prefix()+"-9")
	if err != nil || is {
		t.Fatalf("a member with no pod: %v %v", is, err)
	}
}

// TestADesignatedPrimaryOutsideTheMemberSetIsNotDereferenced covers the
// panic PGS-466 reported. It is already guarded -- loadState designates a
// member that exists, and the observation treats a missing primary as an
// unhealthy one -- and this holds that guard in place, because the failure
// it prevents is a crash loop over the whole cluster rather than one group.
func TestADesignatedPrimaryOutsideTheMemberSetIsNotDereferenced(t *testing.T) {
	r, fp, c := setup(t, "outside")
	bringUp(t, r, fp, c)
	ctx := context.Background()
	g := Groups(c)[len(Groups(c))-1]

	pg := r.Renderer.PgShardGroup(c, g)
	if err := r.Get(ctx, client.ObjectKeyFromObject(pg), pg); err != nil {
		t.Fatal(err)
	}
	pg.Status.Primary = g.Prefix() + "-9"
	if err := r.Status().Update(ctx, pg); err != nil {
		t.Fatal(err)
	}
	st, err := r.loadState(ctx, c, g)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(g.MemberNames(), st.primary) {
		t.Fatalf("loadState designated %q, which is not a member", st.primary)
	}
	reconcile(t, r, c)
}
