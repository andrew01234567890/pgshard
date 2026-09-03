package operator

import (
	"context"
	"errors"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

// TestAGroupThatLostItsStatusRecoversItsSyncSet is the recovery case
// PGS-385 asks for: delete the status a group's failover memory lives in
// and see what comes back. syncSet is what an acknowledged commit may
// exist on, so an empty one refuses failover and switchover -- fail-safe,
// but only useful if it repopulates.
func TestAGroupThatLostItsStatusRecoversItsSyncSet(t *testing.T) {
	r, fp, c := setup(t, "syncrec")
	bringUp(t, r, fp, c)
	ctx := context.Background()
	g := Groups(c)[len(Groups(c))-1]

	before, err := r.loadState(ctx, c, g)
	if err != nil {
		t.Fatal(err)
	}
	if len(before.syncSet) == 0 {
		t.Fatal("the group never had a sync set to lose")
	}

	// As a recreated object or a restore without status leaves it.
	pg := r.Renderer.PgShardGroup(c, g)
	if err := r.Get(ctx, client.ObjectKeyFromObject(pg), pg); err != nil {
		t.Fatal(err)
	}
	pg.Status = pgshardv1alpha1.PgShardGroupStatus{}
	if err := r.Status().Update(ctx, pg); err != nil {
		t.Fatal(err)
	}
	// The memory does not wait for a pass: it is recorded outside the
	// status, so it is there the moment the status is gone.
	lost, err := r.loadState(ctx, c, g)
	if err != nil {
		t.Fatal(err)
	}
	if len(lost.syncSet) != len(before.syncSet) {
		t.Fatalf("straight after the wipe the sync set is %v, want %v", lost.syncSet, before.syncSet)
	}

	// And a pass then rewrites the status from what it observes, so the
	// ordinary path still leads.
	reconcile(t, r, c)
	after, err := r.loadState(ctx, c, g)
	if err != nil {
		t.Fatal(err)
	}
	if len(after.syncSet) != len(before.syncSet) {
		t.Fatalf("after one pass the sync set is %v, want %v", after.syncSet, before.syncSet)
	}
	for name := range before.syncSet {
		if !after.syncSet[name] {
			t.Fatalf("%s was in the sync set and did not come back: %v", name, after.syncSet)
		}
	}
	if after.primary != before.primary {
		t.Fatalf("the primary changed across the wipe: %q then %q", before.primary, after.primary)
	}
	if after.epoch < before.epoch {
		t.Fatalf("the epoch went backwards: %d then %d", before.epoch, after.epoch)
	}
}

// TestStatusLostWhileThePrimaryIsDown is the case that cannot heal from
// observation: syncSet is rebuilt from what a pass sees streaming, and
// with no primary to observe there is nothing to rebuild it from. An empty
// set refuses every failover -- safely, but the cluster stays down until
// somebody intervenes, which is precisely when nobody wants to.
//
// The Lease remembers it, beside the primary and the epoch it already
// carries, so a status wipe during an outage still knows which members an
// acknowledged commit may exist on.
func TestStatusLostWhileThePrimaryIsDown(t *testing.T) {
	r, fp, fa, c := healthyCluster(t, "syncdown")
	shard := Groups(c)[1]
	ctx := context.Background()

	pg := r.Renderer.PgShardGroup(c, shard)
	if err := r.Get(ctx, client.ObjectKeyFromObject(pg), pg); err != nil {
		t.Fatal(err)
	}
	remembered := rememberedSyncSet(shard, pg.Annotations[AnnotationSyncSet])
	if len(remembered) == 0 {
		t.Fatalf("a healthy group must have recorded its synchronous set: %v", pg.Annotations)
	}
	pg.Status = pgshardv1alpha1.PgShardGroupStatus{}
	if err := r.Status().Update(ctx, pg); err != nil {
		t.Fatal(err)
	}

	deletePod(t, "syncdown-shard-0-0")
	fa.set(podIP(1, 0), AgentStatus{}, errors.New("connection refused"))
	fp.standbys[podIP(1, 1)] = StandbyState{InRecovery: true, FlushLSN: 100}
	fp.standbys[podIP(1, 2)] = StandbyState{InRecovery: true, FlushLSN: 200}
	fp.err = errors.New("no primary")

	st, err := r.loadState(ctx, c, shard)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.syncSet) == 0 {
		t.Fatal("with the status gone and no primary to observe, the lease is the only thing that remembers the synchronous set")
	}
	for _, name := range remembered {
		if !st.syncSet[name] {
			t.Fatalf("%s was in the recorded set and did not come back: %v", name, st.syncSet)
		}
	}
	if st.primary == "" {
		t.Fatal("the primary was not recovered from the lease")
	}

	// And with the memory back, the failover it was refusing can proceed.
	reconcile(t, r, c)
	if len(fa.promotes) != 1 {
		t.Fatalf("exactly one promotion expected once the synchronous set was recovered, got %v", fa.promotes)
	}
}

// TestARememberedSyncSetDropsMembersThatNoLongerExist keeps the memory
// honest across a scale-down: a name left behind on the Lease is not a
// candidate for anything.
func TestARememberedSyncSetDropsMembersThatNoLongerExist(t *testing.T) {
	r, fp, c := setup(t, "syncshrink")
	bringUp(t, r, fp, c)
	ctx := context.Background()
	g := Groups(c)[len(Groups(c))-1]

	got := rememberedSyncSet(g, g.MemberName(1)+",syncshrink-shard-0-99")
	if len(got) != 1 || got[0] != g.MemberName(1) {
		t.Fatalf("recovered %v, want only the member that still exists", got)
	}
	if n := len(rememberedSyncSet(g, "")); n != 0 {
		t.Fatalf("an empty record recovered %d members", n)
	}
	_ = ctx
}
