package operator

import (
	"context"
	"testing"

	"k8s.io/utils/ptr"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestASQLReshardIsNotUndoneByAnUnchangedSpec is the reverse reshard: a
// shard set created and switched through SQL leaves spec.shards naming the
// old count, and reading that difference as a request sent the cluster
// straight back where it came from -- automatically, and without anyone
// asking for it.
func TestASQLReshardIsNotUndoneByAnUnchangedSpec(t *testing.T) {
	r, fp, c := setup(t, "noundo")
	t.Cleanup(func() { deleteServicesOf(t, c) })
	c.Spec.Shards = ptr.To(1)
	if err := k8sClient.Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	bringUp(t, r, fp, c)
	get(t, c.Name, c)
	if c.Status.AppliedShards == nil || *c.Status.AppliedShards != 1 {
		t.Fatalf("the operator must record the count it applied: %+v", c.Status.AppliedShards)
	}

	// The catalog is now serving two shards, put there through SQL, while
	// spec.shards still says one.
	def, _ := fp.shardSet(catalog.DefaultShardSet)
	fp.mu.Lock()
	fp.shardSets = []ShardSetInfo{{Name: "g2", Generation: 2, State: catalog.ShardSetServing,
		Ranges: splitAt(def.Ranges[0], 0), PGMajor: def.PGMajor}}
	fp.mu.Unlock()
	reconcile(t, r, c)
	get(t, c.Name, c)

	if c.Status.EffectiveShards != 2 {
		t.Fatalf("the operator did not observe the catalog: effective=%d", c.Status.EffectiveShards)
	}
	for _, s := range allSets(fp) {
		if s.State == catalog.ShardSetDesired {
			t.Fatalf("a pending set was materialized back toward spec.shards: %+v", s)
		}
	}
	cond := condition(t, c.Name, pgshardv1alpha1.ConditionResharding)
	if cond.Reason != "ShardCountConflict" {
		t.Fatalf("the disagreement must be surfaced, not acted on: %+v", cond)
	}

	// Accepting the catalog clears it, and the operator remembers the new
	// count as the one it has applied.
	get(t, c.Name, c)
	c.Spec.Shards = ptr.To(2)
	if err := k8sClient.Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	// The catalog gained a shard, so the cluster has a group to bring up
	// before a pass gets past it.
	reconcile(t, r, c)
	markPodsRunning(t, c)
	for _, g := range Groups(c) {
		for i := 1; i < g.Replicas; i++ {
			fp.streaming[g.MemberName(i)] = true
		}
	}
	reconcile(t, r, c)
	get(t, c.Name, c)
	if c.Status.AppliedShards == nil || *c.Status.AppliedShards != 2 {
		t.Fatalf("accepting the catalog must be recorded: applied=%v effective=%d", derefInt(c.Status.AppliedShards), c.Status.EffectiveShards)
	}
	// The change test that decides whether the status is written at all
	// has to know this field: without it a pass where only appliedShards
	// moved wrote nothing, and the operator forgot every time.
	if cond := condition(t, c.Name, pgshardv1alpha1.ConditionResharding); cond.Reason == "ShardCountConflict" {
		t.Fatalf("the conflict must clear once the spec agrees: %+v", cond)
	}
}

// TestChangingSpecShardsStillReshardsAfterASQLReshard keeps the spec a way
// to reshard: it stops being obeyed only while it has not changed.
func TestChangingSpecShardsStillReshardsAfterASQLReshard(t *testing.T) {
	r, fp, c := setup(t, "stillre")
	t.Cleanup(func() { deleteServicesOf(t, c) })
	c.Spec.Shards = ptr.To(1)
	if err := k8sClient.Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	bringUp(t, r, fp, c)
	def, _ := fp.shardSet(catalog.DefaultShardSet)
	fp.mu.Lock()
	fp.shardSets = []ShardSetInfo{{Name: "g2", Generation: 2, State: catalog.ShardSetServing,
		Ranges: splitAt(def.Ranges[0], 0), PGMajor: def.PGMajor}}
	fp.mu.Unlock()
	reconcile(t, r, c)

	get(t, c.Name, c)
	c.Spec.Shards = ptr.To(4)
	if err := k8sClient.Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, c)
	var desired *ShardSetInfo
	for _, s := range allSets(fp) {
		if s.State == catalog.ShardSetDesired {
			desired = &s
		}
	}
	if desired == nil || len(desired.Ranges) != 4 {
		t.Fatalf("a changed spec.shards must still reshard: %+v", allSets(fp))
	}
	get(t, c.Name, c)
	if c.Status.AppliedShards == nil || *c.Status.AppliedShards != 4 {
		t.Fatalf("the operator must record what it acted on: %+v", c.Status.AppliedShards)
	}
}

func allSets(f *fakeProber) []ShardSetInfo {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]ShardSetInfo(nil), f.shardSets...)
}

func derefInt(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}
