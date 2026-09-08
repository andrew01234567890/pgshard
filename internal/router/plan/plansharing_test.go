package plan

import (
	"context"
	"strconv"
	"testing"
)

// TestOneDeferredPlanResolvesIndependentlyForEachBind guards the
// precondition a plan cache depends on. PGS-197 names the hazard: "a cached
// Plan handed to two sessions must be provably immutable or copied on the
// way out; an append into a shared slice with spare capacity is the same
// aliasing hazard that got PGS-200's valuable half declined."
//
// Today that hazard is unreachable, and the reason is worth pinning rather
// than rediscovering. A deferred plan carries NO route -- Shards and
// ShardKeyValues are nil -- so Resolve's copy of it inherits nothing to
// append into, and each bind allocates its own. Checked: making finish
// reuse the inherited slices changes nothing while they are nil.
//
// So the assertion that can actually fail is the first one, and it is the
// early warning rather than the damage: a planner that starts handing
// deferred plans a preallocated Shards slice makes every later resolution
// share it. Mutation-checked that way -- Shards = make([]int32, 0, 8) on a
// deferred plan fails here immediately, before any cache exists to turn it
// into a cross-session defect.
//
// The write-through checks below cannot fail while that holds. They are
// kept as the second half of the story: if the precondition is ever
// relaxed, they are what turn "carries a route" into "and here is the
// corruption".
func TestOneDeferredPlanResolvesIndependentlyForEachBind(t *testing.T) {
	snap := fixture(t)
	p := New()
	pl, err := p.Plan(context.Background(), session(snap), "select * from orders where tenant_id = $1")
	if err != nil || !pl.Deferred {
		t.Fatalf("plan: %+v %v", pl, err)
	}
	if pl.Shards != nil || pl.ShardKeyValues != nil {
		t.Fatalf("a deferred plan already carries a route: shards=%v values=%v", pl.Shards, pl.ShardKeyValues)
	}

	// Two keys that land on different shards, so a resolution leaking into
	// the other is visible as a wrong shard rather than as a coincidence.
	const a int64 = 42
	var b int64
	for b = 0; b < 1000; b++ {
		if shardOf(t, snap, b) != shardOf(t, snap, a) {
			break
		}
	}
	if b == 1000 {
		t.Skip("this fixture routes every candidate key to one shard")
	}

	first, err := pl.Resolve(intParam(a))
	if err != nil {
		t.Fatal(err)
	}
	second, err := pl.Resolve(intParam(b))
	if err != nil {
		t.Fatal(err)
	}
	if first.Shards[0] != shardOf(t, snap, a) || second.Shards[0] != shardOf(t, snap, b) {
		t.Fatalf("resolved to %v and %v, want %d and %d",
			first.Shards, second.Shards, shardOf(t, snap, a), shardOf(t, snap, b))
	}
	if first.ShardKeyValues[0] != a || second.ShardKeyValues[0] != b {
		t.Fatalf("keys %v and %v, want %d and %d", first.ShardKeyValues, second.ShardKeyValues, a, b)
	}

	// The plan they came from is untouched, so a third bind still starts
	// from a deferred plan rather than from the second one's route.
	if pl.Shards != nil || pl.ShardKeyValues != nil || !pl.Deferred {
		t.Fatalf("resolving mutated the plan it resolved: shards=%v values=%v deferred=%v",
			pl.Shards, pl.ShardKeyValues, pl.Deferred)
	}

	// Neither resolution shares a backing array with the other. Unreachable
	// while a deferred plan carries no route; see the note above.
	wantB := second.Shards[0]
	first.Shards[0] = -1
	first.ShardKeyValues[0] = int64(-1)
	if second.Shards[0] != wantB || second.ShardKeyValues[0] != b {
		t.Fatalf("writing through one resolution changed the other: shards=%v values=%v", second.Shards, second.ShardKeyValues)
	}
	second.Shards[0] = -2
	if first.Shards[0] != -1 {
		t.Fatalf("the two resolutions share a shard slice: %v", first.Shards)
	}
}

func intParam(v int64) BindParams {
	return BindParams{OIDs: []uint32{oidInt8}, Values: [][]byte{[]byte(strconv.FormatInt(v, 10))}}
}
