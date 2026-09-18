package router

import (
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// snapWithHome is a snapshot of one database whose home shard is id.
func snapWithHome(id int32) *snapshot.Snapshot {
	return &snapshot.Snapshot{
		ShardMapGeneration: 7,
		ShardSets:          map[string][]snapshot.Range{DefaultShardSet: {{ShardID: 0}, {ShardID: 1}, {ShardID: 2}}},
		Serving:            map[snapshot.ShardKey]snapshot.Serving{},
		Databases:          map[string]catalog.Database{"app": {Name: "app", HomeShard: id, DefaultPlacement: "unsharded"}},
		Tables:             map[snapshot.TableKey]snapshot.Placement{},
	}
}

// TestAPlanIsBuiltFromOneSnapshot (PGS-894): planSessionAt takes the
// snapshot its caller pinned so that planning and the generation stamped on
// what the plan sends come from the same one. It then read HomeShard from
// e.Home(), which reads the LIVE snapshot -- so the pinning was defeated for
// exactly one field, and a watcher reload between planOp's two lines gave a
// plan whose home shard came from a snapshot it was never made under.
//
// This is a test of planSessionAt rather than of a statement end to end,
// and deliberately so: the whole contract of the function is "derive the
// session from THIS snapshot", the window at the call site is two adjacent
// lines that no test can open from outside, and a test that tried to hit it
// by counting snapshot reads would stop testing the moment someone added
// one -- silently, which is the failure this project keeps recording.
func TestAPlanIsBuiltFromOneSnapshot(t *testing.T) {
	planned, live := snapWithHome(0), snapWithHome(2)
	e := &Executor{
		r:    &Router{cfg: Config{Snapshot: func() *snapshot.Snapshot { return live }}},
		info: pgwire.SessionInfo{Database: "app"},
		home: Shard{Set: DefaultShardSet, ID: 9},
	}

	sess := e.planSessionAt(planned)
	if sess.Snapshot != planned {
		t.Fatalf("planSessionAt kept a snapshot that is not the one it was given")
	}
	if sess.HomeShard != 0 {
		t.Errorf("a plan pinned to the snapshot with home shard 0 was built with home shard %d, which is the LIVE snapshot's: planning and the generation stamped on what it sends come from different snapshots", sess.HomeShard)
	}

	// Home() itself still answers for the live snapshot, which is what its
	// callers outside planning want.
	if got := e.Home(); got.ID != 2 {
		t.Errorf("Home() reported shard %d, want the live snapshot's 2", got.ID)
	}

	// The fallback is keyed to the caller's snapshot too. A statement
	// planned against a snapshot that HAS the database must not fall back
	// to the session-start set because a reload in between dropped it.
	if got := e.homeAt(snapWithHome(1)); got.ID != 1 {
		t.Errorf("homeAt reported shard %d for a snapshot whose home is 1", got.ID)
	}
	gone := snapWithHome(0)
	delete(gone.Databases, "app")
	if got := e.homeAt(gone); got.ID != 9 {
		t.Errorf("homeAt reported shard %d for a snapshot without the database; want the session's own %d", got.ID, 9)
	}
}
