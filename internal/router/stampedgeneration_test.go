package router

import (
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// TestARequestIsStampedFromTheSnapshotItWasPlannedAgainst: planning read the
// snapshot and every send read it again, and the watcher swaps the pointer on
// every reload -- so a plan made under generation G could be stamped G+1 and
// admitted by a pooler already at G+1, which is exactly the case the fence
// exists to refuse. Today a cutover also pauses writes on the sources, so
// PostgreSQL refuses the statement itself; any topology change that bumps the
// generation without that pause reopens the window.
func TestARequestIsStampedFromTheSnapshotItWasPlannedAgainst(t *testing.T) {
	h := newShardedHarness(t)
	shard := Shard{Set: DefaultShardSet, ID: 0}
	e := newExecutor(h.r, pgwire.SessionInfo{ID: 1, Database: "app", User: "app", Auth: &pgwire.AuthResult{SCRAM: &pgwire.SCRAMKeys{}}}, shard)

	planned := *h.snap
	planned.ShardMapGeneration = 7
	planned.Serving = map[snapshot.ShardKey]snapshot.Serving{
		{ShardSet: DefaultShardSet, ShardID: 0}: {Epoch: 2},
	}
	// The watcher publishes a newer one while the statement is in flight.
	moved := planned
	moved.ShardMapGeneration = 8
	moved.Serving = map[snapshot.ShardKey]snapshot.Serving{
		{ShardSet: DefaultShardSet, ShardID: 0}: {Epoch: 3},
	}
	h.snapp.Store(&moved)

	e.stmtSnap = &planned
	got := e.generation()
	if got.GetShardMapGeneration() != 7 || got.GetPrimaryEpoch() != 2 {
		t.Fatalf("stamped generation %d epoch %d, want the planned 7/2: a plan made under one generation must not be sent under another",
			got.GetShardMapGeneration(), got.GetPrimaryEpoch())
	}

	// Between statements there is nothing to pin to, and the live snapshot
	// is what a Sync or a Close should carry.
	e.stmtSnap = nil
	if got := e.generation(); got.GetShardMapGeneration() != 8 {
		t.Fatalf("with no statement in flight the stamp is %d, want the live 8", got.GetShardMapGeneration())
	}
}
