package router

import (
	"context"
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

// TestAStatementInFlightTargetsTheShardSetItWasPlannedAgainst (PGS-962):
// execution named shards by id in the LIVE serving set while the stamp came
// from the planned snapshot. Across a cutover that swaps the serving set the
// two disagree, the epoch lookup misses and the request carries epoch 0.
func TestAStatementInFlightTargetsTheShardSetItWasPlannedAgainst(t *testing.T) {
	h := newShardedHarness(t)
	e := newExecutor(h.r, pgwire.SessionInfo{ID: 1, Database: "app", User: "app", Auth: &pgwire.AuthResult{SCRAM: &pgwire.SCRAMKeys{}}}, Shard{Set: DefaultShardSet, ID: 0})

	planned := *h.snap
	planned.ServingSet = "blue"
	moved := planned
	moved.ServingSet = "green"
	h.snapp.Store(&moved)

	e.stmtSnap = &planned
	if got := e.userSet(); got != "blue" {
		t.Fatalf("a statement planned against the blue set targets %q", got)
	}
	e.stmtSnap = nil
	if got := e.userSet(); got != "green" {
		t.Fatalf("with no statement in flight the set is %q, want the live green", got)
	}
}

// TestTheSnapshotDoesNotOutliveAStatementTheRouterAnswers (PGS-962): only a
// relayed ReadyForQuery cleared the pinned snapshot, so one the router
// answered itself left it behind for the next statement to be stamped and
// targeted from.
func TestTheSnapshotDoesNotOutliveAStatementTheRouterAnswers(t *testing.T) {
	h := newShardedHarness(t)
	e := newExecutor(h.r, pgwire.SessionInfo{ID: 1, Database: "app", User: "app", Auth: &pgwire.AuthResult{SCRAM: &pgwire.SCRAMKeys{}}}, Shard{Set: DefaultShardSet, ID: 0})
	ctx := context.Background()

	for _, sql := range []string{"explain (pgshard) select 1", "commit"} {
		_ = e.SimpleQuery(ctx, sql, discardWriter{})
		if e.stmtSnap != nil {
			t.Fatalf("after %q, answered by the router, the snapshot it was planned against is still pinned", sql)
		}
	}
	if err := e.Parse(ctx, "", "explain (pgshard) select 1", nil, discardWriter{}); err != nil {
		t.Fatal(err)
	}
	if e.stmtSnap == nil {
		t.Fatal("Parse did not pin a snapshot, so the Sync check below proves nothing")
	}
	_ = e.Sync(ctx)
	if e.stmtSnap != nil {
		t.Fatal("after a Sync the router answered, the snapshot is still pinned")
	}
}
