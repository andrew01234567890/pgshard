package router

import (
	"context"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/router/plan"
)

func reviewExecutor(t *testing.T, h *shardedHarness) *Executor {
	t.Helper()
	keys := &pgwire.SCRAMKeys{ClientKey: make([]byte, 32), ServerKey: make([]byte, 32)}
	return newExecutor(h.r, pgwire.SessionInfo{ID: 1, Database: "app", User: "app", Auth: &pgwire.AuthResult{SCRAM: keys}}, Shard{Set: DefaultShardSet, ID: 0})
}

// TestACachedStatementPinsTheSnapshotItWasCheckedAgainst: replanStaleAt
// returned as soon as the planning was unchanged and left stmtSnap unset,
// so the statement was AIMED from the plan made against its own snapshot
// while userSet and generation() read the live one. A reshard landing
// between that check and the Bind's aim sent the write to the old plan's
// shard id under the new set's generation -- which the fence cannot catch,
// because the generation it carries is the current one.
func TestACachedStatementPinsTheSnapshotItWasCheckedAgainst(t *testing.T) {
	h := newShardedHarness(t)
	e := reviewExecutor(t, h)
	ctx := context.Background()
	a, _ := h.twoTenants(t)

	if err := e.Parse(ctx, "s", "select id from orders where tenant_id = "+itoa64(a), nil, discardWriter{}); err != nil {
		t.Fatal(err)
	}
	_ = e.Sync(ctx)
	if e.stmtSnap != nil {
		t.Fatal("the statement is over, so nothing should be pinned")
	}
	// Bind again with the planning unchanged: nothing is replanned, and
	// the snapshot the plan was checked against must be the one the
	// statement is stamped and aimed from.
	if err := e.replanStale(ctx, "s"); err != nil {
		t.Fatal(err)
	}
	if e.stmtSnap == nil {
		t.Fatal("a cached statement left no pinned snapshot, so its aim and its stamp can come from different maps")
	}
	if !snapshot.SamePlanning(e.stmtSnap, e.currentSnapshot()) {
		t.Fatal("the pinned snapshot is not the one the statement was checked against")
	}
}

// TestAForeignPortalHoldsItsPlaceInTheBatch: pump counts every client
// Execute whose response ended, and noteBatch walks the batch's statements
// by that count. An Execute of a portal this router did not bind -- a
// cursor DECLAREd in SQL -- produces a completion and carries no statement
// of ours, so without a place held for it the count shifted and noteBatch
// recorded a LATER statement that had not run.
func TestAForeignPortalHoldsItsPlaceInTheBatch(t *testing.T) {
	h := newShardedHarness(t)
	e := reviewExecutor(t, h)

	e.tx = pgwire.TxInBlock
	executed := []execItem{
		{foreign: true},
		{sql: "savepoint sp", local: true, class: StmtClass{Txn: plan.TxnSavepoint, Savepoint: "sp"}},
	}
	// One completion: the cursor's. The SAVEPOINT did not run.
	e.execDone = 1
	e.noteBatch(executed)
	if e.savepointIndex("sp") >= 0 {
		t.Fatalf("a statement that never ran was recorded: %+v", e.savepoints)
	}
	if !e.txnTouched {
		t.Error("the cursor ran on the shard, so the transaction has touched it")
	}
	// Two completions: the cursor's and the SAVEPOINT's.
	e.execDone = 2
	e.noteBatch(executed)
	if e.savepointIndex("sp") < 0 {
		t.Fatal("the statement that ran was not recorded")
	}

	// And execute() holds that place itself for a portal it did not bind.
	e.batchExec = nil
	if err := e.Execute(context.Background(), "a-cursor-this-router-never-bound", 0, discardWriter{}); err != nil {
		t.Fatalf("executing a portal the backend owns: %v", err)
	}
	if len(e.batchExec) != 1 || !e.batchExec[0].foreign {
		t.Fatalf("the Execute of a backend-owned portal held no place in the batch: %+v", e.batchExec)
	}
}
