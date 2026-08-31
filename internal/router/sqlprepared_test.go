package router

import (
	"context"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/router/plan"
)

func preparedNames(e *Executor) []string {
	var out []string
	for _, p := range e.sqlPrepared {
		out = append(out, p.name)
	}
	return out
}

func note(e *Executor, txn plan.TxnKind, sess plan.SessionKind, name string) {
	e.noteSessionEffect(StmtClass{Txn: txn, Session: sess, SessionName: name, Savepoint: name}, "PREPARE "+name+" AS SELECT 1")
}

func endTxn(t *testing.T, e *Executor, tag string) {
	t.Helper()
	e.tx, e.lastTag = pgwire.TxIdle, tag
	if err := e.afterBatch(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}

// TestASQLPrepareDiesWithTheTransactionThatMadeIt: PostgreSQL drops a
// PREPARE when the transaction that created it rolls back. The router kept
// it, so an EXECUTE of that name errored before a failover and succeeded
// after one -- the replay recreated it on the new backend. A session that
// behaves differently either side of a reconnect is the kind of difference
// nobody can reproduce.
func TestASQLPrepareDiesWithTheTransactionThatMadeIt(t *testing.T) {
	e := &Executor{}

	note(e, plan.TxnNone, plan.SessionPrepare, "before")
	note(e, plan.TxnBegin, plan.SessionNone, "")
	note(e, plan.TxnNone, plan.SessionPrepare, "inside")
	if got := preparedNames(e); len(got) != 2 {
		t.Fatalf("inside the transaction the router should hold both: %v", got)
	}
	endTxn(t, e, "ROLLBACK")
	if got := preparedNames(e); len(got) != 1 || got[0] != "before" {
		t.Fatalf("after ROLLBACK the router holds %v, want only the one prepared before it", got)
	}

	// A committed one survives, or the fix would trade one wrong answer
	// for another.
	note(e, plan.TxnBegin, plan.SessionNone, "")
	note(e, plan.TxnNone, plan.SessionPrepare, "committed")
	endTxn(t, e, "COMMIT")
	if got := preparedNames(e); len(got) != 2 || got[1] != "committed" {
		t.Fatalf("after COMMIT the router holds %v, want the committed PREPARE kept", got)
	}
}

// A DEALLOCATE that rolls back has to come back: PostgreSQL restores it,
// and a length-based undo could only forget additions.
func TestARolledBackDeallocateComesBack(t *testing.T) {
	e := &Executor{}
	note(e, plan.TxnNone, plan.SessionPrepare, "keep")

	note(e, plan.TxnBegin, plan.SessionNone, "")
	note(e, plan.TxnNone, plan.SessionDeallocate, "keep")
	if got := preparedNames(e); len(got) != 0 {
		t.Fatalf("inside the transaction the DEALLOCATE should have taken effect: %v", got)
	}
	endTxn(t, e, "ROLLBACK")
	if got := preparedNames(e); len(got) != 1 || got[0] != "keep" {
		t.Fatalf("after ROLLBACK the router holds %v, want the deallocated statement restored", got)
	}
}

// ROLLBACK TO undoes what a savepoint scoped, in both directions, and
// leaves what came before it alone.
func TestRollbackToASavepointUndoesItsPrepares(t *testing.T) {
	e := &Executor{}
	note(e, plan.TxnBegin, plan.SessionNone, "")
	note(e, plan.TxnNone, plan.SessionPrepare, "outer")
	note(e, plan.TxnSavepoint, plan.SessionNone, "sp")
	note(e, plan.TxnNone, plan.SessionPrepare, "inner")
	note(e, plan.TxnNone, plan.SessionDeallocate, "outer")
	if got := preparedNames(e); len(got) != 1 || got[0] != "inner" {
		t.Fatalf("inside the savepoint: %v", got)
	}

	note(e, plan.TxnRollbackTo, plan.SessionNone, "sp")
	if got := preparedNames(e); len(got) != 1 || got[0] != "outer" {
		t.Fatalf("after ROLLBACK TO the router holds %v, want the savepoint's own state back", got)
	}
	endTxn(t, e, "ROLLBACK")
	if got := preparedNames(e); len(got) != 0 {
		t.Fatalf("after the outer ROLLBACK the router holds %v, want nothing", got)
	}
}
