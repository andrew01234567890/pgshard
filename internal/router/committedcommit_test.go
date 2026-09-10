package router

import (
	"context"
	"strings"
	"testing"
)

// TestACommittedTransactionIsNotReportedAsOneToRetry.
//
// A transaction that writes on one shard and READS on another commits with
// a plain COMMIT: one writer needs no two-phase commit. The readers are
// rolled back first, and their errors were then returned as the COMMIT's
// own -- after the COMMIT had succeeded and its tag was already on the
// wire.
//
// The client therefore saw a COMMIT tag followed by an error, and the error
// was 40001: nameFenceInTxn rewrites a stale-generation refusal into "retry
// the transaction". A client that does as it is told applies its writes
// twice. Nothing about the reader's rollback says the transaction failed --
// it wrote nothing, and the pooler resets the backend when it takes it
// back.
//
// twoPhaseCommit has always got this right, consulting firstError(writers)
// alone. This is the other path agreeing with it.
func TestACommittedTransactionIsNotReportedAsOneToRetry(t *testing.T) {
	h := newTxnHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	sa, sb := h.shardOf(t, a), h.shardOf(t, b)
	conn := h.connect(t, h.dsn())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Write on one shard, read on the other: one writer, one reader.
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", a); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "select * from orders where tenant_id = $1", b); err != nil {
		t.Fatal(err)
	}

	// The reader's shard now refuses everything, ROLLBACK included, which
	// is what a shard behind a stale-generation fence does.
	h.poolers[sb].failRollback = true

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("the transaction committed on shard %d and the client was told it failed: %v", sa, err)
	}
	if !h.ranOn(sa, "commit") {
		t.Fatalf("the writer did not commit: %v", h.poolers[sa].ran())
	}
}

// TestARollbackStillReportsWhatFailed: the same path with ROLLBACK rather
// than COMMIT keeps reporting, because there the other participants' errors
// are the whole answer -- nothing has been told to the client yet.
func TestARollbackStillReportsWhatFailed(t *testing.T) {
	h := newTxnHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	sb := h.shardOf(t, b)
	conn := h.connect(t, h.dsn())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", a); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 2)", b); err != nil {
		t.Fatal(err)
	}
	h.poolers[sb].failRollback = true
	err = tx.Rollback(ctx)
	if err == nil {
		return // a rollback that reports nothing is acceptable; it rolled back
	}
	if !strings.Contains(err.Error(), "rollback refused") && !strings.Contains(err.Error(), "40001") {
		t.Fatalf("a failed rollback reported something else: %v", err)
	}
}
