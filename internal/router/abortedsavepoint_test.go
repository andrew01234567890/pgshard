package router

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestRollbackToSavepointRecoversATransactionAbortedOnItsBackend is the
// ordinary half of savepoint recovery, and the half PGS-883 item 7 did NOT
// cover: the backend is alive and holding the transaction, which PostgreSQL
// aborted because a statement in it failed. ROLLBACK TO SAVEPOINT returns
// the block to in-progress and the client carries on. #1047 covered the
// other half, where the backend is gone and the router answers alone.
//
// It is here because the fake pooler got BOTH halves of this wrong, and
// nothing noticed:
//
//   - it refused ROLLBACK TO in an aborted transaction, admitting only
//     ROLLBACK and COMMIT. PostgreSQL admits it -- xact.c handles
//     TBLOCK_ABORT and TBLOCK_SUBABORT, and postgres.c's
//     IsTransactionExitStmt lists TRANS_STMT_ROLLBACK_TO beside the other
//     two. Recovering a transaction instead of losing it is the entire
//     point of a savepoint.
//   - having answered it, it left the block 'E', so a recovered
//     transaction still reported as dead.
//
// A fake STRICTER than the thing it stands for is the worse direction: the
// router can do exactly the right thing and the test still sees a refusal,
// so the behaviour reads as unsupported and nobody writes the test.
func TestRollbackToSavepointRecoversATransactionAbortedOnItsBackend(t *testing.T) {
	h := newTxnHarness(t)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	tenant, _ := h.twoTenants(t)

	for _, sql := range []string{"begin", "savepoint sp"} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	// A statement the shard rejects, which aborts the transaction ON the
	// backend -- so the router still holds the connection, unlike the
	// failover and failed-DDL paths.
	bad := "select nosuchcolumn from orders where tenant_id = " + itoa(tenant)
	h.poolers[h.shardOf(t, tenant)].script(bad, script{err: "no such column"})
	if _, err := conn.Exec(ctx, bad, pgx.QueryExecModeSimpleProtocol); err == nil {
		t.Fatal("the shard was expected to reject this statement")
	}
	if st := conn.PgConn().TxStatus(); st != 'E' {
		t.Fatalf("transaction status %c after the failed statement, want E", st)
	}

	tag, err := conn.Exec(ctx, "rollback to savepoint sp")
	if err != nil {
		t.Fatalf("ROLLBACK TO SAVEPOINT on a backend-held aborted transaction: %v; PostgreSQL admits it, and it is how a client recovers instead of losing the transaction", err)
	}
	if tag.String() != "ROLLBACK" {
		t.Errorf("ROLLBACK TO answered %q, want ROLLBACK", tag.String())
	}
	if st := conn.PgConn().TxStatus(); st != 'T' {
		t.Fatalf("ReadyForQuery reported %c after the recovery, want T: the transaction is usable again and the client is being told it is not", st)
	}

	// And it really is usable: a write in it commits.
	if _, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 4242)", tenant); err != nil {
		t.Fatalf("a write in the recovered transaction: %v", err)
	}
	tag, err = conn.Exec(ctx, "commit")
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if tag.String() != "COMMIT" {
		t.Errorf("COMMIT answered %q, want COMMIT", tag.String())
	}
	var ran bool
	for i := range h.poolers {
		for _, q := range h.poolers[i].ran() {
			if strings.Contains(q, "4242") {
				ran = true
			}
		}
	}
	if !ran {
		t.Error("no shard ran the write, so the recovered transaction committed nothing")
	}
}
