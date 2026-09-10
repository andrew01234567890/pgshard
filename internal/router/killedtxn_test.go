package router

import (
	"context"
	"testing"
)

// TestAKilledTransactionCannotBeCommittedByAccident.
//
// A transaction killed by a mid-transaction failover left the session
// reporting Idle: dropStream sets TxIdle, and the router never synthesised
// a failed-transaction state. The client got 40001 and ReadyForQuery 'I'.
//
// Statement-level 40001 retry loops are the normal thing to write, so the
// application handles the statement error, carries on, and sends COMMIT.
// That ran on a fresh backend which had never heard of the transaction,
// returned a COMMIT tag, and pgx and JDBC both reported success. Everything
// written before the failover was gone and the client believed it was
// committed.
//
// PostgreSQL answers 'E' and 25P02 for every statement until the
// transaction is ended, which is what a client needs in order to do the
// right thing.
func TestAKilledTransactionCannotBeCommittedByAccident(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	if _, err := conn.Exec(ctx, "begin"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "select 1"); err != nil {
		t.Fatal(err)
	}

	serving := h.fence("fenced")
	if _, err := conn.Exec(ctx, "select 1"); sqlstate(err) != "40001" {
		t.Fatalf("the killed statement reported %v", err)
	}
	h.setSnap(serving)

	// What the retry loop does next: carry on as though the statement
	// alone had failed.
	if _, err := conn.Exec(ctx, "select 1"); sqlstate(err) != "25P02" {
		t.Fatalf("a statement after the transaction was killed reported %v, want 25P02: the client is being told its transaction is still usable", err)
	}
	if conn.PgConn().TxStatus() != 'E' {
		t.Fatalf("status %c", conn.PgConn().TxStatus())
	}

	// And the COMMIT that follows must NOT say the transaction committed.
	tag, err := conn.Exec(ctx, "commit")
	if err != nil {
		t.Fatalf("commit after a killed transaction: %v", err)
	}
	if tag.String() != "ROLLBACK" {
		t.Fatalf("COMMIT answered %q; PostgreSQL answers ROLLBACK for a commit in a failed transaction, and anything else tells the client its writes landed", tag.String())
	}

	// The session is usable again.
	if conn.PgConn().TxStatus() != 'I' {
		t.Fatalf("after the transaction ended the session is %c", conn.PgConn().TxStatus())
	}
	var n int
	if err := conn.QueryRow(ctx, "select 1").Scan(&n); err != nil || n != 1 {
		t.Fatalf("session after the failed transaction ended: %v", err)
	}
}
