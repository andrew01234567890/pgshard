package router

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestRollbackToSavepointRecoversAFailedSequentialDDLTransaction
// (PGS-883 item 7): PostgreSQL answers ROLLBACK TO SAVEPOINT in an aborted
// transaction -- it is how a client recovers one instead of losing it --
// and refuseInFailedTransaction admitted only COMMIT and ROLLBACK.
//
// Measured before the fix:
//
//	BEGIN; SAVEPOINT sp; <sequential DDL fails>; ROLLBACK TO SAVEPOINT sp
//	-> 25P02, and every statement after it, until the transaction ended.
//
// It is recoverable here because of what that path leaves behind:
// runMigration gives the backend up through releaseUntouchedTxn before the
// migration is queued, and only reaches that point when the transaction has
// touched no shard. So the transaction holds nothing anywhere, the prelude
// IS the transaction, and replaying it rebuilds the savepoint with
// everything before it.
func TestRollbackToSavepointRecoversAFailedSequentialDDLTransaction(t *testing.T) {
	newH := func(t *testing.T) *shardedHarness {
		t.Helper()
		q := &fakeQueue{outcome: func(m catalog.DDLMigration) catalog.DDLMigration {
			m.State, m.Error = catalog.MigrationFailed, "relation already exists"
			return m
		}}
		h := newDDLHarness(t, q)
		app := h.snap.Databases["app"]
		app.DDLTransactions = catalog.DDLTransactionsSequential
		h.snap.Databases["app"] = app
		return h
	}

	t.Run("TheTransactionIsUsableAgain", func(t *testing.T) {
		h := newH(t)
		ctx := context.Background()
		conn := h.connect(t, h.dsn())
		for _, sql := range []string{"begin", "set application_name to 'before'", "savepoint sp", "set application_name to 'after'"} {
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
		}
		if _, err := conn.Exec(ctx, "create table t7 (id int primary key)"); err == nil {
			t.Fatal("the failing migration answered success, so this test never reaches the state it is about")
		}
		if _, err := conn.Exec(ctx, "rollback to savepoint sp"); err != nil {
			t.Fatalf("ROLLBACK TO SAVEPOINT after a failed sequential DDL: %v; PostgreSQL answers it, and it is the client's only way to keep the transaction", err)
		}
		// The decisive check. COMMIT answering "COMMIT" proves nothing on
		// its own: endNoTxn answers a COMMIT sent outside a transaction
		// with the same tag. 'T' is the router saying a transaction is
		// open, which is the claim being made to the client.
		if st := conn.PgConn().TxStatus(); st != 'T' {
			t.Fatalf("ReadyForQuery reported %c after the recovery, want T: the client is being told it has no transaction", st)
		}
		// And it is open on a shard, not only in the router's head.
		if _, err := conn.Exec(ctx, "insert into items (id) values (7701)"); err != nil {
			t.Fatalf("a write in the recovered transaction: %v", err)
		}
		tag, err := conn.Exec(ctx, "commit")
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
		if tag.String() != "COMMIT" {
			t.Fatalf("COMMIT answered %q, want COMMIT: anything else means the recovered transaction was not really there", tag.String())
		}
		var ran bool
		for i := range h.poolers {
			for _, q := range h.poolers[i].ran() {
				if strings.Contains(strings.ToLower(q), "insert into items") {
					ran = true
				}
			}
		}
		if !ran {
			t.Error("no shard ran the write, so the recovered transaction committed nothing")
		}
		// The savepoint's own job: the setting staged after it is gone and
		// the one before it survived. Without the prelude replay the
		// backend would know neither.
		var v string
		if err := conn.QueryRow(ctx, "select current_setting('application_name')").Scan(&v); err != nil || v != "before" {
			t.Fatalf("application_name is %q (%v), want \"before\": the rollback did not scope to the savepoint", v, err)
		}
	})

	// The contrast that keeps this honest. A transaction a failover killed
	// AFTER it ran on a shard has lost that work, and no replay can bring
	// it back. Answering ROLLBACK TO there would tell the client the
	// statements before the savepoint survived, which is the lost-write
	// shape failTxn exists to prevent.
	t.Run("AKilledTransactionIsStillRefused", func(t *testing.T) {
		h := newH(t)
		ctx := context.Background()
		conn := h.connect(t, h.dsn())
		for _, sql := range []string{"begin", "savepoint sp", "insert into items (id) values (7702)"} {
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
		}
		serving := h.fence("fenced")
		if _, err := conn.Exec(ctx, "insert into items (id) values (7703)"); sqlstate(err) != "40001" {
			t.Fatalf("the killed statement reported %v, want 40001", err)
		}
		h.setSnap(serving)
		if _, err := conn.Exec(ctx, "rollback to savepoint sp"); sqlstate(err) != "25P02" {
			t.Fatalf("ROLLBACK TO SAVEPOINT reported %v, want 25P02: the transaction's work is gone, and recovering it would tell the client the rows before the savepoint are still there", err)
		}
	})

	// A savepoint the transaction never set cannot be rolled back to, so
	// the transaction stays failed rather than being silently reopened at
	// its beginning -- and is told which of the two things is wrong, as
	// PostgreSQL does (xact.c answers 3B001 here, not 25P02).
	t.Run("AnUnknownSavepointSaysSo", func(t *testing.T) {
		h := newH(t)
		ctx := context.Background()
		conn := h.connect(t, h.dsn())
		for _, sql := range []string{"begin", "savepoint sp"} {
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
		}
		if _, err := conn.Exec(ctx, "create table t8 (id int primary key)"); err == nil {
			t.Fatal("the failing migration answered success")
		}
		_, err := conn.Exec(ctx, "rollback to savepoint other")
		if sqlstate(err) != "3B001" {
			t.Fatalf("ROLLBACK TO an unset savepoint reported %v, want 3B001: 25P02 sends the client to end a transaction whose only problem is the name", err)
		}
		if !strings.Contains(fmt.Sprint(err), `savepoint "other" does not exist`) {
			t.Errorf("the refusal does not name the savepoint: %v", err)
		}
		if st := conn.PgConn().TxStatus(); st != 'E' {
			t.Fatalf("ReadyForQuery reported %c, want E: the transaction is still failed", st)
		}
	})
}
