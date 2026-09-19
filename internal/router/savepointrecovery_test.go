package router

import (
	"context"
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
	// its beginning.
	//
	// 25P02, not PostgreSQL's 3B001 for an unknown name, because the
	// router's record cannot tell the two apart: a batch that fails
	// partway records NONE of the statements the client already watched
	// succeed, so a savepoint set in such a batch is missing here although
	// it existed. "Does not exist" would then name the wrong problem in
	// exactly the case where the transaction is dead (PGS-959).
	t.Run("AnUnknownSavepointIsStillRefused", func(t *testing.T) {
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
		if _, err := conn.Exec(ctx, "rollback to savepoint other"); sqlstate(err) != "25P02" {
			t.Fatalf("ROLLBACK TO an unset savepoint reported %v, want 25P02", err)
		}
		if st := conn.PgConn().TxStatus(); st != 'E' {
			t.Fatalf("ReadyForQuery reported %c, want E: the transaction is still failed", st)
		}
	})

	// The DDL guard must survive the recovery when the migration was
	// APPLIED and only the reopening failed. runMigration recorded
	// txnRanDDL below that failure, so the flag stayed false on the one
	// path where the DDL is in and the transaction still fails -- and a
	// transaction recovered afterwards would run shard statements and
	// commit them as though they were part of the DDL, which is the
	// atomicity confusion the guard exists to prevent. The router had just
	// told this client "the DDL stays applied; end this transaction".
	t.Run("TheDDLGuardSurvivesARecoveredTransaction", func(t *testing.T) {
		q := &fakeQueue{}
		h := newDDLHarness(t, q)
		app := h.snap.Databases["app"]
		app.DDLTransactions = catalog.DDLTransactionsSequential
		h.snap.Databases["app"] = app
		ctx := context.Background()
		conn := h.connect(t, h.dsn())

		// The migration succeeds, and its poolers go away between the
		// queue and the reopening -- which is the only window that
		// produces "applied, but opening the transaction again failed".
		q.outcome = func(m catalog.DDLMigration) catalog.DDLMigration {
			for i := range h.poolers {
				h.poolers[i].gone.Store(true)
			}
			return m
		}
		for _, sql := range []string{"begin", "savepoint sp"} {
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
		}
		_, err := conn.Exec(ctx, "create table t9 (id int primary key)")
		if err == nil {
			t.Fatal("the DDL answered success although reopening the transaction had to fail")
		}
		if !strings.Contains(err.Error(), "was applied") {
			t.Fatalf("this test needs the applied-but-not-reopened path, got: %v", err)
		}
		for i := range h.poolers {
			h.poolers[i].gone.Store(false)
		}
		if _, err := conn.Exec(ctx, "rollback to savepoint sp"); err != nil {
			t.Fatalf("recovering the transaction: %v", err)
		}
		_, err = conn.Exec(ctx, "insert into items (id) values (7704)")
		if err == nil {
			t.Fatal("a shard statement ran in a transaction recovered after DDL had been applied: it would commit as though it were part of the DDL, which is applied on its own")
		}
		if !strings.Contains(err.Error(), "not available after DDL in the same transaction") {
			t.Errorf("refused with the wrong reason: %v", err)
		}
	})
}
