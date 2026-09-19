package router

import (
	"context"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestAnSQLExecuteCountsAsRunningOnAShard (PGS-883 item 6): an SQL-level
// EXECUTE is classified SessionLocal because the router does not route it
// -- it goes to the session's pinned backend. But it RUNS there, unlike
// SET, SHOW, DISCARD or DEALLOCATE, and both DDL guards exempted every
// SessionLocal statement.
//
// Measured before the fix, with the PREPARE made OUTSIDE the transaction:
//
//	BEGIN; EXECUTE ins; CREATE TABLE   -> both succeeded
//	BEGIN; CREATE TABLE; EXECUTE ins   -> the EXECUTE succeeded, where a
//	                                      plain SELECT there is refused
//
// The first is refuseDDLInTransaction not seeing that the transaction had
// already run on a shard; the second is refuseShardStatementAfterDDL
// exempting it. Both matter because DDL here is applied on its own and
// cannot commit or roll back with the shard work beside it.
//
// The PREPARE must be outside the transaction: inside one it is itself a
// statement on a shard, so the DDL is refused for that reason and neither
// ordering can be built.
func TestAnSQLExecuteCountsAsRunningOnAShard(t *testing.T) {
	newH := func(t *testing.T) *shardedHarness {
		t.Helper()
		h := newDDLHarness(t, &fakeQueue{})
		app := h.snap.Databases["app"]
		app.DDLTransactions = catalog.DDLTransactionsSequential
		h.snap.Databases["app"] = app
		return h
	}

	t.Run("DDLAfterAnExecute", func(t *testing.T) {
		h := newH(t)
		ctx := context.Background()
		conn := h.connect(t, h.dsn())
		if _, err := conn.Exec(ctx, "prepare ins as insert into items (id) values (6003)"); err != nil {
			t.Fatalf("prepare outside a transaction: %v", err)
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, "execute ins"); err != nil {
			t.Fatalf("the EXECUTE itself must still work: %v", err)
		}
		_, err = tx.Exec(ctx, "create table t3 (id int primary key)")
		if err == nil {
			t.Fatal("DDL was allowed in a transaction whose EXECUTE had already run on a shard: the DDL is applied on its own, so the two cannot commit together")
		}
		if !strings.Contains(err.Error(), "already run a statement on a shard") {
			t.Errorf("refused with the wrong reason: %v", err)
		}
	})

	t.Run("ExecuteAfterDDL", func(t *testing.T) {
		h := newH(t)
		ctx := context.Background()
		conn := h.connect(t, h.dsn())
		if _, err := conn.Exec(ctx, "prepare ins as insert into items (id) values (6004)"); err != nil {
			t.Fatalf("prepare outside a transaction: %v", err)
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, "create table t4 (id int primary key)"); err != nil {
			t.Fatalf("DDL first must work: %v", err)
		}
		_, err = tx.Exec(ctx, "execute ins")
		if err == nil {
			t.Fatal("an EXECUTE ran on a shard after DDL in the same transaction, where a plain statement there is refused")
		}
		if !strings.Contains(err.Error(), "not available after DDL in the same transaction") {
			t.Errorf("refused with the wrong reason: %v", err)
		}
	})

	// The contrast that keeps the fix honest: a session-local statement
	// that does NOT run on a shard must still be allowed after DDL, or
	// this would refuse a client's SET and RESET for no reason.
	t.Run("ASetIsStillAllowedAfterDDL", func(t *testing.T) {
		h := newH(t)
		ctx := context.Background()
		conn := h.connect(t, h.dsn())
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, "create table t5 (id int primary key)"); err != nil {
			t.Fatalf("DDL first must work: %v", err)
		}
		if _, err := tx.Exec(ctx, "set application_name = 'after-ddl'"); err != nil {
			t.Errorf("a SET after DDL must still be allowed; it touches no shard: %v", err)
		}
	})
}
