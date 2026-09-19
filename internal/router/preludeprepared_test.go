package router

import (
	"context"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestDeallocateIsNotReplayedAsPartOfTheTransaction (PGS-959 item 2):
// DEALLOCATE is not transactional in PostgreSQL, and the router already
// drops the statement from its record of SQL-prepared statements, which a
// fresh backend is given before the transaction is replayed. It also went
// into the transaction prelude, so a replay ran it against a backend that
// never had the statement (26000) and failed the very ROLLBACK TO a client
// sends to recover its transaction.
func TestDeallocateIsNotReplayedAsPartOfTheTransaction(t *testing.T) {
	cases := []struct {
		name   string
		before []string
		inTxn  string
		then   string
		want   string
	}{
		{name: "Deallocate", before: []string{"prepare p as select 1"}, inTxn: "deallocate p", then: "execute p", want: "26000"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := &fakeQueue{outcome: func(m catalog.DDLMigration) catalog.DDLMigration {
				m.State, m.Error = catalog.MigrationFailed, "relation already exists"
				return m
			}}
			h := newDDLHarness(t, q)
			app := h.snap.Databases["app"]
			app.DDLTransactions = catalog.DDLTransactionsSequential
			h.snap.Databases["app"] = app
			ctx := context.Background()
			conn := h.connect(t, h.dsn())
			for _, sql := range append(append([]string{}, tc.before...), "begin", "savepoint sp", tc.inTxn) {
				if _, err := conn.Exec(ctx, sql); err != nil {
					t.Fatalf("%s: %v", sql, err)
				}
			}
			if _, err := conn.Exec(ctx, "create table t7 (id int primary key)"); err == nil {
				t.Fatal("the failing migration answered success, so this test never reaches the replay")
			}
			if _, err := conn.Exec(ctx, "rollback to savepoint sp"); err != nil {
				t.Fatalf("recovering the transaction replayed %q a second time: %v", tc.inTxn, err)
			}
			// Not transactional: rolling back past it does not undo it,
			// in PostgreSQL or here.
			_, err := conn.Exec(ctx, tc.then)
			if got := sqlstate(err); got != tc.want {
				t.Fatalf("%s after the recovery: %v (SQLSTATE %q), want %q", tc.then, err, got, tc.want)
			}
		})
	}
}
