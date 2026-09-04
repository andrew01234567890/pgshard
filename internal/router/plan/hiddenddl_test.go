package plan

import (
	"context"
	"strings"
	"testing"
)

// TestDDLNamingAMigrationWorkingColumnIsRefused closes the half of the
// hidden-column contract that DDL walked straight through.
//
// A statement that SELECTs the working column has always been refused with
// 42703, the same answer PostgreSQL gives for a column that is not there.
// DDL was not: ALTER TABLE ... DROP COLUMN, RENAME COLUMN and CREATE INDEX
// on it were all accepted, because DDL carries a column name as a plain
// string on the command rather than as a ColumnRef.
//
// That is where PGS-590 stops being cosmetic. Introspection still lists the
// working column, so a migration tool that diffs the schema and writes SQL
// from what it read proposes exactly these statements -- and dropping the
// column mid-rewrite destroys the backfill and takes the dual-write
// triggers with it.
func TestDDLNamingAMigrationWorkingColumnIsRefused(t *testing.T) {
	s := rewriteFixture(t, "tenant_id", "id", "amount")
	const hidden = "_pgshard_amount_deadbeef"
	for _, sql := range []string{
		"alter table orders drop column " + hidden,
		"alter table orders alter column " + hidden + " set not null",
		"alter table orders rename column " + hidden + " to amount2",
		"alter table orders rename column amount to " + hidden,
		"create index on orders (" + hidden + ")",
		"select " + hidden + " from orders",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := New().Plan(context.Background(), session(s), sql)
			if err == nil {
				t.Fatal("a statement naming a migration working column must be refused")
			}
			// The same answer as for a column that does not exist: a client
			// that was never meant to see it should be told it is not there,
			// not that it is special.
			if !strings.Contains(err.Error(), "42703") || !strings.Contains(err.Error(), hidden) {
				t.Fatalf("refusal should be 42703 naming the column, got %v", err)
			}
		})
	}

	// The contract is about the working column, not about DDL: ordinary
	// column DDL on the same table still plans.
	for _, sql := range []string{
		"alter table orders drop column amount",
		"alter table orders rename column amount to total",
		"create index on orders (amount)",
	} {
		t.Run("allowed: "+sql, func(t *testing.T) {
			if _, err := New().Plan(context.Background(), session(s), sql); err != nil {
				t.Fatalf("ordinary column DDL must still plan: %v", err)
			}
		})
	}
}
