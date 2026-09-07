package plan

import (
	"context"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
)

// An aggregate anywhere in a SELECT without GROUP BY makes the whole
// statement an aggregate query returning ONE row. Measured on PostgreSQL 18:
//
//	select 1 from t order by max(amount)   -> 1 row
//
// The router classified only the target list, so this planned as a plain
// scatter and concatenated one row per shard -- four rows where the server
// returns one, with no error anywhere. Most shapes of it are rejected by the
// shard itself (a bare column beside an aggregate is 42803), which is why it
// survived: the ones that are not rejected are the ones that project no
// column at all.
func TestAnAggregateInOrderByIsNotAPlainScatter(t *testing.T) {
	snap := fixture(t)
	for _, sql := range []string{
		"select 1 from orders order by max(amount)",
		"select 'x' from orders order by count(*)",
		"select 1 from orders order by sum(amount) desc",
		// An empty select list is the same shape with nothing at all to
		// concatenate: one zero-width row per shard where the server
		// answers with one.
		"select from orders order by max(amount)",
	} {
		if p, err := New().Plan(context.Background(), session(snap), sql); err == nil {
			t.Errorf("%s: planned as %v over %d shards; the server returns one row and this returns one per shard",
				sql, p.Kind, len(p.Shards))
		}
	}
}

// Grouping on the shard key still makes every group shard-local, so the
// ordering is evaluated where the group lives and nothing is combined.
func TestOrderingOnAnAggregateIsFineWhenEveryGroupIsShardLocal(t *testing.T) {
	snap := fixture(t)
	const sql = "select tenant_id, sum(amount) from orders group by tenant_id order by sum(amount)"
	if _, err := New().Plan(context.Background(), session(snap), sql); err != nil {
		t.Fatalf("a shard-local grouping stopped being planned: %v", err)
	}
	// And an ordinary column in ORDER BY is untouched.
	if _, err := New().Plan(context.Background(), session(snap), "select id from orders order by amount"); err != nil {
		t.Fatalf("ordering on a column stopped being planned: %v", err)
	}
}

// A window function in ORDER BY was never examined at all: each shard
// ranked its own rows from 1, and the merge then ordered by those per-shard
// ranks -- an order that is not the one asked for, with nothing to say so.
// It is a window function wherever it appears, so it gets the blocker that
// name has rather than something about aggregates.
func TestAWindowFunctionInOrderByIsRefusedAsOne(t *testing.T) {
	snap := fixture(t)
	_, err := New().Plan(context.Background(), session(snap), "select id from orders order by rank() over (order by amount)")
	if err == nil {
		t.Fatal("a window function in ORDER BY was planned; the merge would order by per-shard ranks")
	}
	if !strings.Contains(err.Error(), "window functions") {
		t.Errorf("refused as %v, want the window-function blocker", err)
	}
}

// A subquery in ORDER BY is not walked, so its relations are never routed
// and nothing checks that every shard computes the same value for it. One
// over a sharded table does not: each shard orders by its own answer and the
// merge sorts by a column that means something different on each.
func TestASubqueryInOrderByIsRefusedAsOne(t *testing.T) {
	snap := fixture(t)
	for _, sql := range []string{
		"select id from orders order by (select max(amount) from orders)",
		"select id from orders order by (select max(amount) from regions)",
	} {
		_, err := New().Plan(context.Background(), session(snap), sql)
		if err == nil {
			t.Errorf("%s: planned, and nothing routed the subquery or compared what each shard answers", sql)
			continue
		}
		if !strings.Contains(err.Error(), "subqueries") {
			t.Errorf("%s: refused as %v, want the subquery blocker", sql, err)
		}
	}
}

// The same classification the target list gets: a function in ORDER BY is
// the same question about the same value, so an undeclared one is refused
// and a declared scalar is not.
func TestAFunctionInOrderByIsClassifiedToo(t *testing.T) {
	snap := fixture(t)
	const sql = "select id from orders order by st_astext(geom)"
	if _, err := New().Plan(context.Background(), session(snap), sql); err == nil {
		t.Fatal("an undeclared function in ORDER BY was not classified")
	} else if !strings.Contains(err.Error(), "st_astext") {
		t.Fatalf("the refusal does not name it: %v", err)
	}
	snap.ScalarFunctions = map[snapshot.FunctionKey]bool{{Database: fixtureDB, Name: "st_astext"}: true}
	if _, err := New().Plan(context.Background(), session(snap), sql); err != nil {
		t.Errorf("a declared scalar in ORDER BY is still refused: %v", err)
	}
}
