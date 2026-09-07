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
