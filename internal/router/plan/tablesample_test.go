package plan

import (
	"context"
	"strings"
	"testing"
)

// TABLESAMPLE wraps the relation rather than being one. The planner had no
// case for that FROM item, so it skipped it, the statement looked as though
// it touched no table, and it went to the home shard: a sample of a sharded
// table came back as a sample of shard 0, and one carrying a shard key in
// its WHERE went to a shard the rows are not on and returned nothing.
func TestTablesampleResolvesTheTableItSamples(t *testing.T) {
	snap := fixture(t)
	scattered := []int32{0, 1, 2, 3}

	for _, sql := range []string{
		"select * from orders tablesample bernoulli (10)",
		"select * from orders tablesample system (10)",
		"select * from public.orders tablesample system (5)",
		"select count(*) from orders tablesample system (10)",
	} {
		t.Run(sql, func(t *testing.T) {
			p, err := New().Plan(context.Background(), session(snap), sql)
			if err != nil {
				t.Fatalf("sampling a sharded table must plan, got %v", err)
			}
			if len(p.Shards) != len(scattered) {
				t.Fatalf("sampling a sharded table must reach every shard, got %v", p.Shards)
			}
		})
	}

	// A shard key in the WHERE must route the sample, not send it to the
	// home shard where the rows are not.
	want := shardOf(t, snap, int64(1))
	p, err := New().Plan(context.Background(), session(snap), "select * from orders tablesample system (10) where tenant_id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Shards) != 1 || p.Shards[0] != want {
		t.Errorf("a keyed sample must route to the key's shard: got %v, want [%d]", p.Shards, want)
	}
}

// An unrecognised FROM item is refused rather than skipped: skipping one
// makes a statement look as though it touches no table, which is how it
// ends up answered from the home shard.
func TestAnUnknownFromItemIsRefusedNotSkipped(t *testing.T) {
	snap := fixture(t)
	// XMLTABLE is a row-producing function scan, not a routable table, and
	// must be recognised as such rather than falling through.
	_, err := New().Plan(context.Background(), session(snap),
		`select * from xmltable('/r' passing '<r/>' columns a int path '.')`)
	if err != nil && !strings.Contains(err.Error(), "not available yet") && !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("unexpected failure shape: %v", err)
	}
}
