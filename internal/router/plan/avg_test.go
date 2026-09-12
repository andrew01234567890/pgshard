package plan

import (
	"context"
	"strings"
	"testing"
)

func mergeSpec(t *testing.T, sql string) *Merge {
	t.Helper()
	p, err := New().Plan(context.Background(), session(fixture(t)), sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	m, err := p.MultiShard()
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return m
}

// An average of averages is not the average: a shard holding one row and a
// shard holding a thousand would count equally. So each shard is asked for
// a sum and a count, and the division happens once, over the totals.
func TestAvgIsSplitIntoASumAndACount(t *testing.T) {
	m := mergeSpec(t, "select avg(qty) from orders")
	if m.ShardSQL != "SELECT pg_catalog.sum(qty), pg_catalog.count(qty) FROM orders" {
		t.Fatalf("shard SQL %q", m.ShardSQL)
	}
	if len(m.Aggregates) != 1 || m.Aggregates[0] != (Agg{Func: AggAvg, Col: 0, Count: 1}) {
		t.Fatalf("aggregates %+v", m.Aggregates)
	}
	// The count is a column the client never asked for, so it is hidden
	// the same way a merge sort's added sort key is.
	if m.Hidden != 1 {
		t.Fatalf("hidden %d, want the count hidden", m.Hidden)
	}
}

// The counts go after the client's own columns, so every column the client
// asked for keeps the position it asked for it in.
func TestAvgKeepsTheClientColumnPositions(t *testing.T) {
	m := mergeSpec(t, "select count(*), avg(amount), sum(qty) from orders")
	if m.ShardSQL != "SELECT count(*), pg_catalog.sum(amount), sum(qty), pg_catalog.count(amount) FROM orders" {
		t.Fatalf("shard SQL %q", m.ShardSQL)
	}
	want := []Agg{{Func: AggCount, Col: 0, Count: -1}, {Func: AggAvg, Col: 1, Count: 3}, {Func: AggSum, Col: 2, Count: -1}}
	if len(m.Aggregates) != len(want) {
		t.Fatalf("aggregates %+v", m.Aggregates)
	}
	for i := range want {
		if m.Aggregates[i] != want[i] {
			t.Fatalf("aggregate %d is %+v, want %+v", i, m.Aggregates[i], want[i])
		}
	}
	if m.Hidden != 1 {
		t.Fatalf("hidden %d", m.Hidden)
	}
}

func TestTwoAveragesEachGetTheirOwnCount(t *testing.T) {
	m := mergeSpec(t, "select avg(qty), avg(amount) from orders")
	if m.ShardSQL != "SELECT pg_catalog.sum(qty), pg_catalog.sum(amount), pg_catalog.count(qty), pg_catalog.count(amount) FROM orders" {
		t.Fatalf("shard SQL %q", m.ShardSQL)
	}
	if m.Aggregates[0].Count == m.Aggregates[1].Count {
		t.Fatalf("both averages read count column %d", m.Aggregates[0].Count)
	}
	if m.Hidden != 2 {
		t.Fatalf("hidden %d, want one count per average", m.Hidden)
	}
}

// The shapes avg() still cannot take, for the same reasons the other
// aggregates cannot.
func TestAvgKeepsTheAggregateRefusals(t *testing.T) {
	for _, c := range []struct{ sql, msg string }{
		{"select avg(distinct qty) from orders", "DISTINCT, FILTER, ORDER BY or OVER"},
		{"select avg(qty) filter (where qty > 1) from orders", "DISTINCT, FILTER, ORDER BY or OVER"},
		{"select avg(qty) + 1 from orders", "must be top-level"},
		{"select avg(sum(qty)) from orders", "nested aggregates"},
	} {
		p, err := New().Plan(context.Background(), session(fixture(t)), c.sql)
		if err == nil {
			_, err = p.MultiShard()
		}
		if err == nil || !strings.Contains(err.Error(), c.msg) {
			t.Errorf("%s: %v, want %q", c.sql, err, c.msg)
		}
	}
}
