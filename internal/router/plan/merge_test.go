package plan

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
)

func mergeOf(t *testing.T, sql string) *Merge {
	t.Helper()
	pl, err := New().Plan(context.Background(), session(fixture(t)), sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	m, err := pl.MultiShard()
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return m
}

func keys(m *Merge) string {
	parts := make([]string, len(m.OrderBy))
	for i, k := range m.OrderBy {
		dir := "asc"
		if k.Desc {
			dir = "desc"
		}
		nulls := "last"
		if k.NullsFirst {
			nulls = "first"
		}
		pos := fmt.Sprint(k.Column)
		if k.FromHidden {
			pos = "h" + pos
		}
		parts[i] = fmt.Sprintf("%s:%s:%s", pos, dir, nulls)
		if k.CCollation {
			parts[i] += ":C"
		}
	}
	return strings.Join(parts, ",")
}

func TestMergeSpecOrderByResolvesColumns(t *testing.T) {
	cases := []struct {
		sql      string
		keys     string
		hidden   int
		shardSQL string
	}{
		{sql: "select id, status from orders order by id", keys: "0:asc:last"},
		{sql: "select id, status from orders order by 2 desc", keys: "1:desc:first"},
		{sql: "select id as k from orders order by k nulls first", keys: "0:asc:first"},
		{sql: "select id, status from orders order by status desc nulls last, id", keys: "1:desc:last,0:asc:last"},
		{sql: "select id, lower(status) from orders order by lower(status)", keys: "1:asc:last"},
		{sql: "select id, status from orders order by status collate \"C\"", keys: "1:asc:last:C"},
		{sql: "select id, status collate \"C\" as s from orders order by s", keys: "1:asc:last:C"},
		{sql: "select id from orders order by status", keys: "h0:asc:last", hidden: 1,
			shardSQL: "SELECT id, status AS __pgshard_sort_0 FROM orders ORDER BY status"},
		{sql: "select * from orders order by created_at desc, id", keys: "h0:desc:first,h1:asc:last", hidden: 2,
			shardSQL: "SELECT *, created_at AS __pgshard_sort_0, id AS __pgshard_sort_1 FROM orders ORDER BY created_at DESC, id"},
		{sql: "select *, id from orders order by id", keys: "h0:asc:last", hidden: 1,
			shardSQL: "SELECT *, id, id AS __pgshard_sort_0 FROM orders ORDER BY id"},
		{sql: "select o.* from orders o order by status", keys: "h0:asc:last", hidden: 1,
			shardSQL: "SELECT o.*, status AS __pgshard_sort_0 FROM orders o ORDER BY status"},
		{sql: "select id from orders order by status collate \"C\" desc", keys: "h0:desc:first:C", hidden: 1,
			shardSQL: "SELECT id, status COLLATE \"C\" AS __pgshard_sort_0 FROM orders ORDER BY status COLLATE \"C\" DESC"},
	}
	for _, c := range cases {
		m := mergeOf(t, c.sql)
		if got := keys(m); got != c.keys {
			t.Errorf("%s: keys %s, want %s", c.sql, got, c.keys)
		}
		if m.Hidden != c.hidden || m.ShardSQL != c.shardSQL {
			t.Errorf("%s: hidden %d shardSQL %q, want %d %q", c.sql, m.Hidden, m.ShardSQL, c.hidden, c.shardSQL)
		}
		if m.Limit != -1 || m.Offset != -1 || len(m.Aggregates) != 0 {
			t.Errorf("%s: unexpected limit/offset/aggregates %+v", c.sql, m)
		}
	}
}

func TestMergeSpecLimitPushdownArithmetic(t *testing.T) {
	cases := []struct {
		sql           string
		limit, offset int64
		shardSQL      string
	}{
		{sql: "select id from orders limit 10", limit: 10, offset: -1, shardSQL: "SELECT id FROM orders LIMIT 10"},
		{sql: "select id from orders limit 10 offset 3", limit: 10, offset: 3, shardSQL: "SELECT id FROM orders LIMIT 13"},
		{sql: "select id from orders offset 3", limit: -1, offset: 3, shardSQL: "SELECT id FROM orders"},
		{sql: "select id from orders order by id limit 0", limit: 0, offset: -1, shardSQL: "SELECT id FROM orders ORDER BY id LIMIT 0"},
		{sql: "select id from orders limit all offset 0", limit: -1, offset: 0, shardSQL: "SELECT id FROM orders LIMIT ALL"},
		{sql: "select id from orders fetch first 5 rows only", limit: 5, offset: -1, shardSQL: "SELECT id FROM orders LIMIT 5"},
		{sql: "select id from orders limit 9223372036854775807 offset 5", limit: 9223372036854775807, offset: 5, shardSQL: "SELECT id FROM orders LIMIT 9223372036854775807"},
		{sql: "select id from orders limit 3000000000 offset 1", limit: 3000000000, offset: 1, shardSQL: "SELECT id FROM orders LIMIT 3000000001"},
		{sql: "select count(*) from orders limit 5 offset 1", limit: 5, offset: 1, shardSQL: "SELECT count(*) FROM orders"},
	}
	for _, c := range cases {
		m := mergeOf(t, c.sql)
		if m.Limit != c.limit || m.Offset != c.offset || m.ShardSQL != c.shardSQL {
			t.Errorf("%s: limit %d offset %d shardSQL %q, want %d %d %q", c.sql, m.Limit, m.Offset, m.ShardSQL, c.limit, c.offset, c.shardSQL)
		}
	}
}

func TestMergeSpecAggregatesAndShardLocalShapes(t *testing.T) {
	m := mergeOf(t, "select count(*), count(id), sum(amount), min(id), max(created_at) from orders")
	if want := []AggFunc{AggCount, AggCount, AggSum, AggMin, AggMax}; fmt.Sprint(m.Aggregates) != fmt.Sprint(want) {
		t.Fatalf("aggregates %v, want %v", m.Aggregates, want)
	}
	if m.ShardSQL != "" || len(m.OrderBy) != 0 {
		t.Fatalf("aggregate query must go to shards unchanged: %+v", m)
	}
	m = mergeOf(t, "select pg_catalog.sum(id) from orders")
	if fmt.Sprint(m.Aggregates) != fmt.Sprint([]AggFunc{AggSum}) {
		t.Fatalf("schema-qualified aggregate: %v", m.Aggregates)
	}
	// Shard-local shapes are concatenated with no aggregate combination.
	for _, sql := range []string{
		"select tenant_id, count(*) from orders group by tenant_id having count(*) > 1 order by 2 desc limit 5",
		"select distinct tenant_id, status from orders",
		"select distinct on (tenant_id) tenant_id, id from orders order by tenant_id, id desc",
		"select o.tenant_id, max(id) from orders o group by 1",
	} {
		m := mergeOf(t, sql)
		if len(m.Aggregates) != 0 {
			t.Errorf("%s: shard-local query must not combine aggregates: %v", sql, m.Aggregates)
		}
	}
	m = mergeOf(t, "select tenant_id, count(*) from orders group by tenant_id having count(*) > 1 order by 2 desc limit 5")
	if keys(m) != "1:desc:first" || m.Limit != 5 || m.ShardSQL != "SELECT tenant_id, count(*) FROM orders GROUP BY tenant_id HAVING count(*) > 1 ORDER BY 2 DESC LIMIT 5" {
		t.Fatalf("grouped merge: %+v", m)
	}
}

func TestMergeSpecRefusals(t *testing.T) {
	cases := []struct{ sql, msg string }{
		{"select avg(id) from orders", "multi-shard avg() is not available yet"},
		{"select count(*) + 1 from orders", "multi-shard aggregates must be top-level"},
		{"select id, count(*) from orders", "multi-shard aggregates must be top-level"},
		{"select sum(id) filter (where id > 1) from orders", "multi-shard aggregates with DISTINCT, FILTER, ORDER BY or OVER"},
		{"select string_agg(status, ',') from orders", "multi-shard string_agg() is not available yet"},
		{"select id from orders group by id", "multi-shard GROUP BY without the shard key"},
		{"select tenant_id from orders group by grouping sets ((tenant_id), ())", "multi-shard GROUP BY without the shard key"},
		{"select distinct id from orders", "multi-shard DISTINCT without the shard key"},
		{"select distinct on (id) id, tenant_id from orders", "multi-shard DISTINCT without the shard key"},
		{"select distinct tenant_id, id from orders order by status", "multi-shard SELECT DISTINCT with ORDER BY expressions outside the select list"},
		{"select id from orders having count(*) > 1", "multi-shard HAVING without GROUP BY on the shard key"},
		{"select id from orders limit $1", "multi-shard LIMIT must be an integer constant"},
		{"select id from orders offset 1 + 1", "multi-shard OFFSET must be an integer constant"},
		{"select id from orders order by id using <", "multi-shard ORDER BY ... USING is not available yet"},
		{"select id from orders order by id fetch first 2 rows with ties", "multi-shard SELECT with FETCH ... WITH TIES"},
		{"select id from orders order by 3", "ORDER BY position 3 is not in select list"},
		{"select id from orders limit -1", "LIMIT must not be negative"},
		{"select row_number() over (order by id) from orders", "multi-shard SELECT with window functions"},
		{"select * from orders for share", "multi-shard SELECT with FOR UPDATE/SHARE"},
		{"select * into tmp from orders", "multi-shard SELECT with SELECT INTO"},
		{"select * from orders union all select * from orders", "cross-shard join is not available yet"},
		{"select * from orders where tenant_id in (select 1)", "multi-shard SELECT with subqueries"},
		// Still cross-shard, for the three reasons a colocated join is not:
		// a sharded pair that is not joined on the key, an unsharded table
		// (home shard only), and a reference table PRESERVED by an outer
		// join, whose unmatched rows every shard would emit.
		{"select * from orders o join order_lines l on o.id = l.order_id", "cross-shard join is not available yet"},
		{"select * from orders o join items i on o.item = i.id", "cross-shard join is not available yet"},
		{"select * from regions r left join orders o on o.region = r.id", "cross-shard join is not available yet"},
		{"select * from orders o full join regions r on o.region = r.id", "cross-shard join is not available yet"},
		{"explain select * from orders", "only a plain SELECT can run on multiple shards"},
		{"declare c cursor for select * from orders", "only a plain SELECT can run on multiple shards"},
	}
	for _, c := range cases {
		pl, err := New().Plan(context.Background(), session(fixture(t)), c.sql)
		if err == nil {
			_, err = pl.MultiShard()
		}
		if err == nil || !strings.Contains(err.Error(), c.msg) {
			t.Errorf("%s: got %v, want %q", c.sql, err, c.msg)
		}
	}
}

func TestMergeSpecOnMultiShardInPlans(t *testing.T) {
	snap := fixture(t)
	pl, err := New().Plan(context.Background(), session(snap), "select id from orders where tenant_id in (1, 2, 3, 4, 5, 6, 7, 8) order by id limit 2")
	if err != nil {
		t.Fatal(err)
	}
	if pl.Kind != In || len(pl.Shards) < 2 {
		t.Fatalf("plan %+v", pl)
	}
	m, err := pl.MultiShard()
	if err != nil {
		t.Fatal(err)
	}
	if keys(m) != "0:asc:last" || m.Limit != 2 || !strings.HasSuffix(m.ShardSQL, "ORDER BY id LIMIT 2") {
		t.Fatalf("In-plan merge %+v", m)
	}
	pl, err = New().Plan(context.Background(), session(snap), "select avg(id) from orders where tenant_id in (1, 2, 3, 4, 5, 6, 7, 8)")
	if err != nil {
		t.Fatalf("an unmergeable In plan is still a plan (it may resolve to one shard): %v", err)
	}
	if _, err := pl.MultiShard(); err == nil || !strings.Contains(err.Error(), "avg()") {
		t.Fatalf("In-plan refusal: %v", err)
	}
	pl, err = New().Plan(context.Background(), session(snap), "select id from orders where tenant_id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pl.MultiShard(); err != nil {
		t.Fatalf("single-shard reads carry a merge spec too: %v", err)
	}
}

// TestMergeSortKeysIndexTheShardRowNotTheSelectList: sort keys were
// positions in the parsed select list, which a star makes unrelated to the
// row the shard returns -- SELECT * ... ORDER BY merged on whichever column
// happened to sit at that index.
func TestMergeSortKeysIndexTheShardRowNotTheSelectList(t *testing.T) {
	// width is what the shard's RowDescription reports, with orders
	// standing at four columns; the planner never sees it.
	cases := []struct {
		sql   string
		width int
		want  []int
	}{
		{sql: "select * from orders order by created_at desc, id", width: 6, want: []int{4, 5}},
		{sql: "select *, id from orders order by id", width: 6, want: []int{5}},
		{sql: "select id, status from orders order by status", width: 2, want: []int{1}},
		{sql: "select id from orders order by status", width: 2, want: []int{1}},
	}
	for _, c := range cases {
		m := mergeOf(t, c.sql)
		width := c.width
		for i, k := range m.OrderBy {
			got := k.Index(width, m.Hidden)
			if got != c.want[i] {
				t.Errorf("%s: key %d lands on column %d of a %d column row, want %d", c.sql, i, got, width, c.want[i])
			}
			if got < 0 || got >= width {
				t.Errorf("%s: key %d is outside the row", c.sql, i)
			}
		}
	}
}

func TestMergeRefusesOrderByPositionWithAStar(t *testing.T) {
	pl, err := New().Plan(context.Background(), session(fixture(t)), "select * from orders order by 2")
	checkRefusal(t, pl, err, "multi-shard ORDER BY by position with * in the select list is not available yet", "0A000")
}

// TestColocatedJoinsMergeAcrossShards is PGS-614. A join whose sharded
// relations are joined on their shard key matches only rows that live
// together, so each shard can run the whole join and the merge above it is
// the one a single table needs.
func TestColocatedJoinsMergeAcrossShards(t *testing.T) {
	for _, c := range []struct{ sql, shardSQL string }{
		{"select * from orders o join order_lines l on o.tenant_id = l.tenant_id", ""},
		{"select * from orders o join order_lines l using (tenant_id)", ""},
		{"select * from orders o natural join order_lines l", ""},
		{"select * from orders o left join order_lines l on o.tenant_id = l.tenant_id", ""},
		{"select * from orders o join regions r on o.region = r.id", ""},
		{"select * from orders o left join regions r on o.region = r.id", ""},
		// The merge is a real one, not a pass-through: the join runs on
		// every shard and the router orders and truncates what comes back.
		{"select o.id from orders o join order_lines l on o.tenant_id = l.tenant_id order by o.id limit 2",
			"SELECT o.id FROM orders o JOIN order_lines l ON o.tenant_id = l.tenant_id ORDER BY o.id LIMIT 2"},
	} {
		pl, err := New().Plan(context.Background(), session(fixture(t)), c.sql)
		if err != nil {
			t.Errorf("%s: %v", c.sql, err)
			continue
		}
		if pl.Kind != Scatter {
			t.Errorf("%s: kind %v, want Scatter", c.sql, pl.Kind)
			continue
		}
		m, err := pl.MultiShard()
		if err != nil {
			t.Errorf("%s: merge: %v", c.sql, err)
			continue
		}
		if c.shardSQL != "" && m.ShardSQL != c.shardSQL {
			t.Errorf("%s: shard SQL %q", c.sql, m.ShardSQL)
		}
	}
}

// A grouped colocated join still needs the shard key in its GROUP BY, and
// the key it is checked against is the one the join is on.
func TestColocatedJoinGroupByStillNeedsTheShardKey(t *testing.T) {
	pl, err := New().Plan(context.Background(), session(fixture(t)),
		"select o.tenant_id, count(*) from orders o join order_lines l on o.tenant_id = l.tenant_id group by o.tenant_id")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pl.MultiShard(); err != nil {
		t.Fatalf("grouping by the shard key the join is on: %v", err)
	}
	// A scatter reports its merge refusal from Plan, not from MultiShard:
	// there is no shard count that could make it plannable later.
	_, err = New().Plan(context.Background(), session(fixture(t)),
		"select o.status, count(*) from orders o join order_lines l on o.tenant_id = l.tenant_id group by o.status")
	if err == nil || !strings.Contains(err.Error(), "GROUP BY without the shard key") {
		t.Fatalf("grouping by a non-key column: %v", err)
	}
}

// A sharded table whose shard-key TYPE has not been verified yet still
// scatters. The type is compared between two relations of a join, where a
// mismatch would route the sides to different shards; one relation has
// nothing to compare against, and requiring a verdict there refused every
// ordinary multi-shard read of a table the controller had not inspected --
// which is every table for as long as a freshly started cluster takes to
// inspect it, reported as "cross-shard join is not available yet".
func TestAnUncheckedShardKeyStillScatters(t *testing.T) {
	snap := fixture(t)
	k := snapshot.TableKey{Database: fixtureDB, SchemaName: "public", TableName: "orders"}
	pl := snap.Tables[k]
	pl.ShardKeyChecked = false
	snap.Tables[k] = pl

	for _, sql := range []string{"select id from orders", "select count(*) from orders", "select id from orders order by id limit 3"} {
		p, err := New().Plan(context.Background(), session(snap), sql)
		if err != nil {
			t.Errorf("%s: %v", sql, err)
			continue
		}
		if p.Kind != Scatter {
			t.Errorf("%s: kind %v, want Scatter", sql, p.Kind)
		}
	}
	// Two of them is the case the verdict is for, and without one the join
	// is refused rather than routed to the wrong shard -- naming the reason,
	// because "join sharded tables on equal shard keys" is advice this
	// statement has already taken.
	_, err := New().Plan(context.Background(), session(snap),
		"select * from orders o join order_lines l on o.tenant_id = l.tenant_id")
	if err == nil || !strings.Contains(err.Error(), `shard key type of "orders" has been inspected`) {
		t.Errorf("a join on an unverified key must say why it is refused: %v", err)
	}
	// A join that is not colocated at all keeps the plain message.
	if _, err := New().Plan(context.Background(), session(snap),
		"select * from orders o join order_lines l on o.id = l.order_id"); err == nil ||
		!strings.Contains(err.Error(), "cross-shard join is not available yet") {
		t.Errorf("a join on a non-key column: %v", err)
	}
}
