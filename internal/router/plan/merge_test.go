package plan

import (
	"context"
	"fmt"
	"strings"
	"testing"
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
		parts[i] = fmt.Sprintf("%d:%s:%s", k.Column, dir, nulls)
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
		{sql: "select id from orders order by status", keys: "1:asc:last", hidden: 1,
			shardSQL: "SELECT id, status AS __pgshard_sort_0 FROM orders ORDER BY status"},
		{sql: "select * from orders order by created_at desc, id", keys: "1:desc:first,2:asc:last", hidden: 2,
			shardSQL: "SELECT *, created_at AS __pgshard_sort_0, id AS __pgshard_sort_1 FROM orders ORDER BY created_at DESC, id"},
		{sql: "select id from orders order by status collate \"C\" desc", keys: "1:desc:first:C", hidden: 1,
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
		{"select * from orders o join regions r on r.id = o.region_id", "cross-shard join is not available yet"},
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
