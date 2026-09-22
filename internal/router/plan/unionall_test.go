package plan

import (
	"context"
	"strings"
	"testing"
)

func planOf(t *testing.T, sql string) (Plan, error) {
	t.Helper()
	return New().Plan(context.Background(), session(fixture(t)), sql)
}

// Each row of each arm lives on one shard, so running the whole statement
// on every shard and concatenating returns each row exactly once.
func TestAUnionAllOfShardedTablesRunsOnEveryShard(t *testing.T) {
	for _, sql := range []string{
		"select id from orders union all select id from orders",
		"select id from orders where qty > 1 union all select id from order_lines",
		"select id from orders union all select id from orders union all select id from order_lines",
	} {
		p, err := planOf(t, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if p.Kind != Scatter || len(p.Shards) < 2 {
			t.Fatalf("%s: kind %v shards %v, want a scatter", sql, p.Kind, p.Shards)
		}
		m, err := p.MultiShard()
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if m.ShardSQL != "" || len(m.OrderBy) != 0 || m.Limit != -1 || len(m.Aggregates) != 0 {
			t.Fatalf("%s: merge %+v, want the text sent as it is and the streams concatenated", sql, m)
		}
	}
}

// The ORDER BY and LIMIT belong to the whole statement: the router merges
// the shards' sorted streams and every shard is asked for no more rows than
// the merge could use.
func TestAUnionAllMergesItsOrderByAndLimit(t *testing.T) {
	m := mergeSpec(t, "select id, qty from orders union all select id, qty from order_lines order by qty desc, 1 limit 5 offset 2")
	if len(m.OrderBy) != 2 || m.OrderBy[0] != (SortKey{Column: 1, Desc: true, NullsFirst: true}) || m.OrderBy[1] != (SortKey{Column: 0}) {
		t.Fatalf("order by %+v", m.OrderBy)
	}
	if m.Limit != 5 || m.Offset != 2 || m.Hidden != 0 {
		t.Fatalf("limit %d offset %d hidden %d", m.Limit, m.Offset, m.Hidden)
	}
	if !strings.HasSuffix(m.ShardSQL, "ORDER BY qty DESC, 1 LIMIT 7") {
		t.Fatalf("shard SQL %q, want the LIMIT pushed down as limit+offset", m.ShardSQL)
	}
}

func TestAUnionAllOrderByAnExpressionIsPostgreSQLsRefusal(t *testing.T) {
	_, err := planOf(t, "select id from orders union all select id from order_lines order by id + 1")
	if err == nil || !strings.Contains(err.Error(), "invalid UNION/INTERSECT/EXCEPT ORDER BY clause") {
		t.Fatalf("err = %v", err)
	}
}

// What a shard cannot answer for its own rows alone stays refused.
func TestSetOperationsThatCombineRowsStayRefused(t *testing.T) {
	for sql, want := range map[string]string{
		"select id from orders union select id from order_lines":                                                           "set operations",
		"select id from orders intersect select id from order_lines":                                                       "set operations",
		"select id from orders except all select id from order_lines":                                                      "set operations",
		"select count(*) from orders union all select count(*) from order_lines":                                           "an aggregate in an arm",
		"select distinct id from orders union all select id from order_lines":                                              "DISTINCT in an arm",
		"select id from orders union all (select id from order_lines limit 1)":                                             "ORDER BY or LIMIT in an arm",
		"select tenant_id from orders union all select tenant_id from orders group by 1":                                   "GROUP BY in an arm",
		"select id from orders union all select id from orders o join order_lines l using (tenant_id)":                     "",
		"select id from orders union all select id from items":                                                             "",
		"select id from orders union all select row_number() over () from orders":                                          "window functions",
		"select id from orders union all select id from orders where id in (select id from order_lines)":                   "subquer",
		"with x as (select 1) select id from orders union all select id from orders":                                       "common table expressions",
		"(select id from orders union all select id from orders order by id limit 3) union all select id from order_lines": "parenthesised set operation",
		"select id from orders union all (select id from orders union all select id from order_lines order by id limit 3)": "parenthesised set operation",
		"select id from orders union all (select id from orders union all select id from order_lines offset 1)":            "parenthesised set operation",
		"select * from orders union all select * from order_lines order by id":                                             "with * in the select list",
		"select id from orders union all select id from orders for update":                                                 "FOR UPDATE",
	} {
		p, err := planOf(t, sql)
		if err == nil {
			if _, err = p.MultiShard(); err == nil && p.Kind == Scatter {
				t.Fatalf("%s: planned as a scatter", sql)
			}
		}
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: err = %v, want it to mention %q", sql, err, want)
		}
	}
}

// One key value for every arm still routes the whole statement to its shard.
func TestAUnionAllOnOneKeyStaysOnOneShard(t *testing.T) {
	p, err := planOf(t, "select id from orders where tenant_id = 1 union all select id from order_lines where tenant_id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Shards) != 1 {
		t.Fatalf("shards %v, want one", p.Shards)
	}
}
