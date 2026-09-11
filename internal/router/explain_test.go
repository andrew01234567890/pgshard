package router

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// pgshardPlan collects the rendered plan of EXPLAIN (PGSHARD).
func pgshardPlan(t *testing.T, conn *pgx.Conn, sql string, mode pgx.QueryExecMode) string {
	t.Helper()
	rows, err := conn.Query(context.Background(), sql, mode)
	if err != nil {
		t.Fatalf("%v: %v", mode, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%v: %v", mode, err)
	}
	if len(out) == 0 {
		t.Fatalf("%v: no plan rows", mode)
	}
	return strings.Join(out, "\n")
}

// The router answers EXPLAIN (PGSHARD) itself, in both protocols: a shard
// knows nothing about the routing decision, so sending it there would come
// back with that shard's PostgreSQL plan and answer a different question.
func TestExplainPgshardIsAnsweredByTheRouter(t *testing.T) {
	h := newShardedHarness(t)
	conn := h.connect(t, h.dsn())
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeCacheStatement, pgx.QueryExecModeCacheDescribe} {
		keyed := pgshardPlan(t, conn, "explain (pgshard) select * from orders where tenant_id = 1", mode)
		if !strings.Contains(keyed, "Route [EqualUnique]") {
			t.Errorf("%v: want a single-shard route, got:\n%s", mode, keyed)
		}
		scatter := pgshardPlan(t, conn, "explain (pgshard) select * from orders", mode)
		if !strings.Contains(scatter, "Route [Scatter]") {
			t.Errorf("%v: want a scatter, got:\n%s", mode, scatter)
		}
	}
	for i := range h.poolers {
		for _, q := range h.poolers[i].ran() {
			if strings.Contains(strings.ToLower(q), "explain") {
				t.Fatalf("EXPLAIN (pgshard) reached shard %d: %q", i, q)
			}
		}
	}
}

// The fanout ceiling refuses the statements a user most wants explained, so
// EXPLAIN has to be reachable from under it -- otherwise the only way to
// learn why a query was refused is to raise the ceiling and run it.
func TestExplainPgshardWorksUnderTheFanoutCeiling(t *testing.T) {
	h := newShardedHarness(t)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	if _, err := conn.Exec(ctx, "set pgshard.fanout = 'single'"); err != nil {
		t.Fatal(err)
	}
	_, err := conn.Exec(ctx, "select * from orders")
	if err == nil || !strings.Contains(err.Error(), "exceeds pgshard.fanout") {
		t.Fatalf("the ceiling must still refuse the statement itself, got %v", err)
	}
	out := pgshardPlan(t, conn, "explain (pgshard) select * from orders", pgx.QueryExecModeSimpleProtocol)
	if !strings.Contains(out, "Route [Scatter]") {
		t.Fatalf("want the scatter explained under the ceiling, got:\n%s", out)
	}
}
