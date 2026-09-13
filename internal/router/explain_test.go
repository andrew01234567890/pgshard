package router

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// pgshardPlan collects the rendered plan of EXPLAIN (PGSHARD).
func pgshardPlan(t *testing.T, conn *pgx.Conn, sql string, mode pgx.QueryExecMode, args ...any) string {
	t.Helper()
	rows, err := conn.Query(context.Background(), sql, append([]any{mode}, args...)...)
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

// PGS-785. A Describe of EXPLAIN (PGSHARD) answered ParameterDescription
// with no parameters, so a driver that prepares and then binds -- pgx in
// its default mode, JDBC -- refused to send the one the statement has and
// failed with "expected 0 arguments, got 1". That left the feature usable
// only from psql, which is the opposite of who needs it: a parameterised
// shard key is exactly the routing a user cannot work out by reading the
// SQL, because the decision does not happen until Bind.
func TestExplainPgshardDescribesTheParametersOfWhatItExplains(t *testing.T) {
	h := newShardedHarness(t)
	conn := h.connect(t, h.dsn())
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeCacheDescribe, pgx.QueryExecModeDescribeExec} {
		out := pgshardPlan(t, conn, "explain (pgshard) select * from orders where tenant_id = $1", mode, int64(1))
		if !strings.Contains(out, "Route [EqualUnique]") {
			t.Errorf("%v: want the keyed route, got:\n%s", mode, out)
		}
		if !strings.Contains(out, "decided at Bind") {
			t.Errorf("%v: want the deferred shard label, got:\n%s", mode, out)
		}
	}
	// Two parameters, only one of them the shard key: the other is typed
	// text, which is what PostgreSQL resolves an undetermined parameter to.
	out := pgshardPlan(t, conn, "explain (pgshard) select * from orders where tenant_id = $1 and note = $2",
		pgx.QueryExecModeCacheStatement, int64(1), "x")
	if !strings.Contains(out, "Route [EqualUnique]") {
		t.Fatalf("want the keyed route, got:\n%s", out)
	}
}

// PGS-786. The pgshard option is boolean, so EXPLAIN (PGSHARD FALSE) asks
// for what PostgreSQL means by EXPLAIN and is routed to a shard like any
// other statement. It used to arrive with the option still on it, and the
// shard answered `unrecognized EXPLAIN option "pgshard"` -- an error about
// a form we define, from a server that has never heard of it.
func TestExplainPgshardFalseReachesTheShardWithoutTheOption(t *testing.T) {
	h := newShardedHarness(t)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	// The fake shard does not answer EXPLAIN; that it was asked at all is
	// the point, and what it was asked is what this checks.
	_, _ = conn.Exec(ctx, "explain (pgshard false, verbose) select * from orders where tenant_id = 1")
	var seen string
	for i := range h.poolers {
		for _, q := range h.poolers[i].ran() {
			if strings.HasPrefix(q, "explain") {
				seen = q
			}
		}
	}
	if seen == "" {
		t.Fatal("EXPLAIN (PGSHARD FALSE) never reached a shard")
	}
	if strings.Contains(seen, "pgshard") {
		t.Errorf("the shard was asked %q, which still carries our own option", seen)
	}
	if !strings.Contains(seen, "verbose") {
		t.Errorf("the shard was asked %q, which lost the option the user did write", seen)
	}
}

// And a repeated option takes the LAST spelling, which is what defGetBoolean
// does. EXPLAIN (PGSHARD TRUE, PGSHARD FALSE) was answered by the router
// where PostgreSQL would have taken the false.
func TestARepeatedPgshardOptionTakesTheLastSpelling(t *testing.T) {
	h := newShardedHarness(t)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	_, _ = conn.Exec(ctx, "explain (pgshard true, pgshard false) select * from orders where tenant_id = 1")
	if !h.ranOn(h.shardOf(t, 1), "explain") {
		t.Fatal("the last spelling was false, so the statement belongs on a shard")
	}
	out := pgshardPlan(t, conn, "explain (pgshard false, pgshard true) select * from orders where tenant_id = 1", pgx.QueryExecModeSimpleProtocol)
	if !strings.Contains(out, "Route [EqualUnique]") {
		t.Fatalf("the last spelling was true, so the router answers it; got:\n%s", out)
	}
}

// PGS-787. Inside a transaction PostgreSQL has already failed, every
// statement is refused with 25P02 until the block ends -- including a
// Describe, which is where postgres.c raises it. The router's two
// self-answered statements never reach a backend, so nothing refused them
// there.
//
// They are not the same case. nextval() over a global sequence hands out a
// value from the router's block and does not take it back, so the gap it
// opens is opened in a transaction that cannot commit, at the one point
// where PostgreSQL would have done nothing at all. EXPLAIN (pgshard) reads
// nothing and changes nothing, and answering it is how a user sees why the
// statement that failed was routed the way it was without first losing the
// transaction.
func TestWhatTheRouterAnswersItselfInAFailedTransaction(t *testing.T) {
	h := newRefHarness(t)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	h.poolers[h.shardOf(t, 1)].script("select body from tickets where tenant_id = 1", script{err: "no such column"})
	if _, err := tx.Exec(ctx, "select body from tickets where tenant_id = 1"); err == nil {
		t.Fatal("the shard was expected to fail this statement")
	}
	if st := conn.PgConn().TxStatus(); st != 'E' {
		t.Fatalf("transaction status %c, want E", st)
	}

	var v int64
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeSimpleProtocol} {
		err = tx.QueryRow(ctx, "select nextval('tickets.id')", mode).Scan(&v)
		if sqlstate(err) != "25P02" {
			t.Errorf("%v: nextval in a failed transaction reported %v, want 25P02: it allocates a value the transaction can never commit", mode, err)
		}
	}
	for i := range h.poolers {
		for _, q := range h.poolers[i].ran() {
			if strings.Contains(q, "nextval") {
				t.Fatalf("the refused nextval reached shard %d: %q", i, q)
			}
		}
	}

	out := pgshardPlan(t, conn, "explain (pgshard) select * from tickets where tenant_id = 1", pgx.QueryExecModeSimpleProtocol)
	if !strings.Contains(out, "Route [") {
		t.Fatalf("EXPLAIN (pgshard) must still answer in a failed transaction, got:\n%s", out)
	}
}
