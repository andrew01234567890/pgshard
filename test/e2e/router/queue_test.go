//go:build integration

package router

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestDDLQueuesBehindAReshardAndRunsOnce drives the whole chain a statement
// takes while the cluster is busy: the router enqueues it, the operation
// queue holds it behind the reshard, the session's statement_timeout ends
// the wait without ending the migration, the same statement sent again
// attaches to the one already queued rather than adding a second, and when
// the reshard finishes the controller runs it once on every shard.
func TestDDLQueuesBehindAReshardAndRunsOnce(t *testing.T) {
	s := startDDLStack(t)
	ctx := context.Background()
	conn := s.connect(t)
	s.awaitSharded(t, conn)

	// orders is the table the stack declares sharded, so the index below
	// has somewhere to fan out to: an index on an unsharded table would run
	// on the home shard alone and prove nothing about running once per
	// shard.
	if _, err := conn.Exec(ctx, "create table orders (tenant_id int8, id int, note text, primary key (tenant_id, id))"); err != nil {
		t.Fatalf("%v\ncontroller log:\n%s", err, s.controllerLog.String())
	}

	reshard := s.startReshard(t)
	if got := s.queue(t); len(got) != 1 || got[0] != "reshard running" {
		t.Fatalf("queue with only the reshard: %v", got)
	}

	const statement = "create index concurrently orders_note_idx on orders (note)"
	notices, timedOut := s.ddlWithTimeout(t, statement, "2s")
	if timedOut.Detail == "" || !strings.Contains(timedOut.Detail, "continues in the background") {
		t.Fatalf("the timeout must say the migration continues: %+v", timedOut)
	}
	if !strings.Contains(timedOut.Detail, "Running the same statement again waits for it") {
		t.Fatalf("the timeout must say a retry attaches: %q", timedOut.Detail)
	}
	if !waitedInQueue(notices) {
		t.Errorf("the session was not told what it waits for: %v", notices)
	}

	queued := s.queue(t)
	if len(queued) != 2 || queued[0] != "reshard running" || queued[1] != "ddl waiting" {
		t.Fatalf("the statement must be queued behind the reshard: %v", queued)
	}
	if got := s.catalogValue(t, `SELECT waiting_for FROM pgshard.operation_queue WHERE kind = 'ddl'`); !strings.Contains(got, reshard) {
		t.Errorf("the queue must name the reshard as what the statement waits for: %q", got)
	}
	if got := s.catalogValue(t, `SELECT count(*)::text FROM pgshard.migrations WHERE kind = 'CREATE INDEX'`); got != "1" {
		t.Fatalf("migrations after the first attempt: %s", got)
	}

	// The retry. Its own timeout ends it again, but it must not enqueue a
	// second CREATE INDEX: the queue slot the first attempt took is the one
	// it waits on.
	retryNotices, retryTimedOut := s.ddlWithTimeout(t, statement, "2s")
	if retryTimedOut.Code != "57014" {
		t.Fatalf("the retry ended with %s, not a statement timeout", retryTimedOut.Code)
	}
	if !attached(retryNotices) {
		t.Errorf("the retry was not told it attached to the migration already queued: %v", retryNotices)
	}
	if got := s.catalogValue(t, `SELECT count(*)::text FROM pgshard.migrations WHERE kind = 'CREATE INDEX'`); got != "1" {
		t.Fatalf("the retry enqueued the statement again: %s migrations", got)
	}
	if got := s.onShards(t, "select to_regclass('orders_note_idx') is null"); !allTrue(got) {
		t.Fatalf("nothing may run while the reshard holds the queue: %v", got)
	}

	s.finishReshard(t, reshard)

	if r := s.awaitMigration(t, "kind = 'CREATE INDEX'"); r.state != "complete" {
		t.Fatalf("migration after the reshard finished: %+v\ncontroller log:\n%s", r, s.controllerLog.String())
	}
	if got := s.onShards(t, "select count(*) = 1 from pg_indexes where indexname = 'orders_note_idx'"); !allTrue(got) {
		t.Fatalf("the index must exist exactly once per shard: %v", got)
	}
	if got := s.queue(t); len(got) != 0 {
		t.Fatalf("the queue must be empty once everything has finished: %v", got)
	}
}

// waitedInQueue reports whether the session was told what its statement is
// waiting for while it waited.
func waitedInQueue(notices []string) bool {
	for _, n := range notices {
		if strings.Contains(n, "waits for") && strings.Contains(n, "reshard") {
			return true
		}
	}
	return false
}

// attached reports whether the session was told it joined a migration that
// was already queued instead of queueing the statement again.
func attached(notices []string) bool {
	for _, n := range notices {
		if strings.Contains(n, "an identical migration") && strings.Contains(n, "waiting for it instead of queueing the statement again") {
			return true
		}
	}
	return false
}

// ddlWithTimeout runs one DDL statement on its own session under
// statement_timeout, and returns the notices it collected and the error it
// ended with. The statement is expected to time out: that is what a client
// sees when the queue is longer than it is willing to wait.
func (s *ddlStack) ddlWithTimeout(tb testing.TB, statement, timeout string) ([]string, *pgconn.PgError) {
	tb.Helper()
	ctx := context.Background()
	var notices []string
	cfg, err := pgx.ParseConfig(s.dsn(appRole, appPassword, appDatabase))
	if err != nil {
		tb.Fatal(err)
	}
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) { notices = append(notices, n.Message) }
	c, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = c.Close(ctx) }()
	if _, err := c.Exec(ctx, "set statement_timeout = '"+timeout+"'"); err != nil {
		tb.Fatal(err)
	}
	_, err = c.Exec(ctx, statement)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		tb.Fatalf("%s: got %v, want a statement timeout\ncontroller log:\n%s", statement, err, s.controllerLog.String())
	}
	if pgErr.Code != "57014" {
		tb.Fatalf("%s: SQLSTATE %s: %s", statement, pgErr.Code, pgErr.Message)
	}
	return notices, pgErr
}

// startReshard writes the rows a real reshard has written once its copy is
// under way, which is the point from which it holds the queue: the target
// shard set, and the workflow at stage copying. The controller in this
// stack has no target groups to copy to, so the rows stand in for the copy
// while the queue rule reads exactly what it would read from a real one.
// The shard set has to be there: a workflow naming a set that does not
// exist is one the reshard reconciler cancels, which is a different queue
// entry than the one this test is about.
func (s *ddlStack) startReshard(tb testing.TB) string {
	tb.Helper()
	s.catalogValue(tb, `INSERT INTO pgshard.shard_sets (shard_set, generation, state, desired_generation)
		VALUES ('g2', 2, 'provisioning', 2) RETURNING shard_set`)
	return s.catalogValue(tb, `INSERT INTO pgshard.workflows (id, kind, state, spec, status)
		VALUES (gen_random_uuid(), 'reshard', 'running',
			'{"shard_set": "g2", "source_set": "default", "source_shards": 3}'::jsonb,
			'{"stage": "copying", "progress": {"tables_ready": 1, "tables_total": 4}}'::jsonb)
		RETURNING id::text`)
}

// finishReshard ends the workflow the way a completed reshard does, which
// takes it out of the queue and lets what waited behind it start.
func (s *ddlStack) finishReshard(tb testing.TB, id string) {
	tb.Helper()
	s.catalogValue(tb, `UPDATE pgshard.workflows SET state = 'completed', status = status || '{"stage": "complete"}'::jsonb
		WHERE id = $1::uuid RETURNING id::text`, id)
	s.catalogValue(tb, `UPDATE pgshard.shard_sets SET state = 'retired' WHERE shard_set = 'g2' RETURNING shard_set`)
}

// queue reads the operation queue as "<kind> <state>", in queue order.
func (s *ddlStack) queue(tb testing.TB) []string {
	tb.Helper()
	got := s.catalogValue(tb, `SELECT coalesce(string_agg(kind || ' ' || state, ',' ORDER BY position), '') FROM pgshard.operation_queue`)
	if got == "" {
		return nil
	}
	return strings.Split(got, ",")
}

func (s *ddlStack) catalogValue(tb testing.TB, sql string, args ...any) string {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cat, err := pgx.Connect(ctx, s.catalogDSN)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = cat.Close(ctx) }()
	var out string
	if err := cat.QueryRow(ctx, sql, args...).Scan(&out); err != nil {
		tb.Fatalf("%s: %v", sql, err)
	}
	return out
}
