//go:build integration

package router

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/andrew01234567890/pgshard/internal/router"
)

// ddlStack is a sharded stack with a third shard: three shards, three
// poolers, a router and a controller whose applier drives DDL.
type ddlStack struct {
	*shardedStack
	shard2DSN string
}

func startDDLStack(tb testing.TB) *ddlStack {
	tb.Helper()
	s := &ddlStack{shardedStack: &shardedStack{stack: startStackWith(tb, nil)}}
	poolerBin, _ := buildBinaries(tb)
	ctx := context.Background()
	for id := 1; id <= 2; id++ {
		addr, dsn := startPostgres(tb, fmt.Sprintf("shard%d", id))
		pooler := fmt.Sprintf("127.0.0.1:%d", freePort(tb))
		err := router.DevBootstrap{CatalogDSN: s.catalogDSN, ShardDSN: dsn, ShardID: int32(id), Database: appDatabase, Role: appRole,
			Password: appPassword, PoolerEndpoint: pooler, Epoch: 1}.Run(ctx)
		if err != nil {
			tb.Fatalf("bootstrap shard %d: %v", id, err)
		}
		host, port, _ := net.SplitHostPort(addr)
		startProcess(tb, &logBuffer{}, "listening on", poolerBin, "run", "--insecure-dev", "--listen", pooler,
			"--pg-host", host, "--pg-port", port, "--pg-database", appDatabase,
			"--catalog-dsn", s.catalogDSN, "--shard-set", router.DefaultShardSet, "--shard-id", fmt.Sprint(id), "--drain-timeout", "5s")
		s.shardDSNs[id] = dsn
		if id == 1 {
			s.shard1DSN = dsn
		} else {
			s.shard2DSN = dsn
		}
	}
	s.startController(tb)
	cat, err := pgx.Connect(ctx, s.catalogDSN)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = cat.Close(ctx) }()
	for _, sql := range []string{
		`INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('default', 0, '[,-3000000000000000000)'), ('default', 1, '[-3000000000000000000,3000000000000000000)'), ('default', 2, '[3000000000000000000,)')`,
		`INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('app', 'public', 'orders', 'sharded', 'tenant_id')`,
		`INSERT INTO pgshard.table_status (database, schema_name, table_name, effective_placement, effective_shard_key) VALUES ('app', 'public', 'orders', 'sharded', 'tenant_id')`,
		`INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', 'regions', 'reference')`,
		// Stands in for the controller's inspection pass, which this stack
		// does not run; regions evaluates nothing per shard.
		`INSERT INTO pgshard.table_status (database, schema_name, table_name, effective_placement, reference_checked_generation) VALUES ('app', 'public', 'regions', 'reference', 0)`,
	} {
		if _, err := cat.Exec(ctx, sql); err != nil {
			tb.Fatalf("%s: %v", sql, err)
		}
	}
	return s
}

func (s *ddlStack) shardDSN(id int) string {
	return strings.Replace(s.shardDSNs[id], "/postgres?", "/"+appDatabase+"?", 1)
}

func (s *ddlStack) shardConn(tb testing.TB, id int) *pgx.Conn {
	tb.Helper()
	conn, err := pgx.Connect(context.Background(), s.shardDSN(id))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// onShards evaluates a boolean SQL expression on every shard.
func (s *ddlStack) onShards(tb testing.TB, sql string, args ...any) []bool {
	tb.Helper()
	out := make([]bool, 3)
	for id := range out {
		conn := s.shardConn(tb, id)
		if err := conn.QueryRow(context.Background(), sql, args...).Scan(&out[id]); err != nil {
			tb.Fatalf("shard %d: %s: %v", id, sql, err)
		}
		_ = conn.Close(context.Background())
	}
	return out
}

func allTrue(b []bool) bool {
	for _, x := range b {
		if !x {
			return false
		}
	}
	return true
}

type migrationRow struct {
	state    string
	perShard string
	err      string
}

func (s *ddlStack) migration(tb testing.TB, where string, args ...any) migrationRow {
	tb.Helper()
	cat, err := pgx.Connect(context.Background(), s.catalogDSN)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = cat.Close(context.Background()) }()
	var r migrationRow
	if err := cat.QueryRow(context.Background(), `SELECT state, per_shard::text, coalesce(error, '') FROM pgshard.migrations WHERE `+where+` ORDER BY created_at DESC LIMIT 1`, args...).Scan(&r.state, &r.perShard, &r.err); err != nil {
		tb.Fatalf("migration row: %v", err)
	}
	return r
}

func (s *ddlStack) awaitMigration(tb testing.TB, where string, args ...any) migrationRow {
	tb.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		r := s.migration(tb, where, args...)
		if r.state == "complete" || r.state == "failed" || time.Now().After(deadline) {
			return r
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestRouterDDLMigrations(t *testing.T) {
	s := startDDLStack(t)
	ctx := context.Background()
	conn := s.connect(t)
	s.awaitSharded(t, conn)
	// The restarted controller must outlive the subtest that restarts it.
	top := t

	t.Run("create_sharded_table_everywhere", func(t *testing.T) {
		tag, err := conn.Exec(ctx, "create table orders (tenant_id int8, id int, primary key (tenant_id, id))")
		if err != nil {
			t.Fatalf("%v\ncontroller log:\n%s", err, s.controllerLog.String())
		}
		if tag.String() != "CREATE TABLE" {
			t.Fatalf("tag %q", tag)
		}
		if got := s.onShards(t, "select to_regclass('public.orders') is not null"); !allTrue(got) {
			t.Fatalf("orders exists per shard: %v", got)
		}
		if got := s.onShards(t, "select tableowner = $1 from pg_tables where tablename = 'orders'", appRole); !allTrue(got) {
			t.Fatalf("orders owned by the client role per shard: %v", got)
		}
		if _, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values (1, 1)"); err != nil {
			t.Fatalf("the client owns the table it created: %v", err)
		}
		r := s.migration(t, "kind = 'CREATE TABLE'")
		if r.state != "complete" || strings.Count(r.perShard, `"applied"`) != 3 {
			t.Fatalf("migration row %+v", r)
		}
	})

	t.Run("client_function_cannot_escalate_through_the_applier", func(t *testing.T) {
		for id := 0; id < 3; id++ {
			shard := s.shardConn(t, id)
			if _, err := shard.Exec(ctx, `create or replace function public.escalate(int) returns bool language plpgsql as $$
				begin reset role; execute format('alter role %I superuser', '`+appRole+`'); return true; end $$`); err != nil {
				t.Fatal(err)
			}
		}
		for tenant := 2; tenant <= 12; tenant++ {
			if _, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", tenant); err != nil {
				t.Fatal(err)
			}
		}
		_, err := conn.Exec(ctx, "alter table orders add constraint orders_escalate check (public.escalate(id))")
		if err == nil {
			t.Fatalf("a CHECK that resets the role and grants superuser was accepted\nmigration: %+v\ncontroller log:\n%s",
				s.migration(t, "statement like '%orders_escalate%'"), s.controllerLog.String())
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Fatalf("expected 42501, got %v\nmigration: %+v", err, s.migration(t, "statement like '%orders_escalate%'"))
		}
		if got := s.onShards(t, "select not rolsuper from pg_roles where rolname = $1", appRole); !allTrue(got) {
			t.Fatalf("client role stayed a plain role per shard: %v", got)
		}
		if got := s.onShards(t, "select not rolsuper and not rolbypassrls and not rolcreaterole and not rolcreatedb from pg_roles where rolname = 'pgshard_ddl'"); !allTrue(got) {
			t.Fatalf("pgshard_ddl is a plain login per shard: %v", got)
		}
		if got := s.onShards(t, "select count(*) = 0 from pg_auth_members where member = 'pgshard_ddl'::regrole"); !allTrue(got) {
			t.Fatalf("pgshard_ddl keeps no membership after the migration: %v", got)
		}

		for id := 0; id < 3; id++ {
			shard := s.shardConn(t, id)
			for _, sql := range []string{
				"do $$ begin if not exists (select 1 from pg_roles where rolname = 'other_tenant') then create role other_tenant; end if; end $$",
				`create or replace function public.hop(int) returns bool language plpgsql as $$
				begin reset role; set role other_tenant; return true; end $$`,
			} {
				if _, err := shard.Exec(ctx, sql); err != nil {
					t.Fatal(err)
				}
			}
		}
		_, err = conn.Exec(ctx, "alter table orders add constraint orders_hop check (public.hop(id))")
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Fatalf("a CHECK that resets the role and hops to another tenant: expected 42501, got %v\nmigration: %+v", err, s.migration(t, "statement like '%orders_hop%'"))
		}
		if got := s.onShards(t, "select count(*) = 0 from pg_auth_members where member = 'pgshard_ddl'::regrole"); !allTrue(got) {
			t.Fatalf("pgshard_ddl keeps no membership after a failed migration: %v", got)
		}
		for tenant := 2; tenant <= 12; tenant++ {
			if _, err := conn.Exec(ctx, "delete from orders where tenant_id = $1", tenant); err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("create_index_concurrently_valid_everywhere", func(t *testing.T) {
		if _, err := conn.Exec(ctx, "create index concurrently orders_id on orders (id)"); err != nil {
			t.Fatal(err)
		}
		if got := s.onShards(t, "select indisvalid from pg_index where indexrelid = 'orders_id'::regclass"); !allTrue(got) {
			t.Fatalf("index valid per shard: %v", got)
		}
	})

	t.Run("lock_holder_delays_but_does_not_block_readers", func(t *testing.T) {
		holder := s.shardConn(t, 1)
		held := make(chan error, 1)
		go func() {
			_, err := holder.Exec(ctx, "begin; select count(*) from orders; select pg_sleep(7); commit")
			held <- err
		}()
		time.Sleep(500 * time.Millisecond)
		start := time.Now()
		done := make(chan error, 1)
		go func() {
			_, err := conn.Exec(ctx, "alter table orders add column note text")
			done <- err
		}()
		time.Sleep(2500 * time.Millisecond)
		reader := s.shardConn(t, 1)
		rstart := time.Now()
		var n int
		if err := reader.QueryRow(ctx, "select count(*) from orders").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if d := time.Since(rstart); d > 3*time.Second {
			t.Fatalf("a new reader waited %s behind the retrying DDL", d)
		}
		if err := <-done; err != nil {
			t.Fatalf("alter: %v\ncontroller log:\n%s", err, s.controllerLog.String())
		}
		if d := time.Since(start); d < 5*time.Second {
			t.Fatalf("alter finished after %s although the lock was held for 7s", d)
		}
		if err := <-held; err != nil {
			t.Fatal(err)
		}
		if got := s.onShards(t, "select exists (select 1 from information_schema.columns where table_name = 'orders' and column_name = 'note')"); !allTrue(got) {
			t.Fatalf("column per shard: %v", got)
		}
		r := s.migration(t, "kind = 'ALTER TABLE'")
		if !strings.Contains(r.perShard, `"attempts": 2`) && !strings.Contains(r.perShard, `"attempts": 3`) && !strings.Contains(r.perShard, `"attempts": 4`) {
			t.Fatalf("shard 1 was not retried: %s", r.perShard)
		}
	})

	t.Run("controller_killed_mid_fanout_converges_after_restart", func(t *testing.T) {
		holder := s.shardConn(t, 2)
		held := make(chan error, 1)
		go func() {
			_, err := holder.Exec(ctx, "begin; select count(*) from orders; select pg_sleep(6); commit")
			held <- err
		}()
		time.Sleep(500 * time.Millisecond)
		if _, err := conn.Exec(ctx, "set pgshard.ddl_async = on"); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, "alter table orders add column resumed int"); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(20 * time.Second)
		for {
			r := s.migration(t, "statement like '%resumed%'")
			if strings.Contains(r.perShard, `"2": {"state": "retrying"`) || strings.Contains(r.perShard, `"2": {"state": "running"`) {
				if strings.Count(r.perShard, `"applied"`) == 2 {
					break
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("migration never reached the retrying shard: %+v", r)
			}
			time.Sleep(100 * time.Millisecond)
		}
		s.killController()
		s.startController(top)
		r := s.awaitMigration(t, "statement like '%resumed%'")
		if r.state != "complete" || strings.Count(r.perShard, `"applied"`) != 3 {
			t.Fatalf("after restart: %+v\ncontroller log:\n%s", r, s.controllerLog.String())
		}
		<-held
		if got := s.onShards(t, "select count(*) = 1 from information_schema.columns where table_name = 'orders' and column_name = 'resumed'"); !allTrue(got) {
			t.Fatalf("column applied exactly once per shard: %v", got)
		}
		if _, err := conn.Exec(ctx, "reset pgshard.ddl_async"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("hard_failure_on_one_shard_names_it_and_leaves_others_applied", func(t *testing.T) {
		if _, err := s.shardConn(t, 1).Exec(ctx, "create index orders_dup on orders (id)"); err != nil {
			t.Fatal(err)
		}
		wctx, cancel := context.WithTimeout(ctx, 90*time.Second)
		defer cancel()
		_, err := conn.Exec(wctx, "create index orders_dup on orders (id)")
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || pe.Code != "42P07" {
			t.Fatalf("expected 42P07, got %v\nmigration: %+v\ncontroller log:\n%s", err, s.migration(t, "statement like '%orders_dup%'"), s.controllerLog.String())
		}
		if !strings.Contains(pe.Detail, "failed on shard 1") || !strings.Contains(pe.Detail, "applied on shard 0, 2") || !strings.Contains(pe.Detail, "DEGRADED") {
			t.Fatalf("detail %q", pe.Detail)
		}
		r := s.migration(t, "statement like '%orders_dup%'")
		if r.state != "failed" || !strings.Contains(r.perShard, `"state": "failed"`) || !strings.Contains(r.perShard, `"sqlstate": "42P07"`) || strings.Count(r.perShard, `"applied"`) != 2 {
			t.Fatalf("migration row %+v", r)
		}
		if got := s.onShards(t, "select to_regclass('orders_dup') is not null"); !allTrue(got) {
			t.Fatalf("index per shard: %v", got)
		}
	})

	t.Run("ddl_async_returns_the_id_and_completes", func(t *testing.T) {
		var notice string
		cfg, err := pgx.ParseConfig(s.dsn(appRole, appPassword, appDatabase))
		if err != nil {
			t.Fatal(err)
		}
		cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) { notice = n.Message }
		c, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close(ctx) }()
		if _, err := c.Exec(ctx, "set pgshard.ddl_async = on"); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		if _, err := c.Exec(ctx, "create table notes (id int primary key, body text)"); err != nil {
			t.Fatal(err)
		}
		if time.Since(start) > 2*time.Second {
			t.Fatal("async DDL waited")
		}
		if !strings.HasPrefix(notice, "migration ") {
			t.Fatalf("notice %q", notice)
		}
		id := strings.Fields(notice)[1]
		r := s.awaitMigration(t, "id = $1", id)
		if r.state != "complete" || r.perShard != `{"0": {"state": "applied", "attempts": 1}}` {
			t.Fatalf("migration %s: %+v", id, r)
		}
		if got := s.onShards(t, "select to_regclass('notes') is not null"); fmt.Sprint(got) != "[true false false]" {
			t.Fatalf("unsharded table per shard: %v", got)
		}
	})

	t.Run("refusals", func(t *testing.T) {
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, "create table t (id int)")
		if sqlstate(err) != "0A000" || !strings.Contains(err.Error(), "inside a transaction block") {
			t.Fatalf("DDL in transaction: %v", err)
		}
		_ = tx.Rollback(ctx)
		for _, c := range []struct{ sql, msg string }{
			// The refusal is the durability one, not the rewrite-class one: an
			// unlogged relation is emptied by crash recovery whatever pgshard
			// can rewrite.
			{"alter table orders set unlogged", "an unlogged relation is emptied by crash recovery"},
			{"alter table orders drop column tenant_id", "cannot be dropped, renamed or retyped"},
			{"drop table orders, notes", "one DDL statement cannot touch both sharded and unsharded tables"},
		} {
			_, err := conn.Exec(ctx, c.sql)
			if sqlstate(err) != "0A000" || !strings.Contains(err.Error(), c.msg) {
				t.Errorf("%s: got %v, want 0A000 %q", c.sql, err, c.msg)
			}
		}
	})

	t.Run("roles_share_one_verifier", func(t *testing.T) {
		for id := 0; id < 3; id++ {
			if _, err := s.shardConn(t, id).Exec(ctx, "alter role "+appRole+" createrole"); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := conn.Exec(ctx, "create role analyst login password 'an-secret'"); err != nil {
			t.Fatal(err)
		}
		var verifiers []string
		for id := 0; id < 3; id++ {
			var v string
			if err := s.shardConn(t, id).QueryRow(ctx, "select rolpassword from pg_authid where rolname = 'analyst'").Scan(&v); err != nil {
				t.Fatal(err)
			}
			verifiers = append(verifiers, v)
		}
		if verifiers[0] == "" || verifiers[0] != verifiers[1] || verifiers[1] != verifiers[2] || !strings.HasPrefix(verifiers[0], "SCRAM-SHA-256$") {
			t.Fatalf("verifiers %q", verifiers)
		}
		cat, err := pgx.Connect(ctx, s.catalogDSN)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cat.Close(ctx) }()
		var v string
		if err := cat.QueryRow(ctx, "select verifier from pgshard.roles where rolname = 'analyst'").Scan(&v); err != nil || v != verifiers[0] {
			t.Fatalf("catalog verifier %q %v", v, err)
		}
		if _, err := conn.Exec(ctx, "drop role analyst"); err != nil {
			t.Fatal(err)
		}
		if got := s.onShards(t, "select not exists (select 1 from pg_roles where rolname = 'analyst')"); !allTrue(got) {
			t.Fatalf("role dropped per shard: %v", got)
		}
	})
	t.Run("online_steps", func(t *testing.T) { testOnlineSteps(t, s, conn, top) })

}

// workload runs readers and writers against orders through the router
// while DDL is applied and reports the slowest statement and any error.
var workloadSeq atomic.Int64

type workload struct {
	stop    chan struct{}
	done    chan struct{}
	mu      sync.Mutex
	slowest time.Duration
	errs    []error
	n       int
}

func startWorkload(tb testing.TB, s *ddlStack) *workload {
	tb.Helper()
	w := &workload{stop: make(chan struct{}), done: make(chan struct{})}
	ctx := context.Background()
	var conns []*pgx.Conn
	for i := 0; i < 3; i++ {
		conns = append(conns, s.connect(tb))
	}
	go func() {
		defer close(w.done)
		i := 0
		for {
			select {
			case <-w.stop:
				return
			default:
			}
			i++
			seq := int(workloadSeq.Add(1))
			// One tenant per connection keeps a session on one shard.
			tenant := int64(i%3 + 1)
			var sql string
			var args []any
			switch i % 3 {
			case 0:
				sql, args = "insert into orders (tenant_id, id, note, region) values ($1, $2, 'w', 1)", []any{tenant, 100000 + seq}
			case 1:
				sql, args = "select count(*) from orders where tenant_id = $1", []any{tenant}
			default:
				sql, args = "update orders set note = 'u' where tenant_id = $1 and id = $2", []any{tenant, 100000 + seq - 2}
			}
			start := time.Now()
			_, err := conns[i%3].Exec(ctx, sql, args...)
			d := time.Since(start)
			w.mu.Lock()
			w.n++
			if d > w.slowest {
				w.slowest = d
			}
			if err != nil && len(w.errs) < 5 {
				w.errs = append(w.errs, fmt.Errorf("%s: %w", sql, err))
			}
			w.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}()
	return w
}

func (w *workload) finish(tb testing.TB) {
	tb.Helper()
	close(w.stop)
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.errs) > 0 {
		tb.Fatalf("workload errors (%d statements): %v", w.n, w.errs)
	}
	if w.slowest > 2*time.Second {
		tb.Fatalf("a workload statement waited %s behind DDL", w.slowest)
	}
	if w.n < 20 {
		tb.Fatalf("workload ran only %d statements", w.n)
	}
	tb.Logf("workload: %d statements, slowest %s", w.n, w.slowest)
}

func testOnlineSteps(t *testing.T, s *ddlStack, conn *pgx.Conn, top *testing.T) {
	ctx := context.Background()
	for _, sql := range []string{
		"create table regions (id int primary key, name text)",
		"alter table orders add column region int",
		"alter table orders add column amount int default 1",
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v\ncontroller log:\n%s", sql, err, s.controllerLog.String())
		}
	}
	for id := 0; id < 3; id++ {
		sc := s.shardConn(t, id)
		for _, sql := range []string{
			"insert into regions values (1, 'eu'), (2, 'us')",
			"insert into orders (tenant_id, id, note, region) select t, i, 'seed', 1 + i % 2 from generate_series(1, 7) t, generate_series(1, 300) i on conflict do nothing",
		} {
			if _, err := sc.Exec(ctx, sql); err != nil {
				t.Fatalf("shard %d: %s: %v", id, sql, err)
			}
		}
	}

	t.Run("check_constraint_is_added_not_valid_then_validated", func(t *testing.T) {
		w := startWorkload(t, s)
		_, err := conn.Exec(ctx, "alter table orders add check (amount >= 0)")
		w.finish(t)
		if err != nil {
			t.Fatalf("%v\ncontroller log:\n%s", err, s.controllerLog.String())
		}
		if got := s.onShards(t, "select convalidated from pg_constraint where conname = 'orders_amount_check'"); !allTrue(got) {
			t.Fatalf("validated per shard: %v", got)
		}
		r := s.migration(t, "statement like '%amount >= 0%'")
		if r.state != "complete" || !strings.Contains(r.perShard, `"step": 2`) {
			t.Fatalf("migration %+v", r)
		}
		var steps int
		cat, err := pgx.Connect(ctx, s.catalogDSN)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cat.Close(ctx) }()
		if err := cat.QueryRow(ctx, "select jsonb_array_length(meta->'steps') from pgshard.migrations where statement like '%amount >= 0%'").Scan(&steps); err != nil || steps != 2 {
			t.Fatalf("steps %d %v", steps, err)
		}
	})

	t.Run("set_not_null_fails_on_a_violating_row_and_leaves_no_constraint", func(t *testing.T) {
		if _, err := s.shardConn(t, 1).Exec(ctx, "insert into orders (tenant_id, id, note) values (2, 999999, null)"); err != nil {
			t.Fatal(err)
		}
		_, err := conn.Exec(ctx, "alter table orders alter column region set not null")
		if sqlstate(err) != "23502" {
			t.Fatalf("expected 23502, got %v\ncontroller log:\n%s", err, s.controllerLog.String())
		}
		if got := s.onShards(t, "select coalesce((select convalidated from pg_constraint where conname = 'orders_region_not_null'), false)"); fmt.Sprint(got) != "[true false false]" {
			t.Fatalf("constraint valid on shard 0, dropped on the failing shard 1, never added on shard 2: %v", got)
		}
		for id := 0; id < 3; id++ {
			if _, err := s.shardConn(t, id).Exec(ctx, "update orders set region = 1 where region is null"); err != nil {
				t.Fatal(err)
			}
		}
		w := startWorkload(t, s)
		_, err = conn.Exec(ctx, "alter table orders alter column region set not null")
		w.finish(t)
		if err != nil {
			t.Fatalf("%v\ncontroller log:\n%s", err, s.controllerLog.String())
		}
		if got := s.onShards(t, "select attnotnull and (select convalidated from pg_constraint where conname = 'orders_region_not_null') from pg_attribute where attrelid = 'orders'::regclass and attname = 'region'"); !allTrue(got) {
			t.Fatalf("column not null per shard: %v", got)
		}
	})

	t.Run("foreign_key_to_a_reference_table_and_unique_via_concurrent_index", func(t *testing.T) {
		w := startWorkload(t, s)
		_, err := conn.Exec(ctx, "alter table orders add foreign key (region) references regions (id)")
		if err == nil {
			_, err = conn.Exec(ctx, "alter table orders add constraint orders_tid unique (tenant_id, id)")
		}
		w.finish(t)
		if err != nil {
			t.Fatalf("%v\ncontroller log:\n%s", err, s.controllerLog.String())
		}
		if got := s.onShards(t, "select convalidated from pg_constraint where conname = 'orders_region_fkey'"); !allTrue(got) {
			t.Fatalf("fk validated per shard: %v", got)
		}
		if got := s.onShards(t, "select exists (select 1 from pg_constraint where conname = 'orders_tid' and contype = 'u') and (select indisvalid from pg_index where indexrelid = 'orders_tid'::regclass)"); !allTrue(got) {
			t.Fatalf("unique constraint per shard: %v", got)
		}
		_, err = conn.Exec(ctx, "alter table orders add foreign key (id) references notes (id)")
		if sqlstate(err) != "0A000" || !strings.Contains(err.Error(), "cross-shard foreign key") {
			t.Fatalf("cross-shard fk: %v\nmigration: %+v", err, s.migration(t, "statement like '%references notes%'"))
		}
	})

	t.Run("reindex_and_drop_index_run_concurrently", func(t *testing.T) {
		w := startWorkload(t, s)
		_, err := conn.Exec(ctx, "reindex table orders")
		if err == nil {
			_, err = conn.Exec(ctx, "drop index orders_id")
		}
		w.finish(t)
		if err != nil {
			t.Fatalf("%v\ncontroller log:\n%s", err, s.controllerLog.String())
		}
		if got := s.onShards(t, "select to_regclass('orders_id') is null"); !allTrue(got) {
			t.Fatalf("index dropped per shard: %v", got)
		}
		cat, err := pgx.Connect(ctx, s.catalogDSN)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cat.Close(ctx) }()
		var stmts []string
		rows, err := cat.Query(ctx, "select statement from pgshard.migrations where strategy = 'concurrent' and (kind = 'REINDEX' or statement like 'DROP INDEX%') order by created_at")
		if err != nil {
			t.Fatal(err)
		}
		if stmts, err = pgx.CollectRows(rows, pgx.RowTo[string]); err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(stmts) != "[REINDEX (CONCURRENTLY) TABLE orders DROP INDEX CONCURRENTLY orders_id]" {
			t.Fatalf("concurrent statements %q", stmts)
		}
	})

	t.Run("detach_partition_concurrently", func(t *testing.T) {
		for _, sql := range []string{
			"create table ev (k int) partition by list (k)",
			"create table ev1 partition of ev for values in (1)",
			"alter table ev detach partition ev1",
		} {
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v\ncontroller log:\n%s", sql, err, s.controllerLog.String())
			}
		}
		var attached bool
		if err := s.shardConn(t, 0).QueryRow(ctx, "select exists (select 1 from pg_inherits where inhrelid = 'ev1'::regclass)").Scan(&attached); err != nil || attached {
			t.Fatalf("ev1 still attached: %v %v", attached, err)
		}
		r := s.migration(t, "statement like '%detach partition%'")
		if r.state != "complete" || !strings.Contains(r.perShard, `"step": 2`) {
			t.Fatalf("migration %+v", r)
		}
	})

	t.Run("controller_killed_mid_steps_resumes_at_the_step", func(t *testing.T) {
		holder := s.shardConn(t, 2)
		held := make(chan error, 1)
		go func() {
			_, err := holder.Exec(ctx, "begin; select count(*) from orders; select pg_sleep(6); commit")
			held <- err
		}()
		time.Sleep(500 * time.Millisecond)
		if _, err := conn.Exec(ctx, "set pgshard.ddl_async = on"); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, "alter table orders add constraint amount_small check (amount < 1000000)"); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(20 * time.Second)
		var seen []string
		for {
			r := s.migration(t, "statement like '%amount_small%'")
			seen = append(seen, r.perShard)
			if strings.Contains(r.perShard, `"2": {"error": "ERROR: canceling statement due to lock timeout (SQLSTATE 55P03)", "state": "retrying"`) && strings.Count(r.perShard, `"step": 2`) == 2 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("migration never reached the retrying shard: %+v\nseen: %s", r, strings.Join(seen, "\n"))
			}
			time.Sleep(100 * time.Millisecond)
		}
		s.killController()
		s.startController(top)
		r := s.awaitMigration(t, "statement like '%amount_small%'")
		if r.state != "complete" || strings.Count(r.perShard, `"step": 2`) != 3 {
			t.Fatalf("after restart: %+v\ncontroller log:\n%s", r, s.controllerLog.String())
		}
		<-held
		if got := s.onShards(t, "select count(*) = 1 from pg_constraint where conname = 'amount_small' and convalidated"); !allTrue(got) {
			t.Fatalf("constraint validated exactly once per shard: %v", got)
		}
		if _, err := conn.Exec(ctx, "reset pgshard.ddl_async"); err != nil {
			t.Fatal(err)
		}
	})
}
