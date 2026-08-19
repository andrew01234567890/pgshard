//go:build integration

package router

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
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
		if got := s.onShards(t, "select not rolsuper and not rolbypassrls from pg_roles where rolname = 'pgshard_ddl'"); !allTrue(got) {
			t.Fatalf("pgshard_ddl is a plain login per shard: %v", got)
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
			{"alter table orders alter column id type bigint", "rewrite-class DDL is not available yet"},
			{"alter table orders set unlogged", "rewrite-class DDL is not available yet"},
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
}
