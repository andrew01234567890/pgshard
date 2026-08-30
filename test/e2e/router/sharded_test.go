//go:build integration

package router

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/placement"
	"github.com/andrew01234567890/pgshard/internal/router"
)

// shardedStack is a stack with a second shard container and pooler and a
// sharded table declared in the catalog.
type shardedStack struct {
	*stack
	shard1DSN string
}

func startShardedStack(tb testing.TB) *shardedStack {
	tb.Helper()
	return startShardedStackWith(tb, nil, nil)
}

// startShardedStackWith is startShardedStack with PostgreSQL options per
// shard.
func startShardedStackWith(tb testing.TB, shard0Opts, shard1Opts []string) *shardedStack {
	tb.Helper()
	return startShardedStackFull(tb, nil, shard0Opts, shard1Opts)
}

// startShardedStackFull is startShardedStackWith with PostgreSQL options
// for the catalog too.
func startShardedStackFull(tb testing.TB, catalogOpts, shard0Opts, shard1Opts []string) *shardedStack {
	tb.Helper()
	s := &shardedStack{stack: startStackFull(tb, catalogOpts, shard0Opts)}
	poolerBin, _ := buildBinaries(tb)
	var shard1Addr string
	shard1Addr, s.shard1DSN = startPostgres(tb, "shard1", shard1Opts...)
	pooler1 := fmt.Sprintf("127.0.0.1:%d", freePort(tb))
	err := router.DevBootstrap{CatalogDSN: s.catalogDSN, ShardDSN: s.shard1DSN, ShardID: 1, Database: appDatabase, Role: appRole,
		Password: appPassword, PoolerEndpoint: pooler1, Epoch: 1}.Run(context.Background())
	if err != nil {
		tb.Fatalf("bootstrap shard 1: %v", err)
	}
	host, port, _ := net.SplitHostPort(shard1Addr)
	startProcess(tb, &logBuffer{}, "listening on", poolerBin, "run", "--insecure-dev", "--listen", pooler1,
		"--pg-host", host, "--pg-port", port, "--pg-database", appDatabase, "--stream-dsn", streamDSN(s.shard1DSN),
		"--catalog-dsn", s.catalogDSN, "--shard-set", router.DefaultShardSet, "--shard-id", "1", "--drain-timeout", "5s")
	s.shardDSNs[1] = s.shard1DSN
	s.startController(tb)
	ctx := context.Background()
	cat, err := pgx.Connect(ctx, s.catalogDSN)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = cat.Close(ctx) }()
	tx, err := cat.Begin(ctx)
	if err != nil {
		tb.Fatal(err)
	}
	for _, sql := range []string{
		`INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('default', 0, '[,0)'), ('default', 1, '[0,)')`,
		`INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('app', 'public', 'orders', 'sharded', 'tenant_id')`,
		`INSERT INTO pgshard.table_status (database, schema_name, table_name, effective_placement, effective_shard_key) VALUES ('app', 'public', 'orders', 'sharded', 'tenant_id')`,
	} {
		if _, err := tx.Exec(ctx, sql); err != nil {
			tb.Fatalf("%s: %v", sql, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		tb.Fatal(err)
	}
	for _, dsn := range []string{s.shardDSN, s.shard1DSN} {
		conn, err := pgx.Connect(ctx, strings.Replace(dsn, "/postgres?", "/"+appDatabase+"?", 1))
		if err != nil {
			tb.Fatal(err)
		}
		if _, err := conn.Exec(ctx, "create table orders (tenant_id int8, id int, primary key (tenant_id, id)); grant all on orders to "+appRole); err != nil {
			tb.Fatal(err)
		}
		_ = conn.Close(ctx)
	}
	return s
}

// awaitSharded waits until the router's snapshot follows the catalog
// through LISTEN/NOTIFY and knows the table as sharded (a locking scatter is
// then refused).
func (s *shardedStack) awaitSharded(tb testing.TB, conn *pgx.Conn) {
	tb.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, err := conn.Exec(ctx, "select * from orders for update", pgx.QueryExecModeSimpleProtocol)
		if sqlstate(err) == "0A000" {
			return
		}
		if time.Now().After(deadline) {
			tb.Fatalf("router never learned the sharded placement (last: %v)\nrouter log:\n%s", err, s.routerLog.String())
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// shardOf maps a tenant to the shard of the two-way split above.
func shardOf(tb testing.TB, tenant int64) int {
	id, err := placement.KeyspaceID(tenant)
	if err != nil {
		tb.Fatal(err)
	}
	if id < 0 {
		return 0
	}
	return 1
}

func (s *shardedStack) rowsOn(tb testing.TB, shard int, tenant int64) int {
	tb.Helper()
	dsn := s.shardDSN
	if shard == 1 {
		dsn = s.shard1DSN
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, strings.Replace(dsn, "/postgres?", "/"+appDatabase+"?", 1))
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var n int
	if err := conn.QueryRow(ctx, "select count(*) from orders where tenant_id = $1", tenant).Scan(&n); err != nil {
		tb.Fatal(err)
	}
	return n
}

func TestRouterShardedRouting(t *testing.T) {
	s := startShardedStack(t)
	ctx := context.Background()
	conn := s.connect(t)
	var t0, t1 int64 = -1, -1
	for i := int64(1); i < 50 && (t0 < 0 || t1 < 0); i++ {
		if shardOf(t, i) == 0 && t0 < 0 {
			t0 = i
		}
		if shardOf(t, i) == 1 && t1 < 0 {
			t1 = i
		}
	}
	s.awaitSharded(t, conn)
	if _, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, $2)", t1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 100)", t0); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 101), ($1, 102)", t1); err != nil {
		t.Fatal(err)
	}
	if n := s.rowsOn(t, 0, t0); n != 1 {
		t.Fatalf("shard 0 has %d rows of tenant %d, want 1", n, t0)
	}
	if n := s.rowsOn(t, 1, t0); n != 0 {
		t.Fatalf("shard 1 has %d rows of tenant %d, want 0", n, t0)
	}
	if n := s.rowsOn(t, 0, t1); n != 0 {
		t.Fatalf("shard 0 has %d rows of tenant %d, want 0", n, t1)
	}
	if n := s.rowsOn(t, 1, t1); n < 3 {
		t.Fatalf("shard 1 has %d rows of tenant %d, want at least 3", n, t1)
	}

	var n int
	if err := conn.QueryRow(ctx, "select count(*) from orders where tenant_id = $1", t1).Scan(&n); err != nil || n < 3 {
		t.Fatalf("keyed select through the router: %d %v", n, err)
	}
	if err := conn.QueryRow(ctx, fmt.Sprintf("select count(*) from orders where tenant_id = %d", t0), pgx.QueryExecModeSimpleProtocol).Scan(&n); err != nil || n != 1 {
		t.Fatalf("simple keyed select: %d %v", n, err)
	}
	if _, err := conn.Exec(ctx, "update orders set id = id where tenant_id = $1", t0); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "delete from orders where tenant_id = $1 and id = 100", t0); err != nil {
		t.Fatal(err)
	}
	if n := s.rowsOn(t, 0, t0); n != 0 {
		t.Fatalf("delete by key left %d rows", n)
	}

	t.Run("transaction_moves_then_pins", func(t *testing.T) {
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 200)", t0); err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 201)", t1)
		if sqlstate(err) != "0A000" || !strings.Contains(err.Error(), "cannot take part in a multi-shard transaction") {
			t.Fatalf("second shard inside a transaction: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if n := s.rowsOn(t, 0, t0); n != 1 {
			t.Fatalf("committed row missing on shard 0: %d", n)
		}
	})

	t.Run("refusals", func(t *testing.T) {
		for _, c := range []struct{ sql, msg string }{
			{"select * from orders for update", "multi-shard SELECT with FOR UPDATE/SHARE is not available yet"},
			{"select avg(id) from orders", "multi-shard avg() is not available yet"},
			{"insert into orders values (1, 2)", "insert requires the shard key"},
			{"update orders set tenant_id = 1 where tenant_id = 2", "shard key is immutable"},
			{"delete from orders", "scatter DELETE without a shard key predicate is not available yet"},
			{"select * from orders where tenant_id::text = '7'::text", "shard key column tenant_id is compared through a cast"},
			// The refusal is the durability one, not the rewrite-class one: an
			// unlogged relation is emptied by crash recovery whatever pgshard
			// can rewrite.
			{"alter table orders set unlogged", "an unlogged relation is emptied by crash recovery"},
			{"create table orders (id int primary key, tenant_id int8)", "primary key or unique constraint (id) on sharded table \"orders\" must include the shard key \"tenant_id\""},
		} {
			_, err := conn.Exec(ctx, c.sql)
			if sqlstate(err) != "0A000" || !strings.Contains(err.Error(), c.msg) {
				t.Errorf("%s: got %v, want 0A000 %q", c.sql, err, c.msg)
			}
		}
	})

	t.Run("unsharded_table_stays_home", func(t *testing.T) {
		if _, err := conn.Exec(ctx, "create table notes (id int primary key, body text)"); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, "insert into notes values (1, 'home')"); err != nil {
			t.Fatal(err)
		}
		home, err := pgx.Connect(ctx, strings.Replace(s.shardDSN, "/postgres?", "/"+appDatabase+"?", 1))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = home.Close(ctx) }()
		var n int
		if err := home.QueryRow(ctx, "select count(*) from notes").Scan(&n); err != nil || n != 1 {
			t.Fatalf("unsharded table on home shard: %d %v", n, err)
		}
	})
}
