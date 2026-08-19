package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// resolverFixture is a catalog and two shard PostgreSQL instances (gids
// are unique per instance, so the shards cannot share one) that accept
// prepared transactions.
type resolverFixture struct {
	t      *testing.T
	pool   *pgxpool.Pool
	shards []string
	res    *Resolver
	dialer *flakyDialer
}

func newResolverFixture(t *testing.T) *resolverFixture {
	t.Helper()
	dsn := startPostgres(t)
	ctx := context.Background()
	conn := connect(t, dsn)
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	f := &resolverFixture{t: t, pool: pool}
	dsns := map[ShardRef]string{}
	for _, id := range []int{0, 1} {
		sdsn := startPostgresWith(t, "-c max_prepared_transactions=16")
		f.shards = append(f.shards, sdsn)
		dsns[ShardRef{Set: "default", ID: int32(id)}] = sdsn
		mustExec(t, conn, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_epoch)
			VALUES ('default', $1, $2, 'serving', 1)`, id, fmt.Sprintf("g%d", id))
		mustExec(t, connect(t, sdsn), "CREATE TABLE t (v text)")
	}
	f.dialer = &flakyDialer{inner: &PgxShardDialer{Pool: pool, DSNs: dsns}, down: -1}
	f.res = &Resolver{Pool: pool, Shards: f.dialer}
	return f
}

// flakyDialer refuses one shard on demand.
type flakyDialer struct {
	inner ShardDialer
	down  int32
}

func (d *flakyDialer) Dial(ctx context.Context, set string, id int32) (ShardConn, error) {
	if id == d.down {
		return nil, fmt.Errorf("shard %s/%d unreachable", set, id)
	}
	return d.inner.Dial(ctx, set, id)
}

func (f *resolverFixture) shardDSN(id int) string { return f.shards[id] }

// prepare leaves a prepared transaction inserting v into t on shard id.
func (f *resolverFixture) prepare(id int, gid, v string) {
	f.t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, f.shardDSN(id))
	if err != nil {
		f.t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, sql := range []string{"BEGIN", "INSERT INTO t VALUES (" + quoteLiteral(v) + ")", "PREPARE TRANSACTION " + quoteLiteral(gid)} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			f.t.Fatalf("%s: %v", sql, err)
		}
	}
}

func (f *resolverFixture) decide(gid, state string, age time.Duration, participants ...int32) {
	f.t.Helper()
	mustExecPool(f.t, f.pool, `INSERT INTO pgshard.xact_decisions (gid, state, participants, created_at) VALUES ($1, $2, $3, now() - $4::interval)`,
		gid, state, participants, age)
}

func mustExecPool(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func (f *resolverFixture) prepared(id int) []string {
	f.t.Helper()
	conn := connect(f.t, f.shardDSN(id))
	rows, err := conn.Query(context.Background(), "SELECT gid FROM pg_prepared_xacts ORDER BY gid")
	if err != nil {
		f.t.Fatal(err)
	}
	gids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		f.t.Fatal(err)
	}
	return gids
}

func (f *resolverFixture) values(id int) []string {
	f.t.Helper()
	conn := connect(f.t, f.shardDSN(id))
	rows, err := conn.Query(context.Background(), "SELECT v FROM t ORDER BY v")
	if err != nil {
		f.t.Fatal(err)
	}
	vs, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		f.t.Fatal(err)
	}
	return vs
}

func (f *resolverFixture) decisions() []string {
	f.t.Helper()
	rows, err := f.pool.Query(context.Background(), "SELECT gid || ':' || state FROM pgshard.xact_decisions ORDER BY gid")
	if err != nil {
		f.t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		f.t.Fatal(err)
	}
	return out
}

func TestResolver(t *testing.T) {
	f := newResolverFixture(t)
	ctx := context.Background()
	f.prepare(0, "pgshard-r-1-1", "committed")
	f.prepare(1, "pgshard-r-1-1", "committed")
	f.decide("pgshard-r-1-1", "commit", 0, 0, 1)
	f.prepare(0, "pgshard-r-1-2", "aborted")
	f.decide("pgshard-r-1-2", "abort", 0, 0)
	f.prepare(1, "pgshard-r-1-3", "stale-preparing")
	f.decide("pgshard-r-1-3", "preparing", time.Minute, 1)
	f.prepare(1, "pgshard-r-1-4", "young-preparing")
	f.decide("pgshard-r-1-4", "preparing", 0, 1)
	f.prepare(0, "pgshard-r-1-5", "orphan")
	f.prepare(0, "foreign-1", "foreign")

	out, err := f.res.Resolve(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if out.Committed != 1 || out.RolledBack != 3 || out.Unresolved != 0 {
		t.Fatalf("outcome %+v; prepared %v / %v; decisions %v", out, f.prepared(0), f.prepared(1), f.decisions())
	}
	if got := f.values(0); strings.Join(got, ",") != "committed" {
		t.Fatalf("shard 0 values %v", got)
	}
	if got := f.values(1); strings.Join(got, ",") != "committed" {
		t.Fatalf("shard 1 values %v", got)
	}
	if got := f.prepared(0); strings.Join(got, ",") != "foreign-1" {
		t.Fatalf("shard 0 prepared %v: foreign gids must be left alone", got)
	}
	if got := f.prepared(1); strings.Join(got, ",") != "pgshard-r-1-4" {
		t.Fatalf("shard 1 prepared %v: a young preparing row must wait for its router", got)
	}
	if got := f.decisions(); strings.Join(got, ",") != "pgshard-r-1-4:preparing" {
		t.Fatalf("decisions left %v", got)
	}
	// A commit-decided gid is committed by the orphan sweep too, never
	// rolled back: here the decision row cannot be finished because a
	// participant is unreachable, and the sweep of the reachable shard
	// still applies the recorded decision.
	f.prepare(1, "pgshard-r-2-1", "late-commit")
	f.decide("pgshard-r-2-1", "commit", 0, 0, 1)
	f.dialer.down = 0
	out, err = f.res.Resolve(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if out.Committed != 1 || out.RolledBack != 0 || out.Unresolved != 2 {
		t.Fatalf("outcome %+v", out)
	}
	if got := f.values(1); strings.Join(got, ",") != "committed,late-commit" {
		t.Fatalf("shard 1 values %v", got)
	}
	if got := f.decisions(); strings.Join(got, ",") != "pgshard-r-1-4:preparing,pgshard-r-2-1:commit" {
		t.Fatalf("decisions %v: an unfinished commit row must stay", got)
	}
	f.dialer.down = -1
	out, err = f.res.Resolve(ctx, "")
	if err != nil || out.Committed != 1 || out.Unresolved != 0 {
		t.Fatalf("outcome %+v err %v", out, err)
	}
	// Once the young row ages out it is aborted.
	f.res.Now = func() time.Time { return time.Now().Add(time.Minute) }
	out, err = f.res.Resolve(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if out.RolledBack != 1 || len(f.prepared(1)) != 0 || len(f.decisions()) != 0 {
		t.Fatalf("outcome %+v prepared %v decisions %v", out, f.prepared(1), f.decisions())
	}
	if got := f.values(1); strings.Join(got, ",") != "committed,late-commit" {
		t.Fatalf("shard 1 values %v", got)
	}
	// Idempotent: a further pass changes nothing.
	out, err = f.res.Resolve(ctx, "")
	if err != nil || out != (Outcome{}) {
		t.Fatalf("outcome %+v err %v", out, err)
	}
}
