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
	return newResolverFixtureWith(t)
}

// newResolverFixtureWith starts the catalog and shards with extra server
// options.
func newResolverFixtureWith(t *testing.T, opts ...string) *resolverFixture {
	t.Helper()
	dsn := startPostgresWith(t, opts...)
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
		sdsn := startPostgresWith(t, append([]string{"-c max_prepared_transactions=16", "-c wal_level=logical"}, opts...)...)
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
	mustExecPool(f.t, f.pool, `INSERT INTO pgshard.xact_decisions (gid, state, participants, created_at, heartbeat_at) VALUES ($1, $2, $3, now() - $4::interval, now() - $4::interval)`,
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
	// A commit decision is applied on every reachable holder even while a
	// participant is unreachable, and the row survives until the whole
	// topology could be searched; the decision counts as committed on the
	// pass that retires it.
	f.prepare(1, "pgshard-r-2-1", "late-commit")
	f.decide("pgshard-r-2-1", "commit", 0, 0, 1)
	f.dialer.down = 0
	out, err = f.res.Resolve(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if out.Committed != 0 || out.RolledBack != 0 || out.Unresolved != 2 {
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

func TestResolverCommitsMovedParticipant(t *testing.T) {
	f := newResolverFixture(t)
	ctx := context.Background()
	// After a reshard the decision's participant id maps to a group that no
	// longer holds the prepared transaction: it sits on another group. The
	// commit decision must be committed where the transaction actually is.
	f.prepare(0, "pgshard-m-1-1", "moved-commit")
	f.decide("pgshard-m-1-1", "commit", 0, 1)
	out, err := f.res.Resolve(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if out.Committed != 1 || out.RolledBack != 0 || out.Unresolved != 0 {
		t.Fatalf("outcome %+v; decisions %v", out, f.decisions())
	}
	if got := f.values(0); strings.Join(got, ",") != "moved-commit" {
		t.Fatalf("shard 0 values %v: the moved participant must be committed, not rolled back", got)
	}
	if got := f.prepared(0); len(got) != 0 {
		t.Fatalf("shard 0 prepared %v", got)
	}
	if got := f.decisions(); len(got) != 0 {
		t.Fatalf("decisions left %v", got)
	}
	// While any shard of the topology cannot be searched, a decision whose
	// listed participants show nothing prepared is kept, never deleted: the
	// transaction may sit on the unreachable group.
	f.prepare(0, "pgshard-m-2-1", "hidden-commit")
	f.decide("pgshard-m-2-1", "commit", 0, 1)
	f.dialer.down = 0
	out, err = f.res.Resolve(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if out.Committed != 0 || out.RolledBack != 0 {
		t.Fatalf("outcome %+v", out)
	}
	if got := f.decisions(); strings.Join(got, ",") != "pgshard-m-2-1:commit" {
		t.Fatalf("decisions %v: the row must survive an incomplete topology search", got)
	}
	f.dialer.down = -1
	out, err = f.res.Resolve(ctx, "")
	if err != nil || out.Committed != 1 || out.Unresolved != 0 {
		t.Fatalf("outcome %+v err %v", out, err)
	}
	if got := f.values(0); strings.Join(got, ",") != "hidden-commit,moved-commit" {
		t.Fatalf("shard 0 values %v", got)
	}
}

// prepareIn leaves a prepared transaction in database db on shard id.
func (f *resolverFixture) prepareIn(id int, db, gid, v string) {
	f.t.Helper()
	ctx := context.Background()
	cfg, err := pgx.ParseConfig(f.shardDSN(id))
	if err != nil {
		f.t.Fatal(err)
	}
	cfg.Database = db
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		f.t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, sql := range []string{"CREATE TABLE IF NOT EXISTS t (v text)", "BEGIN", "INSERT INTO t VALUES (" + quoteLiteral(v) + ")", "PREPARE TRANSACTION " + quoteLiteral(gid)} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			f.t.Fatalf("%s: %v", sql, err)
		}
	}
}

func TestResolverFinishesOtherDatabase(t *testing.T) {
	f := newResolverFixture(t)
	ctx := context.Background()
	mustExec(t, connect(t, f.shardDSN(0)), "CREATE DATABASE appdb")
	f.prepareIn(0, "appdb", "pgshard-d-1-1", "db-commit")
	f.decide("pgshard-d-1-1", "commit", 0, 0)
	f.prepareIn(0, "appdb", "pgshard-d-1-2", "db-orphan")
	out, err := f.res.Resolve(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if out.Committed != 1 || out.RolledBack != 1 || out.Unresolved != 0 {
		t.Fatalf("outcome %+v; prepared %v; decisions %v", out, f.prepared(0), f.decisions())
	}
	if got := f.prepared(0); len(got) != 0 {
		t.Fatalf("shard 0 prepared %v", got)
	}
	if got := f.decisions(); len(got) != 0 {
		t.Fatalf("decisions left %v", got)
	}
	conn, err := f.res.Shards.(ShardDBDialer).DialDatabase(ctx, "default", 0, "appdb")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, "SELECT v FROM t ORDER BY v")
	if err != nil {
		t.Fatal(err)
	}
	vs, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(vs, ",") != "db-commit" {
		t.Fatalf("appdb values %v", vs)
	}
}

func TestResolverSparesPreparingRowWithLiveHeartbeat(t *testing.T) {
	f := newResolverFixture(t)
	ctx := context.Background()
	// The row is far older than the preparing timeout, but its coordinator
	// heartbeat is fresh: the router is alive and merely slow between
	// PREPARE and the commit decision, so the resolver must not abort it.
	f.prepare(1, "pgshard-h-1-1", "slow-live")
	mustExecPool(t, f.pool, `INSERT INTO pgshard.xact_decisions (gid, state, participants, created_at, heartbeat_at)
		VALUES ('pgshard-h-1-1', 'preparing', '{1}', now() - interval '10 minutes', now())`)
	out, err := f.res.Resolve(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if out != (Outcome{}) {
		t.Fatalf("outcome %+v: a live coordinator's transaction was resolved", out)
	}
	if got := f.prepared(1); strings.Join(got, ",") != "pgshard-h-1-1" {
		t.Fatalf("shard 1 prepared %v: the transaction must stay for its coordinator", got)
	}
	if got := f.decisions(); strings.Join(got, ",") != "pgshard-h-1-1:preparing" {
		t.Fatalf("decisions %v", got)
	}
	// Once the heartbeat goes stale the coordinator is presumed dead and
	// the undecided transaction is aborted.
	mustExecPool(t, f.pool, `UPDATE pgshard.xact_decisions SET heartbeat_at = now() - interval '10 minutes' WHERE gid = 'pgshard-h-1-1'`)
	out, err = f.res.Resolve(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if out.RolledBack != 1 || len(f.prepared(1)) != 0 || len(f.decisions()) != 0 {
		t.Fatalf("outcome %+v prepared %v decisions %v", out, f.prepared(1), f.decisions())
	}
}
