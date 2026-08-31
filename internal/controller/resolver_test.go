package controller

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
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

// flakyDialer refuses one shard on demand. It decorates the full dialer,
// not just ShardDialer: a decorator that implemented only the narrow
// interface was how the resolver's database-aware requirement came to be
// discovered at run time instead of stated in its type.
type flakyDialer struct {
	inner *PgxShardDialer
	down  int32
	dials atomic.Int32
	// downDatabase fails only the connections opened to finish a prepared
	// transaction, which are the only ones that name a database.
	downDatabase string
}

func (d *flakyDialer) Dial(ctx context.Context, set string, id int32) (ShardConn, error) {
	d.dials.Add(1)
	if id == d.down {
		return nil, fmt.Errorf("shard %s/%d unreachable", set, id)
	}
	return d.inner.Dial(ctx, set, id)
}

func (d *flakyDialer) DialDatabase(ctx context.Context, set string, id int32, database string) (ShardConn, error) {
	d.dials.Add(1)
	if id == d.down {
		return nil, fmt.Errorf("shard %s/%d unreachable", set, id)
	}
	if database != "" && database == d.downDatabase {
		return nil, fmt.Errorf("database %q unreachable", database)
	}
	return d.inner.DialDatabase(ctx, set, id, database)
}

func (d *flakyDialer) DialDatabaseAs(ctx context.Context, set string, id int32, database, user, password string) (ShardConn, error) {
	d.dials.Add(1)
	if id == d.down {
		return nil, fmt.Errorf("shard %s/%d unreachable", set, id)
	}
	return d.inner.DialDatabaseAs(ctx, set, id, database, user, password)
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

// TestResolverIdleClusterDoesNotScan pins the cost of a resolver with
// nothing to do: between orphan sweeps it must not touch a shard at all.
// It used to dial and query every shard of the topology on every pass, so
// a large cluster paid for the resolver's cadence forever.
func TestResolverIdleClusterDoesNotScan(t *testing.T) {
	parallelPG(t)
	f := newResolverFixture(t)
	ctx := context.Background()
	now := time.Now()
	f.res.Now = func() time.Time { return now }

	if _, err := f.res.Resolve(ctx, ""); err != nil {
		t.Fatal(err)
	}
	swept := f.dialer.dials.Load()
	if swept == 0 {
		t.Fatalf("first pass swept no shard: dials %d", swept)
	}

	for range 3 {
		if _, err := f.res.Resolve(ctx, ""); err != nil {
			t.Fatal(err)
		}
	}
	if got := f.dialer.dials.Load(); got != swept {
		t.Errorf("idle passes dialled %d shards, want none after the sweep", got-swept)
	}

	// A pass due for a sweep searches again, and so does a pass with
	// something in doubt however recently the last sweep ran.
	now = now.Add(DefaultSweepInterval)
	if _, err := f.res.Resolve(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if got := f.dialer.dials.Load(); got == swept {
		t.Error("a pass past the sweep interval did not search the topology")
	}
	swept = f.dialer.dials.Load()
	f.decide("pgshard-h-1-1", "commit", 0, 0, 1)
	if _, err := f.res.Resolve(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if got := f.dialer.dials.Load(); got == swept {
		t.Error("a pass with a decision in doubt did not search the topology")
	}
}

func TestResolver(t *testing.T) {
	parallelPG(t)
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
	parallelPG(t)
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
	parallelPG(t)
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
	conn, err := f.res.Shards.DialDatabase(ctx, "default", 0, "appdb")
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
	parallelPG(t)
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

func TestResolverSparesPreparingRefreshedBetweenScanAndAbort(t *testing.T) {
	parallelPG(t)
	f := newResolverFixture(t)
	ctx := context.Background()
	f.prepare(1, "pgshard-c-1-1", "raced-live")
	f.decide("pgshard-c-1-1", "preparing", 10*time.Minute, 1)
	f.prepare(1, "pgshard-c-1-2", "raced-dead")
	f.decide("pgshard-c-1-2", "preparing", 10*time.Minute, 1)
	shards, err := f.res.listShards(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	holders, scanErrs := f.res.scanPrepared(ctx, shards)
	if len(scanErrs) != 0 {
		t.Fatalf("scan errors %v", scanErrs)
	}
	// The coordinator heartbeats after the staleness snapshot was taken but
	// before the resolver aborts: the abort must land on zero rows and the
	// pass must leave the transaction alone.
	mustExecPool(t, f.pool, `UPDATE pgshard.xact_decisions SET heartbeat_at = now() WHERE gid = 'pgshard-c-1-1'`)
	stale := time.Now().Add(-10 * time.Minute)
	var out Outcome
	if err := f.res.resolveDecision(ctx, decision{GID: "pgshard-c-1-1", State: "preparing", Participants: []int32{1}, LastAlive: stale}, holders, true, &out); err != nil {
		t.Fatal(err)
	}
	if out != (Outcome{}) {
		t.Fatalf("outcome %+v: a freshly heartbeaten transaction was resolved", out)
	}
	if got := f.decisions(); strings.Join(got, ",") != "pgshard-c-1-1:preparing,pgshard-c-1-2:preparing" {
		t.Fatalf("decisions %v", got)
	}
	if got := f.prepared(1); strings.Join(got, ",") != "pgshard-c-1-1,pgshard-c-1-2" {
		t.Fatalf("shard 1 prepared %v", got)
	}
	// The genuinely stale sibling still ages out on the same pass shape.
	if err := f.res.resolveDecision(ctx, decision{GID: "pgshard-c-1-2", State: "preparing", Participants: []int32{1}, LastAlive: stale}, holders, true, &out); err != nil {
		t.Fatal(err)
	}
	if out.RolledBack != 1 {
		t.Fatalf("outcome %+v", out)
	}
	if got := f.prepared(1); strings.Join(got, ",") != "pgshard-c-1-1" {
		t.Fatalf("shard 1 prepared %v", got)
	}
	if got := f.decisions(); strings.Join(got, ",") != "pgshard-c-1-1:preparing" {
		t.Fatalf("decisions %v", got)
	}
}

func TestResolverSweepContinuesPastGonePreparedXact(t *testing.T) {
	parallelPG(t)
	f := newResolverFixture(t)
	ctx := context.Background()
	// "a" was scanned as prepared but its coordinator finished it (and
	// deleted its row) before the sweep: ROLLBACK PREPARED finds nothing.
	// It sorts before the real orphan, which must still be swept.
	f.prepare(0, "pgshard-s-1-b", "real-orphan")
	holders := map[string][]holder{
		"pgshard-s-1-a": {{Shard: ShardRef{Set: "default", ID: 0}}},
		"pgshard-s-1-b": {{Shard: ShardRef{Set: "default", ID: 0}}},
	}
	var out Outcome
	if err := f.res.sweepOrphans(ctx, holders, &out); err != nil {
		t.Fatal(err)
	}
	if out.RolledBack != 1 || out.Committed != 0 {
		t.Fatalf("outcome %+v", out)
	}
	if got := f.prepared(0); len(got) != 0 {
		t.Fatalf("shard 0 prepared %v: the orphan after the gone gid must be swept", got)
	}
	if got := f.values(0); len(got) != 0 {
		t.Fatalf("shard 0 values %v", got)
	}
}

// TestAnUnfinishedParticipantIsNamed: shard ids repeat across shard sets
// and PostgreSQL finishes a prepared transaction only from the database it
// was prepared in, so a resolver failure that named neither left an
// operator searching every set and database for the participant that would
// not budge.
func TestAnUnfinishedParticipantIsNamed(t *testing.T) {
	parallelPG(t)
	f := newResolverFixture(t)
	ctx := context.Background()
	mustExec(t, connect(t, f.shardDSN(0)), "CREATE DATABASE appdb")
	f.prepareIn(0, "appdb", "pgshard-x-1-1", "stuck")
	f.decide("pgshard-x-1-1", "commit", 0, 0)

	var log bytes.Buffer
	f.res.Logger = slog.New(slog.NewTextHandler(&log, nil))
	f.dialer.downDatabase = "appdb"

	out, err := f.res.Resolve(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	// The decision pass fails to finish it, and the orphan sweep then
	// fails on the same prepared transaction, so both count.
	if out.Unresolved == 0 || out.Committed != 0 {
		t.Fatalf("outcome %+v, want the commit left unresolved", out)
	}
	if got := f.decisions(); len(got) != 1 {
		t.Fatalf("a decision the resolver could not finish must be kept: %v", got)
	}
	for _, want := range []string{"COMMIT PREPARED", "default/0", `database \"appdb\"`, "pgshard-x-1-1"} {
		if !strings.Contains(log.String(), want) {
			t.Fatalf("the resolver warning does not say %s:\n%s", want, log.String())
		}
	}
}

// TestAFinishedDecisionIsPrunedBeforeItsXidFreezes: a decision row that
// outlives the transaction it decided becomes unverifiable once its
// transaction id freezes past the clog horizon, and a restore then fences
// a cluster that is perfectly consistent. The router deletes the row when
// it finishes the commit itself; this is the other path, where that delete
// never happened and nothing holds the transaction any more.
func TestAFinishedDecisionIsPrunedBeforeItsXidFreezes(t *testing.T) {
	parallelPG(t)
	f := newResolverFixture(t)
	ctx := context.Background()

	// Committed everywhere and no longer prepared anywhere: exactly what a
	// row looks like when the coordinator died between COMMIT PREPARED and
	// deleting it.
	f.decide("pgshard-finished-1", "commit", time.Hour, 0, 1)
	f.decide("pgshard-finished-2", "abort", time.Hour, 1)
	if got := f.decisions(); len(got) != 2 {
		t.Fatalf("decisions before the pass: %v", got)
	}

	out, err := f.res.Resolve(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if out.Unresolved != 0 {
		t.Fatalf("outcome %+v, want the rows settled", out)
	}
	if got := f.decisions(); len(got) != 0 {
		t.Fatalf("decision rows left to freeze: %v", got)
	}
}

// TestTemplateDialAsksForAGroupNameOnce: a copy pass dials every source and
// target of every database several times every few seconds, and each
// template dial asked the catalog for a group name that almost never
// changes. The name is cached; a dial that fails while using a cached name
// forgets it, so a group that really was renamed costs one failed
// connection rather than a wedged workflow.
func TestTemplateDialAsksForAGroupNameOnce(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	f := newResolverFixture(t)
	ref := ShardRef{Set: "default", ID: 0}
	d := &PgxShardDialer{Pool: f.pool, Template: "postgres://nobody@127.0.0.1:1/{group}?connect_timeout=1"}

	var group string
	if err := f.pool.QueryRow(ctx, `SELECT group_name FROM pgshard.shard_status WHERE shard_set = $1 AND shard_id = $2`, ref.Set, ref.ID).Scan(&group); err != nil {
		t.Fatal(err)
	}
	dsn, cached, err := d.dsn(ctx, ref.Set, ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cached {
		t.Fatal("the first lookup cannot come from the cache")
	}
	if !strings.Contains(dsn, group) {
		t.Fatalf("dsn %q does not carry the group name %q", dsn, group)
	}

	// The catalog changes underneath: a cached lookup keeps answering from
	// the cache, which is the whole point -- it is not asking any more.
	if _, err := f.pool.Exec(ctx, `UPDATE pgshard.shard_status SET group_name = 'renamed' WHERE shard_set = $1 AND shard_id = $2`, ref.Set, ref.ID); err != nil {
		t.Fatal(err)
	}
	dsn, cached, err = d.dsn(ctx, ref.Set, ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !cached || !strings.Contains(dsn, group) {
		t.Fatalf("second lookup: cached=%v dsn=%q, want the cached %q", cached, dsn, group)
	}

	// Forgetting it is what a failed dial does, and the next lookup reads
	// the catalog again.
	d.forgetGroup(ref)
	dsn, cached, err = d.dsn(ctx, ref.Set, ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cached || !strings.Contains(dsn, "renamed") {
		t.Fatalf("after forgetting: cached=%v dsn=%q, want a fresh read of \"renamed\"", cached, dsn)
	}

	// And a dial that fails while using a cached name forgets it, so the
	// next attempt re-reads rather than repeating a name that cannot work.
	d.rememberGroup(ref, "stale")
	if _, err := d.DialDatabase(ctx, ref.Set, ref.ID, ""); err == nil {
		t.Fatal("dialing 127.0.0.1:1 must fail")
	}
	// The retry re-read the catalog, so the cache now holds the current
	// name rather than the stale one. What must not survive is "stale".
	if got, ok := d.cachedGroup(ref); !ok || got != "renamed" {
		t.Fatalf("cached group after a failed dial = %q (present=%v), want the re-read %q", got, ok, "renamed")
	}
}

// TestPreparingTimeoutOutlivesTheCoordinatorHeartbeat: the beat interval
// and this timeout are one invariant across two processes, and they were
// independent constants. A timeout set within a beat or two of the
// interval aborts live coordinators whose beat was merely late -- safe for
// the data, but the client was told the transaction was fine.
func TestPreparingTimeoutOutlivesTheCoordinatorHeartbeat(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  time.Duration
		beat time.Duration
		want time.Duration
	}{
		{"unset uses the default", 0, 0, DefaultPreparingTimeout},
		{"the default is raised too when the beat is slow", 0, 5 * time.Second, 20 * time.Second},
		{"a timeout under the floor is lifted to it", time.Second, 0, catalog.MinPreparingTimeout},
		{"a generous timeout is kept", time.Minute, 0, time.Minute},
		{"a shorter beat lowers the floor with it", time.Second, 100 * time.Millisecond, time.Second},
		{"the floor follows the configured beat", 10 * time.Millisecond, 100 * time.Millisecond, 400 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &Resolver{PreparingTimeout: tc.set, HeartbeatInterval: tc.beat}
			if got := r.preparingTimeout(); got != tc.want {
				t.Fatalf("preparingTimeout() = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestTheDefaultTimeoutSpansSeveralHeartbeats guards the pair of constants
// themselves: a later edit to either that brought them together would put
// every cluster on the default into the spurious-abort window.
func TestTheDefaultTimeoutSpansSeveralHeartbeats(t *testing.T) {
	if DefaultPreparingTimeout < catalog.MinPreparingTimeout {
		t.Fatalf("DefaultPreparingTimeout %s is under the floor of %d heartbeats (%s)",
			DefaultPreparingTimeout, catalog.MinPreparingBeats, catalog.MinPreparingTimeout)
	}
}
