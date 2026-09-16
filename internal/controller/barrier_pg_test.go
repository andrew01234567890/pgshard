package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestBarrierOnPostgres drives a barrier over a real catalog and two shards
// (archive_command is a no-op so pg_stat_archiver advances): a stale
// preparing row and its prepared transaction are drained by the resolver,
// every group gets the restore point, the row is certified and the fence is
// released.
func TestBarrierOnPostgres(t *testing.T) {
	parallelPG(t)
	f := newResolverFixtureWith(t, "-c archive_mode=on", "-c archive_command=/bin/true")
	ctx := context.Background()
	f.prepare(0, "pgshard-stale", "stale")
	f.decide("pgshard-stale", "preparing", time.Minute, 0)
	// The row is a minute stale; a short timeout needs a short heartbeat
	// under it, or the floor that keeps live coordinators alive lifts it.
	f.res.PreparingTimeout, f.res.HeartbeatInterval = time.Second, 100*time.Millisecond
	b := &Barrier{Store: &PGBarrierStore{Pool: f.pool}, Groups: &SQLBarrierGroups{Pool: f.pool, Shards: f.dialer}, Resolver: f.res, Poll: 50 * time.Millisecond}
	srv := &Server{Pool: f.pool, Barrier: b, Resolver: f.res}

	resp, err := srv.CreateBarrier(ctx, &pgshardv1.CreateBarrierRequest{Name: "b1"})
	if err != nil || resp.GetError() != nil {
		t.Fatalf("CreateBarrier: %v %v", err, resp.GetError())
	}
	bar := resp.GetBarrier()
	if bar.GetName() != "b1" || bar.GetRestorePoint() != "pgshard-b1" || !bar.GetCertified() || len(bar.GetGroups()) != 3 || bar.GetId() == "" {
		t.Fatalf("barrier %v", bar)
	}
	for _, g := range bar.GetGroups() {
		if g.GetLsn() == 0 || g.GetTimeline() != 1 || len(g.GetWalSegment()) != 24 {
			t.Fatalf("group point %v", g)
		}
	}
	if got := f.prepared(0); len(got) != 0 {
		t.Fatalf("prepared left on shard 0: %v", got)
	}
	if got := f.decisions(); len(got) != 0 {
		t.Fatalf("decision rows left: %v", got)
	}
	var fenced bool
	if err := f.pool.QueryRow(ctx, `SELECT write_fence FROM pgshard.shard_map_generation`).Scan(&fenced); err != nil || fenced {
		t.Fatalf("fence after the barrier: %v %v", fenced, err)
	}
	// Each group's point names the database system it was written in: a
	// catalog rebuilt by a major upgrade since keeps the manifest's rows
	// but none of its restore points, and a restore has to tell (PGS-825).
	var manifest []byte
	if err := f.pool.QueryRow(ctx, `SELECT per_group FROM pgshard.restore_points WHERE name = 'b1'`).Scan(&manifest); err != nil {
		t.Fatal(err)
	}
	var perGroup map[string]GroupRestorePoint
	if err := json.Unmarshal(manifest, &perGroup); err != nil {
		t.Fatal(err)
	}
	systemOf := func(q interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	}) string {
		var id string
		if err := q.QueryRow(ctx, `SELECT system_identifier::text FROM pg_control_system()`).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	for group, want := range map[string]string{CatalogGroup: systemOf(f.pool), "g0": systemOf(connect(t, f.shardDSN(0))), "g1": systemOf(connect(t, f.shardDSN(1)))} {
		if got := perGroup[group].SystemIdentifier; got != want {
			t.Errorf("the manifest records %s's system as %q, want %q", group, got, want)
		}
	}
	var restorePoint string
	if err := connect(t, f.shardDSN(1)).QueryRow(ctx, `SELECT pg_walfile_name(pg_current_wal_lsn())`).Scan(&restorePoint); err != nil {
		t.Fatal(err)
	}
	if restorePoint <= bar.GetGroups()[2].GetWalSegment() {
		t.Fatalf("shard 1 WAL was not switched past the restore point: now %s, point in %s", restorePoint, bar.GetGroups()[2].GetWalSegment())
	}

	list, err := srv.ListBarriers(ctx, &pgshardv1.ListBarriersRequest{CertifiedOnly: true})
	if err != nil || len(list.GetBarriers()) != 1 || list.GetBarriers()[0].GetId() != bar.GetId() || list.GetBarriers()[0].GetGroups()[1].GetGroup() != "g0" {
		t.Fatalf("ListBarriers: %v %v", err, list)
	}
	if _, err := srv.CreateBarrier(ctx, &pgshardv1.CreateBarrierRequest{Name: "b1"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("duplicate: %v", err)
	}
	if _, err := srv.CreateBarrier(ctx, &pgshardv1.CreateBarrierRequest{Name: "Nope"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad name: %v", err)
	}

	// A prepared transaction nobody decides blocks the drain: the barrier
	// fails, records nothing and releases the fence.
	f.prepare(1, "not-ours", "x")
	f.res.PreparingTimeout = time.Hour
	f.decide("pgshard-live", "preparing", 0, 1)
	f.prepare(1, "pgshard-live", "live")
	b.DrainTimeout = 300 * time.Millisecond
	resp, err = srv.CreateBarrier(ctx, &pgshardv1.CreateBarrierRequest{Name: "b2"})
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "drain: still in flight") || resp != nil {
		t.Fatalf("blocked barrier: %v %v", err, resp)
	}
	if err := f.pool.QueryRow(ctx, `SELECT write_fence FROM pgshard.shard_map_generation`).Scan(&fenced); err != nil || fenced {
		t.Fatalf("fence after the failed barrier: %v %v", fenced, err)
	}
	// The failed attempt keeps its reserved row so the name can never be
	// reused, and is listed uncertified; only b1 is certified.
	list, _ = srv.ListBarriers(ctx, &pgshardv1.ListBarriersRequest{})
	certified := 0
	for _, b := range list.GetBarriers() {
		if b.GetCertified() {
			certified++
		}
	}
	if len(list.GetBarriers()) != 2 || certified != 1 {
		t.Fatalf("expected the failed attempt listed uncertified: %v", list)
	}
	if only, _ := srv.ListBarriers(ctx, &pgshardv1.ListBarriersRequest{CertifiedOnly: true}); len(only.GetBarriers()) != 1 {
		t.Fatalf("certified-only list: %v", only)
	}
	// Re-running the burnt name is refused rather than creating a second
	// physical restore point of the same name.
	again, aerr := srv.CreateBarrier(ctx, &pgshardv1.CreateBarrierRequest{Name: "b2"})
	msg := ""
	if aerr != nil {
		msg = aerr.Error()
	} else {
		msg = again.GetError().GetMessage()
	}
	if !strings.Contains(msg, "choose a new name") {
		t.Fatalf("retry of a burnt name: %v %v", aerr, again)
	}
	if got := f.prepared(1); len(got) != 2 {
		t.Fatalf("foreign and live prepared transactions must survive: %v", got)
	}
}

func TestDecisionWatermarkSurvivesDeletedRows(t *testing.T) {
	parallelPG(t)
	f := newResolverFixtureWith(t)
	ctx := context.Background()
	store := &PGBarrierStore{Pool: f.pool}
	before, err := store.DecisionWatermark(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mustExecPool(t, f.pool, `INSERT INTO pgshard.xact_decisions (gid, state, participants) VALUES ('pgshard-gone', 'commit', '{0}')`)
	mustExecPool(t, f.pool, `DELETE FROM pgshard.xact_decisions WHERE gid = 'pgshard-gone'`)
	after, err := store.DecisionWatermark(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after <= before {
		t.Fatalf("watermark did not advance for a deleted row: before=%d after=%d", before, after)
	}
}

// TestBarrierLockSerializesOnPostgres: the barrier advisory lock admits one
// holder at a time, so two barriers can never raise and clear the shared
// write fence concurrently.
func TestBarrierLockSerializesOnPostgres(t *testing.T) {
	parallelPG(t)
	f := newResolverFixtureWith(t)
	ctx := context.Background()
	store := &PGBarrierStore{Pool: f.pool}

	unlock, err := store.Lock(ctx)
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}
	// A second barrier cannot acquire the lock while the first holds it.
	if _, err := store.Lock(ctx); !errors.Is(err, ErrBarrierBusy) {
		t.Fatalf("second lock: err = %v, want ErrBarrierBusy", err)
	}
	unlock()
	// After release, a new barrier can take it.
	unlock2, err := store.Lock(ctx)
	if err != nil {
		t.Fatalf("lock after release: %v", err)
	}
	unlock2()
}

// TestWriteFenceOwnerCASOnPostgres: the fence is cleared only by its owner, so
// a barrier that lost its lock session cannot clear a fence a later barrier
// has raised.
func TestWriteFenceOwnerCASOnPostgres(t *testing.T) {
	parallelPG(t)
	f := newResolverFixtureWith(t)
	ctx := context.Background()
	fenced := func() bool {
		var v bool
		if err := f.pool.QueryRow(ctx, `SELECT write_fence FROM pgshard.shard_map_generation`).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	if err := catalog.RaiseWriteFence(ctx, f.pool, "run A", "owner-A"); err != nil {
		t.Fatal(err)
	}
	if !fenced() {
		t.Fatal("fence not raised")
	}
	// A stale release from a different owner must not clear it.
	if cleared, err := catalog.ReleaseWriteFence(ctx, f.pool, "owner-B"); err != nil || cleared {
		t.Fatalf("foreign owner cleared the fence: cleared=%v err=%v", cleared, err)
	}
	if !fenced() {
		t.Fatal("fence dropped by a non-owner")
	}
	// The owner clears it.
	if cleared, err := catalog.ReleaseWriteFence(ctx, f.pool, "owner-A"); err != nil || !cleared {
		t.Fatalf("owner failed to clear: cleared=%v err=%v", cleared, err)
	}
	if fenced() {
		t.Fatal("fence still up after owner release")
	}
}

// TestTheBarrierDrainLeavesOutOnlyAPlacementCopyThatHasNotWritten
// (PGS-863): a placement copy's read-only snapshot, begun before the pause,
// is not waited for; the same session is once it holds an xid, and a
// transaction begun before the pause under any other name, or under the
// copy's name by another role, still is.
func TestTheBarrierDrainLeavesOutOnlyAPlacementCopyThatHasNotWritten(t *testing.T) {
	f := newResolverFixtureWith(t)
	ctx := context.Background()
	groups := &SQLBarrierGroups{Pool: f.pool, Shards: f.dialer}
	g := GroupRef{Name: "shard0", Set: "default", ID: 0}

	cp := connect(t, f.shardDSN(0))
	mustExec(t, cp, `SELECT set_config('application_name', $1, false)`, PlacementCopyApplicationName)
	mustExec(t, cp, `CREATE TEMP TABLE scratch (id int)`)
	mustExec(t, cp, `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`)
	mustExec(t, cp, `SELECT 1`)
	other := connect(t, f.shardDSN(0))
	mustExec(t, other, `BEGIN`)
	mustExec(t, other, `SELECT 1`)
	mustExec(t, connect(t, f.shardDSN(0)), `CREATE ROLE app LOGIN`)
	impostor := connect(t, strings.Replace(f.shardDSN(0), "postgres://postgres@", "postgres://app@", 1))
	mustExec(t, impostor, `SELECT set_config('application_name', $1, false)`, PlacementCopyApplicationName)
	mustExec(t, impostor, `BEGIN`)
	mustExec(t, impostor, `SELECT 1`)

	pausedAt, err := groups.PauseWrites(ctx, g, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = groups.PauseWrites(context.Background(), g, false) })
	if n, err := groups.WritersSince(ctx, g, pausedAt); err != nil || n != 2 {
		t.Fatalf("writers with a placement copy's snapshot, another pre-pause transaction and another role under the copy's name open: %d %v, want the two others", n, err)
	}
	mustExec(t, other, `ROLLBACK`)
	mustExec(t, impostor, `ROLLBACK`)
	if n, err := groups.WritersSince(ctx, g, pausedAt); err != nil || n != 0 {
		t.Fatalf("a placement copy's read-only snapshot held the drain: %d %v", n, err)
	}
	mustExec(t, cp, `INSERT INTO scratch VALUES (1)`)
	if n, err := groups.WritersSince(ctx, g, pausedAt); err != nil || n != 1 {
		t.Fatalf("a placement copy holding an xid was not counted: %d %v", n, err)
	}
	mustExec(t, cp, `ROLLBACK`)
}

// TestBarrierPauseAndWriterCountOnPostgres exercises the pause and the writer
// drain against real PostgreSQL: a paused shard refuses writes, an in-flight
// write transaction is counted, and resuming restores writes.
func TestBarrierPauseAndWriterCountOnPostgres(t *testing.T) {
	f := newResolverFixtureWith(t)
	ctx := context.Background()
	groups := &SQLBarrierGroups{Pool: f.pool, Shards: f.dialer}
	g := GroupRef{Name: "shard0", Set: "default", ID: 0}
	shard := connect(t, f.shardDSN(0))
	mustExec(t, shard, `CREATE TABLE paused_t (id int)`)

	if n, err := groups.WritersSince(ctx, g, time.Now()); err != nil || n != 0 {
		t.Fatalf("idle shard writers = %d %v", n, err)
	}
	if n, err := groups.SubscriptionCount(ctx, g); err != nil || n != 0 {
		t.Fatalf("subscriptions on an idle shard = %d %v", n, err)
	}
	// An in-flight write transaction is visible to the drain.
	busy := connect(t, f.shardDSN(0))
	mustExec(t, busy, `BEGIN`)
	mustExec(t, busy, `INSERT INTO paused_t VALUES (1)`)
	deadline := time.Now().Add(10 * time.Second)
	for {
		n, err := groups.WritersSince(ctx, g, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if n >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("an open write transaction was never counted")
		}
		time.Sleep(50 * time.Millisecond)
	}
	mustExec(t, busy, `COMMIT`)

	// A transaction opened BEFORE the pause keeps the read-write mode it
	// started with, so it can still write afterwards and must keep the drain
	// busy even though it has not written yet.
	pre := connect(t, f.shardDSN(0))
	mustExec(t, pre, `BEGIN`)
	mustExec(t, pre, `SELECT 1`)

	pausedAt, err := groups.PauseWrites(ctx, g, true)
	if err != nil {
		t.Fatal(err)
	}
	if pausedAt.IsZero() {
		t.Fatal("pause did not report when it became effective")
	}
	if on, err := groups.PauseEffective(ctx, g); err != nil || !on {
		t.Fatalf("pause not effective on a fresh connection: %v %v", on, err)
	}
	if n, err := groups.WritersSince(ctx, g, pausedAt); err != nil || n < 1 {
		t.Fatalf("a transaction opened before the pause must block the drain: %d %v", n, err)
	}
	// It really can still write, which is why it must be drained.
	if _, err := pre.Exec(ctx, `INSERT INTO paused_t VALUES (99)`); err != nil {
		t.Fatalf("a pre-pause transaction should still be read-write: %v", err)
	}
	mustExec(t, pre, `ROLLBACK`)
	if n, err := groups.WritersSince(ctx, g, pausedAt); err != nil || n != 0 {
		t.Fatalf("drain not clear after the pre-pause transaction ended: %d %v", n, err)
	}
	writer := connect(t, f.shardDSN(0))
	if _, err := writer.Exec(ctx, `INSERT INTO paused_t VALUES (2)`); err == nil || !strings.Contains(err.Error(), "read-only transaction") {
		t.Fatalf("paused shard accepted a write: %v", err)
	}
	// The barrier's own work still runs on a paused group.
	if _, err := groups.CreateRestorePoint(ctx, g, "pgshard-pause-check"); err != nil {
		t.Fatalf("restore point on a paused group: %v", err)
	}
	if _, err := groups.PauseWrites(ctx, g, false); err != nil {
		t.Fatal(err)
	}
	if on, err := groups.PauseEffective(ctx, g); err != nil || on {
		t.Fatalf("pause still effective after resume: %v %v", on, err)
	}
	resumed := connect(t, f.shardDSN(0))
	if _, err := resumed.Exec(ctx, `INSERT INTO paused_t VALUES (3)`); err != nil {
		t.Fatalf("resumed shard still refuses writes: %v", err)
	}
}

// TestDrainCountsAPreparedTransactionPgshardDidNotMake: after PREPARE a
// backend holds no transaction id, and COMMIT PREPARED is transaction
// control, which PostgreSQL allows under a read-only default. So a
// two-phase transaction pgshard never coordinated can commit inside the
// window the pause is meant to have emptied, and land on one side of a
// barrier the other shards know nothing about. The drain counted only
// pgshard's own gids and would have certified over it.
func TestDrainCountsAPreparedTransactionPgshardDidNotMake(t *testing.T) {
	parallelPG(t)
	f := newResolverFixtureWith(t)
	ctx := context.Background()
	groups := &SQLBarrierGroups{Pool: f.pool, Shards: f.dialer}
	g := GroupRef{Name: "shard0", Set: "default", ID: 0}
	shard := connect(t, f.shardDSN(0))
	mustExec(t, shard, `CREATE TABLE outsider (id int)`)

	if gids, err := groups.PreparedGIDs(ctx, g); err != nil || len(gids) != 0 {
		t.Fatalf("idle shard prepared = %v %v", gids, err)
	}
	for _, sql := range []string{`BEGIN`, `INSERT INTO outsider VALUES (1)`, `PREPARE TRANSACTION 'someone-elses-2pc'`} {
		mustExec(t, shard, sql)
	}
	t.Cleanup(func() { mustExec(t, shard, `ROLLBACK PREPARED 'someone-elses-2pc'`) })

	gids, err := groups.PreparedGIDs(ctx, g)
	if err != nil {
		t.Fatal(err)
	}
	if len(gids) != 1 || gids[0] != "someone-elses-2pc" {
		t.Fatalf("prepared = %v, want the drain to see a transaction whatever prepared it, by name", gids)
	}

	// The resolver keeps its own filter: finishing a transaction it did not
	// coordinate is not its decision to make.
	out, err := f.res.Resolve(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if out.Committed != 0 || out.RolledBack != 0 {
		t.Fatalf("the resolver touched a transaction it did not coordinate: %+v", out)
	}
	if n := queryOne[int64](t, shard, `SELECT count(*) FROM pg_prepared_xacts`); n != 1 {
		t.Fatalf("%d prepared transactions after a resolver pass, want the outsider left alone", n)
	}
}

// TestAnUnownedWriteCannotDisturbABarriersFence: the fence a barrier
// raises is stamped with an owner, but the agent's SetWriteFence RPC
// writes it without one. Against a live catalog that would open writes in
// the middle of the barrier that raised it, or -- raising over it -- take
// the owner's stamp away so the barrier's own release matched nothing and
// left the cluster fenced.
func TestAnUnownedWriteCannotDisturbABarriersFence(t *testing.T) {
	parallelPG(t)
	f := newResolverFixtureWith(t)
	ctx := context.Background()

	// No owner: a restored catalog, which is what this path is for.
	if err := catalog.SetWriteFence(ctx, f.pool, true, "restore"); err != nil {
		t.Fatalf("fencing an unowned catalog: %v", err)
	}
	if err := catalog.SetWriteFence(ctx, f.pool, false, ""); err != nil {
		t.Fatalf("unfencing an unowned catalog: %v", err)
	}

	if err := catalog.RaiseWriteFence(ctx, f.pool, "barrier b1", "owner-1"); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		what   string
		active bool
	}{{"release", false}, {"raise", true}} {
		if err := catalog.SetWriteFence(ctx, f.pool, c.active, "someone else"); !errors.Is(err, catalog.ErrFenceOwned) {
			t.Fatalf("an unowned %s of an owned fence returned %v, want ErrFenceOwned", c.what, err)
		}
	}
	fence, err := catalog.ReadWriteFence(ctx, f.pool)
	if err != nil {
		t.Fatal(err)
	}
	if !fence.Active || fence.Reason != "barrier b1" {
		t.Fatalf("the barrier's fence was disturbed: %+v", fence)
	}

	// The owner still releases its own fence, and once it has, the
	// unowned path works again.
	cleared, err := catalog.ReleaseWriteFence(ctx, f.pool, "owner-1")
	if err != nil || !cleared {
		t.Fatalf("owner release: %v %v", cleared, err)
	}
	if err := catalog.SetWriteFence(ctx, f.pool, true, "restore"); err != nil {
		t.Fatalf("after the owner released it: %v", err)
	}
}

// TestABarrierLeavesARetiredSetPaused: a retired shard set keeps the
// permanent write pause its cutover left on it, and that pause is all that
// stops a router still on an old snapshot committing on a set nothing
// replicates from any more. The barrier listed every group in shard_status
// and resumed every group it listed, so the first barrier after any
// completed or rolled-back cutover made every retired set writable for good
// (PGS-817).
func TestABarrierLeavesARetiredSetPaused(t *testing.T) {
	parallelPG(t)
	f := newResolverFixtureWith(t, "-c archive_mode=on", "-c archive_command=/bin/true")
	ctx := context.Background()

	// A retired set on a server of its own, carrying the retirement pause.
	oldDSN := startPostgresWith(t, "-c wal_level=logical", "-c archive_mode=on", "-c archive_command=/bin/true")
	f.dialer.inner.DSNs[ShardRef{Set: "old", ID: 0}] = oldDSN
	cat, err := f.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Retirement is reached through a whole cutover; the state is what this
	// test is about, so it is written directly. The fixture's pool is a
	// superuser, which the shard-set lifecycle trigger lets through as the
	// control plane.
	for _, stmt := range []string{
		`INSERT INTO pgshard.shard_sets (shard_set, generation, state) VALUES ('old', 99, 'retired')`,
		`INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_epoch) VALUES ('old', 0, 'o0', 'retired', 1)`,
	} {
		if _, err := cat.Exec(ctx, stmt); err != nil {
			cat.Release()
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	cat.Release()
	old := connect(t, oldDSN)
	mustExec(t, old, `CREATE TABLE t (v text)`)
	mustExec(t, old, `ALTER SYSTEM SET default_transaction_read_only = on`)
	mustExec(t, old, `SELECT pg_reload_conf()`)
	write := func() error {
		c, err := pgx.Connect(ctx, oldDSN)
		if err != nil {
			return err
		}
		defer func() { _ = c.Close(ctx) }()
		_, err = c.Exec(ctx, `INSERT INTO t VALUES ('after the barrier')`)
		return err
	}
	waitFor(t, 10*time.Second, func() bool {
		var pgErr *pgconn.PgError
		return errors.As(write(), &pgErr) && pgErr.Code == "25006"
	}, "the retirement pause never took, so the barrier below has nothing to lift")

	b := &Barrier{Store: &PGBarrierStore{Pool: f.pool}, Groups: &SQLBarrierGroups{Pool: f.pool, Shards: f.dialer}, Resolver: f.res, Poll: 50 * time.Millisecond}
	srv := &Server{Pool: f.pool, Barrier: b, Resolver: f.res}
	resp, err := srv.CreateBarrier(ctx, &pgshardv1.CreateBarrierRequest{Name: "past-a-retired-set"})
	if err != nil || resp.GetError() != nil {
		t.Fatalf("CreateBarrier: %v %v", err, resp.GetError())
	}
	for _, g := range resp.GetBarrier().GetGroups() {
		if g.GetGroup() == "o0" {
			t.Errorf("the barrier certified a restore point on the retired set: %v", g)
		}
	}

	var pgErr *pgconn.PgError
	if err := write(); !errors.As(err, &pgErr) || pgErr.Code != "25006" {
		t.Fatalf("the retired set took a write after the barrier (err %v): its retirement pause was lifted", err)
	}
}

// TestABarrierDoesNotLiftAWorkflowsClaimedPause (PGS-844): a switch holds its
// sources paused, under its claim, from quiesce until forward replication is
// off. The barrier resumes every group it paused and lifted that pause with
// its own, letting a write land after the positions the switch had checked.
func TestABarrierDoesNotLiftAWorkflowsClaimedPause(t *testing.T) {
	parallelPG(t)
	f := newResolverFixtureWith(t, "-c archive_mode=on", "-c archive_command=/bin/true")
	ctx := context.Background()

	var owner string
	if err := f.pool.QueryRow(ctx, `INSERT INTO pgshard.workflows (id, kind, state, spec, status)
		VALUES (gen_random_uuid(), 'upgrade', 'running', '{}', '{}') RETURNING id::text`).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `UPDATE pgshard.shard_status SET write_paused_by = $1::uuid WHERE shard_set = 'default' AND shard_id = 0`, owner); err != nil {
		t.Fatal(err)
	}
	claimed := connect(t, f.shards[0])
	mustExec(t, claimed, `ALTER SYSTEM SET default_transaction_read_only = on`)
	mustExec(t, claimed, `SELECT pg_reload_conf()`)
	write := func(dsn string) error {
		c, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return err
		}
		defer func() { _ = c.Close(ctx) }()
		_, err = c.Exec(ctx, `INSERT INTO t VALUES ('after the barrier')`)
		return err
	}
	refused := func(err error) bool {
		var pgErr *pgconn.PgError
		return errors.As(err, &pgErr) && pgErr.Code == "25006"
	}
	waitFor(t, 20*time.Second, func() bool { return refused(write(f.shards[0])) }, "the claimed pause never took")

	b := &Barrier{Store: &PGBarrierStore{Pool: f.pool}, Groups: &SQLBarrierGroups{Pool: f.pool, Shards: f.dialer}, Resolver: f.res, Poll: 50 * time.Millisecond}
	srv := &Server{Pool: f.pool, Barrier: b, Resolver: f.res}
	resp, err := srv.CreateBarrier(ctx, &pgshardv1.CreateBarrierRequest{Name: "beside-a-switch"})
	if err != nil || resp.GetError() != nil {
		t.Fatalf("CreateBarrier: %v %v", err, resp.GetError())
	}
	if err := write(f.shards[0]); !refused(err) {
		t.Fatalf("the shard a workflow holds paused took a write after the barrier (err %v): the barrier lifted the claimed pause", err)
	}
	waitFor(t, 20*time.Second, func() bool { return write(f.shards[1]) == nil }, "the barrier left the unclaimed shard paused")
	var still string
	if err := f.pool.QueryRow(ctx, `SELECT coalesce(write_paused_by::text, '') FROM pgshard.shard_status WHERE shard_set = 'default' AND shard_id = 0`).Scan(&still); err != nil || still != owner {
		t.Fatalf("the claim is %q (%v) after the barrier, want %s's still standing", still, err, owner)
	}
}

// retiringGroups retires the shard set named set the first time the barrier
// takes a shard group's restore point: a rollback retiring the set, and its
// Complete finding the pause already standing, in the middle of the run.
// also, when set, runs at the same moment.
type retiringGroups struct {
	*SQLBarrierGroups
	set  string
	also func(context.Context) error
	once sync.Once
}

func (g *retiringGroups) CreateRestorePoint(ctx context.Context, ref GroupRef, name string) (RestorePointResult, error) {
	if !ref.Catalog() {
		var err error
		g.once.Do(func() {
			_, err = g.Pool.Exec(ctx, `UPDATE pgshard.shard_sets SET state = 'retired' WHERE shard_set = $1`, g.set)
			if err == nil && g.also != nil {
				err = g.also(ctx)
			}
		})
		if err != nil {
			return RestorePointResult{}, err
		}
	}
	return g.SQLBarrierGroups.CreateRestorePoint(ctx, ref, name)
}

// TestABarrierDoesNotLiftARetirementPauseRaisedDuringItsRun (PGS-822): the
// barrier lists a set while it serves and pauses it; a rollback retires it
// before the barrier resumes, and Complete's retirement pause is the one
// standing. Resuming every group it had listed lifted that pause for good.
func TestABarrierDoesNotLiftARetirementPauseRaisedDuringItsRun(t *testing.T) {
	parallelPG(t)
	f := newResolverFixtureWith(t, "-c archive_mode=on", "-c archive_command=/bin/true")
	ctx := context.Background()
	var state string
	if err := f.pool.QueryRow(ctx, `SELECT state FROM pgshard.shard_sets WHERE shard_set = 'default'`).Scan(&state); err != nil || state != "serving" {
		t.Fatalf("the fixture's set is %q (%v), want serving", state, err)
	}
	write := func(dsn string) error {
		c, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return err
		}
		defer func() { _ = c.Close(ctx) }()
		_, err = c.Exec(ctx, `INSERT INTO t VALUES ('after the barrier')`)
		return err
	}
	refused := func(err error) bool {
		var pgErr *pgconn.PgError
		return errors.As(err, &pgErr) && pgErr.Code == "25006"
	}
	barrier := func(name string, also func(context.Context) error) error {
		groups := &retiringGroups{SQLBarrierGroups: &SQLBarrierGroups{Pool: f.pool, Shards: f.dialer}, set: "default", also: also}
		b := &Barrier{Store: &PGBarrierStore{Pool: f.pool}, Groups: groups, Resolver: f.res, Poll: 50 * time.Millisecond}
		srv := &Server{Pool: f.pool, Barrier: b, Resolver: f.res}
		_, err := srv.CreateBarrier(ctx, &pgshardv1.CreateBarrierRequest{Name: name})
		return err
	}

	// While a workflow still names the set it may need it writable -- a
	// switch back after the rollback -- so the barrier resumes it as before.
	var wf string
	if err := f.pool.QueryRow(ctx, `INSERT INTO pgshard.workflows (id, kind, state, spec, status)
		VALUES (gen_random_uuid(), 'reshard', 'running', '{"shard_set": "default", "source_set": "older"}', '{}') RETURNING id::text`).Scan(&wf); err != nil {
		t.Fatal(err)
	}
	if err := barrier("beside-a-rollback", nil); err != nil {
		t.Fatalf("CreateBarrier: %v", err)
	}
	for i, dsn := range f.shards {
		waitFor(t, 20*time.Second, func() bool { return write(dsn) == nil }, fmt.Sprintf("the barrier left shard %d paused although a live workflow names its set", i))
	}

	// Once nothing does, the retirement pause is the one that stands.
	if _, err := f.pool.Exec(ctx, `UPDATE pgshard.workflows SET state = 'completed' WHERE id = $1::uuid`, wf); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `UPDATE pgshard.shard_sets SET state = 'serving' WHERE shard_set = 'default'`); err != nil {
		t.Fatal(err)
	}
	if err := barrier("across-a-retirement", nil); err != nil {
		t.Fatalf("CreateBarrier: %v", err)
	}
	for i, dsn := range f.shards {
		if err := write(dsn); !refused(err) {
			t.Fatalf("shard %d of the set retired during the barrier took a write after it (err %v): the barrier lifted the retirement pause", i, err)
		}
	}

	// Nor when the primary started applying a subscription during the run,
	// which the pause would fail with 25006. The barrier refuses to certify
	// across it; whether it resumed is what matters here.
	if _, err := f.pool.Exec(ctx, `UPDATE pgshard.shard_sets SET state = 'serving' WHERE shard_set = 'default'`); err != nil {
		t.Fatal(err)
	}
	sub := connect(t, f.shards[0])
	mustExec(t, sub, `SET default_transaction_read_only = off`)
	_ = barrier("beside-a-subscription", func(ctx context.Context) error {
		if _, err := sub.Exec(ctx, `CREATE SUBSCRIPTION leftover CONNECTION 'host=127.0.0.1 port=1 dbname=postgres' PUBLICATION p
			WITH (connect = false, slot_name = 'leftover')`); err != nil {
			return err
		}
		_, err := sub.Exec(ctx, `ALTER SUBSCRIPTION leftover ENABLE`)
		return err
	})
	waitFor(t, 20*time.Second, func() bool { return write(f.shards[0]) == nil }, "the barrier left a retired shard that still applies a subscription paused")
	if err := write(f.shards[1]); !refused(err) {
		t.Fatalf("shard 1 has no subscription and took a write after the barrier (err %v)", err)
	}
}
