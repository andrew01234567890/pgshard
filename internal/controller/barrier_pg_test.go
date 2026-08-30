package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
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
	f.res.PreparingTimeout = time.Second
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
	if err != nil || resp.GetError() == nil || !strings.Contains(resp.GetError().GetMessage(), "drain: still in flight") {
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

	if n, err := groups.PreparedCount(ctx, g); err != nil || n != 0 {
		t.Fatalf("idle shard prepared = %d %v", n, err)
	}
	for _, sql := range []string{`BEGIN`, `INSERT INTO outsider VALUES (1)`, `PREPARE TRANSACTION 'someone-elses-2pc'`} {
		mustExec(t, shard, sql)
	}
	t.Cleanup(func() { mustExec(t, shard, `ROLLBACK PREPARED 'someone-elses-2pc'`) })

	n, err := groups.PreparedCount(ctx, g)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("prepared = %d, want the drain to see a transaction whatever prepared it", n)
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
