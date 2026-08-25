package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// TestBarrierOnPostgres drives a barrier over a real catalog and two shards
// (archive_command is a no-op so pg_stat_archiver advances): a stale
// preparing row and its prepared transaction are drained by the resolver,
// every group gets the restore point, the row is certified and the fence is
// released.
func TestBarrierOnPostgres(t *testing.T) {
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
	if list, _ := srv.ListBarriers(ctx, &pgshardv1.ListBarriersRequest{}); len(list.GetBarriers()) != 1 {
		t.Fatalf("failed barrier recorded: %v", list)
	}
	if got := f.prepared(1); len(got) != 2 {
		t.Fatalf("foreign and live prepared transactions must survive: %v", got)
	}
}

func TestDecisionWatermarkSurvivesDeletedRows(t *testing.T) {
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
