package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

func TestStreamMonitor(t *testing.T) {
	f := newResolverFixture(t)
	ctx := context.Background()
	m := &StreamMonitor{Pool: f.pool, Shards: f.dialer}
	if n, err := m.Sweep(ctx); err != nil || n != 0 {
		t.Fatalf("sweep without streams: %d %v", n, err)
	}
	if err := catalog.CreateStream(ctx, f.pool, catalog.Stream{Name: "orders", Database: "postgres", TwoPhase: true}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.CreateStream(ctx, f.pool, catalog.Stream{Name: "Bad"}); err == nil {
		t.Fatal("invalid stream name accepted")
	}
	streams, err := catalog.ListStreams(ctx, f.pool)
	if err != nil || len(streams) != 1 || streams[0].State != catalog.StreamCreating || !streams[0].TwoPhase || streams[0].Database != "postgres" || streams[0].CreatedAt.IsZero() {
		t.Fatalf("streams: %+v %v", streams, err)
	}
	mustExec(t, connect(t, f.shardDSN(0)), "SELECT pg_create_logical_replication_slot('pgshard_orders_g0', 'pgoutput', false, true, true)")

	n, err := m.Sweep(ctx)
	if err != nil || n != 2 {
		t.Fatalf("sweep: %d %v", n, err)
	}
	rows, err := catalog.ListStreamStatus(ctx, f.pool, "orders")
	if err != nil || len(rows) != 2 {
		t.Fatalf("status rows: %+v %v", rows, err)
	}
	if r := rows[0]; r.ShardID != 0 || r.Slot != "pgshard_orders_g0" || r.WALStatus != "reserved" || r.Active || r.ConfirmedFlushLSN == 0 || r.RestartLSN == 0 || r.InvalidationReason != "" {
		t.Fatalf("shard 0 status: %+v", r)
	}
	if r := rows[1]; r.ShardID != 1 || r.Slot != "pgshard_orders_g1" || r.WALStatus != "missing" || r.ConfirmedFlushLSN != 0 {
		t.Fatalf("shard 1 status: %+v", r)
	}
	if err := catalog.SetStreamState(ctx, f.pool, "orders", catalog.StreamActive); err != nil {
		t.Fatal(err)
	}

	f.dialer.down = 1
	if n, err := m.Sweep(ctx); err == nil || n != 1 {
		t.Fatalf("sweep with shard 1 down must report the error and still write shard 0: %d %v", n, err)
	}
	f.dialer.down = -1

	if err := catalog.UpsertStreamStatus(ctx, f.pool, catalog.StreamStatus{Stream: "orders", ShardSet: "default", ShardID: 1, Slot: "pgshard_orders_g1", WALStatus: "lost", InvalidationReason: "wal_removed"}); err != nil {
		t.Fatal(err)
	}
	streams, _ = catalog.ListStreams(ctx, f.pool)
	if streams[0].State != catalog.StreamLost {
		t.Fatalf("lost slot must mark the stream lost: %+v", streams)
	}
	if all, err := catalog.ListStreamStatus(ctx, f.pool, ""); err != nil || len(all) != 2 || all[1].InvalidationReason != "wal_removed" {
		t.Fatalf("all status: %+v %v", all, err)
	}
	if err := catalog.DeleteStream(ctx, f.pool, "orders"); err != nil {
		t.Fatal(err)
	}
	if rows, _ := catalog.ListStreamStatus(ctx, f.pool, ""); len(rows) != 0 {
		t.Fatalf("status rows must cascade: %+v", rows)
	}
}

func (d *flakyDialer) DialDatabase(ctx context.Context, set string, id int32, database string) (ShardConn, error) {
	if id == d.down {
		return nil, fmt.Errorf("shard %s/%d unreachable", set, id)
	}
	return d.inner.(*PgxShardDialer).DialDatabase(ctx, set, id, database)
}

func (d *flakyDialer) DialDatabaseAs(ctx context.Context, set string, id int32, database, user, password string) (ShardConn, error) {
	if id == d.down {
		return nil, fmt.Errorf("shard %s/%d unreachable", set, id)
	}
	return d.inner.(*PgxShardDialer).DialDatabaseAs(ctx, set, id, database, user, password)
}

func TestStreamAdminCreatesAndDropsSlotsOnEveryShard(t *testing.T) {
	f := newResolverFixture(t)
	ctx := context.Background()
	a := &StreamAdmin{Pool: f.pool, Shards: f.dialer}
	if _, err := a.Create(ctx, "orders", "", false, ""); err == nil {
		t.Fatal("database is required")
	}
	if _, err := a.Create(ctx, "orders", "postgres", true, "nope"); err == nil {
		t.Fatal("unknown shard set must fail")
	}
	if err := catalog.DeleteStream(ctx, f.pool, "orders"); err != nil {
		t.Fatal(err)
	}
	slots, err := a.Create(ctx, "orders", "postgres", true, "")
	if err != nil || len(slots) != 2 || slots[0].Slot != "pgshard_orders_g0" || slots[1].Slot != "pgshard_orders_g1" || slots[0].LSN == 0 {
		t.Fatalf("create: %+v %v", slots, err)
	}
	streams, _ := catalog.ListStreams(ctx, f.pool)
	if len(streams) != 1 || streams[0].State != catalog.StreamActive {
		t.Fatalf("streams: %+v", streams)
	}
	for id := range 2 {
		var twoPhase, failover bool
		var pubs int
		conn := connect(t, f.shardDSN(id))
		if err := conn.QueryRow(ctx, "SELECT two_phase, failover FROM pg_replication_slots WHERE slot_name = $1", fmt.Sprintf("pgshard_orders_g%d", id)).Scan(&twoPhase, &failover); err != nil {
			t.Fatalf("shard %d slot: %v", id, err)
		}
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM pg_publication WHERE pubname = 'pgshard_all' AND puballtables").Scan(&pubs); err != nil {
			t.Fatal(err)
		}
		if !twoPhase || !failover || pubs != 1 {
			t.Fatalf("shard %d: two_phase=%t failover=%t publications=%d", id, twoPhase, failover, pubs)
		}
	}
	if _, err := a.Create(ctx, "orders", "postgres", true, ""); err == nil {
		t.Fatal("duplicate stream must fail")
	}
	g0 := ShardRef{Set: slots[0].Shard.Set, ID: slots[0].Shard.ID}
	if lsn, err := a.createSlot(ctx, g0, "postgres", "pgshard_orders_g0", true); err != nil || lsn == 0 {
		t.Fatalf("existing slot with matching two_phase must be reused: %d %v", lsn, err)
	}
	if _, err := a.createSlot(ctx, g0, "postgres", "pgshard_orders_g0", false); err == nil || !strings.Contains(err.Error(), "two_phase") {
		t.Fatalf("existing slot with different two_phase must fail: %v", err)
	}

	// A shard that is down leaves the stream creating with the slots made so far.
	f.dialer.down = 1
	if _, err := a.Create(ctx, "events", "postgres", false, ""); err == nil {
		t.Fatal("create with shard 1 down must fail")
	}
	streams, _ = catalog.ListStreams(ctx, f.pool)
	if len(streams) != 2 || streams[0].Name != "events" || streams[0].State != catalog.StreamCreating {
		t.Fatalf("streams: %+v", streams)
	}
	if err := a.Drop(ctx, "events"); err == nil {
		t.Fatal("drop with shard 1 down must fail")
	}
	f.dialer.down = -1
	if err := a.Drop(ctx, "events"); err != nil {
		t.Fatal(err)
	}
	if err := a.Drop(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if err := a.Drop(ctx, "orders"); err != nil {
		t.Fatalf("dropping a missing stream is idempotent: %v", err)
	}
	var n int
	if err := connect(t, f.shardDSN(0)).QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots").Scan(&n); err != nil || n != 0 {
		t.Fatalf("slots left: %d %v", n, err)
	}
	if streams, _ = catalog.ListStreams(ctx, f.pool); len(streams) != 0 {
		t.Fatalf("streams left: %+v", streams)
	}
}
