package controller

import (
	"context"
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
