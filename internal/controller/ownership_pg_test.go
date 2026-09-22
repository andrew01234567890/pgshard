package controller

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/placement"
)

// TestEveryShardRecordsTheRangeItOwns (PGS-878): the check that refuses a
// row landing on a shard its key does not hash to needs each shard to know
// its own range. The pass writes it in every user database, as the same
// inclusive bounds the router locates keys by and the reshard row filters
// select them by, and keeps it that way.
func TestEveryShardRecordsTheRangeItOwns(t *testing.T) {
	ctx := context.Background()
	f := newPlacementFixture(t)
	o := &OwnedRanges{Pool: f.pool, Shards: f.placer.Shards, Logger: slog.New(slog.DiscardHandler)}

	changed, err := o.Pass(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if changed != 2 {
		t.Fatalf("first pass changed %d rows, want one per shard", changed)
	}
	for id := range int32(2) {
		var set string
		var shard int32
		var lo, hi int64
		if err := f.app(id).QueryRow(ctx, `SELECT shard_set, shard_id, lo, hi FROM `+OwnerSchema+`.`+OwnerTable).Scan(&set, &shard, &lo, &hi); err != nil {
			t.Fatal(err)
		}
		if set != "default" || shard != id || lo != f.ranges[id].Start || hi != f.ranges[id].End {
			t.Fatalf("shard %d recorded %s/%d [%d, %d], want default/%d [%d, %d]", id, set, shard, lo, hi, id, f.ranges[id].Start, f.ranges[id].End)
		}
		// The same bounds the router places keys by: a key that hashes
		// here is inside them, and one that hashes elsewhere is not.
		for _, key := range []int64{1, 2, 3, 42, 1 << 40} {
			ks, err := placement.KeyspaceID(key)
			if err != nil {
				t.Fatal(err)
			}
			if inside := ks >= lo && ks <= hi; inside != (f.shardOf(key) == id) {
				t.Fatalf("key %d (keyspace %d) inside shard %d's recorded range: %v, but the router places it on shard %d", key, ks, id, inside, f.shardOf(key))
			}
		}
	}

	if changed, err := o.Pass(ctx); err != nil || changed != 0 {
		t.Fatalf("a second pass changed %d rows (%v), want none", changed, err)
	}
	// A row that no longer says what the shard owns is put back.
	mustExec(t, f.app(1), `UPDATE `+OwnerSchema+`.`+OwnerTable+` SET lo = 0, hi = 0`)
	if changed, err := o.Pass(ctx); err != nil || changed != 1 {
		t.Fatalf("after tampering, a pass changed %d rows (%v), want the one", changed, err)
	}

	// A set being provisioned is covered too, so a reshard target holds
	// its own range before it serves a write. One no shard of which can be
	// reached yet is reported, and holds nobody else's range back.
	tx, err := f.catalog.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	g2, _ := placement.Split(4)
	if err := catalog.MaterializeShardSet(ctx, tx, "g2", 2, catalog.ShardSetProvisioning, g2, 0); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	mustExec(t, f.app(0), `UPDATE `+OwnerSchema+`.`+OwnerTable+` SET lo = 0, hi = 0`)
	changed, err = o.Pass(ctx)
	if err == nil || !strings.Contains(err.Error(), "g2") {
		t.Fatalf("a provisioning set with no reachable shard: %v, want it reported", err)
	}
	if changed != 1 {
		t.Fatalf("with a set unreachable, the serving set's tampered row was not restored: changed %d", changed)
	}
}

// TestAReshardLeavesEachShardsOwnedRangeWhereItIs (PGS-878): the owned range
// is per shard. A reshard that dumped it into a target's schema, or
// published it to a target's subscription, would hand the target its
// source's range, and the check that reads it would refuse every row the
// target is meant to hold.
func TestAReshardLeavesEachShardsOwnedRangeWhereItIs(t *testing.T) {
	parallelPG(t)
	f := newCopyFixture(t)
	for id := range int32(2) {
		src := connect(t, f.appDSN("default", id))
		for _, sql := range ownedRangeDDL {
			mustExec(t, src, sql)
		}
		mustExec(t, src, `INSERT INTO `+OwnerSchema+`.`+OwnerTable+` (shard_set, shard_id, lo, hi) VALUES ('default', $1, $2, $3)`,
			id, f.srcRng[id].Start, f.srcRng[id].End)
	}
	f.startWorkflow()

	deadline := time.Now().Add(2 * time.Minute)
	for {
		f.pass()
		subscribed := 0
		for id := range int32(2) {
			tgt := connect(t, f.appDSN("g2", id))
			if n := queryOne[int64](t, tgt, `SELECT count(*) FROM pg_namespace WHERE nspname = $1`, OwnerSchema); n != 0 {
				t.Fatalf("target %d was given %s by the schema copy: it would own its source's range", id, OwnerSchema)
			}
			if queryOne[int64](t, tgt, `SELECT count(*) FROM pg_subscription`) > 0 {
				subscribed++
			}
		}
		for id := range int32(2) {
			src := connect(t, f.appDSN("default", id))
			if n := queryOne[int64](t, src, `SELECT count(*) FROM pg_publication_tables WHERE schemaname = $1`, OwnerSchema); n != 0 {
				pubs := queryOne[string](t, src, `SELECT string_agg(pubname, ',') FROM pg_publication_tables WHERE schemaname = $1`, OwnerSchema)
				t.Fatalf("source %d publishes %s to the targets, in %s", id, OwnerSchema, pubs)
			}
		}
		if subscribed == 2 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the copy never subscribed both targets")
		}
		time.Sleep(200 * time.Millisecond)
	}
}
