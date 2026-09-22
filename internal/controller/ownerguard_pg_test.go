package controller

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestARowIsRefusedOnAShardItsKeyDoesNotHashTo (PGS-878).
//
// The router places a row by the shard key in the statement. A BEFORE
// trigger that rewrote the key -- or a write made on a shard directly --
// stored the row on a shard its key does not hash to, where no lookup would
// reach it. Every sharded table now carries a check, run after every BEFORE
// trigger, that refuses such a row.
func TestARowIsRefusedOnAShardItsKeyDoesNotHashTo(t *testing.T) {
	ctx := context.Background()
	f := newPlacementFixture(t)
	for id := range int32(2) {
		mustExec(t, f.app(id), `CREATE TABLE orders (tenant_id bigint NOT NULL, id bigint NOT NULL, note text, PRIMARY KEY (tenant_id, id))`)
	}
	declareSharded(t, f, "orders")
	logger := slog.New(slog.DiscardHandler)
	if _, err := (&ShardKeyCheck{Pool: f.pool, Shards: f.placer.Shards, Logger: logger}).Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := (&OwnedRanges{Pool: f.pool, Shards: f.placer.Shards, Logger: logger}).Pass(ctx); err != nil {
		t.Fatal(err)
	}
	guard := &OwnerGuard{Pool: f.pool, Shards: f.placer.Shards, Logger: logger}
	if n, err := guard.Pass(ctx); err != nil || n != 1 {
		t.Fatalf("guard pass: %d %v, want the one table guarded", n, err)
	}
	if n, err := guard.Pass(ctx); err != nil || n != 0 {
		t.Fatalf("a second pass changed %d (%v), want nothing", n, err)
	}

	// Keys that hash to each shard.
	var home, away int64 = -1, -1
	for k := int64(1); k < 200 && (home < 0 || away < 0); k++ {
		if f.shardOf(k) == 0 && home < 0 {
			home = k
		}
		if f.shardOf(k) == 1 && away < 0 {
			away = k
		}
	}
	shard0 := f.app(0)
	refused := func(err error, code string) bool {
		var pe *pgconn.PgError
		return errors.As(err, &pe) && pe.Code == code
	}

	if _, err := shard0.Exec(ctx, `INSERT INTO orders VALUES ($1, 1, 'mine')`, home); err != nil {
		t.Fatalf("a row whose key hashes to this shard: %v", err)
	}
	if _, err := shard0.Exec(ctx, `INSERT INTO orders VALUES ($1, 1, 'not mine')`, away); !refused(err, "23514") {
		t.Fatalf("a row whose key hashes to the other shard: %v, want 23514", err)
	}
	// An UPDATE that leaves the key alone is not rehashed; one that moves it
	// away is refused.
	if _, err := shard0.Exec(ctx, `UPDATE orders SET note = 'still mine' WHERE tenant_id = $1`, home); err != nil {
		t.Fatalf("an update that leaves the key alone: %v", err)
	}
	if _, err := shard0.Exec(ctx, `UPDATE orders SET tenant_id = $2 WHERE tenant_id = $1`, home, away); !refused(err, "23514") {
		t.Fatalf("an update moving the key to the other shard: %v, want 23514", err)
	}

	// The case the ticket is about: a BEFORE trigger rewriting the key. The
	// router would have sent this INSERT here, by the key in the statement.
	// Named to sort AFTER the check: PostgreSQL fires triggers of the same
	// timing in name order, so only a check that runs after every BEFORE
	// trigger sees the key this one writes.
	mustExec(t, shard0, `CREATE FUNCTION rekey() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.tenant_id := `+strconv.FormatInt(away, 10)+`; RETURN NEW; END $$`)
	mustExec(t, shard0, `CREATE TRIGGER zzz_rekey BEFORE INSERT ON orders FOR EACH ROW EXECUTE FUNCTION rekey()`)
	if _, err := shard0.Exec(ctx, `INSERT INTO orders VALUES ($1, 2, 'rekeyed')`, home); !refused(err, "23514") {
		t.Fatalf("a row whose BEFORE trigger moved its key to the other shard: %v, want 23514", err)
	}
	mustExec(t, shard0, `DROP TRIGGER zzz_rekey ON orders`)

	// A reshard's copy applies rows with session_replication_role =
	// replica, where ordinary triggers do not fire: a target is filled
	// before it owns the range, and must not refuse its own rows.
	tx, err := shard0.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO orders VALUES ($1, 3, 'applied')`, away); err != nil {
		t.Fatalf("a replicated row: %v", err)
	}
	_ = tx.Rollback(ctx)

	// A shard that does not know its range refuses rather than guessing.
	mustExec(t, shard0, `DELETE FROM `+OwnerSchema+`.`+OwnerTable)
	if _, err := shard0.Exec(ctx, `INSERT INTO orders VALUES ($1, 4, 'unknown')`, home); !refused(err, "55000") {
		t.Fatalf("a write with no owned range recorded: %v, want 55000", err)
	}

	// No longer sharded: the check goes.
	mustExec(t, f.catalog, `UPDATE pgshard.table_status SET effective_placement = 'unsharded' WHERE table_name = 'orders'`)
	if n, err := guard.Pass(ctx); err != nil || n != 1 {
		t.Fatalf("unsharding: %d %v, want the check removed", n, err)
	}
	for id := range int32(2) {
		if n := queryOne[int64](t, f.app(id), `SELECT count(*) FROM pg_trigger WHERE tgname = $1`, GuardTrigger); n != 0 {
			t.Fatalf("shard %d still has the check on a table that is no longer sharded", id)
		}
	}
}

// TestACutoverDoesNotLetATargetServeBeforeItKnowsItsRange (PGS-878): a
// target's schema copy carries its tables' ownership checks, and each
// refuses a write until the shard knows the range it owns. The owned-range
// pass writes it on a timer; a reshard that switched before the timer fired
// would have made every target refuse every write. The cutover writes each
// target's own range at its gate. This fixture runs no owned-range pass, so
// the gate is the only thing that can have written them.
func TestACutoverDoesNotLetATargetServeBeforeItKnowsItsRange(t *testing.T) {
	parallelPG(t)
	f := newCopyFixture(t)
	id := f.startWorkflow()
	deadline := time.Now().Add(4 * time.Minute)
	var state, stage, msg string
	for {
		f.pass()
		state, stage, msg = f.workflow(id)
		if stage == StageSwitched || state == StateFailed || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if stage != StageSwitched {
		t.Fatalf("reshard did not switch: %s %s %q", state, stage, msg)
	}
	for i := range int32(2) {
		var set string
		var shard int32
		var lo, hi int64
		if err := connect(t, f.appDSN("g2", i)).QueryRow(context.Background(),
			`SELECT shard_set, shard_id, lo, hi FROM `+OwnerSchema+`.`+OwnerTable).Scan(&set, &shard, &lo, &hi); err != nil {
			t.Fatalf("target %d switched without knowing its range: %v", i, err)
		}
		if set != "g2" || shard != i || lo != f.tgtRng[i].Start || hi != f.tgtRng[i].End {
			t.Fatalf("target %d records %s/%d [%d, %d], want g2/%d [%d, %d]", i, set, shard, lo, hi, i, f.tgtRng[i].Start, f.tgtRng[i].End)
		}
	}
}
