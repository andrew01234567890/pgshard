package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// realShards dials one real PostgreSQL for every shard id.
type realShards struct{ dsn string }

func (r realShards) Dial(ctx context.Context, _ string, _ int32) (ShardConn, error) {
	c, err := pgx.Connect(ctx, r.dsn)
	if err != nil {
		return nil, err
	}
	return pgxShardConn{c}, nil
}

func (r realShards) DialDatabase(ctx context.Context, set string, id int32, _ string) (ShardConn, error) {
	return r.Dial(ctx, set, id)
}

// TestTheDrainCountsATransactionThatCouldStillWrite.
//
// The drain asked which transactions HAD written -- backend_xid IS NOT NULL
// -- and a transaction takes an xid only on its first write. One that began
// before the pause and has so far only read has none, and
// default_transaction_read_only does not apply to a transaction that already
// began, so it is still read-write. The drain reported the set quiet, the
// swap sampled positions and disabled forward replication, and that
// transaction then committed on a source about to be retired.
func TestTheDrainCountsATransactionThatCouldStillWrite(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	dsn := startPostgres(t)
	o := &pgCutover{c: &Copier{Shards: realShards{dsn}}, srcSet: "default", srcIDs: []int32{0}}

	// A client that began before the pause and has only read.
	reader := connect(t, dsn)
	tx, err := reader.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var one int
	if err := tx.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil {
		t.Fatal(err)
	}
	var xid *string
	if err := reader.QueryRow(ctx, `SELECT backend_xid::text FROM pg_stat_activity WHERE pid = pg_backend_pid()`).Scan(&xid); err != nil {
		t.Fatal(err)
	}
	if xid != nil {
		t.Fatalf("the premise of this test is a transaction with no xid yet; it has %s", *xid)
	}

	// The pause lands after that transaction began.
	at, err := o.shardNow(ctx, "default", 0)
	if err != nil {
		t.Fatal(err)
	}
	o.pausedAt = map[pausedShard]time.Time{{"default", 0}: at}

	busy, err := o.writingBackends(ctx, "default", []int32{0})
	if err != nil {
		t.Fatal(err)
	}
	if len(busy) == 0 {
		t.Fatal("the drain reported the set quiet while a transaction that began before the pause was still open and still read-write")
	}

	// A transaction that begins AFTER the pause is read-only by default, so
	// a long read started during the cutover must not hold the drain up --
	// counting every open transaction would fail every cutover under load.
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	late := connect(t, dsn)
	lateTx, err := late.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lateTx.Rollback(ctx) }()
	if err := lateTx.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil {
		t.Fatal(err)
	}
	busy, err = o.writingBackends(ctx, "default", []int32{0})
	if err != nil {
		t.Fatal(err)
	}
	if len(busy) != 0 {
		t.Fatalf("%v counted: a read started after the pause cannot write and must not hold the drain", busy)
	}
}

// TestADrainStopsAtTheCutoverBudgetAndNamesItsWriters (PGS-845): before the
// journal the switch is undone once the fence has stood for the cutover
// timeout, so a drain that went on to its own 30s held the fence past an undo
// already due. And "N transactions still open" gave the operator nothing to
// find them by.
func TestADrainStopsAtTheCutoverBudgetAndNamesItsWriters(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	dsn := startPostgres(t)
	fenced := time.Now()
	o := &pgCutover{c: &Copier{Shards: realShards{dsn}, CutoverTimeout: 2 * time.Second}, srcSet: "default", srcIDs: []int32{0},
		wf: &copyWorkflow{cutover: cutoverState{FencedAt: &fenced}}}

	writer := connect(t, strings.Replace(dsn, "?", "?application_name=ledger-batch&", 1))
	tx, err := writer.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var one int
	if err := tx.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil {
		t.Fatal(err)
	}
	at, err := o.shardNow(ctx, "default", 0)
	if err != nil {
		t.Fatal(err)
	}
	o.pausedAt = map[pausedShard]time.Time{{"default", 0}: at}

	start := time.Now()
	err = o.drainWriters(ctx, "default", []int32{0})
	if err == nil {
		t.Fatal("the drain finished with a transaction from before the pause still open")
	}
	if took := time.Since(start); took > 10*time.Second || took < time.Second {
		t.Fatalf("the drain waited %s with a %s cutover budget; it should wait out what is left of it", took.Round(100*time.Millisecond), o.c.CutoverTimeout)
	}
	if !strings.Contains(err.Error(), "ledger-batch") || !strings.Contains(err.Error(), "pid ") {
		t.Fatalf("the drain did not name the transaction it waited for: %v", err)
	}
}

// TestADrainAfterARestartStillWaitsForATransactionFromBeforeThePause
// (PGS-856): the instant a switch's pause was confirmed at lived only in the
// controller's memory. A controller that died while draining under that
// pause left it standing, and the next pass found it standing, skipped
// quiescing and drained without the instant -- so a transaction that began
// before the pause, and had only read so far, was not waited for and could
// write on a source after the positions the targets were checked against.
func TestADrainAfterARestartStillWaitsForATransactionFromBeforeThePause(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	catalogDSN := startPostgres(t)
	dsn := startPostgres(t)
	cat := connect(t, catalogDSN)
	if err := catalog.Migrate(ctx, cat); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, catalogDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	const wfID = "55555555-5555-5555-5555-555555555555"
	mustExec(t, cat, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_epoch)
		VALUES ('default', 0, 'shard0', 'serving', 1)`)
	newPass := func() *pgCutover {
		return &pgCutover{c: &Copier{Pool: pool, Shards: realShards{dsn}}, wf: &copyWorkflow{id: wfID}, srcSet: "default", srcIDs: []int32{0}}
	}

	// Begun before the pause, and only read so far: no xid, still read-write.
	reader := connect(t, dsn)
	tx, err := reader.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var one int
	if err := tx.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil {
		t.Fatal(err)
	}

	if err := newPass().pauseSetClaimed(ctx, "default", []int32{0}, true); err != nil {
		t.Fatal(err)
	}
	// The controller dies here, in the middle of the drain.

	later := newPass()
	if stood, err := later.SourcesPaused(ctx); err != nil || !stood {
		t.Fatalf("the claimed pause is standing: %v %v", stood, err)
	}
	busy, err := later.writingBackends(ctx, "default", []int32{0})
	if err != nil {
		t.Fatal(err)
	}
	if len(busy) != 1 {
		t.Fatalf("a pass after a restart counted %v: the transaction from before the pause must still be waited for", busy)
	}

	// A claim whose instant was never recorded -- the controller died between
	// raising the pause and writing when -- is not one to drain under.
	mustExec(t, cat, `UPDATE pgshard.shard_status SET write_paused_at = NULL WHERE shard_set = 'default' AND shard_id = 0`)
	if stood, err := newPass().SourcesPaused(ctx); err != nil || stood {
		t.Fatalf("a claimed pause with no recorded instant was taken as standing: %v %v", stood, err)
	}

	// Raising the pause again under the standing claim forgets the old
	// instant until the new one is confirmed -- a transaction may have begun
	// while the old pause was gone -- and then records the later one.
	again := newPass()
	if err := again.pauseSetClaimed(ctx, "default", []int32{0}, true); err != nil {
		t.Fatal(err)
	}
	instant := func() *time.Time {
		t.Helper()
		var at *time.Time
		if err := cat.QueryRow(ctx, `SELECT write_paused_at FROM pgshard.shard_status WHERE shard_set = 'default' AND shard_id = 0`).Scan(&at); err != nil {
			t.Fatal(err)
		}
		return at
	}
	first := instant()
	if first == nil {
		t.Fatal("no instant recorded for the raised pause")
	}
	if err := again.claimPause(ctx, "default", []int32{0}); err != nil {
		t.Fatal(err)
	}
	if at := instant(); at != nil {
		t.Fatalf("a re-raise kept the earlier instant %v until the new one is recorded", at)
	}
	time.Sleep(10 * time.Millisecond)
	if err := again.pauseSetClaimed(ctx, "default", []int32{0}, true); err != nil {
		t.Fatal(err)
	}
	if at := instant(); at == nil || !at.After(*first) {
		t.Fatalf("the re-raised pause recorded %v, want later than %v", at, *first)
	}

	// Lifting drops the instant with the claim.
	lift := newPass()
	if err := lift.pauseSetClaimed(ctx, "default", []int32{0}, true); err != nil {
		t.Fatal(err)
	}
	if err := lift.pauseSetClaimed(ctx, "default", []int32{0}, false); err != nil {
		t.Fatal(err)
	}
	var at *time.Time
	if err := cat.QueryRow(ctx, `SELECT write_paused_at FROM pgshard.shard_status WHERE shard_set = 'default' AND shard_id = 0`).Scan(&at); err != nil || at != nil {
		t.Fatalf("the instant outlived the claim: %v %v", at, err)
	}
}
