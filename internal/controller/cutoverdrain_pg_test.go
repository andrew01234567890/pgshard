package controller

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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
	if busy == 0 {
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
	if busy != 0 {
		t.Fatalf("%d backends counted: a read started after the pause cannot write and must not hold the drain", busy)
	}
}
