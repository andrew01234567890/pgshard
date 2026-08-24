package controller

import (
	"context"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/placement"
	"github.com/andrew01234567890/pgshard/test/e2e/oracle"
)

// TestReshardMergeOnPostgres merges two serving shards into one target
// through copy and cutover while a ledger of transfers runs on the sources:
// every source subscribes the single target, the write switch lands, and
// the ledger total on the target is intact.
func TestReshardMergeOnPostgres(t *testing.T) {
	f := newCopyFixtureN(t, 1)
	ctx := context.Background()
	const accounts, opening = 200, int64(1000)
	srcs := []*pgx.Conn{connect(t, f.appDSN("default", 0)), connect(t, f.appDSN("default", 1))}
	byShard := map[int32][]int64{}
	for i := range int64(accounts) {
		sh := int32(f.srcRng.Locate(mustKeyspace(t, i)))
		mustExec(t, srcs[sh], `INSERT INTO accounts VALUES ($1, $2)`, i, opening)
		byShard[sh] = append(byShard[sh], i)
	}
	id := f.startWorkflow()
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET spec = spec || '{"retire_after_seconds": 1}' WHERE id = $1::uuid`, id)

	var stop atomic.Bool
	var transfers atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		conns := []*pgx.Conn{connect(t, f.appDSN("default", 0)), connect(t, f.appDSN("default", 1))}
		rng := rand.New(rand.NewPCG(3, 4))
		for !stop.Load() {
			var migrating bool
			var state string
			if err := f.pool.QueryRow(ctx, `SELECT bool_or(migrating), min(serving_state) FROM pgshard.shard_status WHERE shard_set = 'default'`).Scan(&migrating, &state); err != nil {
				continue
			}
			if state == ServingRetired {
				return
			}
			if migrating {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			sh := int32(rng.IntN(2))
			ids := byShard[sh]
			a, b := ids[rng.IntN(len(ids))], ids[rng.IntN(len(ids))]
			if a == b {
				continue
			}
			tx, err := conns[sh].Begin(ctx)
			if err != nil {
				t.Errorf("begin: %v", err)
				return
			}
			_, err = tx.Exec(ctx, `UPDATE accounts SET balance = balance - 7 WHERE id = $1`, a)
			if err == nil {
				_, err = tx.Exec(ctx, `UPDATE accounts SET balance = balance + 7 WHERE id = $1`, b)
			}
			if err == nil {
				err = tx.Commit(ctx)
			} else {
				_ = tx.Rollback(ctx)
			}
			if err != nil {
				t.Errorf("transfer: %v", err)
				return
			}
			transfers.Add(1)
		}
	}()

	deadline := time.Now().Add(4 * time.Minute)
	var state, stage, msg string
	for {
		f.pass()
		state, stage, msg = f.workflow(id)
		if stage == StageCompleted || state == StateFailed || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	stop.Store(true)
	wg.Wait()
	if stage != StageCompleted {
		t.Fatalf("merge did not complete: %s %s %q", state, stage, msg)
	}
	if transfers.Load() < 50 {
		t.Fatalf("only %d transfers ran", transfers.Load())
	}
	t.Logf("%d transfers during the merge; %s", transfers.Load(), msg)

	tgt := connect(t, f.appDSN("g2", 0))
	ledger := &oracle.Ledger{Expected: accounts * opening, Balances: func(ctx context.Context) (map[string]int64, error) {
		rows, err := tgt.Query(ctx, `SELECT id::text, balance FROM accounts`)
		if err != nil {
			return nil, err
		}
		out := map[string]int64{}
		for rows.Next() {
			var id string
			var b int64
			if err := rows.Scan(&id, &b); err != nil {
				return nil, err
			}
			out[id] = b
		}
		return out, rows.Err()
	}}
	violations, err := ledger.Check(ctx)
	if err != nil || len(violations) > 0 {
		t.Fatalf("ledger on the merged target: %v %v", violations, err)
	}
	if n := queryOne[int64](t, tgt, `SELECT count(*) FROM accounts`); n != accounts {
		t.Fatalf("accounts on target: %d", n)
	}
	for _, table := range []string{"orders", "docs"} {
		if n := queryOne[int64](t, tgt, "SELECT count(*) FROM "+table); n != 2000 {
			t.Errorf("%s on the merged target: %d rows", table, n)
		}
	}
	if n := queryOne[int64](t, tgt, `SELECT count(*) FROM items`) + queryOne[int64](t, tgt, `SELECT count(*) FROM regions`); n != 52 {
		t.Errorf("home and reference rows on the merged target: %d", n)
	}
	if got := queryOne[string](t, f.catalog, `SELECT string_agg(shard_set || ':' || state, ',' ORDER BY generation) FROM pgshard.shard_sets`); got != "default:retired,g2:serving" {
		t.Errorf("shard sets after the merge: %s", got)
	}
	if got := queryOne[string](t, f.catalog, `SELECT string_agg(shard_set || ':' || shard_id || ':' || serving_state || ':' || migrating, ',' ORDER BY shard_set, shard_id) FROM pgshard.shard_status`); got != "default:0:retired:false,default:1:retired:false,g2:0:serving:false" {
		t.Errorf("shard status after the merge: %s", got)
	}
	if n := queryOne[int64](t, f.catalog, `SELECT count(*) FROM pgshard.workflow_locks`); n != 0 {
		t.Errorf("locks left: %d", n)
	}
	var subs string
	if err := f.catalog.QueryRow(ctx, `SELECT status->'progress'->>'subscriptions' FROM pgshard.workflows WHERE id = $1::uuid`, id).Scan(&subs); err != nil || subs != "2" {
		t.Errorf("one subscription per (target, source) pair: %q %v", subs, err)
	}
	for sid := range 2 {
		c := connect(t, f.dsns[ShardRef{Set: "default", ID: int32(sid)}])
		if n := queryOne[int64](t, c, `SELECT count(*) FROM pg_replication_slots`); n != 0 {
			t.Errorf("source %d slots left: %d", sid, n)
		}
	}
	if n := queryOne[int64](t, tgt, `SELECT count(*) FROM pg_subscription`) + queryOne[int64](t, tgt, `SELECT count(*) FROM pg_replication_slots`); n != 0 {
		t.Errorf("replication objects left on the target: %d", n)
	}
	if !strings.Contains(msg, "reshard completed") {
		t.Errorf("final message: %q", msg)
	}
}

func mustKeyspace(t *testing.T, v any) int64 {
	t.Helper()
	id, err := placement.KeyspaceID(v)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
