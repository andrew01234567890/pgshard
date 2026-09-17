package controller

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/placement"
)

// placementFixture is a catalog and two serving shards; placement
// workflows move rows between the shards themselves, so no network or
// subscription is involved.
type placementFixture struct {
	t       *testing.T
	pool    *pgxpool.Pool
	catalog *pgx.Conn
	dsns    map[ShardRef]string
	ranges  placement.RangeSet
	placer  *Placer
}

func newPlacementFixture(t *testing.T) *placementFixture {
	t.Helper()
	ctx := context.Background()
	f := &placementFixture{t: t, dsns: map[ShardRef]string{}}
	catalogDSN := startPostgresWith(t)
	f.catalog = connect(t, catalogDSN)
	if err := catalog.Migrate(ctx, f.catalog); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, catalogDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	f.pool = pool
	for id := range 2 {
		f.dsns[ShardRef{Set: "default", ID: int32(id)}] = startPostgresImage(t, pgImage, nil, logicalOpts...)
	}
	f.ranges, _ = placement.Split(2)
	tx, err := f.catalog.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.MaterializeShardSet(ctx, tx, "default", 1, catalog.ShardSetServing, f.ranges, 0); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	for id := range 2 {
		mustExec(t, f.catalog, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_epoch, primary_endpoint)
			VALUES ('default', $1, $2, 'serving', 1, $3)`, id, fmt.Sprintf("shard%d", id), fmt.Sprintf("shard%d:5432", id))
		c := connect(t, f.dsns[ShardRef{Set: "default", ID: int32(id)}])
		mustExec(t, c, `CREATE DATABASE app`)
	}
	mustExec(t, f.catalog, `INSERT INTO pgshard.serving (shard_set, generation) SELECT 'default', max(desired_generation) FROM pgshard.shard_ranges`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.databases (name, default_placement, home_shard) VALUES ('app', 'unsharded', 0)`)
	f.placer = &Placer{Pool: pool, Shards: &PgxShardDialer{Pool: pool, DSNs: f.dsns}, LagBytes: 1 << 20, BufferTimeout: 20 * time.Second, CopyBatch: 700}
	return f
}

func (f *placementFixture) app(id int32) *pgx.Conn {
	return connect(f.t, strings.Replace(f.dsns[ShardRef{Set: "default", ID: id}], "/postgres?", "/app?", 1))
}

func (f *placementFixture) shardOf(v any) int32 {
	id, err := placement.KeyspaceID(v)
	if err != nil {
		f.t.Fatal(err)
	}
	return int32(f.ranges.Locate(id))
}

func (f *placementFixture) reconcile() Result {
	f.t.Helper()
	return reconcile(f.t, f.catalog)
}

func (f *placementFixture) workflow(table string) (id, state, stage, message string) {
	f.t.Helper()
	err := f.catalog.QueryRow(context.Background(), `SELECT id::text, state, coalesce(status->>'stage', ''), coalesce(status->>'message', '')
		FROM pgshard.workflows WHERE kind = 'table_placement' AND spec->>'table_name' = $1 ORDER BY created_at DESC LIMIT 1`, table).Scan(&id, &state, &stage, &message)
	if err != nil {
		f.t.Fatal(err)
	}
	return
}

// driveUntil runs passes until the workflow of table reaches one of stages
// or fails.
func (f *placementFixture) driveUntil(table string, timeout time.Duration, stages ...string) (id, stage string) {
	f.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if _, err := f.placer.Pass(context.Background()); err != nil {
			f.t.Fatal(err)
		}
		var state, msg string
		id, state, stage, msg = f.workflow(table)
		for _, s := range stages {
			if stage == s {
				return id, stage
			}
		}
		if state == StateFailed || state == StateCancelled || time.Now().After(deadline) {
			f.t.Fatalf("workflow on %s: %s %s %q", table, state, stage, msg)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (f *placementFixture) load(id string) *placementWorkflow {
	f.t.Helper()
	wfs, err := f.placer.list(context.Background())
	if err != nil {
		f.t.Fatal(err)
	}
	for i := range wfs {
		if wfs[i].id == id {
			if err := f.placer.load(context.Background(), &wfs[i]); err != nil {
				f.t.Fatal(err)
			}
			return &wfs[i]
		}
	}
	f.t.Fatalf("workflow %s not listed", id)
	return nil
}

type orderRow struct {
	ID, Tenant, Region int64
	Note               string
}

func (f *placementFixture) orders(conn *pgx.Conn, table string) []orderRow {
	f.t.Helper()
	rows, err := conn.Query(context.Background(), "SELECT id, tenant_id, region_id, note FROM "+table)
	if err != nil {
		f.t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByPos[orderRow])
	if err != nil {
		f.t.Fatal(err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// TestPlacementRekeyOnPostgres re-keys a 10k-row sharded table under
// concurrent inserts, updates (including shard key changes) and deletes:
// the writers pause only while the table fence is up, no write is lost,
// and every row ends on the shard its new key hashes to.
func TestPlacementRekeyOnPostgres(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	for id := range int32(2) {
		c := f.app(id)
		mustExec(t, c, `CREATE TABLE orders (id bigint NOT NULL, tenant_id bigint NOT NULL, region_id bigint NOT NULL, note text, PRIMARY KEY (id, tenant_id, region_id))`)
		mustExec(t, c, `CREATE INDEX orders_note_idx ON orders (note)`)
		mustExec(t, c, `CREATE TABLE tickets (id bigserial PRIMARY KEY, body text)`)
	}
	conns := []*pgx.Conn{f.app(0), f.app(1)}
	for i := range int64(10000) {
		tenant, region := i*7919+13, i%97
		mustExec(t, conns[f.shardOf(tenant)], `INSERT INTO orders VALUES ($1, $2, $3, $4)`, i, tenant, region, fmt.Sprintf("n%d", i))
	}
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('app', 'public', 'orders', 'sharded', 'tenant_id')`)
	if res := f.reconcile(); res.TablesMadeEffective != 1 {
		t.Fatalf("%+v", res)
	}
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET shard_key = 'region_id' WHERE table_name = 'orders'`)
	if res := f.reconcile(); res.WorkflowsCreated != 1 {
		t.Fatalf("%+v", res)
	}

	var stop, paused atomic.Bool
	var writes, pauses atomic.Int64
	var maxPause atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		wconns := []*pgx.Conn{f.app(0), f.app(1)}
		rng := rand.New(rand.NewPCG(1, 2))
		next := int64(10000)
		for !stop.Load() {
			var migrating bool
			var stage string
			if err := f.pool.QueryRow(ctx, `SELECT migrating, coalesce((SELECT status->>'stage' FROM pgshard.workflows WHERE kind = 'table_placement' LIMIT 1), '')
				FROM pgshard.table_status WHERE table_name = 'orders'`).Scan(&migrating, &stage); err != nil {
				continue
			}
			if stage == StagePlacementSwapping || stage == StagePlacementRetiring {
				return
			}
			if migrating {
				if !paused.Swap(true) {
					pauses.Add(1)
				}
				started := time.Now()
				for migrating && !stop.Load() {
					time.Sleep(20 * time.Millisecond)
					_ = f.pool.QueryRow(ctx, `SELECT migrating FROM pgshard.table_status WHERE table_name = 'orders'`).Scan(&migrating)
				}
				if d := time.Since(started).Milliseconds(); d > maxPause.Load() {
					maxPause.Store(d)
				}
				paused.Store(false)
				continue
			}
			i := rng.Int64N(next)
			tenant := i*7919 + 13
			c := wconns[f.shardOf(tenant)]
			var err error
			switch rng.IntN(4) {
			case 0:
				id := next
				next++
				_, err = c.Exec(ctx, `INSERT INTO orders VALUES ($1, $2, $3, $4)`, id, id*7919+13, id%97, fmt.Sprintf("n%d", id))
			case 1:
				_, err = c.Exec(ctx, `UPDATE orders SET note = note || '+' WHERE id = $1`, i)
			case 2:
				_, err = c.Exec(ctx, `UPDATE orders SET region_id = (region_id + 31) % 97 WHERE id = $1 AND id % 5 = 0`, i)
			case 3:
				_, err = c.Exec(ctx, `DELETE FROM orders WHERE id = $1 AND id % 7 = 0`, i)
			}
			if err != nil {
				t.Errorf("writer: %v", err)
				return
			}
			writes.Add(1)
		}
	}()

	id, _ := f.driveUntil("orders", time.Minute, StagePlacementCopying)
	wf := f.load(id)
	if err := f.placer.ensureShadows(ctx, wf); err != nil {
		t.Fatalf("second ensureShadows: %v", err)
	}
	f.driveUntil("orders", 3*time.Minute, StagePlacementCatchUp)
	wf = f.load(id)
	wf.st.Copied = map[string]bool{}
	if err := f.placer.copyAll(ctx, wf); err != nil {
		t.Fatalf("second copyAll: %v", err)
	}

	if n := queryOne[int64](t, conns[0], `SELECT count(*) FROM (SELECT id FROM orders__pgshard_new GROUP BY id, tenant_id, region_id HAVING count(*) > 1) d`); n != 0 {
		t.Fatalf("duplicate rows after a repeated copy: %d", n)
	}
	f.driveUntil("orders", 3*time.Minute, StagePlacementRetiring)
	stop.Store(true)
	wg.Wait()
	if writes.Load() < 100 {
		t.Fatalf("only %d concurrent writes ran", writes.Load())
	}
	t.Logf("%d concurrent writes, %d pauses, longest %dms", writes.Load(), pauses.Load(), maxPause.Load())

	var expected []orderRow
	for id := range int32(2) {
		expected = append(expected, f.orders(conns[id], "orders__pgshard_old")...)
	}
	want := map[int32][]orderRow{}
	for _, r := range expected {
		want[f.shardOf(r.Region)] = append(want[f.shardOf(r.Region)], r)
	}
	for id := range int32(2) {
		got := f.orders(conns[id], "orders")
		w := want[id]
		sort.Slice(w, func(i, j int) bool { return w[i].ID < w[j].ID })
		if len(got) != len(w) {
			have := map[orderRow]bool{}
			for _, r := range got {
				have[r] = true
			}
			extra := map[orderRow]bool{}
			for _, r := range got {
				extra[r] = true
			}
			missing := 0
			for _, r := range w {
				if !have[r] {
					missing++
					if missing <= 5 {
						t.Logf("missing %+v; on other shard: %d", r, queryOne[int64](t, conns[1-id], `SELECT count(*) FROM orders WHERE id = $1`, r.ID))
					}
				}
				delete(extra, r)
			}
			n := 0
			for r := range extra {
				if n < 5 {
					t.Logf("extra %+v", r)
				}
				n++
			}
			t.Fatalf("shard %d: %d rows, want %d (missing %d, extra %d)", id, len(got), len(w), missing, len(extra))
		}
		for i := range got {
			if got[i] != w[i] {
				t.Fatalf("shard %d row %d: %+v want %+v", id, i, got[i], w[i])
			}
		}
	}
	if len(expected) < 10000-2000 || len(expected) > 10000+2000 {
		t.Fatalf("unexpected row count %d", len(expected))
	}
	var eff, key string
	var migrating bool
	var gen int64
	if err := f.catalog.QueryRow(ctx, `SELECT effective_placement, effective_shard_key, migrating, effective_generation FROM pgshard.table_status WHERE table_name = 'orders'`).Scan(&eff, &key, &migrating, &gen); err != nil {
		t.Fatal(err)
	}
	if eff != "sharded" || key != "region_id" || migrating || gen == 0 {
		t.Fatalf("table_status: %s %s %v %d", eff, key, migrating, gen)
	}
	if n := queryOne[int64](t, f.catalog, `SELECT count(*) FROM pgshard.workflow_locks`); n != 0 {
		t.Fatalf("locks left: %d", n)
	}
	var pauseMS int64
	if err := f.catalog.QueryRow(ctx, `SELECT (status->'placement'->>'pause_ms')::bigint FROM pgshard.workflows WHERE id = $1::uuid`, id).Scan(&pauseMS); err != nil {
		t.Fatal(err)
	}
	if pauseMS <= 0 || pauseMS > 20000 {
		t.Fatalf("pause_ms %d", pauseMS)
	}
	t.Logf("table write pause %dms", pauseMS)
	for id := range int32(2) {
		if n := queryOne[int64](t, conns[id], `SELECT count(*) FROM pg_replication_slots`); n != 0 {
			t.Errorf("shard %d slots left: %d", id, n)
		}
		if n := queryOne[int64](t, conns[id], `SELECT count(*) FROM pg_publication`); n != 0 {
			t.Errorf("shard %d publications left: %d", id, n)
		}
	}
	// A repeated swap after the swap is a no-op.
	wf = f.load(id)
	if err := f.placer.swapAll(ctx, wf); err != nil {
		t.Fatalf("second swap: %v", err)
	}
	if res := f.reconcile(); res.WorkflowsCreated != 0 || res.PlacementsCancelled != 0 {
		t.Fatalf("reconcile after swap: %+v", res)
	}

	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET spec = spec || '{"drop_old_after_seconds": 0}' WHERE id = $1::uuid`, id)
	f.driveUntil("orders", time.Minute, StageCompleted)
	for id := range int32(2) {
		if n := queryOne[int64](t, conns[id], `SELECT count(*) FROM pg_tables WHERE tablename LIKE 'orders%'`); n != 1 {
			t.Errorf("shard %d tables named orders*: %d", id, n)
		}
		if n := queryOne[int64](t, conns[id], `SELECT count(*) FROM pg_indexes WHERE tablename = 'orders' AND indexname IN ('orders_pkey', 'orders_note_idx')`); n != 2 {
			t.Errorf("shard %d final index names: %d of 2", id, n)
		}
		if ident := queryOne[string](t, conns[id], `SELECT relreplident::text FROM pg_class WHERE relname = 'orders'`); ident != "d" {
			t.Errorf("shard %d replica identity %s", id, ident)
		}
	}
}

// TestPlacementMovesOnPostgres moves an unsharded table to sharded (the
// shadow is built from the source's definition on the shard that never
// had it), then to reference, cancels a run before its swap, and fails
// runs whose shard key is missing or not covered by a unique constraint.
func TestPlacementMovesOnPostgres(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	home := f.app(0)
	other := f.app(1)
	mustExec(t, home, `CREATE TABLE items (id serial PRIMARY KEY, v text NOT NULL DEFAULT 'x', n int CHECK (n >= 0))`)
	mustExec(t, home, `CREATE INDEX items_v_idx ON items (v)`)
	mustExec(t, home, `INSERT INTO items (v, n) SELECT 'item-' || g, g FROM generate_series(1, 50) g`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('app', 'public', 'items', 'unsharded', NULL)`)
	f.reconcile()

	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'sharded', shard_key = 'id' WHERE table_name = 'items'`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET spec = spec || '{"drop_old_after_seconds": 0}'`)
	f.driveUntil("items", 2*time.Minute, StageCompleted)
	// The queue shows how long an operation has been running from
	// status.started_at. Only a copy wrote it, so every table placement
	// reported that it started when it was created: a move that waited two
	// days behind a reshard said it had been going for two days the moment
	// it began.
	if started := queryOne[bool](t, f.catalog, `SELECT status ? 'started_at' AND (status->>'started_at')::timestamptz >= created_at
		FROM pgshard.workflows WHERE kind = 'table_placement'`); !started {
		t.Error("the placement recorded no started_at, so the queue reads its age from when it was created")
	}
	if late := queryOne[bool](t, f.catalog, `SELECT (status->>'started_at')::timestamptz > updated_at
		FROM pgshard.workflows WHERE kind = 'table_placement'`); late {
		t.Error("started_at moved with the last status write; it is stamped once, when the move leaves preparing")
	}
	if v := queryOne[int64](t, home, `INSERT INTO items (v, n) VALUES ('from-sequence', 2) RETURNING id`); v != 51 {
		t.Fatalf("the sequence must survive the old table's drop: next id %d", v)
	}
	mustExec(t, home, `DELETE FROM items WHERE id = 51`)
	mustExec(t, f.app(f.shardOf(int64(51))), `INSERT INTO items (id, v, n) VALUES (51, 'late', 1)`)
	total := int64(0)
	for id := range int32(2) {
		c := f.app(id)
		n := queryOne[int64](t, c, `SELECT count(*) FROM items`)
		total += n
		stray := queryOne[int64](t, c, fmt.Sprintf(`SELECT count(*) FROM items WHERE NOT (%s)`, RangeFilter("hashint8extended(id::int8, 8816678312871386365)", f.ranges[id])))
		if stray != 0 {
			t.Errorf("shard %d holds %d rows outside its range", id, stray)
		}
	}
	if total != 51 {
		t.Fatalf("items across shards: %d", total)
	}
	if v := queryOne[string](t, other, `SELECT string_agg(conname, ',' ORDER BY conname) FROM pg_constraint WHERE conrelid = 'items'::regclass`); v != "items_id_not_null,items_n_check,items_pkey,items_v_not_null" {
		t.Errorf("constraints on the built shadow: %s", v)
	}
	if n := queryOne[int64](t, other, `SELECT count(*) FROM pg_indexes WHERE tablename = 'items' AND indexname = 'items_v_idx'`); n != 1 {
		t.Errorf("index on the built shadow missing")
	}
	if def := queryOne[string](t, other, `SELECT column_default FROM information_schema.columns WHERE table_name = 'items' AND column_name = 'id'`); !strings.Contains(def, "nextval") {
		t.Errorf("serial default not carried: %s", def)
	}
	if eff := queryOne[string](t, f.catalog, `SELECT effective_placement || ':' || effective_shard_key FROM pgshard.table_status WHERE table_name = 'items'`); eff != "sharded:id" {
		t.Fatalf("effective: %s", eff)
	}

	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'reference', shard_key = NULL WHERE table_name = 'items'`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET spec = spec || '{"drop_old_after_seconds": 0}' WHERE state = 'pending'`)
	f.driveUntil("items", 2*time.Minute, StageCompleted)
	for id := range int32(2) {
		if n := queryOne[int64](t, f.app(id), `SELECT count(*) FROM items`); n != 51 {
			t.Errorf("shard %d reference rows: %d", id, n)
		}
	}
	if eff := queryOne[string](t, f.catalog, `SELECT effective_placement FROM pgshard.table_status WHERE table_name = 'items'`); eff != "reference" {
		t.Fatalf("effective: %s", eff)
	}

	// Cancel: the desired placement reverts while the run copies.
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'unsharded' WHERE table_name = 'items'`)
	f.reconcile()
	id, _ := f.driveUntil("items", time.Minute, StagePlacementCatchUp)
	if n := queryOne[int64](t, other, `SELECT count(*) FROM pg_replication_slots`); n != 0 {
		t.Fatalf("a reference table copies from its home shard only; slots on shard 1: %d", n)
	}
	if n := queryOne[int64](t, home, `SELECT count(*) FROM pg_replication_slots`); n != 1 {
		t.Fatalf("slots on the home shard: %d", n)
	}
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'reference' WHERE table_name = 'items'`)
	if res := f.reconcile(); res.PlacementsCancelled != 1 {
		t.Fatalf("%+v", res)
	}
	if out, err := f.placer.Pass(ctx); err != nil || out.Cancelled != 1 {
		t.Fatalf("cancel pass: %+v %v", out, err)
	}
	_, state, stage, _ := f.workflow("items")
	if state != StateCancelled || stage != StageCancelled {
		t.Fatalf("after cancel: %s %s", state, stage)
	}
	for id := range int32(2) {
		c := f.app(id)
		if n := queryOne[int64](t, c, `SELECT count(*) FROM pg_tables WHERE tablename LIKE 'items%'`); n != 1 {
			t.Errorf("shard %d tables after cancel: %d", id, n)
		}
		if n := queryOne[int64](t, c, `SELECT count(*) FROM pg_replication_slots`) + queryOne[int64](t, c, `SELECT count(*) FROM pg_publication`); n != 0 {
			t.Errorf("shard %d replication objects after cancel: %d", id, n)
		}
	}
	if n := queryOne[int64](t, f.catalog, `SELECT count(*) FROM pgshard.workflow_locks`); n != 0 {
		t.Fatalf("locks after cancel: %d", n)
	}
	if ident := queryOne[string](t, home, `SELECT relreplident::text FROM pg_class WHERE relname = 'items'`); ident != "d" {
		t.Errorf("replica identity after cancel: %s", ident)
	}
	if out, err := f.placer.Pass(ctx); err != nil || out.Driven != 0 {
		t.Fatalf("cancelled workflow %s driven again: %+v %v", id, out, err)
	}

	// Refusals.
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'sharded', shard_key = 'nope' WHERE table_name = 'items'`)
	f.reconcile()
	if _, err := f.placer.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if _, state, _, msg := f.workflow("items"); state != StateFailed || !strings.Contains(msg, `shard key column "nope" does not exist`) {
		t.Fatalf("missing column: %s %q", state, msg)
	}
	if res := f.reconcile(); res.WorkflowsCreated != 0 {
		t.Fatalf("failed change retried: %+v", res)
	}
	// A shard key covered by SOME unique constraint but absent from the
	// primary key must still be refused: PRIMARY KEY(id) cannot stay global
	// once rows split by v, even though UNIQUE(v) contains the shard key.
	mustExec(t, home, `CREATE UNIQUE INDEX items_v_uq ON items (v)`)
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET shard_key = 'v' WHERE table_name = 'items'`)
	f.reconcile()
	if _, err := f.placer.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if _, state, _, msg := f.workflow("items"); state != StateFailed || !strings.Contains(msg, "every global uniqueness key must contain the shard key") {
		t.Fatalf("uncovered key: %s %q", state, msg)
	}
	if n := queryOne[int64](t, f.catalog, `SELECT count(*) FROM pgshard.workflow_locks`); n != 0 {
		t.Fatalf("locks after failures: %d", n)
	}
}

// TestPlacementBackslashKeysAndLateWriteOnPostgres moves a table with
// backslash-bearing text keys to reference placement: the keyset resume
// bound must not mangle the backslash (skipped rows fail the pre-swap
// verification), and a write that lands on the source after the drain but
// before the swap lock must be carried into the shadow before the rename.
func TestPlacementBackslashKeysAndLateWriteOnPostgres(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	f.placer.CopyBatch = 7
	src := f.app(0)
	mustExec(t, src, `CREATE TABLE notes (id text PRIMARY KEY, v text)`)
	for i := range 60 {
		mustExec(t, src, `INSERT INTO notes VALUES ($1, $2)`, fmt.Sprintf(`k\%03d`, i), fmt.Sprintf("v%d", i))
	}
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', 'notes', 'unsharded')`)
	if res := f.reconcile(); res.TablesMadeEffective != 1 {
		t.Fatalf("%+v", res)
	}
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'reference' WHERE table_name = 'notes'`)
	if res := f.reconcile(); res.WorkflowsCreated != 1 {
		t.Fatalf("%+v", res)
	}

	id, _ := f.driveUntil("notes", 2*time.Minute, StagePlacementSwapping)
	wf := f.load(id)
	mustExec(t, src, `INSERT INTO notes VALUES ('late', 'after-drain')`)
	if err := f.placer.verifyPlacement(ctx, wf); err == nil || !isFatal(err) {
		t.Fatalf("verification must flag the shadow behind the source: %v", err)
	}
	if err := f.placer.swapAll(ctx, wf); err != nil {
		t.Fatalf("swapAll: %v", err)
	}
	for id := range int32(2) {
		c := f.app(id)
		if n := queryOne[int64](t, c, `SELECT count(*) FROM notes WHERE id = 'late'`); n != 1 {
			t.Fatalf("shard %d: late write lost by the swap", id)
		}
		if n := queryOne[int64](t, c, `SELECT count(*) FROM notes WHERE id LIKE 'k%'`); n != 60 {
			t.Fatalf("shard %d holds %d of 60 backslash-keyed rows", id, n)
		}
	}
	f.driveUntil("notes", time.Minute, StagePlacementRetiring)
}

// TestAFailedMoveDropsItsShadowsSoTheNextMoveStarts: a move that fails after
// building its shadows, before any shard began its swap, drops them. Saving
// the table's row again then moves the table; before, the new workflow
// refused to build over a shadow the failed one had marked, and only
// dropping a pgshard artifact by hand let it start (PGS-839).
func TestAFailedMoveDropsItsShadowsSoTheNextMoveStarts(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	home := f.app(0)
	mustExec(t, home, `CREATE TABLE ledger (id bigint PRIMARY KEY, v text)`)
	mustExec(t, home, `INSERT INTO ledger SELECT g, 'v' || g FROM generate_series(1, 50) g`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', 'ledger', 'unsharded')`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'sharded', shard_key = 'id' WHERE table_name = 'ledger'`)
	f.reconcile()
	failed, _ := f.driveUntil("ledger", 2*time.Minute, StagePlacementSwapping)

	shadows := func() int64 {
		var n int64
		for s := range int32(2) {
			n += queryOne[int64](t, f.app(s), `SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename = $1`, "ledger"+ShadowSuffix)
		}
		return n
	}
	if shadows() != 2 {
		t.Fatalf("%d shadows before the failure, want one per shard", shadows())
	}
	// A shadow short of a row fails the verification that precedes the
	// first swap.
	mustExec(t, f.app(f.shardOf(int64(7))), `DELETE FROM ledger`+ShadowSuffix+` WHERE id = 7`)
	if _, err := f.placer.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if id, state, _, msg := f.workflow("ledger"); id != failed || state != StateFailed || !strings.Contains(msg, "verification") {
		t.Fatalf("workflow %s is %s (%q), want %s failed by the verification", id, state, msg, failed)
	}
	if n := shadows(); n != 0 {
		t.Fatalf("%d shadow(s) left behind by the failed move", n)
	}

	retry := func() string {
		t.Helper()
		mustExec(t, f.catalog, `UPDATE pgshard.tables SET shard_key = 'id' WHERE table_name = 'ledger'`)
		if res := f.reconcile(); res.WorkflowsCreated != 1 {
			t.Fatalf("saving the row again did not start a move: %+v", res)
		}
		id, _, _, _ := f.workflow("ledger")
		return id
	}

	// A shard the failed move could not reach keeps that move's shadow. The
	// next move refuses to build over it, and says which workflow left it
	// instead of suggesting it may be the user's.
	other := f.app(1)
	mustExec(t, other, `CREATE TABLE ledger`+ShadowSuffix+` (id bigint)`)
	mustExec(t, other, `COMMENT ON TABLE ledger`+ShadowSuffix+` IS 'pgshard:placement:`+failed+`'`)
	refused := retry()
	for range 20 {
		if _, err := f.placer.Pass(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, state, _, _ := f.workflow("ledger"); state == StateFailed {
			break
		}
	}
	if id, state, _, msg := f.workflow("ledger"); id != refused || state != StateFailed ||
		!strings.Contains(msg, "left by placement workflow "+failed+" of app.public.ledger, which ended failed") || !strings.Contains(msg, "row in pgshard.tables again") {
		t.Fatalf("workflow %s is %s (%q), want %s refused naming %s", id, state, msg, refused, failed)
	}
	if n := queryOne[int64](t, other, `SELECT count(*) FROM pg_tables WHERE tablename = $1`, "ledger"+ShadowSuffix); n != 1 {
		t.Fatalf("the refused move dropped a shadow it did not mark: %d left", n)
	}
	if n := shadows(); n != 1 {
		t.Fatalf("%d shadows after the refused move, want only the one it did not mark", n)
	}

	mustExec(t, other, `DROP TABLE ledger`+ShadowSuffix)
	if id := retry(); id == failed || id == refused {
		t.Fatal("a finished workflow was driven again instead of a new one")
	}
	f.driveUntil("ledger", 2*time.Minute, StagePlacementRetiring)
	var rows int64
	for s := range int32(2) {
		rows += queryOne[int64](t, f.app(s), `SELECT count(*) FROM ledger`)
	}
	if rows != 50 {
		t.Fatalf("the retried move placed %d of 50 rows", rows)
	}
}

// TestPlacementVerifyHolderShadowsOnPostgres: verification is keyed to the
// holders of the new placement, not the sources. A sharded-to-unsharded
// move (where a source holds no shadow) must still verify and flag a
// short shadow. A holder shadow missing before any swap began must fail
// closed — never publish the home shard's old slice as the whole table —
// and only the durable swap marker lets a resumed run skip the holders it
// covers.
func TestPlacementVerifyHolderShadowsOnPostgres(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	home := f.app(0)
	mustExec(t, home, `CREATE TABLE gear (id bigint PRIMARY KEY, v text)`)
	mustExec(t, home, `INSERT INTO gear SELECT g, 'g' || g FROM generate_series(1, 40) g`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('app', 'public', 'gear', 'unsharded', NULL)`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'sharded', shard_key = 'id' WHERE table_name = 'gear'`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET spec = spec || '{"drop_old_after_seconds": 0}'`)
	f.driveUntil("gear", 2*time.Minute, StageCompleted)

	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'unsharded', shard_key = NULL WHERE table_name = 'gear'`)
	f.reconcile()
	id, _ := f.driveUntil("gear", 2*time.Minute, StagePlacementSwapping)
	wf := f.load(id)
	mustExec(t, home, `DELETE FROM gear`+ShadowSuffix+` WHERE id = 7`)
	if err := f.placer.verifyPlacement(ctx, wf); err == nil || !isFatal(err) {
		t.Fatalf("a short shadow on the sole holder must fail verification: %v", err)
	}
	mustExec(t, home, `INSERT INTO gear`+ShadowSuffix+` VALUES (7, 'g7')`)
	if err := f.placer.verifyPlacement(ctx, wf); err != nil {
		t.Fatalf("repaired shadow must verify: %v", err)
	}

	// A missing holder shadow with no swap marker is a lost shadow, not a
	// resumed swap: both the verification and the swap must fail closed
	// instead of publishing the old table as the new placement.
	mustExec(t, home, `ALTER TABLE gear`+ShadowSuffix+` RENAME TO gear__renamed_by_swap`)
	if err := f.placer.verifyPlacement(ctx, wf); err == nil || !isFatal(err) {
		t.Fatalf("a missing holder shadow before any swap must fail closed: %v", err)
	}
	if err := f.placer.swapAll(ctx, wf); err == nil || !isFatal(err) {
		t.Fatalf("swapAll without the shadow or a marker must fail closed: %v", err)
	}
	mustExec(t, home, `ALTER TABLE gear__renamed_by_swap RENAME TO gear`+ShadowSuffix)

	// A genuine crash mid-swap: the marker is persisted before the first
	// rename, so a resume skips only the holders it covers and completes.
	holder := wf.rt.Holders()[0]
	renameShadowAsSwapWould(t, f, wf, holder)
	wf.st.Swapped = append(wf.st.Swapped, holder)
	if err := f.placer.save(ctx, wf, "test: marker persisted before the rename"); err != nil {
		t.Fatal(err)
	}
	wf = f.load(id)
	if err := f.placer.verifyPlacement(ctx, wf); err != nil {
		t.Fatalf("a marker-covered holder must skip verification: %v", err)
	}
	if err := f.placer.swapAll(ctx, wf); err != nil {
		t.Fatalf("swapAll resume: %v", err)
	}
	f.driveUntil("gear", 2*time.Minute, StagePlacementRetiring, StageCompleted)
}

// renameShadowAsSwapWould replays the renames of swapOn on one shard, as a
// swap interrupted after its commit would leave them.
func renameShadowAsSwapWould(t *testing.T, f *placementFixture, wf *placementWorkflow, shard int32) {
	t.Helper()
	c := f.app(shard)
	mustExec(t, c, `ALTER TABLE `+wf.spec.TableName+` RENAME TO `+wf.old())
	mustExec(t, c, `ALTER TABLE `+wf.shadow()+` RENAME TO `+wf.spec.TableName)
}

// TestUniqueConstraintsMissingKeyOnPostgres exercises the sharding-safety
// check against every constraint shape that must contain the shard key: a
// covering unique key is safe, a primary key that omits the key is not, an
// INCLUDE-only column does not count, and an exclusion constraint is safe only
// when the shard key is compared with equality.
func TestUniqueConstraintsMissingKeyOnPostgres(t *testing.T) {
	parallelPG(t)
	dsn := startPostgres(t)
	conn := connect(t, dsn)
	ctx := context.Background()
	for _, stmt := range []string{
		`CREATE EXTENSION IF NOT EXISTS btree_gist`,
		`CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)`,
		`CREATE TABLE pk_omits (id int PRIMARY KEY, v int UNIQUE)`,
		`CREATE TABLE pk_covers (id int, v int, PRIMARY KEY (id, v))`,
		`CREATE TABLE include_only (id int, v int, PRIMARY KEY (id, v), UNIQUE (id) INCLUDE (v))`,
		`CREATE TABLE nondet (id int, t text COLLATE ci, PRIMARY KEY (t))`,
		`CREATE TABLE excl_eq (id int, v int, PRIMARY KEY (v), EXCLUDE USING btree (v WITH =))`,
		`CREATE TABLE excl_overlap (id int, span int4range, EXCLUDE USING gist (id WITH =, span WITH &&))`,
		`CREATE TABLE temporal (id int, valid int4range, PRIMARY KEY (id, valid WITHOUT OVERLAPS))`,
	} {
		mustExec(t, conn, stmt)
	}
	cases := []struct {
		table, key string
		want       []string
	}{
		{"pk_omits", "v", []string{"pk_omits_pkey"}},             // PK(id) cannot stay global
		{"pk_covers", "v", nil},                                  // v is a PK key column
		{"include_only", "v", []string{"include_only_id_v_key"}}, // v is only a covering column of the unique index
		{"nondet", "t", []string{"nondet_pkey"}},                 // nondeterministic collation != raw-hash equality
		// An exclusion is per-shard safe when the shard key's own element
		// is compared with equality: rows with different keys can never
		// conflict, wherever they live.
		{"excl_eq", "v", nil},
		{"excl_overlap", "id", nil},
		// Sharding by the overlapping element is not: two spans that
		// overlap can land on different shards, and neither sees the other.
		{"excl_overlap", "span", []string{"excl_overlap_id_span_excl"}},
		// A temporal PRIMARY KEY is an exclusion index; the scalar part is
		// equality, the period part is not.
		{"temporal", "id", nil},
		{"temporal", "valid", []string{"temporal_pkey"}},
	}
	for _, c := range cases {
		got, err := uniqueConstraintsMissingKey(ctx, pgxShardConn{conn}, "public", c.table, c.key)
		if err != nil {
			t.Fatalf("%s/%s: %v", c.table, c.key, err)
		}
		if !slices.Equal(got, c.want) {
			t.Errorf("%s sharded by %s: uncovered = %v, want %v", c.table, c.key, got, c.want)
		}
	}
}

// TestPlacementIdentityColumnsOnPostgres moves a table with GENERATED ALWAYS
// and GENERATED BY DEFAULT identity columns to sharded and asserts the copy
// overrides the system value and that each shard's identity sequences are
// advanced past the copied rows, so a fresh insert neither collides nor is
// rejected.
func TestPlacementIdentityColumnsOnPostgres(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	home := f.app(0)
	mustExec(t, home, `CREATE TABLE things (
		tenant bigint NOT NULL,
		seq bigint GENERATED BY DEFAULT AS IDENTITY,
		tag bigint GENERATED ALWAYS AS IDENTITY (INCREMENT BY 10),
		ser bigserial,
		small smallserial,
		note text,
		twice bigint GENERATED ALWAYS AS (tenant * 2) STORED,
		code text COLLATE "C",
		PRIMARY KEY (tenant, seq))`)
	mustExec(t, home, `INSERT INTO things (tenant, note) SELECT g, 'n' || g FROM generate_series(1, 60) g`)
	const thingsComment = `path C:\x and an ' apostrophe`
	mustExec(t, home, `COMMENT ON TABLE things IS `+quoteLiteralE(s(thingsComment)))
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('app', 'public', 'things', 'unsharded', NULL)`)
	f.reconcile()

	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'sharded', shard_key = 'tenant' WHERE table_name = 'things'`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET spec = spec || '{"drop_old_after_seconds": 0}'`)
	f.driveUntil("things", 2*time.Minute, StageCompleted)

	total := int64(0)
	for id := range int32(2) {
		c := f.app(id)
		n := queryOne[int64](t, c, `SELECT count(*) FROM things`)
		total += n
		if n == 0 {
			continue
		}
		maxSeq := queryOne[int64](t, c, `SELECT max(seq) FROM things`)
		maxTag := queryOne[int64](t, c, `SELECT max(tag) FROM things`)
		maxSer := queryOne[int64](t, c, `SELECT max(ser) FROM things`)
		tenant := queryOne[int64](t, c, `SELECT tenant FROM things LIMIT 1`)
		// GENERATED ALWAYS tag is not given, BY DEFAULT seq and serial ser
		// are auto-assigned; all must exceed the copied maxima on this shard.
		// ser exercises the shadowDDL-built shard's non-identity sequence.
		var seq, tag, ser int64
		if err := c.QueryRow(context.Background(), `INSERT INTO things (tenant, note) VALUES ($1, 'fresh') RETURNING seq, tag, ser`, tenant).Scan(&seq, &tag, &ser); err != nil {
			t.Fatalf("shard %d: fresh insert after swap: %v", id, err)
		}
		if seq <= maxSeq {
			t.Errorf("shard %d: BY DEFAULT identity reused seq %d (max copied %d)", id, seq, maxSeq)
		}
		if tag <= maxTag {
			t.Errorf("shard %d: ALWAYS identity reused tag %d (max copied %d)", id, tag, maxTag)
		}
		if ser <= maxSer {
			t.Errorf("shard %d: serial reused ser %d (max copied %d)", id, ser, maxSer)
		}
	}
	if total != 60 {
		t.Fatalf("things across shards: %d", total)
	}
	for id := range int32(2) {
		// The generated column is a real generated column on every shard and
		// was recomputed from the copied rows, not inserted.
		gen := queryOne[string](t, f.app(id), `SELECT attgenerated::text FROM pg_attribute WHERE attrelid = 'public.things'::regclass AND attname = 'twice'`)
		if gen != "s" {
			t.Errorf("shard %d: twice attgenerated = %q, want stored", id, gen)
		}
		if bad := queryOne[int64](t, f.app(id), `SELECT count(*) FROM things WHERE twice <> tenant * 2`); bad != 0 {
			t.Errorf("shard %d: %d rows with a wrong generated value", id, bad)
		}
		if coll := queryOne[string](t, f.app(id), `SELECT co.collname FROM pg_attribute a JOIN pg_collation co ON co.oid = a.attcollation WHERE a.attrelid = 'public.things'::regclass AND a.attname = 'code'`); coll != "C" {
			t.Errorf("shard %d: code collation = %q, want C (remote shadow dropped COLLATE)", id, coll)
		}
	}
	for id := range int32(2) {
		inc := queryOne[int64](t, f.app(id), `SELECT seqincrement FROM pg_sequence WHERE seqrelid = pg_get_serial_sequence('public.things', 'tag')::regclass`)
		if inc != 10 {
			t.Errorf("shard %d: tag identity sequence increment = %d, want 10 (shadowDDL dropped the option)", id, inc)
		}
		// A smallserial's sequence is declared AS smallint. Recreating it as
		// the default bigint changes no behaviour -- the bounds and nextval
		// are the same -- but the moved table's catalog should say what the
		// source's said.
		typ := queryOne[string](t, f.app(id), `SELECT seqtypid::regtype::text FROM pg_sequence WHERE seqrelid = pg_get_serial_sequence('public.things', 'small')::regclass`)
		if typ != "smallint" {
			t.Errorf("shard %d: small sequence declared AS %s, want smallint", id, typ)
		}
	}
	for id := range int32(2) {
		got := queryOne[string](t, f.app(id), `SELECT obj_description('public.things'::regclass, 'pg_class')`)
		if got != thingsComment {
			t.Errorf("shard %d: table comment not restored: %q", id, got)
		}
	}
}

// TestPlacementRefusesUserArtifactTableOnPostgres: a user table that happens
// to share the __pgshard_new shadow name must never be adopted, written into
// or dropped; the move fails loudly and leaves the table intact.
func TestPlacementRefusesUserArtifactTableOnPostgres(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	home := f.app(0)
	other := f.app(1)
	mustExec(t, home, `CREATE TABLE items (id serial PRIMARY KEY, v text)`)
	mustExec(t, home, `INSERT INTO items (v) SELECT 'x' FROM generate_series(1, 10)`)
	// A pre-existing, unrelated user table with the reserved shadow name.
	mustExec(t, other, `CREATE TABLE items__pgshard_new (keep text)`)
	mustExec(t, other, `INSERT INTO items__pgshard_new VALUES ('precious')`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('app', 'public', 'items', 'unsharded', NULL)`)
	f.reconcile()

	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'sharded', shard_key = 'id' WHERE table_name = 'items'`)
	f.reconcile()
	var state, msg string
	for i := 0; i < 40; i++ {
		if _, err := f.placer.Pass(context.Background()); err != nil {
			t.Fatal(err)
		}
		_, state, _, msg = f.workflow("items")
		if state == StateFailed {
			break
		}
		if state == StateCompleted {
			t.Fatalf("move completed despite a conflicting user table")
		}
	}
	// The refusal has to say both things an operator might be looking at:
	// their own table, and a shadow this workflow left before the
	// controller stamped its artifacts. Only they can tell which.
	if state != StateFailed || !strings.Contains(msg, "does not carry this workflow's marker") {
		t.Fatalf("expected refusal, got %s %q", state, msg)
	}
	if !strings.Contains(msg, "rename it") || !strings.Contains(msg, "drop it and let the workflow rebuild it") {
		t.Fatalf("the refusal must name both cases and what to do about each: %q", msg)
	}
	// The user's table and its row must be untouched.
	if v := queryOne[string](t, other, `SELECT keep FROM items__pgshard_new`); v != "precious" {
		t.Fatalf("user table was modified: %q", v)
	}
	if n := queryOne[int64](t, other, `SELECT count(*) FROM information_schema.columns WHERE table_name = 'items__pgshard_new'`); n != 1 {
		t.Fatalf("user table schema changed: %d columns", n)
	}
}

// TestSequenceInSchemaOnPostgres guards the shadowDDL fix that skips
// ALTER SEQUENCE ... OWNED BY for a serial default whose sequence lives in a
// different schema than the table (PostgreSQL requires them co-schema).
func TestSequenceInSchemaOnPostgres(t *testing.T) {
	parallelPG(t)
	conn := connect(t, startPostgres(t))
	ctx := context.Background()
	for _, stmt := range []string{
		`CREATE SCHEMA app`,
		`CREATE SCHEMA s2`,
		`CREATE SEQUENCE app.local_seq`,
		`CREATE SEQUENCE s2.shared`,
	} {
		mustExec(t, conn, stmt)
	}
	local, err := sequenceInSchema(ctx, pgxShardConn{conn}, "app.local_seq", "app")
	if err != nil || !local {
		t.Fatalf("same-schema sequence: %v %v", local, err)
	}
	cross, err := sequenceInSchema(ctx, pgxShardConn{conn}, "s2.shared", "app")
	if err != nil || cross {
		t.Fatalf("cross-schema sequence must not be reported in app: %v %v", cross, err)
	}
}

// TestPlacementRefusesCrossSchemaSerialOnPostgres: a table whose column
// defaults to a sequence in another schema cannot be moved safely (the
// rebuilt sequence would not be advanced and is typically shared), so the
// placement is refused at preflight.
func TestPlacementRefusesCrossSchemaSerialOnPostgres(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	home := f.app(0)
	mustExec(t, home, `CREATE SCHEMA s2`)
	mustExec(t, home, `CREATE SEQUENCE s2.shared`)
	mustExec(t, home, `CREATE TABLE widgets (id bigint NOT NULL DEFAULT nextval('s2.shared'), tenant bigint NOT NULL, PRIMARY KEY (tenant, id))`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('app', 'public', 'widgets', 'unsharded', NULL)`)
	f.reconcile()

	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'sharded', shard_key = 'tenant' WHERE table_name = 'widgets'`)
	f.reconcile()
	var state, msg string
	for i := 0; i < 40; i++ {
		if _, err := f.placer.Pass(ctx); err != nil {
			t.Fatal(err)
		}
		_, state, _, msg = f.workflow("widgets")
		if state == StateFailed {
			break
		}
		if state == StateCompleted {
			t.Fatal("cross-schema serial move completed instead of being refused")
		}
	}
	if state != StateFailed || !strings.Contains(msg, "another schema") {
		t.Fatalf("expected refusal, got %s %q", state, msg)
	}
}

// TestPlacementRefusesUnsupportedFeaturesOnPostgres: a table carrying a
// rule or a foreign key is refused at preflight, because the shadow build
// recreates neither and the swap would silently drop enforcement.
//
// Two classes have come off that list and are reproduced instead:
// row-level security (TestAMoveKeepsRowLevelSecurity) and user triggers
// (TestAMoveKeepsTriggers). A trigger is still refused when its FUNCTION is
// missing on a target, which is a different refusal and is asserted below.
func TestPlacementRefusesUnsupportedFeaturesOnPostgres(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	home := f.app(0)
	mustExec(t, home, `CREATE TABLE guarded (id bigint PRIMARY KEY, owner text)`)
	mustExec(t, home, `CREATE RULE own_rows AS ON DELETE TO guarded DO INSTEAD NOTHING`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('app', 'public', 'guarded', 'unsharded', NULL)`)
	f.reconcile()

	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'sharded', shard_key = 'id' WHERE table_name = 'guarded'`)
	f.reconcile()
	var state, msg string
	for i := 0; i < 40; i++ {
		if _, err := f.placer.Pass(ctx); err != nil {
			t.Fatal(err)
		}
		_, state, _, msg = f.workflow("guarded")
		if state == StateFailed {
			break
		}
		if state == StateCompleted {
			t.Fatal("move of a table with a rule completed instead of being refused")
		}
	}
	if state != StateFailed || !strings.Contains(msg, "rule own_rows") {
		t.Fatalf("expected refusal naming the rule, got %s %q", state, msg)
	}
	// The rule is untouched.
	if n := queryOne[int64](t, home, `SELECT count(*) FROM pg_rules WHERE rulename = 'own_rows'`); n != 1 {
		t.Fatal("the rule was dropped")
	}
	// The two directions of a foreign key are not the same problem, and the
	// detector now reports only the one it still refuses. An INBOUND key is
	// a constraint on ANOTHER table pointing at this one by OID, which the
	// swap leaves aimed at the retired table; an outbound key is this
	// table's own and is reproduced when it can be (checkForeignKeys).
	mustExec(t, home, `CREATE TABLE parent (pid bigint PRIMARY KEY)`)
	mustExec(t, home, `CREATE TABLE child (id bigint PRIMARY KEY, pid bigint REFERENCES parent(pid))`)
	if got, err := unsupportedTableFeatures(ctx, pgxShardConn{home}, "public", "parent"); err != nil ||
		len(got) != 1 || !strings.HasPrefix(got[0], "inbound foreign key") {
		t.Fatalf("parent is referenced and must still be refused: %v %v", got, err)
	}
	if got, err := unsupportedTableFeatures(ctx, pgxShardConn{home}, "public", "child"); err != nil || len(got) != 0 {
		t.Fatalf("child's own key is not this detector's business: %v %v", got, err)
	}
	// Shapes lost by both shadow paths that carry no policy/trigger/FK.
	mustExec(t, home, `CREATE ROLE reader`)
	for _, c := range []struct{ ddl, table, want string }{
		{`CREATE TABLE ruled (id bigint PRIMARY KEY); CREATE RULE r1 AS ON DELETE TO ruled DO INSTEAD NOTHING`, "ruled", "rule r1"},
		{`CREATE TABLE base (id bigint PRIMARY KEY); CREATE TABLE inh (x int) INHERITS (base)`, "inh", "inheritance/partition membership"},
		{`CREATE TABLE heir (id bigint PRIMARY KEY); CREATE TABLE heir_child (x int) INHERITS (heir)`, "heir", "inheritance/partition membership"},
		{`CREATE TABLE ri (id bigint PRIMARY KEY); ALTER TABLE ri REPLICA IDENTITY FULL`, "ri", "replica identity FULL"},
		// Bound by OID from outside the table, so the swap leaves each of
		// them on the retired table with no error (PGS-790).
		{`CREATE TABLE viewed (id bigint PRIMARY KEY); CREATE VIEW viewed_ids AS SELECT id FROM viewed`, "viewed", "view public.viewed_ids"},
		{`CREATE TABLE mviewed (id bigint PRIMARY KEY); CREATE MATERIALIZED VIEW mviewed_ids AS SELECT id FROM mviewed`, "mviewed", "materialized view public.mviewed_ids"},
		{`CREATE TABLE audited (id bigint PRIMARY KEY); CREATE TABLE audit_feed (id bigint);
			CREATE RULE feed_audited AS ON INSERT TO audit_feed DO ALSO INSERT INTO audited VALUES (NEW.id)`, "audited", "rule feed_audited on public.audit_feed"},
		{`CREATE TABLE counted (id bigint PRIMARY KEY);
			CREATE FUNCTION count_counted() RETURNS bigint LANGUAGE sql STABLE RETURN (SELECT count(*) FROM counted)`, "counted", "function count_counted()"},
		{`CREATE TABLE allowed (who text PRIMARY KEY); CREATE TABLE notes (who text, body text);
			ALTER TABLE notes ENABLE ROW LEVEL SECURITY;
			CREATE POLICY allowed_only ON notes USING (EXISTS (SELECT 1 FROM allowed a WHERE a.who = notes.who))`, "allowed", "policy allowed_only on public.notes"},
		// Bound to the row type, which the rename takes with the table
		// (PGS-885).
		{`CREATE TABLE rowed (id bigint PRIMARY KEY);
			CREATE FUNCTION rowed_all() RETURNS SETOF rowed LANGUAGE plpgsql AS $$ BEGIN RETURN QUERY SELECT * FROM rowed; END $$`, "rowed", "row type used by function rowed_all()"},
		{`CREATE TABLE snapshotted (id bigint PRIMARY KEY); CREATE TABLE snapshot_log (s snapshotted[])`, "snapshotted", "row type used by column s of table snapshot_log"},
		{`CREATE TABLE casted (id bigint PRIMARY KEY); CREATE VIEW casted_null AS SELECT NULL::casted AS r`, "casted", "row type used by column r of view casted_null"},
		// A view that mentions the row type but exposes no column of it
		// records only its own _RETURN rule, so excluding every _RETURN row
		// let this through: after the swap the view is bound to the retired
		// table's row shape for ever, and DROP TABLE on the retired table
		// fails for as long as the view exists.
		{`CREATE TABLE shredded_src (id bigint PRIMARY KEY, amount int); CREATE TABLE shredded_raw (data jsonb);
			CREATE VIEW shredded AS SELECT to_jsonb(jsonb_populate_record(NULL::shredded_src, data)) AS j FROM shredded_raw`,
			"shredded_src", "row type used by view shredded"},
		// The table's OID as a regclass constant (PGS-888).
		{`CREATE TABLE selfnamed (id bigint PRIMARY KEY, me regclass DEFAULT 'selfnamed'::regclass)`, "selfnamed", "reference to the table by OID in default value for column me of table selfnamed"},
		{`CREATE TABLE pointed (id bigint PRIMARY KEY); CREATE TABLE pointer (y int CHECK (y <> 'pointed'::regclass::int))`, "pointed", "reference to the table by OID in constraint pointer_y_check on table pointer"},
		{`CREATE TABLE selfchecked (a int CHECK (a <> 'selfchecked'::regclass::int))`, "selfchecked", "reference to the table by OID in constraint selfchecked_a_check on table selfchecked"},
		{`CREATE TABLE selfgenerated (a int, g int GENERATED ALWAYS AS (a + 'selfgenerated'::regclass::int) STORED)`, "selfgenerated", "reference to the table by OID in default value for column g of table selfgenerated"},
		{`CREATE TABLE indexpointed (id bigint PRIMARY KEY); CREATE TABLE indexpointer (a int);
			CREATE INDEX indexpointer_a ON indexpointer (a) WHERE a <> 'indexpointed'::regclass::int`, "indexpointed", "reference to the table by OID in index indexpointer_a"},
		{`CREATE TABLE selfwhen (a int); CREATE FUNCTION selfwhen_noop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
			CREATE TRIGGER selfwhen_t BEFORE INSERT ON selfwhen FOR EACH ROW WHEN (NEW.a <> 'selfwhen'::regclass::int) EXECUTE FUNCTION selfwhen_noop()`, "selfwhen", "reference to the table by OID in trigger selfwhen_t on table selfwhen"},
		// A rule that writes into the table from a view is the rule, not
		// the view: the view's own _RETURN rule does not touch this table.
		{`CREATE TABLE sink (id bigint PRIMARY KEY); CREATE VIEW sink_entry AS SELECT 1::bigint AS id;
			CREATE RULE into_sink AS ON INSERT TO sink_entry DO INSTEAD INSERT INTO sink VALUES (NEW.id)`, "sink", "rule into_sink on public.sink_entry"},
	} {
		mustExec(t, home, c.ddl)
		got, err := unsupportedTableFeatures(ctx, pgxShardConn{home}, "public", c.table)
		if err != nil || !slices.Contains(got, c.want) {
			t.Fatalf("%s: unsupported = %v (%v), want %q", c.table, got, err, c.want)
		}
		if len(slices.Compact(slices.Clone(got))) != len(got) {
			t.Errorf("%s: %v reports a feature twice", c.table, got)
		}
		for _, f := range got {
			if strings.Contains(f, "by OID in table ") {
				t.Errorf("%s: %q reports an inheriting table as a reference by OID", c.table, f)
			}
			if strings.Contains(f, "_RETURN") {
				t.Errorf("%s: %q names a view's internal rule; the view's column already names it", c.table, f)
			}
		}
	}
	// A subscription applying into the table. It needs a publication to
	// list the table, so another database on the same server publishes it;
	// the slot is made first because CREATE SUBSCRIPTION making its own
	// against its own server waits on itself.
	mustExec(t, home, `CREATE DATABASE pubsrc`)
	pub := connect(t, strings.Replace(f.dsns[ShardRef{Set: "default", ID: 0}], "/postgres?", "/pubsrc?", 1))
	mustExec(t, pub, `CREATE TABLE subbed (id bigint PRIMARY KEY)`)
	mustExec(t, pub, `CREATE PUBLICATION subbed_pub FOR TABLE subbed`)
	mustExec(t, pub, `SELECT pg_create_logical_replication_slot('subbed_sub', 'pgoutput')`)
	mustExec(t, home, `CREATE TABLE subbed (id bigint PRIMARY KEY)`)
	mustExec(t, home, `CREATE SUBSCRIPTION subbed_sub CONNECTION 'host=/tmp user=postgres dbname=pubsrc' PUBLICATION subbed_pub
		WITH (create_slot = false, slot_name = 'subbed_sub', enabled = false, copy_data = false)`)
	if got, err := unsupportedTableFeatures(ctx, pgxShardConn{home}, "public", "subbed"); err != nil || !slices.Contains(got, "subscription subbed_sub") {
		t.Fatalf("subbed: unsupported = %v (%v), want %q", got, err, "subscription subbed_sub")
	}
	// A plpgsql function names the table only as text in its body, resolved
	// when it runs, so it follows the swap and is not refused.
	mustExec(t, home, `CREATE TABLE lookedup (id bigint PRIMARY KEY)`)
	mustExec(t, home, `CREATE FUNCTION look_up() RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN RETURN (SELECT count(*) FROM lookedup); END $$`)
	if got, err := unsupportedTableFeatures(ctx, pgxShardConn{home}, "public", "lookedup"); err != nil || len(got) != 0 {
		t.Fatalf("a function that names the table only in text follows the swap and must not be refused: %v %v", got, err)
	}

	// The table's own expression naming another table by OID survives the
	// swap unchanged, since that table is not moved.
	mustExec(t, home, `CREATE TABLE namesother (a int CHECK (a <> 'lookedup'::regclass::int))`)
	if got, err := unsupportedTableFeatures(ctx, pgxShardConn{home}, "public", "namesother"); err != nil || len(got) != 0 {
		t.Fatalf("a regclass constant naming another table must not be refused: %v %v", got, err)
	}

	// And what is no longer refused: row-level security and user triggers
	// are reproduced, so the feature detector reports neither.
	mustExec(t, home, `CREATE TABLE rlsonly (id bigint PRIMARY KEY, owner text CHECK (owner <> ''), amount int DEFAULT 0 CHECK (amount >= 0))`)
	mustExec(t, home, `ALTER TABLE rlsonly ENABLE ROW LEVEL SECURITY`)
	mustExec(t, home, `CREATE POLICY own_rows ON rlsonly USING (owner = current_user)`)
	mustExec(t, home, `CREATE FUNCTION stamp_owner() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN NEW.owner := current_user; RETURN NEW; END $$`)
	mustExec(t, home, `CREATE TRIGGER stamp_it BEFORE INSERT ON rlsonly FOR EACH ROW EXECUTE FUNCTION stamp_owner()`)
	mustExec(t, home, `GRANT SELECT ON rlsonly TO reader`)
	mustExec(t, home, `GRANT UPDATE (owner) ON rlsonly TO reader`)
	mustExec(t, home, `CREATE ROLE keeper`)
	mustExec(t, home, `ALTER TABLE rlsonly OWNER TO keeper`)
	if got, err := unsupportedTableFeatures(ctx, pgxShardConn{home}, "public", "rlsonly"); err != nil || len(got) != 0 {
		t.Fatalf("row-level security, triggers, owner and grants are reproduced now, not refused: %v %v", got, err)
	}
}

// TestPlacementRefusesGeneratedKeyOnPostgres: a generated column is not part
// of the copied row shape, so a move keyed by it (shard key or primary key)
// is refused up front instead of failing per row inside the copy.
func TestPlacementRefusesGeneratedKeyOnPostgres(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	home := f.app(0)
	mustExec(t, home, `CREATE TABLE gk (id bigint PRIMARY KEY, tenant bigint NOT NULL, dbl bigint GENERATED ALWAYS AS (tenant * 2) STORED)`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('app', 'public', 'gk', 'unsharded', NULL)`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'sharded', shard_key = 'dbl' WHERE table_name = 'gk'`)
	f.reconcile()
	var state, msg string
	for i := 0; i < 40; i++ {
		if _, err := f.placer.Pass(ctx); err != nil {
			t.Fatal(err)
		}
		_, state, _, msg = f.workflow("gk")
		if state == StateFailed {
			break
		}
		if state == StateCompleted {
			t.Fatal("move keyed by a generated column completed instead of being refused")
		}
	}
	if state != StateFailed || !strings.Contains(msg, "is a generated column") {
		t.Fatalf("expected generated-key refusal, got %s %q", state, msg)
	}
}

// TestPlacementFenceRefusesAStaleRouterOnPostgres is the point of fencing in
// the database: between the first per-shard swap and the new placement being
// published, a router still holding the pre-move view is admitted by the
// routing generation and the primary epoch, because neither has moved yet.
// The shard has to refuse it itself, and it has to keep refusing after its
// own swap, or the write lands on the wrong shard and is never replayed.
func TestPlacementFenceRefusesAStaleRouterOnPostgres(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	for id := range int32(2) {
		c := f.app(id)
		mustExec(t, c, `CREATE TABLE moving (id bigint PRIMARY KEY, tenant_id bigint NOT NULL, note text)`)
	}
	client := f.app(0)
	mustExec(t, client, `INSERT INTO moving VALUES (1, 10, 'before')`)

	shape := rowShape{Schema: "public", Name: "moving"}
	live, shadow := shape.qualified("moving"), shape.qualified("moving_shadow")
	admin := connect(t, strings.Replace(f.dsns[ShardRef{Set: "default", ID: 0}], "/postgres?", "/app?", 1))
	mustExec(t, admin, `CREATE TABLE moving_shadow (LIKE moving INCLUDING ALL)`)
	if _, err := admin.Exec(ctx, `SET `+MaintenanceGUC+` = 'on'`); err != nil {
		t.Fatal(err)
	}
	if err := fenceTables(ctx, pgxShardConn{admin}, "public", live, shadow); err != nil {
		t.Fatal(err)
	}

	// A client session is refused on both the live table and the shadow.
	for _, sql := range []string{
		`INSERT INTO moving VALUES (2, 20, 'stale')`,
		`UPDATE moving SET note = 'stale' WHERE id = 1`,
		`DELETE FROM moving WHERE id = 1`,
		`INSERT INTO moving_shadow VALUES (3, 30, 'stale')`,
	} {
		_, err := client.Exec(ctx, sql)
		if err == nil {
			t.Fatalf("the fence admitted %q", sql)
		}
		var pge *pgconn.PgError
		if !errors.As(err, &pge) || pge.Code != "55000" {
			t.Fatalf("%q: %v, want 55000", sql, err)
		}
	}
	// TRUNCATE never fires a row trigger, so it needs its own.
	if _, err := client.Exec(ctx, `TRUNCATE moving`); err == nil {
		t.Fatal("TRUNCATE went straight through the fence")
	} else {
		var pge *pgconn.PgError
		if !errors.As(err, &pge) || pge.Code != "55000" {
			t.Fatalf("TRUNCATE: %v, want 55000", err)
		}
	}
	// The workflow's own session still works, or it could not catch up.
	mustExec(t, admin, `INSERT INTO moving_shadow VALUES (4, 40, 'applied')`)

	// After the swap the shadow carries the live name, and its trigger came
	// with it, so the shard is still fenced.
	mustExec(t, admin, `ALTER TABLE moving RENAME TO moving_old`)
	mustExec(t, admin, `ALTER TABLE moving_shadow RENAME TO moving`)
	if _, err := client.Exec(ctx, `INSERT INTO moving VALUES (5, 50, 'after swap')`); err == nil {
		t.Fatal("a swapped shard must stay fenced until the placement is published")
	} else {
		var pge *pgconn.PgError
		if !errors.As(err, &pge) || pge.Code != "55000" {
			t.Fatalf("after swap: %v, want 55000", err)
		}
	}

	// Releasing lets the client back in.
	if err := unfenceTables(ctx, pgxShardConn{admin}, shape.qualified("moving"), shape.qualified("moving_old")); err != nil {
		t.Fatal(err)
	}
	mustExec(t, client, `INSERT INTO moving VALUES (6, 60, 'after release')`)
}

// TestPlacementNamesAMissingExtensionOnPostgres: an index or an exclusion
// constraint can use an operator class or an operator that belongs to an
// extension -- btree_gist under a temporal key is the case that found this.
// The shadow build then fails on any target that lacks it, with "no default
// operator class for access method gist", and the workflow retries against a
// condition that will not change on its own. The move is refused up front
// instead, naming the extension and the shards without it.
func TestPlacementNamesAMissingExtensionOnPostgres(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()

	// Only the home shard has the extension; the other is where the move
	// would land, and cannot build the shadow.
	home := f.app(0)
	mustExec(t, home, `CREATE EXTENSION btree_gist`)
	// Otherwise movable: the shard key is in the primary key, and the index
	// that needs the extension is not a uniqueness key at all -- a gist
	// index over a scalar column, whose opclass comes from btree_gist. So
	// nothing but the extension stands in the way of this move.
	mustExec(t, home, `CREATE TABLE bookings (id bigint NOT NULL, room bigint NOT NULL, during tstzrange NOT NULL, PRIMARY KEY (id, room))`)
	mustExec(t, home, `CREATE INDEX bookings_room_gist ON bookings USING gist (room)`)
	mustExec(t, f.app(1), `CREATE TABLE bookings (id bigint NOT NULL, room bigint NOT NULL, during tstzrange NOT NULL, PRIMARY KEY (id, room))`)

	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement)
		VALUES ('app', 'public', 'bookings', 'unsharded')`)
	if res := f.reconcile(); res.TablesMadeEffective != 1 {
		t.Fatalf("%+v", res)
	}
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'sharded', shard_key = 'room' WHERE table_name = 'bookings'`)
	if res := f.reconcile(); res.WorkflowsCreated != 1 {
		t.Fatalf("%+v", res)
	}
	if _, err := f.placer.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	_, state, _, msg := f.workflow("bookings")
	if state != StateFailed {
		t.Fatalf("state %s: %q", state, msg)
	}
	for _, want := range []string{"btree_gist", "default/1", "CREATE EXTENSION"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the refusal must name %q: %q", want, msg)
		}
	}
	// The shard that has it is not named as missing it.
	if strings.Contains(msg, "default/0") {
		t.Errorf("a shard that has the extension must not be named: %q", msg)
	}
}

// TestRetirementSaysWhenItLeftATableBehind: dropArtifactTable is right to
// leave a table it did not mark -- it may be a user's, and dropping it
// would be worse -- but the workflow used to report a clean retirement
// while a shard still held the old table. Nobody goes looking behind a
// workflow that says it finished.
func TestRetirementSaysWhenItLeftATableBehind(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	raw := connect(t, startPostgres(t))
	conn := pgxShardConn{raw}
	mustExec(t, raw, `CREATE TABLE orders__pgshard_old (id bigint)`)

	// Not ours: no marker at all, which is what a table from before
	// markers existed looks like, and what a user's table looks like too.
	dropped, err := dropArtifactTable(ctx, conn, "public", "orders__pgshard_old", "pgshard:placement:some-workflow")
	if err != nil {
		t.Fatal(err)
	}
	if dropped {
		t.Fatal("an unmarked table must not be dropped")
	}
	if !tableExistsOrFail(ctx, t, conn, "public", "orders__pgshard_old") {
		t.Fatal("the table was dropped after all")
	}

	// Ours: marked, and dropped.
	mustExec(t, raw, `COMMENT ON TABLE orders__pgshard_old IS 'pgshard:placement:some-workflow'`)
	dropped, err = dropArtifactTable(ctx, conn, "public", "orders__pgshard_old", "pgshard:placement:some-workflow")
	if err != nil {
		t.Fatal(err)
	}
	if !dropped {
		t.Fatal("a table carrying this workflow's marker must be dropped and reported as dropped")
	}
	if tableExistsOrFail(ctx, t, conn, "public", "orders__pgshard_old") {
		t.Fatal("the marked table is still there")
	}

	// Nothing there at all is not the same as leaving something behind.
	dropped, err = dropArtifactTable(ctx, conn, "public", "orders__pgshard_old", "pgshard:placement:some-workflow")
	if err != nil || dropped {
		t.Fatalf("dropping a table that is not there: dropped=%v err=%v", dropped, err)
	}
}

func tableExistsOrFail(ctx context.Context, t *testing.T, conn ShardConn, schema, name string) bool {
	t.Helper()
	ok, err := tableExists(ctx, conn, schema, name)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

// TestPlacementCarriesAnExclusionConstraintOnPostgres: a hand-built shadow
// selected only p, u and c constraints, so a table with an exclusion
// constraint came back from a move without it -- and without the index
// behind it, which the index pass skips because a constraint owns it. A
// sharded table cannot have one yet (every uniqueness key must contain the
// shard key, and that is not yet decided for an exclusion), so the move
// that exercises it is to a reference table.
func TestPlacementCarriesAnExclusionConstraintOnPostgres(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	src := f.app(0)
	mustExec(t, src, `CREATE TABLE slots (id bigint PRIMARY KEY, room text NOT NULL)`)
	mustExec(t, src, `ALTER TABLE slots ADD CONSTRAINT slots_one_per_room EXCLUDE USING btree (room WITH =)`)
	mustExec(t, src, `INSERT INTO slots SELECT g, 'r' || g FROM generate_series(1, 20) g`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', 'slots', 'unsharded')`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'reference' WHERE table_name = 'slots'`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET spec = spec || '{"drop_old_after_seconds": 0}'`)
	f.driveUntil("slots", 2*time.Minute, StageCompleted)

	for id := range int32(2) {
		c := f.app(id)
		// The home shard's copy is named by LIKE, which renames what it
		// copies; what has to survive the move is the constraint, not the
		// name it happened to have.
		if n := queryOne[int64](t, c, `SELECT count(*) FROM pg_constraint WHERE conrelid = 'public.slots'::regclass AND contype = 'x'`); n != 1 {
			t.Errorf("shard %d: %d exclusion constraints, want the one the table had", id, n)
		}
		if _, err := c.Exec(context.Background(), `INSERT INTO slots VALUES (999, 'r1')`); err == nil {
			t.Errorf("shard %d: the moved table accepted a row its exclusion constraint forbids", id)
		}
	}
}

// TestPlacementShardsATableWithATemporalKeyOnPostgres: an exclusion whose
// shard-key element is equality is enforceable one shard at a time, so a
// table carrying one can be sharded. What has to hold after the cutover is
// that the constraint is on every shard and still rejects the conflict it
// was there to reject.
//
// The temporal key here is a UNIQUE, not the primary key: the copy applies
// rows by the primary key, and PostgreSQL cannot match an exclusion
// constraint from an ON CONFLICT column list. A table whose only primary
// key is temporal is refused, which the case below covers.
func TestPlacementShardsATableWithATemporalKeyOnPostgres(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	home := f.app(0)
	// A temporal key over a scalar needs btree_gist on every shard that
	// will hold the table; nothing materializes extensions onto target
	// shards yet, so the fixture installs it.
	for id := range int32(2) {
		mustExec(t, f.app(id), `CREATE EXTENSION IF NOT EXISTS btree_gist`)
	}
	mustExec(t, home, `CREATE TABLE bookings (
		tenant bigint NOT NULL,
		id bigint NOT NULL,
		during int4range NOT NULL,
		note text,
		PRIMARY KEY (tenant, id),
		UNIQUE (tenant, during WITHOUT OVERLAPS))`)
	mustExec(t, home, `INSERT INTO bookings SELECT g, g, int4range(g * 10, g * 10 + 5), 'n' || g FROM generate_series(1, 40) g`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', 'bookings', 'unsharded')`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'sharded', shard_key = 'tenant' WHERE table_name = 'bookings'`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET spec = spec || '{"drop_old_after_seconds": 0}'`)
	f.driveUntil("bookings", 2*time.Minute, StageCompleted)

	total := int64(0)
	for id := range int32(2) {
		c := f.app(id)
		total += queryOne[int64](t, c, `SELECT count(*) FROM bookings`)
		if n := queryOne[int64](t, c, `SELECT count(*) FROM pg_constraint WHERE conrelid = 'public.bookings'::regclass AND conperiod`); n != 1 {
			t.Errorf("shard %d: the temporal key did not survive the move", id)
		}
		var tenant int64
		if err := c.QueryRow(context.Background(), `SELECT tenant FROM bookings LIMIT 1`).Scan(&tenant); err != nil {
			continue
		}
		// The row this shard holds overlaps [tenant*10, tenant*10+5).
		if _, err := c.Exec(context.Background(), `INSERT INTO bookings VALUES ($1, $1 + 1000, int4range($2, $3), 'overlap')`,
			tenant, tenant*10+1, tenant*10+9); err == nil {
			t.Errorf("shard %d: the moved table accepted a booking that overlaps one it holds", id)
		}
	}
	if total != 40 {
		t.Fatalf("bookings across shards: %d", total)
	}
}

// TestPlacementRefusesATemporalPrimaryKeyOnPostgres: the copy applies rows
// by the primary key, and PostgreSQL will not match an exclusion constraint
// from an ON CONFLICT column list. Accepting the table and then failing
// every batch deep in the copy is worse than saying so at the start.
func TestPlacementRefusesATemporalPrimaryKeyOnPostgres(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	// Every shard the table could move to needs btree_gist, or the move is
	// refused for the missing extension before it ever looks at the key.
	for id := range int32(2) {
		mustExec(t, f.app(id), `CREATE EXTENSION IF NOT EXISTS btree_gist`)
	}
	home := f.app(0)
	mustExec(t, home, `CREATE TABLE spans (
		tenant bigint NOT NULL,
		during int4range NOT NULL,
		PRIMARY KEY (tenant, during WITHOUT OVERLAPS))`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', 'spans', 'unsharded')`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'sharded', shard_key = 'tenant' WHERE table_name = 'spans'`)
	f.reconcile()

	var state, msg string
	for range 40 {
		if _, err := f.placer.Pass(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := f.catalog.QueryRow(context.Background(),
			`SELECT state, coalesce(status->>'message', '') FROM pgshard.workflows ORDER BY created_at DESC LIMIT 1`).Scan(&state, &msg); err != nil {
			t.Fatal(err)
		}
		if state == StateFailed {
			break
		}
	}
	if state != StateFailed || !strings.Contains(msg, "temporal key") {
		t.Fatalf("workflow ended %s: %q, want a refusal naming the temporal key", state, msg)
	}
}

// TestAFailedPlacementDropsItsReplication.
//
// fail() releases the fence and the table lock because "a failed workflow is
// never revisited" -- list() selects pending, running and cancelling, so
// nothing drives it again. That reasoning stopped at the fence. The slot
// ensureReplication created on every source stayed, pinning WAL until
// somebody noticed pg_wal filling, and the source table kept REPLICA
// IDENTITY FULL, so every UPDATE logged the whole old row for good.
func TestAFailedPlacementDropsItsReplication(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	home := f.app(0)
	mustExec(t, home, `CREATE TABLE leak (id bigint PRIMARY KEY, v text)`)
	mustExec(t, home, `INSERT INTO leak SELECT g, 'g' || g FROM generate_series(1, 20) g`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('app', 'public', 'leak', 'unsharded', NULL)`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'sharded', shard_key = 'id' WHERE table_name = 'leak'`)
	f.reconcile()
	id, _ := f.driveUntil("leak", 2*time.Minute, StagePlacementSwapping)
	wf := f.load(id)

	// The slots have to exist before this proves anything.
	before := 0
	for _, s := range wf.from.Sources() {
		var ok bool
		if err := f.app(s).QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, wf.slotName(s)).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		if ok {
			before++
		}
	}
	if before == 0 {
		t.Fatal("no replication slot existed before the failure; this test would pass without checking anything")
	}

	if err := f.placer.fail(ctx, wf, fatal("test: verification failed after the copy")); err != nil {
		t.Fatal(err)
	}

	for _, s := range wf.from.Sources() {
		var left bool
		if err := f.app(s).QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, wf.slotName(s)).Scan(&left); err != nil {
			t.Fatal(err)
		}
		if left {
			t.Errorf("shard %d still holds slot %s; it pins WAL and nothing revisits a failed workflow", s, wf.slotName(s))
		}
		var pub bool
		if err := f.app(s).QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`, wf.publicationName()).Scan(&pub); err != nil {
			t.Fatal(err)
		}
		if pub {
			t.Errorf("shard %d still holds publication %s", s, wf.publicationName())
		}
		var identity string
		if err := f.app(s).QueryRow(ctx,
			`SELECT relreplident FROM pg_class WHERE relname = 'leak'`).Scan(&identity); err != nil {
			t.Fatal(err)
		}
		if identity == "f" {
			t.Errorf("shard %d left REPLICA IDENTITY FULL on the source: every UPDATE logs the whole old row", s)
		}
	}
}

// TestAPlacementCarriesAnIdentitySequencePastDeletedRows.
//
// An identity column's sequence is a dependency of kind 'i'.
// moveOwnedSequences moves kind 'a' -- the serial kind -- so the shadow keeps
// the fresh sequence LIKE INCLUDING ALL made for it and the source's is
// dropped with the old table. advanceSequences then set it to the greatest
// value PRESENT, which reissues every identifier whose row was deleted: a
// source at 10 000 whose newest rows are gone hands out 9 501 again, an id
// other systems and change-stream consumers have already seen.
//
// docs/resharding.md says "sequences owned by the old table's columns move to
// the new one", which was true of serial columns and never of identity ones.
func TestAPlacementCarriesAnIdentitySequencePastDeletedRows(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	home := f.app(0)
	mustExec(t, home, `CREATE TABLE tickets (id bigint GENERATED ALWAYS AS IDENTITY, tenant_id bigint NOT NULL, v text, PRIMARY KEY (tenant_id, id))`)
	mustExec(t, home, `INSERT INTO tickets (tenant_id, v) SELECT g, 'v' || g FROM generate_series(1, 30) g`)
	// The newest rows go, so the highest id PRESENT is well below the
	// sequence's position. This is the whole point of the test.
	mustExec(t, home, `DELETE FROM tickets WHERE id > 20`)
	var sourceLast int64
	if err := home.QueryRow(ctx, `SELECT last_value FROM pg_sequences
		WHERE schemaname = 'public' AND sequencename = 'tickets_id_seq'`).Scan(&sourceLast); err != nil {
		t.Fatal(err)
	}
	if sourceLast <= 20 {
		t.Fatalf("the fixture must leave the sequence past the highest surviving id, got %d", sourceLast)
	}

	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('app', 'public', 'tickets', 'unsharded', NULL)`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'sharded', shard_key = 'tenant_id' WHERE table_name = 'tickets'`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET spec = spec || '{"drop_old_after_seconds": 0}'`)
	f.driveUntil("tickets", 3*time.Minute, StageCompleted)

	// Every shard that now holds the table must refuse to reissue an id the
	// source had already handed out.
	for id := range int32(2) {
		c := f.app(id)
		var next int64
		if err := c.QueryRow(ctx, `INSERT INTO tickets (tenant_id, v) VALUES (999, 'after') RETURNING id`).Scan(&next); err != nil {
			t.Fatal(err)
		}
		if next <= sourceLast {
			t.Errorf("shard %d issued id %d, which the source had already issued (its sequence was at %d)", id, next, sourceLast)
		}
	}
}

// brokenShard makes one shard unreachable, as a primary that has gone away
// mid-workflow is.
type brokenShard struct {
	ShardDBDialer
	broken atomic.Int32
}

func (b *brokenShard) DialDatabase(ctx context.Context, set string, id int32, db string) (ShardConn, error) {
	if b.broken.Load() == id+1 {
		return nil, fmt.Errorf("shard %s/%d: connection refused", set, id)
	}
	return b.ShardDBDialer.DialDatabase(ctx, set, id, db)
}

// TestPlacementReleasesTheFenceAfterPublishDespiteAnUnreachableShardOnPostgres.
//
// publish commits the catalog flip -- routers reload and write to the new
// placement -- and only then is the shards' own fence released. SwappedAt,
// which the stage reads to decide the swap is done, was set in memory
// AFTER that commit, so a pass that then failed on an unreachable shard
// left the catalog saying the placement was live and the workflow saying it
// was not. The next pass re-armed the fence and re-ran catch-up and the
// swap, both of which dial every shard -- so one shard being down kept the
// table refused on the healthy ones, for as long as it stayed down, even
// though the placement was already live.
func TestPlacementReleasesTheFenceAfterPublishDespiteAnUnreachableShardOnPostgres(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	broken := &brokenShard{ShardDBDialer: f.placer.Shards}
	f.placer.Shards = broken

	src := f.app(0)
	mustExec(t, src, `CREATE TABLE notes (id text PRIMARY KEY, v text)`)
	for i := range 20 {
		mustExec(t, src, `INSERT INTO notes VALUES ($1, $2)`, fmt.Sprintf("k%02d", i), fmt.Sprintf("v%d", i))
	}
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', 'notes', 'unsharded')`)
	if res := f.reconcile(); res.TablesMadeEffective != 1 {
		t.Fatalf("%+v", res)
	}
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'reference' WHERE table_name = 'notes'`)
	if res := f.reconcile(); res.WorkflowsCreated != 1 {
		t.Fatalf("%+v", res)
	}

	id, _ := f.driveUntil("notes", 2*time.Minute, StagePlacementSwapping)
	wf := f.load(id)
	// Everything the swapping stage does up to and including publish.
	if err := f.placer.fenceShards(ctx, wf); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.placer.catchUp(ctx, wf, true); err != nil {
		t.Fatal(err)
	}
	if err := f.placer.verifyPlacement(ctx, wf); err != nil {
		t.Fatal(err)
	}
	if err := f.placer.swapAll(ctx, wf); err != nil {
		t.Fatal(err)
	}
	if err := f.placer.publish(ctx, wf); err != nil {
		t.Fatal(err)
	}

	// The commit that made the placement live has to have recorded that it
	// did, or the next pass cannot tell this from a swap that never ran.
	var swapped *time.Time
	if err := f.catalog.QueryRow(ctx, `SELECT (status->'placement'->>'swapped_at')::timestamptz FROM pgshard.workflows WHERE id = $1::uuid`, id).Scan(&swapped); err != nil {
		t.Fatal(err)
	}
	if swapped == nil {
		t.Fatal("publish committed the placement without recording that it had swapped")
	}

	// Now a shard goes away, before the fence has been released anywhere.
	broken.broken.Store(2) // shard 1
	if _, err := f.placer.Pass(ctx); err != nil {
		t.Logf("pass reported %v, which is expected while a shard is down", err)
	}

	// The healthy shard must have its table back: the placement is live and
	// nothing about shard 1 being down is a reason to keep refusing writes
	// on shard 0.
	if _, err := src.Exec(ctx, `INSERT INTO notes VALUES ('after', 'unfenced')`); err != nil {
		t.Fatalf("the live table is still write-fenced on a healthy shard: %v", err)
	}
}

// TestABarrierDuringAPlacementCopyDoesNotWaitForItsSnapshot (PGS-863): the
// copy holds one REPEATABLE READ READ ONLY transaction for its whole walk,
// and the barrier's drain counted it as a transaction begun before the
// pause that might still write, so every barrier landing during a copy
// longer than the drain timeout failed. A barrier paused mid-walk now finds
// no writer.
func TestABarrierDuringAPlacementCopyDoesNotWaitForItsSnapshot(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	home := f.app(0)
	mustExec(t, home, `CREATE TABLE items (id int PRIMARY KEY, v text)`)
	mustExec(t, home, `INSERT INTO items SELECT g, 'v' FROM generate_series(1, 50) g`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('app', 'public', 'items', 'unsharded', NULL)`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'sharded', shard_key = 'id' WHERE table_name = 'items'`)
	f.reconcile()

	groups := &SQLBarrierGroups{Pool: f.pool, Shards: f.placer.Shards}
	g := GroupRef{Name: "shard0", Set: "default", ID: 0}
	writers := -1
	var werr error
	f.placer.Shards = onCopyWalk{ShardDBDialer: f.placer.Shards, once: &sync.Once{}, fire: func() {
		pausedAt, err := groups.PauseWrites(ctx, g, true)
		if err == nil {
			writers, err = groups.WritersSince(ctx, g, pausedAt)
		}
		if _, rerr := groups.PauseWrites(ctx, g, false); err == nil {
			err = rerr
		}
		werr = err
	}}
	f.driveUntil("items", time.Minute, StagePlacementCatchUp)
	if werr != nil {
		t.Fatal(werr)
	}
	if writers != 0 {
		t.Fatalf("a barrier paused during the copy counted %d writer(s); the copy's read-only snapshot holds its drain", writers)
	}
}

// onCopyWalk runs fire once, just before the first keyset query of a
// placement copy, while its snapshot is open.
type onCopyWalk struct {
	ShardDBDialer
	once *sync.Once
	fire func()
}

func (d onCopyWalk) DialDatabase(ctx context.Context, set string, id int32, db string) (ShardConn, error) {
	c, err := d.ShardDBDialer.DialDatabase(ctx, set, id, db)
	if err != nil {
		return nil, err
	}
	return copyWalkConn{ShardConn: c, d: d}, nil
}

type copyWalkConn struct {
	ShardConn
	d onCopyWalk
}

func (c copyWalkConn) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if strings.Contains(sql, " AS src") {
		c.d.once.Do(c.d.fire)
	}
	return c.ShardConn.Query(ctx, sql, args...)
}

// TestAPlacementStampsItsStartOnce (PGS-924): the stamp is written when the
// move leaves preparing, and every status write after that leaves it alone.
// A stamp that moved would make the queue's age column read as time since
// the last status write.
func TestAPlacementStampsItsStartOnce(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	const id = "00000000-0000-0000-0000-0000000009a1"
	mustExec(t, f.catalog, `INSERT INTO pgshard.workflows (id, kind, state, spec, status, arrival)
		VALUES ($1, 'table_placement', 'pending', '{"database": "app", "schema_name": "public", "table_name": "items"}', '{"stage": "preparing"}', 1)`, id)
	save := func(state, stage string) {
		t.Helper()
		if _, err := f.catalog.Exec(ctx, savePlacementSQL, id, state, mustJSON(map[string]any{"stage": stage}), nil, nil, stage); err != nil {
			t.Fatal(err)
		}
	}
	save("pending", StagePlacementPreparing)
	if stamped := queryOne[bool](t, f.catalog, `SELECT status ? 'started_at' FROM pgshard.workflows WHERE id = $1::uuid`, id); stamped {
		t.Fatal("a placement still preparing was stamped as started")
	}
	save("running", StagePlacementShadow)
	first := queryOne[string](t, f.catalog, `SELECT status->>'started_at' FROM pgshard.workflows WHERE id = $1::uuid`, id)
	if first == "" {
		t.Fatal("a placement past preparing recorded no started_at")
	}
	save("running", "copying")
	if again := queryOne[string](t, f.catalog, `SELECT status->>'started_at' FROM pgshard.workflows WHERE id = $1::uuid`, id); again != first {
		t.Errorf("started_at moved from %q to %q on a later write", first, again)
	}
}

// TestTheRegclassScanFindsItsConstantOnEveryMajor (PGS-936): the scan that
// refuses a placement move over a stored expression naming the table as a
// regclass constant works by decoding the constant's four bytes out of the
// printed pg_node_tree, because PostgreSQL records no dependency for such a
// constant to find.
//
// That is a bet on a format. If the node-tree text ever prints differently
// -- another major is exactly when it would -- the regex matches nothing,
// the refusal quietly stops firing, and a move silently leaves a constraint
// comparing against a freed OID. Nothing would fail: the rest of the suite
// runs on PostgreSQL 18 alone, so a change in 19 would not be noticed here
// at all.
//
// So the scan is proved against every major pgshard supports, with a
// constant planted for it to find. A failure here means the decode has
// stopped working on that major, not that the schema is unusual.
func TestTheRegclassScanFindsItsConstantOnEveryMajor(t *testing.T) {
	for _, image := range []string{
		"ghcr.io/andrew01234567890/pgshard-postgres:18",
		"ghcr.io/andrew01234567890/pgshard-postgres:19",
	} {
		t.Run(image[strings.LastIndex(image, ":")+1:], func(t *testing.T) {
			parallelPG(t)
			ctx := context.Background()
			conn := connect(t, startPostgresImage(t, image, nil))
			// The table's OWN check constraint and generated column. These
			// are the shapes that need the decode: for a table's own
			// expressions PostgreSQL replaces the whole-object dependency
			// with a sub-object one (eliminate_duplicate_dependencies), so
			// the pg_depend branch of the scan does not see them and only
			// the node-tree decode does. A constant in ANOTHER object -- a
			// check on a second table -- is found by the dependency branch
			// whatever the decode does, so it would prove nothing here.
			mustExec(t, conn, `CREATE TABLE named (a int CHECK (a <> 'named'::regclass::int),
				g int GENERATED ALWAYS AS (a + 'named'::regclass::int) STORED)`)
			found, err := unsupportedTableFeatures(ctx, pgxShardConn{conn}, "public", "named")
			if err != nil {
				t.Fatal(err)
			}
			var byOID int
			for _, f := range found {
				if strings.HasPrefix(f, "reference to the table by OID in ") {
					byOID++
				}
			}
			if byOID < 2 {
				t.Errorf("the regclass scan found %d of the 2 constants the table stores about itself: %v\n"+
					"the decode of the printed pg_node_tree has stopped matching on this major, so the refusal it drives is silently not firing", byOID, found)
			}
		})
	}
}

// TestAMoveStopsBeforeItsSwapWhenTheTableGainedADependent: the preflight
// ran once, at prepare, and a copy can take hours. A view created over the
// table in between is bound to it by OID, and the swap left it reading the
// retired table -- with no error, until retiring the old table failed for
// ever because the view depends on it (PGS-826). The dependents are checked
// again just before the first rename, and the move stops there.
func TestAMoveStopsBeforeItsSwapWhenTheTableGainedADependent(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	home := f.app(0)
	mustExec(t, home, `CREATE TABLE ledger (id bigint PRIMARY KEY, v text)`)
	mustExec(t, home, `INSERT INTO ledger SELECT g, 'row-' || g FROM generate_series(1, 20) g`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('app', 'public', 'ledger', 'unsharded', NULL)`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'sharded', shard_key = 'id' WHERE table_name = 'ledger'`)
	f.reconcile()
	f.driveUntil("ledger", 2*time.Minute, StagePlacementCatchUp, StagePlacementBuffering)

	mustExec(t, home, `CREATE VIEW ledger_rows AS SELECT id, v FROM ledger`)

	deadline := time.Now().Add(2 * time.Minute)
	var state, stage, msg string
	for {
		if _, err := f.placer.Pass(context.Background()); err != nil {
			t.Fatal(err)
		}
		_, state, stage, msg = f.workflow("ledger")
		if state == StateFailed || state == StateCompleted || stage == StagePlacementRetiring || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if state != StateFailed || !strings.Contains(msg, "view public.ledger_rows") {
		t.Fatalf("a move whose table gained a view during the copy went on to %s %s %q; it must stop before the swap naming the view", state, stage, msg)
	}
	// Nothing was renamed: the view still reads the live table, which still
	// holds every row.
	if n := queryOne[int64](t, home, `SELECT count(*) FROM ledger_rows`); n != 20 {
		t.Fatalf("the view reads %d rows after the stopped move, want 20", n)
	}
	if n := queryOne[int64](t, home, `SELECT count(*) FROM pg_class WHERE relname = 'ledger' AND relkind = 'r'`); n != 1 {
		t.Fatal("the table was renamed away despite the move stopping")
	}
	// And writable: the move had fenced the shards before the check, and a
	// stop that left the fence up would leave the table refusing writes.
	mustExec(t, home, `INSERT INTO ledger VALUES (21, 'after-the-stopped-move')`)
}

// TestAFailedPlacementCleansUpItsSourceThroughAWritePause (PGS-858,
// PGS-859): a placement's release of its fence and cleanup on its source --
// dropping the fence triggers and its publication, putting back the replica
// identity it widened, dropping its shadow -- ran without writing through a
// write pause. A barrier's pause, or a switch's on the same shards, refused
// them with 25006; the fail path records the residue and ends anyway, so
// REPLICA IDENTITY FULL stayed on the user's table for good and the shadow
// refused the next move of the table.
func TestAFailedPlacementCleansUpItsSourceThroughAWritePause(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	dsn := startPostgres(t)
	admin := connect(t, dsn)
	mustExec(t, admin, `CREATE TABLE ledger (id int PRIMARY KEY, v text)`)
	mustExec(t, admin, `ALTER TABLE ledger REPLICA IDENTITY FULL`)
	wf := &placementWorkflow{id: "66666666-6666-6666-6666-666666666666", stage: StageFailed,
		spec:  placementSpec{Database: "postgres", SchemaName: "public", TableName: "ledger"},
		st:    placementState{SourceSet: "default", ReplicaIdentityFull: []int32{0}},
		from:  &placementRouter{placement: TablePlacement{Placement: "unsharded"}, home: 0},
		rt:    &placementRouter{placement: TablePlacement{Placement: "unsharded"}, home: 0, ids: []int32{0}},
		shape: rowShape{Schema: "public", Name: "ledger"}}
	mustExec(t, admin, `CREATE PUBLICATION `+wf.publicationName()+` FOR TABLE ledger`)
	if err := fenceTables(ctx, pgxShardConn{admin}, "public", wf.shape.qualified("ledger")); err != nil {
		t.Fatal(err)
	}
	mustExec(t, admin, `CREATE TABLE `+QuoteIdent(wf.shadow())+` (id int PRIMARY KEY, v text)`)
	if err := markPlacementArtifact(ctx, pgxShardConn{admin}, "public", wf.shadow(), wf.placementMarker()); err != nil {
		t.Fatal(err)
	}
	mustExec(t, admin, `ALTER SYSTEM SET default_transaction_read_only = on`)
	mustExec(t, admin, `SELECT pg_reload_conf()`)
	waitReadOnly(t, dsn, true)

	// In the order the fail path runs them.
	p := &Placer{Shards: realShards{dsn}}
	if err := p.releaseShardFence(ctx, wf); err != nil {
		t.Fatalf("releasing the fence on a paused source: %v", err)
	}
	if err := p.dropReplication(ctx, wf); err != nil {
		t.Fatalf("cleanup on a paused source: %v", err)
	}
	if err := p.dropShadows(ctx, wf); err != nil {
		t.Fatalf("dropping the shadow on a paused shard: %v", err)
	}
	check := connect(t, dsn)
	if n := queryOne[int64](t, check, `SELECT count(*) FROM pg_class WHERE relname = $1`, wf.shadow()); n != 0 {
		t.Fatalf("the failed placement's shadow survived its cleanup")
	}
	if n := queryOne[int64](t, check, `SELECT count(*) FROM pg_trigger WHERE tgrelid = 'public.ledger'::regclass AND NOT tgisinternal`); n != 0 {
		t.Fatalf("%d fence trigger(s) left on the table", n)
	}
	if n := queryOne[int64](t, check, `SELECT count(*) FROM pg_publication WHERE pubname = $1`, wf.publicationName()); n != 0 {
		t.Fatalf("the placement's publication survived its cleanup")
	}
	if ident := queryOne[string](t, check, `SELECT relreplident::text FROM pg_class WHERE oid = 'public.ledger'::regclass`); ident != "d" {
		t.Fatalf("replica identity is %q after the cleanup, want the default back", ident)
	}
	waitReadOnly(t, dsn, true)
}

// TestPuttingBackTheReplicaIdentityWaitsBoundedlyForItsLock (PGS-859): the
// restore takes an AccessExclusiveLock with no lock_timeout. Written through
// a pause it no longer failed fast, so behind a long reader it queued every
// new reader of the user's table behind it, and the Placer with it. It now
// waits a bounded time per try, tries a few times, and lets readers through
// between tries.
func TestPuttingBackTheReplicaIdentityWaitsBoundedlyForItsLock(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	dsn := startPostgres(t)
	admin := connect(t, dsn)
	mustExec(t, admin, `CREATE TABLE ledger (id int PRIMARY KEY, v text)`)
	mustExec(t, admin, `ALTER TABLE ledger REPLICA IDENTITY FULL`)
	wf := &placementWorkflow{id: "77777777-7777-7777-7777-777777777777", stage: StageFailed,
		spec:  placementSpec{Database: "postgres", SchemaName: "public", TableName: "ledger"},
		st:    placementState{SourceSet: "default", ReplicaIdentityFull: []int32{0}},
		from:  &placementRouter{placement: TablePlacement{Placement: "unsharded"}, home: 0},
		shape: rowShape{Schema: "public", Name: "ledger"}}
	p := &Placer{Shards: realShards{dsn}}
	identity := func() string {
		return queryOne[string](t, connect(t, dsn), `SELECT relreplident::text FROM pg_class WHERE oid = 'public.ledger'::regclass`)
	}

	holdLedger := func() pgx.Tx {
		tx, err := connect(t, dsn).Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `SELECT count(*) FROM ledger`); err != nil {
			t.Fatal(err)
		}
		return tx
	}

	t.Run("a reader that ends while it retries", func(t *testing.T) {
		tx := holdLedger()
		committed := make(chan error, 1)
		go func() {
			time.Sleep(replicaIdentityLockWait + replicaIdentityLockWait/2)
			committed <- tx.Commit(ctx)
		}()
		if err := p.dropReplication(ctx, wf); err != nil {
			t.Fatalf("the restore gave up although the reader ended before its last try: %v", err)
		}
		if err := <-committed; err != nil {
			t.Fatal(err)
		}
		if got := identity(); got != "d" {
			t.Fatalf("replica identity is %q, want the default back", got)
		}
	})

	mustExec(t, admin, `ALTER TABLE ledger REPLICA IDENTITY FULL`)
	t.Run("a reader that outlasts every try", func(t *testing.T) {
		tx := holdLedger()
		defer func() { _ = tx.Rollback(ctx) }()
		runCtx, cancel := context.WithTimeout(ctx, 6*replicaIdentityLockWait)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- p.dropReplication(runCtx, wf) }()

		// Only a reader arriving while the ALTER waits queues behind it.
		watch := connect(t, dsn)
		for queued := false; !queued; {
			if runCtx.Err() != nil {
				t.Fatal("the restore never waited for its lock")
			}
			queued = queryOne[bool](t, watch, `SELECT EXISTS (SELECT 1 FROM pg_locks WHERE relation = 'public.ledger'::regclass AND NOT granted)`)
			time.Sleep(20 * time.Millisecond)
		}
		readCtx, cancelRead := context.WithTimeout(ctx, 2*replicaIdentityLockWait)
		defer cancelRead()
		var n int64
		if err := connect(t, dsn).QueryRow(readCtx, `SELECT count(*) FROM ledger`).Scan(&n); err != nil {
			t.Fatalf("a new reader of the table queued behind the restore past its lock wait: %v", err)
		}

		err := <-done
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
			t.Fatalf("the restore behind a reader that never ends: %v, want its lock wait to run out (55P03)", err)
		}
		if got := identity(); got != "f" {
			t.Fatalf("replica identity is %q after a restore that could not get its lock", got)
		}
	})
}

// TestAFailedPlacementCleansUpTheSourcesItCanReach (PGS-859): the cleanup
// stopped at the first source it could not reach, so the fail path, which
// runs it once, left the publication, slots and widened replica identity on
// every source after that one.
func TestAFailedPlacementCleansUpTheSourcesItCanReach(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	dsn := startPostgres(t)
	admin := connect(t, dsn)
	mustExec(t, admin, `CREATE TABLE ledger (id int PRIMARY KEY, v text)`)
	mustExec(t, admin, `ALTER TABLE ledger REPLICA IDENTITY FULL`)
	wf := &placementWorkflow{id: "99999999-9999-9999-9999-999999999999", stage: StageFailed,
		spec:  placementSpec{Database: "postgres", SchemaName: "public", TableName: "ledger"},
		st:    placementState{SourceSet: "default", ReplicaIdentityFull: []int32{0, 1}},
		from:  &placementRouter{placement: TablePlacement{Placement: "sharded"}, ids: []int32{0, 1}},
		shape: rowShape{Schema: "public", Name: "ledger"}}
	mustExec(t, admin, `CREATE PUBLICATION `+wf.publicationName()+` FOR TABLE ledger`)

	p := &Placer{Shards: unreachableShard{realShards{dsn}, 0}}
	err := p.dropReplication(ctx, wf)
	if err == nil || !strings.Contains(err.Error(), "default/0") {
		t.Fatalf("the unreachable source was not reported: %v", err)
	}
	if n := queryOne[int64](t, admin, `SELECT count(*) FROM pg_publication WHERE pubname = $1`, wf.publicationName()); n != 0 {
		t.Fatalf("the reachable source kept the publication because an earlier one could not be reached")
	}
	if ident := queryOne[string](t, admin, `SELECT relreplident::text FROM pg_class WHERE oid = 'public.ledger'::regclass`); ident != "d" {
		t.Fatalf("replica identity is %q on the reachable source, want the default back", ident)
	}
}

type unreachableShard struct {
	realShards
	down int32
}

func (u unreachableShard) DialDatabase(ctx context.Context, set string, id int32, db string) (ShardConn, error) {
	if id == u.down {
		return nil, errors.New("connection refused")
	}
	return u.realShards.DialDatabase(ctx, set, id, db)
}

// TestTheReplicaIdentityIsRecordedBeforeItIsWidened (PGS-859): the source
// was recorded in ReplicaIdentityFull only after its ALTER committed, and
// saved later still. A controller that died in between left FULL on the
// user's table with nothing saying it was the workflow's to put back.
func TestTheReplicaIdentityIsRecordedBeforeItIsWidened(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	dsn := startPostgres(t)
	cat := connect(t, dsn)
	if err := catalog.Migrate(ctx, cat); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	mustExec(t, cat, `CREATE TABLE ledger (id int PRIMARY KEY, v text)`)
	const id = "88888888-8888-8888-8888-888888888888"
	mustExec(t, cat, `INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES ($1, $2, $3, '{}', '{}')`, id, KindTablePlacement, StateRunning)
	wf := &placementWorkflow{id: id, state: StateRunning, stage: StagePlacementCopying,
		spec:  placementSpec{Database: "postgres", SchemaName: "public", TableName: "ledger"},
		st:    placementState{SourceSet: "default"},
		shape: rowShape{Schema: "public", Name: "ledger"}}
	p := &Placer{Pool: pool, Shards: realShards{dsn}}

	recorded := func() []int32 {
		var ids []int32
		if err := cat.QueryRow(ctx, `SELECT coalesce(array_agg(v::int), '{}') FROM pgshard.workflows,
			jsonb_array_elements_text(coalesce(status->'placement'->'replica_identity_full', '[]')) v WHERE id = $1`, id).Scan(&ids); err != nil {
			t.Fatal(err)
		}
		return ids
	}
	var atAlter []int32
	conn := alterReplicaIdentityDies{ShardConn: pgxShardConn{connect(t, dsn)}, before: func() { atAlter = recorded() }}
	if err := p.ensureReplication(ctx, wf, conn, 0); err == nil {
		t.Fatal("the ALTER was never reached")
	}
	if !slices.Contains(atAlter, 0) {
		t.Fatalf("when the replica identity was widened the workflow had recorded %v: a controller dying there leaves FULL behind unrecorded", atAlter)
	}
	// And the other half: the ALTER failed, so the record must not outlive
	// it. The ALTER is bounded and gets one try, so a lock timeout is an
	// ordinary outcome rather than the crash the record above is for -- and
	// a record standing over a table this workflow never widened is a claim
	// on somebody else's FULL. The swap's recheck would exempt it as its
	// own, and the teardown would put the identity "back" to DEFAULT over a
	// setting the move never made.
	if after := recorded(); slices.Contains(after, 0) {
		t.Errorf("the workflow still records %v after an ALTER that failed: it claims a FULL it never raised", after)
	}
}

// TestTheRestoreLeavesAnIdentityTheMoveNoLongerOwns: the record says what
// the move DID, not what the table IS. If the identity is no longer the
// FULL this workflow raised -- somebody set USING INDEX, or put it back
// themselves -- then putting "back" a DEFAULT over that changes the
// table's replication semantics on the way out of a workflow that is
// already failing.
//
// The swap's recheck refuses a table whose identity the move did not raise,
// which makes the fail path the likely way to reach this; and now that the
// teardown writes through a retired set's pause, the refusal that used to
// stop it by accident is gone.
func TestTheRestoreLeavesAnIdentityTheMoveNoLongerOwns(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	dsn := startPostgres(t)
	cat := connect(t, dsn)
	if err := catalog.Migrate(ctx, cat); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	mustExec(t, cat, `CREATE TABLE ledger (id int PRIMARY KEY, v text)`)
	mustExec(t, cat, `CREATE UNIQUE INDEX ledger_v_key ON ledger (v)`)
	mustExec(t, cat, `ALTER TABLE ledger ALTER COLUMN v SET NOT NULL`)
	// What the move raised, and then what somebody else set over it.
	mustExec(t, cat, `ALTER TABLE ledger REPLICA IDENTITY FULL`)
	mustExec(t, cat, `ALTER TABLE ledger REPLICA IDENTITY USING INDEX ledger_v_key`)

	const id = "77777777-7777-7777-7777-777777777777"
	mustExec(t, cat, `INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES ($1, $2, $3, '{}', '{}')`, id, KindTablePlacement, StateFailed)
	wf := &placementWorkflow{id: id, state: StateFailed, stage: StageFailed,
		spec:  placementSpec{Database: "postgres", SchemaName: "public", TableName: "ledger"},
		st:    placementState{SourceSet: "default", ReplicaIdentityFull: []int32{0}},
		shape: rowShape{Schema: "public", Name: "ledger"}}
	p := &Placer{Pool: pool, Shards: realShards{dsn}}

	if err := p.dropSourceReplication(ctx, wf, 0); err != nil {
		t.Fatalf("dropping the source replication: %v", err)
	}
	var ident string
	if err := cat.QueryRow(ctx, `SELECT relreplident::text FROM pg_class WHERE oid = 'ledger'::regclass`).Scan(&ident); err != nil {
		t.Fatal(err)
	}
	if ident != "i" {
		t.Errorf("the replica identity is %q after the teardown, want \"i\": the move recorded a FULL it had raised, but the table had been set to USING INDEX since, and putting back DEFAULT changes what every UPDATE and DELETE ships to every subscriber", ident)
	}
}

// alterReplicaIdentityDies stands in for a controller that dies as it
// widens a replica identity.
type alterReplicaIdentityDies struct {
	ShardConn
	before func()
}

func (c alterReplicaIdentityDies) Exec(ctx context.Context, sql string, args ...any) (CommandTag, error) {
	if strings.Contains(sql, "REPLICA IDENTITY FULL") {
		c.before()
		return nil, errors.New("the controller died here")
	}
	return c.ShardConn.Exec(ctx, sql, args...)
}

// TestARecheckRefusesAnIdentityTheMoveDidNotRaise (PGS-859, from the
// pre-merge audit): the recheck before the swap exempted "replica identity
// FULL" wherever it found it, rather than only where this workflow had
// raised it.
//
// The preflight refuses any non-default replica identity, so a table that
// reaches the copy is at DEFAULT and the shadow is built from that. Set FULL
// between the preflight and the copy and the workflow never records it --
// ensureReplication only records what it raises, and the table is already
// FULL -- so the exemption swallowed a dependent the move cannot carry, and
// the swap put back a table at DEFAULT, silently breaking the downstream
// logical replication of its UPDATEs and DELETEs.
func TestARecheckRefusesAnIdentityTheMoveDidNotRaise(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	dsn := startPostgres(t)
	cat := connect(t, dsn)
	if err := catalog.Migrate(ctx, cat); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	mustExec(t, cat, `CREATE TABLE ledger (id int PRIMARY KEY, v text)`)
	const id = "77777777-7777-7777-7777-777777777777"
	mustExec(t, cat, `INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES ($1, $2, $3, '{}', '{}')`, id, KindTablePlacement, StateRunning)
	p := &Placer{Pool: pool, Shards: realShards{dsn}}
	newWF := func() *placementWorkflow {
		return &placementWorkflow{id: id, state: StateRunning, stage: StagePlacementCopying,
			spec:  placementSpec{Database: "postgres", SchemaName: "public", TableName: "ledger"},
			st:    placementState{SourceSet: "default"},
			from:  &placementRouter{placement: TablePlacement{Placement: "unsharded"}, home: 0},
			shape: rowShape{Schema: "public", Name: "ledger"}}
	}

	// At DEFAULT, with nothing else on the table, the recheck passes.
	if err := p.recheckDependents(ctx, newWF()); err != nil {
		t.Fatalf("a table with no dependents was refused: %v", err)
	}

	// FULL that the workflow did not raise is a dependent like any other.
	mustExec(t, cat, `ALTER TABLE ledger REPLICA IDENTITY FULL`)
	err = p.recheckDependents(ctx, newWF())
	if err == nil {
		t.Fatal("a replica identity the move never raised was taken for its own; the swap would have dropped it")
	}
	if !strings.Contains(err.Error(), "replica identity FULL") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}

	// The same identity, recorded as this workflow's, is its own to ignore.
	wf := newWF()
	wf.st.ReplicaIdentityFull = []int32{0}
	if err := p.recheckDependents(ctx, wf); err != nil {
		t.Fatalf("the workflow's own widened identity was refused: %v", err)
	}
}

// TestRetirementDropsTheOldTableThroughAWritePause (PGS-859, from the
// pre-merge audit): dropOld was the one cleanup of the family that was not
// given the write-through.
//
// Retirement lasts an hour by default, and a barrier pauses every serving
// group for its own run, so the DROP TABLE and the constraint and index
// renames are refused with 25006 just as the shadow and replication
// cleanups were. A retirement that cannot finish keeps the placement
// active, and an active placement makes a reshard wait and an upgrade fail
// its preconditions -- with a whole second copy of the table still on disk
// on every shard.
func TestRetirementDropsTheOldTableThroughAWritePause(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	dsn := startPostgres(t)
	admin := connect(t, dsn)
	wf := &placementWorkflow{id: "55555555-5555-5555-5555-555555555555", stage: StagePlacementRetiring,
		spec:  placementSpec{Database: "postgres", SchemaName: "public", TableName: "ledger"},
		st:    placementState{SourceSet: "default"},
		from:  &placementRouter{placement: TablePlacement{Placement: "unsharded"}, home: 0},
		rt:    &placementRouter{placement: TablePlacement{Placement: "unsharded"}, home: 0, ids: []int32{0}},
		shape: rowShape{Schema: "public", Name: "ledger"}}
	// The table as the swap leaves it: the live one carrying the shadow's
	// index names, and the retired one alongside it.
	mustExec(t, admin, `CREATE TABLE ledger (id int PRIMARY KEY, v text)`)
	mustExec(t, admin, `CREATE INDEX `+QuoteIdent("ledger_v_idx"+ShadowSuffix)+` ON ledger (v)`)
	mustExec(t, admin, `CREATE TABLE `+QuoteIdent(wf.old())+` (id int PRIMARY KEY, v text)`)
	if err := markPlacementArtifact(ctx, pgxShardConn{admin}, "public", wf.old(), wf.placementMarker()); err != nil {
		t.Fatal(err)
	}
	mustExec(t, admin, `ALTER SYSTEM SET default_transaction_read_only = on`)
	mustExec(t, admin, `SELECT pg_reload_conf()`)
	waitReadOnly(t, dsn, true)

	p := &Placer{Shards: realShards{dsn}}
	if err := p.dropOld(ctx, wf); err != nil {
		t.Fatalf("retiring on a paused shard: %v", err)
	}
	check := connect(t, dsn)
	if n := queryOne[int64](t, check, `SELECT count(*) FROM pg_class WHERE relname = $1`, wf.old()); n != 0 {
		t.Errorf("the retired table survived its own retirement")
	}
	if n := queryOne[int64](t, check, `SELECT count(*) FROM pg_class WHERE relname LIKE '%' || $1 || '%'`, ShadowSuffix); n != 0 {
		t.Errorf("%d relation(s) kept a shadow-suffixed name after retirement", n)
	}
	// The shard is still paused: only this session wrote through it.
	waitReadOnly(t, dsn, true)
}

// TestRetiringGivesUpItsLockRatherThanQueueing (PGS-943): retirement writes
// through a retired set's write pause, so its backend is counted as a
// writer by any barrier running at the same time. Queued unboundedly on the
// old table's AccessExclusiveLock behind a long reader, it holds an open
// transaction for as long as that reader lasts -- and a barrier taken
// meanwhile waits on it until DrainTimeout and fails.
//
// Retirement is re-driven by the next pass, so giving up costs a pass. A
// lost barrier costs the operator the restore point they were taking, and
// the cause would be nowhere near the symptom.
func TestRetiringGivesUpItsLockRatherThanQueueing(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	dsn := startPostgres(t)
	admin := connect(t, dsn)
	mustExec(t, admin, `CREATE TABLE ledger__pgshard_old (id int PRIMARY KEY, v text)`)
	mustExec(t, admin, `COMMENT ON TABLE ledger__pgshard_old IS 'pgshard:placement:77777777-7777-7777-7777-777777777777'`)

	// A reader that outlasts every try, on its own connection.
	tx, err := connect(t, dsn).Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT count(*) FROM ledger__pgshard_old`); err != nil {
		t.Fatal(err)
	}

	conn := pgxShardConn{connect(t, dsn)}
	started := time.Now()
	_, derr := dropArtifactTable(ctx, conn, "public", "ledger__pgshard_old", "pgshard:placement:77777777-7777-7777-7777-777777777777")
	waited := time.Since(started)

	var pgErr *pgconn.PgError
	if !errors.As(derr, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("the drop answered %v, want a 55P03 lock timeout: unbounded, it holds an open transaction for as long as the reader runs and fails any barrier drained meanwhile", derr)
	}
	if waited > 4*artifactDropLockWait {
		t.Errorf("the drop waited %s for its lock, bound is %s: it is not giving up", waited, artifactDropLockWait)
	}
	// The reader was never blocked by it, which is the other half: a
	// request queued on AccessExclusive also queues every later reader.
	if _, err := tx.Exec(ctx, `SELECT count(*) FROM ledger__pgshard_old`); err != nil {
		t.Fatalf("the reader was disturbed by the drop's wait: %v", err)
	}
}

// readOnlySetDies fails exactly the statement writeThroughPause sends, so a
// teardown can be driven through a source whose pause cannot be written
// through.
type readOnlySetDies struct{ ShardConn }

func (c readOnlySetDies) Exec(ctx context.Context, sql string, args ...any) (CommandTag, error) {
	if strings.Contains(sql, "default_transaction_read_only") {
		return nil, errors.New("the pause could not be written through")
	}
	return c.ShardConn.Exec(ctx, sql, args...)
}

type shardsDialing struct{ conn ShardConn }

func (s shardsDialing) Dial(context.Context, string, int32) (ShardConn, error) { return s.conn, nil }
func (s shardsDialing) DialDatabase(context.Context, string, int32, string) (ShardConn, error) {
	return s.conn, nil
}

// TestTheTeardownDropsItsSlotEvenWhenThePauseCannotBeWrittenThrough
// (PGS-944): dropping a replication slot is not a write to the table, so no
// pause refuses it. Gating it behind writeThroughPause meant a failed SET
// left the slot standing, and a slot left behind pins WAL on the source for
// as long as it stands -- the more expensive failure of the two.
func TestTheTeardownDropsItsSlotEvenWhenThePauseCannotBeWrittenThrough(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	dsn := startPostgres(t)
	admin := connect(t, dsn)

	const id = "66666666-6666-6666-6666-666666666666"
	wf := &placementWorkflow{id: id, stage: StageFailed,
		spec:  placementSpec{Database: "postgres", SchemaName: "public", TableName: "ledger"},
		st:    placementState{SourceSet: "default"},
		shape: rowShape{Schema: "public", Name: "ledger"}}

	slot := wf.slotName(0)
	mustExec(t, admin, `SELECT pg_create_physical_replication_slot($1)`, slot)
	if n := queryOne[int64](t, admin, `SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1`, slot); n != 1 {
		t.Fatalf("the slot was not created, so the assertion below would pass for the wrong reason")
	}

	p := &Placer{Shards: shardsDialing{conn: readOnlySetDies{pgxShardConn{connect(t, dsn)}}}}
	err := p.dropSourceReplication(ctx, wf, 0)
	if err == nil {
		t.Fatal("the write-through was supposed to fail, so this test proves nothing")
	}
	if n := queryOne[int64](t, admin, `SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1`, slot); n != 0 {
		t.Errorf("the slot is still there after a teardown whose write-through failed: it pins WAL on the source for as long as it stands")
	}
}

// dialFails stands in for a source that cannot be reached at all.
type dialFails struct{}

func (dialFails) Dial(context.Context, string, int32) (ShardConn, error) {
	return nil, errors.New("the source is unreachable")
}
func (dialFails) DialDatabase(context.Context, string, int32, string) (ShardConn, error) {
	return nil, errors.New("the source is unreachable")
}

// TestAFailedPlacementRecordsWhatItCouldNotClean (PGS-945): fail() drops the
// replication objects and the shadows best-effort, and RECORDS the failures
// rather than returning them -- a source that cannot be reached must not
// stop the workflow being marked failed, or the next pass gives up the fence
// and the lock again and the workflow never ends.
//
// The recording is the whole point. What is left behind pins WAL on the
// source, and a replica identity left widened makes every UPDATE and DELETE
// ship the whole old row to every subscriber. status.leaked is the only
// place an operator learns either happened, and nothing asserted it was
// written -- a status field written by one path and asserted by none is how
// one silently stops being written.
func TestAFailedPlacementRecordsWhatItCouldNotClean(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	dsn := startPostgres(t)
	cat := connect(t, dsn)
	if err := catalog.Migrate(ctx, cat); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	const id = "55555555-5555-5555-5555-555555555555"
	mustExec(t, cat, `INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES ($1, $2, $3, '{}', '{}')`, id, KindTablePlacement, StateRunning)
	wf := &placementWorkflow{id: id, state: StateRunning, stage: StagePlacementCopying,
		spec:  placementSpec{Database: "postgres", SchemaName: "public", TableName: "ledger"},
		st:    placementState{SourceSet: "default"},
		from:  &placementRouter{placement: TablePlacement{Placement: "unsharded"}, home: 0},
		shape: rowShape{Schema: "public", Name: "ledger"}}

	p := &Placer{Pool: pool, Shards: dialFails{}}
	if err := p.fail(ctx, wf, errors.New("the move failed")); err != nil {
		t.Fatalf("fail() returned %v; an unreachable source must not stop the workflow being marked failed", err)
	}

	state := queryOne[string](t, cat, `SELECT state FROM pgshard.workflows WHERE id = $1::uuid`, id)
	if state != StateFailed {
		t.Fatalf("the workflow is %q, want failed", state)
	}
	leaked := queryOne[string](t, cat, `SELECT coalesce(status->>'leaked', '') FROM pgshard.workflows WHERE id = $1::uuid`, id)
	if leaked == "" {
		t.Fatal("status.leaked is empty after a failure that could not reach its source: the replication objects it left pin WAL, and nothing tells the operator")
	}
	if !strings.Contains(leaked, "unreachable") {
		t.Errorf("status.leaked = %q; it must name what went wrong, not merely that something did", leaked)
	}
}
