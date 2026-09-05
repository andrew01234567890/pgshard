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
// user trigger or a foreign key is refused at preflight, because the shadow
// build recreates neither and the swap would silently drop enforcement.
//
// Row-level security used to be on that list and is not: policies and both
// RLS flags are reproduced now (TestAMoveKeepsRowLevelSecurity), so it is
// the one class this refusal has stopped covering.
func TestPlacementRefusesUnsupportedFeaturesOnPostgres(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	home := f.app(0)
	mustExec(t, home, `CREATE TABLE guarded (id bigint PRIMARY KEY, owner text)`)
	mustExec(t, home, `CREATE FUNCTION stamp_owner() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN NEW.owner := current_user; RETURN NEW; END $$`)
	mustExec(t, home, `CREATE TRIGGER own_rows BEFORE INSERT ON guarded FOR EACH ROW EXECUTE FUNCTION stamp_owner()`)
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
			t.Fatal("move of a table with a user trigger completed instead of being refused")
		}
	}
	if state != StateFailed || !strings.Contains(msg, "trigger own_rows") {
		t.Fatalf("expected refusal naming the trigger, got %s %q", state, msg)
	}
	// The trigger is untouched.
	if n := queryOne[int64](t, home, `SELECT count(*) FROM pg_trigger WHERE tgname = 'own_rows'`); n != 1 {
		t.Fatal("the trigger was dropped")
	}
	// The pure-feature detector covers each unsupported shape.
	mustExec(t, home, `CREATE TABLE parent (pid bigint PRIMARY KEY)`)
	mustExec(t, home, `CREATE TABLE child (id bigint PRIMARY KEY, pid bigint REFERENCES parent(pid))`)
	for _, c := range []struct{ table, want string }{{"child", "foreign key"}, {"parent", "foreign key"}} {
		got, err := unsupportedTableFeatures(ctx, pgxShardConn{home}, "public", c.table)
		if err != nil || len(got) != 1 || !strings.HasPrefix(got[0], c.want) {
			t.Fatalf("%s: unsupported = %v (%v), want one %q", c.table, got, err, c.want)
		}
	}
	if got, err := unsupportedTableFeatures(ctx, pgxShardConn{home}, "public", "parent"); err != nil || len(got) != 1 {
		t.Fatalf("parent inbound fk: %v %v", got, err)
	}
	// Shapes lost by both shadow paths that carry no policy/trigger/FK.
	mustExec(t, home, `CREATE ROLE reader`)
	for _, c := range []struct{ ddl, table, want string }{
		{`CREATE TABLE granted (id bigint PRIMARY KEY); GRANT SELECT ON granted TO reader`, "granted", "table privileges"},
		{`CREATE TABLE colgrant (id bigint PRIMARY KEY, v text); GRANT SELECT (v) ON colgrant TO reader`, "colgrant", "column privileges on v"},
		{`CREATE TABLE ruled (id bigint PRIMARY KEY); CREATE RULE r1 AS ON DELETE TO ruled DO INSTEAD NOTHING`, "ruled", "rule r1"},
		{`CREATE TABLE base (id bigint PRIMARY KEY); CREATE TABLE inh (x int) INHERITS (base)`, "inh", "inheritance/partition membership"},
		{`CREATE TABLE ri (id bigint PRIMARY KEY); ALTER TABLE ri REPLICA IDENTITY FULL`, "ri", "replica identity FULL"},
	} {
		mustExec(t, home, c.ddl)
		got, err := unsupportedTableFeatures(ctx, pgxShardConn{home}, "public", c.table)
		if err != nil || !slices.Contains(got, c.want) {
			t.Fatalf("%s: unsupported = %v (%v), want %q", c.table, got, err, c.want)
		}
	}
	// And what is no longer refused: a table with row-level security and a
	// policy is moved, not stopped.
	mustExec(t, home, `CREATE TABLE rlsonly (id bigint PRIMARY KEY, owner text)`)
	mustExec(t, home, `ALTER TABLE rlsonly ENABLE ROW LEVEL SECURITY`)
	mustExec(t, home, `CREATE POLICY own_rows ON rlsonly USING (owner = current_user)`)
	if got, err := unsupportedTableFeatures(ctx, pgxShardConn{home}, "public", "rlsonly"); err != nil || len(got) != 0 {
		t.Fatalf("row-level security is reproduced now, not refused: %v %v", got, err)
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
