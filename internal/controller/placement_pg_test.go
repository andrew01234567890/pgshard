package controller

import (
	"context"
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
	if err := catalog.MaterializeShardSet(ctx, tx, "default", 1, catalog.ShardSetServing, f.ranges); err != nil {
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
		{"excl_eq", "v", []string{"excl_eq_v_excl"}},             // exclusion refused (not recreated on target shadows)
		{"temporal", "id", []string{"temporal_pkey"}},            // temporal WITHOUT OVERLAPS is an exclusion index
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
	f := newPlacementFixture(t)
	home := f.app(0)
	mustExec(t, home, `CREATE TABLE things (
		tenant bigint NOT NULL,
		seq bigint GENERATED BY DEFAULT AS IDENTITY,
		tag bigint GENERATED ALWAYS AS IDENTITY,
		ser bigserial,
		note text,
		PRIMARY KEY (tenant, seq))`)
	mustExec(t, home, `INSERT INTO things (tenant, note) SELECT g, 'n' || g FROM generate_series(1, 60) g`)
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
}

// TestPlacementRefusesUserArtifactTableOnPostgres: a user table that happens
// to share the __pgshard_new shadow name must never be adopted, written into
// or dropped; the move fails loudly and leaves the table intact.
func TestPlacementRefusesUserArtifactTableOnPostgres(t *testing.T) {
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
	if state != StateFailed || !strings.Contains(msg, "not a pgshard shadow") {
		t.Fatalf("expected refusal, got %s %q", state, msg)
	}
	// The user's table and its row must be untouched.
	if v := queryOne[string](t, other, `SELECT keep FROM items__pgshard_new`); v != "precious" {
		t.Fatalf("user table was modified: %q", v)
	}
	if n := queryOne[int64](t, other, `SELECT count(*) FROM information_schema.columns WHERE table_name = 'items__pgshard_new'`); n != 1 {
		t.Fatalf("user table schema changed: %d columns", n)
	}
}
