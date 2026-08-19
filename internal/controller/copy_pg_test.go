package controller

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/placement"
)

// copyFixture is a catalog, two serving source shards and two non-serving
// target shards on one docker network, so targets can subscribe to sources
// by container name while the test reaches everything through published
// ports.
type copyFixture struct {
	t       *testing.T
	net     string
	pool    *pgxpool.Pool
	catalog *pgx.Conn
	dialer  *PgxShardDialer
	dsns    map[ShardRef]string
	srcRng  placement.RangeSet
	tgtRng  placement.RangeSet
	copier  *Copier
}

var logicalOpts = []string{"-c wal_level=logical", "-c max_prepared_transactions=16", "-c max_replication_slots=16", "-c max_wal_senders=16",
	"-c max_logical_replication_workers=16", "-c max_worker_processes=32", "-c max_sync_workers_per_subscription=4"}

func (f *copyFixture) container(role string, id int) string {
	return fmt.Sprintf("%s-%s%d", f.net, role, id)
}

func newCopyFixture(t *testing.T) *copyFixture {
	t.Helper()
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker unavailable; skipping reshard copy integration test")
	}
	f := &copyFixture{t: t, net: fmt.Sprintf("pgshard-copy-%d", time.Now().UnixNano()%1_000_000)}
	if out, err := exec.Command("docker", "network", "create", f.net).CombinedOutput(); err != nil {
		t.Fatalf("docker network create: %v: %s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "network", "rm", f.net).Run() })
	ctx := context.Background()
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
	f.dsns = map[ShardRef]string{}
	for _, role := range []string{"src", "tgt"} {
		for id := range 2 {
			name := f.container(role, id)
			dsn := startPostgresImage(t, pgImage, []string{"--network", f.net, "--name", name}, logicalOpts...)
			set := "default"
			if role == "tgt" {
				set = "g2"
			}
			f.dsns[ShardRef{Set: set, ID: int32(id)}] = dsn
		}
	}
	f.dialer = &PgxShardDialer{Pool: pool, DSNs: f.dsns}
	f.srcRng, _ = placement.Split(2)
	f.tgtRng = placement.RangeSet{{Start: math.MinInt64, End: -1_000_000_000_000}, {Start: -1_000_000_000_000 + 1, End: math.MaxInt64}}

	tx, err := f.catalog.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.MaterializeShardSet(ctx, tx, "default", 1, catalog.ShardSetServing, f.srcRng); err != nil {
		t.Fatal(err)
	}
	if err := catalog.MaterializeShardSet(ctx, tx, "g2", 2, catalog.ShardSetProvisioning, f.tgtRng); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"src", "tgt"} {
		for id := range 2 {
			set, state := "default", "serving"
			if role == "tgt" {
				set, state = "g2", "provisioning"
			}
			mustExec(t, f.catalog, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_epoch, primary_endpoint)
				VALUES ($1, $2, $3, $4, 1, $5)`, set, id, fmt.Sprintf("%s%d", role, id), state, f.container(role, id)+":5432")
		}
	}
	mustExec(t, f.catalog, `INSERT INTO pgshard.databases (name, default_placement, home_shard) VALUES ('app', 'unsharded', 0)`)
	for _, row := range [][3]string{{"orders", "sharded", "tenant_id"}, {"docs", "sharded", "slug"}, {"regions", "reference", ""}, {"items", "unsharded", ""}} {
		var key *string
		if row[2] != "" {
			key = &row[2]
		}
		mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('app', 'public', $1, $2, $3)`, row[0], row[1], key)
	}
	for id := range 2 {
		c := connect(t, f.dsns[ShardRef{Set: "default", ID: int32(id)}])
		mustExec(t, c, `CREATE DATABASE app`)
		c = connect(t, f.appDSN("default", int32(id)))
		for _, stmt := range []string{
			`CREATE TABLE orders (id bigserial, tenant_id bigint NOT NULL, note text, PRIMARY KEY (tenant_id, id))`,
			`CREATE INDEX orders_note_idx ON orders (note)`,
			`CREATE TABLE docs (slug text PRIMARY KEY, body text)`,
			`CREATE TABLE regions (code text PRIMARY KEY, name text)`,
			`CREATE TABLE items (id serial PRIMARY KEY, v text)`,
			`CREATE TABLE notes (v text)`,
			`CREATE SEQUENCE ticket_seq START 500`,
			`CREATE TYPE mood AS ENUM ('ok', 'meh')`,
			`CREATE VIEW order_count AS SELECT count(*) FROM orders`,
		} {
			mustExec(t, c, stmt)
		}
		mustExec(t, c, `INSERT INTO regions VALUES ('eu', 'Europe'), ('us', 'United States')`)
	}
	f.seed(0, 2000)
	home := connect(t, f.appDSN("default", 0))
	mustExec(t, home, `INSERT INTO items (v) SELECT 'item-' || g FROM generate_series(1, 50) g`)
	mustExec(t, home, `INSERT INTO notes SELECT 'note-' || g FROM generate_series(1, 20) g`)

	f.copier = &Copier{Pool: pool, Shards: f.dialer, Schema: f.materializer(), SourceConnInfo: f.connInfo, LagBytes: 1 << 20, PreparedWait: time.Hour}
	return f
}

func (f *copyFixture) appDSN(set string, id int32) string {
	return strings.Replace(f.dsns[ShardRef{Set: set, ID: id}], "/postgres?", "/app?", 1)
}

// connInfo is the in-network libpq string of one shard database.
func (f *copyFixture) connInfo(_ context.Context, ref ShardRef, database string) (string, error) {
	role := "src"
	if ref.Set == "g2" {
		role = "tgt"
	}
	return fmt.Sprintf("host=%s port=5432 user=postgres dbname=%s", f.container(role, int(ref.ID)), database), nil
}

// materializer is the ExecMaterializer over pg_dump and psql wrappers that
// run the binaries inside a container on the network: the controller host
// has no PostgreSQL binaries and no route to the container names.
func (f *copyFixture) materializer() *ExecMaterializer {
	dir := f.t.TempDir()
	for _, bin := range []string{"pg_dump", "psql"} {
		script := fmt.Sprintf("#!/bin/sh\nexec docker exec -i %s %s \"$@\"\n", f.container("src", 0), bin)
		if err := os.WriteFile(filepath.Join(dir, bin), []byte(script), 0o755); err != nil {
			f.t.Fatal(err)
		}
	}
	return &ExecMaterializer{BinDir: dir, TargetConnInfo: f.connInfo}
}

// seed inserts n orders and docs with keys from start on, each on the source
// shard the serving map assigns.
func (f *copyFixture) seed(start, n int) {
	f.t.Helper()
	conns := []*pgx.Conn{connect(f.t, f.appDSN("default", 0)), connect(f.t, f.appDSN("default", 1))}
	for i := start; i < start+n; i++ {
		tenant := int64(i*7919 + 13)
		slug := fmt.Sprintf("doc-%d", i)
		tid, _ := placement.KeyspaceID(tenant)
		mustExec(f.t, conns[f.srcRng.Locate(tid)], `INSERT INTO orders (tenant_id, note) VALUES ($1, $2)`, tenant, "n"+slug)
		sid, _ := placement.KeyspaceID(slug)
		mustExec(f.t, conns[f.srcRng.Locate(sid)], `INSERT INTO docs VALUES ($1, $2)`, slug, "body")
	}
}

func (f *copyFixture) startWorkflow() string {
	f.t.Helper()
	ranges, err := catalog.ListShardRanges(context.Background(), f.catalog, "g2")
	if err != nil {
		f.t.Fatal(err)
	}
	spec := map[string]any{"shard_set": "g2", "generation": 2, "ranges": specRanges(ranges)}
	mustExec(f.t, f.catalog, `INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES (gen_random_uuid(), $1, $2, $3, $4)`,
		KindReshard, StateRunning, mustJSON(spec), mustJSON(map[string]any{"stage": StageReadyForCopy}))
	return queryOne[string](f.t, f.catalog, `SELECT id::text FROM pgshard.workflows WHERE kind = 'reshard'`)
}

func (f *copyFixture) workflow(id string) (state, stage, message string) {
	f.t.Helper()
	if err := f.catalog.QueryRow(context.Background(), `SELECT state, coalesce(status->>'stage', ''), coalesce(status->>'message', '') FROM pgshard.workflows WHERE id = $1::uuid`, id).Scan(&state, &stage, &message); err != nil {
		f.t.Fatal(err)
	}
	return state, stage, message
}

func (f *copyFixture) pass() CopyOutcome {
	f.t.Helper()
	out, err := f.copier.Pass(context.Background())
	if err != nil {
		f.t.Fatalf("pass: %v", err)
	}
	return out
}

func (f *copyFixture) expectedCounts(table string, ranges placement.RangeSet) map[int32]int64 {
	f.t.Helper()
	counts := map[int32]int64{}
	for id := range 2 {
		c := connect(f.t, f.appDSN("default", int32(id)))
		col := "tenant_id"
		if table == "docs" {
			col = "slug"
		}
		rows, err := c.Query(context.Background(), fmt.Sprintf("SELECT %s FROM %s", col, table))
		if err != nil {
			f.t.Fatal(err)
		}
		for rows.Next() {
			var v any
			if err := rows.Scan(&v); err != nil {
				f.t.Fatal(err)
			}
			kid, err := placement.KeyspaceID(v)
			if err != nil {
				f.t.Fatal(err)
			}
			counts[int32(ranges.Locate(kid))]++
		}
		rows.Close()
	}
	return counts
}

// TestReshardCopyOnPostgres drives a 2 -> 2 reshard with shifted ranges
// through the copy phase: schemas land on the targets, an in-doubt prepared
// transaction holds slot creation until it is resolved, rows (including
// ones written during the copy) land on the targets the placement assigns,
// and cancel removes subscriptions, slots and publications.
func TestReshardCopyOnPostgres(t *testing.T) {
	f := newCopyFixture(t)
	ctx := context.Background()
	id := f.startWorkflow()

	// A prepared transaction anywhere on source 1 blocks slot creation there.
	src1 := connect(t, f.appDSN("default", 1))
	src1Postgres := connect(t, f.dsns[ShardRef{Set: "default", ID: 1}])
	for _, sql := range []string{"BEGIN", "SELECT pg_current_xact_id()", "PREPARE TRANSACTION 'pgshard-indoubt'"} {
		mustExec(t, src1Postgres, sql)
	}
	out := f.pass()
	if out.Driven != 1 || out.Advanced != 1 || out.Failed != 0 {
		t.Fatalf("first pass: %+v", out)
	}
	state, stage, msg := f.workflow(id)
	if state != StateRunning || stage != StageCopying || !strings.Contains(msg, "waits for prepared transactions [pgshard-indoubt]") {
		t.Fatalf("after first pass: %s %s %q", state, stage, msg)
	}
	for id := range 2 {
		tgt := connect(t, f.appDSN("g2", int32(id)))
		if n := queryOne[int64](t, tgt, `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname IN ('orders', 'docs', 'regions', 'items', 'notes', 'orders_note_idx', 'ticket_seq', 'order_count')`); n != 8 {
			t.Fatalf("target %d: %d of 8 schema objects materialized", id, n)
		}
		if v := queryOne[string](t, tgt, `SELECT enum_range(NULL::mood)::text`); v != "{ok,meh}" {
			t.Fatalf("target %d enum: %s", id, v)
		}
		if n := queryOne[int64](t, tgt, `SELECT count(*) FROM pg_subscription WHERE subname LIKE '%\_s1'`); n != 0 {
			t.Fatalf("target %d subscribed to the blocked source", id)
		}
	}
	if n := queryOne[int64](t, src1, `SELECT count(*) FROM pg_publication`); n != 2 {
		t.Fatalf("source 1 publications: %d", n)
	}
	src0 := connect(t, f.appDSN("default", 0))
	if n := queryOne[int64](t, src0, `SELECT count(*) FROM pg_publication`); n != 4 {
		t.Fatalf("source 0 publications (2 targets + ref + home): %d", n)
	}
	var ident string
	if err := src0.QueryRow(ctx, `SELECT relreplident FROM pg_class WHERE relname = 'notes'`).Scan(&ident); err != nil || ident != "f" {
		t.Fatalf("notes without a key must get REPLICA IDENTITY FULL: %q %v", ident, err)
	}
	if err := src0.QueryRow(ctx, `SELECT relreplident FROM pg_class WHERE relname = 'orders'`).Scan(&ident); err != nil || ident != "d" {
		t.Fatalf("orders' primary key covers the shard key: %q %v", ident, err)
	}

	// The resolver (decision: abort) clears the in-doubt transaction and
	// the copy proceeds.
	mustExecPool(t, f.pool, `INSERT INTO pgshard.xact_decisions (gid, state, participants) VALUES ('pgshard-indoubt', 'abort', '{1}')`)
	f.copier.Resolver = &Resolver{Pool: f.pool, Shards: f.dialer}
	f.pass()
	if n := queryOne[int64](t, src1, `SELECT count(*) FROM pg_prepared_xacts`); n != 0 {
		t.Fatal("prepared transaction not resolved")
	}
	_, _, msg = f.workflow(id)
	if strings.Contains(msg, "prepared") {
		t.Fatalf("still blocked: %s", msg)
	}

	// A restarted controller re-drives the workflow from the catalogs alone.
	f.copier = &Copier{Pool: f.pool, Shards: f.dialer, Schema: f.materializer(), SourceConnInfo: f.connInfo, LagBytes: 1 << 20, Resolver: f.copier.Resolver}

	// Writes during the copy land on the targets too.
	f.seed(2000, 300)
	mustExec(t, src0, `UPDATE items SET v = v || '!' WHERE id <= 10`)
	mustExec(t, src0, `DELETE FROM notes WHERE v = 'note-1'`)
	deadline := time.Now().Add(3 * time.Minute)
	for {
		f.pass()
		state, stage, msg = f.workflow(id)
		if stage == StageCatchUpDone {
			break
		}
		if state != StateRunning || time.Now().After(deadline) {
			t.Fatalf("copy did not catch up: %s %s %q", state, stage, msg)
		}
		time.Sleep(time.Second)
	}
	f.seed(2300, 50)
	time.Sleep(2 * time.Second)
	f.pass()
	var progress string
	if err := f.catalog.QueryRow(ctx, `SELECT status->'copy'->'progress' FROM pgshard.workflows WHERE id = $1::uuid`, id).Scan(&progress); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(progress, `"subscriptions": 4`) || !strings.Contains(progress, `"paused": 0`) {
		t.Fatalf("progress %s", progress)
	}

	for _, table := range []string{"orders", "docs"} {
		want := f.expectedCounts(table, f.tgtRng)
		for tid := range 2 {
			tgt := connect(t, f.appDSN("g2", int32(tid)))
			got := queryOne[int64](t, tgt, "SELECT count(*) FROM "+table)
			if got != want[int32(tid)] {
				t.Errorf("%s on target %d: %d rows, placement assigns %d", table, tid, got, want[int32(tid)])
			}
		}
		if want[0]+want[1] != 2350 {
			t.Errorf("%s: %d rows expected in total", table, want[0]+want[1])
		}
	}
	homeTarget := f.tgtRng.Locate(0)
	for tid := range 2 {
		tgt := connect(t, f.appDSN("g2", int32(tid)))
		if n := queryOne[int64](t, tgt, `SELECT count(*) FROM regions`); n != 2 {
			t.Errorf("target %d regions: %d", tid, n)
		}
		items, notes := queryOne[int64](t, tgt, `SELECT count(*) FROM items`), queryOne[int64](t, tgt, `SELECT count(*) FROM notes`)
		bang := queryOne[int64](t, tgt, `SELECT count(*) FROM items WHERE v LIKE '%!'`)
		if tid == homeTarget && (items != 50 || notes != 19 || bang != 10) {
			t.Errorf("home target %d: items %d notes %d updated %d", tid, items, notes, bang)
		}
		if tid != homeTarget && (items != 0 || notes != 0) {
			t.Errorf("target %d must not carry unsharded tables: items %d notes %d", tid, items, notes)
		}
	}

	// Cancel: the set vanishes, the reconciler marks the workflow, the
	// copier cleans every shard.
	if err := f.reconcileDrop(ctx); err != nil {
		t.Fatal(err)
	}
	state, stage, _ = f.workflow(id)
	if state != StateCancelled || stage != StageCancelling {
		t.Fatalf("after drop: %s %s", state, stage)
	}
	out = f.pass()
	if out.Cancelled != 1 {
		t.Fatalf("cancel pass: %+v", out)
	}
	state, stage, _ = f.workflow(id)
	if state != StateCancelled || stage != StageCancelled {
		t.Fatalf("after cancel: %s %s", state, stage)
	}
	for sid := range 2 {
		c := connect(t, f.dsns[ShardRef{Set: "default", ID: int32(sid)}])
		if n := queryOne[int64](t, c, `SELECT count(*) FROM pg_replication_slots`); n != 0 {
			t.Errorf("source %d slots left: %d", sid, n)
		}
		app := connect(t, f.appDSN("default", int32(sid)))
		if n := queryOne[int64](t, app, `SELECT count(*) FROM pg_publication`); n != 0 {
			t.Errorf("source %d publications left: %d", sid, n)
		}
	}
	for tid := range 2 {
		tgt := connect(t, f.appDSN("g2", int32(tid)))
		if n := queryOne[int64](t, tgt, `SELECT count(*) FROM pg_subscription`); n != 0 {
			t.Errorf("target %d subscriptions left: %d", tid, n)
		}
	}
	if out := f.pass(); out.Driven != 0 {
		t.Fatalf("cancelled workflow driven again: %+v", out)
	}
}

// reconcileDrop drops the pending set the way the operator does and runs
// the reconciler, which marks the copy workflow for cancellation.
func (f *copyFixture) reconcileDrop(ctx context.Context) error {
	tx, err := f.catalog.Begin(ctx)
	if err != nil {
		return err
	}
	if err := catalog.DropShardSet(ctx, tx, "g2"); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	res, err := Reconcile(ctx, f.catalog, nil)
	if err != nil {
		return err
	}
	if res.ReshardsCancelled != 1 {
		return fmt.Errorf("reconcile cancelled %d reshards", res.ReshardsCancelled)
	}
	return nil
}
