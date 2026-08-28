package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/placement"
)

// TestUpgradeWorkflowKindOnPostgres: the reconciler creates a kind=upgrade
// workflow for a pending set stamped with a different major and a plain
// reshard workflow for an unstamped one.
func TestUpgradeWorkflowKindOnPostgres(t *testing.T) {
	parallelPG(t)
	dsn := startPostgresWith(t)
	conn := connect(t, dsn)
	ctx := context.Background()
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	ranges, _ := placement.Split(2)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.MaterializeShardSet(ctx, tx, "default", 1, catalog.ShardSetServing, ranges, 0); err != nil {
		t.Fatal(err)
	}
	if err := catalog.MaterializeShardSet(ctx, tx, "g2", 2, catalog.ShardSetDesired, ranges, 0); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetShardSetMajor(ctx, tx, "default", 18); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetShardSetMajor(ctx, tx, "g2", 19); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := Reconcile(ctx, conn, nil); err != nil {
		t.Fatal(err)
	}
	if kind := queryOne[string](t, conn, `SELECT kind FROM pgshard.workflows WHERE spec->>'shard_set' = 'g2'`); kind != KindUpgrade {
		t.Fatalf("workflow kind %s, want upgrade", kind)
	}
	if major := queryOne[string](t, conn, `SELECT spec->>'pg_major' FROM pgshard.workflows WHERE spec->>'shard_set' = 'g2'`); major != "19" {
		t.Fatalf("workflow pg_major %s", major)
	}
}

// TestUpgrade18To19OnPostgres runs the whole online upgrade against real
// PostgreSQL 18 sources and PostgreSQL 19 targets: preconditions, schema
// materialization, logical copy, cutover with the sequence handoff, and
// retirement. The targets end up serving every row on the new major with
// sequences that continue past the source values.
func TestUpgrade18To19OnPostgres(t *testing.T) {
	parallelPG(t)
	f := newUpgradeFixture(t)
	ctx := context.Background()
	id := f.startWorkflowKind(KindUpgrade)
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET spec = spec || '{"retire_after_seconds": 1}' WHERE id = $1::uuid`, id)

	home := connect(t, f.appDSN("default", 0))
	var seq int64
	for range 7 {
		if err := home.QueryRow(ctx, `SELECT nextval('ticket_seq')`).Scan(&seq); err != nil {
			t.Fatal(err)
		}
	}

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
	if stage != StageCompleted {
		t.Fatalf("upgrade did not complete: %s %s %q", state, stage, msg)
	}

	if set := queryOne[string](t, f.catalog, `SELECT shard_set FROM pgshard.shard_sets WHERE state = 'serving'`); set != "g2" {
		t.Fatalf("serving set %s", set)
	}

	// The retired set keeps running for the retirement window and its -rw
	// Service still answers. Nothing reads from it again, so a write made
	// straight to it would be acknowledged and then deleted with the
	// group; being told no is the difference between that and losing data.
	for sid := range 2 {
		old := connect(t, f.appDSN("default", int32(sid)))
		if _, err := old.Exec(ctx, `INSERT INTO orders (tenant_id, note) VALUES (1, 'after-retirement')`); err == nil {
			t.Errorf("retired source %d still accepted a write", sid)
		} else if !strings.Contains(err.Error(), "read-only") {
			t.Errorf("retired source %d refused with %v, want a read-only transaction error", sid, err)
		}
		if _, err := old.Exec(ctx, `SELECT count(*) FROM orders`); err != nil {
			t.Errorf("retired source %d must still answer reads: %v", sid, err)
		}
	}
	for tid := range 2 {
		tgt := connect(t, f.appDSN("g2", int32(tid)))
		if v := queryOne[string](t, tgt, `SHOW server_version_num`); !strings.HasPrefix(v, "19") {
			t.Fatalf("target %d server_version_num %s", tid, v)
		}
	}
	for _, table := range []string{"orders", "docs"} {
		want := f.expectedCounts(table, f.tgtRng)
		var total int64
		for tid := range 2 {
			tgt := connect(t, f.appDSN("g2", int32(tid)))
			got := queryOne[int64](t, tgt, "SELECT count(*) FROM "+table)
			if got != want[int32(tid)] {
				t.Errorf("%s on target %d: %d rows, want %d", table, tid, got, want[int32(tid)])
			}
			total += got
		}
		if total != 2000 {
			t.Errorf("%s: %d rows in total", table, total)
		}
	}

	homeTarget := int32(f.tgtRng.Locate(0))
	tgtHome := connect(t, f.appDSN("g2", homeTarget))
	if v := queryOne[int64](t, tgtHome, `SELECT last_value FROM ticket_seq`); v < seq {
		t.Fatalf("ticket_seq on the target at %d, source reached %d", v, seq)
	}
	var next int64
	if err := tgtHome.QueryRow(ctx, `SELECT nextval('ticket_seq')`).Scan(&next); err != nil || next <= seq {
		t.Fatalf("nextval on the target: %d %v (source reached %d)", next, err, seq)
	}
	if _, err := tgtHome.Exec(ctx, `INSERT INTO items (v) VALUES ('post-upgrade')`); err != nil {
		t.Fatalf("serial insert after the handoff: %v", err)
	}
}

// TestUpgradeRollbackOnPostgres switches an upgrade to the pg19 targets,
// writes on the new primary side, then rolls back: reverse replication
// carries the post-switch rows to the pg18 sources and the serving map
// returns to them.
func TestUpgradeRollbackOnPostgres(t *testing.T) {
	parallelPG(t)
	f := newUpgradeFixture(t)
	id := f.startWorkflowKind(KindUpgrade)

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
		t.Fatalf("upgrade did not switch: %s %s %q", state, stage, msg)
	}

	tenant := int64(999_331)
	tid, _ := placement.KeyspaceID(tenant)
	tgt := connect(t, f.appDSN("g2", int32(f.tgtRng.Locate(tid))))
	mustExec(t, tgt, `INSERT INTO orders (tenant_id, note) VALUES ($1, 'written-on-19')`, tenant)

	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET spec = spec || '{"rollback": true}' WHERE id = $1::uuid`, id)
	f.pass()
	lateWritten := false
	if _, stage, _ := f.workflow(id); stage != StageRolledBack {
		mustExec(t, tgt, `INSERT INTO orders (tenant_id, note) VALUES ($1, 'late-write-on-19')`, tenant)
		lateWritten = true
	}
	for {
		f.pass()
		state, stage, msg = f.workflow(id)
		if stage == StageRolledBack || state == StateFailed || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if stage != StageRolledBack || state != StateCancelled {
		t.Fatalf("rollback did not finish: %s %s %q", state, stage, msg)
	}

	if set := queryOne[string](t, f.catalog, `SELECT shard_set FROM pgshard.shard_sets WHERE state = 'serving'`); set != "default" {
		t.Fatalf("serving set %s after rollback", set)
	}
	if n := queryOne[int64](t, f.catalog, `SELECT count(*) FROM pgshard.shard_status WHERE migrating`); n != 0 {
		t.Fatalf("%d shards left fenced", n)
	}
	src := connect(t, f.appDSN("default", int32(f.srcRng.Locate(tid))))
	waitFor(t, 30*time.Second, func() bool {
		return queryOne[int64](t, src, `SELECT count(*) FROM orders WHERE note = 'written-on-19'`) == 1
	}, "post-switch write must flow back to the source")
	if n := queryOne[int64](t, src, `SELECT count(*) FROM orders WHERE note = 'late-write-on-19'`); lateWritten && n != 1 {
		t.Fatalf("write during the rollback catch-up lost: %d", n)
	}
	for sid := range 2 {
		c := connect(t, f.appDSN("default", int32(sid)))
		if n := queryOne[int64](t, c, `SELECT count(*) FROM pg_subscription`); n != 0 {
			t.Errorf("source %d subscriptions left: %d", sid, n)
		}
	}
}

func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// TestCollectSequencesHeadroomOnPostgres: the carried value covers what a
// session cache or the WAL pre-log window may already have handed out
// without moving pg_current_wal_lsn(), applied in the direction of
// increment_by, clamped to the boundary in that direction, and safe at the
// bigint edges.
func TestCollectSequencesHeadroomOnPostgres(t *testing.T) {
	parallelPG(t)
	dsn := startPostgresWith(t)
	conn := connect(t, dsn)
	ctx := context.Background()
	mustExec(t, conn, `CREATE SEQUENCE app_seq CACHE 5`)
	mustExec(t, conn, `CREATE SEQUENCE tiny_seq MAXVALUE 10`)
	mustExec(t, conn, `CREATE SEQUENCE down_seq INCREMENT -3 START -10 MINVALUE -1000000 MAXVALUE -1`)
	mustExec(t, conn, `CREATE SEQUENCE wide_seq INCREMENT 7`)
	mustExec(t, conn, `CREATE SEQUENCE edge_seq START 9223372036854775800`)
	mustExec(t, conn, `CREATE SEQUENCE edge_down_seq INCREMENT -5 START -9223372036854775800 MINVALUE -9223372036854775807 MAXVALUE -1`)
	mustExec(t, conn, `CREATE SEQUENCE cycle_seq MAXVALUE 20 START 19 CYCLE`)
	for _, seq := range []string{"app_seq", "tiny_seq", "down_seq", "wide_seq", "edge_seq", "edge_down_seq", "cycle_seq"} {
		queryOne[int64](t, conn, `SELECT nextval('`+seq+`')`)
	}
	last := queryOne[int64](t, conn, `SELECT last_value FROM pg_sequences WHERE sequencename = 'app_seq'`)
	values := map[string]seqCarry{}
	if err := collectSequences(ctx, pgxShardConn{conn}, values); err != nil {
		t.Fatal(err)
	}
	if got := values["public.app_seq"]; got.Value != last+32 || !got.Ascending {
		t.Fatalf("app_seq carried as %+v, want last_value %d + 32 headroom, ascending", got, last)
	}
	if got := values["public.tiny_seq"]; got.Value != 10 {
		t.Fatalf("tiny_seq carried as %+v, want the clamp at max_value 10", got)
	}
	if got := values["public.down_seq"]; got.Value != -10-32*3 || got.Ascending {
		t.Fatalf("down_seq carried as %+v, want start -10 minus 32*3 headroom, descending", got)
	}
	if got := values["public.wide_seq"]; got.Value != 1+32*7 {
		t.Fatalf("wide_seq carried as %+v, want start 1 plus 32*7 headroom", got)
	}
	if got := values["public.edge_seq"]; got.Value != 9223372036854775807 {
		t.Fatalf("edge_seq carried as %+v, want the clamp at the bigint maximum", got)
	}
	if got := values["public.edge_down_seq"]; got.Value != -9223372036854775807 {
		t.Fatalf("edge_down_seq carried as %+v, want the clamp at min_value", got)
	}
	if got := values["public.cycle_seq"]; got.Value != 20 {
		t.Fatalf("cycle_seq carried as %+v, want the clamp at max_value 20, never a wrapped value", got)
	}

	// The merge across sources keeps the furthest value per direction: a
	// later source further along must win, an earlier one must not regress
	// the carry.
	merged := map[string]seqCarry{
		"public.app_seq":  {Value: last + 1000, Ascending: true},
		"public.down_seq": {Value: -5000, Ascending: false},
	}
	if err := collectSequences(ctx, pgxShardConn{conn}, merged); err != nil {
		t.Fatal(err)
	}
	if got := merged["public.app_seq"]; got.Value != last+1000 {
		t.Fatalf("ascending merge regressed to %+v", got)
	}
	if got := merged["public.down_seq"]; got.Value != -5000 {
		t.Fatalf("descending merge regressed to %+v", got)
	}

	if err := applySequences(ctx, pgxShardConn{conn}, values); err != nil {
		t.Fatal(err)
	}
	if next := queryOne[int64](t, conn, `SELECT nextval('app_seq')`); next <= last+32 {
		t.Fatalf("nextval after the carry: %d, want past %d", next, last+32)
	}
	if next := queryOne[int64](t, conn, `SELECT nextval('down_seq')`); next >= -10-32*3 {
		t.Fatalf("descending nextval after the carry: %d, want below %d", next, -10-32*3)
	}
	// edge_seq was clamped to the bigint maximum: the target must refuse
	// further values rather than wrap into duplicates.
	if _, err := conn.Exec(ctx, `SELECT nextval('edge_seq')`); err == nil {
		t.Fatal("nextval past the clamped bigint maximum must error, not hand out a duplicate")
	}

	// The carry runs a second time at the swap, by which point the targets
	// are serving and may be past the sources on their own. Moving a live
	// sequence backwards would hand every value between out twice.
	mustExec(t, conn, `SELECT setval('app_seq', 900000, true)`)
	mustExec(t, conn, `SELECT setval('down_seq', -900000, true)`)
	if err := applySequences(ctx, pgxShardConn{conn}, values); err != nil {
		t.Fatal(err)
	}
	if next := queryOne[int64](t, conn, `SELECT nextval('app_seq')`); next <= 900000 {
		t.Fatalf("an ascending sequence was moved back to %d; it was already at 900000", next)
	}
	if next := queryOne[int64](t, conn, `SELECT nextval('down_seq')`); next >= -900000 {
		t.Fatalf("a descending sequence was moved back to %d; it was already at -900000", next)
	}
}

// TestUpgradeRollbackRefusesAfterSchemaDrift: logical replication carries no
// DDL, so an ALTER applied after the switch reaches only the set that is
// serving. Rolling back to a source that never received it either fails on
// reverse apply or silently drops the change, and rollback checked only
// LSNs and sequence positions, so it did neither visibly.
func TestUpgradeRollbackRefusesAfterSchemaDrift(t *testing.T) {
	parallelPG(t)
	f := newUpgradeFixture(t)
	id := f.startWorkflowKind(KindUpgrade)

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
		t.Fatalf("upgrade did not switch: %s %s %q", state, stage, msg)
	}

	for sid := range 2 {
		c := connect(t, f.appDSN("g2", int32(sid)))
		mustExec(t, c, `ALTER TABLE orders ADD COLUMN priority integer NOT NULL DEFAULT 0`)
	}

	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET spec = spec || '{"rollback": true}' WHERE id = $1::uuid`, id)
	for range 8 {
		f.pass()
		state, stage, msg = f.workflow(id)
		if stage == StageRolledBack {
			t.Fatalf("rollback completed onto a source that never received the ALTER: %s %s", state, stage)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !strings.Contains(msg, "schema changed since the switch") {
		t.Fatalf("rollback did not report the drift: %s %s %q", state, stage, msg)
	}
	if set := queryOne[string](t, f.catalog, `SELECT shard_set FROM pgshard.shard_sets WHERE state = 'serving'`); set != "g2" {
		t.Fatalf("serving set moved to %s despite the refusal", set)
	}
}

// TestRollbackWaitsForAWriterThatStartedBeforeTheFence: the fence stops
// routers that have seen it, and default_transaction_read_only is read when
// a transaction starts, so neither ends a write that was already open. The
// rollback checked positions and flipped, and Complete then dropped the
// reverse replication -- so a transaction that committed in that window was
// acknowledged on the set being rolled away from and its row went with the
// replication.
func TestRollbackWaitsForAWriterThatStartedBeforeTheFence(t *testing.T) {
	parallelPG(t)
	f := newUpgradeFixture(t)
	id := f.startWorkflowKind(KindUpgrade)

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
		t.Fatalf("upgrade did not switch: %s %s %q", state, stage, msg)
	}

	tenant := int64(999_337)
	tid, _ := placement.KeyspaceID(tenant)
	ctx := context.Background()
	tgt := connect(t, f.appDSN("g2", int32(f.tgtRng.Locate(tid))))
	tx, err := tgt.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO orders (tenant_id, note) VALUES ($1, 'in-flight-on-19')`, tenant); err != nil {
		t.Fatal(err)
	}

	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET spec = spec || '{"rollback": true}' WHERE id = $1::uuid`, id)
	// Commit while the rollback is running. The drain has to be what lets
	// it through, not luck.
	committed := make(chan error, 1)
	go func() {
		time.Sleep(2 * time.Second)
		committed <- tx.Commit(ctx)
	}()
	for {
		f.pass()
		state, stage, msg = f.workflow(id)
		if stage == StageRolledBack || state == StateFailed || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err := <-committed; err != nil {
		t.Fatalf("the in-flight write could not commit: %v", err)
	}
	if stage != StageRolledBack || state != StateCancelled {
		t.Fatalf("rollback did not finish: %s %s %q", state, stage, msg)
	}
	src := connect(t, f.appDSN("default", int32(f.srcRng.Locate(tid))))
	waitFor(t, 30*time.Second, func() bool {
		return queryOne[int64](t, src, `SELECT count(*) FROM orders WHERE note = 'in-flight-on-19'`) == 1
	}, "a write that was in flight when the rollback began must reach the source")
}
