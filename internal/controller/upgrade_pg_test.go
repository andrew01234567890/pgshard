package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/placement"

	"github.com/andrew01234567890/pgshard/internal/pgsequence"
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
	// A rolled-back run completes with the roles the other way round: the
	// set it calls its source is serving again. Making that one read-only
	// because it is the workflow's source stops the next upgrade before it
	// can create a publication on it.
	if _, err := src.Exec(context.Background(), `INSERT INTO orders (tenant_id, note) VALUES ($1, 'after-rollback')`, tenant); err != nil {
		t.Fatalf("the set serving after a rollback must still take writes: %v", err)
	}
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

// TestSequenceCarryHeadroomOnPostgres: the carried value covers what a
// session cache or the WAL pre-log window may already have handed out
// without moving pg_current_wal_lsn(), applied in the direction of
// increment_by, clamped to the boundary in that direction, and safe at the
// bigint edges.
func TestSequenceCarryHeadroomOnPostgres(t *testing.T) {
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
	// A shard set carries the user databases' sequences. pgshard's own
	// schema and the journal are the control plane's, materialized on the
	// targets by the workflow itself, and carrying them would overwrite
	// what the workflow just wrote.
	mustExec(t, conn, `CREATE SCHEMA pgshard`)
	mustExec(t, conn, `CREATE SCHEMA `+JournalSchema)
	mustExec(t, conn, `CREATE SEQUENCE pgshard.ours`)
	mustExec(t, conn, `CREATE SEQUENCE `+JournalSchema+`.ours`)
	for _, seq := range []string{"pgshard.ours", JournalSchema + ".ours"} {
		queryOne[int64](t, conn, `SELECT nextval('`+seq+`')`)
	}

	values, err := pgsequence.Snapshot(ctx, conn, []string{"pgshard", JournalSchema})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"pgshard.ours", JournalSchema + ".ours"} {
		if got, ok := values[name]; ok {
			t.Errorf("%s is the control plane's, not the workload's, and was carried as %+v", name, got)
		}
	}
	if got := values["public.app_seq"]; got.At != last+32 || !got.Ascending {
		t.Fatalf("app_seq carried as %+v, want last_value %d + 32 headroom, ascending", got, last)
	}
	if got := values["public.tiny_seq"]; got.At != 10 {
		t.Fatalf("tiny_seq carried as %+v, want the clamp at max_value 10", got)
	}
	if got := values["public.down_seq"]; got.At != -10-32*3 || got.Ascending {
		t.Fatalf("down_seq carried as %+v, want start -10 minus 32*3 headroom, descending", got)
	}
	if got := values["public.wide_seq"]; got.At != 1+32*7 {
		t.Fatalf("wide_seq carried as %+v, want start 1 plus 32*7 headroom", got)
	}
	if got := values["public.edge_seq"]; got.At != 9223372036854775807 {
		t.Fatalf("edge_seq carried as %+v, want the clamp at the bigint maximum", got)
	}
	if got := values["public.edge_down_seq"]; got.At != -9223372036854775807 {
		t.Fatalf("edge_down_seq carried as %+v, want the clamp at min_value", got)
	}
	if got := values["public.cycle_seq"]; got.At != 20 {
		t.Fatalf("cycle_seq carried as %+v, want the clamp at max_value 20, never a wrapped value", got)
	}

	// The merge across sources keeps the furthest value per direction: a
	// later source further along must win, an earlier one must not regress
	// the carry.
	merged := map[string]pgsequence.Value{
		"public.app_seq":  {At: last + 1000, Ascending: true},
		"public.down_seq": {At: -5000, Ascending: false},
	}
	pgsequence.Merge(merged, values)
	if got := merged["public.app_seq"]; got.At != last+1000 {
		t.Fatalf("ascending merge regressed to %+v", got)
	}
	if got := merged["public.down_seq"]; got.At != -5000 {
		t.Fatalf("descending merge regressed to %+v", got)
	}

	if err := pgsequence.Apply(ctx, conn, values); err != nil {
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
	if err := pgsequence.Apply(ctx, conn, values); err != nil {
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

// TestARollbackLeavesTheTargetsPausedForComplete drives pgCutover.Rollback
// on its own, which a pass never does -- Copier.rollback calls Complete
// straight after it -- because the window between the two is the defect.
// A router still holding the snapshot from before the flip back can commit
// on a target there, and Complete drops the reverse subscription that
// would carry that row to the source. The pause used to come off the
// moment Rollback returned.
func TestARollbackLeavesTheTargetsPausedForComplete(t *testing.T) {
	parallelPG(t)
	f := newUpgradeFixture(t)
	id := f.startWorkflowKind(KindUpgrade)
	ctx := context.Background()

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
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET spec = spec || '{"rollback": true}' WHERE id = $1::uuid`, id)
	f.pass()
	if _, stage, _ = f.workflow(id); stage != StageRollingBack {
		t.Fatalf("stage %s, want %s", stage, StageRollingBack)
	}

	wfs, err := f.copier.listCopyWorkflows(ctx)
	if err != nil {
		t.Fatal(err)
	}
	idx := slices.IndexFunc(wfs, func(w copyWorkflow) bool { return w.id == id })
	if idx < 0 {
		t.Fatalf("workflow %s not listed", id)
	}
	wf := &wfs[idx]
	var held bool
	if wf.owner, held, err = claimWorkflow(ctx, f.pool, f.copier.Replica, wf.id, f.copier.OwnerLease); err != nil || !held {
		t.Fatalf("claim: held=%v err=%v", held, err)
	}
	wf.fence = wf.state
	ops, err := f.copier.pgCutover(ctx, wf)
	if err != nil {
		t.Fatal(err)
	}
	for {
		err = ops.Rollback(ctx)
		if err == nil || !errors.Is(err, errRetry) || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}

	tenant := int64(999_725)
	tid, _ := placement.KeyspaceID(tenant)
	tgtDSN := f.appDSN("g2", int32(f.tgtRng.Locate(tid)))
	write := func() error {
		c, err := pgx.Connect(ctx, tgtDSN)
		if err != nil {
			return err
		}
		defer func() { _ = c.Close(ctx) }()
		_, err = c.Exec(ctx, `INSERT INTO orders (tenant_id, note) VALUES ($1, 'stale-router-on-19')`, tenant)
		return err
	}
	// Held for a while rather than sampled once: the old code lifted the
	// pause on return, and a single write straight afterwards can land
	// before that reload reaches a new backend and pass for the wrong
	// reason.
	for until := time.Now().Add(3 * time.Second); time.Now().Before(until); time.Sleep(200 * time.Millisecond) {
		var pgErr *pgconn.PgError
		if err := write(); !errors.As(err, &pgErr) || pgErr.Code != "25006" {
			t.Fatalf("a target took a write between Rollback and Complete (err %v); Complete is about to drop the replication that would carry it back", err)
		}
	}

	// A resume after the flip back: the controller's flip-back commit landed
	// and its acknowledgement did not, so the deferred unpause ran and the
	// next pass takes Rollback's already-serving path. That path has to put
	// the pause back, or Complete runs with the targets writable.
	if err := ops.pauseSetClaimed(ctx, ops.wf.set, ops.wf.ids, false); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 30*time.Second, func() bool { return write() == nil }, "the simulated lost acknowledgement did not leave the targets writable")
	if err := ops.Rollback(ctx); err != nil {
		t.Fatalf("resumed rollback: %v", err)
	}
	{
		var pgErr *pgconn.PgError
		if err := write(); !errors.As(err, &pgErr) || pgErr.Code != "25006" {
			t.Fatalf("a resumed rollback left the targets writable going into Complete (err %v)", err)
		}
	}

	// The pause is this run's, claimed, going into Complete -- so the claim
	// being gone afterwards shows Complete abandoned it, not that there never
	// was one.
	if n := queryOne[int64](t, f.catalog, `SELECT count(*) FROM pgshard.shard_status WHERE shard_set = 'g2' AND write_paused_by = $1::uuid`, id); n == 0 {
		t.Fatal("the targets carry no pause claim from this run before Complete, so the check after it proves nothing")
	}
	if err := ops.Complete(ctx); err != nil {
		t.Fatal(err)
	}
	// And still refused afterwards. The targets are the retired set now,
	// and Complete converts this run's claimed pause into the permanent,
	// unclaimed one a retired set keeps until it is torn down. The first
	// version of this fix lifted the pause here, which left the retired set
	// writable for good -- and a stale router could then commit on it with
	// no replication left to carry the row anywhere.
	var pgErr *pgconn.PgError
	if err := write(); !errors.As(err, &pgErr) || pgErr.Code != "25006" {
		t.Fatalf("a retired target took a write after Complete (err %v)", err)
	}
	if n := queryOne[int64](t, f.catalog, `SELECT count(*) FROM pgshard.shard_status WHERE shard_set = 'g2' AND write_paused_by IS NOT NULL`); n != 0 {
		t.Fatalf("%d target shards still carry this run's pause claim; the sweep would lift the retirement pause behind it", n)
	}
}

// TestCleanupWritesThroughARetiredSourcesPause: a retired set carries the
// permanent, unclaimed write pause Complete's tail puts on it. Cleaning up a
// switch whose sources are in that state -- abandonSwitch, when another
// workflow retired them, or Complete run again on a pass after its own tail
// -- deletes journal rows and drops reverse subscriptions and publications
// on those sources, and the pause refused every one of them with 25006. The
// workflow was retried for ever and never failed, holding its slots
// (PGS-816).
func TestCleanupWritesThroughARetiredSourcesPause(t *testing.T) {
	parallelPG(t)
	f := newUpgradeFixture(t)
	id := f.startWorkflowKind(KindUpgrade)
	ctx := context.Background()

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
	wfs, err := f.copier.listCopyWorkflows(ctx)
	if err != nil {
		t.Fatal(err)
	}
	idx := slices.IndexFunc(wfs, func(w copyWorkflow) bool { return w.id == id })
	if idx < 0 {
		t.Fatalf("workflow %s not listed", id)
	}
	wf := &wfs[idx]
	var held bool
	if wf.owner, held, err = claimWorkflow(ctx, f.pool, f.copier.Replica, wf.id, f.copier.OwnerLease); err != nil || !held {
		t.Fatalf("claim: held=%v err=%v", held, err)
	}
	wf.fence = wf.state
	ops, err := f.copier.pgCutover(ctx, wf)
	if err != nil {
		t.Fatal(err)
	}
	journal := wf.cutover.JournalID
	if journal == "" {
		t.Fatal("a switched workflow has no journal id")
	}
	onSources := func(sql string, args ...any) int64 {
		t.Helper()
		var total int64
		for _, s := range ops.srcIDs {
			conn := connect(t, f.appDSN(ops.srcSet, s))
			total += queryOne[int64](t, conn, sql, args...)
		}
		return total
	}
	journalRows := fmt.Sprintf(`SELECT count(*) FROM %s.%s WHERE id = $1::uuid`, JournalSchema, JournalTable)
	reverse := func() int64 {
		var n int64
		for _, s := range ops.srcIDs {
			conn := connect(t, f.appDSN(ops.srcSet, s))
			n += queryOne[int64](t, conn, `SELECT count(*) FROM pg_subscription WHERE subname LIKE $1`, ops.reversePattern(s))
		}
		return n
	}
	if onSources(journalRows, journal) == 0 || reverse() == 0 {
		t.Fatal("the sources hold no journal rows or reverse subscriptions to clean up, so this proves nothing")
	}

	// The retirement pause: permanent and unclaimed, as Complete leaves it.
	if err := ops.pauseSet(ctx, ops.srcSet, ops.srcIDs, true); err != nil {
		t.Fatal(err)
	}
	if err := ops.DropJournal(ctx, journal); err != nil {
		t.Fatalf("dropping the journal on read-only retired sources: %v", err)
	}
	if err := ops.Complete(ctx); err != nil {
		t.Fatalf("completing with read-only retired sources: %v", err)
	}
	if n := onSources(journalRows, journal); n != 0 {
		t.Fatalf("%d journal rows left on the sources", n)
	}
	if n := reverse(); n != 0 {
		t.Fatalf("%d reverse subscriptions left on the sources", n)
	}
}

// TestCompleteLeavesARetiredSetWritableWhileAnotherWorkflowReplicatesIntoIt
// (PGS-837): a switch abandoned because another workflow retired its sources
// completes with that workflow's retired set as its own source set, and
// Complete's tail paused it. The other workflow keeps reverse subscriptions
// into the set for its rollback window; paused, their apply fails with 25006
// and its rollback never finishes. A subscription of another generation on
// the set stands in for that workflow here.
func TestCompleteLeavesARetiredSetWritableWhileAnotherWorkflowReplicatesIntoIt(t *testing.T) {
	parallelPG(t)
	f := newUpgradeFixture(t)
	id := f.startWorkflowKind(KindUpgrade)
	ctx := context.Background()

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
	wfs, err := f.copier.listCopyWorkflows(ctx)
	if err != nil {
		t.Fatal(err)
	}
	idx := slices.IndexFunc(wfs, func(w copyWorkflow) bool { return w.id == id })
	if idx < 0 {
		t.Fatalf("workflow %s not listed", id)
	}
	wf := &wfs[idx]
	var held bool
	if wf.owner, held, err = claimWorkflow(ctx, f.pool, f.copier.Replica, wf.id, f.copier.OwnerLease); err != nil || !held {
		t.Fatalf("claim: held=%v err=%v", held, err)
	}
	wf.fence = wf.state
	ops, err := f.copier.pgCutover(ctx, wf)
	if err != nil {
		t.Fatal(err)
	}
	isPaused := func(s int32) bool {
		t.Helper()
		return queryOne[string](t, connect(t, f.appDSN(ops.srcSet, s)), `SHOW default_transaction_read_only`) == "on"
	}
	paused := func() int {
		t.Helper()
		n := 0
		for _, s := range ops.srcIDs {
			if isPaused(s) {
				n++
			}
		}
		return n
	}

	// The other workflow has to exist for its subscriptions to mean
	// anything. A generation whose workflow has finished left litter, not a
	// claim on the set, and litter must not hold the retirement pause open
	// for ever (PGS-846) -- so the stand-in gets a live workflow of its own.
	otherGen := wf.gen + 7
	mustExec(t, f.catalog, `INSERT INTO pgshard.shard_sets (shard_set, generation, state) VALUES ('other', $1, 'retired')`, otherGen)
	mustExec(t, f.catalog, `INSERT INTO pgshard.workflows (id, kind, state, spec)
		VALUES (gen_random_uuid(), $1, $2, '{"shard_set": "other"}'::jsonb)`, KindReshard, StateRunning)

	other := fmt.Sprintf("pgshard_reshard_g%d_rev_s0_t0", otherGen)
	home := connect(t, f.appDSN(ops.srcSet, ops.srcIDs[0]))
	mustExec(t, home, `CREATE SUBSCRIPTION `+other+` CONNECTION 'host=elsewhere dbname=app' PUBLICATION elsewhere WITH (connect = false, slot_name = NONE)`)
	if err := ops.Complete(ctx); err != nil {
		t.Fatal(err)
	}
	// The primary that carries the other workflow's way back stays writable;
	// its rollback applies there and a pause would fail it with 25006. The
	// decision is per primary (PGS-846), so a primary with nothing on it is
	// still retired properly -- one unreachable or occupied primary used to
	// leave the whole set writable.
	if isPaused(ops.srcIDs[0]) {
		t.Fatal("the primary another workflow still replicates into was paused; its rollback now fails with 25006")
	}
	if len(ops.srcIDs) < 2 {
		t.Fatalf("this test needs a set of at least two primaries to tell per-primary from all-or-nothing; got %d", len(ops.srcIDs))
	}
	if !isPaused(ops.srcIDs[1]) {
		t.Fatal("a primary nothing replicates into was left writable because ANOTHER primary was occupied; it accepts writes nothing reads again")
	}
	if err := ops.pauseSet(ctx, ops.srcSet, ops.srcIDs, false); err != nil {
		t.Fatal(err)
	}

	// The same subscription, once that workflow has finished, is litter: it
	// no longer holds the set writable. This tail runs once, so a workflow
	// that ended without tearing its reverse subscriptions down used to cost
	// the retirement pause permanently.
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET state = $1 WHERE spec->>'shard_set' = 'other'`, StateFailed)
	if err := ops.Complete(ctx); err != nil {
		t.Fatal(err)
	}
	if n := paused(); n != len(ops.srcIDs) {
		t.Fatalf("%d of %d sources paused; a finished workflow's leftover subscription still held the set writable", n, len(ops.srcIDs))
	}
	if err := ops.pauseSet(ctx, ops.srcSet, ops.srcIDs, false); err != nil {
		t.Fatal(err)
	}

	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET state = $1 WHERE spec->>'shard_set' = 'other'`, StateRunning)
	mustExec(t, home, `DROP SUBSCRIPTION `+other)
	// A forward subscription of another generation is not a way back into
	// the set, and must not keep it writable.
	forward := fmt.Sprintf("pgshard_reshard_g%d_t0_s0", wf.gen+7)
	mustExec(t, home, `CREATE SUBSCRIPTION `+forward+` CONNECTION 'host=elsewhere dbname=app' PUBLICATION elsewhere WITH (connect = false, slot_name = NONE)`)
	t.Cleanup(func() { _, _ = home.Exec(context.Background(), `DROP SUBSCRIPTION IF EXISTS `+forward) })
	if err := ops.Complete(ctx); err != nil {
		t.Fatal(err)
	}
	if n := paused(); n != len(ops.srcIDs) {
		t.Fatalf("%d of %d sources paused once nothing else replicates into the retired set", n, len(ops.srcIDs))
	}
}
