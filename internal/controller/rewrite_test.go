package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
)

func rewriteMigration(id string) catalog.DDLMigration {
	return catalog.DDLMigration{ID: id, Database: "app", Statement: "alter table orders alter column amount type bigint",
		Kind: "ALTER TABLE", Strategy: catalog.StrategyRewrite, Scope: "all", State: catalog.MigrationQueued,
		Meta: catalog.MigrationMeta{Rewrite: &catalog.RewriteChange{Schema: "public", Table: "orders", Column: "amount",
			NewType: "bigint", Using: `CAST("amount" AS bigint)`, BatchSize: 2}}}
}

func newRewriteApplier(store *memStore, shards *fakeShards) *Applier {
	return &Applier{Store: store, Shards: shards, RewriteSettle: -1,
		Sleep:   func(context.Context, time.Duration) error { return nil },
		Backoff: Backoff{Min: time.Millisecond, Max: time.Millisecond, Total: 5 * time.Millisecond}}
}

func has(list []string, substr string) bool {
	for _, s := range list {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}

func TestRewriteMigrationRunsAllPhases(t *testing.T) {
	store := &memStore{migrations: []catalog.DDLMigration{rewriteMigration("00000000-0000-0000-0000-00000000ab01")},
		shards: []int32{0, 1}}
	shards := newFakeShards()
	shards.columns = []string{"tenant_id", "id", "amount"}
	shards.pks = []string{"id"}
	shards.oldNotNull = true
	shards.nnPending = true
	shards.batchNext = func(_ int32, call int) []string {
		if call < 2 {
			return []string{fmt.Sprint(3 + call)}
		}
		return nil
	}
	a := newRewriteApplier(store, shards)
	if _, err := a.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	m := store.get(t, "00000000-0000-0000-0000-00000000ab01")
	if m.State != catalog.MigrationComplete {
		t.Fatalf("state = %s error %q", m.State, m.Error)
	}
	if len(m.Meta.Rewrite.Columns) != 3 {
		t.Fatalf("columns not recorded: %+v", m.Meta.Rewrite.Columns)
	}
	hidden := m.Meta.Rewrite.HiddenColumn(m.ID)
	if hidden != "_pgshard_amount_00000000" {
		t.Fatalf("hidden column %q", hidden)
	}
	for _, id := range []int32{0, 1} {
		ran := shards.statements(id)
		for _, want := range []string{
			`ADD COLUMN IF NOT EXISTS "` + hidden + `" bigint`,
			"CREATE OR REPLACE FUNCTION", "CREATE TRIGGER",
			"WITH batch AS",
			`DROP COLUMN "amount"`, `RENAME COLUMN "` + hidden + `" TO "amount"`,
			"NOT NULL \"amount\" NOT VALID", "VALIDATE CONSTRAINT",
			"DROP FUNCTION IF EXISTS",
		} {
			if !has(ran, want) {
				t.Fatalf("shard %d never ran %q:\n%s", id, want, strings.Join(ran, "\n"))
			}
		}
		// Each batch starts at the key the probe before it found, so
		// neither statement rescans the rows the pass already converted.
		if got := fmt.Sprint(shards.batchCursor[id]); got != "[<start> 3 4]" {
			t.Fatalf("shard %d backfilled from %s, want each batch to start at the last key found", id, got)
		}
		if got := shards.batchCalls[id]; got != 3 {
			t.Fatalf("shard %d ran %d backfill passes, want 3: two that converted rows and one that found none left", id, got)
		}
		if got := m.PerShard[shardKey(id)]; got.State != catalog.ShardApplied {
			t.Fatalf("shard %d state %+v", id, got)
		}
	}
}

func TestRewriteAddFormAppliesDefault(t *testing.T) {
	m := rewriteMigration("00000000-0000-0000-0000-00000000ab02")
	m.Meta.Rewrite = &catalog.RewriteChange{Schema: "public", Table: "orders", Column: "token",
		NewType: "uuid", Default: "gen_random_uuid()", Add: true}
	store := &memStore{migrations: []catalog.DDLMigration{m}, shards: []int32{0}}
	shards := newFakeShards()
	shards.columns = []string{"tenant_id", "id"}
	a := newRewriteApplier(store, shards)
	if _, err := a.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := store.get(t, m.ID)
	if got.State != catalog.MigrationComplete {
		t.Fatalf("state = %s error %q", got.State, got.Error)
	}
	ran := shards.statements(0)
	if !has(ran, "BEFORE INSERT ON") || has(ran, "INSERT OR UPDATE") {
		t.Fatalf("add-form trigger must fire on INSERT only:\n%s", strings.Join(ran, "\n"))
	}
	if !has(ran, `SET DEFAULT (gen_random_uuid())`) || has(ran, "DROP COLUMN \"token\"") {
		t.Fatalf("cutover:\n%s", strings.Join(ran, "\n"))
	}
}

func TestRewriteFailureRevertsEveryShard(t *testing.T) {
	store := &memStore{migrations: []catalog.DDLMigration{rewriteMigration("00000000-0000-0000-0000-00000000ab03")},
		shards: []int32{0, 1}}
	shards := newFakeShards()
	shards.columns = []string{"tenant_id", "id", "amount"}
	shards.pks = []string{"id"}
	shards.exec = func(id int32, sql string) error {
		if id == 1 && strings.Contains(sql, "WITH batch AS") {
			return pgErr("22P02", "invalid input syntax")
		}
		return nil
	}
	a := newRewriteApplier(store, shards)
	if _, err := a.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	m := store.get(t, "00000000-0000-0000-0000-00000000ab03")
	if m.State != catalog.MigrationFailed || !strings.Contains(m.Error, "invalid input syntax") {
		t.Fatalf("state = %s error %q", m.State, m.Error)
	}
	hidden := m.Meta.Rewrite.HiddenColumn(m.ID)
	for _, id := range []int32{0, 1} {
		sup := shards.superuserStatements(id)
		if !has(sup, `DROP COLUMN IF EXISTS "`+hidden+`"`) || !has(sup, "DROP TRIGGER IF EXISTS") {
			t.Fatalf("shard %d not reverted:\n%s", id, strings.Join(sup, "\n"))
		}
		if has(shards.statements(id), "RENAME COLUMN") {
			t.Fatalf("shard %d cut over despite the failure", id)
		}
	}
}

func TestRewriteCutoverIsIdempotent(t *testing.T) {
	store := &memStore{migrations: []catalog.DDLMigration{rewriteMigration("00000000-0000-0000-0000-00000000ab04")},
		shards: []int32{0}}
	shards := newFakeShards()
	shards.columns = []string{"tenant_id", "id", "amount"}
	shards.hiddenExists = func(int32) bool { return false }
	a := newRewriteApplier(store, shards)
	if _, err := a.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	m := store.get(t, "00000000-0000-0000-0000-00000000ab04")
	if m.State != catalog.MigrationComplete {
		t.Fatalf("state = %s error %q", m.State, m.Error)
	}
	if has(shards.statements(0), "RENAME COLUMN") {
		t.Fatal("cutover ran again although the hidden column is gone")
	}
}

func TestRepackStepPicksRepackOn19(t *testing.T) {
	m := catalog.DDLMigration{ID: "00000000-0000-0000-0000-00000000ab05", Database: "app",
		Statement: "vacuum (full) orders", Kind: "VACUUM", Strategy: catalog.StrategyRepack, Scope: "all",
		State: catalog.MigrationQueued, Meta: catalog.MigrationMeta{Repack: true,
			Object: catalog.MigrationObject{Kind: "relation", Schema: "public", Name: "orders", Expect: "present"}}}
	for version, want := range map[int]string{190001: `REPACK (CONCURRENTLY) "public"."orders"`, 180004: "vacuum (full) orders"} {
		store := &memStore{migrations: []catalog.DDLMigration{m}, shards: []int32{0}}
		shards := newFakeShards()
		shards.version = version
		a := newRewriteApplier(store, shards)
		if _, err := a.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		got := store.get(t, m.ID)
		if got.State != catalog.MigrationComplete {
			t.Fatalf("v%d: state = %s error %q", version, got.State, got.Error)
		}
		if !has(shards.statements(0), want) {
			t.Fatalf("v%d: never ran %q:\n%s", version, want, strings.Join(shards.statements(0), "\n"))
		}
	}
}

func TestSweepDropsRewriteArtifacts(t *testing.T) {
	store := &memStore{shards: []int32{0}, dbs: []string{"app"}}
	shards := newFakeShards()
	shards.sweepDrops = []string{`DROP TRIGGER IF EXISTS "_pgshard_rw_deadbeef" ON "public"."orders"`,
		`ALTER TABLE "public"."orders" DROP COLUMN IF EXISTS "_pgshard_amount_deadbeef"`}
	a := newRewriteApplier(store, shards)
	if _, err := a.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	sup := shards.superuserStatements(0)
	for _, want := range shards.sweepDrops {
		if !has(sup, want) {
			t.Fatalf("sweep never ran %q:\n%s", want, strings.Join(sup, "\n"))
		}
	}
}

func TestRewriteTriggerSQLShapes(t *testing.T) {
	rw := &catalog.RewriteChange{Schema: "public", Table: "orders", Column: "amount", NewType: "bigint",
		Using: `CAST("amount" AS bigint)`}
	stmts := rewriteTriggerSQL(rw, "00000000-0000-0000-0000-00000000ab06")
	joined := strings.Join(stmts, "\n")
	if !strings.Contains(joined, `NEW."_pgshard_amount_00000000" := (SELECT (CAST("amount" AS bigint)) FROM (SELECT (NEW).*) AS pgshard_row)`) {
		t.Fatalf("type-form trigger body:\n%s", joined)
	}
	if !strings.Contains(joined, "BEFORE INSERT OR UPDATE ON") {
		t.Fatalf("type-form events:\n%s", joined)
	}
	if got := backfillPredicate(rw, "_pgshard_amount_00000000"); got != `"_pgshard_amount_00000000" IS DISTINCT FROM (CAST("amount" AS bigint))` {
		t.Fatalf("predicate %q", got)
	}
	add := &catalog.RewriteChange{Table: "orders", Column: "token", NewType: "uuid", Default: "gen_random_uuid()", Add: true}
	if got := backfillPredicate(add, "_pgshard_token_00000000"); got != `"_pgshard_token_00000000" IS NULL` {
		t.Fatalf("add predicate %q", got)
	}
}

// TestRewriteSettleCoversTheSnapshotFallbackReload pins the bound the whole
// online-rewrite safety argument rests on.
//
// Covering the fallback reload is not enough on its own: a router that
// misses one reload still serves its old view until MaxAge, and only past
// MaxAge does it stop. So the settle has to cover MaxAge, not the reload
// interval -- at exactly the reload interval a router whose snapshot is
// older than that and younger than MaxAge is still answering, and its
// SELECT * would leak the working column the applier is about to add.
//
// The two constants being the same quantity is what turns the wait from a
// hope into a guarantee, and nothing else in the tree would fail if someone
// shortened the settle to the reload interval, which is why this asserts
// the bound that matters rather than the weaker one it used to.
func TestRewriteSettleCoversTheSnapshotFallbackReload(t *testing.T) {
	if DefaultRewriteSettle < snapshot.MaxAge {
		t.Fatalf("settle %s is shorter than the snapshot bound %s: past that a router has either reloaded or stopped, and short of it one can still be serving a view from before the column list was published",
			DefaultRewriteSettle, snapshot.MaxAge)
	}
	if snapshot.MaxAge <= snapshot.DefaultReloadInterval {
		t.Fatalf("MaxAge %s leaves no margin over the fallback reload %s: a healthy router would trip the bound it is meant never to reach",
			snapshot.MaxAge, snapshot.DefaultReloadInterval)
	}
}

func TestBackfillStopsOnlyWhenNoRowsMatchThePredicate(t *testing.T) {
	store := &memStore{migrations: []catalog.DDLMigration{rewriteMigration("00000000-0000-0000-0000-00000000ab0a")},
		shards: []int32{0}}
	shards := newFakeShards()
	shards.columns = []string{"tenant_id", "id", "amount"}
	shards.pks = []string{"id"}
	shards.oldNotNull = true
	shards.nnPending = true
	shards.batchNext = func(_ int32, call int) []string {
		switch call {
		case 0:
			// An empty-string text PK is a key like any other and must
			// not read as "no rows remain".
			return []string{""}
		case 1:
			return []string{"a"}
		}
		return nil
	}
	a := newRewriteApplier(store, shards)
	if _, err := a.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	m := store.get(t, "00000000-0000-0000-0000-00000000ab0a")
	if m.State != catalog.MigrationComplete {
		t.Fatalf("state = %s error %q", m.State, m.Error)
	}
	if got := shards.batchCalls[0]; got != 3 {
		t.Fatalf("ran %d backfill passes, want 3: a pass that returned a key with rows remaining was declared done", got)
	}
}

func TestBackfillFailsWhenItDoesNotConverge(t *testing.T) {
	store := &memStore{migrations: []catalog.DDLMigration{rewriteMigration("00000000-0000-0000-0000-00000000ab09")},
		shards: []int32{0}}
	shards := newFakeShards()
	shards.columns = []string{"tenant_id", "id", "amount"}
	shards.pks = []string{"id"}
	shards.batchNext = func(int32, int) []string { return []string{"42"} }
	a := newRewriteApplier(store, shards)
	if _, err := a.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	m := store.get(t, "00000000-0000-0000-0000-00000000ab09")
	if m.State != catalog.MigrationFailed || !strings.Contains(m.Error, "not converging") {
		t.Fatalf("state %s error %q", m.State, m.Error)
	}
}

// TestAResumedRewriteWaitsOutTheSettleWindowAgain: the column list alone
// does not say the wait for routers to reload it ran. A crash inside the
// wait used to resume straight into ADD COLUMN, so a router still serving
// the view from before the column list would expand SELECT * over the
// hidden column.
func TestAResumedRewriteWaitsOutTheSettleWindowAgain(t *testing.T) {
	waited := func(settled bool) (time.Duration, bool, bool, []string) {
		m := rewriteMigration("00000000-0000-0000-0000-00000000ab0d")
		m.State = catalog.MigrationRunning
		m.Meta.Rewrite.Columns = []string{"tenant_id", "id", "amount"}
		m.Meta.Rewrite.Settled = settled
		m.PerShard = map[string]catalog.ShardMigration{"0": {State: catalog.ShardPending}}
		store := &memStore{migrations: []catalog.DDLMigration{m}, shards: []int32{0}}
		shards := newFakeShards()
		shards.columns = []string{"tenant_id", "id", "amount"}
		shards.pks = []string{"id"}
		a := newRewriteApplier(store, shards)
		a.RewriteSettle = DefaultRewriteSettle
		var slept time.Duration
		var ranWhenSlept []string
		a.Sleep = func(_ context.Context, d time.Duration) error {
			slept, ranWhenSlept = slept+d, shards.statements(0)
			return nil
		}
		if _, err := a.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		got := store.get(t, m.ID)
		if got.State != catalog.MigrationComplete {
			t.Fatalf("state %s: %s", got.State, got.Error)
		}
		return slept, has(ranWhenSlept, "ADD COLUMN"), got.Meta.Rewrite.Settled, shards.statements(0)
	}

	slept, addedFirst, recorded, ran := waited(false)
	if slept < DefaultRewriteSettle {
		t.Fatalf("a resumed rewrite waited %s, want at least one settle window of %s:\n%s", slept, DefaultRewriteSettle, strings.Join(ran, "\n"))
	}
	if addedFirst {
		t.Fatal("the hidden column was added before the wait for routers to reload the column list")
	}
	if !recorded {
		t.Fatal("the finished wait was not recorded, so every later resume waits again")
	}
	if slept, _, _, _ := waited(true); slept != 0 {
		t.Fatalf("a rewrite that recorded a finished wait waited %s again", slept)
	}
}
