package controller

import (
	"errors"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestAMigrationHeldThroughAPlacementIsNotAppliedUnderTheOldOne (PGS-971).
//
// The operation queue holds DDL on a database behind a table placement in it
// until the placement has SWAPPED. So a migration released from the queue has
// been waiting for exactly the event that changes the placement its scope was
// decided from: public.orders was unsharded when ALTER TABLE ... ADD COLUMN
// was planned, so it was queued with scope home -- and by the time it ran,
// orders was on every shard and the column went onto the home shard alone.
func TestAMigrationHeldThroughAPlacementIsNotAppliedUnderTheOldOne(t *testing.T) {
	ctx, pool, exec := holdCatalog(t)
	exec(`INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('default', 0, int8range(NULL, 0)), ('default', 1, int8range(0, NULL))`)
	reconcile(t, connect(t, pool.Config().ConnString()))
	exec(`INSERT INTO pgshard.table_status (database, schema_name, table_name, effective_placement) VALUES ('app', 'public', 'orders', 'unsharded'), ('app', 'public', 'items', 'unsharded')`)
	const move = "00000000-0000-0000-0000-0000000009c1"
	exec(`INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES ($1, 'table_placement', 'running', '{"database": "app", "schema_name": "public", "table_name": "orders"}', '{"stage": "copying"}')`, move)

	enqueue := func(stmt, table string) string {
		t.Helper()
		id, err := catalog.EnqueueMigration(ctx, pool, catalog.DDLMigration{Database: "app", Statement: stmt,
			Kind: "ALTER TABLE", Strategy: "direct", Scope: "home", HomeShard: 0,
			Meta: catalog.MigrationMeta{Placements: []catalog.TablePlacement{{Schema: "public", Table: table, Placement: "unsharded"}}}})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	moved := enqueue("ALTER TABLE public.orders ADD COLUMN note text", "orders")
	stayed := enqueue("ALTER TABLE public.items ADD COLUMN note text", "items")

	shards := newFakeShards()
	a := &Applier{Store: &PGMigrationStore{Pool: pool}, Shards: shards, RewriteSettle: -1}
	if _, err := a.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if m, err := catalog.LoadMigration(ctx, pool, moved); err != nil || m.State != catalog.MigrationQueued {
		t.Fatalf("while the placement copies: %v %v, want the migration held", m.State, err)
	}

	// The placement swaps: orders is sharded now, and the queue lets go.
	exec(`UPDATE pgshard.table_status SET effective_placement = 'sharded', effective_shard_key = 'id' WHERE table_name = 'orders'`)
	exec(`UPDATE pgshard.workflows SET status = '{"stage": "retiring"}' WHERE id = $1`, move)
	if _, err := a.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}

	m, err := catalog.LoadMigration(ctx, pool, moved)
	if err != nil {
		t.Fatal(err)
	}
	if m.State != catalog.MigrationFailed {
		t.Fatalf("the migration planned under the old placement is %s, want failed", m.State)
	}
	for _, want := range []string{"public.orders", "unsharded", "sharded", "re-issue"} {
		if !strings.Contains(m.Error, want) {
			t.Errorf("error %q does not say %q", m.Error, want)
		}
	}
	for _, id := range []int32{0, 1} {
		for _, s := range shards.statements(id) {
			if strings.Contains(s, "orders") {
				t.Errorf("shard %d ran %q, planned under a placement that had moved", id, s)
			}
		}
	}

	// A migration whose own tables did not move is not caught by another's.
	if m, err := catalog.LoadMigration(ctx, pool, stayed); err != nil || m.State != catalog.MigrationComplete {
		t.Fatalf("the migration on the table that did not move: %v %v, want it applied", m.State, err)
	}
}

// TestPlacementDriftReadsWhatTheRouterRead (PGS-971): a false positive fails
// DDL that was planned correctly, so the check must resolve a placement the
// way the router's snapshot does -- an observed status row first, then a
// declared unsharded row with no observation yet, then the database default.
func TestPlacementDriftReadsWhatTheRouterRead(t *testing.T) {
	ctx, pool, exec := holdCatalog(t)
	exec(`UPDATE pgshard.databases SET default_placement = 'reference' WHERE name = 'app'`)
	exec(`INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', 'declared', 'unsharded')`)
	exec(`INSERT INTO pgshard.table_status (database, schema_name, table_name, effective_placement) VALUES ('app', 'public', 'observed', 'sharded')`)

	drift := func(table, placement string) string {
		t.Helper()
		got, err := catalog.PlacementDrift(ctx, pool, "app", []catalog.TablePlacement{{Schema: "public", Table: table, Placement: placement}})
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	for _, c := range []struct {
		table, placement string
		moved            bool
	}{
		{"observed", "sharded", false},
		{"observed", "unsharded", true},
		{"declared", "unsharded", false},
		{"declared", "reference", true},
		{"undeclared", "reference", false},
		{"undeclared", "unsharded", true},
	} {
		if got := drift(c.table, c.placement); (got != "") != c.moved {
			t.Errorf("%s recorded as %s: drift %q, want moved=%v", c.table, c.placement, got, c.moved)
		}
	}
	if got, err := catalog.PlacementDrift(ctx, pool, "app", nil); err != nil || got != "" {
		t.Errorf("no placements recorded: %q %v, want no drift", got, err)
	}
}

// TestAReKeyIsAPlacementChange (PGS-971 review): a re-key leaves the word
// "sharded" alone, and a fresh planner would still refuse what was planned
// before it -- a unique index on (tenant_id, id) enforces nothing across
// shards once the key is region_id. The shard key is part of what is
// compared.
func TestAReKeyIsAPlacementChange(t *testing.T) {
	ctx, pool, exec := holdCatalog(t)
	exec(`INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('default', 0, int8range(NULL, 0)), ('default', 1, int8range(0, NULL))`)
	reconcile(t, connect(t, pool.Config().ConnString()))
	exec(`INSERT INTO pgshard.table_status (database, schema_name, table_name, effective_placement, effective_shard_key) VALUES ('app', 'public', 'orders', 'sharded', 'tenant_id')`)
	id, err := catalog.EnqueueMigration(ctx, pool, catalog.DDLMigration{Database: "app", Statement: "CREATE UNIQUE INDEX uq ON orders (tenant_id, id)",
		Kind: "CREATE INDEX", Strategy: "direct", Scope: "all",
		Meta: catalog.MigrationMeta{Placements: []catalog.TablePlacement{{Schema: "public", Table: "orders", Placement: "sharded", ShardKey: "tenant_id"}}}})
	if err != nil {
		t.Fatal(err)
	}
	exec(`UPDATE pgshard.table_status SET effective_shard_key = 'region_id' WHERE table_name = 'orders'`)
	shards := newFakeShards()
	a := &Applier{Store: &PGMigrationStore{Pool: pool}, Shards: shards, RewriteSettle: -1}
	if _, err := a.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	m, err := catalog.LoadMigration(ctx, pool, id)
	if err != nil {
		t.Fatal(err)
	}
	if m.State != catalog.MigrationFailed || !strings.Contains(m.Error, "sharded by tenant_id") || !strings.Contains(m.Error, "sharded by region_id") {
		t.Fatalf("a unique index planned under the old shard key is %s (%q), want failed naming both keys", m.State, m.Error)
	}
	for _, id := range []int32{0, 1} {
		if got := shards.statements(id); len(got) != 0 {
			t.Errorf("shard %d ran %v", id, got)
		}
	}
}

// TestTheStartItselfRefusesAMovedPlacement (PGS-971 review): the applier's
// check and the guarded start are two statements, and a placement that swaps
// between them -- publishing its new placement and entering retiring, which
// lifts the queue's hold -- let the start through under the old plan. The
// start's own guard now refuses it, in the same statement that would have
// started it; the migration stays queued and the next pass fails it.
func TestTheStartItselfRefusesAMovedPlacement(t *testing.T) {
	ctx, pool, exec := holdCatalog(t)
	exec(`INSERT INTO pgshard.table_status (database, schema_name, table_name, effective_placement) VALUES ('app', 'public', 'orders', 'unsharded')`)
	id, err := catalog.EnqueueMigration(ctx, pool, catalog.DDLMigration{Database: "app", Statement: "ALTER TABLE orders ADD COLUMN note text",
		Kind: "ALTER TABLE", Strategy: "direct", Scope: "home", HomeShard: 0,
		Meta: catalog.MigrationMeta{Placements: []catalog.TablePlacement{{Schema: "public", Table: "orders", Placement: "unsharded"}}}})
	if err != nil {
		t.Fatal(err)
	}
	m, err := catalog.LoadMigration(ctx, pool, id)
	if err != nil {
		t.Fatal(err)
	}
	// The check has passed; now the swap lands, and then the start.
	exec(`UPDATE pgshard.table_status SET effective_placement = 'sharded', effective_shard_key = 'id' WHERE table_name = 'orders'`)
	m.State, m.Meta.ShardSet, m.PerShard = catalog.MigrationRunning, "default", map[string]catalog.ShardMigration{"0": {State: catalog.ShardPending}}
	if err := (&PGMigrationStore{Pool: pool}).Save(ctx, m, 0); !errors.Is(err, catalog.ErrMigrationHeld) {
		t.Fatalf("starting a migration whose placement moved after the check: %v, want the start refused", err)
	}
	if got, err := catalog.LoadMigration(ctx, pool, id); err != nil || got.State != catalog.MigrationQueued {
		t.Fatalf("after the refused start: %v %v, want it still queued", got.State, err)
	}

	// And a start whose placements still hold is not refused by the rule.
	exec(`UPDATE pgshard.table_status SET effective_placement = 'unsharded', effective_shard_key = NULL WHERE table_name = 'orders'`)
	if err := (&PGMigrationStore{Pool: pool}).Save(ctx, m, 0); err != nil {
		t.Fatalf("starting a migration whose placements still hold: %v", err)
	}
}
