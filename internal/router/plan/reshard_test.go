package plan

import (
	"context"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
)

// TestTruncateRefusedWhileResharding: a provisioning shard set (the copy
// phase streams row changes only) turns every TRUNCATE into a refusal,
// including the one on the unsharded table that is otherwise allowed.
func TestTruncateRefusedWhileResharding(t *testing.T) {
	snap := fixture(t)
	p := New()
	if _, err := p.Plan(context.Background(), session(snap), "truncate items"); err != nil {
		t.Fatalf("truncate before reshard: %v", err)
	}
	snap.Serving = map[snapshot.ShardKey]snapshot.Serving{{ShardSet: "g2", ShardID: 0}: {State: "provisioning"}}
	if !snap.Resharding() {
		t.Fatal("snapshot must report the reshard")
	}
	for _, sql := range []string{"truncate items", "truncate orders", "truncate items, regions"} {
		pl, err := p.Plan(context.Background(), session(snap), sql)
		checkRefusal(t, pl, err, "TRUNCATE is not available while a reshard is active")
	}
	if _, err := p.Plan(context.Background(), session(snap), "delete from items"); err != nil {
		t.Fatalf("delete during reshard: %v", err)
	}
	snap.Serving[snapshot.ShardKey{ShardSet: "g2", ShardID: 0}] = snapshot.Serving{State: "serving"}
	if snap.Resharding() {
		t.Fatal("a serving set is not a reshard")
	}
}

// TestPlanListsResolvedTables: the executor holds writes to a table a
// placement workflow is swapping, so every plan names the catalog tables
// it resolved.
func TestPlanListsResolvedTables(t *testing.T) {
	snap := fixture(t)
	p := New()
	pl, err := p.Plan(context.Background(), session(snap), "update orders set note = 'x' where tenant_id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(pl.Tables) != 1 || pl.Tables[0] != (snapshot.TableKey{Database: fixtureDB, SchemaName: "public", TableName: "orders"}) {
		t.Fatalf("tables: %+v", pl.Tables)
	}
	pl, err = p.Plan(context.Background(), session(snap), "select 1")
	if err != nil || len(pl.Tables) != 0 {
		t.Fatalf("select 1: %+v %v", pl.Tables, err)
	}
	if snap.TableMigrating(pl.Tables) {
		t.Fatal("no tables, no fence")
	}
	key := snapshot.TableKey{Database: fixtureDB, SchemaName: "public", TableName: "orders"}
	placement := snap.Tables[key]
	placement.Migrating = true
	snap.Tables[key] = placement
	if !snap.TableMigrating([]snapshot.TableKey{key}) {
		t.Fatal("a migrating table must fence its writes")
	}
	if snap.TableMigrating([]snapshot.TableKey{{Database: fixtureDB, SchemaName: "public", TableName: "regions"}}) {
		t.Fatal("another table must not be fenced")
	}
}

// TestDDLRefusedWhileResharding (PGS-872): a reshard copies by logical
// replication, which carries no DDL, so a schema change on the serving
// shards during the copy breaks the new shards' apply. The applier holds
// such a migration; the planner refuses it at once.
func TestDDLRefusedWhileResharding(t *testing.T) {
	snap := fixture(t)
	p := New()
	ddl := []string{"alter table orders add column extra int", "create index orders_note2 on orders (note)", "create role reporter", "grant select on items to app"}
	for _, sql := range ddl {
		pl, err := p.Plan(context.Background(), session(snap), sql)
		if err != nil || pl.Kind != MigrationKind {
			t.Fatalf("%q before a reshard: %v %v", sql, pl.Kind, err)
		}
	}
	snap.Serving = map[snapshot.ShardKey]snapshot.Serving{{ShardSet: "g2", ShardID: 0}: {State: "provisioning"}}
	for _, sql := range ddl {
		pl, err := p.Plan(context.Background(), session(snap), sql)
		checkRefusal(t, pl, err, "DDL is not available while a reshard is active")
	}
	snap.Serving[snapshot.ShardKey{ShardSet: "g2", ShardID: 0}] = snapshot.Serving{State: "serving"}
	if pl, err := p.Plan(context.Background(), session(snap), ddl[0]); err != nil || pl.Kind != MigrationKind {
		t.Fatalf("DDL after the reshard: %v %v", pl.Kind, err)
	}
}
