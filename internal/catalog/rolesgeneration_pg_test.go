package catalog

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

// nonRolesDesiredTables are the desired-state tables the roles generation
// deliberately does NOT count, because roles materialization does not read
// them: where data lives (databases, tables, shard_ranges, shard_sets) and
// what the planner may push down (functions).
var nonRolesDesiredTables = []string{"databases", "functions", "shard_ranges", "shard_sets", "tables"}

// TestEveryRolesTableBumpsTheGeneration: the generation is the max over the
// desired-state tables roles materialization reads, and the whole reason it
// is defined in the catalog is that the expression had diverged in three of
// the four places that spelled it (0052). Since 0053 it is a singleton
// counter and the four tables carry a trigger that bumps it, so the way to
// leave a table out is now to forget the trigger -- silently, because a
// generation that is merely too LOW still looks like a number and every
// waiter on it returns early.
//
// So every table in the schema carrying a desired_generation column has to
// be classified: it bumps the roles generation, or it is named here as
// state roles materialization does not read. Adding one without doing
// either fails this test, which is the property the single definition is
// for.
func TestEveryRolesTableBumpsTheGeneration(t *testing.T) {
	requireDocker(t)
	ctx := context.Background()
	conn := connect(t, startPostgres(t, candidateImages[0]))
	if err := Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}

	rows, err := conn.Query(ctx, `
		SELECT c.relname, EXISTS (
		           SELECT 1 FROM pg_trigger g
		            WHERE g.tgrelid = c.oid AND NOT g.tgisinternal
		              AND g.tgfoid = 'pgshard.bump_roles_generation'::regproc)
		  FROM pg_class c
		  JOIN pg_namespace n ON n.oid = c.relnamespace
		  JOIN pg_attribute a ON a.attrelid = c.oid
		 WHERE n.nspname = 'pgshard' AND c.relkind IN ('r', 'p')
		   AND a.attname = 'desired_generation' AND NOT a.attisdropped
		 ORDER BY c.relname`)
	if err != nil {
		t.Fatal(err)
	}
	type table struct {
		name  string
		bumps bool
	}
	tables, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (table, error) {
		var tb table
		err := row.Scan(&tb.name, &tb.bumps)
		return tb, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) == 0 {
		t.Fatal("no desired-state tables found; the query, not the schema, is wrong")
	}

	nonRoles := map[string]bool{}
	for _, name := range nonRolesDesiredTables {
		nonRoles[name] = true
	}
	var counted []string
	for _, tb := range tables {
		switch {
		case tb.bumps && nonRoles[tb.name]:
			t.Errorf("pgshard.%s bumps the roles generation and is also listed as a table roles materialization does not read; one of the two is wrong", tb.name)
		case tb.bumps:
			counted = append(counted, tb.name)
		case nonRoles[tb.name]:
		default:
			t.Errorf("pgshard.%s carries desired_generation but has no bump_roles_generation trigger: "+
				"either add the trigger in a migration, or add it to nonRolesDesiredTables if roles "+
				"materialization does not read it", tb.name)
		}
	}
	for _, name := range nonRolesDesiredTables {
		found := false
		for _, tb := range tables {
			found = found || tb.name == name
		}
		if !found {
			t.Errorf("nonRolesDesiredTables names pgshard.%s, which has no desired_generation column", name)
		}
	}
	if len(counted) < 4 {
		t.Errorf("only %v bump the roles generation; all four roles tables must", counted)
	}
}
