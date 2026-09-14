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

// TestTheRolesGenerationCountsEveryRolesTable: the generation is the max
// over the desired-state tables roles materialization reads, and the whole
// reason it is a catalog function is that the expression had diverged in
// three of the four places that spelled it. A fifth roles table would
// reintroduce exactly that -- silently, because a generation that is merely
// too LOW still looks like a number and every waiter on it returns early.
//
// So every table in the schema carrying a desired_generation column has to
// be classified: counted by the function, or named here as state roles
// materialization does not read. Adding one without doing either fails
// this test, which is the property the single definition is for.
func TestTheRolesGenerationCountsEveryRolesTable(t *testing.T) {
	requireDocker(t)
	ctx := context.Background()
	conn := connect(t, startPostgres(t, candidateImages[0]))
	if err := Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}

	// pg_depend answers this only because 0052 writes the function with a
	// standard SQL body; a quoted body records no dependencies at all.
	rows, err := conn.Query(ctx, `
		SELECT c.relname, EXISTS (
		           SELECT 1 FROM pg_depend d
		            WHERE d.classid = 'pg_proc'::regclass
		              AND d.objid = 'pgshard.roles_desired_generation'::regproc
		              AND d.refclassid = 'pg_class'::regclass
		              AND d.refobjid = c.oid)
		  FROM pg_class c
		  JOIN pg_namespace n ON n.oid = c.relnamespace
		  JOIN pg_attribute a ON a.attrelid = c.oid
		 WHERE n.nspname = 'pgshard' AND c.relkind = 'r'
		   AND a.attname = 'desired_generation' AND NOT a.attisdropped
		 ORDER BY c.relname`)
	if err != nil {
		t.Fatal(err)
	}
	type table struct {
		name    string
		counted bool
	}
	tables, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (table, error) {
		var tb table
		err := row.Scan(&tb.name, &tb.counted)
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
		case tb.counted && nonRoles[tb.name]:
			t.Errorf("pgshard.%s is counted by roles_desired_generation and also listed as a table roles materialization does not read; one of the two is wrong", tb.name)
		case tb.counted:
			counted = append(counted, tb.name)
		case nonRoles[tb.name]:
		default:
			t.Errorf("pgshard.%s carries desired_generation but roles_desired_generation does not count it: "+
				"either add it to the function in a migration, or add it to nonRolesDesiredTables if roles "+
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
		t.Errorf("roles_desired_generation counts only %v; it is the max over all four roles tables", counted)
	}
}
