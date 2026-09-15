package catalog

import (
	"context"
	"slices"
	"strings"
	"testing"
)

// TestALocalSchemaAndADistributedTableCannotBothBeDeclared (PGS-870): a
// schema in local_schemas lives on the home shard alone, so it cannot hold
// a sharded or reference table, from either side of the declaration.
func TestALocalSchemaAndADistributedTableCannotBothBeDeclared(t *testing.T) {
	requireDocker(t)
	ctx := context.Background()
	conn := connect(t, startPostgres(t, candidateImages[0]))
	if err := Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	exec := func(sql string) error {
		_, err := conn.Exec(ctx, sql)
		return err
	}
	if err := exec(`INSERT INTO pgshard.databases (name, default_placement, local_schemas) VALUES ('app', 'sharded', '{pgroll}')`); err != nil {
		t.Fatal(err)
	}
	dbs, err := ListDatabases(ctx, conn)
	if err != nil || len(dbs) != 1 || !slices.Equal(dbs[0].LocalSchemas, []string{"pgroll"}) {
		t.Fatalf("listed %+v, %v; want local_schemas [pgroll]", dbs, err)
	}
	if err := exec(`INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'pgroll', 'migrations', 'unsharded')`); err != nil {
		t.Fatalf("an unsharded table in a local schema: %v", err)
	}
	if err := exec(`INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('app', 'pgroll', 'events', 'sharded', 'id')`); err == nil {
		t.Fatal("a sharded table was declared in a local schema")
	} else if !strings.Contains(err.Error(), "local_schemas") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
	for _, reserved := range []string{`'{pgshard}'`, `'{pg_catalog}'`, `'{information_schema}'`, `'{""}'`, `ARRAY[NULL]::text[]`} {
		if err := exec(`UPDATE pgshard.databases SET local_schemas = ` + reserved + ` WHERE name = 'app'`); err == nil {
			t.Errorf("local_schemas = %s was accepted; a reshard drops listed schemas on every shard but one", reserved)
		}
	}
	if err := exec(`INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'audit', 'regions', 'reference')`); err != nil {
		t.Fatal(err)
	}
	if err := exec(`UPDATE pgshard.databases SET local_schemas = '{pgroll,audit}' WHERE name = 'app'`); err == nil {
		t.Fatal("a schema holding a reference table was listed as local")
	} else if !strings.Contains(err.Error(), "audit.regions") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
}
