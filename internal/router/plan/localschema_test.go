package plan

import (
	"context"
	"testing"
)

// TestAStatementAboutALocalSchemaRunsOnTheHomeShard (PGS-870): pgroll's
// init creates its state in a schema of its own -- the schema, tables,
// indexes, functions, event triggers and a two-action ALTER TABLE, then a
// TRUNCATE and an INSERT -- all in one transaction. In a database that fans
// DDL out every statement was refused or would have been queued. With the
// schema listed in local_schemas each runs on the home shard, on the
// client's connection, and a table there is unsharded whatever the
// database's default placement.
func TestAStatementAboutALocalSchemaRunsOnTheHomeShard(t *testing.T) {
	s := fixture(t)
	db := s.Databases[fixtureDB]
	db.DefaultPlacement = "sharded"
	db.LocalSchemas = []string{"pgroll"}
	s.Databases[fixtureDB] = db
	p := New()
	for _, sql := range []string{
		`CREATE SCHEMA IF NOT EXISTS "pgroll"`,
		`CREATE OR REPLACE FUNCTION "pgroll".raw_migration () RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = "pgroll", pg_catalog, pg_temp AS $$ BEGIN RETURN; END; $$`,
		`DROP EVENT TRIGGER IF EXISTS pg_roll_handle_ddl`,
		`CREATE EVENT TRIGGER pg_roll_handle_ddl ON ddl_command_end EXECUTE FUNCTION "pgroll".raw_migration ()`,
		`CREATE TABLE IF NOT EXISTS "pgroll".migrations (schema NAME NOT NULL, name text NOT NULL, migration jsonb NOT NULL, parent text, done boolean NOT NULL DEFAULT FALSE, PRIMARY KEY (schema, name), FOREIGN KEY (schema, parent) REFERENCES "pgroll".migrations (schema, name))`,
		`CREATE UNIQUE INDEX IF NOT EXISTS only_one_active ON "pgroll".migrations (schema, name, done) WHERE done = FALSE`,
		`ALTER TABLE "pgroll".migrations ADD COLUMN IF NOT EXISTS migration_type varchar(32) DEFAULT 'pgroll' CONSTRAINT migration_type_check CHECK (migration_type IN ('pgroll', 'inferred'))`,
		`ALTER TABLE "pgroll".migrations ALTER COLUMN created_at SET DATA TYPE timestamptz USING created_at AT TIME ZONE 'UTC', ALTER COLUMN updated_at SET DATA TYPE timestamptz USING updated_at AT TIME ZONE 'UTC'`,
		`COMMENT ON TABLE "pgroll".migrations IS 'state'`,
		`CREATE SEQUENCE "pgroll".s`,
		`DROP FUNCTION IF EXISTS "pgroll".raw_migration()`,
		`DROP TABLE "pgroll".migrations, "pgroll".pgroll_version`,
		`TRUNCATE TABLE "pgroll".pgroll_version`,
		`INSERT INTO "pgroll".pgroll_version (version) VALUES ('v0.16.3')`,
		`SELECT name FROM pgroll.migrations WHERE done = false`,
	} {
		pl, err := p.Plan(context.Background(), session(s), sql)
		if err != nil {
			t.Errorf("refused: %.70s\n  %v", sql, err)
			continue
		}
		if pl.Kind != Unsharded || pl.Migration != nil || len(pl.Shards) != 1 || pl.Shards[0] != db.HomeShard {
			t.Errorf("%.70s planned %v on %v (migration %v), want the home shard on the client's connection", sql, pl.Kind, pl.Shards, pl.Migration != nil)
		}
	}

	// A statement that is not wholly about the local schema is planned as
	// it was: unqualified, another schema, or both at once.
	for _, sql := range []string{
		`CREATE SCHEMA app_v1`,
		`CREATE TABLE migrations (id int PRIMARY KEY)`,
		`DROP TABLE "pgroll".migrations, public.orders`,
		`CREATE FUNCTION public.f() RETURNS int LANGUAGE sql AS 'select 1'`,
		`CREATE EVENT TRIGGER t ON ddl_command_end EXECUTE FUNCTION raw_migration()`,
		`COMMENT ON COLUMN public.orders.note IS 'x'`,
	} {
		pl, err := p.Plan(context.Background(), session(s), sql)
		if err == nil && pl.Kind == Unsharded && pl.Migration == nil {
			t.Errorf("%.70s ran on the home shard alone; its objects are not all in a local schema", sql)
		}
	}

	// Without the listing, pgroll's state is an ordinary schema again.
	db.LocalSchemas = nil
	s.Databases[fixtureDB] = db
	if pl, err := p.Plan(context.Background(), session(s), `CREATE SCHEMA IF NOT EXISTS "pgroll"`); err != nil || pl.Migration == nil {
		t.Fatalf("CREATE SCHEMA pgroll without the listing: %v, migration %v; want a fanned-out migration", err, pl.Migration != nil)
	}
	if _, err := p.Plan(context.Background(), session(s), `INSERT INTO "pgroll".pgroll_version (version) VALUES ('v')`); err == nil {
		t.Fatal("an undeclared table in a sharded-default database was routed without the listing")
	}
}
