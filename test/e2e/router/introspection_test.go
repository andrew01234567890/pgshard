//go:build integration

package router

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// The working column of an in-flight rewrite is hidden from SELECT * and
// refused by name, and used to be listed by information_schema.columns and
// pg_attribute anyway -- confirmed against a real PostgreSQL before this
// was fixed, which is what settled how much PGS-590 bites. Anything that
// reads the schema and then writes SQL from it, an ORM's model check or a
// migration tool's diff, proposed a column the router would not let it
// name.
func TestIntrospectionDoesNotListTheWorkingColumnOfARewrite(t *testing.T) {
	s := startStack(t)
	ctx := context.Background()
	conn := s.connect(t)
	if _, err := conn.Exec(ctx, `create table probe (id bigint primary key, amount int)`); err != nil {
		t.Fatal(err)
	}

	// The applier would carry the rewrite through; this test wants it
	// pending, which is when the column exists and is hidden.
	s.stopController()

	cat, err := pgx.Connect(ctx, s.catalogDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cat.Close(ctx) }()
	id, err := catalog.EnqueueMigration(ctx, cat, catalog.DDLMigration{
		Database: appDatabase, Statement: "alter table probe alter column amount type bigint",
		Kind: "ALTER TABLE", Strategy: catalog.StrategyRewrite, Scope: "all",
		Meta: catalog.MigrationMeta{Rewrite: &catalog.RewriteChange{Schema: "public", Table: "probe",
			Column: "amount", NewType: "bigint", Using: "amount::bigint", Columns: []string{"id", "amount"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	rw := &catalog.RewriteChange{Column: "amount"}
	working := rw.HiddenColumn(id)
	t.Logf("working column: %s", working)

	// The applier adds it on each shard; add it directly, since the
	// applier is stopped.
	shard, err := pgx.Connect(ctx, streamDSN(s.shardDSN))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = shard.Close(ctx) }()
	if _, err := shard.Exec(ctx, `alter table probe add column `+quote(working)+` bigint`); err != nil {
		t.Fatal(err)
	}

	// What the router already does: the column cannot be named, and is not
	// in SELECT *.
	if _, err := conn.Exec(ctx, `select `+quote(working)+` from probe`); err == nil {
		t.Fatal("naming the working column was accepted")
	}

	var listed bool
	if err := conn.QueryRow(ctx,
		`select exists (select 1 from information_schema.columns where table_name = 'probe' and column_name = $1)`,
		working).Scan(&listed); err != nil {
		t.Fatal(err)
	}
	var inAttribute bool
	if err := conn.QueryRow(ctx,
		`select exists (select 1 from pg_catalog.pg_attribute a join pg_catalog.pg_class c on c.oid = a.attrelid
		 where c.relname = 'probe' and a.attname = $1)`, working).Scan(&inAttribute); err != nil {
		t.Fatal(err)
	}
	if listed {
		t.Error("information_schema.columns lists the working column; a schema diff would propose a column the router refuses to name")
	}
	if inAttribute {
		t.Error("pg_attribute lists the working column")
	}

	// The rest of the table is still there: the filter drops pgshard's own
	// columns, not the client's.
	var visible int
	if err := conn.QueryRow(ctx,
		`select count(*) from information_schema.columns where table_name = 'probe'`).Scan(&visible); err != nil {
		t.Fatal(err)
	}
	if visible != 2 {
		t.Fatalf("information_schema.columns reports %d columns for probe, want the 2 the client created", visible)
	}
}

func quote(s string) string { return `"` + s + `"` }
