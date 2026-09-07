package snapshot

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// pgshard.functions is how an operator tells the router that a non-built-in
// name is a scalar, so a scatter may project it. The rule the loader applies
// is the safety of the whole thing: one aggregate row anywhere in the
// database withdraws the name, however many scalar rows it has, because the
// router matches by name alone -- it does not resolve search_path, so a name
// that is a scalar in one schema and an aggregate in another is not a name
// it can decide.
func TestOneAggregateRowWithdrawsTheName(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `INSERT INTO pgshard.databases (name) VALUES ('app'), ('other')`)
	mustExec(t, conn, `INSERT INTO pgshard.functions (database, schema, name, kind) VALUES
		('app',   'public', 'st_astext',  'scalar'),
		('app',   'ext',    'st_astext',  'scalar'),
		('app',   'public', 'first',      'aggregate'),
		('app',   'public', 'similarity', 'scalar'),
		('app',   'ext',    'similarity', 'aggregate'),
		('other', 'public', 'only_there', 'scalar')`)

	snap, err := Load(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		db, name string
		want     bool
		why      string
	}{
		{"app", "st_astext", true, "a scalar in two schemas and an aggregate in none"},
		{"app", "first", false, "an aggregate"},
		{"app", "similarity", false, "a scalar in one schema and an aggregate in another"},
		{"app", "never_declared", false, "not declared at all"},
		{"other", "only_there", true, "declared in this database"},
		{"app", "only_there", false, "declared in another database"},
	} {
		if got := snap.ScalarFunctions[FunctionKey{Database: c.db, Name: c.name}]; got != c.want {
			t.Errorf("%s.%s: scalar=%v, want %v -- %s", c.db, c.name, got, c.want, c.why)
		}
	}
}

// The rows belong to a database, and go with it.
func TestDroppingADatabaseTakesItsFunctionsWithIt(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `INSERT INTO pgshard.databases (name) VALUES ('app')`)
	mustExec(t, conn, `INSERT INTO pgshard.functions (database, schema, name, kind) VALUES ('app', 'public', 'f', 'scalar')`)
	mustExec(t, conn, `DELETE FROM pgshard.databases WHERE name = 'app'`)
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM pgshard.functions`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("%d function rows survived their database", n)
	}
	// And a row for a database that does not exist cannot be written at all.
	if _, err := conn.Exec(ctx,
		`INSERT INTO pgshard.functions (database, schema, name, kind) VALUES ('gone', 'public', 'f', 'scalar')`); err == nil {
		t.Error("a function was declared for a database that does not exist")
	}
}
