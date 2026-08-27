package controller

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestReferenceCheckReportsWhatEachShardWouldEvaluate is the half of the
// divergence the router cannot see. Every case here writes a different row
// on every shard while 2PC commits them all, and none of it appears in the
// statement, so the check has to come from the shards themselves.
func TestReferenceCheckReportsWhatEachShardWouldEvaluate(t *testing.T) {
	ctx := context.Background()
	f := newPlacementFixture(t)
	check := &ReferenceCheck{Pool: f.pool, Shards: f.placer.Shards, Logger: slog.New(slog.DiscardHandler)}

	for _, ddl := range []string{
		// Deterministic on every shard: a literal, a folded operator, an
		// immutable call, and a generated column over the row's own values.
		`CREATE TABLE clean (
			id bigint PRIMARY KEY,
			region text NOT NULL DEFAULT 'eu',
			weight int NOT NULL DEFAULT 1 + 1,
			name text NOT NULL DEFAULT upper('x'),
			label text GENERATED ALWAYS AS (upper(region)) STORED)`,
		`CREATE TABLE hazards (
			id bigint PRIMARY KEY,
			tag uuid NOT NULL DEFAULT gen_random_uuid(),
			seen timestamptz NOT NULL DEFAULT now(),
			stamped timestamptz NOT NULL DEFAULT current_timestamp,
			n bigserial)`,
		`CREATE TABLE identity_table (id bigint GENERATED ALWAYS AS IDENTITY, note text)`,
		`CREATE TABLE triggered (id bigint PRIMARY KEY, note text)`,
		`CREATE FUNCTION stamp() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.note := clock_timestamp()::text; RETURN NEW; END $$`,
		`CREATE TRIGGER stamp BEFORE INSERT ON triggered FOR EACH ROW EXECUTE FUNCTION stamp()`,
	} {
		for id := range 2 {
			mustExec(t, f.app(int32(id)), ddl)
		}
	}
	for _, name := range []string{"clean", "hazards", "identity_table", "triggered"} {
		mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', $1, 'reference')`, name)
	}
	if _, err := Reconcile(ctx, f.catalog, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatal(err)
	}

	published, err := check.Pass(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if published != 4 {
		t.Fatalf("published = %d, want every reference table inspected", published)
	}
	for _, c := range []struct {
		table string
		want  []string
	}{
		{"clean", nil},
		{"hazards", []string{
			"the default of column n calls nextval(), which pg_proc marks VOLATILE",
			"the default of column seen calls now(), which pg_proc marks STABLE",
			"the default of column stamped uses SQLVALUEFUNCTION, which is not a node proven deterministic",
			"the default of column tag calls gen_random_uuid(), which pg_proc marks VOLATILE",
		}},
		{"identity_table", []string{"column id is an identity column"}},
		{"triggered", []string{"trigger stamp fires on writes"}},
	} {
		got := hazardsOf(t, f.catalog, c.table)
		if strings.Join(got, "\n") != strings.Join(c.want, "\n") {
			t.Fatalf("%s hazards =\n%s\nwant\n%s", c.table, strings.Join(got, "\n"), strings.Join(c.want, "\n"))
		}
	}

	// A second pass has nothing left: the recorded generation matches, so
	// the shards are not dialled again for a table that has not changed.
	if published, err := check.Pass(ctx); err != nil || published != 0 {
		t.Fatalf("second pass published %d (%v), want none", published, err)
	}

	// Drop the hazards and bump the generation: the next pass must clear
	// the table rather than leave it refused for ever.
	for id := range 2 {
		mustExec(t, f.app(int32(id)), `ALTER TABLE hazards DROP COLUMN tag, DROP COLUMN seen, DROP COLUMN stamped, DROP COLUMN n`)
	}
	mustExec(t, f.catalog, `UPDATE pgshard.table_status SET effective_generation = effective_generation + 1 WHERE table_name = 'hazards'`)
	if _, err := check.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if got := hazardsOf(t, f.catalog, "hazards"); len(got) != 0 {
		t.Fatalf("hazards after the columns went away: %v", got)
	}
}

// TestReferenceCheckSeesADriftedShard covers the reason every shard is
// asked rather than one: a table that is only wrong on the second shard is
// exactly the case that would otherwise surface as disagreeing rows.
func TestReferenceCheckSeesADriftedShard(t *testing.T) {
	ctx := context.Background()
	f := newPlacementFixture(t)
	check := &ReferenceCheck{Pool: f.pool, Shards: f.placer.Shards, Logger: slog.New(slog.DiscardHandler)}
	for id := range 2 {
		mustExec(t, f.app(int32(id)), `CREATE TABLE drifted (id bigint PRIMARY KEY, note text)`)
	}
	mustExec(t, f.app(1), `ALTER TABLE drifted ALTER COLUMN note SET DEFAULT clock_timestamp()::text`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', 'drifted', 'reference')`)
	if _, err := Reconcile(ctx, f.catalog, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatal(err)
	}
	if _, err := check.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	got := hazardsOf(t, f.catalog, "drifted")
	if len(got) != 1 || !strings.Contains(got[0], "clock_timestamp()") {
		t.Fatalf("hazards = %v, want the second shard's default reported", got)
	}
}

// TestReferenceCheckLeavesATableUncheckedWhenAShardIsUnreachable: a clean
// result must mean every shard was asked. Publishing what one shard said
// would read as inspected-and-safe for a table half of the cluster was
// never asked about.
func TestReferenceCheckLeavesATableUncheckedWhenAShardIsUnreachable(t *testing.T) {
	ctx := context.Background()
	f := newPlacementFixture(t)
	check := &ReferenceCheck{Pool: f.pool, Shards: f.placer.Shards, Logger: slog.New(slog.DiscardHandler)}
	mustExec(t, f.app(0), `CREATE TABLE lonely (id bigint PRIMARY KEY)`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', 'lonely', 'reference')`)
	if _, err := Reconcile(ctx, f.catalog, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatal(err)
	}
	check.Shards = &PgxShardDialer{Pool: f.pool, DSNs: map[ShardRef]string{
		{Set: "default", ID: 0}: f.dsns[ShardRef{Set: "default", ID: 0}],
		{Set: "default", ID: 1}: "postgres://pgshard@127.0.0.1:1/postgres?connect_timeout=1",
	}}
	published, err := check.Pass(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if published != 0 {
		t.Fatalf("published = %d, want the table left unchecked", published)
	}
	var checked *int64
	if err := f.catalog.QueryRow(ctx, `SELECT reference_checked_generation FROM pgshard.table_status WHERE table_name = 'lonely'`).Scan(&checked); err != nil {
		t.Fatal(err)
	}
	if checked != nil {
		t.Fatalf("reference_checked_generation = %d, want NULL while a shard was never asked", *checked)
	}
}

func hazardsOf(t *testing.T, conn *pgx.Conn, table string) []string {
	t.Helper()
	var got []string
	if err := conn.QueryRow(context.Background(),
		`SELECT reference_hazards FROM pgshard.table_status WHERE database = 'app' AND schema_name = 'public' AND table_name = $1`, table).Scan(&got); err != nil {
		t.Fatal(err)
	}
	slices.Sort(got)
	return got
}
