package controller

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestShardKeyCheckFaultsAKeyTheRouterAndTheCopyWouldDisagreeOn: a table
// already on the shards becomes sharded by an INSERT into pgshard.tables,
// which no CREATE TABLE and no placement workflow ever sees. The router
// hashes the key the client sent and a copy hashes the key the shard
// stored, and for a blank-padded character(n) those are different values
// for keys PostgreSQL calls equal.
func TestShardKeyCheckFaultsAKeyTheRouterAndTheCopyWouldDisagreeOn(t *testing.T) {
	ctx := context.Background()
	f := newPlacementFixture(t)
	check := &ShardKeyCheck{Pool: f.pool, Shards: f.placer.Shards, Logger: slog.New(slog.DiscardHandler)}

	for id := range 2 {
		mustExec(t, f.app(int32(id)), `CREATE TABLE padded (tenant_id character(8) NOT NULL, id bigint, PRIMARY KEY (tenant_id, id))`)
		mustExec(t, f.app(int32(id)), `CREATE TABLE plain (tenant_id text NOT NULL, id bigint, PRIMARY KEY (tenant_id, id))`)
		mustExec(t, f.app(int32(id)), `CREATE TABLE jsonkey (tenant_id jsonb NOT NULL, id bigint)`)
	}
	for _, name := range []string{"padded", "plain", "jsonkey", "notyetcreated"} {
		mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key)
			VALUES ('app', 'public', $1, 'sharded', 'tenant_id')`, name)
	}

	published, err := check.Pass(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if published != 4 {
		t.Fatalf("published = %d, want every sharded table checked", published)
	}
	for _, c := range []struct{ table, want string }{
		{"plain", ""},
		// Declared before the table exists: nothing to fault, and the
		// CREATE TABLE that follows goes through pgshard, which checks it.
		{"notyetcreated", ""},
		// A blank-padded key routes now: the router trims by the column's
		// declared type and the row filter hashes through ::text, so both
		// hash the value with its padding gone.
		{"padded", ""},
		{"jsonkey", "cannot be hashed by a row filter"},
	} {
		got := keyErrorOf(t, f.catalog, c.table)
		switch {
		case c.want == "" && got != "":
			t.Errorf("%s faulted as %q, want a key the router can use", c.table, got)
		case c.want != "" && !strings.Contains(got, c.want):
			t.Errorf("%s faulted as %q, want it to say %q", c.table, got, c.want)
		}
	}

	// A second pass has nothing left: the recorded generation matches, so
	// the shards are not dialled again for a key that has not changed.
	if published, err := check.Pass(ctx); err != nil || published != 0 {
		t.Fatalf("second pass published %d (%v), want none", published, err)
	}

	// Point the faulted table at a key that works: the next pass must
	// clear the fault rather than leave the table refused for ever.
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET shard_key = 'id' WHERE table_name = 'jsonkey'`)
	if _, err := check.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if got := keyErrorOf(t, f.catalog, "jsonkey"); got != "" {
		t.Fatalf("still faulted as %q after the key changed", got)
	}
}

// TestShardKeyCheckSeesADriftedShard covers the reason every shard is
// asked rather than one.
func TestShardKeyCheckSeesADriftedShard(t *testing.T) {
	ctx := context.Background()
	f := newPlacementFixture(t)
	check := &ShardKeyCheck{Pool: f.pool, Shards: f.placer.Shards, Logger: slog.New(slog.DiscardHandler)}
	mustExec(t, f.app(0), `CREATE TABLE drifted (tenant_id text NOT NULL, id bigint)`)
	mustExec(t, f.app(1), `CREATE TABLE drifted (tenant_id character(8) NOT NULL, id bigint)`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key)
		VALUES ('app', 'public', 'drifted', 'sharded', 'tenant_id')`)
	if _, err := check.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	// Both types are hashable on their own; disagreeing about which one
	// the column is, is the fault. A check that asked one shard would see
	// text, call it fine, and route by a hash the other shard's rows were
	// never placed with.
	if got := keyErrorOf(t, f.catalog, "drifted"); !strings.Contains(got, "the shards must agree") {
		t.Fatalf("fault = %q, want the second shard's column type to have been seen", got)
	}
}

// TestShardKeyCheckLeavesATableUncheckedWhenAShardIsUnreachable: a verdict
// for a table nobody looked at is worse than no verdict, because no
// verdict is what the router treats as unproven.
func TestShardKeyCheckLeavesATableUncheckedWhenAShardIsUnreachable(t *testing.T) {
	ctx := context.Background()
	f := newPlacementFixture(t)
	check := &ShardKeyCheck{Pool: f.pool, Logger: slog.New(slog.DiscardHandler)}
	mustExec(t, f.app(0), `CREATE TABLE lonely (tenant_id character(8) NOT NULL, id bigint)`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key)
		VALUES ('app', 'public', 'lonely', 'sharded', 'tenant_id')`)
	check.Shards = &PgxShardDialer{Pool: f.pool, DSNs: map[ShardRef]string{
		{Set: "default", ID: 0}: "postgres://pgshard@127.0.0.1:1/postgres?connect_timeout=1",
		{Set: "default", ID: 1}: f.dsns[ShardRef{Set: "default", ID: 1}],
	}}
	published, err := check.Pass(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if published != 0 {
		t.Fatalf("published = %d, want the table left unchecked", published)
	}
	var checked *int64
	if err := f.catalog.QueryRow(ctx, `SELECT shard_key_checked_generation FROM pgshard.table_status WHERE table_name = 'lonely'`).Scan(&checked); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal(err)
	}
	if checked != nil {
		t.Fatalf("shard_key_checked_generation = %d, want NULL while a shard was never asked", *checked)
	}
}

func keyErrorOf(t *testing.T, conn *pgx.Conn, table string) string {
	t.Helper()
	var got *string
	if err := conn.QueryRow(context.Background(),
		`SELECT shard_key_error FROM pgshard.table_status WHERE database = 'app' AND schema_name = 'public' AND table_name = $1`, table).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got == nil {
		return ""
	}
	return *got
}

func keyTypeOf(t *testing.T, conn *pgx.Conn, table string) string {
	t.Helper()
	var got *string
	if err := conn.QueryRow(context.Background(),
		`SELECT shard_key_type FROM pgshard.table_status WHERE database = 'app' AND schema_name = 'public' AND table_name = $1`, table).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got == nil {
		return ""
	}
	return *got
}

// The router hashes the value the client sent and the shard stores what the
// column's type makes of it. For character varying(n) those differ -- an
// overlength value whose excess is spaces is silently truncated -- so the
// router needs the type, not just a verdict that the type is hashable.
func TestShardKeyCheckRecordsTheColumnType(t *testing.T) {
	ctx := context.Background()
	f := newPlacementFixture(t)
	check := &ShardKeyCheck{Pool: f.pool, Shards: f.placer.Shards, Logger: slog.New(slog.DiscardHandler)}
	for id := range 2 {
		mustExec(t, f.app(int32(id)), `CREATE TABLE codes (tenant_id character varying(8) NOT NULL, id bigint)`)
		mustExec(t, f.app(int32(id)), `CREATE TABLE freetext (tenant_id text NOT NULL, id bigint)`)
	}
	for _, name := range []string{"codes", "freetext", "notyetcreated"} {
		mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key)
			VALUES ('app', 'public', $1, 'sharded', 'tenant_id')`, name)
	}
	if _, err := check.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ table, want string }{
		{"codes", "character varying(8)"},
		{"freetext", "text"},
		// No shard has the table, so no shard reported a type; the router
		// normalises nothing, which is what it did before types existed.
		{"notyetcreated", ""},
	} {
		if got := keyTypeOf(t, f.catalog, c.table); got != c.want {
			t.Errorf("%s: shard_key_type = %q, want %q", c.table, got, c.want)
		}
		if got := keyErrorOf(t, f.catalog, c.table); got != "" {
			t.Errorf("%s: unexpected fault %q", c.table, got)
		}
	}
}

// Both lengths are hashable on their own, so checking each shard in
// isolation passes them both -- and then the router truncates by whichever
// length it happened to record while one shard stores something else.
func TestShardKeyCheckFaultsShardsThatDisagreeOnLength(t *testing.T) {
	ctx := context.Background()
	f := newPlacementFixture(t)
	check := &ShardKeyCheck{Pool: f.pool, Shards: f.placer.Shards, Logger: slog.New(slog.DiscardHandler)}
	mustExec(t, f.app(0), `CREATE TABLE widened (tenant_id character varying(8) NOT NULL, id bigint)`)
	mustExec(t, f.app(1), `CREATE TABLE widened (tenant_id character varying(16) NOT NULL, id bigint)`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key)
		VALUES ('app', 'public', 'widened', 'sharded', 'tenant_id')`)
	if _, err := check.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if got := keyErrorOf(t, f.catalog, "widened"); !strings.Contains(got, "must agree") {
		t.Fatalf("fault = %q, want the shards' disagreement on length to be faulted", got)
	}
	if got := keyTypeOf(t, f.catalog, "widened"); got != "" {
		t.Errorf("a faulted key must record no type to normalise by, got %q", got)
	}
}
