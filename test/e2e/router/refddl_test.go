//go:build integration

package router

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestReferenceDDLCannotSetAPerShardDefault: a reference table is the same
// rows on every shard. The write path refuses `INSERT ... now()` for that
// reason; the DDL path did not, and it is the worse of the two -- ADD COLUMN
// with a DEFAULT gives every EXISTING row a value, so one ordinary statement
// diverged the whole table at once.
//
// Measured before the fix, two shards holding the same three rows:
//
//	alter table regions add column u uuid default gen_random_uuid()   -> no error
//	shard0: 46ac952b… 8f35e059… 9273d587…
//	shard1: 23b5e784… 426e79d4… ec6ae563…
func TestReferenceDDLCannotSetAPerShardDefault(t *testing.T) {
	opts := []string{"-c", "max_prepared_transactions=20"}
	s := startShardedStackFull(t, opts, opts, opts)
	s.declareReferenceAndSequences(t)
	ctx := context.Background()
	conn := s.connect(t)
	s.awaitReference(t, conn)
	for shard := range 2 {
		c, err := pgx.Connect(ctx, s.appDSN(shard))
		if err != nil {
			t.Fatal(err)
		}
		// ALTER TABLE needs ownership, not privileges: without this the
		// statement fails on the shard with 42501 and never reaches the
		// rule under test.
		if _, err := c.Exec(ctx, `alter table regions owner to `+appRole); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Exec(ctx, `delete from regions`); err != nil {
			t.Fatal(err)
		}
		for id := 1; id <= 3; id++ {
			if _, err := c.Exec(ctx, fmt.Sprintf(`insert into regions (id, name) values (%d, 'r%d')`, id, id)); err != nil {
				t.Fatal(err)
			}
		}
		_ = c.Close(ctx)
	}

	for _, sql := range []string{
		`alter table regions add column made timestamptz default clock_timestamp()`,
		`alter table regions add column r double precision default random()`,
		`alter table regions add column u uuid default gen_random_uuid()`,
		// PostgreSQL evaluates this ONCE for the ALTER and stores a
		// metadata-only default -- once per shard, so the stored constants
		// still differ.
		`alter table regions add column seen timestamptz default now()`,
		`alter table regions alter column name set default random()::text`,
	} {
		_, err := conn.Exec(ctx, sql, pgx.QueryExecModeSimpleProtocol)
		if sqlstate(err) != "0A000" || !strings.Contains(err.Error(), "cannot call") {
			t.Errorf("%s: got %v, want a 0A000 naming the call", sql, err)
		}
	}

	// A constant default is the same everywhere and still works, on every
	// shard -- so the rule refuses what diverges and not the feature.
	if _, err := conn.Exec(ctx, `alter table regions add column flag boolean default true`, pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatalf("a constant default must still be allowed: %v", err)
	}
	for shard := range 2 {
		c, err := pgx.Connect(ctx, s.appDSN(shard))
		if err != nil {
			t.Fatal(err)
		}
		var got string
		if err := c.QueryRow(ctx, `select coalesce(string_agg(distinct flag::text, '|'), '') from regions`).Scan(&got); err != nil {
			t.Fatal(err)
		}
		_ = c.Close(ctx)
		if got != "true" {
			t.Errorf("shard %d: flag = %q, want every row true", shard, got)
		}
	}
}
