//go:build integration

package router

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestAReferenceWriteCannotNameASystemColumn: a reference table is the same
// rows on every shard, and a write to one must mean the same thing on each.
// A system column breaks that in a way the volatility rule cannot see --
// ctid is a column, not a call -- and the failure is silent: PostgreSQL
// accepts the statement on every shard and each deletes whatever happens to
// sit at that physical location.
//
// Measured before the fix, with the same three rows inserted in a different
// order on each shard:
//
//	delete from regions where ctid = '(0,1)'  ->  no error
//	shard0: 1=r1,2=r2,3=r3     shard1: 1=r1,2=r2
func TestAReferenceWriteCannotNameASystemColumn(t *testing.T) {
	opts := []string{"-c", "max_prepared_transactions=20"}
	s := startShardedStackFull(t, opts, opts, opts)
	s.declareReferenceAndSequences(t)
	ctx := context.Background()
	conn := s.connect(t)
	s.awaitReference(t, conn)

	// The same rows on both shards, in a different physical order, so a
	// ctid names a different row on each.
	for shard := range 2 {
		c, err := pgx.Connect(ctx, s.appDSN(shard))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Exec(ctx, `delete from regions`); err != nil {
			t.Fatal(err)
		}
		order := []int{1, 2, 3}
		if shard == 1 {
			order = []int{3, 2, 1}
		}
		for _, id := range order {
			if _, err := c.Exec(ctx, fmt.Sprintf(`insert into regions (id, name) values (%d, 'r%d')`, id, id)); err != nil {
				t.Fatal(err)
			}
		}
		_ = c.Close(ctx)
	}

	for _, sql := range []string{
		`delete from regions where ctid = '(0,1)'`,
		`update regions set name = 'moved' where ctid = '(0,2)'`,
		`delete from regions where xmin::text <> '0'`,
		`update regions set name = tableoid::text`,
	} {
		_, err := conn.Exec(ctx, sql, pgx.QueryExecModeSimpleProtocol)
		if sqlstate(err) != "0A000" || !strings.Contains(err.Error(), "system column") {
			t.Errorf("%s: got %v, want a 0A000 naming the system column", sql, err)
		}
	}

	// Nothing was written, so the shards still agree. This is the assertion
	// the refusal exists for: without it the first statement above left
	// them holding different rows.
	var first string
	for shard := range 2 {
		c, err := pgx.Connect(ctx, s.appDSN(shard))
		if err != nil {
			t.Fatal(err)
		}
		var got string
		if err := c.QueryRow(ctx, `select coalesce(string_agg(id::text || '=' || coalesce(name,'-'), ',' order by id), '') from regions`).Scan(&got); err != nil {
			t.Fatal(err)
		}
		_ = c.Close(ctx)
		if shard == 0 {
			first = got
			continue
		}
		if got != first {
			t.Fatalf("the shards disagree after refused writes: shard0 %q, shard1 %q", first, got)
		}
	}
	if first == "" {
		t.Fatal("the fixture must leave rows behind, or the comparison proves nothing")
	}
}
