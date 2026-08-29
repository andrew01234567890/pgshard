package router

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestDecisionLogAgainstPostgres exercises the real decision log, which
// writes its transaction as one pipelined round trip. The pipeline still
// has to behave like the explicit transaction it replaced: the statements
// run in order, a failure aborts the whole thing, and the row a caller is
// told about is the row that is there.
func TestDecisionLogAgainstPostgres(t *testing.T) {
	dsn := startCatalogContainer(t, "ghcr.io/andrew01234567890/pgshard-postgres:18")
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	l := &PGDecisionLog{Pool: pool}

	state := func(gid string) string {
		var s string
		if err := pool.QueryRow(ctx, `SELECT coalesce(max(state), 'absent') FROM pgshard.xact_decisions WHERE gid = $1`, gid).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}

	if err := l.Begin(ctx, "g1", []int32{0, 1}, []string{"1", "2"}); err != nil {
		t.Fatal(err)
	}
	if got := state("g1"); got != "preparing" {
		t.Fatalf("after Begin: %q", got)
	}

	// A second Begin for the same gid violates the primary key: the error
	// has to surface, and nothing may be left half-written.
	err = l.Begin(ctx, "g1", []int32{0}, []string{"3"})
	if err == nil {
		t.Fatal("a duplicate gid must fail")
	}
	if !strings.Contains(err.Error(), "duplicate key") {
		t.Errorf("error %v", err)
	}
	if got := state("g1"); got != "preparing" {
		t.Fatalf("the failed write changed the row: %q", got)
	}

	ok, err := l.Commit(ctx, "g1")
	if err != nil || !ok {
		t.Fatalf("Commit: %v %v", ok, err)
	}
	if got := state("g1"); got != "commit" {
		t.Fatalf("after Commit: %q", got)
	}
	// Deciding twice is not an error, but it is not a decision either.
	if ok, err := l.Commit(ctx, "g1"); err != nil || ok {
		t.Fatalf("second Commit: %v %v", ok, err)
	}
	// Aborting something already decided commit is refused rather than
	// quietly ignored: the two answers are not interchangeable.
	if err := l.Abort(ctx, "g1"); err == nil || !strings.Contains(err.Error(), "already decided commit") {
		t.Fatalf("Abort after commit: %v", err)
	}
	if got := state("g1"); got != "commit" {
		t.Fatalf("a decided row was changed: %q", got)
	}

	// The connection is usable after a failed batch: an aborted pipeline
	// that left the session in a bad state would strand the pool.
	if err := l.Begin(ctx, "g2", []int32{0}, []string{"4"}); err != nil {
		t.Fatalf("the pool was left unusable by the failed write: %v", err)
	}
	if got := state("g2"); got != "preparing" {
		t.Fatalf("after the second Begin: %q", got)
	}
}
