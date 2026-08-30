package operator

import (
	"context"
	"os/exec"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/dockertest"
)

// TestCatalogCarryHasTheSameSequenceSemanticsAsAShardSet: the catalog
// upgrade used to copy raw last_value into an unconditional setval. That
// gave the new catalog no headroom for values a session cache or the WAL
// pre-log window had already handed out on the old one, and let a second
// carry -- the rollback path runs one -- drag a sequence backwards over
// values the new catalog had itself handed out. Both are the same duplicate
// key the shard-set carry has always guarded against.
func TestCatalogCarryHasTheSameSequenceSemanticsAsAShardSet(t *testing.T) {
	if err := exec.Command("docker", "info").Run(); err != nil {
		dockertest.Unavailable(t, "docker unavailable")
	}
	ctx := context.Background()
	from, err := pgx.Connect(ctx, startProbePostgres(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = from.Close(ctx) }()
	to, err := pgx.Connect(ctx, startProbePostgres(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = to.Close(ctx) }()

	for _, conn := range []*pgx.Conn{from, to} {
		mustProbeExec(t, conn, `CREATE SCHEMA pgshard`)
		mustProbeExec(t, conn, `CREATE SEQUENCE pgshard.ids CACHE 50`)
		mustProbeExec(t, conn, `CREATE SEQUENCE public.down_seq INCREMENT -3 START -10 MINVALUE -1000000 MAXVALUE -1`)
	}
	for _, seq := range []string{"pgshard.ids", "public.down_seq"} {
		if _, err := from.Exec(ctx, `SELECT nextval($1::regclass)`, seq); err != nil {
			t.Fatal(err)
		}
	}
	var last int64
	if err := from.QueryRow(ctx, `SELECT last_value FROM pg_sequences WHERE schemaname = 'pgshard' AND sequencename = 'ids'`).Scan(&last); err != nil {
		t.Fatal(err)
	}

	if err := carrySequences(ctx, from, to); err != nil {
		t.Fatal(err)
	}
	var next int64
	if err := to.QueryRow(ctx, `SELECT nextval('pgshard.ids')`).Scan(&next); err != nil {
		t.Fatal(err)
	}
	if next <= last+50 {
		t.Fatalf("the new catalog hands out %d, which the old one may already have cached past %d", next, last)
	}
	if err := to.QueryRow(ctx, `SELECT nextval('public.down_seq')`).Scan(&next); err != nil {
		t.Fatal(err)
	}
	if next >= -10-32*3 {
		t.Fatalf("a descending sequence carried to %d, want past its headroom below %d", next, -10-32*3)
	}

	// A rollback carries a second time, by which point the side about to
	// serve may be ahead of the side being read.
	mustProbeExec(t, to, `SELECT setval('pgshard.ids', 900000, true)`)
	if err := carrySequences(ctx, from, to); err != nil {
		t.Fatal(err)
	}
	if err := to.QueryRow(ctx, `SELECT nextval('pgshard.ids')`).Scan(&next); err != nil {
		t.Fatal(err)
	}
	if next <= 900000 {
		t.Fatalf("the carry moved a live sequence back to %d; it was already at 900000", next)
	}
}
