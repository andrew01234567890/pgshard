package catalog

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestTheRolesGenerationAdvancesInCommitOrder: the generation is a
// watermark -- RoleVerifier.MaterializeStale skips any group recorded at or
// above it -- and a watermark only works if it is monotone with respect to
// COMMIT order.
//
// A sequence is not. desired_generation was stamped by nextval in a BEFORE
// ROW trigger, so a writer took its number when it wrote and not when it
// committed: two overlapping writers commit out of stamp order, a reader
// between the two commits records a group at the higher number, and the
// lower-numbered row never exceeds it. Not late -- never, until an
// unrelated write moves the number (PGS-813).
//
// Since 0053 an AFTER STATEMENT trigger on each of the four tables bumps
// one shared row, so the second writer waits on that row lock until the
// first commits. The lock is the fix; the counter is only what it is on.
// This asserts the property it buys: what a reader sees while a writer is
// open is a generation that writer's commit will exceed.
func TestTheRolesGenerationAdvancesInCommitOrder(t *testing.T) {
	requireDocker(t)
	ctx := context.Background()
	dsn := startPostgres(t, candidateImages[0])
	first := connect(t, dsn)
	if err := Migrate(ctx, first); err != nil {
		t.Fatal(err)
	}
	second, reader := connect(t, dsn), connect(t, dsn)

	gen := func() int64 {
		t.Helper()
		rows, err := reader.Query(ctx, `SELECT pgshard.roles_desired_generation()`)
		if err != nil {
			t.Fatal(err)
		}
		g, err := pgx.CollectOneRow(rows, pgx.RowTo[int64])
		if err != nil {
			t.Fatal(err)
		}
		return g
	}

	before := gen()

	// W1 writes and holds. Under the old mechanism it had already taken its
	// number here, and W2 was free to take the next one and commit first.
	tx1, err := first.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx1.Rollback(ctx) }()
	if _, err := tx1.Exec(ctx, `INSERT INTO pgshard.roles (rolname, login) VALUES ('w1', true)`); err != nil {
		t.Fatal(err)
	}

	if got := gen(); got != before {
		t.Fatalf("generation moved to %d while W1 was still open; an uncommitted write must not be visible", got)
	}

	done := make(chan error, 1)
	go func() {
		_, err := second.Exec(ctx, `INSERT INTO pgshard.roles (rolname, login) VALUES ('w2', true)`)
		done <- err
	}()

	waitForLockWaiter(ctx, t, reader)
	select {
	case err := <-done:
		t.Fatalf("W2 committed while W1 held the generation (%v); the bump did not serialise, so the two can still commit out of order", err)
	default:
	}

	// A group materialized from what the reader saw is recorded at `before`.
	recorded := gen()
	if recorded != before {
		t.Fatalf("generation %d, not %d: W2 got past W1", recorded, before)
	}

	if err := tx1.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	if after := gen(); after <= recorded {
		t.Fatalf("generation %d after both writers committed, but a group was recorded at %d: "+
			"MaterializeStale skips it and neither row is ever materialized on it", after, recorded)
	}

	// The old mechanism, still stamped on the same rows, shows the bug the
	// singleton replaced: W1 wrote first so it carries the LOWER number,
	// even though it committed second.
	var w1, w2 int64
	if err := first.QueryRow(ctx, `SELECT
		(SELECT desired_generation FROM pgshard.roles WHERE rolname = 'w1'),
		(SELECT desired_generation FROM pgshard.roles WHERE rolname = 'w2')`).Scan(&w1, &w2); err != nil {
		t.Fatal(err)
	}
	if w1 >= w2 {
		t.Skipf("W1 stamped %d and W2 %d, so this run did not reproduce the out-of-order stamp; "+
			"the singleton is still what makes the generation safe", w1, w2)
	}
	t.Logf("as before: W1 stamped %d and committed second, W2 stamped %d and committed first -- "+
		"the max over stamps would have been %d, which W1 never exceeds", w1, w2, w2)
}

// waitForLockWaiter blocks until one backend is waiting on a lock, which is
// what makes the serialisation assertion deterministic rather than a race
// against the goroutine getting started.
func waitForLockWaiter(ctx context.Context, t *testing.T, conn Querier) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		rows, err := conn.Query(ctx, `SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'`)
		if err != nil {
			t.Fatal(err)
		}
		n, err := pgx.CollectOneRow(rows, pgx.RowTo[int64])
		if err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no backend ever waited on a lock: W2 was not blocked by W1, so the generation bump does not serialise")
}
