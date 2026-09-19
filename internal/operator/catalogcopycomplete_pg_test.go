package operator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestTheCatalogCopyIsNotCaughtUpWhileATableIsUncopied (PGS-947): the
// catch-up gate asked only the source -- no table-sync slots, no WAL lag. A
// subscription copies its tables a few at a time, and between batches
// neither is true of a table still waiting its turn, so the catalog was cut
// over with pgshard.shard_sets never copied and came up with no serving
// shard set.
//
// A table the target's subscription does not list is the same state held
// still: published, with no sync worker and no lag, and not copied.
func TestTheCatalogCopyIsNotCaughtUpWhileATableIsUncopied(t *testing.T) {
	ctx := context.Background()
	src, tgt := startCatalogPair(t, "ghcr.io/andrew01234567890/pgshard-postgres:18")
	for _, n := range []catalogNode{src, tgt} {
		conn := dialCatalog(t, n.side.DSN)
		err := catalog.Migrate(ctx, conn)
		_ = conn.Close(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}
	p := PgxProber{}
	if err := p.EnsureCatalogCopy(ctx, src.side, tgt.side); err != nil {
		t.Fatalf("ensure copy: %v", err)
	}
	caughtUp := func() (bool, string) {
		t.Helper()
		ok, lag, err := p.CatalogCopyCaughtUp(ctx, src.side.DSN, tgt.side.DSN)
		if err != nil {
			t.Fatal(err)
		}
		return ok, lag
	}
	for deadline := time.Now().Add(60 * time.Second); ; {
		if ok, _ := caughtUp(); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the copy never reported caught up, so the check below proves nothing")
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Published by FOR TABLES IN SCHEMA pgshard at once; the subscription
	// has never heard of it.
	execOn(t, src.side.DSN, `CREATE TABLE pgshard.pgs947_uncopied (id int PRIMARY KEY)`)
	execOn(t, tgt.side.DSN, `CREATE TABLE pgshard.pgs947_uncopied (id int PRIMARY KEY)`)
	// The CREATE TABLE is WAL the slot has to confirm, so a "behind" is
	// the lag and says nothing; wait until lag is not the reason.
	ok, why := caughtUp()
	for deadline := time.Now().Add(30 * time.Second); !ok && strings.Contains(why, "WAL behind") && time.Now().Before(deadline); {
		time.Sleep(200 * time.Millisecond)
		ok, why = caughtUp()
	}
	if ok {
		t.Fatal("the copy reported caught up with a published table the target never copied")
	}
	if !strings.Contains(why, "pgshard.pgs947_uncopied") {
		t.Errorf("the reason does not name the uncopied table: %q", why)
	}

	// The backstop after the fence: the cutover itself refuses.
	err := p.CutoverCatalog(ctx, src.side, tgt.side)
	if err == nil || !strings.Contains(err.Error(), "catalog copy incomplete") {
		t.Fatalf("the cutover onto an incomplete copy answered %v, want a refusal naming the incomplete copy", err)
	}
}
