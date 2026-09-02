package operator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestCutoverCarriesTheShardMapGeneration: the gate before the cutover asks
// only the source -- no table-sync workers, zero WAL lag. Both are true of a
// target that holds nothing, because EnsureCatalogCopy clears the target's
// pgshard schema and the copy is what puts it back.
//
// When it did not, the flip stranded every router: shard_map_generation is a
// singleton, an empty one makes catalog.Generations fail, and a router that
// cannot read the generation refuses to plan and leaves the Service. All
// three routers in one e2e run sat unable to load a snapshot while the
// cluster reported CatalogReady, and writes never resumed.
//
// Deleting the row on the target is a local delete the subscription does not
// undo, which is what makes the broken state reproducible here.
func TestCutoverCarriesTheShardMapGeneration(t *testing.T) {
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
	execOn(t, src.side.DSN, `INSERT INTO pgshard.databases (name) VALUES ('before-cutover')`)

	p := PgxProber{}
	if err := p.EnsureCatalogCopy(ctx, src.side, tgt.side); err != nil {
		t.Fatalf("ensure copy: %v", err)
	}
	waitCatalogRow(t, tgt.side.DSN, "before-cutover", "the copy never reached the target")
	for deadline := time.Now().Add(60 * time.Second); ; {
		ok, _, err := p.CatalogCopyCaughtUp(ctx, src.side.DSN)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the copy never reported caught up")
		}
		time.Sleep(200 * time.Millisecond)
	}

	// The generation the new catalog must come up with.
	execOn(t, src.side.DSN, `UPDATE pgshard.shard_map_generation SET generation = 7`)
	// The state the copy leaves behind: cleared by EnsureCatalogCopy and not
	// delivered. Only the schema migration ever inserts this row, so nothing
	// downstream repairs it.
	execOn(t, tgt.side.DSN, `DELETE FROM pgshard.shard_map_generation`)

	if err := p.CutoverCatalog(ctx, src.side, tgt.side); err != nil {
		t.Fatalf("cutover: %v", err)
	}
	if n := queryOn[int64](t, tgt.side.DSN, `SELECT count(*) FROM pgshard.shard_map_generation`); n != 1 {
		t.Fatalf("the new catalog has %d generation rows; every router would refuse to plan against it", n)
	}
	if got := queryOn[int64](t, tgt.side.DSN, `SELECT generation FROM pgshard.shard_map_generation`); got != 7 {
		t.Errorf("the new catalog came up at generation %d, not the %d the old one served", got, 7)
	}
}

// TestCutoverRefusesACatalogItCannotRepair: the repair reads the source, so a
// source that cannot answer leaves the target unreadable -- and the cutover
// must refuse rather than flip onto it. Belt and braces for the case the
// repair itself cannot cover.
func TestCutoverRefusesACatalogItCannotRepair(t *testing.T) {
	ctx := context.Background()
	node, _ := startCatalogPair(t, "ghcr.io/andrew01234567890/pgshard-postgres:18")
	conn := dialCatalog(t, node.side.DSN)
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `DELETE FROM pgshard.shard_map_generation`); err != nil {
		t.Fatal(err)
	}
	err := catalogTargetIsReadable(ctx, conn)
	if err == nil {
		t.Fatal("an unreadable catalog passed the cutover gate")
	}
	if !strings.Contains(err.Error(), "not readable") || !strings.Contains(err.Error(), "shard_map_generation") {
		t.Fatalf("error %q does not say the target is unreadable and why", err)
	}
}
