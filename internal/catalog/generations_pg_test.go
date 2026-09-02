package catalog

import (
	"context"
	"strings"
	"testing"
)

// TestGenerationsNamesAnEmptySingleton: shard_map_generation is seeded by the
// schema migration, so an empty one is not "no generation set" -- it is a
// catalog the upgrade copy has cleared and not yet refilled. The scalar
// subquery then returns NULL and pgx reports "cannot scan NULL into *int64",
// which names neither the table nor that state.
//
// Three routers logged exactly that line for ten minutes during a catalog
// upgrade under chaos, with nothing in it to act on. The error has to carry
// the diagnosis, and it still has to BE an error: a router that cannot read
// the generation stamps every request with zero and must not serve.
func TestGenerationsNamesAnEmptySingleton(t *testing.T) {
	requireDocker(t)
	ctx := context.Background()
	conn := connect(t, startPostgres(t, candidateImages[0]))
	if err := Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Generations(ctx, conn); err != nil {
		t.Fatalf("a migrated catalog has its singleton row: %v", err)
	}
	if _, err := conn.Exec(ctx, `DELETE FROM pgshard.shard_map_generation`); err != nil {
		t.Fatal(err)
	}
	_, _, err := Generations(ctx, conn)
	if err == nil {
		t.Fatal("an empty singleton returned no error; a router would have served with generation 0")
	}
	for _, want := range []string{"pgshard.shard_map_generation", "no row", "copy"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not say %q; the raw pgx message was the whole problem", err, want)
		}
	}
}
