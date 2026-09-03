package controller

import (
	"context"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/placement"
	"github.com/andrew01234567890/pgshard/internal/router/plan"
)

// TestABlankPaddedKeyHashesWhereItIsStored is the differential this
// re-enablement rests on. A character(n) column stores its value
// blank-padded, its equality ignores trailing spaces, and the ::text cast
// the row filter and the copy use strips them. So the shard hashes the
// trimmed value -- and the router has to hash the same one, or a client's
// 'abc' and the stored 'abc   ' are one key to PostgreSQL and two shards
// to pgshard.
//
// Asked of PostgreSQL rather than asserted: the whole hazard is a
// disagreement with PostgreSQL, so a test that only agrees with itself
// would prove nothing.
func TestABlankPaddedKeyHashesWhereItIsStored(t *testing.T) {
	parallelPG(t)
	dsn := startPostgres(t)
	ctx := context.Background()
	conn := connect(t, dsn)
	mustExec(t, conn, `CREATE TABLE padded (k character(8) PRIMARY KEY)`)

	expr, err := KeyHashExpr("k", "character(8)")
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"abc", "abc ", "abc     ", "a", "", "zz zz"} {
		mustExec(t, conn, `TRUNCATE padded`)
		mustExec(t, conn, `INSERT INTO padded VALUES ($1)`, raw)

		// What the shard would place the row by.
		var want int64
		if err := conn.QueryRow(ctx, `SELECT `+expr+` FROM padded`).Scan(&want); err != nil {
			t.Fatalf("%q: %v", raw, err)
		}

		// What the router hashes for a client sending exactly these bytes.
		norm := plan.NormaliseKeyForType(raw, "character(8)")
		got := placement.HashTextExtended(norm, placement.PartitionSeed)
		if got != want {
			t.Fatalf("key %q: router hashes %d, the shard places by %d (normalised to %q)", raw, got, want, norm)
		}
	}
}

// TestEveryBlankPaddedSpellingOfAKeyRoutesTogether: PostgreSQL calls these
// one key, so pgshard has to as well. Routing them apart is how a read
// misses a row that is there.
func TestEveryBlankPaddedSpellingOfAKeyRoutesTogether(t *testing.T) {
	seen := map[int64]bool{}
	for _, raw := range []string{"abc", "abc ", "abc   ", "abc     "} {
		norm := plan.NormaliseKeyForType(raw, "character(8)")
		seen[placement.HashTextExtended(norm, placement.PartitionSeed)] = true
	}
	if len(seen) != 1 {
		t.Fatalf("the same key spelled %d ways hashed to %d places", 4, len(seen))
	}
}
