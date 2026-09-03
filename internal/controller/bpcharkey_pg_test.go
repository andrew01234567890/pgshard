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

// TestAnOverlongVarcharKeyHashesWhereItIsStored is the other half of the
// same hazard, and the one that is easy to disbelieve. A value too long
// for a character varying(n) is an error -- unless every character past
// the limit is a space, in which case PostgreSQL silently truncates and
// stores the short value. The client sent bytes the shard did not keep,
// so the router has to hash what was kept.
//
// Asked of PostgreSQL for the same reason as above: the router agreeing
// with the router proves nothing about the value a shard placed a row by.
func TestAnOverlongVarcharKeyHashesWhereItIsStored(t *testing.T) {
	parallelPG(t)
	dsn := startPostgres(t)
	ctx := context.Background()
	conn := connect(t, dsn)
	mustExec(t, conn, `CREATE TABLE codes (k character varying(3) PRIMARY KEY)`)

	expr, err := KeyHashExpr("k", "character varying(3)")
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"abc", "abc ", "abc   ", "ab", ""} {
		mustExec(t, conn, `TRUNCATE codes`)
		mustExec(t, conn, `INSERT INTO codes VALUES ($1)`, raw)

		var want int64
		if err := conn.QueryRow(ctx, `SELECT `+expr+` FROM codes`).Scan(&want); err != nil {
			t.Fatalf("%q: %v", raw, err)
		}

		norm := plan.NormaliseKeyForType(raw, "character varying(3)")
		got := placement.HashTextExtended(norm, placement.PartitionSeed)
		if got != want {
			t.Fatalf("key %q: router hashes %d, the shard places by %d (normalised to %q)", raw, got, want, norm)
		}
	}
}

// TestAnOverlongNonSpaceVarcharKeyIsPostgreSQLsErrorToGive: the truncation
// is only for trailing spaces. Anything else is an error, and the router
// must not quietly shorten a value PostgreSQL would refuse -- that would
// route a statement the shard was never going to accept.
func TestAnOverlongNonSpaceVarcharKeyIsPostgreSQLsErrorToGive(t *testing.T) {
	parallelPG(t)
	dsn := startPostgres(t)
	conn := connect(t, dsn)
	mustExec(t, conn, `CREATE TABLE codes (k character varying(3) PRIMARY KEY)`)

	if _, err := conn.Exec(context.Background(), `INSERT INTO codes VALUES ($1)`, "abcd"); err == nil {
		t.Fatal("PostgreSQL accepted a value too long for varchar(3); this test's premise is wrong")
	}
	if norm := plan.NormaliseKeyForType("abcd", "character varying(3)"); norm != "abcd" {
		t.Fatalf("the router shortened %q to %q, hiding an error PostgreSQL owns", "abcd", norm)
	}
}
