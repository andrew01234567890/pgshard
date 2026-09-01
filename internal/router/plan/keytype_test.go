package plan

import (
	"context"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
)

func varcharFixture(t testing.TB, columnType string) *snapshot.Snapshot {
	t.Helper()
	s := fixture(t)
	s.Tables[snapshot.TableKey{Database: fixtureDB, SchemaName: "public", TableName: "codes"}] =
		snapshot.Placement{Placement: "sharded", ShardKey: "code", Generation: 3, ShardKeyType: columnType}
	return s
}

func routeOf(t testing.TB, snap *snapshot.Snapshot, sql string) []int32 {
	t.Helper()
	p, err := New().Plan(context.Background(), session(snap), sql)
	if err != nil {
		t.Fatalf("plan %s: %v", sql, err)
	}
	return p.Shards
}

func TestOverlengthVarcharKeyRoutesWhereItIsStored(t *testing.T) {
	// 'abc' and 'abc   ' hash to different shards, so a router that skips
	// the truncation writes the row to one shard and looks for it on
	// another. PostgreSQL stores both as 'abc' in a varchar(3).
	plain := shardOf(t, fixture(t), "abc")
	padded := shardOf(t, fixture(t), "abc   ")
	if plain == padded {
		t.Fatal("fixture no longer separates the trimmed and padded values; the test proves nothing")
	}

	snap := varcharFixture(t, "character varying(3)")
	got := routeOf(t, snap, "SELECT * FROM codes WHERE code = 'abc   '")
	if len(got) != 1 || got[0] != plain {
		t.Errorf("an overlength key whose excess is spaces must route where PostgreSQL stores it: got %v, want [%d]", got, plain)
	}
	if got := routeOf(t, snap, "SELECT * FROM codes WHERE code = 'abc'"); len(got) != 1 || got[0] != plain {
		t.Errorf("the trimmed key must route to the same shard: got %v, want [%d]", got, plain)
	}
}

func TestKeyNormalisationLeavesEverythingElseAlone(t *testing.T) {
	overlong := shardOf(t, fixture(t), "abcd")
	if got := routeOf(t, varcharFixture(t, "character varying(3)"), "SELECT * FROM codes WHERE code = 'abcd'"); len(got) != 1 || got[0] != overlong {
		// PostgreSQL rejects this value rather than truncating it, so the
		// router must not invent a truncation of its own.
		t.Errorf("an overlength key whose excess is not spaces must not be truncated: got %v, want [%d]", got, overlong)
	}
	padded := shardOf(t, fixture(t), "abc   ")
	for _, typ := range []string{"", "text", "character varying"} {
		if got := routeOf(t, varcharFixture(t, typ), "SELECT * FROM codes WHERE code = 'abc   '"); len(got) != 1 || got[0] != padded {
			t.Errorf("type %q has no length to truncate to: got %v, want [%d]", typ, got, padded)
		}
	}
}

func TestParseCharType(t *testing.T) {
	for _, c := range []struct {
		in    string
		base  string
		n     int
		limit bool
	}{
		{"character varying(8)", "character varying", 8, true},
		{"CHARACTER VARYING(8)", "character varying", 8, true},
		{"character varying", "character varying", 0, false},
		{"text", "text", 0, false},
		{"numeric(10,2)", "numeric", 0, false},
	} {
		base, n, limit := parseCharType(c.in)
		if base != c.base || n != c.n || limit != c.limit {
			t.Errorf("%q: got (%q,%d,%t), want (%q,%d,%t)", c.in, base, n, limit, c.base, c.n, c.limit)
		}
	}
}

func TestOverlengthVarcharKeyRoutesTheSameWhenBound(t *testing.T) {
	// The common case is a bound parameter, which routes at Resolve rather
	// than at plan time and reaches the truncation by a different path.
	plain := shardOf(t, fixture(t), "abc")
	snap := varcharFixture(t, "character varying(3)")

	pl, err := New().Plan(context.Background(), session(snap), "SELECT * FROM codes WHERE code = $1::text")
	if err != nil {
		t.Fatal(err)
	}
	if !pl.Deferred {
		t.Fatal("a parameterised key must defer to Resolve")
	}
	got, err := pl.Resolve(staticParams{1: "abc   "})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Shards) != 1 || got.Shards[0] != plain {
		t.Errorf("a bound overlength key must route where PostgreSQL stores it: got %v, want [%d]", got.Shards, plain)
	}
}
