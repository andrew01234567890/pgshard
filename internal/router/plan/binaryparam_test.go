package plan

import (
	"context"
	"testing"
)

// An undeclared BINARY parameter was read as an integer whenever its length
// happened to be 2, 4 or 8 bytes. A client that sends Parse/Bind/Execute
// with no parameter OIDs and no Describe -- which is legal -- binding the
// binary text 'abcd' against a text shard key therefore had four bytes read
// as an int32 and routed to a shard the row is not on. PostgreSQL infers
// the parameter's type from the comparison, so the key column's recorded
// type is what says which it is, and the router has it.
func TestAnUndeclaredBinaryParameterTakesTheKeyColumnsType(t *testing.T) {
	snap := varcharFixture(t, "text")
	asText := shardOf(t, snap, "abcd")
	asInt := shardOf(t, snap, int64(int32(0x61626364))) // 'abcd' read as an int32
	if asText == asInt {
		t.Fatal("fixture no longer separates the text from its bytes read as an integer; the test proves nothing")
	}
	pl, err := New().Plan(context.Background(), session(snap), "select * from codes where code = $1")
	if err != nil {
		t.Fatal(err)
	}
	got, err := pl.Resolve(BindParams{Formats: []int16{1}, Values: [][]byte{[]byte("abcd")}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Shards) != 1 || got.Shards[0] != asText {
		t.Fatalf("routed to %v, want the shard of the text \"abcd\" (%d)", got.Shards, asText)
	}
}

// The same inference the other way: an int8 key column reads an undeclared
// binary parameter as the integer it is, which is what it did before.
func TestAnUndeclaredBinaryParameterOnAnIntegerKeyStillDecodesAsOne(t *testing.T) {
	snap := fixture(t)
	pl, err := New().Plan(context.Background(), session(snap), "select * from orders where tenant_id = $1")
	if err != nil {
		t.Fatal(err)
	}
	got, err := pl.Resolve(BindParams{Formats: []int16{1}, Values: [][]byte{be64(7)}})
	if err != nil {
		t.Fatal(err)
	}
	if want := shardOf(t, snap, int64(7)); len(got.Shards) != 1 || got.Shards[0] != want {
		t.Fatalf("routed to %v, want the shard of the integer 7 (%d)", got.Shards, want)
	}
}

// The declared type still wins over the column: the column type is what
// PostgreSQL infers from when the client declared nothing, not something
// that overrides what it did declare. (An explicit cast wins too -- it is
// applied over the decoded value either way -- which its own tests cover.)
func TestTheColumnTypeOnlyTypesAnUndeclaredParameter(t *testing.T) {
	for _, c := range []struct {
		name       string
		oid        uint32
		hint       TypeHint
		columnType string
		raw        []byte
		want       any
	}{
		{"declared int8 over a text column", oidInt8, HintNone, "text", []byte("42"), int64(42)},
		{"column types what nothing else does", 0, HintNone, "text", []byte("42"), "42"},
		{"no column type leaves the old rules", 0, HintNone, "", []byte("42"), nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := BindParams{OIDs: []uint32{c.oid}, Values: [][]byte{c.raw}}
			got, err := b.ShardKey(1, c.hint, c.columnType)
			if c.want == nil {
				// '42' with nothing to type it is the ambiguous case, which
				// is refused rather than guessed.
				if err == nil {
					t.Fatalf("an untyped numeric string decoded as %#v", got)
				}
				return
			}
			if err != nil || got != c.want {
				t.Fatalf("got %#v, %v; want %#v", got, err, c.want)
			}
		})
	}
}

// The inferred type has to carry the column's WIDTH, not just its family: a
// binary parameter is decoded by the OID's own width, so mapping an int4
// column to int8 refused the four bytes PostgreSQL sends for it ("int8
// parameter has 4 bytes") -- a statement the old length guess routed
// correctly.
func TestAnUndeclaredBinaryParameterTakesTheKeyColumnsWidth(t *testing.T) {
	for _, c := range []struct {
		columnType string
		raw        []byte
	}{
		{"bigint", be64(7)},
		{"integer", be32(7)},
		{"int4", be32(7)},
		{"smallint", be16(7)},
		{"int2", be16(7)},
	} {
		b := BindParams{Formats: []int16{1}, Values: [][]byte{c.raw}}
		got, err := b.ShardKey(1, HintNone, c.columnType)
		if err != nil {
			t.Errorf("%s: %v", c.columnType, err)
			continue
		}
		if got != any(int64(7)) {
			t.Errorf("%s: decoded %#v, want the integer 7", c.columnType, got)
		}
	}
}
