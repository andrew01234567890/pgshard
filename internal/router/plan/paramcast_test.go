package plan

import (
	"context"
	"testing"
)

// A cast in the statement applies to a parameter whose type the client
// declared, not only to an undeclared one. `WHERE tenant_id = $1::int8`
// with $1 declared text and bound to '7' is the INTEGER 7 to PostgreSQL,
// and the router hashed the STRING "7" -- a different shard, and the row
// silently missing.
func TestACastAppliesToADeclaredParameterToo(t *testing.T) {
	snap := fixture(t)
	asInt := shardOf(t, snap, int64(7))
	asText := shardOf(t, snap, "7")
	if asInt == asText {
		t.Fatal("fixture no longer separates the integer 7 from the string \"7\"; the test proves nothing")
	}
	pl, err := New().Plan(context.Background(), session(snap), "select * from orders where tenant_id = $1::int8")
	if err != nil {
		t.Fatal(err)
	}
	got, err := pl.Resolve(BindParams{OIDs: []uint32{oidText}, Values: [][]byte{[]byte("7")}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Shards) != 1 || got.Shards[0] != asInt {
		t.Fatalf("routed to %v, want the shard of the integer 7 (%d)", got.Shards, asInt)
	}
}

// The other direction: an integer parameter cast to text compares as text
// on the shard, so it has to hash as text here.
func TestAnIntegerParameterCastToTextHashesAsText(t *testing.T) {
	snap := varcharFixture(t, "text")
	asText := shardOf(t, snap, "7")
	pl, err := New().Plan(context.Background(), session(snap), "select * from codes where code = $1::text")
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range []BindParams{
		{OIDs: []uint32{oidInt8}, Values: [][]byte{[]byte("7")}},
		{OIDs: []uint32{oidInt8}, Formats: []int16{1}, Values: [][]byte{be64(7)}},
	} {
		got, err := pl.Resolve(b)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Shards) != 1 || got.Shards[0] != asText {
			t.Fatalf("%v: routed to %v, want the shard of the string \"7\" (%d)", b.Formats, got.Shards, asText)
		}
	}
}
