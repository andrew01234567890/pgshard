package router

import (
	"testing"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// TestRowValuesReadsBothRowShapes: a row arrives either packed, which is
// what the router asks for, or as a Value submessage per column, which is
// what a pooler that predates the request field answers with. Both have to
// tell an empty value from a NULL -- repeated bytes cannot, which is why
// the packed shape names its NULL columns separately.
func TestRowValuesReadsBothRowShapes(t *testing.T) {
	want := [][]byte{[]byte("a"), nil, {}}
	packed := &pgshardv1.DataRow{Packed: [][]byte{[]byte("a"), nil, {}}, Nulls: []uint32{1}}
	legacy := &pgshardv1.DataRow{Columns: []*pgshardv1.Value{{Data: []byte("a")}, {Null: true}, {Data: []byte{}}}}
	for name, row := range map[string]*pgshardv1.DataRow{"packed": packed, "legacy": legacy} {
		got := rowValues(row)
		if len(got) != len(want) {
			t.Fatalf("%s: %d columns, want %d", name, len(got), len(want))
		}
		for i := range want {
			if (got[i] == nil) != (want[i] == nil) || string(got[i]) != string(want[i]) {
				t.Errorf("%s: column %d = %q (nil %v), want %q (nil %v)", name, i, got[i], got[i] == nil, want[i], want[i] == nil)
			}
		}
	}
	// A row of no columns is legal and must not be mistaken for a shape.
	if got := rowValues(&pgshardv1.DataRow{}); len(got) != 0 {
		t.Errorf("empty row: %v", got)
	}
}
