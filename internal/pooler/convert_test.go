package pooler

import "testing"

// TestDataRowPacksOnlyWhenAsked: the packed shape carries the same
// columns without an object per column, but it can only tell an empty
// value from a NULL because it names the NULL columns separately. A
// pooler must not pack unless the router asked, or a router that predates
// the shape reads a row with no columns at all.
func TestDataRowPacksOnlyWhenAsked(t *testing.T) {
	values := [][]byte{[]byte("a"), nil, {}}

	legacy := dataRow(values, false)
	if len(legacy.Packed) != 0 || len(legacy.Nulls) != 0 {
		t.Errorf("unasked row packed: %+v", legacy)
	}
	if len(legacy.Columns) != 3 || !legacy.Columns[1].Null || string(legacy.Columns[0].Data) != "a" {
		t.Errorf("columns = %+v", legacy.Columns)
	}
	if legacy.Columns[2].Null || len(legacy.Columns[2].Data) != 0 {
		t.Errorf("an empty value became NULL: %+v", legacy.Columns[2])
	}

	packed := dataRow(values, true)
	if len(packed.Columns) != 0 {
		t.Errorf("packed row also sent submessages: %+v", packed.Columns)
	}
	if len(packed.Packed) != 3 || string(packed.Packed[0]) != "a" {
		t.Errorf("packed = %+v", packed.Packed)
	}
	if len(packed.Nulls) != 1 || packed.Nulls[0] != 1 {
		t.Errorf("nulls = %v, want just the NULL column", packed.Nulls)
	}
	if packed.Packed[2] == nil {
		t.Error("the empty value must stay an empty value, not become the NULL it is not")
	}
}
