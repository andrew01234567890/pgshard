package plan

import (
	"context"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestOnlyAProjectionOfOneTableIsRoutable.
//
// A view is routed by mapping its output columns back to the base table's,
// so the planner can see a shard key the view exposes under another name.
// That map only exists for the shape a versioned-schema migration tool
// produces: ONE base relation and a target list of plain column references.
// Everything else is recorded as opaque -- routed nowhere and refused --
// because guessing would produce the silently partial answers this whole
// area exists to stop.
func TestOnlyAProjectionOfOneTableIsRoutable(t *testing.T) {
	p := New()
	snap := fixture(t)
	for _, c := range []struct {
		sql   string
		shape string
		base  string
		cols  map[string]string
	}{
		{"create view v as select tenant_id, note from regions", catalog.ViewSimple, "regions",
			map[string]string{"tenant_id": "tenant_id", "note": "note"}},
		// The alias is the whole point: pgroll renames a column by
		// projecting the new physical one under the old name.
		{"create view v as select tenant_id as tenant, note as body from regions", catalog.ViewSimple, "regions",
			map[string]string{"tenant": "tenant_id", "body": "note"}},
		// SELECT * is a projection, but its columns are fixed by the base
		// table at creation time and the planner does not have that list
		// here, so it cannot build a map it can trust.
		{"create view v as select * from regions", catalog.ViewOpaque, "", nil},
		{"create view v as select count(*) from regions", catalog.ViewOpaque, "", nil},
		{"create view v as select tenant_id + 1 from regions", catalog.ViewOpaque, "", nil},
		{"create view v as select distinct tenant_id from regions", catalog.ViewOpaque, "", nil},
		{"create view v as select tenant_id from regions group by tenant_id", catalog.ViewOpaque, "", nil},
		{"create view v as select r.tenant_id from regions r, regions s", catalog.ViewOpaque, "", nil},
		{"create view v as select tenant_id from regions union select tenant_id from regions", catalog.ViewOpaque, "", nil},
	} {
		pl, err := p.Plan(context.Background(), session(snap), c.sql)
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		v := pl.Migration.View
		if v == nil {
			t.Fatalf("%s: no view recorded at all; the relation would look like an undeclared table", c.sql)
		}
		if v.Shape != c.shape || v.BaseName != c.base {
			t.Errorf("%s: shape %q base %q, want %q %q", c.sql, v.Shape, v.BaseName, c.shape, c.base)
			continue
		}
		if len(v.Columns) != len(c.cols) {
			t.Errorf("%s: columns %v want %v", c.sql, v.Columns, c.cols)
			continue
		}
		for out, base := range c.cols {
			if v.Columns[out] != base {
				t.Errorf("%s: column %q maps to %q, want %q", c.sql, out, v.Columns[out], base)
			}
		}
	}
}
