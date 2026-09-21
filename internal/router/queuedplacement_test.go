package router

import (
	"context"
	"slices"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestAQueuedMigrationCarriesThePlacementsItWasPlannedUnder (PGS-971): the
// applier can only refuse a migration whose placement moved while it waited
// in the queue if the router recorded that placement when it queued it. A
// scope of "home" for items and "all" for orders was decided from these; so
// was the view's scope, from the placement of the table under it.
func TestAQueuedMigrationCarriesThePlacementsItWasPlannedUnder(t *testing.T) {
	q := &fakeQueue{}
	h := newDDLHarness(t, q)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())

	for _, c := range []struct {
		sql  string
		want []catalog.TablePlacement
	}{
		{"alter table items add column note text", []catalog.TablePlacement{{Schema: "public", Table: "items", Placement: "unsharded"}}},
		{"alter table orders add column note text", []catalog.TablePlacement{{Schema: "public", Table: "orders", Placement: "sharded"}}},
		{"create index on regions (id)", []catalog.TablePlacement{{Schema: "public", Table: "regions", Placement: "reference"}}},
		// Undeclared: the database default is the placement it was planned
		// under, and declaring the table is the change the applier must see.
		{"alter table ledger add column note text", []catalog.TablePlacement{{Schema: "public", Table: "ledger", Placement: "unsharded"}}},
		{"create view orders_v as select tenant_id, id from orders", []catalog.TablePlacement{{Schema: "public", Table: "orders", Placement: "sharded"}}},
	} {
		if _, err := conn.Exec(ctx, c.sql); err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		if got := q.last(t).Meta.Placements; !slices.Equal(got, c.want) {
			t.Errorf("%s queued with placements %+v, want %+v", c.sql, got, c.want)
		}
	}
}
