package plan

import (
	"context"
	"strings"
	"testing"
)

// A global sequence is a catalog counter. currval was already refused
// because it reads one shard's own sequence object instead; nextval was
// not, and nextval allocates -- so two shards handed out the same numbers
// from a sequence declared global, and the duplicates surfaced as a
// primary key violation or as two rows sharing an id that could not.
func TestUnclaimedNextvalOnAGlobalSequenceIsRefused(t *testing.T) {
	snap := fixture(t)
	for _, sql := range []string{
		"select nextval('invoice_numbers') + 1",
		"select nextval('invoice_numbers'), 1",
		"update orders set amount = nextval('invoice_numbers') where tenant_id = 1",
		"select nextval('invoice_numbers') from orders where tenant_id = 1",
		"insert into orders (tenant_id, id, amount) select 1, 2, nextval('invoice_numbers')",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := New().Plan(context.Background(), session(snap), sql)
			if err == nil || !strings.Contains(err.Error(), "nextval() on the global sequence invoice_numbers") {
				t.Fatalf("want a refusal naming the global sequence, got %v", err)
			}
			if !strings.HasPrefix(err.Error(), "0A000") {
				t.Errorf("want 0A000, got %v", err)
			}
		})
	}
}

// The two shapes the router allocates for itself must keep working, and a
// sequence that is not global still means on a shard what it says.
func TestTheClaimedNextvalShapesStillWork(t *testing.T) {
	snap := fixture(t)
	p, err := New().Plan(context.Background(), session(snap), "select nextval('invoice_numbers')")
	if err != nil || p.NextVal != "invoice_numbers" || p.Kind != SessionLocal {
		t.Fatalf("the router must answer this one itself: kind=%v nextval=%q err=%v", p.Kind, p.NextVal, err)
	}
	if _, err := New().Plan(context.Background(), session(snap), "insert into tickets (tenant_id, id) values (1, nextval('invoice_numbers'))"); err != nil {
		t.Errorf("a sequence column of an INSERT is filled by the router: %v", err)
	}
	if _, err := New().Plan(context.Background(), session(snap), "select nextval('local_thing') + 1"); err != nil {
		t.Errorf("an unregistered sequence lives on a shard and means there what it says: %v", err)
	}
}
