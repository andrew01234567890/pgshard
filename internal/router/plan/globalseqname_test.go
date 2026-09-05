package plan

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// TestTheSequenceFunctionsKnowThePhysicalName.
//
// A global sequence has two names: the registration
// (<database>.<schema>.<table>.<column>) and the object PostgreSQL creates
// for a serial (<table>_<column>_seq). They are the same sequence to a user
// and different objects here.
//
// The DDL guard learned the second name; the function guard did not, so the
// same operation was refused when written one way and forwarded to a shard
// when written the other. currval and setval read and wrote one shard's own
// counter, and SELECT nextval on the physical name handed the client a
// SHARD-LOCAL number as the next id -- which collides with the global
// counter as soon as the client inserts it.
func TestTheSequenceFunctionsKnowThePhysicalName(t *testing.T) {
	snap := fixture(t)
	p := New()
	ctx := context.Background()
	for _, c := range []struct{ sql, want string }{
		{"select currval('tickets_id_seq')", "currval() on the global sequence app.public.tickets.id"},
		{"select currval('public.tickets_id_seq')", "currval() on the global sequence app.public.tickets.id"},
		{"select setval('tickets_id_seq', 1)", "setval() on the global sequence app.public.tickets.id"},
		// pg_sequence_last_value is what pg_sequences is built on, so a tool
		// inspecting sequences reaches the wrong counter without ever
		// writing currval.
		{"select pg_sequence_last_value('tickets_id_seq')", "pg_sequence_last_value() on the global sequence app.public.tickets.id"},
		{"select pg_sequence_last_value('invoice_numbers')", "pg_sequence_last_value() on the global sequence invoice_numbers"},
	} {
		t.Run(c.sql, func(t *testing.T) {
			pl, err := p.Plan(ctx, session(snap), c.sql)
			if err == nil || pl.Kind != Refuse {
				t.Fatalf("expected a refusal, got %+v %v", pl, err)
			}
			var pe *pgwire.Error
			if !errors.As(err, &pe) || pe.Code != "0A000" {
				t.Fatalf("%v, want a 0A000 refusal", err)
			}
			if !strings.Contains(pe.Message, c.want) {
				t.Fatalf("message %q, want it to contain %q", pe.Message, c.want)
			}
		})
	}
}

// A SELECT nextval on the physical name is now ANSWERED from the global
// counter rather than refused, because that is what the user asked for: the
// next value of the sequence behind tickets.id. It used to go to one shard.
func TestNextvalOnThePhysicalNameAllocatesGlobally(t *testing.T) {
	snap := fixture(t)
	for _, sql := range []string{
		"select nextval('tickets_id_seq')",
		"select nextval('public.tickets_id_seq')",
	} {
		pl, err := New().Plan(context.Background(), session(snap), sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if pl.Kind != SessionLocal {
			t.Fatalf("%s: kind %v, want SessionLocal -- Unsharded means it went to one shard's own counter", sql, pl.Kind)
		}
	}
}

// A sequence is a relation, so reading it needs no function at all. On a
// global sequence that read returns the home shard's counter, reported as
// though it were the sequence's -- the same wrong answer currval() is
// refused for, reached the ordinary way a script inspects one.
func TestReadingAGlobalSequenceAsATableIsRefused(t *testing.T) {
	snap := fixture(t)
	for _, c := range []struct{ sql, want string }{
		{"select last_value from tickets_id_seq", "reading the global sequence app.public.tickets.id as a table"},
		{"select * from invoice_numbers", "reading the global sequence invoice_numbers as a table"},
		{"select last_value from public.tickets_id_seq", "reading the global sequence app.public.tickets.id as a table"},
	} {
		pl, err := New().Plan(context.Background(), session(snap), c.sql)
		if err == nil || pl.Kind != Refuse {
			t.Fatalf("%s: expected a refusal, got %+v %v", c.sql, pl, err)
		}
		var pe *pgwire.Error
		if !errors.As(err, &pe) || pe.Code != "0A000" || !strings.Contains(pe.Message, c.want) {
			t.Fatalf("%s: %v, want a 0A000 containing %q", c.sql, err, c.want)
		}
	}
}

// Everything above applies only to a sequence pgshard allocates from.
// Refusing an ordinary sequence would be refusing PostgreSQL.
func TestAnUnregisteredSequenceIsUntouchedByAllOfThis(t *testing.T) {
	snap := fixture(t)
	for _, sql := range []string{
		"select nextval('other_seq')",
		"select currval('other_seq')",
		"select setval('other_seq', 1)",
		"select pg_sequence_last_value('other_seq')",
		"select last_value from other_seq",
		"select * from other_seq",
		// A name that merely LOOKS derived, for a table that has no
		// registered sequence column.
		"select currval('orders_id_seq')",
		"select last_value from orders_id_seq",
	} {
		if _, err := New().Plan(context.Background(), session(snap), sql); err != nil {
			t.Errorf("%s: %v", sql, err)
		}
	}
}
