package plan

import (
	"context"
	"strings"
	"testing"
)

// TestAlteringTheSequenceBehindAGlobalOneIsRefused: pgshard registers a
// global sequence as database.schema.table.column and PostgreSQL names the
// physical sequence a serial creates <table>_<column>_seq. They are the
// same sequence to a user and different objects here, so ALTER SEQUENCE
// tickets_id_seq was fanned out: every shard's own counter was reset and
// the counter the router allocates from carried on untouched, with the
// statement reporting success.
func TestAlteringTheSequenceBehindAGlobalOneIsRefused(t *testing.T) {
	for _, sql := range []string{
		"alter sequence tickets_id_seq restart with 1",
		"alter sequence public.tickets_id_seq increment by 10",
		"alter sequence tickets_id_seq restart",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := New().Plan(context.Background(), session(fixture(t)), sql)
			if err == nil {
				t.Fatal("fanned out to the per-shard sequences")
			}
			if !strings.Contains(err.Error(), "global sequence") {
				t.Fatalf("the refusal must say which sequence it is about: %v", err)
			}
			if !strings.Contains(err.Error(), "each shard's own counter") {
				t.Fatalf("the refusal must say what fanning it out would do: %v", err)
			}
		})
	}
}

// TestAnOrdinarySequenceIsStillAlterable: the refusal is for the objects
// behind registered global sequences, and refusing every ALTER SEQUENCE
// would take away a working operation to protect one that is not.
func TestAnOrdinarySequenceIsStillAlterable(t *testing.T) {
	for _, sql := range []string{
		"alter sequence some_other_seq restart with 1",
		"alter sequence public.unrelated_seq increment by 2",
		// A name that merely looks like one: no tickets.other column is
		// registered, so nothing about this is global.
		"alter sequence tickets_other_seq restart",
	} {
		t.Run(sql, func(t *testing.T) {
			if _, err := New().Plan(context.Background(), session(fixture(t)), sql); err != nil {
				t.Fatalf("an ordinary sequence must still be alterable: %v", err)
			}
		})
	}
}

// TestDestroyingTheSequenceBehindAGlobalOneIsRefused: ALTER SEQUENCE was
// guarded and the statements that DESTROY the same object, or move the name
// the guard is derived from, were not. A DROP takes the column's default
// with it under CASCADE; a RENAME or a SET SCHEMA leaves the next ALTER
// SEQUENCE matching nothing, so it is fanned out again with no one the
// wiser.
func TestDestroyingTheSequenceBehindAGlobalOneIsRefused(t *testing.T) {
	for _, sql := range []string{
		"drop sequence tickets_id_seq",
		"drop sequence public.tickets_id_seq cascade",
		"drop sequence if exists unrelated_seq, tickets_id_seq",
		"alter sequence tickets_id_seq rename to something_else",
		"alter sequence tickets_id_seq set schema other",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := New().Plan(context.Background(), session(fixture(t)), sql)
			if err == nil {
				t.Fatal("fanned out to the per-shard sequences")
			}
			if !strings.Contains(err.Error(), "global sequence "+fixtureDB+".public.tickets.id") {
				t.Fatalf("the refusal must name the global sequence: %v", err)
			}
		})
	}
}

// The other side: what is inert on the physical sequence stays allowed,
// because refusing it would break the migration tools that grant over every
// object they find and would protect nothing.
func TestInertStatementsOnAGlobalSequenceObjectAreAllowed(t *testing.T) {
	for _, sql := range []string{
		"grant select on sequence tickets_id_seq to reader",
		"alter sequence tickets_id_seq owner to someone",
		// And an ordinary sequence is untouched by any of it.
		"drop sequence unrelated_seq",
		"alter sequence unrelated_seq rename to other_seq",
		"drop sequence tickets_other_seq",
	} {
		t.Run(sql, func(t *testing.T) {
			if _, err := New().Plan(context.Background(), session(fixture(t)), sql); err != nil {
				t.Fatalf("must still be planned: %v", err)
			}
		})
	}
}
