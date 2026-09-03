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
