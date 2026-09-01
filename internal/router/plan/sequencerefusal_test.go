package plan

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// TestASequenceAnswerIsNotInvented: a global sequence is a catalog counter
// the router allocates from. The per-shard sequence objects the DDL fanned
// out are not it, so currval and setval naming a global sequence would read
// or write an unrelated physical counter -- an answer that looks ordinary
// and is about a different sequence. lastval has no name at all, so the
// router cannot even tell which sequence is meant.
func TestASequenceAnswerIsNotInvented(t *testing.T) {
	snap := fixture(t)
	p := New()
	ctx := context.Background()
	for _, c := range []struct {
		sql  string
		want string
	}{
		{"select currval('invoice_numbers')", "currval() on the global sequence"},
		{"select setval('invoice_numbers', 42)", "setval() on the global sequence"},
		{"select lastval()", "lastval() is not available"},
		// Buried in a larger statement, which is where a walk earns its keep.
		{"select * from orders where id = currval('invoice_numbers')", "currval() on the global sequence"},
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
			if pe.Hint == "" {
				t.Fatal("a refusal has to say what to do instead")
			}
		})
	}
}

// TestAnOrdinarySequenceIsLeftAlone: a sequence that is not registered as
// global lives on one shard and means there exactly what it says. Refusing
// those too would be refusing PostgreSQL.
func TestAnOrdinarySequenceIsLeftAlone(t *testing.T) {
	snap := fixture(t)
	p := New()
	ctx := context.Background()
	for _, sql := range []string{
		"select currval('some_local_seq')",
		"select setval('some_local_seq', 42)",
	} {
		if _, err := p.Plan(ctx, session(snap), sql); err != nil {
			var pe *pgwire.Error
			if errors.As(err, &pe) && pe.Code == "0A000" && strings.Contains(pe.Message, "sequence") {
				t.Fatalf("%s was refused as a global sequence: %v", sql, err)
			}
		}
	}
}
