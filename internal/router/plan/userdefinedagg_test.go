package plan

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// An aggregate is recognised by name, and a user-defined one -- first(x)
// from an extension, anything from CREATE AGGREGATE -- is a plain FuncCall
// with no agg flags set. It read as a scalar function, so a scatter
// concatenated the shards: SELECT first(status) FROM orders answered with
// one partial row PER SHARD and no error. Nothing in the parse tree tells
// that call from a user-defined scalar, so a scatter refuses both.
func TestAFunctionThisRouterCannotClassifyIsRefusedOnAScatter(t *testing.T) {
	snap := fixture(t)
	for _, c := range []struct{ sql, want string }{
		{"select first(status) from orders", "multi-shard first()"},
		{"select approx_count_distinct(id) from orders", "multi-shard approx_count_distinct()"},
		// Nested under a built-in is the same call.
		{"select upper(first(status)) from orders", "multi-shard first()"},
		// A schema of the user's own is the user's own function, whatever
		// it is named: pgshard.sum(x) is not PostgreSQL's sum.
		{"select analytics.sum(amount) from orders", "multi-shard sum()"},
	} {
		pl, err := New().Plan(context.Background(), session(snap), c.sql)
		if err == nil {
			t.Fatalf("%s: planned as %v with merge %+v; every shard would answer its own partial", c.sql, pl.Kind, pl.merge)
		}
		var pe *pgwire.Error
		if !errors.As(err, &pe) || pe.Code != "0A000" || !strings.Contains(pe.Message, c.want) {
			t.Fatalf("%s: %v, want a 0A000 containing %q", c.sql, err, c.want)
		}
	}
}

// The refusal is for the call the router cannot classify, not for functions:
// PostgreSQL's own scalars still scatter, a single-shard statement is never
// merged, and grouping on the shard key makes every group shard-local, so a
// user-defined aggregate over it is answered correctly by concatenation.
func TestAFunctionThisRouterCannotClassifyIsAllowedWhereItIsAnswerable(t *testing.T) {
	snap := fixture(t)
	for _, sql := range []string{
		"select upper(status), lower(status) from orders",
		"select first(status) from orders where tenant_id = 1",
		"select tenant_id, first(status) from orders group by tenant_id",
	} {
		if _, err := New().Plan(context.Background(), session(snap), sql); err != nil {
			t.Errorf("%s: %v", sql, err)
		}
	}
}

// The names come from the majors pgshard targets rather than a list kept by
// hand: the hand-written one missed every JSON aggregate added after it,
// which a scatter then concatenated.
func TestTheBuiltinNamesCoverTheAggregatesTheHandWrittenListMissed(t *testing.T) {
	for _, name := range []string{"json_agg_strict", "jsonb_agg_strict", "json_object_agg_unique",
		"json_object_agg_unique_strict", "jsonb_object_agg_unique", "jsonb_object_agg_unique_strict"} {
		if !aggregateNames[name] {
			t.Errorf("%s is not recognised as an aggregate", name)
		}
	}
	for _, name := range []string{"upper", "lower", "now", "coalesce", "jsonb_build_object"} {
		if aggregateNames[name] {
			t.Errorf("%s is recognised as an aggregate", name)
		}
	}
}
