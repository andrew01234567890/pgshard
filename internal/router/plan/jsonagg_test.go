package plan

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// TestTheSQLJSONAggregatesAreRecognisedAsAggregates.
//
// JSON_ARRAYAGG and JSON_OBJECTAGG are aggregates that the grammar gives
// their own node types instead of a FuncCall. Every check that looked for an
// aggregate looked for a FuncCall -- by its agg flags or by its name -- so
// none of them saw these, and a scatter fell through to a plain
// concatenation.
//
// The result was a wrong answer with no error: on four shards
// `SELECT json_arrayagg(id) FROM orders` returned FOUR rows, each one shard's
// partial array, and a client reading the first row got a quarter of the
// data. docs/router.md said "every other aggregate" was refused.
func TestTheSQLJSONAggregatesAreRecognisedAsAggregates(t *testing.T) {
	snap := fixture(t)
	for _, c := range []struct{ sql, want string }{
		{"select json_arrayagg(id) from orders", "multi-shard JSON_ARRAYAGG/JSON_OBJECTAGG"},
		{"select json_objectagg(id: status) from orders", "multi-shard JSON_ARRAYAGG/JSON_OBJECTAGG"},
		// The window form is not a FuncCall either, so it escaped the
		// window blocker by the same route.
		{"select json_arrayagg(id) over () from orders", "window functions"},
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

// A single-shard statement is not affected: the merge never runs, so these
// go to the shard and mean there exactly what they say.
func TestTheSQLJSONAggregatesStillWorkOnOneShard(t *testing.T) {
	snap := fixture(t)
	for _, sql := range []string{
		"select json_arrayagg(id) from orders where tenant_id = 1",
		"select json_objectagg(id: status) from orders where tenant_id = 1",
	} {
		if _, err := New().Plan(context.Background(), session(snap), sql); err != nil {
			t.Errorf("%s: %v", sql, err)
		}
	}
}

// And an ordinary scalar over the rows still scatters: this refuses
// aggregates, not functions.
func TestAScalarFunctionStillScatters(t *testing.T) {
	snap := fixture(t)
	for _, sql := range []string{
		"select upper(status) from orders",
		"select id from orders",
	} {
		if _, err := New().Plan(context.Background(), session(snap), sql); err != nil {
			t.Errorf("%s: %v", sql, err)
		}
	}
}
