package plan

import (
	"context"
	"strings"
	"testing"
)

// A refusal has to name the statement the user typed. libpg_query
// abbreviates in its node type names, and the expansion was written with
// trailing spaces, so it never fired on the abbreviations that end a name
// -- which are the common ones. The router refused "CREATE TRIG".
func TestARefusalNamesAStatementThatExists(t *testing.T) {
	snap := fixture(t)
	for _, c := range []struct{ sql, want string }{
		{"create trigger t after insert on orders for each row execute function f()", "CREATE TRIGGER"},
		{"create event trigger e on ddl_command_end execute function f()", "CREATE EVENT TRIGGER"},
		{"create function f() returns int as $$ select 1 $$ language sql", "CREATE FUNCTION"},
		{"create extension pg_stat_statements", "CREATE EXTENSION"},
		{"security label on table orders is 'x'", "SECURITY LABEL"},
	} {
		t.Run(c.want, func(t *testing.T) {
			_, err := New().Plan(context.Background(), session(snap), c.sql)
			if err == nil {
				t.Fatalf("want a refusal for %q", c.sql)
			}
			if !strings.Contains(err.Error(), c.want+" is not supported") {
				t.Errorf("refusal %q does not name %q", err.Error(), c.want)
			}
		})
	}
}
