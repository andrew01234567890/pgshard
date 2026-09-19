//go:build integration

package router

import (
	"slices"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// TestAnExecuteOfACursorDoesNotRecordTheUnnamedStatement (PGS-942): execute()
// looked the portal's statement up as e.stmts[e.portals[portal]], and a
// portal the router never bound -- a cursor DECLAREd in SQL, which lives on
// the backend -- reads back as "" there, the UNNAMED statement. The Execute
// was forwarded correctly and answered by the cursor, but the router acted
// on the unnamed statement as though it had run: a SET left unnamed was
// recorded as session state and handed to the next backend the session
// used, although PostgreSQL never ran it.
func TestAnExecuteOfACursorDoesNotRecordTheUnnamedStatement(t *testing.T) {
	s := startStack(t)
	conn := s.connect(t)
	hj, err := conn.PgConn().Hijack()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hj.Conn.Close() })
	r := newRawConn(hj.Frontend)
	expect := func(what string, got []string, want ...string) {
		t.Helper()
		if !slices.Equal(got, want) {
			t.Fatalf("%s answered %v, want %v", what, got, want)
		}
	}
	show := func() string {
		t.Helper()
		expect("SHOW", r.batch(t, false, &pgproto3.Query{String: "show application_name"}),
			"RowDescription", "DataRow", "CommandComplete", "ReadyForQuery")
		return r.row()[0]
	}
	before := show()

	expect("BEGIN", r.batch(t, false, &pgproto3.Query{String: "begin"}), "CommandComplete", "ReadyForQuery")
	expect("DECLARE", r.batch(t, false, &pgproto3.Query{String: "declare c cursor for select 1"}), "CommandComplete", "ReadyForQuery")
	expect("Parse of an unnamed SET, never executed",
		r.batch(t, false, &pgproto3.Parse{Query: "set application_name = 'pgs942_ghost'"}, &pgproto3.Sync{}),
		"ParseComplete", "ReadyForQuery")
	expect("Execute of the cursor's portal", r.batch(t, false, &pgproto3.Execute{Portal: "c"}, &pgproto3.Sync{}),
		"DataRow", "CommandComplete", "ReadyForQuery")
	expect("COMMIT", r.batch(t, false, &pgproto3.Query{String: "commit"}), "CommandComplete", "ReadyForQuery")

	if after := show(); after != before {
		t.Fatalf("application_name is %q after the transaction, want %q: the router recorded a SET PostgreSQL never ran", after, before)
	}
}
