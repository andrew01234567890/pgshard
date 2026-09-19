package router

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestAPortalWhoseStatementWasClosedIsRefused (PGS-883 item 2): execute()
// reached refuseShardStatementAfterDDL, the multi-shard rule and the shard
// pin only through e.stmts[e.portals[portal]]. Closing the STATEMENT
// deletes e.stmts[name] and leaves e.portals pointing at it, so that lookup
// missed and every check it guards was skipped -- while the Execute was
// still appended to the batch.
//
// Probed before fixing: with a transaction that had already run DDL, the
// Execute reached a shard with no statement behind it. The DDL guard exists
// precisely to stop a statement running on a shard in such a transaction,
// and an orphaned portal walked straight past it.
//
// PostgreSQL closes a statement's portals with it, so this portal could not
// be executed there either.
func TestAPortalWhoseStatementWasClosedIsRefused(t *testing.T) {
	h := newDDLHarness(t, &fakeQueue{})
	app := h.snap.Databases["app"]
	app.DDLTransactions = catalog.DDLTransactionsSequential
	h.snap.Databases["app"] = app

	conn, err := pgx.Connect(context.Background(), h.dsn())
	if err != nil {
		t.Fatal(err)
	}
	hj, err := conn.PgConn().Hijack()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hj.Conn.Close() })
	fe := hj.Frontend

	send := func(msgs ...pgproto3.FrontendMessage) {
		t.Helper()
		for _, m := range msgs {
			fe.Send(m)
		}
		if err := fe.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	// readTo returns the first ErrorResponse seen before ReadyForQuery.
	readTo := func(label string) *pgproto3.ErrorResponse {
		t.Helper()
		var first *pgproto3.ErrorResponse
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			msg, err := fe.Receive()
			if err != nil {
				t.Fatalf("%s: %v", label, err)
			}
			if e, ok := msg.(*pgproto3.ErrorResponse); ok && first == nil {
				first = e
			}
			if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
				return first
			}
		}
		t.Fatalf("%s: no ReadyForQuery", label)
		return nil
	}

	send(&pgproto3.Query{String: "begin"})
	readTo("begin")

	// An UNNAMED statement exists as well, because the bug this guards
	// against is subtle: deleting the portal entry instead would make
	// e.portals[portal] read back as "" and look up the unnamed statement,
	// running one portal's checks against another statement entirely.
	send(&pgproto3.Parse{Query: "select * from items"},
		&pgproto3.Parse{Name: "s", Query: "select * from items"},
		&pgproto3.Bind{DestinationPortal: "p", PreparedStatement: "s"},
		&pgproto3.Close{ObjectType: 'S', Name: "s"},
		&pgproto3.Sync{})
	readTo("parse/bind/close")

	send(&pgproto3.Query{String: "create table t2 (id int primary key)"})
	readTo("ddl")

	send(&pgproto3.Execute{Portal: "p"}, &pgproto3.Sync{})
	e := readTo("execute the orphaned portal")
	if e == nil {
		t.Fatal("executing a portal whose statement was closed was allowed, inside a transaction that had already run DDL: the router's own checks were skipped and the request went to a shard")
	}
	if e.Code != "0A000" {
		t.Errorf("refused with %s, want 0A000: %s", e.Code, e.Message)
	}
	for _, want := range []string{"closed", "portal"} {
		if !strings.Contains(e.Message+" "+e.Hint, want) {
			t.Errorf("the refusal does not mention %q, so the client is not told what went wrong: %s / %s", want, e.Message, e.Hint)
		}
	}
	// Nothing reached a shard for it.
	for i := range h.poolers {
		for _, q := range h.poolers[i].ran() {
			if q == "" {
				t.Errorf("shard %d was sent an empty statement for the orphaned portal", i)
			}
		}
	}
}
