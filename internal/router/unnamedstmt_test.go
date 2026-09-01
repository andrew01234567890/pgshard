package router

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

// TestTheUnnamedStatementOutlivesASync: PostgreSQL keeps the unnamed
// prepared statement until something replaces it, so a client may Parse it
// in one batch and Bind it in the next. A driver doing a separate prepare
// round trip does exactly that -- JDBC with prepareThreshold=0 and pgx's
// exec mode among them.
//
// The router does not pin a session for an unnamed statement, and the
// pooler hands an unreserved session whatever backend is free, having
// reset it. So the Bind reached a backend that had never seen the Parse
// and the client was told "prepared statement does not exist" for a
// sequence the protocol allows.
func TestTheUnnamedStatementOutlivesASync(t *testing.T) {
	h := newHarness(t)
	conn, err := pgx.Connect(context.Background(), h.dsn("app", "secret", "app"))
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
	drain := func() (*pgproto3.ErrorResponse, int) {
		t.Helper()
		var first *pgproto3.ErrorResponse
		parses := 0
		for {
			msg, err := fe.Receive()
			if err != nil {
				t.Fatal(err)
			}
			switch m := msg.(type) {
			case *pgproto3.ErrorResponse:
				if first == nil {
					first = m
				}
			case *pgproto3.ParseComplete:
				parses++
			case *pgproto3.ReadyForQuery:
				return first, parses
			}
		}
	}

	// The prepare round trip on its own. The backend goes back to the pool
	// at the Sync.
	send(&pgproto3.Parse{Query: "select 1"}, &pgproto3.Describe{ObjectType: 'S'}, &pgproto3.Sync{})
	if e, _ := drain(); e != nil {
		t.Fatalf("the prepare round trip failed: %s %s", e.Code, e.Message)
	}

	// The bind round trip, which reaches a backend that never parsed it.
	e, parses := drainAfter(t, send, drain)
	if e != nil {
		t.Fatalf("binding the unnamed statement after a Sync failed: %s %s", e.Code, e.Message)
	}
	// The client asked for no Parse in this batch and must see none: the
	// router's own re-parse is not the client's business, and an extra
	// ParseComplete would desynchronise a driver counting replies.
	if parses != 0 {
		t.Fatalf("the client saw %d ParseComplete for a batch that parsed nothing", parses)
	}

	// Still replaceable, as PostgreSQL's is.
	send(&pgproto3.Parse{Query: "select rows"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if e, parses := drain(); e != nil {
		t.Fatalf("replacing the unnamed statement failed: %s %s", e.Code, e.Message)
	} else if parses != 1 {
		t.Fatalf("a batch that parses once must show one ParseComplete, saw %d", parses)
	}
}

func drainAfter(t *testing.T, send func(...pgproto3.FrontendMessage), drain func() (*pgproto3.ErrorResponse, int)) (*pgproto3.ErrorResponse, int) {
	t.Helper()
	send(&pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	return drain()
}
