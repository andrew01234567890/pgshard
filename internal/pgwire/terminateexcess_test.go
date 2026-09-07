package pgwire

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// A role's connection limit is checked when a session connects, so lowering
// it left every session already open in place: the role kept whatever the
// old allowance had let it take, for as long as those sessions lived.
func TestLoweringALimitShedsTheSessionsAboveIt(t *testing.T) {
	ts := startServer(t, Config{})
	var busy []*rawClient
	for range 4 {
		c := dialRaw(t, ts.addr)
		if res := c.startupAs(ProtocolVersion30, "busy"); res.ready == nil {
			t.Fatalf("startup: %+v", res)
		}
		busy = append(busy, c)
	}
	other := dialRaw(t, ts.addr)
	if res := other.startupAs(ProtocolVersion30, "other"); res.ready == nil {
		t.Fatalf("startup: %+v", res)
	}

	// The limit drops to two, so the two newest of busy's four go and the
	// unrelated role is untouched.
	limit := func(u string) (int32, bool) {
		if u == "busy" {
			return 2, true
		}
		return 0, false
	}
	if n := ts.TerminateExcess(limit); n != 2 {
		t.Fatalf("terminated %d sessions, want the 2 above the limit", n)
	}
	for i, c := range busy[2:] {
		if !endsWithFatal(t, c) {
			t.Fatalf("busy session %d was kept; it is above the limit", i+2)
		}
	}
	// The oldest two are the ones the limit admits, and a pool has settled
	// on them.
	for i, c := range busy[:2] {
		if !stillOpen(t, c) {
			t.Fatalf("busy session %d was ended; it is within the limit", i)
		}
	}
	if !stillOpen(t, other) {
		t.Fatal("a role with no limit of its own lost a session")
	}

	// Running it again finds nothing left to do.
	if n := ts.TerminateExcess(limit); n != 0 {
		t.Fatalf("a second sweep terminated %d more", n)
	}
}

// A role at its limit is not over it: the sweep must not treat "may not
// take another" as "must give one back".
func TestARoleExactlyAtItsLimitKeepsEverySession(t *testing.T) {
	ts := startServer(t, Config{})
	for range 3 {
		c := dialRaw(t, ts.addr)
		if res := c.startupAs(ProtocolVersion30, "busy"); res.ready == nil {
			t.Fatalf("startup: %+v", res)
		}
	}
	if n := ts.TerminateExcess(func(string) (int32, bool) { return 3, true }); n != 0 {
		t.Fatalf("terminated %d sessions of a role that is exactly at its limit", n)
	}
}

func endsWithFatal(t *testing.T, c *rawClient) bool {
	t.Helper()
	for range 20 {
		msg, err := c.fe.Receive()
		if err != nil {
			return true
		}
		if e, ok := msg.(*pgproto3.ErrorResponse); ok && e.Severity == "FATAL" {
			return true
		}
	}
	return false
}

func stillOpen(t *testing.T, c *rawClient) bool {
	t.Helper()
	c.send(&pgproto3.Query{String: "select 1"})
	for range 20 {
		msg, err := c.fe.Receive()
		if err != nil {
			return false
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			return true
		}
	}
	return false
}
