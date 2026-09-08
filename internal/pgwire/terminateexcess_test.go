package pgwire

import (
	"errors"
	"net"
	"testing"
	"time"

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
	// ReadyForQuery is flushed before the session marks itself serving, so
	// a client that has one may still be uncounted for a moment -- and the
	// sweep counts only sessions that finished authenticating. Waiting for
	// the count is the difference between asserting on the state and
	// asserting on whichever part of it had arrived.
	waitServing(t, ts, "busy", 4)
	waitServing(t, ts, "other", 1)

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
	waitServing(t, ts, "busy", 3)
	if n := ts.TerminateExcess(func(string) (int32, bool) { return 3, true }); n != 0 {
		t.Fatalf("terminated %d sessions of a role that is exactly at its limit", n)
	}
}

// endsWithFatal reports whether the session ended. A read that merely times
// out is not an ending: the client sets a deadline, so treating any error
// as "ended" would report every session that was left alone as terminated,
// ten seconds later.
func endsWithFatal(t *testing.T, c *rawClient) bool {
	t.Helper()
	for range 20 {
		msg, err := c.fe.Receive()
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return false
			}
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

// A session publishes the role it claims before it proves it, so a
// revocation can reach a client mid-exchange. Counting those toward the
// role's limit let anyone who could reach the port claim a role, stall at
// the password prompt, and have that role's real sessions shed as the
// newest over the limit -- no password required.
func TestUnauthenticatedClaimantsDoNotEvictARolesSessions(t *testing.T) {
	ts := startServer(t, Config{
		Authenticator: CleartextAuthenticator{Lookup: lookup(map[string]string{"busy": "s3cret"})},
	})
	// Two peers that claim the role and never answer the password prompt.
	for range 2 {
		c := dialRaw(t, ts.addr)
		c.rawStartup(ProtocolVersion30, map[string]string{"user": "busy", "database": "db"})
		if _, ok := c.recv().(*pgproto3.AuthenticationCleartextPassword); !ok {
			t.Fatal("want a password request")
		}
	}
	// And one that is really the role.
	genuine := dialRaw(t, ts.addr)
	genuine.rawStartup(ProtocolVersion30, map[string]string{"user": "busy", "database": "db"})
	if _, ok := genuine.recv().(*pgproto3.AuthenticationCleartextPassword); !ok {
		t.Fatal("want a password request")
	}
	genuine.send(&pgproto3.PasswordMessage{Password: "s3cret"})
	for {
		if _, ok := genuine.recv().(*pgproto3.ReadyForQuery); ok {
			break
		}
	}

	waitServing(t, ts, "busy", 1)
	if n := ts.TerminateExcess(func(string) (int32, bool) { return 2, true }); n != 0 {
		t.Fatalf("terminated %d sessions; the role holds one, and two strangers claiming its name are not its own", n)
	}
	if !stillOpen(t, genuine) {
		t.Fatal("the role's own session was shed to make room for peers that never authenticated")
	}
}

// waitServing blocks until user holds n sessions that finished
// authenticating, which is the set the sweep counts.
func waitServing(t *testing.T, ts *testServer, user string, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := 0
		ts.mu.Lock()
		for _, sess := range ts.sessions {
			if u, serving := sess.role(); serving && u == user {
				got++
			}
		}
		ts.mu.Unlock()
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%q holds %d serving sessions, want %d", user, got, n)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
