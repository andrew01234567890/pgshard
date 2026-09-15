package pooler

import (
	"context"
	"errors"
	"testing"
	"time"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// TestADrainDeadlineDoesNotCloseABackendUnderItsRelay: when Drain's deadline
// passes with a session still mid-statement, it discards that session's
// backend -- and discarding closes it, which writes a Terminate to the same
// pgproto3.Frontend the session's relay goroutine is reading from, then
// clears the connection underneath it. A Frontend is not safe for concurrent
// use. Run under -race, which is what settles whether that happens
// (PGS-820, PGS-752 L14).
func TestADrainDeadlineDoesNotCloseABackendUnderItsRelay(t *testing.T) {
	h := startHarness(t, PoolConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := h.client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The fake backend blocks on this until it is cancelled or closed, so
	// the relay is inside a backend read when the drain gives up.
	if err := stream.Send(queryReq("wedged", "select pg_sleep(30)", gen(7, 3), identity("alice"))); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return h.pg.queries.Load() >= 1 })
	waitFor(t, func() bool { return attachedSession(h) })

	dctx, dcancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer dcancel()
	if err := h.srv.Drain(dctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain with a statement still running: %v", err)
	}
	// The relay was inside a backend read. Closing the backend makes that
	// read fail, and the relay reports the loss on its own goroutine before
	// the statement ends -- which a relay left wedged in the read never
	// does. Ending the client stream alone would prove nothing: that makes
	// Recv fail whatever the server is doing.
	answered := make(chan []*pgshardv1.ExecuteResponse, 1)
	go func() {
		var out []*pgshardv1.ExecuteResponse
		defer func() { answered <- out }()
		for {
			resp, err := stream.Recv()
			if err != nil {
				return
			}
			out = append(out, resp)
			if resp.GetReadyForQuery() != nil {
				return
			}
		}
	}()
	select {
	case rs := <-answered:
		if e := firstError(rs); e.GetSqlstate() != "08006" {
			t.Fatalf("the relay did not report its backend gone: first error %v in %s", e, kinds(rs))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the relay never reported its backend gone after the drain abandoned it")
	}
}
