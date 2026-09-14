package pooler

import (
	"context"
	"errors"
	"testing"
	"time"
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
	// The relay was inside a backend read; closing the backend makes that
	// read fail, and the relay unwinds on its own goroutine. Wait for the
	// stream to end rather than a fixed time, bounded, so a relay that never
	// unwinds fails the test instead of hanging it.
	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		for {
			if _, err := stream.Recv(); err != nil {
				return
			}
		}
	}()
	cancel()
	select {
	case <-recvDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the session's stream never ended after its backend was abandoned")
	}
}
