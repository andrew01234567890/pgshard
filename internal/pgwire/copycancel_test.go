package pgwire

import (
	"context"
	"net"
	"testing"
	"time"
)

// TestACancelThatArrivesBeforeTheCopyIsRegistered: cancelQuery wakes a
// parked COPY read by putting the read deadline in the past, but only if a
// stream is registered when it runs. If the cancel gets there first it
// finds nil, sets nothing, and the read that starts a moment later parks
// for ever -- waiting for a client that is itself waiting for the
// cancellation's result, which is the exact standoff cancelQuery exists to
// break.
//
// The ordering is a race in the running server and is forced here, because
// a test that waits for the losing order to happen by itself is the test
// that passed fifty times and then failed three times in one evening.
func TestACancelThatArrivesBeforeTheCopyIsRegistered(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	s := &session{conn: server}
	ctx, cancel := context.WithCancel(context.Background())
	s.queryCancel, s.queryCtx = cancel, ctx

	// The cancel lands while no COPY is registered: nothing to wake.
	cancel()

	// The executor registers the stream a moment later. Without the fix
	// nothing has put a deadline on the connection and the read below
	// blocks until the test's own deadline.
	s.setCopyIn(&copyInStream{s: s})

	done := make(chan error, 1)
	go func() {
		_, err := server.Read(make([]byte, 1))
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("the read returned without an error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the COPY read parked: a cancel that arrived before the stream was registered woke nothing")
	}
}
