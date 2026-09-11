package pooler

import (
	"context"
	"net"
	"testing"
	"time"
)

// TestACancelIsAbandonedWhenTheBackendHasMovedOn.
//
// A backend is shared. Once the statement a cancel was fired for has
// finished, the pooler resets the backend and hands it to another session,
// and a cancel arriving then interrupts THAT session's statement -- a
// 57014 delivered to a client that never asked for one.
//
// The window was the dial: the backend was read under the lock and a fresh
// connection to PostgreSQL opened outside it, a TCP connect and possibly a
// TLS handshake long. The router fires cancels on client cancellation and
// on timeouts, which are exactly the moments a statement is about to end.
//
// The cancellation connection is still opened outside the lock -- it must
// be -- but nothing is sent on it until the backend is confirmed to be the
// one the cancel was fired for.
func TestACancelIsAbandonedWhenTheBackendHasMovedOn(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	got := make(chan int, 2)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = c.Close() }()
				_ = c.SetReadDeadline(time.Now().Add(time.Second))
				buf := make([]byte, 64)
				// Only actual bytes count: the abandoned case still
				// OPENS a connection, it just sends nothing on it, and
				// the read then ends at EOF with n == 0.
				if n, _ := c.Read(buf); n > 0 {
					got <- n
				}
			}()
		}
	}()

	b := &Backend{pid: 42, secret: []byte{1, 2, 3, 4}}
	d := Dialer{Address: ln.Addr().String(), Timeout: 2 * time.Second}

	// The backend has been handed to somebody else by the time the
	// connection is up: nothing may be sent.
	if err := b.cancel(context.Background(), d, func() bool { return false }); err != nil {
		t.Fatalf("abandoning a cancel is not an error: %v", err)
	}
	select {
	case n := <-got:
		t.Fatalf("%d bytes were sent to PostgreSQL for a backend that had moved on; that is another session's statement cancelled", n)
	case <-time.After(300 * time.Millisecond):
	}

	// And when it is still the right backend, the cancel goes.
	if err := b.cancel(context.Background(), d, func() bool { return true }); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	select {
	case n := <-got:
		if n == 0 {
			t.Fatal("nothing was sent for a cancel that should have gone")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the cancel was never sent")
	}
}
