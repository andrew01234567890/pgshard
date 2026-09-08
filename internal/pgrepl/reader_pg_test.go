package pgrepl

import (
	"context"
	"errors"
	"testing"
	"time"
)

// nextWithin runs one Next and fails if it does not return at all, so that a
// reader which never bounds its wait fails the test instead of hanging it.
func nextWithin(t *testing.T, r *Reader, within, watchdog time.Duration) (any, time.Duration, error) {
	t.Helper()
	type result struct {
		msg any
		err error
	}
	done := make(chan result, 1)
	start := time.Now()
	go func() {
		msg, err := r.Next(within)
		done <- result{msg, err}
	}()
	select {
	case got := <-done:
		return got.msg, time.Since(start), got.err
	case <-time.After(watchdog):
		t.Fatalf("Next(%v) did not return within %v", within, watchdog)
		return nil, 0, nil
	}
}

// testReader checks the three properties the pooler's change stream depends
// on, against a real server: a wait that expires leaves the connection
// usable, a cancelled context ends a wait that is already blocked, and Close
// hands the connection back with no deadline on it.
func testReader(t *testing.T, rc *Conn) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("expiry leaves the connection usable", func(t *testing.T) {
		r := rc.Reader(ctx)
		defer r.Close()
		const within = 200 * time.Millisecond
		for i := 0; i < 3; i++ {
			msg, took, err := nextWithin(t, r, within, 30*time.Second)
			if msg != nil || !IsTimeout(err) {
				t.Fatalf("wait %d: %v (%T)", i, err, msg)
			}
			// Nothing is arriving on this connection, so a wait that
			// returns early returned without ever bounding anything.
			if took < within/2 {
				t.Fatalf("wait %d returned after %v, less than half of %v", i, took, within)
			}
		}
		r.Close()
		// The connection still speaks the protocol after three expiries,
		// which is the whole reason a deadline is the right mechanism:
		// pgconn does not close a connection on a timeout.
		if _, err := rc.IdentifySystem(ctx); err != nil {
			t.Fatalf("after expiry: %v", err)
		}
	})

	t.Run("cancel ends a wait in progress", func(t *testing.T) {
		wctx, wcancel := context.WithCancel(ctx)
		r := rc.Reader(wctx)
		defer r.Close()
		time.AfterFunc(100*time.Millisecond, wcancel)
		// A minute, so that returning at all is the assertion: nothing
		// but the cancel can end this wait.
		_, _, err := nextWithin(t, r, time.Minute, 30*time.Second)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel: %v", err)
		}
	})

	t.Run("close clears the deadline", func(t *testing.T) {
		wctx, wcancel := context.WithCancel(ctx)
		r := rc.Reader(wctx)
		wcancel()
		if _, err := r.Next(time.Minute); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled: %v", err)
		}
		r.Close()
		// Without the clear -- or without Close waiting for a cancel
		// already in flight -- the deadline the hook set is still on the
		// socket and every later read on this connection fails at once.
		if _, err := rc.IdentifySystem(ctx); err != nil {
			t.Fatalf("after close: %v", err)
		}
	})
}
