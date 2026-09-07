package pgwire

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// forceClose cancels the query context and closes the client socket. A
// handler blocked on neither -- on a pooler that never finishes a session,
// which is what a backend PostgreSQL will not interrupt looks like from
// here -- is unmoved by both, and Shutdown then waited for it FOREVER,
// after its own deadline had already expired. A deadline that can be
// exceeded without bound is not one.
func TestShutdownReturnsEvenWhenAForcedSessionDoesNot(t *testing.T) {
	old := forceCloseGrace
	forceCloseGrace = 100 * time.Millisecond
	defer func() { forceCloseGrace = old }()

	wedged := make(chan struct{})
	// Released before the test ends, so the harness's own teardown is not
	// left waiting on the very thing this test wedges.
	defer close(wedged)

	ts := startServer(t, Config{})
	started := make(chan struct{}, 1)
	ts.newExec = func(SessionInfo) (Executor, error) {
		e := NewFakeExecutor()
		e.Delay = func(context.Context) error {
			started <- struct{}{}
			<-wedged
			return nil
		}
		return e, nil
	}
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)
	c.send(&pgproto3.Query{String: "select 1"})
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	returned := make(chan error, 1)
	go func() { returned <- ts.Shutdown(ctx) }()
	select {
	case err := <-returned:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("shutdown err = %v, want it to carry the deadline it exceeded", err)
		}
		// A plain deadline is what a caller gets when it simply ran out of
		// time; this one also has to say the sessions outlived being closed,
		// because that is the part an operator has to act on.
		if !strings.Contains(err.Error(), "sessions still running") {
			t.Fatalf("shutdown err = %v, want it to say the sessions outlived the force-close", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown never returned; a wedged session held it past its own deadline")
	}
}
