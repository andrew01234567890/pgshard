package pgwire

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Until this existed, the only thing that ended a statement the shard would
// not finish was the client asking. A client that has gone away, or one
// waiting for a result that is never coming, asks nothing -- and the
// goroutine, the pooler backend and a share of the drain stay held.
func TestAStatementPastTheRoutersLimitIsCancelledAndSaysWhy(t *testing.T) {
	ts := startServer(t, Config{MaxQueryDuration: 100 * time.Millisecond})
	ts.newExec = func(SessionInfo) (Executor, error) {
		e := NewFakeExecutor()
		e.Delay = func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}
		return e, nil
	}
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)
	c.send(&pgproto3.Query{String: "select 1"})

	done := make(chan pgproto3.BackendMessage, 1)
	go func() { done <- c.recv() }()
	select {
	case msg := <-done:
		er, ok := msg.(*pgproto3.ErrorResponse)
		if !ok {
			t.Fatalf("got %T, want the statement cancelled", msg)
		}
		if er.Code != CodeQueryCanceled {
			t.Fatalf("code = %s, want %s", er.Code, CodeQueryCanceled)
		}
		if er.Message != "canceling statement due to statement timeout" {
			t.Fatalf("message = %q, want PostgreSQL's own words for a timeout", er.Message)
		}
		if er.Detail == "" {
			t.Fatal("a client told its statement timed out is owed which clock ran out")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the statement was never cancelled; the limit did not reach it")
	}
	if _, ok := c.recv().(*pgproto3.ReadyForQuery); !ok {
		t.Fatal("want ReadyForQuery after the cancelled statement")
	}
}

// A client cancel and an expired limit are both 57014, and they are not the
// same event: one says the user asked, the other that the router did.
func TestAClientCancelIsStillReportedAsTheUsersRequest(t *testing.T) {
	ts := startServer(t, Config{MaxQueryDuration: time.Hour})
	started := make(chan struct{}, 1)
	ts.newExec = func(SessionInfo) (Executor, error) {
		e := NewFakeExecutor()
		e.Delay = func(ctx context.Context) error {
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}
		return e, nil
	}
	c := dialRaw(t, ts.addr)
	res := c.startup(ProtocolVersion30)
	c.send(&pgproto3.Query{String: "select 1"})
	<-started
	if !ts.CancelLocal(CancelKey{PID: res.key.ProcessID, Secret: res.key.SecretKey}) {
		t.Fatal("cancel was not accepted")
	}
	er, ok := c.recv().(*pgproto3.ErrorResponse)
	if !ok || er.Code != CodeQueryCanceled {
		t.Fatalf("got %+v, want a cancelled statement", er)
	}
	if er.Message != "canceling statement due to user request" {
		t.Fatalf("message = %q, want the cancel attributed to the user", er.Message)
	}
}

// Zero is unbounded, which is what PostgreSQL's statement_timeout defaults
// to: a router must not start cancelling statements nobody asked it to.
func TestNoLimitLeavesAStatementAlone(t *testing.T) {
	ts := startServer(t, Config{})
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)
	c.send(&pgproto3.Query{String: "select 1"})
	if _, ok := c.recv().(*pgproto3.RowDescription); !ok {
		t.Fatal("an unbounded router runs the statement")
	}
}
