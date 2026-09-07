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
	ts.newExec = func(SessionInfo) (Executor, error) {
		e := NewFakeExecutor()
		// Reports the context it was given rather than ignoring it, so a
		// limit that fires at once -- a zero duration taken as a deadline
		// rather than as no deadline -- fails here instead of passing.
		e.Delay = func(ctx context.Context) error { return ctx.Err() }
		return e, nil
	}
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)
	c.send(&pgproto3.Query{String: "select 1"})
	if _, ok := c.recv().(*pgproto3.RowDescription); !ok {
		t.Fatal("an unbounded router runs the statement")
	}
}

// A batch rolls back before the client is told anything, and that rollback
// gets a context of its own. Reading the session's current context after it
// asks the wrong context whether the statement timed out: the answer is
// about the rollback, and the client is told whatever the transport said
// instead of why its statement was stopped.
func TestATimeoutInsideABatchIsStillReportedAsATimeout(t *testing.T) {
	ts := startServer(t, Config{MaxQueryDuration: 150 * time.Millisecond})
	ts.newExec = func(SessionInfo) (Executor, error) {
		return &slowSecondStatement{Executor: NewFakeExecutor()}, nil
	}
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)
	c.send(&pgproto3.Query{String: "select 1; select 1"})

	for {
		msg := c.recv()
		if er, ok := msg.(*pgproto3.ErrorResponse); ok {
			if er.Code != CodeQueryCanceled || er.Message != "canceling statement due to statement timeout" {
				t.Fatalf("got %s %q, want the statement's own timeout", er.Code, er.Message)
			}
			break
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			t.Fatal("the batch ended without reporting the statement that ran out of time")
		}
	}
	if _, ok := c.recv().(*pgproto3.ReadyForQuery); !ok {
		t.Fatal("want ReadyForQuery after the batch")
	}
}

// slowSecondStatement runs out the clock on the batch's second statement
// and rolls back at once, which is what makes the rollback's own context a
// different answer from the statement's.
type slowSecondStatement struct {
	Executor
	n int
}

func (e *slowSecondStatement) SimpleQuery(ctx context.Context, sql string, w ResultWriter) error {
	e.n++
	if e.n == 2 {
		<-ctx.Done()
		return ctx.Err()
	}
	return e.Executor.SimpleQuery(ctx, sql, w)
}

func (e *slowSecondStatement) EndImplicit(context.Context, bool) error { return nil }

// A COPY FROM STDIN is a loop of socket reads that never looks at the
// context, so the limit has to reach the socket. Without it a client that
// stops sending holds the COPY, its backend and its share of the drain for
// as long as it likes -- which is the hold this limit exists to end.
func TestACopyThatStopsSendingRunsOutOfTimeLikeAnythingElse(t *testing.T) {
	ts := startServer(t, Config{MaxQueryDuration: 200 * time.Millisecond})
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)
	c.send(&pgproto3.Query{String: "copy fake from stdin"})
	if _, ok := c.recv().(*pgproto3.CopyInResponse); !ok {
		t.Fatal("want the COPY to start")
	}
	c.send(&pgproto3.CopyData{Data: []byte("1\n")})
	// And then nothing: the client goes quiet without a CopyDone.

	done := make(chan pgproto3.BackendMessage, 1)
	go func() { done <- c.recv() }()
	select {
	case msg := <-done:
		if _, ok := msg.(*pgproto3.ErrorResponse); !ok {
			t.Fatalf("got %T, want the COPY ended", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the COPY was never ended; the limit did not reach the socket read")
	}
}

// The deadline belongs to the COPY, not to the connection: a transfer that
// finishes must not leave the socket armed to break the client's next idle
// wait for something to send.
func TestAFinishedCopyLeavesTheConnectionUsable(t *testing.T) {
	ts := startServer(t, Config{MaxQueryDuration: 300 * time.Millisecond})
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)
	c.send(&pgproto3.Query{String: "copy fake from stdin"})
	if _, ok := c.recv().(*pgproto3.CopyInResponse); !ok {
		t.Fatal("want the COPY to start")
	}
	c.send(&pgproto3.CopyData{Data: []byte("1\n")})
	c.send(&pgproto3.CopyDone{})
	for {
		if _, ok := c.recv().(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	// Longer than the limit the COPY ran under, and idle throughout.
	time.Sleep(400 * time.Millisecond)
	c.send(&pgproto3.Query{String: "select 1"})
	if _, ok := c.recv().(*pgproto3.RowDescription); !ok {
		t.Fatal("the connection did not survive its own COPY's deadline")
	}
}
