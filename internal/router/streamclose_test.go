package router

import (
	"context"
	"testing"
	"time"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// wedgedStream never answers a Recv, which is what a pooler blocked on a
// backend PostgreSQL will not interrupt looks like from the router.
type wedgedStream struct {
	pgshardv1.Pooler_ExecuteClient
	ctx    context.Context
	held   chan struct{}
	sentAt time.Time
	seenAt time.Time
}

func (s *wedgedStream) Recv() (*pgshardv1.ExecuteResponse, error) {
	<-s.ctx.Done()
	s.seenAt = time.Now()
	close(s.held)
	return nil, s.ctx.Err()
}
func (s *wedgedStream) CloseSend() error         { s.sentAt = time.Now(); return nil }
func (s *wedgedStream) Context() context.Context { return s.ctx }

// close waited for the pooler to finish the session, without a bound. A
// pooler that never finishes one then held this goroutine, and with it the
// router's drain and its shutdown: forceClose cancels the query context and
// closes the client socket, and neither is what this waits on.
func TestClosingAStreamTheePoolerNeverFinishesIsBounded(t *testing.T) {
	old := cancelGrace
	cancelGrace = 50 * time.Millisecond
	defer func() { cancelGrace = old }()

	ctx, cancel := context.WithCancel(context.Background())
	ws := &wedgedStream{ctx: ctx, held: make(chan struct{})}
	ps := &poolerStream{stream: ws, cancel: cancel, recvc: make(chan recvResult, 64),
		credit: make(chan struct{}, streamCredits), done: make(chan struct{}), gone: make(chan struct{})}
	go ps.reader()

	returned := make(chan struct{})
	go func() { ps.close(); close(returned) }()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("close never returned; the router's drain and shutdown wait on this")
	}
	// And it ended the stream rather than leaving the reader on it.
	select {
	case <-ps.done:
	case <-time.After(time.Second):
		t.Fatal("close returned with the reader still running")
	}
	// The bound is a last resort, not the mechanism: the pooler gets the
	// half-close and the whole grace to finish the session on its own before
	// the stream is torn out from under it. Cancelling straight away is what
	// abort is for, and a close that did that would pass everything above.
	if waited := ws.seenAt.Sub(ws.sentAt); waited < cancelGrace/2 {
		t.Fatalf("stream cancelled %v after the half-close; the pooler gets %v to finish first", waited, cancelGrace)
	}
}
