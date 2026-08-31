package router

import (
	"context"
	"errors"
	"sync"
	"time"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// poolerStream is one Execute stream with a reader goroutine so receives can
// be interleaved with context watching.
type poolerStream struct {
	// client is the pooler this stream was opened on. A release must reach
	// that pooler, not whichever one the current snapshot names: the point
	// of dropping a stream is that the endpoint may have moved.
	client pgshardv1.PoolerClient
	stream pgshardv1.Pooler_ExecuteClient
	cancel context.CancelFunc
	recvc  chan recvResult
	done   chan struct{}
	gone   chan struct{}
	once   sync.Once
	first  bool
}

type recvResult struct {
	msg *pgshardv1.ExecuteResponse
	err error
}

func openStream(ctx context.Context, client pgshardv1.PoolerClient) (*poolerStream, error) {
	sctx, cancel := context.WithCancel(ctx)
	st, err := client.Execute(sctx)
	if err != nil {
		cancel()
		return nil, err
	}
	ps := &poolerStream{client: client, stream: st, cancel: cancel, recvc: make(chan recvResult, 64), done: make(chan struct{}), gone: make(chan struct{}), first: true}
	go ps.reader()
	return ps, nil
}

func (ps *poolerStream) reader() {
	defer close(ps.done)
	for {
		msg, err := ps.stream.Recv()
		if err != nil {
			// The stream context is done as soon as the RPC ends, so the
			// terminal error must not race against it.
			select {
			case ps.recvc <- recvResult{err: err}:
			case <-ps.gone:
			}
			return
		}
		select {
		case ps.recvc <- recvResult{msg: msg}:
		case <-ps.stream.Context().Done():
			return
		}
	}
}

// send stamps req and writes it; the identity and database travel on the
// first message.
func (ps *poolerStream) send(req *pgshardv1.ExecuteRequest, sid string, gen *pgshardv1.Generation, ident *pgshardv1.UserIdentity, database string) error {
	req.SessionId = sid
	req.Generation = gen
	// Always: the packed shape carries the same columns with no object per
	// column on either side, and a pooler that predates the field ignores
	// it and answers the old way, which the router still reads.
	req.PackedRows = true
	if ps.first {
		req.User = ident
		req.Database = database
		ps.first = false
	}
	return ps.stream.Send(req)
}

// cancelGrace bounds the wait for a cancelled batch to reach
// ReadyForQuery. Draining keeps the stream in sync and is worth waiting
// for; waiting for ever is not. The cancel is best-effort -- it can fail,
// and a backend can be wedged somewhere PostgreSQL will not interrupt --
// and an unbounded wait then held this goroutine, the pooler session
// behind it and the router's own drain open with nothing able to end
// them. A variable so tests need not spend it.
var cancelGrace = 5 * time.Second

// errCancelGrace ends a batch whose cancellation was never acknowledged.
var errCancelGrace = errors.New("cancelled statement did not finish within the cancel grace")

// recv blocks for the next response. On ctx cancellation it calls onCancel
// once and waits for the batch to drain to ReadyForQuery, which keeps the
// stream in sync -- but only for cancelGrace. Past that the stream is
// aborted: it is stuck mid-batch, so the next statement on it would read
// this one's leftovers, and no answer is coming to put it right.
func (ps *poolerStream) recv(ctx context.Context, onCancel func()) (*pgshardv1.ExecuteResponse, error) {
	select {
	case r := <-ps.recvc:
		return r.msg, r.err
	case <-ctx.Done():
		if onCancel != nil {
			onCancel()
		}
	}
	t := time.NewTimer(cancelGrace)
	defer t.Stop()
	select {
	case r := <-ps.recvc:
		return r.msg, r.err
	case <-t.C:
		// Aborting the gRPC stream is what makes this bounded: it wakes
		// the receiving goroutine, closes done, and lets a later close or
		// Release return instead of waiting on a batch that never ends.
		ps.cancel()
		return nil, errCancelGrace
	}
}

// close half-closes the stream and waits until the pooler has finished the
// session so a following Release is accepted.
func (ps *poolerStream) close() {
	ps.once.Do(func() { close(ps.gone) })
	_ = ps.stream.CloseSend()
	<-ps.done
	ps.cancel()
}

// abort tears the stream down without waiting.
func (ps *poolerStream) abort() {
	ps.once.Do(func() { close(ps.gone) })
	ps.cancel()
	<-ps.done
}
