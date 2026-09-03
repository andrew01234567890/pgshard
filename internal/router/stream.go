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
	// credit bounds how many bytes the reader may hold ahead of the
	// session, one token per streamCreditChunk. The queue was bounded at
	// 64 messages, which says nothing about memory: a row or a COPY chunk
	// runs to whatever the gRPC receive limit allows, so a handful of wide
	// rows on each of many streams retained far more than a count suggests.
	credit chan struct{}
	done   chan struct{}
	gone   chan struct{}
	once   sync.Once
	first  bool
}

type recvResult struct {
	msg *pgshardv1.ExecuteResponse
	err error
	// credits is what the reader took for msg and the session returns
	// once it has it.
	credits int
}

const (
	// streamCreditChunk is the granularity of the read-ahead bound.
	streamCreditChunk = 16 << 10
	// streamCredits is how many chunks one stream may hold ahead, so a
	// megabyte. Small messages still queue freely; a large one is what the
	// bound is for.
	streamCredits = 64
)

// responseBytes is the payload a response carries. Only rows and COPY
// chunks are worth counting: every other message is a header or a short
// string, and walking them all would cost more than it bounds.
func responseBytes(m *pgshardv1.ExecuteResponse) int {
	switch x := m.GetMessage().(type) {
	case *pgshardv1.ExecuteResponse_DataRow:
		n := 0
		for _, v := range x.DataRow.GetPacked() {
			n += len(v)
		}
		for _, c := range x.DataRow.GetColumns() {
			n += len(c.GetData())
		}
		return n
	case *pgshardv1.ExecuteResponse_CopyData:
		return len(x.CopyData.GetData())
	}
	return 0
}

// creditsFor is what a response costs, never more than the whole bound: a
// message larger than the bound must still be deliverable, and with one
// reader taking every token it is.
func creditsFor(m *pgshardv1.ExecuteResponse) int {
	n := (responseBytes(m) + streamCreditChunk - 1) / streamCreditChunk
	return min(max(n, 1), streamCredits)
}

func openStream(ctx context.Context, client pgshardv1.PoolerClient) (*poolerStream, error) {
	sctx, cancel := context.WithCancel(ctx)
	st, err := client.Execute(sctx)
	if err != nil {
		cancel()
		return nil, err
	}
	ps := &poolerStream{client: client, stream: st, cancel: cancel, recvc: make(chan recvResult, 64), credit: make(chan struct{}, streamCredits), done: make(chan struct{}), gone: make(chan struct{}), first: true}
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
		n := creditsFor(msg)
		for range n {
			select {
			case ps.credit <- struct{}{}:
			case <-ps.stream.Context().Done():
				return
			}
		}
		select {
		case ps.recvc <- recvResult{msg: msg, credits: n}:
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
		return ps.took(r)
	case <-ctx.Done():
		if onCancel != nil {
			onCancel()
		}
	}
	t := time.NewTimer(cancelGrace)
	defer t.Stop()
	select {
	case r := <-ps.recvc:
		return ps.took(r)
	case <-t.C:
		// Aborting the gRPC stream is what makes this bounded: it wakes
		// the receiving goroutine, closes done, and lets a later close or
		// Release return instead of waiting on a batch that never ends.
		ps.cancel()
		return nil, errCancelGrace
	}
}

// took returns what the reader spent to hold r, letting it read on.
func (ps *poolerStream) took(r recvResult) (*pgshardv1.ExecuteResponse, error) {
	for range r.credits {
		select {
		case <-ps.credit:
		default:
		}
	}
	return r.msg, r.err
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
