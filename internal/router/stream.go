package router

import (
	"context"
	"sync"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// poolerStream is one Execute stream with a reader goroutine so receives can
// be interleaved with context watching.
type poolerStream struct {
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
	ps := &poolerStream{stream: st, cancel: cancel, recvc: make(chan recvResult, 64), done: make(chan struct{}), gone: make(chan struct{}), first: true}
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
	if ps.first {
		req.User = ident
		req.Database = database
		ps.first = false
	}
	return ps.stream.Send(req)
}

// recv blocks for the next response. On ctx cancellation it calls onCancel
// once and keeps waiting: the batch must be drained to ReadyForQuery to keep
// the stream in sync.
func (ps *poolerStream) recv(ctx context.Context, onCancel func()) (*pgshardv1.ExecuteResponse, error) {
	select {
	case r := <-ps.recvc:
		return r.msg, r.err
	case <-ctx.Done():
		if onCancel != nil {
			onCancel()
		}
	}
	r := <-ps.recvc
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
