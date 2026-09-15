package pooler

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// cancelPort stands in for PostgreSQL's cancellation port. It counts the
// cancel packets it receives and, until hold is closed, keeps each
// connection open after reading one -- which is how a test keeps a cancel
// in the middle of being delivered.
type cancelPort struct {
	ln       net.Listener
	received atomic.Int64
	arrived  chan struct{}
}

func startCancelPort(t *testing.T, hold <-chan struct{}) *cancelPort {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	p := &cancelPort{ln: ln, arrived: make(chan struct{}, 16)}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = c.Close() }()
				_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
				if n, _ := c.Read(make([]byte, 64)); n == 0 {
					return
				}
				p.received.Add(1)
				p.arrived <- struct{}{}
				if hold != nil {
					<-hold
				}
			}()
		}
	}()
	return p
}

func (p *cancelPort) dialer() Dialer {
	return Dialer{Address: p.ln.Addr().String(), Timeout: 2 * time.Second}
}

func numbered(req *pgshardv1.ExecuteRequest, n uint64) *pgshardv1.ExecuteRequest {
	req.Statement = n
	return req
}

// reservedSession reserves session "s" for statement 1 and runs BEGIN on it,
// so the session holds a backend a cancel can reach.
func reservedSession(t *testing.T, h *harness) pgshardv1.Pooler_ExecuteClient {
	t.Helper()
	ctx := context.Background()
	if _, err := h.client.Reserve(ctx, &pgshardv1.ReserveRequest{SessionId: "s", Generation: gen(7, 3), Statement: 1}); err != nil {
		t.Fatal(err)
	}
	stream, err := h.client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, stream, numbered(queryReq("s", "begin", gen(7, 3), identity("alice")), 1))
	return stream
}

// TestACancelForAnEarlierStatementIsIgnored: a Cancel can reach the pooler
// after the statement it was fired for has ended and the next one has
// begun. Delivered then, it interrupts the next statement (PGS-791).
func TestACancelForAnEarlierStatementIsIgnored(t *testing.T) {
	port := startCancelPort(t, nil)
	h := startHarnessWithCancels(t, PoolConfig{}, port.dialer())
	ctx := context.Background()
	stream := reservedSession(t, h)
	roundTrip(t, stream, numbered(queryReq("s", "select 2", gen(7, 3), nil), 2))

	cancel := func(n uint64) {
		t.Helper()
		if _, err := h.client.Cancel(ctx, &pgshardv1.CancelRequest{SessionId: "s", Statement: n}); err != nil {
			t.Fatal(err)
		}
	}
	cancel(1)
	if got := port.received.Load(); got != 0 {
		t.Fatalf("a cancel for statement 1 reached PostgreSQL while the session was on statement 2: %d packet(s)", got)
	}
	cancel(2)
	if got := port.received.Load(); got != 1 {
		t.Fatalf("a cancel for the statement the session is on was not delivered: %d packet(s)", got)
	}
	cancel(0)
	if got := port.received.Load(); got != 2 {
		t.Fatalf("an unnumbered cancel, which is all a router that does not number statements sends, was not delivered: %d packet(s)", got)
	}
}

// TestALaterStatementWaitsForACancelBeingDelivered: a cancel for the
// statement the session is on is let through, and the next statement's
// messages must not reach the backend until PostgreSQL has taken it. Sent
// ahead of the signal, they are what the signal interrupts.
func TestALaterStatementWaitsForACancelBeingDelivered(t *testing.T) {
	hold := make(chan struct{})
	port := startCancelPort(t, hold)
	h := startHarnessWithCancels(t, PoolConfig{}, port.dialer())
	ctx := context.Background()
	stream := reservedSession(t, h)

	cancelled := make(chan error, 1)
	go func() {
		_, err := h.client.Cancel(ctx, &pgshardv1.CancelRequest{SessionId: "s", Statement: 1})
		cancelled <- err
	}()
	select {
	case <-port.arrived:
	case <-time.After(5 * time.Second):
		close(hold)
		t.Fatal("the cancel for statement 1 never reached PostgreSQL")
	}

	if err := stream.Send(numbered(queryReq("s", "select 2", gen(7, 3), nil), 2)); err != nil {
		t.Fatal(err)
	}
	answered := make(chan error, 1)
	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				answered <- err
				return
			}
			if resp.GetReadyForQuery() != nil {
				answered <- nil
				return
			}
		}
	}()
	select {
	case <-answered:
		close(hold)
		t.Fatal("statement 2 ran while the cancel for statement 1 was still being delivered; the signal can land on it")
	case <-time.After(300 * time.Millisecond):
	}
	if h.pg.sawQuery("select 2") {
		close(hold)
		t.Fatal("statement 2 reached the backend while the cancel for statement 1 was still being delivered")
	}

	close(hold)
	if err := <-cancelled; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-answered:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("statement 2 never ran once the cancel had been delivered")
	}
}

// TestAReleaseForAnEarlierReservationIsIgnored: a Release that arrives after
// a later statement reserved the session again would unpin the backend that
// statement is holding.
func TestAReleaseForAnEarlierReservationIsIgnored(t *testing.T) {
	h := startHarness(t, PoolConfig{})
	ctx := context.Background()
	if _, err := h.client.Reserve(ctx, &pgshardv1.ReserveRequest{SessionId: "s", Generation: gen(7, 3), Statement: 3}); err != nil {
		t.Fatal(err)
	}
	stream, err := h.client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, stream, numbered(queryReq("s", "begin", gen(7, 3), identity("alice")), 3))
	_ = stream.CloseSend()
	waitFor(t, func() bool { return !h.attached() })

	if _, err := h.client.Release(ctx, &pgshardv1.ReleaseRequest{SessionId: "s", Statement: 2}); err != nil {
		t.Fatal(err)
	}
	if h.srv.lookup("s") == nil || h.srv.held() != 1 {
		t.Fatal("a release for statement 2 ended the reservation statement 3 made")
	}
	if _, err := h.client.Release(ctx, &pgshardv1.ReleaseRequest{SessionId: "s", Statement: 3}); err != nil {
		t.Fatal(err)
	}
	if h.srv.lookup("s") != nil || h.srv.held() != 0 {
		t.Fatal("the release for the statement that reserved the session did not end the reservation")
	}

	if _, err := h.client.Reserve(ctx, &pgshardv1.ReserveRequest{SessionId: "u", Generation: gen(7, 3), Statement: 9}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.client.Release(ctx, &pgshardv1.ReleaseRequest{SessionId: "u"}); err != nil {
		t.Fatal(err)
	}
	if h.srv.lookup("u") != nil {
		t.Fatal("an unnumbered release, which is what a session ending sends, did not end the reservation")
	}
}

// TestACancelAheadOfItsStatementWaitsForIt (PGS-827): a Cancel travels on its
// own connection and can overtake its statement's first request on the
// Execute stream. Delivered at once, it reached an idle backend, and
// PostgreSQL drops a cancel that arrives while it is reading a command -- the
// statement then ran uncancelled. It is now held, and goes once the
// statement's messages are on their way to the backend.
func TestACancelAheadOfItsStatementWaitsForIt(t *testing.T) {
	port := startCancelPort(t, nil)
	h := startHarnessWithCancels(t, PoolConfig{}, port.dialer())
	ctx := context.Background()
	stream := reservedSession(t, h)

	if _, err := h.client.Cancel(ctx, &pgshardv1.CancelRequest{SessionId: "s", Statement: 2}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if got := port.received.Load(); got != 0 {
		t.Fatalf("a cancel for statement 2 reached PostgreSQL before statement 2 did: %d packet(s)", got)
	}

	roundTrip(t, stream, numbered(queryReq("s", "select 2", gen(7, 3), nil), 2))
	select {
	case <-port.arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("the held cancel was never delivered once its statement reached the backend")
	}
	if !h.pg.sawQuery("select 2") {
		t.Fatal("the held cancel was delivered, but not behind its statement")
	}
}

// TestAHeldCancelWhoseStatementIsNeverSentIsDropped: a cancel held for a
// statement that never reaches the backend must not land on a later one.
func TestAHeldCancelWhoseStatementIsNeverSentIsDropped(t *testing.T) {
	port := startCancelPort(t, nil)
	h := startHarnessWithCancels(t, PoolConfig{}, port.dialer())
	ctx := context.Background()
	stream := reservedSession(t, h)

	if _, err := h.client.Cancel(ctx, &pgshardv1.CancelRequest{SessionId: "s", Statement: 2}); err != nil {
		t.Fatal(err)
	}
	roundTrip(t, stream, numbered(queryReq("s", "select 3", gen(7, 3), nil), 3))
	roundTrip(t, stream, numbered(queryReq("s", "select 4", gen(7, 3), nil), 4))
	time.Sleep(300 * time.Millisecond)
	if got := port.received.Load(); got != 0 {
		t.Fatalf("a cancel held for statement 2, which never came, was delivered to a later statement: %d packet(s)", got)
	}
}

// TestAHeldCancelWaitsPastAnEarlierStatement: a cancel held for statement 3
// is not spent on statement 2's batch; it goes when statement 3's does.
func TestAHeldCancelWaitsPastAnEarlierStatement(t *testing.T) {
	port := startCancelPort(t, nil)
	h := startHarnessWithCancels(t, PoolConfig{}, port.dialer())
	ctx := context.Background()
	stream := reservedSession(t, h)

	if _, err := h.client.Cancel(ctx, &pgshardv1.CancelRequest{SessionId: "s", Statement: 3}); err != nil {
		t.Fatal(err)
	}
	roundTrip(t, stream, numbered(queryReq("s", "select 2", gen(7, 3), nil), 2))
	time.Sleep(200 * time.Millisecond)
	if got := port.received.Load(); got != 0 {
		t.Fatalf("a cancel held for statement 3 was delivered on statement 2's batch: %d packet(s)", got)
	}
	roundTrip(t, stream, numbered(queryReq("s", "select 3", gen(7, 3), nil), 3))
	select {
	case <-port.arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("the cancel held for statement 3 was never delivered once statement 3 was sent")
	}
}
