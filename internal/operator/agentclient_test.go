package operator

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// deadlineAgent records the deadline each RPC arrives with.
type deadlineAgent struct {
	pgshardv1.UnimplementedAgentServer
	deadlines chan time.Duration
}

func (a *deadlineAgent) remaining(ctx context.Context) {
	d, ok := ctx.Deadline()
	if !ok {
		a.deadlines <- 0
		return
	}
	a.deadlines <- time.Until(d)
}

func (a *deadlineAgent) Promote(ctx context.Context, _ *pgshardv1.PromoteRequest) (*pgshardv1.PromoteResponse, error) {
	a.remaining(ctx)
	return &pgshardv1.PromoteResponse{}, nil
}

func (a *deadlineAgent) Demote(ctx context.Context, _ *pgshardv1.DemoteRequest) (*pgshardv1.DemoteResponse, error) {
	a.remaining(ctx)
	return &pgshardv1.DemoteResponse{}, nil
}

func (a *deadlineAgent) Status(context.Context, *pgshardv1.StatusRequest) (*pgshardv1.StatusResponse, error) {
	return &pgshardv1.StatusResponse{Running: true}, nil
}

func (a *deadlineAgent) Reload(ctx context.Context, _ *pgshardv1.ReloadRequest) (*pgshardv1.ReloadResponse, error) {
	a.remaining(ctx)
	return &pgshardv1.ReloadResponse{}, nil
}

func TestPromoteAndDemoteGetALongerDeadlineThanReload(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	agent := &deadlineAgent{deadlines: make(chan time.Duration, 3)}
	srv := grpc.NewServer()
	pgshardv1.RegisterAgentServer(srv, agent)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	c := NewGRPCAgentClient()
	ctx := context.Background()
	addr := lis.Addr().String()
	if err := c.Promote(ctx, addr, 1, "h"); err != nil {
		t.Fatal(err)
	}
	if err := c.Demote(ctx, addr, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reload(ctx, addr); err != nil {
		t.Fatal(err)
	}
	promote, demote, reload := <-agent.deadlines, <-agent.deadlines, <-agent.deadlines
	for name, d := range map[string]time.Duration{"promote": promote, "demote": demote} {
		if d < 90*time.Second {
			t.Errorf("%s deadline %s: a real pg_ctl promote -w with a CHECKPOINT needs minutes, not 30s", name, d)
		}
	}
	if reload > 31*time.Second {
		t.Errorf("reload deadline %s, want <= 30s", reload)
	}
}

func (a *deadlineAgent) ListPreparedTransactions(ctx context.Context, _ *pgshardv1.ListPreparedTransactionsRequest) (*pgshardv1.ListPreparedTransactionsResponse, error) {
	a.remaining(ctx)
	return &pgshardv1.ListPreparedTransactionsResponse{}, nil
}

func (a *deadlineAgent) ListTransactionDecisions(ctx context.Context, _ *pgshardv1.ListTransactionDecisionsRequest) (*pgshardv1.ListTransactionDecisionsResponse, error) {
	a.remaining(ctx)
	return &pgshardv1.ListTransactionDecisionsResponse{}, nil
}

func (a *deadlineAgent) ReconcilePreparedTransactions(ctx context.Context, _ *pgshardv1.ReconcilePreparedTransactionsRequest) (*pgshardv1.ReconcilePreparedTransactionsResponse, error) {
	a.remaining(ctx)
	return &pgshardv1.ReconcilePreparedTransactionsResponse{}, nil
}

func (a *deadlineAgent) SetWriteFence(ctx context.Context, _ *pgshardv1.SetWriteFenceRequest) (*pgshardv1.SetWriteFenceResponse, error) {
	a.remaining(ctx)
	return &pgshardv1.SetWriteFenceResponse{}, nil
}

// TestBarrierRPCsCarryTheirOwnDeadline: the barrier RPCs bounded only the
// dial and then handed the reconcile context straight to the call, so an
// agent that accepted the connection and wedged its handler held the restore
// reconciler for as long as it liked, with the cluster left fenced. The
// server here records the deadline it was given rather than blocking for it,
// so the test costs milliseconds: a call that arrives with no deadline at
// all is the failure being guarded against.
func TestBarrierRPCsCarryTheirOwnDeadline(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	agent := &deadlineAgent{deadlines: make(chan time.Duration, 4)}
	srv := grpc.NewServer()
	pgshardv1.RegisterAgentServer(srv, agent)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	c := NewGRPCAgentClient()
	ctx := context.Background()
	addr := lis.Addr().String()
	if _, err := c.ListPrepared(ctx, addr); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ListTransactionDecisions(ctx, addr); err != nil {
		t.Fatal(err)
	}
	if err := c.SetWriteFence(ctx, addr, 1, true, "restore"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReconcilePrepared(ctx, addr, 1, 0, nil); err != nil {
		t.Fatal(err)
	}
	listPrepared, listDecisions, fence, reconcile := <-agent.deadlines, <-agent.deadlines, <-agent.deadlines, <-agent.deadlines
	for name, d := range map[string]time.Duration{
		"list prepared": listPrepared, "list decisions": listDecisions, "set write fence": fence,
	} {
		if d == 0 {
			t.Errorf("%s arrived with no deadline: a wedged agent holds the reconciler forever", name)
			continue
		}
		if d > 31*time.Second {
			t.Errorf("%s deadline %s, want <= 30s", name, d)
		}
	}
	if reconcile == 0 {
		t.Error("reconcile prepared arrived with no deadline")
	}
	if reconcile <= 31*time.Second {
		t.Errorf("reconcile deadline %s: it commits every in-doubt transaction and needs longer than a list", reconcile)
	}
}

// TestAgentClientKeepsOneConnectionPerAddress: a reconcile pass asks every
// group's primary for its status, and the groups reconcile concurrently.
// Dialling per call meant a TCP handshake and an HTTP/2 preface per group
// per pass, thrown away the moment the answer arrived.
func TestAgentClientKeepsOneConnectionPerAddress(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var conns atomic.Int64
	counted := &countingListener{Listener: lis, accepted: &conns}
	srv := grpc.NewServer()
	pgshardv1.RegisterAgentServer(srv, &deadlineAgent{deadlines: make(chan time.Duration, 64)})
	go func() { _ = srv.Serve(counted) }()
	t.Cleanup(srv.Stop)

	c := NewGRPCAgentClient()
	defer c.Close()
	ctx := context.Background()
	addr := lis.Addr().String()
	for range 10 {
		if _, err := c.Status(ctx, addr); err != nil {
			t.Fatal(err)
		}
	}
	if n := conns.Load(); n != 1 {
		t.Errorf("ten calls to one agent opened %d connections, want the one it keeps", n)
	}

	// A connection that has gone is not kept: the next call dials again
	// rather than waiting on a broken one.
	c.mu.Lock()
	cc := c.conns[addr]
	c.mu.Unlock()
	c.drop(addr, cc)
	if _, err := c.Status(ctx, addr); err != nil {
		t.Fatal(err)
	}
	if n := conns.Load(); n != 2 {
		t.Errorf("after dropping the connection: %d in total, want a second", n)
	}
}

type countingListener struct {
	net.Listener
	accepted *atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.accepted.Add(1)
	}
	return c, err
}

// TestAConnectionToAnAddressNothingDialsAnyMoreIsClosed: connections are
// keyed by pod IP, and a member that is deleted, rolled or failed over never
// answers to that address again. Nothing dropped those connections, so the
// map and the transport goroutines behind it grew for the operator's life.
func TestAConnectionToAnAddressNothingDialsAnyMoreIsClosed(t *testing.T) {
	serve := func() string {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		srv := grpc.NewServer()
		pgshardv1.RegisterAgentServer(srv, &deadlineAgent{deadlines: make(chan time.Duration, 4)})
		go func() { _ = srv.Serve(lis) }()
		t.Cleanup(srv.Stop)
		return lis.Addr().String()
	}
	gone, live := serve(), serve()

	now := time.Now()
	c := NewGRPCAgentClient()
	c.now = func() time.Time { return now }
	ctx := context.Background()
	if _, err := c.Status(ctx, gone); err != nil {
		t.Fatal(err)
	}
	conn := c.conns[gone]
	if conn == nil {
		t.Fatal("no connection was kept for the address that was dialled")
	}

	// The pod behind gone is replaced: nothing dials that address again.
	now = now.Add(agentConnTTL + time.Second)
	if _, err := c.Status(ctx, live); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.conns[gone]; ok {
		t.Errorf("connection to %s kept %s after its last use", gone, agentConnTTL)
	}
	if _, ok := c.used[gone]; ok {
		t.Errorf("last-use entry for %s kept after eviction", gone)
	}
	if s := conn.GetState().String(); s != "SHUTDOWN" {
		t.Errorf("evicted connection state %s, want SHUTDOWN: the entry went but the transport stayed", s)
	}
	if _, ok := c.conns[live]; !ok {
		t.Errorf("connection to the address in use was evicted too")
	}
}
