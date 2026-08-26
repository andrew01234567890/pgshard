package operator

import (
	"context"
	"net"
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

	c := GRPCAgentClient{}
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

	c := GRPCAgentClient{}
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
