package operator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/grpccreds"
	"github.com/andrew01234567890/pgshard/internal/twopc"
)

// TwoPCAgentClient drives the agent RPCs a barrier restore needs.
type TwoPCAgentClient interface {
	// ListTransactionDecisions reads the decision log through the catalog
	// primary's agent at addr.
	ListTransactionDecisions(ctx context.Context, addr string) ([]twopc.Decision, error)
	// ReconcilePrepared finishes the prepared transactions of the shard
	// primary at addr against decisions.
	ReconcilePrepared(ctx context.Context, addr string, epoch uint64, shardID int32, decisions []twopc.Decision) (twopc.Outcome, error)
	// SetWriteFence raises or releases the write fence through the catalog
	// primary's agent at addr.
	SetWriteFence(ctx context.Context, addr string, epoch uint64, active bool, reason string) error
	// ListPrepared lists the pgshard prepared transactions (gid to
	// database) the primary at addr still holds.
	ListPrepared(ctx context.Context, addr string) (map[string]string, error)
}

const (
	// twopcListTimeout bounds the read-only barrier RPCs. They return what
	// the primary already holds, so anything slower is a wedged handler.
	twopcListTimeout = 30 * time.Second
	// twopcFenceTimeout bounds raising and releasing the write fence.
	twopcFenceTimeout = 30 * time.Second
	// twopcReconcileTimeout is longer: reconciling commits or rolls back
	// every in-doubt transaction on the shard, each waiting on the sync
	// standbys like any other commit.
	twopcReconcileTimeout = 5 * time.Minute
)

// ListPrepared calls Agent.ListPreparedTransactions.
func (c *GRPCAgentClient) ListPrepared(ctx context.Context, addr string) (map[string]string, error) {
	cl, err := c.dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, twopcListTimeout)
	defer cancel()
	resp, err := cl.ListPreparedTransactions(ctx, &pgshardv1.ListPreparedTransactionsRequest{})
	if err != nil {
		return nil, withEpoch("list prepared transactions", err)
	}
	out := make(map[string]string, len(resp.GetPrepared()))
	for _, p := range resp.GetPrepared() {
		out[p.GetGid()] = p.GetDatabase()
	}
	return out, nil
}

// ListTransactionDecisions calls Agent.ListTransactionDecisions.
func (c *GRPCAgentClient) ListTransactionDecisions(ctx context.Context, addr string) ([]twopc.Decision, error) {
	cl, err := c.dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, twopcListTimeout)
	defer cancel()
	resp, err := cl.ListTransactionDecisions(ctx, &pgshardv1.ListTransactionDecisionsRequest{})
	if err != nil {
		return nil, withEpoch("list transaction decisions", err)
	}
	out := make([]twopc.Decision, 0, len(resp.GetDecisions()))
	for _, d := range resp.GetDecisions() {
		out = append(out, twopc.DecisionFromProto(d))
	}
	return out, nil
}

// ReconcilePrepared calls Agent.ReconcilePreparedTransactions.
func (c *GRPCAgentClient) ReconcilePrepared(ctx context.Context, addr string, epoch uint64, shardID int32, decisions []twopc.Decision) (twopc.Outcome, error) {
	cl, err := c.dial(ctx, addr)
	if err != nil {
		return twopc.Outcome{}, err
	}
	req := &pgshardv1.ReconcilePreparedTransactionsRequest{Epoch: epoch, ShardId: shardID}
	for _, d := range decisions {
		req.Decisions = append(req.Decisions, twopc.DecisionToProto(d))
	}
	ctx, cancel := context.WithTimeout(ctx, twopcReconcileTimeout)
	defer cancel()
	// A reconcile that failed reports no counts: the RPC fails, so nothing
	// comes back beside the error. Its per-item outcomes -- contradictions,
	// unverifiable, unreadable -- are fields of a SUCCESSFUL reconcile, and
	// are what the restore refuses to unfence on.
	resp, err := cl.ReconcilePreparedTransactions(ctx, req)
	if err != nil {
		return twopc.Outcome{}, withEpoch("reconcile prepared transactions", err)
	}
	return twopc.Outcome{Committed: int(resp.GetCommitted()), RolledBack: int(resp.GetRolledBack()), Contradictions: resp.GetContradictions(), Unverifiable: resp.GetUnverifiable(), Unreadable: resp.GetUnreadable()}, nil
}

// SetWriteFence calls Agent.SetWriteFence.
func (c *GRPCAgentClient) SetWriteFence(ctx context.Context, addr string, epoch uint64, active bool, reason string) error {
	cl, err := c.dial(ctx, addr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, twopcFenceTimeout)
	defer cancel()
	if _, err := cl.SetWriteFence(ctx, &pgshardv1.SetWriteFenceRequest{Epoch: epoch, Active: active, Reason: reason}); err != nil {
		return withEpoch("set write fence", err)
	}
	return nil
}

// BarrierClient asks a cluster's controller for a certified barrier.
type BarrierClient interface {
	CreateBarrier(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, addr, name string) error
}

// GRPCBarrierClient is the production BarrierClient over pgshard.v1.Controller.
type GRPCBarrierClient struct {
	// Creds secures the controller connection; nil dials plaintext, which
	// only a controller run with --insecure-dev accepts.
	Creds credentials.TransportCredentials
	// Issued supplies a cluster's own credentials when the operator issues
	// its certificates; they take precedence over Creds, which cannot chain
	// to every issuing cluster's CA.
	Issued *IssuedCredentials
}

// NewGRPCBarrierClient builds the barrier client from the operator's
// --controller-tls-* files: all three set dials mTLS, none set dials
// plaintext, anything else is an error.
func NewGRPCBarrierClient(certFile, keyFile, caFile string) (GRPCBarrierClient, error) {
	if certFile == "" && keyFile == "" && caFile == "" {
		return GRPCBarrierClient{}, nil
	}
	if certFile == "" || keyFile == "" || caFile == "" {
		return GRPCBarrierClient{}, errors.New("--controller-tls-cert, --controller-tls-key and --controller-tls-ca must be set together")
	}
	// Through grpccreds so a renewed certificate or CA is used from the next
	// connection on.
	creds, err := grpccreds.Dialer(certFile, keyFile, caFile, "", false)
	if err != nil {
		return GRPCBarrierClient{}, err
	}
	return GRPCBarrierClient{Creds: creds}, nil
}

const barrierRPCTimeout = 5 * time.Minute

// CreateBarrier implements BarrierClient.
func (c GRPCBarrierClient) CreateBarrier(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, addr, name string) error {
	creds := c.Creds
	if cluster != nil {
		_, issued, err := c.Issued.For(ctx, cluster)
		if err != nil {
			return err
		}
		if issued != nil {
			creds = issued
		}
	}
	// The controller may still be the pod that serves plaintext only until
	// the move to TLS has rolled it.
	if creds == nil || (cluster != nil && internalTLSPhase(cluster) == pgshardv1alpha1.InternalTLSAccepting) {
		creds = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, barrierRPCTimeout)
	defer cancel()
	resp, err := pgshardv1.NewControllerClient(conn).CreateBarrier(ctx, &pgshardv1.CreateBarrierRequest{Name: name})
	if err != nil {
		return err
	}
	if e := resp.GetError(); e != nil {
		return errors.New(e.GetMessage())
	}
	return nil
}

// DefaultControllerEndpoint is the controller address template of a
// backup policy that sets none.
const DefaultControllerEndpoint = "{cluster}-controller.{namespace}.svc:15500"

// ControllerEndpoint resolves the policy's controller address template for
// one cluster.
func ControllerEndpoint(template, cluster, namespace string) string {
	if template == "" {
		template = DefaultControllerEndpoint
	}
	return strings.NewReplacer("{cluster}", cluster, "{namespace}", namespace).Replace(template)
}

// ScheduledBarrierName names a scheduled barrier after its policy, cluster
// and tick, within the 63 characters a barrier name allows.
func ScheduledBarrierName(policy, cluster string, at time.Time) string {
	stamp := at.UTC().Format("20060102-1504")
	name := fmt.Sprintf("%s-%s-%s", policy, cluster, stamp)
	if len(name) > 63 {
		name = fmt.Sprintf("%s-%s", cluster, stamp)
	}
	if len(name) > 63 {
		name = name[len(name)-63:]
	}
	return strings.Trim(name, "-")
}
