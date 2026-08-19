package operator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
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
}

// ListTransactionDecisions calls Agent.ListTransactionDecisions.
func (c GRPCAgentClient) ListTransactionDecisions(ctx context.Context, addr string) ([]twopc.Decision, error) {
	conn, cl, err := c.dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	resp, err := cl.ListTransactionDecisions(ctx, &pgshardv1.ListTransactionDecisionsRequest{})
	if err != nil {
		return nil, err
	}
	if e := resp.GetError(); e != nil {
		return nil, fmt.Errorf("list transaction decisions: %s", e.GetMessage())
	}
	out := make([]twopc.Decision, 0, len(resp.GetDecisions()))
	for _, d := range resp.GetDecisions() {
		out = append(out, twopc.Decision{GID: d.GetGid(), State: d.GetState(), Participants: d.GetParticipants(), ParticipantXIDs: d.GetParticipantXids()})
	}
	return out, nil
}

// ReconcilePrepared calls Agent.ReconcilePreparedTransactions.
func (c GRPCAgentClient) ReconcilePrepared(ctx context.Context, addr string, epoch uint64, shardID int32, decisions []twopc.Decision) (twopc.Outcome, error) {
	conn, cl, err := c.dial(ctx, addr)
	if err != nil {
		return twopc.Outcome{}, err
	}
	defer func() { _ = conn.Close() }()
	req := &pgshardv1.ReconcilePreparedTransactionsRequest{Epoch: epoch, ShardId: shardID}
	for _, d := range decisions {
		req.Decisions = append(req.Decisions, &pgshardv1.TransactionDecision{Gid: d.GID, State: d.State, Participants: d.Participants, ParticipantXids: d.ParticipantXIDs})
	}
	resp, err := cl.ReconcilePreparedTransactions(ctx, req)
	if err != nil {
		return twopc.Outcome{}, err
	}
	out := twopc.Outcome{Committed: int(resp.GetCommitted()), RolledBack: int(resp.GetRolledBack()), Contradictions: resp.GetContradictions()}
	if e := resp.GetError(); e != nil {
		return out, fmt.Errorf("reconcile prepared transactions: %s", e.GetMessage())
	}
	return out, nil
}

// SetWriteFence calls Agent.SetWriteFence.
func (c GRPCAgentClient) SetWriteFence(ctx context.Context, addr string, epoch uint64, active bool, reason string) error {
	conn, cl, err := c.dial(ctx, addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	resp, err := cl.SetWriteFence(ctx, &pgshardv1.SetWriteFenceRequest{Epoch: epoch, Active: active, Reason: reason})
	if err != nil {
		return err
	}
	if e := resp.GetError(); e != nil {
		return fmt.Errorf("set write fence: %s", e.GetMessage())
	}
	return nil
}

// BarrierClient asks a cluster's controller for a certified barrier.
type BarrierClient interface {
	CreateBarrier(ctx context.Context, addr, name string) error
}

// GRPCBarrierClient is the production BarrierClient over pgshard.v1.Controller.
type GRPCBarrierClient struct{}

const barrierRPCTimeout = 5 * time.Minute

// CreateBarrier implements BarrierClient.
func (GRPCBarrierClient) CreateBarrier(ctx context.Context, addr, name string) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
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
