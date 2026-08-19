package operator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// AgentStatus is the operator's view of one member agent.
type AgentStatus struct {
	Running bool
	Primary bool
	Epoch   uint64
	// LSN is the write LSN on a primary, the replay LSN on a standby.
	LSN uint64
}

// AgentClient drives member agents over pgshard.v1.Agent. addr is host:port
// of the agent's gRPC listener. The transport is plaintext for now; mTLS
// between operator and agents is a later layer.
type AgentClient interface {
	Status(ctx context.Context, addr string) (AgentStatus, error)
	Promote(ctx context.Context, addr string, epoch uint64, holder string) error
	Demote(ctx context.Context, addr string, epoch uint64) error
	// Reload makes the agent reread its config and signal postgres; it is
	// fenced at the agent's current epoch, so it never changes roles.
	// It returns the settings hash the agent loaded.
	Reload(ctx context.Context, addr string) (string, error)
}

// GRPCAgentClient is the production AgentClient.
type GRPCAgentClient struct{}

const agentDialTimeout = 3 * time.Second

func (GRPCAgentClient) dial(ctx context.Context, addr string) (*grpc.ClientConn, pgshardv1.AgentClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	dialCtx, cancel := context.WithTimeout(ctx, agentDialTimeout)
	defer cancel()
	conn.Connect()
	for state := conn.GetState(); !stateReady(state); state = conn.GetState() {
		if !conn.WaitForStateChange(dialCtx, state) {
			_ = conn.Close()
			return nil, nil, fmt.Errorf("agent %s unreachable: %w", addr, dialCtx.Err())
		}
	}
	return conn, pgshardv1.NewAgentClient(conn), nil
}

func stateReady(s interface{ String() string }) bool { return s.String() == "READY" }

// Status calls Agent.Status.
func (c GRPCAgentClient) Status(ctx context.Context, addr string) (AgentStatus, error) {
	conn, cl, err := c.dial(ctx, addr)
	if err != nil {
		return AgentStatus{}, err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(ctx, agentDialTimeout)
	defer cancel()
	resp, err := cl.Status(ctx, &pgshardv1.StatusRequest{})
	if err != nil {
		return AgentStatus{}, err
	}
	st := AgentStatus{Running: resp.GetRunning(), Primary: resp.GetRole() == pgshardv1.StatusResponse_ROLE_PRIMARY, Epoch: resp.GetEpoch(), LSN: resp.GetLsn()}
	if e := resp.GetError(); e != nil {
		return st, errors.New(e.GetMessage())
	}
	return st, nil
}

// Promote calls Agent.Promote and turns an embedded error into a Go error.
func (c GRPCAgentClient) Promote(ctx context.Context, addr string, epoch uint64, holder string) error {
	conn, cl, err := c.dial(ctx, addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	resp, err := cl.Promote(ctx, &pgshardv1.PromoteRequest{Epoch: epoch, LeaseHolder: holder})
	if err != nil {
		return err
	}
	if e := resp.GetError(); e != nil {
		return fmt.Errorf("promote: %s (%s)", e.GetMessage(), e.GetSqlstate())
	}
	return nil
}

// Demote calls Agent.Demote and turns an embedded error into a Go error.
func (c GRPCAgentClient) Demote(ctx context.Context, addr string, epoch uint64) error {
	conn, cl, err := c.dial(ctx, addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	resp, err := cl.Demote(ctx, &pgshardv1.DemoteRequest{Epoch: epoch})
	if err != nil {
		return err
	}
	if e := resp.GetError(); e != nil {
		return fmt.Errorf("demote: %s (%s)", e.GetMessage(), e.GetSqlstate())
	}
	return nil
}

// Reload reads the agent's epoch and calls Agent.Reload at that epoch.
func (c GRPCAgentClient) Reload(ctx context.Context, addr string) (string, error) {
	conn, cl, err := c.dial(ctx, addr)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()
	st, err := cl.Status(ctx, &pgshardv1.StatusRequest{})
	if err != nil {
		return "", err
	}
	resp, err := cl.Reload(ctx, &pgshardv1.ReloadRequest{Epoch: st.GetEpoch()})
	if err != nil {
		return "", err
	}
	if e := resp.GetError(); e != nil {
		return resp.GetSettingsHash(), fmt.Errorf("reload: %s (%s)", e.GetMessage(), e.GetSqlstate())
	}
	return resp.GetSettingsHash(), nil
}
