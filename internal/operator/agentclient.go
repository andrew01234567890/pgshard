package operator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/grpccreds"
)

// AgentStatus is the operator's view of one member agent.
type AgentStatus struct {
	Running bool
	Primary bool
	Epoch   uint64
	// LSN is the write LSN on a primary, the replay LSN on a standby.
	LSN uint64
	// Timeline is the current timeline id.
	Timeline uint32
	// PromotionPending is true while the agent ran pg_ctl promote but has not
	// finished the post-promotion setup; converge re-issues Promote.
	PromotionPending bool
	// Build is what the agent says it is. Empty from one that predates the
	// field, which is itself the answer "older than this".
	Build string
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
	// SetSynchronizedStandbySlots tells the primary's agent which physical
	// slots failover slots must wait for; it returns the slots applied.
	SetSynchronizedStandbySlots(ctx context.Context, addr string, slots []string) ([]string, error)
}

// GRPCAgentClient is the production AgentClient. It keeps one connection
// per agent address: a reconcile pass asks every group's primary for its
// status, and with the groups reconciled concurrently that was a TCP
// handshake and an HTTP/2 preface per group per pass, thrown away the
// moment the answer arrived.
type GRPCAgentClient struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
	// creds is what dial presents. Nil means plaintext, which is what an
	// agent that has not been given GRPCTLS expects; the two have to agree,
	// so this is set from the same material the agents are mounted.
	creds credentials.TransportCredentials
}

// NewGRPCAgentClient builds a client that keeps its connections and dials
// in plaintext, which is what an agent without GRPCTLS serves.
func NewGRPCAgentClient() *GRPCAgentClient {
	return &GRPCAgentClient{conns: map[string]*grpc.ClientConn{}}
}

// NewGRPCAgentClientTLS builds one that presents certFile/keyFile and
// verifies the agent against caFile. serverName is the name the agents'
// certificates carry; empty uses the dial address.
func NewGRPCAgentClientTLS(certFile, keyFile, caFile, serverName string) (*GRPCAgentClient, error) {
	creds, err := grpccreds.Dialer(certFile, keyFile, caFile, serverName, false)
	if err != nil {
		return nil, err
	}
	return &GRPCAgentClient{conns: map[string]*grpc.ClientConn{}, creds: creds}, nil
}

// Close drops every kept connection.
func (c *GRPCAgentClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for addr, cc := range c.conns {
		_ = cc.Close()
		delete(c.conns, addr)
	}
}

// drop forgets a connection that could not be used, so the next call
// dials rather than waiting on the same broken one.
func (c *GRPCAgentClient) drop(addr string, cc *grpc.ClientConn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conns[addr] == cc {
		delete(c.conns, addr)
		_ = cc.Close()
	}
}

const (
	agentDialTimeout = 3 * time.Second
	// agentCallTimeout bounds every RPC after the dial; a wedged agent must
	// fail the reconcile, not hang it.
	agentCallTimeout = 30 * time.Second
	// agentPromoteTimeout bounds Promote and Demote: pg_ctl promote -w plus
	// the post-promote CHECKPOINT can legitimately take minutes, and
	// aborting at 30s caused needless failover churn.
	agentPromoteTimeout = 2 * time.Minute
)

// dial returns the connection to addr, opening one if there is none. The
// wait for READY is kept on every call: a connection that has gone idle or
// broken is reconnected here, so a caller still fails fast on an agent it
// cannot reach rather than inside its own RPC deadline.
func (c *GRPCAgentClient) dial(ctx context.Context, addr string) (pgshardv1.AgentClient, error) {
	c.mu.Lock()
	if c.conns == nil {
		c.conns = map[string]*grpc.ClientConn{}
	}
	conn, ok := c.conns[addr]
	if !ok {
		var err error
		tc := c.creds
		if tc == nil {
			tc = insecure.NewCredentials()
		}
		conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(tc))
		if err != nil {
			c.mu.Unlock()
			return nil, err
		}
		c.conns[addr] = conn
	}
	c.mu.Unlock()

	dialCtx, cancel := context.WithTimeout(ctx, agentDialTimeout)
	defer cancel()
	conn.Connect()
	for state := conn.GetState(); !stateReady(state); state = conn.GetState() {
		if !conn.WaitForStateChange(dialCtx, state) {
			c.drop(addr, conn)
			return nil, fmt.Errorf("agent %s unreachable: %w", addr, dialCtx.Err())
		}
	}
	return pgshardv1.NewAgentClient(conn), nil
}

func stateReady(s interface{ String() string }) bool { return s.String() == "READY" }

// Status calls Agent.Status.
func (c *GRPCAgentClient) Status(ctx context.Context, addr string) (AgentStatus, error) {
	cl, err := c.dial(ctx, addr)
	if err != nil {
		return AgentStatus{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, agentDialTimeout)
	defer cancel()
	resp, err := cl.Status(ctx, &pgshardv1.StatusRequest{})
	if err != nil {
		return AgentStatus{}, err
	}
	st := AgentStatus{Running: resp.GetRunning(), Primary: resp.GetRole() == pgshardv1.StatusResponse_ROLE_PRIMARY, Epoch: resp.GetEpoch(), LSN: resp.GetLsn(), Timeline: resp.GetTimeline(), PromotionPending: resp.GetPromotionPending(), Build: resp.GetBuild()}
	if e := resp.GetError(); e != nil {
		return st, errors.New(e.GetMessage())
	}
	return st, nil
}

// Promote calls Agent.Promote and turns an embedded error into a Go error.
func (c *GRPCAgentClient) Promote(ctx context.Context, addr string, epoch uint64, holder string) error {
	cl, err := c.dial(ctx, addr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, agentPromoteTimeout)
	defer cancel()
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
func (c *GRPCAgentClient) Demote(ctx context.Context, addr string, epoch uint64) error {
	cl, err := c.dial(ctx, addr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, agentPromoteTimeout)
	defer cancel()
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
func (c *GRPCAgentClient) Reload(ctx context.Context, addr string) (string, error) {
	cl, err := c.dial(ctx, addr)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, agentCallTimeout)
	defer cancel()
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

// BackupResult is the operator's view of a completed agent backup.
type BackupResult struct {
	Label        string
	Type         string
	StartLSN     uint64
	StopLSN      uint64
	ArchiveStart string
	ArchiveStop  string
	SizeBytes    uint64
	RepoBytes    uint64
	StartedAt    int64
	FinishedAt   int64
	Log          []string
}

// RepoInfo is the operator's view of a stanza in the repository.
type RepoInfo struct {
	Stanza        string
	StatusCode    int64
	StatusMessage string
	ArchiveMin    string
	ArchiveMax    string
	Backups       []BackupResult
}

// BackupAgentClient drives the backup RPCs of member agents.
type BackupAgentClient interface {
	// Backup runs pgbackrest backup of the given type on the primary at addr;
	// t is full, diff or incr.
	Backup(ctx context.Context, addr string, t string) (BackupResult, error)
	// Expire applies retention on the primary at addr.
	Expire(ctx context.Context, addr string) error
	// Info reads the repository contents through the agent at addr.
	Info(ctx context.Context, addr string) (RepoInfo, error)
}

func backupResultFromProto(i *pgshardv1.BackupInfo) BackupResult {
	return BackupResult{Label: i.GetLabel(), Type: i.GetType(), StartLSN: i.GetStartLsn(), StopLSN: i.GetStopLsn(),
		ArchiveStart: i.GetArchiveStart(), ArchiveStop: i.GetArchiveStop(), SizeBytes: i.GetSizeBytes(), RepoBytes: i.GetRepoSizeBytes(),
		StartedAt: i.GetStartedAt(), FinishedAt: i.GetFinishedAt()}
}

// Backup reads the agent's epoch and calls Agent.Backup at that epoch.
func (c *GRPCAgentClient) Backup(ctx context.Context, addr string, t string) (BackupResult, error) {
	cl, err := c.dial(ctx, addr)
	if err != nil {
		return BackupResult{}, err
	}
	// The backup itself runs pgbackrest synchronously and is bounded by the
	// caller (backupRunTimeout); only the epoch probe gets a short deadline.
	sctx, scancel := context.WithTimeout(ctx, agentCallTimeout)
	st, err := cl.Status(sctx, &pgshardv1.StatusRequest{})
	scancel()
	if err != nil {
		return BackupResult{}, err
	}
	var kind pgshardv1.BackupRequest_Type
	switch t {
	case "full":
		kind = pgshardv1.BackupRequest_TYPE_FULL
	case "diff":
		kind = pgshardv1.BackupRequest_TYPE_DIFF
	case "incr":
		kind = pgshardv1.BackupRequest_TYPE_INCR
	default:
		return BackupResult{}, fmt.Errorf("unknown backup type %q", t)
	}
	resp, err := cl.Backup(ctx, &pgshardv1.BackupRequest{Epoch: st.GetEpoch(), Type: kind})
	if err != nil {
		return BackupResult{}, err
	}
	res := backupResultFromProto(resp.GetInfo())
	res.Log = resp.GetLog()
	if e := resp.GetError(); e != nil {
		return res, fmt.Errorf("backup: %s (%s)", e.GetMessage(), e.GetSqlstate())
	}
	return res, nil
}

// Expire reads the agent's epoch and calls Agent.Expire at that epoch.
func (c *GRPCAgentClient) Expire(ctx context.Context, addr string) error {
	cl, err := c.dial(ctx, addr)
	if err != nil {
		return err
	}
	// Expire can run long on a large repo; bound only the epoch probe, not
	// the Expire RPC (the caller bounds the overall run).
	sctx, scancel := context.WithTimeout(ctx, agentCallTimeout)
	st, err := cl.Status(sctx, &pgshardv1.StatusRequest{})
	scancel()
	if err != nil {
		return err
	}
	resp, err := cl.Expire(ctx, &pgshardv1.ExpireRequest{Epoch: st.GetEpoch()})
	if err != nil {
		return err
	}
	if e := resp.GetError(); e != nil {
		return fmt.Errorf("expire: %s (%s)", e.GetMessage(), e.GetSqlstate())
	}
	return nil
}

// Info calls Agent.RestoreInfo.
func (c *GRPCAgentClient) Info(ctx context.Context, addr string) (RepoInfo, error) {
	cl, err := c.dial(ctx, addr)
	if err != nil {
		return RepoInfo{}, err
	}
	resp, err := cl.RestoreInfo(ctx, &pgshardv1.RestoreInfoRequest{})
	if err != nil {
		return RepoInfo{}, err
	}
	if e := resp.GetError(); e != nil {
		return RepoInfo{}, fmt.Errorf("info: %s (%s)", e.GetMessage(), e.GetSqlstate())
	}
	info := RepoInfo{Stanza: resp.GetStanza(), StatusCode: resp.GetStatusCode(), StatusMessage: resp.GetStatusMessage(), ArchiveMin: resp.GetArchiveMin(), ArchiveMax: resp.GetArchiveMax()}
	for _, b := range resp.GetBackups() {
		info.Backups = append(info.Backups, backupResultFromProto(b))
	}
	return info, nil
}

// SetSynchronizedStandbySlots reads the agent's epoch and calls
// Agent.SetSynchronizedStandbySlots at that epoch.
func (c *GRPCAgentClient) SetSynchronizedStandbySlots(ctx context.Context, addr string, slots []string) ([]string, error) {
	cl, err := c.dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, agentCallTimeout)
	defer cancel()
	st, err := cl.Status(ctx, &pgshardv1.StatusRequest{})
	if err != nil {
		return nil, err
	}
	resp, err := cl.SetSynchronizedStandbySlots(ctx, &pgshardv1.SetSynchronizedStandbySlotsRequest{Epoch: st.GetEpoch(), Slots: slots})
	if err != nil {
		return nil, err
	}
	if e := resp.GetError(); e != nil {
		return nil, fmt.Errorf("set synchronized standby slots: %s (%s)", e.GetMessage(), e.GetSqlstate())
	}
	return resp.GetApplied(), nil
}
