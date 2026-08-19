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
func (c GRPCAgentClient) Backup(ctx context.Context, addr string, t string) (BackupResult, error) {
	conn, cl, err := c.dial(ctx, addr)
	if err != nil {
		return BackupResult{}, err
	}
	defer func() { _ = conn.Close() }()
	st, err := cl.Status(ctx, &pgshardv1.StatusRequest{})
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
func (c GRPCAgentClient) Expire(ctx context.Context, addr string) error {
	conn, cl, err := c.dial(ctx, addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	st, err := cl.Status(ctx, &pgshardv1.StatusRequest{})
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
func (c GRPCAgentClient) Info(ctx context.Context, addr string) (RepoInfo, error) {
	conn, cl, err := c.dial(ctx, addr)
	if err != nil {
		return RepoInfo{}, err
	}
	defer func() { _ = conn.Close() }()
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
