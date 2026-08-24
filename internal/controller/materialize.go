package controller

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/andrew01234567890/pgshard/internal/agentauth"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/schemacopy"
)

// SchemaMaterializer copies the schema of one database from a source
// (reachable through sourceConnInfo) into the same-named, already created
// and empty database on a target shard.
type SchemaMaterializer interface {
	MaterializeSchema(ctx context.Context, target ShardRef, database, sourceConnInfo string) error
}

// DefaultAgentPort is the gRPC port of member agents.
const DefaultAgentPort = 9090

// AgentMaterializer asks the agent of the target's primary to run pg_dump
// and psql inside its pod; the agent's address is the primary endpoint of
// pgshard.shard_status with the agent port.
type AgentMaterializer struct {
	Pool *pgxpool.Pool
	// Port defaults to DefaultAgentPort.
	Port int
	// Timeout bounds one materialization; zero means 10 minutes.
	Timeout time.Duration
}

// MaterializeSchema implements SchemaMaterializer.
func (m *AgentMaterializer) MaterializeSchema(ctx context.Context, target ShardRef, database, sourceConnInfo string) error {
	var endpoint *string
	if err := m.Pool.QueryRow(ctx, `SELECT primary_endpoint FROM pgshard.shard_status WHERE shard_set = $1 AND shard_id = $2`, target.Set, target.ID).Scan(&endpoint); err != nil {
		return fmt.Errorf("target %s/%d: %w", target.Set, target.ID, err)
	}
	if endpoint == nil || *endpoint == "" {
		return fmt.Errorf("target %s/%d has no primary endpoint", target.Set, target.ID)
	}
	addr, err := AgentAddr(*endpoint, m.Port)
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	timeout := m.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	// The catalog pool authenticates with the superuser password the agents
	// derive their auth token from.
	ctx = agentauth.WithToken(ctx, agentauth.Token(m.Pool.Config().ConnConfig.Password))
	resp, err := pgshardv1.NewAgentClient(conn).MaterializeSchema(ctx, &pgshardv1.MaterializeSchemaRequest{SourceConninfo: sourceConnInfo, Database: database})
	if err != nil {
		return fmt.Errorf("agent %s: %w", addr, err)
	}
	if e := resp.GetError(); e != nil {
		return fmt.Errorf("agent %s: %s", addr, e.GetMessage())
	}
	return nil
}

// AgentAddr turns a primary endpoint (host:5432) into the agent's host:port.
func AgentAddr(endpoint string, port int) (string, error) {
	host, _, err := net.SplitHostPort(endpoint)
	if err != nil {
		host = endpoint
	}
	if host == "" {
		return "", fmt.Errorf("endpoint %q has no host", endpoint)
	}
	if port <= 0 {
		port = DefaultAgentPort
	}
	return net.JoinHostPort(host, fmt.Sprint(port)), nil
}

// ExecMaterializer runs pg_dump and psql from BinDir on the controller
// host: the development path when PostgreSQL binaries are available.
type ExecMaterializer struct {
	BinDir string
	// TargetConnInfo resolves the libpq connection string of a target
	// database as seen from the controller host.
	TargetConnInfo func(ctx context.Context, target ShardRef, database string) (string, error)
}

// MaterializeSchema implements SchemaMaterializer.
func (m *ExecMaterializer) MaterializeSchema(ctx context.Context, target ShardRef, database, sourceConnInfo string) error {
	targetConnInfo, err := m.TargetConnInfo(ctx, target, database)
	if err != nil {
		return err
	}
	dump := exec.CommandContext(ctx, filepath.Join(m.BinDir, "pg_dump"), schemacopy.DumpArgs(sourceConnInfo)...)
	restore := exec.CommandContext(ctx, filepath.Join(m.BinDir, "psql"), schemacopy.RestoreArgs(targetConnInfo)...)
	return schemacopy.Run(dump, restore, nil)
}

var errNoMaterializer = errors.New("no schema materializer configured")

// noMaterializer refuses every request; the copier reports it on the
// workflow instead of silently skipping schema materialization.
type noMaterializer struct{}

func (noMaterializer) MaterializeSchema(context.Context, ShardRef, string, string) error {
	return errNoMaterializer
}
