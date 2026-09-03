package operator

import (
	"context"
	"os/exec"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/dockertest"
)

// TestAgentMTLSReachesTheCatalogOnItsOwn: the controller dials member agents
// and cannot see pods, so the mode has to travel through the catalog. It also
// has to travel ALONE: a member restarting into the requirement changes
// nothing else on its shard_status row, and the upsert only writes when
// something differs -- so left out of that test the new mode would sit
// unwritten for as long as the epoch and endpoint held still, and the
// controller would keep dialling the old way.
func TestAgentMTLSReachesTheCatalogOnItsOwn(t *testing.T) {
	if err := exec.Command("docker", "info").Run(); err != nil {
		dockertest.Unavailable(t, "docker unavailable")
	}
	ctx := context.Background()
	dsn := startProbePostgres(t)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}

	g := Group{Cluster: "am", Kind: "shard", ShardID: 0, Generation: 1}
	p := PgxProber{}
	row := ShardStatus{Group: g, Epoch: 3, Endpoint: "am-shard-0-rw:5432"}
	if err := p.PublishShardStatus(ctx, dsn, []ShardStatus{row}); err != nil {
		t.Fatal(err)
	}
	if got := shardAgentMTLS(t, conn, g); got {
		t.Fatal("a shard published without agentMTLS says its agent requires TLS")
	}

	// The primary restarts into the requirement. Epoch and endpoint are
	// unchanged -- this is the whole of the difference.
	row.AgentMTLS = true
	if err := p.PublishShardStatus(ctx, dsn, []ShardStatus{row}); err != nil {
		t.Fatal(err)
	}
	if got := shardAgentMTLS(t, conn, g); !got {
		t.Fatal("the mode did not reach the catalog: the controller would go on dialling this agent in plaintext after it began requiring TLS")
	}

	// And back, which is what a rollback of the setting looks like.
	row.AgentMTLS = false
	if err := p.PublishShardStatus(ctx, dsn, []ShardStatus{row}); err != nil {
		t.Fatal(err)
	}
	if got := shardAgentMTLS(t, conn, g); got {
		t.Fatal("turning agentMTLS off did not reach the catalog")
	}
}

func shardAgentMTLS(t *testing.T, conn *pgx.Conn, g Group) bool {
	t.Helper()
	var v bool
	if err := conn.QueryRow(context.Background(),
		`SELECT agent_mtls FROM pgshard.shard_status WHERE shard_set = $1 AND shard_id = $2`,
		g.ShardSet(), g.ShardID).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}
