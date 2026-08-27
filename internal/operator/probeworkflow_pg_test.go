package operator

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/dockertest"
)

// TestReshardWorkflowSeesUpgradeRunsOnPostgres: an online major upgrade is
// recorded with kind 'upgrade' and reuses the reshard cutover, but the probe
// filtered on kind = 'reshard' alone. mirrorCutoverSpec therefore had nothing
// to mirror for an upgrade: its proceed gates, rollback request, completion
// and retirement were all invisible.
func TestReshardWorkflowSeesUpgradeRunsOnPostgres(t *testing.T) {
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

	mustProbeExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state, spec, status)
		VALUES (gen_random_uuid(), 'upgrade', 'running', '{"shard_set":"g2"}'::jsonb,
		        '{"stage":"switching","message":"carrying sequences"}'::jsonb)`)

	w, err := PgxProber{}.ReshardWorkflow(ctx, dsn, "g2")
	if err != nil {
		t.Fatal(err)
	}
	if w.ID == "" {
		t.Fatal("an upgrade workflow targeting g2 was invisible to the probe")
	}
	if w.Stage != "switching" || w.State != "running" {
		t.Fatalf("state=%q stage=%q", w.State, w.Stage)
	}

	// A set can be the target of a reshard and later an upgrade. The probe
	// returns one workflow, so it must be the newest rather than whichever
	// kind sorts first.
	time.Sleep(time.Second)
	mustProbeExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state, spec, status)
		VALUES (gen_random_uuid(), 'reshard', 'running', '{"shard_set":"g2"}'::jsonb,
		        '{"stage":"copying","message":"newer"}'::jsonb)`)
	w2, err := PgxProber{}.ReshardWorkflow(ctx, dsn, "g2")
	if err != nil {
		t.Fatal(err)
	}
	if w2.Stage != "copying" {
		t.Fatalf("stage=%q: the probe did not return the newest workflow for the set", w2.Stage)
	}
}

func mustProbeExec(t *testing.T, conn *pgx.Conn, sql string) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// startProbePostgres runs a throwaway PostgreSQL for the probe's SQL. The
// operator package has no other container-backed test, so this is a local
// helper rather than a shared fixture.
func startProbePostgres(t *testing.T) string {
	t.Helper()
	const image = "ghcr.io/andrew01234567890/pgshard-postgres:18"
	if exec.Command("docker", "image", "inspect", image).Run() != nil {
		if out, err := exec.Command("docker", "pull", image).CombinedOutput(); err != nil {
			dockertest.Unavailable(t, "image %s unavailable: %v: %s", image, err, out)
		}
	}
	out, err := exec.Command("docker", "run", "-d", "--rm", "-p", "127.0.0.1::5432",
		"--entrypoint", "sh", image, "-ec",
		`initdb -D /tmp/pgdata --auth=trust -U postgres --no-sync >/dev/null &&
		 echo "host all all all trust" >> /tmp/pgdata/pg_hba.conf &&
		 exec postgres -D /tmp/pgdata -c listen_addresses='*'`).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run: %v: %s", err, out)
	}
	id := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() })

	pout, err := exec.Command("docker", "port", id, "5432/tcp").Output()
	if err != nil {
		t.Fatalf("docker port: %v", err)
	}
	hostPort := strings.TrimSpace(strings.SplitN(string(pout), "\n", 2)[0])
	dsn := fmt.Sprintf("postgres://postgres@%s/postgres?sslmode=disable", hostPort)

	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, cerr := pgx.Connect(ctx, dsn)
		cancel()
		if cerr == nil {
			_ = conn.Close(context.Background())
			return dsn
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("postgres did not become ready")
	return ""
}
