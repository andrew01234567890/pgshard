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

// TestPublishShardStatusIsQuietWhenNothingChanged: a Ready cluster reconciles
// every 30 seconds and upserts once per shard. The epoch guard was
// primary_epoch <= EXCLUDED.primary_epoch, which an EQUAL epoch satisfies, so
// every pass rewrote an unchanged row and bumped updated_at. Each write fires
// notify_serving, and every router and pooler watcher answers by reloading
// ranges, statuses, databases, tables, rewrites, fences and sequences.
func TestPublishShardStatusIsQuietWhenNothingChanged(t *testing.T) {
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

	g := Group{Cluster: "rs", Kind: "shard", ShardID: 0, Generation: 1}
	p := PgxProber{}
	if err := p.PublishShardStatus(ctx, dsn, g, 3, "rs-shard-0-rw:5432"); err != nil {
		t.Fatal(err)
	}
	first := shardStatusUpdatedAt(t, conn, g)

	// Three more identical passes, as a healthy cluster would do.
	for i := 0; i < 3; i++ {
		if err := p.PublishShardStatus(ctx, dsn, g, 3, "rs-shard-0-rw:5432"); err != nil {
			t.Fatal(err)
		}
	}
	if again := shardStatusUpdatedAt(t, conn, g); !again.Equal(first) {
		t.Fatalf("an unchanged reconcile rewrote the row (updated_at %s -> %s), firing a serving notification and a full reload on every watcher", first, again)
	}

	// A real change must still land.
	if err := p.PublishShardStatus(ctx, dsn, g, 4, "rs-shard-0-rw:5432"); err != nil {
		t.Fatal(err)
	}
	afterEpoch := shardStatusUpdatedAt(t, conn, g)
	if afterEpoch.Equal(first) {
		t.Fatal("an epoch change did not update the row")
	}
	if err := p.PublishShardStatus(ctx, dsn, g, 4, "rs-shard-0-other:5432"); err != nil {
		t.Fatal(err)
	}
	if shardStatusUpdatedAt(t, conn, g).Equal(afterEpoch) {
		t.Fatal("an endpoint change did not update the row")
	}
}

func shardStatusUpdatedAt(t *testing.T, conn *pgx.Conn, g Group) time.Time {
	t.Helper()
	var at time.Time
	if err := conn.QueryRow(context.Background(),
		`SELECT updated_at FROM pgshard.shard_status WHERE shard_set = $1 AND shard_id = $2`,
		g.ShardSet(), g.ShardID).Scan(&at); err != nil {
		t.Fatal(err)
	}
	return at
}

// TestReshardWorkflowReportsJournalIDs: pgshard.workflows.journal_ids and
// PgShardReshardStatus.journalIds both existed, and nothing carried one to
// the other. A non-empty journal is how a responder knows the cutover passed
// its point of no return -- the difference between backing out with a
// desired-state edit and needing the workflow's own rollback -- and it was
// visible only by connecting to the catalog directly.
func TestReshardWorkflowReportsJournalIDs(t *testing.T) {
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

	// Before the journal: the switch has not committed.
	mustProbeExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state, spec, status)
		VALUES ('00000000-0000-0000-0000-0000000000aa'::uuid, 'reshard', 'running',
		        '{"shard_set":"g2"}'::jsonb, '{"stage":"switching"}'::jsonb)`)
	w, err := PgxProber{}.ReshardWorkflow(ctx, dsn, "g2")
	if err != nil {
		t.Fatal(err)
	}
	if len(w.JournalIDs) != 0 {
		t.Fatalf("journal ids before the journal step: %v", w.JournalIDs)
	}

	// After the journal: the point of no return has passed.
	mustProbeExec(t, conn, `UPDATE pgshard.workflows
		SET journal_ids = ARRAY['j-1','j-2'] WHERE id = '00000000-0000-0000-0000-0000000000aa'::uuid`)
	w, err = PgxProber{}.ReshardWorkflow(ctx, dsn, "g2")
	if err != nil {
		t.Fatal(err)
	}
	if len(w.JournalIDs) != 2 || w.JournalIDs[0] != "j-1" || w.JournalIDs[1] != "j-2" {
		t.Fatalf("journal ids = %v, want [j-1 j-2]: a Kubernetes-only responder cannot tell the switch committed", w.JournalIDs)
	}
}

// TestCertifiedBarrierReadsTheCatalogRowOnPostgres: the fake in the unit
// test answers whatever name it is handed, so it cannot tell the barrier's
// own name from the WAL restore point's. Only a real catalog can, and
// asking for the wrong one finds no row -- which reads as uncertified and
// turns the gate into a blanket refusal of the barriers it exists to admit.
func TestCertifiedBarrierReadsTheCatalogRowOnPostgres(t *testing.T) {
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
	mustProbeExec(t, conn, `INSERT INTO pgshard.restore_points (id, name, shard_map_generation, per_group, certified)
		VALUES (gen_random_uuid(), 'nightly', 7, '{}'::jsonb, true),
		       (gen_random_uuid(), 'aborted', 7, '{}'::jsonb, false)`)

	for _, c := range []struct {
		name string
		want bool
	}{
		{"nightly", true},
		{"aborted", false},
		{"never-taken", false},
		// The name the recovery target uses, which is not what the row is
		// keyed by. This is the case the fake could never fail on.
		{BarrierRestorePoint("nightly"), false},
	} {
		got, err := PgxProber{}.CertifiedBarrier(ctx, dsn, "", c.name)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got != c.want {
			t.Errorf("CertifiedBarrier(%q) = %v, want %v", c.name, got, c.want)
		}
	}
}
