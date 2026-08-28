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
	if err := p.PublishShardStatus(ctx, dsn, []ShardStatus{{Group: g, Epoch: 3, Endpoint: "rs-shard-0-rw:5432"}}); err != nil {
		t.Fatal(err)
	}
	first := shardStatusUpdatedAt(t, conn, g)

	// Three more identical passes, as a healthy cluster would do.
	for i := 0; i < 3; i++ {
		if err := p.PublishShardStatus(ctx, dsn, []ShardStatus{{Group: g, Epoch: 3, Endpoint: "rs-shard-0-rw:5432"}}); err != nil {
			t.Fatal(err)
		}
	}
	if again := shardStatusUpdatedAt(t, conn, g); !again.Equal(first) {
		t.Fatalf("an unchanged reconcile rewrote the row (updated_at %s -> %s), firing a serving notification and a full reload on every watcher", first, again)
	}

	// A real change must still land.
	if err := p.PublishShardStatus(ctx, dsn, []ShardStatus{{Group: g, Epoch: 4, Endpoint: "rs-shard-0-rw:5432"}}); err != nil {
		t.Fatal(err)
	}
	afterEpoch := shardStatusUpdatedAt(t, conn, g)
	if afterEpoch.Equal(first) {
		t.Fatal("an epoch change did not update the row")
	}
	if err := p.PublishShardStatus(ctx, dsn, []ShardStatus{{Group: g, Epoch: 4, Endpoint: "rs-shard-0-other:5432"}}); err != nil {
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

// TestCatalogCopyClearsAShardMapItCannotTruncate: the catalog copy
// truncated every table in the pgshard schema before subscribing, to make
// itself re-runnable. The shard-map tables refuse TRUNCATE outright --
// row-level constraint triggers do not fire on it, so truncating would
// empty the map past the coverage, numbering and workflow-ownership checks
// -- and a freshly migrated target carries a bootstrap shard set row that
// has to go. So the copy could never start, and the catalog half of a
// major upgrade never completed: stage stayed "copying" with the reason
// only in the cluster's status message.
func TestCatalogCopyClearsAShardMapItCannotTruncate(t *testing.T) {
	if err := exec.Command("docker", "info").Run(); err != nil {
		dockertest.Unavailable(t, "docker unavailable")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, startProbePostgres(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}

	// The bootstrap row a fresh catalog carries, and a row in a table that
	// is truncated the ordinary way.
	if _, err := conn.Exec(ctx, `INSERT INTO pgshard.databases (name) VALUES ('app')`); err != nil {
		t.Fatal(err)
	}
	if n := probeCount(t, conn, `SELECT count(*) FROM pgshard.shard_sets`); n == 0 {
		t.Fatal("the fixture must start with a shard set to clear")
	}

	if err := clearCatalogSchema(ctx, conn, "pgshard"); err != nil {
		t.Fatalf("the copy must be able to clear a fresh catalog: %v", err)
	}
	for _, q := range []string{
		`SELECT count(*) FROM pgshard.shard_sets`,
		`SELECT count(*) FROM pgshard.shard_ranges`,
		`SELECT count(*) FROM pgshard.databases`,
	} {
		if n := probeCount(t, conn, q); n != 0 {
			t.Errorf("%s left %d rows behind", q, n)
		}
	}
	// Re-runnable: clearing an already-empty catalog is not an error.
	if err := clearCatalogSchema(ctx, conn, "pgshard"); err != nil {
		t.Fatalf("clearing twice: %v", err)
	}
	// The guard is still there for everyone else.
	if _, err := conn.Exec(ctx, `TRUNCATE pgshard.shard_sets`); err == nil {
		t.Error("the shard map must still refuse TRUNCATE")
	}
}

func probeCount(t *testing.T, conn *pgx.Conn, sql string) int {
	t.Helper()
	var n int
	if err := conn.QueryRow(context.Background(), sql).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestPublishShardStatusWritesEveryShardAtOnce: a pass published one
// group per connection, so a large topology paid a connect and a round
// trip per shard. The whole set now goes in one statement, which must
// still refuse to lower an epoch and still leave an unchanged row alone.
func TestPublishShardStatusWritesEveryShardAtOnce(t *testing.T) {
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

	p := PgxProber{}
	group := func(id int) Group { return Group{Cluster: "rs", Kind: "shard", ShardID: id, Generation: 1} }
	var rows []ShardStatus
	for id := range 4 {
		rows = append(rows, ShardStatus{Group: group(id), Epoch: 3, Endpoint: fmt.Sprintf("rs-shard-%d-rw:5432", id)})
	}
	if err := p.PublishShardStatus(ctx, dsn, rows); err != nil {
		t.Fatal(err)
	}
	for id := range 4 {
		if ep := shardStatusEndpoint(t, conn, group(id)); ep != fmt.Sprintf("rs-shard-%d-rw:5432", id) {
			t.Fatalf("shard %d published %q", id, ep)
		}
	}
	first := shardStatusUpdatedAt(t, conn, group(2))

	if err := p.PublishShardStatus(ctx, dsn, rows); err != nil {
		t.Fatal(err)
	}
	if again := shardStatusUpdatedAt(t, conn, group(2)); !again.Equal(first) {
		t.Errorf("an unchanged batch rewrote a row (updated_at %s -> %s)", first, again)
	}

	// A stale epoch among fresh ones must not lower the fence, and the
	// same shard named twice must not make PostgreSQL refuse the whole
	// statement for touching a row a second time.
	rows[2].Epoch = 2
	rows[2].Endpoint = "rs-shard-2-stale:5432"
	rows = append(rows, ShardStatus{Group: group(3), Epoch: 5, Endpoint: "rs-shard-3-promoted:5432"})
	if err := p.PublishShardStatus(ctx, dsn, rows); err != nil {
		t.Fatal(err)
	}
	if ep := shardStatusEndpoint(t, conn, group(2)); ep != "rs-shard-2-rw:5432" {
		t.Errorf("a lower epoch overwrote the fence: %q", ep)
	}
	if ep := shardStatusEndpoint(t, conn, group(3)); ep != "rs-shard-3-promoted:5432" {
		t.Errorf("the later row for a repeated shard did not win: %q", ep)
	}
}

func shardStatusEndpoint(t *testing.T, conn *pgx.Conn, g Group) string {
	t.Helper()
	var ep string
	if err := conn.QueryRow(context.Background(), "SELECT primary_endpoint FROM pgshard.shard_status WHERE shard_set = $1 AND shard_id = $2", g.ShardSet(), g.ShardID).Scan(&ep); err != nil {
		t.Fatal(err)
	}
	return ep
}
