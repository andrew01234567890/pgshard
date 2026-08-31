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

// catalogNode is one side of a catalog upgrade under test: the DSN this
// process dials it with, and the conninfo the other container's apply
// worker dials it with. They are different addresses and cannot be made
// the same -- which is the whole reason CatalogSide exists.
//
// Nothing here holds a connection open across a fence. Fencing a catalog
// terminates every other client backend, which is what it is for, so a
// test session that survived one would be testing something the operator
// never does.
type catalogNode struct {
	side CatalogSide
}

// startCatalogPair runs two PostgreSQL containers on a user-defined docker
// network, so each can reach the other by name while this process reaches
// both through published ports.
func startCatalogPair(t *testing.T, image string) (src, tgt catalogNode) {
	t.Helper()
	return startCatalogPairAcross(t, image, image)
}

// startCatalogPairAcross is startCatalogPair with a different major on each
// side, which is what a real catalog upgrade is: the old group publishes and
// the new one subscribes.
func startCatalogPairAcross(t *testing.T, srcImage, tgtImage string) (src, tgt catalogNode) {
	t.Helper()
	if err := exec.Command("docker", "info").Run(); err != nil {
		dockertest.Unavailable(t, "docker unavailable")
	}
	for _, image := range []string{srcImage, tgtImage} {
		if exec.Command("docker", "image", "inspect", image).Run() != nil {
			if out, err := exec.Command("docker", "pull", image).CombinedOutput(); err != nil {
				dockertest.Unavailable(t, "image %s unavailable: %v: %s", image, err, out)
			}
		}
	}
	net := fmt.Sprintf("pgshard-catupg-%d", time.Now().UnixNano())
	if out, err := exec.Command("docker", "network", "create", net).CombinedOutput(); err != nil {
		dockertest.Unavailable(t, "docker network create: %v: %s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "network", "rm", net).Run() })

	start := func(name, image string) catalogNode {
		out, err := exec.Command("docker", "run", "-d", "--rm", "--network", net, "--name", name,
			"-p", "127.0.0.1::5432", "--entrypoint", "sh", image, "-ec",
			`initdb -D /tmp/pgdata --auth=trust -U postgres --no-sync >/dev/null &&
			 echo "host all all all trust" >> /tmp/pgdata/pg_hba.conf &&
			 exec postgres -D /tmp/pgdata -c listen_addresses='*' -c wal_level=logical -c max_replication_slots=8 -c max_wal_senders=8`).CombinedOutput()
		if err != nil {
			t.Fatalf("docker run %s: %v: %s", name, err, out)
		}
		id := strings.TrimSpace(string(out))
		t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() })
		pout, err := exec.Command("docker", "port", id, "5432/tcp").Output()
		if err != nil {
			t.Fatalf("docker port %s: %v", name, err)
		}
		hostPort := strings.TrimSpace(strings.SplitN(string(pout), "\n", 2)[0])
		n := catalogNode{side: CatalogSide{
			DSN:      fmt.Sprintf("postgres://postgres@%s/postgres?sslmode=disable", hostPort),
			ConnInfo: fmt.Sprintf("postgres://postgres@%s:5432/postgres?sslmode=disable", name),
		}}
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			conn, cerr := pgx.Connect(ctx, n.side.DSN)
			cancel()
			if cerr == nil {
				_ = conn.Close(context.Background())
				return n
			}
			time.Sleep(300 * time.Millisecond)
		}
		t.Fatalf("%s did not become ready", name)
		return catalogNode{}
	}
	base := fmt.Sprintf("catupg-%d", time.Now().UnixNano())
	return start(base+"-src", srcImage), start(base+"-tgt", tgtImage)
}

// TestCatalogUpgradeCycleOnPostgres drives the SQL the operator's envtests
// never reach: they run a fake Prober, so they prove the reconciler's
// ordering and not one line of what the ordering calls. This is the whole
// cycle on two real servers -- copy, catch up, cut over, a write only the
// new catalog took, and a rollback that has to bring that write back.
func TestCatalogUpgradeCycleOnPostgres(t *testing.T) {
	for _, image := range []string{
		"ghcr.io/andrew01234567890/pgshard-postgres:18",
		"ghcr.io/andrew01234567890/pgshard-postgres:19",
	} {
		t.Run(image[strings.LastIndex(image, ":")+1:], func(t *testing.T) {
			t.Parallel()
			testCatalogUpgradeCycle(t, image)
		})
	}
}

// TestCatalogUpgradeAcrossMajorsOnPostgres is the pair the upgrade actually
// runs: an 18 catalog publishing to a 19 one. The same-major cases above
// prove the mechanics; this proves they hold when the two sides are not the
// same server version, which is the only configuration a real catalog
// upgrade ever has.
//
// The e2e upgrade suite does not cover it: it rolls back before the catalog
// upgrade begins, so catalog rollback across majors was never exercised
// anywhere.
func TestCatalogUpgradeAcrossMajorsOnPostgres(t *testing.T) {
	ctx := context.Background()
	src, tgt := startCatalogPairAcross(t,
		"ghcr.io/andrew01234567890/pgshard-postgres:18",
		"ghcr.io/andrew01234567890/pgshard-postgres:19")
	runCatalogUpgradeCycle(ctx, t, src, tgt)
}

func testCatalogUpgradeCycle(t *testing.T, image string) {
	ctx := context.Background()
	src, tgt := startCatalogPair(t, image)
	runCatalogUpgradeCycle(ctx, t, src, tgt)
}

func runCatalogUpgradeCycle(ctx context.Context, t *testing.T, src, tgt catalogNode) {
	for _, n := range []catalogNode{src, tgt} {
		conn := dialCatalog(t, n.side.DSN)
		err := catalog.Migrate(ctx, conn)
		_ = conn.Close(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}
	execOn(t, src.side.DSN, `INSERT INTO pgshard.databases (name) VALUES ('before-cutover')`)
	// Logical replication does not carry sequences, so the cutover copies
	// their positions itself. Advance one first: left behind, the new
	// catalog would hand out generations the old one already used, and
	// every reader keyed on desired_generation would see two different
	// states under one number.
	for range 5 {
		_ = queryOn[int64](t, src.side.DSN, `SELECT nextval('pgshard.desired_generation_seq')`)
	}
	srcSeq := queryOn[int64](t, src.side.DSN, `SELECT last_value FROM pgshard.desired_generation_seq`)

	p := PgxProber{}
	if err := p.EnsureCatalogCopy(ctx, src.side, tgt.side); err != nil {
		t.Fatalf("ensure copy: %v", err)
	}
	waitCatalogRow(t, tgt.side.DSN, "before-cutover", "the copy never reached the target")

	caughtUp := false
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); {
		ok, lag, err := p.CatalogCopyCaughtUp(ctx, src.side.DSN)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			caughtUp = true
			break
		}
		_ = lag
		time.Sleep(200 * time.Millisecond)
	}
	if !caughtUp {
		t.Fatal("the copy never reported caught up")
	}

	if err := p.CutoverCatalog(ctx, src.side, tgt.side); err != nil {
		t.Fatalf("cutover: %v", err)
	}
	// The old catalog is fenced and the forward subscription is gone.
	if ro := queryOn[bool](t, src.side.DSN, `SELECT current_setting('default_transaction_read_only')::bool`); !ro {
		t.Error("the old catalog still takes writes after the cutover")
	}
	if n := queryOn[int64](t, tgt.side.DSN, `SELECT count(*) FROM pg_subscription WHERE subname = $1`, CatalogUpgradeSubscription); n != 0 {
		t.Error("the forward subscription outlived the cutover")
	}
	// The reverse pair is armed, disabled, with its slot on the new side.
	// The sequence came with it: the next generation the new catalog hands
	// out is past every one the old catalog issued.
	if got := queryOn[int64](t, tgt.side.DSN, `SELECT last_value FROM pgshard.desired_generation_seq`); got < srcSeq {
		t.Errorf("the new catalog restarts desired_generation at %d, behind the %d the old one reached", got, srcSeq)
	}
	if next := queryOn[int64](t, tgt.side.DSN, `SELECT nextval('pgshard.desired_generation_seq')`); next <= srcSeq {
		t.Errorf("the new catalog hands out generation %d, which the old catalog already used (reached %d)", next, srcSeq)
	}
	if n := queryOn[int64](t, tgt.side.DSN, `SELECT count(*) FROM pg_publication WHERE pubname = $1`, CatalogRollbackPublication); n != 1 {
		t.Fatal("no rollback publication: there would be no way back")
	}
	if n := queryOn[int64](t, src.side.DSN, `SELECT count(*) FROM pg_subscription WHERE subname = $1 AND NOT subenabled`, CatalogRollbackSubscription); n != 1 {
		t.Fatal("no disabled rollback subscription on the old catalog")
	}

	// A write only the new catalog took. Losing this is the data loss the
	// reverse stream exists to prevent.
	execOn(t, tgt.side.DSN, `INSERT INTO pgshard.databases (name) VALUES ('after-cutover')`)

	if err := p.RollbackCatalog(ctx, src.side.DSN, tgt.side.DSN); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if n := queryOn[int64](t, src.side.DSN, `SELECT count(*) FROM pgshard.databases WHERE name = 'after-cutover'`); n != 1 {
		t.Fatal("the rollback threw away what the new catalog accepted")
	}
	if ro := queryOn[bool](t, src.side.DSN, `SELECT current_setting('default_transaction_read_only')::bool`); ro {
		t.Error("the catalog serving again is still fenced")
	}
	if ro := queryOn[bool](t, tgt.side.DSN, `SELECT current_setting('default_transaction_read_only')::bool`); !ro {
		t.Error("the rolled-back catalog still takes writes")
	}
	if n := queryOn[int64](t, src.side.DSN, `SELECT count(*) FROM pg_subscription WHERE subname = $1`, CatalogRollbackSubscription); n != 0 {
		t.Error("the rollback subscription outlived the replay")
	}

	// A second rollback is a no-op rather than a refusal: the publication
	// left behind is what tells a finished replay from a cutover that never
	// had a way back.
	if err := p.RollbackCatalog(ctx, src.side.DSN, tgt.side.DSN); err != nil {
		t.Fatalf("second rollback: %v", err)
	}

	// Dropping the subscription took its slot with it: an abandoned slot on
	// the catalog nobody is using would pin WAL there for ever.
	if n := queryOn[int64](t, tgt.side.DSN, `SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1`, CatalogRollbackSubscription); n != 0 {
		t.Error("the reverse slot outlived the replay that finished with it")
	}
}

// TestCatalogUpgradeRetirementDropsTheReverseSlotOnPostgres is the other
// ending: the upgrade is kept, the old group is retired, and the reverse
// slot it left on the serving catalog has to go with it. An unused slot
// pins WAL for ever, and this one is on the catalog every write goes to.
func TestCatalogUpgradeRetirementDropsTheReverseSlotOnPostgres(t *testing.T) {
	ctx := context.Background()
	src, tgt := startCatalogPair(t, "ghcr.io/andrew01234567890/pgshard-postgres:18")
	for _, n := range []catalogNode{src, tgt} {
		conn := dialCatalog(t, n.side.DSN)
		err := catalog.Migrate(ctx, conn)
		_ = conn.Close(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}
	p := PgxProber{}
	if err := p.EnsureCatalogCopy(ctx, src.side, tgt.side); err != nil {
		t.Fatalf("ensure copy: %v", err)
	}
	if err := p.CutoverCatalog(ctx, src.side, tgt.side); err != nil {
		t.Fatalf("cutover: %v", err)
	}
	if n := queryOn[int64](t, tgt.side.DSN, `SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1`, CatalogRollbackSubscription); n != 1 {
		t.Fatal("the cutover left no reverse slot, so there was no way back to drop")
	}

	if err := p.DropCatalogRollback(ctx, tgt.side.DSN); err != nil {
		t.Fatalf("drop rollback: %v", err)
	}
	for _, c := range []struct {
		what, sql string
	}{
		{"slot", `SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1`},
		{"publication", `SELECT count(*) FROM pg_publication WHERE pubname = $1`},
	} {
		if n := queryOn[int64](t, tgt.side.DSN, c.sql, CatalogRollbackSubscription); n != 0 {
			t.Errorf("the reverse %s outlived the rollback window", c.what)
		}
	}
	// Running again is a no-op, because retirement is retried.
	if err := p.DropCatalogRollback(ctx, tgt.side.DSN); err != nil {
		t.Fatalf("second drop: %v", err)
	}
}

func waitCatalogRow(t *testing.T, dsn, name, msg string) {
	t.Helper()
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); {
		if queryOn[int64](t, dsn, `SELECT count(*) FROM pgshard.databases WHERE name = $1`, name) == 1 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal(msg)
}

func dialCatalog(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return conn
}

// queryOn dials for one query. A fence terminates every other client
// backend, so a connection kept between assertions would be the first
// casualty of the thing under test.
func queryOn[T any](t *testing.T, dsn, sql string, args ...any) T {
	t.Helper()
	conn := dialCatalog(t, dsn)
	defer func() { _ = conn.Close(context.Background()) }()
	var v T
	if err := conn.QueryRow(context.Background(), sql, args...).Scan(&v); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return v
}

func execOn(t *testing.T, dsn, sql string) {
	t.Helper()
	conn := dialCatalog(t, dsn)
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(context.Background(), sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}
