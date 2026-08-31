package operator

import (
	"context"
	"os/exec"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/dockertest"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// TestSeedBootstrapRolePublishesACredentialThatCanReachTheRouter: the
// router authenticates against pgshard.roles alone and the migrations
// leave it empty, so the documented first-login flow was circular -- a
// catalog role was needed to create the first catalog role. The one
// credential that does exist is the operator's generated superuser
// password, and this is what makes it usable.
func TestSeedBootstrapRolePublishesACredentialThatCanReachTheRouter(t *testing.T) {
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

	const password = "s3cret-generated-by-the-operator"
	if err := (PgxProber{}).SeedBootstrapRole(ctx, dsn, superuserName, password); err != nil {
		t.Fatal(err)
	}
	var verifier string
	var login bool
	if err := conn.QueryRow(ctx, `SELECT verifier, login FROM pgshard.roles WHERE rolname = $1`, superuserName).Scan(&verifier, &login); err != nil {
		t.Fatalf("the bootstrap role is not in the catalog the router reads: %v", err)
	}
	if !login {
		t.Fatal("a bootstrap role that cannot log in bootstraps nothing")
	}
	// The verifier has to be the one a SCRAM exchange with that password
	// produces, not merely some verifier.
	parsed, err := pgwire.ParseSCRAMVerifier(verifier)
	if err != nil {
		t.Fatalf("stored verifier does not parse: %v", err)
	}
	same, err := pgwire.BuildSCRAMVerifier(password, parsed.Salt, parsed.Iterations)
	if err != nil {
		t.Fatal(err)
	}
	if same.String() != verifier {
		t.Fatal("the stored verifier does not match the password the operator generated")
	}

	// Running again writes nothing: an unchanged password must not churn
	// the row, and a verifier an operator rotated by hand must survive.
	var before, after string
	if err := conn.QueryRow(ctx, `SELECT updated_at::text FROM pgshard.roles WHERE rolname = $1`, superuserName).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := (PgxProber{}).SeedBootstrapRole(ctx, dsn, superuserName, password); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT updated_at::text FROM pgshard.roles WHERE rolname = $1`, superuserName).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("a reconcile that changed nothing rewrote the verifier")
	}

	// A rotated password is published.
	if err := (PgxProber{}).SeedBootstrapRole(ctx, dsn, superuserName, "rotated"); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT verifier FROM pgshard.roles WHERE rolname = $1`, superuserName).Scan(&verifier); err != nil {
		t.Fatal(err)
	}
	parsed, err = pgwire.ParseSCRAMVerifier(verifier)
	if err != nil {
		t.Fatal(err)
	}
	same, err = pgwire.BuildSCRAMVerifier("rotated", parsed.Salt, parsed.Iterations)
	if err != nil {
		t.Fatal(err)
	}
	if same.String() != verifier {
		t.Fatal("a rotated password did not reach the catalog")
	}
}

// TestTheSeededVerifierIsTheOneTheServerHolds: the catalog verifier and the
// server's must be the same string, not two hashes of the same password.
// SCRAM derives ClientKey from the salt, so a second verifier with a fresh
// salt gives the router a key the backend cannot accept: the router
// authenticates the client against the catalog, forwards that key, and
// PostgreSQL answers 28P01 -- a login the guide documents, failing after
// the router has already said yes.
func TestTheSeededVerifierIsTheOneTheServerHolds(t *testing.T) {
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
	// What initdb leaves: a verifier the server salted itself, which the
	// operator never chose and cannot reproduce from the password alone.
	const password = "s3cret-generated-by-the-operator"
	if _, err := conn.Exec(ctx, `ALTER ROLE `+superuserName+` PASSWORD '`+password+`'`); err != nil {
		t.Fatal(err)
	}
	var server string
	if err := conn.QueryRow(ctx, `SELECT rolpassword FROM pg_authid WHERE rolname = $1`, superuserName).Scan(&server); err != nil {
		t.Fatal(err)
	}

	if err := (PgxProber{}).SeedBootstrapRole(ctx, dsn, superuserName, password); err != nil {
		t.Fatal(err)
	}
	var seeded string
	if err := conn.QueryRow(ctx, `SELECT verifier FROM pgshard.roles WHERE rolname = $1`, superuserName).Scan(&seeded); err != nil {
		t.Fatal(err)
	}
	if seeded != server {
		t.Fatalf("the catalog holds a different verifier from the server, so a forwarded ClientKey cannot match:\n catalog %s\n server  %s", seeded, server)
	}
}

// TestEveryGroupGetsTheVerifierTheRouterForwardsAgainst: a shard group runs
// its own initdb, so the same password produces a different verifier there
// than in the catalog. The router recovers ClientKey against the catalog's
// salt, and the shard backend refused it -- 28P01 after the router had
// already accepted the client.
func TestEveryGroupGetsTheVerifierTheRouterForwardsAgainst(t *testing.T) {
	if err := exec.Command("docker", "info").Run(); err != nil {
		dockertest.Unavailable(t, "docker unavailable")
	}
	ctx := context.Background()
	const password = "s3cret-generated-by-the-operator"

	catalogDSN := startProbePostgres(t)
	cconn, err := pgx.Connect(ctx, catalogDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cconn.Close(ctx) }()
	if err := catalog.Migrate(ctx, cconn); err != nil {
		t.Fatal(err)
	}
	if _, err := cconn.Exec(ctx, `ALTER ROLE `+superuserName+` PASSWORD '`+password+`'`); err != nil {
		t.Fatal(err)
	}
	if err := (PgxProber{}).SeedBootstrapRole(ctx, catalogDSN, superuserName, password); err != nil {
		t.Fatal(err)
	}
	published, err := (PgxProber{}).BootstrapVerifier(ctx, catalogDSN, superuserName)
	if err != nil {
		t.Fatal(err)
	}
	if published == "" {
		t.Fatal("nothing published for the router to authenticate against")
	}

	shardDSN := startProbePostgres(t)
	sconn, err := pgx.Connect(ctx, shardDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sconn.Close(ctx) }()
	if _, err := sconn.Exec(ctx, `ALTER ROLE `+superuserName+` PASSWORD '`+password+`'`); err != nil {
		t.Fatal(err)
	}
	var own string
	if err := sconn.QueryRow(ctx, `SELECT rolpassword FROM pg_authid WHERE rolname = $1`, superuserName).Scan(&own); err != nil {
		t.Fatal(err)
	}
	if own == published {
		t.Fatal("two independent initdbs produced the same salt; this test would prove nothing")
	}

	if err := (PgxProber{}).AdoptBootstrapVerifier(ctx, shardDSN, superuserName, published); err != nil {
		t.Fatal(err)
	}
	var adopted string
	if err := sconn.QueryRow(ctx, `SELECT rolpassword FROM pg_authid WHERE rolname = $1`, superuserName).Scan(&adopted); err != nil {
		t.Fatal(err)
	}
	if adopted != published {
		t.Fatalf("the shard does not hold the verifier the router forwards against:\n published %s\n shard     %s", published, adopted)
	}

	// Stored verbatim is not enough: it has to still authenticate the
	// password the user was given. Make the shard demand SCRAM and log in.
	if _, err := sconn.Exec(ctx, `COPY (SELECT 'host all all all scram-sha-256') TO '/tmp/pgdata/pg_hba.conf'`); err != nil {
		t.Fatal(err)
	}
	if _, err := sconn.Exec(ctx, `SELECT pg_reload_conf()`); err != nil {
		t.Fatal(err)
	}
	scram, err := pgx.Connect(ctx, shardDSN+"&password="+password)
	if err != nil {
		t.Fatalf("the adopted verifier does not authenticate the password behind it: %v", err)
	}
	_ = scram.Close(ctx)
}
