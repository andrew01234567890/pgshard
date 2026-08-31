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
