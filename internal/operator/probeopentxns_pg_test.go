package operator

import (
	"context"
	"os/exec"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/dockertest"
)

// TestOpenClientTransactionsCountsOnlyOpenTransactions: the retirement drain
// (PGS-927) waits on this count, so an idle session must not hold it and a
// session inside a transaction -- even an idle one -- must.
func TestOpenClientTransactionsCountsOnlyOpenTransactions(t *testing.T) {
	if err := exec.Command("docker", "info").Run(); err != nil {
		dockertest.Unavailable(t, "docker unavailable")
	}
	ctx := context.Background()
	dsn := startProbePostgres(t)
	count := func() int {
		t.Helper()
		n, err := PgxProber{}.OpenClientTransactions(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}

	idle, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = idle.Close(ctx) }()
	if n := count(); n != 0 {
		t.Fatalf("an idle session counted as %d open transactions", n)
	}

	inTxn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = inTxn.Close(ctx) }()
	mustProbeExec(t, inTxn, `BEGIN`)
	mustProbeExec(t, inTxn, `SELECT 1`)
	if n := count(); n != 1 {
		t.Fatalf("a session idle in a transaction counted as %d, want 1", n)
	}
	mustProbeExec(t, inTxn, `COMMIT`)
	if n := count(); n != 0 {
		t.Fatalf("after COMMIT the count is %d, want 0", n)
	}
}
