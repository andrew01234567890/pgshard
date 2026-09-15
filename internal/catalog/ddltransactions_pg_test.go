package catalog

import (
	"context"
	"strings"
	"testing"
)

// TestADatabaseDeclaresHowItsDDLTransactionsRun (PGS-869): a database runs
// DDL inside a transaction atomically -- refused -- unless it is declared
// sequential, and the declaration reaches what the router reads.
func TestADatabaseDeclaresHowItsDDLTransactionsRun(t *testing.T) {
	requireDocker(t)
	ctx := context.Background()
	conn := connect(t, startPostgres(t, candidateImages[0]))
	if err := Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	mode := func() string {
		t.Helper()
		dbs, err := ListDatabases(ctx, conn)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range dbs {
			if d.Name == "app" {
				return d.DDLTransactions
			}
		}
		t.Fatal("database app is not listed")
		return ""
	}
	if _, err := conn.Exec(ctx, `INSERT INTO pgshard.databases (name) VALUES ('app')`); err != nil {
		t.Fatal(err)
	}
	if got := mode(); got != DDLTransactionsAtomic {
		t.Fatalf("a new database runs DDL transactions %q, want %q", got, DDLTransactionsAtomic)
	}
	if _, err := conn.Exec(ctx, `UPDATE pgshard.databases SET ddl_transactions = 'sequential' WHERE name = 'app'`); err != nil {
		t.Fatal(err)
	}
	if got := mode(); got != DDLTransactionsSequential {
		t.Fatalf("after the declaration the database runs DDL transactions %q", got)
	}
	_, err := conn.Exec(ctx, `UPDATE pgshard.databases SET ddl_transactions = 'deferred' WHERE name = 'app'`)
	if err == nil || !strings.Contains(err.Error(), "databases_ddl_transactions_check") {
		t.Fatalf("an unknown mode: %v, want the check constraint to refuse it", err)
	}
}
