//go:build perfparity

// Package parity smoke-tests the pooling-parity harness: every front-end
// (direct, PgBouncer session/transaction, pgshard router+pooler) must serve
// SELECT 1. Requires docker and a local pgshard-router:dev image.
package parity

import (
	"os/exec"
	"strings"
	"testing"
)

func TestHarnessSmoke(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}
	out, err := exec.Command("./run.sh", "smoke").CombinedOutput()
	if err != nil {
		t.Fatalf("run.sh smoke: %v\n%s", err, out)
	}
	for _, arm := range []string{"direct", "pgbouncer-session", "pgbouncer-txn", "pgshard"} {
		if !strings.Contains(string(out), "SMOKE OK frontend="+arm) {
			t.Errorf("no SMOKE OK for %s:\n%s", arm, out)
		}
	}
}
