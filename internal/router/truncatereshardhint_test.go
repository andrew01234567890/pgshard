package router

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/router/plan"
)

// TestTheTruncateReshardRefusalNamesAWayOut (PGS-954): the third refusal
// keyed on Snapshot.Resharding(), and the one left with the dead end after
// PGS-876 fixed the other two.
//
// Its hint was "DELETE the rows, or wait for the reshard to complete". A
// reshard that FAILED leaves its set in provisioning, which is exactly what
// Resharding() keys on, so waiting for it to complete is advice that can
// never come true.
//
// It is also the MORE reachable of the three. Both DDL refusals are behind
// a check that the catalog lacks the operation queue, so they fire only for
// a router newer than its catalog mid-rollout. This one has no such guard
// and fires on every catalog.
func TestTheTruncateReshardRefusalNamesAWayOut(t *testing.T) {
	h := newDDLHarness(t, &fakeQueue{})
	ctx := context.Background()
	conn := h.connect(t, h.dsn())

	// A set stuck in provisioning is what both a running and a failed
	// reshard look like to a router.
	s := *h.snap
	serving := make(map[snapshot.ShardKey]snapshot.Serving, len(s.Serving))
	for k, v := range s.Serving {
		serving[k] = v
	}
	k := snapshot.ShardKey{ShardSet: DefaultShardSet, ShardID: 0}
	stuck := serving[k]
	stuck.State = "provisioning"
	serving[k] = stuck
	s.Serving = serving
	h.setSnap(&s)

	_, err := conn.Exec(ctx, "truncate items")
	if err == nil {
		t.Fatal("TRUNCATE was accepted while a reshard held the shard map")
	}
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		t.Fatalf("want a PostgreSQL error, got %T: %v", err, err)
	}
	t.Logf("refusal: %s\nhint: %s", pe.Message, pe.Hint)
	if pe.Code != "0A000" {
		t.Errorf("refusal code %s, want 0A000", pe.Code)
	}
	// DELETE stays and leads: it is the one remedy needing no cluster
	// surgery, and dropping it would trade one dead end for another.
	if !strings.Contains(pe.Hint, "DELETE") {
		t.Errorf("the hint no longer offers DELETE, the only remedy that needs no cluster surgery:\n%s", pe.Hint)
	}
	// The check before the action, and what being wrong about it costs.
	for _, want := range []string{"status.reshard", "Failed", "spec.shards", "RUNNING", "upgrade", "docs/resharding.md"} {
		if !strings.Contains(pe.Hint, want) {
			t.Errorf("the hint does not mention %q, so a failed reshard leaves the operator with advice that cannot come true:\n%s", want, pe.Hint)
		}
	}
	// And it is the SAME words the DDL refusals use. Two copies of this
	// paragraph is how this one kept the dead end after the others lost it.
	if !strings.Contains(pe.Hint, plan.ReshardRecoveryHint) {
		t.Errorf("the TRUNCATE hint has drifted from the shared one:\n%s", pe.Hint)
	}
}
