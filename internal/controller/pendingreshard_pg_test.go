package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestAnInPlaceRangeEditSaysWhyItWaits: editing pgshard.shard_ranges on a
// serving set records a workflow that nothing drives. The copier runs
// workflows in 'running', and the only way into 'running' is a set that
// went 'desired' to 'provisioning' -- which spec.shards produces and an
// in-place edit does not.
//
// Leaving it at 'pending' with nothing beside it reads as work about to
// start. Someone who edited shard_ranges and is now looking at
// pgshard.workflows is looking at the one place that can tell them
// otherwise, so it has to say so there.
func TestAnInPlaceRangeEditSaysWhyItWaits(t *testing.T) {
	parallelPG(t)
	dsn := startPostgres(t)
	ctx := context.Background()
	conn := connect(t, dsn)
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range)
		VALUES ('default', 0, int8range(NULL, NULL))`)
	reconcile(t, conn)

	mustExec(t, conn, `BEGIN`)
	mustExec(t, conn, `UPDATE pgshard.shard_ranges SET range = int8range(NULL, 0) WHERE shard_set = 'default' AND shard_id = 0`)
	mustExec(t, conn, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('default', 1, int8range(0, NULL))`)
	mustExec(t, conn, `COMMIT`)

	if res := reconcile(t, conn); res.WorkflowsCreated != 1 {
		t.Fatalf("the edit must be recorded: %+v", res)
	}
	state := queryOne[string](t, conn, `SELECT state FROM pgshard.workflows WHERE kind = 'reshard'`)
	if state != StatePending {
		t.Fatalf("state %q, want %q", state, StatePending)
	}
	msg := queryOne[string](t, conn, `SELECT coalesce(status->>'message', '') FROM pgshard.workflows WHERE kind = 'reshard'`)
	if msg == "" {
		t.Fatal("a workflow nothing will drive must say so on itself")
	}
	// It has to name what to do instead, not merely that it is waiting.
	for _, want := range []string{"not driven yet", "spec.shards", "new shard set", "CancelWorkflow"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("the message does not mention %q: %q", want, msg)
		}
	}

	// And a second pass does not pile up another one.
	if res := reconcile(t, conn); res.WorkflowsCreated != 0 {
		t.Fatalf("a second pass created another workflow: %+v", res)
	}
	if n := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.workflows WHERE kind = 'reshard'`); n != 1 {
		t.Fatalf("%d reshard workflows, want 1", n)
	}
}
