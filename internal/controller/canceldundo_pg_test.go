package controller

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestACancelledCutoverUndoesWhatItStarted (PGS-968): cancel decided whether
// to undo a started cutover from the workflow's STAGE -- and the reconciler
// overwrites that stage with "cancelling" when it marks the run, so the
// check was always false and the undo never ran. The fence itself is lifted
// by the catalog update further down, so no shard was left unwritable; what
// was left was the reverse replication the undo drops, which pins WAL on the
// sources.
//
// The cutover's own record says how far it got, and cancellation does not
// touch it.
func TestACancelledCutoverUndoesWhatItStarted(t *testing.T) {
	parallelPG(t)
	f := newCopyFixture(t)
	ctx := context.Background()
	id := f.startWorkflow()

	// What a run that reached its fence leaves behind: the cutover record.
	fenced := time.Now().UTC().Format(time.RFC3339)
	mustExecPool(t, f.pool, `UPDATE pgshard.workflows
		SET status = status || jsonb_build_object('stage', $2::text, 'cutover', jsonb_build_object('step', $3::text, 'fenced_at', $4::text, 'source_set', 'default'))
		WHERE id = $1::uuid`, id, StageSwitching, StepFence, fenced)
	if _, stage, _ := f.workflow(id); fencedStage(stage) != true {
		t.Fatalf("the fixture must start at a fenced stage, got %s", stage)
	}

	if err := f.reconcileDrop(ctx); err != nil {
		t.Fatal(err)
	}
	// The reconciler has overwritten the stage; only the cutover record is
	// left to say a cutover had started.
	if _, stage, _ := f.workflow(id); stage != StageCancelling {
		t.Fatalf("stage after the reconciler marked it: %s", stage)
	}
	if out := f.pass(); out.Cancelled != 1 {
		t.Fatalf("cancel pass: %+v", out)
	}
	_, _, msg := f.workflow(id)
	if !strings.Contains(msg, "cutover cancelled") {
		t.Fatalf("the cancelled run reports %q: a run that had raised its fence must undo the cutover, not only the forward copy", msg)
	}
	if n := queryOne[int64](t, f.catalog, `SELECT count(*) FROM pgshard.shard_status WHERE migrating`); n != 0 {
		t.Errorf("%d shard(s) left write-fenced", n)
	}
}
