package operator

import (
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/controller"
)

// TestOnlyPreSwitchReshardsAreDroppedOnRevert: cancelling a revert deletes
// the target set's pgshard.shard_status rows, and those rows are how the
// resolver finds shards to search for prepared transactions. Before the
// write switch a target set has taken no client writes and holds none, so
// dropping it hides nothing. Adding a phase at or past the switch to that
// list would delete the rows of a set that can hold a prepared transaction,
// and a decided commit could then be lost with them.
func TestOnlyPreSwitchReshardsAreDroppedOnRevert(t *testing.T) {
	preSwitch := map[string]bool{
		pgshardv1alpha1.ReshardPhasePending:      true,
		pgshardv1alpha1.ReshardPhaseProvisioning: true,
		pgshardv1alpha1.ReshardPhaseCopying:      true,
		pgshardv1alpha1.ReshardPhaseVerifying:    true,
	}
	for _, phase := range []string{
		pgshardv1alpha1.ReshardPhasePending, pgshardv1alpha1.ReshardPhaseProvisioning,
		pgshardv1alpha1.ReshardPhaseCopying, pgshardv1alpha1.ReshardPhaseVerifying,
		pgshardv1alpha1.ReshardPhaseSwitching, pgshardv1alpha1.ReshardPhaseCompleting,
		pgshardv1alpha1.ReshardPhaseCompleted, pgshardv1alpha1.ReshardPhaseCancelled,
		pgshardv1alpha1.ReshardPhaseFailed,
	} {
		if got := cancellableOnRevert(phase); got != preSwitch[phase] {
			t.Errorf("cancellableOnRevert(%q) = %v, want %v", phase, got, preSwitch[phase])
		}
	}
}

// TestEveryCutoverStageMapsToAPhaseThatCannotBeDropped: the phase list is
// only half the guard. What actually decides whether a set may be dropped
// is reshardPhase, and its fallthrough answers Provisioning -- which is
// droppable -- for any stage it does not recognise. A cutover stage added
// around the switch and not mapped here would therefore let a set be
// dropped from under a running switch, taking the shard_status rows the
// resolver needs to find prepared transactions with it.
//
// So every stage the controller can record is walked through the real
// mapping, and the ones at or past the switch must not come out droppable.
func TestEveryCutoverStageMapsToAPhaseThatCannotBeDropped(t *testing.T) {
	// Stages before the switch: the set has taken no client writes.
	for _, stage := range []string{
		controller.StageProvisioning, controller.StageReadyForCopy,
		controller.StageCopying, controller.StageCatchUpDone, controller.StageAwaitingSwitch,
	} {
		if !cancellableOnRevert(reshardPhase(WorkflowInfo{ID: "w", State: "running", Stage: stage})) {
			t.Errorf("stage %q is before the switch but cannot be cancelled by reverting", stage)
		}
	}
	// Stages at or past the switch: the target set is taking writes, or the
	// journal is written and the old set is retired rather than dropped.
	// Each carries the state it is recorded with, since a terminal stage
	// and its state always arrive together.
	for _, tc := range []struct{ stage, state string }{
		{controller.StageSwitching, "running"},
		{controller.StageSwitched, "running"},
		{controller.StageRollingBack, "running"},
		{controller.StageCompleting, "running"},
		{controller.StageRolledBack, "cancelled"},
		{controller.StageCompleted, "completed"},
	} {
		if cancellableOnRevert(reshardPhase(WorkflowInfo{ID: "w", State: tc.state, Stage: tc.stage})) {
			t.Errorf("stage %q is at or past the write switch but its set may be dropped", tc.stage)
		}
	}
}
