package controller

import "testing"

func caughtUpProgress() CopyProgress {
	return CopyProgress{Subscriptions: 2, TablesTotal: 3, TablesReady: 3, LagBytes: 0}
}

// TestAPausedCopyDoesNotAdvance: the progress drive works from is read
// before throttle runs on the same pass. A pass that finds the source's
// standby lag over the watermark disables every subscription and would
// still promote the workflow on the earlier observation -- and
// awaiting_switch_writes is not a copy stage, so throttle never runs again
// to undo it. A transient lag spike in that window stranded a healthy
// reshard until somebody repaired the catalog and subscriptions by hand.
func TestAPausedCopyDoesNotAdvance(t *testing.T) {
	if readyToFinishCopy(StageCopying, caughtUpProgress(), 1024, true) {
		t.Fatal("a throttled copy must stay in the copy stage, where throttle can release it")
	}
	if !readyToFinishCopy(StageCopying, caughtUpProgress(), 1024, false) {
		t.Fatal("an unthrottled copy that is caught up must advance")
	}
}

// TestACopyThatIsNotCaughtUpStays: the pause is an additional reason to
// wait, not a replacement for the original one.
func TestACopyThatIsNotCaughtUpStays(t *testing.T) {
	behind := caughtUpProgress()
	behind.TablesReady = 1
	for _, paused := range []bool{false, true} {
		if readyToFinishCopy(StageCopying, behind, 1024, paused) {
			t.Fatalf("paused=%v: a copy with tables outstanding must not advance", paused)
		}
	}
	lagging := caughtUpProgress()
	lagging.LagBytes = 4096
	if readyToFinishCopy(StageCopying, lagging, 1024, false) {
		t.Fatal("a copy whose lag is over the threshold must not advance")
	}
}

// TestACopyAlreadyPastCatchUpDoesNotAdvanceTwice: reaching the stage is
// what stops it, so a later pass does not re-announce it.
func TestACopyAlreadyPastCatchUpDoesNotAdvanceTwice(t *testing.T) {
	if readyToFinishCopy(StageCatchUpDone, caughtUpProgress(), 1024, false) {
		t.Fatal("a workflow already at catch_up_done must not advance again")
	}
}
