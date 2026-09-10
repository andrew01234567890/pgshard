package router

import (
	"context"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// TestACancelAfterTheCommitDecisionIsNotSent.
//
// onCancel is armed for every phase of a two-phase commit, including the
// COMMIT PREPARED that follows the decision. But once the decision is
// durable there is nothing left that is safe to cancel: interrupting a
// participant's COMMIT PREPARED leaves those rows prepared until the
// resolver's next pass, and the client -- which has already been told
// COMMIT -- cannot read its own writes on that shard meanwhile.
func TestACancelAfterTheCommitDecisionIsNotSent(t *testing.T) {
	h := newShardedHarness(t)
	e := newExecutor(h.r, pgwire.SessionInfo{ID: 1, Database: "app", User: "app",
		Auth: &pgwire.AuthResult{SCRAM: &pgwire.SCRAMKeys{}}}, Shard{Set: DefaultShardSet, ID: 0})

	// Before the decision a cancel is sent, which is the behaviour every
	// other phase relies on.
	e.cancelBackend(context.Background())
	if !e.cancelSent.Load() {
		t.Fatal("a cancel before the decision must still be sent")
	}

	// After it, nothing is sent -- even for a fresh statement context.
	e.cancelSent.Store(false)
	e.decided.Store(true)
	e.cancelBackend(context.Background())
	if e.cancelSent.Load() {
		t.Fatal("a cancel was sent after the commit decision; it can only interrupt a COMMIT PREPARED whose outcome is already durable")
	}

	// And the flag does not leak into the next transaction.
	e.finishTxn("COMMIT")
	if e.decided.Load() {
		t.Fatal("the decision flag survived the transaction; the next statement could never be cancelled")
	}
}
