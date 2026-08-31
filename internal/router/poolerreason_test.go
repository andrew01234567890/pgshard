package router

import (
	"errors"
	"testing"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// TestAFenceSaysItIsAFence: 55000 is object_not_in_prerequisite_state, and
// the pooler answers it both for a topology that moved and for a table
// mid-rewrite. codeStaleGeneration and codeRewriteInProgress are literally
// the same constant, so a caller reading the SQLSTATE cannot tell a fence
// from a condition retrying will not fix.
func TestAFenceSaysItIsAFence(t *testing.T) {
	fence := toPgwireError(&pgshardv1.Error{Sqlstate: codeStaleGeneration, Message: "stale routing generation",
		Reason: pgshardv1.Reason_REASON_STALE_GENERATION})
	rewrite := toPgwireError(&pgshardv1.Error{Sqlstate: codeRewriteInProgress, Message: "under an online schema migration",
		Reason: pgshardv1.Reason_REASON_REWRITE_IN_PROGRESS})
	bare := toPgwireError(&pgshardv1.Error{Sqlstate: codeStaleGeneration, Message: "prepare refused"})

	if !isStaleGeneration(fence) {
		t.Error("a pooler that said it was a fence was not believed")
	}
	if isStaleGeneration(rewrite) {
		t.Error("a rewrite in progress was treated as a topology change, so it would be retried as a failover")
	}
	// An older pooler sends no reason, and the SQLSTATE is all there is.
	// Dropping that fallback would stop buffering failovers mid-upgrade.
	if !isStaleGeneration(bare) {
		t.Error("a pooler that predates the reason field must still be judged by its SQLSTATE")
	}

	// Every existing caller looks for a *pgwire.Error and must still find
	// one, code and message intact.
	var pe *pgwire.Error
	if !errors.As(fence, &pe) || pe.Code != codeStaleGeneration || pe.Message != "stale routing generation" {
		t.Fatalf("the pgwire error did not survive the wrapper: %v", fence)
	}

	// The commit path is stricter: it is new behaviour, so only an explicit
	// reason is rewritten. A bare 55000 there is left exactly as it was.
	if got := namePrepareFailover(fence); !errors.As(got, &pe) || pe.Code != codeFailoverInTxn {
		t.Fatalf("a flip during PREPARE gave the client %v, want %s", got, codeFailoverInTxn)
	}
	if got := namePrepareFailover(bare); !errors.Is(got, bare) {
		t.Errorf("a bare 55000 during PREPARE was rewritten to %v", got)
	}
	if got := namePrepareFailover(rewrite); !errors.Is(got, rewrite) {
		t.Errorf("a rewrite-in-progress during PREPARE was rewritten to %v", got)
	}
	if namePrepareFailover(nil) != nil {
		t.Error("nil must stay nil")
	}
}
