package router

import (
	"context"
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
	if got := nameFenceInTxn(fence); !errors.As(got, &pe) || pe.Code != codeFailoverInTxn {
		t.Fatalf("a flip during PREPARE gave the client %v, want %s", got, codeFailoverInTxn)
	}
	if got := nameFenceInTxn(bare); !errors.Is(got, bare) {
		t.Errorf("a bare 55000 during PREPARE was rewritten to %v", got)
	}
	if got := nameFenceInTxn(rewrite); !errors.Is(got, rewrite) {
		t.Errorf("a rewrite-in-progress during PREPARE was rewritten to %v", got)
	}
	if nameFenceInTxn(nil) != nil {
		t.Error("nil must stay nil")
	}
}

// TestAFlipJoiningASecondShardFailsTheTransaction: a transaction whose
// second statement is the first to touch its shard starts that part idle,
// so the executor's own transaction status says "no transaction" while the
// client is inside one with writes already on another shard. Judged by the
// part, a fence there was waited on and retried, and when the wait ran out
// the pooler's bare 55000 went to the client -- which is what a cutover
// under load produced. Judged by the session, it is what it is: the
// topology moved under an open transaction.
func TestAFlipJoiningASecondShardFailsTheTransaction(t *testing.T) {
	e := &Executor{tx: pgwire.TxIdle}
	if e.multiShardTxn() {
		t.Fatal("a fresh executor is not in a multi-shard transaction")
	}
	if e.inClientTransaction() {
		t.Fatal("an idle session must stay retryable")
	}
	// The transaction wrote on one shard and has just moved to another it
	// has not touched, which is exactly the state switchPart leaves behind.
	e.parked = map[Shard]*txnPart{{Set: "default", ID: 0}: {shard: Shard{Set: "default", ID: 0}, wrote: true}}
	if !e.inClientTransaction() {
		t.Fatal("a part that is idle while another holds the transaction's writes must not be treated as retryable")
	}
	if got := decideFailover(true, true, false, 0, 10); got != failoverFailTxn {
		t.Fatalf("decideFailover = %v, want failoverFailTxn: the client has to be told to retry the transaction", got)
	}
	var pe *pgwire.Error
	if !errors.As(failoverInTxnError(), &pe) || pe.Code != "40001" {
		t.Fatalf("the answer must be 40001, got %v", failoverInTxnError())
	}
}

// TestAFlipAtCommitIsNamedWhereverItLands: several ways of ending a
// transaction reach a participant outside withFailover -- the hidden-writer
// probe, the prepared-capacity check, a single writer's plain COMMIT, and
// two-phase commit. A flip landing on any of them used to reach the client
// as the participant's own 55000, at COMMIT, which is the point where it
// tells them least. nameFenceInTxn is applied to every exit of endTxn, so
// this asserts what that guarantees rather than each path separately.
func TestAFlipAtCommitIsNamedWhereverItLands(t *testing.T) {
	fence := toPgwireError(&pgshardv1.Error{Sqlstate: codeStaleGeneration, Message: "stale routing generation",
		Reason: pgshardv1.Reason_REASON_STALE_GENERATION})
	var pe *pgwire.Error
	if got := nameFenceInTxn(fence); !errors.As(got, &pe) || pe.Code != codeFailoverInTxn {
		t.Fatalf("a declared fence at commit gave %v, want %s", got, codeFailoverInTxn)
	}
	// Nothing else is touched: nil stays nil, and an error that is not a
	// declared fence is the participant's to report.
	if nameFenceInTxn(nil) != nil {
		t.Fatal("a transaction that ended cleanly must not acquire an error")
	}
	other := toPgwireError(&pgshardv1.Error{Sqlstate: "23505", Message: "duplicate key value violates unique constraint"})
	if got := nameFenceInTxn(other); !errors.Is(got, other) {
		t.Fatalf("a constraint violation at commit was rewritten to %v", got)
	}
}

// TestACommitMeetingAFlipTellsTheClientToRetry drives endTxn rather than
// calling the namer: a transaction writes, the map moves under it, and the
// COMMIT is what meets the fence. That is the path a cutover under load
// actually took, and the one a test of the namer alone cannot see.
func TestACommitMeetingAFlipTellsTheClientToRetry(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "insert into t values (1)"); err != nil {
		t.Fatal(err)
	}
	// The map moves while the transaction is open. The pooler holding its
	// writes now refuses the stamp the router is still sending.
	h.fp.gen = 999
	err = tx.Commit(ctx)
	if err == nil {
		t.Fatal("a commit whose shard has moved must not report success")
	}
	if got := sqlstate(err); got != codeFailoverInTxn {
		t.Fatalf("commit gave %s (%v), want %s: at COMMIT a bare 55000 is where it helps least", got, err, codeFailoverInTxn)
	}
}
