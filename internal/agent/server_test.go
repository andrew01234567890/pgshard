package agent

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

func TestPgErrMapsStaleEpochToSQLState55000(t *testing.T) {
	if pgErr(nil) != nil {
		t.Fatal("nil error must map to nil")
	}
	e := pgErr(fmt.Errorf("wrap: %w", ErrStaleEpoch))
	if e.GetSqlstate() != "55000" || e.GetMessage() != "wrap: stale epoch" {
		t.Fatalf("stale: %v", e)
	}
	if e := pgErr(errors.New("boom")); e.GetSqlstate() != "XX000" || e.GetMessage() != "boom" {
		t.Fatalf("other: %v", e)
	}
}

// TestPromoteChecksTheLeaseHolderItWasGiven.
//
// The operator hands the Lease to a member by name and then asks that member
// to promote, passing the name it used. The agent never read it: the field
// was sent and ignored, and the protocol worked only because both sides
// happen to derive the same string from the member name.
//
// A rename, or PodName set to something else, would have failed every
// promotion inside Acquire with ErrLeaseHeld -- a lease conflict, with
// nothing saying the two sides disagreed about who this member is.
func TestPromoteChecksTheLeaseHolderItWasGiven(t *testing.T) {
	in := newTestInstance(t)
	lease := &Lease{holder: "shard-0-1"}
	srv := NewServer(in, in.epoch, lease, in.log, nil)

	_, err := srv.Promote(context.Background(), &pgshardv1.PromoteRequest{Epoch: 1, LeaseHolder: "shard-0-2"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("a promote naming another member's lease must be refused: %v", err)
	}
	if !strings.Contains(err.Error(), "shard-0-1") || !strings.Contains(err.Error(), "shard-0-2") {
		t.Fatalf("the refusal must name both identities, got %v", err)
	}

	// An operator too old to send the field is accepted, so this needs no
	// flag day.
	if _, err := srv.Promote(context.Background(), &pgshardv1.PromoteRequest{Epoch: 1}); status.Code(err) == codes.FailedPrecondition &&
		strings.Contains(err.Error(), "lease holder") {
		t.Fatalf("an empty lease_holder must not be refused: %v", err)
	}
}

// TestAFailedRejoinStillHandsBackThePrimaryLease: Demote released the lease
// only if the whole rejoin succeeded. A rejoin fails for as long as the new
// primary is unreachable, and a member holding the Lease through that
// refuses the designated primary's own fence on every pass -- a group left
// with no primary by the member that no longer has a database.
func TestAFailedRejoinStillHandsBackThePrimaryLease(t *testing.T) {
	in := newTestInstance(t)
	in.rewindFn = func(context.Context, string) error { return errors.New("no common ancestor") }
	in.recloneFn = func(context.Context) error { return errors.New("source unreachable") }
	srv := NewServer(in, in.epoch, nil, in.log, nil)
	released := false
	srv.holdStop = func() { released = true }

	_, err := srv.Demote(context.Background(), &pgshardv1.DemoteRequest{Epoch: 1})
	if err == nil {
		t.Fatal("a rejoin that cannot reach the source must be reported")
	}
	if !released {
		t.Error("the primary lease is still held by a member whose database is down")
	}
}

// PGS-752 L3. A demote that failed after the fence was written could never
// be retried: the fence is stored BEFORE the work it admits, so the retry
// carries the epoch the first attempt already accepted and is refused as
// stale, for ever. Nothing mints a higher epoch for a demotion that has
// already happened, and converge skips a member whose database is down.
func TestADemoteThatFailedCanBeRetriedAtTheSameEpoch(t *testing.T) {
	in := newTestInstance(t)
	in.rewindFn = func(context.Context, string) error { return errors.New("no common ancestor") }
	reclones := 0
	in.recloneFn = func(context.Context) error { reclones++; return errors.New("source unreachable") }
	srv := NewServer(in, in.epoch, nil, in.log, nil)
	srv.holdStop = func() {}

	if _, err := srv.Demote(context.Background(), &pgshardv1.DemoteRequest{Epoch: 7}); err == nil {
		t.Fatal("the rejoin was set up to fail")
	}

	// The source comes back; the controller asks again, as it must, with
	// the epoch it is still on.
	in.recloneFn = func(context.Context) error { reclones++; return nil }
	if _, err := srv.Demote(context.Background(), &pgshardv1.DemoteRequest{Epoch: 7}); err != nil {
		t.Fatalf("the retry was refused: %v", err)
	}
	if reclones != 2 {
		t.Fatalf("the rejoin ran %d times, want 2: the retry was admitted and then did nothing, which is not a retry", reclones)
	}

	// And the fence still holds against a controller that has fallen behind.
	_, err := srv.Demote(context.Background(), &pgshardv1.DemoteRequest{Epoch: 6})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "stale epoch") {
		t.Fatalf("an epoch below the one accepted answered %v; it must be refused as stale, because that is a controller that has been replaced", err)
	}
}

// And a repeat is admitted only when it RESUMES a demotion already begun.
//
// The operator sends Demote at the CURRENT epoch to any member that reports
// itself primary while not being the designated one (failover.go), and that
// member was promoted at that very epoch. Admitting a bare repeat of the
// accepted epoch would let such a call stop a live primary, release its
// lease and rewind it -- precisely what the strictly increasing rule is
// there to prevent. What separates the two cases is observable: a demote
// that is able to fail has already stopped PostgreSQL.
func TestOnlyAStoppedMemberMayRepeatItsEpoch(t *testing.T) {
	in := newTestInstance(t)
	srv := NewServer(in, in.epoch, nil, in.log, nil)
	if err := in.epoch.Accept(7); err != nil {
		t.Fatal(err)
	}

	if !srv.demoteRetry(7) {
		t.Error("a stopped member at the epoch it accepted is resuming its own demotion")
	}
	if srv.demoteRetry(6) {
		t.Error("an epoch below the one accepted is a controller that has been replaced, never a retry")
	}
	if srv.demoteRetry(8) {
		t.Error("a higher epoch is a fresh order and must go through the fence, which stores it")
	}

	// Still running at that epoch: this member was promoted at it, and a
	// Demote carrying it is the operator acting on a stale designation.
	in.sup.mu.Lock()
	in.sup.cmd = &exec.Cmd{}
	in.sup.mu.Unlock()
	t.Cleanup(func() { in.sup.mu.Lock(); in.sup.cmd = nil; in.sup.mu.Unlock() })
	if srv.demoteRetry(7) {
		// Fatal: letting the Demote below run against a member this
		// fixture only pretends is alive turns a clear failure into a
		// panic in the supervisor.
		t.Fatal("a running member repeated its epoch and was taken for a retry: that stops a live primary")
	}
	if _, err := srv.Demote(context.Background(), &pgshardv1.DemoteRequest{Epoch: 7}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Demote at the promotion epoch of a running member answered %v, want it refused as stale", err)
	}
}
