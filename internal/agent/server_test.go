package agent

import (
	"context"
	"errors"
	"fmt"
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
// be retried.
//
// Demote fences with EpochStore.Accept, which takes an epoch strictly
// greater than the one it holds -- the property that makes the fence a fence.
// But the fence is written first and the work happens after it, so a demote
// that stopped PostgreSQL and then failed to rejoin has already moved the
// stored epoch to the epoch it was asked for. The controller retries the
// same operation with the same epoch, and every retry is refused as stale,
// for ever. Converge skips a member whose database is not running, so
// nothing else picks it up either: the group is left short a member until
// something restarts the pod.
//
// A retry at the epoch already accepted is the same controller asking again,
// not a stale one. A LOWER epoch is still refused, which is the part that
// matters.
func TestADemoteThatFailedCanBeRetriedAtTheSameEpoch(t *testing.T) {
	in := newTestInstance(t)
	in.rewindFn = func(context.Context, string) error { return errors.New("no common ancestor") }
	in.recloneFn = func(context.Context) error { return errors.New("source unreachable") }
	srv := NewServer(in, in.epoch, nil, in.log, nil)
	srv.holdStop = func() {}

	if _, err := srv.Demote(context.Background(), &pgshardv1.DemoteRequest{Epoch: 7}); err == nil {
		t.Fatal("the rejoin was set up to fail")
	}

	// The source comes back; the controller asks again, as it must, with
	// the epoch it is still on.
	in.recloneFn = func(context.Context) error { return nil }
	if _, err := srv.Demote(context.Background(), &pgshardv1.DemoteRequest{Epoch: 7}); err != nil {
		t.Fatalf("the retry was refused: %v; the member is fenced at this epoch and no controller will ever send a higher one for this demotion", err)
	}

	// And the fence still holds against a controller that has fallen behind.
	if _, err := srv.Demote(context.Background(), &pgshardv1.DemoteRequest{Epoch: 6}); err == nil {
		t.Fatal("an epoch below the one accepted must still be refused: that is a controller that has been replaced")
	}
}
