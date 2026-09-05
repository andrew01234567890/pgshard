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
