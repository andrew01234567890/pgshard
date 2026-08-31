package router

import (
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/andrew01234567890/pgshard/internal/pooler"
)

// TestAnAttachRaceIsReadFromItsReasonNotItsWording: the pooler refuses a
// second Execute stream for a session in a sentence, and the router decided
// whether to wait and retry or to hand the client a dead connection by
// matching that sentence. Rewording it would have turned a race worth
// waiting through into a failed connection.
func TestAnAttachRaceIsReadFromItsReasonNotItsWording(t *testing.T) {
	st := status.New(codes.FailedPrecondition, "that session is busy")
	detailed, err := st.WithDetails(&errdetails.ErrorInfo{Reason: pooler.ReasonSessionAttached, Domain: pooler.ErrorDomain})
	if err != nil {
		t.Fatal(err)
	}
	if !attachRaced(detailed.Err()) {
		t.Fatal("an attach race carrying its reason was not recognised")
	}
	// The wording alone still works, for a pooler that predates the reason.
	if !attachRaced(status.Error(codes.FailedPrecondition, "session s already has an Execute stream")) {
		t.Fatal("the text fallback stopped working")
	}
	// Another FailedPrecondition is not an attach race.
	if attachRaced(status.Error(codes.FailedPrecondition, "generation 4 is stale")) {
		t.Fatal("an unrelated refusal was taken for an attach race")
	}
}
