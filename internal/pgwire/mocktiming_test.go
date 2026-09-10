package pgwire

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// oneStepExchange answers the SASL request and then abandons the exchange,
// which is all a probe needs: what is being measured is the work the server
// does before it asks for the client's proof.
type oneStepExchange struct{ first string }

func (e oneStepExchange) Request(msg pgproto3.BackendMessage, _ uint32) (pgproto3.FrontendMessage, error) {
	if _, ok := msg.(*pgproto3.AuthenticationSASL); ok {
		return &pgproto3.SASLInitialResponse{AuthMechanism: "SCRAM-SHA-256", Data: []byte(e.first)}, nil
	}
	return nil, errors.New("probe stops here")
}

// TestTheMockExchangeDoesNotAnnounceAMissingRoleOnTheClock.
//
// The mock exchange exists so that a role that does not exist is
// indistinguishable from one whose password is wrong. It used to build its
// throwaway verifier with BuildSCRAMVerifier -- a full 4096-round PBKDF2 --
// and then discard the result by zeroing StoredKey. An existing role costs
// a parse and a few HMACs, so the two were about 1600x apart: anyone could
// enumerate roles with a stopwatch, while the comment above the code said
// they could not.
//
// The bound is calibrated against the PBKDF2 this must not be doing, so the
// test means the same thing on a slow machine as on a fast one.
func TestTheMockExchangeDoesNotAnnounceAMissingRoleOnTheClock(t *testing.T) {
	stored, err := BuildSCRAMVerifier("hunter2", []byte("0123456789abcdef"), DefaultSCRAMIterations)
	if err != nil {
		t.Fatal(err)
	}
	a := SCRAMAuthenticator{MockSecret: []byte("test-secret"),
		Lookup: lookup(map[string]string{"alice": stored.String()})}

	// The cost of the derivation the mock path must not perform, measured
	// here rather than assumed.
	start := time.Now()
	if _, err := BuildSCRAMVerifier("hunter2", []byte("0123456789abcdef"), DefaultSCRAMIterations); err != nil {
		t.Fatal(err)
	}
	pbkdf2Cost := time.Since(start)

	attempt := func(user string) time.Duration {
		best := time.Duration(1<<62 - 1)
		for i := 0; i < 20; i++ {
			start := time.Now()
			_, _ = a.Authenticate(context.Background(), map[string]string{"user": user}, oneStepExchange{first: rfcClientFirst})
			if d := time.Since(start); d < best {
				best = d
			}
		}
		return best
	}
	// The fastest of many attempts, so a scheduler hiccup cannot make a
	// slow path look fast or a fast one look slow.
	ghost, known := attempt("ghost"), attempt("alice")

	// A quarter of one derivation: comfortably above the HMACs both paths
	// really do, and far below the PBKDF2 that used to be there.
	if ghost > pbkdf2Cost/4 {
		t.Fatalf("a missing role costs %v against a %v derivation (a present one costs %v): the mock exchange is deriving a verifier and the clock says which roles exist",
			ghost, pbkdf2Cost, known)
	}
}
