package router

import (
	"errors"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// TestOneCodeMeansRetryTheTransaction: drivers' retry loops test SQLSTATEs
// by exact value -- pgx, JDBC and psycopg all match 40001 and 40P01 and
// nothing broader -- so every outcome the router knows is safe to retry
// has to arrive as the same code. A second code for a second cause is a
// retry that never happens.
func TestOneCodeMeansRetryTheTransaction(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
	}{
		{"a failover inside an open transaction", failoverInTxnError()},
		{"the resolver aborting a presumed-dead coordinator", resolverAbortError("pgshard-1-2-3")},
	} {
		t.Run(c.name, func(t *testing.T) {
			var pe *pgwire.Error
			if !errors.As(c.err, &pe) {
				t.Fatalf("not a protocol error: %v", c.err)
			}
			if pe.Code != "40001" {
				t.Fatalf("code %s, want 40001: a retry loop testing for 40001 would surface this to the application instead", pe.Code)
			}
			// The cause is what separates them, so it has to be said.
			if pe.Detail == "" || pe.Hint == "" {
				t.Fatalf("detail %q hint %q: the code alone does not say which cause it was", pe.Detail, pe.Hint)
			}
		})
	}
}

// TestAnUnknownOutcomeIsNeverARetryCode: 08007 says the transaction may
// still commit. It must not share the retry code, and its hint must not
// invite one -- retrying it is how a client commits the same work twice.
func TestAnUnknownOutcomeIsNeverARetryCode(t *testing.T) {
	var pe *pgwire.Error
	if !errors.As(inDoubtError("pgshard-1-2-3", errors.New("catalog unreachable")), &pe) {
		t.Fatal("not a protocol error")
	}
	if pe.Code != "08007" {
		t.Fatalf("code %s, want 08007", pe.Code)
	}
	if pe.Code == codeRetryTransaction {
		t.Fatal("an unknown outcome must not carry the retry code")
	}
	if pe.Hint == "" {
		t.Fatal("an unknown outcome must tell the client what to do instead of retrying")
	}
}
