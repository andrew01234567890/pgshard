package router

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// TestARouterAnsweredBatchSendsItsCompletions (PGS-939 review, finding 4):
// a batch the router answers itself -- EXPLAIN (pgshard), nextval() over a
// global sequence -- still owes the client the ParseComplete and
// BindComplete a backend would have sent. answerBatch walks the batch for
// Describe and Execute only.
func TestARouterAnsweredBatchSendsItsCompletions(t *testing.T) {
	h := newShardedHarness(t)
	s := newExtendedSession(t, h.dsn())
	// nextval() over a global sequence is the other batch the router
	// answers itself; it needs a registered sequence, so the EXPLAIN case
	// stands for both -- they share answerBatch.
	code, _, _ := s.send("explain through the extended protocol",
		&pgproto3.Parse{Query: "explain (pgshard) select 1"}, &pgproto3.Bind{},
		&pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if code != "" {
		t.Fatalf("EXPLAIN (pgshard) over the extended protocol: %s", code)
	}
	got := strings.Join(s.seen, ",")
	for _, want := range []string{"ParseComplete", "BindComplete"} {
		if !strings.Contains(got, want) {
			t.Errorf("the answer is missing %s: %v", want, s.seen)
		}
	}
	if !strings.HasPrefix(got, "ParseComplete,BindComplete") {
		t.Errorf("the completions must come first, as a backend would send them: %v", s.seen)
	}
}
