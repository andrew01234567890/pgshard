package router

import (
	"context"
	"testing"
)

// TestARefusedReserveKeepsThePoolersReason: a Reserve the pooler refuses is
// a refusal, not a broken connection, and the client has to be able to tell.
// A stale generation carries REASON_STALE_GENERATION, which the router turns
// into 40001 "shard failover; retry the transaction" -- advice the client can
// act on. A connection failure is not retryable and says nothing useful.
//
// The pooler returns that refusal as a FailedPrecondition status with its
// own Error attached as a detail, rather than as an Error inside an OK
// response (PGS-393: a mutation that did not happen must not look like a
// successful RPC to an interceptor or a retry policy). This is the router
// half: it has to recover the detail, because the fallback for a status it
// cannot read is "pooler refused the connection", which loses the SQLSTATE
// and tells the client something false.
//
// Driven through a scatter because that is the path that reserves: a
// participant needs the session's state applied, and applying it pins a
// backend.
func TestARefusedReserveKeepsThePoolersReason(t *testing.T) {
	h := newShardedHarness(t)
	ctx := context.Background()
	conn := h.connect(t, h.dsn()+"&default_query_exec_mode=simple_protocol")

	// Session state, so a scatter participant has something to replay and
	// therefore reserves at all.
	if _, err := conn.Exec(ctx, "set search_path to audit"); err != nil {
		t.Fatal(err)
	}
	// A control first: the same statement works while the generations agree.
	if _, err := conn.Exec(ctx, "select * from events"); err != nil {
		t.Fatalf("control: %v", err)
	}

	// Now every pooler answers Reserve with a stale-generation refusal.
	for _, fp := range h.poolers {
		fp.mu.Lock()
		fp.gen++
		fp.mu.Unlock()
	}
	_, err := conn.Exec(ctx, "select * from events")
	if err == nil {
		t.Fatal("a scatter whose participants cannot reserve must fail")
	}
	// 40001, from the refusal's reason. Without the detail the router
	// falls back to "pooler refused the connection", which is both wrong
	// and not retryable.
	if got := sqlstate(err); got != "40001" {
		t.Fatalf("refused Reserve reached the client as %q (%v), want 40001 from the refusal's reason", got, err)
	}
}
