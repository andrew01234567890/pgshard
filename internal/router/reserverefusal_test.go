package router

import (
	"context"
	"strings"
	"testing"
	"time"
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

// TestARouterStillReadsAnOlderPoolersRefusal: the refusal moved from the
// response body to the status, and a router meets both during a rolling
// upgrade. It reads the old field for one release.
//
// The client-visible error is NOT what distinguishes the two. Without the
// compatibility read the router treats the refusal as a success and sends
// the statement anyway, and the per-request fence refuses that with the
// same reason -- so the client still sees 40001, by a longer road. An
// earlier version of this test asserted the SQLSTATE and passed with the
// compatibility read deleted, which is to say it asserted nothing.
//
// What actually differs is the bookkeeping: the router marks a participant
// reserved that the pooler never pinned, and then releases it. A release
// for a session that was never reserved is the tell.
func TestARouterStillReadsAnOlderPoolersRefusal(t *testing.T) {
	h := newShardedHarness(t)
	ctx := context.Background()
	conn := h.connect(t, h.dsn()+"&default_query_exec_mode=simple_protocol")
	if _, err := conn.Exec(ctx, "set search_path to audit"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "select * from events"); err != nil {
		t.Fatalf("control: %v", err)
	}
	// The control's own releases are asynchronous. Let them land before
	// the counters are reset, or one of them arrives afterwards and reads
	// exactly like the defect this is looking for.
	//
	// Participants only: the session's own pin on its home shard is a
	// reserve that is deliberately never released while the session
	// lives, so counting it means waiting forever.
	for deadline := time.Now().Add(5 * time.Second); !participantsSettled(h); {
		if time.Now().After(deadline) {
			t.Fatal("the control statement never released its participants")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, fp := range h.poolers {
		fp.mu.Lock()
		fp.reserves, fp.releases = nil, nil
		fp.gen++
		fp.mu.Unlock()
		fp.legacyRefusal.Store(true)
	}
	if _, err := conn.Exec(ctx, "select * from events"); err == nil {
		t.Fatal("a scatter whose participants cannot reserve must fail")
	}
	// The release is asynchronous, so give it the time it would have had.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && releasedWithoutReserving(h) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if n := releasedWithoutReserving(h); n != 0 {
		t.Fatalf("%d shards released a participant they never reserved: the router took an older pooler's refusal for a success", n)
	}
}

// participantCounts is how many scatter participants a shard reserved and
// released. The session's own pin is excluded: its session id carries no
// participant marker, and it is held for the life of the session.
func participantCounts(fp *fakePooler) (reserved, released int) {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	for _, s := range fp.reserves {
		if strings.Contains(s, "-x") {
			reserved++
		}
	}
	for _, s := range fp.releases {
		if strings.Contains(s, "-x") {
			released++
		}
	}
	return reserved, released
}

func participantsSettled(h *shardedHarness) bool {
	for _, fp := range h.poolers {
		if reserved, released := participantCounts(fp); released < reserved {
			return false
		}
	}
	return true
}

func releasedWithoutReserving(h *shardedHarness) int {
	n := 0
	for _, fp := range h.poolers {
		if reserved, released := participantCounts(fp); released > reserved {
			n++
		}
	}
	return n
}
