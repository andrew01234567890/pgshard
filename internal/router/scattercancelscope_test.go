package router

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestAScatterCancelNamesOnlyItsOwnStatement pins the invariant that lets a
// scatter tear a participant down asynchronously: the session id a Cancel
// or a Release names belongs to the statement that started it, and to no
// later one.
//
// Cancel is fired from a goroutine (scatter.go, `go p.cancel`) with its own
// timeout, and so is a reserved participant's Release. Both can therefore
// land after the next statement has opened on the same shard. What keeps
// that harmless today is that a participant's session id carries the
// statement's sequence number, so a late teardown names a session that no
// longer exists rather than the one now running.
//
// PGS-616 wants to stop opening a stream per participant per statement,
// which means holding one open, which means a session id that is stable
// across statements -- at which point a late cancel of the first statement
// silently kills the second. Nothing else in the suite would catch that:
// the existing cancel tests assert that the cancel ARRIVES, not that it
// arrives for the right statement. This asserts the scope.
func TestAScatterCancelNamesOnlyItsOwnStatement(t *testing.T) {
	h := newShardedHarness(t)
	conn := h.connect(t, h.dsn()+"&default_query_exec_mode=simple_protocol")
	ctx := context.Background()

	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			asleep := 0
			for _, fp := range h.poolers {
				fp.mu.Lock()
				asleep += len(fp.sleeping)
				fp.mu.Unlock()
			}
			if asleep == len(h.poolers) {
				_ = conn.PgConn().CancelRequest(ctx)
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	if _, err := conn.Exec(ctx, "select pg_sleep(10) from orders"); sqlstate(err) != "57014" {
		t.Fatalf("cancel: %v", err)
	}

	sid := h.sidOf(t)
	cancelled := map[string]bool{}
	for _, fp := range h.poolers {
		for _, c := range fp.cancelled() {
			cancelled[c] = true
		}
	}
	if len(cancelled) == 0 {
		t.Fatal("no participant was cancelled, so this proves nothing")
	}
	before := make([]map[string]int, len(h.poolers))
	for i, fp := range h.poolers {
		before[i] = fp.sessionsSeen()
	}

	// A second scatter on the same session and the same shards.
	rows, err := conn.Query(ctx, "select * from orders")
	if err != nil {
		t.Fatalf("second scatter: %v", err)
	}
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("second scatter rows: %v", err)
	}

	// Whatever the second statement attached on any shard must be a
	// session no cancel has already named. A session id that is stable
	// across statements fails here, which is the point: the first
	// statement's cancel is then aimed at the second statement's work.
	used := 0
	for i, fp := range h.poolers {
		for s, n := range fp.sessionsSeen() {
			if n <= before[i][s] || !strings.HasPrefix(s, sid+"-x") {
				continue
			}
			used++
			if cancelled[s] {
				t.Fatalf("shard %d: the second statement attached %q, a session a cancel has already named", i, s)
			}
		}
	}
	if used == 0 {
		t.Fatal("the second scatter attached no participant session, so this proves nothing")
	}
}
