package router

import (
	"context"
	"strings"
	"testing"
)

// TestAScatterOpensAStreamPerParticipantPerStatement is PGS-616's baseline,
// as a number rather than an argument.
//
// Every participant of every scatter gets its own pooler session, and a
// pooler binds a session to a stream on that stream's first message, so
// each one costs a fresh Execute stream: a gRPC stream open, a channel and
// a reader goroutine, opened and torn down per statement. Nothing is reused
// between statements.
//
// This asserts the count exactly, in both directions. Too many would mean a
// participant is reconnecting mid-statement, which is a bug today. Too few
// is what PGS-616 is for, and when it lands this becomes the acceptance
// assertion with the bound flipped -- fewer than one stream per participant
// per statement in steady state. Until then the number is what makes the
// improvement measurable instead of asserted.
func TestAScatterOpensAStreamPerParticipantPerStatement(t *testing.T) {
	h := newShardedHarness(t)
	conn := h.connect(t, h.dsn()+"&default_query_exec_mode=simple_protocol")
	ctx := context.Background()

	const statements = 3
	for i := 0; i < statements; i++ {
		rows, err := conn.Query(ctx, "select * from orders")
		if err != nil {
			t.Fatalf("scatter %d: %v", i, err)
		}
		for rows.Next() {
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("scatter %d rows: %v", i, err)
		}
	}

	sid := h.sidOf(t)
	sessions, opens := 0, 0
	for _, fp := range h.poolers {
		for s, n := range fp.sessionsSeen() {
			if !strings.HasPrefix(s, sid+"-x") {
				continue
			}
			sessions++
			opens += n
			if n != 1 {
				// Today a participant session belongs to exactly one
				// statement, so more than one stream on it means either a
				// participant reopened mid-statement -- which would have
				// lost its backend -- or the session id has been made
				// stable across statements, which is the PGS-616 change
				// and wants the counts below revisited rather than this
				// line relaxed.
				t.Errorf("participant session %q opened %d streams, want 1", s, n)
			}
		}
	}
	want := statements * len(h.poolers)
	if sessions != want || opens != want {
		t.Fatalf("%d participant sessions and %d stream opens across %d shards and %d statements, want %d of each",
			sessions, opens, len(h.poolers), statements, want)
	}
	t.Logf("%d scatters over %d shards opened %d streams", statements, len(h.poolers), opens)
}
