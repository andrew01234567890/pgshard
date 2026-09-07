package pooler

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// docs/pooler.md said "Every Execute message and every Reserve carries
// Generation", and the three change-stream RPCs carried none and checked
// nothing. A change stream's slot lives on the primary and the call is
// long-lived, so an unfenced one lets a demoted-but-running primary keep
// delivering commits that exist only on the old timeline; CopyTables would
// create the stream slot and export a snapshot on whatever member it
// reached; and Ack advances a slot, which discards WAL.
func TestTheStreamRPCsAreFenced(t *testing.T) {
	view := View{Generation: 7, Epoch: 3, Serving: true}
	for _, c := range []struct {
		name string
		gen  *pgshardv1.Generation
		want string
	}{
		{"missing", nil, "missing routing generation"},
		{"stale generation", gen(6, 3), "stale routing generation"},
		{"stale epoch", gen(7, 2), "stale primary epoch"},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := NewServer(Config{Source: notServing{view}, Stream: StreamConfig{DSN: "postgres://localhost/x", Shard: "shard0"}})
			err := s.runStream(context.Background(), &pgshardv1.StreamRequest{Stream: "orders", Generation: c.gen}, nil, false)
			if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), c.want) {
				t.Errorf("Stream: %v, want FailedPrecondition %q", err, c.want)
			}
			err = s.runCopy(context.Background(), &pgshardv1.CopyTablesRequest{Stream: "orders", Generation: c.gen}, nil)
			if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), c.want) {
				t.Errorf("CopyTables: %v, want FailedPrecondition %q", err, c.want)
			}
			// Ack fails its RPC like the two above it. It used to answer
			// OK with the refusal in the body, so a fenced ack was a
			// successful call to everything that reads only the status.
			_, err = s.Ack(context.Background(), &pgshardv1.AckRequest{Stream: "orders", Lsn: 1, Generation: c.gen})
			if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), c.want) {
				t.Errorf("Ack: %v, want FailedPrecondition %q", err, c.want)
			}
			// And the refusal keeps its classification: a caller can tell
			// "your view is stale, re-read it" from a failure that says
			// nothing about whether a retry helps.
			if got := ackReason(err); got != pgshardv1.Reason_REASON_STALE_GENERATION {
				t.Errorf("Ack reason = %v, want STALE_GENERATION", got)
			}
		})
	}
}

// A stream is long-lived, so the fence that matters is the one that ends it.
// The router's own check runs only after a batch has been received, which is
// one batch too late: those commits have already reached the consumer and a
// position has been recorded for them.
func TestAStreamEndsWhenTheViewMovesUnderIt(t *testing.T) {
	src := NewStaticSource(View{Generation: 7, Epoch: 3})
	s := NewServer(Config{Source: src, Stream: StreamConfig{DSN: "postgres://localhost/x", Shard: "shard0"}})
	// Opened under the view it matches, then the shard fails over.
	if e := streamFence(src.View(), gen(7, 3)); e != nil {
		t.Fatalf("the open must be admitted: %v", e)
	}
	src.Set(View{Generation: 7, Epoch: 4})
	e := streamFence(src.View(), gen(7, 3))
	if e == nil || !strings.Contains(e.GetMessage(), "stale primary epoch") {
		t.Fatalf("a promotion under an open stream: %v", e)
	}
	// The same check refuses a member that has since gone into recovery,
	// which is the case the catalog epoch cannot see at all.
	src.Set(View{Generation: 7, Epoch: 3, Standby: true})
	if e := streamFence(src.View(), gen(7, 3)); e == nil || !strings.Contains(e.GetMessage(), "in recovery") {
		t.Fatalf("a demoted member under an open stream: %v", e)
	}
	_ = s
}

// A pooler whose catalog view has stopped being refreshed refuses streams
// for the same reason it refuses Execute: the numbers in a stale view are
// the last ones it read.
func TestAStaleViewRefusesStreams(t *testing.T) {
	s := NewServer(Config{Source: notServing{View{Generation: 7, Epoch: 3}},
		Stream: StreamConfig{DSN: "postgres://localhost/x", Shard: "shard0", ReceiveTimeout: time.Millisecond}})
	err := s.runStream(context.Background(), &pgshardv1.StreamRequest{Stream: "orders", Generation: gen(7, 3)}, nil, false)
	if !strings.Contains(err.Error(), "catalog view is stale") {
		t.Fatalf("Stream on a stale pooler: %v", err)
	}
}

// ackReason reads the Error the pooler attaches to a refused ack.
func ackReason(err error) pgshardv1.Reason {
	for _, d := range status.Convert(err).Details() {
		if e, ok := d.(*pgshardv1.Error); ok {
			return e.GetReason()
		}
	}
	return pgshardv1.Reason_REASON_UNSPECIFIED
}
