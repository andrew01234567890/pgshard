package controller

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type applyRecorder struct {
	execs []string
}

func (r *applyRecorder) Exec(_ context.Context, sql string, _ ...any) (CommandTag, error) {
	r.execs = append(r.execs, sql)
	return nil, nil
}

func (r *applyRecorder) Query(context.Context, string, ...any) (pgx.Rows, error) { panic("unused") }
func (r *applyRecorder) Close(context.Context) error                             { return nil }

func TestApplyOpsCostsARoundTripPerTargetNotPerRow(t *testing.T) {
	one, two := &applyRecorder{}, &applyRecorder{}
	targets := targetConns{1: one, 2: two}
	var ops []applyOp
	for i := range 300 {
		ops = append(ops, applyOp{shard: 1, sql: "one" + itoa(int64(i))}, applyOp{shard: 2, sql: "two" + itoa(int64(i))})
	}
	if err := applyOps(context.Background(), targets, rowShape{}, "t", ops); err != nil {
		t.Fatal(err)
	}
	for name, r := range map[string]*applyRecorder{"1": one, "2": two} {
		if len(r.execs) != 1 {
			t.Fatalf("target %s took %d round trips for 300 operations, want 1", name, len(r.execs))
		}
		if !strings.HasPrefix(r.execs[0], "BEGIN;") || !strings.HasSuffix(r.execs[0], "COMMIT") {
			t.Fatalf("target %s did not apply its operations in a transaction: %.40s", name, r.execs[0])
		}
	}
	if got := strings.Count(one.execs[0], "one"); got != 300 {
		t.Fatalf("target 1 carried %d of its 300 operations", got)
	}
	if strings.Contains(one.execs[0], "two") {
		t.Fatal("target 1 was sent another target's operations")
	}
}

func TestApplyOpsKeepsEachTargetsOrder(t *testing.T) {
	one := &applyRecorder{}
	ops := []applyOp{{shard: 1, sql: "insert"}, {shard: 1, sql: "delete"}}
	if err := applyOps(context.Background(), targetConns{1: one}, rowShape{}, "t", ops); err != nil {
		t.Fatal(err)
	}
	if want := "BEGIN;insert;delete;COMMIT"; one.execs[0] != want {
		t.Fatalf("got %q, want %q", one.execs[0], want)
	}
}

func TestApplyOpsSplitsAStatementThatWouldGrowUnbounded(t *testing.T) {
	one := &applyRecorder{}
	var ops []applyOp
	for range applyBatchOps + 1 {
		ops = append(ops, applyOp{shard: 1, sql: "x"})
	}
	if err := applyOps(context.Background(), targetConns{1: one}, rowShape{}, "t", ops); err != nil {
		t.Fatal(err)
	}
	if len(one.execs) != 2 {
		t.Fatalf("%d operations went out in %d statements, want 2", len(ops), len(one.execs))
	}
}

// barrierRecorder answers Exec only once every target has reached it, so a
// serial applyOps cannot get past the first one.
type barrierRecorder struct {
	mu      sync.Mutex
	execs   []string
	arrive  chan struct{}
	release chan struct{}
}

func (r *barrierRecorder) Exec(ctx context.Context, sql string, _ ...any) (CommandTag, error) {
	r.mu.Lock()
	r.execs = append(r.execs, sql)
	r.mu.Unlock()
	r.arrive <- struct{}{}
	select {
	case <-r.release:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *barrierRecorder) Query(context.Context, string, ...any) (pgx.Rows, error) { panic("unused") }
func (r *barrierRecorder) Close(context.Context) error                             { return nil }

// A flush costs the largest target's round trip, not the sum of them. The
// targets are separate shards on separate connections, and catch-up
// latency is what decides whether the slot converges before the write
// fence, so this is the difference between converging and not on a split
// into several targets.
func TestApplyOpsSendsToEveryTargetAtOnce(t *testing.T) {
	const n = 4
	arrive := make(chan struct{}, n)
	release := make(chan struct{})
	targets := targetConns{}
	var ops []applyOp
	for i := range n {
		targets[int32(i)] = &barrierRecorder{arrive: arrive, release: release}
		ops = append(ops, applyOp{shard: int32(i), sql: "x"})
	}

	done := make(chan error, 1)
	go func() { done <- applyOps(context.Background(), targets, rowShape{}, "t", ops) }()

	// Every target must be inside Exec before any of them is answered.
	for i := range n {
		select {
		case <-arrive:
		case <-time.After(10 * time.Second):
			close(release)
			t.Fatalf("only %d of %d targets had been sent their operations; the flush costs the sum of the target round trips, not the largest", i, n)
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// A target that fails cancels the ones still in flight rather than leaving
// the flush waiting on them. The slow target is first, so a version that
// did not carry the group's context into applyToTarget would sit on it
// forever; catch-up re-decodes the round either way, because the slot has
// not advanced.
func TestApplyOpsFailingTargetReleasesTheOthers(t *testing.T) {
	slow := &barrierRecorder{arrive: make(chan struct{}, 1), release: make(chan struct{})}
	bad := &errRecorder{err: errors.New("gone")}
	done := make(chan error, 1)
	go func() {
		done <- applyOps(context.Background(), targetConns{1: slow, 0: bad}, rowShape{}, "t",
			[]applyOp{{shard: 1, sql: "y"}, {shard: 0, sql: "x"}})
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a failing target was reported as a successful flush")
		}
		if !errors.Is(err, bad.err) {
			t.Fatalf("the flush reported %v rather than the failure that caused it", err)
		}
	case <-time.After(10 * time.Second):
		close(slow.release)
		t.Fatal("the flush was still waiting on a target that nothing would answer after another target had already failed")
	}
}

type errRecorder struct{ err error }

func (r *errRecorder) Exec(context.Context, string, ...any) (CommandTag, error) {
	return nil, r.err
}
func (r *errRecorder) Query(context.Context, string, ...any) (pgx.Rows, error) { panic("unused") }
func (r *errRecorder) Close(context.Context) error                             { return nil }
