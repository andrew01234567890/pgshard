package router

import (
	"errors"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// TestAPauseThatCannotBeRetriedIsStillNamed: the router retries a write
// pause it can retry -- a transaction that has done nothing else is given
// back and reopened. One that has already written to another shard cannot
// be, so a barrier landing between its statements reaches the client. It
// used to arrive as PostgreSQL's "cannot execute INSERT in a read-only
// transaction", which names neither the pause nor a way out, and which a
// client cannot tell from a transaction it opened read-only itself.
func TestAPauseThatCannotBeRetriedIsStillNamed(t *testing.T) {
	readOnly := pgwire.Errorf(codeReadOnlyTransaction, "cannot execute INSERT in a read-only transaction")

	fenced := &Router{cfg: Config{Snapshot: func() *snapshot.Snapshot { return &snapshot.Snapshot{WriteFence: true} }}}
	e := &Executor{r: fenced}
	got := e.namePauseThatCannotBeRetriedHere(readOnly)
	var pe *pgwire.Error
	if !errors.As(got, &pe) || pe.Code != codeWriteFence {
		t.Fatalf("under a pause the client should get %s, got %v", codeWriteFence, got)
	}
	if pe.Hint == "" {
		t.Error("an error a client is meant to retry has to say so")
	}

	// A router that saw the pause a moment ago answers the same way: the
	// pause is lifted in milliseconds once it waits on nothing, so the
	// statement it refused can easily return after it has finished.
	recent := &Router{cfg: Config{Snapshot: func() *snapshot.Snapshot { return &snapshot.Snapshot{} },
		Buffering: Buffering{Window: time.Minute}}}
	recent.fenceSeen.Store(time.Now().UnixNano())
	if got := (&Executor{r: recent}).namePauseThatCannotBeRetriedHere(readOnly); !errors.As(got, &pe) || pe.Code != codeWriteFence {
		t.Fatalf("a pause seen within the buffering window should be named too, got %v", got)
	}

	// With no pause anywhere, a genuinely read-only transaction keeps
	// PostgreSQL's own error, which is the truthful one there.
	quiet := &Router{cfg: Config{Snapshot: func() *snapshot.Snapshot { return &snapshot.Snapshot{} }}}
	if got := (&Executor{r: quiet}).namePauseThatCannotBeRetriedHere(readOnly); !errors.Is(got, readOnly) {
		t.Fatalf("without a pause the client keeps 25006, got %v", got)
	}
	if got := (&Executor{r: fenced}).namePauseThatCannotBeRetriedHere(nil); got != nil {
		t.Fatalf("nil stays nil, got %v", got)
	}
	other := pgwire.Errorf("42601", "syntax error")
	if got := (&Executor{r: fenced}).namePauseThatCannotBeRetriedHere(other); !errors.Is(got, other) {
		t.Fatalf("an unrelated error is untouched, got %v", got)
	}
}
