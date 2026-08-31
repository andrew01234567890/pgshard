package agent

import (
	"slices"
	"testing"
)

// TestStartupRollbackUndoesInReverseUnlessStartupFinished: the agent takes a
// lease, then starts PostgreSQL, then binds listeners, so each step rests on
// the ones before it and they have to come apart the other way round -- a
// lease released while PostgreSQL is still running is the state this exists
// to avoid, not a step towards it.
func TestStartupRollbackUndoesInReverseUnlessStartupFinished(t *testing.T) {
	var undone []string
	var r startupRollback
	for _, step := range []string{"lease", "postgres", "http", "grpc"} {
		r.push(func() { undone = append(undone, step) })
	}
	r.run()
	if want := []string{"grpc", "http", "postgres", "lease"}; !slices.Equal(undone, want) {
		t.Fatalf("undone %v, want %v", undone, want)
	}

	// A startup that reached the steady state hands these to the shutdown
	// path, so the deferred run must do nothing -- otherwise a healthy
	// agent would stop PostgreSQL and drop its lease the moment Run
	// returned.
	undone = nil
	var ok startupRollback
	ok.push(func() { undone = append(undone, "lease") })
	ok.succeed()
	ok.run()
	if len(undone) != 0 {
		t.Fatalf("a completed startup was rolled back: %v", undone)
	}

	// Nothing acquired, nothing to undo.
	var empty startupRollback
	empty.run()
}
