package controller

import "testing"

func peekWF(id string) *placementWorkflow { return &placementWorkflow{id: id} }

// A peek that fills without reaching a commit is decoded and thrown away:
// nothing in it can be applied until the commit arrives. catchUpSource
// quadruples until one fits, which bounds the attempts within a pass --
// but the size that worked used to be forgotten, so a workflow whose
// transactions are consistently larger than peekChanges paid the whole
// ramp again on every pass, re-decoding the same rows before it could
// apply any of them.
func TestTheSizeThatReachedACommitIsRememberedForTheNextPass(t *testing.T) {
	p := &Placer{}
	wf := peekWF("wf-1")

	if got := p.peekLimit(wf, 0); got != peekChanges {
		t.Fatalf("a source nothing is known about starts at %d, want %d", got, peekChanges)
	}
	p.rememberPeekLimit(wf, 0, peekChanges*4)
	if got := p.peekLimit(wf, 0); got != peekChanges*4 {
		t.Fatalf("the next pass asks for %d, want the %d that worked", got, peekChanges*4)
	}
	// Per source, not per workflow: one shard's transactions say nothing
	// about another's.
	if got := p.peekLimit(wf, 1); got != peekChanges {
		t.Fatalf("another source of the same workflow starts at %d, want %d", got, peekChanges)
	}
	// And per workflow: a different placement of the same shard likewise.
	if got := p.peekLimit(peekWF("wf-2"), 0); got != peekChanges {
		t.Fatalf("another workflow starts at %d, want %d", got, peekChanges)
	}
	// Never below the floor, however small a limit is offered.
	p.rememberPeekLimit(wf, 2, 1)
	if got := p.peekLimit(wf, 2); got != peekChanges {
		t.Fatalf("a smaller limit was remembered as %d", got)
	}
}

// The map must not grow with every placement the process ever drove.
func TestAFinishedWorkflowsPeekLimitsAreForgotten(t *testing.T) {
	p := &Placer{}
	keep, drop := peekWF("keeper"), peekWF("goner")
	p.rememberPeekLimit(keep, 0, peekChanges*4)
	p.rememberPeekLimit(drop, 0, peekChanges*4)
	p.rememberPeekLimit(drop, 1, peekChanges*16)

	p.forgetPeekLimits(drop)
	if got := p.peekLimit(drop, 0); got != peekChanges {
		t.Errorf("a finished workflow still remembers %d", got)
	}
	if got := p.peekLimit(drop, 1); got != peekChanges {
		t.Errorf("a finished workflow still remembers %d on its other source", got)
	}
	if got := p.peekLimit(keep, 0); got != peekChanges*4 {
		t.Errorf("finishing one workflow forgot another's %d", got)
	}
}
