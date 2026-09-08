package dockertest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func refused(context.Context) error { return errors.New("connection refused") }

func probes(running bool, why string, size func() int) dockerProbes {
	return dockerProbes{
		running: func(string) (bool, string) { return running, why },
		logSize: func(string) int { return size() },
		log:     func(string) string { return "the container said this" },
	}
}

func steady(n int) func() int { return func() int { return n } }

func TestWaitReadyReturnsWhenTheServerAnswers(t *testing.T) {
	calls := 0
	connect := func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("not yet")
		}
		return nil
	}
	if err := waitReady("c", connect, probes(true, "running", steady(10)), time.Minute, time.Minute); err != nil {
		t.Fatalf("a server that came up was reported as %v", err)
	}
}

// A container that has exited answers nothing, so waiting the full bound
// for it turns a one-line configuration error into a timeout that says
// only that PostgreSQL never became ready.
func TestWaitReadyGivesUpAtOnceOnAContainerThatStopped(t *testing.T) {
	start := time.Now()
	err := waitReady("c", refused, probes(false, "exited exit=1", steady(10)), time.Minute, time.Minute)
	if err == nil {
		t.Fatal("a stopped container was reported as ready")
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("it waited %s for a container that had already stopped", took)
	}
	for _, want := range []string{"stopped", "exited exit=1", "the container said this"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the failure does not mention %q: %v", want, err)
		}
	}
}

// The bound is on PROGRESS, not on total time: a container still writing
// to its log is still starting, however slowly the machine is running it.
// This is the whole point of the change -- a fixed total fires on a busy
// machine, which is when it is least likely to be a real defect.
func TestWaitReadyKeepsWaitingWhileTheContainerIsStillLogging(t *testing.T) {
	size := 0
	growing := func() int { size += 10; return size }
	start := time.Now()
	err := waitReady("c", refused, probes(true, "running", growing), 300*time.Millisecond, 2*time.Second)
	if err == nil {
		t.Fatal("it never gave up")
	}
	// It cannot have given up on the idle bound, because the log never
	// stopped growing; only the absolute cap can end this.
	if took := time.Since(start); took < 2*time.Second {
		t.Fatalf("it gave up after %s despite the container still logging; the bound is not on progress", took)
	}
	if !strings.Contains(err.Error(), "still logging") {
		t.Errorf("gave up for the wrong reason: %v", err)
	}
}

// And a container that goes quiet without listening is not waited on
// forever either.
func TestWaitReadyGivesUpWhenNothingProgresses(t *testing.T) {
	start := time.Now()
	err := waitReady("c", refused, probes(true, "running", steady(10)), 300*time.Millisecond, time.Minute)
	if err == nil {
		t.Fatal("it never gave up")
	}
	if took := time.Since(start); took > 30*time.Second {
		t.Errorf("it waited %s on a container making no progress", took)
	}
	if !strings.Contains(err.Error(), "logged nothing") {
		t.Errorf("gave up for the wrong reason: %v", err)
	}
}
