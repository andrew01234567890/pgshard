//go:build integration

package router

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestAStartThatNeverBecomesReadySaysWhy: the harness waits for a process to
// log a ready line and, when it does not, printed the captured log. On a
// loaded runner that log is EMPTY -- the process wrote nothing at all -- so
// the failure was
//
//	pgshard-router did not report "listening on":
//
// with nothing after the colon, and a process that DIED was indistinguishable
// from one that was merely slow. Both sightings of that failure (PGS-599)
// looked exactly like this.
func TestAStartThatNeverBecomesReadySaysWhy(t *testing.T) {
	t.Run("exited", func(t *testing.T) {
		expectStartFailure(t, []string{"exited before reporting", "exit status 3", "wrote nothing at all"},
			"/bin/sh", "-c", "exit 3")
	})
	t.Run("still running", func(t *testing.T) {
		old := procReadyTimeout
		procReadyTimeout = 2 * time.Second
		defer func() { procReadyTimeout = old }()
		// exec, so the shell is REPLACED by sleep: killing the shell would
		// leave the child holding the output pipe, and cmd.Wait blocks on
		// that until the child ends of its own accord.
		expectStartFailure(t, []string{"is still running", "working"},
			"/bin/sh", "-c", "echo working; exec sleep 60")
	})
}

// expectStartFailure runs a process that will never become ready and checks
// what the harness said about it.
//
// The checks are in the DEFERRED function on purpose: the helper reports by
// calling Fatalf, which unwinds, so anything written after the call to it
// never runs. A first version of this test put them there and passed while
// asserting nothing -- the mutation check is what found that.
func expectStartFailure(t *testing.T, want []string, bin string, args ...string) {
	t.Helper()
	ft := &fakeT{T: t}
	defer func() {
		_ = recover()
		if ft.msg == "" {
			t.Fatal("the start was expected to fail and did not")
		}
		for _, w := range want {
			if !strings.Contains(ft.msg, w) {
				t.Errorf("the failure must say %q; it said: %s", w, ft.msg)
			}
		}
	}()
	startProcess(ft, &logBuffer{}, "never printed", bin, args...)
}

// fakeT captures the Fatalf a helper would use to fail the test, so the
// helper's own failure path can be asserted.
type fakeT struct {
	*testing.T
	msg string
}

func (f *fakeT) Fatalf(format string, args ...any) {
	f.msg = fmt.Sprintf(format, args...)
	panic(errStop)
}

func (f *fakeT) Fatal(args ...any) { f.msg = fmt.Sprint(args...); panic(errStop) }

// errStop unwinds the helper once its failure is captured.
var errStop = errors.New("stop")
