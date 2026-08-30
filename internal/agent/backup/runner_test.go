package backup

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestALongLineDoesNotWedgeTheCommand: os/exec copies the child's output
// into the pipe from a goroutine that cmd.Wait joins, and the write end is
// closed only once Wait returns. A reader that stopped early -- as a
// Scanner does on a line past its token limit -- left the copy blocked on a
// pipe nobody drained, Wait waiting for the copy, and the close that would
// break the cycle waiting for Wait. Every backup, restore, verify, expire
// and info went through here, and the hang survived the context killing the
// child.
func TestALongLineDoesNotWedgeTheCommand(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh is required to produce output from a real child process")
	}
	r := &Runner{}
	// One line far past any token limit, then a short one after it.
	script := `awk 'BEGIN { s = ""; for (i = 0; i < 200000; i++) s = s "0123456789"; print s; print "after the long line" }'`

	var mu sync.Mutex
	var lines []string
	done := make(chan error, 1)
	go func() {
		done <- r.runBinary(context.Background(), "sh", []string{"-c", script}, func(l string) {
			mu.Lock()
			defer mu.Unlock()
			lines = append(lines, l)
		})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("command: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the command never returned: a line the reader would not read wedged it")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(lines) != 2 {
		t.Fatalf("delivered %d lines, want the long one and the one after it", len(lines))
	}
	if !strings.HasSuffix(lines[0], "[truncated]") || len(lines[0]) > maxLine+32 {
		t.Fatalf("the long line must be delivered bounded and marked, got %d bytes ending %q",
			len(lines[0]), lines[0][max(0, len(lines[0])-16):])
	}
	if lines[1] != "after the long line" {
		t.Fatalf("the line after the long one was lost: %q", lines[1])
	}
}

// TestTheCommandsErrorSurvivesTheReader: a command that fails must still
// report its own failure, not the reader's silence.
func TestTheCommandsErrorSurvivesTheReader(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh is required")
	}
	r := &Runner{}
	err := r.runBinary(context.Background(), "sh", []string{"-c", `echo out; echo err >&2; exit 3`}, func(string) {})
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 3 {
		t.Fatalf("error = %v, want the child's exit status", err)
	}
}
