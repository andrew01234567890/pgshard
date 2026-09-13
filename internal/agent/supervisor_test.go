package agent

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func fakePostgres(t *testing.T, script string) *Supervisor {
	t.Helper()
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "postgres"), []byte("#!/bin/sh\n"+script), 0o755); err != nil {
		t.Fatal(err)
	}
	return NewSupervisor(bin, t.TempDir(), slog.New(slog.DiscardHandler))
}

func TestSupervisorStopEscalatesFromSmartToFast(t *testing.T) {
	sup := fakePostgres(t, "trap '' TERM\ntouch \"$2/ready\"\nexec sleep 100\n")
	if err := sup.Start(); err != nil {
		t.Fatal(err)
	}
	for i := 0; ; i++ {
		if _, err := os.Stat(filepath.Join(sup.pgdata, "ready")); err == nil {
			break
		}
		if i > 500 {
			t.Fatal("fake postgres never became ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := sup.Start(); err == nil {
		t.Fatal("double start must fail")
	}
	if !sup.Running() {
		t.Fatal("not running")
	}
	start := time.Now()
	if err := sup.Stop(context.Background(), ShutdownSmart, 200*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 200*time.Millisecond || d > 5*time.Second {
		t.Fatalf("escalation took %s", d)
	}
	if sup.Running() {
		t.Fatal("still running")
	}
	if err := sup.Stop(context.Background(), ShutdownSmart, time.Second); err != nil {
		t.Fatalf("stop when stopped: %v", err)
	}
}

func TestSupervisorReportsUnexpectedExitOnly(t *testing.T) {
	sup := fakePostgres(t, "exit 3\n")
	got := make(chan error, 1)
	sup.OnUnexpectedExit = func(err error) { got <- err }
	if err := sup.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-got:
		if err == nil {
			t.Fatal("nil error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("unexpected exit not reported")
	}
	sup2 := fakePostgres(t, "exec sleep 100\n")
	sup2.OnUnexpectedExit = func(error) { t.Error("stop reported as unexpected exit") }
	if err := sup2.Start(); err != nil {
		t.Fatal(err)
	}
	if err := sup2.Stop(context.Background(), ShutdownFast, time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
}

func TestZombieOrphansSkipsTrackedAndReapsUntracked(t *testing.T) {
	sup := fakePostgres(t, "")
	pid, err := syscall.ForkExec("/bin/true", []string{"true"}, &syscall.ProcAttr{Env: []string{}})
	if err != nil {
		t.Skip("fork not permitted:", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		zs := sup.zombieOrphans()
		if contains(zs, pid) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("zombie %d never listed: %v", pid, zs)
		}
		time.Sleep(10 * time.Millisecond)
	}
	sup.Track(pid)
	if contains(sup.zombieOrphans(), pid) {
		t.Fatal("tracked pid must not be listed")
	}
	sup.Untrack(pid)
	ctx, cancel := context.WithCancel(context.Background())
	go sup.ReapOrphans(ctx)
	deadline = time.Now().Add(6 * time.Second)
	for contains(sup.zombieOrphans(), pid) {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("zombie not reaped")
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
}

func contains(xs []int, x int) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// TestRunTrackedSurvivesConcurrentReaper spawns many fast-exiting tracked
// children while the reaper runs, so a child that exits in the window between
// Start and Track would be reaped out from under cmd.Wait() (ECHILD) unless
// Start and Track are atomic under the supervisor lock.
func TestRunTrackedSurvivesConcurrentReaper(t *testing.T) {
	sup := fakePostgres(t, "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sup.ReapOrphans(ctx)
	for i := 0; i < 300; i++ {
		if _, err := sup.RunTracked(exec.CommandContext(ctx, "/bin/true")); err != nil {
			t.Fatalf("iteration %d: the reaper stole the tracked child: %v", i, err)
		}
	}
}

// TestCommandCancelKillsTheWholeProcessGroup covers the process pg_rewind
// starts for itself: it recovers an unclean target by running
// `postgres --single` through system(), and a cancel that reached only
// pg_rewind would leave that writing to PGDATA while the caller moves on to
// clear the directory and re-clone.
func TestCommandCancelKillsTheWholeProcessGroup(t *testing.T) {
	bin := t.TempDir()
	script := "#!/bin/sh\nsleep 60 &\necho $! > \"$PGDATA/grandchild.pid\"\nsleep 60\n"
	if err := os.WriteFile(filepath.Join(bin, "pg_rewind"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	sup := NewSupervisor(bin, t.TempDir(), slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := sup.RunTracked(sup.Command(ctx, "pg_rewind"))
		done <- err
	}()

	pidFile := filepath.Join(sup.pgdata, "grandchild.pid")
	var grandchild int
	for i := 0; ; i++ {
		if b, err := os.ReadFile(pidFile); err == nil {
			if grandchild, err = strconv.Atoi(strings.TrimSpace(string(b))); err == nil && grandchild > 0 {
				break
			}
		}
		if i > 500 {
			t.Fatal("fake pg_rewind never reported its grandchild")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		// The grandchild inherits the write end of the pipe RunTracked reads,
		// so an unkilled one holds Wait open for its whole lifetime.
		t.Fatal("RunTracked did not return after cancel: the grandchild still holds the output pipe")
	}

	for i := 0; ; i++ {
		if err := syscall.Kill(grandchild, 0); err == syscall.ESRCH {
			break
		}
		if i > 500 {
			t.Fatalf("grandchild %d survived the cancel", grandchild)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
