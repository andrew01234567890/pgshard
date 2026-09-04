package dockertest

import (
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestASlotIsHeldUntilItIsReleased: flock is per open file description, so
// two acquisitions of the same slot conflict whether they are in one
// process or two. That is what makes this bound real across test binaries
// and not merely across goroutines.
func TestASlotIsHeldUntilItIsReleased(t *testing.T) {
	dir := t.TempDir()
	release, err := acquireSlot(dir, 1)
	if err != nil {
		t.Fatal(err)
	}

	got := make(chan struct{})
	go func() {
		r2, err := acquireSlot(dir, 1)
		if err != nil {
			return
		}
		defer r2()
		close(got)
	}()
	select {
	case <-got:
		t.Fatal("a second holder took the only slot while the first held it")
	case <-time.After(150 * time.Millisecond):
	}

	release()
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("the slot was not free after it was released")
	}
}

// TestTheSlotsAreCountedNotShared: n slots admit n holders and refuse the
// n+1th. A bound that admitted everyone would be a lock file nobody waits
// on, which is the shape this replaces.
func TestTheSlotsAreCountedNotShared(t *testing.T) {
	const n = 3
	dir := t.TempDir()
	var releases []func()
	for range n {
		r, err := acquireSlot(dir, n)
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, r)
	}
	extra := make(chan struct{})
	go func() {
		r, err := acquireSlot(dir, n)
		if err != nil {
			return
		}
		defer r()
		close(extra)
	}()
	select {
	case <-extra:
		t.Fatalf("a %dth holder got in with %d slots", n+1, n)
	case <-time.After(150 * time.Millisecond):
	}
	for _, r := range releases {
		r()
	}
	select {
	case <-extra:
	case <-time.After(5 * time.Second):
		t.Fatal("no slot became free after every holder released")
	}
}

// TestTheBoundHoldsAcrossProcesses is the whole point, so it uses two
// actual processes rather than trusting that flock behaves as documented.
func TestTheBoundHoldsAcrossProcesses(t *testing.T) {
	if _, err := exec.LookPath("flock"); err != nil {
		t.Skip("flock(1) not available")
	}
	dir := t.TempDir()
	release, err := acquireSlot(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	// flock(1) on the same file this process holds: it must fail to take
	// it while we do.
	out, err := exec.Command("flock", "--nonblock", filepath.Join(dir, "slot-0"), "true").CombinedOutput()
	if err == nil {
		t.Fatalf("another process took a slot this one holds: %s", out)
	}
	release()
	if out, err := exec.Command("flock", "--nonblock", filepath.Join(dir, "slot-0"), "true").CombinedOutput(); err != nil {
		t.Fatalf("another process could not take the released slot: %v: %s", err, out)
	}
}

func TestParallelIsSafeWhenTheSlotDirectoryCannotBeMade(t *testing.T) {
	// A path that cannot be a directory: the helper must report and carry
	// on, because the per-process bound still holds and refusing to run
	// the suite over a cache directory is a worse trade than running it
	// with the older bound.
	if _, err := acquireSlot(filepath.Join("/dev/null", "slots"), 1); err == nil {
		t.Fatal("an unusable slot directory was accepted")
	}
}

func TestSlotDirIsNotShared(t *testing.T) {
	if !strings.Contains(slotDir, "pgshard-test-pg-slots") {
		t.Fatalf("slot directory %q does not name itself", slotDir)
	}
	// Keyed by the bound, so a run at four slots and a run at one do not
	// share files and silently give one of them the other's limit.
	if !strings.HasSuffix(slotDir, "/"+strconv.Itoa(GlobalLimit)) {
		t.Fatalf("slot directory %q is not keyed by the bound %d", slotDir, GlobalLimit)
	}
}
