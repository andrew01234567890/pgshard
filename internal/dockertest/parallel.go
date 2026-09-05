package dockertest

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
)

// ParallelEnv overrides how many PostgreSQL-backed tests one package runs at
// once; 1 makes a suite serial again on a runner that cannot afford the
// containers.
const ParallelEnv = "PGSHARD_TEST_PG_PARALLEL"

// pgSlots bounds how many PostgreSQL-backed tests run at once IN THIS
// PROCESS. Admission is per test, not per container: a fixture takes
// several containers one at a time, so a per-container limit deadlocks as
// soon as every slot is held by a test still waiting for its next one.
var pgSlots = make(chan struct{}, Limit)

// GlobalEnv overrides how many PostgreSQL-backed tests run at once across
// every test binary; GlobalLimit is the value.
const GlobalEnv = "PGSHARD_TEST_PG_GLOBAL"

// GlobalLimit bounds the containers in flight across processes.
//
// `go test ./...` runs up to -p packages at once, each its own binary with
// its own copy of pgSlots, so the containers actually reaching the daemon
// were this package's limit times the number of container-heavy packages
// running together. That product is what a runner cannot take, and it was
// held only by the Makefile setting -p to match -- a convention, and one
// that anybody running `go test ./pkg/a ./pkg/b` by hand steps straight
// past. The failure is not subtle and it is not honest either: containers
// that never become ready, in whichever suite was unlucky.
var GlobalLimit = func() int {
	if v, err := strconv.Atoi(os.Getenv(GlobalEnv)); err == nil && v > 0 {
		return v
	}
	return Limit
}()

// slotDir holds the lock files. Under the user's cache rather than a
// fixed path so two users on one machine do not share a bound, and so a
// machine that clears its cache clears nothing that matters.
var slotDir = func() string {
	base, err := os.UserCacheDir()
	if err != nil {
		base = os.TempDir()
	}
	return filepath.Join(base, "pgshard-test-pg-slots", strconv.Itoa(GlobalLimit))
}()

// Limit is the per-package cap derived from GOMAXPROCS, or ParallelEnv.
var Limit = func() int {
	if v, err := strconv.Atoi(os.Getenv(ParallelEnv)); err == nil && v > 0 {
		return v
	}
	n := runtime.GOMAXPROCS(0)
	switch {
	case n <= 2:
		return 1
	case n <= 4:
		return 2
	case n < 12:
		return n / 2
	default:
		return 6
	}
}()

// Parallel marks a PostgreSQL-backed test parallel and holds a slot for its
// whole lifetime. The release is registered before any container cleanup so
// it runs last, after the containers are actually gone.
func Parallel(t testing.TB) {
	t.Helper()
	// A benchmark has no Parallel: it is already the only thing running,
	// and calling it would be a compile error rather than a no-op. The
	// admission below is what actually bounds the containers, and it
	// applies either way.
	if p, ok := t.(interface{ Parallel() }); ok {
		p.Parallel()
	}
	pgSlots <- struct{}{}
	t.Cleanup(func() { <-pgSlots })
	release, err := acquireSlot(slotDir, GlobalLimit)
	if err != nil {
		// The cross-process bound is an optimisation on top of a bound
		// that already holds, so a machine that cannot provide it runs the
		// tests rather than failing them.
		t.Logf("cross-process test slots unavailable, continuing with the per-process bound only: %v", err)
		return
	}
	t.Cleanup(release)
}
