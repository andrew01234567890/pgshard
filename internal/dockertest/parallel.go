package dockertest

import (
	"os"
	"runtime"
	"strconv"
	"testing"
)

// ParallelEnv overrides how many PostgreSQL-backed tests one package runs at
// once; 1 makes a suite serial again on a runner that cannot afford the
// containers.
const ParallelEnv = "PGSHARD_TEST_PG_PARALLEL"

// pgSlots bounds how many PostgreSQL-backed tests run at once. Admission is
// per test, not per container: a fixture takes several containers one at a
// time, so a per-container limit deadlocks as soon as every slot is held by a
// test still waiting for its next container.
//
// The bound is per PROCESS, and `go test ./...` runs up to -p packages
// concurrently, so the containers actually in flight are this limit times the
// number of container-heavy packages running together. The Makefile sets -p
// alongside this so the product stays bounded; raising either without the
// other will swamp a runner.
var pgSlots = make(chan struct{}, Limit)

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
func Parallel(t *testing.T) {
	t.Helper()
	t.Parallel()
	pgSlots <- struct{}{}
	t.Cleanup(func() { <-pgSlots })
}
