// Package dockertest decides what a test should do when the docker daemon or
// a test image is missing.
package dockertest

import (
	"os"
	"testing"
)

// RequireEnv is set where the container-backed tests are expected to run. CI
// sets it, so a missing daemon or unpullable image fails the job instead of
// quietly skipping the suite and reporting green.
const RequireEnv = "PGSHARD_REQUIRE_DOCKER"

// Required reports whether container-backed tests must run.
func Required() bool { return os.Getenv(RequireEnv) == "1" }

// Unavailable ends the test: fatally where containers are required, as a skip
// otherwise. A developer without docker still gets a usable `go test ./...`;
// CI does not get a green run out of tests that never executed.
func Unavailable(t *testing.T, format string, args ...any) {
	t.Helper()
	if Required() {
		t.Fatalf("%s is set, so this must run: "+format, append([]any{RequireEnv}, args...)...)
	}
	t.Skipf(format, args...)
}
