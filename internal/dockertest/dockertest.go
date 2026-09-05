// Package dockertest decides what a test should do when the docker daemon or
// a test image is missing.
package dockertest

import (
	"os"
	"os/exec"
	"strings"
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
func Unavailable(t testing.TB, format string, args ...any) {
	t.Helper()
	if Required() {
		t.Fatalf("%s is set, so this must run: "+format, append([]any{RequireEnv}, args...)...)
	}
	t.Skipf(format, args...)
}

// HostPort is the 127.0.0.1 address Docker published for a container's
// port. Fixtures ask Docker to choose the host port and then read it back,
// rather than picking a free one and binding it a moment later: between
// choosing and binding, any other test or process on the machine can take
// it, and the failure that produces -- "ports are not available", or a
// server that never comes up -- says nothing about the code under test.
func HostPort(t testing.TB, id, port string) string {
	t.Helper()
	out, err := exec.Command("docker", "port", id, port).Output()
	if err != nil {
		t.Fatalf("docker port %s %s: %v", id, port, err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if addr := strings.TrimSpace(line); strings.HasPrefix(addr, "127.0.0.1:") {
			return addr
		}
	}
	t.Fatalf("docker port %s %s: no 127.0.0.1 mapping in %q", id, port, out)
	return ""
}
