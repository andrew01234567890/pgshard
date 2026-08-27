package dockertest

import (
	"fmt"
	"os"
	"testing"
)

// RequireEnvtestEnv is set where the envtest control plane is expected. CI
// sets it, so missing assets fail the job instead of exiting zero and
// reporting a green run for tests that never executed.
const RequireEnvtestEnv = "PGSHARD_REQUIRE_ENVTEST"

// EnvtestAvailable reports whether the control-plane binaries are present.
func EnvtestAvailable() bool { return os.Getenv("KUBEBUILDER_ASSETS") != "" }

// EnvtestMissingMain ends a TestMain when the control plane is absent: exit 1
// with a clear reason where envtest is required, exit 0 otherwise so a
// developer without the assets still gets a usable `go test ./...`. It
// returns the exit code to pass to os.Exit.
func EnvtestMissingMain(what string) int {
	if os.Getenv(RequireEnvtestEnv) == "1" {
		fmt.Fprintf(os.Stderr, "envtest: %s is set but KUBEBUILDER_ASSETS is not; %s must run here. Run 'make envtest-assets'.\n", RequireEnvtestEnv, what)
		return 1
	}
	fmt.Fprintf(os.Stderr, "envtest: KUBEBUILDER_ASSETS is not set; run 'make envtest'. Skipping %s.\n", what)
	return 0
}

// EnvtestMissing fails or skips one test when the control plane is absent.
func EnvtestMissing(t *testing.T, what string) {
	t.Helper()
	if os.Getenv(RequireEnvtestEnv) == "1" {
		t.Fatalf("%s is set but KUBEBUILDER_ASSETS is not; %s must run here", RequireEnvtestEnv, what)
	}
	t.Skipf("KUBEBUILDER_ASSETS is not set; run 'make envtest' to run %s", what)
}
