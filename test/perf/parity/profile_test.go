//go:build perfprofile

// The profiling driver stands up the parity stack with pprof enabled, runs
// the unsharded prepared select workload and captures CPU/allocs/mutex
// profiles from the router and the pooler. Heavy (docker, ~1 min); build
// tag perfprofile keeps it out of make verify.
package parity

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestProfileHotPath(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}
	results := t.TempDir()
	cmd := exec.Command("./run.sh", "profile")
	cmd.Env = append(os.Environ(), "PARITY_RESULTS="+results)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run.sh profile: %v\n%s", err, out)
	}
	for _, f := range []string{"router.cpu.pb.gz", "pooler.cpu.pb.gz", "router.allocs.pb.gz", "pooler.allocs.pb.gz", "router.mutex.pb.gz", "pooler.mutex.pb.gz"} {
		if fi, err := os.Stat(filepath.Join(results, "profiles", f)); err != nil || fi.Size() == 0 {
			t.Errorf("missing or empty profile %s: %v", f, err)
		}
	}
}
