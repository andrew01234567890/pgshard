//go:build e2e || chaos

package e2e

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// failedTB is a testing.TB that reports failure and swallows the cleanups it
// is given: the point of the test is that the dump happens WITHOUT any
// cleanup running.
type failedTB struct {
	testing.TB
	name string
}

func (f failedTB) Failed() bool   { return true }
func (f failedTB) Name() string   { return f.name }
func (f failedTB) Cleanup(func()) {}
func (f failedTB) Helper()        {}

// TestTheDumpIsTakenBeforeTheClusterIsDeleted: t.Cleanup runs LIFO, and every
// suite arms this at the top of its test while the teardown that deletes the
// cluster is registered afterwards -- so the cleanup-based dump always ran
// after the objects were gone. A real failure was diagnosed with a pvcs.txt
// holding nothing but its header while the events showed the claims being
// deleted forty seconds earlier (PGS-704). Delete is armed so the order the
// cleanups were registered in cannot decide whether the dump is worth
// anything.
func TestTheDumpIsTakenBeforeTheClusterIsDeleted(t *testing.T) {
	dir := t.TempDir()
	c := &Cluster{Artifacts: dir, Kubeconfig: filepath.Join(dir, "no-such-kubeconfig")}
	c.GatherOnFailure(failedTB{TB: t, name: "ScenarioThatFailed"})

	// kubectl cannot work here; what matters is that the dump was taken on
	// the way in rather than after the objects were removed.
	_ = c.Delete(context.Background(), "")

	if _, err := os.Stat(filepath.Join(dir, "ScenarioThatFailed", "pvcs.txt")); err != nil {
		t.Fatalf("Delete removed the cluster without dumping its state first: %v", err)
	}
}
