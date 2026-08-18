//go:build e2e

package e2e

import (
	"context"
	"os"
	"testing"
	"time"
)

const smokeManifest = `
apiVersion: v1
kind: Namespace
metadata:
  name: pgshard-e2e-smoke
---
apiVersion: v1
kind: Pod
metadata:
  name: smoke
  namespace: pgshard-e2e-smoke
  labels:
    app: smoke
spec:
  restartPolicy: Never
  containers:
    - name: busybox
      image: busybox:1.37.0
      command: ["sleep", "3600"]
`

func TestSmoke(t *testing.T) {
	c := NewCluster(t)
	c.GatherOnFailure(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if pg := os.Getenv("PG_MAJOR"); pg != "" {
		t.Logf("PG_MAJOR=%s", pg)
	}
	if err := c.Apply(ctx, smokeManifest); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Delete(context.Background(), smokeManifest); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
	if err := c.WaitPodsReady(ctx, "pgshard-e2e-smoke", "app=smoke", 3*time.Minute); err != nil {
		t.Fatal(err)
	}
	c.MustGather(ctx, t.Name())
}
