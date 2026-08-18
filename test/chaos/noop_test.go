//go:build chaos

package chaos

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/test/e2e"
)

//go:embed podchaos-noop.yaml
var noopManifest string

func TestNoopPodChaos(t *testing.T) {
	c := e2e.NewCluster(t)
	c.GatherOnFailure(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := c.Apply(ctx, noopManifest); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Delete(context.Background(), noopManifest); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
	if err := waitExperimentDone(ctx, c, "pgshard-chaos", "noop-pod-kill", 3*time.Minute); err != nil {
		t.Fatal(err)
	}
}

func waitExperimentDone(ctx context.Context, c *e2e.Cluster, ns, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		out, err := c.Kubectl(ctx, nil, "-n", ns, "get", "podchaos", name, "-o", "json")
		if err == nil {
			var obj struct {
				Status struct {
					Conditions []struct {
						Type, Status string
					} `json:"conditions"`
					Experiment struct {
						DesiredPhase string `json:"desiredPhase"`
					} `json:"experiment"`
				} `json:"status"`
			}
			if jerr := json.Unmarshal([]byte(out), &obj); jerr == nil {
				injected := false
				for _, cond := range obj.Status.Conditions {
					if cond.Type == "AllInjected" && cond.Status == "True" {
						injected = true
					}
				}
				if injected || obj.Status.Experiment.DesiredPhase == "Stop" {
					return nil
				}
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("experiment %s/%s not complete after %s: %w", ns, name, timeout, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
}
