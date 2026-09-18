package operator

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/agent"
)

// TestAFencedPrimaryIsGivenTheTimeItsAgentTakesToStop: the operator fences an
// old primary by deleting its Pod with PodFenceGrace, and the kubelet
// SIGKILLs whatever is still running when it runs out. The agent is told how
// long to spend by the ShutdownTimeout the operator renders, and exits within
// that plus agent.TerminationOverhead. This pins the relationship between the
// three, not their values (PGS-800).
func TestAFencedPrimaryIsGivenTheTimeItsAgentTakesToStop(t *testing.T) {
	c := newCluster("grace")
	g := Groups(c)[0]
	cm := Renderer{}.ConfigMap(c, g, g.MemberName(0), nil, nil, false, true)
	var cfg agent.Config
	if err := json.Unmarshal([]byte(cm.Data[agentConfigKey(g.MemberName(0))]), &cfg); err != nil {
		t.Fatal(err)
	}
	budget := time.Duration(cfg.ShutdownTimeout) + agent.TerminationOverhead
	if PodFenceGrace < budget {
		t.Fatalf("PodFenceGrace %s is less than the %s a rendered agent may take to stop: a fenced primary is SIGKILLed mid-shutdown",
			PodFenceGrace, budget)
	}
	if PodFenceGrace%time.Second != 0 {
		t.Fatalf("PodFenceGrace %s is not whole seconds, and the delete rounds it down to %ds", PodFenceGrace, int64(PodFenceGrace/time.Second))
	}
	// Member Pods set no terminationGracePeriodSeconds, so an ordinary delete
	// gets Kubernetes' default. If one is ever rendered, it has to cover the
	// budget too.
	const kubernetesDefaultGrace = 30 * time.Second
	pod := Renderer{}.Pod(c, g, 0, RolePrimary, "pvc", Template(c, g, nil, nil))
	grace := kubernetesDefaultGrace
	if s := pod.Spec.TerminationGracePeriodSeconds; s != nil {
		grace = time.Duration(*s) * time.Second
	}
	if grace < budget {
		t.Fatalf("member Pods are deleted with a %s grace, less than the agent's %s stop", grace, budget)
	}
}
