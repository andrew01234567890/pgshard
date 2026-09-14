package operator

import (
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/agent"
)

// TestAFencedPrimaryIsGivenTheTimeItsAgentTakesToStop: the operator fences an
// old primary by deleting its Pod with podFenceGrace, and the kubelet
// SIGKILLs whatever is still running when it runs out. It was ten seconds
// against an agent that spent the first thirty in a smart shutdown and could
// take ninety in all, so every fenced primary crashed and rejoined through a
// full crash recovery inside pg_rewind (PGS-800). The two numbers now come
// from one place; this pins the relationship, not the values.
func TestAFencedPrimaryIsGivenTheTimeItsAgentTakesToStop(t *testing.T) {
	if podFenceGrace <= agent.TerminationBudget {
		t.Fatalf("podFenceGrace %s does not exceed the agent's stop budget %s: a fenced primary is SIGKILLed mid-shutdown",
			podFenceGrace, agent.TerminationBudget)
	}
	if podFenceGrace%time.Second != 0 {
		t.Fatalf("podFenceGrace %s is not whole seconds, and the delete rounds it down to %ds", podFenceGrace, int64(podFenceGrace/time.Second))
	}
	// Member Pods set no terminationGracePeriodSeconds, so an ordinary delete
	// gets Kubernetes' default. If one is ever rendered, it has to cover the
	// budget too.
	const kubernetesDefaultGrace = 30 * time.Second
	c := newCluster("grace")
	g := Groups(c)[0]
	pod := Renderer{}.Pod(c, g, 0, RolePrimary, "pvc", Template(c, g, nil, nil))
	grace := kubernetesDefaultGrace
	if s := pod.Spec.TerminationGracePeriodSeconds; s != nil {
		grace = time.Duration(*s) * time.Second
	}
	if grace <= agent.TerminationBudget {
		t.Fatalf("member Pods are deleted with a %s grace, not more than the agent's %s stop budget", grace, agent.TerminationBudget)
	}
}
