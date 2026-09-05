package operator

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

// TestEveryProbeBoundsItsOwnTimeout.
//
// The kubelet's default timeoutSeconds is ONE SECOND. /livez asks the kube
// API with a 2s deadline and only then asks the peers, so under an API-server
// stall the handler legitimately needs longer than a second -- and the
// kubelet failed it regardless of what the peers would have said. Three
// failures killed the container, which is PostgreSQL: the agent is PID 1. A
// control-plane incident restarted every primary at once, in the very case
// docs/ha.md says the peer failsafe covers.
//
// So the timeout has to exceed what the handler can honestly take, and the
// liveness one has to exceed the kube deadline in particular.
func TestEveryProbeBoundsItsOwnTimeout(t *testing.T) {
	c := &pgshardv1alpha1.PgShardCluster{}
	c.Name, c.Namespace = "t", "default"
	c.Spec.ReplicasPerShard = 3

	g := Groups(c)[len(Groups(c))-1]
	pod := Renderer{}.Pod(c, g, 0, RolePrimary, "pvc-0", Template(c, g, nil, nil))

	var agent *corev1.Container
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == "postgres" || pod.Spec.Containers[i].LivenessProbe != nil {
			agent = &pod.Spec.Containers[i]
			break
		}
	}
	if agent == nil {
		t.Fatal("no container carries the probes")
	}
	for _, c := range []struct {
		name  string
		probe *corev1.Probe
		least int32
	}{
		// Above the 2s kube deadline plus a concurrent round of peers.
		{"liveness", agent.LivenessProbe, 5},
		{"readiness", agent.ReadinessProbe, 3},
		{"startup", agent.StartupProbe, 2},
	} {
		if c.probe == nil {
			t.Errorf("%s probe missing", c.name)
			continue
		}
		if c.probe.TimeoutSeconds < c.least {
			t.Errorf("%s timeoutSeconds = %d, want at least %d (0 means the kubelet's 1s default)",
				c.name, c.probe.TimeoutSeconds, c.least)
		}
		// A timeout at or past the period leaves no gap between attempts.
		if c.probe.PeriodSeconds > 0 && c.probe.TimeoutSeconds >= c.probe.PeriodSeconds {
			t.Errorf("%s timeoutSeconds %d must stay under periodSeconds %d",
				c.name, c.probe.TimeoutSeconds, c.probe.PeriodSeconds)
		}
	}
}
