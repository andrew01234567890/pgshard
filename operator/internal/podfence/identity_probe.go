package podfence

import (
	"sync"

	corev1 "k8s.io/api/core/v1"
)

const (
	// IdentityProbeAnnotation carries the probe token on disposable
	// identity-probe workloads. When the admission webhooks see it they record
	// the ACTUAL authenticated controller username request.userInfo carries and
	// admit the benign probe object.
	IdentityProbeAnnotation = "pgshard.io/identity-probe"

	// The four controller roles the probe captures.
	IdentityRoleStatefulSet = "statefulset"
	IdentityRoleReplicaSet  = "replicaset"
	IdentityRoleDeployment  = "deployment"
	IdentityRoleHPA         = "hpa"
)

// IdentityObservationStore is a process-shared, thread-safe record of the
// authenticated controller usernames the admission webhooks observed for a probe
// token. The manager creates one and passes it to both the webhook handlers and
// the activation identity prober, so the prober reads exactly what the webhooks
// authenticated — not a self-asserted value.
type IdentityObservationStore struct {
	mu           sync.Mutex
	observations map[string]map[string]string
}

func NewIdentityObservationStore() *IdentityObservationStore {
	return &IdentityObservationStore{observations: map[string]map[string]string{}}
}

func (s *IdentityObservationStore) record(token, role, username string) {
	if token == "" || role == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.observations[token] == nil {
		s.observations[token] = map[string]string{}
	}
	s.observations[token][role] = username
}

// Observed returns a copy of the roles→username map for a token.
func (s *IdentityObservationStore) Observed(token string) map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]string{}
	for role, username := range s.observations[token] {
		out[role] = username
	}
	return out
}

// Forget drops a token's observations after the probe completes.
func (s *IdentityObservationStore) Forget(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.observations, token)
}

func identityProbeToken(annotations map[string]string) string {
	return annotations[IdentityProbeAnnotation]
}

// podIdentityProbeRole derives which controller created a probe pod from its
// controller owner reference.
func podIdentityProbeRole(pod *corev1.Pod) string {
	ref := controllerOwnerRef(pod.OwnerReferences)
	if ref == nil {
		return ""
	}
	switch ref.Kind {
	case "StatefulSet":
		return IdentityRoleStatefulSet
	case replicaSetKind:
		return IdentityRoleReplicaSet
	}
	return ""
}
