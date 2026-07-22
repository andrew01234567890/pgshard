package podfence

import (
	"context"
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

// IdentityOwnerKey names a registered probe owner: "<Kind>/<name>". The prober
// registers the keys BEFORE creating the probe objects (the names are chosen by
// the prober, so no create/registration race exists), and the webhooks verify a
// claimed owner against the LIVE object — Get by name, UID equality with the
// owner reference, and the owner carrying the same probe token — before
// recording. A forged owner reference on an attacker pod therefore never
// records: the fenced-namespace workload webhooks only admit operator-authored
// StatefulSets/Deployments (and deployment-controller-authored ReplicaSets), so
// an attacker cannot materialize a live owner carrying the probe token.
func IdentityOwnerKey(kind, name string) string {
	return kind + "/" + name
}

type identityObservation struct {
	username string
	conflict bool
}

// IdentityObservationStore is a process-shared, thread-safe record of the
// authenticated controller usernames the admission webhooks observed for a probe
// token. Registrations bind a token to the authoritative probe owner keys;
// observations for unregistered tokens or owners are REJECTED. Observations are
// append-only: the first verified record for a role wins, and any later verified
// record with a different username marks the role CONFLICTED, failing the probe
// closed rather than letting a last writer overwrite it.
type IdentityObservationStore struct {
	mu           sync.Mutex
	registered   map[string]map[string]struct{}
	observations map[string]map[string]identityObservation
}

func NewIdentityObservationStore() *IdentityObservationStore {
	return &IdentityObservationStore{
		registered:   map[string]map[string]struct{}{},
		observations: map[string]map[string]identityObservation{},
	}
}

// Register binds a probe token to its authoritative owner keys. It must be
// called before the probe objects are created.
func (s *IdentityObservationStore) Register(token string, ownerKeys ...string) {
	if token == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	owners := map[string]struct{}{}
	for _, key := range ownerKeys {
		owners[key] = struct{}{}
	}
	s.registered[token] = owners
	s.observations[token] = map[string]identityObservation{}
}

func (s *IdentityObservationStore) record(token, role, username, ownerKey string) {
	if token == "" || role == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	owners, registered := s.registered[token]
	if !registered {
		return
	}
	if _, allowed := owners[ownerKey]; !allowed {
		return
	}
	existing, seen := s.observations[token][role]
	if !seen {
		s.observations[token][role] = identityObservation{username: username}
		return
	}
	if existing.username != username {
		existing.conflict = true
		s.observations[token][role] = existing
	}
}

// Observed returns a copy of the roles→username map for a token and whether any
// role saw conflicting verified observations (which fails the probe closed).
func (s *IdentityObservationStore) Observed(token string) (map[string]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]string{}
	conflicted := false
	for role, observation := range s.observations[token] {
		out[role] = observation.username
		conflicted = conflicted || observation.conflict
	}
	return out, conflicted
}

// Forget drops a token's registration and observations after the probe
// completes.
func (s *IdentityObservationStore) Forget(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.registered, token)
	delete(s.observations, token)
}

func identityProbeToken(annotations map[string]string) string {
	return annotations[IdentityProbeAnnotation]
}

// recordPodIdentityObservation records the authenticated username of the
// controller that created a probe pod, but ONLY after authenticating the claimed
// owner chain against the live API: the owner reference must resolve by name to
// a live object with the same UID that itself carries the probe token (and, for
// the replicaset role, whose own controller owner is the live probe Deployment).
// The recorded fact — "username U created a pod controller-owned by live probe
// object O" — is true regardless of the pod's final admission outcome, because
// request.userInfo is authenticated by the API server. It never relaxes the
// admission decision.
func recordPodIdentityObservation(ctx context.Context, reader client.Reader, store *IdentityObservationStore, pod *corev1.Pod, username string) error {
	ref := controllerOwnerRef(pod.OwnerReferences)
	if ref == nil {
		return nil
	}
	switch ref.Kind {
	case "StatefulSet":
		statefulSet := &appsv1.StatefulSet{}
		if err := reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: ref.Name}, statefulSet); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		token := identityProbeToken(statefulSet.Annotations)
		if token == "" || statefulSet.UID != ref.UID {
			return nil
		}
		store.record(token, IdentityRoleStatefulSet, username, IdentityOwnerKey("StatefulSet", statefulSet.Name))
	case replicaSetKind:
		replicaSet := &appsv1.ReplicaSet{}
		if err := reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: ref.Name}, replicaSet); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if replicaSet.UID != ref.UID {
			return nil
		}
		deploymentRef := controllerOwnerRef(replicaSet.OwnerReferences)
		if deploymentRef == nil || deploymentRef.Kind != deploymentKind {
			return nil
		}
		deployment := &appsv1.Deployment{}
		if err := reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: deploymentRef.Name}, deployment); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		token := identityProbeToken(deployment.Annotations)
		if token == "" || deployment.UID != deploymentRef.UID {
			return nil
		}
		store.record(token, IdentityRoleReplicaSet, username, IdentityOwnerKey("Deployment", deployment.Name))
	}
	return nil
}
