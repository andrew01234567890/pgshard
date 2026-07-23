package podfence

import (
	"context"
	"encoding/json"
	"fmt"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// DispatchProbeSentinelAnnotation marks a pod as the activation dispatch
	// probe. A pod carrying it at the sentinel value is always denied by the
	// PodCreate webhook, in every phase, with DispatchProbeSentinelMessage.
	DispatchProbeSentinelAnnotation = "pgshard.io/dispatch-probe-sentinel"
	DispatchProbeSentinelValue      = "v1"
	// DispatchProbeSentinelName is the reserved name the probe uses; it never
	// persists (dryRun=All) and the preflight additionally confirms no object of
	// this name exists.
	DispatchProbeSentinelName = "pgshard-dispatch-probe-sentinel"
	// DispatchProbeSentinelMessage is the exact denial the preflight requires
	// from every backend. Any other outcome means that backend does not dispatch
	// Pod CREATE to this webhook.
	DispatchProbeSentinelMessage = "pgshard dispatch-probe sentinel: this Pod create is always denied by the pgshard PodCreate webhook"

	// The label-gated isolation webhooks (workload, connect, LimitRange) dispatch
	// only for a namespace carrying the isolation-enforcing label, which every
	// API-server backend evaluates from its OWN namespace-informer cache. The
	// pre-enforcement convergence probe therefore submits one sentinel per GATED
	// webhook to each backend: a converged backend (label visible → webhook
	// dispatched) returns the exact sentinel denial below, while a stale backend
	// (label not yet in its cache → webhook skipped) does not. Each handler denies
	// its sentinel FIRST, in every phase, so the exact denial proves dispatch and
	// nothing else.
	//
	// WorkloadDispatchProbeSentinel* marks a dryRun apps workload create.
	WorkloadDispatchProbeSentinelMessage = "pgshard dispatch-probe sentinel: this workload write is always denied by the pgshard workload-integrity webhook"
	// LimitRangeDispatchProbeSentinelMessage answers a dryRun sentinel LimitRange
	// create (which the webhook denies anyway; the distinct message pins the probe
	// to this exact handler).
	LimitRangeDispatchProbeSentinelMessage = "pgshard dispatch-probe sentinel: this LimitRange write is always denied by the pgshard LimitRange webhook"
	// ConnectDispatchProbeSentinelName is a reserved Pod name: a CONNECT
	// (exec/attach/portforward/proxy) addressed to it is ALWAYS denied by the
	// connect webhook with ConnectDispatchProbeSentinelMessage, in every phase and
	// in both webhook entries. No managed pod ever carries this name; a
	// non-dispatching backend instead fails the request with a NotFound for the
	// nonexistent pod, so the probe distinguishes the two without any real pod.
	ConnectDispatchProbeSentinelName    = "pgshard-connect-probe-sentinel"
	ConnectDispatchProbeSentinelMessage = "pgshard dispatch-probe sentinel: connecting to the reserved sentinel Pod is always denied by the pgshard connect webhook"
)

// IsDispatchProbeSentinel reports whether a pod is the reserved dispatch probe.
func IsDispatchProbeSentinel(pod *corev1.Pod) bool {
	return pod != nil && pod.Annotations[DispatchProbeSentinelAnnotation] == DispatchProbeSentinelValue
}

// objectCarriesDispatchProbeSentinel reports whether an admission object's
// metadata carries the dispatch-probe sentinel annotation. It decodes only the
// metadata, so a malformed body that a handler's strict decoder would reject
// cannot dodge the always-deny sentinel branch.
func objectCarriesDispatchProbeSentinel(raw []byte) bool {
	partial := &metav1.PartialObjectMetadata{}
	if err := json.Unmarshal(raw, partial); err != nil {
		return false
	}
	return partial.Annotations[DispatchProbeSentinelAnnotation] == DispatchProbeSentinelValue
}

// namespaceIsolationReceipt authoritatively resolves the isolation phase of a
// namespace. It reads the durable receipt off every PgShardCluster in the
// namespace via the uncached reader, returning the first non-INACTIVE receipt.
// A nil receipt means the namespace is not activating and admission must behave
// exactly as it did before activation. Every admission handler consults it per
// request; there is deliberately no cached fast path, so a manager restart can
// never open an allow-window before the durable phase is loaded.
func namespaceIsolationReceipt(ctx context.Context, reader client.Reader, namespace string) (*pgshardv1alpha1.PostgreSQLIsolationReceipt, error) {
	list := &pgshardv1alpha1.PgShardClusterList{}
	if err := reader.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("read isolation receipt for namespace %q: %w", namespace, err)
	}
	for i := range list.Items {
		receipt := list.Items[i].Status.IsolationReceipt
		if receipt != nil && receipt.Phase != "" && receipt.Phase != pgshardv1alpha1.IsolationInactive {
			return receipt, nil
		}
	}
	return nil, nil
}

func isolationPhase(receipt *pgshardv1alpha1.PostgreSQLIsolationReceipt) pgshardv1alpha1.IsolationPhase {
	if receipt == nil {
		return pgshardv1alpha1.IsolationInactive
	}
	return receipt.Phase
}

// sealedParentMatch returns the sealed parent of the given kind and UID, or nil.
func sealedParentMatch(receipt *pgshardv1alpha1.PostgreSQLIsolationReceipt, kind, uid string) *pgshardv1alpha1.SealedParent {
	if receipt == nil || uid == "" {
		return nil
	}
	for i := range receipt.SealedParents {
		if receipt.SealedParents[i].Kind == kind && receipt.SealedParents[i].UID == uid {
			return &receipt.SealedParents[i]
		}
	}
	return nil
}

// sealedParentMatchesLive returns the sealed record matching the live parent on
// the full tuple: kind, name, UID, the spec incarnation (metadata.generation,
// which unlike resourceVersion never moves on status writes), and — verified
// against the live object, not the pod's forgeable owner reference — the stamped
// contract hash. The sealed resourceVersion is recorded for audit only. A parent
// whose spec drifted after sealing (generation or hash change) no longer matches;
// the reconciler detects that drift and RESEALS rather than deadlocking.
func sealedParentMatchesLive(receipt *pgshardv1alpha1.PostgreSQLIsolationReceipt, kind, name, uid string, liveGeneration int64, liveContractHash string) *pgshardv1alpha1.SealedParent {
	sealed := sealedParentMatch(receipt, kind, uid)
	if sealed != nil && sealed.Name == name && sealed.Generation == liveGeneration && sealed.ContractHash == liveContractHash {
		return sealed
	}
	return nil
}

// podControllerParentSealed resolves a pod's protected parent and returns its
// sealed record when it is sealed at its exact live incarnation, or nil. A
// member pod's controller owner is its StatefulSet; a supporting pod's
// controller owner is a ReplicaSet whose own controller owner is the sealed
// Deployment. The parent is fetched authoritatively and its live spec generation
// and contract hash must equal the sealed record, so a pod referencing a sealed
// UID whose spec was mutated after sealing is rejected.
func podControllerParentSealed(ctx context.Context, reader client.Reader, pod *corev1.Pod, receipt *pgshardv1alpha1.PostgreSQLIsolationReceipt) (*pgshardv1alpha1.SealedParent, error) {
	ref := controllerOwnerRef(pod.OwnerReferences)
	if ref == nil {
		return nil, nil
	}
	switch ref.Kind {
	case "StatefulSet":
		statefulSet := &appsv1.StatefulSet{}
		if err := reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: ref.Name}, statefulSet); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, nil
			}
			return nil, err
		}
		if statefulSet.UID != ref.UID {
			return nil, nil
		}
		return sealedParentMatchesLive(receipt, "StatefulSet", statefulSet.Name, string(statefulSet.UID), statefulSet.Generation, statefulSet.Spec.Template.Annotations[owned.PodContractHashAnnotation]), nil
	case replicaSetKind:
		replicaSet := &appsv1.ReplicaSet{}
		if err := reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: ref.Name}, replicaSet); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, nil
			}
			return nil, err
		}
		if replicaSet.UID != ref.UID {
			return nil, nil
		}
		deploymentRef := controllerOwnerRef(replicaSet.OwnerReferences)
		if deploymentRef == nil || deploymentRef.Kind != deploymentKind {
			return nil, nil
		}
		deployment := &appsv1.Deployment{}
		if err := reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: deploymentRef.Name}, deployment); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, nil
			}
			return nil, err
		}
		if deployment.UID != deploymentRef.UID {
			return nil, nil
		}
		return sealedParentMatchesLive(receipt, "Deployment", deployment.Name, string(deployment.UID), deployment.Generation, deployment.Spec.Template.Annotations[owned.PodContractHashAnnotation]), nil
	}
	return nil, nil
}
