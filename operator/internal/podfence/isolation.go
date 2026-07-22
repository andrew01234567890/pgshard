package podfence

import (
	"context"
	"fmt"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
)

// IsDispatchProbeSentinel reports whether a pod is the reserved dispatch probe.
func IsDispatchProbeSentinel(pod *corev1.Pod) bool {
	return pod != nil && pod.Annotations[DispatchProbeSentinelAnnotation] == DispatchProbeSentinelValue
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

// sealedParentMatchesLive reports whether the live parent matches a sealed record
// on the full tuple: kind, name, UID, and — verified against the live object, not
// the pod's forgeable owner reference — the stamped contract hash. The sealed
// resourceVersion is recorded for audit but not gated on, because a parent's
// resourceVersion drifts on benign controller status writes during the recreate
// ceremony; the live contract hash is the security-relevant binding.
func sealedParentMatchesLive(receipt *pgshardv1alpha1.PostgreSQLIsolationReceipt, kind, name, uid, liveContractHash string) bool {
	sealed := sealedParentMatch(receipt, kind, uid)
	return sealed != nil && sealed.Name == name && sealed.ContractHash == liveContractHash
}

// podControllerParentSealed reports whether a pod's protected parent is sealed in
// the receipt at its exact live incarnation. A member pod's controller owner is
// its StatefulSet; a supporting pod's controller owner is a ReplicaSet whose own
// controller owner is the sealed Deployment. The parent is fetched authoritatively
// and its live contract hash must equal the sealed hash, so a pod referencing a
// sealed UID whose template was mutated after sealing is rejected.
func podControllerParentSealed(ctx context.Context, reader client.Reader, pod *corev1.Pod, receipt *pgshardv1alpha1.PostgreSQLIsolationReceipt) (bool, error) {
	ref := controllerOwnerRef(pod.OwnerReferences)
	if ref == nil {
		return false, nil
	}
	switch ref.Kind {
	case "StatefulSet":
		statefulSet := &appsv1.StatefulSet{}
		if err := reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: ref.Name}, statefulSet); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		if statefulSet.UID != ref.UID {
			return false, nil
		}
		return sealedParentMatchesLive(receipt, "StatefulSet", statefulSet.Name, string(statefulSet.UID), statefulSet.Spec.Template.Annotations[owned.PodContractHashAnnotation]), nil
	case replicaSetKind:
		replicaSet := &appsv1.ReplicaSet{}
		if err := reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: ref.Name}, replicaSet); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		if replicaSet.UID != ref.UID {
			return false, nil
		}
		deploymentRef := controllerOwnerRef(replicaSet.OwnerReferences)
		if deploymentRef == nil || deploymentRef.Kind != deploymentKind {
			return false, nil
		}
		deployment := &appsv1.Deployment{}
		if err := reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: deploymentRef.Name}, deployment); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		if deployment.UID != deploymentRef.UID {
			return false, nil
		}
		return sealedParentMatchesLive(receipt, "Deployment", deployment.Name, string(deployment.UID), deployment.Spec.Template.Annotations[owned.PodContractHashAnnotation]), nil
	}
	return false, nil
}
