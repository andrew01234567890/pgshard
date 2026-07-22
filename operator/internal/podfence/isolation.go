package podfence

import (
	"context"
	"fmt"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
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

// podControllerParentSealed reports whether a pod's protected parent is sealed in
// the receipt. A member pod's controller owner is its StatefulSet; a supporting
// pod's controller owner is a ReplicaSet whose own controller owner is the sealed
// Deployment. During ACTIVATING_RECREATE only pods whose parent is sealed at its
// exact incarnation may be created.
func podControllerParentSealed(ctx context.Context, reader client.Reader, pod *corev1.Pod, receipt *pgshardv1alpha1.PostgreSQLIsolationReceipt) (bool, error) {
	ref := controllerOwnerRef(pod.OwnerReferences)
	if ref == nil {
		return false, nil
	}
	switch ref.Kind {
	case "StatefulSet":
		return sealedParentMatch(receipt, "StatefulSet", string(ref.UID)) != nil, nil
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
		return sealedParentMatch(receipt, "Deployment", string(deploymentRef.UID)) != nil, nil
	}
	return false, nil
}
