package podfence

import (
	"context"
	"fmt"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// LivePodVerdict is the outcome of the shared full-contract validation of one
// live pod during activation inventory.
type LivePodVerdict struct {
	// Reason is empty for a fully valid pod; otherwise it names the violated
	// invariant.
	Reason string
	// SealedParent is the sealed parent the pod's verified live owner chain
	// resolves to, populated only when a receipt with sealed parents was
	// supplied and the chain matched. The activation reconciler counts guarded
	// replacements per sealed parent through it.
	SealedParent *pgshardv1alpha1.SealedParent
}

// ValidateLiveProtectedPod runs the SAME full contract validation the admission
// webhooks apply — managed classification, live-parent resolution and controller
// provenance, the full LiveNormalForm comparator against the stamped parent
// template, contract-hash recomputation, digest pinning, and the
// supporting-generation barrier — against a live BOUND pod during activation
// inventory. When requireSealed is true the pod's verified live owner chain must
// additionally resolve to a sealed parent at its exact sealed incarnation
// (UID + name + spec generation + contract hash).
//
// The node identity is authenticated live: the pod's bound node must exist and
// its UID must equal the node-UID residue the binding webhook stamped. The
// boot-ID and topology residue were authenticated by the binding webhook at bind
// time and are validated structurally by the comparator.
func ValidateLiveProtectedPod(ctx context.Context, reader client.Reader, pod *corev1.Pod, receipt *pgshardv1alpha1.PostgreSQLIsolationReceipt, requireSealed bool) (LivePodVerdict, error) {
	kind, shard, member, clusterName := classifyContractPod(pod)
	if kind == contractPodUnmanaged {
		return LivePodVerdict{Reason: "pod is not a classified managed PostgreSQL pod"}, nil
	}
	if pod.Annotations[owned.PodContractHashAnnotation] == "" {
		return LivePodVerdict{Reason: "pod carries no reconciler contract stamp"}, nil
	}
	if pod.Spec.NodeName == "" {
		return LivePodVerdict{Reason: "pod is not bound to a node"}, nil
	}

	cluster := &pgshardv1alpha1.PgShardCluster{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: clusterName}, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return LivePodVerdict{Reason: "owning PgShardCluster no longer exists"}, nil
		}
		return LivePodVerdict{}, fmt.Errorf("read PgShardCluster for live pod inventory: %w", err)
	}
	if cluster.UID == "" || types.UID(pod.Annotations[owned.PostgreSQLPodClusterUIDAnnotation]) != cluster.UID {
		return LivePodVerdict{Reason: "pod does not belong to the live PgShardCluster UID"}, nil
	}

	node := &corev1.Node{}
	if err := reader.Get(ctx, types.NamespacedName{Name: pod.Spec.NodeName}, node); err != nil {
		if apierrors.IsNotFound(err) {
			return LivePodVerdict{Reason: "pod's bound node no longer exists"}, nil
		}
		return LivePodVerdict{}, fmt.Errorf("read bound node for live pod inventory: %w", err)
	}
	if string(node.UID) != pod.Annotations[NodeUIDAnnotation] {
		return LivePodVerdict{Reason: "pod's node-UID residue does not match the live node"}, nil
	}

	class, template, provenance, response := resolveStampedParent(ctx, reader, pod.Namespace, pod, kind, shard, member, clusterName, cluster)
	if response != nil {
		return LivePodVerdict{Reason: response.Result.Message}, nil
	}
	templateGeneration := template.Annotations[owned.PodSecurityGenerationAnnotation]
	if pod.Annotations[owned.PodSecurityGenerationAnnotation] != templateGeneration {
		return LivePodVerdict{Reason: "pod security generation does not match its stamped parent template"}, nil
	}
	generation, ok := canonicalSecurityGeneration(templateGeneration)
	if !ok {
		return LivePodVerdict{Reason: "stamped parent template carries an invalid security generation"}, nil
	}

	evidence := &owned.BindingEvidence{
		NodeName: pod.Spec.NodeName,
		NodeUID:  string(node.UID),
		BootID:   pod.Annotations[NodeBootIDAnnotation],
		Zone:     pod.Labels[corev1.LabelTopologyZone],
		Region:   pod.Labels[corev1.LabelTopologyRegion],
	}
	nc := owned.NormContext{
		Class:       class,
		ClusterName: clusterName,
		Namespace:   pod.Namespace,
		Shard:       shard,
		Member:      member,
		Provenance:  provenance,
		Binding:     evidence,
	}
	if err := owned.ComparePodToStampedTemplate(nc, pod.ObjectMeta, pod.Spec, template.ObjectMeta, template.Spec, owned.StageLive, true); err != nil {
		return LivePodVerdict{Reason: fmt.Sprintf("live pod does not match its stamped contract: %v", err)}, nil
	}
	want := template.Annotations[owned.PodContractHashAnnotation]
	if want == "" || pod.Annotations[owned.PodContractHashAnnotation] != want {
		return LivePodVerdict{Reason: "live pod contract hash does not match its stamped parent template"}, nil
	}
	got, err := owned.HashAdmittedPod(nc, pod.ObjectMeta, pod.Spec, owned.StageLive, string(cluster.UID), generation)
	if err != nil {
		return LivePodVerdict{}, fmt.Errorf("recompute live pod contract hash: %w", err)
	}
	if got != want {
		return LivePodVerdict{Reason: "live pod contract hash recomputation does not match its stamped parent template"}, nil
	}
	if response := validateSupportingAdmission(cluster, kind, class, provenance, &pod.Spec, want, generation); response != nil {
		return LivePodVerdict{Reason: response.Result.Message}, nil
	}

	verdict := LivePodVerdict{}
	if requireSealed {
		sealed, err := podControllerParentSealed(ctx, reader, pod, receipt)
		if err != nil {
			return LivePodVerdict{}, err
		}
		if sealed == nil {
			return LivePodVerdict{Reason: "pod's parent is not a sealed parent at its exact sealed incarnation"}, nil
		}
		verdict.SealedParent = sealed
	}
	return verdict, nil
}
