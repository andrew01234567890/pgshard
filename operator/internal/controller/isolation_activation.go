package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/operator/internal/podfence"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const isolationActivationBlockedCondition = "IsolationActivationBlocked"

// reconcileIsolationActivation drives the durable per-namespace isolation
// activation state machine. Because preflightConverged is the step-7b stub, in
// production it only ever observes INACTIVE and returns without starting
// activation, so the honest flow is unchanged. The QUIESCE/RECREATE/ACTIVE
// drives are fully built and unit-tested by pre-seeding a receipt.
func (r *PgShardClusterReconciler) reconcileIsolationActivation(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (bool, error) {
	reader := r.authoritativeReader()
	namespace := &corev1.Namespace{}
	if err := reader.Get(ctx, types.NamespacedName{Name: cluster.Namespace}, namespace); err != nil {
		return false, fmt.Errorf("read namespace for isolation activation: %w", err)
	}
	receipt := cluster.Status.IsolationReceipt

	// The receipt is bound to the namespace UID: a recreated namespace can never
	// inherit an activation.
	if receipt != nil && receipt.NamespaceUID != string(namespace.UID) {
		cluster.Status.IsolationReceipt = nil
		if err := r.Status().Update(ctx, cluster); err != nil {
			return false, fmt.Errorf("reset isolation receipt after namespace recreation: %w", err)
		}
		return true, nil
	}

	switch isolationReceiptPhase(receipt) {
	case pgshardv1alpha1.IsolationInactive:
		// Opt-in (trigger + fenced namespace) is checked BEFORE any prerequisite
		// or probe so a non-opted-in cluster stays silently inactive and never
		// probes the API servers.
		if !isolationOptedIn(cluster, namespace) {
			return false, nil
		}
		// A legacy cleartext cluster must never receive an ACTIVE receipt: require
		// the durable server-tls-v1 transport policy and complete TLS checkpoints.
		if !hasReplicationTLSPrerequisite(cluster) {
			r.setIsolationPreflightCondition(cluster, isolationTLSPrerequisiteCondition, "TransportNotHardened", "isolation activation requires the durable server-tls-v1 replication transport policy and complete replication-TLS checkpoints (single-member activation-TLS parity is a ratified follow-up)")
			return false, r.Status().Update(ctx, cluster)
		}
		// Namespace-wide activation seals and recreates every protected pod in the
		// namespace, so it is unsafe with more than one cluster present.
		single, err := r.exactlyOneClusterInNamespace(ctx, cluster)
		if err != nil {
			return false, err
		}
		if !single {
			r.setIsolationPreflightCondition(cluster, isolationMultipleClustersCondition, "MultipleClusters", "isolation activation requires exactly one PgShardCluster in the fenced namespace")
			return false, r.Status().Update(ctx, cluster)
		}
		// A defaulting LimitRange could mutate recreated pods after stamping and
		// break the comparator; refuse activation while any exists (the webhook
		// keeps new ones out during and after activation).
		if name, err := r.limitRangePresent(ctx, cluster.Namespace); err != nil {
			return false, err
		} else if name != "" {
			r.setIsolationPreflightCondition(cluster, isolationLimitRangePresentCondition, "LimitRangePresent", fmt.Sprintf("isolation activation is blocked while LimitRange %q exists in the fenced namespace", name))
			return false, r.Status().Update(ctx, cluster)
		}
		// A supporting-generation roll in progress (a populated prior) means the
		// admissible set is in flux; do not begin activation until every class has
		// converged.
		if supportingRollInProgress(cluster) {
			r.setIsolationPreflightCondition(cluster, isolationSupportingRollingCondition, "SupportingRolling", "isolation activation is withheld while a supporting-generation roll is in progress")
			return false, r.Status().Update(ctx, cluster)
		}
		proof, ok := r.preflightConverged(ctx, cluster)
		if !ok {
			// The preflight surfaced its own typed condition; withhold activation.
			return false, r.Status().Update(ctx, cluster)
		}
		cluster.Status.IsolationReceipt = &pgshardv1alpha1.PostgreSQLIsolationReceipt{
			NamespaceUID:       string(namespace.UID),
			Phase:              pgshardv1alpha1.IsolationActivatingQuiesce,
			SecurityGeneration: currentSecurityGeneration(cluster),
			DispatchTupleHash:  proof.tupleHash,
			ActivatedAt:        metav1.Now(),
		}
		clearIsolationPreflightConditions(cluster)
		if err := r.Status().Update(ctx, cluster); err != nil {
			return false, fmt.Errorf("enter isolation quiesce: %w", err)
		}
		return true, nil
	case pgshardv1alpha1.IsolationActivatingQuiesce:
		return r.driveIsolationQuiesce(ctx, cluster, namespace)
	case pgshardv1alpha1.IsolationActivatingRecreate:
		return r.driveIsolationRecreate(ctx, cluster)
	case pgshardv1alpha1.IsolationActive:
		return r.driveIsolationActive(ctx, cluster)
	}
	return false, nil
}

// driveIsolationQuiesce seals every protected parent at its exact incarnation,
// drains the pinned request-timeout so any pre-quiesce in-flight create has
// landed, then inventories the namespace. When the inventory is clean it advances
// to ACTIVATING_RECREATE.
func (r *PgShardClusterReconciler) driveIsolationQuiesce(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, namespace *corev1.Namespace) (bool, error) {
	if valid, err := r.revalidateDispatchTuple(ctx, cluster); err != nil || !valid {
		return true, err
	}
	receipt := cluster.Status.IsolationReceipt
	if len(receipt.SealedParents) == 0 {
		sealed, err := r.sealProtectedParents(ctx, cluster)
		if err != nil {
			return false, err
		}
		receipt.SealedParents = sealed
		receipt.ActivatedAt = metav1.Now()
		if err := r.Status().Update(ctx, cluster); err != nil {
			return false, fmt.Errorf("seal protected parents: %w", err)
		}
		return true, nil
	}
	// A full-second safety margin covers the one-second truncation of the
	// metav1.Time drain start plus modest clock skew, so the drain never completes
	// early and lets a pre-quiesce in-flight create persist.
	if time.Since(receipt.ActivatedAt.Time) < supportingRevocationDrain+time.Second {
		return true, nil
	}
	blocked, residue, err := r.inventoryNamespace(ctx, cluster)
	if err != nil {
		return false, err
	}
	if blocked != "" {
		return r.blockIsolation(ctx, cluster, blocked)
	}
	// Seal the exact pre-guard protected pod UIDs; recreate authenticates by
	// UID-deletion, never by CreationTimestamp.
	pending, err := r.protectedPodUIDs(ctx, cluster)
	if err != nil {
		return false, err
	}
	receipt.ResidueProfileHash = residue
	receipt.RecreatePendingUIDs = pending
	receipt.Phase = pgshardv1alpha1.IsolationActivatingRecreate
	receipt.ActivatedAt = metav1.Now()
	meta.RemoveStatusCondition(&cluster.Status.Conditions, isolationActivationBlockedCondition)
	if err := r.Status().Update(ctx, cluster); err != nil {
		return false, fmt.Errorf("advance to isolation recreate: %w", err)
	}
	return true, nil
}

// driveIsolationRecreate deletes every sealed pre-guard protected pod (by UID,
// including terminating ones) so the controllers recreate each under the guard,
// and only advances to ACTIVE once none of the sealed UIDs remains and the full
// inventory validates. It never infers authentication from CreationTimestamp.
func (r *PgShardClusterReconciler) driveIsolationRecreate(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (bool, error) {
	if valid, err := r.revalidateDispatchTuple(ctx, cluster); err != nil || !valid {
		return true, err
	}
	receipt := cluster.Status.IsolationReceipt
	reader := r.authoritativeReader()
	pending := map[string]struct{}{}
	for _, uid := range receipt.RecreatePendingUIDs {
		pending[uid] = struct{}{}
	}
	pods := &corev1.PodList{}
	if err := reader.List(ctx, pods, client.InNamespace(cluster.Namespace)); err != nil {
		return false, fmt.Errorf("list pods for isolation recreate: %w", err)
	}
	remaining := 0
	for i := range pods.Items {
		pod := &pods.Items[i]
		if _, sealed := pending[string(pod.UID)]; !sealed {
			continue
		}
		remaining++
		if pod.DeletionTimestamp == nil {
			if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
				return false, fmt.Errorf("delete pre-guard protected pod %s: %w", pod.Name, err)
			}
		}
	}
	if remaining > 0 {
		// Some pre-guard pod is still being deleted (or its guarded replacement not
		// yet created); wait until every sealed UID is gone.
		return true, nil
	}
	blocked, residue, err := r.inventoryNamespace(ctx, cluster)
	if err != nil {
		return false, err
	}
	if blocked != "" {
		return r.blockIsolation(ctx, cluster, blocked)
	}
	receipt.ResidueProfileHash = residue
	receipt.RecreatePendingUIDs = nil
	receipt.Phase = pgshardv1alpha1.IsolationActive
	receipt.MinAcceptableSecurityGeneration = receipt.SecurityGeneration
	receipt.ActivatedAt = metav1.Now()
	meta.RemoveStatusCondition(&cluster.Status.Conditions, isolationActivationBlockedCondition)
	if err := r.Status().Update(ctx, cluster); err != nil {
		return false, fmt.Errorf("activate isolation: %w", err)
	}
	return false, nil
}

// protectedPodUIDs returns the UIDs of every live protected pod in the namespace.
func (r *PgShardClusterReconciler) protectedPodUIDs(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) ([]string, error) {
	pods := &corev1.PodList{}
	if err := r.authoritativeReader().List(ctx, pods, client.InNamespace(cluster.Namespace)); err != nil {
		return nil, fmt.Errorf("list protected pods to seal for recreate: %w", err)
	}
	uids := []string{}
	for i := range pods.Items {
		if isProtectedInventoryPod(&pods.Items[i]) {
			uids = append(uids, string(pods.Items[i].UID))
		}
	}
	sort.Strings(uids)
	return uids, nil
}

// driveIsolationActive re-inventories under enforcement. On drift — a pod that no
// longer validates the full live contract — it does not merely raise a condition;
// it returns the receipt to ACTIVATING_QUIESCE so the parents are re-sealed, the
// namespace re-drained, and every protected pod re-recreated under the guard.
func (r *PgShardClusterReconciler) driveIsolationActive(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (bool, error) {
	blocked, _, err := r.inventoryNamespace(ctx, cluster)
	if err != nil {
		return false, err
	}
	if blocked != "" {
		receipt := cluster.Status.IsolationReceipt
		receipt.Phase = pgshardv1alpha1.IsolationActivatingQuiesce
		receipt.SealedParents = nil
		receipt.RecreatePendingUIDs = nil
		receipt.ActivatedAt = metav1.Now()
		r.blockIsolationCondition(cluster, blocked)
		if err := r.Status().Update(ctx, cluster); err != nil {
			return false, fmt.Errorf("remediate isolation drift by re-quiescing: %w", err)
		}
		return true, nil
	}
	if meta.FindStatusCondition(cluster.Status.Conditions, isolationActivationBlockedCondition) != nil {
		meta.RemoveStatusCondition(&cluster.Status.Conditions, isolationActivationBlockedCondition)
		if err := r.Status().Update(ctx, cluster); err != nil {
			return false, err
		}
	}
	return false, nil
}

func (r *PgShardClusterReconciler) blockIsolationCondition(cluster *pgshardv1alpha1.PgShardCluster, podName string) {
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               isolationActivationBlockedCondition,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: cluster.Generation,
		Reason:             "UnauthenticatedPod",
		Message:            fmt.Sprintf("isolation activation is blocked by pod %s, which does not satisfy the contract at the current generation", podName),
	})
}

func (r *PgShardClusterReconciler) blockIsolation(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, podName string) (bool, error) {
	r.blockIsolationCondition(cluster, podName)
	if err := r.Status().Update(ctx, cluster); err != nil {
		return false, fmt.Errorf("surface isolation activation block: %w", err)
	}
	return true, nil
}

// sealProtectedParents records every protected parent (member StatefulSets and
// supporting Deployments) at its exact live incarnation and contract hash.
func (r *PgShardClusterReconciler) sealProtectedParents(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) ([]pgshardv1alpha1.SealedParent, error) {
	reader := r.authoritativeReader()
	sealed := []pgshardv1alpha1.SealedParent{}

	statefulSets := &appsv1.StatefulSetList{}
	if err := reader.List(ctx, statefulSets, client.InNamespace(cluster.Namespace)); err != nil {
		return nil, fmt.Errorf("list StatefulSets to seal: %w", err)
	}
	for i := range statefulSets.Items {
		set := &statefulSets.Items[i]
		if set.Labels[owned.ComponentLabel] != "postgresql" || !metav1.IsControlledBy(set, cluster) {
			continue
		}
		sealed = append(sealed, pgshardv1alpha1.SealedParent{
			Kind: "StatefulSet", Name: set.Name, UID: string(set.UID), ResourceVersion: set.ResourceVersion,
			ContractHash: set.Spec.Template.Annotations[owned.PodContractHashAnnotation],
		})
	}

	deployments := &appsv1.DeploymentList{}
	if err := reader.List(ctx, deployments, client.InNamespace(cluster.Namespace)); err != nil {
		return nil, fmt.Errorf("list Deployments to seal: %w", err)
	}
	for i := range deployments.Items {
		deployment := &deployments.Items[i]
		component := deployment.Labels[owned.ComponentLabel]
		if (component != "pooler" && component != "orchestrator") || !metav1.IsControlledBy(deployment, cluster) {
			continue
		}
		sealed = append(sealed, pgshardv1alpha1.SealedParent{
			Kind: "Deployment", Name: deployment.Name, UID: string(deployment.UID), ResourceVersion: deployment.ResourceVersion,
			ContractHash: deployment.Spec.Template.Annotations[owned.PodContractHashAnnotation],
		})
	}
	sort.Slice(sealed, func(a, b int) bool {
		if sealed[a].Kind != sealed[b].Kind {
			return sealed[a].Kind < sealed[b].Kind
		}
		return sealed[a].Name < sealed[b].Name
	})
	return sealed, nil
}

// inventoryNamespace enumerates every pod in the namespace. A foreign pod, a
// managed-looking pod with a malformed identity, or a managed pod that is not
// stamped at or above the receipt's security generation blocks activation and is
// named. When the inventory is clean it returns a deterministic residue-profile
// hash of the now-canonical stamped pods.
func (r *PgShardClusterReconciler) inventoryNamespace(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (string, string, error) {
	reader := r.authoritativeReader()
	pods := &corev1.PodList{}
	if err := reader.List(ctx, pods, client.InNamespace(cluster.Namespace)); err != nil {
		return "", "", fmt.Errorf("inventory namespace pods: %w", err)
	}
	floor := currentSecurityGeneration(cluster)
	if cluster.Status.IsolationReceipt != nil && cluster.Status.IsolationReceipt.SecurityGeneration > floor {
		floor = cluster.Status.IsolationReceipt.SecurityGeneration
	}
	fingerprints := make([]string, 0, len(pods.Items))
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil {
			continue
		}
		kind := inventoryClass(pod)
		if kind == "" {
			return pod.Name, "", nil
		}
		hash := pod.Annotations[owned.PodContractHashAnnotation]
		if hash == "" {
			return pod.Name, "", nil
		}
		generation, err := strconv.ParseInt(pod.Annotations[owned.PodSecurityGenerationAnnotation], 10, 64)
		if err != nil || generation < floor {
			return pod.Name, "", nil
		}
		// Every protected pod must be digest-pinned once isolation is active; each
		// was already fully validated (LiveNormalForm + parent + hash + provenance)
		// at its guarded create during RECREATE.
		if owned.ValidateProtectedImagesDigestPinned(&pod.Spec) != nil {
			return pod.Name, "", nil
		}
		fingerprints = append(fingerprints, kind+":"+hash+":"+strconv.FormatInt(generation, 10))
	}
	sort.Strings(fingerprints)
	sum := sha256.Sum256([]byte(strings.Join(fingerprints, "\n")))
	return "", hex.EncodeToString(sum[:]), nil
}

// inventoryClass returns "member" or "supporting" for a protected pod, or "" for
// a foreign or malformed-identity pod (which blocks activation).
func inventoryClass(pod *corev1.Pod) string {
	cluster := pod.Labels[owned.ClusterLabel]
	if cluster == "" {
		return ""
	}
	switch pod.Labels[owned.ComponentLabel] {
	case "postgresql":
		if _, ok := owned.ParseIdentityLabel(pod.Labels[owned.ShardLabel]); !ok {
			return ""
		}
		if _, ok := owned.ParseIdentityLabel(pod.Labels[owned.MemberLabel]); !ok {
			return ""
		}
		return "member"
	case "pooler", "orchestrator":
		return "supporting"
	}
	return ""
}

func isProtectedInventoryPod(pod *corev1.Pod) bool {
	return inventoryClass(pod) != ""
}

func isolationReceiptPhase(receipt *pgshardv1alpha1.PostgreSQLIsolationReceipt) pgshardv1alpha1.IsolationPhase {
	if receipt == nil || receipt.Phase == "" {
		return pgshardv1alpha1.IsolationInactive
	}
	return receipt.Phase
}

// isolationOptedIn reports whether a cluster has explicitly requested activation.
// Activation is OPT-IN and default OFF: the cluster must carry the activation
// annotation, be in a fenced namespace, and neither be terminating. Without the
// annotation a cluster never activates, so existing clusters and the KIND smoke
// are unaffected even though the preflight is now real.
func isolationOptedIn(cluster *pgshardv1alpha1.PgShardCluster, namespace *corev1.Namespace) bool {
	return cluster.DeletionTimestamp == nil &&
		cluster.Annotations[pgshardv1alpha1.IsolationActivationAnnotation] == pgshardv1alpha1.IsolationActivationRequested &&
		namespace.DeletionTimestamp == nil &&
		namespace.Labels[podfence.NamespaceLabel] == podfence.NamespaceLabelValue
}

// hasReplicationTLSPrerequisite reports whether a cluster's durable replication
// transport is hardened enough to activate: the recorded transport policy is
// server-tls-v1 and every shard has a complete replication-TLS checkpoint (CA
// digest plus a server digest for every member). Single-member clusters gate off
// until the ratified activation-TLS parity path lands.
func hasReplicationTLSPrerequisite(cluster *pgshardv1alpha1.PgShardCluster) bool {
	if cluster.Spec.MembersPerShard <= 1 {
		return false
	}
	spec := cluster.Status.PostgreSQLBootstrapSpec
	if spec == nil || spec.ReplicationTransportPolicy != pgshardv1alpha1.ReplicationTransportPolicyServerTLSV1 {
		return false
	}
	if len(cluster.Status.PostgreSQLReplicationTLS) == 0 {
		return false
	}
	for _, shard := range cluster.Status.PostgreSQLReplicationTLS {
		if shard.CASHA256 == "" || len(shard.Members) == 0 {
			return false
		}
		for _, member := range shard.Members {
			if member.ServerSHA256 == "" {
				return false
			}
		}
	}
	return true
}

// limitRangePresent returns the name of any LimitRange in the namespace, or "".
func (r *PgShardClusterReconciler) limitRangePresent(ctx context.Context, namespace string) (string, error) {
	list := &corev1.LimitRangeList{}
	if err := r.authoritativeReader().List(ctx, list, client.InNamespace(namespace)); err != nil {
		return "", fmt.Errorf("list LimitRanges for activation: %w", err)
	}
	if len(list.Items) > 0 {
		return list.Items[0].Name, nil
	}
	return "", nil
}

// supportingRollInProgress reports whether any supporting class still has a
// populated prior generation (a roll that has not converged).
func supportingRollInProgress(cluster *pgshardv1alpha1.PgShardCluster) bool {
	for _, record := range cluster.Status.SupportingGenerations {
		if record.PriorReplicaSetUID != "" {
			return true
		}
	}
	return false
}

func (r *PgShardClusterReconciler) exactlyOneClusterInNamespace(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (bool, error) {
	list := &pgshardv1alpha1.PgShardClusterList{}
	if err := r.authoritativeReader().List(ctx, list, client.InNamespace(cluster.Namespace)); err != nil {
		return false, fmt.Errorf("list PgShardClusters in namespace for activation: %w", err)
	}
	live := 0
	for i := range list.Items {
		if list.Items[i].DeletionTimestamp == nil {
			live++
		}
	}
	return live == 1, nil
}

func currentSecurityGeneration(cluster *pgshardv1alpha1.PgShardCluster) int64 {
	var generation int64 = 1
	for _, contract := range cluster.Status.SupportingContracts {
		if contract.SecurityGeneration > generation {
			generation = contract.SecurityGeneration
		}
	}
	for _, contract := range cluster.Status.PostgreSQLMemberContracts {
		if contract.SecurityGeneration > generation {
			generation = contract.SecurityGeneration
		}
	}
	return generation
}
