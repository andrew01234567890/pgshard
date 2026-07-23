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
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
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

	// Reconcile the isolation-ENFORCING namespace LABEL to match the receipt phase:
	// present for ANY non-INACTIVE phase (QUIESCE/RECREATE/ACTIVE), absent for
	// INACTIVE / no receipt. Every genuinely-new isolation webhook's
	// namespaceSelector requires it, so an un-activated fenced namespace invokes
	// NONE of them — ordinary applies/creates and a manager restart behave exactly
	// as pre-isolation. This is idempotent, so it self-heals after a manager
	// restart. The INACTIVE→QUIESCE transition sets the label BEFORE the QUIESCE
	// write (below) so enforcement is effective the instant activation begins.
	if err := r.reconcileIsolationEnforcingLabel(ctx, namespace, isolationReceiptPhase(receipt) != pgshardv1alpha1.IsolationInactive); err != nil {
		return false, err
	}

	switch isolationReceiptPhase(receipt) {
	case pgshardv1alpha1.IsolationInactive:
		// TODO(isolation-rollout): step 8's distinct-v2-Service upgrade rollout +
		// bridge choreography (staging activation across a bad8a18→new in-place
		// UPGRADE, not a fresh deploy) is deferred and would be sequenced here at the
		// INACTIVE→QUIESCE entry — an upgrade would gate/stage the activation start.
		// Fresh CI deploys are unaffected (they activate directly on opt-in).
		//
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
		// The drain ceremonies rest on the installation-attested maximum
		// whole-request lifetime; without the attestation the bound is an assumption
		// and activation is withheld.
		if r.AttestedRequestTimeout <= 0 {
			r.setIsolationPreflightCondition(cluster, isolationDrainUnattestedCondition, "Unattested", "isolation activation requires the installation-attested maximum API request lifetime (--attested-max-request-timeout); the whole-request drain bound cannot be assumed")
			return false, r.Status().Update(ctx, cluster)
		}
		proof, ok := r.preflightConverged(ctx, cluster)
		if !ok {
			// The preflight surfaced its own typed condition; withhold activation.
			return false, r.Status().Update(ctx, cluster)
		}
		// Namespace exclusivity is claimed race-free through an atomic Lease CREATE
		// before the receipt write: two clusters racing the preflight LIST cannot
		// both hold it.
		if held, err := r.acquireIsolationExclusivityLease(ctx, cluster); err != nil {
			return false, err
		} else if !held {
			r.setIsolationPreflightCondition(cluster, isolationMultipleClustersCondition, "ExclusivityHeld", "the namespace isolation-activation lease is held by another PgShardCluster")
			return false, r.Status().Update(ctx, cluster)
		}
		// Set the enforcing label BEFORE writing the QUIESCE receipt, so the
		// isolation webhooks (WorkloadIntegrity deny-all, etc.) fire the instant
		// activation begins — no window where QUIESCE is written but not enforced.
		if err := r.reconcileIsolationEnforcingLabel(ctx, namespace, true); err != nil {
			return false, err
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
	// Protected templates are frozen during the ceremony, so a supporting roll
	// here means external interference with the admissible set: hold the durable
	// deny phase rather than advancing.
	if supportingRollInProgress(cluster) {
		r.setIsolationPreflightCondition(cluster, isolationSupportingRollingCondition, "SupportingRolling", "a supporting-generation roll began mid-activation; the namespace is held quiesced until it converges")
		return true, r.Status().Update(ctx, cluster)
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
	if drifted, err := r.resealOnSealedParentDrift(ctx, cluster); err != nil || drifted {
		return true, err
	}
	// A full-second safety margin covers the one-second truncation of the
	// metav1.Time drain start plus modest clock skew, so the drain never completes
	// early and lets a pre-quiesce in-flight create persist.
	if time.Since(receipt.ActivatedAt.Time) < r.revocationDrainWindow()+time.Second {
		return true, nil
	}
	// Seal EVERY live pod UID in the namespace — foreign/unclassified/pre-guard as
	// well as protected. QUIESCE does NOT block on old pods that mismatch the
	// (possibly newly-sealed) parent: RECREATE is the only phase that deletes pods,
	// so blocking here would deadlock (an OnDelete member never re-matches without
	// being deleted first). Instead RECREATE UID-deletes every sealed pod, driving
	// cleanup: protected pods are recreated under the guard; foreign pods stay gone.
	pending, err := r.allPodUIDs(ctx, cluster)
	if err != nil {
		return false, err
	}
	receipt.RecreatePendingUIDs = pending
	// Per-class/member floors are enforced from the FIRST guarded create: RECREATE
	// applies each pod's own class/member floor and the per-class CAS barrier, not
	// only ACTIVE, and never a namespace-wide maximum.
	receipt.SecurityFloors = currentSecurityFloors(cluster)
	receipt.MinAcceptableSecurityGeneration = 0
	receipt.Phase = pgshardv1alpha1.IsolationActivatingRecreate
	receipt.ActivatedAt = metav1.Now()
	meta.RemoveStatusCondition(&cluster.Status.Conditions, isolationActivationBlockedCondition)
	if err := r.Status().Update(ctx, cluster); err != nil {
		return false, fmt.Errorf("advance to isolation recreate: %w", err)
	}
	return true, nil
}

// resealOnSealedParentDrift compares every sealed parent to its live object. On
// ANY drift — a parent deleted, recreated under a new UID, or its spec
// generation/contract hash moved — it returns the ceremony to the start of
// QUIESCE with the sealed state cleared, so the parents are re-sealed at their
// new incarnation instead of deadlocking every replacement against a stale seal.
func (r *PgShardClusterReconciler) resealOnSealedParentDrift(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (bool, error) {
	receipt := cluster.Status.IsolationReceipt
	reader := r.authoritativeReader()
	drifted := ""
	for i := range receipt.SealedParents {
		sealed := &receipt.SealedParents[i]
		var liveUID string
		var liveGeneration int64
		var liveHash string
		switch sealed.Kind {
		case "StatefulSet":
			statefulSet := &appsv1.StatefulSet{}
			if err := reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: sealed.Name}, statefulSet); err != nil {
				if !apierrors.IsNotFound(err) {
					return false, fmt.Errorf("read sealed StatefulSet %s for drift detection: %w", sealed.Name, err)
				}
			} else {
				liveUID, liveGeneration, liveHash = string(statefulSet.UID), statefulSet.Generation, statefulSet.Spec.Template.Annotations[owned.PodContractHashAnnotation]
			}
		case "Deployment":
			deployment := &appsv1.Deployment{}
			if err := reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: sealed.Name}, deployment); err != nil {
				if !apierrors.IsNotFound(err) {
					return false, fmt.Errorf("read sealed Deployment %s for drift detection: %w", sealed.Name, err)
				}
			} else {
				liveUID, liveGeneration, liveHash = string(deployment.UID), deployment.Generation, deployment.Spec.Template.Annotations[owned.PodContractHashAnnotation]
			}
		}
		if liveUID != sealed.UID || liveGeneration != sealed.Generation || liveHash != sealed.ContractHash {
			drifted = sealed.Kind + "/" + sealed.Name
			break
		}
	}
	if drifted == "" {
		return false, nil
	}
	receipt.Phase = pgshardv1alpha1.IsolationActivatingQuiesce
	receipt.SealedParents = nil
	receipt.RecreatePendingUIDs = nil
	receipt.ActivatedAt = metav1.Now()
	r.setIsolationPreflightCondition(cluster, isolationSealedParentDriftCondition, "SealedParentDrift", fmt.Sprintf("sealed parent %s drifted from its sealed incarnation during activation; the ceremony re-quiesced to reseal", drifted))
	if err := r.Status().Update(ctx, cluster); err != nil {
		return false, fmt.Errorf("reseal after sealed-parent drift: %w", err)
	}
	return true, nil
}

// driveIsolationRecreate deletes every sealed pre-guard protected pod (by UID,
// including terminating ones) so the controllers recreate each under the guard.
// It advances to ACTIVE only once: no sealed UID remains, NO pod in the
// namespace is still terminating (authoritative API absence), every sealed
// parent's guarded replacement set exists at its sealed cardinality and every
// pod passes the full shared live-contract validation, and a FINAL dispatch
// re-proof succeeds immediately before the ACTIVE status write. It never infers
// authentication from CreationTimestamp.
func (r *PgShardClusterReconciler) driveIsolationRecreate(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (bool, error) {
	if valid, err := r.revalidateDispatchTuple(ctx, cluster); err != nil || !valid {
		return true, err
	}
	if supportingRollInProgress(cluster) {
		r.setIsolationPreflightCondition(cluster, isolationSupportingRollingCondition, "SupportingRolling", "a supporting-generation roll began mid-activation; ACTIVE is withheld until it converges")
		return true, r.Status().Update(ctx, cluster)
	}
	if drifted, err := r.resealOnSealedParentDrift(ctx, cluster); err != nil || drifted {
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
	terminating := 0
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil {
			terminating++
		}
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
	if remaining > 0 || terminating > 0 {
		// A pre-guard pod is still being deleted, or SOME pod has not reached
		// authoritative API absence; the transition blocks on every terminating pod.
		return true, nil
	}
	blocked, waiting, residue, err := r.inventoryNamespace(ctx, cluster, true)
	if err != nil {
		return false, err
	}
	if blocked != "" {
		return r.blockIsolation(ctx, cluster, blocked)
	}
	if waiting != "" {
		// A replacement exists but is not yet fully validated (e.g. not yet bound);
		// wait, do not activate.
		return true, nil
	}
	if complete, err := r.guardedReplacementsComplete(ctx, cluster); err != nil {
		return false, err
	} else if !complete {
		// The controllers have not yet recreated every guarded replacement the
		// sealed parents require; an EMPTY namespace must never activate.
		return true, nil
	}
	// FINAL dispatch re-proof immediately before the ACTIVE CAS, closing the
	// interval between the last revalidation and the status write; subsequent
	// tuple changes re-quiesce via the EndpointSlice/webhook-config watches and
	// the ACTIVE-phase revalidation.
	if valid, err := r.revalidateDispatchTuple(ctx, cluster); err != nil || !valid {
		return true, err
	}
	// The isolation-enforcing namespace label is already present — it was set at the
	// INACTIVE→QUIESCE transition and kept by the idempotent top-of-reconcile for
	// every non-INACTIVE phase, so the ACTIVE write needs no separate label step.
	receipt.ResidueProfileHash = residue
	receipt.RecreatePendingUIDs = nil
	receipt.Phase = pgshardv1alpha1.IsolationActive
	receipt.SecurityFloors = currentSecurityFloors(cluster)
	receipt.MinAcceptableSecurityGeneration = 0
	receipt.ActivatedAt = metav1.Now()
	meta.RemoveStatusCondition(&cluster.Status.Conditions, isolationActivationBlockedCondition)
	if err := r.Status().Update(ctx, cluster); err != nil {
		return false, fmt.Errorf("activate isolation: %w", err)
	}
	return false, nil
}

// guardedReplacementsComplete proves the guarded replacement set: every sealed
// parent must own exactly its sealed cardinality of live, non-terminating,
// fully validated pods (validated by inventoryNamespace, which already resolved
// each pod's sealed parent). It re-lists and re-validates so the count is bound
// to the same authoritative view.
func (r *PgShardClusterReconciler) guardedReplacementsComplete(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (bool, error) {
	receipt := cluster.Status.IsolationReceipt
	// An EMPTY sealed-parent set can never activate: a real cluster always has
	// protected parents, so an empty seal means the ceremony state is incoherent
	// (and an empty namespace would otherwise activate vacuously).
	if len(receipt.SealedParents) == 0 {
		return false, nil
	}
	reader := r.authoritativeReader()
	pods := &corev1.PodList{}
	if err := reader.List(ctx, pods, client.InNamespace(cluster.Namespace)); err != nil {
		return false, fmt.Errorf("list pods for guarded replacement proof: %w", err)
	}
	counts := map[string]int32{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil {
			return false, nil
		}
		verdict, err := podfence.ValidateLiveProtectedPod(ctx, reader, pod, receipt, true)
		if err != nil {
			return false, err
		}
		if verdict.Reason != "" || verdict.SealedParent == nil {
			return false, nil
		}
		counts[verdict.SealedParent.Kind+"/"+verdict.SealedParent.UID]++
	}
	for i := range receipt.SealedParents {
		sealed := &receipt.SealedParents[i]
		if counts[sealed.Kind+"/"+sealed.UID] != sealed.Replicas {
			return false, nil
		}
	}
	return true, nil
}

// allPodUIDs returns the UIDs of EVERY live pod in the namespace — protected and
// foreign alike — so RECREATE deletes them all: protected pods are recreated
// under the guard, and foreign/unclassified/pre-guard pods (which have no sealed
// parent to recreate them) are cleaned up rather than left to block activation.
func (r *PgShardClusterReconciler) allPodUIDs(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) ([]string, error) {
	pods := &corev1.PodList{}
	if err := r.authoritativeReader().List(ctx, pods, client.InNamespace(cluster.Namespace)); err != nil {
		return nil, fmt.Errorf("list pods to seal for recreate: %w", err)
	}
	uids := make([]string, 0, len(pods.Items))
	for i := range pods.Items {
		uids = append(uids, string(pods.Items[i].UID))
	}
	sort.Strings(uids)
	return uids, nil
}

// currentSecurityFloors derives the PER-class/member security-generation floors
// from the recorded contract stamps: each supporting class and each member gets
// its own floor at its own recorded generation.
func currentSecurityFloors(cluster *pgshardv1alpha1.PgShardCluster) []pgshardv1alpha1.IsolationSecurityFloor {
	floors := make([]pgshardv1alpha1.IsolationSecurityFloor, 0, len(cluster.Status.SupportingContracts)+len(cluster.Status.PostgreSQLMemberContracts))
	for _, contract := range cluster.Status.SupportingContracts {
		floors = append(floors, pgshardv1alpha1.IsolationSecurityFloor{Component: contract.Class, MinGeneration: contract.SecurityGeneration})
	}
	for _, contract := range cluster.Status.PostgreSQLMemberContracts {
		floors = append(floors, pgshardv1alpha1.IsolationSecurityFloor{Component: "postgresql", Shard: contract.Shard, Member: contract.Member, MinGeneration: contract.SecurityGeneration})
	}
	sort.Slice(floors, func(a, b int) bool {
		if floors[a].Component != floors[b].Component {
			return floors[a].Component < floors[b].Component
		}
		if floors[a].Shard != floors[b].Shard {
			return floors[a].Shard < floors[b].Shard
		}
		return floors[a].Member < floors[b].Member
	})
	return floors
}

// podInventorySecurityFloor resolves the sealed per-class/member floor for a pod
// from its labels.
func podInventorySecurityFloor(receipt *pgshardv1alpha1.PostgreSQLIsolationReceipt, pod *corev1.Pod) int64 {
	component := pod.Labels[owned.ComponentLabel]
	shard, _ := owned.ParseIdentityLabel(pod.Labels[owned.ShardLabel])
	member, _ := owned.ParseIdentityLabel(pod.Labels[owned.MemberLabel])
	return receipt.SecurityFloorFor(component, shard, member)
}

// driveIsolationActive re-validates dispatch convergence (ACTIVE is not exempt:
// a backend-set or webhook-config change re-quiesces via revalidateDispatchTuple,
// and the EndpointSlice/webhook-config watches wake this reconcile on every such
// event) and re-inventories under enforcement. On drift — a pod that no longer
// validates the full live contract — it does not merely raise a condition; it
// returns the receipt to ACTIVATING_QUIESCE so the parents are re-sealed, the
// namespace re-drained, and every protected pod re-recreated under the guard.
func (r *PgShardClusterReconciler) driveIsolationActive(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (bool, error) {
	if valid, err := r.revalidateDispatchTuple(ctx, cluster); err != nil || !valid {
		return true, err
	}
	// A supporting-generation roll (bounded coexistence: the old generation drains
	// while the new one rolls out) transiently leaves old pods that do not match
	// the current parent. Do NOT re-quiesce on that transient — re-quiescing would
	// freeze the very creates the CAS roll needs to converge (a circular wait).
	// WAIT for the roll to converge, THEN re-inventory. The roll is counted active
	// from sealed intent through CurrentTemplateGeneration==ConvergedGeneration.
	if supportingRollInProgress(cluster) {
		return true, nil
	}
	blocked, _, _, err := r.inventoryNamespace(ctx, cluster, false)
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
		replicas := int32(1)
		if set.Spec.Replicas != nil {
			replicas = *set.Spec.Replicas
		}
		sealed = append(sealed, pgshardv1alpha1.SealedParent{
			Kind: "StatefulSet", Name: set.Name, UID: string(set.UID), ResourceVersion: set.ResourceVersion,
			Generation: set.Generation, Replicas: replicas,
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
		replicas := int32(1)
		if deployment.Spec.Replicas != nil {
			replicas = *deployment.Spec.Replicas
		}
		sealed = append(sealed, pgshardv1alpha1.SealedParent{
			Kind: "Deployment", Name: deployment.Name, UID: string(deployment.UID), ResourceVersion: deployment.ResourceVersion,
			Generation: deployment.Generation, Replicas: replicas,
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

// inventoryNamespace enumerates every pod in the namespace and runs the SHARED
// full live-contract validation on each — the same live-parent, provenance,
// LiveNormalForm-comparator, hash-recomputation, digest-pin, and generation
// checks admission applies, via podfence.ValidateLiveProtectedPod — never a
// coarser label/stamp re-implementation. It returns (blocked, waiting, residue):
// blocked names a pod that permanently fails validation; waiting names a pod
// that is transiently incomplete (terminating, or not yet bound) — under strict
// mode (the RECREATE→ACTIVE transition) such pods hold the transition, while
// steady mode (QUIESCE pre-drain and ACTIVE re-inventory) skips them because
// terminating pre-guard pods are about to be deleted and a freshly created
// guarded pod was already fully validated at admission. Under strict mode the
// pod's sealed parent is also required.
func (r *PgShardClusterReconciler) inventoryNamespace(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, strict bool) (string, string, string, error) {
	reader := r.authoritativeReader()
	pods := &corev1.PodList{}
	if err := reader.List(ctx, pods, client.InNamespace(cluster.Namespace)); err != nil {
		return "", "", "", fmt.Errorf("inventory namespace pods: %w", err)
	}
	receipt := cluster.Status.IsolationReceipt
	fingerprints := make([]string, 0, len(pods.Items))
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil {
			if strict {
				return "", pod.Name, "", nil
			}
			continue
		}
		kind := inventoryClass(pod)
		if kind == "" {
			return pod.Name, "", "", nil
		}
		hash := pod.Annotations[owned.PodContractHashAnnotation]
		if hash == "" {
			return pod.Name, "", "", nil
		}
		if pod.Spec.NodeName == "" {
			// Created but not yet bound: full live validation is impossible. Strict
			// mode waits for the binding; steady mode trusts the guarded admission
			// that just validated the create.
			if strict {
				return "", pod.Name, "", nil
			}
			continue
		}
		generation, err := strconv.ParseInt(pod.Annotations[owned.PodSecurityGenerationAnnotation], 10, 64)
		// Each pod is compared ONLY with its own class/member floor — never a
		// namespace-wide maximum.
		if err != nil || generation < podInventorySecurityFloor(receipt, pod) {
			return pod.Name, "", "", nil
		}
		verdict, err := podfence.ValidateLiveProtectedPod(ctx, reader, pod, cluster.Status.IsolationReceipt, strict)
		if err != nil {
			return "", "", "", err
		}
		if verdict.Reason != "" {
			return pod.Name, "", "", nil
		}
		fingerprints = append(fingerprints, kind+":"+hash+":"+strconv.FormatInt(generation, 10))
	}
	sort.Strings(fingerprints)
	sum := sha256.Sum256([]byte(strings.Join(fingerprints, "\n")))
	return "", "", hex.EncodeToString(sum[:]), nil
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
// server-tls-v1 and the TLS checkpoint set covers the EXACT spec topology — one
// checkpoint per shard 0..spec.shards-1, each with a CA digest and a server
// digest for every member 0..membersPerShard-1, no shard or member missing,
// duplicated, or out of range. Any nonempty-but-incomplete coverage is
// insufficient. Single-member clusters gate off until the ratified
// activation-TLS parity path lands.
func hasReplicationTLSPrerequisite(cluster *pgshardv1alpha1.PgShardCluster) bool {
	if cluster.Spec.MembersPerShard <= 1 {
		return false
	}
	spec := cluster.Status.PostgreSQLBootstrapSpec
	if spec == nil || spec.ReplicationTransportPolicy != pgshardv1alpha1.ReplicationTransportPolicyServerTLSV1 {
		return false
	}
	shards := cluster.Spec.Shards
	if shards < 1 {
		shards = 1
	}
	if len(cluster.Status.PostgreSQLReplicationTLS) != int(shards) {
		return false
	}
	seenShards := map[int32]bool{}
	for _, shard := range cluster.Status.PostgreSQLReplicationTLS {
		if shard.Shard < 0 || shard.Shard >= shards || seenShards[shard.Shard] {
			return false
		}
		seenShards[shard.Shard] = true
		if shard.CASHA256 == "" {
			return false
		}
		if len(shard.Members) != int(cluster.Spec.MembersPerShard) {
			return false
		}
		seenMembers := map[int32]bool{}
		for _, member := range shard.Members {
			if member.Member < 0 || member.Member >= cluster.Spec.MembersPerShard || seenMembers[member.Member] {
				return false
			}
			seenMembers[member.Member] = true
			if member.ServerSHA256 == "" {
				return false
			}
		}
	}
	return true
}

// reconcileIsolationEnforcingLabel ensures the fenced namespace carries the
// isolation-enforcing label iff its isolation is in any non-INACTIVE phase
// (QUIESCE/RECREATE/ACTIVE). Setting/removing a non-fencing label is permitted by
// the namespace webhook (which only pins the fencing label), so the operator
// authors this. It is a no-op when the label already matches, so it never churns
// the namespace.
func (r *PgShardClusterReconciler) reconcileIsolationEnforcingLabel(ctx context.Context, namespace *corev1.Namespace, enforcing bool) error {
	has := namespace.Labels[podfence.NamespaceEnforcingLabel] == podfence.NamespaceEnforcingLabelValue
	if has == enforcing {
		return nil
	}
	updated := namespace.DeepCopy()
	if updated.Labels == nil {
		updated.Labels = map[string]string{}
	}
	if enforcing {
		updated.Labels[podfence.NamespaceEnforcingLabel] = podfence.NamespaceEnforcingLabelValue
	} else {
		delete(updated.Labels, podfence.NamespaceEnforcingLabel)
	}
	if err := r.Patch(ctx, updated, client.MergeFrom(namespace)); err != nil {
		return fmt.Errorf("reconcile isolation-enforcing namespace label: %w", err)
	}
	*namespace = *updated
	return nil
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

// supportingRollInProgress reports whether any supporting class is mid-roll. A
// roll counts as in progress from SEALED INTENT (the revocation barrier or the
// current template generation has advanced past the converged generation,
// including the intent→new-ReplicaSet gap before the prior UID is populated)
// until it fully converges (CurrentTemplateGeneration == ConvergedGeneration and
// no prior remains). Activation waits for the whole window so it never enters or
// completes the ceremony — or re-quiesces mid-ACTIVE — while the admissible set
// is in flux.
func supportingRollInProgress(cluster *pgshardv1alpha1.PgShardCluster) bool {
	for _, record := range cluster.Status.SupportingGenerations {
		if record.PriorReplicaSetUID != "" {
			return true
		}
		if record.CurrentTemplateGeneration != record.ConvergedGeneration {
			return true
		}
		if record.MinGenerationForNewCreates > record.ConvergedGeneration {
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

// isolationExclusivityLeaseName is the namespace-scoped activation lock. Its
// CREATE is atomic, so two clusters racing the preflight LIST cannot both claim
// the namespace; the loser withholds with a typed condition. The Lease is
// owner-referenced to its holder so a deleted cluster releases the claim via
// garbage collection.
const isolationExclusivityLeaseName = "pgshard-isolation-activation"

func (r *PgShardClusterReconciler) acquireIsolationExclusivityLease(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (bool, error) {
	holder := string(cluster.UID)
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      isolationExclusivityLeaseName,
			Namespace: cluster.Namespace,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: pgshardv1alpha1.GroupVersion.String(),
				Kind:       "PgShardCluster",
				Name:       cluster.Name,
				UID:        cluster.UID,
				Controller: ptr.To(true),
			}},
		},
		Spec: coordinationv1.LeaseSpec{HolderIdentity: &holder},
	}
	if err := r.Create(ctx, lease); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return false, fmt.Errorf("create isolation exclusivity lease: %w", err)
		}
		existing := &coordinationv1.Lease{}
		if err := r.authoritativeReader().Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: isolationExclusivityLeaseName}, existing); err != nil {
			return false, fmt.Errorf("read isolation exclusivity lease: %w", err)
		}
		if existing.Spec.HolderIdentity == nil || *existing.Spec.HolderIdentity != holder {
			return false, nil
		}
	}
	return true, nil
}

// holdPlanTemplatesDuringActivation freezes every protected workload's pod
// template at its live value while the activation ceremony (QUIESCE/RECREATE) is
// in progress: the plan's member StatefulSets and supporting Deployments are
// pinned to what is running, so no supporting roll can begin and no sealed
// parent can drift mid-ceremony through the operator's own apply. Template
// changes resume (and bump the security generation) after ACTIVE or when no
// ceremony is running.
//
// SCOPE — Secret-content rotation is DEFERRED, not frozen, during the ceremony.
// This holds the pod TEMPLATE (the stamped contract surface); it does not freeze
// the bytes of a mounted Secret, so a certificate/Secret rotation performed
// mid-ceremony would change a running pod's mounted content without a template
// change. That is acceptable and intentional: activation is a bounded maintenance
// event (one drain window), certificate rotation is an operator-initiated action
// that can wait for it to complete, and any template change is already caught by
// the drift-reseal. Freezing arbitrary Secret content namespace-wide during the
// window would be far more invasive than the risk warrants.
func (r *PgShardClusterReconciler) holdPlanTemplatesDuringActivation(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, plan []client.Object) error {
	receipt := cluster.Status.IsolationReceipt
	phase := isolationReceiptPhase(receipt)
	if phase != pgshardv1alpha1.IsolationActivatingQuiesce && phase != pgshardv1alpha1.IsolationActivatingRecreate {
		return nil
	}
	reader := r.authoritativeReader()
	for _, object := range plan {
		switch workload := object.(type) {
		case *appsv1.StatefulSet:
			live := &appsv1.StatefulSet{}
			if err := reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: workload.Name}, live); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				return fmt.Errorf("read live StatefulSet %s for activation template hold: %w", workload.Name, err)
			}
			if live.Labels[owned.ComponentLabel] == "postgresql" {
				workload.Spec.Template = *live.Spec.Template.DeepCopy()
			}
		case *appsv1.Deployment:
			live := &appsv1.Deployment{}
			if err := reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: workload.Name}, live); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				return fmt.Errorf("read live Deployment %s for activation template hold: %w", workload.Name, err)
			}
			component := live.Labels[owned.ComponentLabel]
			if component == "pooler" || component == "orchestrator" {
				workload.Spec.Template = *live.Spec.Template.DeepCopy()
			}
		}
	}
	return nil
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
