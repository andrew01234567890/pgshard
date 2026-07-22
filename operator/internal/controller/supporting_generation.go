package controller

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// supportingRevocationDrain is the pinned maximum in-flight admission window (the
// API server's --request-timeout). A prior supporting generation may not be
// declared converged until at least this long after its revocation was sealed,
// so any request that was already validating against the old generation has
// drained.
const supportingRevocationDrain = time.Minute

type supportingClass struct {
	class  owned.PodClass
	suffix string
}

func supportingClasses() []supportingClass {
	return []supportingClass{
		{owned.ClassPooler, owned.PoolerSuffix},
		{owned.ClassOrchestrator, owned.OrchestratorSuffix},
	}
}

// supportingDrainObservations counts consecutive authoritative zero-pod LISTs of
// a draining prior generation, keyed by cluster UID and class. It is in-memory:
// on manager restart the count resets to zero, which only makes convergence more
// conservative (two fresh zero LISTs are required again).
var supportingDrainObservations sync.Map

// sealSupportingGenerationIntents runs BEFORE the plan is applied. For each
// supporting class it (a) resets the record if the owning Deployment was
// recreated, (b) serializes rolls — if a prior generation is still draining it
// holds the plan Deployment at its live template so a newer template change is
// deferred until convergence, and (c) advances the revocation barrier
// (MinGenerationForNewCreates) on a security-generation bump, persisting it
// authoritatively before the Deployment mutation so a downgrade create is denied
// the instant the barrier lands. It returns whether a roll is being held.
func (r *PgShardClusterReconciler) sealSupportingGenerationIntents(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, plan []client.Object) (bool, error) {
	reader := r.authoritativeReader()
	changed := false
	holding := false
	for _, sc := range supportingClasses() {
		deploymentName := cluster.Name + sc.suffix
		planDeployment := findPlanDeployment(plan, deploymentName)
		if planDeployment == nil {
			continue
		}
		desiredHash := planDeployment.Spec.Template.Annotations[owned.PodContractHashAnnotation]
		desiredGeneration := parseSecurityGeneration(planDeployment.Spec.Template.Annotations[owned.PodSecurityGenerationAnnotation])

		live := &appsv1.Deployment{}
		liveExists := true
		if err := reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: deploymentName}, live); err != nil {
			if !apierrors.IsNotFound(err) {
				return false, fmt.Errorf("read supporting Deployment %s for generation seal: %w", deploymentName, err)
			}
			liveExists = false
		}

		record := &cluster.Status.SupportingGenerations[supportingGenerationIndex(cluster, string(sc.class))]
		if liveExists {
			if record.DeploymentUID == "" {
				record.DeploymentUID = string(live.UID)
				changed = true
			} else if record.DeploymentUID != string(live.UID) {
				*record = pgshardv1alpha1.SupportingGenerationStatus{Class: string(sc.class), DeploymentUID: string(live.UID)}
				changed = true
			}
		}

		if record.PriorReplicaSetUID != "" && desiredHash != record.CurrentContractHash && liveExists {
			// A prior generation is still draining; hold the Deployment at its
			// live template so this newer change (C) waits for B to converge.
			planDeployment.Spec.Template = *live.Spec.Template.DeepCopy()
			holding = true
			continue
		}

		if desiredHash != record.CurrentContractHash && desiredGeneration > record.MinGenerationForNewCreates {
			record.MinGenerationForNewCreates = desiredGeneration
			record.SealedAt = metav1.Now()
			changed = true
		}
	}
	if changed {
		if err := r.Status().Update(ctx, cluster); err != nil {
			return false, fmt.Errorf("seal supporting generation intents: %w", err)
		}
	}
	return holding, nil
}

// advanceSupportingGenerations runs AFTER the plan is applied. It observes the
// live Deployments and their ReplicaSets through the authoritative reader,
// recomputes {current, prior} deterministically from live ReplicaSet UIDs and
// stamped hashes (never from ready counts), binds a newly created ReplicaSet as
// current, and drives a populated prior generation through revocation and the
// convergence proof. It returns whether a roll is still in progress.
func (r *PgShardClusterReconciler) advanceSupportingGenerations(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (bool, error) {
	reader := r.authoritativeReader()
	changed := false
	rolling := false
	for _, sc := range supportingClasses() {
		deploymentName := cluster.Name + sc.suffix
		live := &appsv1.Deployment{}
		if err := reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: deploymentName}, live); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return false, fmt.Errorf("read supporting Deployment %s for generation advance: %w", deploymentName, err)
		}
		record := &cluster.Status.SupportingGenerations[supportingGenerationIndex(cluster, string(sc.class))]
		if record.DeploymentUID == "" {
			record.DeploymentUID = string(live.UID)
			changed = true
		} else if record.DeploymentUID != string(live.UID) {
			*record = pgshardv1alpha1.SupportingGenerationStatus{Class: string(sc.class), DeploymentUID: string(live.UID)}
			changed = true
		}

		replicaSets, err := r.listOwnedReplicaSets(ctx, reader, cluster.Namespace, live.UID)
		if err != nil {
			return false, err
		}
		desiredHash := live.Spec.Template.Annotations[owned.PodContractHashAnnotation]
		desiredGeneration := parseSecurityGeneration(live.Spec.Template.Annotations[owned.PodSecurityGenerationAnnotation])
		currentReplicaSet := replicaSetByTemplateHash(replicaSets, desiredHash)
		if currentReplicaSet == nil {
			// The Deployment controller has not created the ReplicaSet for the
			// applied template yet; the Deployment's own status change will
			// re-trigger reconciliation to bind it. Do not force a requeue here —
			// a brand-new workload is not a roll in progress.
			continue
		}

		if record.CurrentReplicaSetUID != string(currentReplicaSet.UID) {
			if record.CurrentReplicaSetUID != "" && record.CurrentContractHash != desiredHash &&
				replicaSetByUID(replicaSets, record.CurrentReplicaSetUID) != nil {
				record.PriorReplicaSetUID = record.CurrentReplicaSetUID
				record.PriorContractHash = record.CurrentContractHash
				// A freshly demoted prior is admissible during its rollout until
				// the reconciler durably revokes it.
				record.PriorRevoked = false
			}
			record.CurrentReplicaSetUID = string(currentReplicaSet.UID)
			record.CurrentContractHash = desiredHash
			record.CurrentTemplateGeneration = desiredGeneration
			record.SealedAt = metav1.Now()
			changed = true
		}

		if record.PriorReplicaSetUID != "" {
			converged, mutated, err := r.driveSupportingRevocation(ctx, reader, cluster, live, replicaSets, record)
			if err != nil {
				return false, err
			}
			changed = changed || mutated
			if converged {
				record.PriorReplicaSetUID = ""
				record.PriorContractHash = ""
				record.PriorRevoked = false
				record.ConvergedGeneration = record.CurrentTemplateGeneration
				record.SealedAt = metav1.Now()
				changed = true
			} else {
				rolling = true
			}
		} else if record.ConvergedGeneration < record.CurrentTemplateGeneration {
			record.ConvergedGeneration = record.CurrentTemplateGeneration
			changed = true
		}
	}
	if changed {
		if err := r.Status().Update(ctx, cluster); err != nil {
			return false, fmt.Errorf("advance supporting generations: %w", err)
		}
	}
	return rolling, nil
}

// driveSupportingRevocation enforces the ordered revocation of a prior
// generation: scale its ReplicaSet to zero (sealing the revocation observation),
// prove the Deployment fully rolled forward and the prior ReplicaSet drained,
// wait the pinned request-timeout drain, then delete any late-write pod still
// owned by the prior ReplicaSet until two consecutive authoritative zero LISTs.
func (r *PgShardClusterReconciler) driveSupportingRevocation(ctx context.Context, reader client.Reader, cluster *pgshardv1alpha1.PgShardCluster, deployment *appsv1.Deployment, replicaSets []appsv1.ReplicaSet, record *pgshardv1alpha1.SupportingGenerationStatus) (bool, bool, error) {
	// Revoke the prior generation from the admissible set FIRST and persist it,
	// before scaling down or draining, so admission stops accepting new
	// prior-generation pods before any zero-pod proof.
	if !record.PriorRevoked {
		record.PriorRevoked = true
		record.SealedAt = metav1.Now()
		r.resetSupportingDrain(cluster.UID, record.Class)
		return false, true, nil
	}
	priorReplicaSet := replicaSetByUID(replicaSets, record.PriorReplicaSetUID)
	if priorReplicaSet != nil {
		if priorReplicaSet.Spec.Replicas == nil || *priorReplicaSet.Spec.Replicas != 0 {
			zero := int32(0)
			patch := client.MergeFrom(priorReplicaSet.DeepCopy())
			priorReplicaSet.Spec.Replicas = &zero
			if err := r.Patch(ctx, priorReplicaSet, patch); err != nil {
				return false, false, fmt.Errorf("scale prior supporting ReplicaSet %s to zero: %w", priorReplicaSet.Name, err)
			}
			record.SealedAt = metav1.Now()
			r.resetSupportingDrain(cluster.UID, record.Class)
			return false, true, nil
		}
		if priorReplicaSet.Status.Replicas != 0 {
			return false, false, nil
		}
	}
	if !supportingDeploymentConverged(deployment) {
		return false, false, nil
	}
	// A full-second safety margin covers the metav1.Time one-second truncation and
	// modest clock skew, so the drain never completes early.
	if time.Since(record.SealedAt.Time) < supportingRevocationDrain+time.Second {
		return false, false, nil
	}
	// Sweep EVERY revoked pod of the class: any pod owned by the prior ReplicaSet,
	// and any pod stamped below the security-generation floor (a late write of a
	// revoked generation), not just one prior UID.
	swept, err := r.sweepRevokedSupportingPods(ctx, reader, cluster, record)
	if err != nil {
		return false, false, err
	}
	if swept > 0 {
		r.resetSupportingDrain(cluster.UID, record.Class)
		return false, true, nil
	}
	if r.observeSupportingDrainZero(cluster.UID, record.Class) < 2 {
		return false, false, nil
	}
	r.resetSupportingDrain(cluster.UID, record.Class)
	return true, false, nil
}

// sweepRevokedSupportingPods deletes every pod of the class that belongs to a
// revoked generation: owned by the prior ReplicaSet, or stamped below the
// security-generation floor. It returns the number deleted.
func (r *PgShardClusterReconciler) sweepRevokedSupportingPods(ctx context.Context, reader client.Reader, cluster *pgshardv1alpha1.PgShardCluster, record *pgshardv1alpha1.SupportingGenerationStatus) (int, error) {
	pods := &corev1.PodList{}
	if err := reader.List(ctx, pods, client.InNamespace(cluster.Namespace)); err != nil {
		return 0, fmt.Errorf("list supporting pods to sweep: %w", err)
	}
	deleted := 0
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil {
			continue
		}
		// A pod owned by the prior ReplicaSet is definitionally a prior generation of
		// this class and is swept regardless of its labels, so a late write cannot
		// evade the sweep by stripping its class labels. The below-floor sweep is
		// scoped to the class by label, so it never reaps another class's or another
		// cluster's pods.
		ownedByPrior := record.PriorReplicaSetUID != "" && string(controllerOwnerUID(pod.OwnerReferences)) == record.PriorReplicaSetUID
		inClass := pod.Labels[owned.ComponentLabel] == record.Class && pod.Labels[owned.ClusterLabel] == cluster.Name
		generation, _ := strconv.ParseInt(pod.Annotations[owned.PodSecurityGenerationAnnotation], 10, 64)
		belowFloor := inClass && record.MinGenerationForNewCreates > 0 && generation < record.MinGenerationForNewCreates
		if !ownedByPrior && !belowFloor {
			continue
		}
		if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			return deleted, fmt.Errorf("delete revoked supporting pod %s: %w", pod.Name, err)
		}
		deleted++
	}
	return deleted, nil
}

// supportingDeploymentConverged reports whether a Deployment has fully rolled
// forward: its observed generation caught up and every replica is updated and
// available at the current template.
func supportingDeploymentConverged(deployment *appsv1.Deployment) bool {
	if deployment.Status.ObservedGeneration < deployment.Generation {
		return false
	}
	desired := int32(1)
	if deployment.Spec.Replicas != nil {
		desired = *deployment.Spec.Replicas
	}
	return deployment.Status.UpdatedReplicas == desired && deployment.Status.AvailableReplicas == desired && deployment.Status.Replicas == desired
}

func (r *PgShardClusterReconciler) listOwnedReplicaSets(ctx context.Context, reader client.Reader, namespace string, deploymentUID types.UID) ([]appsv1.ReplicaSet, error) {
	list := &appsv1.ReplicaSetList{}
	if err := reader.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list ReplicaSets for supporting generation reconcile: %w", err)
	}
	owned := make([]appsv1.ReplicaSet, 0, len(list.Items))
	for i := range list.Items {
		if controllerOwnerUID(list.Items[i].OwnerReferences) == deploymentUID {
			owned = append(owned, list.Items[i])
		}
	}
	return owned, nil
}

func (r *PgShardClusterReconciler) listOwnedPods(ctx context.Context, reader client.Reader, namespace string, replicaSetUID string) ([]corev1.Pod, error) {
	list := &corev1.PodList{}
	if err := reader.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list pods for supporting generation reconcile: %w", err)
	}
	owned := make([]corev1.Pod, 0)
	for i := range list.Items {
		if string(controllerOwnerUID(list.Items[i].OwnerReferences)) == replicaSetUID {
			owned = append(owned, list.Items[i])
		}
	}
	return owned, nil
}

func (r *PgShardClusterReconciler) resetSupportingDrain(clusterUID types.UID, class string) {
	supportingDrainObservations.Delete(string(clusterUID) + "/" + class)
}

func (r *PgShardClusterReconciler) observeSupportingDrainZero(clusterUID types.UID, class string) int {
	key := string(clusterUID) + "/" + class
	value, _ := supportingDrainObservations.LoadOrStore(key, 0)
	count := value.(int) + 1
	supportingDrainObservations.Store(key, count)
	return count
}

func supportingGenerationIndex(cluster *pgshardv1alpha1.PgShardCluster, class string) int {
	for i := range cluster.Status.SupportingGenerations {
		if cluster.Status.SupportingGenerations[i].Class == class {
			return i
		}
	}
	cluster.Status.SupportingGenerations = append(cluster.Status.SupportingGenerations, pgshardv1alpha1.SupportingGenerationStatus{Class: class})
	return len(cluster.Status.SupportingGenerations) - 1
}

func findPlanDeployment(plan []client.Object, name string) *appsv1.Deployment {
	for _, object := range plan {
		if deployment, ok := object.(*appsv1.Deployment); ok && deployment.Name == name {
			return deployment
		}
	}
	return nil
}

func replicaSetByTemplateHash(replicaSets []appsv1.ReplicaSet, hash string) *appsv1.ReplicaSet {
	if hash == "" {
		return nil
	}
	for i := range replicaSets {
		if replicaSets[i].Spec.Template.Annotations[owned.PodContractHashAnnotation] == hash {
			return &replicaSets[i]
		}
	}
	return nil
}

func replicaSetByUID(replicaSets []appsv1.ReplicaSet, uid string) *appsv1.ReplicaSet {
	for i := range replicaSets {
		if string(replicaSets[i].UID) == uid {
			return &replicaSets[i]
		}
	}
	return nil
}

func controllerOwnerUID(refs []metav1.OwnerReference) types.UID {
	for i := range refs {
		if refs[i].Controller != nil && *refs[i].Controller {
			return refs[i].UID
		}
	}
	return ""
}

func parseSecurityGeneration(raw string) int64 {
	generation, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || generation < 0 {
		return 0
	}
	return generation
}
