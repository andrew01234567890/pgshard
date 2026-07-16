// Package controller contains Kubernetes reconcilers for pgshard APIs.
package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	readyCondition               = "Ready"
	reconciledCondition          = "ResourcesReconciled"
	supportingAvailableCondition = "SupportingWorkloadsAvailable"
	transportSecurityCondition   = "TransportSecurityReady"
	resourceFinalizer            = "pgshard.io/owned-resources"
	hpaScaleFieldManager         = "pgshard-hpa-scale"
	ownershipMigrationManager    = "pgshard-ownership-migration"
	retryDelay                   = 15 * time.Second
)

// PgShardClusterReconciler owns safe supporting resources while failing closed
// on the unavailable PostgreSQL lifecycle. Ready is never inferred merely from
// desired objects existing; supporting availability comes from workload status.
type PgShardClusterReconciler struct {
	client.Client
	// APIReader bypasses the informer cache for ownership migration, HPA presence
	// gates, replica handoff, deletion-finalizer absence proofs, and post-apply
	// workload status.
	// Writes and plan reconciliation continue through Client.
	APIReader client.Reader
	Images    owned.Images
}

// +kubebuilder:rbac:groups=pgshard.io,resources=pgshardclusters,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=pgshard.io,resources=pgshardclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pgshard.io,resources=pgshardclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps;persistentvolumeclaims;services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

func (r *PgShardClusterReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	cluster := &pgshardv1alpha1.PgShardCluster{}
	if err := r.Get(ctx, request.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !cluster.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(cluster, resourceFinalizer) {
			return ctrl.Result{}, nil
		}
		remaining, err := r.prune(ctx, cluster, nil, true)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("prune resources during cluster deletion: %w", err)
		}
		if remaining {
			return ctrl.Result{RequeueAfter: retryDelay}, nil
		}
		controllerutil.RemoveFinalizer(cluster, resourceFinalizer)
		if err := r.Update(ctx, cluster); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}
	images := r.Images
	if images == (owned.Images{}) {
		images = owned.DefaultImages()
	}
	plan, err := owned.Plan(cluster, images)
	if err != nil {
		statusErr := r.reportFailure(ctx, cluster, "PlanInvalid", fmt.Sprintf("cannot safely plan owned resources: %v", err))
		return ctrl.Result{}, errors.Join(err, statusErr)
	}
	states, err := r.preflightOwnership(ctx, cluster, plan)
	if err != nil {
		statusErr := r.reportFailure(ctx, cluster, "ReconcileFailed", fmt.Sprintf("owned resource reconciliation failed: %v", err))
		return ctrl.Result{}, errors.Join(err, statusErr)
	}
	staleHPA, err := r.ownedHPAForFixedTransition(ctx, cluster)
	if err != nil {
		statusErr := r.reportFailure(ctx, cluster, "ReconcileFailed", fmt.Sprintf("pooler scaling transition failed: %v", err))
		return ctrl.Result{}, errors.Join(err, statusErr)
	}
	if !controllerutil.ContainsFinalizer(cluster, resourceFinalizer) {
		controllerutil.AddFinalizer(cluster, resourceFinalizer)
		if err := r.Update(ctx, cluster); err != nil {
			return ctrl.Result{}, err
		}
	}
	if staleHPA != nil {
		uid := staleHPA.UID
		resourceVersion := staleHPA.ResourceVersion
		if err := r.Delete(ctx, staleHPA, client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("delete HPA before fixed scaling: %w", err)
		}
		if err := r.reportScalingTransition(ctx, cluster); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	if err := r.applyPlan(ctx, cluster, plan, states); err != nil {
		statusErr := r.reportFailure(ctx, cluster, "ReconcileFailed", fmt.Sprintf("owned resource reconciliation failed: %v", err))
		return ctrl.Result{}, errors.Join(err, statusErr)
	}

	available, message, err := r.supportingWorkloadsAvailable(ctx, cluster)
	if err != nil {
		statusErr := r.reportFailure(ctx, cluster, "ObservationFailed", fmt.Sprintf("cannot observe supporting workloads: %v", err))
		return ctrl.Result{}, errors.Join(err, statusErr)
	}
	if err := r.reportSuccess(ctx, cluster, available, message); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	if !available {
		return ctrl.Result{RequeueAfter: retryDelay}, nil
	}
	return ctrl.Result{}, nil
}

func (r *PgShardClusterReconciler) applyPlan(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, plan []client.Object, states map[string]ownershipState) error {
	for _, desired := range plan {
		if err := r.applyObject(ctx, cluster, desired, states[owned.Key(desired)]); err != nil {
			return fmt.Errorf("apply %T %s/%s: %w", desired, desired.GetNamespace(), desired.GetName(), err)
		}
	}
	if _, err := r.prune(ctx, cluster, plan, false); err != nil {
		return fmt.Errorf("prune stale resources: %w", err)
	}
	return nil
}

type ownershipState struct {
	exists bool
	object client.Object
}

func (r *PgShardClusterReconciler) preflightOwnership(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, plan []client.Object) (map[string]ownershipState, error) {
	states := make(map[string]ownershipState, len(plan))
	for _, desired := range plan {
		existing := desired.DeepCopyObject().(client.Object)
		if err := r.Get(ctx, client.ObjectKeyFromObject(desired), existing); apierrors.IsNotFound(err) {
			states[owned.Key(desired)] = ownershipState{}
			continue
		} else if err != nil {
			return nil, fmt.Errorf("check ownership of %T %s/%s: %w", desired, desired.GetNamespace(), desired.GetName(), err)
		}
		if !metav1.IsControlledBy(existing, cluster) {
			return nil, fmt.Errorf("resource collision: existing %T %s/%s is not controlled by PgShardCluster UID %s", existing, existing.GetNamespace(), existing.GetName(), cluster.UID)
		}
		states[owned.Key(desired)] = ownershipState{exists: true, object: existing}
	}
	return states, nil
}

func (r *PgShardClusterReconciler) applyObject(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, desired client.Object, state ownershipState) error {
	desiredDeployment, isDeployment := desired.(*appsv1.Deployment)
	isPoolerDeployment := isDeployment && desiredDeployment.Name == cluster.Name+owned.PoolerSuffix
	isHPAPooler := isPoolerDeployment && desiredDeployment.Spec.Replicas == nil
	created := false
	if !state.exists {
		create := desired.DeepCopyObject().(client.Object)
		removeApplyOwnershipMarker(create)
		if deployment, ok := create.(*appsv1.Deployment); ok && isHPAPooler {
			replicas := poolerMinimum(cluster)
			deployment.Spec.Replicas = &replicas
		}
		if err := r.Create(ctx, create, client.FieldOwner(owned.ManagedByValue)); err != nil {
			if apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("resource appeared after ownership preflight; refusing to adopt it")
			}
			return err
		}
		state = ownershipState{exists: true, object: create}
		created = true
	}

	gvk, err := objectGVK(desired)
	if err != nil {
		return err
	}
	if !created && !applyOwnershipMigrationComplete(state.object) {
		legacyDesired := desired.DeepCopyObject().(client.Object)
		removeApplyOwnershipMarker(legacyDesired)
		aligned, err := r.alignLegacyOwnedFields(ctx, state.object, legacyDesired, isPoolerDeployment)
		if err != nil {
			return err
		}
		state.object = aligned
	}
	if created || !hasApplyOwnership(state.object, owned.ManagedByValue) {
		// Kubernetes names all pre-existing fields "before-first-apply" when
		// the first apply follows a create or untracked update. Establish the
		// apply field set without publishing the completed-migration marker, then
		// remove synthetic and legacy co-owners before the authoritative apply.
		bootstrap := desired.DeepCopyObject().(client.Object)
		removeApplyOwnershipMarker(bootstrap)
		bootstrap.GetObjectKind().SetGroupVersionKind(gvk)
		bootstrap.SetUID(state.object.GetUID())
		if err := r.Patch(ctx, bootstrap, client.Apply, client.FieldOwner(owned.ManagedByValue), client.ForceOwnership); err != nil {
			return fmt.Errorf("bootstrap server-side apply ownership: %w", err)
		}
		state.object = bootstrap
	}
	migrated, err := r.migrateApplyOwnership(ctx, state.object)
	if err != nil {
		return err
	}
	state.object = migrated

	if isHPAPooler {
		if err := r.handoffPoolerReplicas(ctx, cluster, desiredDeployment, state.object.GetUID(), gvk); err != nil {
			return err
		}
	}
	desired = desired.DeepCopyObject().(client.Object)
	desired.GetObjectKind().SetGroupVersionKind(gvk)
	desired.SetUID(state.object.GetUID())
	if err := r.Patch(ctx, desired, client.Apply, client.FieldOwner(owned.ManagedByValue), client.ForceOwnership); err != nil {
		return err
	}
	if isPoolerDeployment && !isHPAPooler {
		return r.relinquishPoolerScaleOwnership(ctx, desiredDeployment, state.object.GetUID(), gvk)
	}
	return nil
}

func removeApplyOwnershipMarker(object client.Object) {
	annotations := maps.Clone(object.GetAnnotations())
	delete(annotations, owned.ApplyOwnershipAnnotation)
	if len(annotations) == 0 {
		annotations = nil
	}
	object.SetAnnotations(annotations)
}

func (r *PgShardClusterReconciler) authoritativeReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *PgShardClusterReconciler) alignLegacyOwnedFields(ctx context.Context, current, desired client.Object, allowLegacyHPAScale bool) (client.Object, error) {
	const maxAttempts = 4
	originalUID := current.GetUID()
	authoritative := current.DeepCopyObject().(client.Object)
	if err := r.authoritativeReader().Get(ctx, client.ObjectKeyFromObject(current), authoritative); err != nil {
		return nil, fmt.Errorf("read authoritative legacy fields before alignment: %w", err)
	}
	if authoritative.GetUID() != originalUID {
		return nil, fmt.Errorf("resource was replaced before legacy field alignment")
	}
	current = authoritative
	var lastConflict error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if applyOwnershipMigrationComplete(current) {
			return current, nil
		}
		if hasUnrelatedTopLevelApplyOwnership(current, allowLegacyHPAScale) {
			return nil, fmt.Errorf("cannot safely align legacy fields while another top-level Apply manager is present")
		}
		aligned, err := legacyAlignedObject(current, desired)
		if err != nil {
			return nil, err
		}
		if err := r.Update(ctx, aligned, client.FieldOwner(ownershipMigrationManager)); err == nil {
			return aligned, nil
		} else if !apierrors.IsConflict(err) {
			return nil, fmt.Errorf("align legacy operator-owned fields: %w", err)
		} else {
			lastConflict = err
		}

		fresh := current.DeepCopyObject().(client.Object)
		if err := r.authoritativeReader().Get(ctx, client.ObjectKeyFromObject(current), fresh); err != nil {
			return nil, fmt.Errorf("reload legacy fields after conflict: %w", err)
		}
		if fresh.GetUID() != originalUID {
			return nil, fmt.Errorf("resource was replaced during legacy field alignment")
		}
		current = fresh
	}
	return nil, fmt.Errorf("align legacy operator-owned fields after %d conflicts: %w", maxAttempts, lastConflict)
}

func hasUnrelatedTopLevelApplyOwnership(object client.Object, allowHPAScale bool) bool {
	for _, entry := range object.GetManagedFields() {
		if entry.Operation != metav1.ManagedFieldsOperationApply || entry.Subresource != "" || entry.Manager == owned.ManagedByValue {
			continue
		}
		if allowHPAScale && entry.Manager == hpaScaleFieldManager {
			continue
		}
		return true
	}
	return false
}

func legacyAlignedObject(current, desired client.Object) (client.Object, error) {
	aligned := current.DeepCopyObject().(client.Object)
	aligned.SetLabels(maps.Clone(desired.GetLabels()))
	aligned.SetAnnotations(maps.Clone(desired.GetAnnotations()))
	aligned.SetOwnerReferences(append([]metav1.OwnerReference(nil), desired.GetOwnerReferences()...))

	switch wanted := desired.(type) {
	case *corev1.ConfigMap:
		got, ok := aligned.(*corev1.ConfigMap)
		if !ok {
			return nil, fmt.Errorf("legacy object type %T does not match desired ConfigMap", current)
		}
		got.Data = maps.Clone(wanted.Data)
		got.BinaryData = maps.Clone(wanted.BinaryData)
		got.Immutable = nil
		if wanted.Immutable != nil {
			immutable := *wanted.Immutable
			got.Immutable = &immutable
		}
	case *corev1.Service:
		got, ok := aligned.(*corev1.Service)
		if !ok {
			return nil, fmt.Errorf("legacy object type %T does not match desired Service", current)
		}
		ports := wanted.Spec.DeepCopy().Ports
		if wanted.Spec.Type == corev1.ServiceTypeNodePort || wanted.Spec.Type == corev1.ServiceTypeLoadBalancer {
			for index := range ports {
				if ports[index].NodePort != 0 {
					continue
				}
				for _, existing := range got.Spec.Ports {
					if existing.Name == ports[index].Name && existing.Protocol == ports[index].Protocol && existing.Port == ports[index].Port {
						ports[index].NodePort = existing.NodePort
						break
					}
				}
			}
		}
		got.Spec.Type = wanted.Spec.Type
		got.Spec.Selector = maps.Clone(wanted.Spec.Selector)
		got.Spec.Ports = ports
		got.Spec.PublishNotReadyAddresses = wanted.Spec.PublishNotReadyAddresses
	case *appsv1.Deployment:
		got, ok := aligned.(*appsv1.Deployment)
		if !ok {
			return nil, fmt.Errorf("legacy object type %T does not match desired Deployment", current)
		}
		replicas := got.Spec.Replicas
		got.Spec = *wanted.Spec.DeepCopy()
		if wanted.Spec.Replicas == nil {
			got.Spec.Replicas = replicas
		}
	case *appsv1.StatefulSet:
		got, ok := aligned.(*appsv1.StatefulSet)
		if !ok {
			return nil, fmt.Errorf("legacy object type %T does not match desired StatefulSet", current)
		}
		got.Spec = *wanted.Spec.DeepCopy()
	case *autoscalingv2.HorizontalPodAutoscaler:
		got, ok := aligned.(*autoscalingv2.HorizontalPodAutoscaler)
		if !ok {
			return nil, fmt.Errorf("legacy object type %T does not match desired HorizontalPodAutoscaler", current)
		}
		got.Spec = *wanted.Spec.DeepCopy()
	case *networkingv1.NetworkPolicy:
		got, ok := aligned.(*networkingv1.NetworkPolicy)
		if !ok {
			return nil, fmt.Errorf("legacy object type %T does not match desired NetworkPolicy", current)
		}
		got.Spec = *wanted.Spec.DeepCopy()
	case *policyv1.PodDisruptionBudget:
		got, ok := aligned.(*policyv1.PodDisruptionBudget)
		if !ok {
			return nil, fmt.Errorf("legacy object type %T does not match desired PodDisruptionBudget", current)
		}
		got.Spec = *wanted.Spec.DeepCopy()
	default:
		return nil, fmt.Errorf("unsupported legacy planned object type %T", desired)
	}
	return aligned, nil
}

func (r *PgShardClusterReconciler) handoffPoolerReplicas(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, desired *appsv1.Deployment, expectedUID types.UID, gvk schema.GroupVersionKind) error {
	const maxAttempts = 4
	var lastConflict error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		current := &appsv1.Deployment{}
		if err := r.authoritativeReader().Get(ctx, client.ObjectKeyFromObject(desired), current); err != nil {
			return fmt.Errorf("read authoritative pooler replicas before HPA handoff: %w", err)
		}
		if current.UID != expectedUID {
			return fmt.Errorf("pooler Deployment was replaced before HPA handoff")
		}
		if hasExactReplicaApplyOwnership(current, hpaScaleFieldManager) {
			return nil
		}
		replicas := poolerMinimum(cluster)
		if current.Spec.Replicas != nil {
			replicas = *current.Spec.Replicas
		}
		metadata := map[string]any{
			"name":      desired.Name,
			"namespace": desired.Namespace,
			"uid":       string(current.UID),
		}
		if current.ResourceVersion != "" {
			metadata["resourceVersion"] = current.ResourceVersion
		}
		handoff := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": gvk.GroupVersion().String(),
			"kind":       gvk.Kind,
			"metadata":   metadata,
			"spec":       map[string]any{"replicas": int64(replicas)},
		}}
		if err := r.Patch(ctx, handoff, client.Apply, client.FieldOwner(hpaScaleFieldManager), client.ForceOwnership); err == nil {
			return nil
		} else if !apierrors.IsConflict(err) {
			return fmt.Errorf("hand off pooler replicas to HPA: %w", err)
		} else {
			lastConflict = err
		}
	}
	return fmt.Errorf("hand off pooler replicas after %d conflicts: %w", maxAttempts, lastConflict)
}

func hasExactReplicaApplyOwnership(object client.Object, manager string) bool {
	found := false
	for _, entry := range object.GetManagedFields() {
		if entry.Manager != manager || entry.Operation != metav1.ManagedFieldsOperationApply || entry.Subresource != "" {
			continue
		}
		if found || entry.FieldsV1 == nil {
			return false
		}
		found = true
		var root map[string]json.RawMessage
		if err := json.Unmarshal(entry.FieldsV1.Raw, &root); err != nil || len(root) != 1 {
			return false
		}
		var spec map[string]json.RawMessage
		if err := json.Unmarshal(root["f:spec"], &spec); err != nil || len(spec) != 1 {
			return false
		}
		var replicas map[string]json.RawMessage
		if err := json.Unmarshal(spec["f:replicas"], &replicas); err != nil || replicas == nil || len(replicas) != 0 {
			return false
		}
	}
	return found
}

func (r *PgShardClusterReconciler) relinquishPoolerScaleOwnership(ctx context.Context, desired *appsv1.Deployment, expectedUID types.UID, gvk schema.GroupVersionKind) error {
	const maxAttempts = 4
	if desired.Spec.Replicas == nil {
		return fmt.Errorf("fixed-scale pooler Deployment has no desired replicas")
	}
	var lastRetry error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		current := &appsv1.Deployment{}
		if err := r.authoritativeReader().Get(ctx, client.ObjectKeyFromObject(desired), current); err != nil {
			return fmt.Errorf("read authoritative pooler ownership before fixed-scale handoff: %w", err)
		}
		if current.UID != expectedUID {
			return fmt.Errorf("pooler Deployment was replaced before fixed-scale handoff")
		}
		if current.Spec.Replicas == nil || *current.Spec.Replicas != *desired.Spec.Replicas || !hasReplicaApplyOwnership(current, owned.ManagedByValue) {
			reclaim := desired.DeepCopy()
			reclaim.GetObjectKind().SetGroupVersionKind(gvk)
			reclaim.UID = current.UID
			reclaim.ResourceVersion = current.ResourceVersion
			if err := r.Patch(ctx, reclaim, client.Apply, client.FieldOwner(owned.ManagedByValue), client.ForceOwnership); err == nil {
				lastRetry = errors.New("fixed pooler replicas were not yet authoritative after re-apply")
				continue
			} else if !apierrors.IsConflict(err) {
				return fmt.Errorf("reclaim fixed pooler replicas before HPA-manager relinquishment: %w", err)
			} else {
				lastRetry = err
				continue
			}
		}
		if !hasApplyOwnership(current, hpaScaleFieldManager) {
			return nil
		}
		metadata := map[string]any{
			"name":      desired.Name,
			"namespace": desired.Namespace,
			"uid":       string(current.UID),
		}
		if current.ResourceVersion != "" {
			metadata["resourceVersion"] = current.ResourceVersion
		}
		relinquish := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": gvk.GroupVersion().String(),
			"kind":       gvk.Kind,
			"metadata":   metadata,
		}}
		if err := r.Patch(ctx, relinquish, client.Apply, client.FieldOwner(hpaScaleFieldManager), client.ForceOwnership); err == nil {
			return nil
		} else if !apierrors.IsConflict(err) {
			return fmt.Errorf("relinquish pooler replicas from HPA manager: %w", err)
		} else {
			lastRetry = err
		}
	}
	return fmt.Errorf("stabilize fixed pooler replicas and relinquish HPA ownership after %d attempts: %w", maxAttempts, lastRetry)
}

func hasApplyOwnership(object client.Object, manager string) bool {
	for _, entry := range object.GetManagedFields() {
		if entry.Manager == manager && entry.Operation == metav1.ManagedFieldsOperationApply && entry.Subresource == "" {
			return true
		}
	}
	return false
}

func hasReplicaApplyOwnership(object client.Object, manager string) bool {
	for _, entry := range object.GetManagedFields() {
		if entry.Manager != manager || entry.Operation != metav1.ManagedFieldsOperationApply || entry.Subresource != "" || entry.FieldsV1 == nil {
			continue
		}
		var root map[string]json.RawMessage
		if err := json.Unmarshal(entry.FieldsV1.Raw, &root); err != nil {
			continue
		}
		var spec map[string]json.RawMessage
		if err := json.Unmarshal(root["f:spec"], &spec); err != nil {
			continue
		}
		var replicas map[string]json.RawMessage
		if err := json.Unmarshal(spec["f:replicas"], &replicas); err == nil && replicas != nil && len(replicas) == 0 {
			return true
		}
	}
	return false
}

func applyOwnershipMigrationComplete(object metav1.Object) bool {
	if object.GetAnnotations()[owned.ApplyOwnershipAnnotation] != owned.ApplyOwnershipVersion {
		return false
	}
	for _, entry := range object.GetManagedFields() {
		if entry.Manager != owned.ManagedByValue || entry.Operation != metav1.ManagedFieldsOperationApply || entry.Subresource != "" || entry.FieldsV1 == nil {
			continue
		}
		var root map[string]json.RawMessage
		if err := json.Unmarshal(entry.FieldsV1.Raw, &root); err != nil {
			continue
		}
		var metadata map[string]json.RawMessage
		if err := json.Unmarshal(root["f:metadata"], &metadata); err != nil {
			continue
		}
		var annotations map[string]json.RawMessage
		if err := json.Unmarshal(metadata["f:annotations"], &annotations); err != nil {
			continue
		}
		var marker map[string]json.RawMessage
		if err := json.Unmarshal(annotations["f:"+owned.ApplyOwnershipAnnotation], &marker); err == nil && marker != nil && len(marker) == 0 {
			return true
		}
	}
	return false
}

func (r *PgShardClusterReconciler) migrateApplyOwnership(ctx context.Context, object client.Object) (client.Object, error) {
	const maxAttempts = 4
	originalUID := object.GetUID()
	current := object
	var lastConflict error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		managedFields := current.GetManagedFields()
		filtered := make([]metav1.ManagedFieldsEntry, 0, len(managedFields))
		removed := false
		migrationComplete := applyOwnershipMigrationComplete(current)
		for _, entry := range managedFields {
			// Kubernetes derives an omitted Update field manager from the client
			// user agent, so pre-SSA releases did not leave one stable manager name.
			// Before the durable marker exists, the type-aware alignment above has
			// reset the operator-controlled fields and every top-level Update field
			// set belongs to that legacy whole-object ownership era. Once the marker
			// and Apply field set both exist, preserve every unrelated later manager.
			legacyOwner := !migrationComplete || entry.Manager == owned.ManagedByValue || entry.Manager == ownershipMigrationManager || entry.Manager == "before-first-apply"
			if entry.Subresource == "" && entry.Operation == metav1.ManagedFieldsOperationUpdate && legacyOwner {
				removed = true
				continue
			}
			filtered = append(filtered, entry)
		}
		if !removed {
			return current, nil
		}
		if len(filtered) == 0 {
			// A singleton empty entry is the Kubernetes API's explicit request to
			// reset managed fields; an empty slice can disappear under omitempty.
			filtered = []metav1.ManagedFieldsEntry{{}}
		}
		migrated := current.DeepCopyObject().(client.Object)
		migrated.SetManagedFields(filtered)
		if err := r.Update(ctx, migrated, client.FieldOwner(ownershipMigrationManager)); err == nil {
			return migrated, nil
		} else if !apierrors.IsConflict(err) {
			return nil, fmt.Errorf("migrate create-time field ownership: %w", err)
		} else {
			lastConflict = err
		}

		fresh := current.DeepCopyObject().(client.Object)
		if err := r.authoritativeReader().Get(ctx, client.ObjectKeyFromObject(current), fresh); err != nil {
			return nil, fmt.Errorf("reload field ownership after conflict: %w", err)
		}
		if fresh.GetUID() != originalUID {
			return nil, fmt.Errorf("resource was replaced during field-ownership migration")
		}
		current = fresh
	}
	return nil, fmt.Errorf("migrate create-time field ownership after %d conflicts: %w", maxAttempts, lastConflict)
}

func (r *PgShardClusterReconciler) ownedHPAForFixedTransition(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (*autoscalingv2.HorizontalPodAutoscaler, error) {
	if cluster.Spec.Pooler.Scaling.Mode != pgshardv1alpha1.ScalingFixed {
		return nil, nil
	}
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + owned.PoolerSuffix}
	if err := r.authoritativeReader().Get(ctx, key, hpa); apierrors.IsNotFound(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	if !metav1.IsControlledBy(hpa, cluster) {
		return nil, fmt.Errorf("resource collision: existing HPA %s/%s is not controlled by PgShardCluster UID %s", hpa.Namespace, hpa.Name, cluster.UID)
	}
	return hpa, nil
}

func objectGVK(object client.Object) (schema.GroupVersionKind, error) {
	switch object.(type) {
	case *corev1.ConfigMap:
		return corev1.SchemeGroupVersion.WithKind("ConfigMap"), nil
	case *corev1.Service:
		return corev1.SchemeGroupVersion.WithKind("Service"), nil
	case *appsv1.Deployment:
		return appsv1.SchemeGroupVersion.WithKind("Deployment"), nil
	case *appsv1.StatefulSet:
		return appsv1.SchemeGroupVersion.WithKind("StatefulSet"), nil
	case *autoscalingv2.HorizontalPodAutoscaler:
		return autoscalingv2.SchemeGroupVersion.WithKind("HorizontalPodAutoscaler"), nil
	case *networkingv1.NetworkPolicy:
		return networkingv1.SchemeGroupVersion.WithKind("NetworkPolicy"), nil
	case *policyv1.PodDisruptionBudget:
		return policyv1.SchemeGroupVersion.WithKind("PodDisruptionBudget"), nil
	default:
		return schema.GroupVersionKind{}, fmt.Errorf("unsupported planned object type %T", object)
	}
}

func (r *PgShardClusterReconciler) prune(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, plan []client.Object, includePVCs bool) (bool, error) {
	desired := make(map[string]struct{}, len(plan))
	for _, object := range plan {
		desired[owned.Key(object)] = struct{}{}
	}

	var existing []client.Object
	reader := client.Reader(r.Client)
	if includePVCs {
		if r.APIReader == nil {
			return false, fmt.Errorf("authoritative API reader is required for deletion finalization")
		}
		reader = r.APIReader
	}
	listOptions := []client.ListOption{client.InNamespace(cluster.Namespace)}
	lists := []client.ObjectList{
		&corev1.ConfigMapList{},
		&corev1.ServiceList{},
		&appsv1.DeploymentList{},
		&appsv1.StatefulSetList{},
		&autoscalingv2.HorizontalPodAutoscalerList{},
		&networkingv1.NetworkPolicyList{},
		&policyv1.PodDisruptionBudgetList{},
	}
	if includePVCs {
		lists = append(lists, &corev1.PersistentVolumeClaimList{})
	}
	for _, list := range lists {
		if err := reader.List(ctx, list, listOptions...); err != nil {
			return false, fmt.Errorf("list %T: %w", list, err)
		}
		existing = append(existing, listObjects(list)...)
	}

	stale := make([]client.Object, 0)
	for _, object := range existing {
		if !metav1.IsControlledBy(object, cluster) {
			continue
		}
		if _, keep := desired[owned.Key(object)]; !keep {
			stale = append(stale, object)
		}
	}
	sort.Slice(stale, func(i, j int) bool { return owned.Key(stale[i]) < owned.Key(stale[j]) })
	for _, object := range stale {
		uid := object.GetUID()
		resourceVersion := object.GetResourceVersion()
		preconditions := client.Preconditions{UID: &uid}
		deleteOptions := []client.DeleteOption{preconditions}
		if !includePVCs {
			preconditions.ResourceVersion = &resourceVersion
			deleteOptions[0] = preconditions
		} else {
			deleteOptions = append(deleteOptions, client.PropagationPolicy(metav1.DeletePropagationForeground))
		}
		if err := r.Delete(ctx, object, deleteOptions...); err != nil && !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("delete %T %s/%s: %w", object, object.GetNamespace(), object.GetName(), err)
		}
	}
	return len(stale) > 0, nil
}

func listObjects(list client.ObjectList) []client.Object {
	var result []client.Object
	switch list := list.(type) {
	case *corev1.ConfigMapList:
		for index := range list.Items {
			result = append(result, &list.Items[index])
		}
	case *corev1.ServiceList:
		for index := range list.Items {
			result = append(result, &list.Items[index])
		}
	case *corev1.PersistentVolumeClaimList:
		for index := range list.Items {
			result = append(result, &list.Items[index])
		}
	case *appsv1.DeploymentList:
		for index := range list.Items {
			result = append(result, &list.Items[index])
		}
	case *appsv1.StatefulSetList:
		for index := range list.Items {
			result = append(result, &list.Items[index])
		}
	case *autoscalingv2.HorizontalPodAutoscalerList:
		for index := range list.Items {
			result = append(result, &list.Items[index])
		}
	case *networkingv1.NetworkPolicyList:
		for index := range list.Items {
			result = append(result, &list.Items[index])
		}
	case *policyv1.PodDisruptionBudgetList:
		for index := range list.Items {
			result = append(result, &list.Items[index])
		}
	}
	return result
}

func (r *PgShardClusterReconciler) supportingWorkloadsAvailable(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (bool, string, error) {
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	etcd := &appsv1.StatefulSet{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + owned.EtcdSuffix}, etcd); err != nil {
		return false, "", err
	}
	orchestrator := &appsv1.Deployment{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + owned.OrchestratorSuffix}, orchestrator); err != nil {
		return false, "", err
	}
	pooler := &appsv1.Deployment{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + owned.PoolerSuffix}, pooler); err != nil {
		return false, "", err
	}

	etcdWanted := int32(0)
	if etcd.Spec.Replicas != nil {
		etcdWanted = *etcd.Spec.Replicas
	}
	orchestratorWanted := int32(0)
	if orchestrator.Spec.Replicas != nil {
		orchestratorWanted = *orchestrator.Spec.Replicas
	}
	poolerWanted := poolerMinimum(cluster)
	if pooler.Spec.Replicas != nil && *pooler.Spec.Replicas > poolerWanted {
		poolerWanted = *pooler.Spec.Replicas
	}
	etcdReady := workloadGenerationObserved(etcd.Generation, etcd.Status.ObservedGeneration) && etcd.Status.ReadyReplicas >= etcdWanted && etcd.Status.UpdatedReplicas >= etcdWanted
	orchestratorReady := workloadGenerationObserved(orchestrator.Generation, orchestrator.Status.ObservedGeneration) && orchestrator.Status.AvailableReplicas >= orchestratorWanted && orchestrator.Status.UpdatedReplicas >= orchestratorWanted
	poolerReady := workloadGenerationObserved(pooler.Generation, pooler.Status.ObservedGeneration) && pooler.Status.AvailableReplicas >= poolerWanted && pooler.Status.UpdatedReplicas >= poolerWanted
	autoscalingReady := true
	autoscalingMessage := "fixed scaling selected"
	if cluster.Spec.Pooler.Scaling.Mode == pgshardv1alpha1.ScalingHPA {
		hpa := &autoscalingv2.HorizontalPodAutoscaler{}
		if err := reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + owned.PoolerSuffix}, hpa); err != nil {
			return false, "", err
		}
		observed := hpa.Status.ObservedGeneration != nil && *hpa.Status.ObservedGeneration >= hpa.Generation
		autoscalingReady = observed && hpaConditionTrue(hpa, autoscalingv2.AbleToScale) && hpaConditionTrue(hpa, autoscalingv2.ScalingActive)
		autoscalingMessage = fmt.Sprintf("HPA active=%t", autoscalingReady)
	}
	message := fmt.Sprintf("etcd %d/%d, orchestrator %d/%d, pooler %d/%d replicas available; %s", etcd.Status.ReadyReplicas, etcdWanted, orchestrator.Status.AvailableReplicas, orchestratorWanted, pooler.Status.AvailableReplicas, poolerWanted, autoscalingMessage)
	return etcdReady && orchestratorReady && poolerReady && autoscalingReady, message, nil
}

func hpaConditionTrue(hpa *autoscalingv2.HorizontalPodAutoscaler, conditionType autoscalingv2.HorizontalPodAutoscalerConditionType) bool {
	for _, condition := range hpa.Status.Conditions {
		if condition.Type == conditionType {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func workloadGenerationObserved(generation, observed int64) bool {
	return generation == 0 || observed >= generation
}

func poolerMinimum(cluster *pgshardv1alpha1.PgShardCluster) int32 {
	if cluster.Spec.Pooler.Scaling.Mode == pgshardv1alpha1.ScalingFixed {
		return cluster.Spec.Pooler.Scaling.Fixed.Replicas
	}
	return cluster.Spec.Pooler.Scaling.HPA.MinReplicas
}

func (r *PgShardClusterReconciler) reportSuccess(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, available bool, availabilityMessage string) error {
	status := metav1.ConditionFalse
	reason := "SupportingWorkloadsProgressing"
	phase := "Reconciling"
	if available {
		status = metav1.ConditionTrue
		reason = "SupportingWorkloadsAvailable"
		phase = "Pending"
	}
	conditions := []metav1.Condition{
		{
			Type:               reconciledCondition,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: cluster.Generation,
			Reason:             "ResourcesApplied",
			Message:            "the deterministic supporting-resource plan is applied and stale owned resources are pruned",
		},
		{
			Type:               supportingAvailableCondition,
			Status:             status,
			ObservedGeneration: cluster.Generation,
			Reason:             reason,
			Message:            availabilityMessage,
		},
		{
			Type:               readyCondition,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: cluster.Generation,
			Reason:             "PostgreSQLLifecycleUnavailable",
			Message:            "PostgreSQL Pods are intentionally absent: bootstrap, replication, fencing integration, promotion, and recovery are not implemented",
		},
		{
			Type:               transportSecurityCondition,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: cluster.Generation,
			Reason:             "EtcdTLSUnavailable",
			Message:            "an etcd ingress NetworkPolicy is reconciled, but authenticated TLS for client and peer traffic is not implemented",
		},
	}
	return r.updateStatus(ctx, cluster, cluster.Generation, phase, conditions)
}

func (r *PgShardClusterReconciler) reportFailure(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, reason, message string) error {
	conditions := []metav1.Condition{
		{
			Type:               reconciledCondition,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: cluster.Generation,
			Reason:             reason,
			Message:            message,
		},
		{
			Type:               supportingAvailableCondition,
			Status:             metav1.ConditionUnknown,
			ObservedGeneration: cluster.Generation,
			Reason:             "ObservationStale",
			Message:            "supporting workload availability is not current because resource reconciliation did not complete",
		},
		{
			Type:               readyCondition,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: cluster.Generation,
			Reason:             reason,
			Message:            message,
		},
		{
			Type:               transportSecurityCondition,
			Status:             metav1.ConditionUnknown,
			ObservedGeneration: cluster.Generation,
			Reason:             "TransportSecurityUnobserved",
			Message:            "transport isolation could not be observed because resource reconciliation did not complete",
		},
	}
	// ObservedGeneration is intentionally not advanced when the plan was not
	// fully applied and pruned.
	return r.updateStatus(ctx, cluster, cluster.Status.ObservedGeneration, "Degraded", conditions)
}

func (r *PgShardClusterReconciler) reportScalingTransition(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) error {
	conditions := []metav1.Condition{
		{
			Type:               reconciledCondition,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: cluster.Generation,
			Reason:             "PoolerScalingTransition",
			Message:            "the HPA was deleted; fixed replicas will be claimed only after its absence is observed",
		},
		{
			Type:               supportingAvailableCondition,
			Status:             metav1.ConditionUnknown,
			ObservedGeneration: cluster.Generation,
			Reason:             "PoolerScalingTransition",
			Message:            "supporting workload availability is not evaluated during the pooler scaling handoff",
		},
		{
			Type:               readyCondition,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: cluster.Generation,
			Reason:             "PostgreSQLLifecycleUnavailable",
			Message:            "PostgreSQL Pods are intentionally absent: bootstrap, replication, fencing integration, promotion, and recovery are not implemented",
		},
		{
			Type:               transportSecurityCondition,
			Status:             metav1.ConditionUnknown,
			ObservedGeneration: cluster.Generation,
			Reason:             "PoolerScalingTransition",
			Message:            "transport resources are not evaluated during the pooler scaling handoff",
		},
	}
	return r.updateStatus(ctx, cluster, cluster.Status.ObservedGeneration, "Reconciling", conditions)
}

func (r *PgShardClusterReconciler) updateStatus(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, observedGeneration int64, phase string, conditions []metav1.Condition) error {
	before := cluster.DeepCopy().Status
	cluster.Status.ObservedGeneration = observedGeneration
	cluster.Status.Phase = phase
	for _, condition := range conditions {
		meta.SetStatusCondition(&cluster.Status.Conditions, condition)
	}
	if statusesEqual(before, cluster.Status) {
		return nil
	}
	return r.Status().Update(ctx, cluster)
}

func statusesEqual(left, right pgshardv1alpha1.PgShardClusterStatus) bool {
	if left.ObservedGeneration != right.ObservedGeneration || left.Phase != right.Phase || len(left.Conditions) != len(right.Conditions) {
		return false
	}
	for index := range left.Conditions {
		if left.Conditions[index] != right.Conditions[index] {
			return false
		}
	}
	return true
}

func (r *PgShardClusterReconciler) SetupWithManager(manager ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(manager).
		For(&pgshardv1alpha1.PgShardCluster{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&appsv1.Deployment{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Named("pgshardcluster").
		Complete(r)
}
