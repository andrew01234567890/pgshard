// Package controller contains Kubernetes reconcilers for pgshard APIs.
package controller

import (
	"context"
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
	retryDelay                   = 15 * time.Second
)

// PgShardClusterReconciler owns safe supporting resources while failing closed
// on the unavailable PostgreSQL lifecycle. Ready is never inferred merely from
// desired objects existing; supporting availability comes from workload status.
type PgShardClusterReconciler struct {
	client.Client
	// APIReader bypasses the informer cache for deletion-finalizer absence
	// proofs. Writes and ordinary reconciliation continue through Client.
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
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
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
	if !state.exists {
		create := desired.DeepCopyObject().(client.Object)
		if deployment, ok := create.(*appsv1.Deployment); ok && deployment.Name == cluster.Name+owned.PoolerSuffix && deployment.Spec.Replicas == nil {
			replicas := poolerMinimum(cluster)
			deployment.Spec.Replicas = &replicas
		}
		if err := r.Create(ctx, create); err != nil {
			if apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("resource appeared after ownership preflight; refusing to adopt it")
			}
			return err
		}
		return nil
	}

	gvk, err := objectGVK(desired)
	if err != nil {
		return err
	}
	if deployment, ok := desired.(*appsv1.Deployment); ok && deployment.Name == cluster.Name+owned.PoolerSuffix && deployment.Spec.Replicas == nil && state.object.GetAnnotations()[owned.ScaleOwnerAnnotation] != "true" {
		handoff := deployment.DeepCopy()
		handoff.GetObjectKind().SetGroupVersionKind(gvk)
		handoff.UID = state.object.GetUID()
		current := state.object.(*appsv1.Deployment)
		if current.Spec.Replicas != nil {
			replicas := *current.Spec.Replicas
			handoff.Spec.Replicas = &replicas
		} else {
			replicas := poolerMinimum(cluster)
			handoff.Spec.Replicas = &replicas
		}
		if err := r.Patch(ctx, handoff, client.Apply, client.FieldOwner(hpaScaleFieldManager), client.ForceOwnership); err != nil {
			return fmt.Errorf("hand off pooler replicas to HPA: %w", err)
		}
	}
	if desired, ok := desired.(*networkingv1.NetworkPolicy); ok {
		current := state.object.(*networkingv1.NetworkPolicy).DeepCopy()
		current.Labels = maps.Clone(desired.Labels)
		current.Annotations = maps.Clone(desired.Annotations)
		current.OwnerReferences = append([]metav1.OwnerReference(nil), desired.OwnerReferences...)
		current.Spec = *desired.Spec.DeepCopy()
		return r.Update(ctx, current)
	}
	desired = desired.DeepCopyObject().(client.Object)
	desired.GetObjectKind().SetGroupVersionKind(gvk)
	desired.SetUID(state.object.GetUID())
	return r.Patch(ctx, desired, client.Apply, client.FieldOwner(owned.ManagedByValue), client.ForceOwnership)
}

func (r *PgShardClusterReconciler) ownedHPAForFixedTransition(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (*autoscalingv2.HorizontalPodAutoscaler, error) {
	if cluster.Spec.Pooler.Scaling.Mode != pgshardv1alpha1.ScalingFixed {
		return nil, nil
	}
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + owned.PoolerSuffix}
	if err := r.Get(ctx, key, hpa); apierrors.IsNotFound(err) {
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
	etcd := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + owned.EtcdSuffix}, etcd); err != nil {
		return false, "", err
	}
	orchestrator := &appsv1.Deployment{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + owned.OrchestratorSuffix}, orchestrator); err != nil {
		return false, "", err
	}
	pooler := &appsv1.Deployment{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + owned.PoolerSuffix}, pooler); err != nil {
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
		if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + owned.PoolerSuffix}, hpa); err != nil {
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
