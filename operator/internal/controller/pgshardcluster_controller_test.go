package controller

import (
	"context"
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReconcileCreatesOwnedPlanAndReportsTruthfulStatus(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	fakeClient := newFakeClient(t, cluster)
	reconciler := &PgShardClusterReconciler{Client: fakeClient}
	request := requestFor(cluster)
	result, err := reconciler.Reconcile(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != retryDelay {
		t.Fatalf("requeue = %#v", result)
	}

	for _, name := range []string{"example-rw", "example-ro", "example-r", "example-shard-0000", "example-shard-0001", "example-etcd", "example-orchestrator"} {
		service := &corev1.Service{}
		if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, service); err != nil {
			t.Fatalf("get Service %s: %v", name, err)
		}
		assertControllerOwner(t, service, cluster)
	}
	for name, target := range map[string]string{"example-rw": "pooler-rw", "example-ro": "pooler-ro", "example-r": "pooler-r"} {
		service := &corev1.Service{}
		if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, service); err != nil {
			t.Fatal(err)
		}
		if service.Spec.Ports[0].TargetPort.StrVal != target {
			t.Fatalf("%s target port = %#v", name, service.Spec.Ports[0].TargetPort)
		}
	}
	for _, object := range []client.Object{
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "example-postgresql-config", Namespace: cluster.Namespace}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "example-topology", Namespace: cluster.Namespace}},
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "example-etcd", Namespace: cluster.Namespace}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "example-orchestrator", Namespace: cluster.Namespace}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "example-pooler", Namespace: cluster.Namespace}},
		&autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: "example-pooler", Namespace: cluster.Namespace}},
		&networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: "example-etcd", Namespace: cluster.Namespace}},
		&policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: "example-pooler", Namespace: cluster.Namespace}},
	} {
		key := client.ObjectKeyFromObject(object)
		if err := fakeClient.Get(ctx, key, object); err != nil {
			t.Fatalf("get %T %s: %v", object, key, err)
		}
		assertControllerOwner(t, object, cluster)
	}

	got := getCluster(t, ctx, fakeClient, cluster)
	if got.Status.Phase != "Reconciling" || got.Status.ObservedGeneration != cluster.Generation {
		t.Fatalf("status = %#v", got.Status)
	}
	assertCondition(t, got, reconciledCondition, metav1.ConditionTrue, "ResourcesApplied")
	assertCondition(t, got, supportingAvailableCondition, metav1.ConditionFalse, "SupportingWorkloadsProgressing")
	assertCondition(t, got, readyCondition, metav1.ConditionFalse, "PostgreSQLLifecycleUnavailable")
	assertCondition(t, got, transportSecurityCondition, metav1.ConditionFalse, "EtcdTLSUnavailable")

	// A steady-state reconcile must preserve condition transition times.
	transition := meta.FindStatusCondition(got.Status.Conditions, readyCondition).LastTransitionTime
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	got = getCluster(t, ctx, fakeClient, cluster)
	if !meta.FindStatusCondition(got.Status.Conditions, readyCondition).LastTransitionTime.Equal(&transition) {
		t.Fatal("steady-state reconcile changed the Ready transition time")
	}
}

func TestReconcileObservesSupportingAvailabilityWithoutClaimingDatabaseReady(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	fakeClient := newFakeClient(t, cluster)
	reconciler := &PgShardClusterReconciler{Client: fakeClient}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}

	etcd := &appsv1.StatefulSet{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: "example-etcd"}
	if err := fakeClient.Get(ctx, key, etcd); err != nil {
		t.Fatal(err)
	}
	etcd.Status.ObservedGeneration = etcd.Generation
	etcd.Status.ReadyReplicas = 3
	etcd.Status.UpdatedReplicas = 3
	if err := fakeClient.Status().Update(ctx, etcd); err != nil {
		t.Fatal(err)
	}
	for name, replicas := range map[string]int32{"example-orchestrator": 3, "example-pooler": 2} {
		deployment := &appsv1.Deployment{}
		if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, deployment); err != nil {
			t.Fatal(err)
		}
		deployment.Status.ObservedGeneration = deployment.Generation
		deployment.Status.AvailableReplicas = replicas
		deployment.Status.UpdatedReplicas = replicas
		if err := fakeClient.Status().Update(ctx, deployment); err != nil {
			t.Fatal(err)
		}
	}
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: "example-pooler"}, hpa); err != nil {
		t.Fatal(err)
	}
	hpa.Status.ObservedGeneration = &hpa.Generation
	hpa.Status.CurrentReplicas = 2
	hpa.Status.DesiredReplicas = 2
	hpa.Status.Conditions = []autoscalingv2.HorizontalPodAutoscalerCondition{
		{Type: autoscalingv2.AbleToScale, Status: corev1.ConditionTrue},
		{Type: autoscalingv2.ScalingActive, Status: corev1.ConditionTrue},
	}
	if err := fakeClient.Status().Update(ctx, hpa); err != nil {
		t.Fatal(err)
	}

	result, err := reconciler.Reconcile(ctx, requestFor(cluster))
	if err != nil {
		t.Fatal(err)
	}
	if result != (ctrl.Result{}) {
		t.Fatalf("result = %#v", result)
	}
	got := getCluster(t, ctx, fakeClient, cluster)
	if got.Status.Phase != "Pending" {
		t.Fatalf("phase = %q", got.Status.Phase)
	}
	assertCondition(t, got, supportingAvailableCondition, metav1.ConditionTrue, "SupportingWorkloadsAvailable")
	assertCondition(t, got, readyCondition, metav1.ConditionFalse, "PostgreSQLLifecycleUnavailable")
	assertCondition(t, got, transportSecurityCondition, metav1.ConditionFalse, "EtcdTLSUnavailable")
}

func TestReconcilePrunesResourcesRemovedByUpdate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	fakeClient := newFakeClient(t, cluster)
	reconciler := &PgShardClusterReconciler{Client: fakeClient}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	driftedHPA := &autoscalingv2.HorizontalPodAutoscaler{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: "example-pooler"}, driftedHPA); err != nil {
		t.Fatal(err)
	}
	driftedHPA.Labels = map[string]string{"changed-by": "someone-else"}
	if err := fakeClient.Update(ctx, driftedHPA); err != nil {
		t.Fatal(err)
	}

	current := getCluster(t, ctx, fakeClient, cluster)
	current.Spec.Shards = 1
	current.Spec.Pooler.Scaling = pgshardv1alpha1.PoolerScaling{Mode: pgshardv1alpha1.ScalingFixed, Fixed: &pgshardv1alpha1.FixedScaling{Replicas: 4}}
	current.Generation = 8
	if err := fakeClient.Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}

	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: "example-pooler"}, &autoscalingv2.HorizontalPodAutoscaler{}); !apierrors.IsNotFound(err) {
		t.Fatalf("HPA was not removed before fixed scaling: %v", err)
	}
	transitioning := getCluster(t, ctx, fakeClient, cluster)
	assertCondition(t, transitioning, reconciledCondition, metav1.ConditionFalse, "PoolerScalingTransition")
	poolerDuringTransition := &appsv1.Deployment{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: "example-pooler"}, poolerDuringTransition); err != nil {
		t.Fatal(err)
	}
	if poolerDuringTransition.Spec.Replicas == nil || *poolerDuringTransition.Spec.Replicas == 4 {
		t.Fatalf("fixed replicas were claimed before HPA absence was observed: %#v", poolerDuringTransition.Spec.Replicas)
	}
	// The second pass observes that the HPA is gone before claiming replicas
	// and pruning the remaining stale plan.
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: "example-shard-0001"}, &corev1.Service{}); !apierrors.IsNotFound(err) {
		t.Fatalf("stale shard Service was not pruned after scaling handoff: %v", err)
	}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: "example-pooler"}, &autoscalingv2.HorizontalPodAutoscaler{}); !apierrors.IsNotFound(err) {
		t.Fatalf("stale HPA was not pruned: %v", err)
	}
	pooler := &appsv1.Deployment{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: "example-pooler"}, pooler); err != nil {
		t.Fatal(err)
	}
	if pooler.Spec.Replicas == nil || *pooler.Spec.Replicas != 4 {
		t.Fatalf("fixed pooler replicas = %#v", pooler.Spec.Replicas)
	}
}

func TestFixedToHPAHandoffPreservesCurrentCapacity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Pooler.Scaling = pgshardv1alpha1.PoolerScaling{Mode: pgshardv1alpha1.ScalingFixed, Fixed: &pgshardv1alpha1.FixedScaling{Replicas: 7}}
	fakeClient := newFakeClient(t, cluster)
	reconciler := &PgShardClusterReconciler{Client: fakeClient}
	request := requestFor(cluster)
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}

	current := getCluster(t, ctx, fakeClient, cluster)
	current.Spec.Pooler.Scaling = pgshardv1alpha1.PoolerScaling{Mode: pgshardv1alpha1.ScalingHPA, HPA: &pgshardv1alpha1.HPAScaling{MinReplicas: 2, MaxReplicas: 10, TargetCPUUtilizationPercentage: 65}}
	current.Generation++
	if err := fakeClient.Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	pooler := &appsv1.Deployment{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: "example-pooler"}, pooler); err != nil {
		t.Fatal(err)
	}
	if pooler.Spec.Replicas == nil || *pooler.Spec.Replicas != 7 {
		t.Fatalf("fixed-to-HPA handoff dropped capacity: %#v", pooler.Spec.Replicas)
	}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: "example-pooler"}, &autoscalingv2.HorizontalPodAutoscaler{}); err != nil {
		t.Fatalf("HPA was not created after scale ownership handoff: %v", err)
	}
}

func TestReconcileRefusesToAdoptDeterministicNameCollision(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	collision := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "example-topology", Namespace: cluster.Namespace},
		Data:       map[string]string{"belongs-to": "another-controller"},
	}
	fakeClient := newFakeClient(t, cluster, collision)
	reconciler := &PgShardClusterReconciler{Client: fakeClient}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err == nil {
		t.Fatal("expected resource collision to fail reconciliation")
	}
	got := &corev1.ConfigMap{}
	if err := fakeClient.Get(ctx, client.ObjectKeyFromObject(collision), got); err != nil {
		t.Fatal(err)
	}
	if got.Data["belongs-to"] != "another-controller" || len(got.OwnerReferences) != 0 {
		t.Fatalf("colliding object was adopted or overwritten: %#v", got)
	}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: "example-postgresql-config"}, &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
		t.Fatalf("plan wrote an earlier artifact before discovering the collision: %v", err)
	}
	status := getCluster(t, ctx, fakeClient, cluster)
	assertCondition(t, status, reconciledCondition, metav1.ConditionFalse, "ReconcileFailed")
	if contains(status.Finalizers, resourceFinalizer) {
		t.Fatalf("collision-only plan acquired a cleanup finalizer: %#v", status.Finalizers)
	}
}

func TestReconcileLeavesHPAOwnedReplicasAndServiceAllocationsAlone(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	fakeClient := newFakeClient(t, cluster)
	reconciler := &PgShardClusterReconciler{Client: fakeClient}
	request := requestFor(cluster)
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}

	pooler := &appsv1.Deployment{}
	poolerKey := types.NamespacedName{Namespace: cluster.Namespace, Name: "example-pooler"}
	if err := fakeClient.Get(ctx, poolerKey, pooler); err != nil {
		t.Fatal(err)
	}
	replicas := int32(7)
	pooler.Spec.Replicas = &replicas
	if err := fakeClient.Update(ctx, pooler); err != nil {
		t.Fatal(err)
	}
	service := &corev1.Service{}
	serviceKey := types.NamespacedName{Namespace: cluster.Namespace, Name: "example-rw"}
	if err := fakeClient.Get(ctx, serviceKey, service); err != nil {
		t.Fatal(err)
	}
	service.Spec.ClusterIP = "10.96.0.42"
	service.Spec.ClusterIPs = []string{"10.96.0.42"}
	if err := fakeClient.Update(ctx, service); err != nil {
		t.Fatal(err)
	}

	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	if err := fakeClient.Get(ctx, poolerKey, pooler); err != nil {
		t.Fatal(err)
	}
	if pooler.Spec.Replicas == nil || *pooler.Spec.Replicas != replicas {
		t.Fatalf("reconcile fought the HPA replica field: %#v", pooler.Spec.Replicas)
	}
	if err := fakeClient.Get(ctx, serviceKey, service); err != nil {
		t.Fatal(err)
	}
	if service.Spec.ClusterIP != "10.96.0.42" || len(service.Spec.ClusterIPs) != 1 || service.Spec.ClusterIPs[0] != "10.96.0.42" {
		t.Fatalf("reconcile cleared Service allocations: %#v", service.Spec)
	}
}

func TestPruneNeverDeletesMerelyLabelMatchedObjects(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	unowned := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name:      "someone-elses-config",
		Namespace: cluster.Namespace,
		Labels: map[string]string{
			owned.ManagedByLabel: owned.ManagedByValue,
			owned.ClusterLabel:   cluster.Name,
		},
	}}
	fakeClient := newFakeClient(t, cluster, unowned)
	reconciler := &PgShardClusterReconciler{Client: fakeClient}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	if err := fakeClient.Get(ctx, client.ObjectKeyFromObject(unowned), &corev1.ConfigMap{}); err != nil {
		t.Fatalf("unowned label-matched object was deleted: %v", err)
	}
}

func TestDeletionFinalizerPrunesOwnedResources(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	fakeClient := newFakeClient(t, cluster)
	reconciler := &PgShardClusterReconciler{Client: fakeClient}
	request := requestFor(cluster)
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	current := getCluster(t, ctx, fakeClient, cluster)
	if !contains(current.Finalizers, resourceFinalizer) {
		t.Fatalf("finalizers = %#v", current.Finalizers)
	}
	controller := true
	blockDeletion := true
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:      "data-example-etcd-0",
		Namespace: cluster.Namespace,
		UID:       types.UID("old-pvc-uid"),
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: pgshardv1alpha1.GroupVersion.String(), Kind: "PgShardCluster",
			Name: cluster.Name, UID: cluster.UID, Controller: &controller, BlockOwnerDeletion: &blockDeletion,
		}},
	}}
	if err := fakeClient.Create(ctx, pvc); err != nil {
		t.Fatal(err)
	}
	if err := fakeClient.Delete(ctx, current); err != nil {
		t.Fatal(err)
	}
	result, err := reconciler.Reconcile(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != retryDelay {
		t.Fatalf("deletion did not wait for observed child absence: %#v", result)
	}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: "example-etcd"}, &appsv1.StatefulSet{}); !apierrors.IsNotFound(err) {
		t.Fatalf("owned StatefulSet survived finalization: %v", err)
	}
	if err := fakeClient.Get(ctx, client.ObjectKeyFromObject(pvc), &corev1.PersistentVolumeClaim{}); !apierrors.IsNotFound(err) {
		t.Fatalf("owned PVC survived supervised cleanup: %v", err)
	}
	deleting := getCluster(t, ctx, fakeClient, cluster)
	if !contains(deleting.Finalizers, resourceFinalizer) {
		t.Fatal("cleanup finalizer was removed before absence was observed")
	}
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	if err := fakeClient.Get(ctx, request.NamespacedName, &pgshardv1alpha1.PgShardCluster{}); !apierrors.IsNotFound(err) {
		t.Fatalf("cluster still exists after finalizer removal: %v", err)
	}

	replacement := validCluster()
	replacement.UID = types.UID("replacement-uid")
	if err := fakeClient.Create(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(ctx, requestFor(replacement)); err != nil {
		t.Fatal(err)
	}
	recreated := &appsv1.StatefulSet{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: replacement.Namespace, Name: "example-etcd"}, recreated); err != nil {
		t.Fatal(err)
	}
	assertControllerOwner(t, recreated, replacement)
}

func TestReconcileReportsPlanFailureWithoutAdvancingObservedGeneration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	fakeClient := newFakeClient(t, cluster)
	reconciler := &PgShardClusterReconciler{Client: fakeClient, Images: owned.Images{Etcd: "etcd-only"}}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err == nil {
		t.Fatal("expected planning failure")
	}
	got := getCluster(t, ctx, fakeClient, cluster)
	if got.Status.Phase != "Degraded" || got.Status.ObservedGeneration != 0 {
		t.Fatalf("status = %#v", got.Status)
	}
	if contains(got.Finalizers, resourceFinalizer) {
		t.Fatalf("invalid plan acquired cleanup finalizer without children: %#v", got.Finalizers)
	}
	assertCondition(t, got, reconciledCondition, metav1.ConditionFalse, "PlanInvalid")
	assertCondition(t, got, readyCondition, metav1.ConditionFalse, "PlanInvalid")
	assertCondition(t, got, transportSecurityCondition, metav1.ConditionUnknown, "TransportSecurityUnobserved")
}

func validCluster() *pgshardv1alpha1.PgShardCluster {
	prometheus := true
	return &pgshardv1alpha1.PgShardCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default", UID: types.UID("example-uid"), Generation: 7},
		Spec: pgshardv1alpha1.PgShardClusterSpec{
			Shards:          2,
			MembersPerShard: 3,
			Durability:      pgshardv1alpha1.DurabilitySynchronous,
			PostgreSQL: pgshardv1alpha1.PostgreSQLSpec{
				Version: pgshardv1alpha1.PostgreSQLMajor18,
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("2Gi")},
					Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")},
				},
			},
			Storage: pgshardv1alpha1.StorageSpec{Size: resource.MustParse("10Gi")},
			Pooler: pgshardv1alpha1.PoolerSpec{Scaling: pgshardv1alpha1.PoolerScaling{Mode: pgshardv1alpha1.ScalingHPA, HPA: &pgshardv1alpha1.HPAScaling{
				MinReplicas: 2, MaxReplicas: 10, TargetCPUUtilizationPercentage: 65,
			}}},
			Services: pgshardv1alpha1.ServiceSet{
				ReadWrite: pgshardv1alpha1.ServiceTemplate{Type: corev1.ServiceTypeClusterIP},
				ReadOnly:  pgshardv1alpha1.ServiceTemplate{Type: corev1.ServiceTypeClusterIP},
				Read:      pgshardv1alpha1.ServiceTemplate{Type: corev1.ServiceTypeClusterIP},
			},
			Backup: pgshardv1alpha1.BackupSpec{Repository: pgshardv1alpha1.BackupRepository{
				Type:       pgshardv1alpha1.RepositoryFilesystem,
				Filesystem: &pgshardv1alpha1.FilesystemRepository{PersistentVolumeClaimName: "backups"},
			}},
			Observability: pgshardv1alpha1.ObservabilitySpec{Prometheus: &prometheus},
		},
	}
}

func newFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := pgshardv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithReturnManagedFields().
		WithStatusSubresource(&pgshardv1alpha1.PgShardCluster{}, &appsv1.Deployment{}, &appsv1.StatefulSet{}, &autoscalingv2.HorizontalPodAutoscaler{}, &policyv1.PodDisruptionBudget{}).
		WithObjects(objects...).
		Build()
}

func requestFor(cluster *pgshardv1alpha1.PgShardCluster) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}}
}

func getCluster(t *testing.T, ctx context.Context, fakeClient client.Client, cluster *pgshardv1alpha1.PgShardCluster) *pgshardv1alpha1.PgShardCluster {
	t.Helper()
	got := &pgshardv1alpha1.PgShardCluster{}
	if err := fakeClient.Get(ctx, client.ObjectKeyFromObject(cluster), got); err != nil {
		t.Fatal(err)
	}
	return got
}

func assertCondition(t *testing.T, cluster *pgshardv1alpha1.PgShardCluster, conditionType string, status metav1.ConditionStatus, reason string) {
	t.Helper()
	condition := meta.FindStatusCondition(cluster.Status.Conditions, conditionType)
	if condition == nil || condition.Status != status || condition.Reason != reason {
		t.Fatalf("condition %s = %#v; all conditions = %#v", conditionType, condition, cluster.Status.Conditions)
	}
}

func assertControllerOwner(t *testing.T, object client.Object, cluster *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	if !metav1.IsControlledBy(object, cluster) {
		t.Fatalf("%T/%s is not controlled by %s: %#v", object, object.GetName(), cluster.Name, object.GetOwnerReferences())
	}
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
