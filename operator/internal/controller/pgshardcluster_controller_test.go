package controller

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utiluuid "k8s.io/apimachinery/pkg/util/uuid"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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

	for _, name := range []string{"example-rw", "example-ro", "example-r", "example-shard-0000", "example-shard-0001", "example-etcd", "example-orchestrator", "example-pooler"} {
		service := &corev1.Service{}
		if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, service); err != nil {
			t.Fatalf("get Service %s: %v", name, err)
		}
		assertControllerOwner(t, service, cluster)
	}
	for name, expected := range map[string]struct {
		port   int32
		target string
	}{
		"example-rw":     {port: owned.PostgreSQLPort, target: "pooler-rw"},
		"example-ro":     {port: owned.PostgreSQLPort, target: "pooler-ro"},
		"example-r":      {port: owned.PostgreSQLPort, target: "pooler-r"},
		"example-pooler": {port: owned.HTTPPort, target: "http"},
	} {
		service := &corev1.Service{}
		if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, service); err != nil {
			t.Fatal(err)
		}
		if service.Spec.Ports[0].Port != expected.port || service.Spec.Ports[0].TargetPort.StrVal != expected.target {
			t.Fatalf("%s port = %#v", name, service.Spec.Ports[0])
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
	assertCondition(t, got, postgresqlAvailableCondition, metav1.ConditionFalse, "PostgreSQLHAUnavailable")
	assertCondition(t, got, readyCondition, metav1.ConditionFalse, "PostgreSQLHAUnavailable")
	assertCondition(t, got, transportSecurityCondition, metav1.ConditionFalse, "TransportTLSUnavailable")

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

func TestReconcileCreatesSingleMemberPrimariesWithPerShardImmutableCredentials(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	fakeClient := newFakeClient(t, cluster)
	reconciler := &PgShardClusterReconciler{Client: fakeClient}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}

	passwords := make(map[int32]string, cluster.Spec.Shards)
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		secret := &corev1.Secret{}
		secretKey := types.NamespacedName{Namespace: cluster.Namespace, Name: owned.PostgreSQLAuthSecretName(cluster.Name, shard)}
		if err := fakeClient.Get(ctx, secretKey, secret); err != nil {
			t.Fatal(err)
		}
		if err := validatePostgreSQLAuthSecret(secret, cluster, shard); err != nil {
			t.Fatalf("generated credential for shard %d is invalid: %v", shard, err)
		}
		if len(secret.Data[owned.PostgreSQLPasswordKey]) != hex.EncodedLen(postgresqlPasswordBytes) {
			t.Fatalf("generated password length for shard %d = %d", shard, len(secret.Data[owned.PostgreSQLPasswordKey]))
		}
		passwords[shard] = string(secret.Data[owned.PostgreSQLPasswordKey])
		statefulSet := &appsv1.StatefulSet{}
		name := owned.PostgreSQLPrimaryStatefulSetName(cluster.Name, shard)
		if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, statefulSet); err != nil {
			t.Fatalf("get PostgreSQL StatefulSet %s: %v", name, err)
		}
		assertControllerOwner(t, statefulSet, cluster)
		statefulSet.Status.ObservedGeneration = statefulSet.Generation
		statefulSet.Status.ReadyReplicas = 1
		statefulSet.Status.UpdatedReplicas = 1
		if err := fakeClient.Status().Update(ctx, statefulSet); err != nil {
			t.Fatalf("update PostgreSQL StatefulSet %s status: %v", name, err)
		}
	}
	if passwords[0] == passwords[1] {
		t.Fatal("different shards received the same PostgreSQL superuser credential")
	}

	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	got := getCluster(t, ctx, fakeClient, cluster)
	assertCondition(t, got, postgresqlAvailableCondition, metav1.ConditionFalse, "PostgreSQLPrimariesProgressing")
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		unchanged := &corev1.Secret{}
		secretKey := types.NamespacedName{Namespace: cluster.Namespace, Name: owned.PostgreSQLAuthSecretName(cluster.Name, shard)}
		if err := fakeClient.Get(ctx, secretKey, unchanged); err != nil {
			t.Fatal(err)
		}
		if string(unchanged.Data[owned.PostgreSQLPasswordKey]) != passwords[shard] {
			t.Fatalf("steady-state reconciliation rotated shard %d PostgreSQL credential", shard)
		}
		statefulSet := &appsv1.StatefulSet{}
		name := owned.PostgreSQLPrimaryStatefulSetName(cluster.Name, shard)
		if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, statefulSet); err != nil {
			t.Fatal(err)
		}
		statefulSet.Status.AvailableReplicas = 1
		if err := fakeClient.Status().Update(ctx, statefulSet); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	got = getCluster(t, ctx, fakeClient, cluster)
	assertCondition(t, got, postgresqlAvailableCondition, metav1.ConditionTrue, "SingleMemberPrimariesAvailable")
	assertCondition(t, got, readyCondition, metav1.ConditionFalse, "DataPlaneUnavailable")
	if len(got.Status.PostgreSQLCredentials) != int(cluster.Spec.Shards) {
		t.Fatalf("recorded PostgreSQL credentials = %#v", got.Status.PostgreSQLCredentials)
	}
}

func TestReconcileRefusesToReplaceMissingCredentialAfterWorkloadCreation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	fakeClient := newFakeClient(t, cluster)
	reconciler := &PgShardClusterReconciler{Client: fakeClient}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: owned.PostgreSQLAuthSecretName(cluster.Name, 0)}
	if err := fakeClient.Get(ctx, key, secret); err != nil {
		t.Fatal(err)
	}
	if err := fakeClient.Delete(ctx, secret); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err == nil || !strings.Contains(err.Error(), "recorded UID") || !strings.Contains(err.Error(), "automatic replacement is unsafe") {
		t.Fatalf("missing credential was not fenced: %v", err)
	}
	if err := fakeClient.Get(ctx, key, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("missing credential was recreated: %v", err)
	}
}

func TestReconcileRejectsUnrecordedCredentialAndRetainedPGDATA(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for name, object := range map[string]client.Object{
		"precreated Secret": func() client.Object {
			cluster := validCluster()
			secret := owned.PostgreSQLAuthSecret(cluster, 0, []byte(strings.Repeat("a", hex.EncodedLen(postgresqlPasswordBytes))))
			secret.UID = "attacker-selected-secret"
			return secret
		}(),
		"retained PGDATA": &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: owned.PostgreSQLPrimaryDataPVCName("example", 0), Namespace: "default"}},
	} {
		name, object := name, object
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cluster := validCluster()
			cluster.Spec.MembersPerShard = 1
			cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
			fakeClient := newFakeClient(t, cluster, object)
			reconciler := &PgShardClusterReconciler{Client: fakeClient}
			_, err := reconciler.Reconcile(ctx, requestFor(cluster))
			if err == nil || (name == "precreated Secret" && !strings.Contains(err.Error(), "refusing to adopt")) || (name == "retained PGDATA" && !strings.Contains(err.Error(), "automatic replacement is unsafe")) {
				t.Fatalf("unsafe credential history was accepted: %v", err)
			}
			statefulSet := &appsv1.StatefulSet{}
			key := types.NamespacedName{Namespace: cluster.Namespace, Name: owned.PostgreSQLPrimaryStatefulSetName(cluster.Name, 0)}
			if getErr := fakeClient.Get(ctx, key, statefulSet); !apierrors.IsNotFound(getErr) {
				t.Fatalf("unsafe credential history created a workload: %v", getErr)
			}
		})
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
	assertCondition(t, got, postgresqlAvailableCondition, metav1.ConditionFalse, "PostgreSQLHAUnavailable")
	assertCondition(t, got, readyCondition, metav1.ConditionFalse, "PostgreSQLHAUnavailable")
	assertCondition(t, got, transportSecurityCondition, metav1.ConditionFalse, "TransportTLSUnavailable")
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

func TestHPAHandoffUsesAuthoritativeReplicas(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	desired := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name:      cluster.Name + owned.PoolerSuffix,
		Namespace: cluster.Namespace,
	}}
	currentReplicas := int32(7)
	latestReplicas := int32(9)
	authoritativePooler := desired.DeepCopy()
	authoritativePooler.UID = types.UID("pooler-uid")
	authoritativePooler.ResourceVersion = "42"
	authoritativePooler.Spec.Replicas = &currentReplicas
	reads := 0
	authoritative := interceptedClient(t, newFakeClient(t), interceptor.Funcs{
		Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, object client.Object, _ ...client.GetOption) error {
			reads++
			source := authoritativePooler.DeepCopy()
			if reads > 1 {
				source.ResourceVersion = "43"
				source.Spec.Replicas = &latestReplicas
			}
			target, ok := object.(*appsv1.Deployment)
			if !ok {
				t.Fatalf("authoritative destination type = %T", object)
			}
			*target = *source
			return nil
		},
	})

	var applied *unstructured.Unstructured
	var options client.PatchOptions
	patches := 0
	writeClient := interceptedClient(t, newFakeClient(t), interceptor.Funcs{
		Patch: func(_ context.Context, _ client.WithWatch, object client.Object, patch client.Patch, opts ...client.PatchOption) error {
			patches++
			if patch.Type() != types.ApplyPatchType {
				t.Fatalf("patch type = %q, want apply", patch.Type())
			}
			if patches == 1 {
				return apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, object.GetName(), errors.New("injected conflict"))
			}
			var ok bool
			applied, ok = object.DeepCopyObject().(*unstructured.Unstructured)
			if !ok {
				t.Fatalf("handoff object type = %T", object)
			}
			options.ApplyOptions(opts)
			return nil
		},
	})
	reconciler := &PgShardClusterReconciler{Client: writeClient, APIReader: authoritative}
	if err := reconciler.handoffPoolerReplicas(
		ctx,
		cluster,
		desired,
		authoritativePooler.UID,
		appsv1.SchemeGroupVersion.WithKind("Deployment"),
	); err != nil {
		t.Fatal(err)
	}
	if applied == nil {
		t.Fatal("HPA handoff did not apply replicas")
	}
	replicas, found, err := unstructured.NestedInt64(applied.Object, "spec", "replicas")
	if err != nil || !found || replicas != int64(latestReplicas) {
		t.Fatalf("applied replicas = %d, found %t, error %v", replicas, found, err)
	}
	if applied.GetUID() != authoritativePooler.UID || applied.GetResourceVersion() != "43" {
		t.Fatalf("handoff preconditions = UID %q RV %q", applied.GetUID(), applied.GetResourceVersion())
	}
	if reads != 2 || patches != 2 {
		t.Fatalf("handoff attempts = %d reads, %d patches; want 2 each", reads, patches)
	}
	if options.FieldManager != hpaScaleFieldManager || options.Force == nil || !*options.Force {
		t.Fatalf("handoff patch options = %#v", options)
	}
}

func TestHPAHandoffCanonicalizesLegacyWholeDeploymentOwnership(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	desired := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name:      cluster.Name + owned.PoolerSuffix,
		Namespace: cluster.Namespace,
	}}
	replicas := int32(7)
	current := desired.DeepCopy()
	current.UID = types.UID("pooler-uid")
	current.ResourceVersion = "42"
	current.Spec.Replicas = &replicas
	current.ManagedFields = []metav1.ManagedFieldsEntry{{
		Manager:    hpaScaleFieldManager,
		Operation:  metav1.ManagedFieldsOperationApply,
		APIVersion: "apps/v1",
		FieldsType: "FieldsV1",
		FieldsV1:   &metav1.FieldsV1{Raw: []byte(`{"f:metadata":{"f:labels":{"f:pgshard.io/cluster":{}}},"f:spec":{"f:replicas":{},"f:template":{"f:spec":{"f:containers":{}}}}}`)},
	}}
	if hasExactReplicaApplyOwnership(current, hpaScaleFieldManager) {
		t.Fatal("legacy whole-Deployment field set was classified as replicas-only")
	}

	patches := 0
	writeClient := interceptedClient(t, newFakeClient(t), interceptor.Funcs{
		Patch: func(_ context.Context, _ client.WithWatch, object client.Object, patch client.Patch, _ ...client.PatchOption) error {
			patches++
			if patch.Type() != types.ApplyPatchType {
				t.Fatalf("patch type = %q, want apply", patch.Type())
			}
			return nil
		},
	})
	reconciler := &PgShardClusterReconciler{Client: writeClient, APIReader: newFakeClient(t, current)}
	if err := reconciler.handoffPoolerReplicas(
		context.Background(),
		cluster,
		desired,
		current.UID,
		appsv1.SchemeGroupVersion.WithKind("Deployment"),
	); err != nil {
		t.Fatal(err)
	}
	if patches != 1 {
		t.Fatalf("canonicalization patches = %d, want 1", patches)
	}
}

func TestExactReplicaApplyOwnership(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		entries []metav1.ManagedFieldsEntry
		want    bool
	}{
		{
			name: "exact",
			entries: []metav1.ManagedFieldsEntry{{
				Manager: hpaScaleFieldManager, Operation: metav1.ManagedFieldsOperationApply, FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:replicas":{}}}`)},
			}},
			want: true,
		},
		{
			name: "extra root field",
			entries: []metav1.ManagedFieldsEntry{{
				Manager: hpaScaleFieldManager, Operation: metav1.ManagedFieldsOperationApply, FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:metadata":{},"f:spec":{"f:replicas":{}}}`)},
			}},
		},
		{
			name: "extra spec field",
			entries: []metav1.ManagedFieldsEntry{{
				Manager: hpaScaleFieldManager, Operation: metav1.ManagedFieldsOperationApply, FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:replicas":{},"f:template":{}}}`)},
			}},
		},
		{
			name: "null leaf",
			entries: []metav1.ManagedFieldsEntry{{
				Manager: hpaScaleFieldManager, Operation: metav1.ManagedFieldsOperationApply, FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:replicas":null}}`)},
			}},
		},
		{
			name: "malformed",
			entries: []metav1.ManagedFieldsEntry{{
				Manager: hpaScaleFieldManager, Operation: metav1.ManagedFieldsOperationApply, FieldsV1: &metav1.FieldsV1{Raw: []byte(`{`)},
			}},
		},
		{
			name: "duplicate manager entries",
			entries: []metav1.ManagedFieldsEntry{
				{Manager: hpaScaleFieldManager, Operation: metav1.ManagedFieldsOperationApply, FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:replicas":{}}}`)}},
				{Manager: hpaScaleFieldManager, Operation: metav1.ManagedFieldsOperationApply, FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:replicas":{}}}`)}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			object := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{ManagedFields: test.entries}}
			if got := hasExactReplicaApplyOwnership(object, hpaScaleFieldManager); got != test.want {
				t.Fatalf("hasExactReplicaApplyOwnership() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestHPAHandoffRejectsReplacedDeployment(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	desired := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name:      cluster.Name + owned.PoolerSuffix,
		Namespace: cluster.Namespace,
	}}
	replacement := desired.DeepCopy()
	replacement.UID = types.UID("replacement-uid")
	authoritative := newFakeClient(t, replacement)
	reconciler := &PgShardClusterReconciler{Client: newFakeClient(t), APIReader: authoritative}
	err := reconciler.handoffPoolerReplicas(
		context.Background(),
		cluster,
		desired,
		types.UID("expected-uid"),
		appsv1.SchemeGroupVersion.WithKind("Deployment"),
	)
	if err == nil || !strings.Contains(err.Error(), "replaced") {
		t.Fatalf("replacement error = %v", err)
	}
}

func TestHPAHandoffBoundsConflicts(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	desired := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name:      cluster.Name + owned.PoolerSuffix,
		Namespace: cluster.Namespace,
	}}
	current := desired.DeepCopy()
	current.UID = types.UID("pooler-uid")
	current.ResourceVersion = "42"
	authoritative := newFakeClient(t, current)
	patches := 0
	writeClient := interceptedClient(t, newFakeClient(t), interceptor.Funcs{
		Patch: func(_ context.Context, _ client.WithWatch, object client.Object, _ client.Patch, _ ...client.PatchOption) error {
			patches++
			return apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, object.GetName(), errors.New("injected conflict"))
		},
	})
	reconciler := &PgShardClusterReconciler{Client: writeClient, APIReader: authoritative}
	err := reconciler.handoffPoolerReplicas(
		context.Background(),
		cluster,
		desired,
		current.UID,
		appsv1.SchemeGroupVersion.WithKind("Deployment"),
	)
	if err == nil || !strings.Contains(err.Error(), "after 4 conflicts") {
		t.Fatalf("conflict exhaustion error = %v", err)
	}
	if patches != 4 {
		t.Fatalf("patch attempts = %d, want 4", patches)
	}
}

func TestFixedScaleHandoffRelinquishesAuthoritativeHPAOwnership(t *testing.T) {
	t.Parallel()
	replicas := int32(7)
	desired := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "example-pooler", Namespace: "default"},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
	}
	current := desired.DeepCopy()
	current.UID = types.UID("pooler-uid")
	current.ResourceVersion = "42"
	current.ManagedFields = []metav1.ManagedFieldsEntry{
		replicaApplyOwner(owned.ManagedByValue),
		legacyHPAApplyOwner(),
	}
	reads := 0
	authoritative := interceptedClient(t, newFakeClient(t), interceptor.Funcs{
		Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, object client.Object, _ ...client.GetOption) error {
			reads++
			source := current.DeepCopy()
			if reads > 1 {
				source.ResourceVersion = "43"
			}
			*object.(*appsv1.Deployment) = *source
			return nil
		},
	})
	patches := 0
	var relinquished *unstructured.Unstructured
	var options client.PatchOptions
	writeClient := interceptedClient(t, newFakeClient(t), interceptor.Funcs{
		Patch: func(_ context.Context, _ client.WithWatch, object client.Object, patch client.Patch, opts ...client.PatchOption) error {
			patches++
			if patch.Type() != types.ApplyPatchType {
				t.Fatalf("patch type = %q, want apply", patch.Type())
			}
			if patches == 1 {
				return apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, object.GetName(), errors.New("injected conflict"))
			}
			relinquished = object.DeepCopyObject().(*unstructured.Unstructured)
			options.ApplyOptions(opts)
			return nil
		},
	})
	reconciler := &PgShardClusterReconciler{Client: writeClient, APIReader: authoritative}
	if err := reconciler.relinquishPoolerScaleOwnership(
		context.Background(), desired, current.UID, appsv1.SchemeGroupVersion.WithKind("Deployment"),
	); err != nil {
		t.Fatal(err)
	}
	if reads != 2 || patches != 2 {
		t.Fatalf("fixed-scale handoff attempts = %d reads, %d patches; want 2 each", reads, patches)
	}
	if relinquished == nil || relinquished.GetUID() != current.UID || relinquished.GetResourceVersion() != "43" {
		t.Fatalf("relinquish preconditions = %#v", relinquished)
	}
	if _, exists := relinquished.Object["spec"]; exists {
		t.Fatalf("relinquish Apply still claims spec: %#v", relinquished.Object)
	}
	if options.FieldManager != hpaScaleFieldManager || options.Force == nil || !*options.Force {
		t.Fatalf("relinquish patch options = %#v", options)
	}
}

func TestFixedScaleHandoffReclaimsLateScaleWriteBeforeRelinquishing(t *testing.T) {
	t.Parallel()
	desiredReplicas := int32(7)
	lateReplicas := int32(1)
	desired := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "example-pooler", Namespace: "default"},
		Spec:       appsv1.DeploymentSpec{Replicas: &desiredReplicas},
	}
	late := desired.DeepCopy()
	late.UID = types.UID("pooler-uid")
	late.ResourceVersion = "42"
	late.Spec.Replicas = &lateReplicas
	late.ManagedFields = []metav1.ManagedFieldsEntry{
		{
			Manager: owned.ManagedByValue, Operation: metav1.ManagedFieldsOperationApply,
			APIVersion: "apps/v1", FieldsType: "FieldsV1",
			FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:metadata":{"f:labels":{}}}`)},
		},
		legacyHPAApplyOwner(),
	}
	corrected := desired.DeepCopy()
	corrected.UID = late.UID
	corrected.ResourceVersion = "43"
	corrected.ManagedFields = []metav1.ManagedFieldsEntry{
		replicaApplyOwner(owned.ManagedByValue),
		legacyHPAApplyOwner(),
	}
	reads := 0
	authoritative := interceptedClient(t, newFakeClient(t), interceptor.Funcs{
		Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, object client.Object, _ ...client.GetOption) error {
			reads++
			source := late
			if reads > 1 {
				source = corrected
			}
			*object.(*appsv1.Deployment) = *source.DeepCopy()
			return nil
		},
	})
	patches := 0
	writeClient := interceptedClient(t, newFakeClient(t), interceptor.Funcs{
		Patch: func(_ context.Context, _ client.WithWatch, object client.Object, patch client.Patch, opts ...client.PatchOption) error {
			patches++
			if patch.Type() != types.ApplyPatchType {
				t.Fatalf("patch type = %q, want apply", patch.Type())
			}
			var options client.PatchOptions
			options.ApplyOptions(opts)
			switch patches {
			case 1:
				reclaim, ok := object.(*appsv1.Deployment)
				if !ok || reclaim.Spec.Replicas == nil || *reclaim.Spec.Replicas != desiredReplicas || reclaim.UID != late.UID || reclaim.ResourceVersion != "42" {
					t.Fatalf("fixed replica reclaim = %#v", object)
				}
				if options.FieldManager != owned.ManagedByValue || options.Force == nil || !*options.Force {
					t.Fatalf("fixed replica reclaim options = %#v", options)
				}
			case 2:
				relinquish, ok := object.(*unstructured.Unstructured)
				if !ok || relinquish.GetUID() != late.UID || relinquish.GetResourceVersion() != "43" {
					t.Fatalf("HPA relinquishment = %#v", object)
				}
				if _, exists := relinquish.Object["spec"]; exists {
					t.Fatalf("HPA relinquishment still claims spec: %#v", relinquish.Object)
				}
				if options.FieldManager != hpaScaleFieldManager || options.Force == nil || !*options.Force {
					t.Fatalf("HPA relinquishment options = %#v", options)
				}
			default:
				t.Fatalf("unexpected patch %d: %#v", patches, object)
			}
			return nil
		},
	})
	reconciler := &PgShardClusterReconciler{Client: writeClient, APIReader: authoritative}
	if err := reconciler.relinquishPoolerScaleOwnership(context.Background(), desired, late.UID, appsv1.SchemeGroupVersion.WithKind("Deployment")); err != nil {
		t.Fatal(err)
	}
	if reads != 2 || patches != 2 {
		t.Fatalf("late-write recovery = %d reads, %d patches; want 2 each", reads, patches)
	}
}

func TestFixedScaleHandoffReclaimsScaleWriteAfterRelinquishConflict(t *testing.T) {
	t.Parallel()
	desiredReplicas := int32(7)
	lateReplicas := int32(1)
	desired := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "example-pooler", Namespace: "default"},
		Spec:       appsv1.DeploymentSpec{Replicas: &desiredReplicas},
	}
	stable := desired.DeepCopy()
	stable.UID = types.UID("pooler-uid")
	stable.ResourceVersion = "42"
	stable.ManagedFields = []metav1.ManagedFieldsEntry{
		replicaApplyOwner(owned.ManagedByValue),
		legacyHPAApplyOwner(),
	}
	raced := stable.DeepCopy()
	raced.ResourceVersion = "43"
	raced.Spec.Replicas = &lateReplicas
	raced.ManagedFields = []metav1.ManagedFieldsEntry{
		{
			Manager: owned.ManagedByValue, Operation: metav1.ManagedFieldsOperationApply,
			APIVersion: "apps/v1", FieldsType: "FieldsV1",
			FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:metadata":{"f:labels":{}}}`)},
		},
		legacyHPAApplyOwner(),
	}
	corrected := stable.DeepCopy()
	corrected.ResourceVersion = "44"
	reads := 0
	authoritative := interceptedClient(t, newFakeClient(t), interceptor.Funcs{
		Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, object client.Object, _ ...client.GetOption) error {
			reads++
			source := stable
			if reads == 2 {
				source = raced
			} else if reads > 2 {
				source = corrected
			}
			*object.(*appsv1.Deployment) = *source.DeepCopy()
			return nil
		},
	})
	patches := 0
	writeClient := interceptedClient(t, newFakeClient(t), interceptor.Funcs{
		Patch: func(_ context.Context, _ client.WithWatch, object client.Object, _ client.Patch, opts ...client.PatchOption) error {
			patches++
			var options client.PatchOptions
			options.ApplyOptions(opts)
			switch patches {
			case 1:
				if options.FieldManager != hpaScaleFieldManager || object.GetResourceVersion() != "42" {
					t.Fatalf("first relinquishment = manager %q object %#v", options.FieldManager, object)
				}
				return apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, object.GetName(), errors.New("injected scale race"))
			case 2:
				reclaim, ok := object.(*appsv1.Deployment)
				if !ok || options.FieldManager != owned.ManagedByValue || reclaim.ResourceVersion != "43" || reclaim.Spec.Replicas == nil || *reclaim.Spec.Replicas != desiredReplicas {
					t.Fatalf("retry replica reclaim = manager %q object %#v", options.FieldManager, object)
				}
				return nil
			case 3:
				if options.FieldManager != hpaScaleFieldManager || object.GetResourceVersion() != "44" {
					t.Fatalf("final relinquishment = manager %q object %#v", options.FieldManager, object)
				}
				return nil
			default:
				t.Fatalf("unexpected patch %d: %#v", patches, object)
				return nil
			}
		},
	})
	reconciler := &PgShardClusterReconciler{Client: writeClient, APIReader: authoritative}
	if err := reconciler.relinquishPoolerScaleOwnership(context.Background(), desired, stable.UID, appsv1.SchemeGroupVersion.WithKind("Deployment")); err != nil {
		t.Fatal(err)
	}
	if reads != 3 || patches != 3 {
		t.Fatalf("conflict-race recovery = %d reads, %d patches; want 3 each", reads, patches)
	}
}

func TestFixedScaleHandoffRejectsReplacementAndBoundsConflicts(t *testing.T) {
	t.Parallel()
	replicas := int32(7)
	desired := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "example-pooler", Namespace: "default"},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
	}
	current := desired.DeepCopy()
	current.UID = types.UID("pooler-uid")
	current.ManagedFields = []metav1.ManagedFieldsEntry{
		replicaApplyOwner(owned.ManagedByValue),
		legacyHPAApplyOwner(),
	}

	replacement := current.DeepCopy()
	replacement.UID = types.UID("replacement-uid")
	reconciler := &PgShardClusterReconciler{Client: newFakeClient(t), APIReader: newFakeClient(t, replacement)}
	if err := reconciler.relinquishPoolerScaleOwnership(context.Background(), desired, current.UID, appsv1.SchemeGroupVersion.WithKind("Deployment")); err == nil || !strings.Contains(err.Error(), "replaced") {
		t.Fatalf("replacement error = %v", err)
	}

	patches := 0
	writeClient := interceptedClient(t, newFakeClient(t), interceptor.Funcs{
		Patch: func(_ context.Context, _ client.WithWatch, object client.Object, _ client.Patch, _ ...client.PatchOption) error {
			patches++
			return apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, object.GetName(), errors.New("injected conflict"))
		},
	})
	reconciler = &PgShardClusterReconciler{Client: writeClient, APIReader: newFakeClient(t, current)}
	err := reconciler.relinquishPoolerScaleOwnership(context.Background(), desired, current.UID, appsv1.SchemeGroupVersion.WithKind("Deployment"))
	if err == nil || !strings.Contains(err.Error(), "after 4 attempts") {
		t.Fatalf("conflict exhaustion error = %v", err)
	}
	if patches != 4 {
		t.Fatalf("relinquish attempts = %d, want 4", patches)
	}
}

func replicaApplyOwner(manager string) metav1.ManagedFieldsEntry {
	return metav1.ManagedFieldsEntry{
		Manager: manager, Operation: metav1.ManagedFieldsOperationApply,
		APIVersion: "apps/v1", FieldsType: "FieldsV1",
		FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:replicas":{}}}`)},
	}
}

func legacyHPAApplyOwner() metav1.ManagedFieldsEntry {
	return metav1.ManagedFieldsEntry{
		Manager: hpaScaleFieldManager, Operation: metav1.ManagedFieldsOperationApply,
		APIVersion: "apps/v1", FieldsType: "FieldsV1",
		FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:metadata":{"f:annotations":{"f:pgshard.io/hpa-scale-handed-off":{}}},"f:spec":{"f:replicas":{}}}`)},
	}
}

func TestLegacyAlignmentUsesAuthoritativeReplicas(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	staleReplicas := int32(2)
	currentReplicas := int32(7)
	latestReplicas := int32(9)
	stale := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "example-pooler", Namespace: "default", UID: types.UID("pooler-uid"), ResourceVersion: "40"},
		Spec:       appsv1.DeploymentSpec{Replicas: &staleReplicas},
	}
	authoritativePooler := stale.DeepCopy()
	authoritativePooler.ResourceVersion = "42"
	authoritativePooler.Spec.Replicas = &currentReplicas
	reads := 0
	authoritative := interceptedClient(t, newFakeClient(t), interceptor.Funcs{
		Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, object client.Object, _ ...client.GetOption) error {
			reads++
			source := authoritativePooler.DeepCopy()
			if reads > 1 {
				source.ResourceVersion = "43"
				source.Spec.Replicas = &latestReplicas
			}
			target, ok := object.(*appsv1.Deployment)
			if !ok {
				t.Fatalf("authoritative destination type = %T", object)
			}
			*target = *source
			return nil
		},
	})
	desired := stale.DeepCopy()
	desired.ResourceVersion = ""
	desired.Spec.Replicas = nil

	var updated *appsv1.Deployment
	updates := 0
	writeClient := interceptedClient(t, newFakeClient(t), interceptor.Funcs{
		Update: func(_ context.Context, _ client.WithWatch, object client.Object, _ ...client.UpdateOption) error {
			updates++
			if updates == 1 {
				return apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, object.GetName(), errors.New("injected conflict"))
			}
			var ok bool
			updated, ok = object.DeepCopyObject().(*appsv1.Deployment)
			if !ok {
				t.Fatalf("alignment object type = %T", object)
			}
			return nil
		},
	})
	reconciler := &PgShardClusterReconciler{Client: writeClient, APIReader: authoritative}
	aligned, err := reconciler.alignLegacyOwnedFields(ctx, stale, desired, true)
	if err != nil {
		t.Fatal(err)
	}
	if updated == nil || updated.Spec.Replicas == nil || *updated.Spec.Replicas != latestReplicas {
		t.Fatalf("legacy alignment replayed cached replicas: %#v", updated)
	}
	if aligned.GetResourceVersion() != "43" {
		t.Fatalf("aligned resource version = %q, want 43", aligned.GetResourceVersion())
	}
	if reads != 2 || updates != 2 {
		t.Fatalf("alignment attempts = %d reads, %d updates; want 2 each", reads, updates)
	}
}

func TestLegacyAlignmentReclassifiesAuthoritativeApplyOwnershipAfterConflict(t *testing.T) {
	t.Parallel()
	replicas := int32(7)
	stale := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "example-pooler", Namespace: "default", UID: types.UID("pooler-uid"), ResourceVersion: "40"},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
	}
	legacyHPAOwner := metav1.ManagedFieldsEntry{
		Manager:    hpaScaleFieldManager,
		Operation:  metav1.ManagedFieldsOperationApply,
		APIVersion: "apps/v1",
		FieldsType: "FieldsV1",
		FieldsV1:   &metav1.FieldsV1{Raw: []byte(`{"f:metadata":{"f:labels":{}},"f:spec":{"f:replicas":{}}}`)},
	}
	authoritativePooler := stale.DeepCopy()
	authoritativePooler.ResourceVersion = "42"
	authoritativePooler.ManagedFields = []metav1.ManagedFieldsEntry{legacyHPAOwner}
	reads := 0
	authoritative := interceptedClient(t, newFakeClient(t), interceptor.Funcs{
		Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, object client.Object, _ ...client.GetOption) error {
			reads++
			source := authoritativePooler.DeepCopy()
			if reads > 1 {
				source.ResourceVersion = "43"
				source.Annotations = map[string]string{owned.ApplyOwnershipAnnotation: owned.ApplyOwnershipVersion}
				source.ManagedFields = append(source.ManagedFields,
					metav1.ManagedFieldsEntry{
						Manager: owned.ManagedByValue, Operation: metav1.ManagedFieldsOperationApply,
						FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:metadata":{"f:annotations":{"f:pgshard.io/apply-ownership":{}}}}`)},
					},
					metav1.ManagedFieldsEntry{Manager: "external-manager", Operation: metav1.ManagedFieldsOperationApply},
				)
			}
			target := object.(*appsv1.Deployment)
			*target = *source
			return nil
		},
	})
	updates := 0
	writeClient := interceptedClient(t, newFakeClient(t), interceptor.Funcs{
		Update: func(_ context.Context, _ client.WithWatch, object client.Object, _ ...client.UpdateOption) error {
			updates++
			if updates > 1 {
				t.Fatal("authoritative Apply ownership was not reclassified before Update")
			}
			return apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, object.GetName(), errors.New("injected conflict"))
		},
	})
	desired := stale.DeepCopy()
	desired.ResourceVersion = ""
	desired.Spec.Replicas = nil
	desired.ManagedFields = nil
	reconciler := &PgShardClusterReconciler{Client: writeClient, APIReader: authoritative}
	aligned, err := reconciler.alignLegacyOwnedFields(context.Background(), stale, desired, true)
	if err != nil {
		t.Fatal(err)
	}
	if reads != 2 || updates != 1 {
		t.Fatalf("alignment attempts = %d reads, %d updates; want 2 reads, 1 update", reads, updates)
	}
	if aligned.GetResourceVersion() != "43" || !applyOwnershipMigrationComplete(aligned) {
		t.Fatalf("alignment did not return authoritative Apply-owned object: %#v", aligned.GetManagedFields())
	}
}

func TestApplyOwnershipMigrationCompleteRequiresOperatorOwnedMarker(t *testing.T) {
	t.Parallel()
	object := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Annotations: map[string]string{owned.ApplyOwnershipAnnotation: owned.ApplyOwnershipVersion},
		ManagedFields: []metav1.ManagedFieldsEntry{{
			Manager: owned.ManagedByValue, Operation: metav1.ManagedFieldsOperationApply,
			FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:data":{"f:current":{}}}`)},
		}},
	}}
	if applyOwnershipMigrationComplete(object) {
		t.Fatal("operator Apply ownership without marker-field ownership completed migration")
	}
	object.ManagedFields = append(object.ManagedFields, metav1.ManagedFieldsEntry{
		Manager: "external-manager", Operation: metav1.ManagedFieldsOperationApply,
		FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:metadata":{"f:annotations":{"f:pgshard.io/apply-ownership":{}}}}`)},
	})
	if applyOwnershipMigrationComplete(object) {
		t.Fatal("external marker-field ownership completed operator migration")
	}
	object.ManagedFields[0].FieldsV1 = &metav1.FieldsV1{Raw: []byte(`{"f:metadata":{"f:annotations":{".":{},"f:pgshard.io/apply-ownership":{}}}}`)}
	if !applyOwnershipMigrationComplete(object) {
		t.Fatal("operator-owned marker was not recognized as completed migration")
	}
}

func TestLegacyAlignmentDoesNotTrustApplyOwnershipWithoutMarker(t *testing.T) {
	t.Parallel()
	current := legacyManagedConfigMap(types.UID("legacy-uid"))
	current.Data = map[string]string{"current": "value", "stale": "value"}
	current.ManagedFields = append(current.ManagedFields, metav1.ManagedFieldsEntry{
		Manager: owned.ManagedByValue, Operation: metav1.ManagedFieldsOperationApply,
		APIVersion: "v1", FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:data":{"f:current":{}}}`)},
	})
	desired := current.DeepCopy()
	desired.Data = map[string]string{"current": "value"}
	desired.ManagedFields = nil
	updates := 0
	writeClient := interceptedClient(t, newFakeClient(t), interceptor.Funcs{
		Update: func(_ context.Context, _ client.WithWatch, object client.Object, _ ...client.UpdateOption) error {
			updates++
			updated := object.(*corev1.ConfigMap)
			if _, exists := updated.Data["stale"]; exists {
				t.Fatalf("legacy alignment retained stale data: %#v", updated.Data)
			}
			return nil
		},
	})
	reconciler := &PgShardClusterReconciler{Client: writeClient, APIReader: newFakeClient(t, current)}
	if _, err := reconciler.alignLegacyOwnedFields(context.Background(), current, desired, false); err != nil {
		t.Fatal(err)
	}
	if updates != 1 {
		t.Fatalf("legacy alignment updates = %d, want 1", updates)
	}
}

func TestLegacyAlignmentAllowsOnlyInternalHPAOwnerForPooler(t *testing.T) {
	t.Parallel()
	replicas := int32(7)
	current := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "example-pooler", Namespace: "default", UID: types.UID("pooler-uid"),
			ManagedFields: []metav1.ManagedFieldsEntry{{
				Manager: hpaScaleFieldManager, Operation: metav1.ManagedFieldsOperationApply,
				APIVersion: "apps/v1", FieldsType: "FieldsV1",
				FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:replicas":{}}}`)},
			}},
		},
		Spec: appsv1.DeploymentSpec{Replicas: &replicas},
	}
	if !hasUnrelatedTopLevelApplyOwnership(current, false) {
		t.Fatal("legacy HPA manager was accepted outside the pooler Deployment")
	}
	if hasUnrelatedTopLevelApplyOwnership(current, true) {
		t.Fatal("legacy HPA manager was rejected for the pooler Deployment")
	}
	withExternal := current.DeepCopy()
	withExternal.ManagedFields = append(withExternal.ManagedFields, metav1.ManagedFieldsEntry{
		Manager: "external-manager", Operation: metav1.ManagedFieldsOperationApply,
	})
	if !hasUnrelatedTopLevelApplyOwnership(withExternal, true) {
		t.Fatal("external Apply manager was accepted alongside the legacy HPA manager")
	}

	desired := current.DeepCopy()
	desired.ManagedFields = nil
	updates := 0
	writeClient := interceptedClient(t, newFakeClient(t), interceptor.Funcs{
		Update: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.UpdateOption) error {
			updates++
			return nil
		},
	})
	reconciler := &PgShardClusterReconciler{Client: writeClient, APIReader: newFakeClient(t, current)}
	if _, err := reconciler.alignLegacyOwnedFields(context.Background(), current, desired, true); err != nil {
		t.Fatal(err)
	}
	if updates != 1 {
		t.Fatalf("legacy alignment updates = %d, want 1", updates)
	}
}

func TestLegacyAlignmentBoundsConflicts(t *testing.T) {
	t.Parallel()
	current := legacyManagedConfigMap(types.UID("legacy-uid"))
	desired := current.DeepCopy()
	desired.Data = map[string]string{"current": "value"}
	authoritative := newFakeClient(t, current.DeepCopy())
	updates := 0
	writeClient := interceptedClient(t, newFakeClient(t), interceptor.Funcs{
		Update: func(_ context.Context, _ client.WithWatch, object client.Object, _ ...client.UpdateOption) error {
			updates++
			return apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, object.GetName(), errors.New("injected conflict"))
		},
	})
	reconciler := &PgShardClusterReconciler{Client: writeClient, APIReader: authoritative}
	_, err := reconciler.alignLegacyOwnedFields(context.Background(), current, desired, false)
	if err == nil || !strings.Contains(err.Error(), "after 4 conflicts") {
		t.Fatalf("conflict exhaustion error = %v", err)
	}
	if updates != 4 {
		t.Fatalf("update attempts = %d, want 4", updates)
	}
}

func TestLegacyAlignmentRejectsReplacementAfterConflict(t *testing.T) {
	t.Parallel()
	current := legacyManagedConfigMap(types.UID("legacy-uid"))
	desired := current.DeepCopy()
	reads := 0
	authoritative := interceptedClient(t, newFakeClient(t), interceptor.Funcs{
		Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, object client.Object, _ ...client.GetOption) error {
			reads++
			source := current.DeepCopy()
			if reads > 1 {
				source.UID = types.UID("replacement-uid")
			}
			target := object.(*corev1.ConfigMap)
			*target = *source
			return nil
		},
	})
	writeClient := interceptedClient(t, newFakeClient(t), interceptor.Funcs{
		Update: func(_ context.Context, _ client.WithWatch, object client.Object, _ ...client.UpdateOption) error {
			return apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, object.GetName(), errors.New("injected conflict"))
		},
	})
	reconciler := &PgShardClusterReconciler{Client: writeClient, APIReader: authoritative}
	_, err := reconciler.alignLegacyOwnedFields(context.Background(), current, desired, false)
	if err == nil || !strings.Contains(err.Error(), "replaced during") {
		t.Fatalf("replacement error = %v", err)
	}
}

func TestLegacyAlignmentRejectsUnrelatedApplyOwner(t *testing.T) {
	t.Parallel()
	current := legacyManagedConfigMap(types.UID("legacy-uid"))
	current.ManagedFields = append(current.ManagedFields, metav1.ManagedFieldsEntry{
		Manager:    "external-manager",
		Operation:  metav1.ManagedFieldsOperationApply,
		APIVersion: "v1",
		FieldsType: "FieldsV1",
		FieldsV1:   &metav1.FieldsV1{Raw: []byte(`{"f:metadata":{"f:annotations":{"f:example.com/external":{}}}}`)},
	})
	authoritative := newFakeClient(t, current.DeepCopy())
	writeClient := interceptedClient(t, newFakeClient(t), interceptor.Funcs{
		Update: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.UpdateOption) error {
			t.Fatal("unsafe legacy alignment reached Update")
			return nil
		},
	})
	reconciler := &PgShardClusterReconciler{Client: writeClient, APIReader: authoritative}
	_, err := reconciler.alignLegacyOwnedFields(context.Background(), current, current.DeepCopy(), false)
	if err == nil || !strings.Contains(err.Error(), "another top-level Apply manager") {
		t.Fatalf("unrelated owner error = %v", err)
	}
}

func TestLegacyServiceAlignmentPreservesAllocations(t *testing.T) {
	t.Parallel()
	singleStack := corev1.IPFamilyPolicySingleStack
	current := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "example-rw",
			Namespace:   "default",
			Annotations: map[string]string{"example.com/remove-me": "true"},
		},
		Spec: corev1.ServiceSpec{
			Type:                  corev1.ServiceTypeLoadBalancer,
			ClusterIP:             "10.96.0.42",
			ClusterIPs:            []string{"10.96.0.42"},
			IPFamilies:            []corev1.IPFamily{corev1.IPv4Protocol},
			IPFamilyPolicy:        &singleStack,
			HealthCheckNodePort:   32042,
			ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyLocal,
			Ports: []corev1.ServicePort{{
				Name: "postgresql", Protocol: corev1.ProtocolTCP, Port: 5432, NodePort: 30432,
			}},
		},
	}
	desired := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: current.Name, Namespace: current.Namespace},
		Spec: corev1.ServiceSpec{
			Type:  corev1.ServiceTypeLoadBalancer,
			Ports: []corev1.ServicePort{{Name: "postgresql", Protocol: corev1.ProtocolTCP, Port: 5432}},
		},
	}
	alignedObject, err := legacyAlignedObject(current, desired)
	if err != nil {
		t.Fatal(err)
	}
	aligned := alignedObject.(*corev1.Service)
	if aligned.Spec.ClusterIP != current.Spec.ClusterIP ||
		len(aligned.Spec.ClusterIPs) != 1 || aligned.Spec.ClusterIPs[0] != current.Spec.ClusterIPs[0] ||
		len(aligned.Spec.IPFamilies) != 1 || aligned.Spec.IPFamilies[0] != current.Spec.IPFamilies[0] ||
		aligned.Spec.IPFamilyPolicy == nil || *aligned.Spec.IPFamilyPolicy != *current.Spec.IPFamilyPolicy ||
		aligned.Spec.HealthCheckNodePort != current.Spec.HealthCheckNodePort ||
		aligned.Spec.ExternalTrafficPolicy != current.Spec.ExternalTrafficPolicy ||
		len(aligned.Spec.Ports) != 1 || aligned.Spec.Ports[0].NodePort != current.Spec.Ports[0].NodePort {
		t.Fatalf("legacy alignment changed Service allocations or API defaults: %#v", aligned.Spec)
	}
	if len(aligned.Annotations) != 0 {
		t.Fatalf("legacy operator annotation survived alignment: %#v", aligned.Annotations)
	}
}

func TestMigrateApplyOwnershipRetriesConflict(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	legacy := legacyManagedConfigMap(types.UID("legacy-uid"))
	base := newFakeClient(t, legacy.DeepCopy())
	updates := 0
	writeClient := interceptedClient(t, base, interceptor.Funcs{
		Update: func(_ context.Context, _ client.WithWatch, object client.Object, _ ...client.UpdateOption) error {
			updates++
			if updates == 1 {
				return apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, object.GetName(), errors.New("injected conflict"))
			}
			return nil
		},
	})
	reconciler := &PgShardClusterReconciler{Client: writeClient, APIReader: base}
	migrated, err := reconciler.migrateApplyOwnership(ctx, legacy.DeepCopy())
	if err != nil {
		t.Fatal(err)
	}
	if updates != 2 {
		t.Fatalf("update attempts = %d, want 2", updates)
	}
	for _, entry := range migrated.GetManagedFields() {
		if entry.Manager == "unknown" && entry.Operation == metav1.ManagedFieldsOperationUpdate {
			t.Fatalf("legacy manager survived migration: %#v", migrated.GetManagedFields())
		}
	}
}

func TestMigrateApplyOwnershipBoundsConflicts(t *testing.T) {
	t.Parallel()
	legacy := legacyManagedConfigMap(types.UID("legacy-uid"))
	base := newFakeClient(t, legacy.DeepCopy())
	updates := 0
	writeClient := interceptedClient(t, base, interceptor.Funcs{
		Update: func(_ context.Context, _ client.WithWatch, object client.Object, _ ...client.UpdateOption) error {
			updates++
			return apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, object.GetName(), errors.New("injected conflict"))
		},
	})
	reconciler := &PgShardClusterReconciler{Client: writeClient, APIReader: base}
	_, err := reconciler.migrateApplyOwnership(context.Background(), legacy.DeepCopy())
	if err == nil || !strings.Contains(err.Error(), "after 4 conflicts") {
		t.Fatalf("conflict exhaustion error = %v", err)
	}
	if updates != 4 {
		t.Fatalf("update attempts = %d, want 4", updates)
	}
}

func TestMigrateApplyOwnershipRejectsReplacementAfterConflict(t *testing.T) {
	t.Parallel()
	legacy := legacyManagedConfigMap(types.UID("legacy-uid"))
	replacement := legacyManagedConfigMap(types.UID("replacement-uid"))
	base := newFakeClient(t, replacement)
	writeClient := interceptedClient(t, newFakeClient(t), interceptor.Funcs{
		Update: func(_ context.Context, _ client.WithWatch, object client.Object, _ ...client.UpdateOption) error {
			return apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, object.GetName(), errors.New("injected conflict"))
		},
	})
	reconciler := &PgShardClusterReconciler{Client: writeClient, APIReader: base}
	_, err := reconciler.migrateApplyOwnership(context.Background(), legacy)
	if err == nil || !strings.Contains(err.Error(), "replaced") {
		t.Fatalf("replacement error = %v", err)
	}
}

func TestMigrateApplyOwnershipPreservesLaterUpdateManager(t *testing.T) {
	t.Parallel()
	current := legacyManagedConfigMap(types.UID("managed-uid"))
	current.Annotations = map[string]string{owned.ApplyOwnershipAnnotation: owned.ApplyOwnershipVersion}
	current.ManagedFields = append(current.ManagedFields, metav1.ManagedFieldsEntry{
		Manager:    owned.ManagedByValue,
		Operation:  metav1.ManagedFieldsOperationApply,
		APIVersion: "v1",
		FieldsType: "FieldsV1",
		FieldsV1:   &metav1.FieldsV1{Raw: []byte(`{"f:data":{".":{},"f:stale":{}},"f:metadata":{"f:annotations":{"f:pgshard.io/apply-ownership":{}}}}`)},
	})
	writeClient := interceptedClient(t, newFakeClient(t), interceptor.Funcs{
		Update: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.UpdateOption) error {
			t.Fatal("completed ownership migration attempted to erase a later Update manager")
			return nil
		},
	})
	reconciler := &PgShardClusterReconciler{Client: writeClient}
	migrated, err := reconciler.migrateApplyOwnership(context.Background(), current)
	if err != nil {
		t.Fatal(err)
	}
	if len(migrated.GetManagedFields()) != 2 || migrated.GetManagedFields()[0].Manager != "unknown" {
		t.Fatalf("later Update manager was not preserved: %#v", migrated.GetManagedFields())
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
	reconciler := &PgShardClusterReconciler{Client: fakeClient, APIReader: fakeClient}
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

func TestDeletionFinalizerUsesAuthoritativeReaderWhenCacheMissesChild(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	controller := true
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:      "data-example-etcd-0",
		Namespace: cluster.Namespace,
		UID:       types.UID("authoritative-pvc-uid"),
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: pgshardv1alpha1.GroupVersion.String(),
			Kind:       "PgShardCluster",
			Name:       cluster.Name,
			UID:        cluster.UID,
			Controller: &controller,
		}},
	}}

	staleCache := newFakeClient(t, cluster)
	authoritative := newFakeClient(t, cluster.DeepCopy(), claim)
	reconciler := &PgShardClusterReconciler{
		Client:    staleCache,
		APIReader: authoritative,
	}
	remaining, err := reconciler.prune(ctx, cluster, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if !remaining {
		t.Fatal("finalization treated an authoritative PVC as absent because the cache missed it")
	}
}

func TestDeletionFinalizerFailsClosedWithoutAuthoritativeReader(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	reconciler := &PgShardClusterReconciler{Client: newFakeClient(t, cluster)}
	remaining, err := reconciler.prune(context.Background(), cluster, nil, true)
	if err == nil {
		t.Fatal("deletion finalization succeeded without an authoritative API reader")
	}
	if remaining {
		t.Fatal("failed deletion finalization reported remaining resources")
	}
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
		WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, options ...client.CreateOption) error {
			if object.GetUID() == "" {
				object.SetUID(types.UID(utiluuid.NewUUID()))
			}
			return kubeClient.Create(ctx, object, options...)
		}}).
		Build()
}

func interceptedClient(t *testing.T, base client.Client, funcs interceptor.Funcs) client.Client {
	t.Helper()
	withWatch, ok := base.(client.WithWatch)
	if !ok {
		t.Fatalf("client %T does not implement client.WithWatch", base)
	}
	return interceptor.NewClient(withWatch, funcs)
}

func legacyManagedConfigMap(uid types.UID) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "legacy-config",
			Namespace:       "default",
			UID:             uid,
			ResourceVersion: "1",
			ManagedFields: []metav1.ManagedFieldsEntry{{
				Manager:    "unknown",
				Operation:  metav1.ManagedFieldsOperationUpdate,
				APIVersion: "v1",
				FieldsType: "FieldsV1",
				FieldsV1:   &metav1.FieldsV1{Raw: []byte(`{"f:data":{".":{},"f:stale":{}}}`)},
			}},
		},
		Data: map[string]string{"stale": "value"},
	}
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
