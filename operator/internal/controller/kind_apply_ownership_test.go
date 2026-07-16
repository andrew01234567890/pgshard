package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const legacyHPAScaleAnnotation = "pgshard.io/hpa-scale-handed-off"

type hpaCacheMissClient struct {
	client.Client
	key client.ObjectKey
}

func TestKINDCRDRejectsUnsafeSpecTransitionsWithoutWebhooks(t *testing.T) {
	if os.Getenv("PGSHARD_KIND_E2E") != "true" {
		t.Skip("set PGSHARD_KIND_E2E=true against a disposable CRD-only KIND cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := pgshardv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kubeClient, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pgshard-crd-fence-%d", os.Getpid())}}
	if err := kubeClient.Create(ctx, namespace); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kubeClient.Delete(context.Background(), namespace) })

	transition := validCluster()
	transition.Name = "crd-transition-fence"
	transition.Namespace = namespace.Name
	transition.UID = ""
	transition.ResourceVersion = ""
	transition.Generation = 0
	if err := kubeClient.Create(ctx, transition); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*pgshardv1alpha1.PgShardCluster){
		"shards":          func(cluster *pgshardv1alpha1.PgShardCluster) { cluster.Spec.Shards++ },
		"membersPerShard": func(cluster *pgshardv1alpha1.PgShardCluster) { cluster.Spec.MembersPerShard = 5 },
		"storage size":    func(cluster *pgshardv1alpha1.PgShardCluster) { cluster.Spec.Storage.Size = resource.MustParse("8Gi") },
	} {
		current := &pgshardv1alpha1.PgShardCluster{}
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(transition), current); err != nil {
			t.Fatal(err)
		}
		before := current.DeepCopy()
		mutate(current)
		if err := kubeClient.Patch(ctx, current, client.MergeFrom(before)); err == nil || !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "immutable") {
			t.Fatalf("CRD admitted %s without any admission webhook installed: %v", name, err)
		}
	}
}

func (c hpaCacheMissClient) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if _, ok := object.(*autoscalingv2.HorizontalPodAutoscaler); ok && key == c.key {
		return apierrors.NewNotFound(autoscalingv2.Resource("horizontalpodautoscalers"), key.Name)
	}
	return c.Client.Get(ctx, key, object, options...)
}

func TestKINDServerSideApplyPrunesAndIsolatesScaleOwnership(t *testing.T) {
	if os.Getenv("PGSHARD_KIND_E2E") != "true" {
		t.Skip("set PGSHARD_KIND_E2E=true against a disposable KIND cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := pgshardv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kubeClient, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pgshard-apply-%d", os.Getpid())}}
	if err := kubeClient.Create(ctx, namespace); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kubeClient.Delete(context.Background(), namespace) })

	cluster := validCluster()
	cluster.Namespace = namespace.Name
	cluster.UID = ""
	cluster.ResourceVersion = ""
	cluster.Generation = 0
	cluster.Spec.MembersPerShard = 5
	cluster.Spec.Pooler.Scaling = pgshardv1alpha1.PoolerScaling{
		Mode:  pgshardv1alpha1.ScalingFixed,
		Fixed: &pgshardv1alpha1.FixedScaling{Replicas: 7},
	}
	cluster.Spec.Services.ReadWrite.Annotations = map[string]string{"example.com/remove-me": "true"}
	cluster.Spec.Observability.OpenTelemetryEndpoint = "http://tempo.invalid:4317"
	if err := kubeClient.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}

	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}}
	reconciler := &PgShardClusterReconciler{Client: kubeClient, APIReader: kubeClient}
	registerCleanup := func(request ctrl.Request) {
		t.Helper()
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cleanupCancel()
			current := &pgshardv1alpha1.PgShardCluster{}
			if err := kubeClient.Get(cleanupCtx, request.NamespacedName, current); err == nil {
				if err := kubeClient.Delete(cleanupCtx, current); err != nil {
					t.Errorf("delete test cluster: %v", err)
					return
				}
				waitForClusterDeletion(t, cleanupCtx, kubeClient, reconciler, request)
			} else if !apierrors.IsNotFound(err) {
				t.Errorf("get test cluster for cleanup: %v", err)
			}
		})
	}
	registerCleanup(request)
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}

	current := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(ctx, request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	oldConfiguration := plannedPostgreSQLConfiguration(t, current)
	current.Spec.PostgreSQL.Parameters = map[string]string{"log_statement": "ddl"}
	current.Spec.Pooler.Scaling = pgshardv1alpha1.PoolerScaling{
		Mode: pgshardv1alpha1.ScalingHPA,
		HPA: &pgshardv1alpha1.HPAScaling{
			MinReplicas: 2, MaxReplicas: 10, TargetCPUUtilizationPercentage: 65,
		},
	}
	current.Spec.Services.ReadWrite.Annotations = nil
	current.Spec.Observability.OpenTelemetryEndpoint = ""
	if err := kubeClient.Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}

	desiredConfiguration := plannedPostgreSQLConfiguration(t, current)
	configuration := &corev1.ConfigMap{}
	configurationKey := client.ObjectKeyFromObject(desiredConfiguration)
	if err := kubeClient.Get(ctx, configurationKey, configuration); err != nil {
		t.Fatal(err)
	}
	assertApplyOwner(t, configuration)
	if len(configuration.Data) != 11 || !strings.Contains(configuration.Data["postgresql.conf"], "log_statement = ddl\n") {
		t.Fatalf("new PostgreSQL configuration was not published: %#v", configuration.Data)
	}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(oldConfiguration), &corev1.ConfigMap{}); err != nil {
		t.Fatalf("old PostgreSQL configuration was removed before the rollout completed: %v", err)
	}
	networkPolicy := &networkingv1.NetworkPolicy{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + owned.EtcdSuffix}, networkPolicy); err != nil {
		t.Fatal(err)
	}
	assertApplyOwner(t, networkPolicy)
	service := &corev1.Service{}
	serviceKey := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + "-rw"}
	if err := kubeClient.Get(ctx, serviceKey, service); err != nil {
		t.Fatal(err)
	}
	if _, exists := service.Annotations["example.com/remove-me"]; exists {
		t.Fatalf("removed Service annotation survived: %#v", service.Annotations)
	}

	for _, name := range []string{cluster.Name + owned.OrchestratorSuffix, cluster.Name + owned.PoolerSuffix} {
		deployment := &appsv1.Deployment{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, deployment); err != nil {
			t.Fatal(err)
		}
		for _, container := range deployment.Spec.Template.Spec.Containers {
			for _, env := range container.Env {
				if env.Name == "OTEL_EXPORTER_OTLP_ENDPOINT" {
					t.Fatalf("removed OTEL environment variable survived on %s/%s", name, container.Name)
				}
			}
		}
	}

	pooler := &appsv1.Deployment{}
	poolerKey := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + owned.PoolerSuffix}
	if err := kubeClient.Get(ctx, poolerKey, pooler); err != nil {
		t.Fatal(err)
	}
	if pooler.Spec.Replicas == nil || *pooler.Spec.Replicas != 7 {
		t.Fatalf("fixed-to-HPA handoff changed capacity: %#v", pooler.Spec.Replicas)
	}
	assertScaleOwnerOnlyClaimsReplicas(t, pooler)

	// The test pooler deliberately never becomes Ready, so a real rolling
	// update cannot complete. Remove the test HPA and scale to zero to let the
	// Deployment controller prove no old Pod can still reference the immutable
	// configuration before exercising the prune boundary.
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	if err := kubeClient.Get(ctx, poolerKey, hpa); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Delete(ctx, hpa); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 15*time.Second, true, func(ctx context.Context) (bool, error) {
		err := kubeClient.Get(ctx, poolerKey, &autoscalingv2.HorizontalPodAutoscaler{})
		return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
	}); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(ctx, poolerKey, pooler); err != nil {
		t.Fatal(err)
	}
	zero := int32(0)
	pooler.Spec.Replicas = &zero
	if err := kubeClient.Update(ctx, pooler); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		current := &appsv1.Deployment{}
		if err := kubeClient.Get(ctx, poolerKey, current); err != nil {
			return false, err
		}
		return deploymentRolloutComplete(current), nil
	}); err != nil {
		t.Fatalf("wait for zero-Pod pooler rollout proof: %v", err)
	}
	if err := kubeClient.Get(ctx, request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	currentPlan, err := owned.Plan(current, owned.DefaultImages())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.prune(ctx, current, currentPlan, false); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(oldConfiguration), &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
		t.Fatalf("stale immutable PostgreSQL configuration survived the completed zero-Pod rollout: %v", err)
	}

	initialHPA := validCluster()
	initialHPA.Name = "initial-hpa"
	initialHPA.Namespace = namespace.Name
	initialHPA.UID = ""
	initialHPA.ResourceVersion = ""
	initialHPA.Generation = 0
	if err := kubeClient.Create(ctx, initialHPA); err != nil {
		t.Fatal(err)
	}
	initialRequest := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: initialHPA.Namespace, Name: initialHPA.Name}}
	registerCleanup(initialRequest)
	if _, err := reconciler.Reconcile(ctx, initialRequest); err != nil {
		t.Fatal(err)
	}
	initialPooler := &appsv1.Deployment{}
	initialPoolerKey := types.NamespacedName{Namespace: initialHPA.Namespace, Name: initialHPA.Name + owned.PoolerSuffix}
	if err := kubeClient.Get(ctx, initialPoolerKey, initialPooler); err != nil {
		t.Fatal(err)
	}
	if initialPooler.Spec.Replicas == nil || *initialPooler.Spec.Replicas != initialHPA.Spec.Pooler.Scaling.HPA.MinReplicas {
		t.Fatalf("initial HPA capacity = %#v, want %d", initialPooler.Spec.Replicas, initialHPA.Spec.Pooler.Scaling.HPA.MinReplicas)
	}
	assertScaleOwnerOnlyClaimsReplicas(t, initialPooler)

	exerciseCompletedLegacyApplyMigration(t, ctx, kubeClient, reconciler, namespace.Name, registerCleanup)
	exerciseLegacyHPAtoFixedMigration(t, ctx, kubeClient, reconciler, namespace.Name, registerCleanup, false)
	exerciseLegacyHPAtoFixedMigration(t, ctx, kubeClient, reconciler, namespace.Name, registerCleanup, true)
	exerciseFullReconcileHPAtoFixedWithCachedMiss(t, ctx, kubeClient, namespace.Name, registerCleanup)

	legacy := validCluster()
	legacy.Name = "legacy-upgrade"
	legacy.Namespace = namespace.Name
	legacy.UID = ""
	legacy.ResourceVersion = ""
	legacy.Generation = 0
	legacy.Spec.MembersPerShard = 5
	legacy.Spec.Pooler.Scaling = pgshardv1alpha1.PoolerScaling{
		Mode:  pgshardv1alpha1.ScalingFixed,
		Fixed: &pgshardv1alpha1.FixedScaling{Replicas: 7},
	}
	legacy.Spec.Services.ReadWrite.Annotations = map[string]string{"example.com/remove-me": "true"}
	legacy.Spec.Observability.OpenTelemetryEndpoint = "http://tempo.invalid:4317"
	if err := kubeClient.Create(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	legacyRequest := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: legacy.Namespace, Name: legacy.Name}}
	registerCleanup(legacyRequest)
	if err := kubeClient.Get(ctx, legacyRequest.NamespacedName, legacy); err != nil {
		t.Fatal(err)
	}
	legacyPlan, err := owned.Plan(legacy, owned.DefaultImages())
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range legacyPlan {
		// The pre-SSA controller created desired objects without a field owner or
		// migration marker. Recreate that exact upgrade boundary against the API.
		removeApplyOwnershipMarker(object)
		if err := kubeClient.Create(ctx, object); err != nil {
			t.Fatalf("create legacy %T %s/%s: %v", object, object.GetNamespace(), object.GetName(), err)
		}
	}

	legacyConfiguration := &corev1.ConfigMap{}
	legacyConfigurationKey := types.NamespacedName{Namespace: legacy.Namespace, Name: legacy.Name + owned.TopologyConfigSuffix}
	if err := kubeClient.Get(ctx, legacyConfigurationKey, legacyConfiguration); err != nil {
		t.Fatal(err)
	}
	if hasApplyOwnership(legacyConfiguration, owned.ManagedByValue) {
		t.Fatalf("legacy fixture unexpectedly has apply ownership: %#v", legacyConfiguration.ManagedFields)
	}
	hasLegacyUpdate := false
	for _, entry := range legacyConfiguration.ManagedFields {
		if entry.Subresource == "" && entry.Operation == metav1.ManagedFieldsOperationUpdate {
			hasLegacyUpdate = true
		}
	}
	if !hasLegacyUpdate {
		t.Fatalf("legacy fixture has no create-time Update owner: %#v", legacyConfiguration.ManagedFields)
	}
	legacyServiceBefore := &corev1.Service{}
	legacyServiceKey := types.NamespacedName{Namespace: legacy.Namespace, Name: legacy.Name + "-rw"}
	if err := kubeClient.Get(ctx, legacyServiceKey, legacyServiceBefore); err != nil {
		t.Fatal(err)
	}

	const externalManager = "pgshard-kind-external"
	externalAnnotation := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":      legacyConfiguration.Name,
			"namespace": legacyConfiguration.Namespace,
			"annotations": map[string]any{
				"example.com/external": "preserve",
			},
		},
	}}
	if err := kubeClient.Patch(ctx, externalAnnotation, client.Apply, client.FieldOwner(externalManager)); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(ctx, legacyRequest); err == nil {
		t.Fatal("legacy migration accepted an unrelated top-level Apply owner")
	}
	if err := kubeClient.Get(ctx, legacyConfigurationKey, legacyConfiguration); err != nil {
		t.Fatal(err)
	}
	if legacyConfiguration.Annotations["example.com/external"] != "preserve" {
		t.Fatalf("failed legacy migration deleted an external annotation: %#v", legacyConfiguration.Annotations)
	}
	externalClear := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":      legacyConfiguration.Name,
			"namespace": legacyConfiguration.Namespace,
		},
	}}
	if err := kubeClient.Patch(ctx, externalClear, client.Apply, client.FieldOwner(externalManager)); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(ctx, legacyConfigurationKey, legacyConfiguration); err != nil {
		t.Fatal(err)
	}
	if _, exists := legacyConfiguration.Annotations["example.com/external"]; exists || hasUnrelatedTopLevelApplyOwnership(legacyConfiguration, false) {
		t.Fatalf("external test manager did not relinquish its field set: annotations=%#v fields=%#v", legacyConfiguration.Annotations, legacyConfiguration.ManagedFields)
	}
	if err := kubeClient.Get(ctx, legacyRequest.NamespacedName, legacy); err != nil {
		t.Fatal(err)
	}

	legacy.Spec.PostgreSQL.Parameters = map[string]string{"log_statement": "ddl"}
	legacy.Spec.Pooler.Scaling = pgshardv1alpha1.PoolerScaling{
		Mode: pgshardv1alpha1.ScalingHPA,
		HPA: &pgshardv1alpha1.HPAScaling{
			MinReplicas: 2, MaxReplicas: 10, TargetCPUUtilizationPercentage: 65,
		},
	}
	legacy.Spec.Services.ReadWrite.Annotations = nil
	legacy.Spec.Observability.OpenTelemetryEndpoint = ""
	if err := kubeClient.Update(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	legacyPoolerBeforeMigration := applyLegacyWholeDeploymentHPAOwnership(t, ctx, kubeClient, legacy, 7)
	if hasExactReplicaApplyOwnership(legacyPoolerBeforeMigration, hpaScaleFieldManager) {
		t.Fatal("interrupted legacy HPA fixture did not retain whole-Deployment ownership")
	}
	if hasApplyOwnership(legacyPoolerBeforeMigration, owned.ManagedByValue) {
		t.Fatalf("interrupted legacy HPA fixture unexpectedly has operator Apply ownership: %#v", legacyPoolerBeforeMigration.ManagedFields)
	}
	if _, err := reconciler.Reconcile(ctx, legacyRequest); err != nil {
		t.Fatal(err)
	}

	if err := kubeClient.Get(ctx, legacyConfigurationKey, legacyConfiguration); err != nil {
		t.Fatal(err)
	}
	assertApplyOwner(t, legacyConfiguration)
	if strings.Contains(legacyConfiguration.Data["cluster.json"], "tempo.invalid") {
		t.Fatalf("legacy topology retained removed OpenTelemetry endpoint: %#v", legacyConfiguration.Data)
	}
	legacyService := &corev1.Service{}
	if err := kubeClient.Get(ctx, legacyServiceKey, legacyService); err != nil {
		t.Fatal(err)
	}
	if _, exists := legacyService.Annotations["example.com/remove-me"]; exists {
		t.Fatalf("legacy Service annotation survived migration: %#v", legacyService.Annotations)
	}
	beforePolicy, afterPolicy := legacyServiceBefore.Spec.IPFamilyPolicy, legacyService.Spec.IPFamilyPolicy
	samePolicy := beforePolicy == nil && afterPolicy == nil || beforePolicy != nil && afterPolicy != nil && *beforePolicy == *afterPolicy
	if legacyService.Spec.ClusterIP != legacyServiceBefore.Spec.ClusterIP ||
		!slices.Equal(legacyService.Spec.ClusterIPs, legacyServiceBefore.Spec.ClusterIPs) ||
		!slices.Equal(legacyService.Spec.IPFamilies, legacyServiceBefore.Spec.IPFamilies) ||
		!samePolicy {
		t.Fatalf("legacy migration changed Service allocations: before=%#v after=%#v", legacyServiceBefore.Spec, legacyService.Spec)
	}
	for _, name := range []string{legacy.Name + owned.OrchestratorSuffix, legacy.Name + owned.PoolerSuffix} {
		deployment := &appsv1.Deployment{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: legacy.Namespace, Name: name}, deployment); err != nil {
			t.Fatal(err)
		}
		for _, container := range deployment.Spec.Template.Spec.Containers {
			for _, env := range container.Env {
				if env.Name == "OTEL_EXPORTER_OTLP_ENDPOINT" {
					t.Fatalf("legacy OTEL environment variable survived on %s/%s", name, container.Name)
				}
			}
		}
	}
	legacyPooler := &appsv1.Deployment{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: legacy.Namespace, Name: legacy.Name + owned.PoolerSuffix}, legacyPooler); err != nil {
		t.Fatal(err)
	}
	if legacyPooler.Spec.Replicas == nil || *legacyPooler.Spec.Replicas != 7 {
		t.Fatalf("legacy fixed-to-HPA handoff changed capacity: %#v", legacyPooler.Spec.Replicas)
	}
	assertScaleOwnerOnlyClaimsReplicas(t, legacyPooler)
	if _, exists := legacyPooler.Annotations[legacyHPAScaleAnnotation]; exists {
		t.Fatalf("interrupted legacy HPA annotation survived migration: %#v", legacyPooler.Annotations)
	}
}

func exerciseCompletedLegacyApplyMigration(
	t *testing.T,
	ctx context.Context,
	kubeClient client.Client,
	reconciler *PgShardClusterReconciler,
	namespace string,
	registerCleanup func(ctrl.Request),
) {
	t.Helper()
	cluster := createBareLegacyCluster(t, ctx, kubeClient, namespace, "legacy-completed", registerCleanup)
	fullConfiguration := plannedTopologyConfiguration(t, cluster)
	fullConfiguration.Data["legacy-only"] = "remove during migration"
	fullPooler := plannedPoolerDeployment(t, cluster)
	removeApplyOwnershipMarker(fullConfiguration)
	removeApplyOwnershipMarker(fullPooler)
	if err := kubeClient.Create(ctx, fullConfiguration.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Create(ctx, fullPooler.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	applyLegacyOperatorOwnership(t, ctx, kubeClient, fullConfiguration)
	applyLegacyOperatorOwnership(t, ctx, kubeClient, fullPooler)

	cluster.Spec.Pooler.Scaling = pgshardv1alpha1.PoolerScaling{
		Mode: pgshardv1alpha1.ScalingHPA,
		HPA: &pgshardv1alpha1.HPAScaling{
			MinReplicas: 2, MaxReplicas: 10, TargetCPUUtilizationPercentage: 65,
		},
	}
	if err := kubeClient.Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	shrunkConfiguration := plannedTopologyConfiguration(t, cluster)
	applyLegacyOperatorOwnership(t, ctx, kubeClient, shrunkConfiguration)
	applyLegacyWholeDeploymentHPAOwnership(t, ctx, kubeClient, cluster, 7)
	desiredHPAPooler := plannedPoolerDeployment(t, cluster)
	legacyHPAPooler := desiredHPAPooler.DeepCopy()
	legacyHPAPooler.Annotations = map[string]string{legacyHPAScaleAnnotation: "true"}
	applyLegacyOperatorOwnership(t, ctx, kubeClient, legacyHPAPooler)

	configurationBefore := &corev1.ConfigMap{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(shrunkConfiguration), configurationBefore); err != nil {
		t.Fatal(err)
	}
	if _, exists := configurationBefore.Data["legacy-only"]; !exists {
		t.Fatalf("completed legacy Apply fixture did not retain its Update-owned key: %#v", configurationBefore.Data)
	}
	if applyOwnershipMigrationComplete(configurationBefore) || !hasApplyOwnership(configurationBefore, owned.ManagedByValue) || !hasTopLevelUpdateOwnership(configurationBefore) {
		t.Fatalf("completed legacy ConfigMap fixture has the wrong ownership boundary: annotations=%#v fields=%#v", configurationBefore.Annotations, configurationBefore.ManagedFields)
	}
	if err := reconciler.applyObject(ctx, cluster, shrunkConfiguration, ownershipState{exists: true, object: configurationBefore}); err != nil {
		t.Fatal(err)
	}
	configurationAfter := &corev1.ConfigMap{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(shrunkConfiguration), configurationAfter); err != nil {
		t.Fatal(err)
	}
	assertApplyOwner(t, configurationAfter)
	if _, exists := configurationAfter.Data["legacy-only"]; exists {
		t.Fatalf("Update-owned key survived completed legacy migration: %#v", configurationAfter.Data)
	}

	poolerBefore := &appsv1.Deployment{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(desiredHPAPooler), poolerBefore); err != nil {
		t.Fatal(err)
	}
	if applyOwnershipMigrationComplete(poolerBefore) || !hasApplyOwnership(poolerBefore, owned.ManagedByValue) || !hasApplyOwnership(poolerBefore, hpaScaleFieldManager) || !hasTopLevelUpdateOwnership(poolerBefore) {
		t.Fatalf("completed legacy HPA fixture has the wrong ownership boundary: annotations=%#v fields=%#v", poolerBefore.Annotations, poolerBefore.ManagedFields)
	}
	if err := reconciler.applyObject(ctx, cluster, desiredHPAPooler, ownershipState{exists: true, object: poolerBefore}); err != nil {
		t.Fatal(err)
	}
	poolerAfter := &appsv1.Deployment{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(desiredHPAPooler), poolerAfter); err != nil {
		t.Fatal(err)
	}
	if poolerAfter.Spec.Replicas == nil || *poolerAfter.Spec.Replicas != 7 {
		t.Fatalf("completed legacy HPA migration changed capacity: %#v", poolerAfter.Spec.Replicas)
	}
	assertApplyOwner(t, poolerAfter)
	assertScaleOwnerOnlyClaimsReplicas(t, poolerAfter)
	if _, exists := poolerAfter.Annotations[legacyHPAScaleAnnotation]; exists {
		t.Fatalf("completed legacy HPA annotation survived migration: %#v", poolerAfter.Annotations)
	}
}

func exerciseLegacyHPAtoFixedMigration(
	t *testing.T,
	ctx context.Context,
	kubeClient client.Client,
	reconciler *PgShardClusterReconciler,
	namespace string,
	registerCleanup func(ctrl.Request),
	completed bool,
) {
	t.Helper()
	name := "legacy-fixed-interrupted"
	if completed {
		name = "legacy-fixed-completed"
	}
	cluster := createBareLegacyCluster(t, ctx, kubeClient, namespace, name, registerCleanup)
	pooler := plannedPoolerDeployment(t, cluster)
	removeApplyOwnershipMarker(pooler)
	if err := kubeClient.Create(ctx, pooler.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	if completed {
		applyLegacyOperatorOwnership(t, ctx, kubeClient, pooler)
	}

	cluster.Spec.Pooler.Scaling = pgshardv1alpha1.PoolerScaling{
		Mode: pgshardv1alpha1.ScalingHPA,
		HPA: &pgshardv1alpha1.HPAScaling{
			MinReplicas: 2, MaxReplicas: 10, TargetCPUUtilizationPercentage: 65,
		},
	}
	if err := kubeClient.Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	applyLegacyWholeDeploymentHPAOwnership(t, ctx, kubeClient, cluster, 7)
	if completed {
		legacyHPAPooler := plannedPoolerDeployment(t, cluster)
		legacyHPAPooler.Annotations = map[string]string{legacyHPAScaleAnnotation: "true"}
		applyLegacyOperatorOwnership(t, ctx, kubeClient, legacyHPAPooler)
	}

	cluster.Spec.Pooler.Scaling = pgshardv1alpha1.PoolerScaling{
		Mode:  pgshardv1alpha1.ScalingFixed,
		Fixed: &pgshardv1alpha1.FixedScaling{Replicas: 6},
	}
	if err := kubeClient.Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	desired := plannedPoolerDeployment(t, cluster)
	current := &appsv1.Deployment{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(desired), current); err != nil {
		t.Fatal(err)
	}
	if !hasApplyOwnership(current, hpaScaleFieldManager) || applyOwnershipMigrationComplete(current) {
		t.Fatalf("legacy HPA-to-fixed fixture has the wrong ownership boundary: annotations=%#v fields=%#v", current.Annotations, current.ManagedFields)
	}
	if completed && (!hasApplyOwnership(current, owned.ManagedByValue) || !hasTopLevelUpdateOwnership(current)) {
		t.Fatalf("completed legacy HPA-to-fixed fixture is incomplete: %#v", current.ManagedFields)
	}
	if err := reconciler.applyObject(ctx, cluster, desired, ownershipState{exists: true, object: current}); err != nil {
		t.Fatal(err)
	}
	result := &appsv1.Deployment{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(desired), result); err != nil {
		t.Fatal(err)
	}
	if result.Spec.Replicas == nil || *result.Spec.Replicas != 6 {
		t.Fatalf("legacy HPA-to-fixed replicas = %#v, want 6", result.Spec.Replicas)
	}
	assertApplyOwner(t, result)
	assertOperatorClaimsReplicas(t, result)
	if hasApplyOwnership(result, hpaScaleFieldManager) {
		t.Fatalf("legacy HPA manager survived fixed-scale migration: %#v", result.ManagedFields)
	}
	if _, exists := result.Annotations[legacyHPAScaleAnnotation]; exists {
		t.Fatalf("legacy HPA annotation survived fixed-scale migration: %#v", result.Annotations)
	}
}

func exerciseFullReconcileHPAtoFixedWithCachedMiss(
	t *testing.T,
	ctx context.Context,
	kubeClient client.Client,
	namespace string,
	registerCleanup func(ctrl.Request),
) {
	t.Helper()
	cluster := validCluster()
	cluster.Name = "hpa-fixed-cache-miss"
	cluster.Namespace = namespace
	cluster.UID = ""
	cluster.ResourceVersion = ""
	cluster.Generation = 0
	if err := kubeClient.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}
	registerCleanup(request)
	reconciler := &PgShardClusterReconciler{Client: kubeClient, APIReader: kubeClient}
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}

	hpaKey := types.NamespacedName{Namespace: namespace, Name: cluster.Name + owned.PoolerSuffix}
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	if err := kubeClient.Get(ctx, hpaKey, hpa); err != nil {
		t.Fatal(err)
	}
	initialReplicas := cluster.Spec.Pooler.Scaling.HPA.MinReplicas
	pooler := &appsv1.Deployment{}
	if err := kubeClient.Get(ctx, hpaKey, pooler); err != nil {
		t.Fatal(err)
	}
	if pooler.Spec.Replicas == nil || *pooler.Spec.Replicas != initialReplicas {
		t.Fatalf("initial HPA pooler replicas = %#v", pooler.Spec.Replicas)
	}
	assertScaleOwnerOnlyClaimsReplicas(t, pooler)

	if err := kubeClient.Get(ctx, request.NamespacedName, cluster); err != nil {
		t.Fatal(err)
	}
	cluster.Spec.Pooler.Scaling = pgshardv1alpha1.PoolerScaling{
		Mode:  pgshardv1alpha1.ScalingFixed,
		Fixed: &pgshardv1alpha1.FixedScaling{Replicas: 6},
	}
	if err := kubeClient.Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}

	staleCache := hpaCacheMissClient{Client: kubeClient, key: hpaKey}
	staleReconciler := &PgShardClusterReconciler{Client: staleCache, APIReader: kubeClient}
	result, err := staleReconciler.Reconcile(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Requeue {
		t.Fatalf("HPA deletion did not stop fixed-scale reconciliation: %#v", result)
	}
	if err := kubeClient.Get(ctx, hpaKey, hpa); !apierrors.IsNotFound(err) {
		t.Fatalf("HPA still exists after authoritative transition gate: %v", err)
	}
	if err := kubeClient.Get(ctx, hpaKey, pooler); err != nil {
		t.Fatal(err)
	}
	if pooler.Spec.Replicas == nil || *pooler.Spec.Replicas != initialReplicas || !hasApplyOwnership(pooler, hpaScaleFieldManager) {
		t.Fatalf("pooler changed before authoritative HPA absence: replicas=%#v fields=%#v", pooler.Spec.Replicas, pooler.ManagedFields)
	}

	if _, err := staleReconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(ctx, hpaKey, pooler); err != nil {
		t.Fatal(err)
	}
	if pooler.Spec.Replicas == nil || *pooler.Spec.Replicas != 6 {
		t.Fatalf("fixed pooler replicas = %#v, want 6", pooler.Spec.Replicas)
	}
	assertApplyOwner(t, pooler)
	assertOperatorClaimsReplicas(t, pooler)
	if hasApplyOwnership(pooler, hpaScaleFieldManager) {
		t.Fatalf("HPA scale manager survived authoritative fixed transition: %#v", pooler.ManagedFields)
	}
}

func createBareLegacyCluster(
	t *testing.T,
	ctx context.Context,
	kubeClient client.Client,
	namespace, name string,
	registerCleanup func(ctrl.Request),
) *pgshardv1alpha1.PgShardCluster {
	t.Helper()
	cluster := validCluster()
	cluster.Name = name
	cluster.Namespace = namespace
	cluster.UID = ""
	cluster.ResourceVersion = ""
	cluster.Generation = 0
	cluster.Spec.MembersPerShard = 5
	cluster.Spec.Pooler.Scaling = pgshardv1alpha1.PoolerScaling{
		Mode:  pgshardv1alpha1.ScalingFixed,
		Fixed: &pgshardv1alpha1.FixedScaling{Replicas: 7},
	}
	if err := kubeClient.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}
	registerCleanup(request)
	if err := kubeClient.Get(ctx, request.NamespacedName, cluster); err != nil {
		t.Fatal(err)
	}
	return cluster
}

func plannedPostgreSQLConfiguration(t *testing.T, cluster *pgshardv1alpha1.PgShardCluster) *corev1.ConfigMap {
	t.Helper()
	plan, err := owned.Plan(cluster, owned.DefaultImages())
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range plan {
		configuration, ok := object.(*corev1.ConfigMap)
		if ok && strings.HasPrefix(configuration.Name, cluster.Name+owned.PostgreSQLConfigSuffix+"-") {
			return configuration
		}
	}
	t.Fatal("planned PostgreSQL configuration is missing")
	return nil
}

func plannedTopologyConfiguration(t *testing.T, cluster *pgshardv1alpha1.PgShardCluster) *corev1.ConfigMap {
	t.Helper()
	plan, err := owned.Plan(cluster, owned.DefaultImages())
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range plan {
		configuration, ok := object.(*corev1.ConfigMap)
		if ok && configuration.Name == cluster.Name+owned.TopologyConfigSuffix {
			return configuration
		}
	}
	t.Fatal("planned topology configuration is missing")
	return nil
}

func plannedPoolerDeployment(t *testing.T, cluster *pgshardv1alpha1.PgShardCluster) *appsv1.Deployment {
	t.Helper()
	plan, err := owned.Plan(cluster, owned.DefaultImages())
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range plan {
		deployment, ok := object.(*appsv1.Deployment)
		if ok && deployment.Name == cluster.Name+owned.PoolerSuffix {
			return deployment
		}
	}
	t.Fatal("planned pooler Deployment is missing")
	return nil
}

func applyLegacyOperatorOwnership(t *testing.T, ctx context.Context, kubeClient client.Client, desired client.Object) client.Object {
	t.Helper()
	applied := desired.DeepCopyObject().(client.Object)
	removeApplyOwnershipMarker(applied)
	applied.SetManagedFields(nil)
	applied.SetResourceVersion("")
	applied.SetGeneration(0)
	applied.SetCreationTimestamp(metav1.Time{})
	gvk, err := objectGVK(applied)
	if err != nil {
		t.Fatal(err)
	}
	current := applied.DeepCopyObject().(client.Object)
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(applied), current); err != nil {
		t.Fatal(err)
	}
	applied.GetObjectKind().SetGroupVersionKind(gvk)
	applied.SetUID(current.GetUID())
	if err := kubeClient.Patch(ctx, applied, client.Apply, client.FieldOwner(owned.ManagedByValue), client.ForceOwnership); err != nil {
		t.Fatal(err)
	}
	result := applied.DeepCopyObject().(client.Object)
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(applied), result); err != nil {
		t.Fatal(err)
	}
	return result
}

func applyLegacyWholeDeploymentHPAOwnership(
	t *testing.T,
	ctx context.Context,
	kubeClient client.Client,
	cluster *pgshardv1alpha1.PgShardCluster,
	replicas int32,
) *appsv1.Deployment {
	t.Helper()
	handoff := plannedPoolerDeployment(t, cluster).DeepCopy()
	current := &appsv1.Deployment{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(handoff), current); err != nil {
		t.Fatal(err)
	}
	handoff.Annotations = map[string]string{legacyHPAScaleAnnotation: "true"}
	handoff.Spec.Replicas = &replicas
	handoff.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("Deployment"))
	handoff.UID = current.UID
	if err := kubeClient.Patch(ctx, handoff, client.Apply, client.FieldOwner(hpaScaleFieldManager), client.ForceOwnership); err != nil {
		t.Fatal(err)
	}
	result := &appsv1.Deployment{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(handoff), result); err != nil {
		t.Fatal(err)
	}
	return result
}

func hasTopLevelUpdateOwnership(object metav1.Object) bool {
	for _, entry := range object.GetManagedFields() {
		if entry.Subresource == "" && entry.Operation == metav1.ManagedFieldsOperationUpdate {
			return true
		}
	}
	return false
}

func assertApplyOwner(t *testing.T, object metav1.Object) {
	t.Helper()
	if !applyOwnershipMigrationComplete(object) {
		t.Fatalf("%s does not own the completed migration marker: annotations=%#v fields=%#v", owned.ManagedByValue, object.GetAnnotations(), object.GetManagedFields())
	}
	found := false
	for _, entry := range object.GetManagedFields() {
		if entry.Subresource == "" && entry.Operation == metav1.ManagedFieldsOperationApply && entry.Manager == owned.ManagedByValue {
			found = true
		}
		if entry.Subresource == "" && entry.Operation == metav1.ManagedFieldsOperationUpdate {
			t.Fatalf("update manager still owns %T fields: %#v", object, entry)
		}
	}
	if !found {
		t.Fatalf("%s does not own an apply field set: %#v", owned.ManagedByValue, object.GetManagedFields())
	}
}

func assertScaleOwnerOnlyClaimsReplicas(t *testing.T, deployment *appsv1.Deployment) {
	t.Helper()
	found := false
	for _, entry := range deployment.ManagedFields {
		if entry.Manager != hpaScaleFieldManager || entry.Operation != metav1.ManagedFieldsOperationApply || entry.Subresource != "" {
			continue
		}
		found = true
		if entry.FieldsV1 == nil {
			t.Fatal("HPA scale manager has no field set")
		}
		var fields map[string]any
		if err := json.Unmarshal(entry.FieldsV1.Raw, &fields); err != nil {
			t.Fatal(err)
		}
		spec, ok := fields["f:spec"].(map[string]any)
		if len(fields) != 1 || !ok || len(spec) != 1 || spec["f:replicas"] == nil {
			t.Fatalf("HPA scale manager owns more than spec.replicas: %s", entry.FieldsV1.Raw)
		}
	}
	if !found {
		t.Fatalf("missing %s managed-fields entry: %#v", hpaScaleFieldManager, deployment.ManagedFields)
	}
	for _, entry := range deployment.ManagedFields {
		if entry.Manager == owned.ManagedByValue && entry.Operation == metav1.ManagedFieldsOperationApply && entry.FieldsV1 != nil {
			var fields map[string]any
			if err := json.Unmarshal(entry.FieldsV1.Raw, &fields); err != nil {
				t.Fatal(err)
			}
			if spec, ok := fields["f:spec"].(map[string]any); ok && spec["f:replicas"] != nil {
				t.Fatalf("operator still co-owns HPA replicas: %s", entry.FieldsV1.Raw)
			}
		}
	}
}

func assertOperatorClaimsReplicas(t *testing.T, deployment *appsv1.Deployment) {
	t.Helper()
	for _, entry := range deployment.ManagedFields {
		if entry.Manager != owned.ManagedByValue || entry.Operation != metav1.ManagedFieldsOperationApply || entry.Subresource != "" || entry.FieldsV1 == nil {
			continue
		}
		var fields map[string]any
		if err := json.Unmarshal(entry.FieldsV1.Raw, &fields); err != nil {
			t.Fatal(err)
		}
		if spec, ok := fields["f:spec"].(map[string]any); ok && spec["f:replicas"] != nil {
			return
		}
	}
	t.Fatalf("operator does not own fixed pooler replicas: %#v", deployment.ManagedFields)
}
