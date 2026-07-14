package controller

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

// This exceeds the 5-second initial delay plus three 10-second liveness
// periods, so a broken process cannot pass immediately before its first restart.
const stableContainerObservation = 40 * time.Second

func TestKINDManagerReconcilesFailClosedDevelopmentCluster(t *testing.T) {
	if os.Getenv("PGSHARD_KIND_MANAGER_E2E") != "true" {
		t.Skip("set PGSHARD_KIND_MANAGER_E2E=true against the installed development manager")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pgshard-manager-smoke-%d", os.Getpid())}}
	if err := kubeClient.Create(ctx, namespace); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kubeClient.Delete(context.Background(), namespace) })

	contents, err := os.ReadFile("../../config/samples/pgshard_v1alpha1_development.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cluster := &pgshardv1alpha1.PgShardCluster{}
	if err := yaml.UnmarshalStrict(contents, cluster); err != nil {
		t.Fatal(err)
	}
	cluster.Namespace = namespace.Name
	if err := kubeClient.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}

	current := waitForManagerStatus(t, ctx, kubeClient, client.ObjectKeyFromObject(cluster))
	if !contains(current.Finalizers, resourceFinalizer) {
		t.Fatalf("manager did not install its cleanup finalizer: %q", current.Finalizers)
	}
	assertCondition(t, current, reconciledCondition, metav1.ConditionTrue, "ResourcesApplied")
	assertCondition(t, current, supportingAvailableCondition, metav1.ConditionFalse, "SupportingWorkloadsProgressing")
	assertCondition(t, current, readyCondition, metav1.ConditionFalse, "PostgreSQLLifecycleUnavailable")
	assertCondition(t, current, transportSecurityCondition, metav1.ConditionFalse, "EtcdTLSUnavailable")

	waitForEtcdQuorum(t, ctx, kubeClient, namespace.Name, cluster.Name)
	waitForStablePods(t, ctx, kubeClient, namespace.Name, cluster.Name, "etcd", 3, true)
	waitForStablePods(t, ctx, kubeClient, namespace.Name, cluster.Name, "orchestrator", 3, false)
	waitForStablePods(t, ctx, kubeClient, namespace.Name, cluster.Name, "pooler", 1, false)
	waitForStableManagerPod(t, ctx, kubeClient)
	assertFailClosedApplicationServices(t, ctx, kubeClient, namespace.Name, cluster.Name)
	assertNoPostgreSQLWorkload(t, ctx, kubeClient, namespace.Name, cluster.Name)
}

func waitForStableManagerPod(t *testing.T, ctx context.Context, kubeClient client.Client) {
	t.Helper()
	pods := &corev1.PodList{}
	err := wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		pods = &corev1.PodList{}
		if err := kubeClient.List(ctx, pods,
			client.InNamespace("pgshard-system"),
			client.MatchingLabels{"app.kubernetes.io/name": "pgshard-operator", "app.kubernetes.io/component": "controller-manager"},
		); err != nil {
			return false, err
		}
		if len(pods.Items) != 1 || len(pods.Items[0].Status.ContainerStatuses) != 1 {
			return false, nil
		}
		status := pods.Items[0].Status.ContainerStatuses[0]
		if status.RestartCount != 0 {
			return false, fmt.Errorf("manager pod %s restarted %d times", pods.Items[0].Name, status.RestartCount)
		}
		return pods.Items[0].Status.Phase == corev1.PodRunning && status.Ready && status.State.Running != nil && time.Since(status.State.Running.StartedAt.Time) >= stableContainerObservation, nil
	})
	if err != nil {
		t.Fatalf("wait for stable manager pod: %v; last pods = %#v", err, pods.Items)
	}
}

func waitForManagerStatus(t *testing.T, ctx context.Context, kubeClient client.Client, key client.ObjectKey) *pgshardv1alpha1.PgShardCluster {
	t.Helper()
	current := &pgshardv1alpha1.PgShardCluster{}
	err := wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := kubeClient.Get(ctx, key, current); err != nil {
			return false, err
		}
		condition := meta.FindStatusCondition(current.Status.Conditions, readyCondition)
		return current.Status.ObservedGeneration == current.Generation && current.Status.Phase == "Reconciling" && condition != nil && condition.Status == metav1.ConditionFalse && condition.Reason == "PostgreSQLLifecycleUnavailable", nil
	})
	if err != nil {
		t.Fatalf("wait for manager status: %v; last status = %#v", err, current.Status)
	}
	return current.DeepCopy()
}

func waitForEtcdQuorum(t *testing.T, ctx context.Context, kubeClient client.Client, namespace, cluster string) {
	t.Helper()
	statefulSet := &appsv1.StatefulSet{}
	key := types.NamespacedName{Namespace: namespace, Name: cluster + owned.EtcdSuffix}
	err := wait.PollUntilContextTimeout(ctx, time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := kubeClient.Get(ctx, key, statefulSet); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		return statefulSet.Status.ObservedGeneration >= statefulSet.Generation && statefulSet.Status.ReadyReplicas == 3 && statefulSet.Status.UpdatedReplicas == 3, nil
	})
	if err != nil {
		t.Fatalf("wait for etcd quorum: %v; last status = %#v", err, statefulSet.Status)
	}
}

func waitForStablePods(t *testing.T, ctx context.Context, kubeClient client.Client, namespace, cluster, component string, wanted int, wantReady bool) {
	t.Helper()
	pods := &corev1.PodList{}
	err := wait.PollUntilContextTimeout(ctx, time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
		pods = &corev1.PodList{}
		if err := kubeClient.List(ctx, pods, client.InNamespace(namespace), client.MatchingLabels{owned.ClusterLabel: cluster, owned.ComponentLabel: component}); err != nil {
			return false, err
		}
		if len(pods.Items) != wanted {
			return false, nil
		}
		for index := range pods.Items {
			pod := &pods.Items[index]
			if pod.Status.Phase != corev1.PodRunning || len(pod.Status.ContainerStatuses) != 1 {
				return false, nil
			}
			status := pod.Status.ContainerStatuses[0]
			if status.RestartCount != 0 {
				return false, fmt.Errorf("%s pod %s restarted %d times", component, pod.Name, status.RestartCount)
			}
			if !wantReady && status.Ready {
				return false, fmt.Errorf("fail-closed %s pod %s unexpectedly became ready", component, pod.Name)
			}
			if wantReady && !status.Ready {
				return false, nil
			}
			if status.State.Running == nil || time.Since(status.State.Running.StartedAt.Time) < stableContainerObservation {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("wait for stable %s pods: %v; last pods = %#v", component, err, pods.Items)
	}
}

func assertFailClosedApplicationServices(t *testing.T, ctx context.Context, kubeClient client.Client, namespace, cluster string) {
	t.Helper()
	for _, suffix := range []string{"-rw", "-ro", "-r"} {
		serviceName := cluster + suffix
		service := &corev1.Service{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: serviceName}, service); err != nil {
			t.Fatal(err)
		}
		if service.Spec.PublishNotReadyAddresses {
			t.Fatalf("application Service %s publishes unready addresses", serviceName)
		}
		slices := &discoveryv1.EndpointSliceList{}
		if err := kubeClient.List(ctx, slices, client.InNamespace(namespace), client.MatchingLabels{discoveryv1.LabelServiceName: serviceName}); err != nil {
			t.Fatal(err)
		}
		for _, slice := range slices.Items {
			for _, endpoint := range slice.Endpoints {
				if endpoint.Conditions.Ready == nil || *endpoint.Conditions.Ready {
					t.Fatalf("application Service %s has ready endpoint %v", serviceName, endpoint.Addresses)
				}
			}
		}
	}
}

func assertNoPostgreSQLWorkload(t *testing.T, ctx context.Context, kubeClient client.Client, namespace, cluster string) {
	t.Helper()
	statefulSets := &appsv1.StatefulSetList{}
	if err := kubeClient.List(ctx, statefulSets, client.InNamespace(namespace), client.MatchingLabels{owned.ClusterLabel: cluster}); err != nil {
		t.Fatal(err)
	}
	if len(statefulSets.Items) != 1 || statefulSets.Items[0].Labels[owned.ComponentLabel] != "etcd" {
		t.Fatalf("unexpected stateful workloads = %#v", statefulSets.Items)
	}
	pods := &corev1.PodList{}
	if err := kubeClient.List(ctx, pods, client.InNamespace(namespace), client.MatchingLabels{owned.ClusterLabel: cluster, owned.ComponentLabel: "postgresql"}); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("PostgreSQL pods exist before lifecycle support: %#v", pods.Items)
	}
}
