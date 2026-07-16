package controller

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	kubeClient := newKINDClient(t)

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pgshard-manager-smoke-%d", os.Getpid())}}
	if err := kubeClient.Create(ctx, namespace); err != nil {
		t.Fatal(err)
	}
	deleteNamespaceAtCleanup(t, kubeClient, namespace)

	cluster := readDevelopmentSample(t)
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
	assertCondition(t, current, postgresqlAvailableCondition, metav1.ConditionFalse, "PostgreSQLHAUnavailable")
	assertCondition(t, current, readyCondition, metav1.ConditionFalse, "PostgreSQLHAUnavailable")
	assertCondition(t, current, transportSecurityCondition, metav1.ConditionFalse, "TransportTLSUnavailable")
	assertPostgreSQLRoleProfiles(t, ctx, kubeClient, current)

	waitForEtcdQuorum(t, ctx, kubeClient, namespace.Name, cluster.Name)
	waitForStablePods(t, ctx, kubeClient, namespace.Name, cluster.Name, "etcd", 3, true)
	waitForStablePods(t, ctx, kubeClient, namespace.Name, cluster.Name, "orchestrator", 3, false)
	waitForStablePods(t, ctx, kubeClient, namespace.Name, cluster.Name, "pooler", 1, false)
	waitForStableManagerPod(t, ctx, kubeClient)
	assertFailClosedApplicationServices(t, ctx, kubeClient, namespace.Name, cluster.Name)
	assertNoPostgreSQLWorkload(t, ctx, kubeClient, namespace.Name, cluster.Name)
}

func TestKINDManagerRunsSingleMemberPostgreSQL18Primaries(t *testing.T) {
	if os.Getenv("PGSHARD_KIND_MANAGER_E2E") != "true" {
		t.Skip("set PGSHARD_KIND_MANAGER_E2E=true against the installed admission manager")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	kubeClient := newKINDClient(t)

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: fmt.Sprintf("pgshard-manager-postgresql-%d", os.Getpid()),
		Labels: map[string]string{
			"pod-security.kubernetes.io/enforce":         "restricted",
			"pod-security.kubernetes.io/enforce-version": "latest",
		},
	}}
	if err := kubeClient.Create(ctx, namespace); err != nil {
		t.Fatal(err)
	}
	deleteNamespaceAtCleanup(t, kubeClient, namespace)

	cluster := readSingleMemberSample(t)
	cluster.Namespace = namespace.Name
	if err := kubeClient.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	waitForSingleMemberPostgreSQL(t, ctx, kubeClient, client.ObjectKeyFromObject(cluster))

	shardZeroPod := cluster.Name + "-shard-0000-primary-0"
	shardOnePod := cluster.Name + "-shard-0001-primary-0"
	if got := strings.TrimSpace(runKubectl(t, ctx, "--namespace", namespace.Name, "exec", shardZeroPod, "--",
		"psql", "-X", "-U", "postgres", "-d", "postgres", "-Atc",
		"SELECT current_setting('server_version_num')::integer / 10000, pg_is_in_recovery()")); got != "18|f" {
		t.Fatalf("PostgreSQL identity = %q", got)
	}
	runKubectl(t, ctx, "--namespace", namespace.Name, "exec", shardZeroPod, "--",
		"psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "postgres", "-c",
		"CREATE TABLE live_marker (shard integer PRIMARY KEY, note text NOT NULL); INSERT INTO live_marker VALUES (0, 'kind-persistent');")
	service := cluster.Name + "-shard-0000"
	query := fmt.Sprintf(`PGPASSWORD="$POSTGRES_PASSWORD" psql -X -w -h %s -U postgres -d postgres -Atc "SELECT note FROM live_marker WHERE shard = 0"`, service)
	if got := strings.TrimSpace(runKubectl(t, ctx, "--namespace", namespace.Name, "exec", shardOnePod, "--", "bash", "-ceu", query)); got != "kind-persistent" {
		t.Fatalf("cross-shard-service query = %q", got)
	}

	before := &corev1.Pod{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: shardZeroPod}, before); err != nil {
		t.Fatal(err)
	}
	statefulSet := cluster.Name + "-shard-0000-primary"
	runKubectl(t, ctx, "--namespace", namespace.Name, "rollout", "restart", "statefulset/"+statefulSet)
	runKubectl(t, ctx, "--namespace", namespace.Name, "rollout", "status", "statefulset/"+statefulSet, "--timeout=120s")
	waitForRecreatedReadyPod(t, ctx, kubeClient, types.NamespacedName{Namespace: namespace.Name, Name: shardZeroPod}, before.UID)
	if got := strings.TrimSpace(runKubectl(t, ctx, "--namespace", namespace.Name, "exec", shardZeroPod, "--",
		"psql", "-X", "-U", "postgres", "-d", "postgres", "-Atc", "SELECT note FROM live_marker WHERE shard = 0")); got != "kind-persistent" {
		t.Fatalf("query after StatefulSet restart = %q", got)
	}
}

func newKINDClient(t *testing.T) client.Client {
	t.Helper()
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
	return kubeClient
}

func deleteNamespaceAtCleanup(t *testing.T, kubeClient client.Client, namespace *corev1.Namespace) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := kubeClient.Delete(ctx, namespace); err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("delete test namespace %s: %v", namespace.Name, err)
			return
		}
		err := wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
			current := &corev1.Namespace{}
			if err := kubeClient.Get(ctx, types.NamespacedName{Name: namespace.Name}, current); apierrors.IsNotFound(err) {
				return true, nil
			} else if err != nil {
				return false, err
			}
			return false, nil
		})
		if err != nil {
			t.Errorf("wait for test namespace %s deletion: %v", namespace.Name, err)
		}
	})
}

func waitForSingleMemberPostgreSQL(t *testing.T, ctx context.Context, kubeClient client.Client, key client.ObjectKey) {
	t.Helper()
	current := &pgshardv1alpha1.PgShardCluster{}
	err := wait.PollUntilContextTimeout(ctx, time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := kubeClient.Get(ctx, key, current); err != nil {
			return false, err
		}
		condition := meta.FindStatusCondition(current.Status.Conditions, postgresqlAvailableCondition)
		return condition != nil && condition.Status == metav1.ConditionTrue && condition.Reason == "SingleMemberPrimariesAvailable", nil
	})
	if err != nil {
		t.Fatalf("wait for single-member PostgreSQL primaries: %v; last status = %#v", err, current.Status)
	}
}

func waitForRecreatedReadyPod(t *testing.T, ctx context.Context, kubeClient client.Client, key types.NamespacedName, previousUID types.UID) {
	t.Helper()
	pod := &corev1.Pod{}
	err := wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		pod = &corev1.Pod{}
		if err := kubeClient.Get(ctx, key, pod); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		if pod.UID == previousUID || len(pod.Status.ContainerStatuses) != 1 {
			return false, nil
		}
		return pod.Status.Phase == corev1.PodRunning && pod.Status.ContainerStatuses[0].Ready, nil
	})
	if err != nil {
		t.Fatalf("wait for recreated PostgreSQL Pod: %v; last Pod = %#v", err, pod)
	}
}

func runKubectl(t *testing.T, ctx context.Context, arguments ...string) string {
	t.Helper()
	command := exec.CommandContext(ctx, "kubectl", arguments...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl %s: %v\n%s", strings.Join(arguments, " "), err, output)
	}
	return string(output)
}

func assertPostgreSQLRoleProfiles(t *testing.T, ctx context.Context, kubeClient client.Client, cluster *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	configuration := &corev1.ConfigMap{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + owned.PostgreSQLConfigSuffix}
	if err := kubeClient.Get(ctx, key, configuration); err != nil {
		t.Fatal(err)
	}
	wantDocuments := 1 + int(cluster.Spec.MembersPerShard)*2
	if len(configuration.Data) != wantDocuments {
		t.Fatalf("PostgreSQL configuration documents = %#v", configuration.Data)
	}
	common := configuration.Data["postgresql.conf"]
	for _, setting := range []string{
		"hot_standby = on\n",
		"idle_replication_slot_timeout = 0\n",
		"listen_addresses = '*'\n",
		"wal_level = logical\n",
	} {
		if !strings.Contains(common, setting) {
			t.Fatalf("common PostgreSQL configuration is missing %q:\n%s", setting, common)
		}
	}
	for ordinal := int32(0); ordinal < cluster.Spec.MembersPerShard; ordinal++ {
		memberName := fmt.Sprintf("pgshard_member_%04d", ordinal)
		standby := configuration.Data[fmt.Sprintf("standby-%04d.conf", ordinal)]
		for _, setting := range []string{
			"hot_standby_feedback = on\n",
			"primary_slot_name = '" + memberName + "'\n",
			"sync_replication_slots = on\n",
			"wal_receiver_status_interval = 1s\n",
		} {
			if !strings.Contains(standby, setting) {
				t.Fatalf("standby %d configuration is missing %q:\n%s", ordinal, setting, standby)
			}
		}
		primary := configuration.Data[fmt.Sprintf("primary-%04d.conf", ordinal)]
		candidates := make([]string, 0, cluster.Spec.MembersPerShard-1)
		for candidate := int32(0); candidate < cluster.Spec.MembersPerShard; candidate++ {
			if candidate == ordinal {
				continue
			}
			candidates = append(candidates, fmt.Sprintf("pgshard_member_%04d", candidate))
		}
		joinedCandidates := strings.Join(candidates, ",")
		wantPrimarySettings := []string{
			"synchronized_standby_slots = '" + joinedCandidates + "'\n",
		}
		if cluster.Spec.Durability == pgshardv1alpha1.DurabilitySynchronous {
			wantPrimarySettings = append(wantPrimarySettings, "synchronous_standby_names = 'ANY 1 ("+joinedCandidates+")'\n")
		} else {
			wantPrimarySettings = append(wantPrimarySettings, "synchronous_standby_names = ''\n")
		}
		for _, setting := range wantPrimarySettings {
			if !strings.Contains(primary, setting) {
				t.Fatalf("primary %d configuration is missing %q:\n%s", ordinal, setting, primary)
			}
		}
	}
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
		return current.Status.ObservedGeneration == current.Generation && current.Status.Phase == "Reconciling" && condition != nil && condition.Status == metav1.ConditionFalse && condition.Reason == "PostgreSQLHAUnavailable", nil
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
