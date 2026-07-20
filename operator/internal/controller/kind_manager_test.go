package controller

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/operator/internal/podfence"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// This exceeds the 5-second initial delay plus three 10-second liveness
// periods, so a broken process cannot pass immediately before its first restart.
const stableContainerObservation = 40 * time.Second

func TestMain(m *testing.M) {
	ctrl.SetLogger(logr.Discard())
	os.Exit(m.Run())
}

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
	if len(current.Status.PostgreSQLReplicationCredentials) != 0 {
		t.Fatalf("direct runtime staged replication credentials: %#v", current.Status.PostgreSQLReplicationCredentials)
	}
	assertKINDHAAuthorityDiscoveryFoundation(t, ctx, kubeClient, current)
	assertPostgreSQLRoleProfiles(t, ctx, kubeClient, current)

	waitForStablePods(t, ctx, kubeClient, namespace.Name, cluster.Name, "orchestrator", 3, true)
	waitForStablePods(t, ctx, kubeClient, namespace.Name, cluster.Name, "pooler", 1, false)
	waitForStableManagerPod(t, ctx, kubeClient)
	assertFailClosedApplicationServices(t, ctx, kubeClient, namespace.Name, cluster.Name)
	assertNoPostgreSQLWorkload(t, ctx, kubeClient, namespace.Name, cluster.Name)
	assertOrchestratorReadinessTracksLeaseIdentity(t, ctx, kubeClient, namespace.Name, cluster.Name)
	assertKINDWritableLeaseReplacementFailsClosed(t, ctx, kubeClient, current)
}

func assertKINDHAAuthorityDiscoveryFoundation(t *testing.T, ctx context.Context, kubeClient client.Client, cluster *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	if len(cluster.Status.PostgreSQLWritableLeases) != int(cluster.Spec.Shards) {
		t.Fatalf("writable-term Lease checkpoints = %#v", cluster.Status.PostgreSQLWritableLeases)
	}
	checkpoints := make(map[int32]pgshardv1alpha1.PostgreSQLWritableLeaseStatus, len(cluster.Status.PostgreSQLWritableLeases))
	for _, checkpoint := range cluster.Status.PostgreSQLWritableLeases {
		if checkpoint.Shard < 0 || checkpoint.Shard >= cluster.Spec.Shards || checkpoint.LeaseName != owned.PostgreSQLWritableLeaseName(cluster.Name, checkpoint.Shard) || checkpoint.LeaseUID == "" {
			t.Fatalf("invalid writable-term Lease checkpoint: %#v", checkpoint)
		}
		checkpoints[checkpoint.Shard] = checkpoint
	}

	topologyConfig := &corev1.ConfigMap{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + owned.TopologyConfigSuffix}, topologyConfig); err != nil {
		t.Fatalf("get topology ConfigMap: %v", err)
	}
	type topologyMember struct {
		Ordinal        int32  `json:"ordinal"`
		InstanceID     string `json:"instanceId"`
		DNSName        string `json:"dnsName"`
		PostgreSQLPort int32  `json:"postgresqlPort"`
		AgentHTTPPort  int32  `json:"agentHttpPort"`
		PhysicalSlot   string `json:"physicalSlot"`
	}
	type topologyShard struct {
		ID            int32  `json:"id"`
		Service       string `json:"service"`
		WritableLease struct {
			Namespace string    `json:"namespace"`
			Name      string    `json:"name"`
			UID       types.UID `json:"uid"`
		} `json:"writableLease"`
		Members []topologyMember `json:"members"`
	}
	var topology struct {
		SchemaVersion    string          `json:"schemaVersion"`
		Cluster          string          `json:"cluster"`
		ClusterObjectUID types.UID       `json:"clusterObjectUID"`
		Namespace        string          `json:"namespace"`
		Shards           []topologyShard `json:"shards"`
	}
	rawTopology := topologyConfig.Data["cluster.json"]
	if !json.Valid([]byte(rawTopology)) || strings.Contains(rawTopology, "\n  ") {
		t.Fatalf("topology is not compact valid JSON: %q", rawTopology)
	}
	if err := json.Unmarshal([]byte(rawTopology), &topology); err != nil {
		t.Fatalf("decode topology: %v", err)
	}
	if topology.SchemaVersion != "pgshard.topology.v1" || topology.Cluster != cluster.Name || topology.ClusterObjectUID != cluster.UID || topology.Namespace != cluster.Namespace || len(topology.Shards) != int(cluster.Spec.Shards) {
		t.Fatalf("topology identity/shape = %#v", topology)
	}
	for _, forbidden := range []string{`"role"`, `"primary"`, `"serving"`, `"ready"`} {
		if strings.Contains(rawTopology, forbidden) {
			t.Fatalf("topology contains runtime authority field %s: %s", forbidden, rawTopology)
		}
	}

	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		checkpoint, ok := checkpoints[shard]
		if !ok {
			t.Fatalf("missing writable-term Lease checkpoint for shard %d", shard)
		}
		liveLease := &coordinationv1.Lease{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: checkpoint.LeaseName}, liveLease); err != nil {
			t.Fatalf("get writable-term Lease for shard %d: %v", shard, err)
		}
		if liveLease.UID != checkpoint.LeaseUID || !metav1.IsControlledBy(liveLease, cluster) || !reflect.DeepEqual(liveLease.Spec, coordinationv1.LeaseSpec{}) {
			t.Fatalf("writable-term Lease for shard %d = %#v, checkpoint=%#v", shard, liveLease, checkpoint)
		}

		discovery := topology.Shards[shard]
		if discovery.ID != shard || discovery.Service != fmt.Sprintf("%s-shard-%04d", cluster.Name, shard) || discovery.WritableLease.Namespace != cluster.Namespace || discovery.WritableLease.Name != checkpoint.LeaseName || discovery.WritableLease.UID != checkpoint.LeaseUID || len(discovery.Members) != int(cluster.Spec.MembersPerShard) {
			t.Fatalf("topology discovery for shard %d = %#v", shard, discovery)
		}
		for member := int32(0); member < cluster.Spec.MembersPerShard; member++ {
			memberDiscovery := discovery.Members[member]
			instanceID := owned.PostgreSQLMemberStatefulSetName(cluster.Name, shard, member) + "-0"
			wantDNS := fmt.Sprintf("%s.%s-shard-%04d.%s.svc", instanceID, cluster.Name, shard, cluster.Namespace)
			if memberDiscovery.Ordinal != member || memberDiscovery.InstanceID != instanceID || memberDiscovery.DNSName != wantDNS || memberDiscovery.PostgreSQLPort != 5432 || memberDiscovery.AgentHTTPPort != 8080 || memberDiscovery.PhysicalSlot != fmt.Sprintf("pgshard_member_%04d", member) {
				t.Fatalf("topology discovery for shard %d member %d = %#v", shard, member, memberDiscovery)
			}
		}

		agentName := owned.PostgreSQLAgentServiceAccountName(cluster.Name, shard)
		serviceAccount := &corev1.ServiceAccount{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: agentName}, serviceAccount); err != nil {
			t.Fatalf("get PostgreSQL agent ServiceAccount for shard %d: %v", shard, err)
		}
		role := &rbacv1.Role{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: agentName}, role); err != nil {
			t.Fatalf("get PostgreSQL agent Role for shard %d: %v", shard, err)
		}
		binding := &rbacv1.RoleBinding{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: agentName}, binding); err != nil {
			t.Fatalf("get PostgreSQL agent RoleBinding for shard %d: %v", shard, err)
		}
		if serviceAccount.AutomountServiceAccountToken == nil || *serviceAccount.AutomountServiceAccountToken || len(role.Rules) != 1 || !reflect.DeepEqual(role.Rules[0].ResourceNames, []string{checkpoint.LeaseName}) || !reflect.DeepEqual(role.Rules[0].Verbs, []string{"get", "update"}) || binding.RoleRef.Name != agentName || len(binding.Subjects) != 1 || binding.Subjects[0].Name != agentName || binding.Subjects[0].Namespace != cluster.Namespace {
			t.Fatalf("PostgreSQL agent authority for shard %d is not exact: ServiceAccount=%#v Role=%#v RoleBinding=%#v", shard, serviceAccount, role, binding)
		}

		policy := &networkingv1.NetworkPolicy{}
		policyName := fmt.Sprintf("%s-shard-%04d-ingress", cluster.Name, shard)
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: policyName}, policy); err != nil {
			t.Fatalf("get PostgreSQL NetworkPolicy for shard %d: %v", shard, err)
		}
		diagnosticRuleFound := false
		for _, ingress := range policy.Spec.Ingress {
			if len(ingress.Ports) != 1 || ingress.Ports[0].Port == nil || ingress.Ports[0].Port.IntVal != 8080 {
				continue
			}
			if diagnosticRuleFound || len(ingress.From) != 1 || ingress.From[0].PodSelector == nil || ingress.From[0].NamespaceSelector != nil || ingress.From[0].IPBlock != nil || !maps.Equal(ingress.From[0].PodSelector.MatchLabels, map[string]string{owned.ClusterLabel: cluster.Name, owned.ComponentLabel: "orchestrator"}) {
				t.Fatalf("PostgreSQL diagnostic ingress for shard %d is broader than the orchestrator: %#v", shard, ingress)
			}
			diagnosticRuleFound = true
		}
		if !diagnosticRuleFound {
			t.Fatalf("PostgreSQL diagnostic ingress for shard %d is missing", shard)
		}
	}
}

func assertKINDWritableLeaseReplacementFailsClosed(t *testing.T, ctx context.Context, kubeClient client.Client, cluster *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	checkpoint := cluster.Status.PostgreSQLWritableLeases[0]
	liveLease := &coordinationv1.Lease{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: checkpoint.LeaseName}
	if err := kubeClient.Get(ctx, key, liveLease); err != nil {
		t.Fatalf("get writable-term Lease before replacement: %v", err)
	}
	uid := liveLease.UID
	resourceVersion := liveLease.ResourceVersion
	if err := kubeClient.Delete(ctx, liveLease, client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}); err != nil {
		t.Fatalf("delete writable-term Lease: %v", err)
	}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		err := kubeClient.Get(ctx, key, &coordinationv1.Lease{})
		return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
	}); err != nil {
		t.Fatalf("wait for writable-term Lease deletion: %v", err)
	}
	replacement := owned.PostgreSQLWritableLease(cluster, checkpoint.Shard)
	if err := kubeClient.Create(ctx, replacement); err != nil {
		t.Fatalf("create replacement writable-term Lease: %v", err)
	}
	if replacement.UID == "" || replacement.UID == checkpoint.LeaseUID {
		t.Fatalf("replacement writable-term Lease UID = %s, recorded UID = %s", replacement.UID, checkpoint.LeaseUID)
	}
	failed := &pgshardv1alpha1.PgShardCluster{}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 15*time.Second, true, func(ctx context.Context) (bool, error) {
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(cluster), failed); err != nil {
			return false, err
		}
		condition := meta.FindStatusCondition(failed.Status.Conditions, readyCondition)
		return failed.Status.Phase == "Degraded" && condition != nil && condition.Status == metav1.ConditionFalse && condition.Reason == "WritableLeaseReconcileFailed", nil
	}); err != nil {
		t.Fatalf("wait for replacement writable-term Lease to fail closed: %v; status=%#v", err, failed.Status)
	}
	if !reflect.DeepEqual(failed.Status.PostgreSQLWritableLeases, cluster.Status.PostgreSQLWritableLeases) {
		t.Fatalf("replacement writable-term Lease changed checkpoints: before=%#v after=%#v", cluster.Status.PostgreSQLWritableLeases, failed.Status.PostgreSQLWritableLeases)
	}
	assertNoPostgreSQLWorkload(t, ctx, kubeClient, cluster.Namespace, cluster.Name)
}

func TestKINDRestoreTopologyMismatchIsRejectedBeforeMutation(t *testing.T) {
	if os.Getenv("PGSHARD_KIND_MANAGER_E2E") != "true" {
		t.Skip("set PGSHARD_KIND_MANAGER_E2E=true against the installed admission manager")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	kubeClient := newKINDClient(t)

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pgshard-restore-preflight-%d", os.Getpid())}}
	if err := kubeClient.Create(ctx, namespace); err != nil {
		t.Fatal(err)
	}
	deleteNamespaceAtCleanup(t, kubeClient, namespace)
	sentinel := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "no-mutation-sentinel", Namespace: namespace.Name}, Data: map[string]string{"value": "unchanged"}}
	if err := kubeClient.Create(ctx, sentinel); err != nil {
		t.Fatal(err)
	}

	mismatch, _ := signedRestore(t, restoreTestTopology(5), restoreTestTopology(3))
	prepareLiveRestore(mismatch, namespace.Name)
	err := kubeClient.Create(ctx, mismatch)
	if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "RestoreTopologyMismatch") {
		t.Fatalf("five-to-three create error = %T %v, want API Invalid RestoreTopologyMismatch", err, err)
	}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(mismatch), &pgshardv1alpha1.PgShardRestore{}); !apierrors.IsNotFound(err) {
		t.Fatalf("mismatched restore persisted: %v", err)
	}
	assertRestoreNamespaceHasNoTargets(t, ctx, kubeClient, namespace.Name, sentinel, 0)

	boundaryMismatch, _ := signedRestore(t, restoreTestTopology(5), restoreTestTopology(5))
	prepareLiveRestore(boundaryMismatch, namespace.Name)
	boundaryMismatch.Name = "restore-boundary-mismatch"
	boundaryMismatch.Spec.DestinationTopology.Shards[0].End = "3689348814741910322"
	boundaryMismatch.Spec.DestinationTopology.Shards[1].Start = "3689348814741910322"
	err = kubeClient.Create(ctx, boundaryMismatch)
	if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "RestoreTopologyMismatch") {
		t.Fatalf("same-count boundary mismatch create error = %T %v, want API Invalid RestoreTopologyMismatch", err, err)
	}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(boundaryMismatch), &pgshardv1alpha1.PgShardRestore{}); !apierrors.IsNotFound(err) {
		t.Fatalf("boundary-mismatched restore persisted: %v", err)
	}
	assertRestoreNamespaceHasNoTargets(t, ctx, kubeClient, namespace.Name, sentinel, 0)

	exact, keySecret := signedRestore(t, restoreTestTopology(5), restoreTestTopology(5))
	prepareLiveRestore(exact, namespace.Name)
	keySecret.Namespace = namespace.Name
	keySecret.UID = ""
	keySecret.ResourceVersion = ""
	if err := kubeClient.Create(ctx, keySecret); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Create(ctx, exact); err != nil {
		t.Fatal(err)
	}
	current := &pgshardv1alpha1.PgShardRestore{}
	if err := wait.PollUntilContextTimeout(ctx, time.Second, time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(exact), current); err != nil {
			return false, err
		}
		condition := meta.FindStatusCondition(current.Status.Conditions, restorePreflightCondition)
		return condition != nil && condition.Status == metav1.ConditionUnknown && condition.Reason == "DestinationTopologyResolverUnavailable", nil
	}); err != nil {
		t.Fatalf("wait for request validation without destination evidence: %v; status=%#v", err, current.Status)
	}
	assertRestoreCondition(t, current, restoreReadyCondition, metav1.ConditionFalse, "DestinationTopologyResolverUnavailable")
	if current.Status.Phase != pgshardv1alpha1.RestorePhasePending || current.Status.VerificationKeyUID != keySecret.UID || current.Status.ManifestSHA256 == "" || current.Status.TopologySHA256 == "" || current.Status.DestinationTopologySHA256 != "" {
		t.Fatalf("unresolved live preflight status = %#v", current.Status)
	}

	reordered := current.DeepCopy()
	reordered.Spec.Manifest.Topology.Shards[0], reordered.Spec.Manifest.Topology.Shards[1] = reordered.Spec.Manifest.Topology.Shards[1], reordered.Spec.Manifest.Topology.Shards[0]
	reordered.Spec.DestinationTopology.Shards[0], reordered.Spec.DestinationTopology.Shards[1] = reordered.Spec.DestinationTopology.Shards[1], reordered.Spec.DestinationTopology.Shards[0]
	if err := kubeClient.Update(ctx, reordered); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "restore specification is immutable") {
		t.Fatalf("signed shard reorder update error = %T %v, want immutable API rejection", err, err)
	}
	pinnedKeyUID := current.Status.VerificationKeyUID
	if err := kubeClient.Delete(ctx, keySecret); err != nil {
		t.Fatal(err)
	}
	triggerRestoreReconcile(t, ctx, kubeClient, client.ObjectKeyFromObject(exact), "verification-key-missing")
	if err := wait.PollUntilContextTimeout(ctx, time.Second, time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(exact), current); err != nil {
			return false, err
		}
		condition := meta.FindStatusCondition(current.Status.Conditions, restorePreflightCondition)
		return condition != nil && condition.Status == metav1.ConditionUnknown && condition.Reason == "VerificationKeyUnavailable", nil
	}); err != nil {
		t.Fatalf("wait for missing pinned verification key: %v; status=%#v", err, current.Status)
	}
	if current.Status.VerificationKeyUID != pinnedKeyUID {
		t.Fatalf("missing verification key cleared pinned UID: %#v", current.Status)
	}
	replacementKey := keySecret.DeepCopy()
	replacementKey.UID = ""
	replacementKey.ResourceVersion = ""
	if err := kubeClient.Create(ctx, replacementKey); err != nil {
		t.Fatal(err)
	}
	if replacementKey.UID == pinnedKeyUID {
		t.Fatalf("replacement verification key reused UID %q", pinnedKeyUID)
	}
	triggerRestoreReconcile(t, ctx, kubeClient, client.ObjectKeyFromObject(exact), "verification-key-replaced")
	if err := wait.PollUntilContextTimeout(ctx, time.Second, time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(exact), current); err != nil {
			return false, err
		}
		condition := meta.FindStatusCondition(current.Status.Conditions, restorePreflightCondition)
		return condition != nil && condition.Status == metav1.ConditionFalse && condition.Reason == "VerificationKeyReplaced", nil
	}); err != nil {
		t.Fatalf("wait for replacement verification key rejection: %v; status=%#v", err, current.Status)
	}
	if current.Status.Phase != pgshardv1alpha1.RestorePhaseRejected || current.Status.VerificationKeyUID != pinnedKeyUID {
		t.Fatalf("replacement verification key rebound live restore: %#v", current.Status)
	}
	assertRestoreNamespaceHasNoTargets(t, ctx, kubeClient, namespace.Name, sentinel, 1)
}

func triggerRestoreReconcile(t *testing.T, ctx context.Context, kubeClient client.Client, key client.ObjectKey, value string) {
	t.Helper()
	restore := &pgshardv1alpha1.PgShardRestore{}
	if err := kubeClient.Get(ctx, key, restore); err != nil {
		t.Fatal(err)
	}
	if restore.Annotations == nil {
		restore.Annotations = make(map[string]string, 1)
	}
	restore.Annotations["test.pgshard.io/reconcile"] = value
	if err := kubeClient.Update(ctx, restore); err != nil {
		t.Fatal(err)
	}
}

func prepareLiveRestore(restore *pgshardv1alpha1.PgShardRestore, namespace string) {
	restore.Namespace = namespace
	restore.UID = ""
	restore.ResourceVersion = ""
	restore.Generation = 0
}

func assertRestoreNamespaceHasNoTargets(t *testing.T, ctx context.Context, kubeClient client.Client, namespace string, sentinel *corev1.ConfigMap, expectedSecrets int) {
	t.Helper()
	currentSentinel := &corev1.ConfigMap{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(sentinel), currentSentinel); err != nil {
		t.Fatal(err)
	}
	if !apiequality.Semantic.DeepEqual(currentSentinel.Data, sentinel.Data) {
		t.Fatalf("restore preflight changed sentinel data: %#v", currentSentinel.Data)
	}
	configMaps := &corev1.ConfigMapList{}
	if err := kubeClient.List(ctx, configMaps, client.InNamespace(namespace)); err != nil {
		t.Fatal(err)
	}
	for index := range configMaps.Items {
		name := configMaps.Items[index].Name
		if name != sentinel.Name && name != "kube-root-ca.crt" {
			t.Fatalf("restore preflight created unexpected ConfigMap %q: %#v", name, configMaps.Items)
		}
	}
	secrets := &corev1.SecretList{}
	if err := kubeClient.List(ctx, secrets, client.InNamespace(namespace)); err != nil {
		t.Fatal(err)
	}
	if len(secrets.Items) != expectedSecrets {
		t.Fatalf("restore preflight Secrets = %d, want caller-created %d: %#v", len(secrets.Items), expectedSecrets, secrets.Items)
	}
	for description, list := range map[string]client.ObjectList{
		"Clusters":     &pgshardv1alpha1.PgShardClusterList{},
		"PVCs":         &corev1.PersistentVolumeClaimList{},
		"Services":     &corev1.ServiceList{},
		"Jobs":         &batchv1.JobList{},
		"Deployments":  &appsv1.DeploymentList{},
		"StatefulSets": &appsv1.StatefulSetList{},
	} {
		if err := kubeClient.List(ctx, list, client.InNamespace(namespace)); err != nil {
			t.Fatal(err)
		}
		if meta.LenList(list) != 0 {
			t.Fatalf("restore preflight created %s: %#v", description, list)
		}
	}
}

func TestKINDManagerRunsSingleMemberPostgreSQL18Primaries(t *testing.T) {
	if os.Getenv("PGSHARD_KIND_MANAGER_E2E") != "true" {
		t.Skip("set PGSHARD_KIND_MANAGER_E2E=true against the installed admission manager")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
	defer cancel()
	kubeClient := newKINDClient(t)

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: fmt.Sprintf("pgshard-manager-postgresql-%d", os.Getpid()),
		Labels: map[string]string{
			"pod-security.kubernetes.io/enforce":         "restricted",
			"pod-security.kubernetes.io/enforce-version": "latest",
			podfence.NamespaceLabel:                      podfence.NamespaceLabelValue,
		},
	}}
	if err := kubeClient.Create(ctx, namespace); err != nil {
		t.Fatal(err)
	}
	deleteNamespaceAtCleanup(t, kubeClient, namespace)
	assertFencingNamespaceLabelImmutable(t, ctx, kubeClient, namespace.Name)

	cluster := readSingleMemberSample(t)
	cluster.Namespace = namespace.Name
	cluster.Spec.Databases = []pgshardv1alpha1.DatabaseTemplate{
		{Name: "app", Shards: 2, Cells: []int32{0, 1}},
		{Name: "analytics", Shards: 1, Cells: []int32{0}},
		{Name: "dedicated", Shards: 1, Cells: []int32{1}},
	}
	if err := kubeClient.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	waitForSingleMemberPostgreSQL(t, ctx, kubeClient, client.ObjectKeyFromObject(cluster))
	waitForPoolerCatalogTLS(t, ctx, kubeClient, namespace.Name, cluster.Name)
	assertPostgreSQLStatusMetadataImmutable(t, ctx, kubeClient, types.NamespacedName{
		Namespace: namespace.Name,
		Name:      owned.PostgreSQLShardStatefulSetName(cluster.Name, 0) + "-0",
	})
	assertPostgreSQLSpecImmutable(t, ctx, kubeClient, types.NamespacedName{
		Namespace: namespace.Name,
		Name:      owned.PostgreSQLShardStatefulSetName(cluster.Name, 0) + "-0",
	})
	current := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(cluster), current); err != nil {
		t.Fatal(err)
	}
	verifiedHandshake, handshakeErr := podfence.NewSecretHandshakeCodec(
		kubeClient,
		podfence.SecretReceiptKeyRef{
			Secret:           types.NamespacedName{Namespace: defaultPodFencingKeyNamespace, Name: defaultPodFencingKeySecret},
			DataKey:          defaultPodFencingKeyData,
			AnchorSecret:     types.NamespacedName{Namespace: defaultPodFencingKeyNamespace, Name: defaultPodFencingAnchorSecret},
			AnchorAnnotation: defaultPodFencingAnchorAnnotation,
		},
	).Verify(ctx, current)
	if handshakeErr != nil {
		t.Fatal(handshakeErr)
	}
	if !verifiedHandshake {
		t.Fatalf("PostgreSQL Pod fencing admission handshake = %#v", current.Annotations)
	}
	assertClusterFencingMetadataImmutable(t, ctx, kubeClient, client.ObjectKeyFromObject(current))
	shardZeroBootstrap := bootstrapForShard(t, current, 0)
	shardOneBootstrap := bootstrapForShard(t, current, 1)

	shardZeroPod := owned.PostgreSQLShardStatefulSetName(cluster.Name, 0) + "-0"
	shardOnePod := owned.PostgreSQLShardStatefulSetName(cluster.Name, 1) + "-0"
	postgresqlBootstrapImage := os.Getenv("PGSHARD_KIND_POSTGRES_BOOTSTRAP_IMAGE")
	if postgresqlBootstrapImage == "" {
		postgresqlBootstrapImage = "pgshard/postgres-agent:dev"
	}
	postgresqlBootstrapPullPolicy := corev1.PullIfNotPresent
	if postgresqlBootstrapImage == "pgshard/postgres-agent:dev" {
		postgresqlBootstrapPullPolicy = corev1.PullNever
	}
	initialShardZero := &corev1.Pod{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: shardZeroPod}, initialShardZero); err != nil {
		t.Fatal(err)
	}
	if len(initialShardZero.Spec.InitContainers) != 1 || initialShardZero.Spec.InitContainers[0].Image != postgresqlBootstrapImage || initialShardZero.Spec.InitContainers[0].ImagePullPolicy != postgresqlBootstrapPullPolicy || initialShardZero.Annotations["pgshard.io/shardschema-migration-sha256"] == "" {
		t.Fatalf("shard-0000 bootstrap image contract = %#v", initialShardZero)
	}
	if len(initialShardZero.Status.InitContainerStatuses) != 1 || initialShardZero.Status.InitContainerStatuses[0].ImageID == "" || initialShardZero.Status.InitContainerStatuses[0].RestartCount != 0 || initialShardZero.Status.InitContainerStatuses[0].State.Terminated == nil || initialShardZero.Status.InitContainerStatuses[0].State.Terminated.ExitCode != 0 {
		t.Fatalf("shard-0000 bootstrap completion = %#v", initialShardZero.Status.InitContainerStatuses)
	}
	configurationSourceMounts := 0
	configurationRuntimeMounts := 0
	configurationDigest := ""
	for _, variable := range initialShardZero.Spec.InitContainers[0].Env {
		if variable.Name == "PGSHARD_POSTGRESQL_CONFIG_SHA256" {
			configurationDigest = variable.Value
		}
	}
	if configurationDigest == "" || configurationDigest != initialShardZero.Annotations[owned.ConfigHashAnnotation] {
		t.Fatalf("shard-0000 authenticated configuration digest = %q, annotations = %#v", configurationDigest, initialShardZero.Annotations)
	}
	for _, mount := range initialShardZero.Spec.InitContainers[0].VolumeMounts {
		switch mount.MountPath {
		case "/etc/pgshard/postgresql-source":
			configurationSourceMounts++
			if mount.Name != "postgresql-config" || !mount.ReadOnly {
				t.Fatalf("shard-0000 configuration source mount = %#v", mount)
			}
		case "/etc/pgshard/postgresql":
			configurationRuntimeMounts++
			if mount.Name != "postgresql-runtime-config" || mount.ReadOnly {
				t.Fatalf("shard-0000 runtime configuration mount = %#v", mount)
			}
		}
	}
	if configurationSourceMounts != 1 || configurationRuntimeMounts != 1 {
		t.Fatalf("shard-0000 authenticated configuration mounts = source %d, runtime %d, want 1 each", configurationSourceMounts, configurationRuntimeMounts)
	}
	if got := strings.TrimSpace(runKubectl(t, ctx, "--namespace", namespace.Name, "exec", "--container", "postgresql", shardZeroPod, "--",
		"bash", "-ceu", "test -f /etc/pgshard/postgresql/database-genesis.sql && test ! -L /etc/pgshard/postgresql/database-genesis.sql && test -f /etc/pgshard/postgresql/database-topology-preflight.sql && test ! -L /etc/pgshard/postgresql/database-topology-preflight.sql")); got != "" {
		t.Fatalf("shard-0000 copied database topology check = %q", got)
	}
	if current.Status.CatalogAccess == nil || current.Status.CatalogAccess.SecretName == "" || current.Status.CatalogAccess.SecretUID == "" {
		t.Fatalf("catalog access identity was not checkpointed: %#v", current.Status.CatalogAccess)
	}
	for _, rejection := range []struct {
		name     string
		database string
		sslMode  string
	}{
		{name: "plaintext-catalog", database: "shardschema", sslMode: "disable"},
		{name: "other-database", database: "postgres", sslMode: "require"},
	} {
		assertCatalogLoginRejected(t, ctx, kubeClient, namespace.Name, cluster.Name,
			initialShardZero.Spec.Containers[0].Image, current.Status.CatalogAccess.SecretName,
			owned.CatalogServiceName(cluster.Name), rejection.name, rejection.database, rejection.sslMode)
	}
	if got := strings.TrimSpace(runKubectl(t, ctx, "--namespace", namespace.Name, "exec", "--container", "postgresql", shardZeroPod, "--",
		"psql", "-X", "-U", "postgres", "-d", "postgres", "-Atc",
		"SELECT current_setting('server_version_num')::integer / 10000, pg_is_in_recovery()")); got != "18|f" {
		t.Fatalf("PostgreSQL identity = %q", got)
	}
	if got, want := strings.TrimSpace(runKubectl(t, ctx, "--namespace", namespace.Name, "exec", "--container", "postgresql", shardZeroPod, "--",
		"cat", "/var/lib/postgresql/18/docker/.pgshard-bootstrap-complete")), "cluster_uid="+string(current.UID)+"\nshard=0000"; got != want {
		t.Fatalf("PostgreSQL bootstrap marker = %q, want %q", got, want)
	}
	if got := strings.TrimSpace(runKubectl(t, ctx, "--namespace", namespace.Name, "exec", "--container", "postgresql", shardZeroPod, "--",
		"psql", "-X", "-U", "postgres", "-d", "shardschema", "-Atc",
		"SELECT (SELECT string_agg(shard_id::text || ':' || shard_number::text || ':' || state, ',' ORDER BY shard_number) FROM pgshard_catalog.shards), (SELECT count(*) FROM pgshard_catalog.shard_restore_incarnations WHERE state = 'active')")); got != "shard-0000:0:active,shard-0001:1:active|2" {
		t.Fatalf("shardschema inventory = %q", got)
	}
	if got := strings.TrimSpace(runKubectl(t, ctx, "--namespace", namespace.Name, "exec", "--container", "postgresql", shardOnePod, "--",
		"psql", "-X", "-U", "postgres", "-d", "postgres", "-Atc",
		"SELECT count(*) FROM pg_catalog.pg_database WHERE datname = 'shardschema'")); got != "0" {
		t.Fatalf("non-home shard shardschema database count = %q", got)
	}
	databaseSnapshot := strings.TrimSpace(runKubectl(t, ctx, "--namespace", namespace.Name, "exec", "--container", "postgresql", shardZeroPod, "--",
		"psql", "-X", "-U", "postgres", "-d", "shardschema", "-Atc",
		"SELECT string_agg(database_name::text || '=' || logical_database_id::text, ',' ORDER BY database_name) FROM pgshard_catalog.logical_databases"))
	databaseSnapshotParts := strings.Split(databaseSnapshot, ",")
	if len(databaseSnapshotParts) != 3 || !strings.HasPrefix(databaseSnapshotParts[0], "analytics=") || !strings.HasPrefix(databaseSnapshotParts[1], "app=") || !strings.HasPrefix(databaseSnapshotParts[2], "dedicated=") || strings.HasSuffix(databaseSnapshotParts[0], "=") || strings.HasSuffix(databaseSnapshotParts[1], "=") || strings.HasSuffix(databaseSnapshotParts[2], "=") {
		t.Fatalf("database genesis identity snapshot = %q", databaseSnapshot)
	}
	if got := strings.TrimSpace(runKubectl(t, ctx, "--namespace", namespace.Name, "exec", "--container", "postgresql", shardZeroPod, "--",
		"psql", "-X", "-U", "postgres", "-d", "shardschema", "-Atc",
		"SELECT string_agg(databases.database_name::text || ':' || ranges.range_start::text || ':' || shards.shard_number::text, ',' ORDER BY databases.database_name, ranges.range_start) FROM pgshard_catalog.logical_databases AS databases JOIN pgshard_catalog.active_routing_epochs AS active ON active.logical_database_id = databases.logical_database_id JOIN pgshard_catalog.routing_ranges AS ranges ON ranges.logical_database_id = active.logical_database_id AND ranges.routing_epoch = active.routing_epoch JOIN pgshard_catalog.database_shard_placements AS placements ON placements.logical_database_id = ranges.logical_database_id AND placements.database_shard_id = ranges.database_shard_id AND placements.state = 'active' JOIN pgshard_catalog.shards AS shards ON shards.shard_id = placements.shard_id")); got != "analytics:0:0,app:0:0,app:9223372036854775808:1,dedicated:0:1" {
		t.Fatalf("database genesis routing = %q", got)
	}
	catalogSnapshot := strings.TrimSpace(runKubectl(t, ctx, "--namespace", namespace.Name, "exec", "--container", "postgresql", shardZeroPod, "--",
		"psql", "-X", "-U", "postgres", "-d", "shardschema", "-Atc",
		"SELECT state.catalog_epoch, string_agg(incarnations.shard_id::text || '=' || incarnations.restore_incarnation::text, ',' ORDER BY incarnations.shard_id) FROM pgshard_catalog.cluster_state AS state CROSS JOIN pgshard_catalog.shard_restore_incarnations AS incarnations WHERE state.singleton AND incarnations.state = 'active' GROUP BY state.catalog_epoch"))
	snapshotFields := strings.SplitN(catalogSnapshot, "|", 2)
	if len(snapshotFields) != 2 || snapshotFields[1] == "" {
		t.Fatalf("invalid pre-restart shardschema snapshot = %q", catalogSnapshot)
	}
	if epoch, err := strconv.ParseUint(snapshotFields[0], 10, 64); err != nil || epoch == 0 {
		t.Fatalf("invalid pre-restart catalog epoch = %q: %v", snapshotFields[0], err)
	}
	runKubectl(t, ctx, "--namespace", namespace.Name, "exec", "--container", "postgresql", shardZeroPod, "--",
		"psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "postgres", "-c",
		"CREATE TABLE live_marker (shard integer PRIMARY KEY, note text NOT NULL); INSERT INTO live_marker VALUES (0, 'kind-persistent');")
	runKubectl(t, ctx, "--namespace", namespace.Name, "exec", "--container", "postgresql", shardZeroPod, "--",
		"psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "postgres", "-c",
		"SELECT pg_catalog.pg_create_logical_replication_slot('pgshard_kind_restart', 'pgoutput');")
	runKubectl(t, ctx, "--namespace", namespace.Name, "exec", "--container", "postgresql", shardZeroPod, "--",
		"psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "postgres", "-c",
		"BEGIN; INSERT INTO live_marker VALUES (99, 'prepared-restart'); PREPARE TRANSACTION 'pgshard_kind_restart';")
	service := cluster.Name + "-shard-0000"
	shardZeroSecret := &corev1.Secret{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: shardZeroBootstrap.SecretName}, shardZeroSecret); err != nil {
		t.Fatal(err)
	}
	shardOneSecret := &corev1.Secret{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: shardOneBootstrap.SecretName}, shardOneSecret); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(shardZeroSecret.Data[owned.PostgreSQLPasswordKey], shardOneSecret.Data[owned.PostgreSQLPasswordKey]) {
		t.Fatal("different shards received the same PostgreSQL credential")
	}
	shardOne := &corev1.Pod{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: shardOnePod}, shardOne); err != nil {
		t.Fatal(err)
	}
	if got := runPostgreSQLServiceQuery(t, ctx, kubeClient, namespace.Name, cluster.Name, shardOne.Spec.Containers[0].Image, shardZeroBootstrap.SecretName, service, "SELECT note FROM live_marker WHERE shard = 0"); got != "kind-persistent" {
		t.Fatalf("cross-shard-service query = %q", got)
	}
	poolerService := cluster.Name + "-rw"
	if got := runPostgreSQLServiceQuery(t, ctx, kubeClient, namespace.Name, cluster.Name, shardOne.Spec.Containers[0].Image, shardZeroBootstrap.SecretName, poolerService, "SELECT note FROM live_marker WHERE shard = 0"); got != "kind-persistent" {
		t.Fatalf("shard-zero compatibility relay query = %q", got)
	}
	assertUnsupportedApplicationServicesHaveNoEndpoints(t, ctx, kubeClient, namespace.Name, cluster.Name)
	assertPoolerCatalogDatabaseRejected(t, ctx, kubeClient, namespace.Name, cluster.Name, shardOne.Spec.Containers[0].Image, shardZeroBootstrap.SecretName, poolerService)
	assertPostgreSQLServiceQueryDenied(t, ctx, kubeClient, namespace.Name, "unlabeled", nil, shardOne.Spec.Containers[0].Image, shardZeroBootstrap.SecretName, service)
	assertPostgreSQLServiceQueryDenied(t, ctx, kubeClient, namespace.Name, "wrong-cluster", map[string]string{
		owned.ClusterLabel:   "another-cluster",
		owned.ComponentLabel: "pooler",
	}, shardOne.Spec.Containers[0].Image, shardZeroBootstrap.SecretName, service)
	if got := runPostgreSQLServiceQuery(t, ctx, kubeClient, namespace.Name, cluster.Name, shardOne.Spec.Containers[0].Image, shardZeroBootstrap.SecretName, service, "SELECT note FROM live_marker WHERE shard = 0"); got != "kind-persistent" {
		t.Fatalf("authorized query after denied clients = %q", got)
	}

	before := &corev1.Pod{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: shardZeroPod}, before); err != nil {
		t.Fatal(err)
	}
	oldShardZeroHash := before.Annotations[owned.ConfigHashAnnotation]
	oldShardOneHash := shardOne.Annotations[owned.ConfigHashAnnotation]
	if oldShardZeroHash == "" || oldShardOneHash == "" {
		t.Fatalf("PostgreSQL Pods lack configuration hashes: shard-0000=%q shard-0001=%q", oldShardZeroHash, oldShardOneHash)
	}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &pgshardv1alpha1.PgShardCluster{}
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(cluster), latest); err != nil {
			return err
		}
		latest.Spec.PostgreSQL.Parameters = maps.Clone(latest.Spec.PostgreSQL.Parameters)
		if latest.Spec.PostgreSQL.Parameters == nil {
			latest.Spec.PostgreSQL.Parameters = make(map[string]string, 1)
		}
		latest.Spec.PostgreSQL.Parameters["log_statement"] = "ddl"
		return kubeClient.Update(ctx, latest)
	}); err != nil {
		t.Fatalf("publish desired PostgreSQL configuration: %v", err)
	}
	var desiredShardZeroHash, desiredShardOneHash string
	if err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, time.Minute, true, func(ctx context.Context) (bool, error) {
		shardZeroStatefulSet := &appsv1.StatefulSet{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: owned.PostgreSQLShardStatefulSetName(cluster.Name, 0)}, shardZeroStatefulSet); err != nil {
			return false, err
		}
		shardOneStatefulSet := &appsv1.StatefulSet{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: owned.PostgreSQLShardStatefulSetName(cluster.Name, 1)}, shardOneStatefulSet); err != nil {
			return false, err
		}
		desiredShardZeroHash = shardZeroStatefulSet.Spec.Template.Annotations[owned.ConfigHashAnnotation]
		desiredShardOneHash = shardOneStatefulSet.Spec.Template.Annotations[owned.ConfigHashAnnotation]
		if desiredShardZeroHash == "" || desiredShardOneHash == "" || desiredShardZeroHash == oldShardZeroHash || desiredShardOneHash == oldShardOneHash {
			return false, nil
		}
		currentShardZero := &corev1.Pod{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: shardZeroPod}, currentShardZero); err != nil {
			return false, err
		}
		currentShardOne := &corev1.Pod{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: shardOnePod}, currentShardOne); err != nil {
			return false, err
		}
		if currentShardZero.UID != before.UID || currentShardOne.UID != shardOne.UID || currentShardZero.Annotations[owned.ConfigHashAnnotation] != oldShardZeroHash || currentShardOne.Annotations[owned.ConfigHashAnnotation] != oldShardOneHash {
			return false, fmt.Errorf("OnDelete configuration publication restarted a PostgreSQL Pod: shard-0000=%s/%s shard-0001=%s/%s", currentShardZero.UID, currentShardZero.Annotations[owned.ConfigHashAnnotation], currentShardOne.UID, currentShardOne.Annotations[owned.ConfigHashAnnotation])
		}
		return true, nil
	}); err != nil {
		t.Fatalf("wait for inert OnDelete PostgreSQL template update: %v", err)
	}
	runKubectl(t, ctx, "--namespace", namespace.Name, "delete", "pod", shardZeroPod, "--wait=false")
	waitForRecreatedReadyPod(t, ctx, kubeClient, types.NamespacedName{Namespace: namespace.Name, Name: shardZeroPod}, before.UID)
	recreatedShardZero := &corev1.Pod{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: shardZeroPod}, recreatedShardZero); err != nil {
		t.Fatal(err)
	}
	untouchedShardOne := &corev1.Pod{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: shardOnePod}, untouchedShardOne); err != nil {
		t.Fatal(err)
	}
	if recreatedShardZero.UID == before.UID || recreatedShardZero.Annotations[owned.ConfigHashAnnotation] != desiredShardZeroHash {
		t.Fatalf("explicit shard-0000 restart did not adopt desired template: UID=%s hash=%q, old UID=%s desired hash=%q", recreatedShardZero.UID, recreatedShardZero.Annotations[owned.ConfigHashAnnotation], before.UID, desiredShardZeroHash)
	}
	if untouchedShardOne.UID != shardOne.UID || untouchedShardOne.Annotations[owned.ConfigHashAnnotation] != oldShardOneHash || desiredShardOneHash == oldShardOneHash {
		t.Fatalf("shard-0001 changed during shard-0000 rollout: UID=%s hash=%q, old UID=%s old hash=%q desired hash=%q", untouchedShardOne.UID, untouchedShardOne.Annotations[owned.ConfigHashAnnotation], shardOne.UID, oldShardOneHash, desiredShardOneHash)
	}
	if got := strings.TrimSpace(runKubectl(t, ctx, "--namespace", namespace.Name, "exec", "--container", "postgresql", shardZeroPod, "--",
		"psql", "-X", "-U", "postgres", "-d", "postgres", "-Atc", "SELECT note FROM live_marker WHERE shard = 0")); got != "kind-persistent" {
		t.Fatalf("query after StatefulSet restart = %q", got)
	}
	waitForPoolerCatalogTLS(t, ctx, kubeClient, namespace.Name, cluster.Name)
	if got := runPostgreSQLServiceQuery(t, ctx, kubeClient, namespace.Name, cluster.Name, shardOne.Spec.Containers[0].Image, shardZeroBootstrap.SecretName, poolerService, "SELECT note FROM live_marker WHERE shard = 0"); got != "kind-persistent" {
		t.Fatalf("compatibility relay query after shard-zero restart = %q", got)
	}
	if got := strings.TrimSpace(runKubectl(t, ctx, "--namespace", namespace.Name, "exec", "--container", "postgresql", shardZeroPod, "--",
		"psql", "-X", "-U", "postgres", "-d", "shardschema", "-Atc",
		"SELECT state.catalog_epoch, string_agg(incarnations.shard_id::text || '=' || incarnations.restore_incarnation::text, ',' ORDER BY incarnations.shard_id) FROM pgshard_catalog.cluster_state AS state CROSS JOIN pgshard_catalog.shard_restore_incarnations AS incarnations WHERE state.singleton AND incarnations.state = 'active' GROUP BY state.catalog_epoch")); got != catalogSnapshot {
		t.Fatalf("idempotent shardschema restart changed snapshot from %q to %q", catalogSnapshot, got)
	}
	if got := strings.TrimSpace(runKubectl(t, ctx, "--namespace", namespace.Name, "exec", "--container", "postgresql", shardZeroPod, "--",
		"psql", "-X", "-U", "postgres", "-d", "shardschema", "-Atc",
		"SELECT string_agg(database_name::text || '=' || logical_database_id::text, ',' ORDER BY database_name) FROM pgshard_catalog.logical_databases")); got != databaseSnapshot {
		t.Fatalf("idempotent shard-zero restart changed database identities from %q to %q", databaseSnapshot, got)
	}
	if got := strings.TrimSpace(runKubectl(t, ctx, "--namespace", namespace.Name, "exec", "--container", "postgresql", shardZeroPod, "--",
		"psql", "-X", "-U", "postgres", "-d", "postgres", "-Atc",
		"SELECT (SELECT count(*) FROM pg_catalog.pg_prepared_xacts WHERE gid = 'pgshard_kind_restart'), (SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = 'pgshard_kind_restart' AND slot_type = 'logical' AND NOT active)")); got != "1|1" {
		t.Fatalf("restart lost prepared transaction or logical slot: %q", got)
	}
	runKubectl(t, ctx, "--namespace", namespace.Name, "exec", "--container", "postgresql", shardZeroPod, "--",
		"psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "postgres", "-c",
		"ROLLBACK PREPARED 'pgshard_kind_restart';")
	runKubectl(t, ctx, "--namespace", namespace.Name, "exec", "--container", "postgresql", shardZeroPod, "--",
		"psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "postgres", "-c",
		"SELECT pg_catalog.pg_drop_replication_slot('pgshard_kind_restart');")

	managerRestored := false
	t.Cleanup(func() {
		if managerRestored {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		for _, arguments := range [][]string{
			{"--namespace", "pgshard-system", "scale", "deployment/pgshard-controller-manager", "--replicas=1"},
			{"--namespace", "pgshard-system", "rollout", "status", "deployment/pgshard-controller-manager", "--timeout=120s"},
		} {
			output, err := exec.CommandContext(cleanupCtx, "kubectl", arguments...).CombinedOutput()
			if err != nil {
				t.Errorf("restore manager with kubectl %s: %v\n%s", strings.Join(arguments, " "), err, output)
			}
		}
	})
	runKubectl(t, ctx, "--namespace", "pgshard-system", "scale", "deployment/pgshard-controller-manager", "--replicas=0")
	waitForManagerReplicas(t, ctx, kubeClient, 0)
	beforeForceDelete := &corev1.Pod{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: shardZeroPod}, beforeForceDelete); err != nil {
		t.Fatal(err)
	}
	if !contains(beforeForceDelete.Finalizers, owned.PostgreSQLPodTerminationFinalizer) {
		t.Fatalf("PostgreSQL Pod lacks its termination fence: %q", beforeForceDelete.Finalizers)
	}
	boundNode := &corev1.Node{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Name: beforeForceDelete.Spec.NodeName}, boundNode); err != nil {
		t.Fatal(err)
	}
	if beforeForceDelete.Annotations[podfence.NodeUIDAnnotation] != string(boundNode.UID) || beforeForceDelete.Annotations[podfence.NodeBootIDAnnotation] != boundNode.Status.NodeInfo.BootID {
		t.Fatalf("PostgreSQL Pod binding identity = %#v, node = %#v", beforeForceDelete.Annotations, boundNode.ObjectMeta)
	}
	if beforeForceDelete.Annotations[owned.PostgreSQLPodClusterUIDAnnotation] != string(current.UID) {
		t.Fatalf("PostgreSQL Pod binding cluster identity = %#v, want %s", beforeForceDelete.Annotations, current.UID)
	}
	for key, value := range map[string]string{
		owned.ManagedByLabel: owned.ManagedByValue, owned.ComponentLabel: "postgresql", owned.ClusterLabel: cluster.Name,
		owned.ShardLabel: "0000", owned.RoleLabel: "primary", owned.MemberLabel: "0000",
	} {
		if beforeForceDelete.Labels[key] != value {
			t.Fatalf("PostgreSQL Pod binding label %s = %q, want %q", key, beforeForceDelete.Labels[key], value)
		}
	}
	zeroGrace := int64(0)
	if err := kubeClient.Delete(ctx, beforeForceDelete, &client.DeleteOptions{GracePeriodSeconds: &zeroGrace}); err != nil {
		t.Fatal(err)
	}
	terminating := &corev1.Pod{}
	err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		terminating = &corev1.Pod{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: shardZeroPod}, terminating); err != nil {
			return false, err
		}
		if terminating.DeletionTimestamp == nil {
			return false, nil
		}
		if !contains(terminating.Finalizers, owned.PostgreSQLPodTerminationFinalizer) || podHasTerminalPhase(terminating) || podfence.HasTerminationAttestation(terminating) {
			return false, fmt.Errorf("PostgreSQL Pod escaped its fail-closed webhook-outage fence: %#v", terminating)
		}
		return false, nil
	})
	if err == nil || !wait.Interrupted(err) {
		t.Fatalf("force-deleted PostgreSQL Pod outage observation ended unexpectedly: %v; last Pod = %#v", err, terminating)
	}
	forceDeleteClaim := &corev1.PersistentVolumeClaim{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: shardZeroBootstrap.PVCName}, forceDeleteClaim); err != nil {
		t.Fatal(err)
	}
	if !postgresqlDataPVCIsProtected(forceDeleteClaim) || forceDeleteClaim.Annotations[owned.RetainedFromAnnotation] != "" {
		t.Fatalf("force deletion released PostgreSQL data before process termination was reconciled: %#v", forceDeleteClaim.ObjectMeta)
	}
	runKubectl(t, ctx, "--namespace", "pgshard-system", "scale", "deployment/pgshard-controller-manager", "--replicas=1")
	runKubectl(t, ctx, "--namespace", "pgshard-system", "rollout", "status", "deployment/pgshard-controller-manager", "--timeout=120s")
	waitForManagerReplicas(t, ctx, kubeClient, 1)
	managerRestored = true
	waitForRecreatedReadyPod(t, ctx, kubeClient, types.NamespacedName{Namespace: namespace.Name, Name: shardZeroPod}, beforeForceDelete.UID)
	if got := strings.TrimSpace(runKubectl(t, ctx, "--namespace", namespace.Name, "exec", "--container", "postgresql", shardZeroPod, "--",
		"psql", "-X", "-U", "postgres", "-d", "postgres", "-Atc", "SELECT note FROM live_marker WHERE shard = 0")); got != "kind-persistent" {
		t.Fatalf("query after force-deleted Pod recovery = %q", got)
	}

	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(cluster), current); err != nil {
		t.Fatal(err)
	}
	bootstraps := append([]pgshardv1alpha1.PostgreSQLBootstrapStatus(nil), current.Status.PostgreSQLBootstraps...)
	for _, bootstrap := range bootstraps {
		claim := &corev1.PersistentVolumeClaim{}
		key := types.NamespacedName{Namespace: namespace.Name, Name: bootstrap.PVCName}
		if err := kubeClient.Get(ctx, key, claim); err != nil {
			t.Fatal(err)
		}
		secret := &corev1.Secret{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: bootstrap.SecretName}, secret); err != nil {
			t.Fatal(err)
		}
		if secret.UID != bootstrap.SecretUID || !bootstrap.PVCFenceDetached || !postgresqlCredentialIsDataAnchored(secret, bootstrap) || len(claim.OwnerReferences) != 0 || !postgresqlDataPVCIsProtected(claim) || claim.UID != bootstrap.PVCUID {
			t.Fatalf("PostgreSQL stabilized data fence: secret=%#v claim=%#v bootstrap=%#v", secret.ObjectMeta, claim.ObjectMeta, bootstrap)
		}
	}
	if err := kubeClient.Delete(ctx, current, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil {
		t.Fatal(err)
	}
	err = wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		err := kubeClient.Get(ctx, client.ObjectKeyFromObject(cluster), &pgshardv1alpha1.PgShardCluster{})
		return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
	})
	if err != nil {
		t.Fatalf("wait for foreground PgShardCluster deletion: %v", err)
	}
	for _, bootstrap := range bootstraps {
		claim := &corev1.PersistentVolumeClaim{}
		key := types.NamespacedName{Namespace: namespace.Name, Name: bootstrap.PVCName}
		if err := kubeClient.Get(ctx, key, claim); err != nil {
			t.Fatalf("foreground deletion removed retained PostgreSQL data PVC %s: %v", bootstrap.PVCName, err)
		}
		if claim.UID != bootstrap.PVCUID || claim.Annotations[owned.RetainedFromAnnotation] != namespace.Name+"/"+cluster.Name || postgresqlDataPVCIsProtected(claim) {
			t.Fatalf("retained PostgreSQL data PVC identity = %#v, want UID %s", claim.ObjectMeta, bootstrap.PVCUID)
		}
		if len(claim.OwnerReferences) != 0 {
			t.Fatalf("retained PostgreSQL data PVC still has its creation fence: %#v", claim.OwnerReferences)
		}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: bootstrap.SecretName}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
			t.Fatalf("credential creation fence survived Retain finalization: %v", err)
		}
	}
}

func waitForPoolerCatalogTLS(t *testing.T, ctx context.Context, kubeClient client.Client, namespace, cluster string) {
	t.Helper()
	type catalogStatus struct {
		Phase                 string  `json:"phase"`
		ConnectionUp          bool    `json:"connection_up"`
		Ready                 bool    `json:"ready"`
		ReadinessReason       string  `json:"readiness_reason"`
		CatalogEpoch          *string `json:"catalog_epoch"`
		SuccessfulConnections string  `json:"successful_connections"`
		LastFailure           *string `json:"last_failure"`
	}
	type poolerStatus struct {
		Ready   bool          `json:"ready"`
		Catalog catalogStatus `json:"catalog"`
	}
	statusPath := fmt.Sprintf("/api/v1/namespaces/%s/services/http:%s-pooler:http/proxy/status", namespace, cluster)
	metricsPath := fmt.Sprintf("/api/v1/namespaces/%s/services/http:%s-pooler:http/proxy/metrics", namespace, cluster)
	readinessPath := fmt.Sprintf("/api/v1/namespaces/%s/services/http:%s-pooler:http/proxy/readyz", namespace, cluster)
	var lastOutput string
	var lastErr error
	err := wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		output, err := exec.CommandContext(ctx, "kubectl", "get", "--raw", statusPath).CombinedOutput()
		lastOutput, lastErr = string(output), err
		if err != nil {
			return false, nil
		}
		var status poolerStatus
		if err := json.Unmarshal(output, &status); err != nil {
			lastErr = err
			return false, nil
		}
		if !status.Ready || status.Catalog.Phase != "connected" || !status.Catalog.ConnectionUp || !status.Catalog.Ready || status.Catalog.ReadinessReason != "ready" || status.Catalog.CatalogEpoch == nil || status.Catalog.LastFailure != nil {
			lastErr = fmt.Errorf("catalog status not ready: %#v", status)
			return false, nil
		}
		catalogEpoch, epochErr := strconv.ParseUint(*status.Catalog.CatalogEpoch, 10, 64)
		successfulConnections, connectionErr := strconv.ParseUint(status.Catalog.SuccessfulConnections, 10, 64)
		if epochErr != nil || strconv.FormatUint(catalogEpoch, 10) != *status.Catalog.CatalogEpoch ||
			connectionErr != nil || successfulConnections == 0 ||
			strconv.FormatUint(successfulConnections, 10) != status.Catalog.SuccessfulConnections {
			lastErr = fmt.Errorf("catalog status counters are not canonical: %#v", status.Catalog)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("wait for pooler authenticated catalog TLS: %v; last error = %v; last output = %q", err, lastErr, lastOutput)
	}
	metrics, err := exec.CommandContext(ctx, "kubectl", "get", "--raw", metricsPath).CombinedOutput()
	if err != nil {
		t.Fatalf("read pooler metrics through Service proxy: %v\n%s", err, metrics)
	}
	for _, sample := range []string{
		"pgshard_pooler_ready 1",
		"pgshard_pooler_catalog_ready 1",
		"pgshard_pooler_catalog_connection_up 1",
	} {
		if !strings.Contains(string(metrics), sample) {
			t.Fatalf("pooler metrics lack %q:\n%s", sample, metrics)
		}
	}
	readiness, err := exec.CommandContext(ctx, "kubectl", "get", "--raw", readinessPath).CombinedOutput()
	if err != nil || strings.TrimSpace(string(readiness)) != `{"ready":true,"reason":"ready"}` {
		t.Fatalf("pooler /readyz = error %v, output %q; want ready compatibility relay", err, readiness)
	}
}

func assertClusterFencingMetadataImmutable(t *testing.T, ctx context.Context, kubeClient client.Client, key types.NamespacedName) {
	t.Helper()
	for _, test := range []struct {
		name   string
		mutate func(*pgshardv1alpha1.PgShardCluster)
		want   string
	}{
		{
			name: "removal",
			mutate: func(cluster *pgshardv1alpha1.PgShardCluster) {
				delete(cluster.Annotations, podfence.HandshakeChallengeAnnotation)
				delete(cluster.Annotations, podfence.HandshakeReceiptAnnotation)
			},
			want: "preserved or replaced",
		},
		{
			name: "replacement",
			mutate: func(cluster *pgshardv1alpha1.PgShardCluster) {
				cluster.Annotations[podfence.HandshakeChallengeAnnotation] = "forged-challenge"
				cluster.Annotations[podfence.HandshakeReceiptAnnotation] = "forged-receipt"
			},
			want: "only be established or repaired by the pgshard controller",
		},
	} {
		t.Run("cluster fencing metadata "+test.name+" is denied", func(t *testing.T) {
			err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				cluster := &pgshardv1alpha1.PgShardCluster{}
				if err := kubeClient.Get(ctx, key, cluster); err != nil {
					return err
				}
				test.mutate(cluster)
				return kubeClient.Update(ctx, cluster)
			})
			if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("fencing metadata %s error = %v, want webhook denial", test.name, err)
			}
		})
	}
}

func assertFencingNamespaceLabelImmutable(t *testing.T, ctx context.Context, kubeClient client.Client, name string) {
	t.Helper()
	for _, test := range []struct {
		name   string
		update func(*corev1.Namespace) error
	}{
		{name: "main resource", update: func(namespace *corev1.Namespace) error { return kubeClient.Update(ctx, namespace) }},
		{name: "status subresource", update: func(namespace *corev1.Namespace) error { return kubeClient.Status().Update(ctx, namespace) }},
	} {
		t.Run("namespace label is immutable through "+test.name, func(t *testing.T) {
			current := &corev1.Namespace{}
			if err := kubeClient.Get(ctx, types.NamespacedName{Name: name}, current); err != nil {
				t.Fatal(err)
			}
			delete(current.Labels, podfence.NamespaceLabel)
			err := test.update(current)
			if !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), "immutable once PostgreSQL Pod fencing is enabled") {
				t.Fatalf("fencing namespace label removal error = %v, want webhook denial", err)
			}
		})
	}
	current := &corev1.Namespace{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Name: name}, current); err != nil {
		t.Fatal(err)
	}
	if current.Labels[podfence.NamespaceLabel] != podfence.NamespaceLabelValue {
		t.Fatalf("namespace fencing label changed despite webhook denials: %#v", current.Labels)
	}
}

func assertPostgreSQLStatusMetadataImmutable(t *testing.T, ctx context.Context, kubeClient client.Client, key types.NamespacedName) {
	t.Helper()
	for _, test := range []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{name: "managed label", mutate: func(pod *corev1.Pod) { delete(pod.Labels, owned.ManagedByLabel) }},
		{name: "cluster UID annotation", mutate: func(pod *corev1.Pod) { delete(pod.Annotations, owned.PostgreSQLPodClusterUIDAnnotation) }},
		{name: "runtime annotation", mutate: func(pod *corev1.Pod) { delete(pod.Annotations, owned.PostgreSQLRuntimeAnnotation) }},
		{name: "termination finalizer", mutate: func(pod *corev1.Pod) { pod.Finalizers = nil }},
	} {
		t.Run("status protects "+test.name, func(t *testing.T) {
			err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				current := &corev1.Pod{}
				if err := kubeClient.Get(ctx, key, current); err != nil {
					return err
				}
				test.mutate(current)
				return kubeClient.Status().Update(ctx, current)
			})
			if !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), "identity changed during a status update") {
				t.Fatalf("protected PostgreSQL status metadata removal error = %v, want webhook denial", err)
			}
		})
	}
	current := &corev1.Pod{}
	if err := kubeClient.Get(ctx, key, current); err != nil {
		t.Fatal(err)
	}
	if !podfence.IsManagedPostgreSQLPod(current) {
		t.Fatalf("PostgreSQL Pod identity changed despite status webhook denials: %#v", current.ObjectMeta)
	}
	baselineUID := current.UID
	baselineRuntime, hasBaselineRuntime := current.Annotations[owned.PostgreSQLRuntimeAnnotation]
	if !hasBaselineRuntime || baselineRuntime == "" {
		t.Fatalf("PostgreSQL Pod lacks its runtime identity before ordinary webhook denial: %#v", current.ObjectMeta)
	}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1.Pod{}
		if err := kubeClient.Get(ctx, key, latest); err != nil {
			return err
		}
		delete(latest.Annotations, owned.PostgreSQLRuntimeAnnotation)
		return kubeClient.Update(ctx, latest)
	})
	if !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("ordinary update removed PostgreSQL runtime identity: %v, want webhook denial", err)
	}
	stored := &corev1.Pod{}
	if err := kubeClient.Get(ctx, key, stored); err != nil {
		t.Fatal(err)
	}
	storedRuntime, hasStoredRuntime := stored.Annotations[owned.PostgreSQLRuntimeAnnotation]
	if stored.UID != baselineUID || !hasStoredRuntime || storedRuntime != baselineRuntime || !podfence.IsManagedPostgreSQLPod(stored) {
		t.Fatalf("PostgreSQL Pod identity changed despite ordinary webhook denial: %#v", stored.ObjectMeta)
	}
}

func assertPostgreSQLSpecImmutable(t *testing.T, ctx context.Context, kubeClient client.Client, key types.NamespacedName) {
	t.Helper()
	baseline := &corev1.Pod{}
	if err := kubeClient.Get(ctx, key, baseline); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		update func(*corev1.Pod) error
	}{
		{
			name: "main resource",
			update: func(pod *corev1.Pod) error {
				pod.Spec.Containers[0].Image = "invalid.example/pgshard-denied:latest"
				return kubeClient.Update(ctx, pod)
			},
		},
		{
			name: "ephemeralcontainers subresource",
			update: func(pod *corev1.Pod) error {
				pod.Spec.EphemeralContainers = append(pod.Spec.EphemeralContainers, corev1.EphemeralContainer{
					EphemeralContainerCommon: corev1.EphemeralContainerCommon{
						Name:            "pgshard-denied-debug",
						Image:           pod.Spec.Containers[0].Image,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Command:         []string{"sleep", "3600"},
						SecurityContext: pod.Spec.Containers[0].SecurityContext.DeepCopy(),
					},
				})
				return kubeClient.SubResource("ephemeralcontainers").Update(ctx, pod)
			},
		},
		{
			name: "resize subresource",
			update: func(pod *corev1.Pod) error {
				pod.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("300m")
				return kubeClient.SubResource("resize").Update(ctx, pod)
			},
		},
	} {
		t.Run("spec is immutable through "+test.name, func(t *testing.T) {
			err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				current := &corev1.Pod{}
				if err := kubeClient.Get(ctx, key, current); err != nil {
					return err
				}
				return test.update(current)
			})
			if !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), "spec and generation are immutable") {
				t.Fatalf("PostgreSQL Pod %s update error = %v, want webhook denial", test.name, err)
			}
			stored := &corev1.Pod{}
			if err := kubeClient.Get(ctx, key, stored); err != nil {
				t.Fatal(err)
			}
			if stored.Generation != baseline.Generation || !apiequality.Semantic.DeepEqual(stored.Spec, baseline.Spec) {
				t.Fatalf("PostgreSQL Pod changed after denied %s update: generation %d -> %d", test.name, baseline.Generation, stored.Generation)
			}
		})
	}
}

func TestKINDManagerRejectsPostgreSQLRuntimeChange(t *testing.T) {
	if os.Getenv("PGSHARD_KIND_MANAGER_E2E") != "true" {
		t.Skip("set PGSHARD_KIND_MANAGER_E2E=true against the installed direct-runtime manager")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	kubeClient := newKINDClient(t)

	managerKey := types.NamespacedName{Namespace: "pgshard-system", Name: "pgshard-controller-manager"}
	manager := &appsv1.Deployment{}
	if err := kubeClient.Get(ctx, managerKey, manager); err != nil {
		t.Fatal(err)
	}
	if len(manager.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("manager containers = %#v", manager.Spec.Template.Spec.Containers)
	}
	initialArgs := append([]string(nil), manager.Spec.Template.Spec.Containers[0].Args...)
	for _, argument := range initialArgs {
		if strings.HasPrefix(argument, "--postgresql-runtime=") {
			t.Fatalf("runtime transition test requires the direct default manager, got %q", argument)
		}
	}

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: fmt.Sprintf("pgshard-runtime-contract-%d", os.Getpid()),
		Labels: map[string]string{
			"pod-security.kubernetes.io/enforce":         "restricted",
			"pod-security.kubernetes.io/enforce-version": "latest",
			podfence.NamespaceLabel:                      podfence.NamespaceLabelValue,
		},
	}}
	if err := kubeClient.Create(ctx, namespace); err != nil {
		t.Fatal(err)
	}
	deleteNamespaceAtCleanup(t, kubeClient, namespace)

	cluster := readSingleMemberSample(t)
	cluster.Name = "runtime-contract"
	cluster.Namespace = namespace.Name
	cluster.Spec.Shards = 1
	cluster.Spec.Databases = nil
	if err := kubeClient.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	waitForSingleMemberPostgreSQL(t, ctx, kubeClient, client.ObjectKeyFromObject(cluster))

	statefulSetKey := types.NamespacedName{Namespace: namespace.Name, Name: owned.PostgreSQLShardStatefulSetName(cluster.Name, 0)}
	statefulSet := &appsv1.StatefulSet{}
	if err := kubeClient.Get(ctx, statefulSetKey, statefulSet); err != nil {
		t.Fatal(err)
	}
	initialTemplate := statefulSet.Spec.Template.DeepCopy()
	podKey := types.NamespacedName{Namespace: namespace.Name, Name: statefulSet.Name + "-0"}
	pod := &corev1.Pod{}
	if err := kubeClient.Get(ctx, podKey, pod); err != nil {
		t.Fatal(err)
	}
	initialPodUID := pod.UID
	initialContainerID := pod.Status.ContainerStatuses[0].ContainerID
	initialRestarts := pod.Status.ContainerStatuses[0].RestartCount

	managerRestored := false
	t.Cleanup(func() {
		if managerRestored {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		if err := replaceManagerArguments(cleanupCtx, kubeClient, initialArgs); err != nil {
			t.Errorf("restore direct manager arguments: %v", err)
			return
		}
		output, err := exec.CommandContext(cleanupCtx, "kubectl", "--namespace", "pgshard-system", "rollout", "status", "deployment/pgshard-controller-manager", "--timeout=120s").CombinedOutput()
		if err != nil {
			t.Errorf("wait for restored direct manager: %v\n%s", err, output)
		}
	})

	agentArgs := append(append([]string(nil), initialArgs...), "--postgresql-runtime=agent-quarantine")
	if err := replaceManagerArguments(ctx, kubeClient, agentArgs); err != nil {
		t.Fatal(err)
	}
	runKubectl(t, ctx, "--namespace", "pgshard-system", "rollout", "status", "deployment/pgshard-controller-manager", "--timeout=120s")
	waitForManagerReplicas(t, ctx, kubeClient, 1)

	current := &pgshardv1alpha1.PgShardCluster{}
	if err := wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(cluster), current); err != nil {
			return false, err
		}
		condition := meta.FindStatusCondition(current.Status.Conditions, reconciledCondition)
		return condition != nil && condition.Status == metav1.ConditionFalse && condition.Reason == "PostgreSQLRuntimeChangeRejected", nil
	}); err != nil {
		t.Fatalf("wait for creation-time runtime rejection: %v; status=%#v", err, current.Status)
	}
	if err := kubeClient.Get(ctx, statefulSetKey, statefulSet); err != nil {
		t.Fatal(err)
	}
	if !apiequality.Semantic.DeepEqual(statefulSet.Spec.Template, *initialTemplate) {
		t.Fatal("runtime flag change mutated the direct OnDelete StatefulSet template")
	}
	if err := kubeClient.Get(ctx, podKey, pod); err != nil {
		t.Fatal(err)
	}
	if pod.UID != initialPodUID || len(pod.Status.ContainerStatuses) != 1 || pod.Status.ContainerStatuses[0].ContainerID != initialContainerID || pod.Status.ContainerStatuses[0].RestartCount != initialRestarts || pod.Status.ContainerStatuses[0].State.Running == nil {
		t.Fatalf("runtime flag change replaced or restarted the direct Pod: %#v", pod.Status.ContainerStatuses)
	}

	if err := replaceManagerArguments(ctx, kubeClient, initialArgs); err != nil {
		t.Fatal(err)
	}
	runKubectl(t, ctx, "--namespace", "pgshard-system", "rollout", "status", "deployment/pgshard-controller-manager", "--timeout=120s")
	waitForManagerReplicas(t, ctx, kubeClient, 1)
	managerRestored = true
}

func TestKINDManagerRunsAgentQuarantine(t *testing.T) {
	if os.Getenv("PGSHARD_KIND_MANAGER_E2E") != "true" {
		t.Skip("set PGSHARD_KIND_MANAGER_E2E=true against an admission manager in agent-quarantine mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	kubeClient := newKINDClient(t)

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: fmt.Sprintf("pgshard-manager-agent-%d", os.Getpid()),
		Labels: map[string]string{
			"pod-security.kubernetes.io/enforce":         "restricted",
			"pod-security.kubernetes.io/enforce-version": "latest",
			podfence.NamespaceLabel:                      podfence.NamespaceLabelValue,
		},
	}}
	if err := kubeClient.Create(ctx, namespace); err != nil {
		t.Fatal(err)
	}
	deleteNamespaceAtCleanup(t, kubeClient, namespace)

	haCluster := readDevelopmentSample(t)
	haCluster.Name = "agent-quarantine-ha"
	haCluster.Namespace = namespace.Name
	haCluster.Spec.Shards = 1
	haCluster.Spec.Databases = nil
	// Keep all three PostgreSQL members schedulable on the single KIND worker,
	// including while a deleted standby is being replaced. The development
	// sample's production-oriented one-CPU request leaves no room for that
	// replacement alongside KIND's platform and pgshard workloads.
	haCluster.Spec.PostgreSQL.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("250m"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("1"),
			corev1.ResourceMemory: resource.MustParse("2Gi"),
		},
	}
	if err := kubeClient.Create(ctx, haCluster); err != nil {
		t.Fatal(err)
	}
	haCurrent := &pgshardv1alpha1.PgShardCluster{}
	if err := wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(haCluster), haCurrent); err != nil {
			return false, err
		}
		if len(haCurrent.Status.PostgreSQLBootstraps) != int(haCluster.Spec.MembersPerShard) || len(haCurrent.Status.PostgreSQLReplicationCredentials) != 1 ||
			len(haCurrent.Status.PostgreSQLCatalogCandidates) != int(haCluster.Spec.MembersPerShard) {
			return false, nil
		}
		recorded := haCurrent.Status.PostgreSQLReplicationCredentials[0]
		catalogAccess := haCurrent.Status.CatalogAccess
		if recorded.Shard != 0 || recorded.SecretUID == "" || !validCatalogAccessDigest(recorded.MaterialSHA256) ||
			catalogAccess == nil || catalogAccess.SecretUID == "" || !validCatalogAccessDigest(catalogAccess.ClientSHA256) || !validCatalogAccessDigest(catalogAccess.ServerSHA256) {
			return false, nil
		}
		for member, checkpoint := range haCurrent.Status.PostgreSQLCatalogCandidates {
			if checkpoint.Member != int32(member) || checkpoint.ConfigMapName != owned.PostgreSQLCatalogCandidateConfigMapName(haCluster.Name, int32(member)) ||
				checkpoint.ConfigMapUID == "" || !validCatalogAccessDigest(checkpoint.PayloadSHA256) {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		t.Fatalf("wait for complete multi-member catalog candidate status: %v; status=%#v", err, haCurrent.Status)
	}
	recordedReplication := &haCurrent.Status.PostgreSQLReplicationCredentials[0]
	replicationSecret := &corev1.Secret{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: recordedReplication.SecretName}, replicationSecret); err != nil {
		t.Fatal(err)
	}
	if err := validateCheckpointedPostgreSQLReplicationCredential(replicationSecret, haCurrent, recordedReplication); err != nil {
		t.Fatal(err)
	}
	replicationPassword, hasReplicationPassword := replicationSecret.Data[owned.PostgreSQLReplicationPasswordKey]
	immutable := replicationSecret.Immutable != nil && *replicationSecret.Immutable
	if !immutable || len(replicationSecret.Data) != 1 || !hasReplicationPassword || len(replicationPassword) != hex.EncodedLen(postgresqlPasswordBytes) {
		t.Fatalf("staged multi-member replication Secret metadata: immutable=%t dataKeys=%d hasPassword=%t passwordBytes=%d uid=%q", immutable, len(replicationSecret.Data), hasReplicationPassword, len(replicationPassword), replicationSecret.UID)
	}
	catalogAccessSecret := &corev1.Secret{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: haCurrent.Status.CatalogAccess.SecretName}, catalogAccessSecret); err != nil {
		t.Fatal(err)
	}
	if err := validateCheckpointedCatalogAccess(catalogAccessSecret, haCurrent, haCurrent.Status.CatalogAccess); err != nil {
		t.Fatal(err)
	}
	sourceName := owned.PostgreSQLMemberStatefulSetName(haCluster.Name, 0, 0)
	source := &appsv1.StatefulSet{}
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: sourceName}, source)
		return err == nil, client.IgnoreNotFound(err)
	}); err != nil {
		t.Fatalf("wait for replication bootstrap source: %v", err)
	}
	if source.Spec.Template.Labels[owned.MemberLabel] != "0000" {
		t.Fatalf("replication bootstrap source member labels = %#v", source.Spec.Template.Labels)
	}
	if _, role := source.Spec.Template.Labels[owned.RoleLabel]; role {
		t.Fatalf("replication bootstrap source received a serving role: %#v", source.Spec.Template.Labels)
	}
	sourceAgent := source.Spec.Template.Spec.Containers[0]
	if agentEnvironmentValue(sourceAgent.Env, "PGSHARD_POSTGRES_MODE") != "replication-bootstrap-primary" ||
		agentEnvironmentValue(sourceAgent.Env, "PGSHARD_POSTGRES_HBA_FILE") != "/etc/pgshard/replication-bootstrap-primary.pg_hba.conf" ||
		agentEnvironmentValue(sourceAgent.Env, "PGSHARD_POSTGRES_GENERATION_DURABILITY") != "remote-apply-any-one" ||
		agentEnvironmentValue(sourceAgent.Env, "PGSHARD_POSTGRES_SYNCHRONOUS_STANDBY_NAMES") != "pgshard_member_0001,pgshard_member_0002" ||
		source.Spec.Template.Annotations[owned.PostgreSQLGenerationDurabilityAnnotation] != "remote-apply-any-one" ||
		source.Spec.Template.Annotations[owned.PostgreSQLSynchronousStandbysAnnotation] != "pgshard_member_0001,pgshard_member_0002" {
		t.Fatalf("replication bootstrap source environment = %#v", sourceAgent.Env)
	}
	if podContainerHasNamedVolumeMount(sourceAgent.VolumeMounts, "replication-credential") {
		t.Fatalf("replication bootstrap source agent retained the replication credential: %#v", sourceAgent.VolumeMounts)
	}
	sourceBootstrap := source.Spec.Template.Spec.InitContainers[0]
	if !podContainerHasVolumeMount(sourceBootstrap.VolumeMounts, "replication-credential", true) ||
		agentEnvironmentValue(sourceBootstrap.Env, "PGSHARD_MEMBERS_PER_SHARD") != "3" ||
		agentEnvironmentValue(sourceBootstrap.Env, "PGSHARD_REPLICATION_MATERIAL_SHA256") != recordedReplication.MaterialSHA256 {
		t.Fatalf("replication bootstrap source initialization = %#v", sourceBootstrap)
	}
	replicationProjection := podVolumeByName(t, source.Spec.Template.Spec.Volumes, "replication-credential").Secret
	if replicationProjection == nil || replicationProjection.SecretName != recordedReplication.SecretName || replicationProjection.DefaultMode == nil || *replicationProjection.DefaultMode != 0o440 || !reflect.DeepEqual(projectedSecretItemKeys(replicationProjection.Items), []string{owned.PostgreSQLReplicationPasswordKey}) {
		t.Fatalf("replication bootstrap source projection = %#v", replicationProjection)
	}
	sourcePodName := sourceName + "-0"
	sourcePod := &corev1.Pod{}
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: sourcePodName}, sourcePod); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		return sourcePod.Status.Phase == corev1.PodRunning && len(sourcePod.Status.ContainerStatuses) == 1 && sourcePod.Status.ContainerStatuses[0].State.Running != nil, nil
	}); err != nil {
		t.Fatalf("wait for replication bootstrap source Pod: %v; Pod=%#v", err, sourcePod)
	}
	if !podfence.IsManagedPostgreSQLPod(sourcePod) || sourcePod.Spec.NodeName == "" ||
		sourcePod.Annotations[podfence.NodeUIDAnnotation] == "" || sourcePod.Annotations[podfence.NodeBootIDAnnotation] == "" {
		t.Fatalf("replication bootstrap source lacks fenced binding identity: %#v", sourcePod.ObjectMeta)
	}
	sourceKey := types.NamespacedName{Namespace: namespace.Name, Name: sourcePodName}
	assertPostgreSQLStatusMetadataImmutable(t, ctx, kubeClient, sourceKey)
	assertPostgreSQLSpecImmutable(t, ctx, kubeClient, sourceKey)
	if podReady(sourcePod) || sourcePod.Status.ContainerStatuses[0].Ready {
		t.Fatalf("replication bootstrap source became routable: conditions=%#v containers=%#v", sourcePod.Status.Conditions, sourcePod.Status.ContainerStatuses)
	}
	var sourceStatus struct {
		PostgresProcess string `json:"postgres_process"`
	}
	sourceStatusPath := fmt.Sprintf("/api/v1/namespaces/%s/pods/http:%s:8080/proxy/status", namespace.Name, sourcePodName)
	if err := wait.PollUntilContextTimeout(ctx, time.Second, time.Minute, true, func(ctx context.Context) (bool, error) {
		output, err := exec.CommandContext(ctx, "kubectl", "get", "--raw", sourceStatusPath).CombinedOutput()
		return err == nil && json.Unmarshal(output, &sourceStatus) == nil && sourceStatus.PostgresProcess == "running_replication_bootstrap", nil
	}); err != nil {
		t.Fatalf("wait for running replication bootstrap source: %v; status=%#v", err, sourceStatus)
	}
	replicationState := strings.TrimSpace(runKubectl(
		t,
		ctx,
		"--namespace", namespace.Name,
		"exec", sourcePodName,
		"--container=postgresql",
		"--",
		"psql", "-X", "--no-password", "--host=/run/pgshard/postgres", "--username=postgres", "--dbname=postgres", "--no-align", "--tuples-only",
		"--command=SELECT CASE WHEN rolcanlogin AND rolreplication AND NOT rolsuper AND NOT rolinherit AND NOT rolcreaterole AND NOT rolcreatedb AND NOT rolbypassrls AND rolpassword LIKE 'SCRAM-SHA-256$4096:%' THEN 'safe' ELSE 'unsafe' END FROM pg_catalog.pg_authid WHERE rolname = 'pgshard_replication'; SELECT string_agg(slot_name, ',' ORDER BY slot_name) FROM pg_catalog.pg_replication_slots WHERE slot_type = 'physical';",
	))
	if replicationState != "safe\npgshard_member_0001,pgshard_member_0002" {
		t.Fatalf("replication bootstrap materialized state = %q", replicationState)
	}
	assertKINDPhysicalStandbys(t, ctx, kubeClient, haCurrent, sourcePodName)
	assertKINDOrchestratorObservationRBAC(t, ctx, haCurrent)
	assertKINDOrchestratorBindsControllerEndpoints(t, ctx, kubeClient, haCurrent)
	assertKINDSynchronousGenerationWaitsForRemoteReplay(t, ctx, kubeClient, haCurrent, sourcePodName)
	assertKINDCatalogCandidateConfigurationsInert(t, ctx, kubeClient, haCurrent)
	assertKINDCatalogCandidateReplacementFailsClosed(t, ctx, kubeClient, haCurrent)

	cluster := readSingleMemberSample(t)
	cluster.Name = "agent-quarantine"
	cluster.Namespace = namespace.Name
	cluster.Spec.Shards = 1
	cluster.Spec.Databases = nil
	if err := kubeClient.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}

	key := client.ObjectKeyFromObject(cluster)
	current := &pgshardv1alpha1.PgShardCluster{}
	var checkpoint pgshardv1alpha1.PostgreSQLWritableLeaseStatus
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := kubeClient.Get(ctx, key, current); err != nil {
			return false, err
		}
		if len(current.Status.PostgreSQLWritableLeases) != 1 || len(current.Status.PostgreSQLBootstraps) != 1 {
			return false, nil
		}
		checkpoint = current.Status.PostgreSQLWritableLeases[0]
		return checkpoint.Shard == 0 && checkpoint.LeaseName == owned.PostgreSQLWritableLeaseName(cluster.Name, 0) && checkpoint.LeaseUID != "", nil
	}); err != nil {
		t.Fatalf("wait for exact writable-term Lease checkpoint: %v; status=%#v", err, current.Status)
	}

	statefulSetName := owned.PostgreSQLShardStatefulSetName(cluster.Name, 0)
	statefulSet := &appsv1.StatefulSet{}
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: statefulSetName}, statefulSet)
		return err == nil, client.IgnoreNotFound(err)
	}); err != nil {
		t.Fatalf("wait for agent-quarantine StatefulSet: %v", err)
	}
	template := statefulSet.Spec.Template.Spec
	if template.ServiceAccountName != owned.PostgreSQLAgentServiceAccountName(cluster.Name, 0) || template.AutomountServiceAccountToken == nil || *template.AutomountServiceAccountToken || len(template.Containers) != 1 {
		t.Fatalf("agent-quarantine Pod identity = %#v", template)
	}
	agent := template.Containers[0]
	if agent.Image != "pgshard/postgres-agent:dev" || agent.ImagePullPolicy != corev1.PullNever || agent.StartupProbe == nil || agent.LivenessProbe == nil || agent.ReadinessProbe == nil || agent.ReadinessProbe.HTTPGet == nil || agent.ReadinessProbe.HTTPGet.Path != "/readyz" {
		t.Fatalf("agent-quarantine container = %#v", agent)
	}
	if agentEnvironmentValue(agent.Env, "PGSHARD_WRITABLE_LEASE_NAME") != checkpoint.LeaseName || agentEnvironmentValue(agent.Env, "PGSHARD_WRITABLE_LEASE_UID") != string(checkpoint.LeaseUID) || agentEnvironmentValue(agent.Env, "PGSHARD_POSTGRES_MODE") != "quarantine" {
		t.Fatalf("agent-quarantine exact Lease environment = %#v", agent.Env)
	}
	apiVolume := podVolumeByName(t, template.Volumes, "kubernetes-api").Projected
	if apiVolume == nil || apiVolume.DefaultMode == nil || *apiVolume.DefaultMode != 0o440 || len(apiVolume.Sources) != 3 || apiVolume.Sources[0].ServiceAccountToken == nil || apiVolume.Sources[0].ServiceAccountToken.ExpirationSeconds == nil || *apiVolume.Sources[0].ServiceAccountToken.ExpirationSeconds != 600 || apiVolume.Sources[0].ServiceAccountToken.Audience != "" {
		t.Fatalf("agent-quarantine API projection = %#v", apiVolume)
	}
	if apiVolume.Sources[1].ConfigMap == nil || apiVolume.Sources[1].ConfigMap.Name != "kube-root-ca.crt" || apiVolume.Sources[2].DownwardAPI == nil {
		t.Fatalf("agent-quarantine namespace trust projection = %#v", apiVolume.Sources)
	}
	for _, mount := range agent.VolumeMounts {
		if mount.Name == "runtime" && mount.MountPath != "/run/pgshard" {
			t.Fatalf("agent runtime mount bypasses private child creation: %#v", mount)
		}
		if mount.Name == "bootstrap-secret" || mount.Name == "catalog-server-tls" || mount.Name == "catalog-bootstrap-auth" {
			t.Fatalf("running agent received bootstrap or catalog credentials: %#v", agent.VolumeMounts)
		}
	}

	podName := statefulSetName + "-0"
	pod := &corev1.Pod{}
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: podName}, pod); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		return pod.Status.Phase == corev1.PodRunning && len(pod.Status.ContainerStatuses) == 1 && pod.Status.ContainerStatuses[0].State.Running != nil, nil
	}); err != nil {
		t.Fatalf("wait for agent-quarantine Pod: %v; Pod=%#v", err, pod)
	}

	liveLease := &coordinationv1.Lease{}
	if err := wait.PollUntilContextTimeout(ctx, time.Second, time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: checkpoint.LeaseName}, liveLease); err != nil {
			return false, err
		}
		return liveLease.Spec.HolderIdentity != nil && liveLease.Spec.LeaseTransitions != nil && *liveLease.Spec.LeaseTransitions > 0 && liveLease.Spec.RenewTime != nil, nil
	}); err != nil {
		t.Fatalf("wait for agent Lease acquisition: %v; Lease=%#v", err, liveLease)
	}
	if liveLease.UID != checkpoint.LeaseUID || liveLease.Spec.LeaseDurationSeconds == nil || *liveLease.Spec.LeaseDurationSeconds != 15 {
		t.Fatalf("agent Lease identity/timing = %#v, checkpoint=%#v", liveLease, checkpoint)
	}
	holderParts := strings.Split(*liveLease.Spec.HolderIdentity, "/")
	if len(holderParts) != 3 || holderParts[0] != podName || holderParts[1] != string(pod.UID) || len(holderParts[2]) != 24 || strings.IndexFunc(holderParts[2], func(character rune) bool {
		return !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f')
	}) >= 0 {
		t.Fatalf("agent Lease holder does not bind stable member, Pod UID, and process incarnation: %q", *liveLease.Spec.HolderIdentity)
	}

	type agentStatus struct {
		Identity *struct {
			ClusterID  string `json:"cluster_id"`
			InstanceID string `json:"instance_id"`
		} `json:"identity"`
		PostgresProcess string `json:"postgres_process"`
		Lease           *struct {
			OwnerInstance string `json:"owner_instance"`
			Epoch         string `json:"epoch"`
		} `json:"lease"`
	}
	statusPath := fmt.Sprintf("/api/v1/namespaces/%s/pods/http:%s:8080/proxy/status", namespace.Name, podName)
	var observed agentStatus
	var lastStatusOutput string
	if err := wait.PollUntilContextTimeout(ctx, time.Second, time.Minute, true, func(ctx context.Context) (bool, error) {
		output, err := exec.CommandContext(ctx, "kubectl", "get", "--raw", statusPath).CombinedOutput()
		lastStatusOutput = string(output)
		if err != nil || json.Unmarshal(output, &observed) != nil {
			return false, nil
		}
		return observed.Identity != nil && observed.Lease != nil && observed.PostgresProcess == "running_quarantined", nil
	}); err != nil {
		t.Fatalf("wait for agent quarantine status: %v; last output=%q", err, lastStatusOutput)
	}
	if observed.Identity.ClusterID != cluster.Name || observed.Identity.InstanceID != podName || observed.Lease.OwnerInstance != podName || observed.Lease.Epoch != strconv.FormatInt(int64(*liveLease.Spec.LeaseTransitions), 10) {
		t.Fatalf("agent status does not match Kubernetes identity: status=%#v Lease=%#v", observed, liveLease.Spec)
	}
	assertKINDOrchestratorBindsControllerEndpoints(t, ctx, kubeClient, current)

	waitForQuarantinedPostgreSQL(t, ctx, namespace.Name, podName, "initial acquisition")
	assertDurableWritableGeneration(
		t,
		ctx,
		namespace.Name,
		podName,
		cluster.Name,
		current.UID,
		checkpoint,
		*liveLease.Spec.HolderIdentity,
		*liveLease.Spec.LeaseTransitions,
	)
	if output, err := exec.CommandContext(ctx, "kubectl", "--namespace", namespace.Name, "exec", podName, "--container=postgresql", "--", "pg_isready", "--quiet", "--host=127.0.0.1", "--port=5432").CombinedOutput(); err == nil {
		t.Fatalf("agent quarantine exposed PostgreSQL TCP: %s", output)
	}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: podName}, pod); err != nil {
		t.Fatal(err)
	}
	if podReady(pod) || pod.Status.ContainerStatuses[0].Ready {
		t.Fatalf("agent quarantine became routable: conditions=%#v containers=%#v", pod.Status.Conditions, pod.Status.ContainerStatuses)
	}

	initialPodUID := pod.UID
	initialRestarts := pod.Status.ContainerStatuses[0].RestartCount
	initialContainerID := pod.Status.ContainerStatuses[0].ContainerID
	initialStartedAt := pod.Status.ContainerStatuses[0].State.Running.StartedAt
	initialHolder := *liveLease.Spec.HolderIdentity
	initialTerm := *liveLease.Spec.LeaseTransitions
	managerRestored := false
	t.Cleanup(func() {
		if managerRestored {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		for _, arguments := range [][]string{
			{"--namespace", "pgshard-system", "scale", "deployment/pgshard-controller-manager", "--replicas=1"},
			{"--namespace", "pgshard-system", "rollout", "status", "deployment/pgshard-controller-manager", "--timeout=120s"},
		} {
			output, err := exec.CommandContext(cleanupCtx, "kubectl", arguments...).CombinedOutput()
			if err != nil {
				t.Errorf("restore manager with kubectl %s: %v\n%s", strings.Join(arguments, " "), err, output)
			}
		}
	})

	runKubectl(t, ctx, "--namespace", "pgshard-system", "scale", "deployment/pgshard-controller-manager", "--replicas=0")
	waitForManagerReplicas(t, ctx, kubeClient, 0)
	bindingKey := types.NamespacedName{Namespace: namespace.Name, Name: owned.PostgreSQLAgentServiceAccountName(cluster.Name, 0)}
	var bindingAbsentSince time.Time
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		binding := &rbacv1.RoleBinding{}
		err := kubeClient.Get(ctx, bindingKey, binding)
		if apierrors.IsNotFound(err) {
			if bindingAbsentSince.IsZero() {
				bindingAbsentSince = time.Now()
			}
			return time.Since(bindingAbsentSince) >= time.Second, nil
		}
		if err != nil {
			return false, err
		}
		bindingAbsentSince = time.Time{}
		uid := binding.UID
		resourceVersion := binding.ResourceVersion
		if err := kubeClient.Delete(ctx, binding, client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		return false, nil
	}); err != nil {
		t.Fatalf("remove agent Lease permission after manager shutdown: %v", err)
	}
	serviceAccountIdentity := "system:serviceaccount:" + namespace.Name + ":" + bindingKey.Name
	for _, verb := range []string{"get", "update"} {
		output, _ := exec.CommandContext(ctx, "kubectl", "auth", "can-i", verb, "lease/"+checkpoint.LeaseName, "--namespace", namespace.Name, "--as="+serviceAccountIdentity).CombinedOutput()
		if got := strings.TrimSpace(string(output)); got != "no" {
			t.Fatalf("agent can %s its Lease after RoleBinding removal: %q", verb, got)
		}
	}

	var fenced agentStatus
	var lastFenceOutput string
	if err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		output, err := exec.CommandContext(ctx, "kubectl", "get", "--raw", statusPath).CombinedOutput()
		lastFenceOutput = string(output)
		fenced = agentStatus{}
		if err != nil || json.Unmarshal(output, &fenced) != nil || fenced.Lease != nil || (fenced.PostgresProcess != "fenced" && fenced.PostgresProcess != "validated") {
			return false, nil
		}
		output, err = exec.CommandContext(ctx, "kubectl", "--namespace", namespace.Name, "exec", podName, "--container=postgresql", "--", "pg_isready", "--quiet", "--host=/run/pgshard/postgres", "--port=5432").CombinedOutput()
		lastFenceOutput += string(output)
		return err != nil, nil
	}); err != nil {
		t.Fatalf("wait for authorization-loss PostgreSQL fence: %v; last output=%q status=%#v", err, lastFenceOutput, fenced)
	}
	healthPath := fmt.Sprintf("/api/v1/namespaces/%s/pods/http:%s:8080/proxy/healthz", namespace.Name, podName)
	for observation := 0; observation < 4; observation++ {
		if output, err := exec.CommandContext(ctx, "kubectl", "get", "--raw", healthPath).CombinedOutput(); err != nil {
			t.Fatalf("agent HTTP health stopped after PostgreSQL fencing: %v\n%s", err, output)
		}
		time.Sleep(2 * time.Second)
	}

	runKubectl(t, ctx, "--namespace", "pgshard-system", "scale", "deployment/pgshard-controller-manager", "--replicas=1")
	runKubectl(t, ctx, "--namespace", "pgshard-system", "rollout", "status", "deployment/pgshard-controller-manager", "--timeout=120s")
	waitForManagerReplicas(t, ctx, kubeClient, 1)
	managerRestored = true
	var recoveredLease coordinationv1.Lease
	if err := wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: checkpoint.LeaseName}, &recoveredLease); err != nil {
			return false, err
		}
		return recoveredLease.Spec.LeaseTransitions != nil && *recoveredLease.Spec.LeaseTransitions > initialTerm && recoveredLease.Spec.HolderIdentity != nil && *recoveredLease.Spec.HolderIdentity != initialHolder && recoveredLease.Spec.RenewTime != nil, nil
	}); err != nil {
		t.Fatalf("wait for a fresh term after Lease permission recovery: %v; Lease=%#v", err, recoveredLease)
	}
	var recovered agentStatus
	if err := wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, time.Minute, true, func(ctx context.Context) (bool, error) {
		output, err := exec.CommandContext(ctx, "kubectl", "get", "--raw", statusPath).CombinedOutput()
		lastStatusOutput = string(output)
		recovered = agentStatus{}
		if err != nil || json.Unmarshal(output, &recovered) != nil {
			return false, nil
		}
		return recovered.Lease != nil && recovered.PostgresProcess == "running_quarantined" && recovered.Lease.Epoch == strconv.FormatInt(int64(*recoveredLease.Spec.LeaseTransitions), 10), nil
	}); err != nil {
		t.Fatalf("wait for quarantined PostgreSQL recovery: %v; last output=%q", err, lastStatusOutput)
	}
	waitForQuarantinedPostgreSQL(t, ctx, namespace.Name, podName, "coordination recovery")
	assertDurableWritableGeneration(
		t,
		ctx,
		namespace.Name,
		podName,
		cluster.Name,
		current.UID,
		checkpoint,
		*recoveredLease.Spec.HolderIdentity,
		*recoveredLease.Spec.LeaseTransitions,
	)
	if err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: podName}, pod); err != nil {
			return false, err
		}
		return pod.UID == initialPodUID && len(pod.Status.ContainerStatuses) == 1 && pod.Status.ContainerStatuses[0].State.Running != nil && pod.Status.ContainerStatuses[0].ContainerID == initialContainerID && pod.Status.ContainerStatuses[0].State.Running.StartedAt.Equal(&initialStartedAt), nil
	}); err != nil {
		t.Fatalf("wait for recovered agent Pod status: %v; Pod=%#v", err, pod)
	}
	if pod.Status.ContainerStatuses[0].RestartCount != initialRestarts {
		t.Fatalf("agent container restarted during recoverable Lease permission loss: %d -> %d", initialRestarts, pod.Status.ContainerStatuses[0].RestartCount)
	}

	stableTerm := *recoveredLease.Spec.LeaseTransitions
	stableHolder := *recoveredLease.Spec.HolderIdentity
	lastRenewTime := recoveredLease.Spec.RenewTime.Time
	renewals := 0
	observationDeadline := time.Now().Add(stableContainerObservation)
	for time.Now().Before(observationDeadline) {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(2 * time.Second):
		}

		observedLease := &coordinationv1.Lease{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: checkpoint.LeaseName}, observedLease); err != nil {
			t.Fatal(err)
		}
		if observedLease.Spec.LeaseTransitions == nil || *observedLease.Spec.LeaseTransitions != stableTerm || observedLease.Spec.HolderIdentity == nil || *observedLease.Spec.HolderIdentity != stableHolder || observedLease.Spec.RenewTime == nil {
			t.Fatalf("recovered authority changed during stable renewal observation: %#v", observedLease.Spec)
		}
		if observedLease.Spec.RenewTime.Time.After(lastRenewTime) {
			renewals++
			lastRenewTime = observedLease.Spec.RenewTime.Time
		}

		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: podName}, pod); err != nil {
			t.Fatal(err)
		}
		if pod.UID != initialPodUID || len(pod.Status.ContainerStatuses) != 1 || pod.Status.ContainerStatuses[0].RestartCount != initialRestarts || pod.Status.ContainerStatuses[0].State.Running == nil || pod.Status.ContainerStatuses[0].ContainerID != initialContainerID || !pod.Status.ContainerStatuses[0].State.Running.StartedAt.Equal(&initialStartedAt) {
			t.Fatalf("agent container changed during stable renewal observation: %#v", pod.Status.ContainerStatuses)
		}
	}
	if renewals < 2 {
		t.Fatalf("recovered authority completed %d observable renewals in %s, want at least 2", renewals, stableContainerObservation)
	}
	if output, err := exec.CommandContext(ctx, "kubectl", "--namespace", namespace.Name, "exec", podName, "--container=postgresql", "--", "pg_isready", "--quiet", "--host=/run/pgshard/postgres", "--port=5432").CombinedOutput(); err != nil {
		t.Fatalf("quarantined postmaster stopped during stable renewal observation: %v\n%s", err, output)
	}
	var stable agentStatus
	output, err := exec.CommandContext(ctx, "kubectl", "get", "--raw", statusPath).CombinedOutput()
	if err != nil || json.Unmarshal(output, &stable) != nil || stable.Lease == nil || stable.PostgresProcess != "running_quarantined" || stable.Lease.Epoch != strconv.FormatInt(int64(stableTerm), 10) {
		t.Fatalf("agent status changed during stable renewal observation: error=%v output=%q status=%#v", err, output, stable)
	}
	processPin := pinQuarantinedPostgreSQLProcess(t, ctx, namespace.Name, podName)
	if err := provePinnedPostgreSQLProcessAbsent(t, ctx, kubeClient, namespace.Name, podName, initialPodUID, processPin); err == nil || !strings.Contains(err.Error(), "pinned postmaster or process group remains live") {
		t.Fatalf("live PostgreSQL process pin was not detected before shutdown: %v", err)
	}

	// Stop the workload controller while leaving the exact Lease permission in
	// place, then scale the member down. No replacement process can race the old
	// agent's conditional release, so the empty-holder transition is observable.
	managerRestored = false
	runKubectl(t, ctx, "--namespace", "pgshard-system", "scale", "deployment/pgshard-controller-manager", "--replicas=0")
	waitForManagerReplicas(t, ctx, kubeClient, 0)
	runKubectl(t, ctx, "--namespace", namespace.Name, "scale", "statefulset/"+statefulSetName, "--replicas=0")

	releasedLease := &coordinationv1.Lease{}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		releasedLease = &coordinationv1.Lease{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: checkpoint.LeaseName}, releasedLease); err != nil {
			return false, err
		}
		if releasedLease.UID != checkpoint.LeaseUID || releasedLease.Spec.LeaseTransitions == nil || *releasedLease.Spec.LeaseTransitions != stableTerm {
			return false, fmt.Errorf("clean shutdown changed the Lease identity or fencing term: %#v", releasedLease)
		}
		if releasedLease.Spec.HolderIdentity != nil {
			if *releasedLease.Spec.HolderIdentity != stableHolder {
				return false, fmt.Errorf("clean shutdown changed the holder instead of releasing it: %#v", releasedLease.Spec)
			}
			return false, nil
		}
		if err := provePinnedPostgreSQLProcessAbsent(t, ctx, kubeClient, namespace.Name, podName, initialPodUID, processPin); err != nil {
			return false, fmt.Errorf("Lease holder cleared before immediate process-absence proof: %w", err)
		}
		return true, nil
	}); err != nil {
		t.Fatalf("wait for exact clean Lease release after PostgreSQL fence: %v; Lease=%#v", err, releasedLease)
	}

	runKubectl(t, ctx, "--namespace", "pgshard-system", "scale", "deployment/pgshard-controller-manager", "--replicas=1")
	runKubectl(t, ctx, "--namespace", "pgshard-system", "rollout", "status", "deployment/pgshard-controller-manager", "--timeout=120s")
	waitForManagerReplicas(t, ctx, kubeClient, 1)
	managerRestored = true

	restartedPod := &corev1.Pod{}
	if err := wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		restartedPod = &corev1.Pod{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: podName}, restartedPod); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		return restartedPod.UID != initialPodUID && restartedPod.Status.Phase == corev1.PodRunning && len(restartedPod.Status.ContainerStatuses) == 1 && restartedPod.Status.ContainerStatuses[0].State.Running != nil, nil
	}); err != nil {
		t.Fatalf("wait for cleanly restarted agent Pod: %v; Pod=%#v", err, restartedPod)
	}
	restartedLease := &coordinationv1.Lease{}
	if err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, time.Minute, true, func(ctx context.Context) (bool, error) {
		restartedLease = &coordinationv1.Lease{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: checkpoint.LeaseName}, restartedLease); err != nil {
			return false, err
		}
		return restartedLease.Spec.LeaseTransitions != nil && *restartedLease.Spec.LeaseTransitions == stableTerm+1 && restartedLease.Spec.HolderIdentity != nil && *restartedLease.Spec.HolderIdentity != "", nil
	}); err != nil {
		t.Fatalf("wait for immediate post-release Lease claim: %v; Lease=%#v", err, restartedLease)
	}
	restartedHolder := strings.Split(*restartedLease.Spec.HolderIdentity, "/")
	if len(restartedHolder) != 3 || restartedHolder[0] != podName || restartedHolder[1] != string(restartedPod.UID) || *restartedLease.Spec.HolderIdentity == stableHolder {
		t.Fatalf("post-release holder does not bind the replacement Pod: Pod=%s Lease=%#v", restartedPod.UID, restartedLease.Spec)
	}
	waitForQuarantinedPostgreSQL(t, ctx, namespace.Name, podName, "clean-release replacement")
	assertDurableWritableGeneration(
		t,
		ctx,
		namespace.Name,
		podName,
		cluster.Name,
		current.UID,
		checkpoint,
		*restartedLease.Spec.HolderIdentity,
		*restartedLease.Spec.LeaseTransitions,
	)
}

func assertKINDCatalogCandidateConfigurationsInert(t *testing.T, ctx context.Context, kubeClient client.Client, cluster *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	desired, err := owned.DesiredPostgreSQLCatalogCandidateConfigMaps(cluster)
	if err != nil {
		t.Fatalf("build exact catalog candidate configurations: %v", err)
	}
	if len(desired) != int(cluster.Spec.MembersPerShard) || len(cluster.Status.PostgreSQLCatalogCandidates) != len(desired) {
		t.Fatalf("catalog candidate desired/status cardinality = %d/%d, want %d", len(desired), len(cluster.Status.PostgreSQLCatalogCandidates), cluster.Spec.MembersPerShard)
	}
	topology := &corev1.ConfigMap{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + owned.TopologyConfigSuffix}, topology); err != nil {
		t.Fatalf("get published discovery topology: %v", err)
	}
	type discoveryMember struct {
		Ordinal        int32  `json:"ordinal"`
		InstanceID     string `json:"instanceId"`
		DNSName        string `json:"dnsName"`
		PostgreSQLPort int32  `json:"postgresqlPort"`
		AgentHTTPPort  int32  `json:"agentHttpPort"`
		PhysicalSlot   string `json:"physicalSlot"`
	}
	type candidateDocument struct {
		Shard             int32 `json:"shard"`
		Member            int32 `json:"member"`
		DiscoveryTopology struct {
			ConfigMap struct {
				Name string `json:"name"`
			} `json:"configMap"`
			Members []discoveryMember `json:"members"`
			SHA256  string            `json:"sha256"`
		} `json:"discoveryTopology"`
	}
	names := make(map[string]struct{}, len(desired))
	uids := make(map[types.UID]struct{}, len(desired))
	digests := make(map[string]struct{}, len(desired))
	for member, wanted := range desired {
		checkpoint := cluster.Status.PostgreSQLCatalogCandidates[member]
		configuration := &corev1.ConfigMap{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: checkpoint.ConfigMapName}, configuration); err != nil {
			t.Fatalf("get catalog candidate member %d: %v", member, err)
		}
		if checkpoint.Member != int32(member) || checkpoint.ConfigMapName != wanted.Name || checkpoint.ConfigMapUID != configuration.UID ||
			checkpoint.PayloadSHA256 != owned.PostgreSQLCatalogCandidatePayloadSHA256(configuration) ||
			configuration.Immutable == nil || !*configuration.Immutable || len(configuration.Data) != 1 || len(configuration.BinaryData) != 0 ||
			!maps.Equal(configuration.Data, wanted.Data) || !maps.Equal(configuration.Labels, wanted.Labels) || !maps.Equal(configuration.Annotations, wanted.Annotations) ||
			!metav1.IsControlledBy(configuration, cluster) || len(configuration.Finalizers) != 0 {
			t.Fatalf("catalog candidate member %d is not exact and inert: checkpoint=%#v ConfigMap=%#v", member, checkpoint, configuration)
		}
		if _, role := configuration.Labels[owned.RoleLabel]; role {
			t.Fatalf("catalog candidate member %d carries a serving role: %#v", member, configuration.Labels)
		}
		if _, duplicate := names[configuration.Name]; duplicate {
			t.Fatalf("catalog candidate ConfigMap name %s is duplicated", configuration.Name)
		}
		if _, duplicate := uids[configuration.UID]; duplicate {
			t.Fatalf("catalog candidate ConfigMap UID %s is duplicated", configuration.UID)
		}
		if _, duplicate := digests[checkpoint.PayloadSHA256]; duplicate {
			t.Fatalf("catalog candidate payload digest %s is duplicated", checkpoint.PayloadSHA256)
		}
		names[configuration.Name] = struct{}{}
		uids[configuration.UID] = struct{}{}
		digests[checkpoint.PayloadSHA256] = struct{}{}

		var document candidateDocument
		if err := json.Unmarshal([]byte(configuration.Data["candidate.json"]), &document); err != nil {
			t.Fatalf("decode catalog candidate member %d: %v", member, err)
		}
		if document.Shard != 0 || document.Member != int32(member) || document.DiscoveryTopology.ConfigMap.Name != topology.Name ||
			!validCatalogAccessDigest(document.DiscoveryTopology.SHA256) ||
			len(document.DiscoveryTopology.Members) != int(cluster.Spec.MembersPerShard) {
			t.Fatalf("catalog candidate member %d discovery reference = %#v", member, document.DiscoveryTopology)
		}
		for ordinal, discovery := range document.DiscoveryTopology.Members {
			memberID := int32(ordinal)
			instanceID := owned.PostgreSQLMemberStatefulSetName(cluster.Name, 0, memberID) + "-0"
			wantDNS := fmt.Sprintf("%s.%s-shard-0000.%s.svc", instanceID, cluster.Name, cluster.Namespace)
			if discovery.Ordinal != memberID || discovery.InstanceID != instanceID || discovery.DNSName != wantDNS ||
				discovery.PostgreSQLPort != 5432 || discovery.AgentHTTPPort != 8080 || discovery.PhysicalSlot != fmt.Sprintf("pgshard_member_%04d", memberID) {
				t.Fatalf("catalog candidate %d discovery member %d = %#v", member, ordinal, discovery)
			}
		}
	}
	assertKINDCatalogCandidatesNotConsumed(t, ctx, kubeClient, cluster)
}

func assertKINDCatalogCandidateReplacementFailsClosed(t *testing.T, ctx context.Context, kubeClient client.Client, cluster *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	checkpoints := append([]pgshardv1alpha1.PostgreSQLCatalogCandidateStatus(nil), cluster.Status.PostgreSQLCatalogCandidates...)
	catalogAccess := cluster.Status.CatalogAccess.DeepCopy()
	checkpoint := checkpoints[1]
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: checkpoint.ConfigMapName}
	original := &corev1.ConfigMap{}
	if err := kubeClient.Get(ctx, key, original); err != nil {
		t.Fatalf("get catalog candidate before replacement: %v", err)
	}
	uid := original.UID
	resourceVersion := original.ResourceVersion
	if err := kubeClient.Delete(ctx, original, client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}); err != nil {
		t.Fatalf("delete catalog candidate: %v", err)
	}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		err := kubeClient.Get(ctx, key, &corev1.ConfigMap{})
		return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
	}); err != nil {
		t.Fatalf("wait for catalog candidate deletion: %v", err)
	}
	desired, err := owned.DesiredPostgreSQLCatalogCandidateConfigMaps(cluster)
	if err != nil {
		t.Fatalf("build replacement catalog candidate: %v", err)
	}
	replacement := desired[checkpoint.Member].DeepCopy()
	if err := kubeClient.Create(ctx, replacement); err != nil {
		t.Fatalf("create replacement catalog candidate: %v", err)
	}
	if replacement.UID == "" || replacement.UID == checkpoint.ConfigMapUID {
		t.Fatalf("replacement catalog candidate UID = %s, recorded UID = %s", replacement.UID, checkpoint.ConfigMapUID)
	}

	failed := &pgshardv1alpha1.PgShardCluster{}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(cluster), failed); err != nil {
			return false, err
		}
		condition := meta.FindStatusCondition(failed.Status.Conditions, reconciledCondition)
		return failed.Status.Phase == "Degraded" && condition != nil && condition.Status == metav1.ConditionFalse &&
			condition.Reason == "CatalogCandidateReconcileFailed" &&
			strings.Contains(condition.Message, string(checkpoint.ConfigMapUID)) &&
			strings.Contains(condition.Message, string(replacement.UID)), nil
	}); err != nil {
		t.Fatalf("wait for manager to report both recorded UID %s and replacement UID %s: %v; status=%#v", checkpoint.ConfigMapUID, replacement.UID, err, failed.Status)
	}
	if !reflect.DeepEqual(failed.Status.PostgreSQLCatalogCandidates, checkpoints) || !reflect.DeepEqual(failed.Status.CatalogAccess, catalogAccess) {
		t.Fatalf("replacement changed catalog checkpoints: candidates before=%#v after=%#v access before=%#v after=%#v", checkpoints, failed.Status.PostgreSQLCatalogCandidates, catalogAccess, failed.Status.CatalogAccess)
	}
	observed := &corev1.ConfigMap{}
	if err := kubeClient.Get(ctx, key, observed); err != nil {
		t.Fatalf("get rejected replacement catalog candidate: %v", err)
	}
	if observed.UID != replacement.UID {
		t.Fatalf("controller adopted or replaced rejected catalog candidate UID %s with %s", replacement.UID, observed.UID)
	}
	assertKINDCatalogCandidateDiagnosticsUnavailable(t, ctx, kubeClient, failed)
	assertKINDCatalogCandidatesNotConsumed(t, ctx, kubeClient, failed)
}

func assertKINDCatalogCandidateDiagnosticsUnavailable(t *testing.T, ctx context.Context, kubeClient client.Client, cluster *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	deployment := &appsv1.Deployment{}
	deploymentKey := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + owned.OrchestratorSuffix}
	if err := kubeClient.Get(ctx, deploymentKey, deployment); err != nil {
		t.Fatal(err)
	}
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas < 1 {
		t.Fatalf("orchestrator deployment has no desired replicas: %#v", deployment.Spec.Replicas)
	}
	wantedReplicas := int(*deployment.Spec.Replicas)
	var last string
	if err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		pods := &corev1.PodList{}
		if err := kubeClient.List(ctx, pods, client.InNamespace(cluster.Namespace), client.MatchingLabels{
			owned.ClusterLabel:   cluster.Name,
			owned.ComponentLabel: "orchestrator",
		}); err != nil {
			return false, err
		}
		active := make([]corev1.Pod, 0, wantedReplicas)
		for _, pod := range pods.Items {
			if pod.DeletionTimestamp == nil && pod.Status.Phase == corev1.PodRunning {
				active = append(active, pod)
			}
		}
		if len(active) != wantedReplicas {
			last = fmt.Sprintf("active orchestrator replicas=%d want=%d", len(active), wantedReplicas)
			return false, nil
		}
		for _, pod := range active {
			var snapshot struct {
				CoordinationReady bool `json:"coordination_ready"`
				CatalogCandidates struct {
					Phase           string  `json:"phase"`
					FreshCandidates int     `json:"fresh_candidates"`
					Failure         *string `json:"failure"`
					DiagnosticOnly  bool    `json:"diagnostic_only"`
				} `json:"catalog_candidates"`
			}
			statusPath := fmt.Sprintf("/api/v1/namespaces/%s/pods/http:%s:8080/proxy/status", cluster.Namespace, pod.Name)
			requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			output, err := exec.CommandContext(requestCtx, "kubectl", "get", "--raw", statusPath).CombinedOutput()
			cancel()
			if err != nil || json.Unmarshal(output, &snapshot) != nil || !snapshot.CoordinationReady || snapshot.CatalogCandidates.Phase != "unavailable" || snapshot.CatalogCandidates.FreshCandidates != 0 || snapshot.CatalogCandidates.Failure == nil || *snapshot.CatalogCandidates.Failure != "validation_failed" || !snapshot.CatalogCandidates.DiagnosticOnly {
				last = fmt.Sprintf("pod=%s error=%v output=%q snapshot=%#v", pod.Name, err, output, snapshot)
				return false, nil
			}
			readyPath := fmt.Sprintf("/api/v1/namespaces/%s/pods/http:%s:8080/proxy/readyz", cluster.Namespace, pod.Name)
			requestCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
			output, err = exec.CommandContext(requestCtx, "kubectl", "get", "--raw", readyPath).CombinedOutput()
			cancel()
			if err != nil {
				last = fmt.Sprintf("pod=%s readiness error=%v output=%q", pod.Name, err, output)
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		t.Fatalf("wait for fail-closed catalog-candidate diagnostics with live readiness: %v; last=%s", err, last)
	}
}

func assertKINDCatalogCandidatesNotConsumed(t *testing.T, ctx context.Context, kubeClient client.Client, cluster *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	statefulSets := &appsv1.StatefulSetList{}
	deployments := &appsv1.DeploymentList{}
	if err := kubeClient.List(ctx, statefulSets, client.InNamespace(cluster.Namespace)); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.List(ctx, deployments, client.InNamespace(cluster.Namespace)); err != nil {
		t.Fatal(err)
	}
	for _, workload := range statefulSets.Items {
		for _, volume := range workload.Spec.Template.Spec.Volumes {
			if volume.ConfigMap != nil && strings.HasSuffix(volume.ConfigMap.Name, owned.PostgreSQLCatalogCandidateSuffix) {
				t.Fatalf("StatefulSet %s consumed inert catalog candidate %s", workload.Name, volume.ConfigMap.Name)
			}
		}
	}
	for _, workload := range deployments.Items {
		for _, volume := range workload.Spec.Template.Spec.Volumes {
			if volume.ConfigMap != nil && strings.HasSuffix(volume.ConfigMap.Name, owned.PostgreSQLCatalogCandidateSuffix) {
				t.Fatalf("Deployment %s consumed inert catalog candidate %s", workload.Name, volume.ConfigMap.Name)
			}
			if volume.Secret != nil && cluster.Status.CatalogAccess != nil && volume.Secret.SecretName == cluster.Status.CatalogAccess.SecretName {
				t.Fatalf("Deployment %s consumed staged multi-member catalog access %s", workload.Name, volume.Secret.SecretName)
			}
		}
	}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: owned.CatalogServiceName(cluster.Name)}, &corev1.Service{}); !apierrors.IsNotFound(err) {
		t.Fatalf("multi-member catalog candidate foundation published catalog Service: %v", err)
	}
}

func TestKINDManagerDeletePolicyReleasesBoundPostgreSQLPVC(t *testing.T) {
	if os.Getenv("PGSHARD_KIND_MANAGER_E2E") != "true" {
		t.Skip("set PGSHARD_KIND_MANAGER_E2E=true against the installed admission manager")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	kubeClient := newKINDClient(t)

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: fmt.Sprintf("pgshard-manager-delete-%d", os.Getpid()),
		Labels: map[string]string{
			"pod-security.kubernetes.io/enforce":         "restricted",
			"pod-security.kubernetes.io/enforce-version": "latest",
			podfence.NamespaceLabel:                      podfence.NamespaceLabelValue,
		},
	}}
	if err := kubeClient.Create(ctx, namespace); err != nil {
		t.Fatal(err)
	}
	deleteNamespaceAtCleanup(t, kubeClient, namespace)

	cluster := readSingleMemberSample(t)
	cluster.Name = "delete-bound"
	cluster.Namespace = namespace.Name
	cluster.Spec.Shards = 1
	cluster.Spec.Storage.DeletionPolicy = pgshardv1alpha1.DeletionDelete
	if err := kubeClient.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	waitForSingleMemberPostgreSQL(t, ctx, kubeClient, client.ObjectKeyFromObject(cluster))

	current := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(cluster), current); err != nil {
		t.Fatal(err)
	}
	bootstrap := bootstrapForShard(t, current, 0)
	claimKey := types.NamespacedName{Namespace: namespace.Name, Name: bootstrap.PVCName}
	claim := &corev1.PersistentVolumeClaim{}
	if err := kubeClient.Get(ctx, claimKey, claim); err != nil {
		t.Fatal(err)
	}
	if claim.Status.Phase != corev1.ClaimBound || claim.UID != bootstrap.PVCUID {
		t.Fatalf("PostgreSQL data claim was not bound to its checkpointed UID: phase=%s metadata=%#v checkpoint=%s", claim.Status.Phase, claim.ObjectMeta, bootstrap.PVCUID)
	}
	secret := &corev1.Secret{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: bootstrap.SecretName}, secret); err != nil {
		t.Fatal(err)
	}
	if len(claim.OwnerReferences) != 0 || !postgresqlDataPVCIsProtected(claim) || !postgresqlCredentialIsDataAnchored(secret, bootstrap) {
		t.Fatalf("Delete-policy PostgreSQL data fence was not stabilized: claim=%#v secret=%#v", claim.ObjectMeta, secret.ObjectMeta)
	}
	if bootstrap.PVCStorageClassName == nil || claim.Spec.StorageClassName == nil || *claim.Spec.StorageClassName != *bootstrap.PVCStorageClassName {
		t.Fatalf("PostgreSQL data claim storage class = %#v, checkpoint = %#v", claim.Spec.StorageClassName, bootstrap.PVCStorageClassName)
	}
	podKey := types.NamespacedName{Namespace: namespace.Name, Name: owned.PostgreSQLShardStatefulSetName(cluster.Name, 0) + "-0"}
	pod := &corev1.Pod{}
	if err := kubeClient.Get(ctx, podKey, pod); err != nil {
		t.Fatal(err)
	}
	if pod.Status.Phase != corev1.PodRunning {
		t.Fatalf("PostgreSQL Pod was not running against the bound claim: %#v", pod.Status)
	}
	mountedCheckpointedClaim := false
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == bootstrap.PVCName {
			mountedCheckpointedClaim = true
			break
		}
	}
	if !mountedCheckpointedClaim {
		t.Fatalf("PostgreSQL Pod does not mount checkpointed data claim %s: %#v", bootstrap.PVCName, pod.Spec.Volumes)
	}

	if err := kubeClient.Delete(ctx, current, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		err := kubeClient.Get(ctx, client.ObjectKeyFromObject(cluster), &pgshardv1alpha1.PgShardCluster{})
		return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
	}); err != nil {
		t.Fatalf("Delete policy finalizer deadlocked on a bound PostgreSQL PVC: %v", err)
	}
	for description, object := range map[string]client.Object{
		"PostgreSQL Pod":       &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podKey.Name, Namespace: podKey.Namespace}},
		"PostgreSQL PVC":       &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: claimKey.Name, Namespace: claimKey.Namespace}},
		"PostgreSQL state":     &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: owned.PostgreSQLShardStatefulSetName(cluster.Name, 0), Namespace: namespace.Name}},
		"credential PVC fence": &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: bootstrap.SecretName, Namespace: namespace.Name}},
	} {
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(object), object); !apierrors.IsNotFound(err) {
			t.Fatalf("%s survived completed Delete-policy finalization: %v", description, err)
		}
	}
}

func TestKINDManagerRetainPolicyReleasesExplicitlyDeletingPostgreSQLPVC(t *testing.T) {
	if os.Getenv("PGSHARD_KIND_MANAGER_E2E") != "true" {
		t.Skip("set PGSHARD_KIND_MANAGER_E2E=true against the installed admission manager")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	kubeClient := newKINDClient(t)

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: fmt.Sprintf("pgshard-manager-retain-delete-%d", os.Getpid()),
		Labels: map[string]string{
			"pod-security.kubernetes.io/enforce":         "restricted",
			"pod-security.kubernetes.io/enforce-version": "latest",
			podfence.NamespaceLabel:                      podfence.NamespaceLabelValue,
		},
	}}
	if err := kubeClient.Create(ctx, namespace); err != nil {
		t.Fatal(err)
	}
	deleteNamespaceAtCleanup(t, kubeClient, namespace)

	cluster := readSingleMemberSample(t)
	cluster.Name = "retain-explicit-delete"
	cluster.Namespace = namespace.Name
	cluster.Spec.Shards = 1
	if err := kubeClient.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	waitForSingleMemberPostgreSQL(t, ctx, kubeClient, client.ObjectKeyFromObject(cluster))

	current := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(cluster), current); err != nil {
		t.Fatal(err)
	}
	bootstrap := bootstrapForShard(t, current, 0)
	claimKey := types.NamespacedName{Namespace: namespace.Name, Name: bootstrap.PVCName}
	claim := &corev1.PersistentVolumeClaim{}
	if err := kubeClient.Get(ctx, claimKey, claim); err != nil {
		t.Fatal(err)
	}
	if claim.Status.Phase != corev1.ClaimBound || claim.UID != bootstrap.PVCUID || !postgresqlDataPVCIsProtected(claim) {
		t.Fatalf("PostgreSQL data claim was not the exact bound protected PVC: phase=%s metadata=%#v checkpoint=%s", claim.Status.Phase, claim.ObjectMeta, bootstrap.PVCUID)
	}

	if err := kubeClient.Delete(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		deleting := &corev1.PersistentVolumeClaim{}
		if err := kubeClient.Get(ctx, claimKey, deleting); err != nil {
			return false, err
		}
		return deleting.UID == bootstrap.PVCUID && deleting.DeletionTimestamp != nil && postgresqlDataPVCIsProtected(deleting), nil
	}); err != nil {
		t.Fatalf("explicit Delete did not stop at the exact protected PostgreSQL data PVC: %v", err)
	}

	if err := kubeClient.Delete(ctx, current, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		err := kubeClient.Get(ctx, client.ObjectKeyFromObject(cluster), &pgshardv1alpha1.PgShardCluster{})
		return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
	}); err != nil {
		t.Fatalf("Retain finalizer deadlocked behind an explicitly deleting PostgreSQL data PVC: %v", err)
	}
	for description, object := range map[string]client.Object{
		"PostgreSQL Pod":       &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: owned.PostgreSQLShardStatefulSetName(cluster.Name, 0) + "-0", Namespace: namespace.Name}},
		"PostgreSQL PVC":       &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: claimKey.Name, Namespace: claimKey.Namespace}},
		"PostgreSQL state":     &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: owned.PostgreSQLShardStatefulSetName(cluster.Name, 0), Namespace: namespace.Name}},
		"credential PVC fence": &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: bootstrap.SecretName, Namespace: namespace.Name}},
	} {
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(object), object); !apierrors.IsNotFound(err) {
			t.Fatalf("%s survived completed explicit-delete Retain finalization: %v", description, err)
		}
	}
}

func runPostgreSQLServiceQuery(t *testing.T, ctx context.Context, kubeClient client.Client, namespace, cluster, image, secret, host, query string) string {
	t.Helper()
	clientPod := postgreSQLClientPod(namespace, fmt.Sprintf("pgshard-sql-client-%d-%d", os.Getpid(), time.Now().UnixNano()), map[string]string{
		owned.ClusterLabel:   cluster,
		owned.ComponentLabel: "pooler",
	}, image, secret, host, query)
	if err := kubeClient.Create(ctx, clientPod); err != nil {
		t.Fatal(err)
	}
	err := wait.PollUntilContextTimeout(ctx, time.Second, time.Minute, true, func(ctx context.Context) (bool, error) {
		current := &corev1.Pod{}
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(clientPod), current); err != nil {
			return false, err
		}
		if current.Status.Phase == corev1.PodFailed {
			return false, fmt.Errorf("PostgreSQL client Pod failed")
		}
		return current.Status.Phase == corev1.PodSucceeded, nil
	})
	if err != nil {
		t.Fatalf("wait for PostgreSQL client Pod: %v", err)
	}
	return strings.TrimSpace(runKubectl(t, ctx, "--namespace", namespace, "logs", clientPod.Name))
}

func assertKINDOrchestratorObservationRBAC(t *testing.T, ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	identity := "system:serviceaccount:" + cluster.Namespace + ":" + cluster.Name + owned.OrchestratorSuffix
	statefulSets := make([]string, 0, cluster.Spec.Shards*cluster.Spec.MembersPerShard)
	pods := make([]string, 0, cluster.Spec.Shards*cluster.Spec.MembersPerShard)
	endpoints := make([]string, 0, cluster.Spec.Shards)
	writableLeases := make([]string, 0, cluster.Spec.Shards)
	catalogCandidates := make([]string, 0, cluster.Spec.MembersPerShard)
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		endpoints = append(endpoints, fmt.Sprintf("%s-shard-%04d", cluster.Name, shard))
		writableLeases = append(writableLeases, owned.PostgreSQLWritableLeaseName(cluster.Name, shard))
		for member := int32(0); member < cluster.Spec.MembersPerShard; member++ {
			name := owned.PostgreSQLMemberStatefulSetName(cluster.Name, shard, member)
			statefulSets = append(statefulSets, name)
			pods = append(pods, name+"-0")
		}
	}
	for member := int32(0); member < cluster.Spec.MembersPerShard; member++ {
		catalogCandidates = append(catalogCandidates, owned.PostgreSQLCatalogCandidateConfigMapName(cluster.Name, member))
	}

	for _, name := range statefulSets {
		assertKINDCanI(t, ctx, identity, cluster.Namespace, true, "get", "statefulsets.apps/"+name)
	}
	for _, name := range pods {
		assertKINDCanI(t, ctx, identity, cluster.Namespace, true, "get", "pods/"+name)
	}
	for _, name := range endpoints {
		assertKINDCanI(t, ctx, identity, cluster.Namespace, true, "get", "endpoints/"+name)
	}
	for _, name := range writableLeases {
		assertKINDCanI(t, ctx, identity, cluster.Namespace, true, "get", "leases.coordination.k8s.io/"+name)
	}
	assertKINDCanISubresource(t, ctx, identity, cluster.Namespace, true, "get", "pgshardclusters.pgshard.io", cluster.Name, "status")
	for _, name := range catalogCandidates {
		assertKINDCanI(t, ctx, identity, cluster.Namespace, true, "get", "configmaps/"+name)
	}
	orchestratorLease := cluster.Name + owned.OrchestratorLeaseSuffix
	assertKINDCanI(t, ctx, identity, cluster.Namespace, true, "get", "leases.coordination.k8s.io/"+orchestratorLease)
	assertKINDCanI(t, ctx, identity, cluster.Namespace, true, "update", "leases.coordination.k8s.io/"+orchestratorLease)

	for _, denied := range []struct {
		resource     string
		exactAllowed string
	}{
		{resource: "statefulsets.apps", exactAllowed: statefulSets[0]},
		{resource: "pods", exactAllowed: pods[0]},
		{resource: "endpoints", exactAllowed: endpoints[0]},
		{resource: "leases.coordination.k8s.io", exactAllowed: writableLeases[0]},
		{resource: "configmaps", exactAllowed: catalogCandidates[0]},
	} {
		assertKINDCanI(t, ctx, identity, cluster.Namespace, false, "get", denied.resource+"/foreign-object")
		for _, verb := range []string{"list", "watch"} {
			assertKINDCanI(t, ctx, identity, cluster.Namespace, false, verb, denied.resource)
		}
		for _, verb := range []string{"update", "patch", "delete"} {
			assertKINDCanI(t, ctx, identity, cluster.Namespace, false, verb, denied.resource+"/"+denied.exactAllowed)
		}
		for _, verb := range []string{"create", "deletecollection"} {
			assertKINDCanI(t, ctx, identity, cluster.Namespace, false, verb, denied.resource)
		}
	}
	for _, verb := range []string{"patch", "delete"} {
		assertKINDCanI(t, ctx, identity, cluster.Namespace, false, verb, "leases.coordination.k8s.io/"+orchestratorLease)
	}
	assertKINDCanI(t, ctx, identity, cluster.Namespace, false, "get", "pgshardclusters.pgshard.io/"+cluster.Name)
	assertKINDCanISubresource(t, ctx, identity, cluster.Namespace, false, "get", "pgshardclusters.pgshard.io", "foreign-object", "status")
	for _, verb := range []string{"update", "patch", "delete"} {
		assertKINDCanISubresource(t, ctx, identity, cluster.Namespace, false, verb, "pgshardclusters.pgshard.io", cluster.Name, "status")
	}
	for _, verb := range []string{"create", "deletecollection"} {
		assertKINDCanI(t, ctx, identity, cluster.Namespace, false, verb, "leases.coordination.k8s.io")
	}
	for _, resource := range []string{"secrets", "persistentvolumeclaims"} {
		assertKINDCanI(t, ctx, identity, cluster.Namespace, false, "get", resource+"/foreign-object")
		for _, verb := range []string{"list", "watch", "create", "deletecollection"} {
			assertKINDCanI(t, ctx, identity, cluster.Namespace, false, verb, resource)
		}
	}
}

func assertKINDCanI(t *testing.T, ctx context.Context, identity, namespace string, allowed bool, verb, resource string) {
	t.Helper()
	output, err := exec.CommandContext(ctx, "kubectl", "auth", "can-i", verb, resource, "--namespace", namespace, "--as="+identity).CombinedOutput()
	want := "no"
	if allowed {
		want = "yes"
	}
	if got := strings.TrimSpace(string(output)); got != want {
		t.Fatalf("kubectl auth can-i %s %s as %s = %q (error=%v), want %q", verb, resource, identity, got, err, want)
	}
}

func assertKINDCanISubresource(t *testing.T, ctx context.Context, identity, namespace string, allowed bool, verb, resource, name, subresource string) {
	t.Helper()
	output, err := exec.CommandContext(ctx, "kubectl", "auth", "can-i", verb, resource+"/"+name, "--subresource="+subresource, "--namespace", namespace, "--as="+identity).CombinedOutput()
	want := "no"
	if allowed {
		want = "yes"
	}
	if got := strings.TrimSpace(string(output)); got != want {
		t.Fatalf("kubectl auth can-i %s %s/%s --subresource=%s as %s = %q (error=%v), want %q", verb, resource, name, subresource, identity, got, err, want)
	}
}

func assertKINDOrchestratorBindsControllerEndpoints(t *testing.T, ctx context.Context, kubeClient client.Client, cluster *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	const wantedCollectionState = "fresh_diagnostic_evidence"
	wantedMembers := int(cluster.Spec.Shards * cluster.Spec.MembersPerShard)
	wantedCandidates := int(cluster.Spec.MembersPerShard)
	wantedCatalogPhase := "fresh"
	wantedCatalogMaximumAgeMS := uint64(5000)
	if cluster.Spec.MembersPerShard == 1 {
		wantedCandidates = 0
		wantedCatalogPhase = "disabled"
		wantedCatalogMaximumAgeMS = 0
	}
	deployment := &appsv1.Deployment{}
	deploymentKey := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + owned.OrchestratorSuffix}
	if err := kubeClient.Get(ctx, deploymentKey, deployment); err != nil {
		t.Fatal(err)
	}
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas < 1 {
		t.Fatalf("orchestrator deployment has no desired replicas: %#v", deployment.Spec.Replicas)
	}
	wantedReplicas := int(*deployment.Spec.Replicas)
	var lastObservation string
	if err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, time.Minute, true, func(ctx context.Context) (bool, error) {
		controllerEndpoints := &corev1.Endpoints{}
		endpointKey := types.NamespacedName{Namespace: cluster.Namespace, Name: fmt.Sprintf("%s-shard-%04d", cluster.Name, 0)}
		if err := kubeClient.Get(ctx, endpointKey, controllerEndpoints); err != nil {
			lastObservation = fmt.Sprintf("read controller Endpoints: %v", err)
			return false, client.IgnoreNotFound(err)
		}
		addresses := 0
		for _, subset := range controllerEndpoints.Subsets {
			for _, address := range append(append([]corev1.EndpointAddress(nil), subset.Addresses...), subset.NotReadyAddresses...) {
				if address.TargetRef == nil || address.TargetRef.Kind != "Pod" || address.TargetRef.Name == "" || address.TargetRef.UID == "" || (address.TargetRef.APIVersion != "" && address.TargetRef.APIVersion != "v1") {
					lastObservation = fmt.Sprintf("noncanonical controller endpoint address: %#v", address)
					return false, nil
				}
				addresses++
			}
		}
		if addresses != int(cluster.Spec.MembersPerShard) {
			lastObservation = fmt.Sprintf("controller endpoint address count=%d want=%d", addresses, cluster.Spec.MembersPerShard)
			return false, nil
		}

		pods := &corev1.PodList{}
		if err := kubeClient.List(ctx, pods, client.InNamespace(cluster.Namespace), client.MatchingLabels{
			owned.ClusterLabel:   cluster.Name,
			owned.ComponentLabel: "orchestrator",
		}); err != nil {
			return false, err
		}
		active := make([]corev1.Pod, 0, wantedReplicas)
		for _, pod := range pods.Items {
			if pod.DeletionTimestamp == nil && pod.Status.Phase == corev1.PodRunning {
				active = append(active, pod)
			}
		}
		if len(active) != wantedReplicas {
			lastObservation = fmt.Sprintf("active orchestrator replicas=%d want=%d", len(active), wantedReplicas)
			return false, nil
		}
		for _, pod := range active {
			var snapshot struct {
				CoordinationReady bool `json:"coordination_ready"`
				Topology          *struct {
					AgentStatusCollection string `json:"agent_status_collection"`
				} `json:"topology"`
				AgentStatus struct {
					Phase           string  `json:"phase"`
					ExpectedMembers int     `json:"expected_members"`
					FreshMembers    int     `json:"fresh_members"`
					MaximumAgeMS    uint64  `json:"maximum_age_ms"`
					Failure         *string `json:"failure"`
					DiagnosticOnly  bool    `json:"diagnostic_only"`
				} `json:"agent_status"`
				CatalogCandidates struct {
					Phase              string  `json:"phase"`
					ExpectedCandidates int     `json:"expected_candidates"`
					FreshCandidates    int     `json:"fresh_candidates"`
					MaximumAgeMS       uint64  `json:"maximum_age_ms"`
					Failure            *string `json:"failure"`
					DiagnosticOnly     bool    `json:"diagnostic_only"`
				} `json:"catalog_candidates"`
			}
			statusPath := fmt.Sprintf("/api/v1/namespaces/%s/pods/http:%s:8080/proxy/status", cluster.Namespace, pod.Name)
			requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			output, err := exec.CommandContext(requestCtx, "kubectl", "get", "--raw", statusPath).CombinedOutput()
			cancel()
			if err != nil || json.Unmarshal(output, &snapshot) != nil || snapshot.Topology == nil || snapshot.Topology.AgentStatusCollection != wantedCollectionState || !snapshot.CoordinationReady || snapshot.AgentStatus.Phase != "fresh" || snapshot.AgentStatus.ExpectedMembers != wantedMembers || snapshot.AgentStatus.FreshMembers != wantedMembers || snapshot.AgentStatus.MaximumAgeMS != 5000 || snapshot.AgentStatus.Failure != nil || !snapshot.AgentStatus.DiagnosticOnly || snapshot.CatalogCandidates.Phase != wantedCatalogPhase || snapshot.CatalogCandidates.ExpectedCandidates != wantedCandidates || snapshot.CatalogCandidates.FreshCandidates != wantedCandidates || snapshot.CatalogCandidates.MaximumAgeMS != wantedCatalogMaximumAgeMS || snapshot.CatalogCandidates.Failure != nil || !snapshot.CatalogCandidates.DiagnosticOnly {
				lastObservation = fmt.Sprintf("orchestrator %s status error=%v output=%q snapshot=%#v", pod.Name, err, output, snapshot)
				return false, nil
			}
			readyPath := fmt.Sprintf("/api/v1/namespaces/%s/pods/http:%s:8080/proxy/readyz", cluster.Namespace, pod.Name)
			requestCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
			output, err = exec.CommandContext(requestCtx, "kubectl", "get", "--raw", readyPath).CombinedOutput()
			cancel()
			if err != nil {
				lastObservation = fmt.Sprintf("orchestrator %s readiness changed after diagnostic binding: %v output=%q", pod.Name, err, output)
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		t.Fatalf("wait for fresh identity-bound diagnostics from every orchestrator replica: %v; last=%s", err, lastObservation)
	}
}

func assertKINDPhysicalStandbys(t *testing.T, ctx context.Context, kubeClient client.Client, cluster *pgshardv1alpha1.PgShardCluster, sourcePodName string) {
	t.Helper()
	const shard = int32(0)
	namespace := cluster.Namespace
	wantSourceHost := fmt.Sprintf("%s-0.%s-shard-%04d.%s.svc", owned.PostgreSQLMemberStatefulSetName(cluster.Name, shard, 0), cluster.Name, shard, namespace)
	type standbyIdentity struct {
		podName string
		podUID  types.UID
		pvcName string
		pvcUID  types.UID
	}
	standbys := make(map[int32]standbyIdentity, cluster.Spec.MembersPerShard-1)

	for member := int32(1); member < cluster.Spec.MembersPerShard; member++ {
		statefulSetName := owned.PostgreSQLMemberStatefulSetName(cluster.Name, shard, member)
		statefulSet := &appsv1.StatefulSet{}
		if err := wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
			err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: statefulSetName}, statefulSet)
			return err == nil, client.IgnoreNotFound(err)
		}); err != nil {
			t.Fatalf("wait for physical standby member %d StatefulSet: %v", member, err)
		}
		if _, hasRole := statefulSet.Spec.Template.Labels[owned.RoleLabel]; hasRole {
			t.Fatalf("physical standby member %d received a serving role: %#v", member, statefulSet.Spec.Template.Labels)
		}
		if statefulSet.Spec.Template.Spec.ServiceAccountName != owned.PostgreSQLStandbyServiceAccountName(cluster.Name, shard) ||
			statefulSet.Spec.Template.Spec.AutomountServiceAccountToken == nil || *statefulSet.Spec.Template.Spec.AutomountServiceAccountToken ||
			len(statefulSet.Spec.Template.Spec.Containers) != 1 {
			t.Fatalf("physical standby member %d Pod identity = %#v", member, statefulSet.Spec.Template.Spec)
		}
		standby := statefulSet.Spec.Template.Spec.Containers[0]
		slotName := fmt.Sprintf("pgshard_member_%04d", member)
		if agentEnvironmentValue(standby.Env, "PGSHARD_POSTGRES_MODE") != "replication-standby" ||
			agentEnvironmentValue(standby.Env, "PGSHARD_POSTGRES_PRIMARY_HOST") != wantSourceHost ||
			agentEnvironmentValue(standby.Env, "PGSHARD_POSTGRES_PRIMARY_SLOT_NAME") != slotName {
			t.Fatalf("physical standby member %d source and slot environment = %#v", member, standby.Env)
		}
		if podContainerHasNamedVolumeMount(standby.VolumeMounts, "replication-credential") ||
			podContainerHasNamedVolumeMount(standby.VolumeMounts, "kubernetes-api") {
			t.Fatalf("running physical standby member %d retained privileged material: %#v", member, standby.VolumeMounts)
		}
		var pvcName string
		for _, volume := range statefulSet.Spec.Template.Spec.Volumes {
			if volume.Name == "data" && volume.PersistentVolumeClaim != nil {
				pvcName = volume.PersistentVolumeClaim.ClaimName
			}
		}
		if pvcName == "" {
			t.Fatalf("physical standby member %d has no persistent data volume: %#v", member, statefulSet.Spec.Template.Spec.Volumes)
		}
		pvc := &corev1.PersistentVolumeClaim{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pvcName}, pvc); err != nil {
			t.Fatal(err)
		}

		podName := statefulSetName + "-0"
		pod := waitForRunningPhysicalStandby(t, ctx, kubeClient, types.NamespacedName{Namespace: namespace, Name: podName}, "")
		standbys[member] = standbyIdentity{podName: podName, podUID: pod.UID, pvcName: pvcName, pvcUID: pvc.UID}
	}

	wantStreams := "pgshard_member_0001:pgshard_member_0001:streaming,pgshard_member_0002:pgshard_member_0002:streaming"
	waitForPhysicalReplicationStreams(t, ctx, namespace, sourcePodName, wantStreams)
	for member := int32(1); member < cluster.Spec.MembersPerShard; member++ {
		assertKINDPhysicalStandbyFailClosed(t, ctx, kubeClient, namespace, standbys[member].podName, member)
	}
	assertFailClosedApplicationServices(t, ctx, kubeClient, namespace, cluster.Name)

	if _, err := runPostgreSQLPodQuery(ctx, namespace, sourcePodName, "CREATE TABLE IF NOT EXISTS pgshard_kind_physical_replication (id integer PRIMARY KEY, note text NOT NULL); INSERT INTO pgshard_kind_physical_replication VALUES (1, 'before-restart') ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note; SELECT pg_catalog.pg_switch_wal();"); err != nil {
		t.Fatalf("write physical-replication marker on source: %v", err)
	}
	for member := int32(1); member < cluster.Spec.MembersPerShard; member++ {
		waitForPhysicalStandbyReplay(t, ctx, namespace, standbys[member].podName, 1, "before-restart")
	}

	restartedMember := int32(1)
	restarted := standbys[restartedMember]
	before := &corev1.Pod{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: restarted.podName}, before); err != nil {
		t.Fatal(err)
	}
	uid := before.UID
	resourceVersion := before.ResourceVersion
	if err := kubeClient.Delete(ctx, before, client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}); err != nil {
		t.Fatalf("delete physical standby member %d Pod: %v", restartedMember, err)
	}
	waitForRunningPhysicalStandby(t, ctx, kubeClient, types.NamespacedName{Namespace: namespace, Name: restarted.podName}, restarted.podUID)
	reusedPVC := &corev1.PersistentVolumeClaim{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: restarted.pvcName}, reusedPVC); err != nil {
		t.Fatal(err)
	}
	if reusedPVC.UID != restarted.pvcUID {
		t.Fatalf("physical standby member %d recreated its PVC: before=%q after=%q", restartedMember, restarted.pvcUID, reusedPVC.UID)
	}
	waitForPhysicalReplicationStreams(t, ctx, namespace, sourcePodName, wantStreams)
	assertKINDPhysicalStandbyFailClosed(t, ctx, kubeClient, namespace, restarted.podName, restartedMember)
	waitForPhysicalStandbyReplay(t, ctx, namespace, restarted.podName, 1, "before-restart")
	if _, err := runPostgreSQLPodQuery(ctx, namespace, sourcePodName, "INSERT INTO pgshard_kind_physical_replication VALUES (2, 'after-restart') ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note; SELECT pg_catalog.pg_switch_wal();"); err != nil {
		t.Fatalf("write post-restart physical-replication marker on source: %v", err)
	}
	for member := int32(1); member < cluster.Spec.MembersPerShard; member++ {
		waitForPhysicalStandbyReplay(t, ctx, namespace, standbys[member].podName, 2, "after-restart")
	}
}

func assertKINDSynchronousGenerationWaitsForRemoteReplay(t *testing.T, ctx context.Context, kubeClient client.Client, cluster *pgshardv1alpha1.PgShardCluster, sourcePodName string) {
	t.Helper()
	const shard = int32(0)
	if cluster.Spec.MembersPerShard != 3 || cluster.Spec.Durability != pgshardv1alpha1.DurabilitySynchronous {
		t.Fatalf("synchronous generation fixture topology = members %d durability %q", cluster.Spec.MembersPerShard, cluster.Spec.Durability)
	}
	if len(cluster.Status.PostgreSQLWritableLeases) != 1 {
		t.Fatalf("synchronous generation fixture writable Leases = %#v", cluster.Status.PostgreSQLWritableLeases)
	}
	checkpoint := cluster.Status.PostgreSQLWritableLeases[0]
	namespace := cluster.Namespace
	standbyPods := []string{
		owned.PostgreSQLMemberStatefulSetName(cluster.Name, shard, 1) + "-0",
		owned.PostgreSQLMemberStatefulSetName(cluster.Name, shard, 2) + "-0",
	}
	for _, podName := range standbyPods {
		setPhysicalStandbyReplayPaused(t, ctx, namespace, podName, true)
	}
	replayResumed := false
	t.Cleanup(func() {
		if replayResumed {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		for _, podName := range standbyPods {
			if _, err := runPostgreSQLPodQuery(cleanupCtx, namespace, podName, "SELECT pg_catalog.pg_wal_replay_resume();"); err != nil {
				t.Errorf("resume physical replay on %s during cleanup: %v", podName, err)
			}
		}
	})

	leaseKey := types.NamespacedName{Namespace: namespace, Name: checkpoint.LeaseName}
	initialLease := &coordinationv1.Lease{}
	if err := kubeClient.Get(ctx, leaseKey, initialLease); err != nil {
		t.Fatal(err)
	}
	if initialLease.Spec.HolderIdentity == nil || initialLease.Spec.LeaseTransitions == nil || initialLease.Spec.RenewTime == nil {
		t.Fatalf("initial synchronous source Lease = %#v", initialLease.Spec)
	}
	initialHolder := *initialLease.Spec.HolderIdentity
	initialTerm := *initialLease.Spec.LeaseTransitions

	managerRestored := false
	t.Cleanup(func() {
		if managerRestored {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		for _, arguments := range [][]string{
			{"--namespace", "pgshard-system", "scale", "deployment/pgshard-controller-manager", "--replicas=1"},
			{"--namespace", "pgshard-system", "rollout", "status", "deployment/pgshard-controller-manager", "--timeout=120s"},
		} {
			output, err := exec.CommandContext(cleanupCtx, "kubectl", arguments...).CombinedOutput()
			if err != nil {
				t.Errorf("restore manager with kubectl %s: %v\n%s", strings.Join(arguments, " "), err, output)
			}
		}
	})

	runKubectl(t, ctx, "--namespace", "pgshard-system", "scale", "deployment/pgshard-controller-manager", "--replicas=0")
	waitForManagerReplicas(t, ctx, kubeClient, 0)
	bindingKey := types.NamespacedName{Namespace: namespace, Name: owned.PostgreSQLAgentServiceAccountName(cluster.Name, shard)}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		binding := &rbacv1.RoleBinding{}
		err := kubeClient.Get(ctx, bindingKey, binding)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		uid := binding.UID
		resourceVersion := binding.ResourceVersion
		if err := kubeClient.Delete(ctx, binding, client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		return false, nil
	}); err != nil {
		t.Fatalf("remove synchronous source Lease permission: %v", err)
	}

	type sourceStatus struct {
		PostgresProcess string `json:"postgres_process"`
	}
	statusPath := fmt.Sprintf("/api/v1/namespaces/%s/pods/http:%s:8080/proxy/status", namespace, sourcePodName)
	readStatus := func(ctx context.Context) (sourceStatus, error) {
		var status sourceStatus
		output, err := exec.CommandContext(ctx, "kubectl", "get", "--raw", statusPath).CombinedOutput()
		if err != nil {
			return status, fmt.Errorf("read source status: %w: %s", err, output)
		}
		if err := json.Unmarshal(output, &status); err != nil {
			return status, fmt.Errorf("decode source status: %w", err)
		}
		return status, nil
	}
	var lastStatus sourceStatus
	if err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		status, err := readStatus(ctx)
		if err != nil {
			return false, nil
		}
		lastStatus = status
		return status.PostgresProcess == "fenced" || status.PostgresProcess == "validated", nil
	}); err != nil {
		t.Fatalf("wait for synchronous source authority fence: %v; status=%#v", err, lastStatus)
	}

	runKubectl(t, ctx, "--namespace", "pgshard-system", "scale", "deployment/pgshard-controller-manager", "--replicas=1")
	runKubectl(t, ctx, "--namespace", "pgshard-system", "rollout", "status", "deployment/pgshard-controller-manager", "--timeout=120s")
	waitForManagerReplicas(t, ctx, kubeClient, 1)
	managerRestored = true

	recoveredLease := &coordinationv1.Lease{}
	if err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, time.Minute, true, func(ctx context.Context) (bool, error) {
		recoveredLease = &coordinationv1.Lease{}
		if err := kubeClient.Get(ctx, leaseKey, recoveredLease); err != nil {
			return false, err
		}
		return recoveredLease.Spec.LeaseTransitions != nil && *recoveredLease.Spec.LeaseTransitions > initialTerm &&
			recoveredLease.Spec.HolderIdentity != nil && *recoveredLease.Spec.HolderIdentity != initialHolder && recoveredLease.Spec.RenewTime != nil, nil
	}); err != nil {
		t.Fatalf("wait for synchronous source higher term: %v; Lease=%#v", err, recoveredLease)
	}
	waitTerm := *recoveredLease.Spec.LeaseTransitions
	waitHolder := *recoveredLease.Spec.HolderIdentity
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		status, err := readStatus(ctx)
		if err != nil {
			return false, nil
		}
		lastStatus = status
		return status.PostgresProcess == "starting_replication_bootstrap", nil
	}); err != nil {
		t.Fatalf("wait for synchronous source publication start: %v; status=%#v", err, lastStatus)
	}

	// Local generation publication is capped at ten seconds. Holding both
	// standbys' replay paused beyond that boundary proves the composed remote
	// path remains Starting and is governed by Lease/shutdown authority instead.
	waitDeadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(waitDeadline) {
		status, err := readStatus(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if status.PostgresProcess != "starting_replication_bootstrap" {
			t.Fatalf("synchronous source left Starting before remote replay: %#v", status)
		}
		stableLease := &coordinationv1.Lease{}
		if err := kubeClient.Get(ctx, leaseKey, stableLease); err != nil {
			t.Fatal(err)
		}
		if stableLease.Spec.LeaseTransitions == nil || *stableLease.Spec.LeaseTransitions != waitTerm || stableLease.Spec.HolderIdentity == nil || *stableLease.Spec.HolderIdentity != waitHolder {
			t.Fatalf("synchronous source churned Lease term while awaiting replay: %#v", stableLease.Spec)
		}
		time.Sleep(500 * time.Millisecond)
	}

	setPhysicalStandbyReplayPaused(t, ctx, namespace, standbyPods[1], false)
	if err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, time.Minute, true, func(ctx context.Context) (bool, error) {
		status, err := readStatus(ctx)
		if err != nil {
			return false, nil
		}
		lastStatus = status
		return status.PostgresProcess == "running_replication_bootstrap", nil
	}); err != nil {
		t.Fatalf("wait for synchronous source after candidate replay: %v; status=%#v", err, lastStatus)
	}
	finalLease := &coordinationv1.Lease{}
	if err := kubeClient.Get(ctx, leaseKey, finalLease); err != nil {
		t.Fatal(err)
	}
	if finalLease.Spec.LeaseTransitions == nil || *finalLease.Spec.LeaseTransitions != waitTerm || finalLease.Spec.HolderIdentity == nil || *finalLease.Spec.HolderIdentity != waitHolder {
		t.Fatalf("synchronous source changed Lease term across remote publication: %#v", finalLease.Spec)
	}

	synchronousState, err := runPostgreSQLPodQuery(ctx, namespace, sourcePodName, "SELECT pg_catalog.current_setting('synchronous_standby_names') || '|' || pg_catalog.current_setting('synchronous_commit') || '|' || (EXISTS (SELECT 1 FROM pg_catalog.pg_stat_replication WHERE application_name = 'pgshard_member_0002' AND state = 'streaming' AND sync_state IN ('sync', 'quorum')))::text;")
	if err != nil || synchronousState != "ANY 1 (pgshard_member_0001, pgshard_member_0002)|local|true" {
		t.Fatalf("active synchronous source contract = %q, error=%v", synchronousState, err)
	}
	sourceGeneration, err := runPostgreSQLPodQuery(ctx, namespace, sourcePodName, "SELECT pg_catalog.encode(generation, 'hex') FROM pgshard_internal.writable_generation WHERE singleton;")
	if err != nil || sourceGeneration == "" {
		t.Fatalf("read source generation row: bytes=%q error=%v", sourceGeneration, err)
	}
	standbyGeneration, err := runPostgreSQLPodQuery(ctx, namespace, standbyPods[1], "SELECT pg_catalog.encode(generation, 'hex') FROM pgshard_internal.writable_generation WHERE singleton;")
	if err != nil || standbyGeneration != sourceGeneration {
		t.Fatalf("synchronous generation was not replayed to second candidate: source=%q standby=%q error=%v", sourceGeneration, standbyGeneration, err)
	}
	if _, err := runPostgreSQLPodQuery(ctx, namespace, standbyPods[0], "SELECT pg_catalog.pg_wal_replay_resume();"); err != nil {
		t.Fatalf("resume first synchronous candidate: %v", err)
	}
	replayResumed = true
	for member, podName := range standbyPods {
		assertKINDPhysicalStandbyFailClosed(t, ctx, kubeClient, namespace, podName, int32(member+1))
	}
	sourcePod := &corev1.Pod{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: sourcePodName}, sourcePod); err != nil {
		t.Fatal(err)
	}
	if podReady(sourcePod) || sourcePod.Status.ContainerStatuses[0].Ready {
		t.Fatalf("synchronous source became routable: conditions=%#v containers=%#v", sourcePod.Status.Conditions, sourcePod.Status.ContainerStatuses)
	}
}

func assertKINDPhysicalStandbyFailClosed(t *testing.T, ctx context.Context, kubeClient client.Client, namespace, podName string, member int32) {
	t.Helper()
	pod := &corev1.Pod{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, pod); err != nil {
		t.Fatal(err)
	}
	if _, hasRole := pod.Labels[owned.RoleLabel]; hasRole {
		t.Fatalf("physical standby member %d Pod received a serving role: %#v", member, pod.Labels)
	}
	if podReady(pod) || len(pod.Status.ContainerStatuses) != 1 || pod.Status.ContainerStatuses[0].Name != "postgresql" || pod.Status.ContainerStatuses[0].Ready {
		t.Fatalf("physical standby member %d became routable: conditions=%#v containers=%#v", member, pod.Status.Conditions, pod.Status.ContainerStatuses)
	}
	output, err := exec.CommandContext(ctx, "kubectl", "--namespace", namespace, "exec", podName, "--container=postgresql", "--", "pg_isready", "--quiet", "--timeout=2", "--host=127.0.0.1", "--port=5432").CombinedOutput()
	var exitError *exec.ExitError
	if err == nil {
		t.Fatalf("physical standby member %d exposed PostgreSQL TCP: %s", member, output)
	} else if !errors.As(err, &exitError) || exitError.ExitCode() != 2 {
		t.Fatalf("inspect physical standby member %d TCP state: %v: %s", member, err, output)
	}
}

func waitForRunningPhysicalStandby(t *testing.T, ctx context.Context, kubeClient client.Client, key types.NamespacedName, previousUID types.UID) *corev1.Pod {
	t.Helper()
	pod := &corev1.Pod{}
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 4*time.Minute, true, func(ctx context.Context) (bool, error) {
		pod = &corev1.Pod{}
		if err := kubeClient.Get(ctx, key, pod); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		if pod.UID == previousUID || len(pod.Status.ContainerStatuses) != 1 || pod.Status.ContainerStatuses[0].Name != "postgresql" {
			return false, nil
		}
		return pod.Status.Phase == corev1.PodRunning && pod.Status.ContainerStatuses[0].State.Running != nil, nil
	}); err != nil {
		t.Fatalf("wait for running physical standby Pod %s: %v; last Pod=%#v", key, err, pod)
	}
	return pod
}

func waitForPhysicalReplicationStreams(t *testing.T, ctx context.Context, namespace, sourcePodName, want string) {
	t.Helper()
	var last string
	var lastErr error
	query := "SELECT string_agg(replication.application_name || ':' || slots.slot_name || ':' || replication.state, ',' ORDER BY replication.application_name) FROM pg_catalog.pg_stat_replication AS replication JOIN pg_catalog.pg_replication_slots AS slots ON slots.active_pid = replication.pid WHERE replication.application_name IN ('pgshard_member_0001', 'pgshard_member_0002');"
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		last, lastErr = runPostgreSQLPodQuery(ctx, namespace, sourcePodName, query)
		return lastErr == nil && last == want, nil
	}); err != nil {
		t.Fatalf("wait for exact physical replication streams: %v; last=%q error=%v", err, last, lastErr)
	}
}

func waitForPhysicalStandbyReplay(t *testing.T, ctx context.Context, namespace, podName string, id int, note string) {
	t.Helper()
	want := "true|" + note
	var last string
	var lastErr error
	query := fmt.Sprintf("SELECT pg_catalog.pg_is_in_recovery()::text || '|' || note FROM pgshard_kind_physical_replication WHERE id = %d;", id)
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		last, lastErr = runPostgreSQLPodQuery(ctx, namespace, podName, query)
		return lastErr == nil && last == want, nil
	}); err != nil {
		t.Fatalf("wait for physical replay on Pod %s: %v; want=%q last=%q error=%v", podName, err, want, last, lastErr)
	}
}

func setPhysicalStandbyReplayPaused(t *testing.T, ctx context.Context, namespace, podName string, paused bool) {
	t.Helper()
	action := "pause"
	request := "SELECT pg_catalog.pg_wal_replay_pause();"
	want := "paused"
	if !paused {
		action = "resume"
		request = "SELECT pg_catalog.pg_wal_replay_resume();"
		want = "not paused"
	}
	if _, err := runPostgreSQLPodQuery(ctx, namespace, podName, request); err != nil {
		t.Fatalf("request physical replay %s on %s: %v", action, podName, err)
	}
	var state string
	var stateErr error
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		state, stateErr = runPostgreSQLPodQuery(ctx, namespace, podName, "SELECT pg_catalog.pg_get_wal_replay_pause_state();")
		return stateErr == nil && state == want, nil
	}); err != nil {
		t.Fatalf("wait for physical replay %s on %s: %v; want=%q state=%q error=%v", action, podName, err, want, state, stateErr)
	}
}

func runPostgreSQLPodQuery(ctx context.Context, namespace, podName, query string) (string, error) {
	arguments := []string{
		"--namespace", namespace,
		"exec", podName,
		"--container=postgresql",
		"--",
		"psql", "-X", "--no-password", "--host=/run/pgshard/postgres", "--username=postgres", "--dbname=postgres", "--no-align", "--tuples-only",
		"--set=ON_ERROR_STOP=1",
		"--command=" + query,
	}
	output, err := exec.CommandContext(ctx, "kubectl", arguments...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("kubectl %s: %w: %s", strings.Join(arguments, " "), err, strings.TrimSpace(string(output)))
	}
	return strings.TrimSpace(string(output)), nil
}

func assertPostgreSQLServiceQueryDenied(t *testing.T, ctx context.Context, kubeClient client.Client, namespace, suffix string, labels map[string]string, image, secret, host string) {
	t.Helper()
	clientPod := postgreSQLClientPod(namespace, fmt.Sprintf("pgshard-sql-client-%s-%d-%d", suffix, os.Getpid(), time.Now().UnixNano()), labels, image, secret, host, "SELECT 1")
	if err := kubeClient.Create(ctx, clientPod); err != nil {
		t.Fatal(err)
	}
	current := &corev1.Pod{}
	err := wait.PollUntilContextTimeout(ctx, time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		current = &corev1.Pod{}
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(clientPod), current); err != nil {
			return false, err
		}
		if current.Status.Phase == corev1.PodSucceeded {
			return false, fmt.Errorf("network policy admitted PostgreSQL traffic from Pod %s with labels %#v", clientPod.Name, labels)
		}
		return current.Status.Phase == corev1.PodFailed, nil
	})
	if err != nil {
		t.Fatalf("wait for denied PostgreSQL client Pod: %v; last status = %#v", err, current.Status)
	}
	output := strings.TrimSpace(runKubectl(t, ctx, "--namespace", namespace, "logs", clientPod.Name))
	if !strings.Contains(output, "connection to server") || !strings.Contains(output, "timeout expired") {
		t.Fatalf("denied PostgreSQL client failed for an unexpected reason: %q", output)
	}
}

func assertCatalogLoginRejected(t *testing.T, ctx context.Context, kubeClient client.Client, namespace, cluster, image, secret, host, suffix, database, sslMode string) {
	t.Helper()
	clientPod := postgreSQLClientPod(namespace, fmt.Sprintf("pgshard-catalog-client-%s-%d-%d", suffix, os.Getpid(), time.Now().UnixNano()), map[string]string{
		owned.ClusterLabel:   cluster,
		owned.ComponentLabel: "pooler",
	}, image, secret, host, "SELECT 1")
	container := &clientPod.Spec.Containers[0]
	container.Args = []string{"-X", "-w", "-h", host, "-U", "pgshard_pooler_catalog", "-d", database, "-Atc", "SELECT 1"}
	container.Env[1].ValueFrom.SecretKeyRef.Key = owned.CatalogPasswordKey
	container.Env = append(container.Env, corev1.EnvVar{Name: "PGSSLMODE", Value: sslMode})
	if err := kubeClient.Create(ctx, clientPod); err != nil {
		t.Fatal(err)
	}
	current := &corev1.Pod{}
	err := wait.PollUntilContextTimeout(ctx, time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		current = &corev1.Pod{}
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(clientPod), current); err != nil {
			return false, err
		}
		if current.Status.Phase == corev1.PodSucceeded {
			return false, fmt.Errorf("catalog login unexpectedly reached database %s with sslmode=%s", database, sslMode)
		}
		return current.Status.Phase == corev1.PodFailed, nil
	})
	if err != nil {
		t.Fatalf("wait for rejected catalog login Pod: %v; last status = %#v", err, current.Status)
	}
	output := strings.TrimSpace(runKubectl(t, ctx, "--namespace", namespace, "logs", clientPod.Name))
	if !strings.Contains(output, "pg_hba.conf rejects connection") ||
		!strings.Contains(output, "user \"pgshard_pooler_catalog\"") ||
		!strings.Contains(output, "database \""+database+"\"") {
		t.Fatalf("catalog login was rejected for an unexpected reason: %q", output)
	}
}

func assertPoolerCatalogDatabaseRejected(t *testing.T, ctx context.Context, kubeClient client.Client, namespace, cluster, image, secret, host string) {
	t.Helper()
	clientPod := postgreSQLClientPod(namespace, fmt.Sprintf("pgshard-pooler-catalog-client-%d-%d", os.Getpid(), time.Now().UnixNano()), map[string]string{
		owned.ClusterLabel:   cluster,
		owned.ComponentLabel: "pooler",
	}, image, secret, host, "SELECT 1")
	clientPod.Spec.Containers[0].Args = []string{"-X", "-w", "-h", host, "-U", "postgres", "-d", "shardschema", "-Atc", "SELECT 1"}
	if err := kubeClient.Create(ctx, clientPod); err != nil {
		t.Fatal(err)
	}
	current := &corev1.Pod{}
	err := wait.PollUntilContextTimeout(ctx, time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		current = &corev1.Pod{}
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(clientPod), current); err != nil {
			return false, err
		}
		if current.Status.Phase == corev1.PodSucceeded {
			return false, fmt.Errorf("application pooler exposed shardschema")
		}
		return current.Status.Phase == corev1.PodFailed, nil
	})
	if err != nil {
		t.Fatalf("wait for pooler catalog rejection Pod: %v; last status = %#v", err, current.Status)
	}
	output := strings.TrimSpace(runKubectl(t, ctx, "--namespace", namespace, "logs", clientPod.Name))
	if !strings.Contains(output, "shardschema is not available through the application pooler") {
		t.Fatalf("application pooler rejected shardschema for an unexpected reason: %q", output)
	}
}

func postgreSQLClientPod(namespace, name string, labels map[string]string, image, secret, host, query string) *corev1.Pod {
	allowPrivilegeEscalation := false
	readOnlyRootFilesystem := true
	runAsNonRoot := true
	automount := false
	postgresUID := int64(999)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    maps.Clone(labels),
		},
		Spec: corev1.PodSpec{
			AutomountServiceAccountToken: &automount,
			RestartPolicy:                corev1.RestartPolicyNever,
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot:   &runAsNonRoot,
				RunAsUser:      &postgresUID,
				RunAsGroup:     &postgresUID,
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Containers: []corev1.Container{{
				Name:    "psql",
				Image:   image,
				Command: []string{"psql"},
				Args:    []string{"-X", "-w", "-h", host, "-U", "postgres", "-d", "postgres", "-Atc", query},
				Env: []corev1.EnvVar{
					{Name: "PGCONNECT_TIMEOUT", Value: "5"},
					{
						Name: "PGPASSWORD",
						ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: secret},
							Key:                  owned.PostgreSQLPasswordKey,
						}},
					},
				},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: &allowPrivilegeEscalation,
					ReadOnlyRootFilesystem:   &readOnlyRootFilesystem,
					RunAsNonRoot:             &runAsNonRoot,
					RunAsUser:                &postgresUID,
					RunAsGroup:               &postgresUID,
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
			}},
		},
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

func agentEnvironmentValue(environment []corev1.EnvVar, name string) string {
	for _, variable := range environment {
		if variable.Name == name {
			return variable.Value
		}
	}
	return ""
}

func podVolumeByName(t *testing.T, volumes []corev1.Volume, name string) corev1.VolumeSource {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name == name {
			return volume.VolumeSource
		}
	}
	t.Fatalf("Pod volume %q not found: %#v", name, volumes)
	return corev1.VolumeSource{}
}

func podContainerHasVolumeMount(mounts []corev1.VolumeMount, name string, readOnly bool) bool {
	for _, mount := range mounts {
		if mount.Name == name && mount.ReadOnly == readOnly {
			return true
		}
	}
	return false
}

func podContainerHasNamedVolumeMount(mounts []corev1.VolumeMount, name string) bool {
	for _, mount := range mounts {
		if mount.Name == name {
			return true
		}
	}
	return false
}

func projectedSecretItemKeys(items []corev1.KeyToPath) []string {
	keys := make([]string, 0, len(items))
	for _, item := range items {
		keys = append(keys, item.Key)
	}
	return keys
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func deleteNamespaceAtCleanup(t *testing.T, kubeClient client.Client, namespace *corev1.Namespace) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if t.Failed() {
			logNamespaceDiagnostics(t, ctx, namespace.Name)
		}
		clusters := &pgshardv1alpha1.PgShardClusterList{}
		if err := kubeClient.List(ctx, clusters, client.InNamespace(namespace.Name)); err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("list test clusters in namespace %s: %v", namespace.Name, err)
		} else {
			for index := range clusters.Items {
				cluster := &clusters.Items[index]
				if cluster.DeletionTimestamp == nil {
					if err := kubeClient.Delete(ctx, cluster, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil && !apierrors.IsNotFound(err) {
						t.Errorf("delete test cluster %s/%s: %v", cluster.Namespace, cluster.Name, err)
					}
				}
				key := client.ObjectKeyFromObject(cluster)
				if err := wait.PollUntilContextTimeout(ctx, time.Second, 90*time.Second, true, func(ctx context.Context) (bool, error) {
					err := kubeClient.Get(ctx, key, &pgshardv1alpha1.PgShardCluster{})
					return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
				}); err != nil {
					t.Errorf("wait for test cluster %s/%s deletion: %v", key.Namespace, key.Name, err)
				}
			}
		}
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

func logNamespaceDiagnostics(t *testing.T, ctx context.Context, namespace string) {
	t.Helper()
	for _, args := range [][]string{
		{"--namespace", namespace, "get", "pods,statefulsets,persistentvolumeclaims", "--output=wide"},
		{"--namespace", namespace, "get", "pods", "--output=jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{range .status.containerStatuses[*]}  {.name}: waiting={.state.waiting.reason}: {.state.waiting.message}; terminated={.state.terminated.reason}: {.state.terminated.message}{\"\\n\"}{end}{end}"},
		{"--namespace", namespace, "get", "events", "--sort-by=.lastTimestamp"},
		{"--namespace", namespace, "describe", "pods"},
		{"--namespace", namespace, "logs", "--selector=" + owned.ClusterLabel, "--all-containers", "--tail=100", "--prefix"},
	} {
		output, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
		t.Logf("kubectl %s\n%s", strings.Join(args, " "), strings.TrimSpace(string(output)))
		if err != nil {
			t.Logf("diagnostic command failed: %v", err)
		}
	}
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
	configurations := &corev1.ConfigMapList{}
	if err := kubeClient.List(ctx, configurations, client.InNamespace(cluster.Namespace), client.MatchingLabels{owned.ClusterLabel: cluster.Name, owned.ComponentLabel: "configuration"}); err != nil {
		t.Fatal(err)
	}
	var configuration *corev1.ConfigMap
	prefix := cluster.Name + owned.PostgreSQLConfigSuffix + "-"
	for index := range configurations.Items {
		if strings.HasPrefix(configurations.Items[index].Name, prefix) {
			if configuration != nil {
				t.Fatalf("multiple active PostgreSQL configurations found: %s and %s", configuration.Name, configurations.Items[index].Name)
			}
			configuration = &configurations.Items[index]
		}
	}
	if configuration == nil {
		t.Fatalf("PostgreSQL configuration with prefix %q not found", prefix)
	}
	wantDocuments := 3 + int(cluster.Spec.MembersPerShard)*2
	if len(configuration.Data) != wantDocuments {
		t.Fatalf("PostgreSQL configuration documents = %#v", configuration.Data)
	}
	databaseTopologyPreflight := configuration.Data["database-topology-preflight.sql"]
	for _, statement := range []string{
		"FOR UPDATE;\n",
		"RestoreTopologyMismatch: shardschema logical database topology conflicts",
	} {
		if !strings.Contains(databaseTopologyPreflight, statement) {
			t.Fatalf("database topology preflight is missing %q:\n%s", statement, databaseTopologyPreflight)
		}
	}
	databaseGenesis := configuration.Data["database-genesis.sql"]
	for _, statement := range []string{
		"BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;\n",
		"database genesis contains an undeclared active logical database",
		"COMMIT;\n",
	} {
		if !strings.Contains(databaseGenesis, statement) {
			t.Fatalf("database genesis is missing %q:\n%s", statement, databaseGenesis)
		}
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

func waitForManagerReplicas(t *testing.T, ctx context.Context, kubeClient client.Client, wanted int32) {
	t.Helper()
	deployment := &appsv1.Deployment{}
	managerPods := &corev1.PodList{}
	key := types.NamespacedName{Namespace: "pgshard-system", Name: "pgshard-controller-manager"}
	err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		deployment = &appsv1.Deployment{}
		if err := kubeClient.Get(ctx, key, deployment); err != nil {
			return false, err
		}
		if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != wanted || deployment.Status.ObservedGeneration < deployment.Generation {
			return false, nil
		}
		if wanted == 0 {
			if deployment.Status.Replicas != 0 || deployment.Status.ReadyReplicas != 0 || deployment.Status.AvailableReplicas != 0 {
				return false, nil
			}
			// Deployment replica counters exclude a terminating Pod before its
			// preStop hook and webhook server have necessarily exited.  The
			// outage fixture must wait for the process itself to disappear or a
			// cached admission connection can still authenticate termination.
			managerPods = &corev1.PodList{}
			if err := kubeClient.List(ctx, managerPods,
				client.InNamespace("pgshard-system"),
				client.MatchingLabels{"app.kubernetes.io/name": "pgshard-operator", "app.kubernetes.io/component": "controller-manager"},
			); err != nil {
				return false, err
			}
			return len(managerPods.Items) == 0, nil
		}
		return deployment.Status.UpdatedReplicas == wanted && deployment.Status.ReadyReplicas == wanted && deployment.Status.AvailableReplicas == wanted, nil
	})
	if err != nil {
		t.Fatalf("wait for manager replicas %d: %v; last status = %#v; last pods = %#v", wanted, err, deployment.Status, managerPods.Items)
	}
}

func waitForQuarantinedPostgreSQL(t *testing.T, ctx context.Context, namespace, podName, phase string) {
	t.Helper()
	var probeOutput []byte
	var probeErr error
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		probeOutput, probeErr = exec.CommandContext(probeCtx, "kubectl", "--namespace", namespace, "exec", podName, "--container=postgresql", "--", "pg_isready", "--quiet", "--host=/run/pgshard/postgres", "--port=5432").CombinedOutput()
		cancel()
		return probeErr == nil, nil
	}); err != nil {
		t.Fatalf("wait for quarantined PostgreSQL during %s: %v; last probe error=%v\n%s", phase, err, probeErr, probeOutput)
	}
}

func assertDurableWritableGeneration(
	t *testing.T,
	ctx context.Context,
	namespace string,
	podName string,
	clusterName string,
	clusterUID types.UID,
	checkpoint pgshardv1alpha1.PostgreSQLWritableLeaseStatus,
	holder string,
	term int32,
) {
	t.Helper()
	execCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	output, err := exec.CommandContext(
		execCtx,
		"kubectl",
		"--namespace", namespace,
		"exec", podName,
		"--container=postgresql",
		"--",
		"cat", "/var/lib/postgresql/18/docker/.pgshard-writable-generation",
	).CombinedOutput()
	if err != nil {
		t.Fatalf("read durable writable generation: %v\n%s", err, output)
	}
	want := fmt.Sprintf(
		"format=1\ncluster_name=%s\ncluster_uid=%s\nshard=%d\nlease_namespace=%s\nlease_name=%s\nlease_uid=%s\nholder=%s\nterm=%d\n",
		clusterName,
		clusterUID,
		checkpoint.Shard,
		namespace,
		checkpoint.LeaseName,
		checkpoint.LeaseUID,
		holder,
		term,
	)
	if string(output) != want {
		t.Fatalf("durable writable generation = %q, want %q", output, want)
	}
}

type postgreSQLProcessPin struct {
	pid          uint64
	processGroup uint64
	startTime    uint64
}

func pinQuarantinedPostgreSQLProcess(t *testing.T, ctx context.Context, namespace, podName string) postgreSQLProcessPin {
	t.Helper()
	execCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	output, err := exec.CommandContext(
		execCtx,
		"kubectl",
		"--namespace", namespace,
		"exec", podName,
		"--container=postgresql",
		"--",
		"sh", "-ceu",
		`set -f
IFS= read -r pid < /run/pgshard/postgres/postmaster.external.pid
case "$pid" in ''|*[!0-9]*) exit 41 ;; esac
stat=$(cat "/proc/$pid/stat")
stat=${stat##*) }
set -- $stat
test "$#" -ge 20
test "$1" != Z
process_group=$3
shift 19
start_time=$1
case "$process_group:$start_time" in *[!0-9:]*|:*|*:) exit 41 ;; esac
printf '%s %s %s\n' "$pid" "$process_group" "$start_time"`,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("pin quarantined PostgreSQL process: %v\n%s", err, output)
	}
	fields := strings.Fields(string(output))
	if len(fields) != 3 {
		t.Fatalf("pin quarantined PostgreSQL process returned %q", output)
	}
	values := make([]uint64, len(fields))
	for index, field := range fields {
		value, err := strconv.ParseUint(field, 10, 64)
		if err != nil || value == 0 {
			t.Fatalf("pin quarantined PostgreSQL process returned invalid value %q", output)
		}
		values[index] = value
	}
	return postgreSQLProcessPin{pid: values[0], processGroup: values[1], startTime: values[2]}
}

func provePinnedPostgreSQLProcessAbsent(t *testing.T, ctx context.Context, kubeClient client.Client, namespace, podName string, podUID types.UID, pin postgreSQLProcessPin) error {
	t.Helper()
	const absentMarker = "pgshard-postgres-absent"
	const liveMarker = "pgshard-postgres-live"
	const missingContainer = `error: unable to upgrade connection: container not found ("postgresql")`
	const missingContainerInternal = `error: Internal error occurred: unable to upgrade connection: container not found ("postgresql")`
	current := &corev1.Pod{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, current); apierrors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("read exact PostgreSQL Pod: %w", err)
	}
	if current.UID != podUID {
		return fmt.Errorf("Pod identity changed: got %s, want %s", current.UID, podUID)
	}
	for _, status := range current.Status.ContainerStatuses {
		if status.Name == "postgresql" && status.State.Terminated != nil {
			return nil
		}
	}

	probe := fmt.Sprintf(`for stat_path in /proc/[0-9]*/stat; do
  test -r "$stat_path" || continue
  stat=$(cat "$stat_path") || continue
  stat=${stat##*) }
  set -- $stat
  test "$#" -ge 20 || exit 43
  process_group=$3
  shift 19
  start_time=$1
  pid=${stat_path#/proc/}
  pid=${pid%%/stat}
  if { test "$pid" = %d && test "$start_time" = %d; } || test "$process_group" = %d; then
    printf 'pgshard-postgres-live\n'
    exit 42
  fi
done
printf 'pgshard-postgres-absent\n'`, pin.pid, pin.startTime, pin.processGroup)
	execCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	command := exec.CommandContext(
		execCtx,
		"kubectl",
		"--namespace", namespace,
		"exec", podName,
		"--container=postgresql",
		"--",
		"sh", "-ceu", probe,
	)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, execErr := command.Output()
	cancel()
	trimmed := strings.TrimSpace(string(output))
	trimmedStderr := strings.TrimSpace(stderr.String())
	if execErr == nil && trimmed == absentMarker && trimmedStderr == "" {
		return nil
	}
	if exitError, ok := execErr.(*exec.ExitError); ok && exitError.ExitCode() == 42 && trimmed == liveMarker {
		return fmt.Errorf("pinned postmaster or process group remains live")
	}
	if current.DeletionTimestamp != nil && execErr != nil && trimmed == "" && (trimmedStderr == missingContainer || trimmedStderr == missingContainerInternal) {
		return nil
	}
	return fmt.Errorf("process-table probe failed: %v; stdout=%q; stderr=%q", execErr, output, stderr.String())
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

type podRuntimeIdentity struct {
	uid       types.UID
	startedAt time.Time
}

func assertOrchestratorReadinessTracksLeaseIdentity(t *testing.T, ctx context.Context, kubeClient client.Client, namespace, cluster string) {
	t.Helper()
	incarnations := capturePodRuntimeIdentities(t, ctx, kubeClient, namespace, cluster, "orchestrator", 3)
	managerRestored := false
	t.Cleanup(func() {
		if managerRestored {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		commands := [][]string{
			{"--namespace", "pgshard-system", "scale", "deployment/pgshard-controller-manager", "--replicas=1"},
			{"--namespace", "pgshard-system", "rollout", "status", "deployment/pgshard-controller-manager", "--timeout=120s"},
		}
		for _, arguments := range commands {
			output, err := exec.CommandContext(cleanupCtx, "kubectl", arguments...).CombinedOutput()
			if err != nil {
				t.Errorf("restore quorum fixture with kubectl %s: %v\n%s", strings.Join(arguments, " "), err, output)
			}
		}
	})

	runKubectl(t, ctx, "--namespace", "pgshard-system", "scale", "deployment/pgshard-controller-manager", "--replicas=0")
	waitForManagerReplicas(t, ctx, kubeClient, 0)
	lease := &coordinationv1.Lease{}
	leaseKey := types.NamespacedName{Namespace: namespace, Name: cluster + owned.OrchestratorLeaseSuffix}
	if err := kubeClient.Get(ctx, leaseKey, lease); err != nil {
		t.Fatal(err)
	}
	oldLeaseUID := lease.UID
	uid := lease.UID
	resourceVersion := lease.ResourceVersion
	if err := kubeClient.Delete(ctx, lease, client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}); err != nil {
		t.Fatal(err)
	}
	waitForExistingPodReadiness(t, ctx, kubeClient, namespace, cluster, "orchestrator", incarnations, false)

	runKubectl(t, ctx, "--namespace", "pgshard-system", "scale", "deployment/pgshard-controller-manager", "--replicas=1")
	runKubectl(t, ctx, "--namespace", "pgshard-system", "rollout", "status", "deployment/pgshard-controller-manager", "--timeout=120s")
	waitForManagerReplicas(t, ctx, kubeClient, 1)
	managerRestored = true
	if err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, time.Minute, true, func(ctx context.Context) (bool, error) {
		current := &coordinationv1.Lease{}
		if err := kubeClient.Get(ctx, leaseKey, current); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		return current.UID != "" && current.UID != oldLeaseUID, nil
	}); err != nil {
		t.Fatalf("wait for replacement orchestrator Lease: %v", err)
	}
	// A Lease UID change is a new coordination universe. Existing processes
	// remain fail closed; a bounded rollout establishes new process identities.
	runKubectl(t, ctx, "--namespace", namespace, "rollout", "restart", "deployment/"+cluster+owned.OrchestratorSuffix)
	runKubectl(t, ctx, "--namespace", namespace, "rollout", "status", "deployment/"+cluster+owned.OrchestratorSuffix, "--timeout=180s")
	waitForStablePods(t, ctx, kubeClient, namespace, cluster, "orchestrator", 3, true)
}

func capturePodRuntimeIdentities(t *testing.T, ctx context.Context, kubeClient client.Client, namespace, cluster, component string, wanted int) map[string]podRuntimeIdentity {
	t.Helper()
	pods := &corev1.PodList{}
	if err := kubeClient.List(ctx, pods, client.InNamespace(namespace), client.MatchingLabels{owned.ClusterLabel: cluster, owned.ComponentLabel: component}); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != wanted {
		t.Fatalf("capture %s runtime identities: got %d pods, want %d", component, len(pods.Items), wanted)
	}
	identities := make(map[string]podRuntimeIdentity, wanted)
	for index := range pods.Items {
		pod := &pods.Items[index]
		if len(pod.Status.ContainerStatuses) != 1 || pod.Status.ContainerStatuses[0].State.Running == nil {
			t.Fatalf("capture %s runtime identity for %s: %#v", component, pod.Name, pod.Status)
		}
		identities[pod.Name] = podRuntimeIdentity{uid: pod.UID, startedAt: pod.Status.ContainerStatuses[0].State.Running.StartedAt.Time}
	}
	return identities
}

func replaceManagerArguments(ctx context.Context, kubeClient client.Client, arguments []string) error {
	key := types.NamespacedName{Namespace: "pgshard-system", Name: "pgshard-controller-manager"}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		deployment := &appsv1.Deployment{}
		if err := kubeClient.Get(ctx, key, deployment); err != nil {
			return err
		}
		if len(deployment.Spec.Template.Spec.Containers) != 1 {
			return fmt.Errorf("manager has %d containers, want 1", len(deployment.Spec.Template.Spec.Containers))
		}
		deployment.Spec.Template.Spec.Containers[0].Args = append([]string(nil), arguments...)
		return kubeClient.Update(ctx, deployment)
	})
}

func waitForExistingPodReadiness(t *testing.T, ctx context.Context, kubeClient client.Client, namespace, cluster, component string, identities map[string]podRuntimeIdentity, wanted bool) {
	t.Helper()
	pods := &corev1.PodList{}
	err := wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, time.Minute, true, func(ctx context.Context) (bool, error) {
		pods = &corev1.PodList{}
		if err := kubeClient.List(ctx, pods, client.InNamespace(namespace), client.MatchingLabels{owned.ClusterLabel: cluster, owned.ComponentLabel: component}); err != nil {
			return false, err
		}
		if len(pods.Items) != len(identities) {
			return false, fmt.Errorf("%s pod count changed from %d to %d", component, len(identities), len(pods.Items))
		}
		for index := range pods.Items {
			pod := &pods.Items[index]
			identity, ok := identities[pod.Name]
			if !ok || identity.uid != pod.UID || len(pod.Status.ContainerStatuses) != 1 {
				return false, fmt.Errorf("%s pod incarnation changed: %#v", component, pod.ObjectMeta)
			}
			status := pod.Status.ContainerStatuses[0]
			if status.RestartCount != 0 || status.State.Running == nil || !status.State.Running.StartedAt.Time.Equal(identity.startedAt) {
				return false, fmt.Errorf("%s pod %s restarted during Lease transition: %#v", component, pod.Name, status)
			}
			if status.Ready != wanted {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("wait for existing %s pods ready=%t: %v; last pods = %#v", component, wanted, err, pods.Items)
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

func assertUnsupportedApplicationServicesHaveNoEndpoints(t *testing.T, ctx context.Context, kubeClient client.Client, namespace, cluster string) {
	t.Helper()
	for _, suffix := range []string{"-ro", "-r"} {
		serviceName := cluster + suffix
		service := &corev1.Service{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: serviceName}, service); err != nil {
			t.Fatal(err)
		}
		if service.Spec.Selector != nil {
			t.Fatalf("unsupported application Service %s has selector %#v", serviceName, service.Spec.Selector)
		}
		slices := &discoveryv1.EndpointSliceList{}
		if err := kubeClient.List(ctx, slices, client.InNamespace(namespace), client.MatchingLabels{discoveryv1.LabelServiceName: serviceName}); err != nil {
			t.Fatal(err)
		}
		for _, slice := range slices.Items {
			if len(slice.Endpoints) != 0 {
				t.Fatalf("unsupported application Service %s has endpoints %#v", serviceName, slice.Endpoints)
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
	if len(statefulSets.Items) != 0 {
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
