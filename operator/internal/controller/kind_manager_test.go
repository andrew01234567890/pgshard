package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/operator/internal/podfence"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
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
	assertRestoreNamespaceHasNoTargets(t, ctx, kubeClient, namespace.Name, sentinel, 1)
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
	if err := kubeClient.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	waitForSingleMemberPostgreSQL(t, ctx, kubeClient, client.ObjectKeyFromObject(cluster))
	waitForPoolerCatalogTLS(t, ctx, kubeClient, namespace.Name, cluster.Name)
	assertPostgreSQLStatusMetadataImmutable(t, ctx, kubeClient, types.NamespacedName{
		Namespace: namespace.Name,
		Name:      cluster.Name + "-shard-0000-primary-0",
	})
	assertPostgreSQLSpecImmutable(t, ctx, kubeClient, types.NamespacedName{
		Namespace: namespace.Name,
		Name:      cluster.Name + "-shard-0000-primary-0",
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

	shardZeroPod := cluster.Name + "-shard-0000-primary-0"
	shardOnePod := cluster.Name + "-shard-0001-primary-0"
	initialShardZero := &corev1.Pod{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: shardZeroPod}, initialShardZero); err != nil {
		t.Fatal(err)
	}
	if len(initialShardZero.Spec.InitContainers) != 1 || initialShardZero.Spec.InitContainers[0].Image != "pgshard/postgres-agent:dev" || initialShardZero.Spec.InitContainers[0].ImagePullPolicy != corev1.PullNever || initialShardZero.Annotations["pgshard.io/shardschema-migration-sha256"] == "" {
		t.Fatalf("shard-0000 bootstrap image contract = %#v", initialShardZero)
	}
	if len(initialShardZero.Status.InitContainerStatuses) != 1 || initialShardZero.Status.InitContainerStatuses[0].ImageID == "" || initialShardZero.Status.InitContainerStatuses[0].RestartCount != 0 || initialShardZero.Status.InitContainerStatuses[0].State.Terminated == nil || initialShardZero.Status.InitContainerStatuses[0].State.Terminated.ExitCode != 0 {
		t.Fatalf("shard-0000 bootstrap completion = %#v", initialShardZero.Status.InitContainerStatuses)
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
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: owned.PostgreSQLPrimaryStatefulSetName(cluster.Name, 0)}, shardZeroStatefulSet); err != nil {
			return false, err
		}
		shardOneStatefulSet := &appsv1.StatefulSet{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace.Name, Name: owned.PostgreSQLPrimaryStatefulSetName(cluster.Name, 1)}, shardOneStatefulSet); err != nil {
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
	if got := strings.TrimSpace(runKubectl(t, ctx, "--namespace", namespace.Name, "exec", "--container", "postgresql", shardZeroPod, "--",
		"psql", "-X", "-U", "postgres", "-d", "shardschema", "-Atc",
		"SELECT state.catalog_epoch, string_agg(incarnations.shard_id::text || '=' || incarnations.restore_incarnation::text, ',' ORDER BY incarnations.shard_id) FROM pgshard_catalog.cluster_state AS state CROSS JOIN pgshard_catalog.shard_restore_incarnations AS incarnations WHERE state.singleton AND incarnations.state = 'active' GROUP BY state.catalog_epoch")); got != catalogSnapshot {
		t.Fatalf("idempotent shardschema restart changed snapshot from %q to %q", catalogSnapshot, got)
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
		if status.Ready || status.Catalog.Phase != "connected" || !status.Catalog.ConnectionUp || !status.Catalog.Ready || status.Catalog.ReadinessReason != "ready" || status.Catalog.CatalogEpoch == nil || status.Catalog.LastFailure != nil {
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
		"pgshard_pooler_ready 0",
		"pgshard_pooler_catalog_ready 1",
		"pgshard_pooler_catalog_connection_up 1",
	} {
		if !strings.Contains(string(metrics), sample) {
			t.Fatalf("pooler metrics lack %q:\n%s", sample, metrics)
		}
	}
	readiness, err := exec.CommandContext(ctx, "kubectl", "get", "--raw", readinessPath).CombinedOutput()
	if err == nil || (!strings.Contains(string(readiness), "data_plane_unavailable") && !strings.Contains(string(readiness), "ServiceUnavailable")) {
		t.Fatalf("pooler /readyz = error %v, output %q; want HTTP 503 data_plane_unavailable", err, readiness)
	}
	assertFailClosedApplicationServices(t, ctx, kubeClient, namespace, cluster)
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
			cluster := &pgshardv1alpha1.PgShardCluster{}
			if err := kubeClient.Get(ctx, key, cluster); err != nil {
				t.Fatal(err)
			}
			test.mutate(cluster)
			err := kubeClient.Update(ctx, cluster)
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
			current := &corev1.Pod{}
			if err := kubeClient.Get(ctx, key, current); err != nil {
				t.Fatal(err)
			}
			err := test.update(current)
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
	podKey := types.NamespacedName{Namespace: namespace.Name, Name: cluster.Name + "-shard-0000-primary-0"}
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
		"PostgreSQL state":     &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: owned.PostgreSQLPrimaryStatefulSetName(cluster.Name, 0), Namespace: namespace.Name}},
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
		"PostgreSQL Pod":       &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: cluster.Name + "-shard-0000-primary-0", Namespace: namespace.Name}},
		"PostgreSQL PVC":       &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: claimKey.Name, Namespace: claimKey.Namespace}},
		"PostgreSQL state":     &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: owned.PostgreSQLPrimaryStatefulSetName(cluster.Name, 0), Namespace: namespace.Name}},
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
