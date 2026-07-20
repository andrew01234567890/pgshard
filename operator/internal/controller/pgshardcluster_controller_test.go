package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"testing"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/operator/internal/pki"
	"github.com/andrew01234567890/pgshard/operator/internal/podfence"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
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
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

type createPodAfterListReader struct {
	client.Reader
	writer   client.Client
	pod      *corev1.Pod
	injected bool
}

func developmentReconciler(kubeClient client.Client, apiReader client.Reader) *PgShardClusterReconciler {
	return &PgShardClusterReconciler{
		Client:    kubeClient,
		APIReader: apiReader,
		Images:    owned.DevelopmentImages(),
	}
}

func (r *createPodAfterListReader) List(ctx context.Context, list client.ObjectList, options ...client.ListOption) error {
	if err := r.Reader.List(ctx, list, options...); err != nil {
		return err
	}
	if _, ok := list.(*corev1.PodList); !ok || r.injected {
		return nil
	}
	r.injected = true
	return r.writer.Create(ctx, r.pod)
}

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

	for _, name := range []string{"example-rw", "example-ro", "example-r", "example-shard-0000", "example-shard-0001", "example-orchestrator", "example-pooler"} {
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
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "example-topology", Namespace: cluster.Namespace}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "example-orchestrator", Namespace: cluster.Namespace}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "example-orchestrator", Namespace: cluster.Namespace}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "example-orchestrator", Namespace: cluster.Namespace}},
		&coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "example-orch-lease", Namespace: cluster.Namespace}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "example-orchestrator", Namespace: cluster.Namespace}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "example-pooler", Namespace: cluster.Namespace}},
		&autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: "example-pooler", Namespace: cluster.Namespace}},
		&policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: "example-pooler", Namespace: cluster.Namespace}},
	} {
		key := client.ObjectKeyFromObject(object)
		if err := fakeClient.Get(ctx, key, object); err != nil {
			t.Fatalf("get %T %s: %v", object, key, err)
		}
		assertControllerOwner(t, object, cluster)
	}
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		name := owned.PostgreSQLAgentServiceAccountName(cluster.Name, shard)
		for _, object := range []client.Object{
			&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}},
			&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}},
			&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}},
		} {
			key := client.ObjectKeyFromObject(object)
			if err := fakeClient.Get(ctx, key, object); err != nil {
				t.Fatalf("get cell %d API identity %T %s: %v", shard, object, key, err)
			}
			assertControllerOwner(t, object, cluster)
		}
	}
	assertControllerOwner(t, getPostgreSQLConfigMap(t, ctx, fakeClient, cluster), cluster)

	got := getCluster(t, ctx, fakeClient, cluster)
	if got.Status.CatalogAccess != nil {
		t.Fatalf("unsupported multi-member cluster received catalog access: %#v", got.Status.CatalogAccess)
	}
	if len(got.Status.PostgreSQLReplicationCredentials) != 0 {
		t.Fatalf("direct multi-member cluster received replication credentials: %#v", got.Status.PostgreSQLReplicationCredentials)
	}
	if got.Status.Phase != "Reconciling" || got.Status.ObservedGeneration != cluster.Generation {
		t.Fatalf("status = %#v", got.Status)
	}
	assertCondition(t, got, reconciledCondition, metav1.ConditionTrue, "ResourcesApplied")
	assertCondition(t, got, supportingAvailableCondition, metav1.ConditionFalse, "SupportingWorkloadsProgressing")
	assertCondition(t, got, postgresqlAvailableCondition, metav1.ConditionFalse, "PostgreSQLHAUnavailable")
	assertCondition(t, got, readyCondition, metav1.ConditionFalse, "PostgreSQLHAUnavailable")
	assertCondition(t, got, transportSecurityCondition, metav1.ConditionFalse, "TransportTLSUnavailable")
	postgresql := meta.FindStatusCondition(got.Status.Conditions, postgresqlAvailableCondition)
	ready := meta.FindStatusCondition(got.Status.Conditions, readyCondition)
	for name, condition := range map[string]*metav1.Condition{"PostgreSQLAvailable": postgresql, "Ready": ready} {
		if condition == nil || !strings.Contains(condition.Message, "direct runtime composes no multi-member PostgreSQL data plane") || strings.Contains(condition.Message, "sources and physical standbys are composed") {
			t.Fatalf("direct multi-member %s condition overclaims PostgreSQL composition: %#v", name, condition)
		}
	}

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

func TestReconcileCheckpointsExactPostgreSQLWritableLeaseIdentities(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	fakeClient := newFakeClient(t, cluster)
	reconciler := developmentReconciler(fakeClient, nil)

	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	got := getCluster(t, ctx, fakeClient, cluster)
	if len(got.Status.PostgreSQLWritableLeases) != int(cluster.Spec.Shards) {
		t.Fatalf("writable-term Lease checkpoints = %#v", got.Status.PostgreSQLWritableLeases)
	}
	initial := append([]pgshardv1alpha1.PostgreSQLWritableLeaseStatus(nil), got.Status.PostgreSQLWritableLeases...)
	for shard, checkpoint := range got.Status.PostgreSQLWritableLeases {
		if checkpoint.Shard != int32(shard) || checkpoint.LeaseName != owned.PostgreSQLWritableLeaseName(cluster.Name, int32(shard)) || checkpoint.LeaseUID == "" {
			t.Fatalf("writable-term Lease checkpoint %d = %#v", shard, checkpoint)
		}
		lease := &coordinationv1.Lease{}
		if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: checkpoint.LeaseName}, lease); err != nil {
			t.Fatal(err)
		}
		if lease.UID != checkpoint.LeaseUID {
			t.Fatalf("writable-term Lease %s UID = %s, want %s", lease.Name, lease.UID, checkpoint.LeaseUID)
		}
		if err := validatePostgreSQLWritableLeaseMetadata(lease, got, checkpoint.Shard); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(lease.Spec, coordinationv1.LeaseSpec{}) {
			t.Fatalf("operator populated runtime Lease fields: %#v", lease.Spec)
		}
	}

	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	if current := getCluster(t, ctx, fakeClient, cluster); !reflect.DeepEqual(current.Status.PostgreSQLWritableLeases, initial) {
		t.Fatalf("steady reconciliation changed writable-term Lease identities: before=%#v after=%#v", initial, current.Status.PostgreSQLWritableLeases)
	}
}

func TestPostgreSQLWritableLeaseRuntimeSpecAcceptsOnlyCompleteStates(t *testing.T) {
	t.Parallel()
	holder := "cluster-shard-0000-0/pod-uid/0123456789abcdef01234567"
	emptyHolder := ""
	whitespaceHolder := " " + holder
	duration := int32(15)
	zeroDuration := int32(0)
	oversizedDuration := int32(301)
	transitions := int32(3)
	zeroTransitions := int32(0)
	now := metav1.NewMicroTime(time.Unix(1_700_000_000, 0).UTC())
	zeroTime := metav1.MicroTime{}
	complete := func(holder *string) coordinationv1.LeaseSpec {
		return coordinationv1.LeaseSpec{
			HolderIdentity:       holder,
			LeaseDurationSeconds: &duration,
			AcquireTime:          &now,
			RenewTime:            &now,
			LeaseTransitions:     &transitions,
		}
	}
	preferredHolder := holder
	strategy := coordinationv1.OldestEmulationVersion

	for _, test := range []struct {
		name    string
		spec    coordinationv1.LeaseSpec
		wantErr string
	}{
		{name: "pristine envelope", spec: coordinationv1.LeaseSpec{}},
		{name: "occupied term", spec: complete(&holder)},
		{name: "released term", spec: complete(nil)},
		{name: "empty holder string", spec: complete(&emptyHolder), wantErr: "holder identity is invalid"},
		{name: "whitespace holder", spec: complete(&whitespaceHolder), wantErr: "holder identity is invalid"},
		{name: "released term without duration", spec: coordinationv1.LeaseSpec{AcquireTime: &now, RenewTime: &now, LeaseTransitions: &transitions}, wantErr: "partial or invalid released runtime state"},
		{name: "released term without acquire time", spec: coordinationv1.LeaseSpec{LeaseDurationSeconds: &duration, RenewTime: &now, LeaseTransitions: &transitions}, wantErr: "partial or invalid released runtime state"},
		{name: "released term without renew time", spec: coordinationv1.LeaseSpec{LeaseDurationSeconds: &duration, AcquireTime: &now, LeaseTransitions: &transitions}, wantErr: "partial or invalid released runtime state"},
		{name: "released term without transitions", spec: coordinationv1.LeaseSpec{LeaseDurationSeconds: &duration, AcquireTime: &now, RenewTime: &now}, wantErr: "partial or invalid released runtime state"},
		{name: "zero duration", spec: coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &zeroDuration, AcquireTime: &now, RenewTime: &now, LeaseTransitions: &transitions}, wantErr: "holder duration"},
		{name: "oversized duration", spec: coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &oversizedDuration, AcquireTime: &now, RenewTime: &now, LeaseTransitions: &transitions}, wantErr: "holder duration"},
		{name: "zero acquire time", spec: coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &duration, AcquireTime: &zeroTime, RenewTime: &now, LeaseTransitions: &transitions}, wantErr: "holder duration"},
		{name: "zero renew time", spec: coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &duration, AcquireTime: &now, RenewTime: &zeroTime, LeaseTransitions: &transitions}, wantErr: "holder duration"},
		{name: "zero transitions", spec: coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &duration, AcquireTime: &now, RenewTime: &now, LeaseTransitions: &zeroTransitions}, wantErr: "holder duration"},
		{name: "preferred holder", spec: coordinationv1.LeaseSpec{PreferredHolder: &preferredHolder}, wantErr: "coordinated leader-election fields"},
		{name: "strategy", spec: coordinationv1.LeaseSpec{Strategy: &strategy}, wantErr: "coordinated leader-election fields"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validatePostgreSQLWritableLeaseRuntimeSpec(test.spec)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("valid runtime state rejected: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("runtime validation error = %v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestReconcileAcceptsCompleteReleasedPostgreSQLWritableLease(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	fakeClient := newFakeClient(t, cluster)
	reconciler := developmentReconciler(fakeClient, nil)

	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	checkpointed := getCluster(t, ctx, fakeClient, cluster)
	checkpoint := checkpointed.Status.PostgreSQLWritableLeases[0]
	lease := &coordinationv1.Lease{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: checkpoint.LeaseName}
	if err := fakeClient.Get(ctx, key, lease); err != nil {
		t.Fatal(err)
	}
	duration := int32(15)
	transitions := int32(4)
	now := metav1.NewMicroTime(time.Unix(1_700_000_000, 0).UTC())
	lease.Spec = coordinationv1.LeaseSpec{
		LeaseDurationSeconds: &duration,
		AcquireTime:          &now,
		RenewTime:            &now,
		LeaseTransitions:     &transitions,
	}
	if err := fakeClient.Update(ctx, lease); err != nil {
		t.Fatal(err)
	}

	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatalf("complete released Lease blocked reconciliation: %v", err)
	}
	current := getCluster(t, ctx, fakeClient, cluster)
	if !reflect.DeepEqual(current.Status.PostgreSQLWritableLeases, checkpointed.Status.PostgreSQLWritableLeases) {
		t.Fatalf("released Lease changed identity checkpoints: before=%#v after=%#v", checkpointed.Status.PostgreSQLWritableLeases, current.Status.PostgreSQLWritableLeases)
	}
}

func TestRecordedPostgreSQLWritableLeaseLossFailsClosed(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		recreate bool
		want     string
	}{
		{name: "missing", want: "is missing; explicit recovery is required"},
		{name: "recreated", recreate: true, want: "expected recorded UID"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			cluster := validCluster()
			fakeClient := newFakeClient(t, cluster)
			reconciler := developmentReconciler(fakeClient, nil)
			if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
				t.Fatal(err)
			}
			before := getCluster(t, ctx, fakeClient, cluster)
			checkpoint := before.Status.PostgreSQLWritableLeases[0]
			lease := &coordinationv1.Lease{}
			key := types.NamespacedName{Namespace: cluster.Namespace, Name: checkpoint.LeaseName}
			if err := fakeClient.Get(ctx, key, lease); err != nil {
				t.Fatal(err)
			}
			if err := fakeClient.Delete(ctx, lease); err != nil {
				t.Fatal(err)
			}
			var replacementUID types.UID
			if test.recreate {
				replacement := owned.PostgreSQLWritableLease(before, checkpoint.Shard)
				if err := fakeClient.Create(ctx, replacement); err != nil {
					t.Fatal(err)
				}
				if replacement.UID == checkpoint.LeaseUID {
					t.Fatalf("replacement reused recorded UID %s", checkpoint.LeaseUID)
				}
				replacementUID = replacement.UID
			}

			if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("reconcile error = %v, want %q", err, test.want)
			}
			after := getCluster(t, ctx, fakeClient, cluster)
			if !reflect.DeepEqual(after.Status.PostgreSQLWritableLeases, before.Status.PostgreSQLWritableLeases) {
				t.Fatalf("failed reconciliation changed Lease checkpoints: before=%#v after=%#v", before.Status.PostgreSQLWritableLeases, after.Status.PostgreSQLWritableLeases)
			}
			if test.recreate {
				replacement := &coordinationv1.Lease{}
				if err := fakeClient.Get(ctx, key, replacement); err != nil {
					t.Fatal(err)
				}
				if replacement.UID != replacementUID {
					t.Fatalf("controller replaced colliding Lease UID %s with %s", replacementUID, replacement.UID)
				}
			}
		})
	}
}

func TestUncheckpointedPostgreSQLWritableLeaseMustBeEmpty(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	lease := owned.PostgreSQLWritableLease(cluster, 0)
	lease.UID = "preexisting-lease-uid"
	lease.ResourceVersion = "1"
	holder := "example-shard-0000-0/pod-uid"
	duration := int32(15)
	transitions := int32(1)
	now := metav1.NewMicroTime(time.Now())
	lease.Spec = coordinationv1.LeaseSpec{
		HolderIdentity:       &holder,
		LeaseDurationSeconds: &duration,
		AcquireTime:          &now,
		RenewTime:            &now,
		LeaseTransitions:     &transitions,
	}
	fakeClient := newFakeClient(t, cluster, lease)
	reconciler := developmentReconciler(fakeClient, nil)

	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err == nil || !strings.Contains(err.Error(), "is not an empty coordination envelope") {
		t.Fatalf("reconcile error = %v", err)
	}
	if got := getCluster(t, ctx, fakeClient, cluster); len(got.Status.PostgreSQLWritableLeases) != 0 {
		t.Fatalf("untrusted runtime Lease was checkpointed: %#v", got.Status.PostgreSQLWritableLeases)
	}
}

func TestReconcileCreatesSingleMemberPrimariesWithPerShardImmutableCredentials(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	fakeClient := newFakeClient(t, cluster)
	reconciler := developmentReconciler(fakeClient, nil)
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	got := getCluster(t, ctx, fakeClient, cluster)
	if got.Status.CatalogAccess == nil || !owned.CatalogAccessSecretNameIsValid(cluster.Name, got.Status.CatalogAccess.SecretName) || got.Status.CatalogAccess.SecretUID == "" || got.Status.CatalogAccess.ClientSHA256 == "" || got.Status.CatalogAccess.ServerSHA256 == "" {
		t.Fatalf("catalog access checkpoint = %#v", got.Status.CatalogAccess)
	}
	catalogAccess := &corev1.Secret{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: got.Status.CatalogAccess.SecretName}, catalogAccess); err != nil {
		t.Fatal(err)
	}
	if catalogAccess.UID != got.Status.CatalogAccess.SecretUID {
		t.Fatalf("catalog access Secret UID = %s, want %s", catalogAccess.UID, got.Status.CatalogAccess.SecretUID)
	}
	if err := validateCatalogAccessSecret(catalogAccess, cluster, got.Status.CatalogAccess.SecretName); err != nil {
		t.Fatalf("generated catalog access Secret is invalid: %v", err)
	}
	if observed := catalogAccessStatus(catalogAccess); observed.ClientSHA256 != got.Status.CatalogAccess.ClientSHA256 || observed.ServerSHA256 != got.Status.CatalogAccess.ServerSHA256 {
		t.Fatalf("catalog access material checkpoint = %#v, want %#v", got.Status.CatalogAccess, observed)
	}
	catalogPassword := string(catalogAccess.Data[owned.CatalogPasswordKey])

	passwords := make(map[int32]string, cluster.Spec.Shards)
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		bootstrap := bootstrapForShard(t, got, shard)
		secret := &corev1.Secret{}
		secretKey := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.SecretName}
		if err := fakeClient.Get(ctx, secretKey, secret); err != nil {
			t.Fatal(err)
		}
		if err := validatePostgreSQLAuthSecret(secret, cluster, bootstrap, bootstrap.SecretName); err != nil {
			t.Fatalf("generated credential for shard %d is invalid: %v", shard, err)
		}
		claim := &corev1.PersistentVolumeClaim{}
		if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}, claim); err != nil {
			t.Fatal(err)
		}
		if claim.UID != bootstrap.PVCUID {
			t.Fatalf("generated data PVC UID = %s, want %s", claim.UID, bootstrap.PVCUID)
		}
		if !bootstrap.PVCFenceDetached || !postgresqlCredentialIsDataAnchored(secret, bootstrap) {
			t.Fatalf("credential Secret was not anchored to the exact data PVC: secret=%#v bootstrap=%#v", secret.OwnerReferences, bootstrap)
		}
		if len(claim.OwnerReferences) != 0 || !postgresqlDataPVCIsProtected(claim) {
			t.Fatalf("data PVC was not independently protected before workload publication: %#v", claim.ObjectMeta)
		}
		if len(secret.Data[owned.PostgreSQLPasswordKey]) != hex.EncodedLen(postgresqlPasswordBytes) {
			t.Fatalf("generated password length for shard %d = %d", shard, len(secret.Data[owned.PostgreSQLPasswordKey]))
		}
		passwords[shard] = string(secret.Data[owned.PostgreSQLPasswordKey])
		statefulSet := &appsv1.StatefulSet{}
		name := owned.PostgreSQLShardStatefulSetName(cluster.Name, shard)
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
	got = getCluster(t, ctx, fakeClient, cluster)
	assertCondition(t, got, postgresqlAvailableCondition, metav1.ConditionFalse, "PostgreSQLPrimariesProgressing")
	unchangedCatalogAccess := &corev1.Secret{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: got.Status.CatalogAccess.SecretName}, unchangedCatalogAccess); err != nil {
		t.Fatal(err)
	}
	if unchangedCatalogAccess.UID != got.Status.CatalogAccess.SecretUID || string(unchangedCatalogAccess.Data[owned.CatalogPasswordKey]) != catalogPassword {
		t.Fatal("steady-state reconciliation replaced or rotated catalog access material")
	}
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		bootstrap := bootstrapForShard(t, got, shard)
		unchanged := &corev1.Secret{}
		secretKey := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.SecretName}
		if err := fakeClient.Get(ctx, secretKey, unchanged); err != nil {
			t.Fatal(err)
		}
		if string(unchanged.Data[owned.PostgreSQLPasswordKey]) != passwords[shard] {
			t.Fatalf("steady-state reconciliation rotated shard %d PostgreSQL credential", shard)
		}
		statefulSet := &appsv1.StatefulSet{}
		name := owned.PostgreSQLShardStatefulSetName(cluster.Name, shard)
		if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, statefulSet); err != nil {
			t.Fatal(err)
		}
		statefulSet.Status.AvailableReplicas = 1
		if err := fakeClient.Status().Update(ctx, statefulSet); err != nil {
			t.Fatal(err)
		}
		pod := &corev1.Pod{
			ObjectMeta: *statefulSet.Spec.Template.ObjectMeta.DeepCopy(),
			Spec:       *statefulSet.Spec.Template.Spec.DeepCopy(),
		}
		pod.Name = statefulSet.Name + "-0"
		pod.Namespace = cluster.Namespace
		pod.Spec.NodeName = "node-a"
		if err := fakeClient.Create(ctx, pod); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	got = getCluster(t, ctx, fakeClient, cluster)
	assertCondition(t, got, postgresqlAvailableCondition, metav1.ConditionFalse, "PostgreSQLPodFencingUnavailable")
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		pod := &corev1.Pod{}
		key := types.NamespacedName{Namespace: cluster.Namespace, Name: owned.PostgreSQLShardStatefulSetName(cluster.Name, shard) + "-0"}
		if err := fakeClient.Get(ctx, key, pod); err != nil {
			t.Fatal(err)
		}
		pod.Annotations[podfence.NodeUIDAnnotation] = "node-uid-a"
		pod.Annotations[podfence.NodeBootIDAnnotation] = "boot-a"
		if err := fakeClient.Update(ctx, pod); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	got = getCluster(t, ctx, fakeClient, cluster)
	assertCondition(t, got, postgresqlAvailableCondition, metav1.ConditionTrue, "SingleMemberPrimariesAvailable")
	assertCondition(t, got, readyCondition, metav1.ConditionFalse, "DataPlaneUnavailable")
	if len(got.Status.PostgreSQLBootstraps) != int(cluster.Spec.Shards) {
		t.Fatalf("recorded PostgreSQL bootstraps = %#v", got.Status.PostgreSQLBootstraps)
	}
}

func TestReconcileCheckpointsNonServingMultiMemberSourceStorage(t *testing.T) {
	t.Parallel()
	for _, members := range []int32{3, 5} {
		members := members
		t.Run(fmt.Sprintf("members=%d", members), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			cluster := validCluster()
			cluster.Spec.Shards = 1
			cluster.Spec.MembersPerShard = members
			base := newFakeClient(t, cluster)
			reconciler := developmentReconciler(base, nil)
			reconciler.Images.PostgreSQLRuntime = owned.PostgreSQLRuntimeAgentQuarantine

			if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
				t.Fatal(err)
			}
			markSupportingWorkloadsAvailable(t, ctx, base, cluster)
			result, err := reconciler.Reconcile(ctx, requestFor(cluster))
			if err != nil {
				t.Fatal(err)
			}
			if result.RequeueAfter != bootstrapIntegrityInterval {
				t.Fatalf("source-storage integrity requeue = %#v, want %s", result, bootstrapIntegrityInterval)
			}
			current := getCluster(t, ctx, base, cluster)
			if current.Status.PostgreSQLBootstrapSpec == nil || current.Status.PostgreSQLBootstrapSpec.MembersPerShard != members || current.Status.PostgreSQLBootstrapSpec.PostgreSQLRuntime != owned.PostgreSQLRuntimeAgentQuarantine.String() {
				t.Fatalf("multi-member source-storage snapshot = %#v", current.Status.PostgreSQLBootstrapSpec)
			}
			if current.Status.CatalogAccess != nil {
				t.Fatalf("multi-member source storage created catalog access: %#v", current.Status.CatalogAccess)
			}
			if len(current.Status.PostgreSQLBootstraps) != int(members) {
				t.Fatalf("multi-member source storage has %d records, want %d: %#v", len(current.Status.PostgreSQLBootstraps), members, current.Status.PostgreSQLBootstraps)
			}
			resourceNames := make(map[string]struct{}, 2*members)
			resourceUIDs := make(map[types.UID]struct{}, 2*members)
			for member := int32(0); member < members; member++ {
				bootstrap := bootstrapForMember(t, current, 0, member)
				secret := &corev1.Secret{}
				if err := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.SecretName}, secret); err != nil {
					t.Fatal(err)
				}
				claim := &corev1.PersistentVolumeClaim{}
				if err := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}, claim); err != nil {
					t.Fatal(err)
				}
				if secret.UID != bootstrap.SecretUID || claim.UID != bootstrap.PVCUID || !bootstrap.PVCFenceDetached || !postgresqlCredentialIsDataAnchored(secret, bootstrap) || len(claim.OwnerReferences) != 0 || !postgresqlDataPVCIsProtected(claim) {
					t.Fatalf("source-storage lifecycle is incomplete for member %d: secret=%#v claim=%#v bootstrap=%#v", member, secret.ObjectMeta, claim.ObjectMeta, bootstrap)
				}
				wantMember := fmt.Sprintf("%04d", member)
				if secret.Labels[owned.MemberLabel] != wantMember || claim.Labels[owned.MemberLabel] != wantMember {
					t.Fatalf("source-storage member labels = secret %q claim %q, want %q", secret.Labels[owned.MemberLabel], claim.Labels[owned.MemberLabel], wantMember)
				}
				if _, exists := secret.Labels[owned.RoleLabel]; exists {
					t.Fatalf("non-serving source credential carries a role label: %#v", secret.Labels)
				}
				if role, exists := claim.Labels[owned.RoleLabel]; exists {
					t.Fatalf("non-serving source storage carries authorizing role label %q", role)
				}
				for _, name := range []string{bootstrap.SecretName, bootstrap.PVCName} {
					if _, duplicate := resourceNames[name]; duplicate {
						t.Fatalf("source-storage name %q is shared by multiple members", name)
					}
					resourceNames[name] = struct{}{}
				}
				for _, uid := range []types.UID{bootstrap.SecretUID, bootstrap.PVCUID} {
					if _, duplicate := resourceUIDs[uid]; duplicate {
						t.Fatalf("source-storage UID %q is shared by multiple members", uid)
					}
					resourceUIDs[uid] = struct{}{}
				}
			}
			statefulSets := &appsv1.StatefulSetList{}
			pods := &corev1.PodList{}
			budgets := &policyv1.PodDisruptionBudgetList{}
			if err := base.List(ctx, statefulSets, client.InNamespace(cluster.Namespace)); err != nil {
				t.Fatal(err)
			}
			if err := base.List(ctx, pods, client.InNamespace(cluster.Namespace)); err != nil {
				t.Fatal(err)
			}
			if err := base.List(ctx, budgets, client.InNamespace(cluster.Namespace), client.MatchingLabels{owned.ComponentLabel: "postgresql"}); err != nil {
				t.Fatal(err)
			}
			if len(statefulSets.Items) != int(members) || len(pods.Items) != 0 || len(budgets.Items) != 0 {
				t.Fatalf("physical-member composition = StatefulSets=%d Pods=%d PostgreSQL PDBs=%d", len(statefulSets.Items), len(pods.Items), len(budgets.Items))
			}
			source := appsv1.StatefulSet{}
			if err := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: owned.PostgreSQLMemberStatefulSetName(cluster.Name, 0, 0)}, &source); err != nil {
				t.Fatal(err)
			}
			if source.Name != owned.PostgreSQLMemberStatefulSetName(cluster.Name, 0, 0) || source.Spec.Template.Labels[owned.MemberLabel] != "0000" {
				t.Fatalf("bootstrap-source identity = %#v", source.ObjectMeta)
			}
			if _, role := source.Spec.Template.Labels[owned.RoleLabel]; role {
				t.Fatalf("bootstrap source received a serving role: %#v", source.Spec.Template.Labels)
			}
			if observed, err := owned.ObservePostgreSQLRuntime(source.Spec.Template.Annotations, source.Spec.Template.Spec); err != nil || observed != owned.PostgreSQLRuntimeAgentQuarantine {
				t.Fatalf("bootstrap-source runtime = %q, %v", observed, err)
			}
			assertCondition(t, current, postgresqlAvailableCondition, metav1.ConditionFalse, "PostgreSQLHAUnavailable")
			assertCondition(t, current, readyCondition, metav1.ConditionFalse, "PostgreSQLHAUnavailable")
			postgresqlCondition := meta.FindStatusCondition(current.Status.Conditions, postgresqlAvailableCondition)
			ready := meta.FindStatusCondition(current.Status.Conditions, readyCondition)
			if postgresqlCondition == nil || !strings.Contains(postgresqlCondition.Message, "bootstrap sources and physical standbys are composed") ||
				ready == nil || !strings.Contains(ready.Message, "bootstrap sources and physical standbys are composed") {
				t.Fatalf("multi-member status does not describe composed non-serving members: PostgreSQL=%#v Ready=%#v", postgresqlCondition, ready)
			}

			direct := developmentReconciler(base, nil)
			if _, err := direct.Reconcile(ctx, requestFor(cluster)); err == nil || !strings.Contains(err.Error(), "durable multi-member PostgreSQL source storage requires runtime \"agent-quarantine\"") {
				t.Fatalf("direct manager accepted agent source storage: %v", err)
			}
			unchangedCluster := getCluster(t, ctx, base, cluster)
			for member := int32(0); member < members; member++ {
				before := bootstrapForMember(t, current, 0, member)
				after := bootstrapForMember(t, unchangedCluster, 0, member)
				if after.SecretUID != before.SecretUID || after.PVCUID != before.PVCUID {
					t.Fatalf("rejected runtime transition changed source storage for member %d: before=%#v after=%#v", member, before, after)
				}
			}
		})
	}
}

func TestDirectMultiMemberRuntimeDoesNotCreateSourceStorage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	base := newFakeClient(t, cluster)
	if _, err := developmentReconciler(base, nil).Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	current := getCluster(t, ctx, base, cluster)
	if current.Status.PostgreSQLBootstrapSpec != nil || len(current.Status.PostgreSQLBootstraps) != 0 {
		t.Fatalf("direct multi-member runtime checkpointed source storage: %#v", current.Status)
	}
	secrets := &corev1.SecretList{}
	claims := &corev1.PersistentVolumeClaimList{}
	statefulSets := &appsv1.StatefulSetList{}
	if err := base.List(ctx, secrets, client.InNamespace(cluster.Namespace), client.MatchingLabels{owned.ComponentLabel: "postgresql"}); err != nil {
		t.Fatal(err)
	}
	if err := base.List(ctx, claims, client.InNamespace(cluster.Namespace), client.MatchingLabels{owned.ComponentLabel: "postgresql"}); err != nil {
		t.Fatal(err)
	}
	if err := base.List(ctx, statefulSets, client.InNamespace(cluster.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(secrets.Items) != 0 || len(claims.Items) != 0 || len(statefulSets.Items) != 0 {
		t.Fatalf("direct multi-member runtime created source storage or workloads: Secrets=%d PVCs=%d StatefulSets=%d", len(secrets.Items), len(claims.Items), len(statefulSets.Items))
	}
}

func TestLegacyMemberZeroCredentialMetadataMigratesWithoutChangingIdentity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 3
	name := legacyPostgreSQLAuthSecretPrefix(cluster.Name, 0) + strings.Repeat("a", hex.EncodedLen(bootstrapNameRandomBytes))
	secret := owned.PostgreSQLMemberAuthSecret(cluster, 0, 0, name, []byte(strings.Repeat("b", hex.EncodedLen(postgresqlPasswordBytes))))
	secret.UID = "legacy-member-zero-secret"
	secret.ResourceVersion = "1"
	delete(secret.Labels, owned.MemberLabel)
	bootstrap := pgshardv1alpha1.PostgreSQLBootstrapStatus{Shard: 0, Member: 0, SecretName: name, SecretUID: secret.UID}
	base := newFakeClient(t, cluster, secret)
	reconciler := developmentReconciler(base, nil)

	observed := &corev1.Secret{}
	if err := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, observed); err != nil {
		t.Fatal(err)
	}
	originalUID := observed.UID
	if err := reconciler.migrateLegacyPostgreSQLAuthSecretMetadata(ctx, cluster, bootstrap, observed); err != nil {
		t.Fatal(err)
	}
	migrated := &corev1.Secret{}
	if err := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, migrated); err != nil {
		t.Fatal(err)
	}
	if migrated.UID != originalUID || migrated.Labels[owned.MemberLabel] != "0000" {
		t.Fatalf("legacy credential migration changed identity or missed member label: %#v", migrated.ObjectMeta)
	}
	if err := validatePostgreSQLAuthSecret(migrated, cluster, bootstrap, name); err != nil {
		t.Fatalf("migrated credential is invalid: %v", err)
	}
}

func TestReconcileUpgradesMemberlessSourceStorageStatusFromPreviousRelease(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 3
	cluster.Status.PostgreSQLBootstrapSpec = bootstrapSpecStatus(cluster, owned.PostgreSQLRuntimeAgentQuarantine)
	legacySecretName := legacyPostgreSQLAuthSecretPrefix(cluster.Name, 0) + strings.Repeat("a", hex.EncodedLen(bootstrapNameRandomBytes))
	legacyPVCName := fmt.Sprintf("%s-shard-0000-data-%s", cluster.Name, strings.Repeat("b", hex.EncodedLen(bootstrapNameRandomBytes)))
	legacySecretUID := types.UID("legacy-source-secret")
	legacyPVCUID := types.UID("legacy-source-pvc")
	cluster.Status.PostgreSQLBootstraps = []pgshardv1alpha1.PostgreSQLBootstrapStatus{{
		Shard: 0, Member: 0, SecretName: legacySecretName, SecretUID: legacySecretUID,
		PVCFenceDetached: true, PVCName: legacyPVCName, PVCUID: legacyPVCUID,
		PVCStorageClassName: cluster.Spec.Storage.StorageClassName,
	}}

	encoded, err := json.Marshal(cluster)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(encoded, &document); err != nil {
		t.Fatal(err)
	}
	status := document["status"].(map[string]any)
	bootstraps := status["postgresqlBootstraps"].([]any)
	delete(bootstraps[0].(map[string]any), "member")
	legacyEncoded, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	legacyCluster := &pgshardv1alpha1.PgShardCluster{}
	if err := json.Unmarshal(legacyEncoded, legacyCluster); err != nil {
		t.Fatal(err)
	}
	if legacyCluster.Status.PostgreSQLBootstraps[0].Member != 0 {
		t.Fatalf("memberless status did not decode as stable member zero: %#v", legacyCluster.Status.PostgreSQLBootstraps)
	}

	secret := owned.PostgreSQLMemberAuthSecret(legacyCluster, 0, 0, legacySecretName, []byte(strings.Repeat("c", hex.EncodedLen(postgresqlPasswordBytes))))
	secret.UID = legacySecretUID
	secret.ResourceVersion = "1"
	delete(secret.Labels, owned.MemberLabel)
	claim := owned.PostgreSQLMemberDataPVC(legacyCluster, 0, 0, legacyPVCName, legacyCluster.Spec.Storage.Size, legacyCluster.Spec.Storage.StorageClassName, legacySecretName, legacySecretUID)
	claim.UID = legacyPVCUID
	claim.ResourceVersion = "1"
	claim.OwnerReferences = nil
	claim.Finalizers = []string{owned.PostgreSQLDataProtectionFinalizer}
	controller := true
	blockDeletion := true
	secret.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: corev1.SchemeGroupVersion.String(), Kind: "PersistentVolumeClaim",
		Name: legacyPVCName, UID: legacyPVCUID, Controller: &controller, BlockOwnerDeletion: &blockDeletion,
	}}

	base := newFakeClient(t, legacyCluster, secret, claim)
	reconciler := developmentReconciler(base, nil)
	reconciler.Images.PostgreSQLRuntime = owned.PostgreSQLRuntimeAgentQuarantine
	if _, err := reconciler.Reconcile(ctx, requestFor(legacyCluster)); err != nil {
		t.Fatal(err)
	}
	upgraded := getCluster(t, ctx, base, legacyCluster)
	if len(upgraded.Status.PostgreSQLBootstraps) != 3 {
		t.Fatalf("upgraded source-storage records = %#v", upgraded.Status.PostgreSQLBootstraps)
	}
	memberZero := bootstrapForMember(t, upgraded, 0, 0)
	if memberZero.SecretName != legacySecretName || memberZero.SecretUID != legacySecretUID || memberZero.PVCName != legacyPVCName || memberZero.PVCUID != legacyPVCUID {
		t.Fatalf("member-zero API identity changed during status upgrade: %#v", memberZero)
	}
	migratedSecret := &corev1.Secret{}
	if err := base.Get(ctx, types.NamespacedName{Namespace: legacyCluster.Namespace, Name: legacySecretName}, migratedSecret); err != nil {
		t.Fatal(err)
	}
	if migratedSecret.Labels[owned.MemberLabel] != "0000" {
		t.Fatalf("member-zero credential metadata was not migrated: %#v", migratedSecret.Labels)
	}
}

func TestPostgreSQLFinalizationPodUsesImmutableMemberIdentity(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	bootstrap := pgshardv1alpha1.PostgreSQLBootstrapStatus{Shard: 2, Member: 1}
	statefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: owned.PostgreSQLMemberStatefulSetName(cluster.Name, bootstrap.Shard, bootstrap.Member),
		UID:  "statefulset-uid",
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: statefulSet.Name + "-0",
		UID:  "pod-uid",
		Annotations: map[string]string{
			owned.PostgreSQLPodClusterUIDAnnotation: string(cluster.UID),
		},
		Labels: map[string]string{
			owned.ClusterLabel: cluster.Name, owned.ComponentLabel: "postgresql",
			owned.ShardLabel: "0002", owned.MemberLabel: "0001", owned.RoleLabel: "replica",
		},
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(statefulSet, appsv1.SchemeGroupVersion.WithKind("StatefulSet"))},
	}}
	if err := validatePostgreSQLFinalizationPod(pod, cluster, bootstrap); err != nil {
		t.Fatalf("role-neutral member identity was rejected: %v", err)
	}
	pod.Labels[owned.RoleLabel] = "primary"
	if err := validatePostgreSQLFinalizationPod(pod, cluster, bootstrap); err != nil {
		t.Fatalf("mutable role changed the protected member identity: %v", err)
	}
	pod.Labels[owned.MemberLabel] = "0000"
	if err := validatePostgreSQLFinalizationPod(pod, cluster, bootstrap); err == nil {
		t.Fatal("another member identity was accepted for protected data")
	}
	pod.Labels[owned.MemberLabel] = "0001"
	pod.OwnerReferences = nil
	if err := validatePostgreSQLFinalizationPod(pod, cluster, bootstrap); err == nil {
		t.Fatal("member Pod without its exact StatefulSet controller was accepted")
	}

	legacyStatefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: owned.LegacyPostgreSQLPrimaryStatefulSetName(cluster.Name, 0),
		UID:  "legacy-statefulset-uid",
	}}
	legacyBootstrap := pgshardv1alpha1.PostgreSQLBootstrapStatus{Shard: 0, Member: 0}
	legacyPod := pod.DeepCopy()
	legacyPod.Name = legacyStatefulSet.Name + "-0"
	legacyPod.Labels[owned.ShardLabel] = "0000"
	legacyPod.Labels[owned.MemberLabel] = "0000"
	legacyPod.Labels[owned.RoleLabel] = "replica"
	legacyPod.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(legacyStatefulSet, appsv1.SchemeGroupVersion.WithKind("StatefulSet"))}
	if err := validatePostgreSQLFinalizationPod(legacyPod, cluster, legacyBootstrap); err == nil {
		t.Fatal("legacy primary-named Pod accepted a replica role")
	}
	legacyPod.Labels[owned.RoleLabel] = "primary"
	if err := validatePostgreSQLFinalizationPod(legacyPod, cluster, legacyBootstrap); err != nil {
		t.Fatalf("legacy member-zero primary was rejected: %v", err)
	}
}

func TestMultiMemberSourceStorageRejectsBootstrapImageBeforeDurableWrites(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	base := newFakeClient(t, cluster)
	images := owned.DefaultImages()
	images.PostgreSQLRuntime = owned.PostgreSQLRuntimeAgentQuarantine
	images.PostgreSQLBootstrap = "ghcr.io/andrew01234567890/pgshard-postgres-agent:main"
	reconciler := &PgShardClusterReconciler{Client: base, Images: images}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err == nil || !strings.Contains(err.Error(), "immutable sha256 digest") {
		t.Fatalf("mutable source-storage bootstrap image error = %v", err)
	}
	current := getCluster(t, ctx, base, cluster)
	if controllerutil.ContainsFinalizer(current, resourceFinalizer) || current.Status.PostgreSQLBootstrapSpec != nil || len(current.Status.PostgreSQLBootstraps) != 0 || len(current.Status.PostgreSQLWritableLeases) != 0 {
		t.Fatalf("invalid source-storage image crossed a durable barrier: %#v", current)
	}
	secrets := &corev1.SecretList{}
	claims := &corev1.PersistentVolumeClaimList{}
	if err := base.List(ctx, secrets, client.InNamespace(cluster.Namespace), client.MatchingLabels{owned.ComponentLabel: "postgresql"}); err != nil {
		t.Fatal(err)
	}
	if err := base.List(ctx, claims, client.InNamespace(cluster.Namespace), client.MatchingLabels{owned.ComponentLabel: "postgresql"}); err != nil {
		t.Fatal(err)
	}
	if len(secrets.Items) != 0 || len(claims.Items) != 0 {
		t.Fatalf("invalid source-storage image created Secrets=%d PVCs=%d", len(secrets.Items), len(claims.Items))
	}
}

func TestMultiMemberSourceStorageIdentityLossFailsClosed(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name         string
		kind         string
		replace      bool
		mutateSource bool
		orphanPod    bool
		want         string
	}{
		{name: "missing bootstrap credential", kind: "secret", want: "is missing; explicit recovery is required"},
		{name: "replaced bootstrap credential", kind: "secret", replace: true, want: "expected recorded UID"},
		{name: "missing data", kind: "pvc", want: "is missing; restore is required"},
		{name: "replaced data", kind: "pvc", replace: true, want: "expected recorded UID"},
		{name: "missing writable Lease", kind: "lease", want: "is missing; explicit recovery is required"},
		{name: "missing replication credential fences mutated source", kind: "replication-secret", mutateSource: true, want: "is missing; explicit recovery is required"},
		{name: "missing replication credential fences orphaned source Pod", kind: "replication-secret", orphanPod: true, want: "is missing; explicit recovery is required"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			cluster := validCluster()
			cluster.Spec.Shards = 1
			base := newFakeClient(t, cluster)
			reconciler := developmentReconciler(base, nil)
			reconciler.Images.PostgreSQLRuntime = owned.PostgreSQLRuntimeAgentQuarantine
			if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
				t.Fatal(err)
			}
			current := getCluster(t, ctx, base, cluster)
			bootstrap := bootstrapForShard(t, current, 0)
			if test.mutateSource {
				source := &appsv1.StatefulSet{}
				key := types.NamespacedName{Namespace: cluster.Namespace, Name: owned.PostgreSQLMemberStatefulSetName(cluster.Name, 0, 0)}
				if err := base.Get(ctx, key, source); err != nil {
					t.Fatal(err)
				}
				source.Spec.Template.Labels[owned.RoleLabel] = "primary"
				if err := base.Update(ctx, source); err != nil {
					t.Fatal(err)
				}
			}
			var orphanPodKey types.NamespacedName
			if test.orphanPod {
				source := &appsv1.StatefulSet{}
				sourceKey := types.NamespacedName{Namespace: cluster.Namespace, Name: owned.PostgreSQLMemberStatefulSetName(cluster.Name, 0, 0)}
				if err := base.Get(ctx, sourceKey, source); err != nil {
					t.Fatal(err)
				}
				orphan := &corev1.Pod{
					ObjectMeta: *source.Spec.Template.ObjectMeta.DeepCopy(),
					Spec:       *source.Spec.Template.Spec.DeepCopy(),
				}
				orphan.Name = source.Name + "-0"
				orphan.Namespace = cluster.Namespace
				if err := base.Create(ctx, orphan); err != nil {
					t.Fatal(err)
				}
				orphanPodKey = client.ObjectKeyFromObject(orphan)
				if err := base.Delete(ctx, source); err != nil {
					t.Fatal(err)
				}
			}

			switch test.kind {
			case "secret":
				key := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.SecretName}
				secret := &corev1.Secret{}
				if err := base.Get(ctx, key, secret); err != nil {
					t.Fatal(err)
				}
				if err := base.Delete(ctx, secret); err != nil {
					t.Fatal(err)
				}
				if test.replace {
					replacement := owned.PostgreSQLAuthSecret(cluster, 0, bootstrap.SecretName, []byte(strings.Repeat("a", hex.EncodedLen(postgresqlPasswordBytes))))
					replacement.UID = "replacement-secret-uid"
					if err := base.Create(ctx, replacement); err != nil {
						t.Fatal(err)
					}
				}
			case "pvc":
				key := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}
				claim := &corev1.PersistentVolumeClaim{}
				if err := base.Get(ctx, key, claim); err != nil {
					t.Fatal(err)
				}
				controllerutil.RemoveFinalizer(claim, owned.PostgreSQLDataProtectionFinalizer)
				if err := base.Update(ctx, claim); err != nil {
					t.Fatal(err)
				}
				if err := base.Delete(ctx, claim); err != nil {
					t.Fatal(err)
				}
				if test.replace {
					replacement := owned.PostgreSQLDataPVC(cluster, 0, bootstrap.PVCName, cluster.Spec.Storage.Size, bootstrap.PVCStorageClassName, bootstrap.SecretName, bootstrap.SecretUID)
					replacement.UID = "replacement-pvc-uid"
					if err := base.Create(ctx, replacement); err != nil {
						t.Fatal(err)
					}
				}
			case "lease":
				checkpoint := current.Status.PostgreSQLWritableLeases[0]
				lease := &coordinationv1.Lease{}
				key := types.NamespacedName{Namespace: cluster.Namespace, Name: checkpoint.LeaseName}
				if err := base.Get(ctx, key, lease); err != nil {
					t.Fatal(err)
				}
				if err := base.Delete(ctx, lease); err != nil {
					t.Fatal(err)
				}
			case "replication-secret":
				checkpoint := current.Status.PostgreSQLReplicationCredentials[0]
				secret := &corev1.Secret{}
				key := types.NamespacedName{Namespace: cluster.Namespace, Name: checkpoint.SecretName}
				if err := base.Get(ctx, key, secret); err != nil {
					t.Fatal(err)
				}
				if err := base.Delete(ctx, secret); err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatalf("unknown test kind %q", test.kind)
			}

			if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("reconcile error = %v, want %q", err, test.want)
			}
			after := bootstrapForShard(t, getCluster(t, ctx, base, cluster), 0)
			if after.SecretUID != bootstrap.SecretUID || after.PVCUID != bootstrap.PVCUID {
				t.Fatalf("identity failure changed source-storage checkpoint: before=%#v after=%#v", bootstrap, after)
			}
			statefulSets := &appsv1.StatefulSetList{}
			if err := base.List(ctx, statefulSets, client.InNamespace(cluster.Namespace)); err != nil {
				t.Fatal(err)
			}
			for index := range statefulSets.Items {
				if statefulSets.Items[index].DeletionTimestamp == nil {
					t.Fatalf("identity failure did not fence every PostgreSQL member: %#v", statefulSets.Items)
				}
			}
			if test.orphanPod {
				orphan := &corev1.Pod{}
				if err := base.Get(ctx, orphanPodKey, orphan); err == nil && orphan.DeletionTimestamp == nil {
					t.Fatalf("identity failure left orphaned bootstrap source Pod running: %#v", orphan)
				} else if err != nil && !apierrors.IsNotFound(err) {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestMultiMemberFencingAttemptsEveryShardAndMemberAfterCollision(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 2
	base := newFakeClient(t, cluster)
	reconciler := developmentReconciler(base, nil)
	reconciler.Images.PostgreSQLRuntime = owned.PostgreSQLRuntimeAgentQuarantine
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	current := getCluster(t, ctx, base, cluster)
	for shard := int32(0); shard < current.Spec.Shards; shard++ {
		for member := int32(0); member < current.Spec.MembersPerShard; member++ {
			statefulSet := &appsv1.StatefulSet{}
			key := types.NamespacedName{Namespace: current.Namespace, Name: owned.PostgreSQLMemberStatefulSetName(current.Name, shard, member)}
			if err := base.Get(ctx, key, statefulSet); err != nil {
				t.Fatal(err)
			}
			pod := &corev1.Pod{ObjectMeta: *statefulSet.Spec.Template.ObjectMeta.DeepCopy(), Spec: *statefulSet.Spec.Template.Spec.DeepCopy()}
			pod.Name = statefulSet.Name + "-0"
			pod.Namespace = current.Namespace
			if err := base.Create(ctx, pod); err != nil {
				t.Fatal(err)
			}
			if shard == 0 && member == 0 {
				statefulSet.OwnerReferences = nil
				if err := base.Update(ctx, statefulSet); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	err := reconciler.fenceMultiMemberPostgreSQLMembers(ctx, current)
	if err == nil || !strings.Contains(err.Error(), "not controlled by PgShardCluster UID") {
		t.Fatalf("member fencing collision error = %v", err)
	}
	for shard := int32(0); shard < current.Spec.Shards; shard++ {
		for member := int32(0); member < current.Spec.MembersPerShard; member++ {
			pod := &corev1.Pod{}
			key := types.NamespacedName{Namespace: current.Namespace, Name: owned.PostgreSQLMemberStatefulSetName(current.Name, shard, member) + "-0"}
			if err := base.Get(ctx, key, pod); err == nil && pod.DeletionTimestamp == nil {
				t.Fatalf("shard %d member %d Pod was not fenced after another controller collided: %#v", shard, member, pod)
			} else if err != nil && !apierrors.IsNotFound(err) {
				t.Fatal(err)
			}
			statefulSet := &appsv1.StatefulSet{}
			if err := base.Get(ctx, types.NamespacedName{Namespace: current.Namespace, Name: owned.PostgreSQLMemberStatefulSetName(current.Name, shard, member)}, statefulSet); shard == 0 && member == 0 {
				if err != nil {
					t.Fatalf("colliding controller disappeared unexpectedly: %v", err)
				}
			} else if err == nil && statefulSet.DeletionTimestamp == nil {
				t.Fatalf("shard %d member %d controller was not fenced: %#v", shard, member, statefulSet)
			} else if err != nil && !apierrors.IsNotFound(err) {
				t.Fatal(err)
			}
		}
	}
}

func TestMultiMemberSourceStorageFinalizationHonorsDeletionPolicy(t *testing.T) {
	t.Parallel()
	for _, policy := range []pgshardv1alpha1.StorageDeletionPolicy{pgshardv1alpha1.DeletionRetain, pgshardv1alpha1.DeletionDelete} {
		policy := policy
		t.Run(string(policy), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			cluster := validCluster()
			cluster.Spec.Shards = 1
			cluster.Spec.Storage.DeletionPolicy = policy
			base := newFakeClient(t, cluster)
			reconciler := developmentReconciler(base, base)
			reconciler.Images.PostgreSQLRuntime = owned.PostgreSQLRuntimeAgentQuarantine
			if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
				t.Fatal(err)
			}
			current := getCluster(t, ctx, base, cluster)
			bootstrap := bootstrapForShard(t, current, 0)
			if err := base.Delete(ctx, current); err != nil {
				t.Fatal(err)
			}
			for range 16 {
				if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); client.IgnoreNotFound(err) != nil {
					t.Fatal(err)
				}
				if err := base.Get(ctx, requestFor(cluster).NamespacedName, &pgshardv1alpha1.PgShardCluster{}); apierrors.IsNotFound(err) {
					break
				}
			}
			if err := base.Get(ctx, requestFor(cluster).NamespacedName, &pgshardv1alpha1.PgShardCluster{}); !apierrors.IsNotFound(err) {
				t.Fatalf("cluster finalization did not complete: %v", err)
			}
			if err := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.SecretName}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
				t.Fatalf("source-storage credential survived finalization: %v", err)
			}
			claim := &corev1.PersistentVolumeClaim{}
			err := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}, claim)
			if policy == pgshardv1alpha1.DeletionDelete {
				if !apierrors.IsNotFound(err) {
					t.Fatalf("Delete policy retained source storage: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if claim.Annotations[owned.RetainedFromAnnotation] != cluster.Namespace+"/"+cluster.Name || postgresqlDataPVCIsProtected(claim) || len(claim.OwnerReferences) != 0 {
				t.Fatalf("Retain policy did not release source storage safely: %#v", claim.ObjectMeta)
			}
			if _, exists := claim.Labels[owned.RoleLabel]; exists {
				t.Fatalf("retained source storage acquired a role label: %#v", claim.Labels)
			}
		})
	}
}

func TestPostgreSQLRuntimeChangeIsRejectedBeforeOnDeleteMutation(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name          string
		staleTemplate bool
		wantObject    string
	}{
		{name: "existing direct StatefulSet", wantObject: "StatefulSet"},
		{name: "agent template over live direct Pod", staleTemplate: true, wantObject: "Pod"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			cluster := validCluster()
			cluster.Spec.Shards = 1
			cluster.Spec.MembersPerShard = 1
			cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
			base := newFakeClient(t, cluster)
			reconciler := developmentReconciler(base, base)
			if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
				t.Fatal(err)
			}

			statefulSetKey := types.NamespacedName{Namespace: cluster.Namespace, Name: owned.PostgreSQLShardStatefulSetName(cluster.Name, 0)}
			statefulSet := &appsv1.StatefulSet{}
			if err := base.Get(ctx, statefulSetKey, statefulSet); err != nil {
				t.Fatal(err)
			}
			originalTemplate := statefulSet.Spec.Template.DeepCopy()
			pod := &corev1.Pod{ObjectMeta: *originalTemplate.ObjectMeta.DeepCopy(), Spec: *originalTemplate.Spec.DeepCopy()}
			pod.Name = statefulSet.Name + "-0"
			pod.Namespace = cluster.Namespace
			pod.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(statefulSet, appsv1.SchemeGroupVersion.WithKind("StatefulSet"))}
			if err := base.Create(ctx, pod); err != nil {
				t.Fatal(err)
			}

			agentImages := owned.DevelopmentImages()
			agentImages.PostgreSQLRuntime = owned.PostgreSQLRuntimeAgentQuarantine
			// Corrupt only the durable checkpoint to the requested runtime so this
			// test continues exercising the live workload defense independently.
			currentCluster := getCluster(t, ctx, base, cluster)
			currentCluster.Status.PostgreSQLBootstrapSpec.PostgreSQLRuntime = owned.PostgreSQLRuntimeAgentQuarantine.String()
			if err := base.Status().Update(ctx, currentCluster); err != nil {
				t.Fatal(err)
			}
			if test.staleTemplate {
				plan, err := owned.Plan(currentCluster, agentImages)
				if err != nil {
					t.Fatal(err)
				}
				for _, planned := range plan {
					desired, ok := planned.(*appsv1.StatefulSet)
					if !ok || desired.Name != statefulSet.Name {
						continue
					}
					statefulSet.Spec.Template = *desired.Spec.Template.DeepCopy()
				}
				if err := base.Update(ctx, statefulSet); err != nil {
					t.Fatal(err)
				}
			}

			reconciler.Images = agentImages
			_, err := reconciler.Reconcile(ctx, requestFor(cluster))
			if err == nil || !strings.Contains(err.Error(), test.wantObject+" "+statefulSet.Name) || !strings.Contains(err.Error(), "runtime selection is fixed at workload creation") {
				t.Fatalf("runtime transition error = %v", err)
			}
			currentPod := &corev1.Pod{}
			if err := base.Get(ctx, client.ObjectKeyFromObject(pod), currentPod); err != nil {
				t.Fatal(err)
			}
			if observed, err := owned.ObservePostgreSQLRuntime(currentPod.Annotations, currentPod.Spec); err != nil || observed != owned.PostgreSQLRuntimeDirect {
				t.Fatalf("live direct Pod changed after rejected runtime transition: %q, %v", observed, err)
			}
			currentStatefulSet := &appsv1.StatefulSet{}
			if err := base.Get(ctx, statefulSetKey, currentStatefulSet); err != nil {
				t.Fatal(err)
			}
			if !test.staleTemplate && !reflect.DeepEqual(currentStatefulSet.Spec.Template, *originalTemplate) {
				t.Fatal("rejected runtime transition mutated the OnDelete StatefulSet template")
			}
		})
	}
}

func TestMultiMemberRuntimeContractInspectsEveryStandby(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	base := newFakeClient(t, cluster)
	reconciler := developmentReconciler(base, base)
	reconciler.Images.PostgreSQLRuntime = owned.PostgreSQLRuntimeAgentQuarantine
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}

	name := owned.PostgreSQLMemberStatefulSetName(cluster.Name, 0, cluster.Spec.MembersPerShard-1)
	standby := &appsv1.StatefulSet{}
	if err := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, standby); err != nil {
		t.Fatal(err)
	}
	standby.Spec.Template.Annotations[owned.PostgreSQLRuntimeAnnotation] = owned.PostgreSQLRuntimeDirect.String()
	if err := base.Update(ctx, standby); err != nil {
		t.Fatal(err)
	}

	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err == nil ||
		!strings.Contains(err.Error(), "observe StatefulSet "+name+" runtime") {
		t.Fatalf("mutated standby runtime was not rejected before planning: %v", err)
	}
	observed := &appsv1.StatefulSet{}
	if err := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, observed); err != nil {
		t.Fatal(err)
	}
	if observed.Spec.Template.Annotations[owned.PostgreSQLRuntimeAnnotation] != owned.PostgreSQLRuntimeDirect.String() {
		t.Fatalf("runtime rejection rewrote the OnDelete standby template: %#v", observed.Spec.Template.Annotations)
	}
}

func TestPostgreSQLRuntimeChangeIsRejectedAfterWorkloadDeletion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	base := newFakeClient(t, cluster)
	reconciler := developmentReconciler(base, base)
	agentImages := owned.DevelopmentImages()
	agentImages.PostgreSQLRuntime = owned.PostgreSQLRuntimeAgentQuarantine
	reconciler.Images = agentImages
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}

	currentCluster := getCluster(t, ctx, base, cluster)
	if got := currentCluster.Status.PostgreSQLBootstrapSpec.PostgreSQLRuntime; got != owned.PostgreSQLRuntimeAgentQuarantine.String() {
		t.Fatalf("durable PostgreSQL runtime = %q, want agent-quarantine", got)
	}
	statefulSet := &appsv1.StatefulSet{}
	statefulSetKey := types.NamespacedName{Namespace: cluster.Namespace, Name: owned.PostgreSQLShardStatefulSetName(cluster.Name, 0)}
	if err := base.Get(ctx, statefulSetKey, statefulSet); err != nil {
		t.Fatal(err)
	}
	if err := base.Delete(ctx, statefulSet); err != nil {
		t.Fatal(err)
	}

	reconciler.Images = owned.DevelopmentImages()
	_, err := reconciler.Reconcile(ctx, requestFor(cluster))
	if err == nil || !strings.Contains(err.Error(), "durable PostgreSQL runtime is \"agent-quarantine\"") || !strings.Contains(err.Error(), "manager requested \"direct\"") {
		t.Fatalf("runtime transition error after workload deletion = %v", err)
	}
	if err := base.Get(ctx, statefulSetKey, &appsv1.StatefulSet{}); !apierrors.IsNotFound(err) {
		t.Fatalf("rejected runtime transition recreated StatefulSet: %v", err)
	}
}

func TestLegacyRuntimeCheckpointMigratesToDirectBeforeFlagValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	cluster.Status.PostgreSQLBootstrapSpec = bootstrapSpecStatus(cluster, owned.PostgreSQLRuntimeDirect)
	cluster.Status.PostgreSQLBootstrapSpec.PostgreSQLRuntime = ""
	base := newFakeClient(t, cluster)
	reconciler := developmentReconciler(base, base)
	agentImages := owned.DevelopmentImages()
	agentImages.PostgreSQLRuntime = owned.PostgreSQLRuntimeAgentQuarantine
	reconciler.Images = agentImages

	_, err := reconciler.Reconcile(ctx, requestFor(cluster))
	if err == nil || !strings.Contains(err.Error(), "durable PostgreSQL runtime is \"direct\"") || !strings.Contains(err.Error(), "manager requested \"agent-quarantine\"") {
		t.Fatalf("legacy runtime transition error = %v", err)
	}
	if got := getCluster(t, ctx, base, cluster).Status.PostgreSQLBootstrapSpec.PostgreSQLRuntime; got != owned.PostgreSQLRuntimeDirect.String() {
		t.Fatalf("migrated legacy PostgreSQL runtime = %q, want direct", got)
	}
}

func TestRoleNeutralPostgreSQLIdentityMigrationNeverPublishesTwoControllers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	base := newFakeClient(t, cluster)
	reconciler := developmentReconciler(base, base)
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}

	currentCluster := getCluster(t, ctx, base, cluster)
	bootstrap := bootstrapForShard(t, currentCluster, 0)
	currentName := owned.PostgreSQLShardStatefulSetName(cluster.Name, 0)
	legacyName := owned.LegacyPostgreSQLPrimaryStatefulSetName(cluster.Name, 0)
	current := &appsv1.StatefulSet{}
	if err := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: currentName}, current); err != nil {
		t.Fatal(err)
	}
	if err := base.Delete(ctx, current); err != nil {
		t.Fatal(err)
	}

	legacy := current.DeepCopy()
	legacy.Name = legacyName
	legacy.UID = "legacy-statefulset-uid"
	legacy.ResourceVersion = ""
	legacy.CreationTimestamp = metav1.Time{}
	legacy.ManagedFields = nil
	legacy.Status = appsv1.StatefulSetStatus{}
	if err := base.Create(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	legacyPod := &corev1.Pod{
		ObjectMeta: *legacy.Spec.Template.ObjectMeta.DeepCopy(),
		Spec:       *legacy.Spec.Template.Spec.DeepCopy(),
	}
	legacyPod.Name = legacyName + "-0"
	legacyPod.Namespace = cluster.Namespace
	legacyPod.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(legacy, appsv1.SchemeGroupVersion.WithKind("StatefulSet"))}
	if !podSpecReferencesPostgreSQLDataPVC(legacyPod.Spec, bootstrap.PVCName) {
		t.Fatalf("legacy upgrade fixture does not mount checkpointed PVC %s", bootstrap.PVCName)
	}
	if err := base.Create(ctx, legacyPod); err != nil {
		t.Fatal(err)
	}

	assertNoDualControllers := func() bool {
		t.Helper()
		legacyFound := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: legacyName}, &appsv1.StatefulSet{}) == nil
		currentFound := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: currentName}, &appsv1.StatefulSet{}) == nil
		if legacyFound && currentFound {
			t.Fatal("legacy and role-neutral PostgreSQL StatefulSets simultaneously reference one PGDATA")
		}
		return currentFound
	}

	roleNeutralCreated := false
	for attempt := 0; attempt < 6; attempt++ {
		result, err := reconciler.Reconcile(ctx, requestFor(cluster))
		if err != nil {
			t.Fatalf("migration reconcile %d: %v", attempt, err)
		}
		roleNeutralCreated = assertNoDualControllers()
		if roleNeutralCreated {
			break
		}
		if !result.Requeue && result.RequeueAfter == 0 {
			t.Fatalf("migration reconcile %d stopped before role-neutral workload creation", attempt)
		}
	}
	if !roleNeutralCreated {
		t.Fatal("role-neutral PostgreSQL StatefulSet was not created after the legacy Pod absence barrier")
	}
	if err := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: legacyPod.Name}, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("legacy PostgreSQL Pod remains after migration: %v", err)
	}
}

func TestReplicationCredentialsAreStagedCheckpointedAndFailClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Status.PostgreSQLBootstrapSpec = bootstrapSpecStatus(cluster, owned.PostgreSQLRuntimeAgentQuarantine)
	base := newFakeClient(t, cluster)
	reconciler := developmentReconciler(base, base)
	for range cluster.Spec.Shards {
		current := getCluster(t, ctx, base, cluster)
		if err := reconciler.ensurePostgreSQLReplicationCredentials(ctx, current); err != nil {
			t.Fatal(err)
		}
	}

	current := getCluster(t, ctx, base, cluster)
	if len(current.Status.PostgreSQLReplicationCredentials) != int(cluster.Spec.Shards) {
		t.Fatalf("replication credential checkpoints = %#v", current.Status.PostgreSQLReplicationCredentials)
	}
	names := make(map[string]struct{}, cluster.Spec.Shards)
	uids := make(map[types.UID]struct{}, cluster.Spec.Shards)
	for index := range current.Status.PostgreSQLReplicationCredentials {
		recorded := &current.Status.PostgreSQLReplicationCredentials[index]
		if recorded.Shard != int32(index) || recorded.SecretUID == "" || !validCatalogAccessDigest(recorded.MaterialSHA256) {
			t.Fatalf("replication credential checkpoint = %#v", recorded)
		}
		if _, duplicate := names[recorded.SecretName]; duplicate {
			t.Fatalf("replication Secret name was reused: %s", recorded.SecretName)
		}
		if _, duplicate := uids[recorded.SecretUID]; duplicate {
			t.Fatalf("replication Secret UID was reused: %s", recorded.SecretUID)
		}
		names[recorded.SecretName] = struct{}{}
		uids[recorded.SecretUID] = struct{}{}
		secret := &corev1.Secret{}
		key := types.NamespacedName{Namespace: current.Namespace, Name: recorded.SecretName}
		if err := base.Get(ctx, key, secret); err != nil {
			t.Fatal(err)
		}
		if err := validateCheckpointedPostgreSQLReplicationCredential(secret, current, recorded); err != nil {
			t.Fatal(err)
		}
		if secret.Immutable == nil || !*secret.Immutable || len(secret.Data) != 1 || len(secret.Data[owned.PostgreSQLReplicationPasswordKey]) != hex.EncodedLen(postgresqlPasswordBytes) {
			t.Fatalf("replication credential Secret = %#v", secret)
		}
	}

	recorded := &current.Status.PostgreSQLReplicationCredentials[0]
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: current.Namespace, Name: recorded.SecretName}
	if err := base.Get(ctx, key, secret); err != nil {
		t.Fatal(err)
	}
	password := append([]byte(nil), secret.Data[owned.PostgreSQLReplicationPasswordKey]...)
	if password[0] == 'a' {
		password[0] = 'b'
	} else {
		password[0] = 'a'
	}
	secret.Data[owned.PostgreSQLReplicationPasswordKey] = password
	if err := base.Update(ctx, secret); err != nil {
		t.Fatal(err)
	}
	current = getCluster(t, ctx, base, cluster)
	if err := reconciler.ensurePostgreSQLReplicationCredentials(ctx, current); err == nil || !strings.Contains(err.Error(), "material differs from the checkpointed creation result") {
		t.Fatalf("changed replication material error = %v", err)
	}
}

func TestReplicationCredentialReconciliationReindexesAfterSortedInsert(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Status.PostgreSQLBootstrapSpec = bootstrapSpecStatus(cluster, owned.PostgreSQLRuntimeAgentQuarantine)
	base := newFakeClient(t, cluster)
	reconciler := developmentReconciler(base, base)
	current := getCluster(t, ctx, base, cluster)
	if err := reconciler.ensurePostgreSQLReplicationCredentials(ctx, current); err != nil {
		t.Fatal(err)
	}

	current = getCluster(t, ctx, base, cluster)
	shardZero, found := postgreSQLReplicationCredentialForShard(current, 0)
	if !found {
		t.Fatal("shard-zero replication credential was not checkpointed")
	}
	oldShardZeroName := shardZero.SecretName
	shardOne, found := postgreSQLReplicationCredentialForShard(current, 1)
	if !found {
		t.Fatal("shard-one replication credential was not checkpointed")
	}
	checkpointedShardOne := *shardOne
	for _, recorded := range current.Status.PostgreSQLReplicationCredentials {
		secret := &corev1.Secret{}
		key := types.NamespacedName{Namespace: current.Namespace, Name: recorded.SecretName}
		if err := base.Get(ctx, key, secret); err != nil {
			t.Fatal(err)
		}
		if err := base.Delete(ctx, secret); err != nil {
			t.Fatal(err)
		}
	}
	current.Status.PostgreSQLReplicationCredentials = []pgshardv1alpha1.PostgreSQLReplicationCredentialStatus{checkpointedShardOne}
	if err := base.Status().Update(ctx, current); err != nil {
		t.Fatal(err)
	}

	current = getCluster(t, ctx, base, cluster)
	err := reconciler.ensurePostgreSQLReplicationCredentials(ctx, current)
	if err == nil || !strings.Contains(err.Error(), "replication credential Secret "+checkpointedShardOne.SecretName) || !strings.Contains(err.Error(), "is missing; explicit recovery is required") {
		t.Fatalf("broken later-shard credential error = %v", err)
	}
	after := getCluster(t, ctx, base, cluster)
	newShardZero, found := postgreSQLReplicationCredentialForShard(after, 0)
	if !found || newShardZero.SecretName == oldShardZeroName || newShardZero.SecretUID == "" || newShardZero.MaterialSHA256 == "" {
		t.Fatalf("missing lower shard was not safely recreated: %#v", after.Status.PostgreSQLReplicationCredentials)
	}
	observedShardOne, found := postgreSQLReplicationCredentialForShard(after, 1)
	if !found || *observedShardOne != checkpointedShardOne {
		t.Fatalf("broken later-shard checkpoint changed: before=%#v after=%#v", checkpointedShardOne, observedShardOne)
	}
}

func TestReplicationCredentialLifecycleRecoversEveryCommittedWriteResponse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		stage          string
		firstCallFails bool
	}{
		{name: "intent checkpoint", stage: "intent", firstCallFails: true},
		{name: "Secret creation", stage: "create"},
		{name: "Secret UID checkpoint", stage: "uid", firstCallFails: true},
		{name: "material installation", stage: "material"},
		{name: "material checkpoint", stage: "digest", firstCallFails: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			cluster := validCluster()
			cluster.Spec.Shards = 1
			cluster.Status.PostgreSQLBootstrapSpec = bootstrapSpecStatus(cluster, owned.PostgreSQLRuntimeAgentQuarantine)
			base := newFakeClient(t, cluster)
			injected := false
			writeClient := interceptedClient(t, base, interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, kubeClient client.Client, subresource string, object client.Object, options ...client.SubResourceUpdateOption) error {
					candidate, ok := object.(*pgshardv1alpha1.PgShardCluster)
					if subresource == "status" && ok && len(candidate.Status.PostgreSQLReplicationCredentials) == 1 && !injected {
						recorded := candidate.Status.PostgreSQLReplicationCredentials[0]
						matches := test.stage == "intent" && recorded.SecretUID == "" ||
							test.stage == "uid" && recorded.SecretUID != "" && recorded.MaterialSHA256 == "" ||
							test.stage == "digest" && recorded.MaterialSHA256 != ""
						if matches {
							injected = true
							if err := kubeClient.SubResource(subresource).Update(ctx, object, options...); err != nil {
								return err
							}
							return apierrors.NewTimeoutError("injected lost replication "+test.stage+" response", 1)
						}
					}
					return kubeClient.SubResource(subresource).Update(ctx, object, options...)
				},
				Create: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, options ...client.CreateOption) error {
					secret, ok := object.(*corev1.Secret)
					if test.stage != "create" || !ok || secret.Labels[owned.ComponentLabel] != "postgresql-replication" || injected {
						return kubeClient.Create(ctx, object, options...)
					}
					injected = true
					if err := kubeClient.Create(ctx, object, options...); err != nil {
						return err
					}
					return apierrors.NewTimeoutError("injected lost replication create response", 1)
				},
				Update: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, options ...client.UpdateOption) error {
					secret, ok := object.(*corev1.Secret)
					if test.stage != "material" || !ok || secret.Labels[owned.ComponentLabel] != "postgresql-replication" || injected {
						return kubeClient.Update(ctx, object, options...)
					}
					injected = true
					if err := kubeClient.Update(ctx, object, options...); err != nil {
						return err
					}
					return apierrors.NewTimeoutError("injected lost replication material response", 1)
				},
			})

			current := getCluster(t, ctx, base, cluster)
			err := developmentReconciler(writeClient, base).ensurePostgreSQLReplicationCredentials(ctx, current)
			if test.firstCallFails {
				if err == nil || !strings.Contains(err.Error(), "injected lost replication") {
					t.Fatalf("first call error = %v, want injected response loss", err)
				}
			} else if err != nil {
				t.Fatalf("committed write outcome was not recovered: %v", err)
			}
			if !injected {
				t.Fatal("configured write response loss was not injected")
			}

			current = getCluster(t, ctx, base, cluster)
			if err := developmentReconciler(base, base).ensurePostgreSQLReplicationCredentials(ctx, current); err != nil {
				t.Fatalf("retry did not converge: %v", err)
			}
			current = getCluster(t, ctx, base, cluster)
			if len(current.Status.PostgreSQLReplicationCredentials) != 1 {
				t.Fatalf("replication credential checkpoints = %#v", current.Status.PostgreSQLReplicationCredentials)
			}
			recorded := &current.Status.PostgreSQLReplicationCredentials[0]
			secrets := &corev1.SecretList{}
			if err := base.List(ctx, secrets, client.InNamespace(cluster.Namespace), client.MatchingLabels{
				owned.ClusterLabel: cluster.Name, owned.ComponentLabel: "postgresql-replication",
			}); err != nil {
				t.Fatal(err)
			}
			if len(secrets.Items) != 1 {
				t.Fatalf("replication Secret count = %d, want one", len(secrets.Items))
			}
			if err := validateCheckpointedPostgreSQLReplicationCredential(&secrets.Items[0], current, recorded); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReplicationCredentialDeletionIsAnObservedFinalizerBarrier(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Finalizers = []string{resourceFinalizer}
	cluster.Status.PostgreSQLBootstrapSpec = bootstrapSpecStatus(cluster, owned.PostgreSQLRuntimeAgentQuarantine)
	base := newFakeClient(t, cluster)
	current := getCluster(t, ctx, base, cluster)
	if err := developmentReconciler(base, base).ensurePostgreSQLReplicationCredentials(ctx, current); err != nil {
		t.Fatal(err)
	}
	current = getCluster(t, ctx, base, cluster)
	recorded := &current.Status.PostgreSQLReplicationCredentials[0]
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: recorded.SecretName}
	secret := &corev1.Secret{}
	if err := base.Get(ctx, key, secret); err != nil {
		t.Fatal(err)
	}
	secret.Finalizers = []string{"test.pgshard.io/hold"}
	if err := base.Update(ctx, secret); err != nil {
		t.Fatal(err)
	}
	if err := base.Delete(ctx, current); err != nil {
		t.Fatal(err)
	}

	reconciler := developmentReconciler(base, base)
	result, err := reconciler.Reconcile(ctx, requestFor(cluster))
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != retryDelay {
		t.Fatalf("replication credential deletion barrier result = %#v", result)
	}
	deletingCluster := getCluster(t, ctx, base, cluster)
	if !controllerutil.ContainsFinalizer(deletingCluster, resourceFinalizer) {
		t.Fatal("cluster finalizer was released while replication material remained")
	}
	terminatingSecret := &corev1.Secret{}
	if err := base.Get(ctx, key, terminatingSecret); err != nil || terminatingSecret.DeletionTimestamp == nil {
		t.Fatalf("replication Secret was not held deleting: secret=%#v error=%v", terminatingSecret.ObjectMeta, err)
	}
	terminatingSecret.Finalizers = nil
	if err := base.Update(ctx, terminatingSecret); err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
	for range 6 {
		if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); client.IgnoreNotFound(err) != nil {
			t.Fatal(err)
		}
		if err := base.Get(ctx, requestFor(cluster).NamespacedName, &pgshardv1alpha1.PgShardCluster{}); apierrors.IsNotFound(err) {
			break
		}
	}
	if err := base.Get(ctx, key, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("recorded replication Secret survived finalization: %v", err)
	}
	if err := base.Get(ctx, requestFor(cluster).NamespacedName, &pgshardv1alpha1.PgShardCluster{}); !apierrors.IsNotFound(err) {
		t.Fatalf("cluster finalized before replication Secret absence: %v", err)
	}
}

func TestReplicationCredentialFinalizationCannotMaterializeALatePreUIDCreate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Status.PostgreSQLBootstrapSpec = bootstrapSpecStatus(cluster, owned.PostgreSQLRuntimeAgentQuarantine)
	base := newFakeClient(t, cluster)
	current := getCluster(t, ctx, base, cluster)
	name := owned.PostgreSQLReplicationSecretPrefix(current.Name, 0) + strings.Repeat("a", 32)
	current.Status.PostgreSQLReplicationCredentials = []pgshardv1alpha1.PostgreSQLReplicationCredentialStatus{{Shard: 0, SecretName: name}}
	if err := base.Status().Update(ctx, current); err != nil {
		t.Fatal(err)
	}

	absentReader := interceptedClient(t, base, interceptor.Funcs{
		Get: func(ctx context.Context, kubeClient client.WithWatch, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
			if _, ok := object.(*corev1.Secret); ok && key.Namespace == current.Namespace && key.Name == name {
				return apierrors.NewNotFound(corev1.Resource("secrets"), name)
			}
			return kubeClient.Get(ctx, key, object, options...)
		},
	})
	deleting, err := developmentReconciler(base, absentReader).deletePostgreSQLReplicationCredentialsForFinalization(ctx, current)
	if err != nil || deleting {
		t.Fatalf("absent pre-UID replication intent barrier = deleting %t, error %v", deleting, err)
	}

	delayed := owned.PostgreSQLReplicationIntentSecret(current, 0, name)
	if err := base.Create(ctx, delayed); err != nil {
		t.Fatal(err)
	}
	observed := &corev1.Secret{}
	if err := base.Get(ctx, client.ObjectKeyFromObject(delayed), observed); err != nil {
		t.Fatal(err)
	}
	if err := validatePostgreSQLReplicationIntentSecret(observed, current, 0, name); err != nil {
		t.Fatalf("late Create was not the empty replication intent: %v", err)
	}
	if len(observed.Data) != 0 || len(observed.StringData) != 0 || !metav1.IsControlledBy(observed, current) {
		t.Fatalf("late replication Create carried material or escaped cluster GC: %#v", observed)
	}
}

func TestCatalogAccessCreationIntentRecoversEveryWriteResponseWindow(t *testing.T) {
	t.Parallel()
	newCluster := func() *pgshardv1alpha1.PgShardCluster {
		cluster := validCluster()
		cluster.Spec.MembersPerShard = 1
		cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
		return cluster
	}
	listCatalogSecrets := func(t *testing.T, ctx context.Context, kubeClient client.Client, cluster *pgshardv1alpha1.PgShardCluster) []corev1.Secret {
		t.Helper()
		secrets := &corev1.SecretList{}
		if err := kubeClient.List(ctx, secrets, client.InNamespace(cluster.Namespace), client.MatchingLabels{
			owned.ClusterLabel: cluster.Name, owned.ComponentLabel: "shardschema",
		}); err != nil {
			t.Fatal(err)
		}
		return secrets.Items
	}

	t.Run("intent update rejected", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		cluster := newCluster()
		base := newFakeClient(t, cluster)
		injected := false
		writeClient := interceptedClient(t, base, interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, kubeClient client.Client, subresource string, object client.Object, options ...client.SubResourceUpdateOption) error {
				candidate, ok := object.(*pgshardv1alpha1.PgShardCluster)
				if subresource == "status" && ok && candidate.Status.CatalogAccess != nil && candidate.Status.CatalogAccess.SecretUID == "" && !injected {
					injected = true
					return apierrors.NewTimeoutError("injected uncommitted catalog intent update", 1)
				}
				return kubeClient.SubResource(subresource).Update(ctx, object, options...)
			},
		})
		current := getCluster(t, ctx, base, cluster)
		if err := developmentReconciler(writeClient, base).ensureCatalogAccess(ctx, current); err == nil || !strings.Contains(err.Error(), "uncommitted catalog intent update") {
			t.Fatalf("intent update error = %v", err)
		}
		if secrets := listCatalogSecrets(t, ctx, base, cluster); len(secrets) != 0 {
			t.Fatalf("catalog Secret was created before its intent: %d", len(secrets))
		}
		current = getCluster(t, ctx, base, cluster)
		if current.Status.CatalogAccess != nil {
			t.Fatalf("uncommitted intent became durable: %#v", current.Status.CatalogAccess)
		}
		if err := developmentReconciler(base, base).ensureCatalogAccess(ctx, current); err != nil {
			t.Fatal(err)
		}
		current = getCluster(t, ctx, base, cluster)
		if current.Status.CatalogAccess == nil || current.Status.CatalogAccess.SecretUID == "" || len(listCatalogSecrets(t, ctx, base, cluster)) != 1 {
			t.Fatalf("retry did not create exactly one checkpointed Secret: %#v", current.Status.CatalogAccess)
		}
	})

	t.Run("intent response lost after commit", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		cluster := newCluster()
		base := newFakeClient(t, cluster)
		injected := false
		writeClient := interceptedClient(t, base, interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, kubeClient client.Client, subresource string, object client.Object, options ...client.SubResourceUpdateOption) error {
				candidate, ok := object.(*pgshardv1alpha1.PgShardCluster)
				if subresource == "status" && ok && candidate.Status.CatalogAccess != nil && candidate.Status.CatalogAccess.SecretUID == "" && !injected {
					injected = true
					if err := kubeClient.SubResource(subresource).Update(ctx, object, options...); err != nil {
						return err
					}
					return apierrors.NewTimeoutError("injected lost catalog intent response", 1)
				}
				return kubeClient.SubResource(subresource).Update(ctx, object, options...)
			},
		})
		current := getCluster(t, ctx, base, cluster)
		if err := developmentReconciler(writeClient, base).ensureCatalogAccess(ctx, current); err == nil || !strings.Contains(err.Error(), "lost catalog intent response") {
			t.Fatalf("intent response error = %v", err)
		}
		persisted := getCluster(t, ctx, base, cluster)
		if persisted.Status.CatalogAccess == nil || persisted.Status.CatalogAccess.SecretUID != "" || len(listCatalogSecrets(t, ctx, base, cluster)) != 0 {
			t.Fatalf("lost intent response state = %#v", persisted.Status.CatalogAccess)
		}
		intentName := persisted.Status.CatalogAccess.SecretName
		if err := developmentReconciler(base, base).ensureCatalogAccess(ctx, persisted); err != nil {
			t.Fatal(err)
		}
		persisted = getCluster(t, ctx, base, cluster)
		if persisted.Status.CatalogAccess.SecretUID == "" || persisted.Status.CatalogAccess.SecretName != intentName || len(listCatalogSecrets(t, ctx, base, cluster)) != 1 {
			t.Fatalf("retry did not preserve the durable creation identity: %#v", persisted.Status.CatalogAccess)
		}
	})

	t.Run("Secret create response lost after commit", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		cluster := newCluster()
		base := newFakeClient(t, cluster)
		createAttempts := 0
		writeClient := interceptedClient(t, base, interceptor.Funcs{
			Create: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, options ...client.CreateOption) error {
				secret, ok := object.(*corev1.Secret)
				if !ok || secret.Labels[owned.ComponentLabel] != "shardschema" {
					return kubeClient.Create(ctx, object, options...)
				}
				createAttempts++
				if err := kubeClient.Create(ctx, object, options...); err != nil {
					return err
				}
				return apierrors.NewTimeoutError("injected lost catalog Secret create response", 1)
			},
		})
		current := getCluster(t, ctx, base, cluster)
		if err := developmentReconciler(writeClient, base).ensureCatalogAccess(ctx, current); err != nil {
			t.Fatalf("committed Secret outcome was not recovered: %v", err)
		}
		current = getCluster(t, ctx, base, cluster)
		if createAttempts != 1 || current.Status.CatalogAccess == nil || current.Status.CatalogAccess.SecretUID == "" || len(listCatalogSecrets(t, ctx, base, cluster)) != 1 {
			t.Fatalf("lost create response duplicated or failed to checkpoint: attempts=%d status=%#v", createAttempts, current.Status.CatalogAccess)
		}
	})

	t.Run("late empty Secret create is adopted before material exists", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		cluster := newCluster()
		base := newFakeClient(t, cluster)
		var delayed *corev1.Secret
		firstClient := interceptedClient(t, base, interceptor.Funcs{
			Create: func(_ context.Context, _ client.WithWatch, object client.Object, _ ...client.CreateOption) error {
				secret, ok := object.(*corev1.Secret)
				if !ok || secret.Labels[owned.ComponentLabel] != "shardschema" {
					return fmt.Errorf("unexpected create during delayed catalog test: %T", object)
				}
				delayed = secret.DeepCopy()
				return apierrors.NewTimeoutError("injected outcome-unknown catalog Secret create", 1)
			},
		})
		current := getCluster(t, ctx, base, cluster)
		if err := developmentReconciler(firstClient, base).ensureCatalogAccess(ctx, current); err == nil || !strings.Contains(err.Error(), "outcome-unknown catalog Secret create") {
			t.Fatalf("initial create error = %v", err)
		}
		persisted := getCluster(t, ctx, base, cluster)
		if delayed == nil || persisted.Status.CatalogAccess == nil || persisted.Status.CatalogAccess.SecretUID != "" || len(listCatalogSecrets(t, ctx, base, cluster)) != 0 {
			t.Fatalf("outcome-unknown create state = status %#v delayed %#v", persisted.Status.CatalogAccess, delayed)
		}
		if len(delayed.Data) != 0 || delayed.Immutable != nil {
			t.Fatalf("outcome-unknown Create carried key material: %#v", delayed)
		}
		intentName := persisted.Status.CatalogAccess.SecretName

		injected := false
		retryClient := interceptedClient(t, base, interceptor.Funcs{
			Create: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, options ...client.CreateOption) error {
				secret, ok := object.(*corev1.Secret)
				if ok && secret.Labels[owned.ComponentLabel] == "shardschema" && !injected {
					injected = true
					if err := base.Create(ctx, delayed); err != nil {
						return err
					}
				}
				return kubeClient.Create(ctx, object, options...)
			},
		})
		if err := developmentReconciler(retryClient, base).ensureCatalogAccess(ctx, persisted); err != nil {
			t.Fatalf("late empty Create was not recovered: %v", err)
		}
		persisted = getCluster(t, ctx, base, cluster)
		secrets := listCatalogSecrets(t, ctx, base, cluster)
		if !injected || len(secrets) != 1 || secrets[0].Name != intentName || secrets[0].Immutable == nil || !*secrets[0].Immutable || len(secrets[0].Data) == 0 || persisted.Status.CatalogAccess.SecretName != intentName || persisted.Status.CatalogAccess.SecretUID == "" || persisted.Status.CatalogAccess.ClientSHA256 == "" || persisted.Status.CatalogAccess.ServerSHA256 == "" {
			t.Fatalf("late create escaped the permanent identity: injected=%t status=%#v secrets=%#v", injected, persisted.Status.CatalogAccess, secrets)
		}
	})

	t.Run("material update response lost after commit", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		cluster := newCluster()
		base := newFakeClient(t, cluster)
		updates := 0
		writeClient := interceptedClient(t, base, interceptor.Funcs{
			Update: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, options ...client.UpdateOption) error {
				secret, ok := object.(*corev1.Secret)
				if !ok || secret.Labels[owned.ComponentLabel] != "shardschema" {
					return kubeClient.Update(ctx, object, options...)
				}
				updates++
				if err := kubeClient.Update(ctx, object, options...); err != nil {
					return err
				}
				return apierrors.NewTimeoutError("injected lost catalog material update response", 1)
			},
		})
		current := getCluster(t, ctx, base, cluster)
		if err := developmentReconciler(writeClient, base).ensureCatalogAccess(ctx, current); err != nil {
			t.Fatalf("committed material outcome was not recovered: %v", err)
		}
		current = getCluster(t, ctx, base, cluster)
		if updates != 1 || current.Status.CatalogAccess == nil || current.Status.CatalogAccess.ClientSHA256 == "" || current.Status.CatalogAccess.ServerSHA256 == "" || len(listCatalogSecrets(t, ctx, base, cluster)) != 1 {
			t.Fatalf("lost material response was not checkpointed: updates=%d status=%#v", updates, current.Status.CatalogAccess)
		}
	})

	t.Run("material checkpoint rejected before commit", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		cluster := newCluster()
		base := newFakeClient(t, cluster)
		injected := false
		writeClient := interceptedClient(t, base, interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, kubeClient client.Client, subresource string, object client.Object, options ...client.SubResourceUpdateOption) error {
				candidate, ok := object.(*pgshardv1alpha1.PgShardCluster)
				if subresource == "status" && ok && candidate.Status.CatalogAccess != nil && candidate.Status.CatalogAccess.ClientSHA256 != "" && !injected {
					injected = true
					return apierrors.NewTimeoutError("injected uncommitted catalog material checkpoint", 1)
				}
				return kubeClient.SubResource(subresource).Update(ctx, object, options...)
			},
		})
		current := getCluster(t, ctx, base, cluster)
		if err := developmentReconciler(writeClient, base).ensureCatalogAccess(ctx, current); err == nil || !strings.Contains(err.Error(), "uncommitted catalog material checkpoint") {
			t.Fatalf("material checkpoint error = %v", err)
		}
		persisted := getCluster(t, ctx, base, cluster)
		secrets := listCatalogSecrets(t, ctx, base, cluster)
		if persisted.Status.CatalogAccess == nil || persisted.Status.CatalogAccess.SecretUID == "" || persisted.Status.CatalogAccess.ClientSHA256 != "" || persisted.Status.CatalogAccess.ServerSHA256 != "" || len(secrets) != 1 || secrets[0].Immutable == nil || !*secrets[0].Immutable || len(secrets[0].Data) == 0 {
			t.Fatalf("uncommitted material checkpoint state = status %#v secrets %#v", persisted.Status.CatalogAccess, secrets)
		}
		if err := developmentReconciler(base, base).ensureCatalogAccess(ctx, persisted); err != nil {
			t.Fatalf("retry did not adopt installed material: %v", err)
		}
		persisted = getCluster(t, ctx, base, cluster)
		if persisted.Status.CatalogAccess.ClientSHA256 == "" || persisted.Status.CatalogAccess.ServerSHA256 == "" || persisted.Status.CatalogAccess.SecretUID != secrets[0].UID {
			t.Fatalf("retry did not checkpoint installed material: %#v", persisted.Status.CatalogAccess)
		}
	})

	t.Run("material checkpoint response lost after commit", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		cluster := newCluster()
		base := newFakeClient(t, cluster)
		injected := false
		writeClient := interceptedClient(t, base, interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, kubeClient client.Client, subresource string, object client.Object, options ...client.SubResourceUpdateOption) error {
				candidate, ok := object.(*pgshardv1alpha1.PgShardCluster)
				if subresource == "status" && ok && candidate.Status.CatalogAccess != nil && candidate.Status.CatalogAccess.ClientSHA256 != "" && !injected {
					injected = true
					if err := kubeClient.SubResource(subresource).Update(ctx, object, options...); err != nil {
						return err
					}
					return apierrors.NewTimeoutError("injected lost catalog material checkpoint response", 1)
				}
				return kubeClient.SubResource(subresource).Update(ctx, object, options...)
			},
		})
		current := getCluster(t, ctx, base, cluster)
		if err := developmentReconciler(writeClient, base).ensureCatalogAccess(ctx, current); err == nil || !strings.Contains(err.Error(), "lost catalog material checkpoint response") {
			t.Fatalf("material checkpoint response error = %v", err)
		}
		persisted := getCluster(t, ctx, base, cluster)
		secrets := listCatalogSecrets(t, ctx, base, cluster)
		if persisted.Status.CatalogAccess == nil || persisted.Status.CatalogAccess.ClientSHA256 == "" || persisted.Status.CatalogAccess.ServerSHA256 == "" || len(secrets) != 1 {
			t.Fatalf("lost material checkpoint was not durable: status %#v secrets %#v", persisted.Status.CatalogAccess, secrets)
		}
		if err := developmentReconciler(base, base).ensureCatalogAccess(ctx, persisted); err != nil {
			t.Fatalf("retry did not validate committed material checkpoint: %v", err)
		}
		if got := getCluster(t, ctx, base, cluster).Status.CatalogAccess; !reflect.DeepEqual(got, persisted.Status.CatalogAccess) {
			t.Fatalf("retry changed a committed material checkpoint: got %#v want %#v", got, persisted.Status.CatalogAccess)
		}
	})

	t.Run("late material update loses by resource version or is adopted", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		cluster := newCluster()
		base := newFakeClient(t, cluster)
		var delayed *corev1.Secret
		firstClient := interceptedClient(t, base, interceptor.Funcs{
			Update: func(_ context.Context, _ client.WithWatch, object client.Object, _ ...client.UpdateOption) error {
				secret, ok := object.(*corev1.Secret)
				if !ok || secret.Labels[owned.ComponentLabel] != "shardschema" {
					return fmt.Errorf("unexpected update during delayed material test: %T", object)
				}
				delayed = secret.DeepCopy()
				return apierrors.NewTimeoutError("injected outcome-unknown catalog material update", 1)
			},
		})
		current := getCluster(t, ctx, base, cluster)
		if err := developmentReconciler(firstClient, base).ensureCatalogAccess(ctx, current); err == nil || !strings.Contains(err.Error(), "outcome-unknown catalog material update") {
			t.Fatalf("initial material update error = %v", err)
		}
		persisted := getCluster(t, ctx, base, cluster)
		secrets := listCatalogSecrets(t, ctx, base, cluster)
		if delayed == nil || persisted.Status.CatalogAccess == nil || persisted.Status.CatalogAccess.SecretUID == "" || persisted.Status.CatalogAccess.ClientSHA256 != "" || len(secrets) != 1 || len(secrets[0].Data) != 0 {
			t.Fatalf("outcome-unknown material state = status %#v delayed %#v secrets %#v", persisted.Status.CatalogAccess, delayed, secrets)
		}

		injected := false
		retryClient := interceptedClient(t, base, interceptor.Funcs{
			Update: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, options ...client.UpdateOption) error {
				secret, ok := object.(*corev1.Secret)
				if ok && secret.Labels[owned.ComponentLabel] == "shardschema" && !injected {
					injected = true
					if err := base.Update(ctx, delayed); err != nil {
						return err
					}
				}
				return kubeClient.Update(ctx, object, options...)
			},
		})
		if err := developmentReconciler(retryClient, base).ensureCatalogAccess(ctx, persisted); err != nil {
			t.Fatalf("late material winner was not recovered: %v", err)
		}
		persisted = getCluster(t, ctx, base, cluster)
		secrets = listCatalogSecrets(t, ctx, base, cluster)
		if !injected || len(secrets) != 1 || secrets[0].UID != persisted.Status.CatalogAccess.SecretUID || persisted.Status.CatalogAccess.ClientSHA256 == "" || persisted.Status.CatalogAccess.ServerSHA256 == "" {
			t.Fatalf("material update race escaped one UID: injected=%t status=%#v secrets=%#v", injected, persisted.Status.CatalogAccess, secrets)
		}
	})

	t.Run("identity checkpoint response rejected", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		cluster := newCluster()
		base := newFakeClient(t, cluster)
		injected := false
		writeClient := interceptedClient(t, base, interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, kubeClient client.Client, subresource string, object client.Object, options ...client.SubResourceUpdateOption) error {
				candidate, ok := object.(*pgshardv1alpha1.PgShardCluster)
				if subresource == "status" && ok && candidate.Status.CatalogAccess != nil && candidate.Status.CatalogAccess.SecretUID != "" && !injected {
					injected = true
					return apierrors.NewTimeoutError("injected uncommitted catalog identity checkpoint", 1)
				}
				return kubeClient.SubResource(subresource).Update(ctx, object, options...)
			},
		})
		current := getCluster(t, ctx, base, cluster)
		if err := developmentReconciler(writeClient, base).ensureCatalogAccess(ctx, current); err == nil || !strings.Contains(err.Error(), "uncommitted catalog identity checkpoint") {
			t.Fatalf("identity checkpoint error = %v", err)
		}
		persisted := getCluster(t, ctx, base, cluster)
		secrets := listCatalogSecrets(t, ctx, base, cluster)
		if persisted.Status.CatalogAccess == nil || persisted.Status.CatalogAccess.SecretUID != "" || len(secrets) != 1 || persisted.Status.CatalogAccess.SecretName != secrets[0].Name {
			t.Fatalf("uncommitted checkpoint did not preserve one recoverable Secret: status=%#v secrets=%#v", persisted.Status.CatalogAccess, secrets)
		}
		if err := developmentReconciler(base, base).ensureCatalogAccess(ctx, persisted); err != nil {
			t.Fatal(err)
		}
		persisted = getCluster(t, ctx, base, cluster)
		if persisted.Status.CatalogAccess.SecretUID != secrets[0].UID || len(listCatalogSecrets(t, ctx, base, cluster)) != 1 {
			t.Fatalf("retry did not checkpoint the existing Secret: %#v", persisted.Status.CatalogAccess)
		}
	})
}

func TestCatalogAccessIntentRejectsNoncanonicalMetadataBeforeMaterialUpdate(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*corev1.Secret)
	}{
		{
			name: "extra annotation",
			mutate: func(secret *corev1.Secret) {
				secret.Annotations["reflector.v1.k8s.emberstack.com/reflection-allowed"] = "true"
			},
		},
		{
			name: "extra owner",
			mutate: func(secret *corev1.Secret) {
				secret.OwnerReferences = append(secret.OwnerReferences, metav1.OwnerReference{
					APIVersion: "v1", Kind: "ConfigMap", Name: "foreign", UID: "foreign-uid",
				})
			},
		},
		{
			name: "blocking finalizer",
			mutate: func(secret *corev1.Secret) {
				secret.Finalizers = []string{"foreign.example/block"}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			cluster := validCluster()
			cluster.Spec.MembersPerShard = 1
			cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
			base := newFakeClient(t, cluster)
			current := getCluster(t, ctx, base, cluster)
			name := owned.CatalogAccessSecretPrefix(current.Name) + strings.Repeat("a", 32)
			current.Status.CatalogAccess = &pgshardv1alpha1.CatalogAccessStatus{SecretName: name}
			if err := base.Status().Update(ctx, current); err != nil {
				t.Fatal(err)
			}

			intent := owned.CatalogAccessIntentSecret(current, name)
			intent.UID = types.UID("intent-" + strings.ReplaceAll(test.name, " ", "-"))
			test.mutate(intent)
			if err := base.Create(ctx, intent); err != nil {
				t.Fatal(err)
			}

			secretUpdates := 0
			writeClient := interceptedClient(t, base, interceptor.Funcs{
				Update: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, options ...client.UpdateOption) error {
					if _, ok := object.(*corev1.Secret); ok {
						secretUpdates++
					}
					return kubeClient.Update(ctx, object, options...)
				},
			})
			current = getCluster(t, ctx, base, cluster)
			err := developmentReconciler(writeClient, base).ensureCatalogAccess(ctx, current)
			if err == nil || !strings.Contains(err.Error(), "metadata is not bound to the exact PgShardCluster") {
				t.Fatalf("noncanonical intent error = %v", err)
			}
			observed := &corev1.Secret{}
			if err := base.Get(ctx, client.ObjectKeyFromObject(intent), observed); err != nil {
				t.Fatal(err)
			}
			if secretUpdates != 0 || len(observed.Data) != 0 || observed.Immutable != nil {
				t.Fatalf("noncanonical intent received material: updates=%d secret=%#v", secretUpdates, observed)
			}
		})
	}
}

func TestCatalogAccessDeletionIsAnObservedFinalizerBarrier(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	cluster.Finalizers = []string{resourceFinalizer}
	base := newFakeClient(t, cluster)
	current := getCluster(t, ctx, base, cluster)
	if err := developmentReconciler(base, base).ensureCatalogAccess(ctx, current); err != nil {
		t.Fatal(err)
	}
	current = getCluster(t, ctx, base, cluster)
	recorded := current.Status.CatalogAccess
	if recorded == nil || recorded.SecretUID == "" {
		t.Fatalf("catalog access was not checkpointed: %#v", recorded)
	}
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: recorded.SecretName}
	if err := base.Get(ctx, key, secret); err != nil {
		t.Fatal(err)
	}
	secret.Finalizers = []string{"test.pgshard.io/hold"}
	secret.Annotations["example.test/added-after-checkpoint"] = "mutable"
	if err := base.Update(ctx, secret); err != nil {
		t.Fatal(err)
	}
	if err := base.Delete(ctx, current); err != nil {
		t.Fatal(err)
	}
	reconciler := developmentReconciler(base, base)
	result, err := reconciler.Reconcile(ctx, requestFor(cluster))
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != retryDelay {
		t.Fatalf("catalog deletion barrier result = %#v", result)
	}
	deletingCluster := getCluster(t, ctx, base, cluster)
	if !controllerutil.ContainsFinalizer(deletingCluster, resourceFinalizer) {
		t.Fatal("cluster finalizer was released while catalog key material remained")
	}
	terminatingSecret := &corev1.Secret{}
	if err := base.Get(ctx, key, terminatingSecret); err != nil || terminatingSecret.DeletionTimestamp == nil {
		t.Fatalf("catalog Secret was not held deleting: secret=%#v error=%v", terminatingSecret.ObjectMeta, err)
	}
	terminatingSecret.Finalizers = nil
	if err := base.Update(ctx, terminatingSecret); err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
	for range 6 {
		if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); client.IgnoreNotFound(err) != nil {
			t.Fatal(err)
		}
		if err := base.Get(ctx, requestFor(cluster).NamespacedName, &pgshardv1alpha1.PgShardCluster{}); apierrors.IsNotFound(err) {
			break
		}
	}
	if err := base.Get(ctx, key, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("recorded catalog Secret survived finalization: %v", err)
	}
	if err := base.Get(ctx, requestFor(cluster).NamespacedName, &pgshardv1alpha1.PgShardCluster{}); !apierrors.IsNotFound(err) {
		t.Fatalf("cluster finalized before catalog Secret absence: %v", err)
	}
}

func TestCatalogAccessFinalizationCannotOrphanLateKeyMaterialBeforeUIDCheckpoint(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	base := newFakeClient(t, cluster)
	current := getCluster(t, ctx, base, cluster)
	name := owned.CatalogAccessSecretPrefix(current.Name) + strings.Repeat("a", 32)
	current.Status.CatalogAccess = &pgshardv1alpha1.CatalogAccessStatus{SecretName: name}
	if err := base.Status().Update(ctx, current); err != nil {
		t.Fatal(err)
	}

	// Model an outcome-unknown Create that has not reached the API server when
	// the authoritative finalization read proves the name absent.
	absentReader := interceptedClient(t, base, interceptor.Funcs{
		Get: func(ctx context.Context, kubeClient client.WithWatch, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
			if _, ok := object.(*corev1.Secret); ok && key.Namespace == current.Namespace && key.Name == name {
				return apierrors.NewNotFound(corev1.Resource("secrets"), name)
			}
			return kubeClient.Get(ctx, key, object, options...)
		},
	})
	deleting, err := developmentReconciler(base, absentReader).deleteCatalogAccessForFinalization(ctx, current)
	if err != nil || deleting {
		t.Fatalf("absent pre-UID intent barrier = deleting %t, error %v", deleting, err)
	}

	// A delayed request can subsequently create only the owner-bound empty
	// intent that was originally dispatched. No credential or private key has
	// been generated yet, and material installation requires a checkpointed UID.
	delayed := owned.CatalogAccessIntentSecret(current, name)
	delayed.UID = "late-empty-intent-uid"
	if err := base.Create(ctx, delayed); err != nil {
		t.Fatal(err)
	}
	observed := &corev1.Secret{}
	if err := base.Get(ctx, client.ObjectKeyFromObject(delayed), observed); err != nil {
		t.Fatal(err)
	}
	if err := validateCatalogAccessIntentSecret(observed, current, name); err != nil {
		t.Fatalf("late Create was not the empty intent: %v", err)
	}
	if len(observed.Data) != 0 || len(observed.StringData) != 0 || !metav1.IsControlledBy(observed, current) {
		t.Fatalf("late Create carried material or escaped cluster GC: %#v", observed)
	}
	observed.Finalizers = []string{"test.pgshard.io/hold-late-intent"}
	if err := base.Update(ctx, observed); err != nil {
		t.Fatal(err)
	}
	if err := base.Delete(ctx, observed); err != nil {
		t.Fatal(err)
	}
	deleting, err = developmentReconciler(base, base).deleteCatalogAccessForFinalization(ctx, current)
	if err != nil || !deleting {
		t.Fatalf("already deleting late empty intent cleanup = deleting %t, error %v", deleting, err)
	}
}

func TestCatalogAccessSecretLossReplacementAndMutationFailClosed(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, context.Context, client.Client, *pgshardv1alpha1.PgShardCluster, *corev1.Secret)
		want   string
	}{
		{
			name: "missing",
			mutate: func(t *testing.T, ctx context.Context, kubeClient client.Client, _ *pgshardv1alpha1.PgShardCluster, secret *corev1.Secret) {
				t.Helper()
				if err := kubeClient.Delete(ctx, secret); err != nil {
					t.Fatal(err)
				}
			},
			want: "is missing; explicit recovery is required",
		},
		{
			name: "replacement",
			mutate: func(t *testing.T, ctx context.Context, kubeClient client.Client, cluster *pgshardv1alpha1.PgShardCluster, secret *corev1.Secret) {
				t.Helper()
				if err := kubeClient.Delete(ctx, secret); err != nil {
					t.Fatal(err)
				}
				material, err := newCatalogAccessMaterial(cluster)
				if err != nil {
					t.Fatal(err)
				}
				replacement := owned.CatalogAccessIntentSecret(cluster, secret.Name)
				immutable := true
				replacement.Immutable = &immutable
				replacement.Data = material
				replacement.UID = "replacement-catalog-access-uid"
				if err := kubeClient.Create(ctx, replacement); err != nil {
					t.Fatal(err)
				}
			},
			want: "expected recorded UID",
		},
		{
			name: "unexpected key",
			mutate: func(t *testing.T, ctx context.Context, kubeClient client.Client, _ *pgshardv1alpha1.PgShardCluster, secret *corev1.Secret) {
				t.Helper()
				delete(secret.Data, owned.CatalogTLSPrivateKeyKey)
				if err := kubeClient.Update(ctx, secret); err != nil {
					t.Fatal(err)
				}
			},
			want: "unexpected key set",
		},
		{
			name: "changed material",
			mutate: func(t *testing.T, ctx context.Context, kubeClient client.Client, _ *pgshardv1alpha1.PgShardCluster, secret *corev1.Secret) {
				t.Helper()
				password := append([]byte(nil), secret.Data[owned.CatalogPasswordKey]...)
				if password[0] == 'a' {
					password[0] = 'b'
				} else {
					password[0] = 'a'
				}
				secret.Data[owned.CatalogPasswordKey] = password
				if err := kubeClient.Update(ctx, secret); err != nil {
					t.Fatal(err)
				}
			},
			want: "material differs from the checkpointed creation result",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			cluster := validCluster()
			cluster.Spec.MembersPerShard = 1
			cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
			kubeClient := newFakeClient(t, cluster)
			reconciler := developmentReconciler(kubeClient, kubeClient)
			if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
				t.Fatal(err)
			}
			current := getCluster(t, ctx, kubeClient, cluster)
			if current.Status.CatalogAccess == nil {
				t.Fatal("catalog access identity was not checkpointed")
			}
			recorded := *current.Status.CatalogAccess
			secret := &corev1.Secret{}
			if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: recorded.SecretName}, secret); err != nil {
				t.Fatal(err)
			}
			test.mutate(t, ctx, kubeClient, current, secret)

			_, err := reconciler.Reconcile(ctx, requestFor(cluster))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("reconcile error = %v, want %q", err, test.want)
			}
			after := getCluster(t, ctx, kubeClient, cluster)
			if after.Status.CatalogAccess == nil || *after.Status.CatalogAccess != recorded {
				t.Fatalf("failed reconciliation changed catalog access checkpoint: before=%#v after=%#v", recorded, after.Status.CatalogAccess)
			}
		})
	}
}

func TestCatalogAccessNearExpiryDegradesReconciliationWithoutRotation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	kubeClient := newFakeClient(t, cluster)
	reconciler := developmentReconciler(kubeClient, kubeClient)
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	current := getCluster(t, ctx, kubeClient, cluster)
	recorded := current.Status.CatalogAccess
	if recorded == nil {
		t.Fatal("catalog access identity was not checkpointed")
	}
	secret := &corev1.Secret{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: recorded.SecretName}, secret); err != nil {
		t.Fatal(err)
	}
	issued := time.Now().UTC().Add(-(5*365*24*time.Hour - 179*24*time.Hour))
	bundle, err := pki.GenerateStaticServerBundle(
		issued,
		rand.Reader,
		"near-expiry catalog CA",
		owned.CatalogTLSDNSNames(cluster.Name, cluster.Namespace),
	)
	if err != nil {
		t.Fatal(err)
	}
	secret.Data[owned.CatalogCACertificateKey] = bundle.CACertificate
	secret.Data[owned.CatalogTLSCertificateKey] = bundle.ServerCertificate
	secret.Data[owned.CatalogTLSPrivateKeyKey] = bundle.ServerPrivateKey
	if err := kubeClient.Update(ctx, secret); err != nil {
		t.Fatal(err)
	}
	current.Status.CatalogAccess = catalogAccessStatus(secret)
	if err := kubeClient.Status().Update(ctx, current); err != nil {
		t.Fatal(err)
	}

	_, err = reconciler.Reconcile(ctx, requestFor(cluster))
	if err == nil || !strings.Contains(err.Error(), "zero-downtime certificate rotation is not implemented") {
		t.Fatalf("near-expiry reconciliation error = %v", err)
	}
	degraded := getCluster(t, ctx, kubeClient, cluster)
	assertCondition(t, degraded, reconciledCondition, metav1.ConditionFalse, "CatalogAccessReconcileFailed")
	if degraded.Status.CatalogAccess == nil || degraded.Status.CatalogAccess.SecretUID != recorded.SecretUID {
		t.Fatalf("near-expiry reconciliation rotated catalog access: before=%#v after=%#v", recorded, degraded.Status.CatalogAccess)
	}
}

func TestReconcileRefusesPostgreSQLWorkloadsWithoutPodFencingNamespace(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		members int32
		agent   bool
	}{
		{name: "direct singleton", members: 1},
		{name: "agent multi-member", members: 3, agent: true},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			cluster := validCluster()
			cluster.Spec.MembersPerShard = test.members
			if test.members == 1 {
				cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
			}
			base := newFakeClient(t, cluster)
			namespace := &corev1.Namespace{}
			if err := base.Get(ctx, types.NamespacedName{Name: cluster.Namespace}, namespace); err != nil {
				t.Fatal(err)
			}
			delete(namespace.Labels, podfence.NamespaceLabel)
			if err := base.Update(ctx, namespace); err != nil {
				t.Fatal(err)
			}

			reconciler := developmentReconciler(base, base)
			if test.agent {
				reconciler.Images.PostgreSQLRuntime = owned.PostgreSQLRuntimeAgentQuarantine
			}
			if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err == nil || !strings.Contains(err.Error(), "must be labelled pgshard.io/pod-fencing=enabled") {
				t.Fatalf("unfenced namespace reconcile error = %v", err)
			}
			got := getCluster(t, ctx, base, cluster)
			assertCondition(t, got, readyCondition, metav1.ConditionFalse, "PodFencingUnavailable")
			if len(got.Status.PostgreSQLBootstraps) != 0 || controllerutil.ContainsFinalizer(got, resourceFinalizer) {
				t.Fatalf("unfenced namespace crossed the PostgreSQL creation barrier: status=%#v finalizers=%#v", got.Status, got.Finalizers)
			}
			secrets := &corev1.SecretList{}
			claims := &corev1.PersistentVolumeClaimList{}
			statefulSets := &appsv1.StatefulSetList{}
			if err := base.List(ctx, secrets, client.InNamespace(cluster.Namespace)); err != nil {
				t.Fatal(err)
			}
			if err := base.List(ctx, claims, client.InNamespace(cluster.Namespace)); err != nil {
				t.Fatal(err)
			}
			if err := base.List(ctx, statefulSets, client.InNamespace(cluster.Namespace)); err != nil {
				t.Fatal(err)
			}
			if len(secrets.Items) != 0 || len(claims.Items) != 0 || len(statefulSets.Items) != 0 {
				t.Fatalf("unfenced namespace created PostgreSQL resources: secrets=%d claims=%d StatefulSets=%d", len(secrets.Items), len(claims.Items), len(statefulSets.Items))
			}
		})
	}
}

func TestReconcileDirectMultiMemberSkipsUnusedPodFencingPreflight(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	base := newFakeClient(t, cluster)
	namespace := &corev1.Namespace{}
	if err := base.Get(ctx, types.NamespacedName{Name: cluster.Namespace}, namespace); err != nil {
		t.Fatal(err)
	}
	delete(namespace.Labels, podfence.NamespaceLabel)
	if err := base.Update(ctx, namespace); err != nil {
		t.Fatal(err)
	}
	current := getCluster(t, ctx, base, cluster)
	delete(current.Annotations, podfence.HandshakeChallengeAnnotation)
	delete(current.Annotations, podfence.HandshakeReceiptAnnotation)
	if err := base.Update(ctx, current); err != nil {
		t.Fatal(err)
	}

	reconciler := developmentReconciler(base, base)
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	got := getCluster(t, ctx, base, cluster)
	if !controllerutil.ContainsFinalizer(got, resourceFinalizer) || len(got.Status.PostgreSQLBootstraps) != 0 {
		t.Fatalf("direct multi-member reconciliation = status %#v finalizers %#v", got.Status, got.Finalizers)
	}
}

func TestReconcileRefusesToReplaceMissingCredentialAfterWorkloadCreation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	fakeClient := newFakeClient(t, cluster)
	reconciler := developmentReconciler(fakeClient, nil)
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	current := getCluster(t, ctx, fakeClient, cluster)
	bootstrap := bootstrapForShard(t, current, 0)
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.SecretName}
	if err := fakeClient.Get(ctx, key, secret); err != nil {
		t.Fatal(err)
	}
	if err := fakeClient.Delete(ctx, secret); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err == nil || !strings.Contains(err.Error(), "recorded UID") || !strings.Contains(err.Error(), "explicit recovery is required") {
		t.Fatalf("missing credential was not fenced: %v", err)
	}
	if err := fakeClient.Get(ctx, key, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("missing credential was recreated: %v", err)
	}
}

func TestReconcileNeverAdoptsUnrecordedBootstrapChildren(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	orphanSecret := owned.PostgreSQLAuthSecret(cluster, 0, owned.PostgreSQLAuthSecretPrefix(cluster.Name, 0)+"orphan", []byte(strings.Repeat("a", hex.EncodedLen(postgresqlPasswordBytes))))
	orphanSecret.UID = "unrecorded-secret"
	orphanClaim := owned.PostgreSQLDataPVC(cluster, 0, owned.PostgreSQLDataPVCPrefix(cluster.Name, 0)+"orphan", cluster.Spec.Storage.Size, cluster.Spec.Storage.StorageClassName, orphanSecret.Name, orphanSecret.UID)
	orphanClaim.UID = "unrecorded-pvc"
	fakeClient := newFakeClient(t, cluster, orphanSecret, orphanClaim)
	reconciler := developmentReconciler(fakeClient, nil)
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	current := getCluster(t, ctx, fakeClient, cluster)
	bootstrap := bootstrapForShard(t, current, 0)
	if bootstrap.SecretName == orphanSecret.Name || bootstrap.PVCName == orphanClaim.Name {
		t.Fatalf("unrecorded bootstrap child was adopted: %#v", bootstrap)
	}
	statefulSet := &appsv1.StatefulSet{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: owned.PostgreSQLShardStatefulSetName(cluster.Name, 0)}
	if err := fakeClient.Get(ctx, key, statefulSet); err != nil {
		t.Fatal(err)
	}
	if got := statefulSet.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName; got != bootstrap.PVCName {
		t.Fatalf("workload data PVC = %q, want recorded %q", got, bootstrap.PVCName)
	}
	if err := fakeClient.Get(ctx, client.ObjectKeyFromObject(orphanSecret), &corev1.Secret{}); err != nil {
		t.Fatalf("unrecorded Secret should remain an unused crash orphan until cluster deletion: %v", err)
	}
	if err := fakeClient.Get(ctx, client.ObjectKeyFromObject(orphanClaim), &corev1.PersistentVolumeClaim{}); err != nil {
		t.Fatalf("unrecorded PVC should remain unused: %v", err)
	}
}

func TestReconcileReusesCheckpointedBootstrapIntentAfterStatusFailure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for name, failPVCCheckpoint := range map[string]bool{"credential UID": false, "PVC UID": true} {
		name, failPVCCheckpoint := name, failPVCCheckpoint
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cluster := validCluster()
			cluster.Spec.Shards = 1
			cluster.Spec.MembersPerShard = 1
			cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
			base := newFakeClient(t, cluster)
			injected := errors.New("injected bootstrap identity checkpoint failure")
			failing := interceptedClient(t, base, interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, kubeClient client.Client, subresource string, object client.Object, options ...client.SubResourceUpdateOption) error {
					current, ok := object.(*pgshardv1alpha1.PgShardCluster)
					if subresource == "status" && ok && len(current.Status.PostgreSQLBootstraps) == 1 {
						bootstrap := current.Status.PostgreSQLBootstraps[0]
						atTarget := bootstrap.SecretUID != "" && ((!failPVCCheckpoint && bootstrap.PVCUID == "") || (failPVCCheckpoint && bootstrap.PVCUID != ""))
						if atTarget {
							return injected
						}
					}
					return kubeClient.SubResource(subresource).Update(ctx, object, options...)
				},
			})
			if _, err := developmentReconciler(failing, nil).Reconcile(ctx, requestFor(cluster)); err == nil || !strings.Contains(err.Error(), injected.Error()) {
				t.Fatalf("bootstrap checkpoint failure was not surfaced: %v", err)
			}

			partial := getCluster(t, ctx, base, cluster)
			bootstrap := bootstrapForShard(t, partial, 0)
			if bootstrap.SecretName == "" || bootstrap.PVCName == "" {
				t.Fatalf("child names were not checkpointed before creation: %#v", bootstrap)
			}
			secret := &corev1.Secret{}
			if err := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.SecretName}, secret); err != nil {
				t.Fatal(err)
			}
			var claimUID types.UID
			if failPVCCheckpoint {
				claim := &corev1.PersistentVolumeClaim{}
				if err := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}, claim); err != nil {
					t.Fatal(err)
				}
				claimUID = claim.UID
			} else if err := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}, &corev1.PersistentVolumeClaim{}); !apierrors.IsNotFound(err) {
				t.Fatalf("PVC was created before the credential UID checkpoint: %v", err)
			}

			if _, err := developmentReconciler(base, nil).Reconcile(ctx, requestFor(cluster)); err != nil {
				t.Fatal(err)
			}
			complete := bootstrapForShard(t, getCluster(t, ctx, base, cluster), 0)
			if complete.SecretName != bootstrap.SecretName || complete.PVCName != bootstrap.PVCName || complete.SecretUID != secret.UID || complete.PVCUID == "" || (claimUID != "" && complete.PVCUID != claimUID) {
				t.Fatalf("reconcile did not reuse the checkpointed creation intent: partial=%#v complete=%#v", bootstrap, complete)
			}
			secrets := &corev1.SecretList{}
			if err := base.List(ctx, secrets, client.InNamespace(cluster.Namespace), client.MatchingLabels{owned.ClusterLabel: cluster.Name, owned.ComponentLabel: "postgresql"}); err != nil {
				t.Fatal(err)
			}
			claims := &corev1.PersistentVolumeClaimList{}
			if err := base.List(ctx, claims, client.InNamespace(cluster.Namespace), client.MatchingLabels{owned.ClusterLabel: cluster.Name, owned.ComponentLabel: "postgresql"}); err != nil {
				t.Fatal(err)
			}
			if len(secrets.Items) != 1 || len(claims.Items) != 1 {
				t.Fatalf("checkpoint recovery duplicated bootstrap children: secrets=%d PVCs=%d", len(secrets.Items), len(claims.Items))
			}
		})
	}
}

func TestReconcilePersistsStorageClassBeforePVCCreate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	cluster.Spec.Storage.StorageClassName = nil
	storageClass := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{
		Name:        "authoritative-default",
		Annotations: map[string]string{"storageclass.kubernetes.io/is-default-class": "true"},
	}}
	base := newFakeClient(t, cluster, storageClass)
	injected := errors.New("stop after checking PVC dispatch prerequisites")
	observed := false
	writeClient := interceptedClient(t, base, interceptor.Funcs{
		Create: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, options ...client.CreateOption) error {
			claim, ok := object.(*corev1.PersistentVolumeClaim)
			if !ok {
				return kubeClient.Create(ctx, object, options...)
			}
			persisted := getCluster(t, ctx, base, cluster)
			bootstrap := bootstrapForShard(t, persisted, 0)
			if bootstrap.SecretUID == "" || !bootstrap.PVCFenceDetached || bootstrap.PVCUID != "" || bootstrap.PVCStorageClassName == nil || *bootstrap.PVCStorageClassName != storageClass.Name {
				t.Fatalf("persisted state at PVC dispatch = %#v", bootstrap)
			}
			secret := &corev1.Secret{}
			if err := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.SecretName}, secret); err != nil {
				t.Fatal(err)
			}
			if secret.UID != bootstrap.SecretUID || len(secret.OwnerReferences) != 0 || !postgresqlDataPVCIsCreationFenced(claim, bootstrap) {
				t.Fatalf("PVC dispatch was not ordered after durable fence detachment: secret=%#v claim=%#v", secret.ObjectMeta, claim.ObjectMeta)
			}
			if claim.Spec.StorageClassName == nil || *claim.Spec.StorageClassName != storageClass.Name {
				t.Fatalf("PVC class at dispatch = %#v", claim.Spec.StorageClassName)
			}
			observed = true
			return injected
		},
	})
	if _, err := developmentReconciler(writeClient, nil).Reconcile(ctx, requestFor(cluster)); err == nil || !strings.Contains(err.Error(), injected.Error()) {
		t.Fatalf("PVC dispatch interceptor was not reached: %v", err)
	}
	if !observed {
		t.Fatal("PVC create was not observed after the durable storage-class checkpoint")
	}
}

func TestReconcileFencesBypassedProvisionedSpecMutation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for name, mutate := range map[string]func(*pgshardv1alpha1.PgShardCluster){
		"shards": func(cluster *pgshardv1alpha1.PgShardCluster) { cluster.Spec.Shards++ },
		"members": func(cluster *pgshardv1alpha1.PgShardCluster) {
			cluster.Spec.MembersPerShard = 3
			cluster.Spec.Durability = pgshardv1alpha1.DurabilitySynchronous
		},
		"storage": func(cluster *pgshardv1alpha1.PgShardCluster) { cluster.Spec.Storage.Size = resource.MustParse("20Gi") },
		"deletion policy": func(cluster *pgshardv1alpha1.PgShardCluster) {
			cluster.Spec.Storage.DeletionPolicy = pgshardv1alpha1.DeletionDelete
		},
		"database topology": func(cluster *pgshardv1alpha1.PgShardCluster) {
			cluster.Spec.Databases = []pgshardv1alpha1.DatabaseTemplate{{Name: "app", Shards: 1, Cells: []int32{0}}}
		},
	} {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cluster := validCluster()
			cluster.Spec.Shards = 1
			cluster.Spec.MembersPerShard = 1
			cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
			fakeClient := newFakeClient(t, cluster)
			reconciler := developmentReconciler(fakeClient, nil)
			if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
				t.Fatal(err)
			}
			current := getCluster(t, ctx, fakeClient, cluster)
			mutate(current)
			if err := fakeClient.Update(ctx, current); err != nil {
				t.Fatal(err)
			}
			if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err == nil || !strings.Contains(err.Error(), "differs from the provisioned PostgreSQL bootstrap spec") {
				t.Fatalf("bypassed %s mutation was not fenced: %v", name, err)
			}
		})
	}
}

func TestReconcileMigratesOnlyEmptyLegacyDatabaseTopologyCheckpoint(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for name, declared := range map[string]bool{"empty": false, "declared": true} {
		name, declared := name, declared
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cluster := validCluster()
			cluster.Spec.Shards = 1
			cluster.Spec.MembersPerShard = 1
			cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
			if declared {
				cluster.Spec.Databases = []pgshardv1alpha1.DatabaseTemplate{{Name: "app", Shards: 1, Cells: []int32{0}}}
			}
			cluster.Status.PostgreSQLBootstrapSpec = bootstrapSpecStatus(cluster, owned.PostgreSQLRuntimeDirect)
			cluster.Status.PostgreSQLBootstrapSpec.DatabaseTopologySHA256 = ""
			fakeClient := newFakeClient(t, cluster)
			_, err := developmentReconciler(fakeClient, nil).Reconcile(ctx, requestFor(cluster))
			if declared {
				if err == nil || !strings.Contains(err.Error(), "predates the declared database topology") {
					t.Fatalf("declared topology accepted an unbound legacy checkpoint: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			current := getCluster(t, ctx, fakeClient, cluster)
			if current.Status.PostgreSQLBootstrapSpec == nil || current.Status.PostgreSQLBootstrapSpec.DatabaseTopologySHA256 != current.Spec.DatabaseTopologySHA256() {
				t.Fatalf("empty legacy topology checkpoint was not migrated: %#v", current.Status.PostgreSQLBootstrapSpec)
			}
		})
	}
}

func TestReconcileRefusesMissingOrReplacedPostgreSQLDataPVC(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for name, replace := range map[string]bool{"missing": false, "replaced": true} {
		name, replace := name, replace
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cluster := validCluster()
			cluster.Spec.Shards = 1
			cluster.Spec.MembersPerShard = 1
			cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
			fakeClient := newFakeClient(t, cluster)
			reconciler := developmentReconciler(fakeClient, nil)
			if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
				t.Fatal(err)
			}
			current := getCluster(t, ctx, fakeClient, cluster)
			bootstrap := bootstrapForShard(t, current, 0)
			claim := &corev1.PersistentVolumeClaim{}
			key := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}
			if err := fakeClient.Get(ctx, key, claim); err != nil {
				t.Fatal(err)
			}
			claim.Finalizers = nil
			if err := fakeClient.Update(ctx, claim); err != nil {
				t.Fatal(err)
			}
			if err := fakeClient.Delete(ctx, claim); err != nil {
				t.Fatal(err)
			}
			if replace {
				replacement := owned.PostgreSQLDataPVC(cluster, 0, bootstrap.PVCName, cluster.Spec.Storage.Size, bootstrap.PVCStorageClassName, bootstrap.SecretName, bootstrap.SecretUID)
				replacement.UID = "replacement-pvc-uid"
				if err := fakeClient.Create(ctx, replacement); err != nil {
					t.Fatal(err)
				}
			}
			_, err := reconciler.Reconcile(ctx, requestFor(cluster))
			if err == nil || (replace && !strings.Contains(err.Error(), "expected recorded UID")) || (!replace && !strings.Contains(err.Error(), "restore is required")) {
				t.Fatalf("%s data PVC was not fenced: %v", name, err)
			}
		})
	}
}

func TestProtectedPostgreSQLDataPVCReservesItsNameUntilWorkloadPruning(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	fakeClient := newFakeClient(t, cluster)
	reconciler := developmentReconciler(fakeClient, nil)
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	current := getCluster(t, ctx, fakeClient, cluster)
	bootstrap := bootstrapForShard(t, current, 0)
	claim := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}
	if err := fakeClient.Get(ctx, key, claim); err != nil {
		t.Fatal(err)
	}
	if !postgresqlDataPVCIsProtected(claim) || len(claim.OwnerReferences) != 0 {
		t.Fatalf("steady data PVC is not independently protected: %#v", claim.ObjectMeta)
	}
	if err := fakeClient.Delete(ctx, claim); err != nil {
		t.Fatal(err)
	}
	terminating := &corev1.PersistentVolumeClaim{}
	if err := fakeClient.Get(ctx, key, terminating); err != nil || terminating.DeletionTimestamp == nil || terminating.UID != bootstrap.PVCUID {
		t.Fatalf("protected PVC did not reserve its exact name after deletion: claim=%#v error=%v", terminating.ObjectMeta, err)
	}
	replacement := owned.PostgreSQLDataPVC(cluster, 0, bootstrap.PVCName, cluster.Spec.Storage.Size, bootstrap.PVCStorageClassName, bootstrap.SecretName, bootstrap.SecretUID)
	if err := fakeClient.Create(ctx, replacement); !apierrors.IsAlreadyExists(err) {
		t.Fatalf("same-name replacement bypassed protected PVC: %v", err)
	}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err == nil || !strings.Contains(err.Error(), "is deleting; restore is required") {
		t.Fatalf("controller did not fail closed on protected data deletion: %v", err)
	}
}

func TestPostgreSQLDataFenceStabilizationRecoversLostUpdateResponses(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"protect", "detach", "anchor"} {
		stage := stage
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			cluster := validCluster()
			cluster.Spec.Shards = 1
			cluster.Spec.MembersPerShard = 1
			cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
			base := newFakeClient(t, cluster)
			injected := false
			writeClient := interceptedClient(t, base, interceptor.Funcs{
				Update: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, options ...client.UpdateOption) error {
					matches := false
					switch typed := object.(type) {
					case *corev1.PersistentVolumeClaim:
						if stage == "protect" {
							matches = postgresqlDataPVCIsProtected(typed) && len(typed.OwnerReferences) != 0
						} else if stage == "detach" {
							matches = postgresqlDataPVCIsProtected(typed) && len(typed.OwnerReferences) == 0
						}
					case *corev1.Secret:
						matches = stage == "anchor" && len(typed.OwnerReferences) == 1 && typed.OwnerReferences[0].Kind == "PersistentVolumeClaim"
					}
					if matches && !injected {
						injected = true
						if err := kubeClient.Update(ctx, object, options...); err != nil {
							return err
						}
						return apierrors.NewTimeoutError("injected lost stabilization update response", 1)
					}
					return kubeClient.Update(ctx, object, options...)
				},
			})
			reconciler := developmentReconciler(writeClient, base)
			if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err == nil || !strings.Contains(err.Error(), "lost stabilization update response") {
				t.Fatalf("%s update response was not lost: %v", stage, err)
			}
			if !injected {
				t.Fatalf("%s stabilization update was not reached", stage)
			}
			statefulSet := &appsv1.StatefulSet{}
			statefulSetKey := types.NamespacedName{Namespace: cluster.Namespace, Name: owned.PostgreSQLShardStatefulSetName(cluster.Name, 0)}
			if err := base.Get(ctx, statefulSetKey, statefulSet); !apierrors.IsNotFound(err) {
				t.Fatalf("workload was published before %s stabilization became certain: %v", stage, err)
			}
			if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
				t.Fatal(err)
			}
			current := getCluster(t, ctx, base, cluster)
			bootstrap := bootstrapForShard(t, current, 0)
			claim := &corev1.PersistentVolumeClaim{}
			if err := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}, claim); err != nil {
				t.Fatal(err)
			}
			secret := &corev1.Secret{}
			if err := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.SecretName}, secret); err != nil {
				t.Fatal(err)
			}
			if len(claim.OwnerReferences) != 0 || !postgresqlDataPVCIsProtected(claim) || !postgresqlCredentialIsDataAnchored(secret, bootstrap) {
				t.Fatalf("%s recovery did not converge: claim=%#v secret=%#v bootstrap=%#v", stage, claim.ObjectMeta, secret.ObjectMeta, bootstrap)
			}
		})
	}
}

func TestReconcileWaitsForPodFencingAdmissionHandshake(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	base := newFakeClient(t, cluster)
	current := getCluster(t, ctx, base, cluster)
	current.Annotations[podfence.HandshakeChallengeAnnotation] = "forged"
	current.Annotations[podfence.HandshakeReceiptAnnotation] = "forged"
	if err := base.Update(ctx, current); err != nil {
		t.Fatal(err)
	}

	reconciler := developmentReconciler(base, base)
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	current = getCluster(t, ctx, base, cluster)
	if current.Annotations[podfence.HandshakeChallengeAnnotation] == "" || current.Annotations[podfence.HandshakeChallengeAnnotation] == "forged" || current.Annotations[podfence.HandshakeReceiptAnnotation] != "" || contains(current.Finalizers, resourceFinalizer) {
		t.Fatalf("unacknowledged fencing handshake crossed creation barrier: annotations=%#v finalizers=%#v", current.Annotations, current.Finalizers)
	}

	codec := podfence.NewStaticHandshakeCodec([]byte(testPodFencingKey))
	admitted := interceptedClient(t, base, interceptor.Funcs{Patch: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, patch client.Patch, options ...client.PatchOption) error {
		if candidate, ok := object.(*pgshardv1alpha1.PgShardCluster); ok {
			receipt, err := codec.Receipt(ctx, candidate)
			if err != nil {
				return err
			}
			candidate.Annotations[podfence.HandshakeReceiptAnnotation] = receipt
		}
		return kubeClient.Patch(ctx, object, patch, options...)
	}})
	reconciler = developmentReconciler(admitted, base)
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	current = getCluster(t, ctx, base, cluster)
	verified, err := codec.Verify(ctx, current)
	if err != nil {
		t.Fatal(err)
	}
	if !verified || contains(current.Finalizers, resourceFinalizer) {
		t.Fatalf("admission handshake was not durably acknowledged before requeue: annotations=%#v finalizers=%#v", current.Annotations, current.Finalizers)
	}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	current = getCluster(t, ctx, base, cluster)
	if !contains(current.Finalizers, resourceFinalizer) {
		t.Fatalf("acknowledged fencing handshake did not open creation barrier: %#v", current.Finalizers)
	}
}

func TestAgentMultiMemberReconcileWaitsForPodFencingAdmissionHandshake(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	base := newFakeClient(t, cluster)
	current := getCluster(t, ctx, base, cluster)
	current.Annotations[podfence.HandshakeChallengeAnnotation] = "forged"
	current.Annotations[podfence.HandshakeReceiptAnnotation] = "forged"
	if err := base.Update(ctx, current); err != nil {
		t.Fatal(err)
	}

	reconciler := developmentReconciler(base, base)
	reconciler.Images.PostgreSQLRuntime = owned.PostgreSQLRuntimeAgentQuarantine
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	current = getCluster(t, ctx, base, cluster)
	if current.Annotations[podfence.HandshakeChallengeAnnotation] == "" || current.Annotations[podfence.HandshakeChallengeAnnotation] == "forged" || current.Annotations[podfence.HandshakeReceiptAnnotation] != "" || contains(current.Finalizers, resourceFinalizer) || len(current.Status.PostgreSQLBootstraps) != 0 {
		t.Fatalf("unacknowledged multi-member fencing handshake crossed creation barrier: annotations=%#v finalizers=%#v bootstraps=%#v", current.Annotations, current.Finalizers, current.Status.PostgreSQLBootstraps)
	}

	codec := podfence.NewStaticHandshakeCodec([]byte(testPodFencingKey))
	admitted := interceptedClient(t, base, interceptor.Funcs{Patch: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, patch client.Patch, options ...client.PatchOption) error {
		if candidate, ok := object.(*pgshardv1alpha1.PgShardCluster); ok {
			receipt, err := codec.Receipt(ctx, candidate)
			if err != nil {
				return err
			}
			candidate.Annotations[podfence.HandshakeReceiptAnnotation] = receipt
		}
		return kubeClient.Patch(ctx, object, patch, options...)
	}})
	reconciler = developmentReconciler(admitted, base)
	reconciler.Images.PostgreSQLRuntime = owned.PostgreSQLRuntimeAgentQuarantine
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	current = getCluster(t, ctx, base, cluster)
	verified, err := codec.Verify(ctx, current)
	if err != nil {
		t.Fatal(err)
	}
	if !verified || contains(current.Finalizers, resourceFinalizer) || len(current.Status.PostgreSQLBootstraps) != 0 {
		t.Fatalf("acknowledged multi-member handshake crossed requeue barrier: annotations=%#v finalizers=%#v bootstraps=%#v", current.Annotations, current.Finalizers, current.Status.PostgreSQLBootstraps)
	}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	current = getCluster(t, ctx, base, cluster)
	if !contains(current.Finalizers, resourceFinalizer) || len(current.Status.PostgreSQLBootstraps) == 0 {
		t.Fatalf("acknowledged multi-member handshake did not open creation barrier: finalizers=%#v bootstraps=%#v", current.Finalizers, current.Status.PostgreSQLBootstraps)
	}
}

func TestPodFencingHandshakeRecoversReceiptOnlyState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	base := newFakeClient(t, cluster)
	current := getCluster(t, ctx, base, cluster)
	delete(current.Annotations, podfence.HandshakeChallengeAnnotation)
	current.Annotations[podfence.HandshakeReceiptAnnotation] = "v1.forged"
	if err := base.Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	reconciler := developmentReconciler(base, base)
	ready, err := reconciler.ensurePostgreSQLPodFencingHandshake(ctx, current)
	if err != nil {
		t.Fatal(err)
	}
	if ready {
		t.Fatal("receipt-only Pod fencing state was accepted")
	}
	current = getCluster(t, ctx, base, cluster)
	if current.Annotations[podfence.HandshakeChallengeAnnotation] == "" || current.Annotations[podfence.HandshakeReceiptAnnotation] != "" {
		t.Fatalf("receipt-only Pod fencing state was not rotated: %#v", current.Annotations)
	}
}

func TestReconcilePinsDefaultStorageClassBeforeCreateAndSurvivesRotation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	cluster.Spec.Storage.StorageClassName = nil
	olderDefault := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{
		Name:              "older-default",
		CreationTimestamp: metav1.NewTime(time.Unix(100, 0)),
		Annotations: map[string]string{
			"storageclass.kubernetes.io/is-default-class": "true",
		},
	}}
	selectedDefault := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{
		Name:              "selected-default",
		CreationTimestamp: metav1.NewTime(time.Unix(200, 0)),
		Annotations: map[string]string{
			"storageclass.kubernetes.io/is-default-class": "true",
		},
	}}
	fakeClient := newFakeClient(t, cluster, olderDefault, selectedDefault)
	reconciler := developmentReconciler(fakeClient, nil)
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}

	current := getCluster(t, ctx, fakeClient, cluster)
	bootstrap := bootstrapForShard(t, current, 0)
	if bootstrap.PVCUID == "" || bootstrap.PVCStorageClassName == nil || *bootstrap.PVCStorageClassName != selectedDefault.Name {
		t.Fatalf("initial PVC checkpoint = %#v", bootstrap)
	}
	claim := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}
	if err := fakeClient.Get(ctx, key, claim); err != nil {
		t.Fatal(err)
	}
	if claim.Spec.StorageClassName == nil || *claim.Spec.StorageClassName != selectedDefault.Name {
		t.Fatalf("PVC was not created with the resolved class: %#v", claim.Spec.StorageClassName)
	}
	// The durable creation intent, rather than current default annotations,
	// remains authoritative after controller restart and class rotation.
	if err := fakeClient.Delete(ctx, selectedDefault); err != nil {
		t.Fatal(err)
	}
	if _, err := developmentReconciler(fakeClient, nil).Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatalf("restart after default rotation failed: %v", err)
	}

	if err := fakeClient.Get(ctx, key, claim); err != nil {
		t.Fatal(err)
	}
	replacement := "user-selected"
	claim.Spec.StorageClassName = &replacement
	if err := fakeClient.Update(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err == nil || !strings.Contains(err.Error(), "storage class differs from its recorded API value") {
		t.Fatalf("post-create storage-class transition was not fenced: %v", err)
	}
}

func TestReconcileRequiresDefaultOrExplicitStorageClassBeforeCreate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	cluster.Spec.Storage.StorageClassName = nil
	fakeClient := newFakeClient(t, cluster)
	if _, err := developmentReconciler(fakeClient, nil).Reconcile(ctx, requestFor(cluster)); err == nil || !strings.Contains(err.Error(), "no default StorageClass is available") {
		t.Fatalf("missing default StorageClass error = %v", err)
	}
	if current := getCluster(t, ctx, fakeClient, cluster); len(current.Status.PostgreSQLBootstraps) != 0 {
		t.Fatalf("bootstrap intent was published without a resolved class: %#v", current.Status.PostgreSQLBootstraps)
	}

	explicit := validCluster()
	explicit.Name = "explicit-empty"
	explicit.UID = "explicit-empty-uid"
	explicit.Spec.Shards = 1
	explicit.Spec.MembersPerShard = 1
	explicit.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	empty := ""
	explicit.Spec.Storage.StorageClassName = &empty
	explicitClient := newFakeClient(t, explicit)
	if _, err := developmentReconciler(explicitClient, nil).Reconcile(ctx, requestFor(explicit)); err != nil {
		t.Fatal(err)
	}
	bootstrap := bootstrapForShard(t, getCluster(t, ctx, explicitClient, explicit), 0)
	if bootstrap.PVCStorageClassName == nil || *bootstrap.PVCStorageClassName != "" {
		t.Fatalf("explicit empty storage class intent = %#v", bootstrap.PVCStorageClassName)
	}
}

func TestResolvePostgreSQLStorageClassUsesKubernetesTieBreaker(t *testing.T) {
	t.Parallel()
	created := metav1.NewTime(time.Unix(100, 0))
	cluster := validCluster()
	cluster.Spec.Storage.StorageClassName = nil
	reader := newFakeClient(t,
		&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{
			Name:              "zeta",
			CreationTimestamp: created,
			Annotations:       map[string]string{"storageclass.kubernetes.io/is-default-class": "true"},
		}},
		&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{
			Name:              "alpha",
			CreationTimestamp: created,
			Annotations:       map[string]string{"storageclass.beta.kubernetes.io/is-default-class": "true"},
		}},
	)
	selected, err := resolvePostgreSQLStorageClass(context.Background(), reader, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if selected == nil || *selected != "alpha" {
		t.Fatalf("selected default StorageClass = %#v, want alpha", selected)
	}
}

func TestDeletionPolicyRetainsPostgreSQLDataByDefault(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	fakeClient := newFakeClient(t, cluster)
	reconciler := developmentReconciler(fakeClient, fakeClient)
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	current := getCluster(t, ctx, fakeClient, cluster)
	bootstrap := bootstrapForShard(t, current, 0)
	if err := fakeClient.Delete(ctx, current); err != nil {
		t.Fatal(err)
	}
	result, err := reconciler.Reconcile(ctx, requestFor(cluster))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Requeue && result.RequeueAfter == 0 {
		t.Fatalf("retention was not observed in a separate pass: %#v", result)
	}
	for range 8 {
		retained := &corev1.PersistentVolumeClaim{}
		key := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}
		if err := fakeClient.Get(ctx, key, retained); err == nil && retained.Annotations[owned.RetainedFromAnnotation] == cluster.Namespace+"/"+cluster.Name {
			break
		}
		if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); client.IgnoreNotFound(err) != nil {
			t.Fatal(err)
		}
	}
	retained := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}
	if err := fakeClient.Get(ctx, key, retained); err != nil {
		t.Fatal(err)
	}
	if metav1.IsControlledBy(retained, cluster) || retained.Annotations[owned.RetainedFromAnnotation] != cluster.Namespace+"/"+cluster.Name || postgresqlDataPVCIsProtected(retained) {
		t.Fatalf("PostgreSQL data PVC was not safely retained outside garbage collection: %#v", retained.ObjectMeta)
	}
}

func TestRetainWaitsForLatePostgreSQLPodBeforeReleasingData(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	base := newFakeClient(t, cluster)
	reconciler := developmentReconciler(base, base)
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	current := getCluster(t, ctx, base, cluster)
	bootstrap := bootstrapForShard(t, current, 0)
	statefulSet := &appsv1.StatefulSet{}
	statefulSetKey := types.NamespacedName{Namespace: cluster.Namespace, Name: owned.PostgreSQLShardStatefulSetName(cluster.Name, 0)}
	if err := base.Get(ctx, statefulSetKey, statefulSet); err != nil {
		t.Fatal(err)
	}
	if err := base.Delete(ctx, current); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	if err := base.Get(ctx, statefulSetKey, &appsv1.StatefulSet{}); !apierrors.IsNotFound(err) {
		t.Fatalf("PostgreSQL StatefulSet was not absent before the late Pod create: %v", err)
	}

	latePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            statefulSet.Name + "-0",
			Namespace:       cluster.Namespace,
			Labels:          maps.Clone(statefulSet.Spec.Template.Labels),
			Annotations:     maps.Clone(statefulSet.Spec.Template.Annotations),
			Finalizers:      append([]string(nil), statefulSet.Spec.Template.Finalizers...),
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(statefulSet, appsv1.SchemeGroupVersion.WithKind("StatefulSet"))},
		},
		Spec: corev1.PodSpec{NodeName: "node-a", Volumes: []corev1.Volume{
			{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: bootstrap.PVCName}}},
			{Name: "bootstrap-secret", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: bootstrap.SecretName}}},
		}},
	}
	latePod.Annotations[podfence.NodeUIDAnnotation] = "node-uid-a"
	latePod.Annotations[podfence.NodeBootIDAnnotation] = "boot-a"
	if err := base.Create(ctx, latePod); err != nil {
		t.Fatal(err)
	}
	credentialOnlyPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "completed-sql-client", Namespace: cluster.Namespace},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "psql",
			Env: []corev1.EnvVar{{
				Name: "PGPASSWORD",
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: bootstrap.SecretName},
					Key:                  owned.PostgreSQLPasswordKey,
				}},
			}},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
	}
	activeCredentialPod := credentialOnlyPod.DeepCopy()
	activeCredentialPod.Name = "active-sql-client"
	activeCredentialPod.Status.Phase = corev1.PodRunning
	if err := base.Create(ctx, credentialOnlyPod); err != nil {
		t.Fatal(err)
	}
	if err := base.Create(ctx, activeCredentialPod); err != nil {
		t.Fatal(err)
	}
	claimKey := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}
	assertProtected := func(stage string) {
		t.Helper()
		claim := &corev1.PersistentVolumeClaim{}
		if err := base.Get(ctx, claimKey, claim); err != nil {
			t.Fatalf("%s: read PostgreSQL data PVC: %v", stage, err)
		}
		if !postgresqlDataPVCIsProtected(claim) || claim.Annotations[owned.RetainedFromAnnotation] != "" {
			t.Fatalf("%s released PostgreSQL data before the late-Pod barrier: %#v", stage, claim.ObjectMeta)
		}
	}

	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	assertProtected("credential deletion")
	if err := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.SecretName}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("credential tombstone remained before the Pod barrier: %v", err)
	}
	if err := base.Get(ctx, client.ObjectKeyFromObject(latePod), &corev1.Pod{}); err != nil {
		t.Fatalf("late Pod disappeared before its authoritative observation: %v", err)
	}
	if err := base.Get(ctx, client.ObjectKeyFromObject(credentialOnlyPod), &corev1.Pod{}); err != nil {
		t.Fatalf("credential-only Pod was treated as a data-mounting Pod: %v", err)
	}

	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	assertProtected("late Pod deletion")
	terminating := &corev1.Pod{}
	if err := base.Get(ctx, client.ObjectKeyFromObject(latePod), terminating); err != nil {
		t.Fatalf("late PostgreSQL Pod disappeared without a terminal proof: %v", err)
	}
	if terminating.DeletionTimestamp == nil || !controllerutil.ContainsFinalizer(terminating, owned.PostgreSQLPodTerminationFinalizer) {
		t.Fatalf("late PostgreSQL Pod was not held behind its termination fence: %#v", terminating.ObjectMeta)
	}
	for _, credentialPod := range []*corev1.Pod{credentialOnlyPod, activeCredentialPod} {
		if err := base.Get(ctx, client.ObjectKeyFromObject(credentialPod), &corev1.Pod{}); err != nil {
			t.Fatalf("credential-only Pod %s blocked or was deleted by the PGDATA barrier: %v", credentialPod.Name, err)
		}
	}

	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	assertProtected("nonterminal deleting Pod")
	if err := base.Get(ctx, client.ObjectKeyFromObject(latePod), terminating); err != nil || !controllerutil.ContainsFinalizer(terminating, owned.PostgreSQLPodTerminationFinalizer) {
		t.Fatalf("nonterminal PostgreSQL Pod lost its termination fence: pod=%#v err=%v", terminating.ObjectMeta, err)
	}
	terminating.Status.Phase = corev1.PodFailed
	if err := base.Status().Update(ctx, terminating); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	assertProtected("control-plane terminal phase")
	if err := base.Get(ctx, client.ObjectKeyFromObject(latePod), terminating); err != nil || !controllerutil.ContainsFinalizer(terminating, owned.PostgreSQLPodTerminationFinalizer) {
		t.Fatalf("unattested terminal PostgreSQL Pod lost its fence: pod=%#v err=%v", terminating.ObjectMeta, err)
	}
	terminating.Status.Conditions = append(terminating.Status.Conditions, testTerminationAttestation(t, terminating))
	if err := base.Status().Update(ctx, terminating); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	assertProtected("terminal Pod fence release")
	released := &corev1.Pod{}
	if err := base.Get(ctx, client.ObjectKeyFromObject(latePod), released); err == nil {
		if controllerutil.ContainsFinalizer(released, owned.PostgreSQLPodTerminationFinalizer) || !podHasTerminalPhase(released) {
			t.Fatalf("terminal PostgreSQL Pod fence was not released: %#v", released)
		}
		// The fake client does not perform the API server's automatic final
		// deletion after the last finalizer is patched away.
		if err := base.Delete(ctx, released); err != nil {
			t.Fatal(err)
		}
	} else if !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}

	for range 3 {
		if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); client.IgnoreNotFound(err) != nil {
			t.Fatal(err)
		}
	}
	retained := &corev1.PersistentVolumeClaim{}
	if err := base.Get(ctx, claimKey, retained); err != nil {
		t.Fatal(err)
	}
	if postgresqlDataPVCIsProtected(retained) || retained.Annotations[owned.RetainedFromAnnotation] != cluster.Namespace+"/"+cluster.Name {
		t.Fatalf("PostgreSQL data was not retained after both absence barriers: %#v", retained.ObjectMeta)
	}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); client.IgnoreNotFound(err) != nil {
		t.Fatal(err)
	}
	for _, credentialPod := range []*corev1.Pod{credentialOnlyPod, activeCredentialPod} {
		if err := base.Get(ctx, client.ObjectKeyFromObject(credentialPod), &corev1.Pod{}); err != nil {
			t.Fatalf("credential-only Pod %s blocked or was removed by completed finalization: %v", credentialPod.Name, err)
		}
	}
}

func TestPodCreatedAfterFinalAbsenceCannotBindDuringClusterDeletion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	base := newFakeClient(t, cluster)
	reconciler := developmentReconciler(base, base)
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	current := getCluster(t, ctx, base, cluster)
	statefulSet := &appsv1.StatefulSet{}
	statefulSetKey := types.NamespacedName{Namespace: cluster.Namespace, Name: owned.PostgreSQLShardStatefulSetName(cluster.Name, 0)}
	if err := base.Get(ctx, statefulSetKey, statefulSet); err != nil {
		t.Fatal(err)
	}
	if err := base.Delete(ctx, current); err != nil {
		t.Fatal(err)
	}
	deleting := getCluster(t, ctx, base, cluster)
	latePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            statefulSet.Name + "-0",
			Namespace:       cluster.Namespace,
			Labels:          maps.Clone(statefulSet.Spec.Template.Labels),
			Annotations:     maps.Clone(statefulSet.Spec.Template.Annotations),
			Finalizers:      append([]string(nil), statefulSet.Spec.Template.Finalizers...),
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(statefulSet, appsv1.SchemeGroupVersion.WithKind("StatefulSet"))},
		},
		Spec: *statefulSet.Spec.Template.Spec.DeepCopy(),
	}
	reader := &createPodAfterListReader{Reader: base, writer: base, pod: latePod}
	if pending, err := developmentReconciler(base, reader).deletePostgreSQLPodsForFinalization(ctx, deleting); err != nil {
		t.Fatal(err)
	} else if pending {
		t.Fatal("pre-injection Pod snapshot unexpectedly reported a pending Pod")
	}
	if !reader.injected {
		t.Fatal("late Pod was not committed after the authoritative absence snapshot")
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", UID: "node-uid-a"}, Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{BootID: "boot-a"}}}
	if err := base.Create(ctx, node); err != nil {
		t.Fatal(err)
	}
	binding := &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{Name: latePod.Name, Namespace: latePod.Namespace, UID: latePod.UID},
		Target:     corev1.ObjectReference{Kind: "Node", Name: node.Name},
	}
	raw, err := json.Marshal(binding)
	if err != nil {
		t.Fatal(err)
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := pgshardv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	request := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Name: latePod.Name, Namespace: latePod.Namespace, Operation: admissionv1.Create, SubResource: "binding", Object: runtime.RawExtension{Raw: raw},
	}}
	for name, handler := range map[string]admission.Handler{
		"mutating":   podfence.NewBindingAttestor(base, scheme),
		"validating": podfence.NewBindingValidator(base, scheme),
	} {
		response := handler.Handle(ctx, request)
		if response.Allowed || response.Result == nil || !strings.Contains(response.Result.Message, "PgShardCluster is deleting") {
			t.Fatalf("%s binding admitted a Pod committed after the deletion absence snapshot: %#v", name, response)
		}
	}
}

func TestPostgreSQLPodTerminationFenceRequiresAuthenticatedKubeletAttestation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	cluster.Status.PostgreSQLBootstrapSpec = bootstrapSpecStatus(cluster, owned.PostgreSQLRuntimeDirect)
	bootstrap := pgshardv1alpha1.PostgreSQLBootstrapStatus{
		Shard: 0, PVCName: "recorded-data", PVCUID: "recorded-data-uid",
		SecretName: "recorded-secret", SecretUID: "recorded-secret-uid", PVCFenceDetached: true,
		PVCStorageClassName: cluster.Spec.Storage.StorageClassName,
	}
	cluster.Status.PostgreSQLBootstraps = []pgshardv1alpha1.PostgreSQLBootstrapStatus{bootstrap}
	statefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name:      owned.PostgreSQLShardStatefulSetName(cluster.Name, 0),
		Namespace: cluster.Namespace,
		UID:       "statefulset-uid",
	}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              statefulSet.Name + "-0",
			Namespace:         cluster.Namespace,
			UID:               "postgresql-pod-uid",
			DeletionTimestamp: &metav1.Time{Time: time.Unix(100, 0)},
			Finalizers:        []string{owned.PostgreSQLPodTerminationFinalizer},
			Annotations: map[string]string{
				owned.PostgreSQLPodClusterUIDAnnotation: string(cluster.UID),
				podfence.NodeUIDAnnotation:              "node-uid-a",
				podfence.NodeBootIDAnnotation:           "boot-a",
			},
			Labels: map[string]string{
				owned.ClusterLabel: cluster.Name, owned.ComponentLabel: "postgresql", owned.ShardLabel: "0000",
				owned.RoleLabel: "primary", owned.MemberLabel: "0000",
			},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(statefulSet, appsv1.SchemeGroupVersion.WithKind("StatefulSet"))},
		},
		Spec: corev1.PodSpec{NodeName: "node-a", Volumes: []corev1.Volume{{
			Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: bootstrap.PVCName}},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	base := newFakeClient(t, cluster, pod)
	reconciler := developmentReconciler(base, base)

	released, err := reconciler.releaseTerminatedPostgreSQLPodFences(ctx, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if released {
		t.Fatal("nonterminal force-deleted PostgreSQL Pod released its termination fence")
	}
	current := &corev1.Pod{}
	if err := base.Get(ctx, client.ObjectKeyFromObject(pod), current); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(current, owned.PostgreSQLPodTerminationFinalizer) {
		t.Fatalf("nonterminal Pod finalizers = %q", current.Finalizers)
	}

	current.Status.Phase = corev1.PodFailed
	if err := base.Status().Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	released, err = reconciler.releaseTerminatedPostgreSQLPodFences(ctx, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if released {
		t.Fatal("control-plane-authored terminal phase released the PostgreSQL Pod fence")
	}
	if err := base.Get(ctx, client.ObjectKeyFromObject(pod), current); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(current, owned.PostgreSQLPodTerminationFinalizer) {
		t.Fatalf("unattested terminal Pod finalizers = %q", current.Finalizers)
	}
	current.Status.Conditions = append(current.Status.Conditions, testTerminationAttestation(t, current))
	if err := base.Status().Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	released, err = reconciler.releaseTerminatedPostgreSQLPodFences(ctx, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if !released {
		t.Fatal("attested terminal PostgreSQL Pod did not release its termination fence")
	}
	if err := base.Get(ctx, client.ObjectKeyFromObject(pod), current); err == nil {
		if controllerutil.ContainsFinalizer(current, owned.PostgreSQLPodTerminationFinalizer) {
			t.Fatalf("terminal Pod finalizers = %q", current.Finalizers)
		}
	} else if !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
}

func TestDeletionRefusesPostgreSQLPodWithoutTerminationFence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	base := newFakeClient(t, cluster)
	reconciler := developmentReconciler(base, base)
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	current := getCluster(t, ctx, base, cluster)
	bootstrap := bootstrapForShard(t, current, 0)
	statefulSet := &appsv1.StatefulSet{}
	statefulSetKey := types.NamespacedName{Namespace: cluster.Namespace, Name: owned.PostgreSQLShardStatefulSetName(cluster.Name, 0)}
	if err := base.Get(ctx, statefulSetKey, statefulSet); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: statefulSet.Name + "-0", Namespace: cluster.Namespace, Labels: maps.Clone(statefulSet.Spec.Template.Labels),
			Annotations:     maps.Clone(statefulSet.Spec.Template.Annotations),
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(statefulSet, appsv1.SchemeGroupVersion.WithKind("StatefulSet"))},
		},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
			Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: bootstrap.PVCName}},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if err := base.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if err := base.Delete(ctx, current); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err == nil || !strings.Contains(err.Error(), "lacks its termination finalizer") {
		t.Fatalf("deletion accepted an unfenced PostgreSQL Pod: %v", err)
	}
	if err := base.Get(ctx, statefulSetKey, &appsv1.StatefulSet{}); err != nil {
		t.Fatalf("workload pruning began before termination-fence verification: %v", err)
	}
}

func TestFinalizationRefusesUncheckpointedDeletingPVCWithoutCredentialFence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, policy := range []pgshardv1alpha1.StorageDeletionPolicy{pgshardv1alpha1.DeletionRetain, pgshardv1alpha1.DeletionDelete} {
		policy := policy
		t.Run(string(policy), func(t *testing.T) {
			t.Parallel()
			cluster := validCluster()
			cluster.Spec.Storage.DeletionPolicy = policy
			cluster.Status.PostgreSQLBootstrapSpec = bootstrapSpecStatus(cluster, owned.PostgreSQLRuntimeDirect)
			bootstrap := pgshardv1alpha1.PostgreSQLBootstrapStatus{
				Shard: 0, SecretName: "expected-secret", SecretUID: "expected-secret-uid", PVCFenceDetached: true,
				PVCName: "deleting-collision", PVCStorageClassName: cluster.Spec.Storage.StorageClassName,
			}
			cluster.Status.PostgreSQLBootstraps = []pgshardv1alpha1.PostgreSQLBootstrapStatus{bootstrap}
			claim := owned.PostgreSQLDataPVC(cluster, 0, bootstrap.PVCName, cluster.Spec.Storage.Size, bootstrap.PVCStorageClassName, "foreign-secret", "foreign-secret-uid")
			claim.UID = "collision-uid"
			claim.Finalizers = []string{owned.PostgreSQLDataProtectionFinalizer}
			claim.DeletionTimestamp = &metav1.Time{Time: time.Unix(100, 0)}
			base := newFakeClient(t, cluster, claim)
			reconciler := developmentReconciler(base, base)
			var err error
			if policy == pgshardv1alpha1.DeletionRetain {
				_, err = reconciler.retainPostgreSQLPVCs(ctx, cluster)
			} else {
				_, err = reconciler.deletePostgreSQLPVCs(ctx, cluster)
			}
			if err == nil || !strings.Contains(err.Error(), "without its exact credential creation fence") {
				t.Fatalf("uncheckpointed deleting PVC collision was not rejected: %v", err)
			}
			unchanged := &corev1.PersistentVolumeClaim{}
			if err := base.Get(ctx, client.ObjectKeyFromObject(claim), unchanged); err != nil {
				t.Fatal(err)
			}
			if !postgresqlDataPVCIsProtected(unchanged) {
				t.Fatalf("controller removed protection from an uncheckpointed collision: %#v", unchanged.ObjectMeta)
			}
		})
	}
}

func TestRetainFinalizationReleasesExplicitlyDeletingPostgreSQLData(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	fakeClient := newFakeClient(t,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: cluster.Namespace}},
		cluster,
	)
	reconciler := developmentReconciler(fakeClient, fakeClient)
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	current := getCluster(t, ctx, fakeClient, cluster)
	bootstrap := bootstrapForShard(t, current, 0)
	claim := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}
	if err := fakeClient.Get(ctx, key, claim); err != nil {
		t.Fatal(err)
	}
	if err := fakeClient.Delete(ctx, claim); err != nil {
		t.Fatal(err)
	}
	deleting := &corev1.PersistentVolumeClaim{}
	if err := fakeClient.Get(ctx, key, deleting); err != nil {
		t.Fatalf("protected PostgreSQL data PVC did not remain observable after Delete: %v", err)
	}
	if deleting.UID != bootstrap.PVCUID || deleting.DeletionTimestamp == nil || !postgresqlDataPVCIsProtected(deleting) {
		t.Fatalf("explicit Delete did not block on the exact protected PostgreSQL data PVC: metadata=%#v checkpoint=%s", deleting.ObjectMeta, bootstrap.PVCUID)
	}
	if err := fakeClient.Delete(ctx, current); err != nil {
		t.Fatal(err)
	}
	for range 16 {
		if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); client.IgnoreNotFound(err) != nil {
			t.Fatal(err)
		}
	}
	if err := fakeClient.Get(ctx, requestFor(cluster).NamespacedName, &pgshardv1alpha1.PgShardCluster{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Retain finalizer deadlocked behind an explicitly deleting PVC: %v", err)
	}
	if err := fakeClient.Get(ctx, key, &corev1.PersistentVolumeClaim{}); !apierrors.IsNotFound(err) {
		t.Fatalf("explicitly deleting PostgreSQL data PVC survived Retain finalization: %v", err)
	}
}

func TestExplicitDeletePolicyDeletesPostgreSQLData(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	cluster.Spec.Storage.DeletionPolicy = pgshardv1alpha1.DeletionDelete
	fakeClient := newFakeClient(t, cluster)
	reconciler := developmentReconciler(fakeClient, fakeClient)
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	current := getCluster(t, ctx, fakeClient, cluster)
	bootstrap := bootstrapForShard(t, current, 0)
	claim := &corev1.PersistentVolumeClaim{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}, claim); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.SecretName}, secret); err != nil {
		t.Fatal(err)
	}
	if len(claim.OwnerReferences) != 0 || !postgresqlDataPVCIsProtected(claim) || !postgresqlCredentialIsDataAnchored(secret, bootstrap) {
		t.Fatalf("Delete-policy data fence was not stabilized: claim=%#v secret=%#v", claim.ObjectMeta, secret.ObjectMeta)
	}
	if err := fakeClient.Delete(ctx, current); err != nil {
		t.Fatal(err)
	}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}
	for range 4 {
		if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); client.IgnoreNotFound(err) != nil {
			t.Fatal(err)
		}
		if err := fakeClient.Get(ctx, key, &corev1.PersistentVolumeClaim{}); apierrors.IsNotFound(err) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if err := fakeClient.Get(ctx, key, &corev1.PersistentVolumeClaim{}); !apierrors.IsNotFound(err) {
		t.Fatalf("explicit Delete policy retained PostgreSQL data: %v", err)
	}
}

func TestFinalizationDeletesLatePVCWithRecordedCredentialFence(t *testing.T) {
	t.Parallel()
	for _, policy := range []pgshardv1alpha1.StorageDeletionPolicy{pgshardv1alpha1.DeletionRetain, pgshardv1alpha1.DeletionDelete} {
		policy := policy
		t.Run(string(policy), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			cluster := validCluster()
			cluster.Spec.Shards = 1
			cluster.Spec.MembersPerShard = 1
			cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
			cluster.Spec.Storage.DeletionPolicy = policy
			base := newFakeClient(t, cluster)
			reconciler := developmentReconciler(base, base)
			if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
				t.Fatal(err)
			}
			current := getCluster(t, ctx, base, cluster)
			bootstrap := bootstrapForShard(t, current, 0)
			original := &corev1.PersistentVolumeClaim{}
			key := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}
			if err := base.Get(ctx, key, original); err != nil {
				t.Fatal(err)
			}
			original.Finalizers = nil
			if err := base.Update(ctx, original); err != nil {
				t.Fatal(err)
			}
			if err := base.Delete(ctx, original); err != nil {
				t.Fatal(err)
			}
			late := owned.PostgreSQLDataPVC(cluster, 0, bootstrap.PVCName, cluster.Spec.Storage.Size, bootstrap.PVCStorageClassName, bootstrap.SecretName, bootstrap.SecretUID)
			late.UID = "late-pvc-uid"
			if err := base.Create(ctx, late); err != nil {
				t.Fatal(err)
			}
			if err := base.Delete(ctx, current); err != nil {
				t.Fatal(err)
			}
			for range 16 {
				if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); client.IgnoreNotFound(err) != nil {
					t.Fatal(err)
				}
				if err := base.Get(ctx, requestFor(cluster).NamespacedName, &pgshardv1alpha1.PgShardCluster{}); apierrors.IsNotFound(err) {
					break
				}
			}
			if err := base.Get(ctx, key, &corev1.PersistentVolumeClaim{}); !apierrors.IsNotFound(err) {
				t.Fatalf("late creation-fenced PVC survived finalization: %v", err)
			}
			if err := base.Get(ctx, requestFor(cluster).NamespacedName, &pgshardv1alpha1.PgShardCluster{}); !apierrors.IsNotFound(err) {
				t.Fatalf("late PVC wedged cluster finalization: %v", err)
			}
		})
	}
}

func TestFinalizationDeletesLateClusterOwnedCredential(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	base := newFakeClient(t, cluster)
	reconciler := developmentReconciler(base, base)
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	current := getCluster(t, ctx, base, cluster)
	bootstrap := bootstrapForShard(t, current, 0)
	secretKey := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.SecretName}
	expected := &corev1.Secret{}
	if err := base.Get(ctx, secretKey, expected); err != nil {
		t.Fatal(err)
	}
	if err := base.Delete(ctx, expected); err != nil {
		t.Fatal(err)
	}
	late := owned.PostgreSQLAuthSecret(cluster, 0, bootstrap.SecretName, []byte(strings.Repeat("a", hex.EncodedLen(postgresqlPasswordBytes))))
	late.UID = "late-secret-uid"
	if err := base.Create(ctx, late); err != nil {
		t.Fatal(err)
	}
	if err := base.Delete(ctx, current); err != nil {
		t.Fatal(err)
	}
	for range 16 {
		if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); client.IgnoreNotFound(err) != nil {
			t.Fatal(err)
		}
		if err := base.Get(ctx, requestFor(cluster).NamespacedName, &pgshardv1alpha1.PgShardCluster{}); apierrors.IsNotFound(err) {
			break
		}
	}
	if err := base.Get(ctx, secretKey, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("late cluster-owned credential survived finalization: %v", err)
	}
	if err := base.Get(ctx, requestFor(cluster).NamespacedName, &pgshardv1alpha1.PgShardCluster{}); !apierrors.IsNotFound(err) {
		t.Fatalf("late credential wedged cluster finalization: %v", err)
	}
}

func TestDeletionPoliciesResolveUnknownCredentialCreateBeforeFinalization(t *testing.T) {
	t.Parallel()
	for _, policy := range []pgshardv1alpha1.StorageDeletionPolicy{pgshardv1alpha1.DeletionRetain, pgshardv1alpha1.DeletionDelete} {
		for _, committed := range []bool{false, true} {
			policy, committed := policy, committed
			name := fmt.Sprintf("%s/committed=%t", policy, committed)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				ctx := context.Background()
				cluster := validCluster()
				cluster.Spec.Shards = 1
				cluster.Spec.MembersPerShard = 1
				cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
				cluster.Spec.Storage.DeletionPolicy = policy
				base := newFakeClient(t, cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: cluster.Namespace}})
				var delayed *corev1.Secret
				pvcCreates := 0
				writeClient := interceptedClient(t, base, interceptor.Funcs{
					Create: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, options ...client.CreateOption) error {
						switch child := object.(type) {
						case *corev1.Secret:
							delayed = child.DeepCopy()
							if committed {
								if err := kubeClient.Create(ctx, object, options...); err != nil {
									return err
								}
							}
							return apierrors.NewTimeoutError("injected unknown credential create", 1)
						case *corev1.PersistentVolumeClaim:
							pvcCreates++
						}
						return kubeClient.Create(ctx, object, options...)
					},
				})
				reconciler := developmentReconciler(writeClient, base)
				if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err == nil || !strings.Contains(err.Error(), "unknown credential create") {
					t.Fatalf("credential create outcome was not left unknown: %v", err)
				}
				current := getCluster(t, ctx, base, cluster)
				bootstrap := bootstrapForShard(t, current, 0)
				if bootstrap.SecretUID != "" || bootstrap.PVCFenceDetached || pvcCreates != 0 {
					t.Fatalf("data creation advanced past unknown credential: bootstrap=%#v pvcCreates=%d", bootstrap, pvcCreates)
				}
				if err := base.Delete(ctx, current); err != nil {
					t.Fatal(err)
				}
				for range 8 {
					if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); client.IgnoreNotFound(err) != nil {
						t.Fatal(err)
					}
					if err := base.Get(ctx, requestFor(cluster).NamespacedName, &pgshardv1alpha1.PgShardCluster{}); apierrors.IsNotFound(err) {
						break
					}
				}
				if err := base.Get(ctx, requestFor(cluster).NamespacedName, &pgshardv1alpha1.PgShardCluster{}); !apierrors.IsNotFound(err) {
					t.Fatalf("cluster did not finalize unknown credential create: %v", err)
				}
				secretKey := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.SecretName}
				if err := base.Get(ctx, secretKey, &corev1.Secret{}); !apierrors.IsNotFound(err) {
					t.Fatalf("credential survived finalization: %v", err)
				}
				if !committed {
					if delayed == nil || !postgresqlCredentialIsClusterFenced(delayed, cluster) {
						t.Fatalf("late credential create lacks the deleted cluster fence: %#v", delayed)
					}
					if err := base.Create(ctx, delayed); err != nil {
						t.Fatalf("materialize late credential create: %v", err)
					}
				}
			})
		}
	}
}

func TestDeletionPoliciesFenceDelayedPVCOutcomeWithoutRecreating(t *testing.T) {
	t.Parallel()
	for _, policy := range []pgshardv1alpha1.StorageDeletionPolicy{pgshardv1alpha1.DeletionRetain, pgshardv1alpha1.DeletionDelete} {
		policy := policy
		t.Run(string(policy), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			cluster := validCluster()
			cluster.Spec.Shards = 1
			cluster.Spec.MembersPerShard = 1
			cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
			cluster.Spec.Storage.DeletionPolicy = policy
			namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: cluster.Namespace}}
			base := newFakeClient(t, cluster, namespace)
			var delayed *corev1.PersistentVolumeClaim
			createAttempts := 0
			writeClient := interceptedClient(t, base, interceptor.Funcs{
				Create: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, options ...client.CreateOption) error {
					claim, isClaim := object.(*corev1.PersistentVolumeClaim)
					if !isClaim {
						return kubeClient.Create(ctx, object, options...)
					}
					createAttempts++
					if createAttempts == 1 {
						delayed = claim.DeepCopy()
						return apierrors.NewTimeoutError("injected outcome-unknown PVC create", 1)
					}
					return kubeClient.Create(ctx, object, options...)
				},
			})
			reconciler := developmentReconciler(writeClient, base)
			if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err == nil || !strings.Contains(err.Error(), "outcome-unknown PVC create") {
				t.Fatalf("initial create did not preserve its unknown outcome: %v", err)
			}

			current := getCluster(t, ctx, base, cluster)
			bootstrap := bootstrapForShard(t, current, 0)
			if bootstrap.PVCUID != "" {
				t.Fatalf("unknown create was checkpointed as complete: %#v", bootstrap)
			}
			key := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}
			if err := base.Get(ctx, key, &corev1.PersistentVolumeClaim{}); !apierrors.IsNotFound(err) {
				t.Fatalf("delayed PVC became visible before deletion: %v", err)
			}
			if err := base.Delete(ctx, current); err != nil {
				t.Fatal(err)
			}

			for range 12 {
				if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); client.IgnoreNotFound(err) != nil {
					t.Fatal(err)
				}
				if err := base.Get(ctx, requestFor(cluster).NamespacedName, &pgshardv1alpha1.PgShardCluster{}); apierrors.IsNotFound(err) {
					break
				}
			}
			if createAttempts != 1 {
				t.Fatalf("PVC create attempts = %d, want only the original delayed create", createAttempts)
			}
			if err := base.Get(ctx, requestFor(cluster).NamespacedName, &pgshardv1alpha1.PgShardCluster{}); !apierrors.IsNotFound(err) {
				t.Fatalf("cluster did not finish deletion after fencing the delayed PVC create: %v", err)
			}
			if err := base.Get(ctx, key, &corev1.PersistentVolumeClaim{}); !apierrors.IsNotFound(err) {
				t.Fatalf("finalization created replacement storage for an absent outcome: %v", err)
			}
			if err := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.SecretName}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
				t.Fatalf("credential creation fence survived finalization: %v", err)
			}

			// Model the original timed-out request reaching storage only after
			// finalization. The fake client has no garbage collector, so assert the
			// late object still carries the deleted Secret's exact GC fence.
			if delayed == nil {
				t.Fatal("delayed PVC create was not captured")
			}
			if err := base.Create(ctx, delayed); err != nil {
				t.Fatalf("materialize original delayed create: %v", err)
			}
			late := &corev1.PersistentVolumeClaim{}
			if err := base.Get(ctx, key, late); err != nil {
				t.Fatal(err)
			}
			if !postgresqlDataPVCIsCreationFenced(late, bootstrap) {
				t.Fatalf("late PVC create escaped the deleted-Secret GC fence: %#v", late.OwnerReferences)
			}
		})
	}
}

func TestDeletionPoliciesDiscardLateCommittedPVCCreateAfterAbsentObservation(t *testing.T) {
	t.Parallel()
	for _, policy := range []pgshardv1alpha1.StorageDeletionPolicy{pgshardv1alpha1.DeletionRetain, pgshardv1alpha1.DeletionDelete} {
		policy := policy
		t.Run(string(policy), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			cluster := validCluster()
			cluster.Spec.Shards = 1
			cluster.Spec.MembersPerShard = 1
			cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
			cluster.Spec.Storage.DeletionPolicy = policy
			namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: cluster.Namespace}}
			base := newFakeClient(t, cluster, namespace)
			createAttempts := 0
			writeClient := interceptedClient(t, base, interceptor.Funcs{
				Create: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, options ...client.CreateOption) error {
					if _, isClaim := object.(*corev1.PersistentVolumeClaim); !isClaim {
						return kubeClient.Create(ctx, object, options...)
					}
					createAttempts++
					if createAttempts == 1 {
						if err := kubeClient.Create(ctx, object, options...); err != nil {
							return err
						}
						return apierrors.NewTimeoutError("injected lost PVC create response", 1)
					}
					return kubeClient.Create(ctx, object, options...)
				},
			})
			hideCommittedClaim := false
			hiddenReads := 0
			reader := interceptedClient(t, base, interceptor.Funcs{
				Get: func(ctx context.Context, kubeClient client.WithWatch, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
					if hideCommittedClaim && hiddenReads < 1 {
						if _, isClaim := object.(*corev1.PersistentVolumeClaim); isClaim {
							hiddenReads++
							return apierrors.NewNotFound(schema.GroupResource{Resource: "persistentvolumeclaims"}, key.Name)
						}
					}
					return kubeClient.Get(ctx, key, object, options...)
				},
			})
			reconciler := developmentReconciler(writeClient, reader)
			if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err == nil || !strings.Contains(err.Error(), "lost PVC create response") {
				t.Fatalf("committed create did not lose its response: %v", err)
			}

			current := getCluster(t, ctx, base, cluster)
			bootstrap := bootstrapForShard(t, current, 0)
			if bootstrap.PVCUID != "" {
				t.Fatalf("lost response was checkpointed as complete: %#v", bootstrap)
			}
			if err := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}, &corev1.PersistentVolumeClaim{}); err != nil {
				t.Fatalf("committed PVC is not visible: %v", err)
			}
			if err := base.Delete(ctx, current); err != nil {
				t.Fatal(err)
			}
			hideCommittedClaim = true
			for range 12 {
				if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); client.IgnoreNotFound(err) != nil {
					t.Fatal(err)
				}
				if err := base.Get(ctx, requestFor(cluster).NamespacedName, &pgshardv1alpha1.PgShardCluster{}); apierrors.IsNotFound(err) {
					break
				}
			}
			if hiddenReads != 1 || createAttempts != 1 {
				t.Fatalf("committed create resolution: hidden reads=%d attempts=%d, want one absent observation and no recreate", hiddenReads, createAttempts)
			}
			if err := base.Get(ctx, requestFor(cluster).NamespacedName, &pgshardv1alpha1.PgShardCluster{}); !apierrors.IsNotFound(err) {
				t.Fatalf("cluster did not finalize after committed-create resolution: %v", err)
			}
			key := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}
			if err := base.Get(ctx, key, &corev1.PersistentVolumeClaim{}); !apierrors.IsNotFound(err) {
				t.Fatalf("late committed PVC was retained or recreated after an authoritative absent observation: %v", err)
			}
		})
	}
}

func TestRetainDoesNotRecreateExplicitlyDeletedPVCBeforeUIDCheckpoint(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	base := newFakeClient(t, cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: cluster.Namespace}})
	createAttempts := 0
	abandonmentCheckpointed := false
	injected := errors.New("injected PVC UID checkpoint failure")
	writeClient := interceptedClient(t, base, interceptor.Funcs{
		Create: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, options ...client.CreateOption) error {
			if _, isClaim := object.(*corev1.PersistentVolumeClaim); isClaim {
				createAttempts++
			}
			return kubeClient.Create(ctx, object, options...)
		},
		SubResourceUpdate: func(ctx context.Context, kubeClient client.Client, subresource string, object client.Object, options ...client.SubResourceUpdateOption) error {
			current, ok := object.(*pgshardv1alpha1.PgShardCluster)
			abandoning := false
			if subresource == "status" && ok && len(current.Status.PostgreSQLBootstraps) == 1 {
				bootstrap := current.Status.PostgreSQLBootstraps[0]
				if bootstrap.PVCUID != "" {
					return injected
				}
				abandoning = bootstrap.PVCCreationAbandoned
			}
			err := kubeClient.SubResource(subresource).Update(ctx, object, options...)
			if err == nil && abandoning {
				abandonmentCheckpointed = true
			}
			return err
		},
	})
	reconciler := developmentReconciler(writeClient, base)
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err == nil || !strings.Contains(err.Error(), injected.Error()) {
		t.Fatalf("PVC UID checkpoint failure was not surfaced: %v", err)
	}

	current := getCluster(t, ctx, base, cluster)
	bootstrap := bootstrapForShard(t, current, 0)
	if bootstrap.PVCUID != "" || bootstrap.PVCCreationAbandoned {
		t.Fatalf("failed PVC UID checkpoint changed durable outcome state: %#v", bootstrap)
	}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}
	claim := &corev1.PersistentVolumeClaim{}
	if err := base.Get(ctx, key, claim); err != nil {
		t.Fatal(err)
	}
	if !postgresqlDataPVCIsCreationFenced(claim, bootstrap) || postgresqlDataPVCIsProtected(claim) {
		t.Fatalf("pre-checkpoint PostgreSQL data PVC has the wrong lifecycle fence: %#v", claim.ObjectMeta)
	}
	if err := base.Delete(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := base.Get(ctx, key, &corev1.PersistentVolumeClaim{}); !apierrors.IsNotFound(err) {
		t.Fatalf("explicit pre-checkpoint PVC deletion was not authoritative: %v", err)
	}
	if err := base.Delete(ctx, current); err != nil {
		t.Fatal(err)
	}
	for range 16 {
		if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); client.IgnoreNotFound(err) != nil {
			t.Fatal(err)
		}
		if err := base.Get(ctx, requestFor(cluster).NamespacedName, &pgshardv1alpha1.PgShardCluster{}); apierrors.IsNotFound(err) {
			break
		}
	}
	if createAttempts != 1 || !abandonmentCheckpointed {
		t.Fatalf("Retain finalization attempts=%d abandonmentCheckpointed=%t, want one create and a durable abandonment", createAttempts, abandonmentCheckpointed)
	}
	if err := base.Get(ctx, requestFor(cluster).NamespacedName, &pgshardv1alpha1.PgShardCluster{}); !apierrors.IsNotFound(err) {
		t.Fatalf("cluster did not finalize after abandoning the deleted pre-checkpoint PVC: %v", err)
	}
	if err := base.Get(ctx, key, &corev1.PersistentVolumeClaim{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Retain recreated or retained replacement storage after explicit deletion: %v", err)
	}
	if err := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.SecretName}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("credential creation fence survived abandoned PVC finalization: %v", err)
	}
}

func TestFinalizationDoesNotCreateDataBeforeCredentialCheckpoint(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, policy := range []pgshardv1alpha1.StorageDeletionPolicy{pgshardv1alpha1.DeletionRetain, pgshardv1alpha1.DeletionDelete} {
		policy := policy
		t.Run(string(policy), func(t *testing.T) {
			t.Parallel()
			cluster := validCluster()
			cluster.Spec.Storage.DeletionPolicy = policy
			cluster.Status.PostgreSQLBootstrapSpec = bootstrapSpecStatus(cluster, owned.PostgreSQLRuntimeDirect)
			cluster.Status.PostgreSQLBootstraps = []pgshardv1alpha1.PostgreSQLBootstrapStatus{{
				Shard: 0, SecretName: "intent-secret", PVCName: "intent-data", PVCStorageClassName: cluster.Spec.Storage.StorageClassName,
			}}
			base := newFakeClient(t, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: cluster.Namespace}})
			createAttempts := 0
			writeClient := interceptedClient(t, base, interceptor.Funcs{
				Create: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, options ...client.CreateOption) error {
					if _, ok := object.(*corev1.PersistentVolumeClaim); ok {
						createAttempts++
					}
					return kubeClient.Create(ctx, object, options...)
				},
			})
			reconciler := developmentReconciler(writeClient, base)
			var err error
			if policy == pgshardv1alpha1.DeletionRetain {
				_, err = reconciler.retainPostgreSQLPVCs(ctx, cluster)
			} else {
				_, err = reconciler.deletePostgreSQLPVCs(ctx, cluster)
			}
			if err != nil {
				t.Fatal(err)
			}
			if createAttempts != 0 {
				t.Fatalf("PVC creates before credential checkpoint = %d", createAttempts)
			}
		})
	}
}

func TestNamespaceUnavailableForCreateRecognizesAbsenceBarrier(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	active := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "active"}}
	terminating := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:              "terminating",
		DeletionTimestamp: &metav1.Time{Time: time.Unix(100, 0)},
		Finalizers:        []string{"test.example/finalizer"},
	}}
	reader := newFakeClient(t, active, terminating)
	for _, test := range []struct {
		name        string
		unavailable bool
	}{
		{name: "active", unavailable: false},
		{name: "terminating", unavailable: true},
		{name: "absent", unavailable: true},
	} {
		unavailable, err := namespaceUnavailableForCreate(ctx, reader, test.name)
		if err != nil {
			t.Fatalf("namespace %s: %v", test.name, err)
		}
		if unavailable != test.unavailable {
			t.Errorf("namespace %s unavailable = %t, want %t", test.name, unavailable, test.unavailable)
		}
	}
}

func TestDeletePolicyPrunesWorkloadBeforeDataClaim(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	cluster.Spec.Storage.DeletionPolicy = pgshardv1alpha1.DeletionDelete
	base := newFakeClient(t, cluster)
	if _, err := developmentReconciler(base, base).Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	current := getCluster(t, ctx, base, cluster)
	bootstrap := bootstrapForShard(t, current, 0)
	if err := base.Delete(ctx, current); err != nil {
		t.Fatal(err)
	}

	writeClient := interceptedClient(t, base, interceptor.Funcs{
		Delete: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, options ...client.DeleteOption) error {
			claim, ok := object.(*corev1.PersistentVolumeClaim)
			if ok && claim.Name == bootstrap.PVCName {
				// Model pvc-protection holding the exact data claim after a
				// successful delete request.
				return nil
			}
			return kubeClient.Delete(ctx, object, options...)
		},
	})
	reconciler := developmentReconciler(writeClient, base)
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	statefulSet := &appsv1.StatefulSet{}
	statefulSetKey := types.NamespacedName{Namespace: cluster.Namespace, Name: owned.PostgreSQLShardStatefulSetName(cluster.Name, 0)}
	if err := base.Get(ctx, statefulSetKey, statefulSet); !apierrors.IsNotFound(err) {
		t.Fatalf("Delete policy requested data deletion before pruning its StatefulSet: %v", err)
	}
	if err := base.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}, &corev1.PersistentVolumeClaim{}); err != nil {
		t.Fatalf("pvc-protection fixture did not retain the data claim: %v", err)
	}
}

func TestDeletionUsesCheckpointedPolicyWhenAdmissionIsBypassed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	cluster.Spec.Storage.DeletionPolicy = pgshardv1alpha1.DeletionRetain
	fakeClient := newFakeClient(t, cluster)
	reconciler := developmentReconciler(fakeClient, fakeClient)
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	current := getCluster(t, ctx, fakeClient, cluster)
	bootstrap := bootstrapForShard(t, current, 0)
	if current.Status.PostgreSQLBootstrapSpec == nil || current.Status.PostgreSQLBootstrapSpec.DeletionPolicy != pgshardv1alpha1.DeletionRetain {
		t.Fatalf("provisioned deletion policy = %#v", current.Status.PostgreSQLBootstrapSpec)
	}
	// Simulate an API client that bypasses both CEL and webhook update checks.
	current.Spec.Storage.DeletionPolicy = pgshardv1alpha1.DeletionDelete
	if err := fakeClient.Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	if err := fakeClient.Delete(ctx, current); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	claim := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}
	if err := fakeClient.Get(ctx, key, claim); err != nil {
		t.Fatalf("bypassed Retain-to-Delete mutation destroyed PostgreSQL data: %v", err)
	}
	for range 8 {
		if claim.Annotations[owned.RetainedFromAnnotation] == cluster.Namespace+"/"+cluster.Name {
			break
		}
		if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); client.IgnoreNotFound(err) != nil {
			t.Fatal(err)
		}
		if err := fakeClient.Get(ctx, key, claim); err != nil {
			t.Fatal(err)
		}
	}
	if claim.UID != bootstrap.PVCUID || claim.Annotations[owned.RetainedFromAnnotation] != cluster.Namespace+"/"+cluster.Name || postgresqlDataPVCIsProtected(claim) {
		t.Fatalf("retained PostgreSQL data identity = %#v, want UID %s", claim.ObjectMeta, bootstrap.PVCUID)
	}
}

func TestReconcileObservesSupportingAvailabilityWithoutClaimingDatabaseReady(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	fakeClient := newFakeClient(t, cluster)
	reconciler := developmentReconciler(fakeClient, nil)
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}

	markSupportingWorkloadsAvailable(t, ctx, fakeClient, cluster)

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

func markSupportingWorkloadsAvailable(t *testing.T, ctx context.Context, kubeClient client.Client, cluster *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	for name, replicas := range map[string]int32{cluster.Name + owned.OrchestratorSuffix: 3, cluster.Name + owned.PoolerSuffix: 2} {
		deployment := &appsv1.Deployment{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, deployment); err != nil {
			t.Fatal(err)
		}
		deployment.Status.ObservedGeneration = deployment.Generation
		deployment.Status.AvailableReplicas = replicas
		deployment.Status.UpdatedReplicas = replicas
		if err := kubeClient.Status().Update(ctx, deployment); err != nil {
			t.Fatal(err)
		}
	}
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + owned.PoolerSuffix}, hpa); err != nil {
		t.Fatal(err)
	}
	hpa.Status.ObservedGeneration = &hpa.Generation
	hpa.Status.CurrentReplicas = 2
	hpa.Status.DesiredReplicas = 2
	hpa.Status.Conditions = []autoscalingv2.HorizontalPodAutoscalerCondition{
		{Type: autoscalingv2.AbleToScale, Status: corev1.ConditionTrue},
		{Type: autoscalingv2.ScalingActive, Status: corev1.ConditionTrue},
	}
	if err := kubeClient.Status().Update(ctx, hpa); err != nil {
		t.Fatal(err)
	}
}

func TestReconcilePrunesResourcesRemovedByUpdate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	fakeClient := newFakeClient(t, cluster)
	reconciler := developmentReconciler(fakeClient, nil)
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
	obsolete := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name:      "example-obsolete",
		Namespace: cluster.Namespace,
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(
			cluster,
			pgshardv1alpha1.GroupVersion.WithKind("PgShardCluster"),
		)},
	}}
	if err := fakeClient.Create(ctx, obsolete); err != nil {
		t.Fatal(err)
	}

	current := getCluster(t, ctx, fakeClient, cluster)
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
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: obsolete.Name}, &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
		t.Fatalf("stale owned resource was not pruned after scaling handoff: %v", err)
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
	reconciler := developmentReconciler(fakeClient, nil)
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
	reconciler := developmentReconciler(writeClient, authoritative)
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
	reconciler := developmentReconciler(writeClient, newFakeClient(t, current))
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
	reconciler := developmentReconciler(newFakeClient(t), authoritative)
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
	reconciler := developmentReconciler(writeClient, authoritative)
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
	reconciler := developmentReconciler(writeClient, authoritative)
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
	reconciler := developmentReconciler(writeClient, authoritative)
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
	reconciler := developmentReconciler(writeClient, authoritative)
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
	reconciler := developmentReconciler(newFakeClient(t), newFakeClient(t, replacement))
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
	reconciler = developmentReconciler(writeClient, newFakeClient(t, current))
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
	reconciler := developmentReconciler(writeClient, authoritative)
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
	reconciler := developmentReconciler(writeClient, authoritative)
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
	reconciler := developmentReconciler(writeClient, newFakeClient(t, current))
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
	reconciler := developmentReconciler(writeClient, newFakeClient(t, current))
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
	reconciler := developmentReconciler(writeClient, authoritative)
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
	reconciler := developmentReconciler(writeClient, authoritative)
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
	reconciler := developmentReconciler(writeClient, authoritative)
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
	reconciler := developmentReconciler(writeClient, base)
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
	reconciler := developmentReconciler(writeClient, base)
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
	reconciler := developmentReconciler(writeClient, base)
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
	reconciler := developmentReconciler(writeClient, nil)
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
	reconciler := developmentReconciler(fakeClient, nil)
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
	configurations := &corev1.ConfigMapList{}
	if err := fakeClient.List(ctx, configurations, client.InNamespace(cluster.Namespace)); err != nil {
		t.Fatal(err)
	}
	for _, configuration := range configurations.Items {
		if strings.HasPrefix(configuration.Name, cluster.Name+owned.PostgreSQLConfigSuffix+"-") {
			t.Fatalf("plan wrote %s before discovering the collision", configuration.Name)
		}
	}
	status := getCluster(t, ctx, fakeClient, cluster)
	assertCondition(t, status, reconciledCondition, metav1.ConditionFalse, "ReconcileFailed")
	if !contains(status.Finalizers, resourceFinalizer) {
		t.Fatalf("collision failure did not retain the cleanup finalizer: %#v", status.Finalizers)
	}
}

func TestReconcileLeavesHPAOwnedReplicasAndServiceAllocationsAlone(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	fakeClient := newFakeClient(t, cluster)
	reconciler := developmentReconciler(fakeClient, nil)
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
	reconciler := developmentReconciler(fakeClient, nil)
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	if err := fakeClient.Get(ctx, client.ObjectKeyFromObject(unowned), &corev1.ConfigMap{}); err != nil {
		t.Fatalf("unowned label-matched object was deleted: %v", err)
	}
}

func TestPostgreSQLConfigurationRetentionTracksStatefulSetRollout(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	oldConfiguration := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name:      cluster.Name + owned.PostgreSQLConfigSuffix + "-old",
		Namespace: cluster.Namespace,
	}}
	newConfigurationName := cluster.Name + owned.PostgreSQLConfigSuffix + "-new"
	replicas := int32(1)
	workload := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "postgresql-primary",
			Namespace:       cluster.Namespace,
			Generation:      2,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cluster, pgshardv1alpha1.GroupVersion.WithKind("PgShardCluster"))},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:       &replicas,
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType},
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
				Name: "postgresql-config",
				VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: newConfigurationName},
				}},
			}}}},
		},
		Status: appsv1.StatefulSetStatus{
			ObservedGeneration: 1,
			Replicas:           1,
			UpdatedReplicas:    1,
			CurrentRevision:    "revision-old",
			UpdateRevision:     "revision-new",
		},
	}

	if !retainPostgreSQLConfigurationDuringRollout(cluster, oldConfiguration, []client.Object{workload}) {
		t.Fatal("stale controller observation did not retain the previous PostgreSQL configuration")
	}

	complete := workload.DeepCopy()
	complete.Status.ObservedGeneration = complete.Generation
	if retainPostgreSQLConfigurationDuringRollout(cluster, oldConfiguration, []client.Object{complete}) {
		t.Fatal("completed rollout retained an unreferenced PostgreSQL configuration")
	}
	partiallyUpdated := complete.DeepCopy()
	partiallyUpdated.Status.UpdatedReplicas = 0
	if !retainPostgreSQLConfigurationDuringRollout(cluster, oldConfiguration, []client.Object{partiallyUpdated}) {
		t.Fatal("partial OnDelete rollout did not retain the previous PostgreSQL configuration")
	}

	stillReferenced := complete.DeepCopy()
	stillReferenced.Spec.Template.Spec.Volumes[0].ConfigMap.Name = oldConfiguration.Name
	if !retainPostgreSQLConfigurationDuringRollout(cluster, oldConfiguration, []client.Object{stillReferenced}) {
		t.Fatal("completed workload template did not retain the PostgreSQL configuration it still references")
	}

	unowned := workload.DeepCopy()
	unowned.OwnerReferences = nil
	if retainPostgreSQLConfigurationDuringRollout(cluster, oldConfiguration, []client.Object{unowned}) {
		t.Fatal("unowned workload delayed PostgreSQL configuration pruning")
	}
	if retainPostgreSQLConfigurationDuringRollout(cluster, oldConfiguration, nil) {
		t.Fatal("unused PostgreSQL configuration was retained without a workload")
	}
}

func TestRetiredEtcdStorageCleanupWaitsForAuthoritativeStatefulSetAbsence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	claim := retiredEtcdPVC(cluster, 0)
	statefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: cluster.Name + legacyEtcdSuffix, Namespace: cluster.Namespace,
		UID: types.UID("legacy-etcd-statefulset-uid"), ResourceVersion: "1",
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cluster, pgshardv1alpha1.GroupVersion.WithKind("PgShardCluster"))},
	}}
	kubeClient := newFakeClient(t, cluster, claim, statefulSet)
	reconciler := developmentReconciler(kubeClient, kubeClient)

	cleaning, err := reconciler.cleanupRetiredEtcdStorage(ctx, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if !cleaning {
		t.Fatal("existing retired StatefulSet did not hold the storage absence barrier")
	}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(claim), &corev1.PersistentVolumeClaim{}); err != nil {
		t.Fatalf("retired etcd PVC was deleted while its StatefulSet still existed: %v", err)
	}
}

func TestRetiredEtcdStorageCleanupWaitsForAnyAuthoritativePodMountAbsence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	claim := retiredEtcdPVC(cluster, 0)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "arbitrary-pvc-reader", Namespace: cluster.Namespace,
			UID: types.UID("arbitrary-pod-uid"), ResourceVersion: "1",
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "reader", Image: "example.invalid/reader"}}, Volumes: []corev1.Volume{{
			Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim.Name}},
		}}},
	}
	kubeClient := newFakeClient(t, cluster, claim, pod)
	reconciler := developmentReconciler(kubeClient, kubeClient)

	cleaning, err := reconciler.cleanupRetiredEtcdStorage(ctx, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if !cleaning {
		t.Fatal("arbitrary Pod mounting retired etcd storage did not hold the absence barrier")
	}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(claim), &corev1.PersistentVolumeClaim{}); err != nil {
		t.Fatalf("retired etcd PVC was deleted while its Pod still existed: %v", err)
	}
}

func TestRetiredEtcdStorageCleanupDeletesOnlyValidatedLegacyClaims(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	claims := []client.Object{cluster}
	for ordinal := 0; ordinal < legacyEtcdPVCCount; ordinal++ {
		claims = append(claims, retiredEtcdPVC(cluster, ordinal))
	}
	lookalike := retiredEtcdPVC(cluster, legacyEtcdPVCCount)
	claims = append(claims, lookalike)
	kubeClient := newFakeClient(t, claims...)
	reconciler := developmentReconciler(kubeClient, kubeClient)

	cleaning, err := reconciler.cleanupRetiredEtcdStorage(ctx, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if !cleaning {
		t.Fatal("deleted retired etcd storage was reported absent in the same observation pass")
	}
	for ordinal := 0; ordinal < legacyEtcdPVCCount; ordinal++ {
		key := types.NamespacedName{Namespace: cluster.Namespace, Name: fmt.Sprintf("data-%s%s-%d", cluster.Name, legacyEtcdSuffix, ordinal)}
		if err := kubeClient.Get(ctx, key, &corev1.PersistentVolumeClaim{}); !apierrors.IsNotFound(err) {
			t.Fatalf("retired etcd PVC %s survived cleanup: %v", key.Name, err)
		}
	}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(lookalike), &corev1.PersistentVolumeClaim{}); err != nil {
		t.Fatalf("out-of-contract PVC was deleted: %v", err)
	}
	cleaning, err = reconciler.cleanupRetiredEtcdStorage(ctx, cluster)
	if err != nil || cleaning {
		t.Fatalf("observed cleanup completion = (%t, %v), want (false, nil)", cleaning, err)
	}
}

func TestRetiredEtcdStorageCleanupFailsClosedOnMetadataMismatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	claim := retiredEtcdPVC(cluster, 0)
	claim.Labels[owned.ComponentLabel] = "postgresql"
	kubeClient := newFakeClient(t, cluster, claim)
	reconciler := developmentReconciler(kubeClient, kubeClient)

	cleaning, err := reconciler.cleanupRetiredEtcdStorage(ctx, cluster)
	if err == nil || !strings.Contains(err.Error(), "not bound to the exact") {
		t.Fatalf("metadata mismatch error = %v", err)
	}
	if cleaning {
		t.Fatal("rejected retired etcd PVC was reported as an active cleanup")
	}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(claim), &corev1.PersistentVolumeClaim{}); err != nil {
		t.Fatalf("metadata-mismatched PVC was deleted: %v", err)
	}
}

func TestRetiredEtcdStorageCleanupRequiresAuthoritativeReader(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	claim := retiredEtcdPVC(cluster, 0)
	kubeClient := newFakeClient(t, cluster, claim)
	reconciler := developmentReconciler(kubeClient, nil)

	cleaning, err := reconciler.cleanupRetiredEtcdStorage(ctx, cluster)
	if err == nil || !strings.Contains(err.Error(), "authoritative API reader") {
		t.Fatalf("missing authoritative reader error = %v", err)
	}
	if cleaning {
		t.Fatal("failed cleanup reported work in progress")
	}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(claim), &corev1.PersistentVolumeClaim{}); err != nil {
		t.Fatalf("PVC was deleted without authoritative evidence: %v", err)
	}
}

func TestRuntimeLeaseEventsFilterRenewalsButKeepEnvelopeLifecycle(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	holderIdentity := "orchestrator-a"
	duration := int32(15)
	transitions := int32(1)
	acquired := metav1.NowMicro()
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:            cluster.Name + owned.OrchestratorLeaseSuffix,
			Namespace:       cluster.Namespace,
			UID:             types.UID("lease-uid"),
			ResourceVersion: "1",
			Generation:      1,
			Labels:          map[string]string{owned.ClusterLabel: cluster.Name},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cluster, pgshardv1alpha1.GroupVersion.WithKind("PgShardCluster"))},
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holderIdentity,
			LeaseDurationSeconds: &duration,
			AcquireTime:          &acquired,
			RenewTime:            &acquired,
			LeaseTransitions:     &transitions,
		},
	}
	renewed := lease.DeepCopy()
	renewed.ResourceVersion = "2"
	renewed.Generation = 2
	renewTime := metav1.NowMicro()
	renewed.Spec.RenewTime = &renewTime
	predicates := runtimeLeaseEvents()
	if predicates.Update(event.UpdateEvent{ObjectOld: lease, ObjectNew: renewed}) {
		t.Fatal("runtime Lease renewal enqueued a full cluster reconciliation")
	}
	missingRenewal := renewed.DeepCopy()
	missingRenewal.Spec.RenewTime = nil
	if !predicates.Update(event.UpdateEvent{ObjectOld: renewed, ObjectNew: missingRenewal}) {
		t.Fatal("missing runtime Lease renewal timestamp was filtered")
	}
	zeroRenewal := renewed.DeepCopy()
	zero := metav1.MicroTime{}
	zeroRenewal.Spec.RenewTime = &zero
	if !predicates.Update(event.UpdateEvent{ObjectOld: renewed, ObjectNew: zeroRenewal}) {
		t.Fatal("zero runtime Lease renewal timestamp was filtered")
	}
	empty := lease.DeepCopy()
	empty.Spec = coordinationv1.LeaseSpec{}
	partialEmpty := empty.DeepCopy()
	partialEmpty.Spec.RenewTime = &renewTime
	if !predicates.Update(event.UpdateEvent{ObjectOld: empty, ObjectNew: partialEmpty}) {
		t.Fatal("renewal-only mutation of an empty Lease was filtered")
	}
	newHolderIdentity := "orchestrator-b"
	holderChanged := renewed.DeepCopy()
	holderChanged.Spec.HolderIdentity = &newHolderIdentity
	if !predicates.Update(event.UpdateEvent{ObjectOld: renewed, ObjectNew: holderChanged}) {
		t.Fatal("runtime Lease holder transition was filtered")
	}
	releasedRuntimeState := renewed.DeepCopy()
	releasedRuntimeState.Spec.HolderIdentity = nil
	if !predicates.Update(event.UpdateEvent{ObjectOld: renewed, ObjectNew: releasedRuntimeState}) {
		t.Fatal("released runtime Lease transition was filtered")
	}

	envelopeChanged := renewed.DeepCopy()
	envelopeChanged.Labels[owned.ComponentLabel] = "wrong"
	if !predicates.Update(event.UpdateEvent{ObjectOld: renewed, ObjectNew: envelopeChanged}) {
		t.Fatal("Lease envelope drift was filtered")
	}
	ownershipChanged := renewed.DeepCopy()
	ownershipChanged.OwnerReferences = nil
	if !predicates.Update(event.UpdateEvent{ObjectOld: renewed, ObjectNew: ownershipChanged}) {
		t.Fatal("Lease ownership change was filtered")
	}
	deleting := renewed.DeepCopy()
	deletionTimestamp := metav1.Now()
	deleting.DeletionTimestamp = &deletionTimestamp
	if !predicates.Update(event.UpdateEvent{ObjectOld: renewed, ObjectNew: deleting}) {
		t.Fatal("Lease deletion transition was filtered")
	}
	if !predicates.Create(event.CreateEvent{Object: lease}) || !predicates.Delete(event.DeleteEvent{Object: lease}) {
		t.Fatal("Lease create or delete event was filtered")
	}
}

func TestDeletionFinalizerPrunesOwnedResources(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	fakeClient := newFakeClient(t, cluster)
	reconciler := developmentReconciler(fakeClient, fakeClient)
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
		Name:      "stale-control-plane-data",
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
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: "example-orch-lease"}, &coordinationv1.Lease{}); !apierrors.IsNotFound(err) {
		t.Fatalf("owned Lease survived finalization: %v", err)
	}
	if err := fakeClient.Get(ctx, client.ObjectKeyFromObject(pvc), &corev1.PersistentVolumeClaim{}); !apierrors.IsNotFound(err) {
		t.Fatalf("owned PVC survived supervised cleanup: %v", err)
	}
	deleting := getCluster(t, ctx, fakeClient, cluster)
	if !contains(deleting.Finalizers, resourceFinalizer) {
		t.Fatal("cleanup finalizer was removed before absence was observed")
	}
	for range 1 + int(cluster.Spec.Shards) {
		if _, err := reconciler.Reconcile(ctx, request); client.IgnoreNotFound(err) != nil {
			t.Fatal(err)
		}
		if err := fakeClient.Get(ctx, request.NamespacedName, &pgshardv1alpha1.PgShardCluster{}); apierrors.IsNotFound(err) {
			break
		}
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
	recreated := &coordinationv1.Lease{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: replacement.Namespace, Name: "example-orch-lease"}, recreated); err != nil {
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
		Name:      "stale-control-plane-data",
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
		Images:    owned.DevelopmentImages(),
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
	reconciler := developmentReconciler(newFakeClient(t, cluster), nil)
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
	reconciler := &PgShardClusterReconciler{Client: fakeClient, Images: owned.Images{Orchestrator: "orchestrator-only"}}
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err == nil {
		t.Fatal("expected planning failure")
	}
	got := getCluster(t, ctx, fakeClient, cluster)
	if got.Status.Phase != "Degraded" || got.Status.ObservedGeneration != 0 {
		t.Fatalf("status = %#v", got.Status)
	}
	if contains(got.Finalizers, resourceFinalizer) {
		t.Fatalf("invalid image configuration installed a cleanup finalizer: %#v", got.Finalizers)
	}
	assertCondition(t, got, reconciledCondition, metav1.ConditionFalse, "PlanInvalid")
	assertCondition(t, got, readyCondition, metav1.ConditionFalse, "PlanInvalid")
	assertCondition(t, got, transportSecurityCondition, metav1.ConditionUnknown, "TransportSecurityUnobserved")
}

func TestReconcileRejectsSingletonBootstrapImageBeforeDurableProvisioning(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		image      string
		zeroImages bool
	}{
		{name: "zero-value reconciler", zeroImages: true},
		{name: "missing"},
		{name: "mutable remote", image: "ghcr.io/andrew01234567890/pgshard-postgres-agent:main"},
		{name: "invalid digest shaped", image: "registry.example/UPPER/postgres-agent@sha256:" + strings.Repeat("a", 64)},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			cluster := validCluster()
			cluster.Spec.MembersPerShard = 1
			cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
			images := owned.DefaultImages()
			images.PostgreSQLBootstrap = test.image
			fakeClient := newFakeClient(t, cluster)
			reconciler := &PgShardClusterReconciler{Client: fakeClient, Images: images}
			if test.zeroImages {
				reconciler.Images = owned.Images{}
			}

			if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err == nil {
				t.Fatal("expected invalid PostgreSQL bootstrap image to fail")
			}
			got := getCluster(t, ctx, fakeClient, cluster)
			if got.Status.Phase != "Degraded" || got.Status.ObservedGeneration != 0 {
				t.Fatalf("status = %#v", got.Status)
			}
			if contains(got.Finalizers, resourceFinalizer) {
				t.Fatalf("invalid bootstrap image installed a cleanup finalizer: %#v", got.Finalizers)
			}
			if got.Status.PostgreSQLBootstrapSpec != nil || len(got.Status.PostgreSQLBootstraps) != 0 {
				t.Fatalf("invalid bootstrap image recorded durable bootstrap state: %#v", got.Status)
			}
			assertCondition(t, got, reconciledCondition, metav1.ConditionFalse, "PlanInvalid")

			secrets := &corev1.SecretList{}
			if err := fakeClient.List(ctx, secrets, client.InNamespace(cluster.Namespace)); err != nil {
				t.Fatal(err)
			}
			claims := &corev1.PersistentVolumeClaimList{}
			if err := fakeClient.List(ctx, claims, client.InNamespace(cluster.Namespace)); err != nil {
				t.Fatal(err)
			}
			statefulSets := &appsv1.StatefulSetList{}
			if err := fakeClient.List(ctx, statefulSets, client.InNamespace(cluster.Namespace)); err != nil {
				t.Fatal(err)
			}
			if len(secrets.Items) != 0 || len(claims.Items) != 0 || len(statefulSets.Items) != 0 {
				t.Fatalf("invalid bootstrap image provisioned resources: secrets=%d claims=%d StatefulSets=%d", len(secrets.Items), len(claims.Items), len(statefulSets.Items))
			}
		})
	}
}

func validCluster() *pgshardv1alpha1.PgShardCluster {
	prometheus := true
	storageClass := "test-storage"
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
			Storage: pgshardv1alpha1.StorageSpec{Size: resource.MustParse("10Gi"), StorageClassName: &storageClass, DeletionPolicy: pgshardv1alpha1.DeletionRetain},
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

func retiredEtcdPVC(cluster *pgshardv1alpha1.PgShardCluster, ordinal int) *corev1.PersistentVolumeClaim {
	volumeMode := corev1.PersistentVolumeFilesystem
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            fmt.Sprintf("data-%s%s-%d", cluster.Name, legacyEtcdSuffix, ordinal),
			Namespace:       cluster.Namespace,
			UID:             types.UID(fmt.Sprintf("legacy-etcd-pvc-%d", ordinal)),
			ResourceVersion: "1",
			Labels: map[string]string{
				"app.kubernetes.io/name": "pgshard",
				owned.ManagedByLabel:     owned.ManagedByValue,
				owned.InstanceLabel:      cluster.Name,
				owned.ComponentLabel:     "etcd",
				owned.ClusterLabel:       cluster.Name,
			},
			Annotations:     map[string]string{owned.ApplyOwnershipAnnotation: owned.ApplyOwnershipVersion},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cluster, pgshardv1alpha1.GroupVersion.WithKind("PgShardCluster"))},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("2Gi"),
			}},
			VolumeMode: &volumeMode,
		},
	}
}

func bootstrapForShard(t *testing.T, cluster *pgshardv1alpha1.PgShardCluster, shard int32) pgshardv1alpha1.PostgreSQLBootstrapStatus {
	return bootstrapForMember(t, cluster, shard, 0)
}

func bootstrapForMember(t *testing.T, cluster *pgshardv1alpha1.PgShardCluster, shard, member int32) pgshardv1alpha1.PostgreSQLBootstrapStatus {
	t.Helper()
	for _, bootstrap := range cluster.Status.PostgreSQLBootstraps {
		if bootstrap.Shard == shard && bootstrap.Member == member {
			return bootstrap
		}
	}
	t.Fatalf("PostgreSQL bootstrap for shard %d member %d not found: %#v", shard, member, cluster.Status.PostgreSQLBootstraps)
	return pgshardv1alpha1.PostgreSQLBootstrapStatus{}
}

func getPostgreSQLConfigMap(t *testing.T, ctx context.Context, kubeClient client.Client, cluster *pgshardv1alpha1.PgShardCluster) *corev1.ConfigMap {
	t.Helper()
	configurations := &corev1.ConfigMapList{}
	if err := kubeClient.List(ctx, configurations, client.InNamespace(cluster.Namespace)); err != nil {
		t.Fatal(err)
	}
	prefix := cluster.Name + owned.PostgreSQLConfigSuffix + "-"
	var found *corev1.ConfigMap
	for index := range configurations.Items {
		if !strings.HasPrefix(configurations.Items[index].Name, prefix) {
			continue
		}
		if found != nil {
			t.Fatalf("multiple PostgreSQL configuration ConfigMaps found: %s and %s", found.Name, configurations.Items[index].Name)
		}
		found = configurations.Items[index].DeepCopy()
	}
	if found == nil {
		t.Fatalf("PostgreSQL configuration ConfigMap with prefix %q not found", prefix)
	}
	return found
}

const testPodFencingKey = "0123456789abcdef0123456789abcdef"

func testTerminationAttestation(t *testing.T, pod *corev1.Pod) corev1.PodCondition {
	t.Helper()
	receipt, err := podfence.NewStaticHandshakeCodec([]byte(testPodFencingKey)).TerminationReceipt(context.Background(), pod)
	if err != nil {
		t.Fatal(err)
	}
	return podfence.NewTerminationAttestation(pod, metav1.Now(), receipt)
}

func newFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	objects = withPodFencingNamespaces(t, objects)
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
		WithStatusSubresource(&pgshardv1alpha1.PgShardCluster{}, &pgshardv1alpha1.PgShardRestore{}, &appsv1.Deployment{}, &appsv1.StatefulSet{}, &autoscalingv2.HorizontalPodAutoscaler{}, &policyv1.PodDisruptionBudget{}).
		WithObjects(objects...).
		WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, options ...client.CreateOption) error {
			if object.GetUID() == "" {
				object.SetUID(types.UID(utiluuid.NewUUID()))
			}
			return kubeClient.Create(ctx, object, options...)
		}}).
		Build()
}

func withPodFencingNamespaces(t *testing.T, objects []client.Object) []client.Object {
	t.Helper()
	prepared := append([]client.Object(nil), objects...)
	namespaces := make(map[string]*corev1.Namespace)
	for _, object := range prepared {
		if namespace, ok := object.(*corev1.Namespace); ok {
			namespaces[namespace.Name] = namespace
		}
	}
	for _, object := range prepared {
		cluster, ok := object.(*pgshardv1alpha1.PgShardCluster)
		if !ok {
			continue
		}
		if cluster.UID == "" {
			cluster.UID = types.UID(utiluuid.NewUUID())
		}
		if cluster.Annotations == nil {
			cluster.Annotations = make(map[string]string, 2)
		}
		cluster.Annotations[podfence.HandshakeChallengeAnnotation] = "test-admission-handshake"
		receipt, err := podfence.NewStaticHandshakeCodec([]byte(testPodFencingKey)).Receipt(context.Background(), cluster)
		if err != nil {
			t.Fatal(err)
		}
		cluster.Annotations[podfence.HandshakeReceiptAnnotation] = receipt
		namespace := namespaces[cluster.Namespace]
		if namespace == nil {
			namespace = &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: cluster.Namespace}}
			prepared = append(prepared, namespace)
			namespaces[cluster.Namespace] = namespace
		}
		if namespace.Labels == nil {
			namespace.Labels = make(map[string]string, 1)
		}
		namespace.Labels[podfence.NamespaceLabel] = podfence.NamespaceLabelValue
	}
	immutable := true
	managedLabels := map[string]string{owned.ManagedByLabel: owned.ManagedByValue}
	prepared = append(prepared,
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: defaultPodFencingKeyNamespace,
				Name:      defaultPodFencingKeySecret,
				Labels:    maps.Clone(managedLabels),
				Annotations: map[string]string{
					podfence.SecretKeyContinuityAnnotation: podfence.SecretKeyContinuityValue,
				},
			},
			Type:      corev1.SecretTypeOpaque,
			Immutable: &immutable,
			Data:      map[string][]byte{defaultPodFencingKeyData: []byte(testPodFencingKey)},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: defaultPodFencingKeyNamespace,
				Name:      defaultPodFencingAnchorSecret,
				Labels:    maps.Clone(managedLabels),
				Annotations: map[string]string{
					defaultPodFencingAnchorAnnotation: podfence.SecretHandshakeKeyFingerprint([]byte(testPodFencingKey)),
				},
			},
			Type: corev1.SecretTypeOpaque,
		},
	)
	return prepared
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
