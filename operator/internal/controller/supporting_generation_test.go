package controller

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	genTestNamespace = "database"
	genHashA         = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	genHashB         = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	genHashC         = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

func genCluster(name string, uid types.UID) *pgshardv1alpha1.PgShardCluster {
	return &pgshardv1alpha1.PgShardCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: genTestNamespace, UID: uid},
		Spec:       pgshardv1alpha1.PgShardClusterSpec{MembersPerShard: 1},
	}
}

func poolerDeploymentForClass(clusterName string, uid types.UID, hash string, generation int64, replicas int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: clusterName + owned.PoolerSuffix, Namespace: genTestNamespace, UID: uid, Generation: 3,
			Labels: map[string]string{owned.ManagedByLabel: owned.ManagedByValue, owned.ComponentLabel: "pooler", owned.ClusterLabel: clusterName},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
				owned.PodContractHashAnnotation:       hash,
				owned.PodSecurityGenerationAnnotation: strconv.FormatInt(generation, 10),
			}}},
		},
		Status: appsv1.DeploymentStatus{ObservedGeneration: 3, Replicas: replicas, UpdatedReplicas: replicas, AvailableReplicas: replicas},
	}
}

func ownedReplicaSet(name string, uid, deploymentUID types.UID, hash string, replicas, statusReplicas int32) *appsv1.ReplicaSet {
	controller := true
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: genTestNamespace, UID: uid,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "x", UID: deploymentUID, Controller: &controller}},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{owned.PodContractHashAnnotation: hash}}},
		},
		Status: appsv1.ReplicaSetStatus{Replicas: statusReplicas},
	}
}

func genReconciler(t *testing.T, objects ...client.Object) (*PgShardClusterReconciler, client.Client) {
	t.Helper()
	fakeClient := newFakeClient(t, objects...)
	// Tests run with the drain bound attested at the conservative default; the
	// unattested-withholds regression zeroes it explicitly.
	return &PgShardClusterReconciler{Client: fakeClient, APIReader: fakeClient, AttestedRequestTimeout: supportingRevocationDrain}, fakeClient
}

func reloadGenerationRecord(t *testing.T, kubeClient client.Client, cluster *pgshardv1alpha1.PgShardCluster, class string) pgshardv1alpha1.SupportingGenerationStatus {
	t.Helper()
	reloaded := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), reloaded); err != nil {
		t.Fatal(err)
	}
	for _, record := range reloaded.Status.SupportingGenerations {
		if record.Class == class {
			return record
		}
	}
	return pgshardv1alpha1.SupportingGenerationStatus{}
}

func TestAdvanceSupportingGenerationsBindsNewReplicaSet(t *testing.T) {
	t.Parallel()
	cluster := genCluster("bindcase", "bindcase-uid")
	deployment := poolerDeploymentForClass(cluster.Name, "deploy-uid", genHashB, 1, 2)
	replicaSetB := ownedReplicaSet("bindcase-pooler-b", "rs-b", deployment.UID, genHashB, 2, 2)
	reconciler, kubeClient := genReconciler(t, cluster, deployment, replicaSetB)

	if _, err := reconciler.advanceSupportingGenerations(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	record := reloadGenerationRecord(t, kubeClient, cluster, "pooler")
	if record.CurrentReplicaSetUID != "rs-b" || record.CurrentContractHash != genHashB || record.DeploymentUID != "deploy-uid" {
		t.Fatalf("bind record = %#v", record)
	}
	if record.PriorReplicaSetUID != "" {
		t.Fatalf("fresh bind carried a prior generation: %#v", record)
	}
}

func TestAdvanceSupportingGenerationsCoexistsAndBindsPrior(t *testing.T) {
	t.Parallel()
	cluster := genCluster("rollcase", "rollcase-uid")
	cluster.Status.SupportingGenerations = []pgshardv1alpha1.SupportingGenerationStatus{{
		Class: "pooler", DeploymentUID: "deploy-uid",
		CurrentReplicaSetUID: "rs-a", CurrentContractHash: genHashA, CurrentTemplateGeneration: 1,
		MinGenerationForNewCreates: 1, ConvergedGeneration: 1,
	}}
	deployment := poolerDeploymentForClass(cluster.Name, "deploy-uid", genHashB, 1, 2)
	replicaSetA := ownedReplicaSet("rollcase-pooler-a", "rs-a", deployment.UID, genHashA, 1, 1)
	replicaSetB := ownedReplicaSet("rollcase-pooler-b", "rs-b", deployment.UID, genHashB, 2, 2)
	reconciler, kubeClient := genReconciler(t, cluster, deployment, replicaSetA, replicaSetB)

	// Bind B and move A to prior: both remain admissible, so neither is fenced.
	if _, err := reconciler.advanceSupportingGenerations(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	record := reloadGenerationRecord(t, kubeClient, cluster, "pooler")
	if record.CurrentReplicaSetUID != "rs-b" || record.PriorReplicaSetUID != "rs-a" || record.PriorContractHash != genHashA {
		t.Fatalf("coexistence record = %#v", record)
	}
}

func TestAdvanceSupportingGenerationsConvergesAfterDrain(t *testing.T) {
	t.Parallel()
	cluster := genCluster("convergecase", "convergecase-uid")
	// Post-bind state: B current, A prior already scaled to zero and drained past
	// the revocation timeout.
	cluster.Status.SupportingGenerations = []pgshardv1alpha1.SupportingGenerationStatus{{
		Class: "pooler", DeploymentUID: "deploy-uid",
		CurrentReplicaSetUID: "rs-b", CurrentContractHash: genHashB, CurrentTemplateGeneration: 1,
		PriorReplicaSetUID: "rs-a", PriorContractHash: genHashA, PriorRevoked: true, MinGenerationForNewCreates: 1,
		SealedAt: metav1.NewTime(time.Now().Add(-2 * supportingRevocationDrain)),
	}}
	deployment := poolerDeploymentForClass(cluster.Name, "deploy-uid", genHashB, 1, 2)
	replicaSetA := ownedReplicaSet("convergecase-pooler-a", "rs-a", deployment.UID, genHashA, 0, 0)
	replicaSetB := ownedReplicaSet("convergecase-pooler-b", "rs-b", deployment.UID, genHashB, 2, 2)
	reconciler, kubeClient := genReconciler(t, cluster, deployment, replicaSetA, replicaSetB)

	// Two consecutive authoritative zero LISTs are required before convergence.
	for pass := 0; pass < 2; pass++ {
		fresh := &pgshardv1alpha1.PgShardCluster{}
		if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), fresh); err != nil {
			t.Fatal(err)
		}
		if _, err := reconciler.advanceSupportingGenerations(context.Background(), fresh); err != nil {
			t.Fatal(err)
		}
	}
	record := reloadGenerationRecord(t, kubeClient, cluster, "pooler")
	if record.PriorReplicaSetUID != "" || record.PriorContractHash != "" || record.ConvergedGeneration != 1 {
		t.Fatalf("post-convergence record = %#v", record)
	}
}

func TestDriveSupportingRevocationDeletesLateWritePods(t *testing.T) {
	t.Parallel()
	controller := true
	cluster := genCluster("latecase", "latecase-uid")
	cluster.Status.SupportingGenerations = []pgshardv1alpha1.SupportingGenerationStatus{{
		Class: "pooler", DeploymentUID: "deploy-uid",
		CurrentReplicaSetUID: "rs-b", CurrentContractHash: genHashB, CurrentTemplateGeneration: 2,
		PriorReplicaSetUID: "rs-a", PriorContractHash: genHashA, PriorRevoked: true,
		MinGenerationForNewCreates: 2,
		SealedAt:                   metav1.NewTime(time.Now().Add(-2 * supportingRevocationDrain)),
	}}
	deployment := poolerDeploymentForClass(cluster.Name, "deploy-uid", genHashB, 2, 2)
	replicaSetA := ownedReplicaSet("latecase-pooler-a", "rs-a", deployment.UID, genHashA, 0, 0)
	replicaSetB := ownedReplicaSet("latecase-pooler-b", "rs-b", deployment.UID, genHashB, 2, 2)
	lateWrite := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "late-write", Namespace: genTestNamespace, UID: "late-uid",
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: replicaSetA.Name, UID: "rs-a", Controller: &controller}},
	}}
	reconciler, kubeClient := genReconciler(t, cluster, deployment, replicaSetA, replicaSetB, lateWrite)

	if _, err := reconciler.advanceSupportingGenerations(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	remaining := &corev1.Pod{}
	if err := kubeClient.Get(context.Background(), client.ObjectKey{Namespace: genTestNamespace, Name: "late-write"}, remaining); err == nil {
		t.Fatalf("late-write pod owned by the draining prior ReplicaSet was not deleted")
	}
	// The prior generation is not converged while a late write had to be reaped.
	record := reloadGenerationRecord(t, kubeClient, cluster, "pooler")
	if record.PriorReplicaSetUID != "rs-a" {
		t.Fatalf("prior cleared despite a late write: %#v", record)
	}
}

func TestDriveSupportingRevocationSealsRevokeBeforeDraining(t *testing.T) {
	t.Parallel()
	controller := true
	cluster := genCluster("revokefirstcase", "revokefirstcase-uid")
	// Prior not yet revoked; everything else (drain elapsed, RS at zero) would
	// otherwise permit a sweep. Revocation must be sealed and persisted FIRST.
	cluster.Status.SupportingGenerations = []pgshardv1alpha1.SupportingGenerationStatus{{
		Class: "pooler", DeploymentUID: "deploy-uid",
		CurrentReplicaSetUID: "rs-b", CurrentContractHash: genHashB, CurrentTemplateGeneration: 2,
		PriorReplicaSetUID: "rs-a", PriorContractHash: genHashA, PriorRevoked: false,
		MinGenerationForNewCreates: 2,
		SealedAt:                   metav1.NewTime(time.Now().Add(-2 * supportingRevocationDrain)),
	}}
	deployment := poolerDeploymentForClass(cluster.Name, "deploy-uid", genHashB, 2, 2)
	replicaSetA := ownedReplicaSet("revokefirstcase-pooler-a", "rs-a", deployment.UID, genHashA, 0, 0)
	replicaSetB := ownedReplicaSet("revokefirstcase-pooler-b", "rs-b", deployment.UID, genHashB, 2, 2)
	lateWrite := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "late-write", Namespace: genTestNamespace, UID: "late-uid",
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: replicaSetA.Name, UID: "rs-a", Controller: &controller}},
	}}
	reconciler, kubeClient := genReconciler(t, cluster, deployment, replicaSetA, replicaSetB, lateWrite)

	if _, err := reconciler.advanceSupportingGenerations(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	// The revocation is sealed first; no pod is reaped on this pass.
	if err := kubeClient.Get(context.Background(), client.ObjectKey{Namespace: genTestNamespace, Name: "late-write"}, &corev1.Pod{}); err != nil {
		t.Fatal("a pod was reaped before the revocation was durably sealed")
	}
	record := reloadGenerationRecord(t, kubeClient, cluster, "pooler")
	if !record.PriorRevoked {
		t.Fatalf("prior generation was not revoked first: %#v", record)
	}
	// The drain timer is reset by the revocation seal, so convergence cannot be
	// claimed on the same pass.
	if record.PriorReplicaSetUID != "rs-a" {
		t.Fatalf("prior cleared on the revocation-seal pass: %#v", record)
	}
}

func TestSealSupportingGenerationIntentsAdvancesBarrierOnSecurityBump(t *testing.T) {
	t.Parallel()
	cluster := genCluster("bumpcase", "bumpcase-uid")
	cluster.Status.SupportingGenerations = []pgshardv1alpha1.SupportingGenerationStatus{{
		Class: "pooler", DeploymentUID: "deploy-uid",
		CurrentReplicaSetUID: "rs-a", CurrentContractHash: genHashA, CurrentTemplateGeneration: 1,
		MinGenerationForNewCreates: 1,
	}}
	liveDeployment := poolerDeploymentForClass(cluster.Name, "deploy-uid", genHashA, 1, 2)
	reconciler, kubeClient := genReconciler(t, cluster, liveDeployment)

	// The plan raises the security generation (a security-strengthening change).
	planDeployment := poolerDeploymentForClass(cluster.Name, "deploy-uid", genHashB, 2, 2)
	holding, err := reconciler.sealSupportingGenerationIntents(context.Background(), cluster, []client.Object{planDeployment})
	if err != nil {
		t.Fatal(err)
	}
	if holding {
		t.Fatalf("a non-serialized security bump reported holding")
	}
	record := reloadGenerationRecord(t, kubeClient, cluster, "pooler")
	if record.MinGenerationForNewCreates != 2 {
		t.Fatalf("revocation barrier not advanced before the mutation: %#v", record)
	}
}

func TestSealSupportingGenerationIntentsSerializesRolls(t *testing.T) {
	t.Parallel()
	cluster := genCluster("serialcase", "serialcase-uid")
	cluster.Status.SupportingGenerations = []pgshardv1alpha1.SupportingGenerationStatus{{
		Class: "pooler", DeploymentUID: "deploy-uid",
		CurrentReplicaSetUID: "rs-b", CurrentContractHash: genHashB, CurrentTemplateGeneration: 1,
		PriorReplicaSetUID: "rs-a", PriorContractHash: genHashA,
		MinGenerationForNewCreates: 1,
	}}
	liveDeployment := poolerDeploymentForClass(cluster.Name, "deploy-uid", genHashB, 1, 2)
	reconciler, _ := genReconciler(t, cluster, liveDeployment)

	// While A is still draining (prior populated), a third template C arrives.
	planDeployment := poolerDeploymentForClass(cluster.Name, "deploy-uid", genHashC, 1, 2)
	holding, err := reconciler.sealSupportingGenerationIntents(context.Background(), cluster, []client.Object{planDeployment})
	if err != nil {
		t.Fatal(err)
	}
	if !holding {
		t.Fatalf("a roll starting while a prior generation drains was not serialized")
	}
	if got := planDeployment.Spec.Template.Annotations[owned.PodContractHashAnnotation]; got != genHashB {
		t.Fatalf("plan Deployment was not held at the live template: hash=%s", got)
	}
}

func TestAdvanceSupportingGenerationsRecoversFromCrashDeterministically(t *testing.T) {
	t.Parallel()
	cluster := genCluster("crashcase", "crashcase-uid")
	// The manager crashed after applying B's template but before binding B: status
	// still records A as current.
	cluster.Status.SupportingGenerations = []pgshardv1alpha1.SupportingGenerationStatus{{
		Class: "pooler", DeploymentUID: "deploy-uid",
		CurrentReplicaSetUID: "rs-a", CurrentContractHash: genHashA, CurrentTemplateGeneration: 1,
		MinGenerationForNewCreates: 1, ConvergedGeneration: 1,
	}}
	deployment := poolerDeploymentForClass(cluster.Name, "deploy-uid", genHashB, 1, 2)
	replicaSetA := ownedReplicaSet("crashcase-pooler-a", "rs-a", deployment.UID, genHashA, 1, 1)
	replicaSetB := ownedReplicaSet("crashcase-pooler-b", "rs-b", deployment.UID, genHashB, 2, 2)
	reconciler, kubeClient := genReconciler(t, cluster, deployment, replicaSetA, replicaSetB)

	// Recovery recomputes {current, prior} from the live ReplicaSet UIDs, never
	// from ready counts: B is bound as current and A becomes the prior.
	if _, err := reconciler.advanceSupportingGenerations(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	record := reloadGenerationRecord(t, kubeClient, cluster, "pooler")
	if record.CurrentReplicaSetUID != "rs-b" || record.CurrentContractHash != genHashB {
		t.Fatalf("crash recovery did not bind the live current ReplicaSet: %#v", record)
	}
	if record.PriorReplicaSetUID != "rs-a" || record.PriorContractHash != genHashA {
		t.Fatalf("crash recovery did not recover the prior generation: %#v", record)
	}
}

func TestSupportingGenerationDeploymentRecreateResetsRecord(t *testing.T) {
	t.Parallel()
	cluster := genCluster("resetcase", "resetcase-uid")
	cluster.Status.SupportingGenerations = []pgshardv1alpha1.SupportingGenerationStatus{{
		Class: "pooler", DeploymentUID: "old-deploy-uid",
		CurrentReplicaSetUID: "rs-old", CurrentContractHash: genHashA, CurrentTemplateGeneration: 1,
		PriorReplicaSetUID: "rs-older", PriorContractHash: genHashB,
	}}
	deployment := poolerDeploymentForClass(cluster.Name, "new-deploy-uid", genHashA, 1, 2)
	replicaSet := ownedReplicaSet("resetcase-pooler-a", "rs-new", deployment.UID, genHashA, 2, 2)
	reconciler, kubeClient := genReconciler(t, cluster, deployment, replicaSet)

	if _, err := reconciler.advanceSupportingGenerations(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	record := reloadGenerationRecord(t, kubeClient, cluster, "pooler")
	if record.DeploymentUID != "new-deploy-uid" || record.CurrentReplicaSetUID != "rs-new" ||
		record.PriorReplicaSetUID != "" || !strings.EqualFold(record.CurrentContractHash, genHashA) {
		t.Fatalf("record was not rebuilt after Deployment recreation: %#v", record)
	}
}
