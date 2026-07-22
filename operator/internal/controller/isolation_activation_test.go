package controller

import (
	"context"
	"testing"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func isolationNamespace(uid types.UID) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: genTestNamespace, UID: uid}}
}

// fakeDispatchProber returns a fixed proof, standing in for the live per-backend
// probe that the fake client cannot exercise.
type fakeDispatchProber struct {
	proof dispatchProof
	err   error
}

func (f fakeDispatchProber) Prove(ctx context.Context) (dispatchProof, error) { return f.proof, f.err }

// convergedDispatch matches the empty tuple hash used by the drive tests, so
// revalidateDispatchTuple treats the in-progress activation as still valid.
func convergedDispatch(tupleHash string) fakeDispatchProber {
	return fakeDispatchProber{proof: dispatchProof{converged: true, tupleHash: tupleHash}}
}

func clusterControllerRef(cluster *pgshardv1alpha1.PgShardCluster) metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{
		APIVersion: pgshardv1alpha1.GroupVersion.String(), Kind: "PgShardCluster",
		Name: cluster.Name, UID: cluster.UID, Controller: &controller,
	}
}

func reloadReceipt(t *testing.T, kubeClient client.Client, cluster *pgshardv1alpha1.PgShardCluster) *pgshardv1alpha1.PostgreSQLIsolationReceipt {
	t.Helper()
	reloaded := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), reloaded); err != nil {
		t.Fatal(err)
	}
	return reloaded.Status.IsolationReceipt
}

func TestReconcileIsolationActivationStaysInactive(t *testing.T) {
	t.Parallel()
	cluster := genCluster("inactivecase", "inactivecase-uid")
	reconciler, kubeClient := genReconciler(t, isolationNamespace("ns-uid"), cluster)
	activating, err := reconciler.reconcileIsolationActivation(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	// preflightConverged is the step-7b stub (false), so activation never starts
	// and no receipt is written: pre-activation behavior is unchanged.
	if activating {
		t.Fatalf("isolation activation started while the preflight seam is stubbed off")
	}
	if reloadReceipt(t, kubeClient, cluster) != nil {
		t.Fatalf("an isolation receipt was written while INACTIVE")
	}
}

func TestReconcileIsolationActivationResetsOnNamespaceRecreation(t *testing.T) {
	t.Parallel()
	cluster := genCluster("resetnscase", "resetnscase-uid")
	cluster.Status.IsolationReceipt = &pgshardv1alpha1.PostgreSQLIsolationReceipt{
		NamespaceUID: "stale-ns-uid", Phase: pgshardv1alpha1.IsolationActive,
	}
	reconciler, kubeClient := genReconciler(t, isolationNamespace("live-ns-uid"), cluster)
	if _, err := reconciler.reconcileIsolationActivation(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if reloadReceipt(t, kubeClient, cluster) != nil {
		t.Fatalf("receipt bound to a stale namespace UID was not reset")
	}
}

func TestDriveIsolationQuiesceSealsThenAdvances(t *testing.T) {
	t.Parallel()
	cluster := genCluster("quiescecase", "quiescecase-uid")
	cluster.Status.IsolationReceipt = &pgshardv1alpha1.PostgreSQLIsolationReceipt{
		NamespaceUID: "ns-uid", Phase: pgshardv1alpha1.IsolationActivatingQuiesce, ActivatedAt: metav1.Now(),
	}
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "quiescecase-member", Namespace: genTestNamespace, UID: "sts-uid",
			Labels:          map[string]string{owned.ComponentLabel: "postgresql", owned.ClusterLabel: cluster.Name},
			OwnerReferences: []metav1.OwnerReference{clusterControllerRef(cluster)},
		},
		Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{owned.PodContractHashAnnotation: genHashA}}}},
	}
	deployment := poolerDeploymentForClass(cluster.Name, "deploy-uid", genHashB, 1, 2)
	deployment.OwnerReferences = []metav1.OwnerReference{clusterControllerRef(cluster)}
	reconciler, kubeClient := genReconciler(t, isolationNamespace("ns-uid"), cluster, statefulSet, deployment)
	reconciler.DispatchProber = convergedDispatch("")

	// First pass seals the parents at their exact incarnation.
	if _, err := reconciler.reconcileIsolationActivation(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	receipt := reloadReceipt(t, kubeClient, cluster)
	if len(receipt.SealedParents) != 2 {
		t.Fatalf("sealed parents = %#v", receipt.SealedParents)
	}
	var sealedDeployment, sealedStatefulSet bool
	for _, parent := range receipt.SealedParents {
		if parent.Kind == "Deployment" && parent.UID == "deploy-uid" && parent.ContractHash == genHashB {
			sealedDeployment = true
		}
		if parent.Kind == "StatefulSet" && parent.UID == "sts-uid" && parent.ContractHash == genHashA {
			sealedStatefulSet = true
		}
	}
	if !sealedDeployment || !sealedStatefulSet {
		t.Fatalf("parents not sealed at their incarnations: %#v", receipt.SealedParents)
	}

	// After the drain elapses and with a clean (empty) pod inventory, quiesce
	// advances to recreate.
	fresh := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), fresh); err != nil {
		t.Fatal(err)
	}
	fresh.Status.IsolationReceipt.ActivatedAt = metav1.NewTime(time.Now().Add(-2 * supportingRevocationDrain))
	if err := kubeClient.Status().Update(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcileIsolationActivation(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	if got := reloadReceipt(t, kubeClient, cluster).Phase; got != pgshardv1alpha1.IsolationActivatingRecreate {
		t.Fatalf("phase after drain = %q, want ACTIVATING_RECREATE", got)
	}
}

func TestDriveIsolationQuiesceBlocksOnForeignPod(t *testing.T) {
	t.Parallel()
	cluster := genCluster("blockcase", "blockcase-uid")
	cluster.Status.IsolationReceipt = &pgshardv1alpha1.PostgreSQLIsolationReceipt{
		NamespaceUID: "ns-uid", Phase: pgshardv1alpha1.IsolationActivatingQuiesce,
		ActivatedAt:   metav1.NewTime(time.Now().Add(-2 * supportingRevocationDrain)),
		SealedParents: []pgshardv1alpha1.SealedParent{{Kind: "StatefulSet", Name: "x", UID: "sts-uid"}},
	}
	foreign := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "intruder", Namespace: genTestNamespace}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c"}}}}
	reconciler, kubeClient := genReconciler(t, isolationNamespace("ns-uid"), cluster, foreign)
	reconciler.DispatchProber = convergedDispatch("")

	if _, err := reconciler.reconcileIsolationActivation(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	receipt := reloadReceipt(t, kubeClient, cluster)
	if receipt.Phase != pgshardv1alpha1.IsolationActivatingQuiesce {
		t.Fatalf("phase advanced despite a foreign pod: %q", receipt.Phase)
	}
	reloaded := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), reloaded); err != nil {
		t.Fatal(err)
	}
	if condition := meta.FindStatusCondition(reloaded.Status.Conditions, isolationActivationBlockedCondition); condition == nil || condition.Status != metav1.ConditionTrue {
		t.Fatalf("IsolationActivationBlocked condition not surfaced: %#v", reloaded.Status.Conditions)
	}
}

func TestDriveIsolationRecreateReguardsThenActivates(t *testing.T) {
	t.Parallel()
	activatedAt := metav1.Now()
	cluster := genCluster("recreatecase", "recreatecase-uid")
	cluster.Status.IsolationReceipt = &pgshardv1alpha1.PostgreSQLIsolationReceipt{
		NamespaceUID: "ns-uid", Phase: pgshardv1alpha1.IsolationActivatingRecreate, SecurityGeneration: 1, ActivatedAt: activatedAt,
	}
	// A pre-guard member pod (created before the recreate phase) must be deleted.
	preGuard := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "recreatecase-member-0", Namespace: genTestNamespace, UID: "pre-uid",
			CreationTimestamp: metav1.NewTime(activatedAt.Add(-time.Hour)),
			Labels:            map[string]string{owned.ClusterLabel: cluster.Name, owned.ComponentLabel: "postgresql", owned.ShardLabel: "0000", owned.MemberLabel: "0000"},
			Annotations:       map[string]string{owned.PodContractHashAnnotation: genHashA, owned.PodSecurityGenerationAnnotation: "1"},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "postgresql"}}},
	}
	reconciler, kubeClient := genReconciler(t, isolationNamespace("ns-uid"), cluster, preGuard)
	reconciler.DispatchProber = convergedDispatch("")

	// First pass deletes the pre-guard pod and stays in RECREATE.
	if _, err := reconciler.reconcileIsolationActivation(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(context.Background(), client.ObjectKey{Namespace: genTestNamespace, Name: "recreatecase-member-0"}, &corev1.Pod{}); err == nil {
		t.Fatalf("pre-guard pod was not deleted during recreate")
	}
	if got := reloadReceipt(t, kubeClient, cluster).Phase; got != pgshardv1alpha1.IsolationActivatingRecreate {
		t.Fatalf("phase advanced before the pre-guard pod drained: %q", got)
	}

	// With no pre-guard pods left and a clean inventory, recreate activates.
	fresh := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), fresh); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcileIsolationActivation(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	receipt := reloadReceipt(t, kubeClient, cluster)
	if receipt.Phase != pgshardv1alpha1.IsolationActive {
		t.Fatalf("phase after reguard = %q, want ACTIVE", receipt.Phase)
	}
	if receipt.MinAcceptableSecurityGeneration != 1 || receipt.ResidueProfileHash == "" {
		t.Fatalf("active receipt = %#v", receipt)
	}
}

func TestIsolationBuildAllowsActivationDefault(t *testing.T) {
	t.Parallel()
	if !isolationBuildAllowsActivation {
		t.Fatal("the default build must permit activation; the bridge ceiling is a build-tag opt-out")
	}
}
