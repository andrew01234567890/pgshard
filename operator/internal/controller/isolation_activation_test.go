package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/operator/internal/podfence"
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
// probe that the fake client cannot exercise. When enforcingProof is set,
// ProveEnforcing returns it instead, so tests can drive a converged base proof
// alongside a not-yet-converged gated proof (a stale backend's label lag).
type fakeDispatchProber struct {
	proof          dispatchProof
	enforcingProof *dispatchProof
	err            error
}

func (f fakeDispatchProber) Prove(ctx context.Context, namespace string) (dispatchProof, error) {
	return f.proof, f.err
}

func (f fakeDispatchProber) ProveEnforcing(ctx context.Context, namespace string) (dispatchProof, error) {
	if f.enforcingProof != nil {
		return *f.enforcingProof, f.err
	}
	return f.proof, f.err
}

// convergedDispatch matches the empty tuple hash used by the drive tests, so
// revalidateDispatchTuple treats the in-progress activation as still valid.
func convergedDispatch(tupleHash string) fakeDispatchProber {
	return fakeDispatchProber{proof: dispatchProof{converged: true, backends: 2, tupleHash: tupleHash}}
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

func TestDriveIsolationQuiesceSealsForeignPodForCleanup(t *testing.T) {
	t.Parallel()
	// QUIESCE must NOT block on a foreign pod (that deadlocks — nothing deletes it
	// while quiesced). It seals EVERY pod UID and advances to RECREATE, which
	// UID-deletes the foreign pod (cleanup, not blocking).
	cluster := genCluster("cleanupcase", "cleanupcase-uid")
	cluster.Status.IsolationReceipt = &pgshardv1alpha1.PostgreSQLIsolationReceipt{
		NamespaceUID: "ns-uid", Phase: pgshardv1alpha1.IsolationActivatingQuiesce,
		ActivatedAt:   metav1.NewTime(time.Now().Add(-2 * supportingRevocationDrain)),
		SealedParents: []pgshardv1alpha1.SealedParent{{Kind: "StatefulSet", Name: "x", UID: "sts-uid"}},
	}
	sealedLive := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: genTestNamespace, UID: "sts-uid"}}
	foreign := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "intruder", Namespace: genTestNamespace, UID: "intruder-uid"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c"}}}}
	reconciler, kubeClient := genReconciler(t, isolationNamespace("ns-uid"), cluster, sealedLive, foreign)
	reconciler.DispatchProber = convergedDispatch("")

	// QUIESCE seals the foreign pod's UID and advances to RECREATE.
	if _, err := reconciler.reconcileIsolationActivation(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	receipt := reloadReceipt(t, kubeClient, cluster)
	if receipt.Phase != pgshardv1alpha1.IsolationActivatingRecreate {
		t.Fatalf("QUIESCE did not advance to RECREATE (it must not block on a foreign pod): %q", receipt.Phase)
	}
	sealedForeign := false
	for _, uid := range receipt.RecreatePendingUIDs {
		if uid == "intruder-uid" {
			sealedForeign = true
		}
	}
	if !sealedForeign {
		t.Fatalf("foreign pod UID was not sealed for recreate cleanup: %#v", receipt.RecreatePendingUIDs)
	}

	// RECREATE UID-deletes the foreign pod.
	fresh := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), fresh); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcileIsolationActivation(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(context.Background(), client.ObjectKey{Namespace: genTestNamespace, Name: "intruder"}, &corev1.Pod{}); err == nil {
		t.Fatal("foreign pod was not cleaned up during RECREATE")
	}
}

// guardedPoolerChain builds a fully valid guarded replacement chain in the
// fenced namespace: a stamped, digest-pinned pooler Deployment, its ReplicaSet,
// a topology Node, and a BOUND live pod that passes the full shared
// live-contract validation.
func guardedPoolerChain(t *testing.T, cluster *pgshardv1alpha1.PgShardCluster) (*appsv1.Deployment, *appsv1.ReplicaSet, *corev1.Node, *corev1.Pod) {
	t.Helper()
	controller := true
	template := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      map[string]string{owned.ClusterLabel: cluster.Name, owned.ComponentLabel: "pooler"},
			Annotations: map[string]string{owned.PostgreSQLPodClusterUIDAnnotation: string(cluster.UID)},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "workload", Image: "pgshard/example@sha256:" + strings.Repeat("0", 64)}}},
	}
	if _, err := owned.ApplyContractStamp(&template, owned.ClassPooler, string(cluster.UID), 0, 0, 1); err != nil {
		t.Fatal(err)
	}
	replicas := int32(1)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: cluster.Name + owned.PoolerSuffix, Namespace: genTestNamespace, UID: "guard-deploy-uid", Generation: 1,
			Labels:          map[string]string{owned.ManagedByLabel: owned.ManagedByValue, owned.ComponentLabel: "pooler", owned.ClusterLabel: cluster.Name},
			OwnerReferences: []metav1.OwnerReference{clusterControllerRef(cluster)},
		},
		Spec: appsv1.DeploymentSpec{Replicas: &replicas, Template: template},
	}
	replicaSet := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: deployment.Name + "-77abcde", Namespace: genTestNamespace, UID: "guard-rs-uid",
			Labels:          map[string]string{"pod-template-hash": "77abcde"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: deployment.Name, UID: deployment.UID, Controller: &controller}},
		},
		Spec: appsv1.ReplicaSetSpec{Replicas: &replicas, Template: *template.DeepCopy()},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "guard-node", UID: "guard-node-uid",
			Labels: map[string]string{corev1.LabelTopologyZone: "zone-a", corev1.LabelTopologyRegion: "region-a"},
		},
		Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{BootID: "guard-boot"}},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: replicaSet.Name + "-xyz", Namespace: genTestNamespace, UID: "guard-pod-uid",
			Labels: map[string]string{
				owned.ClusterLabel: cluster.Name, owned.ComponentLabel: "pooler", "pod-template-hash": "77abcde",
				corev1.LabelTopologyZone: "zone-a", corev1.LabelTopologyRegion: "region-a",
			},
			Annotations:     map[string]string{},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: replicaSet.Name, UID: replicaSet.UID, Controller: &controller}},
		},
		Spec: *template.Spec.DeepCopy(),
	}
	for key, value := range template.Annotations {
		pod.Annotations[key] = value
	}
	pod.Spec.NodeName = node.Name
	pod.Annotations[podfence.NodeUIDAnnotation] = string(node.UID)
	pod.Annotations[podfence.NodeBootIDAnnotation] = node.Status.NodeInfo.BootID
	return deployment, replicaSet, node, pod
}

func TestDriveIsolationRecreateReguardsThenActivates(t *testing.T) {
	t.Parallel()
	activatedAt := metav1.Now()
	cluster := genCluster("recreatecase", "recreatecase-uid")
	// The pooler class has a recorded contract at generation 1, so the ACTIVE
	// receipt seals a per-class pooler floor.
	cluster.Status.SupportingContracts = []pgshardv1alpha1.SupportingContractStatus{{Class: "pooler", ContractHash: genHashB, SecurityGeneration: 1}}
	deployment, replicaSet, node, replacement := guardedPoolerChain(t, cluster)
	cluster.Status.IsolationReceipt = &pgshardv1alpha1.PostgreSQLIsolationReceipt{
		NamespaceUID: "ns-uid", Phase: pgshardv1alpha1.IsolationActivatingRecreate,
		SecurityGeneration: 1, ActivatedAt: activatedAt,
		SecurityFloors: []pgshardv1alpha1.IsolationSecurityFloor{{Component: "pooler", MinGeneration: 1}},
		// QUIESCE sealed the protected pods to recreate by exact UID, and the
		// parents (with their guarded replacement cardinality) at their exact
		// incarnation.
		RecreatePendingUIDs: []string{"pre-uid"},
		SealedParents: []pgshardv1alpha1.SealedParent{{
			Kind: "Deployment", Name: deployment.Name, UID: string(deployment.UID),
			Generation: deployment.Generation, Replicas: 1,
			ContractHash: deployment.Spec.Template.Annotations[owned.PodContractHashAnnotation],
		}},
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
	reconciler, kubeClient := genReconciler(t, isolationNamespace("ns-uid"), cluster, deployment, replicaSet, node, preGuard)
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

	// With the sealed UIDs gone but the guarded replacement NOT yet created, the
	// namespace must NOT activate: an empty namespace is not a completed recreate.
	fresh := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), fresh); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcileIsolationActivation(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	if got := reloadReceipt(t, kubeClient, cluster).Phase; got != pgshardv1alpha1.IsolationActivatingRecreate {
		t.Fatalf("phase over an empty namespace = %q, want ACTIVATING_RECREATE (no replacements exist)", got)
	}

	// Once the controllers recreate the guarded replacement (bound + fully valid
	// under the shared live-contract validation, at the sealed cardinality),
	// recreate activates.
	if err := kubeClient.Create(context.Background(), replacement); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), fresh); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcileIsolationActivation(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	receipt := reloadReceipt(t, kubeClient, cluster)
	if receipt.Phase != pgshardv1alpha1.IsolationActive {
		t.Fatalf("phase after guarded replacements = %q, want ACTIVE", receipt.Phase)
	}
	if receipt.ResidueProfileHash == "" {
		t.Fatalf("active receipt missing residue profile: %#v", receipt)
	}
	// The ACTIVE receipt seals the PER-class pooler floor (never a namespace-wide
	// scalar).
	if receipt.SecurityFloorFor("pooler", 0, 0) != 1 {
		t.Fatalf("active receipt did not seal the per-class pooler floor: %#v", receipt.SecurityFloors)
	}
}

func TestReconcileIsolationActivationManagesEnforcingNamespaceLabel(t *testing.T) {
	t.Parallel()
	reload := func(t *testing.T, kubeClient client.Client, name string) *corev1.Namespace {
		t.Helper()
		ns := &corev1.Namespace{}
		if err := kubeClient.Get(context.Background(), client.ObjectKey{Name: name}, ns); err != nil {
			t.Fatal(err)
		}
		return ns
	}

	// Entering activation — QUIESCE is the FIRST non-INACTIVE phase — makes the
	// operator set the isolation-enforcing namespace label, so every genuinely-new
	// isolation webhook's selector (WorkloadIntegrity, PodConnect, LimitRange)
	// begins matching this namespace the instant activation begins; the same
	// non-INACTIVE predicate keeps it set through RECREATE and ACTIVE.
	cluster := genCluster("enforcinglabelcase", "enforcinglabelcase-uid")
	cluster.Status.IsolationReceipt = &pgshardv1alpha1.PostgreSQLIsolationReceipt{
		NamespaceUID: "ns-uid", Phase: pgshardv1alpha1.IsolationActivatingQuiesce, ActivatedAt: metav1.Now(),
	}
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "enforcinglabelcase-member", Namespace: genTestNamespace, UID: "sts-uid",
			Labels:          map[string]string{owned.ComponentLabel: "postgresql", owned.ClusterLabel: cluster.Name},
			OwnerReferences: []metav1.OwnerReference{clusterControllerRef(cluster)},
		},
		Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{owned.PodContractHashAnnotation: genHashA}}}},
	}
	reconciler, kubeClient := genReconciler(t, isolationNamespace("ns-uid"), cluster, statefulSet)
	reconciler.DispatchProber = convergedDispatch("")
	if _, err := reconciler.reconcileIsolationActivation(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if reload(t, kubeClient, genTestNamespace).Labels[podfence.NamespaceEnforcingLabel] != podfence.NamespaceEnforcingLabelValue {
		t.Fatal("entering activation (QUIESCE) did not set the isolation-enforcing namespace label")
	}

	// Returning to INACTIVE (receipt cleared; the namespace is not opted-in, so it
	// does not re-activate) makes the operator REMOVE the label, so the new
	// isolation webhooks stop firing and the namespace behaves exactly as it did
	// before isolation existed — ordinary applies/creates survive a manager restart.
	inactive := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), inactive); err != nil {
		t.Fatal(err)
	}
	inactive.Status.IsolationReceipt = nil
	if err := kubeClient.Status().Update(context.Background(), inactive); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcileIsolationActivation(context.Background(), inactive); err != nil {
		t.Fatal(err)
	}
	if _, present := reload(t, kubeClient, genTestNamespace).Labels[podfence.NamespaceEnforcingLabel]; present {
		t.Fatal("returning to INACTIVE did not remove the isolation-enforcing namespace label")
	}
}

func convergeCluster(name string) *pgshardv1alpha1.PgShardCluster {
	cluster := genCluster(name, types.UID(name+"-uid"))
	cluster.Status.IsolationReceipt = &pgshardv1alpha1.PostgreSQLIsolationReceipt{
		NamespaceUID: "ns-uid", Phase: pgshardv1alpha1.IsolationActivatingConverge,
		DispatchTupleHash: "base-tuple", ActivatedAt: metav1.Now(),
	}
	return cluster
}

func TestDriveIsolationConvergeAdvancesToQuiesceOnProvenGatedDispatch(t *testing.T) {
	t.Parallel()
	cluster := convergeCluster("convergecase")
	reconciler, kubeClient := genReconciler(t, isolationNamespace("ns-uid"), cluster)
	enforcing := dispatchProof{converged: true, backends: 2, tupleHash: "enforcing-tuple"}
	reconciler.DispatchProber = fakeDispatchProber{proof: dispatchProof{converged: true, backends: 2, tupleHash: "base-tuple"}, enforcingProof: &enforcing}
	reconciler.IdentityProber = fakeIdentityProber{matched: true}

	if _, err := reconciler.reconcileIsolationActivation(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	receipt := reloadReceipt(t, kubeClient, cluster)
	if receipt.Phase != pgshardv1alpha1.IsolationActivatingQuiesce {
		t.Fatalf("proven converge did not advance to quiesce: %#v", receipt)
	}
	// Enforcement is bound to the ENFORCING tuple: every later revalidation
	// re-proves the gated webhooks, never just the base PodCreate sentinel.
	if receipt.DispatchTupleHash != "enforcing-tuple" {
		t.Fatalf("quiesce receipt is not bound to the enforcing proof tuple: %#v", receipt)
	}
}

func TestDriveIsolationConvergeHoldsWhileGatedDispatchUnproven(t *testing.T) {
	t.Parallel()
	cluster := convergeCluster("staleconvergecase")
	reconciler, kubeClient := genReconciler(t, isolationNamespace("ns-uid"), cluster)
	// A stale backend's namespace informer has not observed the enforcing label:
	// the gated proof stays unconverged even though the base proof is fine.
	stale := dispatchProof{converged: false, reason: "10.0.0.2 skipped the workload webhook"}
	reconciler.DispatchProber = fakeDispatchProber{proof: dispatchProof{converged: true, backends: 2, tupleHash: "base-tuple"}, enforcingProof: &stale}
	reconciler.IdentityProber = fakeIdentityProber{matched: true}

	if _, err := reconciler.reconcileIsolationActivation(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	receipt := reloadReceipt(t, kubeClient, cluster)
	if receipt.Phase != pgshardv1alpha1.IsolationActivatingConverge {
		t.Fatalf("unproven gated dispatch advanced past converge: %#v", receipt)
	}
	reloaded := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), reloaded); err != nil {
		t.Fatal(err)
	}
	if meta.FindStatusCondition(reloaded.Status.Conditions, isolationDispatchNotConvergedCondition) == nil {
		t.Fatalf("gated-dispatch hold did not surface its condition: %#v", reloaded.Status.Conditions)
	}
}

func TestDriveIsolationConvergeRunsIdentityProbeAfterGatedDispatch(t *testing.T) {
	t.Parallel()
	cluster := convergeCluster("identityconvergecase")
	reconciler, kubeClient := genReconciler(t, isolationNamespace("ns-uid"), cluster)
	reconciler.DispatchProber = fakeDispatchProber{proof: dispatchProof{converged: true, backends: 2, tupleHash: "enforcing-tuple"}}
	reconciler.IdentityProber = fakeIdentityProber{matched: false, detail: "deployment-controller observed mallory"}

	if _, err := reconciler.reconcileIsolationActivation(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	receipt := reloadReceipt(t, kubeClient, cluster)
	if receipt.Phase != pgshardv1alpha1.IsolationActivatingConverge {
		t.Fatalf("identity mismatch advanced past converge: %#v", receipt)
	}
	reloaded := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), reloaded); err != nil {
		t.Fatal(err)
	}
	if meta.FindStatusCondition(reloaded.Status.Conditions, isolationControllerIdentityMismatchCond) == nil {
		t.Fatalf("identity mismatch did not surface its condition: %#v", reloaded.Status.Conditions)
	}
}

func TestDriveIsolationConvergeReListsLimitRangesAfterConvergence(t *testing.T) {
	t.Parallel()
	cluster := convergeCluster("limitconvergecase")
	// A LimitRange slipped in AFTER the INACTIVE preflight check but before the
	// gated LimitRange webhook was dispatching everywhere (the TOCTOU window). The
	// post-convergence re-list catches it; once convergence is proven no backend
	// admits a new one, so a clean re-list is durable.
	limitRange := &corev1.LimitRange{ObjectMeta: metav1.ObjectMeta{Name: "sneaky-defaults", Namespace: genTestNamespace}}
	reconciler, kubeClient := genReconciler(t, isolationNamespace("ns-uid"), cluster, limitRange)
	reconciler.DispatchProber = fakeDispatchProber{proof: dispatchProof{converged: true, backends: 2, tupleHash: "enforcing-tuple"}}
	reconciler.IdentityProber = fakeIdentityProber{matched: true}

	if _, err := reconciler.reconcileIsolationActivation(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	receipt := reloadReceipt(t, kubeClient, cluster)
	if receipt.Phase != pgshardv1alpha1.IsolationActivatingConverge {
		t.Fatalf("a live LimitRange did not hold the converge state: %#v", receipt)
	}
	reloaded := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), reloaded); err != nil {
		t.Fatal(err)
	}
	if meta.FindStatusCondition(reloaded.Status.Conditions, isolationLimitRangePresentCondition) == nil {
		t.Fatalf("post-convergence LimitRange re-list did not surface its condition: %#v", reloaded.Status.Conditions)
	}
}

func TestDriveIsolationQuiesceReListsLimitRangesAfterDrain(t *testing.T) {
	t.Parallel()
	// A LimitRange that skipped a stale backend's webhook just before convergence
	// can still COMMIT during the quiesce drain window. QUIESCE must re-list AFTER
	// the drain and HOLD (not advance to RECREATE) while one exists — otherwise its
	// defaulting would mutate every recreated pod after stamping and reject each
	// contract-checked replacement, deadlocking RECREATE.
	cluster := genCluster("quiescelimitcase", "quiescelimitcase-uid")
	cluster.Status.IsolationReceipt = &pgshardv1alpha1.PostgreSQLIsolationReceipt{
		NamespaceUID: "ns-uid", Phase: pgshardv1alpha1.IsolationActivatingQuiesce,
		ActivatedAt:   metav1.NewTime(time.Now().Add(-2 * supportingRevocationDrain)),
		SealedParents: []pgshardv1alpha1.SealedParent{{Kind: "StatefulSet", Name: "x", UID: "sts-uid"}},
	}
	sealedLive := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: genTestNamespace, UID: "sts-uid"}}
	sneaky := &corev1.LimitRange{ObjectMeta: metav1.ObjectMeta{Name: "sneaky-defaults", Namespace: genTestNamespace}}
	reconciler, kubeClient := genReconciler(t, isolationNamespace("ns-uid"), cluster, sealedLive, sneaky)
	reconciler.DispatchProber = convergedDispatch("")

	if _, err := reconciler.reconcileIsolationActivation(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if got := reloadReceipt(t, kubeClient, cluster).Phase; got != pgshardv1alpha1.IsolationActivatingQuiesce {
		t.Fatalf("QUIESCE advanced to %q despite a LimitRange landing during the drain", got)
	}
	reloaded := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), reloaded); err != nil {
		t.Fatal(err)
	}
	if meta.FindStatusCondition(reloaded.Status.Conditions, isolationLimitRangePresentCondition) == nil {
		t.Fatalf("post-drain LimitRange re-list did not surface its condition: %#v", reloaded.Status.Conditions)
	}

	// Once the LimitRange is removed the drain re-list is clean and QUIESCE advances.
	if err := kubeClient.Delete(context.Background(), sneaky); err != nil {
		t.Fatal(err)
	}
	fresh := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), fresh); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcileIsolationActivation(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	if got := reloadReceipt(t, kubeClient, cluster).Phase; got != pgshardv1alpha1.IsolationActivatingRecreate {
		t.Fatalf("QUIESCE did not advance after the LimitRange was removed: %q", got)
	}
}

func TestReconcileIsolationEnforcingLabelNormalizesBogusValue(t *testing.T) {
	t.Parallel()
	// A present-but-bogus value must NOT be treated as "absent" (which would let a
	// pre-seeded value survive and later wedge a bogus->enforced write). The
	// reconciler self-heals it: overwrite to the exact enforced value when
	// enforcing, remove it when not.
	enforcingNS := isolationNamespace("ns-uid")
	enforcingNS.Labels = map[string]string{podfence.NamespaceEnforcingLabel: "bogus"}
	reconciler, kubeClient := genReconciler(t, enforcingNS)
	if err := reconciler.reconcileIsolationEnforcingLabel(context.Background(), enforcingNS, true); err != nil {
		t.Fatal(err)
	}
	live := &corev1.Namespace{}
	if err := kubeClient.Get(context.Background(), client.ObjectKey{Name: genTestNamespace}, live); err != nil {
		t.Fatal(err)
	}
	if live.Labels[podfence.NamespaceEnforcingLabel] != podfence.NamespaceEnforcingLabelValue {
		t.Fatalf("enforcing reconcile did not normalize a bogus value to enforced: %#v", live.Labels)
	}

	emptyNS := isolationNamespace("ns-uid")
	emptyNS.Labels = map[string]string{podfence.NamespaceEnforcingLabel: ""}
	reconciler2, kubeClient2 := genReconciler(t, emptyNS)
	if err := reconciler2.reconcileIsolationEnforcingLabel(context.Background(), emptyNS, false); err != nil {
		t.Fatal(err)
	}
	live2 := &corev1.Namespace{}
	if err := kubeClient2.Get(context.Background(), client.ObjectKey{Name: genTestNamespace}, live2); err != nil {
		t.Fatal(err)
	}
	if _, present := live2.Labels[podfence.NamespaceEnforcingLabel]; present {
		t.Fatalf("non-enforcing reconcile left a present-but-empty value instead of removing it: %#v", live2.Labels)
	}
}

func TestDriveIsolationActiveReQuiescesOnBackendChange(t *testing.T) {
	t.Parallel()
	// A stale API-server backend is published after ACTIVE: the dispatch tuple
	// changes. The operator continuously re-proves while ACTIVE and, on the
	// detected change, immediately re-quiesces (fail-closed remediation limiting
	// exposure). This is the ratified immutable-membership constraint's enforcement.
	cluster := genCluster("membershipcase", "membershipcase-uid")
	cluster.Status.IsolationReceipt = &pgshardv1alpha1.PostgreSQLIsolationReceipt{
		NamespaceUID: "ns-uid", Phase: pgshardv1alpha1.IsolationActive, DispatchTupleHash: "tuple-old",
		SealedParents: []pgshardv1alpha1.SealedParent{{Kind: "StatefulSet", Name: "x", UID: "sts-uid"}},
	}
	reconciler, kubeClient := genReconciler(t, isolationNamespace("ns-uid"), cluster)
	// The prober now reports a DIFFERENT tuple (a newly published backend).
	reconciler.DispatchProber = fakeDispatchProber{proof: dispatchProof{converged: true, backends: 2, tupleHash: "tuple-new"}}

	if _, err := reconciler.reconcileIsolationActivation(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	receipt := reloadReceipt(t, kubeClient, cluster)
	if receipt.Phase != pgshardv1alpha1.IsolationActivatingQuiesce {
		t.Fatalf("ACTIVE did not re-quiesce on a backend-set change: %q", receipt.Phase)
	}
	if receipt.SealedParents != nil {
		t.Fatalf("re-quiesce did not clear the sealed parents for re-enumeration: %#v", receipt.SealedParents)
	}
}

func TestSupportingRollInProgressCoversIntentGap(t *testing.T) {
	t.Parallel()
	converged := genCluster("conv", "conv-uid")
	converged.Status.SupportingGenerations = []pgshardv1alpha1.SupportingGenerationStatus{{
		Class: "pooler", CurrentTemplateGeneration: 2, ConvergedGeneration: 2, MinGenerationForNewCreates: 2,
	}}
	if supportingRollInProgress(converged) {
		t.Fatal("a fully converged class was reported as rolling")
	}

	// SEALED INTENT before the new ReplicaSet exists (no prior UID yet): the
	// barrier advanced past the converged generation, so the roll is in progress.
	intent := genCluster("intent", "intent-uid")
	intent.Status.SupportingGenerations = []pgshardv1alpha1.SupportingGenerationStatus{{
		Class: "pooler", CurrentTemplateGeneration: 1, ConvergedGeneration: 1, MinGenerationForNewCreates: 2,
	}}
	if !supportingRollInProgress(intent) {
		t.Fatal("the intent→new-ReplicaSet gap was not counted as a roll in progress")
	}

	// Current template generation ahead of converged (rolling out).
	rolling := genCluster("roll", "roll-uid")
	rolling.Status.SupportingGenerations = []pgshardv1alpha1.SupportingGenerationStatus{{
		Class: "pooler", CurrentTemplateGeneration: 2, ConvergedGeneration: 1,
	}}
	if !supportingRollInProgress(rolling) {
		t.Fatal("an unconverged current generation was not counted as rolling")
	}
}

func TestDriveIsolationActiveWaitsForSupportingRoll(t *testing.T) {
	t.Parallel()
	cluster := genCluster("activerollcase", "activerollcase-uid")
	cluster.Status.IsolationReceipt = &pgshardv1alpha1.PostgreSQLIsolationReceipt{
		NamespaceUID: "ns-uid", Phase: pgshardv1alpha1.IsolationActive, DispatchTupleHash: "",
	}
	// A supporting roll is in progress (bounded coexistence). driveIsolationActive
	// must WAIT — re-quiescing would freeze the very creates the CAS roll needs.
	cluster.Status.SupportingGenerations = []pgshardv1alpha1.SupportingGenerationStatus{{
		Class: "pooler", CurrentReplicaSetUID: "rs-b", PriorReplicaSetUID: "rs-a",
	}}
	reconciler, kubeClient := genReconciler(t, isolationNamespace("ns-uid"), cluster)
	reconciler.DispatchProber = convergedDispatch("")

	activating, err := reconciler.reconcileIsolationActivation(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if !activating {
		t.Fatal("ACTIVE with a supporting roll did not request a requeue to wait for convergence")
	}
	if receipt := reloadReceipt(t, kubeClient, cluster); receipt.Phase != pgshardv1alpha1.IsolationActive {
		t.Fatalf("ACTIVE re-quiesced during a legitimate supporting roll (circular wait): %q", receipt.Phase)
	}
}

func TestIsolationBuildAllowsActivationDefault(t *testing.T) {
	t.Parallel()
	if !isolationBuildAllowsActivation {
		t.Fatal("the default build must permit activation; the bridge ceiling is a build-tag opt-out")
	}
}
