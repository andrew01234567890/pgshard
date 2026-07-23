package podfence

import (
	"context"
	"strings"
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func TestIdentityObservationStoreRegistrationAndConflicts(t *testing.T) {
	t.Parallel()
	store := NewIdentityObservationStore()
	stsKey := IdentityOwnerKey("StatefulSet", "probe-sts")
	deployKey := IdentityOwnerKey("Deployment", "probe-deploy")
	store.Register("tok", stsKey, deployKey)

	// Unregistered token and unregistered owner are both rejected.
	store.record("other", IdentityRoleStatefulSet, "attacker", stsKey)
	store.record("tok", IdentityRoleStatefulSet, "attacker", IdentityOwnerKey("StatefulSet", "forged"))
	if observed, _ := store.Observed("tok"); len(observed) != 0 {
		t.Fatalf("an unverified observation was recorded: %#v", observed)
	}

	store.record("tok", IdentityRoleStatefulSet, "system:node:worker", stsKey)
	store.record("tok", IdentityRoleHPA, "system:serviceaccount:kube-system:horizontal-pod-autoscaler", deployKey)
	observed, conflicted := store.Observed("tok")
	if conflicted || observed[IdentityRoleStatefulSet] != "system:node:worker" || observed[IdentityRoleHPA] == "" {
		t.Fatalf("store did not record verified observations: %#v conflicted=%v", observed, conflicted)
	}

	// Append-only: a later differing verified write never overwrites; it marks the
	// role conflicted, which fails the probe closed.
	store.record("tok", IdentityRoleStatefulSet, "system:serviceaccount:default:attacker", stsKey)
	observed, conflicted = store.Observed("tok")
	if observed[IdentityRoleStatefulSet] != "system:node:worker" {
		t.Fatalf("a later writer overwrote a verified observation: %#v", observed)
	}
	if !conflicted {
		t.Fatal("conflicting verified observations were not flagged")
	}

	// The returned map is a copy: mutating it must not affect the store.
	observed[IdentityRoleStatefulSet] = "tampered"
	if fresh, _ := store.Observed("tok"); fresh[IdentityRoleStatefulSet] != "system:node:worker" {
		t.Fatal("Observed returned a live reference, not a copy")
	}
	store.Forget("tok")
	if fresh, _ := store.Observed("tok"); len(fresh) != 0 {
		t.Fatal("Forget did not drop the token")
	}
}

func TestPodCreateRecordsStatefulSetIdentityProbe(t *testing.T) {
	t.Parallel()
	scheme := workloadScheme(t)
	store := NewIdentityObservationStore()
	store.Register("tok", IdentityOwnerKey("StatefulSet", "pgshard-idprobe-sts-tok"))
	liveProbeSTS := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: "pgshard-idprobe-sts-tok", Namespace: testWorkloadNS, UID: "sts-uid",
		Annotations: map[string]string{IdentityProbeAnnotation: "tok"},
	}}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testWorkloadCluster(), liveProbeSTS).Build()
	validator := NewPodCreateValidator(reader, testControllerIdentities(), scheme).WithIdentityProbeStore(store)

	controller := true
	probePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pgshard-idprobe-sts-tok-0", Namespace: testWorkloadNS,
			Annotations:     map[string]string{IdentityProbeAnnotation: "tok"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "pgshard-idprobe-sts-tok", UID: "sts-uid", Controller: &controller}},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "probe", Image: "registry.k8s.io/pause:3.9"}}},
	}
	request := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Name: probePod.Name, Namespace: testWorkloadNS, Operation: admissionv1.Create,
		Object:   runtime.RawExtension{Raw: marshalObject(t, probePod)},
		UserInfo: authenticationv1.UserInfo{Username: testControllerIdentities().StatefulSetController},
	}}
	if response := validator.Handle(context.Background(), request); !response.Allowed {
		t.Fatalf("INACTIVE namespace denied a benign probe pod: %#v", response)
	}
	observed, conflicted := store.Observed("tok")
	if conflicted || observed[IdentityRoleStatefulSet] != testControllerIdentities().StatefulSetController {
		t.Fatalf("statefulset-controller identity not recorded, got %#v", observed)
	}
}

func TestPodCreateRejectsForgedOwnerIdentityObservation(t *testing.T) {
	t.Parallel()
	scheme := workloadScheme(t)
	store := NewIdentityObservationStore()
	store.Register("tok", IdentityOwnerKey("StatefulSet", "pgshard-idprobe-sts-tok"))
	liveProbeSTS := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: "pgshard-idprobe-sts-tok", Namespace: testWorkloadNS, UID: "sts-uid",
		Annotations: map[string]string{IdentityProbeAnnotation: "tok"},
	}}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testWorkloadCluster(), liveProbeSTS).Build()
	validator := NewPodCreateValidator(reader, testControllerIdentities(), scheme).WithIdentityProbeStore(store)

	controller := true
	// An attacker forges an owner reference claiming the live probe StatefulSet
	// but with the WRONG UID (they cannot create the real object: the workload
	// webhook only admits operator-authored StatefulSets in a fenced namespace).
	forged := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "attacker-pod", Namespace: testWorkloadNS,
			Annotations:     map[string]string{IdentityProbeAnnotation: "tok"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "pgshard-idprobe-sts-tok", UID: "forged-uid", Controller: &controller}},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "x", Image: "attacker"}}},
	}
	request := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Name: forged.Name, Namespace: testWorkloadNS, Operation: admissionv1.Create,
		Object:   runtime.RawExtension{Raw: marshalObject(t, forged)},
		UserInfo: authenticationv1.UserInfo{Username: "system:serviceaccount:default:attacker"},
	}}
	validator.Handle(context.Background(), request)
	if observed, _ := store.Observed("tok"); len(observed) != 0 {
		t.Fatalf("a forged owner reference poisoned the identity observations: %#v", observed)
	}
}

func TestPodCreateIgnoresIdentityObservationFromNonControllerUsername(t *testing.T) {
	t.Parallel()
	scheme := workloadScheme(t)
	store := NewIdentityObservationStore()
	store.Register("tok", IdentityOwnerKey("StatefulSet", "pgshard-idprobe-sts-tok"))
	liveProbeSTS := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: "pgshard-idprobe-sts-tok", Namespace: testWorkloadNS, UID: "sts-uid",
		Annotations: map[string]string{IdentityProbeAnnotation: "tok"},
	}}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testWorkloadCluster(), liveProbeSTS).Build()
	validator := NewPodCreateValidator(reader, testControllerIdentities(), scheme).WithIdentityProbeStore(store)

	controller := true
	// An attacker who READ the live probe object presents the CORRECT owner name +
	// UID + token, but authenticates as a non-controller principal. Recording is
	// gated on the authenticated username equalling the configured statefulset
	// controller, so the attacker cannot poison (or, via conflict, permanently
	// block) the observation — a DoS on activation.
	poison := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "attacker-pod", Namespace: testWorkloadNS,
			Annotations:     map[string]string{IdentityProbeAnnotation: "tok"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "pgshard-idprobe-sts-tok", UID: "sts-uid", Controller: &controller}},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "x", Image: "attacker"}}},
	}
	request := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Name: poison.Name, Namespace: testWorkloadNS, Operation: admissionv1.Create,
		Object:   runtime.RawExtension{Raw: marshalObject(t, poison)},
		UserInfo: authenticationv1.UserInfo{Username: "system:serviceaccount:default:attacker"},
	}}
	validator.Handle(context.Background(), request)
	if observed, conflicted := store.Observed("tok"); len(observed) != 0 || conflicted {
		t.Fatalf("a non-controller username poisoned the identity observation: observed=%#v conflicted=%v", observed, conflicted)
	}

	// The genuine statefulset controller referencing the same live probe object IS
	// recorded, proving the gate is not over-broad.
	genuine := poison.DeepCopy()
	genuine.Name = "pgshard-idprobe-sts-tok-0"
	genuineRequest := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Name: genuine.Name, Namespace: testWorkloadNS, Operation: admissionv1.Create,
		Object:   runtime.RawExtension{Raw: marshalObject(t, genuine)},
		UserInfo: authenticationv1.UserInfo{Username: testControllerIdentities().StatefulSetController},
	}}
	validator.Handle(context.Background(), genuineRequest)
	if observed, conflicted := store.Observed("tok"); observed[IdentityRoleStatefulSet] != testControllerIdentities().StatefulSetController || conflicted {
		t.Fatalf("the genuine statefulset controller observation was not recorded: observed=%#v conflicted=%v", observed, conflicted)
	}
}

func TestWorkloadIntegrityRecordsDeploymentIdentityProbe(t *testing.T) {
	t.Parallel()
	scheme := workloadScheme(t)
	store := NewIdentityObservationStore()
	store.Register("tok", IdentityOwnerKey("Deployment", "pgshard-idprobe-deploy-tok"))
	liveProbeDeployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name: "pgshard-idprobe-deploy-tok", Namespace: testWorkloadNS, UID: "deploy-uid",
		Annotations: map[string]string{IdentityProbeAnnotation: "tok"},
	}}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testWorkloadCluster(), liveProbeDeployment).Build()
	validator := NewWorkloadIntegrityValidator(reader, testControllerIdentities(), scheme).WithIdentityProbeStore(store)

	controller := true
	replicaSet := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pgshard-idprobe-deploy-tok-abc", Namespace: testWorkloadNS,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "pgshard-idprobe-deploy-tok", UID: "deploy-uid", Controller: &controller}},
		},
		Spec: appsv1.ReplicaSetSpec{
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{IdentityProbeAnnotation: "tok"}}},
		},
	}
	request := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Name: replicaSet.Name, Namespace: testWorkloadNS, Operation: admissionv1.Create,
		Resource: metav1.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"},
		Object:   runtime.RawExtension{Raw: marshalObject(t, replicaSet)},
		UserInfo: authenticationv1.UserInfo{Username: testControllerIdentities().DeploymentController},
	}}
	if response := validator.Handle(context.Background(), request); !response.Allowed {
		t.Fatalf("probe ReplicaSet by the deployment controller was denied: %#v", response)
	}
	observed, conflicted := store.Observed("tok")
	if conflicted || observed[IdentityRoleDeployment] != testControllerIdentities().DeploymentController {
		t.Fatalf("deployment-controller identity not recorded, got %#v", observed)
	}
}

func TestHandleScaleGatesSupportingScaleToOperatorOrHPA(t *testing.T) {
	t.Parallel()
	scheme := workloadScheme(t)
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testWorkloadCluster()).Build()
	identities := testControllerIdentities()
	identities.HorizontalPodAutoscalerController = "system:serviceaccount:kube-system:horizontal-pod-autoscaler"
	validator := NewWorkloadIntegrityValidator(reader, identities, scheme)

	scaleRequest := func(username string) admission.Request {
		return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
			Name: "example-pooler", Namespace: testWorkloadNS, Operation: admissionv1.Update, SubResource: "scale",
			Resource: metav1.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
			UserInfo: authenticationv1.UserInfo{Username: username},
		}}
	}

	if response := validator.Handle(context.Background(), scaleRequest("system:serviceaccount:default:attacker")); response.Allowed {
		t.Fatal("an arbitrary caller was allowed to rescale a supporting Deployment (revoked-generation revival)")
	}
	if response := validator.Handle(context.Background(), scaleRequest(identities.Operator)); !response.Allowed {
		t.Fatalf("the operator was denied a supporting scale: %#v", response)
	}
	if response := validator.Handle(context.Background(), scaleRequest(identities.HorizontalPodAutoscalerController)); !response.Allowed {
		t.Fatalf("the HPA controller was denied a supporting scale: %#v", response)
	}
}

func TestHandleScaleFreezesHPADuringActivation(t *testing.T) {
	t.Parallel()
	scheme := workloadScheme(t)
	identities := testControllerIdentities()
	identities.HorizontalPodAutoscalerController = "system:serviceaccount:kube-system:horizontal-pod-autoscaler"

	scaleRequest := func(username string) admission.Request {
		return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
			Name: "example-pooler", Namespace: testWorkloadNS, Operation: admissionv1.Update, SubResource: "scale",
			Resource: metav1.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
			UserInfo: authenticationv1.UserInfo{Username: username},
		}}
	}

	// During QUIESCE and RECREATE an HPA scale is DENIED — a scale would bump the
	// sealed Deployment generation, drift the sealed parent, and livelock the
	// ceremony. The operator is never frozen.
	for _, phase := range []pgshardv1alpha1.IsolationPhase{pgshardv1alpha1.IsolationActivatingQuiesce, pgshardv1alpha1.IsolationActivatingRecreate} {
		reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(isolationReceiptCluster(phase)).Build()
		validator := NewWorkloadIntegrityValidator(reader, identities, scheme)
		if response := validator.Handle(context.Background(), scaleRequest(identities.HorizontalPodAutoscalerController)); response.Allowed ||
			!strings.Contains(response.Result.Message, "HorizontalPodAutoscaler scaling is suspended") {
			t.Fatalf("HPA scale during %s was not frozen: %#v", phase, response)
		}
		// The operator (draining a prior ReplicaSet) is never frozen.
		if response := validator.Handle(context.Background(), scaleRequest(identities.Operator)); !response.Allowed {
			t.Fatalf("the operator was frozen out of a supporting scale during %s: %#v", phase, response)
		}
	}

	// ACTIVE (and un-activated) permit HPA scaling again.
	for _, cluster := range []*pgshardv1alpha1.PgShardCluster{isolationReceiptCluster(pgshardv1alpha1.IsolationActive), testWorkloadCluster()} {
		reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
		validator := NewWorkloadIntegrityValidator(reader, identities, scheme)
		if response := validator.Handle(context.Background(), scaleRequest(identities.HorizontalPodAutoscalerController)); !response.Allowed {
			t.Fatalf("HPA scale outside the ceremony was denied: %#v", response)
		}
	}
}

func TestEnforceIsolationGenerationFloorIsPerClass(t *testing.T) {
	t.Parallel()
	// The receipt raised only the pooler floor to 2 (a benign pooler bump); the
	// member floor stays 1. A member pod at generation 1 must NOT be rejected
	// against the pooler's floor — each pod is compared only with its own floor.
	receipt := &pgshardv1alpha1.PostgreSQLIsolationReceipt{
		SecurityFloors: []pgshardv1alpha1.IsolationSecurityFloor{
			{Component: "pooler", MinGeneration: 2},
			{Component: "postgresql", Shard: 0, Member: 0, MinGeneration: 1},
		},
	}
	member := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Labels:      map[string]string{owned.ClusterLabel: testClusterName, owned.ComponentLabel: "postgresql", owned.ShardLabel: "0000", owned.MemberLabel: "0000"},
		Annotations: map[string]string{owned.PodSecurityGenerationAnnotation: "1"},
	}}
	if response := enforceIsolationGenerationFloor(member, receipt); response != nil {
		t.Fatalf("a member at its own floor (1) was rejected against another class's floor: %#v", response)
	}

	// A pooler pod at generation 1 IS below its own floor (2) and is denied.
	pooler := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Labels:      map[string]string{owned.ClusterLabel: testClusterName, owned.ComponentLabel: "pooler"},
		Annotations: map[string]string{owned.PodSecurityGenerationAnnotation: "1"},
	}}
	if response := enforceIsolationGenerationFloor(pooler, receipt); response == nil || response.Allowed {
		t.Fatalf("a pooler pod below its own floor was admitted: %#v", response)
	}
}

// The operator's supporting-revocation ceremony drains the prior ReplicaSet
// through the /scale SUBRESOURCE. This regression drives the actual webhook
// handler with the exact admission requests the API server generates for the
// operator identity: the /scale path must admit the complete ceremony, and the
// main-resource path (the old code path) must deny the operator — proving the
// subresource route is required, not optional.
func TestOperatorRevocationCeremonyAdmittedViaScaleSubresource(t *testing.T) {
	t.Parallel()
	scheme := workloadScheme(t)
	identities := testControllerIdentities()
	controller := true
	priorReplicaSet := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "example-pooler-prior", Namespace: testWorkloadNS, UID: "prior-rs-uid",
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "example-pooler", UID: "deploy-uid", Controller: &controller}},
		},
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testWorkloadCluster(), priorReplicaSet).Build()
	validator := NewWorkloadIntegrityValidator(reader, identities, scheme)

	scaleToZero := validator.Handle(context.Background(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Name: priorReplicaSet.Name, Namespace: testWorkloadNS, Operation: admissionv1.Update, SubResource: "scale",
		Resource: metav1.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"},
		UserInfo: authenticationv1.UserInfo{Username: identities.Operator},
	}})
	if !scaleToZero.Allowed {
		t.Fatalf("the operator's /scale drain of the prior ReplicaSet was denied (the ceremony would deadlock): %#v", scaleToZero)
	}

	mainResource := validator.Handle(context.Background(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Name: priorReplicaSet.Name, Namespace: testWorkloadNS, Operation: admissionv1.Update,
		Resource:  metav1.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"},
		Object:    runtime.RawExtension{Raw: marshalObject(t, priorReplicaSet)},
		OldObject: runtime.RawExtension{Raw: marshalObject(t, priorReplicaSet)},
		UserInfo:  authenticationv1.UserInfo{Username: identities.Operator},
	}})
	if mainResource.Allowed {
		t.Fatal("a main-resource ReplicaSet update by the operator was allowed; the deployment-controller authorship gate is gone")
	}
}
