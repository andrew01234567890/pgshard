package podfence

import (
	"context"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func TestIdentityObservationStoreRecordsAndForgets(t *testing.T) {
	t.Parallel()
	store := NewIdentityObservationStore()
	store.record("tok", IdentityRoleStatefulSet, "system:node:worker")
	store.record("tok", IdentityRoleHPA, "system:serviceaccount:kube-system:horizontal-pod-autoscaler")
	store.record("", IdentityRoleDeployment, "ignored")

	observed := store.Observed("tok")
	if observed[IdentityRoleStatefulSet] != "system:node:worker" || observed[IdentityRoleHPA] == "" {
		t.Fatalf("store did not record observations: %#v", observed)
	}
	// The returned map is a copy: mutating it must not affect the store.
	observed[IdentityRoleStatefulSet] = "tampered"
	if store.Observed("tok")[IdentityRoleStatefulSet] != "system:node:worker" {
		t.Fatal("Observed returned a live reference, not a copy")
	}
	store.Forget("tok")
	if len(store.Observed("tok")) != 0 {
		t.Fatal("Forget did not drop the token")
	}
}

func TestPodCreateRecordsStatefulSetIdentityProbe(t *testing.T) {
	t.Parallel()
	scheme := workloadScheme(t)
	store := NewIdentityObservationStore()
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testWorkloadCluster()).Build()
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
	if got := store.Observed("tok")[IdentityRoleStatefulSet]; got != testControllerIdentities().StatefulSetController {
		t.Fatalf("statefulset-controller identity not recorded, got %q", got)
	}
}

func TestWorkloadIntegrityRecordsDeploymentIdentityProbe(t *testing.T) {
	t.Parallel()
	scheme := workloadScheme(t)
	store := NewIdentityObservationStore()
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testWorkloadCluster()).Build()
	validator := NewWorkloadIntegrityValidator(reader, testControllerIdentities(), scheme).WithIdentityProbeStore(store)

	replicaSet := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: "pgshard-idprobe-deploy-tok-abc", Namespace: testWorkloadNS},
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
	if got := store.Observed("tok")[IdentityRoleDeployment]; got != testControllerIdentities().DeploymentController {
		t.Fatalf("deployment-controller identity not recorded, got %q", got)
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
