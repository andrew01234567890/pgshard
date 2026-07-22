package podfence

import (
	"context"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const testOperatorNamespace = "pgshard-system"

func connectRequest(namespace, name, subresource string) admission.Request {
	return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Connect, SubResource: subresource, Namespace: namespace, Name: name,
	}}
}

func managerPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pgshard-controller-manager-abcde", Namespace: testOperatorNamespace,
			Labels: map[string]string{managerNameLabel: managerNameValue, managerComponentLabel: managerComponentValue},
		},
		Spec: corev1.PodSpec{ServiceAccountName: ManagerServiceAccountName},
	}
}

func TestPodConnectDenyValidatorDeniesFencedNamespaceAccess(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	validator := NewPodConnectDenyValidator(fake.NewClientBuilder().WithScheme(scheme).Build(), testOperatorNamespace)
	for _, subresource := range []string{"exec", "attach", "portforward", "proxy"} {
		subresource := subresource
		t.Run(subresource, func(t *testing.T) {
			t.Parallel()
			response := validator.Handle(context.Background(), connectRequest("database", "example-shard-0000-0", subresource))
			if response.Allowed || response.Result == nil || !strings.Contains(response.Result.Message, "fenced namespace") {
				t.Fatalf("fenced %s connect response = %#v", subresource, response)
			}
		})
	}
}

func TestPodConnectDenyValidatorProtectsOnlyTheManagerPod(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	manager := managerPod()
	bystander := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "some-tenant-tool", Namespace: testOperatorNamespace,
			Labels: map[string]string{managerNameLabel: "other-operator", managerComponentLabel: managerComponentValue},
		},
		Spec: corev1.PodSpec{ServiceAccountName: "tenant"},
	}
	build := func(objects ...client.Object) *PodConnectDenyValidator {
		return NewPodConnectDenyValidator(fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build(), testOperatorNamespace)
	}

	for _, subresource := range []string{"exec", "attach", "portforward", "proxy"} {
		if response := build(manager, bystander).Handle(context.Background(), connectRequest(testOperatorNamespace, manager.Name, subresource)); response.Allowed ||
			!strings.Contains(response.Result.Message, "controller-manager Pod is not permitted") {
			t.Fatalf("manager %s connect accepted: %#v", subresource, response)
		}
	}

	if response := build(manager, bystander).Handle(context.Background(), connectRequest(testOperatorNamespace, bystander.Name, "exec")); !response.Allowed {
		t.Fatalf("non-manager operator-namespace connect denied: %#v", response.Result)
	}

	if response := build(manager).Handle(context.Background(), connectRequest(testOperatorNamespace, "ghost", "exec")); !response.Allowed {
		t.Fatalf("connect to a non-existent target denied: %#v", response.Result)
	}

	// A pod that merely borrows the manager labels but not the manager service
	// account is not the manager and stays reachable.
	decoy := managerPod()
	decoy.Name = "decoy"
	decoy.Spec.ServiceAccountName = "tenant"
	if response := build(decoy).Handle(context.Background(), connectRequest(testOperatorNamespace, decoy.Name, "exec")); !response.Allowed {
		t.Fatalf("label-only decoy connect denied: %#v", response.Result)
	}
}

func TestPodConnectDenyValidatorIgnoresNonConnectOperations(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	validator := NewPodConnectDenyValidator(fake.NewClientBuilder().WithScheme(scheme).Build(), testOperatorNamespace)
	// Kubelet liveness/readiness exec probes run through the CRI, not the
	// pods/exec API subresource, so this webhook never sees them; only genuine
	// CONNECT admission is handled, and anything else is a bad request.
	request := connectRequest("database", "example-shard-0000-0", "exec")
	request.Operation = admissionv1.Update
	if response := validator.Handle(context.Background(), request); response.Allowed || response.Result == nil || response.Result.Code != 400 {
		t.Fatalf("non-connect operation response = %#v", response)
	}
}
