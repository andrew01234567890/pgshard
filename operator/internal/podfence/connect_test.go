package podfence

import (
	"context"
	"strings"
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
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

func TestPodConnectDenyValidatorAnswersDispatchProbeSentinelInEveryPhase(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	// The reserved sentinel Pod name is denied with the EXACT message in every
	// phase and in the operator namespace too — the per-backend convergence probe
	// distinguishes a dispatching backend (this denial) from a stale one (a
	// NotFound for the nonexistent pod), so the response must never depend on
	// phase or namespace.
	for _, phase := range []pgshardv1alpha1.IsolationPhase{
		pgshardv1alpha1.IsolationInactive,
		pgshardv1alpha1.IsolationActivatingConverge,
		pgshardv1alpha1.IsolationActive,
	} {
		builder := fake.NewClientBuilder().WithScheme(scheme)
		if phase != pgshardv1alpha1.IsolationInactive {
			builder = builder.WithObjects(isolationReceiptCluster(phase))
		}
		validator := NewPodConnectDenyValidator(builder.Build(), testOperatorNamespace)
		response := validator.Handle(context.Background(), connectRequest(testWorkloadNS, ConnectDispatchProbeSentinelName, "exec"))
		if response.Allowed || response.Result.Message != ConnectDispatchProbeSentinelMessage {
			t.Fatalf("connect dispatch-probe sentinel under %q = %#v", phase, response.Result)
		}
	}
	operatorEntry := NewPodConnectDenyValidator(fake.NewClientBuilder().WithScheme(scheme).Build(), testOperatorNamespace)
	response := operatorEntry.Handle(context.Background(), connectRequest(testOperatorNamespace, ConnectDispatchProbeSentinelName, "exec"))
	if response.Allowed || response.Result.Message != ConnectDispatchProbeSentinelMessage {
		t.Fatalf("operator-namespace connect dispatch-probe sentinel = %#v", response.Result)
	}
}

func TestPodConnectDenyValidatorDeniesFencedNamespaceOnlyWhenActive(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	for _, subresource := range []string{"exec", "attach", "portforward", "proxy"} {
		subresource := subresource
		t.Run(subresource, func(t *testing.T) {
			t.Parallel()
			// No isolation receipt (INACTIVE / un-activated fenced namespace): the
			// honest flow and admin/CI debugging must be permitted.
			inactive := NewPodConnectDenyValidator(fake.NewClientBuilder().WithScheme(scheme).Build(), testOperatorNamespace)
			if response := inactive.Handle(context.Background(), connectRequest(testWorkloadNS, "example-shard-0000-0", subresource)); !response.Allowed {
				t.Fatalf("un-activated fenced %s connect was denied: %#v", subresource, response.Result)
			}

			// QUIESCE and RECREATE are also permitted (the bounded transition).
			for _, phase := range []pgshardv1alpha1.IsolationPhase{pgshardv1alpha1.IsolationActivatingQuiesce, pgshardv1alpha1.IsolationActivatingRecreate} {
				cluster := isolationReceiptCluster(phase)
				validator := NewPodConnectDenyValidator(fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(), testOperatorNamespace)
				if response := validator.Handle(context.Background(), connectRequest(testWorkloadNS, "example-shard-0000-0", subresource)); !response.Allowed {
					t.Fatalf("fenced %s connect during %s was denied: %#v", subresource, phase, response.Result)
				}
			}

			// ACTIVE: the ratified dedicated-namespace protection denies interactive
			// access.
			active := isolationReceiptCluster(pgshardv1alpha1.IsolationActive)
			validator := NewPodConnectDenyValidator(fake.NewClientBuilder().WithScheme(scheme).WithObjects(active).Build(), testOperatorNamespace)
			if response := validator.Handle(context.Background(), connectRequest(testWorkloadNS, "example-shard-0000-0", subresource)); response.Allowed ||
				response.Result == nil || !strings.Contains(response.Result.Message, "isolation is active") {
				t.Fatalf("ACTIVE fenced %s connect response = %#v", subresource, response)
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
