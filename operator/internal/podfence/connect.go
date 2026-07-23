package podfence

import (
	"context"
	"fmt"
	"net/http"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const (
	managerNameLabel      = "app.kubernetes.io/name"
	managerNameValue      = "pgshard-operator"
	managerComponentLabel = "app.kubernetes.io/component"
	managerComponentValue = "controller-manager"

	// ManagerServiceAccountName is the deployed service account of the
	// controller-manager Pod (kustomize namePrefix pgshard-).
	ManagerServiceAccountName = "pgshard-controller-manager"
)

// PodConnectDenyValidator refuses interactive Pod access (exec/attach/
// portforward/proxy) that would let a caller read a managed pod's mounted keys
// or the controller-manager's projected token. The admission object for a
// CONNECT is a Pod*Options with no pod labels and no old object, so selection is
// by namespace: a single handler serves two webhook entries.
//
// In a fenced workload namespace the CONNECT deny is PHASE-AWARE, consistent
// with the rest of the opt-in isolation model: pre-activation (INACTIVE, or no
// receipt) a fenced namespace behaves like a normal namespace and interactive
// access is ALLOWED (admin/debug/CI). Once isolation begins enforcing — CONVERGE,
// QUIESCE, RECREATE, or ACTIVE — interactive access is DENIED for the whole
// enforcing lifecycle, not only at ACTIVE. These subresources are long-running
// and admission is checked only at connection start, so a CONNECT admitted mid-
// ceremony could be retained past ACTIVE; denying from the first enforcing phase
// means pod recreation in RECREATE terminates any earlier stream and nothing can
// attach to a replacement.
//
// In the operator namespace only the controller-manager Pod is protected —
// ALWAYS, regardless of any workload namespace's activation phase — because it
// guards the manager service account's projected token. The handler
// authoritatively GETs the request-named target and denies when it is a manager
// Pod, leaving every other operator-namespace Pod unaffected. Kubelet
// liveness/readiness exec probes run through the CRI, not this API subresource,
// so probes are never affected.
type PodConnectDenyValidator struct {
	reader            client.Reader
	operatorNamespace string
}

func NewPodConnectDenyValidator(reader client.Reader, operatorNamespace string) *PodConnectDenyValidator {
	return &PodConnectDenyValidator{reader: reader, operatorNamespace: operatorNamespace}
}

func (v *PodConnectDenyValidator) Handle(ctx context.Context, request admission.Request) admission.Response {
	if request.Operation != admissionv1.Connect {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("unexpected Pod connect request %s", request.Operation))
	}
	// The dispatch-convergence sentinel is always denied FIRST, in every phase and
	// for both webhook entries: the pre-enforcement convergence probe issues a
	// CONNECT to the reserved sentinel Pod name against each API-server backend
	// and requires exactly this response to prove that backend dispatches this
	// label-gated webhook (a stale backend instead 404s the nonexistent pod).
	if request.Name == ConnectDispatchProbeSentinelName {
		return admission.Denied(ConnectDispatchProbeSentinelMessage)
	}
	if request.Namespace == v.operatorNamespace {
		return v.denyManagerConnect(ctx, request)
	}
	// The webhook's namespaceSelector restricts this entry to fenced workload
	// namespaces that are ALSO isolation-enforcing (any non-INACTIVE phase), so an
	// un-activated (INACTIVE / no-receipt) namespace never invokes it and its
	// honest flow and admin/CI debugging survive a manager restart exactly as
	// pre-isolation.
	//
	// Interactive access is denied for the WHOLE enforcing lifecycle — CONVERGE,
	// QUIESCE, RECREATE, and ACTIVE — not only ACTIVE. exec/attach/portforward/
	// proxy are long-running: Kubernetes evaluates admission only at connection
	// START and classifies these subresources as long-running (outside the
	// attested request timeout). If QUIESCE/RECREATE admitted a CONNECT, an
	// attacker could open a stream into a pod during the ceremony and RETAIN it
	// after ACTIVE. Denying from the first enforcing phase means any pre-CONVERGE
	// stream is terminated when its pod is UID-deleted and recreated in RECREATE,
	// and no new stream can attach at all. The namespace carries the enforcing
	// label from CONVERGE onward, so this webhook dispatches for every enforcing
	// phase; the phase check is the authoritative decision (defense in depth for
	// the at-most-one-reconcile label-propagation lag).
	receipt, err := namespaceIsolationReceipt(ctx, v.reader, request.Namespace)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	if isolationPhase(receipt) != pgshardv1alpha1.IsolationInactive {
		return admission.Denied("interactive access to a managed PostgreSQL Pod is not permitted while namespace isolation is activating or active")
	}
	return admission.Allowed("namespace isolation is not active; interactive access is permitted")
}

// denyManagerConnect resolves the request-named target Pod authoritatively and
// denies the CONNECT only when it is a controller-manager Pod. It fails closed:
// a resolution error other than NotFound denies, while a NotFound target (the
// CONNECT would fail regardless) is allowed.
func (v *PodConnectDenyValidator) denyManagerConnect(ctx context.Context, request admission.Request) admission.Response {
	pod := &corev1.Pod{}
	if err := v.reader.Get(ctx, types.NamespacedName{Namespace: request.Namespace, Name: request.Name}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return admission.Allowed("connect target Pod does not exist")
		}
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("read connect target Pod: %w", err))
	}
	if isManagerPod(pod) {
		return admission.Denied("interactive access to the pgshard controller-manager Pod is not permitted")
	}
	return admission.Allowed("connect target is not the pgshard controller-manager")
}

func isManagerPod(pod *corev1.Pod) bool {
	return pod.Labels[managerNameLabel] == managerNameValue &&
		pod.Labels[managerComponentLabel] == managerComponentValue &&
		pod.Spec.ServiceAccountName == ManagerServiceAccountName
}
