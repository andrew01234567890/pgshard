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
// access is ALLOWED (admin/debug/CI). The caller-path hardening is part of the
// ratified ACTIVE enforcement — CONNECT is denied only once the namespace's
// isolation is durably ACTIVE (the dedicated-namespace protection). QUIESCE and
// RECREATE also allow it (the ceremony recreates every protected pod, and admin
// access during the bounded transition is acceptable and matches the other
// phase-aware webhooks which are permissive until ACTIVE).
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
	if request.Namespace == v.operatorNamespace {
		return v.denyManagerConnect(ctx, request)
	}
	// The webhook's namespaceSelector restricts this entry to fenced workload
	// namespaces. Interactive access is denied ONLY once isolation is ACTIVE; an
	// un-activated (INACTIVE / QUIESCE / RECREATE / no-receipt) fenced namespace
	// permits it, so the honest un-activated flow and admin/CI debugging are not
	// blocked.
	receipt, err := namespaceIsolationReceipt(ctx, v.reader, request.Namespace)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	if isolationPhase(receipt) == pgshardv1alpha1.IsolationActive {
		return admission.Denied("interactive access to a managed PostgreSQL Pod is not permitted while namespace isolation is active")
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
