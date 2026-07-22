package podfence

import (
	"context"
	"fmt"
	"net/http"

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
// by namespace: a single handler serves two webhook entries. In a fenced
// workload namespace every CONNECT is denied unconditionally (the namespace is
// dedicated; break-glass is the node/kubelet TCB path or the activation recreate
// ceremony). In the operator namespace only the controller-manager Pod is
// protected — the handler authoritatively GETs the request-named target and
// denies when it is a manager Pod, leaving every other operator-namespace Pod
// unaffected. Kubelet liveness/readiness exec probes run through the CRI, not
// this API subresource, so probes are never affected.
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
	// namespaces, where interactive access is never allowed.
	return admission.Denied("interactive access to a managed PostgreSQL Pod in a fenced namespace is not permitted")
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
