package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/andrew01234567890/pgshard/operator/internal/podfence"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// backendProbe is the outcome of the sentinel dryRun create against one live
// API-server backend.
type backendProbe struct {
	sliceName        string
	sliceRV          string
	address          string
	port             int32
	sentinelObserved bool
	outcome          string
}

// aggregateDispatchProof folds the per-backend probe outcomes into a proof.
// Convergence requires at least one enumerated backend and the exact sentinel
// denial from EVERY one; an empty backend set is treated as unenumerable HA (the
// D8 envelope) rather than silently converged.
func aggregateDispatchProof(webhookConfigResourceVersion string, probes []backendProbe) dispatchProof {
	if len(probes) == 0 {
		return dispatchProof{
			tupleHash: dispatchTupleHash(webhookConfigResourceVersion, probes),
			converged: false,
			reason:    dispatchUnconvergedReasonUnsupportedHA,
		}
	}
	converged := true
	var diverged []string
	for _, probe := range probes {
		if !probe.sentinelObserved {
			converged = false
			diverged = append(diverged, fmt.Sprintf("%s(%s)", probe.address, probe.outcome))
		}
	}
	proof := dispatchProof{
		tupleHash: dispatchTupleHash(webhookConfigResourceVersion, probes),
		converged: converged,
		backends:  len(probes),
	}
	if !converged {
		proof.reason = fmt.Sprintf("API-server backend(s) did not dispatch the sentinel to the pgshard webhook: %s", strings.Join(diverged, ", "))
	}
	return proof
}

// dispatchTupleHash binds the proof to the exact backend tuple: the webhook-config
// resourceVersion plus each backend's EndpointSlice name, EndpointSlice
// resourceVersion, address, and port, in a deterministic order. Any change to the
// backend set or the config invalidates it.
func dispatchTupleHash(webhookConfigResourceVersion string, probes []backendProbe) string {
	rows := make([]string, 0, len(probes))
	for _, probe := range probes {
		rows = append(rows, fmt.Sprintf("%s|%s|%s|%d", probe.sliceName, probe.sliceRV, probe.address, probe.port))
	}
	sort.Strings(rows)
	sum := sha256.Sum256([]byte(webhookConfigResourceVersion + "\n" + strings.Join(rows, "\n")))
	return hex.EncodeToString(sum[:])
}

// dispatchProbeSentinelPod is the reserved sentinel the probe submits with
// dryRun=All to each backend; the PodCreate webhook always denies it with the
// exact sentinel message. It carries a restricted-Pod-Security-valid spec so
// admission does not reject it for PSA before the webhook runs, and a per-probe
// nonce name so it never collides with a real object.
func dispatchProbeSentinelPod(namespace, name string) *corev1.Pod {
	allowPrivilegeEscalation := false
	runAsNonRoot := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: map[string]string{podfence.DispatchProbeSentinelAnnotation: podfence.DispatchProbeSentinelValue},
		},
		Spec: corev1.PodSpec{
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot:   &runAsNonRoot,
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Containers: []corev1.Container{{
				Name:  "sentinel",
				Image: "pgshard/sentinel@sha256:" + strings.Repeat("0", 64),
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: &allowPrivilegeEscalation,
					RunAsNonRoot:             &runAsNonRoot,
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
					SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
				},
			}},
		},
	}
}

// serverDispatchProber is the real dispatch-convergence prober. It enumerates the
// live API-server backends from the `kubernetes` Service EndpointSlices and probes
// each backend endpoint directly over authenticated TLS with a dryRun sentinel.
//
// INTEGRATION-ONLY: the per-backend TLS dial to an individual endpoint IP cannot
// be exercised by the fake-client unit tests; the enumeration, tuple binding, and
// convergence aggregation are covered by aggregateDispatchProof/dispatchTupleHash
// unit tests, and the composition/invalidation by the fake prober tests. Behind a
// VIP that hides individual backends, enumeration returns them from the
// EndpointSlices; a provider that rewrites membership without EndpointSlice
// publication is outside the D8 support envelope and yields ha-unsupported.
type serverDispatchProber struct {
	reader                client.Reader
	baseConfig            *rest.Config
	webhookConfigName     string
	perBackendDialTimeout time.Duration
}

// NewServerDispatchProber builds the real prober. The operator namespace is no
// longer used (the probe runs in the activating cluster's fenced namespace,
// passed to Prove).
func NewServerDispatchProber(reader client.Reader, baseConfig *rest.Config, _ string, webhookConfigName string) *serverDispatchProber {
	return &serverDispatchProber{
		reader:                reader,
		baseConfig:            baseConfig,
		webhookConfigName:     webhookConfigName,
		perBackendDialTimeout: 5 * time.Second,
	}
}

func (p *serverDispatchProber) Prove(ctx context.Context, namespace string) (dispatchProof, error) {
	// A per-probe nonce name so the sentinel never collides with a real object; the
	// webhook recognizes the sentinel by annotation, not name.
	sentinelName := fmt.Sprintf("%s-%d", podfence.DispatchProbeSentinelName, time.Now().UnixNano())
	webhookConfig := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := p.reader.Get(ctx, client.ObjectKey{Name: p.webhookConfigName}, webhookConfig); err != nil {
		return dispatchProof{}, fmt.Errorf("read validating webhook configuration %q: %w", p.webhookConfigName, err)
	}
	slices := &discoveryv1.EndpointSliceList{}
	if err := p.reader.List(ctx, slices, client.InNamespace("default"), client.MatchingLabels{discoveryv1.LabelServiceName: "kubernetes"}); err != nil {
		return dispatchProof{}, fmt.Errorf("list kubernetes EndpointSlices: %w", err)
	}
	probes := []backendProbe{}
	for i := range slices.Items {
		slice := &slices.Items[i]
		port := int32(443)
		for _, servicePort := range slice.Ports {
			if servicePort.Port != nil {
				port = *servicePort.Port
			}
		}
		for _, endpoint := range slice.Endpoints {
			if endpoint.Conditions.Ready != nil && !*endpoint.Conditions.Ready {
				continue
			}
			for _, address := range endpoint.Addresses {
				probe := backendProbe{sliceName: slice.Name, sliceRV: slice.ResourceVersion, address: address, port: port}
				p.probeBackend(ctx, namespace, sentinelName, &probe)
				probes = append(probes, probe)
			}
		}
	}
	proof := aggregateDispatchProof(webhookConfig.ResourceVersion, probes)
	// Belt: the dryRun sentinel must never have persisted.
	if err := p.confirmSentinelAbsent(ctx, namespace, sentinelName); err != nil {
		return dispatchProof{}, err
	}
	return proof, nil
}

// probeBackend addresses one backend endpoint directly over TLS with
// serverName=kubernetes.default.svc and submits a dryRun sentinel Pod create in
// the fenced namespace, recording whether the exact sentinel denial came back.
func (p *serverDispatchProber) probeBackend(ctx context.Context, namespace, sentinelName string, probe *backendProbe) {
	config := rest.CopyConfig(p.baseConfig)
	config.Host = "https://" + net.JoinHostPort(probe.address, strconv.Itoa(int(probe.port)))
	config.TLSClientConfig.ServerName = "kubernetes.default.svc"
	config.Timeout = p.perBackendDialTimeout
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		probe.outcome = fmt.Sprintf("client: %v", err)
		return
	}
	dialCtx, cancel := context.WithTimeout(ctx, p.perBackendDialTimeout)
	defer cancel()
	_, err = clientset.CoreV1().Pods(namespace).Create(dialCtx, dispatchProbeSentinelPod(namespace, sentinelName), metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	if err == nil {
		probe.outcome = "admitted"
		return
	}
	if isExactSentinelDenial(err) {
		probe.sentinelObserved = true
		probe.outcome = "sentinel"
		return
	}
	probe.outcome = fmt.Sprintf("other: %v", err)
}

// isExactSentinelDenial reports whether an error is exactly the pgshard PodCreate
// webhook's sentinel denial: a Forbidden status whose message is the API server's
// webhook-denial wrapper around the exact sentinel message, from the PodCreate
// webhook. An arbitrary denial that merely contains the sentinel text as a
// substring does not qualify, so a backend that denies for another reason is
// correctly counted as unconverged.
func isExactSentinelDenial(err error) bool {
	var status apierrors.APIStatus
	if !errors.As(err, &status) {
		return false
	}
	details := status.Status()
	if details.Reason != metav1.StatusReasonForbidden || details.Code != 403 {
		return false
	}
	expected := fmt.Sprintf("admission webhook %q denied the request: %s", podfence.PodCreateWebhookName, podfence.DispatchProbeSentinelMessage)
	return details.Message == expected
}

func (p *serverDispatchProber) confirmSentinelAbsent(ctx context.Context, namespace, sentinelName string) error {
	sentinel := &corev1.Pod{}
	err := p.reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: sentinelName}, sentinel)
	if err == nil {
		return fmt.Errorf("dispatch-probe sentinel unexpectedly persisted in %s", namespace)
	}
	if apierrors.IsNotFound(err) {
		return nil
	}
	return fmt.Errorf("confirm dispatch-probe sentinel absence: %w", err)
}
