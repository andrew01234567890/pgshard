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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// backendProbe is the outcome of the sentinel probes against one live
// API-server backend.
type backendProbe struct {
	sliceName        string
	sliceRV          string
	address          string
	port             int32
	sentinelObserved bool
	outcome          string
}

// The proof mode is folded into the tuple hash so the two proofs can never
// satisfy each other: a base proof (always-on PodCreate sentinel only, run
// before the enforcing label exists) can never stand in for an enforcing proof
// (which additionally proves each LABEL-GATED isolation webhook dispatches),
// and vice versa.
const (
	dispatchProbeModeBase      = "base"
	dispatchProbeModeEnforcing = "enforcing"
)

// aggregateDispatchProof folds the per-backend probe outcomes into a proof.
// Convergence requires at least one enumerated backend and the exact sentinel
// denial set from EVERY one; an empty backend set is treated as unenumerable HA
// (the D8 envelope) rather than silently converged.
func aggregateDispatchProof(mode, webhookConfigResourceVersion string, probes []backendProbe) dispatchProof {
	if len(probes) == 0 {
		return dispatchProof{
			tupleHash: dispatchTupleHash(mode, webhookConfigResourceVersion, probes),
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
		tupleHash: dispatchTupleHash(mode, webhookConfigResourceVersion, probes),
		converged: converged,
		backends:  len(probes),
	}
	if !converged {
		proof.reason = fmt.Sprintf("API-server backend(s) did not dispatch every sentinel to the pgshard webhooks: %s", strings.Join(diverged, ", "))
	}
	return proof
}

// dispatchTupleHash binds the proof to the exact backend tuple: the probe mode,
// the webhook-config resourceVersion, plus each backend's EndpointSlice name,
// EndpointSlice resourceVersion, address, and port, in a deterministic order.
// Any change to the mode, backend set, or config invalidates it.
func dispatchTupleHash(mode, webhookConfigResourceVersion string, probes []backendProbe) string {
	rows := make([]string, 0, len(probes))
	for _, probe := range probes {
		rows = append(rows, fmt.Sprintf("%s|%s|%s|%d", probe.sliceName, probe.sliceRV, probe.address, probe.port))
	}
	sort.Strings(rows)
	sum := sha256.Sum256([]byte(mode + "\n" + webhookConfigResourceVersion + "\n" + strings.Join(rows, "\n")))
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

// dispatchProbeSentinelDeployment is the reserved dryRun sentinel the ENFORCING
// probe submits per backend to prove the label-gated workload-integrity webhook
// dispatches for the enforcing namespace; the webhook always denies it with the
// exact workload sentinel message before any other check. It is a fully valid
// (schema-passing) Deployment so the request reaches validating admission.
func dispatchProbeSentinelDeployment(namespace, name string) *appsv1.Deployment {
	labels := map[string]string{"pgshard.io/dispatch-probe": "sentinel"}
	pod := dispatchProbeSentinelPod(namespace, name)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: map[string]string{podfence.DispatchProbeSentinelAnnotation: podfence.DispatchProbeSentinelValue},
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       pod.Spec,
			},
		},
	}
}

// dispatchProbeSentinelLimitRange is the reserved dryRun sentinel proving the
// label-gated LimitRange webhook dispatches; the webhook denies every LimitRange
// but answers the sentinel with its distinct exact message.
func dispatchProbeSentinelLimitRange(namespace, name string) *corev1.LimitRange {
	return &corev1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: map[string]string{podfence.DispatchProbeSentinelAnnotation: podfence.DispatchProbeSentinelValue},
		},
		Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
			Type: corev1.LimitTypeContainer,
			Max:  corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
		}}},
	}
}

// RATIFIED SUPPORT CONSTRAINT — IMMUTABLE CONTROL-PLANE MEMBERSHIP FOR THE WHOLE
// ACTIVE LIFETIME.
//
// The dispatch proof is a point-in-time enumeration + per-backend sentinel probe.
// It proves that every CURRENTLY-published API-server backend routes Pod CREATE
// to this webhook. It CANNOT prevent a NEW backend (e.g. an initialized-but-
// admission-stale API server) from being published and serving one preassigned-
// nodeName Pod CREATE before the operator reacts — an admission gate is
// necessarily downstream of routing, so a newly published stale backend leaves a
// brief bypass window, and re-quiescing afterward cannot undo any key disclosure.
//
// The operator mitigates this best-effort and FAILS CLOSED on detection: the
// EndpointSlice + ValidatingWebhookConfiguration WATCHES wake a reconcile on any
// change, driveIsolationActive re-proves the tuple EVERY reconcile while ACTIVE,
// and any backend-set/RV change immediately re-quiesces (driveIsolation* →
// revalidateDispatchTuple), dropping enforcement to the durable deny phase and
// limiting exposure to the detection latency. A ≤1-backend enumeration that
// cannot be proven complete (opaque VIP / single published address) is refused
// unless the admin attests the namespace via
// --allow-unenumerable-ha-isolation-namespaces.
//
// Because the residual window cannot be closed in-band, immutable control-plane
// API-server membership for the ENTIRE ACTIVE lifetime is a RATIFIED support
// constraint (documented on the flag): control-plane upgrades/scaling while
// ACTIVE are unsupported and trigger a re-quiesce (availability), with a brief
// worst-case exposure between publication and detection.
//
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

// Prove is the BASE proof: it proves every live backend dispatches Pod CREATE to
// the always-on PodCreate webhook. It runs during the INACTIVE preflight, before
// the isolation-enforcing label exists, so it deliberately does NOT probe the
// label-gated webhooks (they cannot dispatch yet by design).
func (p *serverDispatchProber) Prove(ctx context.Context, namespace string) (dispatchProof, error) {
	return p.prove(ctx, namespace, false)
}

// ProveEnforcing is the ENFORCING proof: per backend, it additionally requires
// the exact sentinel denial from each LABEL-GATED isolation webhook — workload
// integrity (dryRun sentinel Deployment), LimitRange (dryRun sentinel
// LimitRange), and connect (a CONNECT to the reserved sentinel Pod name, which
// only a dispatching backend can deny). A backend whose namespace-informer cache
// has not yet observed the enforcing label skips those webhooks and fails its
// probes, so the proof converges only when EVERY backend genuinely dispatches
// them for the enforcing namespace. It drives the pre-enforcement CONVERGE state
// and every in-activation revalidation (QUIESCE/RECREATE/ACTIVE), where the
// label must remain both set and effective.
func (p *serverDispatchProber) ProveEnforcing(ctx context.Context, namespace string) (dispatchProof, error) {
	return p.prove(ctx, namespace, true)
}

func (p *serverDispatchProber) prove(ctx context.Context, namespace string, enforcing bool) (dispatchProof, error) {
	// A per-probe nonce name so the sentinel never collides with a real object; the
	// webhooks recognize the sentinel by annotation, not name.
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
				p.probeBackend(ctx, namespace, sentinelName, &probe, enforcing)
				probes = append(probes, probe)
			}
		}
	}
	mode := dispatchProbeModeBase
	if enforcing {
		mode = dispatchProbeModeEnforcing
	}
	proof := aggregateDispatchProof(mode, webhookConfig.ResourceVersion, probes)
	// Belt: the dryRun sentinels must never have persisted.
	if err := p.confirmSentinelAbsent(ctx, namespace, sentinelName, enforcing); err != nil {
		return dispatchProof{}, err
	}
	return proof, nil
}

// probeBackend addresses one backend endpoint directly over TLS with
// serverName=kubernetes.default.svc and submits the sentinel set for the probe
// mode, recording whether EVERY exact sentinel denial came back.
func (p *serverDispatchProber) probeBackend(ctx context.Context, namespace, sentinelName string, probe *backendProbe, enforcing bool) {
	config := rest.CopyConfig(p.baseConfig)
	config.Host = "https://" + net.JoinHostPort(probe.address, strconv.Itoa(int(probe.port)))
	config.TLSClientConfig.ServerName = "kubernetes.default.svc"
	config.Timeout = p.perBackendDialTimeout
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		probe.outcome = fmt.Sprintf("client: %v", err)
		return
	}
	dryRun := metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}}
	type sentinelProbe struct {
		webhook string
		message string
		submit  func(context.Context) error
	}
	sentinels := []sentinelProbe{{
		webhook: podfence.PodCreateWebhookName,
		message: podfence.DispatchProbeSentinelMessage,
		submit: func(ctx context.Context) error {
			_, err := clientset.CoreV1().Pods(namespace).Create(ctx, dispatchProbeSentinelPod(namespace, sentinelName), dryRun)
			return err
		},
	}}
	if enforcing {
		sentinels = append(sentinels,
			sentinelProbe{
				webhook: podfence.WorkloadWebhookName,
				message: podfence.WorkloadDispatchProbeSentinelMessage,
				submit: func(ctx context.Context) error {
					_, err := clientset.AppsV1().Deployments(namespace).Create(ctx, dispatchProbeSentinelDeployment(namespace, sentinelName), dryRun)
					return err
				},
			},
			sentinelProbe{
				webhook: podfence.LimitRangeWebhookName,
				message: podfence.LimitRangeDispatchProbeSentinelMessage,
				submit: func(ctx context.Context) error {
					_, err := clientset.CoreV1().LimitRanges(namespace).Create(ctx, dispatchProbeSentinelLimitRange(namespace, sentinelName), dryRun)
					return err
				},
			},
			// CONNECT subresources cannot be dry-run; the reserved sentinel Pod name
			// is denied by the connect webhook in every phase, and never exists, so a
			// dispatching backend returns the exact denial while a stale backend
			// falls through to a NotFound for the nonexistent pod.
			sentinelProbe{
				webhook: podfence.PodConnectFencedWebhookName,
				message: podfence.ConnectDispatchProbeSentinelMessage,
				submit: func(ctx context.Context) error {
					return clientset.CoreV1().RESTClient().Post().
						Resource("pods").Namespace(namespace).Name(podfence.ConnectDispatchProbeSentinelName).
						SubResource("exec").Do(ctx).Error()
				},
			},
		)
	}
	var failures []string
	for _, sentinel := range sentinels {
		dialCtx, cancel := context.WithTimeout(ctx, p.perBackendDialTimeout)
		err := sentinel.submit(dialCtx)
		cancel()
		switch {
		case err == nil:
			failures = append(failures, sentinel.webhook+": admitted")
		case !isExactWebhookSentinelDenial(err, sentinel.webhook, sentinel.message):
			failures = append(failures, fmt.Sprintf("%s: %v", sentinel.webhook, err))
		}
	}
	if len(failures) > 0 {
		probe.outcome = strings.Join(failures, "; ")
		return
	}
	probe.sentinelObserved = true
	probe.outcome = "sentinel"
}

// isExactWebhookSentinelDenial reports whether an error is exactly the named
// pgshard webhook's sentinel denial: a Forbidden status whose message is the API
// server's webhook-denial wrapper around the exact sentinel message. An
// arbitrary denial that merely contains the sentinel text as a substring does
// not qualify, so a backend that denies for another reason is correctly counted
// as unconverged.
func isExactWebhookSentinelDenial(err error, webhookName, sentinelMessage string) bool {
	var status apierrors.APIStatus
	if !errors.As(err, &status) {
		return false
	}
	details := status.Status()
	if details.Reason != metav1.StatusReasonForbidden || details.Code != 403 {
		return false
	}
	expected := fmt.Sprintf("admission webhook %q denied the request: %s", webhookName, sentinelMessage)
	return details.Message == expected
}

func (p *serverDispatchProber) confirmSentinelAbsent(ctx context.Context, namespace, sentinelName string, enforcing bool) error {
	holders := []client.Object{&corev1.Pod{}}
	if enforcing {
		holders = append(holders, &appsv1.Deployment{}, &corev1.LimitRange{})
	}
	for _, holder := range holders {
		err := p.reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: sentinelName}, holder)
		if err == nil {
			return fmt.Errorf("dispatch-probe sentinel %T unexpectedly persisted in %s", holder, namespace)
		}
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("confirm dispatch-probe sentinel absence: %w", err)
		}
	}
	return nil
}
