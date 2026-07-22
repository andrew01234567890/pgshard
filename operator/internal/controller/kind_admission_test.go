package controller

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/operator/internal/pki"
	"github.com/andrew01234567890/pgshard/operator/internal/podfence"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

func TestKINDAdmissionWebhooksUseManagedTLSAndRejectUnsafeSpec(t *testing.T) {
	if os.Getenv("PGSHARD_KIND_MANAGER_E2E") != "true" {
		t.Skip("set PGSHARD_KIND_MANAGER_E2E=true against the installed admission manager")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := pgshardv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kubeClient, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}

	assertManagedAdmissionTLS(t, ctx, kubeClient)
	assertFencingKeyLossFailsReadiness(t, ctx, kubeClient)
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pgshard-admission-smoke-%d", os.Getpid())}}
	if err := kubeClient.Create(ctx, namespace); err != nil {
		t.Fatal(err)
	}
	deleteNamespaceAtCleanup(t, kubeClient, namespace)

	valid := readDevelopmentSample(t)
	valid.Namespace = namespace.Name
	valid.Spec.Shards = 0
	valid.Spec.MembersPerShard = 0
	valid.Spec.Durability = ""
	valid.Spec.PostgreSQL.Version = ""
	valid.Spec.Pooler = pgshardv1alpha1.PoolerSpec{}
	valid.Spec.Services = pgshardv1alpha1.ServiceSet{}
	valid.Spec.Observability.Prometheus = nil
	valid.Spec.Databases = []pgshardv1alpha1.DatabaseTemplate{
		{Name: "implicit"},
		{Name: "shards-only", Shards: 1},
	}
	valid = waitForDefaultedDryRunCreate(t, ctx, kubeClient, valid)
	if valid.Spec.Shards != 1 || valid.Spec.MembersPerShard != 3 || valid.Spec.Durability != pgshardv1alpha1.DurabilitySynchronous || valid.Spec.PostgreSQL.Version != pgshardv1alpha1.PostgreSQLMajor18 || valid.Spec.Storage.DeletionPolicy != pgshardv1alpha1.DeletionRetain || valid.Spec.Pooler.Scaling.Mode != pgshardv1alpha1.ScalingHPA || valid.Spec.Observability.Prometheus == nil || !*valid.Spec.Observability.Prometheus {
		t.Fatalf("admission defaults = %#v", valid.Spec)
	}
	for _, database := range valid.Spec.Databases {
		if database.Shards != 1 || len(database.Cells) != 1 || database.Cells[0] != 0 {
			t.Fatalf("database admission defaults = %#v", valid.Spec.Databases)
		}
	}

	invalid := readDevelopmentSample(t)
	invalid.Name = "unsafe-synchronous-singleton"
	invalid.Namespace = namespace.Name
	invalid.Spec.MembersPerShard = 1
	invalid.Spec.Durability = pgshardv1alpha1.DurabilitySynchronous
	err = kubeClient.Create(ctx, invalid)
	if err == nil || !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "synchronous durability requires at least 3 members per shard") {
		t.Fatalf("unsafe create error = %v", err)
	}

}

func TestKINDAdmissionRemainsAvailableDuringCompatibleManagerRestart(t *testing.T) {
	if os.Getenv("PGSHARD_KIND_MANAGER_E2E") != "true" {
		t.Skip("set PGSHARD_KIND_MANAGER_E2E=true against the installed admission manager")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	kubeClient := newKINDClient(t)

	before := waitForOnlyReadyManagerPod(t, ctx, kubeClient, "")
	if before.Spec.TerminationGracePeriodSeconds == nil || *before.Spec.TerminationGracePeriodSeconds != 20 ||
		len(before.Spec.Containers) != 1 || before.Spec.Containers[0].Lifecycle == nil ||
		before.Spec.Containers[0].Lifecycle.PreStop == nil || before.Spec.Containers[0].Lifecycle.PreStop.Sleep == nil ||
		before.Spec.Containers[0].Lifecycle.PreStop.Sleep.Seconds != 5 {
		t.Fatalf("manager Pod lacks the compatible-rollout drain contract: %#v", before.Spec)
	}

	probe := readDevelopmentSample(t)
	probe.Name = fmt.Sprintf("pgshard-admission-rollout-probe-%d", os.Getpid())
	probe.Namespace = "default"
	type probeResult struct {
		attempts int
		failures int
		firstErr error
	}
	started := make(chan struct{})
	rolloutFinished := make(chan struct{})
	result := make(chan probeResult, 1)
	go func() {
		observation := probeResult{}
		for {
			candidate := probe.DeepCopy()
			err := kubeClient.Create(ctx, candidate, &client.CreateOptions{DryRun: []string{metav1.DryRunAll}})
			observation.attempts++
			if err != nil {
				observation.failures++
				if observation.firstErr == nil {
					observation.firstErr = err
				}
			}
			if observation.attempts == 1 {
				close(started)
			}
			if observation.attempts >= 100 {
				select {
				case <-rolloutFinished:
					result <- observation
					return
				default:
				}
			}
			select {
			case <-ctx.Done():
				if observation.firstErr == nil {
					observation.firstErr = ctx.Err()
				}
				result <- observation
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()

	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	rolloutErr := restartManagerAndWait(ctx)
	close(rolloutFinished)
	observation := <-result
	if rolloutErr != nil {
		t.Fatal(rolloutErr)
	}
	if observation.attempts < 100 || observation.failures != 0 {
		t.Fatalf("admission during compatible manager restart: attempts=%d failures=%d first error=%v", observation.attempts, observation.failures, observation.firstErr)
	}

	after := waitForOnlyReadyManagerPod(t, ctx, kubeClient, before.UID)
	if after.Spec.Containers[0].Image != before.Spec.Containers[0].Image {
		t.Fatalf("compatible manager restart changed image %q -> %q", before.Spec.Containers[0].Image, after.Spec.Containers[0].Image)
	}
}

func restartManagerAndWait(ctx context.Context) error {
	for _, arguments := range [][]string{
		{"--namespace", "pgshard-system", "rollout", "restart", "deployment/pgshard-controller-manager"},
		{"--namespace", "pgshard-system", "rollout", "status", "deployment/pgshard-controller-manager", "--timeout=120s"},
	} {
		output, err := exec.CommandContext(ctx, "kubectl", arguments...).CombinedOutput()
		if err != nil {
			return fmt.Errorf("kubectl %s: %w: %s", strings.Join(arguments, " "), err, strings.TrimSpace(string(output)))
		}
	}
	return nil
}

func waitForOnlyReadyManagerPod(t *testing.T, ctx context.Context, kubeClient client.Client, previousUID types.UID) *corev1.Pod {
	t.Helper()
	pods := &corev1.PodList{}
	err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		pods = &corev1.PodList{}
		if err := kubeClient.List(ctx, pods,
			client.InNamespace("pgshard-system"),
			client.MatchingLabels{"app.kubernetes.io/name": "pgshard-operator", "app.kubernetes.io/component": "controller-manager"},
		); err != nil {
			return false, err
		}
		if len(pods.Items) != 1 || pods.Items[0].DeletionTimestamp != nil || pods.Items[0].UID == previousUID || len(pods.Items[0].Status.ContainerStatuses) != 1 {
			return false, nil
		}
		status := pods.Items[0].Status.ContainerStatuses[0]
		if status.RestartCount != 0 {
			return false, fmt.Errorf("manager Pod %s restarted %d times", pods.Items[0].Name, status.RestartCount)
		}
		return pods.Items[0].Status.Phase == corev1.PodRunning && status.Ready, nil
	})
	if err != nil {
		t.Fatalf("wait for compatible manager replacement: %v; last Pods = %#v", err, pods.Items)
	}
	return pods.Items[0].DeepCopy()
}

func waitForDefaultedDryRunCreate(
	t *testing.T,
	ctx context.Context,
	kubeClient client.Client,
	original *pgshardv1alpha1.PgShardCluster,
) *pgshardv1alpha1.PgShardCluster {
	t.Helper()
	var (
		created *pgshardv1alpha1.PgShardCluster
		lastErr error
	)
	err := wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		candidate := original.DeepCopy()
		lastErr = kubeClient.Create(ctx, candidate, &client.CreateOptions{DryRun: []string{metav1.DryRunAll}})
		if lastErr != nil {
			// Pod readiness can recover just before the API server observes the
			// Service endpoint again. Retry only control-plane availability errors;
			// deterministic admission denials must still fail immediately.
			if apierrors.IsInternalError(lastErr) || apierrors.IsServiceUnavailable(lastErr) ||
				apierrors.IsTimeout(lastErr) || apierrors.IsServerTimeout(lastErr) || apierrors.IsTooManyRequests(lastErr) {
				return false, nil
			}
			return false, lastErr
		}
		created = candidate
		return true, nil
	})
	if err != nil {
		t.Fatalf("wait for defaulted dry-run create after webhook readiness recovery: %v (last create error: %v)", err, lastErr)
	}
	return created
}

func assertFencingKeyLossFailsReadiness(t *testing.T, ctx context.Context, kubeClient client.Client) {
	t.Helper()
	manager := waitForOnlyReadyManagerPod(t, ctx, kubeClient, "")
	managerUID := manager.UID
	key := types.NamespacedName{Namespace: "pgshard-system", Name: "pgshard-webhook-fencing-key"}
	original := &corev1.Secret{}
	if err := kubeClient.Get(ctx, key, original); err != nil {
		t.Fatal(err)
	}
	valid := func() *corev1.Secret {
		immutable := true
		return &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   key.Namespace,
				Name:        key.Name,
				Labels:      maps.Clone(original.Labels),
				Annotations: maps.Clone(original.Annotations),
			},
			Type:      original.Type,
			Immutable: &immutable,
			Data:      map[string][]byte{pki.PodFencingKeyKey: bytes.Clone(original.Data[pki.PodFencingKeyKey])},
		}
	}
	replace := func(ctx context.Context, secret *corev1.Secret) {
		current := &corev1.Secret{}
		err := kubeClient.Get(ctx, key, current)
		if err == nil {
			if err := kubeClient.Delete(ctx, current); err != nil {
				t.Fatal(err)
			}
		} else if !apierrors.IsNotFound(err) {
			t.Fatal(err)
		}
		if secret != nil {
			if err := kubeClient.Create(ctx, secret); err != nil {
				t.Fatal(err)
			}
		}
	}
	dirty := false
	defer func() {
		if !dirty {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		replace(cleanupCtx, valid())
		waitForManagerReadiness(t, cleanupCtx, kubeClient, managerUID, true)
	}()

	dirty = true
	replace(ctx, nil)
	waitForManagerReadiness(t, ctx, kubeClient, managerUID, false)
	replace(ctx, valid())
	dirty = false
	waitForManagerReadiness(t, ctx, kubeClient, managerUID, true)

	malformed := valid()
	malformed.Data[pki.PodFencingKeyKey] = make([]byte, podfence.SecretKeyBytes-1)
	dirty = true
	replace(ctx, malformed)
	waitForManagerReadiness(t, ctx, kubeClient, managerUID, false)
	replace(ctx, valid())
	dirty = false
	waitForManagerReadiness(t, ctx, kubeClient, managerUID, true)

	different := valid()
	different.Data[pki.PodFencingKeyKey][0] ^= 0xff
	dirty = true
	replace(ctx, different)
	waitForManagerReadiness(t, ctx, kubeClient, managerUID, false)
	replace(ctx, valid())
	dirty = false
	waitForManagerReadiness(t, ctx, kubeClient, managerUID, true)
}

func waitForManagerReadiness(t *testing.T, ctx context.Context, kubeClient client.Client, expectedUID types.UID, wanted bool) {
	t.Helper()
	pods := &corev1.PodList{}
	err := wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		pods = &corev1.PodList{}
		if err := kubeClient.List(ctx, pods,
			client.InNamespace("pgshard-system"),
			client.MatchingLabels{"app.kubernetes.io/name": "pgshard-operator", "app.kubernetes.io/component": "controller-manager"},
		); err != nil {
			return false, err
		}
		if len(pods.Items) != 1 {
			return false, fmt.Errorf("manager Pod count changed from one to %d", len(pods.Items))
		}
		pod := &pods.Items[0]
		if pod.UID != expectedUID {
			return false, fmt.Errorf("manager Pod changed from UID %s to %s", expectedUID, pod.UID)
		}
		if pod.DeletionTimestamp != nil {
			return false, fmt.Errorf("manager Pod %s entered deletion", pod.Name)
		}
		if pod.Status.Phase != corev1.PodRunning {
			return false, fmt.Errorf("manager Pod %s left Running phase for %s", pod.Name, pod.Status.Phase)
		}
		if len(pod.Status.ContainerStatuses) != 1 {
			return false, fmt.Errorf("manager Pod %s has %d container statuses", pod.Name, len(pod.Status.ContainerStatuses))
		}
		status := pod.Status.ContainerStatuses[0]
		if status.RestartCount != 0 {
			return false, fmt.Errorf("manager Pod %s restarted %d times", pod.Name, status.RestartCount)
		}
		if status.State.Running == nil {
			return false, fmt.Errorf("manager Pod %s container left Running state", pod.Name)
		}
		return status.Ready == wanted, nil
	})
	if err != nil {
		t.Fatalf("wait for manager readiness %t: %v; last Pods = %#v", wanted, err, pods.Items)
	}
}

func assertManagedAdmissionTLS(t *testing.T, ctx context.Context, kubeClient client.Client) {
	t.Helper()
	caSecret := &corev1.Secret{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: "pgshard-system", Name: "pgshard-webhook-ca"}, caSecret); err != nil {
		t.Fatal(err)
	}
	servingSecret := &corev1.Secret{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: "pgshard-system", Name: "pgshard-webhook-certificate"}, servingSecret); err != nil {
		t.Fatal(err)
	}
	fencingKeySecret := &corev1.Secret{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: "pgshard-system", Name: "pgshard-webhook-fencing-key"}, fencingKeySecret); err != nil {
		t.Fatal(err)
	}
	if caSecret.Labels[pki.ManagedByLabel] != pki.ManagedByValue || servingSecret.Labels[pki.ManagedByLabel] != pki.ManagedByValue || fencingKeySecret.Labels[pki.ManagedByLabel] != pki.ManagedByValue {
		t.Fatalf("webhook Secret ownership = %#v / %#v / %#v", caSecret.Labels, servingSecret.Labels, fencingKeySecret.Labels)
	}
	if fencingKeySecret.Immutable == nil || !*fencingKeySecret.Immutable || len(fencingKeySecret.Data) != 1 || len(fencingKeySecret.Data[pki.PodFencingKeyKey]) != podfence.SecretKeyBytes {
		t.Fatalf("webhook Pod fencing key = %#v", fencingKeySecret)
	}
	if caSecret.Annotations[pki.PodFencingKeyFingerprintAnnotation] != podfence.SecretHandshakeKeyFingerprint(fencingKeySecret.Data[pki.PodFencingKeyKey]) {
		t.Fatal("webhook CA Secret does not anchor the Pod fencing key")
	}
	if _, err := tls.X509KeyPair(servingSecret.Data[pki.TLSCertificateKey], servingSecret.Data[pki.TLSPrivateKeyKey]); err != nil {
		t.Fatalf("serving key pair: %v", err)
	}
	block, rest := pem.Decode(servingSecret.Data[pki.TLSCertificateKey])
	if block == nil || block.Type != "CERTIFICATE" || len(bytes.TrimSpace(rest)) != 0 {
		t.Fatal("serving Secret does not contain exactly one certificate")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caSecret.Data[pki.CACertificateKey]) {
		t.Fatal("CA Secret does not contain a certificate")
	}
	if _, err := certificate.Verify(x509.VerifyOptions{DNSName: "pgshard-webhook-service.pgshard-system.svc", Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
		t.Fatalf("verify serving certificate: %v", err)
	}

	mutating := &admissionregistrationv1.MutatingWebhookConfiguration{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Name: "pgshard-mutating-webhook-configuration"}, mutating); err != nil {
		t.Fatal(err)
	}
	validating := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Name: "pgshard-validating-webhook-configuration"}, validating); err != nil {
		t.Fatal(err)
	}
	if len(mutating.Webhooks) != 4 || len(validating.Webhooks) != 11 {
		t.Fatalf("injected CA bundles = %#v / %#v", mutating.Webhooks, validating.Webhooks)
	}
	for _, webhook := range mutating.Webhooks {
		if !bytes.Equal(webhook.ClientConfig.CABundle, caSecret.Data[pki.CACertificateKey]) {
			t.Fatalf("mutating webhook %s CA bundle was not injected", webhook.Name)
		}
	}
	for _, webhook := range validating.Webhooks {
		if !bytes.Equal(webhook.ClientConfig.CABundle, caSecret.Data[pki.CACertificateKey]) {
			t.Fatalf("validating webhook %s CA bundle was not injected", webhook.Name)
		}
	}
}

func readDevelopmentSample(t *testing.T) *pgshardv1alpha1.PgShardCluster {
	return readClusterSample(t, "../../config/samples/pgshard_v1alpha1_development.yaml")
}

func readSingleMemberSample(t *testing.T) *pgshardv1alpha1.PgShardCluster {
	return readClusterSample(t, "../../config/samples/pgshard_v1alpha1_single_member.yaml")
}

func readClusterSample(t *testing.T, path string) *pgshardv1alpha1.PgShardCluster {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cluster := &pgshardv1alpha1.PgShardCluster{}
	if err := yaml.UnmarshalStrict(contents, cluster); err != nil {
		t.Fatal(err)
	}
	return cluster
}
