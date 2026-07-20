package pki

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/operator/internal/podfence"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const (
	testNamespace                   = "pgshard-system"
	testServiceName                 = "pgshard-webhook-service"
	testCASecretName                = "pgshard-webhook-ca"
	testServingSecretName           = "pgshard-webhook-certificate"
	testFencingKeySecretName        = "pgshard-webhook-fencing-key"
	testMutatingConfigurationName   = "pgshard-mutating-webhook-configuration"
	testValidatingConfigurationName = "pgshard-validating-webhook-configuration"
)

func TestBootstrapCreatesValidIdempotentMaterial(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	kubeClient := newTestClient(t, installObjects()...)
	provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), func() time.Time { return now })

	if err := provisioner.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	caSecret := getSecret(t, kubeClient, testCASecretName)
	servingSecret := getSecret(t, kubeClient, testServingSecretName)
	fencingKeySecret := getSecret(t, kubeClient, testFencingKeySecretName)
	if servingSecret.Type != corev1.SecretTypeOpaque {
		t.Fatalf("initialized serving Secret type = %q", servingSecret.Type)
	}
	if fencingKeySecret.Immutable == nil || !*fencingKeySecret.Immutable || len(fencingKeySecret.Data) != 1 || len(fencingKeySecret.Data[PodFencingKeyKey]) != podfence.SecretKeyBytes {
		t.Fatalf("initialized Pod fencing key Secret = %#v", fencingKeySecret)
	}
	if caSecret.Annotations[PodFencingKeyFingerprintAnnotation] != podfence.SecretHandshakeKeyFingerprint(fencingKeySecret.Data[PodFencingKeyKey]) {
		t.Fatal("CA Secret does not anchor the initialized Pod fencing key")
	}
	if fencingKeySecret.Annotations[podfence.SecretKeyContinuityAnnotation] != podfence.SecretKeyContinuityValue {
		t.Fatal("initialized Pod fencing key lacks its continuity marker")
	}
	if caSecret.Annotations[PodFencingKeyFreshBootstrapAnnotation] != PodFencingKeyFreshBootstrapAnchored {
		t.Fatal("fresh Pod fencing key bootstrap did not become durably anchored")
	}
	if len(caSecret.Data) != 2 {
		t.Fatalf("continuity metadata changed the rollback-compatible CA data shape: %#v", caSecret.Data)
	}
	authority, err := parseCertificateAuthority(caSecret.Data[CACertificateKey], caSecret.Data[CAPrivateKeyKey], now)
	if err != nil {
		t.Fatal(err)
	}
	serving, err := parseServingCertificate(servingSecret.Data[TLSCertificateKey], servingSecret.Data[TLSPrivateKeyKey])
	if err != nil {
		t.Fatal(err)
	}
	if servingCertificateNeedsRenewal(serving, authority, provisioner.dnsNames(), now) {
		t.Fatal("generated serving certificate unexpectedly needs renewal")
	}
	if !bytes.Equal(servingSecret.Data[CACertificateKey], caSecret.Data[CACertificateKey]) {
		t.Fatal("serving Secret does not carry the current CA certificate")
	}
	assertInjectedBundles(t, kubeClient, caSecret.Data[CACertificateKey])
	if err := provisioner.Checker(nil); err != nil {
		t.Fatal(err)
	}
	assertFileMode(t, provisioner.certificateDirectory, TLSPrivateKeyKey, 0o600)
	assertFileMode(t, provisioner.certificateDirectory, TLSCertificateKey, 0o644)
	assertFileMode(t, provisioner.certificateDirectory, CACertificateKey, 0o644)

	beforeCAResourceVersion := caSecret.ResourceVersion
	beforeServingResourceVersion := servingSecret.ResourceVersion
	beforeFencingKeyResourceVersion := fencingKeySecret.ResourceVersion
	beforeCertificate := bytes.Clone(servingSecret.Data[TLSCertificateKey])
	beforeFencingKey := bytes.Clone(fencingKeySecret.Data[PodFencingKeyKey])
	if err := provisioner.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	caSecret = getSecret(t, kubeClient, testCASecretName)
	servingSecret = getSecret(t, kubeClient, testServingSecretName)
	fencingKeySecret = getSecret(t, kubeClient, testFencingKeySecretName)
	if caSecret.ResourceVersion != beforeCAResourceVersion || servingSecret.ResourceVersion != beforeServingResourceVersion || fencingKeySecret.ResourceVersion != beforeFencingKeyResourceVersion || !bytes.Equal(servingSecret.Data[TLSCertificateKey], beforeCertificate) || !bytes.Equal(fencingKeySecret.Data[PodFencingKeyKey], beforeFencingKey) {
		t.Fatal("idempotent bootstrap rewrote valid certificate resources")
	}
}

func TestBootstrapRenewsOnlyTheServingCertificate(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	kubeClient := newTestClient(t, installObjects()...)
	provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), func() time.Time { return now })
	if err := provisioner.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	oldCA := bytes.Clone(getSecret(t, kubeClient, testCASecretName).Data[CACertificateKey])
	oldServing := bytes.Clone(getSecret(t, kubeClient, testServingSecretName).Data[TLSCertificateKey])
	oldFencingKey := bytes.Clone(getSecret(t, kubeClient, testFencingKeySecretName).Data[PodFencingKeyKey])

	now = now.Add(61 * 24 * time.Hour)
	if err := provisioner.Checker(nil); err != nil {
		t.Fatalf("near-expiry certificate should remain ready while renewal runs: %v", err)
	}
	if err := provisioner.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	newCA := getSecret(t, kubeClient, testCASecretName).Data[CACertificateKey]
	newServing := getSecret(t, kubeClient, testServingSecretName).Data[TLSCertificateKey]
	newFencingKey := getSecret(t, kubeClient, testFencingKeySecretName).Data[PodFencingKeyKey]
	if !bytes.Equal(oldCA, newCA) {
		t.Fatal("leaf renewal replaced the CA")
	}
	if bytes.Equal(oldServing, newServing) {
		t.Fatal("near-expiry serving certificate was not renewed")
	}
	if !bytes.Equal(oldFencingKey, newFencingKey) {
		t.Fatal("leaf renewal replaced the durable Pod fencing key")
	}
	assertInjectedBundles(t, kubeClient, oldCA)
	if err := provisioner.Checker(nil); err != nil {
		t.Fatal(err)
	}
}

func TestCheckerRejectsExpiredServingCertificate(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	kubeClient := newTestClient(t, installObjects()...)
	provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), func() time.Time { return now })
	if err := provisioner.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(91 * 24 * time.Hour)
	if err := provisioner.Checker(nil); err == nil || !strings.Contains(err.Error(), "expired, untrusted, or has incorrect DNS names") {
		t.Fatalf("Checker() error = %v", err)
	}
}

func TestCheckerRejectsMissingOrMalformedPodFencingKey(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(context.Context, client.Client, *corev1.Secret) error
		want   string
	}{
		{
			name: "deleted",
			mutate: func(ctx context.Context, kubeClient client.Client, secret *corev1.Secret) error {
				return kubeClient.Delete(ctx, secret)
			},
			want: "get pre-created Pod fencing key Secret",
		},
		{
			name: "malformed",
			mutate: func(ctx context.Context, kubeClient client.Client, secret *corev1.Secret) error {
				if err := kubeClient.Delete(ctx, secret); err != nil {
					return err
				}
				immutable := true
				return kubeClient.Create(ctx, &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: secret.Namespace,
						Name:      secret.Name,
						Labels:    map[string]string{ManagedByLabel: ManagedByValue},
						Annotations: map[string]string{
							podfence.SecretKeyContinuityAnnotation: podfence.SecretKeyContinuityValue,
						},
					},
					Type:      corev1.SecretTypeOpaque,
					Immutable: &immutable,
					Data:      map[string][]byte{PodFencingKeyKey: make([]byte, podfence.SecretKeyBytes-1)},
				})
			},
			want: "exactly one 32-byte hmac.key",
		},
		{
			name: "different valid key",
			mutate: func(ctx context.Context, kubeClient client.Client, secret *corev1.Secret) error {
				if err := kubeClient.Delete(ctx, secret); err != nil {
					return err
				}
				replacement := bytes.Clone(secret.Data[PodFencingKeyKey])
				replacement[0] ^= 0xff
				immutable := true
				return kubeClient.Create(ctx, &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: secret.Namespace,
						Name:      secret.Name,
						Labels:    map[string]string{ManagedByLabel: ManagedByValue},
						Annotations: map[string]string{
							podfence.SecretKeyContinuityAnnotation: podfence.SecretKeyContinuityValue,
						},
					},
					Type:      corev1.SecretTypeOpaque,
					Immutable: &immutable,
					Data:      map[string][]byte{PodFencingKeyKey: replacement},
				})
			},
			want: "does not match the anchored fingerprint",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			kubeClient := newTestClient(t, installObjects()...)
			provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), time.Now)
			if err := provisioner.Bootstrap(ctx); err != nil {
				t.Fatal(err)
			}
			secret := getSecret(t, kubeClient, testFencingKeySecretName)
			if err := test.mutate(ctx, kubeClient, secret); err != nil {
				t.Fatal(err)
			}
			if err := provisioner.Checker(nil); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Checker() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestBootstrapRefusesRecreatedFencingKeyAfterContinuityIsAnchored(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		replacement func(*corev1.Secret) *corev1.Secret
		want        string
	}{
		{
			name: "empty",
			replacement: func(original *corev1.Secret) *corev1.Secret {
				return &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Namespace: original.Namespace, Name: original.Name, Labels: map[string]string{ManagedByLabel: ManagedByValue}},
					Type:       corev1.SecretTypeOpaque,
				}
			},
			want: "continuity fingerprint exists",
		},
		{
			name: "different valid key",
			replacement: func(original *corev1.Secret) *corev1.Secret {
				key := bytes.Clone(original.Data[PodFencingKeyKey])
				key[0] ^= 0xff
				immutable := true
				return &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Namespace: original.Namespace, Name: original.Name, Labels: map[string]string{ManagedByLabel: ManagedByValue}},
					Type:       corev1.SecretTypeOpaque,
					Immutable:  &immutable,
					Data:       map[string][]byte{PodFencingKeyKey: key},
				}
			},
			want: "does not match the anchored fingerprint",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			kubeClient := newTestClient(t, installObjects()...)
			provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), time.Now)
			if err := provisioner.Bootstrap(ctx); err != nil {
				t.Fatal(err)
			}
			original := getSecret(t, kubeClient, testFencingKeySecretName)
			if err := kubeClient.Delete(ctx, original); err != nil {
				t.Fatal(err)
			}
			if err := kubeClient.Create(ctx, test.replacement(original)); err != nil {
				t.Fatal(err)
			}
			if err := provisioner.Bootstrap(ctx); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Bootstrap() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestBootstrapCompletesPreAnchoredLegacyFencingKeyWithoutReplacingIt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	kubeClient := newTestClient(t, installObjects()...)
	provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), time.Now)
	if err := provisioner.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	keyBefore := getSecret(t, kubeClient, testFencingKeySecretName)
	caSecret := getSecret(t, kubeClient, testCASecretName)
	delete(caSecret.Annotations, PodFencingKeyFreshBootstrapAnnotation)
	if err := kubeClient.Update(ctx, caSecret); err != nil {
		t.Fatal(err)
	}
	delete(keyBefore.Annotations, podfence.SecretKeyContinuityAnnotation)
	if err := kubeClient.Update(ctx, keyBefore); err != nil {
		t.Fatal(err)
	}
	cluster := clusterWithHandshakeReceipt(t, keyBefore.Data[PodFencingKeyKey], "existing-receipt")
	if err := kubeClient.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	pod := podWithTerminationReceipt(t, keyBefore.Data[PodFencingKeyKey], "existing-terminal-pod")
	if err := kubeClient.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}

	if err := provisioner.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	keyAfter := getSecret(t, kubeClient, testFencingKeySecretName)
	caSecret = getSecret(t, kubeClient, testCASecretName)
	if !bytes.Equal(keyAfter.Data[PodFencingKeyKey], keyBefore.Data[PodFencingKeyKey]) {
		t.Fatal("continuity migration replaced the existing Pod fencing key")
	}
	if caSecret.Annotations[PodFencingKeyFingerprintAnnotation] != podfence.SecretHandshakeKeyFingerprint(keyBefore.Data[PodFencingKeyKey]) {
		t.Fatal("continuity migration changed the pre-anchored Pod fencing key")
	}
	if err := provisioner.Checker(nil); err != nil {
		t.Fatal(err)
	}
}

func TestBootstrapRefusesAutomaticLegacyKeyAdoption(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		empty bool
		want  string
	}{
		{name: "initialized key", want: "pin its fingerprint before rolling out this manager"},
		{name: "empty key", empty: true, want: "has no authorized bootstrap"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			kubeClient := newTestClient(t, installObjects()...)
			provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), time.Now)
			if err := provisioner.Bootstrap(ctx); err != nil {
				t.Fatal(err)
			}
			original := getSecret(t, kubeClient, testFencingKeySecretName)
			caSecret := getSecret(t, kubeClient, testCASecretName)
			delete(caSecret.Annotations, PodFencingKeyFingerprintAnnotation)
			delete(caSecret.Annotations, PodFencingKeyFreshBootstrapAnnotation)
			if err := kubeClient.Update(ctx, caSecret); err != nil {
				t.Fatal(err)
			}
			if test.empty {
				if err := kubeClient.Delete(ctx, original); err != nil {
					t.Fatal(err)
				}
				empty := &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Namespace: original.Namespace, Name: original.Name, Labels: map[string]string{ManagedByLabel: ManagedByValue}},
					Type:       corev1.SecretTypeOpaque,
				}
				if err := kubeClient.Create(ctx, empty); err != nil {
					t.Fatal(err)
				}
			} else {
				delete(original.Annotations, podfence.SecretKeyContinuityAnnotation)
				if err := kubeClient.Update(ctx, original); err != nil {
					t.Fatal(err)
				}
				if err := kubeClient.Create(ctx, clusterWithHandshakeReceipt(t, original.Data[PodFencingKeyKey], "legacy-resigned-receipt")); err != nil {
					t.Fatal(err)
				}
			}
			if err := provisioner.Bootstrap(ctx); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Bootstrap() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestBootstrapUpgradesOriginMainKeylessAdmissionStateInTwoPhases(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	kubeClient := newTestClient(t, installObjects()...)
	provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), time.Now)
	if err := provisioner.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	resetToOriginMainKeylessState(t, ctx, kubeClient)
	legacyMetadataKey := bytes.Repeat([]byte{0xff}, podfence.SecretKeyBytes)
	if err := kubeClient.Create(ctx, clusterWithHandshakeReceipt(t, legacyMetadataKey, "unsigned-keyless-cluster")); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Create(ctx, podWithTerminationReceipt(t, legacyMetadataKey, "unsigned-keyless-pod")); err != nil {
		t.Fatal(err)
	}

	if _, err := provisioner.ensureAuthority(ctx); err != nil {
		t.Fatal(err)
	}
	caSecret := getSecret(t, kubeClient, testCASecretName)
	keySecret := getSecret(t, kubeClient, testFencingKeySecretName)
	if caSecret.Annotations[PodFencingKeyLegacyUpgradeAnnotation] != PodFencingKeyLegacyUpgradePending {
		t.Fatal("origin/main upgrade did not persist authorization before key generation")
	}
	if len(keySecret.Data) != 0 || keySecret.Immutable != nil {
		t.Fatal("origin/main upgrade generated key material during its authorization phase")
	}

	if err := provisioner.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	caSecret = getSecret(t, kubeClient, testCASecretName)
	keySecret = getSecret(t, kubeClient, testFencingKeySecretName)
	if caSecret.Annotations[PodFencingKeyLegacyUpgradeAnnotation] != PodFencingKeyLegacyUpgradeAnchored ||
		caSecret.Annotations[PodFencingKeyFingerprintAnnotation] != podfence.SecretHandshakeKeyFingerprint(keySecret.Data[PodFencingKeyKey]) ||
		keySecret.Annotations[podfence.SecretKeyContinuityAnnotation] != podfence.SecretKeyContinuityValue {
		t.Fatalf("origin/main keyless upgrade did not complete: CA=%#v key=%#v", caSecret.Annotations, keySecret.Annotations)
	}
	if len(caSecret.Data) != 2 || len(keySecret.Data) != 1 || keySecret.Immutable == nil || !*keySecret.Immutable {
		t.Fatal("origin/main keyless upgrade changed rollback data shapes or left the key mutable")
	}
	keyBeforeRestart := bytes.Clone(keySecret.Data[PodFencingKeyKey])
	delete(keySecret.Annotations, podfence.SecretKeyContinuityAnnotation)
	if err := kubeClient.Update(ctx, keySecret); err != nil {
		t.Fatal(err)
	}
	restarted := newTestProvisioner(t, kubeClient, t.TempDir(), time.Now)
	if err := restarted.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	keySecret = getSecret(t, kubeClient, testFencingKeySecretName)
	if !bytes.Equal(keySecret.Data[PodFencingKeyKey], keyBeforeRestart) || keySecret.Annotations[podfence.SecretKeyContinuityAnnotation] != podfence.SecretKeyContinuityValue {
		t.Fatal("legacy-anchored restart did not preserve and complete the generated key")
	}
}

func TestBootstrapRequiresIndependentProofForOriginMainKeylessUpgrade(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, context.Context, client.Client)
		want   string
	}{
		{
			name: "manifest request",
			mutate: func(t *testing.T, ctx context.Context, kubeClient client.Client) {
				keySecret := getSecret(t, kubeClient, testFencingKeySecretName)
				delete(keySecret.Annotations, PodFencingKeyUpgradeRequestAnnotation)
				if err := kubeClient.Update(ctx, keySecret); err != nil {
					t.Fatal(err)
				}
			},
			want: "has no authorized bootstrap",
		},
		{
			name: "serving material",
			mutate: func(t *testing.T, ctx context.Context, kubeClient client.Client) {
				serving := getSecret(t, kubeClient, testServingSecretName)
				serving.Data = nil
				if err := kubeClient.Update(ctx, serving); err != nil {
					t.Fatal(err)
				}
			},
			want: "requires serving material initialized by the existing CA",
		},
		{
			name: "webhook trust",
			mutate: func(t *testing.T, ctx context.Context, kubeClient client.Client) {
				mutating := &admissionregistrationv1.MutatingWebhookConfiguration{}
				if err := kubeClient.Get(ctx, types.NamespacedName{Name: testMutatingConfigurationName}, mutating); err != nil {
					t.Fatal(err)
				}
				findMutatingWebhook(mutating.Webhooks, mutatingWebhookName).ClientConfig.CABundle = nil
				if err := kubeClient.Update(ctx, mutating); err != nil {
					t.Fatal(err)
				}
			},
			want: "both existing PgShardCluster webhooks",
		},
		{
			name: "receipt-capable webhook trust",
			mutate: func(t *testing.T, ctx context.Context, kubeClient client.Client) {
				caSecret := getSecret(t, kubeClient, testCASecretName)
				mutating := &admissionregistrationv1.MutatingWebhookConfiguration{}
				if err := kubeClient.Get(ctx, types.NamespacedName{Name: testMutatingConfigurationName}, mutating); err != nil {
					t.Fatal(err)
				}
				findMutatingWebhook(mutating.Webhooks, podfence.HandshakeWebhookName).ClientConfig.CABundle = bytes.Clone(caSecret.Data[CACertificateKey])
				if err := kubeClient.Update(ctx, mutating); err != nil {
					t.Fatal(err)
				}
			},
			want: "newly introduced receipt webhooks",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			kubeClient := newTestClient(t, installObjects()...)
			provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), time.Now)
			if err := provisioner.Bootstrap(ctx); err != nil {
				t.Fatal(err)
			}
			resetToOriginMainKeylessState(t, ctx, kubeClient)
			test.mutate(t, ctx, kubeClient)
			if err := provisioner.Bootstrap(ctx); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Bootstrap() error = %v, want %q", err, test.want)
			}
			keySecret := getSecret(t, kubeClient, testFencingKeySecretName)
			if len(keySecret.Data) != 0 {
				t.Fatal("failed keyless-upgrade proof generated key material")
			}
		})
	}
}

func TestBootstrapDoesNotTreatRecreatedCAAsFreshInstall(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	kubeClient := newTestClient(t, installObjects()...)
	provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), time.Now)
	if err := provisioner.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	caSecret := getSecret(t, kubeClient, testCASecretName)
	if err := kubeClient.Delete(ctx, caSecret); err != nil {
		t.Fatal(err)
	}
	recreated := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: caSecret.Namespace, Name: caSecret.Name, Labels: map[string]string{ManagedByLabel: ManagedByValue}},
		Type:       corev1.SecretTypeOpaque,
	}
	if err := kubeClient.Create(ctx, recreated); err != nil {
		t.Fatal(err)
	}
	if err := provisioner.Bootstrap(ctx); err == nil || !strings.Contains(err.Error(), "fencing key Secret is already initialized") {
		t.Fatalf("Bootstrap() error = %v", err)
	}
	caSecret = getSecret(t, kubeClient, testCASecretName)
	if caSecret.Annotations[PodFencingKeyFreshBootstrapAnnotation] != "" || len(caSecret.Data) != 0 {
		t.Fatal("recreated CA Secret was granted fresh-install authority")
	}
}

func TestBootstrapDoesNotTreatRecreatedAuthoritySecretsAsFreshInstall(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	kubeClient := newTestClient(t, installObjects()...)
	provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), time.Now)
	if err := provisioner.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{testCASecretName, testFencingKeySecretName} {
		if err := kubeClient.Delete(ctx, getSecret(t, kubeClient, name)); err != nil {
			t.Fatal(err)
		}
	}
	managedLabels := map[string]string{ManagedByLabel: ManagedByValue}
	if err := kubeClient.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testCASecretName, Labels: managedLabels},
		Type:       corev1.SecretTypeOpaque,
	}); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testFencingKeySecretName,
			Labels:    managedLabels,
			Annotations: map[string]string{
				PodFencingKeyUpgradeRequestAnnotation: PodFencingKeyUpgradeRequestValue,
			},
		},
		Type: corev1.SecretTypeOpaque,
	}); err != nil {
		t.Fatal(err)
	}

	if err := provisioner.Bootstrap(ctx); err == nil || !strings.Contains(err.Error(), "serving Secret is already initialized") {
		t.Fatalf("Bootstrap() error = %v", err)
	}
	if caSecret := getSecret(t, kubeClient, testCASecretName); len(caSecret.Data) != 0 || len(caSecret.Annotations) != 0 {
		t.Fatal("recreated authority Secrets were granted fresh-install authority")
	}
}

func TestFreshBootstrapRefusesEstablishedPostgreSQLLifecycles(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		object client.Object
		want   string
	}{
		{
			name: "cluster",
			object: &pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{
				Namespace: testNamespace,
				Name:      "established-cluster",
				Finalizers: []string{
					owned.ClusterResourceFinalizer,
				},
			}},
			want: "has an established PostgreSQL lifecycle",
		},
		{
			name: "pre-provisioning cluster handshake",
			object: &pgshardv1alpha1.PgShardCluster{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testNamespace,
					Name:      "pre-provisioning-handshake",
					Annotations: map[string]string{
						podfence.HandshakeChallengeAnnotation: "pending-challenge",
					},
				},
				Spec: pgshardv1alpha1.PgShardClusterSpec{MembersPerShard: 1},
			},
			want: "has an established PostgreSQL lifecycle",
		},
		{
			name:   "Pod",
			object: podWithTerminationReceipt(t, bytes.Repeat([]byte{0xff}, podfence.SecretKeyBytes), "established-pod"),
			want:   "managed PostgreSQL Pod",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			objects := installObjects()
			objects = append(objects, test.object.DeepCopyObject().(client.Object))
			kubeClient := newTestClient(t, objects...)
			provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), time.Now)
			if err := provisioner.Bootstrap(ctx); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Bootstrap() error = %v, want %q", err, test.want)
			}
			caSecret := getSecret(t, kubeClient, testCASecretName)
			keySecret := getSecret(t, kubeClient, testFencingKeySecretName)
			if len(caSecret.Data) != 0 || len(keySecret.Data) != 0 {
				t.Fatal("unsafe fresh bootstrap mutated authority material")
			}
		})
	}
}

func TestBootstrapResumesFreshKeyAfterCAInitialization(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	kubeClient := newTestClient(t, installObjects()...)
	provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), time.Now)
	if _, err := provisioner.ensureAuthority(ctx); err != nil {
		t.Fatal(err)
	}
	caSecret := getSecret(t, kubeClient, testCASecretName)
	if caSecret.Annotations[PodFencingKeyFreshBootstrapAnnotation] != PodFencingKeyFreshBootstrapPending {
		t.Fatal("CA initialization did not record pending fresh-key authority")
	}
	if err := provisioner.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	caSecret = getSecret(t, kubeClient, testCASecretName)
	if caSecret.Annotations[PodFencingKeyFreshBootstrapAnnotation] != PodFencingKeyFreshBootstrapAnchored {
		t.Fatal("resumed fresh-key bootstrap did not complete its durable anchor")
	}
}

func TestBootstrapRejectsUnknownFreshBootstrapState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	kubeClient := newTestClient(t, installObjects()...)
	provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), time.Now)
	if err := provisioner.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	caSecret := getSecret(t, kubeClient, testCASecretName)
	caSecret.Annotations[PodFencingKeyFreshBootstrapAnnotation] = "forged"
	if err := kubeClient.Update(ctx, caSecret); err != nil {
		t.Fatal(err)
	}
	if err := provisioner.Bootstrap(ctx); err == nil || !strings.Contains(err.Error(), "unsupported fresh-bootstrap state") {
		t.Fatalf("Bootstrap() error = %v", err)
	}
	if err := provisioner.Checker(nil); err == nil || !strings.Contains(err.Error(), "unsupported fresh-bootstrap state") {
		t.Fatalf("Checker() error = %v", err)
	}
}

func TestBootstrapRefusesPreAnchoredKeyThatCannotVerifyReceiptHistory(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		object func(*testing.T, []byte) client.Object
		want   string
	}{
		{name: "cluster", object: func(t *testing.T, key []byte) client.Object {
			return clusterWithHandshakeReceipt(t, key, "mismatched-cluster")
		}, want: "handshake receipt does not match"},
		{name: "agent multi-member cluster", object: func(t *testing.T, key []byte) client.Object {
			cluster := clusterWithHandshakeReceipt(t, key, "mismatched-agent-multi-member-cluster")
			cluster.Spec.MembersPerShard = 3
			cluster.Status.PostgreSQLBootstrapSpec = &pgshardv1alpha1.PostgreSQLBootstrapSpecStatus{
				MembersPerShard:   3,
				PostgreSQLRuntime: owned.PostgreSQLRuntimeAgentQuarantine.String(),
			}
			return cluster
		}, want: "handshake receipt does not match"},
		{name: "Pod", object: func(t *testing.T, key []byte) client.Object {
			return podWithTerminationReceipt(t, key, "mismatched-pod")
		}, want: "termination receipt does not match"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			kubeClient := newTestClient(t, installObjects()...)
			provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), time.Now)
			if err := provisioner.Bootstrap(ctx); err != nil {
				t.Fatal(err)
			}
			keySecret := getSecret(t, kubeClient, testFencingKeySecretName)
			delete(keySecret.Annotations, podfence.SecretKeyContinuityAnnotation)
			if err := kubeClient.Update(ctx, keySecret); err != nil {
				t.Fatal(err)
			}
			clearFreshBootstrapState(t, ctx, kubeClient)
			differentKey := bytes.Repeat([]byte{0xff}, podfence.SecretKeyBytes)
			if err := kubeClient.Create(ctx, test.object(t, differentKey)); err != nil {
				t.Fatal(err)
			}
			if err := provisioner.Bootstrap(ctx); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Bootstrap() error = %v, want %q", err, test.want)
			}
			keySecret = getSecret(t, kubeClient, testFencingKeySecretName)
			if keySecret.Annotations[podfence.SecretKeyContinuityAnnotation] != "" {
				t.Fatal("failed migration wrote the continuity completion marker")
			}
		})
	}
}

func TestBootstrapClassifiesIncompleteHandshakeHistoryByLifecycle(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		annotations map[string]string
		established bool
	}{
		{name: "challenge only", annotations: map[string]string{podfence.HandshakeChallengeAnnotation: "challenge"}},
		{name: "receipt only", annotations: map[string]string{podfence.HandshakeReceiptAnnotation: "v1.receipt"}},
		{name: "established without metadata", established: true},
		{name: "established challenge only", annotations: map[string]string{podfence.HandshakeChallengeAnnotation: "challenge"}, established: true},
		{name: "established receipt only", annotations: map[string]string{podfence.HandshakeReceiptAnnotation: "v1.receipt"}, established: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			kubeClient := newTestClient(t, installObjects()...)
			provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), time.Now)
			if err := provisioner.Bootstrap(ctx); err != nil {
				t.Fatal(err)
			}
			keySecret := getSecret(t, kubeClient, testFencingKeySecretName)
			delete(keySecret.Annotations, podfence.SecretKeyContinuityAnnotation)
			if err := kubeClient.Update(ctx, keySecret); err != nil {
				t.Fatal(err)
			}
			clearFreshBootstrapState(t, ctx, kubeClient)
			cluster := &pgshardv1alpha1.PgShardCluster{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   testNamespace,
					Name:        "incomplete-" + strings.ReplaceAll(test.name, " ", "-"),
					Annotations: test.annotations,
				},
				Spec: pgshardv1alpha1.PgShardClusterSpec{MembersPerShard: 1},
			}
			if test.established {
				cluster.Finalizers = []string{owned.ClusterResourceFinalizer}
				cluster.Status.PostgreSQLBootstraps = []pgshardv1alpha1.PostgreSQLBootstrapStatus{{Shard: 0}}
			}
			if err := kubeClient.Create(ctx, cluster); err != nil {
				t.Fatal(err)
			}

			err := provisioner.Bootstrap(ctx)
			keySecret = getSecret(t, kubeClient, testFencingKeySecretName)
			if test.established {
				if err == nil || !strings.Contains(err.Error(), "incomplete Pod fencing handshake metadata") {
					t.Fatalf("Bootstrap() error = %v", err)
				}
				if keySecret.Annotations[podfence.SecretKeyContinuityAnnotation] != "" {
					t.Fatal("failed migration wrote the continuity completion marker")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if keySecret.Annotations[podfence.SecretKeyContinuityAnnotation] != podfence.SecretKeyContinuityValue {
				t.Fatal("repairable pre-provisioning metadata blocked continuity completion")
			}
		})
	}
}

func TestBootstrapIgnoresReceiptsOutsideManagedFencingIdentities(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	kubeClient := newTestClient(t, installObjects()...)
	provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), time.Now)
	if err := provisioner.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	keySecret := getSecret(t, kubeClient, testFencingKeySecretName)
	delete(keySecret.Annotations, podfence.SecretKeyContinuityAnnotation)
	if err := kubeClient.Update(ctx, keySecret); err != nil {
		t.Fatal(err)
	}
	clearFreshBootstrapState(t, ctx, kubeClient)
	differentKey := bytes.Repeat([]byte{0xff}, podfence.SecretKeyBytes)
	directMultiMember := clusterWithHandshakeReceipt(t, differentKey, "direct-multi-member")
	directMultiMember.Spec.MembersPerShard = 3
	directMultiMember.Status.PostgreSQLBootstraps = nil
	if err := kubeClient.Create(ctx, directMultiMember); err != nil {
		t.Fatal(err)
	}
	unprovisioned := clusterWithHandshakeReceipt(t, differentKey, "unprovisioned-single-member")
	unprovisioned.Finalizers = nil
	unprovisioned.Status.PostgreSQLBootstraps = nil
	if err := kubeClient.Create(ctx, unprovisioned); err != nil {
		t.Fatal(err)
	}
	unmanagedPod := podWithTerminationReceipt(t, differentKey, "unmanaged-terminal-pod")
	unmanagedPod.Labels = nil
	unmanagedPod.Finalizers = nil
	if err := kubeClient.Create(ctx, unmanagedPod); err != nil {
		t.Fatal(err)
	}
	partiallyManagedPod := podWithTerminationReceipt(t, differentKey, "partially-managed-terminal-pod")
	delete(partiallyManagedPod.Labels, owned.ClusterLabel)
	if err := kubeClient.Create(ctx, partiallyManagedPod); err != nil {
		t.Fatal(err)
	}

	if err := provisioner.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	keySecret = getSecret(t, kubeClient, testFencingKeySecretName)
	if keySecret.Annotations[podfence.SecretKeyContinuityAnnotation] != podfence.SecretKeyContinuityValue {
		t.Fatal("unrelated receipt markers blocked continuity completion")
	}
}

func TestBootstrapRefusesAnchorLossAfterContinuityCompleted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	kubeClient := newTestClient(t, installObjects()...)
	provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), time.Now)
	if err := provisioner.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	caSecret := getSecret(t, kubeClient, testCASecretName)
	delete(caSecret.Annotations, PodFencingKeyFingerprintAnnotation)
	if err := kubeClient.Update(ctx, caSecret); err != nil {
		t.Fatal(err)
	}
	if err := provisioner.Bootstrap(ctx); err == nil || !strings.Contains(err.Error(), "fingerprint anchor is missing") {
		t.Fatalf("Bootstrap() error = %v", err)
	}
	if err := provisioner.Checker(nil); err == nil || !strings.Contains(err.Error(), "fingerprint anchor is missing") {
		t.Fatalf("Checker() error = %v", err)
	}
}

func TestBootstrapRechecksReceiptWrittenDuringFreshAnchorUpdate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	injected := false
	kubeClient := newTestClientWithInterceptors(t, installObjects(), interceptor.Funcs{
		Update: func(ctx context.Context, underlying client.WithWatch, object client.Object, options ...client.UpdateOption) error {
			secret, isSecret := object.(*corev1.Secret)
			anchorWrite := isSecret && secret.Name == testCASecretName && secret.Annotations[PodFencingKeyFingerprintAnnotation] != ""
			if err := underlying.Update(ctx, object, options...); err != nil {
				return err
			}
			if anchorWrite && !injected {
				injected = true
				differentKey := bytes.Repeat([]byte{0xff}, podfence.SecretKeyBytes)
				return underlying.Create(ctx, clusterWithHandshakeReceipt(t, differentKey, "receipt-during-anchor"))
			}
			return nil
		},
	})
	provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), time.Now)
	err := provisioner.Bootstrap(ctx)
	if !injected || err == nil || !strings.Contains(err.Error(), "has an established PostgreSQL lifecycle") {
		t.Fatalf("Bootstrap() injected = %t, error = %v", injected, err)
	}
	keySecret := getSecret(t, kubeClient, testFencingKeySecretName)
	if keySecret.Annotations[podfence.SecretKeyContinuityAnnotation] != "" {
		t.Fatal("failed migration wrote the continuity completion marker")
	}
	caSecret := getSecret(t, kubeClient, testCASecretName)
	if caSecret.Annotations[PodFencingKeyFingerprintAnnotation] == "" || caSecret.Annotations[PodFencingKeyFreshBootstrapAnnotation] != PodFencingKeyFreshBootstrapAnchored {
		t.Fatal("failed post-anchor scan did not preserve the durable fresh-install anchor")
	}
}

func TestBootstrapCompletesInterruptedAnchoredMigration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	kubeClient := newTestClient(t, installObjects()...)
	provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), time.Now)
	if err := provisioner.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	keySecret := getSecret(t, kubeClient, testFencingKeySecretName)
	delete(keySecret.Annotations, podfence.SecretKeyContinuityAnnotation)
	if err := kubeClient.Update(ctx, keySecret); err != nil {
		t.Fatal(err)
	}
	clearFreshBootstrapState(t, ctx, kubeClient)
	key := keySecret.Data[PodFencingKeyKey]
	if err := kubeClient.Create(ctx, clusterWithHandshakeReceipt(t, key, "interrupted-cluster")); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Create(ctx, podWithTerminationReceipt(t, key, "interrupted-pod")); err != nil {
		t.Fatal(err)
	}

	if err := provisioner.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	keySecret = getSecret(t, kubeClient, testFencingKeySecretName)
	if keySecret.Annotations[podfence.SecretKeyContinuityAnnotation] != podfence.SecretKeyContinuityValue {
		t.Fatal("interrupted migration did not write its completion marker")
	}
}

func TestContinuityMetadataPreservesPreviousManagerSecretDataShapes(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	kubeClient := newTestClient(t, installObjects()...)
	provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), func() time.Time { return now })
	if err := provisioner.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	caSecret := getSecret(t, kubeClient, testCASecretName)
	keySecret := getSecret(t, kubeClient, testFencingKeySecretName)
	if len(caSecret.Data) != 2 || len(keySecret.Data) != 1 {
		t.Fatalf("previous-manager Secret data shapes changed: CA=%#v key=%#v", caSecret.Data, keySecret.Data)
	}
	if _, err := parseCertificateAuthority(caSecret.Data[CACertificateKey], caSecret.Data[CAPrivateKeyKey], now); err != nil {
		t.Fatalf("previous manager cannot parse anchored CA Secret: %v", err)
	}
}

func TestBootstrapRefusesUnmanagedOrMalformedState(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func([]client.Object)
		want   string
	}{
		{
			name: "unmanaged CA Secret",
			mutate: func(objects []client.Object) {
				objects[0].SetLabels(nil)
			},
			want: "is not labeled as managed",
		},
		{
			name: "unexpected CA Secret data",
			mutate: func(objects []client.Object) {
				objects[0].(*corev1.Secret).Data = map[string][]byte{"unexpected": []byte("do not replace")}
			},
			want: "contain exactly",
		},
		{
			name: "mutable initialized fencing key",
			mutate: func(objects []client.Object) {
				objects[4].(*corev1.Secret).Data = map[string][]byte{PodFencingKeyKey: make([]byte, podfence.SecretKeyBytes)}
			},
			want: "must be immutable",
		},
		{
			name: "oversized initialized fencing key",
			mutate: func(objects []client.Object) {
				immutable := true
				secret := objects[4].(*corev1.Secret)
				secret.Immutable = &immutable
				secret.Data = map[string][]byte{PodFencingKeyKey: make([]byte, podfence.SecretKeyBytes+1)}
			},
			want: "exactly one 32-byte",
		},
		{
			name: "foreign CA bundle",
			mutate: func(objects []client.Object) {
				objects[2].(*admissionregistrationv1.MutatingWebhookConfiguration).Webhooks[0].ClientConfig.CABundle = []byte("foreign")
			},
			want: "not owned by the configured CA Secret",
		},
		{
			name: "fail-open webhook",
			mutate: func(objects []client.Object) {
				ignore := admissionregistrationv1.Ignore
				objects[2].(*admissionregistrationv1.MutatingWebhookConfiguration).Webhooks[0].FailurePolicy = &ignore
			},
			want: "failurePolicy",
		},
		{
			name: "narrowed webhook selector",
			mutate: func(objects []client.Object) {
				objects[3].(*admissionregistrationv1.ValidatingWebhookConfiguration).Webhooks[0].NamespaceSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"admission": "optional"}}
			},
			want: "unexpected namespaceSelector",
		},
		{
			name: "wrong webhook Service",
			mutate: func(objects []client.Object) {
				objects[3].(*admissionregistrationv1.ValidatingWebhookConfiguration).Webhooks[0].ClientConfig.Service.Name = "another-service"
			},
			want: "references Service",
		},
		{
			name: "wrong webhook path",
			mutate: func(objects []client.Object) {
				path := "/another-path"
				objects[2].(*admissionregistrationv1.MutatingWebhookConfiguration).Webhooks[0].ClientConfig.Service.Path = &path
			},
			want: "references Service path",
		},
		{
			name: "wrong restore webhook resource",
			mutate: func(objects []client.Object) {
				configuration := objects[3].(*admissionregistrationv1.ValidatingWebhookConfiguration)
				webhook := findValidatingWebhook(configuration.Webhooks, restoreWebhookName)
				webhook.Rules[0].Resources = []string{"pgshardclusters"}
			},
			want: `validating webhook "vpgshardrestore.kb.io": rules do not exactly cover`,
		},
		{
			name: "wrong webhook port",
			mutate: func(objects []client.Object) {
				port := int32(8443)
				objects[3].(*admissionregistrationv1.ValidatingWebhookConfiguration).Webhooks[0].ClientConfig.Service.Port = &port
			},
			want: "references Service port",
		},
		{
			name: "missing webhook port",
			mutate: func(objects []client.Object) {
				objects[3].(*admissionregistrationv1.ValidatingWebhookConfiguration).Webhooks[0].ClientConfig.Service.Port = nil
			},
			want: "does not specify Service port",
		},
		{
			name: "extra webhook",
			mutate: func(objects []client.Object) {
				configuration := objects[2].(*admissionregistrationv1.MutatingWebhookConfiguration)
				configuration.Webhooks = append(configuration.Webhooks, configuration.Webhooks[0])
			},
			want: "want exactly four",
		},
		{
			name: "missing restore webhook",
			mutate: func(objects []client.Object) {
				configuration := objects[3].(*admissionregistrationv1.ValidatingWebhookConfiguration)
				configuration.Webhooks = slices.DeleteFunc(configuration.Webhooks, func(webhook admissionregistrationv1.ValidatingWebhook) bool {
					return webhook.Name == restoreWebhookName
				})
			},
			want: "want exactly six",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			objects := installObjects()
			test.mutate(objects)
			kubeClient := newTestClient(t, objects...)
			provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), time.Now)
			err := provisioner.Bootstrap(context.Background())
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Bootstrap() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestBootstrapDoesNotReplaceMalformedNonEmptyServingSecret(t *testing.T) {
	t.Parallel()
	objects := installObjects()
	kubeClient := newTestClient(t, objects...)
	provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), time.Now)
	if err := provisioner.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	servingSecret := getSecret(t, kubeClient, testServingSecretName)
	servingSecret.Data[TLSCertificateKey] = []byte("not PEM")
	servingSecret.Data[TLSPrivateKeyKey] = []byte("not PEM")
	if err := kubeClient.Update(context.Background(), servingSecret); err != nil {
		t.Fatal(err)
	}
	err := provisioner.Bootstrap(context.Background())
	if err == nil || !strings.Contains(err.Error(), "validate managed serving Secret") {
		t.Fatalf("Bootstrap() error = %v", err)
	}
	got := getSecret(t, kubeClient, testServingSecretName)
	if !bytes.Equal(got.Data[TLSCertificateKey], []byte("not PEM")) {
		t.Fatal("malformed non-empty serving Secret was overwritten")
	}
}

func TestBootstrapRefusesIncompleteServingSecret(t *testing.T) {
	t.Parallel()
	objects := installObjects()
	servingSecret := objects[1].(*corev1.Secret)
	servingSecret.Data = map[string][]byte{"unexpected": []byte("do not replace")}
	kubeClient := newTestClient(t, objects...)
	provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), time.Now)
	err := provisioner.Bootstrap(context.Background())
	if err == nil || !strings.Contains(err.Error(), "serving Secret is already initialized") {
		t.Fatalf("Bootstrap() error = %v", err)
	}
	got := getSecret(t, kubeClient, testServingSecretName)
	if !bytes.Equal(got.Data["unexpected"], []byte("do not replace")) || got.Type != corev1.SecretTypeOpaque {
		t.Fatal("incomplete serving Secret was overwritten")
	}
}

func TestBootstrapRefusesServingSecretWithForeignCAOrExtraData(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*corev1.Secret)
		want   string
	}{
		{
			name: "foreign CA",
			mutate: func(secret *corev1.Secret) {
				secret.Data[CACertificateKey] = []byte("foreign")
			},
			want: "does not match the managed CA Secret",
		},
		{
			name: "extra key",
			mutate: func(secret *corev1.Secret) {
				secret.Data["unexpected"] = []byte("do not replace")
			},
			want: "contain exactly",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			kubeClient := newTestClient(t, installObjects()...)
			provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), time.Now)
			if err := provisioner.Bootstrap(context.Background()); err != nil {
				t.Fatal(err)
			}
			servingSecret := getSecret(t, kubeClient, testServingSecretName)
			test.mutate(servingSecret)
			if err := kubeClient.Update(context.Background(), servingSecret); err != nil {
				t.Fatal(err)
			}
			err := provisioner.Bootstrap(context.Background())
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Bootstrap() error = %v, want %q", err, test.want)
			}
			got := getSecret(t, kubeClient, testServingSecretName)
			if !bytes.Equal(got.Data["unexpected"], servingSecret.Data["unexpected"]) || !bytes.Equal(got.Data[CACertificateKey], servingSecret.Data[CACertificateKey]) {
				t.Fatal("invalid serving Secret was overwritten")
			}
		})
	}
}

func TestBootstrapRevalidatesConcurrentWebhookChanges(t *testing.T) {
	t.Parallel()
	var changed bool
	kubeClient := newTestClientWithInterceptors(t, installObjects(), interceptor.Funcs{
		Patch: func(ctx context.Context, underlying client.WithWatch, object client.Object, patch client.Patch, options ...client.PatchOption) error {
			if _, ok := object.(*admissionregistrationv1.MutatingWebhookConfiguration); ok && !changed {
				live := &admissionregistrationv1.MutatingWebhookConfiguration{}
				if err := underlying.Get(ctx, types.NamespacedName{Name: testMutatingConfigurationName}, live); err != nil {
					return err
				}
				ignore := admissionregistrationv1.Ignore
				live.Webhooks[0].FailurePolicy = &ignore
				if err := underlying.Update(ctx, live); err != nil {
					return err
				}
				changed = true
			}
			return underlying.Patch(ctx, object, patch, options...)
		},
	})
	provisioner := newTestProvisioner(t, kubeClient, t.TempDir(), time.Now)
	err := provisioner.Bootstrap(context.Background())
	if !changed || err == nil || !strings.Contains(err.Error(), "failurePolicy") {
		t.Fatalf("Bootstrap() changed = %t, error = %v", changed, err)
	}
	mutating := &admissionregistrationv1.MutatingWebhookConfiguration{}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: testMutatingConfigurationName}, mutating); err != nil {
		t.Fatal(err)
	}
	if mutating.Webhooks[0].FailurePolicy == nil || *mutating.Webhooks[0].FailurePolicy != admissionregistrationv1.Ignore || len(mutating.Webhooks[0].ClientConfig.CABundle) != 0 {
		t.Fatalf("concurrent webhook state was overwritten: %#v", mutating.Webhooks[0])
	}
}

func TestBootstrapTimesOutWaitingForInstallResources(t *testing.T) {
	t.Parallel()
	kubeClient := newTestClient(t)
	provisioner, err := New(Config{
		Client:                      kubeClient,
		Namespace:                   testNamespace,
		ServiceName:                 testServiceName,
		CASecretName:                testCASecretName,
		ServingSecretName:           testServingSecretName,
		FencingKeySecretName:        testFencingKeySecretName,
		MutatingConfigurationName:   testMutatingConfigurationName,
		ValidatingConfigurationName: testValidatingConfigurationName,
		CertificateDirectory:        t.TempDir(),
		BootstrapTimeout:            5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = provisioner.Bootstrap(context.Background())
	if err == nil || !strings.Contains(err.Error(), "last retryable error") {
		t.Fatalf("Bootstrap() error = %v", err)
	}
}

func TestNewRejectsUnsafeCertificateDirectory(t *testing.T) {
	t.Parallel()
	_, err := New(Config{
		Client:                      newTestClient(t),
		Namespace:                   testNamespace,
		ServiceName:                 testServiceName,
		CASecretName:                testCASecretName,
		ServingSecretName:           testServingSecretName,
		FencingKeySecretName:        testFencingKeySecretName,
		MutatingConfigurationName:   testMutatingConfigurationName,
		ValidatingConfigurationName: testValidatingConfigurationName,
		CertificateDirectory:        "/",
	})
	if err == nil || !strings.Contains(err.Error(), "absolute non-root") {
		t.Fatalf("New() error = %v", err)
	}
}

func newTestProvisioner(t *testing.T, kubeClient client.Client, directory string, now func() time.Time) *Provisioner {
	t.Helper()
	provisioner, err := New(Config{
		Client:                      kubeClient,
		Namespace:                   testNamespace,
		ServiceName:                 testServiceName,
		CASecretName:                testCASecretName,
		ServingSecretName:           testServingSecretName,
		FencingKeySecretName:        testFencingKeySecretName,
		MutatingConfigurationName:   testMutatingConfigurationName,
		ValidatingConfigurationName: testValidatingConfigurationName,
		CertificateDirectory:        directory,
		BootstrapTimeout:            time.Second,
		Now:                         now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return provisioner
}

func installObjects() []client.Object {
	managedLabels := map[string]string{ManagedByLabel: ManagedByValue}
	failurePolicy := admissionregistrationv1.Fail
	matchPolicy := admissionregistrationv1.Equivalent
	sideEffects := admissionregistrationv1.SideEffectClassNone
	timeoutSeconds := int32(5)
	servicePort := webhookServicePort
	scope := admissionregistrationv1.AllScopes
	reinvocationPolicy := admissionregistrationv1.NeverReinvocationPolicy
	serviceReference := func(path string) admissionregistrationv1.WebhookClientConfig {
		return admissionregistrationv1.WebhookClientConfig{Service: &admissionregistrationv1.ServiceReference{Namespace: testNamespace, Name: testServiceName, Path: &path, Port: &servicePort}}
	}
	clusterRules := func() []admissionregistrationv1.RuleWithOperations {
		return []admissionregistrationv1.RuleWithOperations{{
			Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update},
			Rule: admissionregistrationv1.Rule{
				APIGroups:   []string{"pgshard.io"},
				APIVersions: []string{"v1alpha1"},
				Resources:   []string{"pgshardclusters"},
				Scope:       &scope,
			},
		}}
	}
	coreResourceRules := func(operation admissionregistrationv1.OperationType, resources ...string) []admissionregistrationv1.RuleWithOperations {
		return []admissionregistrationv1.RuleWithOperations{{
			Operations: []admissionregistrationv1.OperationType{operation},
			Rule: admissionregistrationv1.Rule{
				APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: resources, Scope: &scope,
			},
		}}
	}
	mutatingWebhook := func(name, path string, rules []admissionregistrationv1.RuleWithOperations) admissionregistrationv1.MutatingWebhook {
		return admissionregistrationv1.MutatingWebhook{
			Name: name, ClientConfig: serviceReference(path), Rules: rules,
			FailurePolicy: &failurePolicy, MatchPolicy: &matchPolicy, SideEffects: &sideEffects, TimeoutSeconds: &timeoutSeconds,
			AdmissionReviewVersions: []string{"v1"}, NamespaceSelector: &metav1.LabelSelector{}, ObjectSelector: &metav1.LabelSelector{},
			ReinvocationPolicy: &reinvocationPolicy,
		}
	}
	validatingWebhook := func(name, path string, rules []admissionregistrationv1.RuleWithOperations) admissionregistrationv1.ValidatingWebhook {
		return admissionregistrationv1.ValidatingWebhook{
			Name: name, ClientConfig: serviceReference(path), Rules: rules,
			FailurePolicy: &failurePolicy, MatchPolicy: &matchPolicy, SideEffects: &sideEffects, TimeoutSeconds: &timeoutSeconds,
			AdmissionReviewVersions: []string{"v1"}, NamespaceSelector: &metav1.LabelSelector{}, ObjectSelector: &metav1.LabelSelector{},
		}
	}
	clusterMutating := mutatingWebhook(mutatingWebhookName, mutatingWebhookPath, clusterRules())
	bindingMutating := mutatingWebhook(podfence.BindingWebhookName, podfence.BindingWebhookPath, coreResourceRules(admissionregistrationv1.Create, "pods/binding"))
	bindingMutating.NamespaceSelector = podFencingNamespaceSelector()
	statusMutating := mutatingWebhook(podfence.StatusWebhookName, podfence.StatusWebhookPath, coreResourceRules(admissionregistrationv1.Update, "pods/status"))
	statusMutating.ObjectSelector = postgreSQLPodSelector()
	handshakeMutating := mutatingWebhook(podfence.HandshakeWebhookName, podfence.HandshakeWebhookPath, []admissionregistrationv1.RuleWithOperations{{
		Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Update},
		Rule: admissionregistrationv1.Rule{
			APIGroups: []string{"pgshard.io"}, APIVersions: []string{"v1alpha1"}, Resources: []string{"pgshardclusters"}, Scope: &scope,
		},
	}})
	handshakeMutating.NamespaceSelector = podFencingNamespaceSelector()
	clusterValidating := validatingWebhook(validatingWebhookName, validatingWebhookPath, clusterRules())
	restoreValidating := validatingWebhook(restoreWebhookName, restoreWebhookPath, []admissionregistrationv1.RuleWithOperations{{
		Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update},
		Rule: admissionregistrationv1.Rule{
			APIGroups: []string{"pgshard.io"}, APIVersions: []string{"v1alpha1"}, Resources: []string{"pgshardrestores"}, Scope: &scope,
		},
	}})
	metadataValidating := validatingWebhook(podfence.MetadataWebhookName, podfence.MetadataWebhookPath, coreResourceRules(admissionregistrationv1.Update, "pods", "pods/ephemeralcontainers", "pods/resize"))
	metadataValidating.ObjectSelector = postgreSQLPodSelector()
	namespaceValidating := validatingWebhook(podfence.NamespaceWebhookName, podfence.NamespaceWebhookPath, []admissionregistrationv1.RuleWithOperations{{
		Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Update},
		Rule: admissionregistrationv1.Rule{
			APIGroups: []string{""}, APIVersions: []string{"v1"},
			Resources: []string{"namespaces", "namespaces/status", "namespaces/finalize"}, Scope: &scope,
		},
	}})
	namespaceValidating.ObjectSelector = podFencingNamespaceSelector()
	statusValidating := validatingWebhook(podfence.StatusValidationWebhookName, podfence.StatusValidationWebhookPath, coreResourceRules(admissionregistrationv1.Update, "pods/status"))
	statusValidating.ObjectSelector = postgreSQLPodSelector()
	bindingValidating := validatingWebhook(podfence.BindingValidationWebhookName, podfence.BindingValidationWebhookPath, coreResourceRules(admissionregistrationv1.Create, "pods/binding"))
	bindingValidating.NamespaceSelector = podFencingNamespaceSelector()
	return []client.Object{
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testCASecretName, Labels: managedLabels}, Type: corev1.SecretTypeOpaque},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testServingSecretName, Labels: managedLabels}, Type: corev1.SecretTypeOpaque},
		&admissionregistrationv1.MutatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{Name: testMutatingConfigurationName},
			Webhooks:   []admissionregistrationv1.MutatingWebhook{clusterMutating, bindingMutating, statusMutating, handshakeMutating},
		},
		&admissionregistrationv1.ValidatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{Name: testValidatingConfigurationName},
			Webhooks:   []admissionregistrationv1.ValidatingWebhook{clusterValidating, restoreValidating, metadataValidating, namespaceValidating, statusValidating, bindingValidating},
		},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testFencingKeySecretName,
			Labels:    managedLabels,
			Annotations: map[string]string{
				PodFencingKeyUpgradeRequestAnnotation: PodFencingKeyUpgradeRequestValue,
			},
		}, Type: corev1.SecretTypeOpaque},
	}
}

func newTestClient(t *testing.T, objects ...client.Object) client.Client {
	return newTestClientWithInterceptors(t, objects, interceptor.Funcs{})
}

func newTestClientWithInterceptors(t *testing.T, objects []client.Object, functions interceptor.Funcs) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := pgshardv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := admissionregistrationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).WithInterceptorFuncs(functions).Build()
}

func clusterWithHandshakeReceipt(t *testing.T, key []byte, name string) *pgshardv1alpha1.PgShardCluster {
	t.Helper()
	cluster := &pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{
		Namespace: testNamespace,
		Name:      name,
		UID:       types.UID(name + "-uid"),
		Annotations: map[string]string{
			podfence.HandshakeChallengeAnnotation: name + "-challenge",
		},
	}}
	cluster.Spec.MembersPerShard = 1
	cluster.Status.PostgreSQLBootstraps = []pgshardv1alpha1.PostgreSQLBootstrapStatus{{Shard: 0}}
	cluster.Finalizers = []string{owned.ClusterResourceFinalizer}
	receipt, err := podfence.NewStaticHandshakeCodec(key).Receipt(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	cluster.Annotations[podfence.HandshakeReceiptAnnotation] = receipt
	return cluster
}

func podWithTerminationReceipt(t *testing.T, key []byte, name string) *corev1.Pod {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      name,
			UID:       types.UID(name + "-uid"),
			Labels: map[string]string{
				owned.ManagedByLabel: owned.ManagedByValue,
				owned.ComponentLabel: "postgresql",
				owned.ClusterLabel:   "example",
				owned.ShardLabel:     "0",
				owned.RoleLabel:      "primary",
				owned.MemberLabel:    "0",
			},
			Annotations: map[string]string{
				owned.PostgreSQLPodClusterUIDAnnotation: "cluster-uid",
				podfence.NodeUIDAnnotation:              "node-uid",
				podfence.NodeBootIDAnnotation:           "boot-id",
			},
			Finalizers: []string{owned.PostgreSQLPodTerminationFinalizer},
		},
		Spec: corev1.PodSpec{NodeName: "node-a"},
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "postgresql", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
			}},
		},
	}
	receipt, err := podfence.NewStaticHandshakeCodec(key).TerminationReceipt(context.Background(), pod)
	if err != nil {
		t.Fatal(err)
	}
	pod.Status.Conditions = []corev1.PodCondition{podfence.NewTerminationAttestation(pod, metav1.Now(), receipt)}
	return pod
}

func getSecret(t *testing.T, kubeClient client.Client, name string) *corev1.Secret {
	t.Helper()
	secret := &corev1.Secret{}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: name}, secret); err != nil {
		t.Fatal(err)
	}
	return secret
}

func resetToOriginMainKeylessState(t *testing.T, ctx context.Context, kubeClient client.Client) {
	t.Helper()
	caSecret := getSecret(t, kubeClient, testCASecretName)
	delete(caSecret.Annotations, PodFencingKeyFingerprintAnnotation)
	delete(caSecret.Annotations, PodFencingKeyFreshBootstrapAnnotation)
	delete(caSecret.Annotations, PodFencingKeyLegacyUpgradeAnnotation)
	if err := kubeClient.Update(ctx, caSecret); err != nil {
		t.Fatal(err)
	}
	keySecret := getSecret(t, kubeClient, testFencingKeySecretName)
	if err := kubeClient.Delete(ctx, keySecret); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testFencingKeySecretName,
			Labels:    map[string]string{ManagedByLabel: ManagedByValue},
			Annotations: map[string]string{
				PodFencingKeyUpgradeRequestAnnotation: PodFencingKeyUpgradeRequestValue,
			},
		},
		Type: corev1.SecretTypeOpaque,
	}); err != nil {
		t.Fatal(err)
	}

	mutating := &admissionregistrationv1.MutatingWebhookConfiguration{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Name: testMutatingConfigurationName}, mutating); err != nil {
		t.Fatal(err)
	}
	for index := range mutating.Webhooks {
		if mutating.Webhooks[index].Name != mutatingWebhookName {
			mutating.Webhooks[index].ClientConfig.CABundle = nil
		}
	}
	if err := kubeClient.Update(ctx, mutating); err != nil {
		t.Fatal(err)
	}
	validating := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Name: testValidatingConfigurationName}, validating); err != nil {
		t.Fatal(err)
	}
	for index := range validating.Webhooks {
		if validating.Webhooks[index].Name != validatingWebhookName {
			validating.Webhooks[index].ClientConfig.CABundle = nil
		}
	}
	if err := kubeClient.Update(ctx, validating); err != nil {
		t.Fatal(err)
	}
}

func clearFreshBootstrapState(t *testing.T, ctx context.Context, kubeClient client.Client) {
	t.Helper()
	caSecret := getSecret(t, kubeClient, testCASecretName)
	delete(caSecret.Annotations, PodFencingKeyFreshBootstrapAnnotation)
	if err := kubeClient.Update(ctx, caSecret); err != nil {
		t.Fatal(err)
	}
}

func assertInjectedBundles(t *testing.T, kubeClient client.Client, wanted []byte) {
	t.Helper()
	mutating := &admissionregistrationv1.MutatingWebhookConfiguration{}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: testMutatingConfigurationName}, mutating); err != nil {
		t.Fatal(err)
	}
	validating := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: testValidatingConfigurationName}, validating); err != nil {
		t.Fatal(err)
	}
	if len(mutating.Webhooks) != 4 || len(validating.Webhooks) != 6 {
		t.Fatalf("CA bundles were not injected: mutating=%#v validating=%#v", mutating.Webhooks, validating.Webhooks)
	}
	for _, webhook := range mutating.Webhooks {
		if !bytes.Equal(webhook.ClientConfig.CABundle, wanted) {
			t.Fatalf("mutating webhook %s CA bundle was not injected", webhook.Name)
		}
	}
	for _, webhook := range validating.Webhooks {
		if !bytes.Equal(webhook.ClientConfig.CABundle, wanted) {
			t.Fatalf("validating webhook %s CA bundle was not injected", webhook.Name)
		}
	}
}

func assertFileMode(t *testing.T, directory, name string, wanted os.FileMode) {
	t.Helper()
	info, err := os.Stat(filepath.Join(directory, name))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != wanted {
		t.Fatalf("%s mode = %o, want %o", name, info.Mode().Perm(), wanted)
	}
}
