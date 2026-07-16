package pki

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/operator/internal/podfence"
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
	if fencingKeySecret.Immutable == nil || !*fencingKeySecret.Immutable || len(fencingKeySecret.Data) != 1 || len(fencingKeySecret.Data[PodFencingKeyKey]) != podFencingKeyBytes {
		t.Fatalf("initialized Pod fencing key Secret = %#v", fencingKeySecret)
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
				objects[4].(*corev1.Secret).Data = map[string][]byte{PodFencingKeyKey: make([]byte, podFencingKeyBytes)}
			},
			want: "must be immutable",
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
			name: "wrong webhook port",
			mutate: func(objects []client.Object) {
				port := int32(8443)
				objects[3].(*admissionregistrationv1.ValidatingWebhookConfiguration).Webhooks[0].ClientConfig.Service.Port = &port
			},
			want: "references Service port",
		},
		{
			name: "extra webhook",
			mutate: func(objects []client.Object) {
				configuration := objects[2].(*admissionregistrationv1.MutatingWebhookConfiguration)
				configuration.Webhooks = append(configuration.Webhooks, configuration.Webhooks[0])
			},
			want: "want exactly four",
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
	if err == nil || !strings.Contains(err.Error(), "must be empty or contain exactly") {
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
	servicePort := int32(443)
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
			Webhooks:   []admissionregistrationv1.ValidatingWebhook{clusterValidating, metadataValidating, namespaceValidating, statusValidating, bindingValidating},
		},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testFencingKeySecretName, Labels: managedLabels}, Type: corev1.SecretTypeOpaque},
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
	if err := admissionregistrationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).WithInterceptorFuncs(functions).Build()
}

func getSecret(t *testing.T, kubeClient client.Client, name string) *corev1.Secret {
	t.Helper()
	secret := &corev1.Secret{}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: name}, secret); err != nil {
		t.Fatal(err)
	}
	return secret
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
	if len(mutating.Webhooks) != 4 || len(validating.Webhooks) != 5 {
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
