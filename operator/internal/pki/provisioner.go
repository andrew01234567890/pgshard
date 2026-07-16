// Package pki provisions and maintains the operator admission webhook's
// self-signed serving certificate and durable receipt key.
package pki

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"time"

	"github.com/andrew01234567890/pgshard/operator/internal/podfence"
	"github.com/go-logr/logr"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ManagedByLabel   = "app.kubernetes.io/managed-by"
	ManagedByValue   = "pgshard-operator"
	PodFencingKeyKey = "hmac.key"
	// PodFencingKeyFingerprintKey anchors key continuity in the independent CA
	// Secret so replacing the key Secret cannot silently invalidate receipts.
	PodFencingKeyFingerprintKey = "pod-fencing-key.sha256"

	defaultBootstrapTimeout    = 90 * time.Second
	defaultMaintenanceInterval = time.Hour
	retryInterval              = 250 * time.Millisecond
	mutatingWebhookName        = "mpgshardcluster.kb.io"
	validatingWebhookName      = "vpgshardcluster.kb.io"
	mutatingWebhookPath        = "/mutate-pgshard-io-v1alpha1-pgshardcluster"
	validatingWebhookPath      = "/validate-pgshard-io-v1alpha1-pgshardcluster"
	webhookServicePort         = int32(443)
	webhookTimeoutSeconds      = int32(5)
)

type Config struct {
	Client                      client.Client
	Namespace                   string
	ServiceName                 string
	CASecretName                string
	ServingSecretName           string
	FencingKeySecretName        string
	MutatingConfigurationName   string
	ValidatingConfigurationName string
	CertificateDirectory        string
	BootstrapTimeout            time.Duration
	MaintenanceInterval         time.Duration
	Now                         func() time.Time
	Random                      io.Reader
	Logger                      logr.Logger
}

type Provisioner struct {
	client                      client.Client
	namespace                   string
	serviceName                 string
	caSecretName                string
	servingSecretName           string
	fencingKeySecretName        string
	mutatingConfigurationName   string
	validatingConfigurationName string
	certificateDirectory        string
	bootstrapTimeout            time.Duration
	maintenanceInterval         time.Duration
	now                         func() time.Time
	random                      io.Reader
	logger                      logr.Logger
}

type material struct {
	ca      *certificateAuthority
	serving *servingCertificate
}

type configurations struct {
	mutating   *admissionregistrationv1.MutatingWebhookConfiguration
	validating *admissionregistrationv1.ValidatingWebhookConfiguration
}

type webhookPolicy struct {
	name                    string
	clientConfig            admissionregistrationv1.WebhookClientConfig
	rules                   []admissionregistrationv1.RuleWithOperations
	failurePolicy           *admissionregistrationv1.FailurePolicyType
	matchPolicy             *admissionregistrationv1.MatchPolicyType
	namespaceSelector       *metav1.LabelSelector
	objectSelector          *metav1.LabelSelector
	sideEffects             *admissionregistrationv1.SideEffectClass
	timeoutSeconds          *int32
	admissionReviewVersions []string
	matchConditionCount     int
}

func New(config Config) (*Provisioner, error) {
	if config.Client == nil {
		return nil, fmt.Errorf("Kubernetes client is required")
	}
	for _, item := range []struct {
		field string
		value string
	}{
		{field: "namespace", value: config.Namespace},
		{field: "service name", value: config.ServiceName},
		{field: "CA Secret name", value: config.CASecretName},
		{field: "serving Secret name", value: config.ServingSecretName},
		{field: "fencing key Secret name", value: config.FencingKeySecretName},
		{field: "mutating configuration name", value: config.MutatingConfigurationName},
		{field: "validating configuration name", value: config.ValidatingConfigurationName},
	} {
		field, value := item.field, item.value
		if messages := validation.IsDNS1123Subdomain(value); len(messages) != 0 {
			return nil, fmt.Errorf("invalid %s %q: %s", field, value, messages[0])
		}
	}
	if !filepath.IsAbs(config.CertificateDirectory) || filepath.Clean(config.CertificateDirectory) == string(filepath.Separator) {
		return nil, fmt.Errorf("certificate directory must be an absolute non-root path")
	}
	if config.BootstrapTimeout <= 0 {
		config.BootstrapTimeout = defaultBootstrapTimeout
	}
	if config.MaintenanceInterval <= 0 {
		config.MaintenanceInterval = defaultMaintenanceInterval
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.Random == nil {
		config.Random = rand.Reader
	}
	if config.Logger.GetSink() == nil {
		config.Logger = logr.Discard()
	}
	return &Provisioner{
		client:                      config.Client,
		namespace:                   config.Namespace,
		serviceName:                 config.ServiceName,
		caSecretName:                config.CASecretName,
		servingSecretName:           config.ServingSecretName,
		fencingKeySecretName:        config.FencingKeySecretName,
		mutatingConfigurationName:   config.MutatingConfigurationName,
		validatingConfigurationName: config.ValidatingConfigurationName,
		certificateDirectory:        filepath.Clean(config.CertificateDirectory),
		bootstrapTimeout:            config.BootstrapTimeout,
		maintenanceInterval:         config.MaintenanceInterval,
		now:                         config.Now,
		random:                      config.Random,
		logger:                      config.Logger,
	}, nil
}

// Bootstrap waits for the pre-created install resources, provisions their
// certificate data, and writes the local files before the webhook server starts.
func (p *Provisioner) Bootstrap(ctx context.Context) error {
	var lastRetryable error
	err := wait.PollUntilContextTimeout(ctx, retryInterval, p.bootstrapTimeout, true, func(ctx context.Context) (bool, error) {
		err := p.ensureOnce(ctx)
		if err == nil {
			return true, nil
		}
		if !retryableAPIError(err) {
			return false, err
		}
		lastRetryable = err
		return false, nil
	})
	if err != nil && lastRetryable != nil {
		return fmt.Errorf("bootstrap webhook PKI after last retryable error %q: %w", lastRetryable, err)
	}
	if err != nil {
		return fmt.Errorf("bootstrap webhook PKI: %w", err)
	}
	return nil
}

// Start periodically renews the leaf certificate and writes it into this Pod's
// private emptyDir. Every manager replica runs maintenance because every
// webhook server needs its own local files refreshed.
func (p *Provisioner) Start(ctx context.Context) error {
	ticker := time.NewTicker(p.maintenanceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := p.Bootstrap(ctx); err != nil && ctx.Err() == nil {
				p.logger.Error(err, "webhook certificate maintenance failed")
			}
		}
	}
}

func (*Provisioner) NeedLeaderElection() bool { return false }

// Checker fails readiness when the local serving material or the durable Pod
// fencing key is unusable. A valid certificate remains ready inside its
// renewal window while maintenance replaces it, avoiding an admission outage
// at the renewal threshold.
func (p *Provisioner) Checker(request *http.Request) error {
	ctx := context.Background()
	if request != nil {
		ctx = request.Context()
	}
	if err := p.checkPodFencingKey(ctx); err != nil {
		return err
	}
	authorityPEM, err := os.ReadFile(filepath.Join(p.certificateDirectory, CACertificateKey))
	if err != nil {
		return fmt.Errorf("read local CA certificate: %w", err)
	}
	certificatePEM, err := os.ReadFile(filepath.Join(p.certificateDirectory, TLSCertificateKey))
	if err != nil {
		return fmt.Errorf("read local serving certificate: %w", err)
	}
	privateKeyPEM, err := os.ReadFile(filepath.Join(p.certificateDirectory, TLSPrivateKeyKey))
	if err != nil {
		return fmt.Errorf("read local serving private key: %w", err)
	}
	authorityCertificate, err := parseCertificate(authorityPEM)
	if err != nil {
		return fmt.Errorf("parse local CA certificate: %w", err)
	}
	serving, err := parseServingCertificate(certificatePEM, privateKeyPEM)
	if err != nil {
		return err
	}
	authority := &certificateAuthority{certificate: authorityCertificate}
	if !servingCertificateIsUsable(serving, authority, p.dnsNames(), p.now()) {
		return fmt.Errorf("local serving certificate is expired, untrusted, or has incorrect DNS names")
	}
	return nil
}

func (p *Provisioner) ensureOnce(ctx context.Context) error {
	authority, err := p.ensureAuthority(ctx)
	if err != nil {
		return err
	}
	if err := p.ensurePodFencingKey(ctx); err != nil {
		return err
	}
	configs, err := p.readConfigurations(ctx, authority.certificatePEM)
	if err != nil {
		return err
	}
	serving, err := p.ensureServingCertificate(ctx, authority)
	if err != nil {
		return err
	}
	if err := writeMaterial(p.certificateDirectory, &material{ca: authority, serving: serving}); err != nil {
		return err
	}
	if err := p.injectCABundle(ctx, configs, authority.certificatePEM); err != nil {
		return err
	}
	return nil
}

func (p *Provisioner) ensurePodFencingKey(ctx context.Context) error {
	secret, err := p.readPodFencingKey(ctx)
	if err != nil {
		return err
	}
	anchor, anchored, err := p.readPodFencingKeyAnchor(ctx)
	if err != nil {
		return err
	}
	if len(secret.Data) == 0 {
		if err := validatePodFencingKeyMetadata(secret); err != nil {
			return err
		}
		if secret.Immutable != nil && *secret.Immutable {
			return fmt.Errorf("empty Pod fencing key Secret %s/%s is immutable", secret.Namespace, secret.Name)
		}
		if anchored {
			return fmt.Errorf("Pod fencing key Secret is empty but a continuity fingerprint exists; restore the original key or perform explicit fencing recovery")
		}
		value := make([]byte, podfence.SecretKeyBytes)
		if _, err := io.ReadFull(p.random, value); err != nil {
			return fmt.Errorf("generate Pod fencing key: %w", err)
		}
		secret.Data = map[string][]byte{PodFencingKeyKey: value}
		immutable := true
		secret.Immutable = &immutable
		if err := p.client.Update(ctx, secret); err != nil {
			return fmt.Errorf("initialize Pod fencing key Secret: %w", err)
		}
	}
	key, err := podfence.ValidateSecretHandshakeKey(secret, PodFencingKeyKey)
	if err != nil {
		return err
	}
	if anchored {
		return podfence.ValidateSecretHandshakeKeyFingerprint(anchor, PodFencingKeyFingerprintKey, key)
	}
	anchor.Data[PodFencingKeyFingerprintKey] = podfence.SecretHandshakeKeyFingerprint(key)
	if err := p.client.Update(ctx, anchor); err != nil {
		return fmt.Errorf("anchor Pod fencing key fingerprint: %w", err)
	}
	return p.checkPodFencingKey(ctx)
}

func (p *Provisioner) checkPodFencingKey(ctx context.Context) error {
	secret, err := p.readPodFencingKey(ctx)
	if err != nil {
		return err
	}
	key, err := podfence.ValidateSecretHandshakeKey(secret, PodFencingKeyKey)
	if err != nil {
		return err
	}
	anchor, anchored, err := p.readPodFencingKeyAnchor(ctx)
	if err != nil {
		return err
	}
	if !anchored {
		return fmt.Errorf("Pod fencing key continuity fingerprint is missing")
	}
	return podfence.ValidateSecretHandshakeKeyFingerprint(anchor, PodFencingKeyFingerprintKey, key)
}

func (p *Provisioner) readPodFencingKey(ctx context.Context) (*corev1.Secret, error) {
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: p.namespace, Name: p.fencingKeySecretName}
	if err := p.client.Get(ctx, key, secret); err != nil {
		return nil, fmt.Errorf("get pre-created Pod fencing key Secret: %w", err)
	}
	return secret, nil
}

func (p *Provisioner) readPodFencingKeyAnchor(ctx context.Context) (*corev1.Secret, bool, error) {
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: p.namespace, Name: p.caSecretName}
	if err := p.client.Get(ctx, key, secret); err != nil {
		return nil, false, fmt.Errorf("get Pod fencing key anchor Secret: %w", err)
	}
	if err := validateManagedSecret(secret, corev1.SecretTypeOpaque); err != nil {
		return nil, false, err
	}
	fingerprint, exists := secret.Data[PodFencingKeyFingerprintKey]
	if exists && len(fingerprint) != podfence.SecretKeyBytes {
		return nil, false, fmt.Errorf("Pod fencing key continuity fingerprint must be exactly %d bytes", podfence.SecretKeyBytes)
	}
	return secret, exists, nil
}

func validatePodFencingKeyMetadata(secret *corev1.Secret) error {
	if secret.Labels[ManagedByLabel] != ManagedByValue {
		return fmt.Errorf("Secret %s/%s is not labeled as managed by %s", secret.Namespace, secret.Name, ManagedByValue)
	}
	if secret.Type != corev1.SecretTypeOpaque {
		return fmt.Errorf("managed Secret %s/%s has type %q, want %q", secret.Namespace, secret.Name, secret.Type, corev1.SecretTypeOpaque)
	}
	return nil
}

func (p *Provisioner) ensureAuthority(ctx context.Context) (*certificateAuthority, error) {
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: p.namespace, Name: p.caSecretName}
	if err := p.client.Get(ctx, key, secret); err != nil {
		return nil, fmt.Errorf("get pre-created CA Secret: %w", err)
	}
	if err := validateManagedSecret(secret, corev1.SecretTypeOpaque); err != nil {
		return nil, err
	}
	if len(secret.Data) == 0 {
		authority, err := generateCertificateAuthority(p.now(), p.random, p.serviceName+"."+p.namespace+".svc")
		if err != nil {
			return nil, err
		}
		secret.Data = map[string][]byte{
			CACertificateKey: authority.certificatePEM,
			CAPrivateKeyKey:  authority.privateKeyPEM,
		}
		if err := p.client.Update(ctx, secret); err != nil {
			return nil, fmt.Errorf("initialize CA Secret: %w", err)
		}
		return authority, nil
	}
	certificatePEM, certificateExists := secret.Data[CACertificateKey]
	privateKeyPEM, privateKeyExists := secret.Data[CAPrivateKeyKey]
	fingerprint, fingerprintExists := secret.Data[PodFencingKeyFingerprintKey]
	wantedData := 2
	if fingerprintExists {
		wantedData++
		if len(fingerprint) != podfence.SecretKeyBytes {
			return nil, fmt.Errorf("managed CA Secret %s must be exactly %d bytes", PodFencingKeyFingerprintKey, podfence.SecretKeyBytes)
		}
	}
	if len(secret.Data) != wantedData || !certificateExists || !privateKeyExists {
		return nil, fmt.Errorf("managed CA Secret must be empty or contain exactly %s and %s, with optional %s", CACertificateKey, CAPrivateKeyKey, PodFencingKeyFingerprintKey)
	}
	authority, err := parseCertificateAuthority(certificatePEM, privateKeyPEM, p.now())
	if err != nil {
		return nil, fmt.Errorf("validate managed CA Secret: %w", err)
	}
	return authority, nil
}

func (p *Provisioner) ensureServingCertificate(ctx context.Context, authority *certificateAuthority) (*servingCertificate, error) {
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: p.namespace, Name: p.servingSecretName}
	if err := p.client.Get(ctx, key, secret); err != nil {
		return nil, fmt.Errorf("get pre-created serving Secret: %w", err)
	}
	if err := validateManagedServingSecret(secret); err != nil {
		return nil, err
	}
	if len(secret.Data) != 0 {
		certificatePEM, certificateExists := secret.Data[TLSCertificateKey]
		privateKeyPEM, privateKeyExists := secret.Data[TLSPrivateKeyKey]
		caCertificatePEM, caCertificateExists := secret.Data[CACertificateKey]
		if len(secret.Data) != 3 || !certificateExists || !privateKeyExists || !caCertificateExists {
			return nil, fmt.Errorf("managed serving Secret must be empty or contain exactly %s, %s, and %s", TLSCertificateKey, TLSPrivateKeyKey, CACertificateKey)
		}
		if !bytes.Equal(caCertificatePEM, authority.certificatePEM) {
			return nil, fmt.Errorf("managed serving Secret CA certificate does not match the managed CA Secret")
		}
		serving, err := parseServingCertificate(certificatePEM, privateKeyPEM)
		if err != nil {
			return nil, fmt.Errorf("validate managed serving Secret: %w", err)
		}
		if !servingCertificateNeedsRenewal(serving, authority, p.dnsNames(), p.now()) {
			return serving, nil
		}
	}
	serving, err := generateServingCertificate(p.now(), p.random, authority, p.dnsNames())
	if err != nil {
		return nil, err
	}
	secret.Data = map[string][]byte{
		TLSCertificateKey: serving.certificatePEM,
		TLSPrivateKeyKey:  serving.privateKeyPEM,
		CACertificateKey:  authority.certificatePEM,
	}
	if err := p.client.Update(ctx, secret); err != nil {
		return nil, fmt.Errorf("initialize or renew serving Secret: %w", err)
	}
	return serving, nil
}

func validateManagedServingSecret(secret *corev1.Secret) error {
	return validateManagedSecret(secret, corev1.SecretTypeOpaque)
}

func validateManagedSecret(secret *corev1.Secret, wantedType corev1.SecretType) error {
	if err := validateManagedSecretMetadata(secret); err != nil {
		return err
	}
	if secret.Type != wantedType {
		return fmt.Errorf("managed Secret %s/%s has type %q, want %q", secret.Namespace, secret.Name, secret.Type, wantedType)
	}
	return nil
}

func validateManagedSecretMetadata(secret *corev1.Secret) error {
	if secret.Labels[ManagedByLabel] != ManagedByValue {
		return fmt.Errorf("Secret %s/%s is not labeled as managed by %s", secret.Namespace, secret.Name, ManagedByValue)
	}
	if secret.Immutable != nil && *secret.Immutable {
		return fmt.Errorf("managed Secret %s/%s is immutable", secret.Namespace, secret.Name)
	}
	return nil
}

func (p *Provisioner) readConfigurations(ctx context.Context, caBundle []byte) (*configurations, error) {
	mutating := &admissionregistrationv1.MutatingWebhookConfiguration{}
	if err := p.client.Get(ctx, types.NamespacedName{Name: p.mutatingConfigurationName}, mutating); err != nil {
		return nil, fmt.Errorf("get mutating webhook configuration: %w", err)
	}
	if len(mutating.Webhooks) != 4 {
		return nil, fmt.Errorf("mutating webhook configuration contains %d webhooks, want exactly four", len(mutating.Webhooks))
	}
	for _, expected := range []struct {
		name, path        string
		rules             func([]admissionregistrationv1.RuleWithOperations) bool
		namespace, object *metav1.LabelSelector
	}{
		{name: mutatingWebhookName, path: mutatingWebhookPath, rules: matchesPgShardClusterRules},
		{name: podfence.BindingWebhookName, path: podfence.BindingWebhookPath, rules: matchesPostgreSQLBindingRules, namespace: podFencingNamespaceSelector()},
		{name: podfence.StatusWebhookName, path: podfence.StatusWebhookPath, rules: matchesPostgreSQLStatusRules, object: postgreSQLPodSelector()},
		{name: podfence.HandshakeWebhookName, path: podfence.HandshakeWebhookPath, rules: matchesPostgreSQLHandshakeRules, namespace: podFencingNamespaceSelector()},
	} {
		webhook := findMutatingWebhook(mutating.Webhooks, expected.name)
		if webhook == nil {
			return nil, fmt.Errorf("mutating webhook configuration does not contain %q", expected.name)
		}
		if webhook.ReinvocationPolicy != nil && *webhook.ReinvocationPolicy != admissionregistrationv1.NeverReinvocationPolicy {
			return nil, fmt.Errorf("mutating webhook %q has reinvocationPolicy %q, want Never", webhook.Name, *webhook.ReinvocationPolicy)
		}
		if err := p.validateWebhookPolicy(policyForMutating(webhook), caBundle, expected.name, expected.path, expected.rules, expected.namespace, expected.object); err != nil {
			return nil, fmt.Errorf("mutating webhook %q: %w", webhook.Name, err)
		}
	}

	validating := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := p.client.Get(ctx, types.NamespacedName{Name: p.validatingConfigurationName}, validating); err != nil {
		return nil, fmt.Errorf("get validating webhook configuration: %w", err)
	}
	if len(validating.Webhooks) != 5 {
		return nil, fmt.Errorf("validating webhook configuration contains %d webhooks, want exactly five", len(validating.Webhooks))
	}
	for _, expected := range []struct {
		name, path        string
		rules             func([]admissionregistrationv1.RuleWithOperations) bool
		namespace, object *metav1.LabelSelector
	}{
		{name: validatingWebhookName, path: validatingWebhookPath, rules: matchesPgShardClusterRules},
		{name: podfence.MetadataWebhookName, path: podfence.MetadataWebhookPath, rules: matchesPostgreSQLMetadataRules, object: postgreSQLPodSelector()},
		{name: podfence.NamespaceWebhookName, path: podfence.NamespaceWebhookPath, rules: matchesPostgreSQLNamespaceRules, object: podFencingNamespaceSelector()},
		{name: podfence.StatusValidationWebhookName, path: podfence.StatusValidationWebhookPath, rules: matchesPostgreSQLStatusRules, object: postgreSQLPodSelector()},
		{name: podfence.BindingValidationWebhookName, path: podfence.BindingValidationWebhookPath, rules: matchesPostgreSQLBindingRules, namespace: podFencingNamespaceSelector()},
	} {
		webhook := findValidatingWebhook(validating.Webhooks, expected.name)
		if webhook == nil {
			return nil, fmt.Errorf("validating webhook configuration does not contain %q", expected.name)
		}
		if err := p.validateWebhookPolicy(policyForValidating(webhook), caBundle, expected.name, expected.path, expected.rules, expected.namespace, expected.object); err != nil {
			return nil, fmt.Errorf("validating webhook %q: %w", webhook.Name, err)
		}
	}
	return &configurations{mutating: mutating, validating: validating}, nil
}

func (p *Provisioner) validateWebhookPolicy(policy webhookPolicy, caBundle []byte, wantedName, wantedPath string, matchesRules func([]admissionregistrationv1.RuleWithOperations) bool, wantedNamespaceSelector, wantedObjectSelector *metav1.LabelSelector) error {
	if policy.name != wantedName {
		return fmt.Errorf("has name %q, want %q", policy.name, wantedName)
	}
	if policy.failurePolicy == nil || *policy.failurePolicy != admissionregistrationv1.Fail {
		return fmt.Errorf("has failurePolicy %v, want Fail", policy.failurePolicy)
	}
	if policy.matchPolicy == nil || *policy.matchPolicy != admissionregistrationv1.Equivalent {
		return fmt.Errorf("has matchPolicy %v, want Equivalent", policy.matchPolicy)
	}
	if policy.sideEffects == nil || *policy.sideEffects != admissionregistrationv1.SideEffectClassNone {
		return fmt.Errorf("has sideEffects %v, want None", policy.sideEffects)
	}
	if policy.timeoutSeconds == nil || *policy.timeoutSeconds != webhookTimeoutSeconds {
		return fmt.Errorf("has timeoutSeconds %v, want %d", policy.timeoutSeconds, webhookTimeoutSeconds)
	}
	if !slices.Equal(policy.admissionReviewVersions, []string{"v1"}) {
		return fmt.Errorf("has admissionReviewVersions %q, want [v1]", policy.admissionReviewVersions)
	}
	if !selectorsEqual(policy.namespaceSelector, wantedNamespaceSelector) || !selectorsEqual(policy.objectSelector, wantedObjectSelector) || policy.matchConditionCount != 0 {
		return fmt.Errorf("has unexpected namespaceSelector, objectSelector, or matchConditions")
	}
	if !matchesRules(policy.rules) {
		return fmt.Errorf("rules do not exactly cover the required operations")
	}
	return p.validateClientConfig(policy.clientConfig, caBundle, wantedPath)
}

func selectorsEqual(actual, wanted *metav1.LabelSelector) bool {
	if wanted == nil || len(wanted.MatchLabels) == 0 && len(wanted.MatchExpressions) == 0 {
		return actual == nil || len(actual.MatchLabels) == 0 && len(actual.MatchExpressions) == 0
	}
	return actual != nil && maps.Equal(actual.MatchLabels, wanted.MatchLabels) && reflect.DeepEqual(actual.MatchExpressions, wanted.MatchExpressions)
}

func matchesPgShardClusterRules(rules []admissionregistrationv1.RuleWithOperations) bool {
	if len(rules) != 1 {
		return false
	}
	rule := rules[0]
	return slices.Equal(rule.Operations, []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update}) &&
		slices.Equal(rule.APIGroups, []string{"pgshard.io"}) &&
		slices.Equal(rule.APIVersions, []string{"v1alpha1"}) &&
		slices.Equal(rule.Resources, []string{"pgshardclusters"}) &&
		(rule.Scope == nil || *rule.Scope == admissionregistrationv1.AllScopes)
}

func matchesPostgreSQLBindingRules(rules []admissionregistrationv1.RuleWithOperations) bool {
	return matchesCoreRules(rules, []admissionregistrationv1.OperationType{admissionregistrationv1.Create}, "pods/binding")
}

func matchesPostgreSQLStatusRules(rules []admissionregistrationv1.RuleWithOperations) bool {
	return matchesCoreRules(rules, []admissionregistrationv1.OperationType{admissionregistrationv1.Update}, "pods/status")
}

func matchesPostgreSQLHandshakeRules(rules []admissionregistrationv1.RuleWithOperations) bool {
	if len(rules) != 1 {
		return false
	}
	rule := rules[0]
	return slices.Equal(rule.Operations, []admissionregistrationv1.OperationType{admissionregistrationv1.Update}) &&
		slices.Equal(rule.APIGroups, []string{"pgshard.io"}) && slices.Equal(rule.APIVersions, []string{"v1alpha1"}) &&
		slices.Equal(rule.Resources, []string{"pgshardclusters"}) &&
		(rule.Scope == nil || *rule.Scope == admissionregistrationv1.AllScopes)
}

func matchesPostgreSQLMetadataRules(rules []admissionregistrationv1.RuleWithOperations) bool {
	return matchesCoreResourceRules(rules, []admissionregistrationv1.OperationType{admissionregistrationv1.Update}, []string{"pods", "pods/ephemeralcontainers", "pods/resize"})
}

func matchesPostgreSQLNamespaceRules(rules []admissionregistrationv1.RuleWithOperations) bool {
	if len(rules) != 1 {
		return false
	}
	rule := rules[0]
	return slices.Equal(rule.Operations, []admissionregistrationv1.OperationType{admissionregistrationv1.Update}) &&
		slices.Equal(rule.APIGroups, []string{""}) && slices.Equal(rule.APIVersions, []string{"v1"}) &&
		slices.Equal(rule.Resources, []string{"namespaces", "namespaces/status", "namespaces/finalize"}) &&
		(rule.Scope == nil || *rule.Scope == admissionregistrationv1.AllScopes)
}

func matchesCoreRules(rules []admissionregistrationv1.RuleWithOperations, operations []admissionregistrationv1.OperationType, resource string) bool {
	return matchesCoreResourceRules(rules, operations, []string{resource})
}

func matchesCoreResourceRules(rules []admissionregistrationv1.RuleWithOperations, operations []admissionregistrationv1.OperationType, resources []string) bool {
	if len(rules) != 1 {
		return false
	}
	rule := rules[0]
	return slices.Equal(rule.Operations, operations) && slices.Equal(rule.APIGroups, []string{""}) &&
		slices.Equal(rule.APIVersions, []string{"v1"}) && slices.Equal(rule.Resources, resources) &&
		(rule.Scope == nil || *rule.Scope == admissionregistrationv1.AllScopes)
}

func podFencingNamespaceSelector() *metav1.LabelSelector {
	return &metav1.LabelSelector{MatchLabels: map[string]string{podfence.NamespaceLabel: podfence.NamespaceLabelValue}}
}

func postgreSQLPodSelector() *metav1.LabelSelector {
	return &metav1.LabelSelector{MatchLabels: map[string]string{
		"app.kubernetes.io/component": "postgresql",
		ManagedByLabel:                ManagedByValue,
	}}
}

func findMutatingWebhook(webhooks []admissionregistrationv1.MutatingWebhook, name string) *admissionregistrationv1.MutatingWebhook {
	for index := range webhooks {
		if webhooks[index].Name == name {
			return &webhooks[index]
		}
	}
	return nil
}

func findValidatingWebhook(webhooks []admissionregistrationv1.ValidatingWebhook, name string) *admissionregistrationv1.ValidatingWebhook {
	for index := range webhooks {
		if webhooks[index].Name == name {
			return &webhooks[index]
		}
	}
	return nil
}

func policyForMutating(webhook *admissionregistrationv1.MutatingWebhook) webhookPolicy {
	return webhookPolicy{
		name: webhook.Name, clientConfig: webhook.ClientConfig, rules: webhook.Rules,
		failurePolicy: webhook.FailurePolicy, matchPolicy: webhook.MatchPolicy,
		namespaceSelector: webhook.NamespaceSelector, objectSelector: webhook.ObjectSelector,
		sideEffects: webhook.SideEffects, timeoutSeconds: webhook.TimeoutSeconds,
		admissionReviewVersions: webhook.AdmissionReviewVersions, matchConditionCount: len(webhook.MatchConditions),
	}
}

func policyForValidating(webhook *admissionregistrationv1.ValidatingWebhook) webhookPolicy {
	return webhookPolicy{
		name: webhook.Name, clientConfig: webhook.ClientConfig, rules: webhook.Rules,
		failurePolicy: webhook.FailurePolicy, matchPolicy: webhook.MatchPolicy,
		namespaceSelector: webhook.NamespaceSelector, objectSelector: webhook.ObjectSelector,
		sideEffects: webhook.SideEffects, timeoutSeconds: webhook.TimeoutSeconds,
		admissionReviewVersions: webhook.AdmissionReviewVersions, matchConditionCount: len(webhook.MatchConditions),
	}
}

func (p *Provisioner) validateClientConfig(config admissionregistrationv1.WebhookClientConfig, caBundle []byte, wantedPath string) error {
	if config.URL != nil || config.Service == nil {
		return fmt.Errorf("must use a Kubernetes Service reference")
	}
	if config.Service.Name != p.serviceName || config.Service.Namespace != p.namespace {
		return fmt.Errorf("references Service %s/%s, want %s/%s", config.Service.Namespace, config.Service.Name, p.namespace, p.serviceName)
	}
	if config.Service.Path == nil {
		return fmt.Errorf("does not specify Service path %q", wantedPath)
	}
	if *config.Service.Path != wantedPath {
		return fmt.Errorf("references Service path %q, want %q", *config.Service.Path, wantedPath)
	}
	if config.Service.Port != nil && *config.Service.Port != webhookServicePort {
		return fmt.Errorf("references Service port %d, want %d", *config.Service.Port, webhookServicePort)
	}
	if len(config.CABundle) != 0 && !bytes.Equal(config.CABundle, caBundle) {
		return fmt.Errorf("contains a CA bundle that is not owned by the configured CA Secret")
	}
	return nil
}

func (p *Provisioner) injectCABundle(ctx context.Context, configs *configurations, caBundle []byte) error {
	mutatingBefore := configs.mutating.DeepCopy()
	mutatingChanged := false
	for index := range configs.mutating.Webhooks {
		if !bytes.Equal(configs.mutating.Webhooks[index].ClientConfig.CABundle, caBundle) {
			configs.mutating.Webhooks[index].ClientConfig.CABundle = caBundle
			mutatingChanged = true
		}
	}
	if mutatingChanged {
		patch := client.MergeFromWithOptions(mutatingBefore, client.MergeFromWithOptimisticLock{})
		if err := p.client.Patch(ctx, configs.mutating, patch); err != nil {
			return fmt.Errorf("inject mutating webhook CA bundle: %w", err)
		}
	}

	validatingBefore := configs.validating.DeepCopy()
	validatingChanged := false
	for index := range configs.validating.Webhooks {
		if !bytes.Equal(configs.validating.Webhooks[index].ClientConfig.CABundle, caBundle) {
			configs.validating.Webhooks[index].ClientConfig.CABundle = caBundle
			validatingChanged = true
		}
	}
	if validatingChanged {
		patch := client.MergeFromWithOptions(validatingBefore, client.MergeFromWithOptimisticLock{})
		if err := p.client.Patch(ctx, configs.validating, patch); err != nil {
			return fmt.Errorf("inject validating webhook CA bundle: %w", err)
		}
	}
	return nil
}

func (p *Provisioner) dnsNames() []string {
	base := p.serviceName + "." + p.namespace
	return []string{base + ".svc", base + ".svc.cluster.local"}
}

func retryableAPIError(err error) bool {
	return apierrors.IsNotFound(err) || apierrors.IsConflict(err) || apierrors.IsTimeout(err) ||
		apierrors.IsServerTimeout(err) || apierrors.IsTooManyRequests(err) || apierrors.IsServiceUnavailable(err)
}

func writeMaterial(directory string, contents *material) error {
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create certificate directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return fmt.Errorf("set certificate directory permissions: %w", err)
	}
	files := []struct {
		name     string
		contents []byte
		mode     os.FileMode
	}{
		{name: TLSPrivateKeyKey, contents: contents.serving.privateKeyPEM, mode: 0o600},
		{name: TLSCertificateKey, contents: contents.serving.certificatePEM, mode: 0o644},
		{name: CACertificateKey, contents: contents.ca.certificatePEM, mode: 0o644},
	}
	for _, file := range files {
		if err := writeAtomic(filepath.Join(directory, file.name), file.contents, file.mode); err != nil {
			return err
		}
	}
	return nil
}

func writeAtomic(path string, contents []byte, mode os.FileMode) error {
	if existing, err := os.ReadFile(path); err == nil && bytes.Equal(existing, contents) {
		if err := os.Chmod(path, mode); err != nil {
			return fmt.Errorf("set permissions on %s: %w", filepath.Base(path), err)
		}
		return nil
	} else if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read existing %s: %w", filepath.Base(path), err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+"-")
	if err != nil {
		return fmt.Errorf("create temporary %s: %w", filepath.Base(path), err)
	}
	temporaryName := temporary.Name()
	defer func() { _ = os.Remove(temporaryName) }()
	if err := temporary.Chmod(mode); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("set temporary %s permissions: %w", filepath.Base(path), err)
	}
	if _, err := temporary.Write(contents); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write temporary %s: %w", filepath.Base(path), err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync temporary %s: %w", filepath.Base(path), err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary %s: %w", filepath.Base(path), err)
	}
	if err := os.Rename(temporaryName, path); err != nil {
		return fmt.Errorf("replace %s: %w", filepath.Base(path), err)
	}
	return nil
}
