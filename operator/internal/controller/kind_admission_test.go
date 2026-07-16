package controller

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/operator/internal/pki"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

func TestKINDAdmissionWebhooksUseManagedTLSAndRejectUnsafeSpec(t *testing.T) {
	if os.Getenv("PGSHARD_KIND_MANAGER_E2E") != "true" {
		t.Skip("set PGSHARD_KIND_MANAGER_E2E=true against the installed admission manager")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
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
	if err := kubeClient.Create(ctx, valid, &client.CreateOptions{DryRun: []string{metav1.DryRunAll}}); err != nil {
		t.Fatalf("dry-run defaulted create: %v", err)
	}
	if valid.Spec.Shards != 1 || valid.Spec.MembersPerShard != 3 || valid.Spec.Durability != pgshardv1alpha1.DurabilitySynchronous || valid.Spec.PostgreSQL.Version != pgshardv1alpha1.PostgreSQLMajor18 || valid.Spec.Storage.DeletionPolicy != pgshardv1alpha1.DeletionRetain || valid.Spec.Pooler.Scaling.Mode != pgshardv1alpha1.ScalingHPA || valid.Spec.Observability.Prometheus == nil || !*valid.Spec.Observability.Prometheus {
		t.Fatalf("admission defaults = %#v", valid.Spec)
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
	if fencingKeySecret.Immutable == nil || !*fencingKeySecret.Immutable || len(fencingKeySecret.Data) != 1 || len(fencingKeySecret.Data[pki.PodFencingKeyKey]) != 32 {
		t.Fatalf("webhook Pod fencing key = %#v", fencingKeySecret)
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
	if len(mutating.Webhooks) != 4 || len(validating.Webhooks) != 5 {
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
