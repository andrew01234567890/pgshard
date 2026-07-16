package podfence

import (
	"bytes"
	"context"
	"strings"
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestSecretHandshakeCodecValidatesAnchoredKeyState(t *testing.T) {
	t.Parallel()
	keyName := types.NamespacedName{Namespace: "pgshard-system", Name: "receipt-key"}
	anchorName := types.NamespacedName{Namespace: "pgshard-system", Name: "receipt-anchor"}
	dataKey := "hmac.key"
	anchorAnnotation := "pgshard.io/pod-fencing-key-sha256"
	key := []byte("0123456789abcdef0123456789abcdef")
	immutable := true
	baseKey := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: keyName.Namespace,
			Name:      keyName.Name,
			Labels:    map[string]string{owned.ManagedByLabel: owned.ManagedByValue},
			Annotations: map[string]string{
				SecretKeyContinuityAnnotation: SecretKeyContinuityValue,
			},
		},
		Type:      corev1.SecretTypeOpaque,
		Immutable: &immutable,
		Data:      map[string][]byte{dataKey: bytes.Clone(key)},
	}
	baseAnchor := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: anchorName.Namespace,
			Name:      anchorName.Name,
			Labels:    map[string]string{owned.ManagedByLabel: owned.ManagedByValue},
			Annotations: map[string]string{
				anchorAnnotation: SecretHandshakeKeyFingerprint(key),
			},
		},
		Type: corev1.SecretTypeOpaque,
	}
	for _, test := range []struct {
		name       string
		mutate     func(*corev1.Secret, *corev1.Secret)
		omitAnchor bool
		want       string
	}{
		{name: "valid"},
		{
			name: "unmanaged",
			mutate: func(key, _ *corev1.Secret) {
				key.Labels = nil
			},
			want: "is not labeled as managed",
		},
		{
			name: "wrong type",
			mutate: func(key, _ *corev1.Secret) {
				key.Type = corev1.SecretTypeTLS
			},
			want: "has type",
		},
		{
			name: "mutable",
			mutate: func(key, _ *corev1.Secret) {
				key.Immutable = nil
			},
			want: "must be immutable",
		},
		{
			name: "oversized",
			mutate: func(key, _ *corev1.Secret) {
				key.Data[dataKey] = make([]byte, SecretKeyBytes+1)
			},
			want: "exactly one 32-byte",
		},
		{
			name: "missing continuity marker",
			mutate: func(key, _ *corev1.Secret) {
				key.Annotations = nil
			},
			want: "lacks its continuity marker",
		},
		{
			name: "different valid key",
			mutate: func(key, _ *corev1.Secret) {
				key.Data[dataKey][0] ^= 0xff
			},
			want: "does not match the anchored fingerprint",
		},
		{
			name:       "missing anchor",
			omitAnchor: true,
			want:       "read Pod fencing handshake anchor Secret",
		},
		{
			name: "unmanaged anchor",
			mutate: func(_, anchor *corev1.Secret) {
				anchor.Labels = nil
			},
			want: "is not labeled as managed",
		},
		{
			name: "wrong anchor type",
			mutate: func(_, anchor *corev1.Secret) {
				anchor.Type = corev1.SecretTypeTLS
			},
			want: "has type",
		},
		{
			name: "missing anchor fingerprint",
			mutate: func(_, anchor *corev1.Secret) {
				anchor.Annotations = nil
			},
			want: "canonical SHA-256 annotation",
		},
		{
			name: "malformed anchor fingerprint",
			mutate: func(_, anchor *corev1.Secret) {
				anchor.Annotations[anchorAnnotation] = "not-a-fingerprint"
			},
			want: "canonical SHA-256 annotation",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			keySecret := baseKey.DeepCopy()
			anchorSecret := baseAnchor.DeepCopy()
			if test.mutate != nil {
				test.mutate(keySecret, anchorSecret)
			}
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(keySecret)
			if !test.omitAnchor {
				builder = builder.WithObjects(anchorSecret)
			}
			reader := builder.Build()
			codec := NewSecretHandshakeCodec(reader, keyName, dataKey, anchorName, anchorAnnotation)
			cluster := &pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{
				Namespace: "application", Name: "database", UID: "cluster-uid",
				Annotations: map[string]string{HandshakeChallengeAnnotation: "challenge"},
			}}
			_, err := codec.Receipt(context.Background(), cluster)
			if test.want == "" && err != nil {
				t.Fatal(err)
			}
			if test.want != "" && (err == nil || !strings.Contains(err.Error(), test.want)) {
				t.Fatalf("Receipt() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestStaticHandshakeCodecRequiresAnExactLengthKey(t *testing.T) {
	t.Parallel()
	cluster := &pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{
		Namespace: "application", Name: "database", UID: "cluster-uid",
		Annotations: map[string]string{HandshakeChallengeAnnotation: "challenge"},
	}}
	for _, size := range []int{SecretKeyBytes - 1, SecretKeyBytes + 1} {
		if _, err := NewStaticHandshakeCodec(make([]byte, size)).Receipt(context.Background(), cluster); err == nil || !strings.Contains(err.Error(), "exactly 32 bytes") {
			t.Fatalf("%d-byte static key error = %v", size, err)
		}
	}
}
