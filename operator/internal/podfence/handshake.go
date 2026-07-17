package podfence

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	handshakeReceiptVersion = "v1"
	// SecretKeyBytes is the exact length of a pgshard receipt key.
	SecretKeyBytes = sha256.Size
	// SecretKeyContinuityAnnotation records that the independent fingerprint
	// anchor was durably installed for this key generation.
	SecretKeyContinuityAnnotation = "pgshard.io/pod-fencing-key-continuity"
	SecretKeyContinuityValue      = "v1"
)

type HandshakeCodec struct {
	key func(context.Context) ([]byte, error)
}

// SecretReceiptKeyRef names the Secret data and independent continuity anchor
// that together authorize receipt signing and verification.
type SecretReceiptKeyRef struct {
	Secret           types.NamespacedName
	DataKey          string
	AnchorSecret     types.NamespacedName
	AnchorAnnotation string
}

func NewSecretHandshakeCodec(reader client.Reader, ref SecretReceiptKeyRef) *HandshakeCodec {
	return &HandshakeCodec{key: func(ctx context.Context) ([]byte, error) {
		if reader == nil {
			return nil, fmt.Errorf("Pod fencing handshake Secret reader is required")
		}
		value := &corev1.Secret{}
		if err := reader.Get(ctx, ref.Secret, value); err != nil {
			return nil, fmt.Errorf("read Pod fencing handshake Secret %s: %w", ref.Secret, err)
		}
		key, err := ValidateSecretHandshakeKey(value, ref.DataKey)
		if err != nil {
			return nil, err
		}
		anchor := &corev1.Secret{}
		if err := reader.Get(ctx, ref.AnchorSecret, anchor); err != nil {
			return nil, fmt.Errorf("read Pod fencing handshake anchor Secret %s: %w", ref.AnchorSecret, err)
		}
		if err := ValidateSecretHandshakeKeyFingerprint(anchor, ref.AnchorAnnotation, key); err != nil {
			return nil, err
		}
		return key, nil
	}}
}

func NewStaticHandshakeCodec(key []byte) *HandshakeCodec {
	key = bytes.Clone(key)
	return &HandshakeCodec{key: func(context.Context) ([]byte, error) {
		if len(key) != SecretKeyBytes {
			return nil, fmt.Errorf("Pod fencing handshake key must be exactly %d bytes", SecretKeyBytes)
		}
		return key, nil
	}}
}

// ValidateSecretHandshakeKey returns the exact immutable key from an
// operator-owned Secret.
func ValidateSecretHandshakeKey(secret *corev1.Secret, dataKey string) ([]byte, error) {
	key, err := ValidateSecretHandshakeKeyCandidate(secret, dataKey)
	if err != nil {
		return nil, err
	}
	if secret.Annotations[SecretKeyContinuityAnnotation] != SecretKeyContinuityValue {
		return nil, fmt.Errorf("Pod fencing handshake Secret %s/%s lacks its continuity marker", secret.Namespace, secret.Name)
	}
	return key, nil
}

// ValidateSecretHandshakeKeyCandidate validates a pre-anchor key during a
// bounded migration without treating it as runtime signing authority.
func ValidateSecretHandshakeKeyCandidate(secret *corev1.Secret, dataKey string) ([]byte, error) {
	if err := validateOwnedOpaqueSecret(secret); err != nil {
		return nil, err
	}
	if secret.Immutable == nil || !*secret.Immutable {
		return nil, fmt.Errorf("Pod fencing handshake Secret %s/%s must be immutable", secret.Namespace, secret.Name)
	}
	key, exists := secret.Data[dataKey]
	if dataKey == "" || len(secret.Data) != 1 || !exists || len(key) != SecretKeyBytes {
		return nil, fmt.Errorf("Pod fencing handshake Secret %s/%s must contain exactly one %d-byte %s", secret.Namespace, secret.Name, SecretKeyBytes, dataKey)
	}
	return bytes.Clone(key), nil
}

// SecretHandshakeKeyFingerprint returns the continuity anchor for a receipt
// key. The fingerprint is not secret.
func SecretHandshakeKeyFingerprint(key []byte) string {
	fingerprint := sha256.Sum256(key)
	return hex.EncodeToString(fingerprint[:])
}

// ValidateSecretHandshakeKeyFingerprint proves that key is the generation
// anchored outside its replaceable key Secret.
func ValidateSecretHandshakeKeyFingerprint(secret *corev1.Secret, annotationKey string, key []byte) error {
	if len(key) != SecretKeyBytes {
		return fmt.Errorf("Pod fencing handshake key must be exactly %d bytes", SecretKeyBytes)
	}
	if err := validateOwnedOpaqueSecret(secret); err != nil {
		return err
	}
	encoded, exists := secret.Annotations[annotationKey]
	fingerprint, err := hex.DecodeString(encoded)
	if annotationKey == "" || !exists || err != nil || len(fingerprint) != sha256.Size || encoded != hex.EncodeToString(fingerprint) {
		return fmt.Errorf("Pod fencing handshake anchor Secret %s/%s must contain a canonical SHA-256 annotation %s", secret.Namespace, secret.Name, annotationKey)
	}
	wanted := sha256.Sum256(key)
	if !hmac.Equal(fingerprint, wanted[:]) {
		return fmt.Errorf("Pod fencing handshake key does not match the anchored fingerprint; restore the original key or perform explicit fencing recovery")
	}
	return nil
}

func validateOwnedOpaqueSecret(secret *corev1.Secret) error {
	if secret == nil {
		return fmt.Errorf("Pod fencing handshake Secret is required")
	}
	if secret.Labels[owned.ManagedByLabel] != owned.ManagedByValue {
		return fmt.Errorf("Secret %s/%s is not labeled as managed by %s", secret.Namespace, secret.Name, owned.ManagedByValue)
	}
	if secret.Type != corev1.SecretTypeOpaque {
		return fmt.Errorf("managed Secret %s/%s has type %q, want %q", secret.Namespace, secret.Name, secret.Type, corev1.SecretTypeOpaque)
	}
	return nil
}

func (c *HandshakeCodec) Receipt(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (string, error) {
	challenge := cluster.Annotations[HandshakeChallengeAnnotation]
	if cluster.UID == "" || cluster.Namespace == "" || cluster.Name == "" || challenge == "" {
		return "", fmt.Errorf("Pod fencing handshake requires cluster UID, namespace, name, and challenge")
	}
	payload, err := json.Marshal(struct {
		Version   string    `json:"version"`
		Namespace string    `json:"namespace"`
		Name      string    `json:"name"`
		UID       types.UID `json:"uid"`
		Challenge string    `json:"challenge"`
	}{
		Version: handshakeReceiptVersion, Namespace: cluster.Namespace, Name: cluster.Name,
		UID: cluster.UID, Challenge: challenge,
	})
	if err != nil {
		return "", fmt.Errorf("encode Pod fencing handshake payload: %w", err)
	}
	return c.authenticate(ctx, payload)
}

func (c *HandshakeCodec) Verify(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (bool, error) {
	receipt := cluster.Annotations[HandshakeReceiptAnnotation]
	if receipt == "" || cluster.Annotations[HandshakeChallengeAnnotation] == "" {
		return false, nil
	}
	expected, err := c.Receipt(ctx, cluster)
	if err != nil {
		return false, err
	}
	return hmac.Equal([]byte(receipt), []byte(expected)), nil
}

func (c *HandshakeCodec) TerminationReceipt(ctx context.Context, pod *corev1.Pod) (string, error) {
	if pod.UID == "" || pod.Namespace == "" || pod.Name == "" || pod.Spec.NodeName == "" ||
		pod.Annotations[NodeUIDAnnotation] == "" || pod.Annotations[NodeBootIDAnnotation] == "" || !hasTerminalPhase(pod) {
		return "", fmt.Errorf("Pod termination receipt requires Pod identity, binding identity, and terminal phase")
	}
	payload, err := json.Marshal(struct {
		Version    string          `json:"version"`
		Namespace  string          `json:"namespace"`
		Name       string          `json:"name"`
		UID        types.UID       `json:"uid"`
		Generation int64           `json:"generation"`
		NodeName   string          `json:"nodeName"`
		NodeUID    string          `json:"nodeUID"`
		NodeBootID string          `json:"nodeBootID"`
		Phase      corev1.PodPhase `json:"phase"`
	}{
		Version: handshakeReceiptVersion, Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID,
		Generation: pod.Generation, NodeName: pod.Spec.NodeName, NodeUID: pod.Annotations[NodeUIDAnnotation],
		NodeBootID: pod.Annotations[NodeBootIDAnnotation], Phase: pod.Status.Phase,
	})
	if err != nil {
		return "", fmt.Errorf("encode Pod termination receipt payload: %w", err)
	}
	return c.authenticate(ctx, payload)
}

func (c *HandshakeCodec) VerifyTermination(ctx context.Context, pod *corev1.Pod) (bool, error) {
	if !HasTerminationAttestation(pod) {
		return false, nil
	}
	condition, _ := terminationAttestation(pod)
	receipt := terminationReceiptFromMessage(condition.Message)
	expected, err := c.TerminationReceipt(ctx, pod)
	if err != nil {
		return false, err
	}
	return hmac.Equal([]byte(receipt), []byte(expected)), nil
}

func (c *HandshakeCodec) authenticate(ctx context.Context, payload []byte) (string, error) {
	if c == nil || c.key == nil {
		return "", fmt.Errorf("Pod fencing receipt codec is required")
	}
	key, err := c.key(ctx)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, key)
	if _, err := mac.Write(payload); err != nil {
		return "", fmt.Errorf("authenticate Pod fencing receipt payload: %w", err)
	}
	return handshakeReceiptVersion + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}
