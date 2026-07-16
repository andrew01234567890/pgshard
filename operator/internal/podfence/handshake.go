package podfence

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const handshakeReceiptVersion = "v1"

type HandshakeCodec struct {
	key func(context.Context) ([]byte, error)
}

func NewSecretHandshakeCodec(reader client.Reader, secret types.NamespacedName, dataKey string) *HandshakeCodec {
	return &HandshakeCodec{key: func(ctx context.Context) ([]byte, error) {
		if reader == nil {
			return nil, fmt.Errorf("Pod fencing handshake Secret reader is required")
		}
		value := &corev1.Secret{}
		if err := reader.Get(ctx, secret, value); err != nil {
			return nil, fmt.Errorf("read Pod fencing handshake Secret %s: %w", secret, err)
		}
		key := value.Data[dataKey]
		if len(key) < sha256.Size {
			return nil, fmt.Errorf("Pod fencing handshake Secret %s key %q is missing or too short", secret, dataKey)
		}
		return key, nil
	}}
}

func NewStaticHandshakeCodec(key []byte) *HandshakeCodec {
	return &HandshakeCodec{key: func(context.Context) ([]byte, error) {
		if len(key) < sha256.Size {
			return nil, fmt.Errorf("Pod fencing handshake key is too short")
		}
		return key, nil
	}}
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
	if receipt == "" {
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
