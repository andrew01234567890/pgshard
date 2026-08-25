package operator

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/andrew01234567890/pgshard/internal/agentauth"
)

// clusterAgentToken derives the agent auth token from the cluster's
// superuser Secret; agents derive the same token from their password file.
func clusterAgentToken(ctx context.Context, c client.Client, namespace, cluster string) (string, error) {
	var sec corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: SecretName(cluster)}, &sec); err != nil {
		return "", err
	}
	return agentauth.Token(string(sec.Data[secretKey]))
}

// withClusterAgentToken stamps ctx so agent RPCs authenticate; a missing
// secret leaves ctx unchanged (the agents are not up either).
func withClusterAgentToken(ctx context.Context, c client.Client, namespace, cluster string) context.Context {
	tok, err := clusterAgentToken(ctx, c, namespace, cluster)
	if err != nil {
		return ctx
	}
	return agentauth.WithToken(ctx, tok)
}
