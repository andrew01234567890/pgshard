package operator

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/andrew01234567890/pgshard/internal/agentauth"
)

// mountedAgentToken reads the cluster's own agent token, the one the
// operator generates and mounts into every member. Unlike the derived
// token it is independent of the superuser password, which is the whole
// point of it.
func mountedAgentToken(ctx context.Context, c client.Client, namespace, cluster string) (string, error) {
	var sec corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: AgentSecretName(cluster)}, &sec); err != nil {
		return "", err
	}
	return strings.TrimSpace(string(sec.Data[agentTokenKey])), nil
}

// withClusterAgentToken stamps ctx so agent RPCs authenticate; a missing
// secret leaves ctx unchanged (the agents are not up either).
func withClusterAgentToken(ctx context.Context, c client.Client, namespace, cluster string) context.Context {
	return withClusterAgentTokens(ctx, c, namespace, cluster)
}

// withClusterAgentTokens is the same for a caller that reaches agents of
// more than one cluster. Every cluster's agent token is generated
// independently -- ensureAgentSecret makes a fresh random one for a cluster
// that has none, and nothing copies it between clusters -- so a pass that
// spans two of them has to carry both.
func withClusterAgentTokens(ctx context.Context, c client.Client, namespace string, clusters ...string) context.Context {
	var tokens []string
	for _, name := range clusters {
		if tok, err := mountedAgentToken(ctx, c, namespace, name); err == nil && tok != "" {
			tokens = append(tokens, tok)
		}
	}
	if len(tokens) == 0 {
		return ctx
	}
	return agentauth.WithTokens(ctx, tokens...)
}
