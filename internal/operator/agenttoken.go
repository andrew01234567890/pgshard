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
	tok, err := mountedAgentToken(ctx, c, namespace, cluster)
	if err != nil || tok == "" {
		return ctx
	}
	return agentauth.WithToken(ctx, tok)
}
