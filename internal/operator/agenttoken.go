package operator

import (
	"context"
	"strings"

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
//
// It sends the cluster's own token and the derived one, in that order, for
// the reason the main reconcile loop does: one caller then reaches an agent
// that has been rolled onto the cluster token and one that has not. Sending
// only the derived token -- which these callers used to do -- is what kept
// the derived token alive, because it could not be withdrawn while anything
// still depended on it. PGS-572 removes the derived half once no caller
// sends it.
func withClusterAgentToken(ctx context.Context, c client.Client, namespace, cluster string) context.Context {
	var tokens []string
	if tok, err := mountedAgentToken(ctx, c, namespace, cluster); err == nil && tok != "" {
		tokens = append(tokens, tok)
	}
	if tok, err := clusterAgentToken(ctx, c, namespace, cluster); err == nil && tok != "" {
		tokens = append(tokens, tok)
	}
	if len(tokens) == 0 {
		return ctx
	}
	return agentauth.WithTokens(ctx, tokens...)
}
