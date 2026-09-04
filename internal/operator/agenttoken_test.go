package operator

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc/metadata"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/andrew01234567890/pgshard/internal/agentauth"
)

// TestEveryAgentCallerSendsTheClustersOwnToken: the backup and restore
// reconcilers once sent only a token derived from the superuser password,
// so anything holding that password held one that unlocks Promote, Demote,
// Rewind and Reclone (PGS-428). They send the cluster's own token, and now
// nothing else: the derived token is gone (PGS-572), so a superuser
// password is no longer a control-plane credential.
func TestEveryAgentCallerSendsTheClustersOwnToken(t *testing.T) {
	const cluster, ns = "demo", "default"
	mounted := "the-clusters-own-token"
	cl := fakeClient(t,
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: AgentSecretName(cluster), Namespace: ns},
			Data:       map[string][]byte{agentTokenKey: []byte(mounted + "\n")},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: SecretName(cluster), Namespace: ns},
			Data:       map[string][]byte{secretKey: []byte("superuser-password")},
		},
	)

	sent := tokensIn(withClusterAgentToken(context.Background(), cl, ns, cluster), t)
	if len(sent) != 1 {
		t.Fatalf("sent %d tokens %v, want only the cluster's own", len(sent), sent)
	}
	if sent[0] != mounted {
		t.Errorf("token = %q, want the cluster's own %q (trimmed of its newline)", sent[0], mounted)
	}
}

// The superuser Secret is not a fallback. A cluster whose agent Secret is
// missing sends nothing rather than reaching agents on a token derived from
// the superuser password -- that fallback is what made the password a
// control-plane credential.
func TestAMissingAgentSecretSendsNoToken(t *testing.T) {
	const cluster, ns = "demo", "default"
	cl := fakeClient(t, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: SecretName(cluster), Namespace: ns},
		Data:       map[string][]byte{secretKey: []byte("superuser-password")},
	})
	if got := tokensIn(withClusterAgentToken(context.Background(), cl, ns, cluster), t); len(got) != 0 {
		t.Fatalf("sent %v, want nothing: the superuser password is not an agent token", got)
	}
}

// Neither secret means the agents are not up either, and the context is
// left alone rather than carrying an empty credential.
func TestNoSecretsSendsNoToken(t *testing.T) {
	cl := fakeClient(t)
	if got := tokensIn(withClusterAgentToken(context.Background(), cl, "default", "demo"), t); len(got) != 0 {
		t.Fatalf("sent %v, want nothing", got)
	}
}

func tokensIn(ctx context.Context, t *testing.T) []string {
	t.Helper()
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return nil
	}
	var out []string
	for _, v := range md.Get(agentauth.MetadataKey) {
		if strings.TrimSpace(v) != "" {
			out = append(out, v)
		}
	}
	return out
}
