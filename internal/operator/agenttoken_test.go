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
// reconcilers sent only the token derived from the superuser password, so
// the derived token could not be withdrawn -- anything holding the
// superuser password holds one that unlocks Promote, Demote, Rewind and
// Reclone (PGS-428). They now send the cluster's own token first and the
// derived one second, which is what lets PGS-572 drop the derived half.
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
	derived, err := agentauth.Token("superuser-password")
	if err != nil {
		t.Fatal(err)
	}
	if len(sent) != 2 {
		t.Fatalf("sent %d tokens %v, want the cluster's own and the derived one", len(sent), sent)
	}
	if sent[0] != mounted {
		t.Errorf("first token = %q, want the cluster's own %q (trimmed of its newline)", sent[0], mounted)
	}
	if sent[1] != derived {
		t.Errorf("second token = %q, want the derived one so an agent not yet rolled is still reachable", sent[1])
	}
}

// A cluster whose agent Secret is missing still reaches agents on the
// derived token; losing that would strand a cluster mid-rollout.
func TestAMissingAgentSecretStillSendsTheDerivedToken(t *testing.T) {
	const cluster, ns = "demo", "default"
	cl := fakeClient(t, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: SecretName(cluster), Namespace: ns},
		Data:       map[string][]byte{secretKey: []byte("superuser-password")},
	})
	if got := tokensIn(withClusterAgentToken(context.Background(), cl, ns, cluster), t); len(got) != 1 {
		t.Fatalf("sent %v, want only the derived token", got)
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
