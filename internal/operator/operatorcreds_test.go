package operator

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/grpccreds"
	"github.com/andrew01234567890/pgshard/internal/pki"
)

// issuedListener serves an unimplemented Agent on 127.0.0.1 with the issued
// certificate of role from c's Secrets, admitting the callers pki allows.
func issuedListener(t *testing.T, r *ClusterReconciler, c *pgshardv1alpha1.PgShardCluster, role string) string {
	t.Helper()
	sec := secretOf(t, r, c.Namespace, RoleTLSSecretName(c.Name, role))
	dir := t.TempDir()
	cert, key, ca := writeTempPEM(t, dir, "tls.crt", sec.Data["tls.crt"]), writeTempPEM(t, dir, "tls.key", sec.Data["tls.key"]), writeTempPEM(t, dir, "ca.crt", sec.Data["ca.crt"])
	var opts []grpccreds.Option
	if allow, ok := pki.AllowedCallers(role); ok && role == pki.RoleAgent {
		opts = append(opts, grpccreds.Authorize(allow))
	}
	creds, err := grpccreds.Listener(cert, key, ca, false, opts...)
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer(grpc.Creds(creds))
	pgshardv1.RegisterAgentServer(g, pgshardv1.UnimplementedAgentServer{})
	go func() { _ = g.Serve(l) }()
	t.Cleanup(g.Stop)
	return l.Addr().String()
}

// PGS-860: the operator's agent client took credentials only from
// process-wide flags the deployment does not set, so it dialled an issuing
// cluster's agents -- which require mTLS -- in plaintext, and every Promote,
// Demote, fence and backup call failed. And one process-wide certificate
// could not chain to every issuing cluster's CA anyway. Each cluster's
// agents are now dialled with that cluster's own operator credentials,
// recorded beside each member's mode as the reconciler observes its pod.
func TestAnIssuingClustersAgentIsDialledWithThatClustersCredentials(t *testing.T) {
	ctx := context.Background()
	c := issuingCluster("creds")
	other := issuingCluster("elsewhere")
	r := pkiReconciler(t, time.Now(), c, other)
	for _, cl := range []*pgshardv1alpha1.PgShardCluster{c, other} {
		if err := r.reconcilePKI(ctx, cl); err != nil {
			t.Fatal(err)
		}
	}
	agentAddrOnLoopback := issuedListener(t, r, c, pki.RoleAgent)

	// The reconciler observes the member's pod and records how to dial it.
	g := Groups(c)[1]
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: g.MemberNames()[0], Namespace: c.Namespace,
		Annotations: map[string]string{AnnotationAgentMTLS: "true"}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.1.2.3"}}
	if err := r.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	modes := &AgentTLSModes{}
	r.AgentTLS, r.OperatorCreds = modes, &IssuedCredentials{Client: r.Client}
	if _, err := r.observePod(ctx, c, g, 0, groupState{}, MemberTemplate{}, false); err != nil {
		t.Fatal(err)
	}
	recorded := modes.Credentials(agentAddr("10.1.2.3"))
	if recorded == nil || !modes.Requires(agentAddr("10.1.2.3")) {
		t.Fatal("observing an issuing cluster's member recorded no credentials to dial its agent with")
	}

	call := func(client *GRPCAgentClient, addr string) error {
		t.Helper()
		defer client.Close()
		cl, err := client.dial(ctx, addr)
		if err != nil {
			return err
		}
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err = cl.Status(cctx, &pgshardv1.StatusRequest{})
		if status.Code(err) == codes.Unimplemented {
			return nil
		}
		return err
	}
	withModes := func(m *AgentTLSModes) *GRPCAgentClient {
		cl := NewGRPCAgentClient()
		cl.RequiresTLS, cl.CredsFor = m.Requires, m.Credentials
		return cl
	}

	// The same credentials against the member's agent, here on loopback.
	loop := &AgentTLSModes{}
	loop.Set(agentAddrOnLoopback, true)
	loop.SetCredentials(agentAddrOnLoopback, recorded)
	if err := call(withModes(loop), agentAddrOnLoopback); err != nil {
		t.Fatalf("the operator could not reach an issuing cluster's agent with that cluster's credentials: %v", err)
	}
	// Without them it dials plaintext, which the agent refuses.
	bare := &AgentTLSModes{}
	bare.Set(agentAddrOnLoopback, true)
	if err := call(withModes(bare), agentAddrOnLoopback); err == nil {
		t.Fatal("an agent requiring mTLS answered a caller with no credentials")
	}
	// A pooler's certificate is valid for the same name; it must not answer
	// as an agent.
	poolerOnLoopback := issuedListener(t, r, c, pki.RolePooler)
	impostor := &AgentTLSModes{}
	impostor.Set(poolerOnLoopback, true)
	impostor.SetCredentials(poolerOnLoopback, recorded)
	// The agent client waits for the connection to be ready, so the reason
	// is not in its error; the role check is pinned by mutation.
	if err := call(withModes(impostor), poolerOnLoopback); err == nil {
		t.Fatal("a pooler's certificate answered the operator dialling an agent")
	}
	// Another cluster's credentials chain to another CA.
	otherAgent, _, err := (&IssuedCredentials{Client: r.Client}).For(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	foreign := &AgentTLSModes{}
	foreign.Set(agentAddrOnLoopback, true)
	foreign.SetCredentials(agentAddrOnLoopback, otherAgent)
	if err := call(withModes(foreign), agentAddrOnLoopback); err == nil {
		t.Fatal("another cluster's operator credentials reached this cluster's agent")
	}
}

// The credentials are built once per Secret version, so a kept connection is
// not redialled on every pass, and rebuilt when the Secret changes.
func TestIssuedCredentialsFollowTheSecret(t *testing.T) {
	ctx := context.Background()
	c := issuingCluster("follow")
	r := pkiReconciler(t, time.Now(), c)
	if err := r.reconcilePKI(ctx, c); err != nil {
		t.Fatal(err)
	}
	o := &IssuedCredentials{Client: r.Client}
	a1, c1, err := o.For(ctx, c)
	if err != nil || a1 == nil || c1 == nil {
		t.Fatalf("credentials: %v %v %v", a1, c1, err)
	}
	a2, _, _ := o.For(ctx, c)
	if a2 != a1 {
		t.Fatal("the same Secret built new credentials, which redials every kept connection")
	}
	sec := secretOf(t, r, c.Namespace, RoleTLSSecretName(c.Name, pki.RoleOperator))
	sec.Labels = map[string]string{"touched": "yes"}
	if err := r.Update(ctx, &sec); err != nil {
		t.Fatal(err)
	}
	if a3, _, _ := o.For(ctx, c); a3 == a1 {
		t.Fatal("a changed Secret kept the old credentials")
	}
	plain := newCluster("plain")
	if a, ctl, err := o.For(ctx, plain); a != nil || ctl != nil || err != nil {
		t.Fatalf("a cluster the operator does not issue for got credentials: %v %v %v", a, ctl, err)
	}
}

func writeTempPEM(t *testing.T, dir, name string, b []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// The operator's scheduled barriers dial the controller's Service host with
// the cluster's own credentials, and only a controller may answer.
func TestAnIssuingClustersControllerIsDialledWithThatClustersCredentials(t *testing.T) {
	ctx := context.Background()
	c := issuingCluster("barriers")
	r := pkiReconciler(t, time.Now(), c)
	if err := r.reconcilePKI(ctx, c); err != nil {
		t.Fatal(err)
	}
	_, controllerCreds, err := (&IssuedCredentials{Client: r.Client}).For(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	serve := func(role string) string {
		t.Helper()
		sec := secretOf(t, r, c.Namespace, RoleTLSSecretName(c.Name, role))
		dir := t.TempDir()
		creds, err := grpccreds.Listener(writeTempPEM(t, dir, "tls.crt", sec.Data["tls.crt"]), writeTempPEM(t, dir, "tls.key", sec.Data["tls.key"]), writeTempPEM(t, dir, "ca.crt", sec.Data["ca.crt"]), false)
		if err != nil {
			t.Fatal(err)
		}
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		g := grpc.NewServer(grpc.Creds(creds))
		pgshardv1.RegisterControllerServer(g, pgshardv1.UnimplementedControllerServer{})
		go func() { _ = g.Serve(l) }()
		t.Cleanup(g.Stop)
		return l.Addr().String()
	}
	// A backup policy may name any endpoint for the controller; it resolves,
	// here, to whichever listener the test says.
	call := func(listener string) error {
		t.Helper()
		host := "barriers.example.internal:1"
		cc, err := grpc.NewClient("passthrough:///"+host, grpc.WithTransportCredentials(controllerCreds),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "tcp", listener)
			}))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cc.Close() }()
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err = pgshardv1.NewControllerClient(cc).CreateBarrier(cctx, &pgshardv1.CreateBarrierRequest{Name: "b"})
		if status.Code(err) == codes.Unimplemented {
			return nil
		}
		return err
	}
	if err := call(serve(pki.RoleController)); err != nil {
		t.Fatalf("the operator could not reach an issuing cluster's controller with that cluster's credentials: %v", err)
	}
	if err := call(serve(pki.RoleRouter)); err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("a router's certificate answering the operator dialling the controller was not refused: %v", err)
	}
	// A certificate from the cluster's CA that names the controller's host
	// but is not the controller's: refused for its identity, not its name.
	caSec := secretOf(t, r, c.Namespace, CASecretName(c.Name))
	ca, err := pki.LoadCA(pki.Material{CertPEM: caSec.Data["ca.crt"], KeyPEM: caSec.Data["ca.key"]})
	if err != nil {
		t.Fatal(err)
	}
	forged, err := ca.Issue(pki.Request{Identity: pki.Identity{Namespace: c.Namespace, Cluster: c.Name, Role: pki.RoleAdmin},
		DNSNames: []string{ControllerName(c.Name) + "." + c.Namespace + ".svc"}, Server: true}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	creds, err := grpccreds.Listener(writeTempPEM(t, dir, "tls.crt", forged.CertPEM), writeTempPEM(t, dir, "tls.key", forged.KeyPEM), writeTempPEM(t, dir, "ca.crt", caSec.Data["ca.crt"]), false)
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer(grpc.Creds(creds))
	pgshardv1.RegisterControllerServer(g, pgshardv1.UnimplementedControllerServer{})
	go func() { _ = g.Serve(l) }()
	t.Cleanup(g.Stop)
	if err := call(l.Addr().String()); err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("a certificate for the controller's host with another role's identity was not refused for it: %v", err)
	}
}
