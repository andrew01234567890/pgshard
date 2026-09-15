package operator

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/pki"
)

func issuingCluster(name string) *pgshardv1alpha1.PgShardCluster {
	c := newCluster(name)
	c.Spec.InternalTLS = pgshardv1alpha1.InternalTLSSpec{Issue: true}
	return c
}

func pkiReconciler(t *testing.T, at time.Time, objs ...client.Object) *ClusterReconciler {
	t.Helper()
	return &ClusterReconciler{Client: fakeClient(t, objs...), Now: func() time.Time { return at }}
}

func secretOf(t *testing.T, r *ClusterReconciler, ns, name string) corev1.Secret {
	t.Helper()
	var sec corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, &sec); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return sec
}

func TestEveryWorkloadGetsItsOwnCertificate(t *testing.T) {
	c := issuingCluster("mint")
	r := pkiReconciler(t, time.Now(), c)
	if err := r.reconcilePKI(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for role := range issuedRoles {
		sec := secretOf(t, r, c.Namespace, RoleTLSSecretName(c.Name, role))
		id, err := pki.IdentityOf(sec.Data["tls.crt"])
		if err != nil {
			t.Fatalf("%s: %v", role, err)
		}
		if id.Role != role || id.Cluster != c.Name || id.Namespace != c.Namespace {
			t.Fatalf("%s carries identity %+v", role, id)
		}
		if seen[string(sec.Data["tls.crt"])] {
			t.Fatalf("%s shares a certificate with another role, which is the thing this exists to stop", role)
		}
		seen[string(sec.Data["tls.crt"])] = true
		if len(sec.Data["ca.crt"]) == 0 {
			t.Fatalf("%s has no CA to verify its peers against", role)
		}
	}
}

// TestTheSigningKeyNeverLeavesTheOperator is the property that makes this
// worth doing: a workload that could sign could mint any identity it liked.
func TestTheSigningKeyNeverLeavesTheOperator(t *testing.T) {
	c := issuingCluster("nokey")
	r := pkiReconciler(t, time.Now(), c)
	if err := r.reconcilePKI(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	ca := secretOf(t, r, c.Namespace, CASecretName(c.Name))
	if len(ca.Data["ca.key"]) == 0 {
		t.Fatal("the authority has no key")
	}
	for role := range issuedRoles {
		sec := secretOf(t, r, c.Namespace, RoleTLSSecretName(c.Name, role))
		for k, v := range sec.Data {
			if string(v) == string(ca.Data["ca.key"]) {
				t.Fatalf("%s holds the CA signing key as %q", role, k)
			}
		}
	}
}

func TestAnIssuedCertificateIsLeftAloneWhileItIsCurrent(t *testing.T) {
	c := issuingCluster("stable")
	r := pkiReconciler(t, time.Now(), c)
	ctx := context.Background()
	if err := r.reconcilePKI(ctx, c); err != nil {
		t.Fatal(err)
	}
	before := secretOf(t, r, c.Namespace, RoleTLSSecretName(c.Name, pki.RoleRouter))
	if err := r.reconcilePKI(ctx, c); err != nil {
		t.Fatal(err)
	}
	after := secretOf(t, r, c.Namespace, RoleTLSSecretName(c.Name, pki.RoleRouter))
	if string(before.Data["tls.crt"]) != string(after.Data["tls.crt"]) {
		t.Fatal("a current certificate was reissued; every pass would roll every pod")
	}
}

func TestACertificateNearingExpiryIsReissued(t *testing.T) {
	c := issuingCluster("renew")
	start := time.Now()
	r := pkiReconciler(t, start, c)
	ctx := context.Background()
	if err := r.reconcilePKI(ctx, c); err != nil {
		t.Fatal(err)
	}
	before := secretOf(t, r, c.Namespace, RoleTLSSecretName(c.Name, pki.RoleRouter))
	r.Now = func() time.Time { return start.Add(pki.LeafLifetime * 3 / 4) }
	if err := r.reconcilePKI(ctx, c); err != nil {
		t.Fatal(err)
	}
	after := secretOf(t, r, c.Namespace, RoleTLSSecretName(c.Name, pki.RoleRouter))
	if string(before.Data["tls.crt"]) == string(after.Data["tls.crt"]) {
		t.Fatal("a certificate near expiry was not reissued")
	}
	if string(before.Data["ca.crt"]) != string(after.Data["ca.crt"]) {
		t.Fatal("renewing a leaf must not disturb the authority")
	}
}

// TestRotatingTheCAReissuesEveryCertificate is the case that breaks a
// cluster silently if it is missed: a leaf signed by the previous
// authority stays valid on its own terms and is trusted by nobody.
func TestRotatingTheCAReissuesEveryCertificate(t *testing.T) {
	c := issuingCluster("rotate")
	r := pkiReconciler(t, time.Now(), c)
	ctx := context.Background()
	if err := r.reconcilePKI(ctx, c); err != nil {
		t.Fatal(err)
	}
	before := secretOf(t, r, c.Namespace, RoleTLSSecretName(c.Name, pki.RoleAgent))
	ca := secretOf(t, r, c.Namespace, CASecretName(c.Name))
	ca.Data = map[string][]byte{"ca.crt": nil, "ca.key": nil} // as a corrupted or hand-edited one looks
	if err := r.Update(ctx, &ca); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcilePKI(ctx, c); err != nil {
		t.Fatal(err)
	}
	after := secretOf(t, r, c.Namespace, RoleTLSSecretName(c.Name, pki.RoleAgent))
	if string(before.Data["ca.crt"]) == string(after.Data["ca.crt"]) {
		t.Fatal("the authority was not replaced")
	}
	if string(before.Data["tls.crt"]) == string(after.Data["tls.crt"]) {
		t.Fatal("the leaf still chains to an authority nothing trusts any more")
	}
}

func TestNothingIsIssuedForAClusterThatSuppliesItsOwn(t *testing.T) {
	c := newCluster("given")
	r := pkiReconciler(t, time.Now(), c)
	if err := r.reconcilePKI(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	var sec corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: c.Namespace, Name: CASecretName(c.Name)}, &sec); err == nil {
		t.Fatal("an authority was minted for a cluster that did not ask for one")
	}
}

// TestTheAgentAndThePoolerMountDifferentCertificates is what the issuing
// mode is for. They share a pod, and while they shared a certificate no
// listener could tell one from the other -- nor either from a router.
func TestTheAgentAndThePoolerMountDifferentCertificates(t *testing.T) {
	c := issuingCluster("split")
	g := Group{Cluster: c.Name, Kind: "shard", ShardID: 0, Replicas: 3}
	pod := Renderer{}.Pod(c, g, 0, RolePrimary, g.MemberName(0), Template(c, g, nil, nil))

	claim := map[string]string{}
	for _, v := range pod.Spec.Volumes {
		if v.Secret != nil {
			claim[v.Name] = v.Secret.SecretName
		}
	}
	agentSecret := claim[internalTLSVolume]
	poolerSecret := claim[poolerTLSVolume]
	if agentSecret != RoleTLSSecretName(c.Name, pki.RoleAgent) {
		t.Fatalf("the agent mounts %q", agentSecret)
	}
	if poolerSecret != RoleTLSSecretName(c.Name, pki.RolePooler) {
		t.Fatalf("the pooler mounts %q", poolerSecret)
	}
	if agentSecret == poolerSecret {
		t.Fatal("two identities in one pod must not come from one Secret")
	}

	// And each container reads the one it was given, not merely mounts it.
	for _, ct := range pod.Spec.Containers {
		want := internalTLSMountPath
		if strings.Contains(ct.Name, "pooler") {
			want = poolerTLSMountPath
		}
		for _, a := range ct.Args {
			if strings.HasPrefix(a, "/etc/pgshard-") && !strings.HasPrefix(a, want) {
				t.Fatalf("%s reads %q, want material under %s", ct.Name, a, want)
			}
		}
	}
}

func TestASuppliedSecretIsStillMountedEverywhere(t *testing.T) {
	c := newCluster("given2")
	c.Spec.InternalTLS = pgshardv1alpha1.InternalTLSSpec{SecretRef: &corev1.LocalObjectReference{Name: "supplied"}}
	g := Group{Cluster: c.Name, Kind: "shard", ShardID: 0, Replicas: 3}
	pod := Renderer{}.Pod(c, g, 0, RolePrimary, g.MemberName(0), Template(c, g, nil, nil))
	for _, v := range pod.Spec.Volumes {
		if v.Name == poolerTLSVolume {
			t.Fatal("a cluster that supplies its own certificate must not gain an issued mount")
		}
	}
	if ref := internalTLSRefFor(c, pki.RolePooler); ref == nil || ref.Name != c.Spec.InternalTLS.SecretRef.Name {
		t.Fatalf("the supplied secret must still serve every role: %+v", ref)
	}
}

// serverRoles are the workloads the operator hands --tls-cert and starts a
// listener with, paired with the address something else dials them on. A
// certificate mounted as a server has to say it may serve, and has to name
// the address callers use, or every caller fails the handshake.
func serverRoles(c *pgshardv1alpha1.PgShardCluster) map[string]string {
	return map[string]string{
		pki.RoleRouter:     RouterName(c.Name) + "." + c.Namespace + ".svc",
		pki.RoleController: ControllerName(c.Name) + "." + c.Namespace + ".svc",
	}
}

// TestEveryListenerGetsACertificateItCanServeWith: the controller is
// mounted with --tls-cert and serves gRPC on it -- barriers, workflows,
// DDL and the resolver all arrive that way -- but it was issued a
// client-only certificate: no ServerAuth EKU and no DNS name for its
// Service. Both are fatal on their own, and a caller that verifies either
// one cannot complete a handshake, so turning on issued certificates took
// the controller off the air.
func TestEveryListenerGetsACertificateItCanServeWith(t *testing.T) {
	c := issuingCluster("listen")
	r := pkiReconciler(t, time.Now(), c)
	if err := r.reconcilePKI(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	for role, dialed := range serverRoles(c) {
		sec := secretOf(t, r, c.Namespace, RoleTLSSecretName(c.Name, role))
		block, _ := pem.Decode(sec.Data["tls.crt"])
		if block == nil {
			t.Fatalf("%s: no certificate in the secret", role)
		}
		crt, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatalf("%s: %v", role, err)
		}
		if !slices.Contains(crt.ExtKeyUsage, x509.ExtKeyUsageServerAuth) {
			t.Errorf("%s serves TLS but its certificate has no ServerAuth usage: %v", role, crt.ExtKeyUsage)
		}
		if err := crt.VerifyHostname(dialed); err != nil {
			t.Errorf("%s is dialled at %s and its certificate does not name it: %v", role, dialed, err)
		}
	}
}

// TestIssuedCertificatesTurnAuthorisationOn pins the pairing: the flag is
// only correct where the certificates carry identities, so it must follow
// issuing and nothing else. On without them, every caller is refused; off
// with them, the identities are carried and ignored.
func TestIssuedCertificatesTurnAuthorisationOn(t *testing.T) {
	const flag = "--tls-authorize-callers"
	for _, tc := range []struct {
		name string
		tls  pgshardv1alpha1.InternalTLSSpec
		want bool
	}{
		{"issued", pgshardv1alpha1.InternalTLSSpec{Issue: true}, true},
		{"supplied", pgshardv1alpha1.InternalTLSSpec{SecretRef: &corev1.LocalObjectReference{Name: "given"}}, false},
		{"insecure", pgshardv1alpha1.InternalTLSSpec{Insecure: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster("authz-" + tc.name)
			c.Spec.InternalTLS = tc.tls
			g := Group{Cluster: c.Name, Kind: "shard", ShardID: 0, Replicas: 3}
			pod := Renderer{}.Pod(c, g, 0, RolePrimary, g.MemberName(0), Template(c, g, nil, nil))
			var pooler bool
			for _, ct := range pod.Spec.Containers {
				if strings.Contains(ct.Name, "pooler") {
					pooler = slices.Contains(ct.Args, flag)
				}
			}
			if pooler != tc.want {
				t.Fatalf("pooler authorisation is %v, want %v", pooler, tc.want)
			}
			dep := Renderer{}.RouterDeployment(c)
			if dep != nil {
				for _, ct := range dep.Spec.Template.Spec.Containers {
					if got := slices.Contains(ct.Args, flag); got != tc.want {
						t.Fatalf("router authorisation is %v, want %v", got, tc.want)
					}
				}
			}
		})
	}
}

func TestTheAgentAuthorisesOnlyWithIssuedCertificates(t *testing.T) {
	for _, tc := range []struct {
		name string
		tls  pgshardv1alpha1.InternalTLSSpec
		want bool
	}{
		{"issued", pgshardv1alpha1.InternalTLSSpec{Issue: true, AgentMTLS: true}, true},
		{"supplied", pgshardv1alpha1.InternalTLSSpec{
			SecretRef: &corev1.LocalObjectReference{Name: "given"}, AgentMTLS: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster("agentauthz-" + tc.name)
			c.Spec.InternalTLS = tc.tls
			tls := agentGRPCTLS(c)
			if tls.CertFile == "" {
				t.Fatal("agentMTLS must give the agent material")
			}
			if tls.AuthorizeCallers != tc.want {
				t.Fatalf("agent authorisation is %v, want %v", tls.AuthorizeCallers, tc.want)
			}
		})
	}
}

// The consumer certificate exists so an operator has something to hand a
// change-stream consumer other than the router's own, which is also a
// credential for every pooler and for the controller. It must be client
// only and carry no DNS names: a consumer dials the VStream API and serves
// nothing, so a leaked one cannot be stood up as a server for anything.
func TestTheConsumerCertificateIsClientOnly(t *testing.T) {
	c := issuingCluster("consumer")
	r := pkiReconciler(t, time.Now(), c)
	if err := r.reconcilePKI(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	sec := secretOf(t, r, c.Namespace, RoleTLSSecretName(c.Name, pki.RoleConsumer))
	block, _ := pem.Decode(sec.Data["tls.crt"])
	if block == nil {
		t.Fatal("no certificate in the consumer secret")
	}
	crt, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(crt.DNSNames) != 0 {
		t.Errorf("the consumer certificate is valid to serve %v", crt.DNSNames)
	}
	if slices.Contains(crt.ExtKeyUsage, x509.ExtKeyUsageServerAuth) {
		t.Error("the consumer certificate may be used as a server")
	}
	if !slices.Contains(crt.ExtKeyUsage, x509.ExtKeyUsageClientAuth) {
		t.Error("the consumer certificate cannot be used as a client, which is all it is for")
	}
}

// TestEveryCallerVerifiesANameTheIssuedCertificateCarries (PGS-860): a
// router dials a shard's pooler at the member's headless-Service host, peer
// routers by IP, and the controller dials an agent at the member host too.
// None of those is a name an issued certificate carries, so every such
// handshake failed hostname verification and an issuing cluster could not
// route a statement. The callers verify a role-wide name instead; this pins
// that the name the operator hands each caller is one the server's issued
// certificate is valid for -- and that the member address is not, which is
// why the name is needed.
func TestEveryCallerVerifiesANameTheIssuedCertificateCarries(t *testing.T) {
	c := issuingCluster("names")
	r := pkiReconciler(t, time.Now(), c)
	if err := r.reconcilePKI(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	certOf := func(role string) *x509.Certificate {
		t.Helper()
		sec := secretOf(t, r, c.Namespace, RoleTLSSecretName(c.Name, role))
		block, _ := pem.Decode(sec.Data["tls.crt"])
		if block == nil {
			t.Fatalf("%s: no certificate", role)
		}
		crt, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		return crt
	}
	flag := func(args []string, name string) string {
		t.Helper()
		for _, a := range args {
			if v, ok := strings.CutPrefix(a, "--"+name+"="); ok {
				return v
			}
		}
		t.Fatalf("no --%s in %v", name, args)
		return ""
	}
	routerArgs := Renderer{}.RouterDeployment(c).Spec.Template.Spec.Containers[0].Args
	controllerArgs := Renderer{}.ControllerDeployment(c).Spec.Template.Spec.Containers[0].Args
	shard := Groups(c)[1]
	member := shard.MemberHost(shard.MemberNames()[0], c.Namespace)
	for _, check := range []struct {
		caller, flag, role, dialled string
		args                        []string
	}{
		{"router -> pooler", "pooler-tls-server-name", pki.RolePooler, member, routerArgs},
		{"router -> peer router", "peer-tls-server-name", pki.RoleRouter, "10.0.0.7", routerArgs},
		{"controller -> agent", "agent-tls-server-name", pki.RoleAgent, member, controllerArgs},
	} {
		crt := certOf(check.role)
		name := flag(check.args, check.flag)
		if err := crt.VerifyHostname(name); err != nil {
			t.Errorf("%s verifies %q, which the %s certificate does not name: %v", check.caller, name, check.role, err)
		}
		if err := crt.VerifyHostname(check.dialled); err == nil {
			t.Errorf("%s: the %s certificate names the dialled address %s, so the test no longer shows why the name is needed", check.caller, check.role, check.dialled)
		}
	}
	// A member's certificate must not be valid for another role's Service:
	// those callers verify the Service host, and only the controller's and
	// the router's own certificates may answer there.
	for _, role := range []string{pki.RolePooler, pki.RoleAgent} {
		for _, host := range []string{ControllerName(c.Name) + "." + c.Namespace + ".svc", RouterName(c.Name) + "." + c.Namespace + ".svc"} {
			if err := certOf(role).VerifyHostname(host); err == nil {
				t.Errorf("the %s certificate is valid to serve %s", role, host)
			}
		}
	}
}

// TestAChangeToWhatARoleServesReissuesItsCertificate (PGS-860): certificates
// were reissued only near expiry or on a CA change, so removing a name a role
// may serve waited up to a certificate's life -- member certificates carrying
// the namespace wildcards stayed valid for the controller's and router's
// hosts. A stored certificate that names anything else is reissued, and one
// that names exactly the role's names is left alone, or every pass would
// churn the Secrets and roll the members.
func TestAChangeToWhatARoleServesReissuesItsCertificate(t *testing.T) {
	ctx := context.Background()
	c := issuingCluster("names-change")
	r := pkiReconciler(t, time.Now(), c)
	if err := r.reconcilePKI(ctx, c); err != nil {
		t.Fatal(err)
	}
	name := RoleTLSSecretName(c.Name, pki.RolePooler)
	settled := secretOf(t, r, c.Namespace, name)
	if err := r.reconcilePKI(ctx, c); err != nil {
		t.Fatal(err)
	}
	if again := secretOf(t, r, c.Namespace, name); string(again.Data["tls.crt"]) != string(settled.Data["tls.crt"]) {
		t.Fatal("a certificate naming exactly its role's names was reissued")
	}

	caSec := secretOf(t, r, c.Namespace, CASecretName(c.Name))
	ca, err := pki.LoadCA(pki.Material{CertPEM: caSec.Data["ca.crt"], KeyPEM: caSec.Data["ca.key"]})
	if err != nil {
		t.Fatal(err)
	}
	old, err := ca.Issue(pki.Request{Identity: pki.Identity{Namespace: c.Namespace, Cluster: c.Name, Role: pki.RolePooler},
		DNSNames: append(roleDNSNames(c, pki.RolePooler), "*."+c.Namespace+".svc", "*."+c.Namespace+".pod"), Server: true, Client: true}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	settled.Data["tls.crt"], settled.Data["tls.key"] = old.CertPEM, old.KeyPEM
	if err := r.Update(ctx, &settled); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcilePKI(ctx, c); err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(secretOf(t, r, c.Namespace, name).Data["tls.crt"])
	crt, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := crt.VerifyHostname(ControllerName(c.Name) + "." + c.Namespace + ".svc"); err == nil {
		t.Fatalf("a certificate issued with names the role no longer serves was kept: %v", crt.DNSNames)
	}
}
