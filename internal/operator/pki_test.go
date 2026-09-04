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
