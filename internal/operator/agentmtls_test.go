package operator

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/grpccreds"
	corev1 "k8s.io/api/core/v1"
)

// TestTheOperatorCanReachAnAgentThatRequiresMTLS: the agent gained the
// ability to require client certificates, which is worth nothing until a
// caller can present one. This drives the real GRPCAgentClient against a
// listener configured exactly as the agent configures its own, so the two
// halves are checked together rather than each alone -- a server that
// requires certificates and a client that cannot send them is a cluster
// whose operator can no longer promote anything.
func TestTheOperatorCanReachAnAgentThatRequiresMTLS(t *testing.T) {
	dir := t.TempDir()
	ca := newAgentTestCA(t)
	srvCert, srvKey := ca.issue(t, "agent", 11)
	cliCert, cliKey := ca.issue(t, "operator", 12)
	caPath := writeAgentFile(t, dir, "ca.crt", ca.pem)

	listener, err := grpccreds.Listener(
		writeAgentFile(t, dir, "s.crt", srvCert), writeAgentFile(t, dir, "s.key", srvKey), caPath, false)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer(grpc.Creds(listener))
	pgshardv1.RegisterAgentServer(g, &pgshardv1.UnimplementedAgentServer{})
	go func() { _ = g.Serve(ln) }()
	t.Cleanup(g.Stop)
	addr := ln.Addr().String()

	t.Run("a plaintext operator cannot reach it", func(t *testing.T) {
		c := NewGRPCAgentClient()
		t.Cleanup(c.Close)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := c.Status(ctx, addr); status.Code(err) == codes.Unimplemented {
			t.Fatal("a plaintext client reached an agent that requires certificates")
		}
	})

	t.Run("an operator with a certificate reaches it", func(t *testing.T) {
		c, err := NewGRPCAgentClientTLS(
			writeAgentFile(t, dir, "c.crt", cliCert), writeAgentFile(t, dir, "c.key", cliKey), caPath, "localhost")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(c.Close)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Unimplemented means the call arrived and the stub answered it,
		// which is what reaching the agent means here.
		if _, err := c.Status(ctx, addr); status.Code(err) != codes.Unimplemented {
			t.Fatalf("a properly issued operator could not reach the agent: %v", err)
		}
	})

	t.Run("missing material is refused at construction, not at the first call", func(t *testing.T) {
		if _, err := NewGRPCAgentClientTLS("", "", "", ""); err == nil {
			t.Fatal("a client with no material must fail to build rather than quietly dial plaintext")
		}
	})
}

type agentTestCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newAgentTestCA(t *testing.T) *agentTestCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "agent-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &agentTestCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func (c *agentTestCA) issue(t *testing.T, cn string, serial int64) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &key.PublicKey, c.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func writeAgentFile(t *testing.T, dir, name string, b []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestTheAgentHoldsItsTLSMaterialBeforeItNeedsIt: every way of staging the
// switch to agent mTLS needs the certificates present on every member
// before anything requires them, because members restart one at a time and
// an agent serving plaintext while the operator dials TLS is unreachable.
// Mounting is therefore separate from, and earlier than, requiring.
func TestTheAgentHoldsItsTLSMaterialBeforeItNeedsIt(t *testing.T) {
	withTLS := &pgshardv1alpha1.PgShardCluster{}
	withTLS.Spec.InternalTLS.SecretRef = &corev1.LocalObjectReference{Name: "internal-tls"}

	if has(agentMounts(withTLS), internalTLSVolume) != true {
		t.Error("a cluster with internal TLS must mount it into the agent, so a later switch has something to use")
	}
	if has(agentMounts(&pgshardv1alpha1.PgShardCluster{}), internalTLSVolume) {
		t.Error("a cluster without internal TLS must not mount a volume the pod does not carry")
	}
	// The mount alone must not turn it on: the agent serves plaintext until
	// GRPCTLS is set, which is what keeps this change inert.
	for _, m := range agentMounts(withTLS) {
		if m.Name == internalTLSVolume && !m.ReadOnly {
			t.Error("the agent's copy of the material must be read-only")
		}
	}
}

func has(mounts []corev1.VolumeMount, name string) bool {
	for _, m := range mounts {
		if m.Name == name {
			return true
		}
	}
	return false
}
