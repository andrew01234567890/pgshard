package grpccreds_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
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
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/grpccreds"
	"github.com/andrew01234567890/pgshard/internal/pki"
)

// TestListenerRejectsAnythingWithoutAVerifiedClientCertificate is the whole
// point of the package. A listener that negotiates TLS but does not require
// and verify a client certificate looks identical from the outside, serves
// happily, and authenticates nobody -- so the property is asserted against
// a real server rather than by reading the tls.Config.
func TestListenerRejectsAnythingWithoutAVerifiedClientCertificate(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	other := newTestCA(t)
	srvCert, srvKey := ca.issue(t, "server", 2)
	cliCert, cliKey := ca.issue(t, "client", 3)
	strangerCert, strangerKey := other.issue(t, "stranger", 4)

	creds, err := grpccreds.Listener(
		writeFile(t, dir, "tls.crt", srvCert),
		writeFile(t, dir, "tls.key", srvKey),
		writeFile(t, dir, "ca.crt", ca.pem), false)
	if err != nil {
		t.Fatal(err)
	}
	addr := serve(t, creds)

	trusted := x509.NewCertPool()
	trusted.AppendCertsFromPEM(ca.pem)

	t.Run("a plaintext client cannot reach the service", func(t *testing.T) {
		if err := call(t, addr, insecure.NewCredentials()); status.Code(err) == codes.Unimplemented {
			t.Fatal("plaintext reached the service")
		}
	})

	t.Run("TLS without a client certificate is not enough", func(t *testing.T) {
		tc := credentials.NewTLS(&tls.Config{RootCAs: trusted, MinVersion: tls.VersionTLS13})
		if err := call(t, addr, tc); status.Code(err) == codes.Unimplemented {
			t.Fatal("a client presenting no certificate reached the service")
		}
	})

	t.Run("a certificate from another CA is not enough", func(t *testing.T) {
		pair, err := tls.X509KeyPair(strangerCert, strangerKey)
		if err != nil {
			t.Fatal(err)
		}
		tc := credentials.NewTLS(&tls.Config{RootCAs: trusted, Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS13})
		if err := call(t, addr, tc); status.Code(err) == codes.Unimplemented {
			t.Fatal("a certificate issued by an untrusted CA reached the service")
		}
	})

	t.Run("a certificate from the named CA gets through", func(t *testing.T) {
		pair, err := tls.X509KeyPair(cliCert, cliKey)
		if err != nil {
			t.Fatal(err)
		}
		tc := credentials.NewTLS(&tls.Config{RootCAs: trusted, Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS13})
		if err := call(t, addr, tc); status.Code(err) != codes.Unimplemented {
			t.Fatalf("a properly issued client was refused: %v", err)
		}
	})
}

// TestListenerIsFailClosed: a missing flag must not silently downgrade the
// listener to plaintext, and --insecure-dev must not be combinable with TLS
// material, so "which mode am I in" is never ambiguous.
func TestListenerIsFailClosed(t *testing.T) {
	if _, err := grpccreds.Listener("", "", "", false); err == nil {
		t.Error("no TLS material and no --insecure-dev must be an error, not plaintext")
	}
	if _, err := grpccreds.Listener("cert", "", "", false); err == nil {
		t.Error("a partial set of TLS flags must be an error")
	}
	if _, err := grpccreds.Listener("cert", "key", "ca", true); err == nil {
		t.Error("--insecure-dev combined with TLS flags must be an error")
	}
	if _, err := grpccreds.Listener("", "", "", true); err != nil {
		t.Errorf("--insecure-dev alone is the documented way to serve plaintext: %v", err)
	}
	if _, err := grpccreds.Listener("/nonexistent/cert", "/nonexistent/key", "/nonexistent/ca", false); err == nil {
		t.Error("unreadable material must be an error")
	}
}

func serve(t *testing.T, creds credentials.TransportCredentials) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer(grpc.Creds(creds))
	pgshardv1.RegisterAgentServer(g, &pgshardv1.UnimplementedAgentServer{})
	go func() { _ = g.Serve(ln) }()
	t.Cleanup(g.Stop)
	return ln.Addr().String()
}

func call(t *testing.T, addr string, tc credentials.TransportCredentials) error {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(tc))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = pgshardv1.NewAgentClient(conn).Status(ctx, &pgshardv1.StatusRequest{})
	return err
}

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
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
	return &testCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func (c *testCA) issue(t *testing.T, cn string, serial int64) (certPEM, keyPEM []byte) {
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

func writeFile(t *testing.T, dir, name string, b []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestDialerVerifiesTheServerItReaches: a client that presents its own
// certificate but does not verify the server's would connect happily to
// anything that answered on the address, which is the half of mutual TLS
// that is easy to leave out. Asserted against a real server rather than by
// reading the tls.Config.
func TestDialerVerifiesTheServerItReaches(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	impostorCA := newTestCA(t)
	srvCert, srvKey := ca.issue(t, "server", 5)
	cliCert, cliKey := ca.issue(t, "client", 6)
	impostorCert, impostorKey := impostorCA.issue(t, "server", 7)

	listener, err := grpccreds.Listener(
		writeFile(t, dir, "s.crt", srvCert), writeFile(t, dir, "s.key", srvKey),
		writeFile(t, dir, "ca.crt", ca.pem), false)
	if err != nil {
		t.Fatal(err)
	}
	impostorListener, err := grpccreds.Listener(
		writeFile(t, dir, "i.crt", impostorCert), writeFile(t, dir, "i.key", impostorKey),
		writeFile(t, dir, "ica.crt", impostorCA.pem), false)
	if err != nil {
		t.Fatal(err)
	}

	clientCert := writeFile(t, dir, "c.crt", cliCert)
	clientKey := writeFile(t, dir, "c.key", cliKey)
	caPath := writeFile(t, dir, "trust.crt", ca.pem)

	t.Run("reaches a server the CA vouches for", func(t *testing.T) {
		tc, err := grpccreds.Dialer(clientCert, clientKey, caPath, "localhost", false)
		if err != nil {
			t.Fatal(err)
		}
		if err := call(t, serve(t, listener), tc); status.Code(err) != codes.Unimplemented {
			t.Fatalf("a properly issued client could not reach its server: %v", err)
		}
	})

	t.Run("refuses a server the CA does not vouch for", func(t *testing.T) {
		tc, err := grpccreds.Dialer(clientCert, clientKey, caPath, "localhost", false)
		if err != nil {
			t.Fatal(err)
		}
		if err := call(t, serve(t, impostorListener), tc); status.Code(err) == codes.Unimplemented {
			t.Fatal("the client reached a server presenting a certificate from an untrusted CA")
		}
	})

	t.Run("is fail-closed on partial material", func(t *testing.T) {
		if _, err := grpccreds.Dialer("", "", "", "", false); err == nil {
			t.Error("no material and no insecure dialling must be an error, not plaintext")
		}
		if _, err := grpccreds.Dialer(clientCert, clientKey, caPath, "", true); err == nil {
			t.Error("insecure dialling combined with TLS material must be an error")
		}
		if _, err := grpccreds.Dialer("", "", "", "", true); err != nil {
			t.Errorf("insecure dialling alone is the documented plaintext path: %v", err)
		}
	})
}

// TestAuthorizeRefusesAnIdentityTheListenerDoesNotWant is the point of the
// whole exercise: one CA stands behind every internal workload, so an
// agent's certificate verifies against a pooler's listener exactly as a
// router's does. Chaining is authenticity; this is authority.
func TestAuthorizeRefusesAnIdentityTheListenerDoesNotWant(t *testing.T) {
	ca, err := pki.NewCA("demo", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	write := func(name string, b []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	caFile := write("ca.crt", ca.CertPEM)
	issue := func(name, role, member string, req pki.Request) (string, string) {
		req.Identity = pki.Identity{Namespace: "ns", Cluster: "demo", Role: role, Member: member}
		m, err := ca.Issue(req, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		return write(name+".crt", m.CertPEM), write(name+".key", m.KeyPEM)
	}
	srvCert, srvKey := issue("pooler", pki.RolePooler, "", pki.Request{DNSNames: []string{"pooler.ns.svc"}, Server: true})
	routerCert, routerKey := issue("router", pki.RoleRouter, "", pki.Request{Client: true})
	agentCert, agentKey := issue("agent", pki.RoleAgent, "m0", pki.Request{Client: true})

	listener, err := grpccreds.Listener(srvCert, srvKey, caFile, false,
		grpccreds.Authorize(func(id pki.Identity) bool { return id.Role == pki.RoleRouter }))
	if err != nil {
		t.Fatal(err)
	}
	addr := serve(t, listener)
	for _, tc := range []struct {
		name       string
		cert, key  string
		wantAccept bool
	}{
		{"a router is what this listener serves", routerCert, routerKey, true},
		{"an agent holds a certificate from the same CA", agentCert, agentKey, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dialer, err := grpccreds.Dialer(tc.cert, tc.key, caFile, "pooler.ns.svc", false)
			if err != nil {
				t.Fatal(err)
			}
			err = call(t, addr, dialer)
			// The server is Unimplemented for everything, so reaching it
			// at all is the accept: a refused peer never gets that far.
			if tc.wantAccept && status.Code(err) != codes.Unimplemented {
				t.Fatalf("a peer it should serve was refused: %v", err)
			}
			if !tc.wantAccept && status.Code(err) == codes.Unimplemented {
				t.Fatal("a peer whose identity the listener does not serve reached the service")
			}
		})
	}
}

// TestAListenerWithoutAuthorizeStillAcceptsAnyoneFromItsCA pins what the
// option does and does not change, so the previous behaviour is not
// mistaken for a regression later.
func TestAListenerWithoutAuthorizeStillAcceptsAnyoneFromItsCA(t *testing.T) {
	ca, err := pki.NewCA("demo", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	write := func(name string, b []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	caFile := write("ca.crt", ca.CertPEM)
	srv, err := ca.Issue(pki.Request{Identity: pki.Identity{Namespace: "ns", Cluster: "demo", Role: pki.RolePooler},
		DNSNames: []string{"pooler.ns.svc"}, Server: true}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	agent, err := ca.Issue(pki.Request{Identity: pki.Identity{Namespace: "ns", Cluster: "demo", Role: pki.RoleAgent, Member: "m0"},
		Client: true}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	listener, err := grpccreds.Listener(write("s.crt", srv.CertPEM), write("s.key", srv.KeyPEM), caFile, false)
	if err != nil {
		t.Fatal(err)
	}
	dialer, err := grpccreds.Dialer(write("a.crt", agent.CertPEM), write("a.key", agent.KeyPEM), caFile, "pooler.ns.svc", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := call(t, serve(t, listener), dialer); status.Code(err) != codes.Unimplemented {
		t.Fatalf("without Authorize a listener must accept any peer from its CA: %v", err)
	}
}
