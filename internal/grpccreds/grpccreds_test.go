package grpccreds_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
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
	return c.issueUntil(t, cn, serial, time.Now().Add(time.Hour))
}

func (c *testCA) issueUntil(t *testing.T, cn string, serial int64, notAfter time.Time) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     notAfter,
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

// serverSerial connects to addr presenting the client pair and reports the
// serial of the certificate the server presented, or why it was refused.
func serverSerial(t *testing.T, addr string, roots []byte, certPEM, keyPEM []byte) (int64, error) {
	t.Helper()
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(roots)
	conn, err := tls.Dial("tcp", addr, &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{pair},
		ServerName: "localhost", MinVersion: tls.VersionTLS13, NextProtos: []string{"h2"}})
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close() }()
	if err := conn.Handshake(); err != nil {
		return 0, err
	}
	// Under TLS 1.3 a server that refuses the client's certificate says so
	// after the client's handshake has returned, so read what the server
	// sends first: gRPC's HTTP/2 settings, or the alert.
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err != nil {
		return 0, err
	}
	return conn.ConnectionState().PeerCertificates[0].SerialNumber.Int64(), nil
}

// TestAListenerUsesRenewedFilesOnTheNextConnection (PGS-799): a renewal is
// written into the same files, and a listener that read them once at start
// kept presenting the old certificate until it expired and trusting only
// the old CA.
func TestAListenerUsesRenewedFilesOnTheNextConnection(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	srvCert, srvKey := ca.issue(t, "server", 10)
	cliCert, cliKey := ca.issue(t, "client", 20)
	certFile := writeFile(t, dir, "tls.crt", srvCert)
	keyFile := writeFile(t, dir, "tls.key", srvKey)
	caFile := writeFile(t, dir, "ca.crt", ca.pem)
	creds, err := grpccreds.Listener(certFile, keyFile, caFile, false)
	if err != nil {
		t.Fatal(err)
	}
	addr := serve(t, creds)
	if serial, err := serverSerial(t, addr, ca.pem, cliCert, cliKey); err != nil || serial != 10 {
		t.Fatalf("before renewal: serial %d, %v", serial, err)
	}

	renewedCert, renewedKey := ca.issue(t, "server", 11)
	writeFile(t, dir, "tls.crt", renewedCert)
	writeFile(t, dir, "tls.key", renewedKey)
	if serial, err := serverSerial(t, addr, ca.pem, cliCert, cliKey); err != nil || serial != 11 {
		t.Fatalf("after renewal the listener presented serial %d (%v), want the renewed 11", serial, err)
	}

	// A new CA, with a server certificate from it: clients of the old CA are
	// refused and clients of the new one admitted, without a restart.
	rotated := newTestCA(t)
	newSrvCert, newSrvKey := rotated.issue(t, "server", 30)
	newCliCert, newCliKey := rotated.issue(t, "client", 31)
	writeFile(t, dir, "tls.crt", newSrvCert)
	writeFile(t, dir, "tls.key", newSrvKey)
	writeFile(t, dir, "ca.crt", rotated.pem)
	if _, err := serverSerial(t, addr, rotated.pem, cliCert, cliKey); err == nil {
		t.Fatal("a client certificate from the replaced CA was still accepted")
	}
	if serial, err := serverSerial(t, addr, rotated.pem, newCliCert, newCliKey); err != nil || serial != 30 {
		t.Fatalf("after CA rotation: serial %d, %v", serial, err)
	}

	// A half-written renewal -- a key that does not match the certificate --
	// keeps the last good pair rather than failing every handshake.
	writeFile(t, dir, "tls.key", srvKey)
	if serial, err := serverSerial(t, addr, rotated.pem, newCliCert, newCliKey); err != nil || serial != 30 {
		t.Fatalf("with a mismatched key on disk: serial %d, %v, want the last good pair", serial, err)
	}
}

// TestADialerUsesRenewedFilesOnTheNextConnection is the client side: the
// certificate it presents and the CA it verifies servers against are read
// again for every new connection.
func TestADialerUsesRenewedFilesOnTheNextConnection(t *testing.T) {
	oldCA, newCA := newTestCA(t), newTestCA(t)
	srvDir := t.TempDir()
	srvCert, srvKey := newCA.issue(t, "server", 40)
	listener, err := grpccreds.Listener(writeFile(t, srvDir, "tls.crt", srvCert), writeFile(t, srvDir, "tls.key", srvKey),
		writeFile(t, srvDir, "ca.crt", newCA.pem), false)
	if err != nil {
		t.Fatal(err)
	}
	addr := serve(t, listener)

	dir := t.TempDir()
	cliCert, cliKey := oldCA.issue(t, "client", 41)
	dialer, err := grpccreds.Dialer(writeFile(t, dir, "tls.crt", cliCert), writeFile(t, dir, "tls.key", cliKey),
		writeFile(t, dir, "ca.crt", oldCA.pem), "localhost", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := call(t, addr, dialer); status.Code(err) == codes.Unimplemented {
		t.Fatal("a dialer holding another CA's material reached the service")
	}

	renewedCert, renewedKey := newCA.issue(t, "client", 42)
	writeFile(t, dir, "tls.crt", renewedCert)
	writeFile(t, dir, "tls.key", renewedKey)
	writeFile(t, dir, "ca.crt", newCA.pem)
	if err := call(t, addr, dialer); status.Code(err) != codes.Unimplemented {
		t.Fatalf("after renewal the same dialer must reach the service: %v", err)
	}
}

// TestEmptyFilesAreRefusedAtStart: reading the files at every handshake must
// not loosen what start refuses. Empty files once compared equal to nothing
// read yet and were never parsed, leaving a dialer with no client
// certificate and the system's roots.
func TestEmptyFilesAreRefusedAtStart(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, caFile := writeFile(t, dir, "tls.crt", nil), writeFile(t, dir, "tls.key", nil), writeFile(t, dir, "ca.crt", nil)
	if _, err := grpccreds.Listener(certFile, keyFile, caFile, false); err == nil {
		t.Error("a listener started from empty files")
	}
	if _, err := grpccreds.Dialer(certFile, keyFile, caFile, "localhost", false); err == nil {
		t.Error("a dialer started from empty files")
	}
}

// TestAListenerAcceptingPlaintextServesBothAndStillAuthorisesTLS (PGS-236):
// while a cluster moves from plaintext to mutual TLS, a listener serves
// callers still dialling plaintext and callers already dialling TLS on one
// port. A TLS caller gets the full check: a certificate is required, it
// must chain to the CA, and its identity must be one the listener serves.
func TestAListenerAcceptingPlaintextServesBothAndStillAuthorisesTLS(t *testing.T) {
	ca, err := pki.NewCA("demo", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	caFile := writeFile(t, dir, "ca.crt", ca.CertPEM)
	issue := func(name, role string, req pki.Request) (string, string) {
		req.Identity = pki.Identity{Namespace: "ns", Cluster: "demo", Role: role}
		m, err := ca.Issue(req, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		return writeFile(t, dir, name+".crt", m.CertPEM), writeFile(t, dir, name+".key", m.KeyPEM)
	}
	srvCert, srvKey := issue("pooler", pki.RolePooler, pki.Request{DNSNames: []string{"pooler.ns.svc"}, Server: true})
	routerCert, routerKey := issue("router", pki.RoleRouter, pki.Request{Client: true})
	agentCert, agentKey := issue("agent", pki.RoleAgent, pki.Request{Client: true})
	onlyRouters := grpccreds.Authorize(func(id pki.Identity) bool { return id.Role == pki.RoleRouter })

	either, err := grpccreds.Listener(srvCert, srvKey, caFile, false, onlyRouters, grpccreds.AcceptPlaintext())
	if err != nil {
		t.Fatal(err)
	}
	tlsOnly, err := grpccreds.Listener(srvCert, srvKey, caFile, false, onlyRouters)
	if err != nil {
		t.Fatal(err)
	}
	eitherAddr, tlsOnlyAddr := serve(t, either), serve(t, tlsOnly)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.CertPEM)
	dialAs := func(cert, key string) credentials.TransportCredentials {
		d, err := grpccreds.Dialer(cert, key, caFile, "pooler.ns.svc", false)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	reached := func(err error) bool { return status.Code(err) == codes.Unimplemented }

	for _, tc := range []struct {
		name  string
		addr  string
		creds credentials.TransportCredentials
		want  bool
	}{
		{"a plaintext caller reaches a listener accepting plaintext", eitherAddr, insecure.NewCredentials(), true},
		{"a router dialling TLS reaches it", eitherAddr, dialAs(routerCert, routerKey), true},
		{"an agent dialling TLS is still refused its identity", eitherAddr, dialAs(agentCert, agentKey), false},
		{"a TLS caller with no certificate is still refused", eitherAddr, credentials.NewTLS(&tls.Config{RootCAs: roots, ServerName: "pooler.ns.svc", MinVersion: tls.VersionTLS13}), false},
		{"a plaintext caller does not reach a listener that requires TLS", tlsOnlyAddr, insecure.NewCredentials(), false},
		{"a router dialling TLS reaches a listener that requires it", tlsOnlyAddr, dialAs(routerCert, routerKey), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := call(t, tc.addr, tc.creds); reached(err) != tc.want {
				t.Fatalf("reached the service = %v, want %v: %v", reached(err), tc.want, err)
			}
		})
	}

	if _, err := grpccreds.Listener("", "", "", true, grpccreds.AcceptPlaintext()); err == nil {
		t.Error("accepting plaintext alongside TLS without TLS material must be refused")
	}
}

// TestAListenerAcceptingPlaintextDropsAConnectionThatSendsNothing: the
// listener reads before it knows which handshake to run, and that read must
// be bounded like a handshake, or a client that connects and waits holds a
// server goroutine for good.
func TestAListenerAcceptingPlaintextDropsAConnectionThatSendsNothing(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	srvCert, srvKey := ca.issue(t, "server", 2)
	creds, err := grpccreds.Listener(writeFile(t, dir, "tls.crt", srvCert), writeFile(t, dir, "tls.key", srvKey),
		writeFile(t, dir, "ca.crt", ca.pem), false, grpccreds.AcceptPlaintext())
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer(grpc.Creds(creds), grpc.ConnectionTimeout(300*time.Millisecond))
	pgshardv1.RegisterAgentServer(g, &pgshardv1.UnimplementedAgentServer{})
	go func() { _ = g.Serve(ln) }()
	t.Cleanup(g.Stop)

	silent, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = silent.Close() }()
	if err := silent.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var ne net.Error
	if _, err := silent.Read(make([]byte, 1)); err == nil {
		t.Fatal("the server sent something to a client that never spoke")
	} else if errors.As(err, &ne) && ne.Timeout() {
		t.Fatal("the server still held a connection that sent nothing past its connection timeout")
	}
	if err := call(t, ln.Addr().String(), insecure.NewCredentials()); status.Code(err) != codes.Unimplemented {
		t.Fatalf("the listener stopped serving after dropping a silent connection: %v", err)
	}
}

// TestARenewalThatCannotBeUsedIsReportedOnce (PGS-847): a renewal that
// cannot be used keeps the last good material, and did so silently until
// the certificate in use expired. It is logged and counted once per distinct
// failure however many handshakes read it, and the expiry of the certificate
// in use is exported so an alert can fire before it.
func TestARenewalThatCannotBeUsedIsReportedOnce(t *testing.T) {
	var logs syncBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	dir := t.TempDir()
	ca := newTestCA(t)
	firstExpiry := time.Now().Add(48 * time.Hour).Truncate(time.Second)
	srvCert, srvKey := ca.issueUntil(t, "server", 10, firstExpiry)
	cliCert, cliKey := ca.issue(t, "client", 20)
	certFile := writeFile(t, dir, "tls.crt", srvCert)
	keyFile := writeFile(t, dir, "tls.key", srvKey)
	caFile := writeFile(t, dir, "ca.crt", ca.pem)
	creds, err := grpccreds.Listener(certFile, keyFile, caFile, false)
	if err != nil {
		t.Fatal(err)
	}
	// The same files dialled with as well, which is what the controller
	// does: two loads of one file must still gather as one series.
	if _, err := grpccreds.Dialer(certFile, keyFile, caFile, "", false); err != nil {
		t.Fatal(err)
	}
	addr := serve(t, creds)
	reg := prometheus.NewRegistry()
	reg.MustRegister(grpccreds.Collector())
	metric := func(name string) float64 {
		t.Helper()
		families, err := reg.Gather()
		if err != nil {
			t.Fatalf("gather: %v", err)
		}
		for _, f := range families {
			if f.GetName() != name {
				continue
			}
			for _, m := range f.GetMetric() {
				for _, l := range m.GetLabel() {
					if l.GetName() == "cert_file" && l.GetValue() == certFile {
						if f.GetType() == dto.MetricType_COUNTER {
							return m.GetCounter().GetValue()
						}
						return m.GetGauge().GetValue()
					}
				}
			}
		}
		t.Fatalf("no %s for %s", name, certFile)
		return 0
	}
	handshake := func(want int64) {
		t.Helper()
		if serial, err := serverSerial(t, addr, ca.pem, cliCert, cliKey); err != nil || serial != want {
			t.Fatalf("serial %d, %v; want %d", serial, err, want)
		}
	}
	if got := metric("pgshard_tls_certificate_not_after_seconds"); got != float64(firstExpiry.Unix()) {
		t.Fatalf("expiry exported as %v, want %d", got, firstExpiry.Unix())
	}

	writeFile(t, dir, "tls.crt", []byte("not a certificate"))
	for range 3 {
		handshake(10)
	}
	if got := metric("pgshard_tls_material_reload_failures_total"); got != 1 {
		t.Fatalf("one unusable renewal read by three handshakes counted %v times", got)
	}
	reported := 0
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "could not be reloaded") && strings.Contains(line, certFile) {
			reported++
		}
	}
	if n := reported; n != 1 {
		t.Fatalf("one unusable renewal logged %d times:\n%s", n, logs.String())
	}

	// A read that lands in the middle of a swap -- a certificate without its
	// key -- heals at the next read and is not a failure worth an alert.
	writeFile(t, dir, "tls.crt", srvCert)
	halfCert, halfKey := ca.issueUntil(t, "server", 13, firstExpiry)
	writeFile(t, dir, "tls.crt", halfCert)
	handshake(10)
	writeFile(t, dir, "tls.key", halfKey)
	handshake(13)
	if got := metric("pgshard_tls_material_reload_failures_total"); got != 1 {
		t.Fatalf("a swap caught half way and healed by the next read: counted %v, want still 1", got)
	}

	// A different unusable renewal is another failure, noticed by the
	// scrape itself with no connection being made.
	otherCert, _ := ca.issue(t, "server", 12)
	writeFile(t, dir, "tls.crt", otherCert)
	metric("pgshard_tls_material_reload_failures_total")
	if got := metric("pgshard_tls_material_reload_failures_total"); got != 2 {
		t.Fatalf("a second, different unusable renewal read by two scrapes: counted %v, want 2", got)
	}

	// So is a file that cannot be read -- once, however often it is tried.
	if err := os.Remove(filepath.Join(dir, "tls.key")); err != nil {
		t.Fatal(err)
	}
	handshake(13)
	handshake(13)
	if got := metric("pgshard_tls_material_reload_failures_total"); got != 3 {
		t.Fatalf("an unreadable key tried repeatedly: counted %v, want 3", got)
	}

	// A usable one takes over, and the exported expiry follows it.
	renewedExpiry := firstExpiry.Add(90 * 24 * time.Hour)
	renewedCert, renewedKey := ca.issueUntil(t, "server", 11, renewedExpiry)
	writeFile(t, dir, "tls.key", renewedKey)
	writeFile(t, dir, "tls.crt", renewedCert)
	handshake(11)
	if got := metric("pgshard_tls_certificate_not_after_seconds"); got != float64(renewedExpiry.Unix()) {
		t.Fatalf("expiry after renewal exported as %v, want %d", got, renewedExpiry.Unix())
	}
}

// syncBuffer is a bytes.Buffer a logger can write from a handshake's
// goroutine while the test reads it.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
