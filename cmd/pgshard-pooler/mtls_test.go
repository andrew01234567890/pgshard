package main

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
)

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
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &testCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func (ca *testCA) issue(t *testing.T, cn string, serial int64) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, ca.cert, &key.PublicKey, ca.key)
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

func writeFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestPoolerListenerRejectsNonMTLSPeers(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	srvCert, srvKey := ca.issue(t, "pooler", 2)
	cliCert, cliKey := ca.issue(t, "router", 3)
	caFile := writeFile(t, dir, "ca.crt", ca.pem)
	creds, err := grpccreds.Listener(writeFile(t, dir, "tls.crt", srvCert), writeFile(t, dir, "tls.key", srvKey), caFile, false)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer(grpc.Creds(creds))
	pgshardv1.RegisterPoolerServer(g, &pgshardv1.UnimplementedPoolerServer{})
	go func() { _ = g.Serve(ln) }()
	defer g.Stop()

	call := func(tc credentials.TransportCredentials) error {
		conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(tc))
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err = pgshardv1.NewPoolerClient(conn).Release(ctx, &pgshardv1.ReleaseRequest{})
		return err
	}

	if err := call(insecure.NewCredentials()); status.Code(err) == codes.Unimplemented {
		t.Fatal("plaintext client must not reach the pooler service")
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca.pem)
	if err := call(credentials.NewTLS(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13})); status.Code(err) == codes.Unimplemented {
		t.Fatal("TLS client without a client certificate must be rejected")
	}
	pair, err := tls.X509KeyPair(cliCert, cliKey)
	if err != nil {
		t.Fatal(err)
	}
	err = call(credentials.NewTLS(&tls.Config{RootCAs: pool, Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS13}))
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("mTLS client must reach the service, got %v", err)
	}
}
