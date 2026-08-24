package operator

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
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

type fakeController struct {
	pgshardv1.UnimplementedControllerServer
	names []string
}

func (f *fakeController) CreateBarrier(_ context.Context, req *pgshardv1.CreateBarrierRequest) (*pgshardv1.CreateBarrierResponse, error) {
	f.names = append(f.names, req.GetName())
	return &pgshardv1.CreateBarrierResponse{}, nil
}

// writeSelfSignedPair writes one self-signed certificate that serves as CA,
// server and client identity for 127.0.0.1 and returns the file paths.
func writeSelfSignedPair(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "pgshard-test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, IsCA: true, BasicConstraintsValid: true,
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile, keyFile = filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

func TestGRPCBarrierClientDialsControllerWithMTLS(t *testing.T) {
	certFile, keyFile := writeSelfSignedPair(t)
	if _, err := NewGRPCBarrierClient(certFile, "", ""); err == nil || !strings.Contains(err.Error(), "must be set together") {
		t.Fatalf("partial flags: %v", err)
	}
	if _, err := NewGRPCBarrierClient(certFile, keyFile, keyFile); err == nil || !strings.Contains(err.Error(), "no certificates found") {
		t.Fatalf("bad CA: %v", err)
	}
	plain, err := NewGRPCBarrierClient("", "", "")
	if err != nil || plain.Creds != nil {
		t.Fatalf("no flags must dial plaintext: %+v %v", plain, err)
	}
	secure, err := NewGRPCBarrierClient(certFile, keyFile, certFile)
	if err != nil || secure.Creds == nil {
		t.Fatalf("mTLS client: %+v %v", secure, err)
	}

	serverCert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(serverCert.Leaf)
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{serverCert}, ClientCAs: pool,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})))
	ctl := &fakeController{}
	pgshardv1.RegisterControllerServer(srv, ctl)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := secure.CreateBarrier(ctx, lis.Addr().String(), "b1"); err != nil {
		t.Fatalf("mTLS CreateBarrier: %v", err)
	}
	if strings.Join(ctl.names, ",") != "b1" {
		t.Fatalf("controller saw %v", ctl.names)
	}
	shortCtx, cancelShort := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelShort()
	if err := plain.CreateBarrier(shortCtx, lis.Addr().String(), "b2"); err == nil {
		t.Fatal("plaintext client reached an mTLS controller")
	}
	if len(ctl.names) != 1 {
		t.Fatalf("controller saw %v", ctl.names)
	}
}
