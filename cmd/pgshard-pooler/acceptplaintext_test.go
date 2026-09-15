package main

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
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

// TestAPoolerMovingToTLSServesRoutersDiallingEitherWay (PGS-236): while
// routers roll from plaintext to mutual TLS, a pooler started with
// --tls-accept-plaintext serves both, and a TLS caller's identity is still
// checked.
func TestAPoolerMovingToTLSServesRoutersDiallingEitherWay(t *testing.T) {
	now := time.Now()
	ca, err := pki.NewCA("demo", now)
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
	issue := func(role string, dns ...string) (string, string) {
		m, err := ca.Issue(pki.Request{Identity: pki.Identity{Namespace: "default", Cluster: "demo", Role: role}, DNSNames: dns, Server: len(dns) > 0, Client: true}, now)
		if err != nil {
			t.Fatal(err)
		}
		return write(role+".crt", m.CertPEM), write(role+".key", m.KeyPEM)
	}
	srvCert, srvKey := issue(pki.RolePooler, "demo.default.svc")

	ctx, stop := context.WithCancel(context.Background())
	out, errb := &syncBuffer{}, &syncBuffer{}
	done := make(chan int, 1)
	go func() {
		done <- runPooler(ctx, []string{"--listen", "127.0.0.1:0", "--drain-timeout", "1s",
			"--tls-cert", srvCert, "--tls-key", srvKey, "--tls-ca", caFile, "--tls-authorize-callers", "--tls-accept-plaintext"}, out, errb)
	}()
	t.Cleanup(func() { stop(); <-done })
	listening := regexp.MustCompile(`listening on (\S+) \(([^)]*)\)`)
	var addr string
	for deadline := time.Now().Add(5 * time.Second); addr == "" && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if m := listening.FindStringSubmatch(out.String()); m != nil {
			addr = m[1]
			if m[2] != "mTLS, and plaintext while moving to mTLS" {
				t.Errorf("the pooler says it listens with %q", m[2])
			}
		}
	}
	if addr == "" {
		t.Fatalf("the pooler never listened: %s", errb.String())
	}

	reached := func(creds credentials.TransportCredentials) codes.Code {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = pgshardv1.NewPoolerClient(conn).Release(ctx, &pgshardv1.ReleaseRequest{})
		return status.Code(err)
	}
	as := func(role string) credentials.TransportCredentials {
		cert, key := issue(role)
		d, err := grpccreds.Dialer(cert, key, caFile, "demo.default.svc", false)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	// The pooler answers an empty Release itself, so both callers that get
	// through see the same answer, and one refused at the handshake sees
	// Unavailable.
	plain, tlsRouter := reached(insecure.NewCredentials()), reached(as(pki.RoleRouter))
	if plain == codes.Unavailable || plain != tlsRouter {
		t.Errorf("a router dialling plaintext got %v and one dialling TLS %v; both should reach the service", plain, tlsRouter)
	}
	if got := reached(as(pki.RoleAgent)); got != codes.Unavailable {
		t.Errorf("an agent's certificate over TLS got %v, want it refused at the handshake", got)
	}
}
