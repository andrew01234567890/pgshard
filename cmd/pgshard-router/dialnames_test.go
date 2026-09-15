package main

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

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/grpccreds"
	"github.com/andrew01234567890/pgshard/internal/pki"
)

// PGS-860: with issued certificates the router dials a pooler at its
// member's headless-Service host, which the pooler's certificate does not
// name, and every handshake failed. The router verifies the role-wide name
// the operator gives it instead, and checks the server is a pooler.
func TestTheRouterReachesAnIssuedPoolerByTheNameItsCertificateCarries(t *testing.T) {
	now := time.Now()
	ca, err := pki.NewCA("demo", now)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	write := func(name string, b []byte) string {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	caFile := write("ca.crt", ca.CertPEM)
	issue := func(role string, dns ...string) (string, string) {
		t.Helper()
		m, err := ca.Issue(pki.Request{Identity: pki.Identity{Namespace: "default", Cluster: "demo", Role: role}, DNSNames: dns, Server: len(dns) > 0, Client: true}, now)
		if err != nil {
			t.Fatal(err)
		}
		return write(role+".crt", m.CertPEM), write(role+".key", m.KeyPEM)
	}
	serve := func(role string) string {
		t.Helper()
		cert, key := issue(role, "demo", "demo.default", "demo.default.svc", "*.demo.default.svc")
		allow, _ := pki.AllowedCallers(pki.RolePooler)
		creds, err := grpccreds.Listener(cert, key, caFile, false, grpccreds.Authorize(allow))
		if err != nil {
			t.Fatal(err)
		}
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		g := grpc.NewServer(grpc.Creds(creds))
		pgshardv1.RegisterPoolerServer(g, pgshardv1.UnimplementedPoolerServer{})
		go func() { _ = g.Serve(l) }()
		t.Cleanup(g.Stop)
		return l.Addr().String()
	}
	routerCert, routerKey := issue(pki.RoleRouter, "demo-router.default.svc")
	call := func(addr, serverName string) error {
		t.Helper()
		creds, err := dialCredentials(routerCert, routerKey, caFile, false, serverName, true, pki.RolePooler)
		if err != nil {
			t.Fatal(err)
		}
		cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cc.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = pgshardv1.NewPoolerClient(cc).Health(ctx, &pgshardv1.HealthRequest{})
		if err == nil {
			return nil
		}
		// Past the handshake the unimplemented server answers Unimplemented.
		if status.Code(err) == codes.Unimplemented {
			return nil
		}
		return err
	}

	pooler := serve(pki.RolePooler)
	if err := call(pooler, ""); err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("dialling the pooler by its address verified: %v; the premise is that the issued certificate does not name it", err)
	}
	if err := call(pooler, "demo.default.svc"); err != nil {
		t.Fatalf("the router could not reach an issued pooler by the name its certificate carries: %v", err)
	}
	// A certificate of another role is valid for the same name; it must not
	// answer as a pooler.
	agent := serve(pki.RoleAgent)
	if err := call(agent, "demo.default.svc"); err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("an agent's certificate answering a router dialling a pooler was not refused for its identity: %v", err)
	}
}
