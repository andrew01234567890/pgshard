package main

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/andrew01234567890/pgshard/internal/cli"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/grpccreds"
	"github.com/andrew01234567890/pgshard/internal/pki"
)

// TestAControllerMovingToTLSServesPlaintextCallersAndStillNarrowsTLSOnes
// (PGS-236): with --tls-accept-plaintext the controller serves a caller
// still dialling plaintext -- an old router's CreateStream, the operator's
// CreateBarrier -- while a TLS caller keeps both checks: its identity must be
// admitted, and its role may call only its methods.
func TestAControllerMovingToTLSServesPlaintextCallersAndStillNarrowsTLSOnes(t *testing.T) {
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
	srvCert, srvKey := issue(pki.RoleController, "demo-controller.default.svc")
	routerCert, routerKey := issue(pki.RoleRouter)

	serve := func(acceptPlaintext bool) string {
		creds, err := grpccreds.Listener(srvCert, srvKey, caFile, false, listenerOptions(true, acceptPlaintext, pki.RoleController)...)
		if err != nil {
			t.Fatal(err)
		}
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		g := grpc.NewServer(serverOptions(creds, true, false, acceptPlaintext)...)
		pgshardv1.RegisterControllerServer(g, &pgshardv1.UnimplementedControllerServer{})
		go func() { _ = g.Serve(l) }()
		t.Cleanup(g.Stop)
		return l.Addr().String()
	}
	router, err := grpccreds.Dialer(routerCert, routerKey, caFile, "demo-controller.default.svc", false)
	if err != nil {
		t.Fatal(err)
	}
	cancel := func(addr string, creds credentials.TransportCredentials) codes.Code {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_, err = pgshardv1.NewControllerClient(conn).CancelWorkflow(ctx, &pgshardv1.CancelWorkflowRequest{})
		return status.Code(err)
	}

	moving := serve(true)
	if got := cancel(moving, insecure.NewCredentials()); got != codes.Unimplemented {
		t.Errorf("a plaintext caller of a controller moving to TLS: %v, want it served", got)
	}
	if got := cancel(moving, router); got != codes.PermissionDenied {
		t.Errorf("a router over TLS calling CancelWorkflow on a controller moving to TLS: %v, want its role refused the method", got)
	}
	if got := cancel(serve(false), insecure.NewCredentials()); got == codes.Unimplemented {
		t.Error("a plaintext caller reached a controller that requires TLS")
	}

	var out, errb bytes.Buffer
	if code := runController(context.Background(), []string{"--catalog-dsn", "postgres://x", "--insecure-dev", "--tls-accept-plaintext"}, &out, &errb); code != cli.ExitUsage || !strings.Contains(errb.String(), "needs TLS material") {
		t.Errorf("--tls-accept-plaintext with --insecure-dev: code %d, stderr %q", code, errb.String())
	}
}
