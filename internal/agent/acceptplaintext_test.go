package agent

import (
	"context"
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

// TestAnAgentMovingToTLSServesTheOperatorDiallingEitherWay (PGS-236): the
// operator and controller switch an agent's callers to TLS member by member;
// grpcTLS.acceptPlaintext keeps a caller that has not switched yet from being
// refused, while a TLS caller's identity is still checked.
func TestAnAgentMovingToTLSServesTheOperatorDiallingEitherWay(t *testing.T) {
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
	srvCert, srvKey := issue(pki.RoleAgent, "demo.default.svc")
	files := TLSFiles{CertFile: srvCert, KeyFile: srvKey, CAFile: caFile, AuthorizeCallers: true, AcceptPlaintext: true}

	serve := func(files TLSFiles) string {
		creds, err := files.listenerCredentials()
		if err != nil {
			t.Fatal(err)
		}
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		g := grpc.NewServer(grpc.Creds(creds))
		pgshardv1.RegisterAgentServer(g, &pgshardv1.UnimplementedAgentServer{})
		go func() { _ = g.Serve(l) }()
		t.Cleanup(g.Stop)
		return l.Addr().String()
	}
	reached := func(addr string, creds credentials.TransportCredentials) bool {
		cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cc.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = pgshardv1.NewAgentClient(cc).Status(ctx, &pgshardv1.StatusRequest{})
		return status.Code(err) == codes.Unimplemented
	}
	as := func(role string) credentials.TransportCredentials {
		cert, key := issue(role)
		d, err := grpccreds.Dialer(cert, key, caFile, "demo.default.svc", false)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}

	moving := serve(files)
	if !reached(moving, insecure.NewCredentials()) {
		t.Error("an operator still dialling plaintext was refused by an agent moving to TLS")
	}
	if !reached(moving, as(pki.RoleOperator)) {
		t.Error("an operator dialling TLS was refused by an agent moving to TLS")
	}
	if reached(moving, as(pki.RoleRouter)) {
		t.Error("a router's certificate reached an agent over TLS")
	}
	files.AcceptPlaintext = false
	if reached(serve(files), insecure.NewCredentials()) {
		t.Error("a plaintext caller reached an agent that requires TLS")
	}
	if _, err := (TLSFiles{AcceptPlaintext: true}).listenerCredentials(); err == nil {
		t.Error("acceptPlaintext without certificate files must be refused, not served as plaintext")
	}
}
