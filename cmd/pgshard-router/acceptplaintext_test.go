package main

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
	"github.com/andrew01234567890/pgshard/internal/pki"
)

// TestARouterMovingToTLSDialsPlaintextAndServesBoth (PGS-236): in the first
// step of a move to mutual TLS a router already holds the material its own
// listeners serve with, but the poolers, peers and controller it dials may
// not accept TLS yet. --tls-dial-plaintext keeps its dials plaintext, and
// --tls-accept-plaintext lets its peer-cancel and change-stream listeners
// serve old routers and consumers still dialling plaintext alongside new
// ones dialling TLS.
func TestARouterMovingToTLSDialsPlaintextAndServesBoth(t *testing.T) {
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
	m, err := ca.Issue(pki.Request{Identity: pki.Identity{Namespace: "default", Cluster: "demo", Role: pki.RoleRouter}, DNSNames: []string{"demo-router.default.svc"}, Server: true, Client: true}, now)
	if err != nil {
		t.Fatal(err)
	}
	cert, key := write("router.crt", m.CertPEM), write("router.key", m.KeyPEM)

	serveWith := func(creds credentials.TransportCredentials) string {
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
	reached := func(addr string, creds credentials.TransportCredentials) bool {
		cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cc.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = pgshardv1.NewPoolerClient(cc).Release(ctx, &pgshardv1.ReleaseRequest{})
		return status.Code(err) == codes.Unimplemented
	}

	plaintextPooler := serveWith(insecure.NewCredentials())
	dialling, err := dialCredentials(cert, key, caFile, false, true, "demo.default.svc", true, pki.RolePooler)
	if err != nil {
		t.Fatal(err)
	}
	if !reached(plaintextPooler, dialling) {
		t.Error("a router holding TLS material with --tls-dial-plaintext could not reach a pooler still serving plaintext")
	}

	listener, err := peerCredentials(cert, key, caFile, false, true, true, pki.RoleRouter)
	if err != nil {
		t.Fatal(err)
	}
	peer := serveWith(listener)
	tlsPeer, err := dialCredentials(cert, key, caFile, false, false, "demo-router.default.svc", true, pki.RoleRouter)
	if err != nil {
		t.Fatal(err)
	}
	if !reached(peer, insecure.NewCredentials()) {
		t.Error("an old router dialling plaintext could not reach a peer listener accepting plaintext")
	}
	if !reached(peer, tlsPeer) {
		t.Error("a new router dialling TLS could not reach a peer listener accepting plaintext")
	}
	required, err := peerCredentials(cert, key, caFile, false, false, true, pki.RoleRouter)
	if err != nil {
		t.Fatal(err)
	}
	if reached(serveWith(required), insecure.NewCredentials()) {
		t.Error("a plaintext caller reached a peer listener that requires TLS")
	}
}
