package pki

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"testing"
	"time"
)

var now = time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

func newCA(t *testing.T) *CA {
	t.Helper()
	ca, err := NewCA("demo", now)
	if err != nil {
		t.Fatal(err)
	}
	return ca
}

func TestAnIdentityRoundTripsThroughItsURI(t *testing.T) {
	for _, id := range []Identity{
		{Namespace: "ns", Cluster: "demo", Role: RoleRouter},
		{Namespace: "ns", Cluster: "demo", Role: RoleAgent, Member: "demo-shard-0-2"},
	} {
		got, err := ParseIdentity(id.String())
		if err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		if got != id {
			t.Fatalf("%s round-tripped to %+v", id, got)
		}
	}
}

func TestAForeignURIIsNotAnIdentityWithMissingParts(t *testing.T) {
	for _, raw := range []string{
		"spiffe://ns/demo/router",
		"pgshard://ns/demo",
		"pgshard:///demo/router",
		"https://example.test/demo/router",
	} {
		if id, err := ParseIdentity(raw); err == nil {
			t.Fatalf("%q was accepted as %+v", raw, id)
		}
	}
}

// TestAnIssuedCertificateIsAcceptedByARealHandshake is the assertion that
// matters: the fields can all look right and the certificate still fail to
// verify. This runs one, as a client, against a server that requires and
// verifies a client certificate, exactly as grpccreds configures it.
func TestAnIssuedCertificateIsAcceptedByARealHandshake(t *testing.T) {
	ca := newCA(t)
	server, err := ca.Issue(Request{
		Identity: Identity{Namespace: "ns", Cluster: "demo", Role: RolePooler, Member: "demo-shard-0-0"},
		DNSNames: []string{"demo-shard-0-0.ns.svc"}, Server: true}, now)
	if err != nil {
		t.Fatal(err)
	}
	client, err := ca.Issue(Request{
		Identity: Identity{Namespace: "ns", Cluster: "demo", Role: RoleRouter}, Client: true}, now)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca.CertPEM) {
		t.Fatal("the CA's own PEM did not parse")
	}
	sc, err := tls.X509KeyPair(server.CertPEM, server.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	cc, err := tls.X509KeyPair(client.CertPEM, client.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	peer := handshake(t,
		&tls.Config{Certificates: []tls.Certificate{sc}, ClientCAs: pool,
			ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13},
		&tls.Config{Certificates: []tls.Certificate{cc}, RootCAs: pool,
			ServerName: "demo-shard-0-0.ns.svc", MinVersion: tls.VersionTLS13})
	id, err := IdentityOfCert(peer)
	if err != nil {
		t.Fatal(err)
	}
	if want := (Identity{Namespace: "ns", Cluster: "demo", Role: RoleRouter}); id != want {
		t.Fatalf("the server saw %+v, want %+v", id, want)
	}
}

// TestAClientOnlyCertificateCannotServe is the constrained EKU doing its
// job: holding a router's client certificate must not let anything stand
// up a listener with it.
func TestAClientOnlyCertificateCannotServe(t *testing.T) {
	ca := newCA(t)
	client, err := ca.Issue(Request{
		Identity: Identity{Namespace: "ns", Cluster: "demo", Role: RoleRouter},
		DNSNames: []string{"impostor.ns.svc"}, Client: true}, now)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca.CertPEM)
	sc, err := tls.X509KeyPair(client.CertPEM, client.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	if err := handshakeErr(t,
		&tls.Config{Certificates: []tls.Certificate{sc}, MinVersion: tls.VersionTLS13},
		&tls.Config{RootCAs: pool, ServerName: "impostor.ns.svc", MinVersion: tls.VersionTLS13}); err == nil {
		t.Fatal("a client-only certificate was accepted as a server")
	}
}

func TestACertificateFromAnotherCAIsRefused(t *testing.T) {
	ours, theirs := newCA(t), newCA(t)
	client, err := theirs.Issue(Request{
		Identity: Identity{Namespace: "ns", Cluster: "demo", Role: RoleRouter}, Client: true}, now)
	if err != nil {
		t.Fatal(err)
	}
	server, err := ours.Issue(Request{
		Identity: Identity{Namespace: "ns", Cluster: "demo", Role: RolePooler},
		DNSNames: []string{"pooler.ns.svc"}, Server: true}, now)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ours.CertPEM)
	sc, _ := tls.X509KeyPair(server.CertPEM, server.KeyPEM)
	cc, _ := tls.X509KeyPair(client.CertPEM, client.KeyPEM)
	if err := handshakeErr(t,
		&tls.Config{Certificates: []tls.Certificate{sc}, ClientCAs: pool,
			ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13},
		&tls.Config{Certificates: []tls.Certificate{cc}, RootCAs: pool,
			ServerName: "pooler.ns.svc", MinVersion: tls.VersionTLS13}); err == nil {
		t.Fatal("a certificate from a different CA was accepted")
	}
}

func TestTheCACannotIssueAnIntermediate(t *testing.T) {
	ca := newCA(t)
	cert, err := parseCert(ca.CertPEM)
	if err != nil {
		t.Fatal(err)
	}
	if !cert.MaxPathLenZero || cert.MaxPathLen != 0 {
		t.Fatalf("the CA must not be able to sign another CA: MaxPathLen=%d zero=%v", cert.MaxPathLen, cert.MaxPathLenZero)
	}
}

func TestACertificateWithNoUseIsRefused(t *testing.T) {
	ca := newCA(t)
	if _, err := ca.Issue(Request{Identity: Identity{Namespace: "ns", Cluster: "demo", Role: RoleRouter}}, now); err == nil {
		t.Fatal("a certificate that is neither client nor server was issued")
	}
	if _, err := ca.Issue(Request{Identity: Identity{Cluster: "demo", Role: RoleRouter}, Client: true}, now); err == nil {
		t.Fatal("an identity with no namespace was issued")
	}
}

func TestALoadedCAIssuesCertificatesTheOriginalTrusts(t *testing.T) {
	ca := newCA(t)
	again, err := LoadCA(ca.Material)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := again.Issue(Request{
		Identity: Identity{Namespace: "ns", Cluster: "demo", Role: RoleAgent, Member: "m0"}, Client: true}, now)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca.CertPEM)
	cert, err := parseCert(leaf.CertPEM)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cert.Verify(x509.VerifyOptions{Roots: pool, CurrentTime: now,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		t.Fatalf("a certificate from the reloaded CA does not chain to it: %v", err)
	}
}

func TestRenewalStartsWithTimeToSpare(t *testing.T) {
	ca := newCA(t)
	leaf, err := ca.Issue(Request{
		Identity: Identity{Namespace: "ns", Cluster: "demo", Role: RoleRouter}, Client: true}, now)
	if err != nil {
		t.Fatal(err)
	}
	if NeedsRenewal(leaf.CertPEM, now) {
		t.Fatal("a certificate issued now already needs renewing")
	}
	if !NeedsRenewal(leaf.CertPEM, now.Add(LeafLifetime)) {
		t.Fatal("an expiring certificate does not need renewing")
	}
	// Two thirds through, with a third of its life left: renewal starts
	// while the certificate is still valid, so a rollout has room to fail
	// and be retried before anything stops working.
	if !NeedsRenewal(leaf.CertPEM, now.Add(LeafLifetime*3/4)) {
		t.Fatal("renewal leaves no room for a rollout")
	}
	if NeedsRenewal(leaf.CertPEM, now.Add(LeafLifetime/2)) {
		t.Fatal("renewal starts far earlier than it needs to")
	}
}

// handshake completes a TLS handshake over an in-memory pipe and returns
// the certificate the server saw.
func handshake(t *testing.T, server, client *tls.Config) *x509.Certificate {
	t.Helper()
	peer, err := run(server, client)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	return peer
}

// handshakeErr asserts the handshake was refused, and refused for a
// reason: a deadline means both ends were still waiting, which proves
// nothing about whether the certificate would ever have been accepted.
func handshakeErr(t *testing.T, server, client *tls.Config) error {
	t.Helper()
	_, err := run(server, client)
	if err != nil && os.IsTimeout(err) {
		t.Fatalf("the handshake timed out rather than being refused: %v", err)
	}
	return err
}

func run(server, client *tls.Config) (*x509.Certificate, error) {
	// A loopback socket rather than net.Pipe. A pipe is unbuffered, so the
	// alert a rejecting side sends blocks until its peer reads it, and a
	// refusal that should be instant instead waits out a deadline -- the
	// test still passes, ten seconds later, having proved that both ends
	// gave up rather than that either refused.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	defer func() { _ = ln.Close() }()

	type result struct {
		peer *x509.Certificate
		err  error
	}
	srv := make(chan result, 1)
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			srv <- result{err: err}
			return
		}
		defer func() { _ = raw.Close() }()
		_ = raw.SetDeadline(time.Now().Add(10 * time.Second))
		s := tls.Server(raw, server)
		if err := s.HandshakeContext(context.Background()); err != nil {
			srv <- result{err: err}
			return
		}
		var peer *x509.Certificate
		if certs := s.ConnectionState().PeerCertificates; len(certs) > 0 {
			peer = certs[0]
		}
		srv <- result{peer: peer}
	}()
	cli := make(chan error, 1)
	go func() {
		raw, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			cli <- err
			return
		}
		defer func() { _ = raw.Close() }()
		_ = raw.SetDeadline(time.Now().Add(10 * time.Second))
		cl := tls.Client(raw, client)
		cli <- cl.HandshakeContext(context.Background())
	}()

	var got result
	select {
	case got = <-srv:
		if got.err != nil {
			return nil, got.err
		}
		if err := <-cli; err != nil {
			return nil, err
		}
	case err := <-cli:
		if err != nil {
			return nil, err
		}
		got = <-srv
		if got.err != nil {
			return nil, got.err
		}
	}
	return got.peer, nil
}

func TestOnlyARouterMayCallAPooler(t *testing.T) {
	allow, ok := AllowedCallers(RolePooler)
	if !ok {
		t.Fatal("a pooler has no rule")
	}
	id := func(role string) Identity {
		return Identity{Namespace: "ns", Cluster: "demo", Role: role}
	}
	if !allow(id(RoleRouter)) {
		t.Fatal("a pooler must serve routers")
	}
	for _, role := range []string{RoleAgent, RolePooler, RoleController, RoleOperator, RoleAdmin} {
		if allow(id(role)) {
			t.Fatalf("a pooler must not serve %s", role)
		}
	}
}

func TestTheChangeStreamHasNoRuleBecauseItsCallersHaveNoIdentity(t *testing.T) {
	// Consumers of the change stream are outside the cluster. A rule here
	// would refuse exactly the traffic the listener exists to serve, so
	// its absence is deliberate and this says so out loud.
	for _, role := range []string{RoleAdmin, "vstream", "consumer"} {
		if _, ok := AllowedCallers(role); ok {
			t.Fatalf("%s gained a caller rule; check that its callers carry identities", role)
		}
	}
}

func TestAgentAndControllerServeTheirOwnCallers(t *testing.T) {
	for _, tc := range []struct {
		listener string
		serves   []string
		refuses  []string
	}{
		{RoleAgent, []string{RoleController, RoleOperator}, []string{RoleRouter, RolePooler, RoleAgent}},
		{RoleController, []string{RoleRouter, RoleOperator}, []string{RoleAgent, RolePooler}},
	} {
		allow, ok := AllowedCallers(tc.listener)
		if !ok {
			t.Fatalf("%s has no rule", tc.listener)
		}
		for _, r := range tc.serves {
			if !allow(Identity{Namespace: "ns", Cluster: "demo", Role: r}) {
				t.Fatalf("%s must serve %s", tc.listener, r)
			}
		}
		for _, r := range tc.refuses {
			if allow(Identity{Namespace: "ns", Cluster: "demo", Role: r}) {
				t.Fatalf("%s must not serve %s", tc.listener, r)
			}
		}
	}
}
