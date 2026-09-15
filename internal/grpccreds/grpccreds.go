// Package grpccreds builds the transport credentials pgshard's internal
// gRPC listeners use.
//
// It exists so there is one definition of what "mTLS" means here rather
// than one per binary. The settings below are the security property --
// client certificates required and verified against a named CA, TLS 1.3
// floor -- and a second copy that quietly said RequireAnyClientCert, or
// omitted ClientCAs, would authenticate nothing while looking identical at
// the call site.
package grpccreds

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/andrew01234567890/pgshard/internal/pki"
)

// Option adjusts what a listener or dialler accepts.
type Option func(*options)

type options struct {
	allow func(pki.Identity) bool
}

// Authorize restricts a listener to peers whose certificate carries a
// pgshard identity allow accepts.
//
// Verifying that a peer chains to the CA is not the same as knowing who it
// is: with one CA behind every internal workload, a certificate issued to
// an agent is as acceptable to a pooler as a router's. This is where that
// stops being true, and it is deliberately fail-closed -- a peer with no
// identity, or an unreadable one, is refused rather than treated as
// unnamed and allowed.
func Authorize(allow func(pki.Identity) bool) Option {
	return func(o *options) { o.allow = allow }
}

func apply(opts []Option) options {
	var o options
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// verifyIdentity builds the callback tls runs after the chain has been
// verified. It runs only once the chain is good, so it decides authority
// and never authenticity.
func verifyIdentity(allow func(pki.Identity) bool) func([][]byte, [][]*x509.Certificate) error {
	if allow == nil {
		return nil
	}
	return func(_ [][]byte, chains [][]*x509.Certificate) error {
		if len(chains) == 0 || len(chains[0]) == 0 {
			return errors.New("no verified certificate chain to take an identity from")
		}
		id, err := pki.IdentityOfCert(chains[0][0])
		if err != nil {
			return err
		}
		if !allow(id) {
			return fmt.Errorf("peer identity %s is not allowed on this listener", id)
		}
		return nil
	}
}

// Listener returns the credentials an internal gRPC server listens with.
//
// It is deliberately fail-closed: without the three files, and without an
// explicit insecureDev, it returns an error rather than falling back to
// plaintext. A server that serves unauthenticated traffic because a flag
// was missing is the failure this shape prevents.
func Listener(certFile, keyFile, caFile string, insecureDev bool, opts ...Option) (credentials.TransportCredentials, error) {
	if insecureDev {
		if certFile != "" || keyFile != "" || caFile != "" {
			return nil, errors.New("--insecure-dev cannot be combined with TLS flags")
		}
		return insecure.NewCredentials(), nil
	}
	if certFile == "" || keyFile == "" || caFile == "" {
		return nil, errors.New("--tls-cert, --tls-key and --tls-ca are required (or --insecure-dev)")
	}
	m, err := loadMaterial(certFile, keyFile, caFile)
	if err != nil {
		return nil, err
	}
	verify := verifyIdentity(apply(opts).allow)
	config := func(cert tls.Certificate, pool *x509.CertPool) *tls.Config {
		return &tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: pool,
			ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13,
			NextProtos: []string{"h2"}, VerifyPeerCertificate: verify}
	}
	cfg := config(m.current())
	cfg.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		return config(m.current()), nil
	}
	return credentials.NewTLS(cfg), nil
}

// Dialer returns the credentials an internal gRPC client dials with: it
// presents certFile/keyFile and verifies the server against caFile.
//
// The mirror of Listener, and fail-closed the same way, so a caller cannot
// end up in plaintext because a flag was forgotten. serverName is the name
// the server's certificate must carry; empty uses the dial address.
func Dialer(certFile, keyFile, caFile, serverName string, insecureDev bool, opts ...Option) (credentials.TransportCredentials, error) {
	if insecureDev {
		if certFile != "" || keyFile != "" || caFile != "" {
			return nil, errors.New("insecure dialling cannot be combined with TLS material")
		}
		return insecure.NewCredentials(), nil
	}
	if certFile == "" || keyFile == "" || caFile == "" {
		return nil, errors.New("a client certificate, key and CA are all required (or insecure dialling)")
	}
	m, err := loadMaterial(certFile, keyFile, caFile)
	if err != nil {
		return nil, err
	}
	return &reloadingDialer{m: m, serverName: serverName, verify: verifyIdentity(apply(opts).allow)}, nil
}

// material is one set of TLS files, read again at every handshake.
//
// Certificates are renewed into the same files: the operator reissues a
// role's certificate into its Secret well before it expires, and the kubelet
// swaps the mounted files in place. A process that parsed them once at start
// kept presenting, and trusting, what it read then -- until it restarted, or
// until the certificate it held expired and every handshake failed
// (PGS-799). Read at the handshake, a renewal takes effect on the next
// connection. The files are small and handshakes are rare, since gRPC keeps
// its connections, so they are read every time and parsed only when their
// bytes change. A read that fails, or a certificate and key that do not
// match because the swap landed between the two reads, keeps the last good
// material; the next handshake tries again.
type material struct {
	certFile, keyFile, caFile string

	mu   sync.Mutex
	raw  [3][]byte
	cert tls.Certificate
	pool *x509.CertPool
}

func loadMaterial(certFile, keyFile, caFile string) (*material, error) {
	m := &material{certFile: certFile, keyFile: keyFile, caFile: caFile}
	if err := m.refresh(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *material) refresh() error {
	var raw [3][]byte
	for i, f := range []string{m.certFile, m.keyFile, m.caFile} {
		b, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		raw[i] = b
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// m.pool is nil until the first parse succeeds: empty files compare
	// equal to nothing read yet, and must still be parsed -- and refused.
	if m.pool != nil && bytes.Equal(raw[0], m.raw[0]) && bytes.Equal(raw[1], m.raw[1]) && bytes.Equal(raw[2], m.raw[2]) {
		return nil
	}
	cert, err := tls.X509KeyPair(raw[0], raw[1])
	if err != nil {
		return err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(raw[2]) {
		return fmt.Errorf("%s: no certificates found", m.caFile)
	}
	m.raw, m.cert, m.pool = raw, cert, pool
	return nil
}

// current is what to present and trust for a handshake starting now.
func (m *material) current() (tls.Certificate, *x509.CertPool) {
	_ = m.refresh()
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cert, m.pool
}

// reloadingDialer builds the TLS configuration afresh for every handshake,
// so the certificate it presents and the CA it verifies the server against
// are the files as they are now. The verification is crypto/tls's own; only
// its inputs are read later.
type reloadingDialer struct {
	m          *material
	serverName string
	verify     func([][]byte, [][]*x509.Certificate) error
}

func (d *reloadingDialer) creds() credentials.TransportCredentials {
	cert, pool := d.m.current()
	return credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool,
		ServerName: d.serverName, MinVersion: tls.VersionTLS13, VerifyPeerCertificate: d.verify})
}

func (d *reloadingDialer) ClientHandshake(ctx context.Context, authority string, conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return d.creds().ClientHandshake(ctx, authority, conn)
}

func (d *reloadingDialer) ServerHandshake(net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("grpccreds: dialer credentials cannot accept connections")
}

func (d *reloadingDialer) Info() credentials.ProtocolInfo { return d.creds().Info() }

func (d *reloadingDialer) Clone() credentials.TransportCredentials {
	c := *d
	return &c
}

// OverrideServerName is part of credentials.TransportCredentials.
func (d *reloadingDialer) OverrideServerName(name string) error {
	d.serverName = name
	return nil
}
