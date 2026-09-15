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
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/andrew01234567890/pgshard/internal/pki"
)

// Option adjusts what a listener or dialler accepts.
type Option func(*options)

type options struct {
	allow           func(pki.Identity) bool
	acceptPlaintext bool
}

// AcceptPlaintext makes a TLS listener also serve plaintext connections on
// the same port, for the length of a move from plaintext to mutual TLS.
//
// A cluster cannot switch every caller and every listener at once: members
// roll one at a time and routers roll as a Deployment, so for a while some
// callers still dial plaintext while others dial TLS. A listener that
// accepts both is what lets that mixed cluster keep serving. A connection
// that opens with a TLS record gets the full handshake -- client
// certificate required and verified, Authorize applied -- and one that does
// not is served as --insecure-dev serves it. It is a transition setting:
// until it is removed the listener is no more protected than a plaintext
// one.
func AcceptPlaintext() Option {
	return func(o *options) { o.acceptPlaintext = true }
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
	o := apply(opts)
	if insecureDev {
		if certFile != "" || keyFile != "" || caFile != "" {
			return nil, errors.New("--insecure-dev cannot be combined with TLS flags")
		}
		if o.acceptPlaintext {
			return nil, errors.New("accepting plaintext alongside TLS needs TLS material; --insecure-dev serves plaintext only")
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
	verify := verifyIdentity(o.allow)
	config := func(cert tls.Certificate, pool *x509.CertPool) *tls.Config {
		return &tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: pool,
			ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13,
			NextProtos: []string{"h2"}, VerifyPeerCertificate: verify}
	}
	cfg := config(m.current())
	cfg.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		return config(m.current()), nil
	}
	if o.acceptPlaintext {
		return eitherListener{tls: credentials.NewTLS(cfg)}, nil
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

// DialerPEM returns dialler credentials from certificate, key and CA held in
// memory rather than files: what a process that reads another workload's
// Secret through the API dials with. It is fail-closed like Dialer. The
// material is fixed; a caller whose Secret changes builds new credentials.
func DialerPEM(certPEM, keyPEM, caPEM []byte, serverName string, opts ...Option) (credentials.TransportCredentials, error) {
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("no CA certificates found")
	}
	return credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool, ServerName: serverName,
		MinVersion: tls.VersionTLS13, VerifyPeerCertificate: verifyIdentity(apply(opts).allow)}), nil
}

// material is one set of TLS files, read again at every handshake.
//
// A renewal that cannot be used -- unreadable, corrupt, a key that does not
// match -- keeps the last good material, and would otherwise do so silently
// until the certificate in use expired: the outage renewal exists to
// prevent, with nothing pointing at it (PGS-847). Each distinct failure is
// logged and counted once, and not parsed again at every handshake; the
// expiry of the certificate in use is exported so an alert can fire first.
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

	// mu is held across the reads as well as the parse: two handshakes
	// that read either side of a file swap and installed what they read in
	// the other order put the older generation back.
	mu       sync.Mutex
	raw      [3][]byte
	cert     tls.Certificate
	pool     *x509.CertPool
	notAfter time.Time
	// failed fingerprints the last files, or read error, that could not be
	// used, and failErr is what they failed with. reported is set once the
	// same failure has been read twice: a certificate read before the
	// kubelet's swap and a key read after it do not match either, and the
	// next read heals that. Content that fails, is replaced by good content
	// and then comes back unchanged is not reported again.
	failed   [32]byte
	failErr  error
	reported bool
	failures int
}

// loaded is every material this process has loaded, for Collector, and
// logged the failure last logged for each certificate file: a process that
// listens and dials with the same files loads them twice, and says so once.
var (
	loadedMu sync.Mutex
	loaded   []*material
	logged   = map[string][32]byte{}
)

func loadMaterial(certFile, keyFile, caFile string) (*material, error) {
	m := &material{certFile: certFile, keyFile: keyFile, caFile: caFile}
	if err := m.refresh(); err != nil {
		return nil, err
	}
	loadedMu.Lock()
	loaded = append(loaded, m)
	loadedMu.Unlock()
	return m, nil
}

func (m *material) refresh() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var raw [3][]byte
	for i, f := range []string{m.certFile, m.keyFile, m.caFile} {
		b, err := os.ReadFile(f)
		if err != nil {
			return m.fail(sha256.Sum256([]byte(err.Error())), err)
		}
		raw[i] = b
	}
	// m.pool is nil until the first parse succeeds: empty files compare
	// equal to nothing read yet, and must still be parsed -- and refused.
	if m.pool != nil && bytes.Equal(raw[0], m.raw[0]) && bytes.Equal(raw[1], m.raw[1]) && bytes.Equal(raw[2], m.raw[2]) {
		return nil
	}
	sum := sha256.New()
	for _, b := range raw {
		sum.Write(b)
		sum.Write([]byte{0})
	}
	var fingerprint [32]byte
	sum.Sum(fingerprint[:0])
	if m.failErr != nil && fingerprint == m.failed {
		return m.fail(fingerprint, m.failErr)
	}
	cert, err := tls.X509KeyPair(raw[0], raw[1])
	if err != nil {
		return m.fail(fingerprint, err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return m.fail(fingerprint, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(raw[2]) {
		return m.fail(fingerprint, fmt.Errorf("%s: no certificates found", m.caFile))
	}
	m.raw, m.cert, m.pool, m.notAfter = raw, cert, pool, leaf.NotAfter
	m.failErr = nil
	return nil
}

// fail records a refresh that could not be used, and reports it the second
// time the same failure is read. Only a failure with a last good material to
// fall back on is reported: without one the caller returns the error and the
// process does not start.
func (m *material) fail(fingerprint [32]byte, err error) error {
	if m.failErr == nil || fingerprint != m.failed {
		m.failed, m.failErr, m.reported = fingerprint, err, false
		return err
	}
	if m.reported || m.pool == nil {
		return m.failErr
	}
	m.reported = true
	err = m.failErr
	m.failures++
	loadedMu.Lock()
	first := logged[m.certFile] != fingerprint
	logged[m.certFile] = fingerprint
	loadedMu.Unlock()
	if first {
		slog.Default().Warn("internal TLS material could not be reloaded; still using the previous certificate",
			"cert_file", m.certFile, "key_file", m.keyFile, "ca_file", m.caFile, "not_after", m.notAfter, "err", err)
	}
	return err
}

var (
	notAfterDesc = prometheus.NewDesc("pgshard_tls_certificate_not_after_seconds",
		"Expiry, as a Unix time, of the internal TLS certificate in use for each certificate file.", []string{"cert_file"}, nil)
	reloadFailuresDesc = prometheus.NewDesc("pgshard_tls_material_reload_failures_total",
		"Distinct renewals of internal TLS material that could not be used, so the previous certificate stayed in use.", []string{"cert_file"}, nil)
)

type collector struct{}

// Collector exports the expiry of the internal TLS certificates this process
// uses, and the renewals of them it could not use.
func Collector() prometheus.Collector { return collector{} }

func (collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- notAfterDesc
	ch <- reloadFailuresDesc
}

// Collect reports one series per certificate file, for the material the
// next connection would use: each is read again first, so a renewal that
// cannot be used is noticed at the scrape even while no connection is
// made. A process that listens and dials with the same files loads them
// twice; the earliest expiry and the larger failure count speak for both.
func (collector) Collect(ch chan<- prometheus.Metric) {
	type state struct {
		notAfter time.Time
		failures int
	}
	byFile := map[string]state{}
	loadedMu.Lock()
	ms := append([]*material(nil), loaded...)
	loadedMu.Unlock()
	for _, m := range ms {
		_ = m.refresh()
		m.mu.Lock()
		st, seen := byFile[m.certFile]
		if !seen || m.notAfter.Before(st.notAfter) {
			st.notAfter = m.notAfter
		}
		st.failures = max(st.failures, m.failures)
		byFile[m.certFile] = st
		m.mu.Unlock()
	}
	for file, st := range byFile {
		ch <- prometheus.MustNewConstMetric(notAfterDesc, prometheus.GaugeValue, float64(st.notAfter.Unix()), file)
		ch <- prometheus.MustNewConstMetric(reloadFailuresDesc, prometheus.CounterValue, float64(st.failures), file)
	}
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
