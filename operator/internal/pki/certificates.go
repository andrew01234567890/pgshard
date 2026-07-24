package pki

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"slices"
	"sort"
	"strings"
	"time"
)

const (
	CACertificateKey  = "ca.crt"
	CAPrivateKeyKey   = "ca.key"
	TLSCertificateKey = "tls.crt"
	TLSPrivateKeyKey  = "tls.key"

	caValidity   = 10 * 365 * 24 * time.Hour
	leafValidity = 90 * 24 * time.Hour
	renewBefore  = 30 * 24 * time.Hour
	// Static server certificates are intentionally longer lived than admission
	// certificates. PostgreSQL does not yet have a Secret-to-writable-file
	// reload sidecar, so the operator fails closed well before this material
	// expires instead of pretending it can rotate it without a restart.
	staticServerValidity = 5 * 365 * 24 * time.Hour
	staticRenewBefore    = 180 * 24 * time.Hour
	certificateSkew      = 5 * time.Minute

	// StaticRenewBefore is the minimum remaining static-material validity the
	// operator accepts before failing closed instead of pretending it can
	// rotate certificates without a restart.
	StaticRenewBefore = staticRenewBefore
)

// StaticServerBundle contains one self-signed CA certificate and its issued
// server keypair. The CA private key is discarded after issuance.
type StaticServerBundle struct {
	CACertificate     []byte
	ServerCertificate []byte
	ServerPrivateKey  []byte
}

type certificateAuthority struct {
	certificate    *x509.Certificate
	privateKey     *ecdsa.PrivateKey
	certificatePEM []byte
	privateKeyPEM  []byte
}

type servingCertificate struct {
	certificate    *x509.Certificate
	certificatePEM []byte
	privateKeyPEM  []byte
}

func generateCertificateAuthority(now time.Time, random io.Reader, commonName string) (*certificateAuthority, error) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), random)
	if err != nil {
		return nil, fmt.Errorf("generate CA private key: %w", err)
	}
	serial, err := randomSerial(random)
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: commonName, Organization: []string{"pgshard"}},
		NotBefore:             now.Add(-certificateSkew),
		NotAfter:              now.Add(caValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            0,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(random, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, fmt.Errorf("create CA certificate: %w", err)
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return nil, fmt.Errorf("marshal CA private key: %w", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("parse generated CA certificate: %w", err)
	}
	return &certificateAuthority{
		certificate:    certificate,
		privateKey:     privateKey,
		certificatePEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		privateKeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER}),
	}, nil
}

func parseCertificateAuthority(certificatePEM, privateKeyPEM []byte, now time.Time) (*certificateAuthority, error) {
	return parseCertificateAuthorityWithMinimumLifetime(certificatePEM, privateKeyPEM, now, leafValidity)
}

func parseCertificateAuthorityWithMinimumLifetime(certificatePEM, privateKeyPEM []byte, now time.Time, minimumRemaining time.Duration) (*certificateAuthority, error) {
	if _, err := tls.X509KeyPair(certificatePEM, privateKeyPEM); err != nil {
		return nil, fmt.Errorf("CA certificate and private key do not form a key pair: %w", err)
	}
	authority, err := parseCertificateAuthorityCertificate(certificatePEM, now, minimumRemaining)
	if err != nil {
		return nil, err
	}
	privateKey, err := parseECDSAPrivateKey(privateKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("parse CA private key: %w", err)
	}
	authority.privateKey = privateKey
	authority.privateKeyPEM = slices.Clone(privateKeyPEM)
	return authority, nil
}

func parseCertificateAuthorityCertificate(certificatePEM []byte, now time.Time, minimumRemaining time.Duration) (*certificateAuthority, error) {
	certificate, err := parseCertificate(certificatePEM)
	if err != nil {
		return nil, fmt.Errorf("parse CA certificate: %w", err)
	}
	if !certificate.BasicConstraintsValid || !certificate.IsCA || certificate.KeyUsage&x509.KeyUsageCertSign == 0 {
		return nil, fmt.Errorf("CA certificate does not permit certificate signing")
	}
	if err := certificate.CheckSignatureFrom(certificate); err != nil {
		return nil, fmt.Errorf("CA certificate is not self-signed: %w", err)
	}
	if now.Before(certificate.NotBefore) {
		return nil, fmt.Errorf("CA certificate is not valid before %s", certificate.NotBefore.UTC().Format(time.RFC3339))
	}
	if !certificate.NotAfter.After(now.Add(minimumRemaining)) {
		return nil, fmt.Errorf("CA certificate expires too soon at %s; automated CA rotation is not implemented", certificate.NotAfter.UTC().Format(time.RFC3339))
	}
	return &certificateAuthority{
		certificate:    certificate,
		certificatePEM: slices.Clone(certificatePEM),
	}, nil
}

func generateServingCertificate(now time.Time, random io.Reader, authority *certificateAuthority, dnsNames []string) (*servingCertificate, error) {
	return generateServingCertificateWithValidity(now, random, authority, dnsNames, leafValidity)
}

func generateServingCertificateWithValidity(now time.Time, random io.Reader, authority *certificateAuthority, dnsNames []string, validity time.Duration) (*servingCertificate, error) {
	names := normalizedDNSNames(dnsNames)
	if len(names) == 0 {
		return nil, fmt.Errorf("at least one serving DNS name is required")
	}
	if validity <= 0 {
		return nil, fmt.Errorf("serving certificate validity must be positive")
	}
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), random)
	if err != nil {
		return nil, fmt.Errorf("generate serving private key: %w", err)
	}
	serial, err := randomSerial(random)
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: names[0], Organization: []string{"pgshard"}},
		DNSNames:              names,
		NotBefore:             now.Add(-certificateSkew),
		NotAfter:              now.Add(validity),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	if !template.NotAfter.Before(authority.certificate.NotAfter) {
		return nil, fmt.Errorf("CA certificate lifetime is too short for a serving certificate")
	}
	der, err := x509.CreateCertificate(random, template, authority.certificate, &privateKey.PublicKey, authority.privateKey)
	if err != nil {
		return nil, fmt.Errorf("create serving certificate: %w", err)
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return nil, fmt.Errorf("marshal serving private key: %w", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("parse generated serving certificate: %w", err)
	}
	return &servingCertificate{
		certificate:    certificate,
		certificatePEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		privateKeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER}),
	}, nil
}

// GenerateStaticServerBundle creates a long-lived self-signed CA and one
// exact-DNS server certificate for a PostgreSQL endpoint.
func GenerateStaticServerBundle(now time.Time, random io.Reader, commonName string, dnsNames []string) (*StaticServerBundle, error) {
	if random == nil {
		return nil, fmt.Errorf("certificate randomness source is required")
	}
	if strings.TrimSpace(commonName) == "" {
		return nil, fmt.Errorf("CA common name is required")
	}
	authority, err := generateCertificateAuthority(now, random, commonName)
	if err != nil {
		return nil, err
	}
	server, err := generateServingCertificateWithValidity(now, random, authority, dnsNames, staticServerValidity)
	if err != nil {
		return nil, err
	}
	return &StaticServerBundle{
		CACertificate:     slices.Clone(authority.certificatePEM),
		ServerCertificate: slices.Clone(server.certificatePEM),
		ServerPrivateKey:  slices.Clone(server.privateKeyPEM),
	}, nil
}

// ValidateStaticServerBundle verifies the self-signed CA, server key pairing,
// exact DNS names, server-auth chain, and sufficient remaining lifetime. The
// CA private key is deliberately discarded after issuance and is not required.
func ValidateStaticServerBundle(bundle *StaticServerBundle, dnsNames []string, now time.Time) error {
	if bundle == nil {
		return fmt.Errorf("static server certificate bundle is required")
	}
	authority, err := parseCertificateAuthorityCertificate(bundle.CACertificate, now, staticRenewBefore)
	if err != nil {
		return err
	}
	server, err := parseServingCertificate(bundle.ServerCertificate, bundle.ServerPrivateKey)
	if err != nil {
		return err
	}
	if !servingCertificateIsUsable(server, authority, dnsNames, now) {
		return fmt.Errorf("server certificate is not valid for the exact configured DNS names and CA")
	}
	if !server.certificate.NotAfter.After(now.Add(staticRenewBefore)) {
		return fmt.Errorf("server certificate expires too soon at %s; zero-downtime certificate rotation is not implemented", server.certificate.NotAfter.UTC().Format(time.RFC3339))
	}
	return nil
}

// ReplicationTLSServerMaterial is one member's issued server keypair.
type ReplicationTLSServerMaterial struct {
	Certificate []byte
	PrivateKey  []byte
	NotAfter    time.Time
}

// ReplicationTLSBundle contains one self-signed replication CA certificate and
// one issued server keypair per shard member. The CA private key is discarded
// after issuance, so a partially installed bundle can never be completed later.
type ReplicationTLSBundle struct {
	CACertificate []byte
	CANotAfter    time.Time
	Servers       map[int32]ReplicationTLSServerMaterial
}

// GenerateReplicationTLSBundle creates a long-lived self-signed CA and one
// server certificate per member. Every leaf carries exactly one DNS SAN: the
// member's stable Pod DNS name.
func GenerateReplicationTLSBundle(now time.Time, random io.Reader, caCommonName string, memberDNSNames map[int32][]string) (*ReplicationTLSBundle, error) {
	if random == nil {
		return nil, fmt.Errorf("certificate randomness source is required")
	}
	if strings.TrimSpace(caCommonName) == "" {
		return nil, fmt.Errorf("CA common name is required")
	}
	if len(memberDNSNames) == 0 {
		return nil, fmt.Errorf("at least one member DNS name is required")
	}
	members := make([]int32, 0, len(memberDNSNames))
	for member, names := range memberDNSNames {
		if member < 0 {
			return nil, fmt.Errorf("member %d is not a valid shard member", member)
		}
		if len(names) != 1 || strings.TrimSpace(names[0]) == "" {
			return nil, fmt.Errorf("member %d requires exactly one non-empty DNS name", member)
		}
		members = append(members, member)
	}
	slices.Sort(members)
	authority, err := generateCertificateAuthority(now, random, caCommonName)
	if err != nil {
		return nil, err
	}
	bundle := &ReplicationTLSBundle{
		CACertificate: slices.Clone(authority.certificatePEM),
		CANotAfter:    authority.certificate.NotAfter,
		Servers:       make(map[int32]ReplicationTLSServerMaterial, len(members)),
	}
	for _, member := range members {
		server, err := generateServingCertificateWithValidity(now, random, authority, memberDNSNames[member], staticServerValidity)
		if err != nil {
			return nil, fmt.Errorf("issue replication server certificate for member %d: %w", member, err)
		}
		bundle.Servers[member] = ReplicationTLSServerMaterial{
			Certificate: slices.Clone(server.certificatePEM),
			PrivateKey:  slices.Clone(server.privateKeyPEM),
			NotAfter:    server.certificate.NotAfter,
		}
	}
	return bundle, nil
}

// ValidateReplicationTLSCA verifies the self-signed replication CA certificate
// and its remaining lifetime. The CA private key is deliberately discarded
// after issuance and is not required.
func ValidateReplicationTLSCA(caCertificate []byte, now time.Time) (time.Time, error) {
	authority, err := parseCertificateAuthorityCertificate(caCertificate, now, staticRenewBefore)
	if err != nil {
		return time.Time{}, err
	}
	return authority.certificate.NotAfter, nil
}

// ValidateReplicationTLSServer verifies one member's server keypair: key
// pairing, a server-auth chain to the exact replication CA, exactly one exact
// DNS SAN, and sufficient remaining lifetime.
func ValidateReplicationTLSServer(caCertificate, serverCertificate, serverPrivateKey []byte, dnsNames []string, now time.Time) (time.Time, error) {
	if len(dnsNames) != 1 || strings.TrimSpace(dnsNames[0]) == "" {
		return time.Time{}, fmt.Errorf("replication server certificates require exactly one non-empty DNS name")
	}
	authority, err := parseCertificateAuthorityCertificate(caCertificate, now, staticRenewBefore)
	if err != nil {
		return time.Time{}, err
	}
	server, err := parseServingCertificate(serverCertificate, serverPrivateKey)
	if err != nil {
		return time.Time{}, err
	}
	if len(server.certificate.DNSNames) != 1 {
		return time.Time{}, fmt.Errorf("replication server certificate must carry exactly one DNS name")
	}
	if !servingCertificateIsUsable(server, authority, dnsNames, now) {
		return time.Time{}, fmt.Errorf("replication server certificate is not valid for the exact configured DNS name and CA")
	}
	if !server.certificate.NotAfter.After(now.Add(staticRenewBefore)) {
		return time.Time{}, fmt.Errorf("replication server certificate expires too soon at %s; zero-downtime certificate rotation is not implemented", server.certificate.NotAfter.UTC().Format(time.RFC3339))
	}
	return server.certificate.NotAfter, nil
}

func parseServingCertificate(certificatePEM, privateKeyPEM []byte) (*servingCertificate, error) {
	if _, err := tls.X509KeyPair(certificatePEM, privateKeyPEM); err != nil {
		return nil, fmt.Errorf("serving certificate and private key do not form a key pair: %w", err)
	}
	certificate, err := parseCertificate(certificatePEM)
	if err != nil {
		return nil, fmt.Errorf("parse serving certificate: %w", err)
	}
	if certificate.IsCA {
		return nil, fmt.Errorf("serving certificate is a CA")
	}
	if _, err := parseECDSAPrivateKey(privateKeyPEM); err != nil {
		return nil, fmt.Errorf("parse serving private key: %w", err)
	}
	return &servingCertificate{
		certificate:    certificate,
		certificatePEM: slices.Clone(certificatePEM),
		privateKeyPEM:  slices.Clone(privateKeyPEM),
	}, nil
}

func servingCertificateNeedsRenewal(serving *servingCertificate, authority *certificateAuthority, dnsNames []string, now time.Time) bool {
	return !servingCertificateIsUsable(serving, authority, dnsNames, now) || !serving.certificate.NotAfter.After(now.Add(renewBefore))
}

func servingCertificateIsUsable(serving *servingCertificate, authority *certificateAuthority, dnsNames []string, now time.Time) bool {
	names := normalizedDNSNames(dnsNames)
	if len(names) == 0 {
		return false
	}
	roots := x509.NewCertPool()
	roots.AddCert(authority.certificate)
	if _, err := serving.certificate.Verify(x509.VerifyOptions{
		DNSName:     names[0],
		Roots:       roots,
		CurrentTime: now,
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		return false
	}
	return slices.Equal(normalizedDNSNames(serving.certificate.DNSNames), names)
}

func parseCertificate(contents []byte) (*x509.Certificate, error) {
	block, rest := pem.Decode(contents)
	if block == nil || block.Type != "CERTIFICATE" || len(strings.TrimSpace(string(rest))) != 0 {
		return nil, fmt.Errorf("expected exactly one CERTIFICATE PEM block")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, err
	}
	return certificate, nil
}

func parseECDSAPrivateKey(contents []byte) (*ecdsa.PrivateKey, error) {
	block, rest := pem.Decode(contents)
	if block == nil || block.Type != "PRIVATE KEY" || len(strings.TrimSpace(string(rest))) != 0 {
		return nil, fmt.Errorf("expected exactly one PRIVATE KEY PEM block")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	privateKey, ok := parsed.(*ecdsa.PrivateKey)
	if !ok || privateKey.Curve.Params().Name != elliptic.P256().Params().Name {
		return nil, fmt.Errorf("expected an ECDSA P-256 private key")
	}
	return privateKey, nil
}

func randomSerial(random io.Reader) (*big.Int, error) {
	contents := make([]byte, 16)
	if _, err := io.ReadFull(random, contents); err != nil {
		return nil, fmt.Errorf("generate certificate serial number: %w", err)
	}
	contents[0] &= 0x7f
	contents[len(contents)-1] |= 1
	return new(big.Int).SetBytes(contents), nil
}

func normalizedDNSNames(names []string) []string {
	result := slices.Clone(names)
	sort.Strings(result)
	return slices.Compact(result)
}
