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

	caValidity      = 10 * 365 * 24 * time.Hour
	leafValidity    = 90 * 24 * time.Hour
	renewBefore     = 30 * 24 * time.Hour
	certificateSkew = 5 * time.Minute
)

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
	if _, err := tls.X509KeyPair(certificatePEM, privateKeyPEM); err != nil {
		return nil, fmt.Errorf("CA certificate and private key do not form a key pair: %w", err)
	}
	certificate, err := parseCertificate(certificatePEM)
	if err != nil {
		return nil, fmt.Errorf("parse CA certificate: %w", err)
	}
	privateKey, err := parseECDSAPrivateKey(privateKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("parse CA private key: %w", err)
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
	if !certificate.NotAfter.After(now.Add(leafValidity)) {
		return nil, fmt.Errorf("CA certificate expires too soon at %s; automated CA rotation is not implemented", certificate.NotAfter.UTC().Format(time.RFC3339))
	}
	return &certificateAuthority{
		certificate:    certificate,
		privateKey:     privateKey,
		certificatePEM: slices.Clone(certificatePEM),
		privateKeyPEM:  slices.Clone(privateKeyPEM),
	}, nil
}

func generateServingCertificate(now time.Time, random io.Reader, authority *certificateAuthority, dnsNames []string) (*servingCertificate, error) {
	names := normalizedDNSNames(dnsNames)
	if len(names) == 0 {
		return nil, fmt.Errorf("at least one serving DNS name is required")
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
		NotAfter:              now.Add(leafValidity),
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
