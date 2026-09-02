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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Listener returns the credentials an internal gRPC server listens with.
//
// It is deliberately fail-closed: without the three files, and without an
// explicit insecureDev, it returns an error rather than falling back to
// plaintext. A server that serves unauthenticated traffic because a flag
// was missing is the failure this shape prevents.
func Listener(certFile, keyFile, caFile string, insecureDev bool) (credentials.TransportCredentials, error) {
	if insecureDev {
		if certFile != "" || keyFile != "" || caFile != "" {
			return nil, errors.New("--insecure-dev cannot be combined with TLS flags")
		}
		return insecure.NewCredentials(), nil
	}
	if certFile == "" || keyFile == "" || caFile == "" {
		return nil, errors.New("--tls-cert, --tls-key and --tls-ca are required (or --insecure-dev)")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%s: no certificates found", caFile)
	}
	return credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: pool,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13}), nil
}

// Dialer returns the credentials an internal gRPC client dials with: it
// presents certFile/keyFile and verifies the server against caFile.
//
// The mirror of Listener, and fail-closed the same way, so a caller cannot
// end up in plaintext because a flag was forgotten. serverName is the name
// the server's certificate must carry; empty uses the dial address.
func Dialer(certFile, keyFile, caFile, serverName string, insecureDev bool) (credentials.TransportCredentials, error) {
	if insecureDev {
		if certFile != "" || keyFile != "" || caFile != "" {
			return nil, errors.New("insecure dialling cannot be combined with TLS material")
		}
		return insecure.NewCredentials(), nil
	}
	if certFile == "" || keyFile == "" || caFile == "" {
		return nil, errors.New("a client certificate, key and CA are all required (or insecure dialling)")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	pemBytes, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("%s: no certificates found", caFile)
	}
	return credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool,
		ServerName: serverName, MinVersion: tls.VersionTLS13}), nil
}
