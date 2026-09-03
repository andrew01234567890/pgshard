// Package pki issues the certificates pgshard's internal gRPC uses.
//
// It exists because verifying that a peer chains to a CA is not the same
// as knowing who the peer is. Every internal listener required a client
// certificate and checked only the chain, while one operator-supplied
// secret served the agent, the router and the pooler alike -- so a single
// certificate spanned three trust boundaries and any postgres pod held one
// every pooler and router would accept. Certificates have to differ per
// workload before a listener can authorise anything, and that means
// something has to issue them.
package pki

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"strings"
	"time"
)

// Scheme is the URI SAN scheme an internal identity carries. A URI SAN
// rather than a DNS name or a CN: it is the field meant for naming a
// workload, nothing resolves it, and it cannot be confused with a host a
// certificate is also valid to serve.
const Scheme = "pgshard"

// Identity names one workload. Member is empty for a role that is not
// per-pod, so a router's identity is the same on every router replica
// while an agent's names the member it runs beside.
type Identity struct {
	Namespace string
	Cluster   string
	Role      string
	Member    string
}

// Roles. Authorisation is written against these, so they are constants
// rather than free text: a listener that allowed "pooler " would allow
// nothing and look like it allowed something.
const (
	RoleAgent      = "agent"
	RoleRouter     = "router"
	RolePooler     = "pooler"
	RoleController = "controller"
	RoleOperator   = "operator"
	RoleAdmin      = "admin"
)

// URI renders the identity as it appears in a certificate.
func (i Identity) URI() *url.URL {
	p := "/" + i.Cluster + "/" + i.Role
	if i.Member != "" {
		p += "/" + i.Member
	}
	return &url.URL{Scheme: Scheme, Host: i.Namespace, Path: p}
}

func (i Identity) String() string { return i.URI().String() }

// ParseIdentity reads an identity back out of a URI SAN. It is strict
// about shape: an unparseable or foreign URI is not an identity with
// missing parts, it is not one of ours at all.
func ParseIdentity(raw string) (Identity, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return Identity{}, err
	}
	if u.Scheme != Scheme {
		return Identity{}, fmt.Errorf("not a pgshard identity: %q", raw)
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if u.Host == "" || len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return Identity{}, fmt.Errorf("malformed pgshard identity: %q", raw)
	}
	id := Identity{Namespace: u.Host, Cluster: parts[0], Role: parts[1]}
	if len(parts) > 2 {
		id.Member = strings.Join(parts[2:], "/")
	}
	return id, nil
}

// Material is a PEM-encoded certificate and its private key.
type Material struct {
	CertPEM []byte
	KeyPEM  []byte
}

// CA is an issuing authority: its own certificate and the key that signs.
type CA struct {
	Material
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

// CALifetime is how long a freshly minted CA is valid for. Long enough
// that rotating it is planned rather than an emergency, short enough that
// it is not effectively permanent.
const CALifetime = 5 * 365 * 24 * time.Hour

// LeafLifetime is how long an issued workload certificate lasts. Short,
// because the operator reissues them and a short life is what makes a
// leaked one stop mattering.
const LeafLifetime = 90 * 24 * time.Hour

// NewCA mints a self-signed authority named for the cluster.
func NewCA(cluster string, now time.Time) (*CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serial, err := serialNumber()
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "pgshard internal CA: " + cluster},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(CALifetime),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            0,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	m, err := encode(der, key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &CA{Material: m, cert: cert, key: key}, nil
}

// LoadCA reads an authority back from the PEM the operator stored.
func LoadCA(m Material) (*CA, error) {
	cert, err := parseCert(m.CertPEM)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(m.KeyPEM)
	if block == nil {
		return nil, errors.New("no PEM block in the CA key")
	}
	k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	key, ok := k.(*ecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("CA key is %T, want an ECDSA key", k)
	}
	if !cert.IsCA {
		return nil, errors.New("the stored CA certificate is not a CA")
	}
	return &CA{Material: m, cert: cert, key: key}, nil
}

// NotAfter is when the authority expires.
func (ca *CA) NotAfter() time.Time { return ca.cert.NotAfter }

// Request is one certificate to issue.
type Request struct {
	Identity Identity
	// DNSNames are the names a server certificate is valid to serve. A
	// client-only certificate carries none.
	DNSNames []string
	// Server and Client say what the certificate may be used for. Both are
	// legitimate together -- a router dials poolers and serves its peers --
	// but neither being set is a certificate that can do nothing, which is
	// a mistake rather than a choice.
	Server bool
	Client bool
}

// Issue signs a leaf certificate for req.
func (ca *CA) Issue(req Request, now time.Time) (Material, error) {
	if !req.Server && !req.Client {
		return Material{}, errors.New("a certificate must be usable as a client, a server, or both")
	}
	if req.Identity.Namespace == "" || req.Identity.Cluster == "" || req.Identity.Role == "" {
		return Material{}, fmt.Errorf("incomplete identity: %+v", req.Identity)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return Material{}, err
	}
	serial, err := serialNumber()
	if err != nil {
		return Material{}, err
	}
	var eku []x509.ExtKeyUsage
	if req.Server {
		eku = append(eku, x509.ExtKeyUsageServerAuth)
	}
	if req.Client {
		eku = append(eku, x509.ExtKeyUsageClientAuth)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: req.Identity.String()},
		URIs:                  []*url.URL{req.Identity.URI()},
		DNSNames:              req.DNSNames,
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(LeafLifetime),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           eku,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		return Material{}, err
	}
	return encode(der, key)
}

// IdentityOf reads the identity out of a PEM certificate.
func IdentityOf(certPEM []byte) (Identity, error) {
	cert, err := parseCert(certPEM)
	if err != nil {
		return Identity{}, err
	}
	return IdentityOfCert(cert)
}

// IdentityOfCert reads the identity out of a parsed certificate. Exactly
// one pgshard URI is required: none is an unidentified peer, and several
// would leave which one authorises it up to the reader.
func IdentityOfCert(cert *x509.Certificate) (Identity, error) {
	var found []Identity
	for _, u := range cert.URIs {
		if u.Scheme != Scheme {
			continue
		}
		id, err := ParseIdentity(u.String())
		if err != nil {
			return Identity{}, err
		}
		found = append(found, id)
	}
	switch len(found) {
	case 1:
		return found[0], nil
	case 0:
		return Identity{}, errors.New("the certificate carries no pgshard identity")
	default:
		return Identity{}, fmt.Errorf("the certificate carries %d pgshard identities", len(found))
	}
}

// NeedsRenewal reports whether a certificate is close enough to expiry to
// reissue. Renewing at a third of the remaining life leaves room for a
// rollout to finish, and for it to fail and be retried, before anything
// stops working.
func NeedsRenewal(certPEM []byte, now time.Time) bool {
	cert, err := parseCert(certPEM)
	if err != nil {
		return true
	}
	life := cert.NotAfter.Sub(cert.NotBefore)
	return !now.Before(cert.NotAfter.Add(-life / 3))
}

func parseCert(certPEM []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return nil, errors.New("no PEM block in the certificate")
	}
	return x509.ParseCertificate(block.Bytes)
}

func encode(der []byte, key *ecdsa.PrivateKey) (Material, error) {
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return Material{}, err
	}
	return Material{
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	}, nil
}

func serialNumber() (*big.Int, error) {
	return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
}

// callers is who may call each listener. It is a table rather than
// configuration because it is a property of the system, not of a
// deployment: the router is the only thing that calls a pooler, and a
// cluster where that is untrue is a cluster with a bug, not one with a
// different policy.
//
// The router's change-stream listener is deliberately absent. Its callers
// are the cluster's consumers, which hold no pgshard identity, so a rule
// there would refuse the traffic it exists to serve.
var callers = map[string][]string{
	RolePooler:     {RoleRouter},
	RoleAgent:      {RoleController, RoleOperator},
	RoleController: {RoleRouter, RoleOperator},
	RoleRouter:     {RoleRouter},
}

// AllowedCallers reports whether an identity may call a listener serving
// role, and whether role has a rule at all. A listener with no rule
// authorises nothing and keeps accepting whatever chains to the CA.
func AllowedCallers(role string) (func(Identity) bool, bool) {
	allowed, ok := callers[role]
	if !ok {
		return nil, false
	}
	set := make(map[string]bool, len(allowed))
	for _, a := range allowed {
		set[a] = true
	}
	return func(id Identity) bool { return set[id.Role] }, true
}
