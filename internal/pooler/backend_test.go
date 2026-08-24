package pooler

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// scriptedPG answers the startup exchange with a fixed script: every step
// receives the client's message and picks the backend's reply.
func scriptedPG(t *testing.T, l net.Listener, wrap func(net.Conn) net.Conn, script func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) bool) {
	t.Helper()
	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		if wrap != nil {
			conn = wrap(conn)
		}
		be := pgproto3.NewBackend(bufio.NewReader(conn), conn)
		msg, err := be.ReceiveStartupMessage()
		if err != nil {
			return
		}
		for script(be, msg) {
			_ = be.Flush()
			if msg, err = be.Receive(); err != nil {
				return
			}
		}
		_ = be.Flush()
	}()
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func dialWith(t *testing.T, d Dialer) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := dialBackend(ctx, d, "app", "alice", testKey(0x11), testKey(0x22))
	return err
}

func TestBackendRefusesAuthenticationOkWithoutSASL(t *testing.T) {
	l := listen(t)
	scriptedPG(t, l, nil, func(be *pgproto3.Backend, _ pgproto3.FrontendMessage) bool {
		be.Send(&pgproto3.AuthenticationOk{})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		return false
	})
	err := dialWith(t, Dialer{Address: l.Addr().String(), Timeout: time.Second})
	if err == nil || !strings.Contains(err.Error(), "without a verified SCRAM-SHA-256 exchange") {
		t.Fatalf("trust-style AuthenticationOk must be refused: %v", err)
	}
}

func TestBackendRefusesAuthenticationOkBeforeServerSignature(t *testing.T) {
	l := listen(t)
	scriptedPG(t, l, nil, func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) bool {
		switch m := msg.(type) {
		case *pgproto3.StartupMessage:
			be.Send(&pgproto3.AuthenticationSASL{AuthMechanisms: []string{"SCRAM-SHA-256"}})
			_ = be.SetAuthType(pgproto3.AuthTypeSASL)
			return true
		case *pgproto3.SASLInitialResponse:
			_, nonce, _ := strings.Cut(string(m.Data), ",r=")
			salt := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))
			be.Send(&pgproto3.AuthenticationSASLContinue{Data: []byte("r=" + nonce + "serverpart,s=" + salt + ",i=4096")})
			_ = be.SetAuthType(pgproto3.AuthTypeSASLContinue)
			return true
		case *pgproto3.SASLResponse:
			be.Send(&pgproto3.AuthenticationOk{})
			be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			return false
		}
		return false
	})
	err := dialWith(t, Dialer{Address: l.Addr().String(), Timeout: time.Second})
	if err == nil || !strings.Contains(err.Error(), "without a verified SCRAM-SHA-256 exchange") {
		t.Fatalf("AuthenticationOk without SASLFinal must be refused: %v", err)
	}
}

func TestBackendRefusesForgedServerSignature(t *testing.T) {
	l := listen(t)
	scriptedPG(t, l, nil, func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) bool {
		switch m := msg.(type) {
		case *pgproto3.StartupMessage:
			be.Send(&pgproto3.AuthenticationSASL{AuthMechanisms: []string{"SCRAM-SHA-256"}})
			_ = be.SetAuthType(pgproto3.AuthTypeSASL)
			return true
		case *pgproto3.SASLInitialResponse:
			_, nonce, _ := strings.Cut(string(m.Data), ",r=")
			salt := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))
			be.Send(&pgproto3.AuthenticationSASLContinue{Data: []byte("r=" + nonce + "serverpart,s=" + salt + ",i=4096")})
			_ = be.SetAuthType(pgproto3.AuthTypeSASLContinue)
			return true
		case *pgproto3.SASLResponse:
			be.Send(&pgproto3.AuthenticationSASLFinal{Data: []byte("v=" + base64.StdEncoding.EncodeToString(make([]byte, 32)))})
			be.Send(&pgproto3.AuthenticationOk{})
			be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			return false
		}
		return false
	})
	err := dialWith(t, Dialer{Address: l.Addr().String(), Timeout: time.Second})
	if err == nil || !strings.Contains(err.Error(), "server signature mismatch") {
		t.Fatalf("forged server signature must be refused: %v", err)
	}
}

func TestBackendDialRefusesWhenTLSIsDeclined(t *testing.T) {
	l := listen(t)
	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		be := pgproto3.NewBackend(bufio.NewReader(conn), conn)
		if _, err := be.ReceiveStartupMessage(); err != nil {
			return
		}
		_, _ = conn.Write([]byte{'N'})
	}()
	err := dialWith(t, Dialer{Address: l.Addr().String(), Timeout: time.Second, TLS: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}}) //nolint:gosec // test
	if err == nil || !strings.Contains(err.Error(), "declined TLS") {
		t.Fatalf("declined SSLRequest must fail the dial: %v", err)
	}
}

func TestBackendDialUpgradesToTLS(t *testing.T) {
	l := listen(t)
	serverTLS := selfSignedTLS(t)
	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		be := pgproto3.NewBackend(bufio.NewReader(conn), conn)
		if _, err := be.ReceiveStartupMessage(); err != nil {
			return
		}
		if _, err := conn.Write([]byte{'S'}); err != nil {
			return
		}
		tc := tls.Server(conn, serverTLS)
		if err := tc.Handshake(); err != nil {
			return
		}
		be = pgproto3.NewBackend(bufio.NewReader(tc), tc)
		if _, err := be.ReceiveStartupMessage(); err != nil {
			return
		}
		be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "28000", Message: "over tls"})
		_ = be.Flush()
	}()
	err := dialWith(t, Dialer{Address: l.Addr().String(), Timeout: time.Second, TLS: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}}) //nolint:gosec // test
	if err == nil || !strings.Contains(err.Error(), "over tls") {
		t.Fatalf("startup must run over the TLS session: %v", err)
	}
}

func selfSignedTLS(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "pooler-test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, MinVersion: tls.VersionTLS12}
}
