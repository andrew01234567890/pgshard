package pgwire

import (
	"context"
	"crypto/md5" //nolint:gosec // MD5 is mandated by the PostgreSQL protocol.
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
)

// AuthResult is what an Authenticator returns on success. SCRAM is populated
// only after a SCRAM exchange and must never be logged.
type AuthResult struct {
	SCRAM *SCRAMKeys
}

// AuthExchange lets an Authenticator talk to the client during startup.
type AuthExchange interface {
	// Request sends an authentication request and returns the client's
	// reply. authType is the pgproto3 AuthType* constant matching msg so the
	// reply can be decoded.
	Request(msg pgproto3.BackendMessage, authType uint32) (pgproto3.FrontendMessage, error)
}

// Authenticator authenticates a session before it is granted an Executor.
// A returned *Error is relayed as-is; any other error becomes 28000.
type Authenticator interface {
	Authenticate(ctx context.Context, startup map[string]string, ex AuthExchange) (*AuthResult, error)
}

// TrustAuthenticator accepts every connection.
type TrustAuthenticator struct{}

// Authenticate implements Authenticator.
func (TrustAuthenticator) Authenticate(context.Context, map[string]string, AuthExchange) (*AuthResult, error) {
	return &AuthResult{}, nil
}

// PasswordLookup returns the stored secret for a user. For cleartext and MD5
// it may be the plain password or an "md5<hex>" hash; for SCRAM it must be a
// SCRAM-SHA-256 verifier string. Any error fails
// authentication.
type PasswordLookup func(ctx context.Context, user string) (string, error)

func authFailed() error {
	return Errorf(CodeInvalidPassword, "password authentication failed")
}

// CleartextAuthenticator requests the password in the clear (only sensible
// over TLS) and compares it with the stored secret.
type CleartextAuthenticator struct{ Lookup PasswordLookup }

// Authenticate implements Authenticator.
func (a CleartextAuthenticator) Authenticate(ctx context.Context, startup map[string]string, ex AuthExchange) (*AuthResult, error) {
	user := startup["user"]
	reply, err := ex.Request(&pgproto3.AuthenticationCleartextPassword{}, pgproto3.AuthTypeCleartextPassword)
	if err != nil {
		return nil, err
	}
	pw, ok := reply.(*pgproto3.PasswordMessage)
	if !ok {
		return nil, Errorf(CodeProtocolViolation, "expected password response, got %T", reply)
	}
	secret, err := a.Lookup(ctx, user)
	if err != nil {
		return nil, authFailed()
	}
	if strings.HasPrefix(secret, "md5") {
		if subtle.ConstantTimeCompare([]byte(md5Hex(pw.Password+user)), []byte(secret[3:])) != 1 {
			return nil, authFailed()
		}
		return &AuthResult{}, nil
	}
	if subtle.ConstantTimeCompare([]byte(pw.Password), []byte(secret)) != 1 {
		return nil, authFailed()
	}
	return &AuthResult{}, nil
}

// MD5Authenticator runs the salted MD5 challenge.
type MD5Authenticator struct{ Lookup PasswordLookup }

// Authenticate implements Authenticator.
func (a MD5Authenticator) Authenticate(ctx context.Context, startup map[string]string, ex AuthExchange) (*AuthResult, error) {
	user := startup["user"]
	var salt [4]byte
	if _, err := rand.Read(salt[:]); err != nil {
		return nil, err
	}
	reply, err := ex.Request(&pgproto3.AuthenticationMD5Password{Salt: salt}, pgproto3.AuthTypeMD5Password)
	if err != nil {
		return nil, err
	}
	pw, ok := reply.(*pgproto3.PasswordMessage)
	if !ok {
		return nil, Errorf(CodeProtocolViolation, "expected password response, got %T", reply)
	}
	secret, err := a.Lookup(ctx, user)
	if err != nil {
		return nil, authFailed()
	}
	inner := secret
	if !strings.HasPrefix(secret, "md5") {
		inner = "md5" + md5Hex(secret+user)
	}
	want := "md5" + md5Hex(inner[3:]+string(salt[:]))
	if subtle.ConstantTimeCompare([]byte(pw.Password), []byte(want)) != 1 {
		return nil, authFailed()
	}
	return &AuthResult{}, nil
}

func md5Hex(s string) string {
	sum := md5.Sum([]byte(s)) //nolint:gosec // protocol-mandated
	return hex.EncodeToString(sum[:])
}

// SCRAMAuthenticator runs SCRAM-SHA-256 from a stored verifier. Only the
// plain mechanism is advertised; -PLUS, GSS and ident requests are refused.
type SCRAMAuthenticator struct{ Lookup PasswordLookup }

// Authenticate implements Authenticator.
func (a SCRAMAuthenticator) Authenticate(ctx context.Context, startup map[string]string, ex AuthExchange) (*AuthResult, error) {
	user := startup["user"]
	reply, err := ex.Request(&pgproto3.AuthenticationSASL{AuthMechanisms: []string{"SCRAM-SHA-256"}}, pgproto3.AuthTypeSASL)
	if err != nil {
		return nil, err
	}
	initial, ok := reply.(*pgproto3.SASLInitialResponse)
	if !ok {
		return nil, Errorf(CodeProtocolViolation, "expected SASLInitialResponse, got %T", reply)
	}
	switch initial.AuthMechanism {
	case "SCRAM-SHA-256":
	case "SCRAM-SHA-256-PLUS":
		return nil, Errorf(CodeInvalidAuthorization, "SCRAM-SHA-256-PLUS (channel binding) is not supported by this server")
	default:
		return nil, Errorf(CodeInvalidAuthorization, "client selected an invalid SASL authentication mechanism %q", initial.AuthMechanism)
	}
	secret, err := a.Lookup(ctx, user)
	verifier, perr := ParseSCRAMVerifier(secret)
	if err != nil || perr != nil {
		// Run a throwaway exchange so a missing user is indistinguishable
		// from a wrong password, as PostgreSQL does with a mock verifier.
		verifier, err = BuildSCRAMVerifier(user, nil, DefaultSCRAMIterations)
		if err != nil {
			return nil, err
		}
		verifier.StoredKey = make([]byte, len(verifier.StoredKey))
	}
	srv := newSCRAMServer(verifier)
	serverFirst, err := srv.handleClientFirst(initial.Data)
	if err != nil {
		return nil, err
	}
	reply, err = ex.Request(&pgproto3.AuthenticationSASLContinue{Data: serverFirst}, pgproto3.AuthTypeSASLContinue)
	if err != nil {
		return nil, err
	}
	final, ok := reply.(*pgproto3.SASLResponse)
	if !ok {
		return nil, Errorf(CodeProtocolViolation, "expected SASLResponse, got %T", reply)
	}
	serverFinal, err := srv.handleClientFinal(final.Data)
	if err != nil {
		return nil, err
	}
	// AuthenticationSASLFinal is sent without waiting for a reply.
	if _, err := ex.Request(&pgproto3.AuthenticationSASLFinal{Data: serverFinal}, pgproto3.AuthTypeSASLFinal); err != nil {
		return nil, err
	}
	return &AuthResult{SCRAM: srv.keys}, nil
}
