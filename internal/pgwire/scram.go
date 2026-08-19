package pgwire

import (
	"bytes"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// DefaultSCRAMIterations matches PostgreSQL's scram_iterations default.
const DefaultSCRAMIterations = 4096

const scramNonceLen = 18

// SCRAMVerifier is the parsed form of a PostgreSQL SCRAM-SHA-256 secret:
// "SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>".
type SCRAMVerifier struct {
	Iterations int
	Salt       []byte
	StoredKey  []byte
	ServerKey  []byte
}

// ParseSCRAMVerifier parses a stored verifier string.
func ParseSCRAMVerifier(s string) (*SCRAMVerifier, error) {
	const prefix = "SCRAM-SHA-256$"
	if !strings.HasPrefix(s, prefix) {
		return nil, errors.New("scram: verifier does not start with SCRAM-SHA-256$")
	}
	parts := strings.Split(s[len(prefix):], "$")
	if len(parts) != 2 {
		return nil, errors.New("scram: malformed verifier")
	}
	iterSalt := strings.SplitN(parts[0], ":", 2)
	keys := strings.SplitN(parts[1], ":", 2)
	if len(iterSalt) != 2 || len(keys) != 2 {
		return nil, errors.New("scram: malformed verifier")
	}
	iter, err := strconv.Atoi(iterSalt[0])
	if err != nil || iter <= 0 {
		return nil, errors.New("scram: bad iteration count")
	}
	salt, err := base64.StdEncoding.DecodeString(iterSalt[1])
	if err != nil {
		return nil, fmt.Errorf("scram: bad salt: %w", err)
	}
	stored, err := base64.StdEncoding.DecodeString(keys[0])
	if err != nil || len(stored) != sha256.Size {
		return nil, errors.New("scram: bad StoredKey")
	}
	server, err := base64.StdEncoding.DecodeString(keys[1])
	if err != nil || len(server) != sha256.Size {
		return nil, errors.New("scram: bad ServerKey")
	}
	return &SCRAMVerifier{Iterations: iter, Salt: salt, StoredKey: stored, ServerKey: server}, nil
}

// String encodes the verifier in PostgreSQL's pg_authid format.
func (v *SCRAMVerifier) String() string {
	e := base64.StdEncoding.EncodeToString
	return fmt.Sprintf("SCRAM-SHA-256$%d:%s$%s:%s", v.Iterations, e(v.Salt), e(v.StoredKey), e(v.ServerKey))
}

// BuildSCRAMVerifier derives a verifier from a password the same way
// PostgreSQL does (PBKDF2-HMAC-SHA-256 over the SASLprep'd password). The
// password is used as-is; callers needing SASLprep normalisation must apply
// it first. A nil salt draws a random 16-byte one.
func BuildSCRAMVerifier(password string, salt []byte, iterations int) (*SCRAMVerifier, error) {
	if iterations <= 0 {
		iterations = DefaultSCRAMIterations
	}
	if salt == nil {
		salt = make([]byte, 16)
		if _, err := rand.Read(salt); err != nil {
			return nil, err
		}
	}
	salted, err := pbkdf2.Key(sha256.New, password, salt, iterations, sha256.Size)
	if err != nil {
		return nil, err
	}
	clientKey := hmacSHA256(salted, []byte("Client Key"))
	stored := sha256.Sum256(clientKey)
	return &SCRAMVerifier{
		Iterations: iterations,
		Salt:       salt,
		StoredKey:  stored[:],
		ServerKey:  hmacSHA256(salted, []byte("Server Key")),
	}, nil
}

func hmacSHA256(key, msg []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(msg)
	return m.Sum(nil)
}

// SCRAMKeys are the client and server keys recovered from a successful
// exchange. They let a proxy authenticate onward without the password.
type SCRAMKeys struct {
	ClientKey []byte
	ServerKey []byte
}

// scramServer drives one SCRAM-SHA-256 exchange (RFC 5802 / RFC 7677) as the
// server. Only the "n" and "y" channel-binding flags are accepted because
// SCRAM-SHA-256-PLUS is not advertised.
type scramServer struct {
	verifier    *SCRAMVerifier
	clientFirst string // client-first-message-bare
	serverFirst string
	nonce       string
	keys        *SCRAMKeys
	// serverNonce overrides the random nonce; tests use it for fixed vectors.
	serverNonce string
}

func newSCRAMServer(v *SCRAMVerifier) *scramServer { return &scramServer{verifier: v} }

func scramErr(format string, args ...any) error {
	return Errorf(CodeInvalidAuthorization, "SCRAM: "+format, args...)
}

// handleClientFirst validates the client-first-message and returns the
// server-first-message.
func (s *scramServer) handleClientFirst(msg []byte) ([]byte, error) {
	m := string(msg)
	// gs2-header: cbind-flag "," [authzid] ","
	var bare string
	switch {
	case strings.HasPrefix(m, "n,"):
		bare = m[2:]
	case strings.HasPrefix(m, "y,"):
		bare = m[2:]
	case strings.HasPrefix(m, "p="):
		return nil, scramErr("channel binding is not supported by this server")
	default:
		return nil, scramErr("malformed client-first-message: bad channel binding flag")
	}
	comma := strings.IndexByte(bare, ',')
	if comma < 0 {
		return nil, scramErr("malformed client-first-message: missing authzid separator")
	}
	if comma != 0 {
		return nil, scramErr("client uses authorization identity, but it is not supported")
	}
	bare = bare[1:]
	s.clientFirst = bare
	attrs, err := parseSCRAMAttrs(bare)
	if err != nil {
		return nil, err
	}
	if _, reserved := attrs["m"]; reserved {
		return nil, scramErr("client requires an unsupported extension")
	}
	if _, ok := attrs["n"]; !ok {
		return nil, scramErr("malformed client-first-message: missing username")
	}
	clientNonce, ok := attrs["r"]
	if !ok || clientNonce == "" || !isPrintableSCRAM(clientNonce) {
		return nil, scramErr("malformed client-first-message: bad nonce")
	}
	serverNonce := s.serverNonce
	if serverNonce == "" {
		raw := make([]byte, scramNonceLen)
		if _, err := rand.Read(raw); err != nil {
			return nil, err
		}
		serverNonce = base64.StdEncoding.EncodeToString(raw)
	}
	s.nonce = clientNonce + serverNonce
	s.serverFirst = fmt.Sprintf("r=%s,s=%s,i=%d", s.nonce,
		base64.StdEncoding.EncodeToString(s.verifier.Salt), s.verifier.Iterations)
	return []byte(s.serverFirst), nil
}

// handleClientFinal verifies the proof and returns the server-final-message.
func (s *scramServer) handleClientFinal(msg []byte) ([]byte, error) {
	m := string(msg)
	proofIdx := strings.LastIndex(m, ",p=")
	if proofIdx < 0 {
		return nil, scramErr("malformed client-final-message: missing proof")
	}
	withoutProof := m[:proofIdx]
	attrs, err := parseSCRAMAttrs(m)
	if err != nil {
		return nil, err
	}
	switch attrs["c"] {
	case base64.StdEncoding.EncodeToString([]byte("n,,")), base64.StdEncoding.EncodeToString([]byte("y,,")):
	default:
		return nil, scramErr("unexpected channel-binding data in client-final-message")
	}
	if attrs["r"] != s.nonce {
		return nil, scramErr("nonce mismatch")
	}
	proof, err := base64.StdEncoding.DecodeString(attrs["p"])
	if err != nil || len(proof) != sha256.Size {
		return nil, scramErr("malformed proof")
	}
	authMessage := []byte(s.clientFirst + "," + s.serverFirst + "," + withoutProof)
	clientSig := hmacSHA256(s.verifier.StoredKey, authMessage)
	clientKey := make([]byte, sha256.Size)
	for i := range clientKey {
		clientKey[i] = proof[i] ^ clientSig[i]
	}
	stored := sha256.Sum256(clientKey)
	if subtle.ConstantTimeCompare(stored[:], s.verifier.StoredKey) != 1 {
		return nil, Errorf(CodeInvalidPassword, "password authentication failed")
	}
	s.keys = &SCRAMKeys{ClientKey: clientKey, ServerKey: bytes.Clone(s.verifier.ServerKey)}
	serverSig := hmacSHA256(s.verifier.ServerKey, authMessage)
	return []byte("v=" + base64.StdEncoding.EncodeToString(serverSig)), nil
}

func parseSCRAMAttrs(s string) (map[string]string, error) {
	out := map[string]string{}
	for _, part := range strings.Split(s, ",") {
		if len(part) < 2 || part[1] != '=' {
			return nil, scramErr("malformed attribute %q", part)
		}
		out[part[:1]] = part[2:]
	}
	return out, nil
}

func isPrintableSCRAM(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x21 || s[i] > 0x7e || s[i] == ',' {
			return false
		}
	}
	return true
}
