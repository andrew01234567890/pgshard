package pooler

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

// scramClient authenticates with a ClientKey/ServerKey pair instead of a
// password: proof = ClientKey XOR HMAC(H(ClientKey), authMessage), and the
// server signature is checked with ServerKey. Only "n,," channel binding.
type scramClient struct {
	user        string
	clientKey   []byte
	serverKey   []byte
	nonce       string
	clientFirst string
	authMessage []byte
}

func newSCRAMClient(user string, clientKey, serverKey []byte) (*scramClient, error) {
	if len(clientKey) != sha256.Size || len(serverKey) != sha256.Size {
		return nil, errors.New("scram: keys must be 32 bytes")
	}
	raw := make([]byte, 18)
	if _, err := rand.Read(raw); err != nil {
		return nil, err
	}
	c := &scramClient{user: user, clientKey: clientKey, serverKey: serverKey,
		nonce: base64.StdEncoding.EncodeToString(raw)}
	c.clientFirst = "n=" + escapeSCRAMName(user) + ",r=" + c.nonce
	return c, nil
}

func escapeSCRAMName(s string) string {
	return strings.NewReplacer("=", "=3D", ",", "=2C").Replace(s)
}

func (c *scramClient) clientFirstMessage() []byte { return []byte("n,," + c.clientFirst) }

func (c *scramClient) clientFinalMessage(serverFirst []byte) ([]byte, error) {
	attrs := map[string]string{}
	for _, part := range strings.Split(string(serverFirst), ",") {
		if len(part) < 2 || part[1] != '=' {
			return nil, fmt.Errorf("scram: malformed server-first attribute %q", part)
		}
		attrs[part[:1]] = part[2:]
	}
	if _, ok := attrs["m"]; ok {
		return nil, errors.New("scram: server requires an unsupported extension")
	}
	if !strings.HasPrefix(attrs["r"], c.nonce) || len(attrs["r"]) == len(c.nonce) {
		return nil, errors.New("scram: server nonce does not extend the client nonce")
	}
	withoutProof := "c=biws,r=" + attrs["r"]
	c.authMessage = []byte(c.clientFirst + "," + string(serverFirst) + "," + withoutProof)
	stored := sha256.Sum256(c.clientKey)
	sig := hmacSHA256(stored[:], c.authMessage)
	proof := make([]byte, sha256.Size)
	for i := range proof {
		proof[i] = c.clientKey[i] ^ sig[i]
	}
	return []byte(withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof)), nil
}

func (c *scramClient) verifyServerFinal(serverFinal []byte) error {
	m := string(serverFinal)
	if strings.HasPrefix(m, "e=") {
		return fmt.Errorf("scram: server error: %s", m[2:])
	}
	if !strings.HasPrefix(m, "v=") {
		return errors.New("scram: malformed server-final-message")
	}
	got, err := base64.StdEncoding.DecodeString(m[2:])
	if err != nil {
		return errors.New("scram: malformed server signature")
	}
	if subtle.ConstantTimeCompare(got, hmacSHA256(c.serverKey, c.authMessage)) != 1 {
		return errors.New("scram: server signature mismatch")
	}
	return nil
}

func hmacSHA256(key, msg []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(msg)
	return m.Sum(nil)
}
