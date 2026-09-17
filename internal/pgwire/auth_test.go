package pgwire

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestUnknownUserMockSaltIsStablePerUser(t *testing.T) {
	a := SCRAMAuthenticator{MockSecret: []byte("test-secret")}
	first := string(a.mockSalt("ghost"))
	if second := string(a.mockSalt("ghost")); first != second {
		t.Fatal("mock salt must be deterministic for one user")
	}
	if string(a.mockSalt("ghost")) == string(a.mockSalt("other")) {
		t.Fatal("mock salt must differ between users")
	}
	if string(SCRAMAuthenticator{MockSecret: []byte("x")}.mockSalt("ghost")) == string(a.mockSalt("ghost")) {
		t.Fatal("mock salt must depend on the server secret")
	}
	if len(a.mockSalt("ghost")) != 16 {
		t.Fatalf("salt length %d", len(a.mockSalt("ghost")))
	}
}

// TestACredentialStoreThatCannotBeReadIsNotAWrongPassword (PGS-926): a
// lookup that fails to look says nothing about the password it was asked
// about. Answering 28P01 told clients holding the right credentials that
// they were wrong, and every driver takes that as final -- so a connection
// that would have succeeded a second later was never retried, and a cluster
// moving to issued TLS lost writes in the window.
func TestACredentialStoreThatCannotBeReadIsNotAWrongPassword(t *testing.T) {
	scram, err := BuildSCRAMVerifier("s3cret", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var unavailable bool
	look := func(_ context.Context, user string) (string, error) {
		if unavailable {
			return "", fmt.Errorf("dialing the catalog: %w", ErrLookupUnavailable)
		}
		if user != "alice" {
			return "", errors.New("no such user")
		}
		return scram.String(), nil
	}
	for _, c := range []struct {
		name string
		auth Authenticator
		pw   string
	}{
		{"scram", SCRAMAuthenticator{Lookup: look}, "s3cret"},
		{"cleartext", CleartextAuthenticator{Lookup: func(_ context.Context, user string) (string, error) {
			if unavailable {
				return "", fmt.Errorf("dialing the catalog: %w", ErrLookupUnavailable)
			}
			if user != "alice" {
				return "", errors.New("no such user")
			}
			return "s3cret", nil
		}}, "s3cret"},
	} {
		t.Run(c.name, func(t *testing.T) {
			unavailable = false
			ts := startServer(t, Config{Authenticator: c.auth})
			conn, err := pgxConnect(t, ts.addr, "alice", c.pw, "")
			if err != nil {
				t.Fatalf("with the store readable: %v", err)
			}
			_ = conn.Close(context.Background())

			// A role that does not exist still answers exactly as a wrong
			// password does: that is what the mock exchange is for, and it
			// must not have been widened by any of this.
			_, err = pgxConnect(t, ts.addr, "carol", c.pw, "")
			assertAuthFailure(t, err, CodeInvalidPassword)
			_, err = pgxConnect(t, ts.addr, "alice", "wrong", "")
			assertAuthFailure(t, err, CodeInvalidPassword)

			unavailable = true
			_, err = pgxConnect(t, ts.addr, "alice", c.pw, "")
			assertAuthFailure(t, err, CodeCannotConnectNow)
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && strings.Contains(pgErr.Message+pgErr.Detail, "catalog") {
				t.Errorf("the failure of the lookup reached the client: %q / %q", pgErr.Message, pgErr.Detail)
			}
		})
	}
}
