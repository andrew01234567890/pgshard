package pgwire

import "testing"

func TestUnknownUserMockSaltIsStablePerUser(t *testing.T) {
	a := SCRAMAuthenticator{MockSecret: []byte("test-secret")}
	if string(a.mockSalt("ghost")) != string(a.mockSalt("ghost")) {
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
