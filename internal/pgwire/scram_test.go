package pgwire

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

// Vector from RFC 7677 section 3 (user "user", password "pencil").
const (
	rfcClientFirst = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO"
	rfcServerNonce = "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0"
	rfcServerFirst = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
	rfcClientFinal = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
	rfcServerFinal = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
)

func rfcVerifier(t *testing.T) *SCRAMVerifier {
	t.Helper()
	salt, _ := base64.StdEncoding.DecodeString("W22ZaJ0SNY7soEsUEjb6gQ==")
	v, err := BuildSCRAMVerifier("pencil", salt, 4096)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestSCRAMRFC7677Vector(t *testing.T) {
	v := rfcVerifier(t)
	srv := newSCRAMServer(v)
	srv.serverNonce = rfcServerNonce
	first, err := srv.handleClientFirst([]byte(rfcClientFirst))
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != rfcServerFirst {
		t.Fatalf("server-first = %q, want %q", first, rfcServerFirst)
	}
	final, err := srv.handleClientFinal([]byte(rfcClientFinal))
	if err != nil {
		t.Fatal(err)
	}
	if string(final) != rfcServerFinal {
		t.Fatalf("server-final = %q, want %q", final, rfcServerFinal)
	}
	if srv.keys == nil || len(srv.keys.ClientKey) != 32 || string(srv.keys.ServerKey) != string(v.ServerKey) {
		t.Fatalf("keys not recovered: %+v", srv.keys)
	}
	if !keysEqual(hmacSHA256(nil, nil), hmacSHA256(nil, nil)) {
		t.Fatal("keysEqual broken")
	}
}

func TestSCRAMWrongProofFails(t *testing.T) {
	srv := newSCRAMServer(rfcVerifier(t))
	srv.serverNonce = rfcServerNonce
	if _, err := srv.handleClientFirst([]byte(rfcClientFirst)); err != nil {
		t.Fatal(err)
	}
	bad := "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVA="
	_, err := srv.handleClientFinal([]byte(bad))
	var pe *Error
	if !errors.As(err, &pe) || pe.Code != CodeInvalidPassword {
		t.Fatalf("err = %v, want 28P01", err)
	}
	if srv.keys != nil {
		t.Fatal("keys must not be exposed after a failed proof")
	}
}

func TestSCRAMRejectsChannelBindingAndAuthzid(t *testing.T) {
	cases := map[string]string{
		"p=tls-server-end-point,,n=user,r=abc": "channel binding",
		"n,a=other,n=user,r=abc":               "authorization identity",
		"n,,r=abc":                             "missing username",
		"x,,n=user,r=abc":                      "bad channel binding flag",
		"n,,n=user,r=":                         "bad nonce",
		"n,,n=user,r=abc,m=ext":                "unsupported extension",
	}
	for in, want := range cases {
		srv := newSCRAMServer(rfcVerifier(t))
		_, err := srv.handleClientFirst([]byte(in))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: err = %v, want containing %q", in, err, want)
		}
	}
	srv := newSCRAMServer(rfcVerifier(t))
	srv.serverNonce = rfcServerNonce
	if _, err := srv.handleClientFirst([]byte(rfcClientFirst)); err != nil {
		t.Fatal(err)
	}
	for in, want := range map[string]string{
		"c=cD10bHMtdW5pcXVlLCwsCg==,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=AAAA": "channel-binding data",
		"c=biws,r=other,p=AAAA": "nonce mismatch",
		"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=AAAA": "malformed proof",
		"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0":        "missing proof",
	} {
		if _, err := srv.handleClientFinal([]byte(in)); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: err = %v, want containing %q", in, err, want)
		}
	}
}

func TestSCRAMVerifierRoundTrip(t *testing.T) {
	v := rfcVerifier(t)
	s := v.String()
	if s[:len("SCRAM-SHA-256$4096:")] != "SCRAM-SHA-256$4096:" {
		t.Fatalf("bad prefix: %s", s)
	}
	back, err := ParseSCRAMVerifier(s)
	if err != nil {
		t.Fatal(err)
	}
	if back.String() != s {
		t.Fatalf("round trip mismatch: %s vs %s", back.String(), s)
	}
	for _, bad := range []string{"", "md5abc", "SCRAM-SHA-256$x:y$z", "SCRAM-SHA-256$0:AA==$AA==:AA==", "SCRAM-SHA-256$4096:AA==$AA==:AA=="} {
		if _, err := ParseSCRAMVerifier(bad); err == nil {
			t.Errorf("ParseSCRAMVerifier(%q) accepted", bad)
		}
	}
	r, err := BuildSCRAMVerifier("pw", nil, 0)
	if err != nil || r.Iterations != DefaultSCRAMIterations || len(r.Salt) != 16 {
		t.Fatalf("defaults: %+v %v", r, err)
	}
}
