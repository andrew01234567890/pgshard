package pki

import (
	"crypto/rand"
	"strings"
	"testing"
	"time"
)

func TestStaticServerBundleRoundTrip(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.July, 17, 12, 0, 0, 0, time.UTC)
	names := []string{
		"example-shardschema",
		"example-shardschema.default",
		"example-shardschema.default.svc",
		"example-shardschema.default.svc.cluster.local",
	}
	bundle, err := GenerateStaticServerBundle(now, rand.Reader, "example catalog CA", names)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateStaticServerBundle(bundle, names, now); err != nil {
		t.Fatal(err)
	}
	if err := ValidateStaticServerBundle(bundle, []string{names[0]}, now); err == nil || !strings.Contains(err.Error(), "exact configured DNS names") {
		t.Fatalf("wrong DNS set error = %v", err)
	}
	if err := ValidateStaticServerBundle(bundle, []string{"other.default.svc"}, now); err == nil || !strings.Contains(err.Error(), "exact configured DNS names") {
		t.Fatalf("wrong DNS name error = %v", err)
	}
}

func TestStaticServerBundleRejectsMismatchedKeys(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.July, 17, 12, 0, 0, 0, time.UTC)
	names := []string{"example-shardschema.default.svc"}
	first, err := GenerateStaticServerBundle(now, rand.Reader, "first catalog CA", names)
	if err != nil {
		t.Fatal(err)
	}
	second, err := GenerateStaticServerBundle(now, rand.Reader, "second catalog CA", names)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func(*StaticServerBundle)
		want   string
	}{
		{
			name: "CA certificate",
			mutate: func(bundle *StaticServerBundle) {
				bundle.CACertificate = second.CACertificate
			},
			want: "exact configured DNS names",
		},
		{
			name: "server key",
			mutate: func(bundle *StaticServerBundle) {
				bundle.ServerPrivateKey = second.ServerPrivateKey
			},
			want: "serving certificate and private key do not form a key pair",
		},
		{
			name: "server from another CA",
			mutate: func(bundle *StaticServerBundle) {
				bundle.ServerCertificate = second.ServerCertificate
				bundle.ServerPrivateKey = second.ServerPrivateKey
			},
			want: "exact configured DNS names",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			candidate := &StaticServerBundle{
				CACertificate:     append([]byte(nil), first.CACertificate...),
				ServerCertificate: append([]byte(nil), first.ServerCertificate...),
				ServerPrivateKey:  append([]byte(nil), first.ServerPrivateKey...),
			}
			test.mutate(candidate)
			if err := ValidateStaticServerBundle(candidate, names, now); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestStaticServerBundleFailsBeforeExpiry(t *testing.T) {
	t.Parallel()
	issued := time.Date(2026, time.July, 17, 12, 0, 0, 0, time.UTC)
	names := []string{"example-shardschema.default.svc"}
	bundle, err := GenerateStaticServerBundle(issued, rand.Reader, "example catalog CA", names)
	if err != nil {
		t.Fatal(err)
	}
	checkAt := issued.Add(staticServerValidity - staticRenewBefore + time.Second)
	if err := ValidateStaticServerBundle(bundle, names, checkAt); err == nil || !strings.Contains(err.Error(), "zero-downtime certificate rotation is not implemented") {
		t.Fatalf("near-expiry error = %v", err)
	}
}

func TestGenerateStaticServerBundleValidatesInputs(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.July, 17, 12, 0, 0, 0, time.UTC)
	if _, err := GenerateStaticServerBundle(now, nil, "example", []string{"example.svc"}); err == nil || !strings.Contains(err.Error(), "randomness source") {
		t.Fatalf("nil randomness error = %v", err)
	}
	if _, err := GenerateStaticServerBundle(now, rand.Reader, "  ", []string{"example.svc"}); err == nil || !strings.Contains(err.Error(), "common name") {
		t.Fatalf("empty common name error = %v", err)
	}
	if _, err := GenerateStaticServerBundle(now, rand.Reader, "example", nil); err == nil || !strings.Contains(err.Error(), "DNS name") {
		t.Fatalf("empty DNS names error = %v", err)
	}
	if err := ValidateStaticServerBundle(nil, []string{"example.svc"}, now); err == nil || !strings.Contains(err.Error(), "bundle is required") {
		t.Fatalf("nil bundle error = %v", err)
	}
}
