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

func TestReplicationTLSBundleRoundTrip(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.July, 17, 12, 0, 0, 0, time.UTC)
	memberDNSNames := map[int32][]string{
		0: {"demo-shard-0000-0.demo-shard-0000.default.svc"},
		1: {"demo-shard-0000-m0001-0.demo-shard-0000.default.svc"},
		2: {"demo-shard-0000-m0002-0.demo-shard-0000.default.svc"},
	}
	bundle, err := GenerateReplicationTLSBundle(now, rand.Reader, "example replication CA", memberDNSNames)
	if err != nil {
		t.Fatal(err)
	}
	caNotAfter, err := ValidateReplicationTLSCA(bundle.CACertificate, now)
	if err != nil {
		t.Fatal(err)
	}
	if !caNotAfter.Equal(bundle.CANotAfter) {
		t.Fatalf("CA NotAfter = %v, want %v", caNotAfter, bundle.CANotAfter)
	}
	if len(bundle.Servers) != len(memberDNSNames) {
		t.Fatalf("issued servers = %d, want %d", len(bundle.Servers), len(memberDNSNames))
	}
	for member, names := range memberDNSNames {
		material := bundle.Servers[member]
		notAfter, err := ValidateReplicationTLSServer(bundle.CACertificate, material.Certificate, material.PrivateKey, names, now)
		if err != nil {
			t.Fatalf("member %d: %v", member, err)
		}
		if !notAfter.Equal(material.NotAfter) {
			t.Fatalf("member %d NotAfter = %v, want %v", member, notAfter, material.NotAfter)
		}
		for other, otherNames := range memberDNSNames {
			if other == member {
				continue
			}
			if _, err := ValidateReplicationTLSServer(bundle.CACertificate, material.Certificate, material.PrivateKey, otherNames, now); err == nil {
				t.Fatalf("member %d certificate validated for member %d's DNS name", member, other)
			}
		}
	}
}

func TestReplicationTLSBundleRejectsMismatchedMaterial(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.July, 17, 12, 0, 0, 0, time.UTC)
	names := map[int32][]string{0: {"demo-shard-0000-0.demo-shard-0000.default.svc"}}
	first, err := GenerateReplicationTLSBundle(now, rand.Reader, "first replication CA", names)
	if err != nil {
		t.Fatal(err)
	}
	second, err := GenerateReplicationTLSBundle(now, rand.Reader, "second replication CA", names)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateReplicationTLSServer(second.CACertificate, first.Servers[0].Certificate, first.Servers[0].PrivateKey, names[0], now); err == nil || !strings.Contains(err.Error(), "exact configured DNS name") {
		t.Fatalf("foreign CA error = %v", err)
	}
	if _, err := ValidateReplicationTLSServer(first.CACertificate, first.Servers[0].Certificate, second.Servers[0].PrivateKey, names[0], now); err == nil || !strings.Contains(err.Error(), "do not form a key pair") {
		t.Fatalf("mismatched key error = %v", err)
	}
	if _, err := ValidateReplicationTLSServer(first.Servers[0].Certificate, first.Servers[0].Certificate, first.Servers[0].PrivateKey, names[0], now); err == nil {
		t.Fatal("leaf certificate was accepted as a CA")
	}
	checkAt := now.Add(staticServerValidity - staticRenewBefore + time.Second)
	if _, err := ValidateReplicationTLSServer(first.CACertificate, first.Servers[0].Certificate, first.Servers[0].PrivateKey, names[0], checkAt); err == nil || !strings.Contains(err.Error(), "zero-downtime certificate rotation is not implemented") {
		t.Fatalf("near-expiry error = %v", err)
	}
}

func TestReplicationTLSBundleRequiresExactlyOneSANPerLeaf(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.July, 17, 12, 0, 0, 0, time.UTC)
	if _, err := GenerateReplicationTLSBundle(now, rand.Reader, "example replication CA", map[int32][]string{
		0: {"demo-a.default.svc", "demo-b.default.svc"},
	}); err == nil || !strings.Contains(err.Error(), "exactly one non-empty DNS name") {
		t.Fatalf("multi-SAN issuance error = %v", err)
	}
	if _, err := GenerateReplicationTLSBundle(now, rand.Reader, "example replication CA", map[int32][]string{0: {" "}}); err == nil || !strings.Contains(err.Error(), "exactly one non-empty DNS name") {
		t.Fatalf("blank SAN issuance error = %v", err)
	}
	if _, err := GenerateReplicationTLSBundle(now, rand.Reader, "example replication CA", nil); err == nil || !strings.Contains(err.Error(), "at least one member") {
		t.Fatalf("empty member set error = %v", err)
	}
	if _, err := GenerateReplicationTLSBundle(now, rand.Reader, "example replication CA", map[int32][]string{-1: {"demo.default.svc"}}); err == nil || !strings.Contains(err.Error(), "not a valid shard member") {
		t.Fatalf("negative member error = %v", err)
	}

	names := []string{"demo-shard-0000-0.demo-shard-0000.default.svc"}
	multiSAN, err := GenerateStaticServerBundle(now, rand.Reader, "example catalog CA", []string{names[0], "second.default.svc"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateReplicationTLSServer(multiSAN.CACertificate, multiSAN.ServerCertificate, multiSAN.ServerPrivateKey, names, now); err == nil || !strings.Contains(err.Error(), "exactly one DNS name") {
		t.Fatalf("multi-SAN validation error = %v", err)
	}
	if _, err := ValidateReplicationTLSServer(multiSAN.CACertificate, multiSAN.ServerCertificate, multiSAN.ServerPrivateKey, []string{names[0], "second.default.svc"}, now); err == nil || !strings.Contains(err.Error(), "exactly one non-empty DNS name") {
		t.Fatalf("multi-name validation request error = %v", err)
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
