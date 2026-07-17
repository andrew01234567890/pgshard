package restore

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
)

func TestCanonicalManifestV1Golden(t *testing.T) {
	t.Parallel()
	const (
		wantPayload = "706773686172642e726573746f72652d6d616e69666573742e76310000000001000000136261636b75702d612d323032362d30372d313700000001410000000231380000000100000001370000000100000001000000000000000130000000143138343436373434303733373039353531363136"
		wantSHA256  = "6fa139044064d13741c2251a7452befbc3d32160b9326d3d4203f91430a0c65d"
		wantKey     = "03a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b8"
		wantSig     = "llQgnEoViqdb4MHelORCxYYKodR5sLyFgueEpzUhklgs8qQSSKiU/+zUGNTt70vhOaYH5knhO4Kao2SVKiNNDw=="
	)
	manifest := validManifest(topologyFromBoundaries("7", []string{"0", keyspaceEndDecimal}))
	payload, err := CanonicalManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	publicKey, privateKey := deterministicKey(t)
	digest := sha256.Sum256(payload)
	got := []string{
		hex.EncodeToString(payload),
		hex.EncodeToString(digest[:]),
		hex.EncodeToString(publicKey),
		base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, payload)),
	}
	want := []string{wantPayload, wantSHA256, wantKey, wantSig}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("canonical vector = %#v, want %#v", got, want)
	}
}

func TestPreflightRequiresExactBackupTopology(t *testing.T) {
	t.Parallel()
	publicKey, privateKey := deterministicKey(t)

	tests := []struct {
		name             string
		manifestTopology pgshardv1alpha1.RestoreTopology
		destination      pgshardv1alpha1.RestoreTopology
		wantFields       []string
	}{
		{
			name:             "five shards cannot restore into three",
			manifestTopology: topologyFive(),
			destination:      topologyThree(),
			wantFields: []string{
				"shardCount", "shards", "shards[0].end", "shards[1].end",
				"shards[1].start", "shards[2].end", "shards[2].start",
			},
		},
		{
			name: "equal count with different ranges",
			manifestTopology: pgshardv1alpha1.RestoreTopology{
				PostgreSQLMajor: "18", HashVersion: 1, HashSeed: "7", ShardCount: 2,
				Shards: []pgshardv1alpha1.RestoreShardRange{
					{Ordinal: 0, Start: "0", End: "9223372036854775808"},
					{Ordinal: 1, Start: "9223372036854775808", End: keyspaceEndDecimal},
				},
			},
			destination: pgshardv1alpha1.RestoreTopology{
				PostgreSQLMajor: "18", HashVersion: 1, HashSeed: "7", ShardCount: 2,
				Shards: []pgshardv1alpha1.RestoreShardRange{
					{Ordinal: 0, Start: "0", End: "9223372036854775807"},
					{Ordinal: 1, Start: "9223372036854775807", End: keyspaceEndDecimal},
				},
			},
			wantFields: []string{"shards[0].end", "shards[1].start"},
		},
		{
			name:             "hash seed differs",
			manifestTopology: topologyFive(),
			destination: func() pgshardv1alpha1.RestoreTopology {
				topology := topologyFive()
				topology.HashSeed = "8"
				return topology
			}(),
			wantFields: []string{"hashSeed"},
		},
		{
			name:             "PostgreSQL major differs",
			manifestTopology: topologyFive(),
			destination: func() pgshardv1alpha1.RestoreTopology {
				topology := topologyFive()
				topology.PostgreSQLMajor = "17"
				return topology
			}(),
			wantFields: []string{"postgresqlMajor"},
		},
		{
			name:             "hash version differs",
			manifestTopology: topologyFive(),
			destination: func() pgshardv1alpha1.RestoreTopology {
				topology := topologyFive()
				topology.HashVersion = 2
				return topology
			}(),
			wantFields: []string{"hashVersion"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			manifest := validManifest(test.manifestTopology)
			signature := signManifest(t, privateKey, manifest)
			_, err := Preflight(manifest, signature, publicKey, "B", &test.destination)
			var mismatch *RestoreTopologyMismatchError
			if !errors.As(err, &mismatch) {
				t.Fatalf("error = %T %v, want RestoreTopologyMismatchError", err, err)
			}
			if !reflect.DeepEqual(mismatch.Fields, test.wantFields) {
				t.Fatalf("fields = %#v, want %#v", mismatch.Fields, test.wantFields)
			}
			if mismatch.ManifestSHA256 == "" || mismatch.ManifestTopologyFingerprint == "" || mismatch.DestinationTopologyFingerprint == "" || mismatch.ManifestTopologyFingerprint == mismatch.DestinationTopologyFingerprint {
				t.Fatalf("mismatch fingerprints = %#v", mismatch)
			}
			if got := err.Error(); len(got) < len("RestoreTopologyMismatch") || got[:len("RestoreTopologyMismatch")] != "RestoreTopologyMismatch" {
				t.Fatalf("stable error code missing from %q", got)
			}
		})
	}
}

func TestPreflightAcceptsOmittedOrExactRequestedTopology(t *testing.T) {
	t.Parallel()
	publicKey, privateKey := deterministicKey(t)
	manifest := validManifest(topologyFive())
	signature := signManifest(t, privateKey, manifest)

	omitted, err := Preflight(manifest, signature, publicKey, "B", nil)
	if err != nil {
		t.Fatal(err)
	}
	exactTopology := topologyFive()
	exact, err := Preflight(manifest, signature, publicKey, "B", &exactTopology)
	if err != nil {
		t.Fatal(err)
	}
	if omitted.ManifestSHA256 == "" || omitted.TopologySHA256 == "" || !reflect.DeepEqual(omitted, exact) {
		t.Fatalf("omitted result %#v differs from exact %#v", omitted, exact)
	}
}

func TestAuthoritativeDestinationRequiresProofAndExactTopology(t *testing.T) {
	t.Parallel()
	publicKey, privateKey := deterministicKey(t)
	manifest := validManifest(topologyFive())
	verified, err := Preflight(manifest, signManifest(t, privateKey, manifest), publicKey, "B", nil)
	if err != nil {
		t.Fatal(err)
	}
	if fingerprint, err := VerifyAuthoritativeDestination(manifest, verified, ProvenAbsentDestination()); err != nil || fingerprint != "" {
		t.Fatalf("proven absence = fingerprint %q, error %v", fingerprint, err)
	}
	if fingerprint, err := VerifyAuthoritativeDestination(manifest, verified, ExistingDestination(topologyFive())); err != nil || fingerprint != verified.TopologySHA256 {
		t.Fatalf("exact existing destination = fingerprint %q, error %v", fingerprint, err)
	}
	if _, err := VerifyAuthoritativeDestination(manifest, verified, AuthoritativeDestination{}); err == nil {
		t.Fatal("unproven destination was accepted")
	}
	otherManifest := validManifest(topologyThree())
	if _, err := VerifyAuthoritativeDestination(otherManifest, verified, ProvenAbsentDestination()); err == nil {
		t.Fatal("verification result was accepted for a different manifest")
	}
	forged := &PreflightResult{ManifestSHA256: verified.ManifestSHA256, TopologySHA256: verified.TopologySHA256}
	if _, err := VerifyAuthoritativeDestination(manifest, forged, ProvenAbsentDestination()); err == nil {
		t.Fatal("forged verification result was accepted")
	}
	if _, err := VerifyAuthoritativeDestination(manifest, verified, ExistingDestination(topologyThree())); err == nil {
		t.Fatal("mismatched authoritative destination was accepted")
	}
}

func TestPreflightVerifiesSignatureBeforeTopology(t *testing.T) {
	t.Parallel()
	publicKey, privateKey := deterministicKey(t)
	manifest := validManifest(topologyFive())
	manifest.Topology.Shards[1].Start = "1"
	validSignature := signManifest(t, privateKey, manifest)

	_, err := Preflight(manifest, base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)), publicKey, "B", nil)
	var signatureError *SignatureError
	if !errors.As(err, &signatureError) {
		t.Fatalf("unsigned malformed topology error = %T %v, want SignatureError", err, err)
	}

	_, err = Preflight(manifest, validSignature, publicKey, "B", nil)
	var manifestError *InvalidManifestError
	if !errors.As(err, &manifestError) {
		t.Fatalf("signed malformed topology error = %T %v, want InvalidManifestError", err, err)
	}
}

func TestTopologyValidationRejectsNoncanonicalOrIncompleteKeyspace(t *testing.T) {
	t.Parallel()
	publicKey, privateKey := deterministicKey(t)
	tests := []struct {
		name   string
		mutate func(*pgshardv1alpha1.RestoreTopology)
	}{
		{name: "leading zero seed", mutate: func(topology *pgshardv1alpha1.RestoreTopology) { topology.HashSeed = "07" }},
		{name: "seed overflow", mutate: func(topology *pgshardv1alpha1.RestoreTopology) { topology.HashSeed = keyspaceEndDecimal }},
		{name: "duplicate ordinal", mutate: func(topology *pgshardv1alpha1.RestoreTopology) { topology.Shards[1].Ordinal = 0 }},
		{name: "gap", mutate: func(topology *pgshardv1alpha1.RestoreTopology) { topology.Shards[1].Start = "3689348814741910324" }},
		{name: "overlap", mutate: func(topology *pgshardv1alpha1.RestoreTopology) { topology.Shards[1].Start = "3689348814741910322" }},
		{name: "short keyspace", mutate: func(topology *pgshardv1alpha1.RestoreTopology) { topology.Shards[4].End = "18446744073709551615" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			topology := topologyFive()
			test.mutate(&topology)
			manifest := validManifest(topology)
			_, err := Preflight(manifest, signManifest(t, privateKey, manifest), publicKey, "B", nil)
			var invalid *InvalidManifestError
			if !errors.As(err, &invalid) {
				t.Fatalf("error = %T %v, want InvalidManifestError", err, err)
			}
		})
	}
}

func TestTopologyValidationBoundsDecimalBeforeParsing(t *testing.T) {
	t.Parallel()
	topology := topologyFive()
	topology.Shards[0].End = strings.Repeat("9", 1<<20)
	if err := validateTopology(topology, true); err == nil {
		t.Fatal("unbounded decimal was accepted")
	}
}

func deterministicKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	for index := range seed {
		seed[index] = byte(index)
	}
	privateKey := ed25519.NewKeyFromSeed(seed)
	return privateKey.Public().(ed25519.PublicKey), privateKey
}

func signManifest(t *testing.T, privateKey ed25519.PrivateKey, manifest pgshardv1alpha1.RestoreManifest) string {
	t.Helper()
	payload, err := CanonicalManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
}

func validManifest(topology pgshardv1alpha1.RestoreTopology) pgshardv1alpha1.RestoreManifest {
	return pgshardv1alpha1.RestoreManifest{
		ManifestVersion: 1,
		BackupSetID:     "backup-a-2026-07-17",
		SourceDatabase:  "A",
		Topology:        topology,
	}
}

func topologyFive() pgshardv1alpha1.RestoreTopology {
	boundaries := []string{
		"0",
		"3689348814741910323",
		"7378697629483820646",
		"11068046444225730969",
		"14757395258967641292",
		keyspaceEndDecimal,
	}
	return topologyFromBoundaries("7", boundaries)
}

func topologyThree() pgshardv1alpha1.RestoreTopology {
	return topologyFromBoundaries("7", []string{
		"0", "6148914691236517205", "12297829382473034410", keyspaceEndDecimal,
	})
}

func topologyFromBoundaries(seed string, boundaries []string) pgshardv1alpha1.RestoreTopology {
	shards := make([]pgshardv1alpha1.RestoreShardRange, len(boundaries)-1)
	for index := range shards {
		shards[index] = pgshardv1alpha1.RestoreShardRange{
			Ordinal: int32(index), Start: boundaries[index], End: boundaries[index+1],
		}
	}
	return pgshardv1alpha1.RestoreTopology{
		PostgreSQLMajor: "18",
		HashVersion:     1,
		HashSeed:        seed,
		ShardCount:      int32(len(shards)),
		Shards:          shards,
	}
}
