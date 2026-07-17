// Package restore implements mutation-free restore validation.
package restore

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
)

const keyspaceEndDecimal = "18446744073709551616"

const (
	canonicalManifestDomain = "pgshard.restore-manifest.v1\x00"
	canonicalTopologyDomain = "pgshard.restore-topology.v1\x00"
)

var keyspaceEnd = func() *big.Int {
	value, ok := new(big.Int).SetString(keyspaceEndDecimal, 10)
	if !ok {
		panic("invalid fixed keyspace end")
	}
	return value
}()

// VerificationKeyDataKey is the only accepted key in a restore verification
// Secret. The value is a raw Ed25519 public key, not PEM or base64.
const VerificationKeyDataKey = "ed25519.pub"

// RestoreTopologyMismatchError is the permanent, typed mismatch returned
// before any restore target mutation.
type RestoreTopologyMismatchError struct {
	Fields                         []string
	ManifestSHA256                 string
	ManifestTopologyFingerprint    string
	DestinationTopologyFingerprint string
}

func (e *RestoreTopologyMismatchError) Error() string {
	return fmt.Sprintf(
		"RestoreTopologyMismatch: destination topology differs from backup in %s (manifest=%s destination=%s)",
		strings.Join(e.Fields, ","), e.ManifestTopologyFingerprint, e.DestinationTopologyFingerprint,
	)
}

// InvalidManifestError identifies a signed but unsupported or malformed backup
// manifest. It is permanent for this restore request.
type InvalidManifestError struct {
	Field  string
	Reason string
}

func (e *InvalidManifestError) Error() string {
	return fmt.Sprintf("invalid backup manifest %s: %s", e.Field, e.Reason)
}

// InvalidDestinationError identifies a malformed destination request that is
// not a comparison with a valid backup topology.
type InvalidDestinationError struct {
	Field  string
	Reason string
}

func (e *InvalidDestinationError) Error() string {
	return fmt.Sprintf("invalid restore destination %s: %s", e.Field, e.Reason)
}

// SignatureError prevents untrusted manifest fields from reaching topology
// comparison or status.
type SignatureError struct {
	Reason string
}

func (e *SignatureError) Error() string {
	return "backup manifest signature is invalid: " + e.Reason
}

// PreflightResult is safe to checkpoint in PgShardRestore status.
type PreflightResult struct {
	ManifestSHA256 string
	TopologySHA256 string
	verified       bool
}

// AuthoritativeDestination can only be constructed as a proven absence or an
// existing destination with a catalog-derived topology. Its zero value is
// deliberately invalid.
type AuthoritativeDestination struct {
	kind     authoritativeDestinationKind
	topology pgshardv1alpha1.RestoreTopology
}

type authoritativeDestinationKind uint8

const (
	authoritativeDestinationUnknown authoritativeDestinationKind = iota
	authoritativeDestinationAbsent
	authoritativeDestinationExisting
)

// ProvenAbsentDestination records an authoritative catalog absence result.
func ProvenAbsentDestination() AuthoritativeDestination {
	return AuthoritativeDestination{kind: authoritativeDestinationAbsent}
}

// ExistingDestination records an authoritative catalog topology result.
func ExistingDestination(topology pgshardv1alpha1.RestoreTopology) AuthoritativeDestination {
	return AuthoritativeDestination{kind: authoritativeDestinationExisting, topology: topology}
}

// CanonicalManifest returns the exact version-1 binary payload signed by
// backup publication and verified by restore preflight. It uses fixed field
// order, big-endian integers, and u32-length-prefixed string bytes so Go and
// Rust implementations do not depend on JSON serializer behavior.
func CanonicalManifest(manifest pgshardv1alpha1.RestoreManifest) ([]byte, error) {
	if len(manifest.Topology.Shards) > pgshardv1alpha1.MaximumShards {
		return nil, &InvalidManifestError{Field: "topology.shards", Reason: "must contain at most 128 ranges"}
	}
	var payload bytes.Buffer
	payload.Grow(256 + len(manifest.Topology.Shards)*64)
	payload.WriteString(canonicalManifestDomain)
	writeInt32(&payload, manifest.ManifestVersion)
	if err := writeString(&payload, "backupSetID", manifest.BackupSetID, 128); err != nil {
		return nil, err
	}
	if err := writeString(&payload, "sourceDatabase", manifest.SourceDatabase, 63); err != nil {
		return nil, err
	}
	if err := writeTopology(&payload, manifest.Topology); err != nil {
		return nil, err
	}
	return payload.Bytes(), nil
}

// Preflight verifies the manifest signature before interpreting its topology,
// then requires any caller-supplied destination expectation to match exactly.
// It does not establish whether the destination exists; callers must separately
// call VerifyAuthoritativeDestination with catalog-derived evidence.
func Preflight(
	manifest pgshardv1alpha1.RestoreManifest,
	signature string,
	publicKey []byte,
	destinationDatabase string,
	destination *pgshardv1alpha1.RestoreTopology,
) (*PreflightResult, error) {
	if len(publicKey) != ed25519.PublicKeySize {
		return nil, &SignatureError{Reason: "verification key must contain exactly 32 raw bytes"}
	}
	payload, err := CanonicalManifest(manifest)
	if err != nil {
		return nil, err
	}
	decodedSignature, err := base64.StdEncoding.Strict().DecodeString(signature)
	if err != nil || len(decodedSignature) != ed25519.SignatureSize || base64.StdEncoding.EncodeToString(decodedSignature) != signature {
		return nil, &SignatureError{Reason: "signature must be canonical base64 for exactly 64 bytes"}
	}
	if !ed25519.Verify(ed25519.PublicKey(publicKey), payload, decodedSignature) {
		return nil, &SignatureError{Reason: "Ed25519 verification failed"}
	}

	if err := validateManifest(manifest); err != nil {
		return nil, err
	}
	if err := validateDatabaseName("destinationDatabase", destinationDatabase, true); err != nil {
		return nil, err
	}
	manifestHash := sha256.Sum256(payload)
	manifestSHA256 := hex.EncodeToString(manifestHash[:])
	topologyFingerprint, err := TopologyFingerprint(manifest.Topology)
	if err != nil {
		return nil, err
	}
	if destination != nil {
		if err := validateTopology(*destination, false); err != nil {
			return nil, err
		}
		if err := compareTopologies(manifest.Topology, *destination, manifestSHA256, topologyFingerprint); err != nil {
			return nil, err
		}
	}
	return &PreflightResult{
		ManifestSHA256: manifestSHA256,
		TopologySHA256: topologyFingerprint,
		verified:       true,
	}, nil
}

// VerifyAuthoritativeDestination requires catalog-derived proof that the
// destination is absent or compares its exact live topology with the backup.
// It returns the destination fingerprint when the database exists.
func VerifyAuthoritativeDestination(
	manifest pgshardv1alpha1.RestoreManifest,
	verified *PreflightResult,
	destination AuthoritativeDestination,
) (string, error) {
	if verified == nil || !verified.verified || verified.ManifestSHA256 == "" || verified.TopologySHA256 == "" {
		return "", &InvalidDestinationError{Field: "authoritativeTopology", Reason: "signed manifest verification result is required"}
	}
	payload, err := CanonicalManifest(manifest)
	if err != nil {
		return "", err
	}
	manifestDigest := sha256.Sum256(payload)
	topologyFingerprint, err := TopologyFingerprint(manifest.Topology)
	if err != nil {
		return "", err
	}
	if hex.EncodeToString(manifestDigest[:]) != verified.ManifestSHA256 || topologyFingerprint != verified.TopologySHA256 {
		return "", &InvalidDestinationError{Field: "authoritativeTopology", Reason: "verification result does not belong to this manifest"}
	}
	switch destination.kind {
	case authoritativeDestinationAbsent:
		return "", nil
	case authoritativeDestinationExisting:
		if err := validateTopology(destination.topology, false); err != nil {
			return "", err
		}
		fingerprint, err := destinationTopologyFingerprint(destination.topology)
		if err != nil {
			return "", err
		}
		if err := compareTopologies(manifest.Topology, destination.topology, verified.ManifestSHA256, verified.TopologySHA256); err != nil {
			return "", err
		}
		return fingerprint, nil
	default:
		return "", &InvalidDestinationError{Field: "authoritativeTopology", Reason: "destination absence or topology has not been proven"}
	}
}

func validateManifest(manifest pgshardv1alpha1.RestoreManifest) error {
	if manifest.ManifestVersion != pgshardv1alpha1.RestoreManifestVersionV1 {
		return &InvalidManifestError{Field: "manifestVersion", Reason: "only version 1 is supported"}
	}
	if err := validateOpaqueIdentity("backupSetID", manifest.BackupSetID, 128); err != nil {
		return err
	}
	if err := validateDatabaseName("sourceDatabase", manifest.SourceDatabase, false); err != nil {
		return err
	}
	return validateTopology(manifest.Topology, true)
}

func validateOpaqueIdentity(field, value string, maximumBytes int) error {
	if value == "" || len(value) > maximumBytes || !utf8.ValidString(value) {
		return &InvalidManifestError{Field: field, Reason: fmt.Sprintf("must be valid UTF-8 containing 1 to %d bytes", maximumBytes)}
	}
	for _, character := range value {
		if character == 0 || character < 0x20 || character == 0x7f {
			return &InvalidManifestError{Field: field, Reason: "must not contain NUL or control characters"}
		}
	}
	return nil
}

func validateDatabaseName(field, value string, destination bool) error {
	invalid := value == "" || len(value) > 63 || !utf8.ValidString(value) || strings.ContainsRune(value, 0)
	if !invalid {
		for _, character := range value {
			if character < 0x20 || character == 0x7f {
				invalid = true
				break
			}
		}
	}
	if !invalid {
		return nil
	}
	reason := "must be valid UTF-8 containing 1 to 63 bytes and no NUL or control characters"
	if destination {
		return &InvalidDestinationError{Field: field, Reason: reason}
	}
	return &InvalidManifestError{Field: field, Reason: reason}
}

func validateTopology(topology pgshardv1alpha1.RestoreTopology, manifest bool) error {
	invalid := func(field, reason string) error {
		if manifest {
			return &InvalidManifestError{Field: "topology." + field, Reason: reason}
		}
		return &InvalidDestinationError{Field: "destinationTopology." + field, Reason: reason}
	}
	if manifest {
		if topology.PostgreSQLMajor != pgshardv1alpha1.PostgreSQLMajor18 {
			return invalid("postgresqlMajor", "only PostgreSQL 18 is supported")
		}
		if topology.HashVersion != pgshardv1alpha1.RoutingHashVersionV1 {
			return invalid("hashVersion", "only routing hash version 1 is supported")
		}
	} else {
		major, err := strconv.ParseUint(topology.PostgreSQLMajor, 10, 16)
		if err != nil || major == 0 || strconv.FormatUint(major, 10) != topology.PostgreSQLMajor {
			return invalid("postgresqlMajor", "must be canonical positive decimal")
		}
		if topology.HashVersion < 1 {
			return invalid("hashVersion", "must be positive")
		}
	}
	seed, err := strconv.ParseUint(topology.HashSeed, 10, 64)
	if err != nil || strconv.FormatUint(seed, 10) != topology.HashSeed {
		return invalid("hashSeed", "must be canonical unsigned 64-bit decimal")
	}
	if topology.ShardCount < 1 || topology.ShardCount > pgshardv1alpha1.MaximumShards {
		return invalid("shardCount", "must be between 1 and 128")
	}
	if len(topology.Shards) != int(topology.ShardCount) {
		return invalid("shards", "length must equal shardCount")
	}

	previousEnd := big.NewInt(0)
	for index, shard := range topology.Shards {
		prefix := fmt.Sprintf("shards[%d]", index)
		if shard.Ordinal != int32(index) {
			return invalid(prefix+".ordinal", "must equal its ordered zero-based position")
		}
		start, ok := canonicalDecimal(shard.Start)
		if !ok || start.Cmp(keyspaceEnd) >= 0 {
			return invalid(prefix+".start", "must be canonical decimal below 2^64")
		}
		end, ok := canonicalDecimal(shard.End)
		if !ok || end.Cmp(keyspaceEnd) > 0 {
			return invalid(prefix+".end", "must be canonical decimal at most 2^64")
		}
		if start.Cmp(previousEnd) != 0 {
			return invalid(prefix+".start", "must equal the preceding range end without a gap or overlap")
		}
		if end.Cmp(start) <= 0 {
			return invalid(prefix+".end", "must be greater than start")
		}
		previousEnd = end
	}
	if previousEnd.Cmp(keyspaceEnd) != 0 {
		return invalid(fmt.Sprintf("shards[%d].end", len(topology.Shards)-1), "final range must end at 2^64")
	}
	return nil
}

func canonicalDecimal(value string) (*big.Int, bool) {
	if value == "" || len(value) > 20 || (len(value) > 1 && value[0] == '0') {
		return nil, false
	}
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return nil, false
		}
	}
	parsed, ok := new(big.Int).SetString(value, 10)
	return parsed, ok && parsed.Sign() >= 0
}

// TopologyFingerprint validates and hashes the canonical binary topology.
func TopologyFingerprint(topology pgshardv1alpha1.RestoreTopology) (string, error) {
	if err := validateTopology(topology, true); err != nil {
		return "", err
	}
	return fingerprintTopology(topology)
}

func destinationTopologyFingerprint(topology pgshardv1alpha1.RestoreTopology) (string, error) {
	if err := validateTopology(topology, false); err != nil {
		return "", err
	}
	return fingerprintTopology(topology)
}

func fingerprintTopology(topology pgshardv1alpha1.RestoreTopology) (string, error) {
	var payload bytes.Buffer
	payload.WriteString(canonicalTopologyDomain)
	if err := writeTopology(&payload, topology); err != nil {
		return "", fmt.Errorf("encode canonical restore topology: %w", err)
	}
	digest := sha256.Sum256(payload.Bytes())
	return hex.EncodeToString(digest[:]), nil
}

func writeTopology(payload *bytes.Buffer, topology pgshardv1alpha1.RestoreTopology) error {
	if len(topology.Shards) > pgshardv1alpha1.MaximumShards {
		return &InvalidManifestError{Field: "topology.shards", Reason: "must contain at most 128 ranges"}
	}
	if err := writeString(payload, "topology.postgresqlMajor", topology.PostgreSQLMajor, 8); err != nil {
		return err
	}
	writeInt32(payload, topology.HashVersion)
	if err := writeString(payload, "topology.hashSeed", topology.HashSeed, 20); err != nil {
		return err
	}
	writeInt32(payload, topology.ShardCount)
	writeUint32(payload, uint32(len(topology.Shards)))
	for index, shard := range topology.Shards {
		writeInt32(payload, shard.Ordinal)
		if err := writeString(payload, fmt.Sprintf("topology.shards[%d].start", index), shard.Start, 20); err != nil {
			return err
		}
		if err := writeString(payload, fmt.Sprintf("topology.shards[%d].end", index), shard.End, 20); err != nil {
			return err
		}
	}
	return nil
}

func writeString(payload *bytes.Buffer, field, value string, maximum int) error {
	if len(value) > maximum {
		return &InvalidManifestError{Field: field, Reason: fmt.Sprintf("must contain at most %d bytes", maximum)}
	}
	writeUint32(payload, uint32(len(value)))
	payload.WriteString(value)
	return nil
}

func writeInt32(payload *bytes.Buffer, value int32) {
	writeUint32(payload, uint32(value))
}

func writeUint32(payload *bytes.Buffer, value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	payload.Write(encoded[:])
}

func compareTopologies(
	manifest, destination pgshardv1alpha1.RestoreTopology,
	manifestSHA256, manifestFingerprint string,
) error {
	fields := make([]string, 0)
	if manifest.PostgreSQLMajor != destination.PostgreSQLMajor {
		fields = append(fields, "postgresqlMajor")
	}
	if manifest.HashVersion != destination.HashVersion {
		fields = append(fields, "hashVersion")
	}
	if manifest.HashSeed != destination.HashSeed {
		fields = append(fields, "hashSeed")
	}
	if manifest.ShardCount != destination.ShardCount {
		fields = append(fields, "shardCount")
	}
	if len(manifest.Shards) != len(destination.Shards) {
		fields = append(fields, "shards")
	}
	for index := range min(len(manifest.Shards), len(destination.Shards)) {
		if manifest.Shards[index].Ordinal != destination.Shards[index].Ordinal {
			fields = append(fields, fmt.Sprintf("shards[%d].ordinal", index))
		}
		if manifest.Shards[index].Start != destination.Shards[index].Start {
			fields = append(fields, fmt.Sprintf("shards[%d].start", index))
		}
		if manifest.Shards[index].End != destination.Shards[index].End {
			fields = append(fields, fmt.Sprintf("shards[%d].end", index))
		}
	}
	if len(fields) == 0 {
		return nil
	}
	destinationFingerprint, err := destinationTopologyFingerprint(destination)
	if err != nil {
		return errors.New("validated destination topology could not be fingerprinted: " + err.Error())
	}
	slices.Sort(fields)
	fields = slices.Compact(fields)
	return &RestoreTopologyMismatchError{
		Fields:                         fields,
		ManifestSHA256:                 manifestSHA256,
		ManifestTopologyFingerprint:    manifestFingerprint,
		DestinationTopologyFingerprint: destinationFingerprint,
	}
}
