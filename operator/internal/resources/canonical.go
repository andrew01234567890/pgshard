package resources

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"

	cbor "github.com/fxamacker/cbor/v2"
	corev1 "k8s.io/api/core/v1"
)

// PodClass identifies the kind of protected pod a template produces. It is a
// domain-separation input to the contract hash so two structurally identical
// templates of different classes can never collide.
type PodClass string

const (
	ClassSource       PodClass = "source"
	ClassStandby      PodClass = "standby"
	ClassSingleMember PodClass = "single-member"
	ClassPooler       PodClass = "pooler"
	ClassOrchestrator PodClass = "orchestrator"
)

const (
	// PodContractHashAnnotation carries the reconciler-stamped full-contract
	// hash on a workload's pod template (and therefore on every pod the
	// controller creates from it). It is deliberately EXCLUDED from its own
	// canonical hash input.
	PodContractHashAnnotation = "pgshard.io/contract-hash"
	// PodSecurityGenerationAnnotation carries the monotonic per-(class,member)
	// security generation the template was stamped at. Its integrity is bound
	// through the hash's domain key, and the annotation is re-derived from the
	// authoritative generation argument at hash time, so its stamped value can
	// never diverge from what was hashed.
	PodSecurityGenerationAnnotation = "pgshard.io/security-generation"

	contractHashDomain = "pgshard.pod-contract.v1"
)

// canonicalEncMode is a true canonical CBOR encoder: RFC 8949 §4.2.1 Core
// Deterministic Encoding — definite-length (length-framed) items, shortest
// integer forms, and map keys sorted in bytewise lexicographic order. The same
// normalized value therefore always produces identical bytes, on the machine
// that stamps a template and on any machine that later recomputes the hash.
var canonicalEncMode = mustCanonicalEncMode()

func mustCanonicalEncMode() cbor.EncMode {
	mode, err := cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		panic(fmt.Sprintf("initialize canonical CBOR encoder: %v", err))
	}
	return mode
}

// ComputeContractStamp returns the domain-separated, length-framed full-contract
// hash of a pod template:
//
//	contractHash = HMAC_SHA256(
//	    key = lengthFramed("pgshard.pod-contract.v1" ‖ class ‖ clusterUID ‖ shard ‖ member ‖ securityGeneration),
//	    msg = canonicalCBOR(normalizedTemplateMetadataAndSpec))
//
// It never mutates the caller's template and is self-consistent: the value it
// returns for a freshly-built template equals the value it returns for the same
// template after it has been stamped, because normalization re-derives the
// security-generation annotation from the authoritative argument and excludes
// the contract-hash annotation entirely.
func ComputeContractStamp(class PodClass, clusterUID string, shard, member int32, securityGeneration int64, template *corev1.PodTemplateSpec) (string, error) {
	if template == nil {
		return "", fmt.Errorf("pod template is required to compute a contract stamp")
	}
	normalized, err := normalizePodTemplateForContract(template, securityGeneration)
	if err != nil {
		return "", err
	}
	message, err := canonicalEncMode.Marshal(normalized)
	if err != nil {
		return "", fmt.Errorf("canonical-encode normalized pod template: %w", err)
	}
	mac := hmac.New(sha256.New, contractDomainKey(class, clusterUID, shard, member, securityGeneration))
	if _, err := mac.Write(message); err != nil {
		return "", fmt.Errorf("hash normalized pod template: %w", err)
	}
	return hex.EncodeToString(mac.Sum(nil)), nil
}

// ApplyContractStamp computes the stamp and writes both stamp annotations onto
// the supplied template (mutating it), returning the hash. The reconciler uses
// this so the controller propagates the stamp to every pod it creates.
func ApplyContractStamp(template *corev1.PodTemplateSpec, class PodClass, clusterUID string, shard, member int32, securityGeneration int64) (string, error) {
	hash, err := ComputeContractStamp(class, clusterUID, shard, member, securityGeneration, template)
	if err != nil {
		return "", err
	}
	if template.Annotations == nil {
		template.Annotations = make(map[string]string, 2)
	}
	template.Annotations[PodSecurityGenerationAnnotation] = strconv.FormatInt(securityGeneration, 10)
	template.Annotations[PodContractHashAnnotation] = hash
	return hash, nil
}

// normalizePodTemplateForContract produces the canonical input to the contract
// hash.
//
// STEP-1 SCOPE / BOUNDARY: this is a deliberately minimal, clearly-marked
// skeleton. For step 1 it only needs to be (a) deterministic and (b)
// self-consistent between stamp time and recompute time — which it achieves by
// re-deriving the security-generation annotation from the authoritative
// argument, excluding the contract-hash annotation, and canonical-CBOR-encoding
// the whole template metadata+spec. The full per-class CreateNormalForm /
// LiveNormalForm capability comparison (ordinal identity derivation, projected
// token relational tuple, priority/toleration/topology handling, server-metadata
// stripping) is STEP 2's job and will replace the body of this function; the
// hash construction, domain key, annotation exclusion, and canonical encoder
// established here do not change.
func normalizePodTemplateForContract(template *corev1.PodTemplateSpec, securityGeneration int64) (any, error) {
	normalized := template.DeepCopy()
	if normalized.Annotations == nil {
		normalized.Annotations = make(map[string]string, 1)
	}
	// Re-derive the security generation from the authoritative argument (never
	// trust a value already on the template) and exclude the contract-hash
	// annotation from its own input.
	normalized.Annotations[PodSecurityGenerationAnnotation] = strconv.FormatInt(securityGeneration, 10)
	delete(normalized.Annotations, PodContractHashAnnotation)

	// Round-trip through the Kubernetes types' own JSON marshaling into a
	// generic tree so every embedded custom type (resource.Quantity, etc.)
	// serializes exactly as the API does, then hand the tree to the canonical
	// CBOR encoder. UseNumber keeps integers exact rather than float64. Both
	// passes are deterministic, so the whole normalization is stable.
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return nil, fmt.Errorf("encode normalized pod template: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var tree any
	if err := decoder.Decode(&tree); err != nil {
		return nil, fmt.Errorf("decode normalized pod template: %w", err)
	}
	return tree, nil
}

// contractDomainKey builds an unambiguous, length-framed HMAC key from the
// domain-separation tuple. Every component is prefixed with its 8-byte
// big-endian length so no concatenation collision is possible.
func contractDomainKey(class PodClass, clusterUID string, shard, member int32, securityGeneration int64) []byte {
	parts := [][]byte{
		[]byte(contractHashDomain),
		[]byte(class),
		[]byte(clusterUID),
		[]byte(strconv.FormatInt(int64(shard), 10)),
		[]byte(strconv.FormatInt(int64(member), 10)),
		[]byte(strconv.FormatInt(securityGeneration, 10)),
	}
	var buffer bytes.Buffer
	var length [8]byte
	for _, part := range parts {
		binary.BigEndian.PutUint64(length[:], uint64(len(part)))
		buffer.Write(length[:])
		buffer.Write(part)
	}
	return buffer.Bytes()
}
