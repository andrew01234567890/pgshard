package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	RestoreManifestVersionV1 int32 = 1
	RoutingHashVersionV1     int32 = 1

	RestorePhasePending         RestorePhase = "Pending"
	RestorePhaseRejected        RestorePhase = "Rejected"
	RestorePhasePreflightPassed RestorePhase = "PreflightPassed"
)

type RestorePhase string

// PgShardRestoreSpec is immutable because every field is part of one restore
// attempt. ManifestSignature authenticates only Manifest; the destination and
// verification-key reference are immutable request inputs, not signed fields.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="restore specification is immutable"
// +kubebuilder:validation:XValidation:rule="!has(self.destinationTopology) || (self.destinationTopology.postgresqlMajor == self.manifest.topology.postgresqlMajor && self.destinationTopology.hashVersion == self.manifest.topology.hashVersion && self.destinationTopology.hashSeed == self.manifest.topology.hashSeed && self.destinationTopology.shardCount == self.manifest.topology.shardCount && self.destinationTopology.shards.size() == self.manifest.topology.shards.size())",message="RestoreTopologyMismatch: requested destination PostgreSQL, hash, and shard-count configuration must match the backup manifest"
type PgShardRestoreSpec struct {
	// Manifest is the immutable, signed backup-set projection. Restore execution
	// will fetch this projection from repository metadata; the preflight API keeps
	// its fields as a strongly typed object and signs their versioned canonical
	// binary encoding.
	Manifest RestoreManifest `json:"manifest"`

	// ManifestSignature is canonical RFC 4648 base64 containing one Ed25519
	// signature over the version-1 canonical binary encoding of Manifest.
	// +kubebuilder:validation:MinLength=88
	// +kubebuilder:validation:MaxLength=88
	ManifestSignature string `json:"manifestSignature"`

	// VerificationKeySecretRef names an immutable Opaque Secret in this
	// namespace whose only key is ed25519.pub containing exactly 32 raw bytes.
	VerificationKeySecretRef corev1.LocalObjectReference `json:"verificationKeySecretRef"`

	// DestinationDatabase is the configurable restore name. The execution slice
	// will require it to be absent or reserved but non-serving.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	DestinationDatabase string `json:"destinationDatabase"`

	// DestinationTopology is an optional caller expectation. Omission never
	// proves that the destination is absent. Before a restore may proceed, the
	// controller must independently prove from authoritative catalog state that
	// the destination is absent or has exactly the manifest topology.
	DestinationTopology *RestoreTopology `json:"destinationTopology,omitempty"`
}

type RestoreManifest struct {
	// +kubebuilder:validation:Enum=1
	ManifestVersion int32 `json:"manifestVersion"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	BackupSetID string `json:"backupSetID"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	SourceDatabase string          `json:"sourceDatabase"`
	Topology       RestoreTopology `json:"topology"`
}

// RestoreTopology is the complete logical topology identity. Placement,
// database UUIDs, physical cells, and Kubernetes Nodes are deliberately absent.
type RestoreTopology struct {
	// +kubebuilder:validation:Enum="18"
	PostgreSQLMajor string `json:"postgresqlMajor"`
	// +kubebuilder:validation:Enum=1
	HashVersion int32 `json:"hashVersion"`
	// HashSeed is canonical unsigned decimal so the full u64 domain survives
	// Kubernetes JSON and JavaScript clients without precision loss.
	// +kubebuilder:validation:MaxLength=20
	// +kubebuilder:validation:Pattern=`^(0|[1-9][0-9]{0,19})$`
	HashSeed string `json:"hashSeed"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=128
	ShardCount int32 `json:"shardCount"`
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=128
	// Order is signed and semantically significant.
	// +listType=atomic
	Shards []RestoreShardRange `json:"shards"`
}

type RestoreShardRange struct {
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=127
	Ordinal int32 `json:"ordinal"`
	// Start and End are canonical unsigned decimal half-open boundaries. End may
	// equal 2^64 for the final range.
	// +kubebuilder:validation:MaxLength=20
	// +kubebuilder:validation:Pattern=`^(0|[1-9][0-9]{0,19})$`
	Start string `json:"start"`
	// +kubebuilder:validation:MaxLength=20
	// +kubebuilder:validation:Pattern=`^(0|[1-9][0-9]{0,19})$`
	End string `json:"end"`
}

type PgShardRestoreStatus struct {
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +kubebuilder:validation:Enum=Pending;Rejected;PreflightPassed
	Phase RestorePhase `json:"phase,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// ManifestSHA256 binds status to the verified canonical manifest.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	ManifestSHA256 string `json:"manifestSHA256,omitempty"`
	// TopologySHA256 is the exact logical topology fingerprint.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	TopologySHA256 string `json:"topologySHA256,omitempty"`
	// DestinationTopologySHA256 records the authoritative destination topology
	// compared during preflight. It is empty when absence has been proven or no
	// authoritative destination comparison has completed.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	DestinationTopologySHA256 string `json:"destinationTopologySHA256,omitempty"`
	// VerificationKeyUID binds preflight to the exact immutable public-key
	// Secret. Execution must revalidate this identity before any mutation.
	VerificationKeyUID types.UID `json:"verificationKeyUID,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=pgsr
// +kubebuilder:printcolumn:name="Destination",type=string,JSONPath=`.spec.destinationDatabase`
// +kubebuilder:printcolumn:name="Shards",type=integer,JSONPath=`.spec.manifest.topology.shardCount`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=='Ready')].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type PgShardRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              PgShardRestoreSpec   `json:"spec,omitempty"`
	Status            PgShardRestoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type PgShardRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PgShardRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PgShardRestore{}, &PgShardRestoreList{})
}
