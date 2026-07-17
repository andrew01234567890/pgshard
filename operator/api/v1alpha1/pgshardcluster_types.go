package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	PostgreSQLMajor18 = "18"
	MaximumShards     = 128
	// MaximumClusterNameLength preserves the public API limit from the first
	// operator release. Longer workload identities are bounded independently.
	MaximumClusterNameLength = 50

	DurabilitySynchronous  DurabilityMode = "Synchronous"
	DurabilityAsynchronous DurabilityMode = "Asynchronous"

	ScalingHPA   PoolerScalingMode = "HPA"
	ScalingFixed PoolerScalingMode = "Fixed"

	RepositoryS3         BackupRepositoryType = "S3"
	RepositoryFilesystem BackupRepositoryType = "Filesystem"

	DeletionRetain StorageDeletionPolicy = "Retain"
	DeletionDelete StorageDeletionPolicy = "Delete"
)

type DurabilityMode string
type PoolerScalingMode string
type BackupRepositoryType string
type StorageDeletionPolicy string

// PgShardClusterSpec describes one namespaced pgshard installation.
// +kubebuilder:validation:XValidation:rule="self.shards == oldSelf.shards",message="shards is immutable until online resharding is implemented"
// +kubebuilder:validation:XValidation:rule="self.membersPerShard == oldSelf.membersPerShard",message="membersPerShard is immutable until membership transitions are implemented"
// +kubebuilder:validation:XValidation:rule="self.durability == oldSelf.durability",message="durability is immutable until replication-mode transitions are implemented"
type PgShardClusterSpec struct {
	// Shards is the number of logical hash ranges. The catalog remains on shard-0000.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=128
	// +kubebuilder:default=1
	Shards int32 `json:"shards,omitempty"`

	// MembersPerShard is the number of physical PostgreSQL members per shard.
	// Milestone 1 supports the safe odd values 1, 3, and 5.
	// +kubebuilder:validation:Enum=1;3;5
	// +kubebuilder:default=3
	MembersPerShard int32 `json:"membersPerShard,omitempty"`

	// Durability defaults to synchronous replication. Asynchronous is an explicit
	// durability downgrade and never disables PostgreSQL local WAL durability.
	// +kubebuilder:validation:Enum=Synchronous;Asynchronous
	// +kubebuilder:default=Synchronous
	Durability DurabilityMode `json:"durability,omitempty"`

	PostgreSQL    PostgreSQLSpec    `json:"postgresql"`
	Storage       StorageSpec       `json:"storage"`
	Pooler        PoolerSpec        `json:"pooler,omitempty"`
	Services      ServiceSet        `json:"services,omitempty"`
	Backup        BackupSpec        `json:"backup"`
	Observability ObservabilitySpec `json:"observability,omitempty"`

	// Databases reserves the shared-topology database names. Database lifecycle
	// will move to PgShardDatabase without changing the cluster topology.
	// +listType=map
	// +listMapKey=name
	Databases []DatabaseTemplate `json:"databases,omitempty"`
}

type PostgreSQLSpec struct {
	// Version is the PostgreSQL major. Only PostgreSQL 18 is accepted.
	// +kubebuilder:validation:Enum="18"
	// +kubebuilder:default="18"
	Version string `json:"version,omitempty"`

	// Resources must contain positive CPU and memory requests and limits.
	Resources corev1.ResourceRequirements `json:"resources"`

	// Parameters contains a deliberately small set of safe tuning overrides.
	// Durability-critical and operator-owned settings are rejected.
	Parameters map[string]string `json:"parameters,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="!oldSelf.hasValue() ? quantity(self.size).compareTo(quantity('4Gi')) >= 0 : quantity(self.size).compareTo(quantity(oldSelf.value().size)) == 0 || (quantity(oldSelf.value().size).compareTo(quantity('4Gi')) < 0 && quantity(self.size).compareTo(quantity('4Gi')) >= 0)",message="storage size is immutable except for a one-time upgrade from a legacy size below 4Gi to at least 4Gi",optionalOldSelf=true
// +kubebuilder:validation:XValidation:rule="has(self.storageClassName) == has(oldSelf.storageClassName) && (!has(self.storageClassName) || self.storageClassName == oldSelf.storageClassName)",message="storage class is immutable after cluster creation"
// +kubebuilder:validation:XValidation:rule="self.deletionPolicy == oldSelf.deletionPolicy",message="deletion policy is immutable after cluster creation"
type StorageSpec struct {
	// Size is the capacity of each PostgreSQL data volume.
	Size resource.Quantity `json:"size"`
	// StorageClassName is used for PostgreSQL data volumes and the supporting
	// etcd quorum's independently sized volumes.
	StorageClassName *string `json:"storageClassName,omitempty"`
	// DeletionPolicy controls PostgreSQL data PVC handling when the cluster is
	// deleted. Retain is the safe default; Delete must be selected explicitly.
	// +kubebuilder:validation:Enum=Retain;Delete
	// +kubebuilder:default=Retain
	DeletionPolicy StorageDeletionPolicy `json:"deletionPolicy,omitempty"`
}

type DatabaseTemplate struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

type PoolerSpec struct {
	Scaling PoolerScaling `json:"scaling,omitempty"`
}

type PoolerScaling struct {
	// +kubebuilder:validation:Enum=HPA;Fixed
	// +kubebuilder:default=HPA
	Mode  PoolerScalingMode `json:"mode,omitempty"`
	HPA   *HPAScaling       `json:"hpa,omitempty"`
	Fixed *FixedScaling     `json:"fixed,omitempty"`
}

type HPAScaling struct {
	// +kubebuilder:validation:Minimum=2
	// +kubebuilder:default=2
	MinReplicas int32 `json:"minReplicas,omitempty"`
	// +kubebuilder:validation:Minimum=2
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=10
	MaxReplicas int32 `json:"maxReplicas,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=65
	TargetCPUUtilizationPercentage int32 `json:"targetCPUUtilizationPercentage,omitempty"`
}

type FixedScaling struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	Replicas int32 `json:"replicas"`
}

type ServiceSet struct {
	ReadWrite ServiceTemplate `json:"rw,omitempty"`
	ReadOnly  ServiceTemplate `json:"ro,omitempty"`
	Read      ServiceTemplate `json:"r,omitempty"`
}

type ServiceTemplate struct {
	// +kubebuilder:validation:Enum=ClusterIP;NodePort;LoadBalancer
	// +kubebuilder:default=ClusterIP
	Type        corev1.ServiceType `json:"type,omitempty"`
	Annotations map[string]string  `json:"annotations,omitempty"`
}

type BackupSpec struct {
	Repository BackupRepository `json:"repository"`
}

type BackupRepository struct {
	// +kubebuilder:validation:Enum=S3;Filesystem
	Type       BackupRepositoryType  `json:"type"`
	S3         *S3Repository         `json:"s3,omitempty"`
	Filesystem *FilesystemRepository `json:"filesystem,omitempty"`
}

type S3Repository struct {
	Bucket               string                      `json:"bucket"`
	Endpoint             string                      `json:"endpoint,omitempty"`
	Region               string                      `json:"region,omitempty"`
	Prefix               string                      `json:"prefix,omitempty"`
	CredentialsSecretRef corev1.LocalObjectReference `json:"credentialsSecretRef"`
}

type FilesystemRepository struct {
	PersistentVolumeClaimName string `json:"persistentVolumeClaimName"`
}

type ObservabilitySpec struct {
	// +kubebuilder:default=true
	Prometheus *bool `json:"prometheus,omitempty"`
	// +kubebuilder:default=false
	ServiceMonitor        bool   `json:"serviceMonitor,omitempty"`
	OpenTelemetryEndpoint string `json:"openTelemetryEndpoint,omitempty"`
}

// PgShardClusterStatus never reports readiness until the operator has actually
// reconciled and observed the data plane.
type PgShardClusterStatus struct {
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +kubebuilder:validation:Enum=Pending;Reconciling;Ready;Degraded
	Phase string `json:"phase,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// PostgreSQLBootstrapSpec records the topology and storage contract before
	// any PostgreSQL credential or data volume is created. It provides a
	// defensive fence when admission is unavailable or bypassed.
	PostgreSQLBootstrapSpec *PostgreSQLBootstrapSpecStatus `json:"postgresqlBootstrapSpec,omitempty"`
	// PostgreSQLBootstraps records the API-assigned Secret and PVC identities for
	// each shard. Missing or replaced children require an explicit recovery;
	// the controller never silently adopts or regenerates them.
	// +listType=map
	// +listMapKey=shard
	PostgreSQLBootstraps []PostgreSQLBootstrapStatus `json:"postgresqlBootstraps,omitempty"`
}

// PostgreSQLBootstrapSpecStatus is the provisioned data-plane contract.
type PostgreSQLBootstrapSpecStatus struct {
	Shards           int32                 `json:"shards"`
	MembersPerShard  int32                 `json:"membersPerShard"`
	Durability       DurabilityMode        `json:"durability"`
	StorageSize      string                `json:"storageSize"`
	StorageClassName *string               `json:"storageClassName,omitempty"`
	DeletionPolicy   StorageDeletionPolicy `json:"deletionPolicy"`
}

// PostgreSQLBootstrapStatus binds one shard to randomly named, API-identified
// bootstrap resources. Names are durable creation intents; UIDs are filled only
// after the API server confirms each child. PVCFenceDetached is checkpointed
// only after the credential Secret has been detached from cluster garbage
// collection, making that exact Secret UID a durable owner for any outcome-
// unknown PVC create. After PVCUID is checkpointed, the controller protects and
// detaches that exact live PVC and anchors the Secret tombstone back to its UID.
// Retain finalization can instead record an absent uncheckpointed outcome as
// abandoned, after which no later outcome is retained. A workload cannot
// consume an incomplete, abandoned, or unstabilized record.
type PostgreSQLBootstrapStatus struct {
	// +kubebuilder:validation:Minimum=0
	Shard      int32     `json:"shard"`
	SecretName string    `json:"secretName"`
	SecretUID  types.UID `json:"secretUID"`
	// PVCFenceDetached proves the exact credential Secret is independent of the
	// cluster and may be used as the PVC's creation-intent owner.
	PVCFenceDetached bool      `json:"pvcFenceDetached,omitempty"`
	PVCName          string    `json:"pvcName,omitempty"`
	PVCUID           types.UID `json:"pvcUID,omitempty"`
	// PVCCreationAbandoned is set only during Retain finalization after an
	// authoritative read finds no PVC before its UID was checkpointed. No
	// workload can use an uncheckpointed claim. The controller never recreates
	// or retains a later outcome from this creation intent; the detached Secret
	// remains its garbage-collection fence until finalization removes it.
	PVCCreationAbandoned bool `json:"pvcCreationAbandoned,omitempty"`
	// PVCStorageClassName records the explicit or operator-resolved class before
	// the PVC create is dispatched, including an explicitly empty class.
	PVCStorageClassName *string `json:"pvcStorageClassName,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=pgsc
// +kubebuilder:printcolumn:name="Shards",type=integer,JSONPath=`.spec.shards`
// +kubebuilder:printcolumn:name="Members",type=integer,JSONPath=`.spec.membersPerShard`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=='Ready')].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type PgShardCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              PgShardClusterSpec   `json:"spec,omitempty"`
	Status            PgShardClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type PgShardClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PgShardCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PgShardCluster{}, &PgShardClusterList{})
}
