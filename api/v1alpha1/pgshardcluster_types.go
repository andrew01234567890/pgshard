package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Condition types reported on PgShardCluster.status.conditions.
const (
	ConditionReady              = "Ready"
	ConditionProgressing        = "Progressing"
	ConditionDegraded           = "Degraded"
	ConditionPrimaryHealthy     = "PrimaryHealthy"
	ConditionReplicationHealthy = "ReplicationHealthy"
	ConditionFenced             = "Fenced"
	ConditionBackupHealthy      = "BackupHealthy"
	ConditionResharding         = "Resharding"
	ConditionServingWrites      = "ServingWrites"
	ConditionRouterReady        = "RouterReady"
	ConditionTuningApplied      = "TuningApplied"
)

// PostgreSQLSpec selects the PostgreSQL build and its base configuration.
type PostgreSQLSpec struct {
	// +kubebuilder:validation:Enum=18;19
	Major int `json:"major"`
	// +optional
	Image string `json:"image,omitempty"`
	// +kubebuilder:validation:Enum=oltp;mixed;analytics
	// +kubebuilder:default=oltp
	// +optional
	Profile string `json:"profile,omitempty"`
	// Parameters are extra postgresql.conf settings. Keys that pgshard owns
	// (fsync, full_page_writes, wal_level, max_prepared_transactions, ssl,
	// synchronous_commit) are rejected.
	// +kubebuilder:validation:XValidation:rule="!('fsync' in self)",message="parameters must not set fsync"
	// +kubebuilder:validation:XValidation:rule="!('full_page_writes' in self)",message="parameters must not set full_page_writes"
	// +kubebuilder:validation:XValidation:rule="!('wal_level' in self)",message="parameters must not set wal_level"
	// +kubebuilder:validation:XValidation:rule="!('max_prepared_transactions' in self)",message="parameters must not set max_prepared_transactions"
	// +kubebuilder:validation:XValidation:rule="!('ssl' in self)",message="parameters must not set ssl"
	// +kubebuilder:validation:XValidation:rule="!('synchronous_commit' in self)",message="parameters must not set synchronous_commit"
	// +optional
	Parameters map[string]string `json:"parameters,omitempty"`
}

// StorageSpec describes a persistent volume request.
type StorageSpec struct {
	Size resource.Quantity `json:"size"`
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`
}

// CatalogSpec configures the control-plane catalog group.
type CatalogSpec struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=3
	// +optional
	Replicas int         `json:"replicas,omitempty"`
	Storage  StorageSpec `json:"storage"`
}

// DurabilitySpec configures synchronous replication.
type DurabilitySpec struct {
	// +kubebuilder:validation:Enum=on;remote_apply
	// +kubebuilder:default=on
	// +optional
	SynchronousCommit string `json:"synchronousCommit,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=1
	// +optional
	MinSyncStandbys int `json:"minSyncStandbys,omitempty"`
}

// HPASpec configures router autoscaling.
type HPASpec struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=70
	// +optional
	CPUUtilization int `json:"cpuUtilization,omitempty"`
}

// TLSSpec references a TLS Secret.
type TLSSpec struct {
	// +optional
	SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`
}

// RouterSpec configures the stateless router deployment.
// +kubebuilder:validation:XValidation:rule="self.maxReplicas >= self.minReplicas",message="router.maxReplicas must be >= router.minReplicas"
type RouterSpec struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=2
	// +optional
	MinReplicas int `json:"minReplicas,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=10
	// +optional
	MaxReplicas int `json:"maxReplicas,omitempty"`
	// +kubebuilder:default={}
	// +optional
	HPA HPASpec `json:"hpa,omitempty"`
	// +optional
	TLS TLSSpec `json:"tls,omitempty"`
}

// AdminSpec toggles the admin API.
type AdminSpec struct {
	// +kubebuilder:default=true
	// +optional
	Enabled *bool `json:"enabled,omitempty"`
}

// BackupSpec links the cluster to a PgShardBackupPolicy.
type BackupSpec struct {
	// +optional
	PolicyRef string `json:"policyRef,omitempty"`
}

// ReshardingSpec controls resharding behaviour.
type ReshardingSpec struct {
	// +kubebuilder:default="24h"
	// +optional
	RetireOldGroupsAfter *metav1.Duration `json:"retireOldGroupsAfter,omitempty"`
	// +kubebuilder:validation:Enum=none;switchWrites;complete
	// +kubebuilder:default=none
	// +optional
	PauseBefore string `json:"pauseBefore,omitempty"`
}

// UpgradeSpec controls major-version upgrades.
type UpgradeSpec struct {
	// +kubebuilder:validation:Enum=online;offline
	// +kubebuilder:default=online
	// +optional
	Strategy string `json:"strategy,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	// +optional
	MaxParallelGroups int `json:"maxParallelGroups,omitempty"`
}

// PgShardClusterSpec is the desired state of a PgShardCluster.
// +kubebuilder:validation:XValidation:rule="self.replicasPerShard >= 3",message="replicasPerShard must be >= 3"
// +kubebuilder:validation:XValidation:rule="self.catalog.replicas >= 3",message="catalog.replicas must be >= 3 for HA"
type PgShardClusterSpec struct {
	PostgreSQL PostgreSQLSpec `json:"postgresql"`
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
	Catalog   CatalogSpec                 `json:"catalog"`
	// +kubebuilder:validation:Minimum=1
	// +optional
	Shards *int `json:"shards,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=3
	// +optional
	ReplicasPerShard int         `json:"replicasPerShard,omitempty"`
	Storage          StorageSpec `json:"storage"`
	// +kubebuilder:default={}
	// +optional
	Durability DurabilitySpec `json:"durability,omitempty"`
	// +kubebuilder:default={minReplicas: 2, maxReplicas: 10}
	// +optional
	Router RouterSpec `json:"router,omitempty"`
	// +kubebuilder:default={}
	// +optional
	Admin AdminSpec `json:"admin,omitempty"`
	// +optional
	Backup BackupSpec `json:"backup,omitempty"`
	// +kubebuilder:default={}
	// +optional
	Resharding ReshardingSpec `json:"resharding,omitempty"`
	// +kubebuilder:default={}
	// +optional
	Upgrade UpgradeSpec `json:"upgrade,omitempty"`
}

// MemberStatus reports one PostgreSQL instance in a group.
type MemberStatus struct {
	Name string `json:"name"`
	// +optional
	Role string `json:"role,omitempty"`
	// +optional
	Ready bool `json:"ready,omitempty"`
	// +optional
	ReplayLagBytes int64 `json:"replayLagBytes,omitempty"`
}

// ShardStatus reports one shard and its members.
type ShardStatus struct {
	ID int `json:"id"`
	// +optional
	RangeStart int64 `json:"rangeStart,omitempty"`
	// +optional
	RangeEnd int64 `json:"rangeEnd,omitempty"`
	// +optional
	Primary string `json:"primary,omitempty"`
	// +optional
	Epoch int64 `json:"epoch,omitempty"`
	// +optional
	Members []MemberStatus `json:"members,omitempty"`
}

// DerivedSetting is a PostgreSQL setting the operator computed.
type DerivedSetting struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	// +optional
	Reason string `json:"reason,omitempty"`
}

// TuningStatus lists the settings derived from the profile and resources.
type TuningStatus struct {
	// +optional
	Derived []DerivedSetting `json:"derived,omitempty"`
}

// PgShardClusterStatus is the observed state of a PgShardCluster.
type PgShardClusterStatus struct {
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	ShardMapGeneration int64 `json:"shardMapGeneration,omitempty"`
	// +optional
	Shards []ShardStatus `json:"shards,omitempty"`
	// +optional
	Tuning TuningStatus `json:"tuning,omitempty"`
}

// PgShardCluster is a sharded PostgreSQL cluster.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=psc
// +kubebuilder:printcolumn:name="Shards",type=integer,JSONPath=`.spec.shards`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type PgShardCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec PgShardClusterSpec `json:"spec"`
	// +optional
	Status PgShardClusterStatus `json:"status,omitempty"`
}

// PgShardClusterList is a list of PgShardCluster.
// +kubebuilder:object:root=true
type PgShardClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PgShardCluster `json:"items"`
}

func init() {
	register(&PgShardCluster{}, &PgShardClusterList{})
}
