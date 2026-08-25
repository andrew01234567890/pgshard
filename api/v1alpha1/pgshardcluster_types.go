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
	ConditionRolloutInProgress  = "RolloutInProgress"
)

// Rollout phases reported in status.rollout.phase.
const (
	RolloutPhaseIdle       = "Idle"
	RolloutPhaseReloading  = "Reloading"
	RolloutPhaseRestarting = "Restarting"
	RolloutPhaseSwitchover = "Switchover"
	RolloutPhaseRebuilding = "Rebuilding"
	RolloutPhaseExpanding  = "Expanding"
	RolloutPhaseHeld       = "Held"
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
	// MinSyncStandbys is the number of synchronous standby acknowledgements
	// every commit requires (ANY n). It must be at least 1: asynchronous
	// durability (0) is not supported, because automatic failover could then
	// promote a standby that never acknowledged a committed write and lose it.
	// +kubebuilder:validation:Minimum=1
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

// InternalTLSSpec configures router<->pooler transport security. Plaintext
// is never the default: it must be requested with insecure, which is
// unsupported outside development environments.
// +kubebuilder:validation:XValidation:rule="(has(self.secretRef) && self.secretRef.name != \"\") || (has(self.insecure) && self.insecure)",message="internalTLS requires secretRef, or insecure: true to explicitly opt into plaintext (unsupported outside development)"
// +kubebuilder:validation:XValidation:rule="!((has(self.secretRef) && self.secretRef.name != \"\") && has(self.insecure) && self.insecure)",message="internalTLS.secretRef and internalTLS.insecure are mutually exclusive"
type InternalTLSSpec struct {
	// SecretRef names a Secret with tls.crt, tls.key and ca.crt; the
	// poolers refuse clients whose certificate does not chain to ca.crt
	// and the routers dial with the certificate.
	// +optional
	SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`
	// Insecure runs the pooler port as plaintext gRPC. Unsupported outside
	// development; the network must be protected by other means.
	// +optional
	Insecure bool `json:"insecure,omitempty"`
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
// +kubebuilder:validation:XValidation:rule="self.replicasPerShard >= 3 || (has(self.unsafeSingleReplica) && self.unsafeSingleReplica)",message="replicasPerShard must be >= 3"
// +kubebuilder:validation:XValidation:rule="self.catalog.replicas >= 3 || (has(self.unsafeSingleReplica) && self.unsafeSingleReplica)",message="catalog.replicas must be >= 3 for HA"
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
	ReplicasPerShard int `json:"replicasPerShard,omitempty"`
	// UnsafeSingleReplica relaxes the replicasPerShard and catalog.replicas
	// >= 3 requirement so test and development clusters can run one member
	// per group. Single-member groups have no synchronous standby and no
	// failover candidate: unsupported for production.
	// +optional
	UnsafeSingleReplica bool        `json:"unsafeSingleReplica,omitempty"`
	Storage             StorageSpec `json:"storage"`
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
	// InternalTLS configures mutual TLS between routers and the pooler
	// sidecars. Either secretRef (a Secret with tls.crt, tls.key and
	// ca.crt) or the explicit insecure override must be set; there is no
	// implicit plaintext fallback.
	InternalTLS InternalTLSSpec `json:"internalTLS"`
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
	// PVC is the claim the member's data directory lives on.
	// +optional
	PVC string `json:"pvc,omitempty"`
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

// RolloutStatus summarises the rolling operation in flight across groups.
type RolloutStatus struct {
	// +optional
	Phase string `json:"phase,omitempty"`
	// Member is the member being reloaded, restarted or rebuilt.
	// +optional
	Member string `json:"member,omitempty"`
	// +optional
	Reason string `json:"reason,omitempty"`
	// LastRestartToken is the pgshard.io/restart value the last completed
	// whole-cluster restart carried.
	// +optional
	LastRestartToken string `json:"lastRestartToken,omitempty"`
}

// ClusterCatalogUpgradeStatus is the catalog group's blue/green major
// upgrade in flight: it runs after every shard set reached the new major.
type ClusterCatalogUpgradeStatus struct {
	FromMajor int `json:"fromMajor"`
	ToMajor   int `json:"toMajor"`
	// Generation names the new-major catalog group (catalog-g<n>).
	Generation int64 `json:"generation"`
	// Stage is one of provisioning, copying, catching_up, cutover, retiring.
	// +optional
	Stage string `json:"stage,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// Blockers lists the preconditions holding the upgrade back.
	// +optional
	Blockers []string `json:"blockers,omitempty"`
	// RetiredGeneration is the old catalog group kept up for rollback
	// after the cutover, until retirement deletes it.
	// +optional
	RetiredGeneration int64 `json:"retiredGeneration,omitempty"`
	// +optional
	RetiredMajor int `json:"retiredMajor,omitempty"`
	// +optional
	SwitchedAt *metav1.Time `json:"switchedAt,omitempty"`
	// RollbackRequested mirrors the pgshard.io/catalog-upgrade=rollback
	// annotation.
	// +optional
	RollbackRequested bool `json:"rollbackRequested,omitempty"`
}

// ClusterReshardStatus points at the reshard in flight.
type ClusterReshardStatus struct {
	// Name is the PgShardReshard object.
	Name string `json:"name"`
	// ShardSet is the pending catalog shard set; Generation its generation.
	ShardSet   string `json:"shardSet"`
	Generation int64  `json:"generation"`
	// Shards is the number of target groups being provisioned.
	Shards int `json:"shards"`
	// PGMajor is the PostgreSQL major of the pending set when the run is a
	// blue/green major upgrade; zero for a topology reshard.
	// +optional
	PGMajor int `json:"pgMajor,omitempty"`
	// +optional
	Phase string `json:"phase,omitempty"`
	// RetiredShardSet, RetiredGeneration and RetiredShards describe the
	// old set kept up for reverse replication after the write switch.
	// +optional
	RetiredShardSet string `json:"retiredShardSet,omitempty"`
	// +optional
	RetiredGeneration int64 `json:"retiredGeneration,omitempty"`
	// RetiredPGMajor is the major the retired groups still run.
	// +optional
	RetiredPGMajor int `json:"retiredPGMajor,omitempty"`
	// +optional
	RetiredShards int `json:"retiredShards,omitempty"`
}

// ClusterPlacementWorkflowStatus is one table placement workflow the
// controller runs (a shard key change, or a move between unsharded,
// sharded and reference placement).
type ClusterPlacementWorkflowStatus struct {
	// WorkflowID is the pgshard.workflows row.
	WorkflowID string `json:"workflowId"`
	// Table is the qualified table: database.schema.table.
	Table string `json:"table"`
	// From and To describe the placements, e.g. "sharded(tenant_id)".
	From string `json:"from"`
	To   string `json:"to"`
	// State is the workflow state; Phase its stage.
	State string `json:"state"`
	// +optional
	Phase string `json:"phase,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// PauseMS is the table-scoped write pause of the swap, once done.
	// +optional
	PauseMS int64 `json:"pauseMs,omitempty"`
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
	// EffectiveShards is the shard count of the serving shard set as
	// materialized in the catalog; zero until the catalog exists.
	// +optional
	EffectiveShards int `json:"effectiveShards,omitempty"`
	// ServingGeneration is the generation of the serving shard set; it
	// names the serving shard groups (shard-<id> for 1, shard-<id>-g<n>
	// after).
	// +optional
	ServingGeneration int64 `json:"servingGeneration,omitempty"`
	// ServingPGMajor is the PostgreSQL major the serving set's groups run,
	// as stamped in the catalog; zero when the catalog predates upgrades.
	// +optional
	ServingPGMajor int `json:"servingPGMajor,omitempty"`
	// Reshard is the resharding run in flight, if any.
	// +optional
	Reshard *ClusterReshardStatus `json:"reshard,omitempty"`
	// CatalogGeneration names the active catalog group (catalog for 0 or
	// 1, catalog-g<n> after a catalog major upgrade).
	// +optional
	CatalogGeneration int64 `json:"catalogGeneration,omitempty"`
	// CatalogPGMajor is the PostgreSQL major the active catalog group
	// runs, probed from the server; zero until first probed.
	// +optional
	CatalogPGMajor int `json:"catalogPGMajor,omitempty"`
	// CatalogUpgrade is the catalog group upgrade in flight, if any.
	// +optional
	CatalogUpgrade *ClusterCatalogUpgradeStatus `json:"catalogUpgrade,omitempty"`
	// PlacementWorkflows lists the table placement workflows that are
	// active or ended since the last completed one was observed.
	// +optional
	// +listType=map
	// +listMapKey=workflowId
	PlacementWorkflows []ClusterPlacementWorkflowStatus `json:"placementWorkflows,omitempty"`
	// +optional
	Tuning TuningStatus `json:"tuning,omitempty"`
	// +optional
	Rollout RolloutStatus `json:"rollout,omitempty"`
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
