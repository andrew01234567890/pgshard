package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// Phases reported in PgShardReshard.status.phase.
const (
	ReshardPhasePending      = "Pending"
	ReshardPhaseProvisioning = "Provisioning"
	ReshardPhaseCopying      = "Copying"
	ReshardPhaseVerifying    = "Verifying"
	ReshardPhaseSwitching    = "Switching"
	ReshardPhaseCompleting   = "Completing"
	ReshardPhaseCompleted    = "Completed"
	ReshardPhaseCancelled    = "Cancelled"
	ReshardPhaseFailed       = "Failed"
)

// Condition types reported on PgShardReshard.status.conditions.
const (
	ReshardConditionTargetsReady = "TargetsReady"
	ReshardConditionWorkflow     = "WorkflowCreated"
	ReshardConditionSwitched     = "WritesSwitched"
)

// AnnotationProceed on a PgShardReshard lists the pause points
// (switchWrites, complete) the operator may pass; comma-separated.
const AnnotationProceed = "pgshard.io/proceed"

// AnnotationUpgrade on a PgShardReshard controls an upgrade-mode run;
// "rollback" returns serving to the old-major groups while they are still
// current over reverse replication (before retirement).
const (
	AnnotationUpgrade     = "pgshard.io/upgrade"
	UpgradeActionRollback = "rollback"
)

// AnnotationRollback asks a switched run of ANY mode to return serving to
// the set it switched from, while reverse replication still keeps that set
// current and before retirement tears it down.
//
// The machinery was never upgrade-specific: the controller triggers on
// spec.Rollback at StageSwitched and its rollback path names no kind
// (internal/controller/cutover.go). Only the operator's mirroring was
// gated on mode == upgrade, so an ordinary reshard could be rolled back
// only by editing pgshard.workflows by hand -- during an incident, which
// is where that goes wrong.
//
// AnnotationUpgrade keeps working for upgrade runs; this is the name that
// does not lie about what it covers.
const AnnotationRollback = "pgshard.io/rollback"

// AnnotationCatalogUpgrade on a PgShardCluster controls the catalog
// group's major upgrade; "rollback" returns serving to the old-major
// catalog group before its retirement deletes it.
const AnnotationCatalogUpgrade = "pgshard.io/catalog-upgrade"

// Reshard modes.
const (
	ReshardModeReshard = "reshard"
	ReshardModeUpgrade = "upgrade"
)

// ReshardRange is one target shard and the inclusive keyspace interval it
// will own.
type ReshardRange struct {
	// +kubebuilder:validation:Minimum=0
	ShardID    int   `json:"shardId"`
	RangeStart int64 `json:"rangeStart"`
	RangeEnd   int64 `json:"rangeEnd"`
}

// PgShardReshardSpec records one change of the shard map: the generation it
// starts from and the target ranges of the pending shard set. The catalog
// table pgshard.shard_ranges is the source of truth; this object mirrors it.
// +kubebuilder:validation:XValidation:rule="size(self.targetRanges) == self.targetShards",message="targetRanges must list exactly targetShards ranges"
// +kubebuilder:validation:XValidation:rule="self.targetGeneration > self.fromGeneration",message="targetGeneration must be greater than fromGeneration"
type PgShardReshardSpec struct {
	ClusterName string `json:"clusterName"`
	// FromGeneration is the serving shard set generation being replaced.
	// +kubebuilder:validation:Minimum=1
	FromGeneration int64 `json:"fromGeneration"`
	// TargetGeneration is the pending shard set generation; its catalog
	// name is targetShardSet.
	// +kubebuilder:validation:Minimum=2
	TargetGeneration int64 `json:"targetGeneration"`
	// +kubebuilder:validation:MinLength=1
	TargetShardSet string `json:"targetShardSet"`
	// +kubebuilder:validation:Minimum=1
	TargetShards int `json:"targetShards"`
	// +kubebuilder:validation:MinItems=1
	TargetRanges []ReshardRange `json:"targetRanges"`
	// Mode is reshard (topology change) or upgrade (blue/green major
	// replacement with a 1:1 range map).
	// +kubebuilder:validation:Enum=reshard;upgrade
	// +kubebuilder:default=reshard
	// +optional
	Mode string `json:"mode,omitempty"`
	// TargetMajor is the PostgreSQL major the target groups run; set in
	// upgrade mode.
	// +optional
	TargetMajor int `json:"targetMajor,omitempty"`
}

// ReshardTargetStatus is the readiness of one target group.
type ReshardTargetStatus struct {
	ShardID int    `json:"shardId"`
	Group   string `json:"group"`
	// +optional
	Ready bool `json:"ready,omitempty"`
	// +optional
	Primary string `json:"primary,omitempty"`
}

// PgShardReshardStatus is the observed state of a reshard.
type PgShardReshardStatus struct {
	// +kubebuilder:validation:Enum=Pending;Provisioning;Copying;Verifying;Switching;Completing;Completed;Cancelled;Failed
	// +optional
	Phase string `json:"phase,omitempty"`
	// WorkflowID is the pgshard.workflows row driving the reshard.
	// +optional
	WorkflowID string `json:"workflowId,omitempty"`
	// +optional
	Targets []ReshardTargetStatus `json:"targets,omitempty"`
	// +optional
	JournalIDs []string `json:"journalIds,omitempty"`
	// CutoverPause is how long routers held writes: fence raised to new
	// map published.
	// +optional
	CutoverPause *metav1.Duration `json:"cutoverPause,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// PgShardReshard is the record of one resharding run.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterName`
// +kubebuilder:printcolumn:name="Shards",type=integer,JSONPath=`.spec.targetShards`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type PgShardReshard struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec PgShardReshardSpec `json:"spec"`
	// +optional
	Status PgShardReshardStatus `json:"status,omitempty"`
}

// PgShardReshardList is a list of PgShardReshard.
// +kubebuilder:object:root=true
type PgShardReshardList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PgShardReshard `json:"items"`
}

func init() {
	register(&PgShardReshard{}, &PgShardReshardList{})
}
