package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// Phases reported in PgShardReshard.status.phase.
const (
	ReshardPhasePending      = "Pending"
	ReshardPhaseProvisioning = "Provisioning"
	ReshardPhaseCopying      = "Copying"
	ReshardPhaseVerifying    = "Verifying"
	ReshardPhaseSwitching    = "Switching"
	ReshardPhaseCompleted    = "Completed"
	ReshardPhaseCancelled    = "Cancelled"
	ReshardPhaseFailed       = "Failed"
)

// Condition types reported on PgShardReshard.status.conditions.
const (
	ReshardConditionTargetsReady = "TargetsReady"
	ReshardConditionWorkflow     = "WorkflowCreated"
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
	// +kubebuilder:validation:Enum=Pending;Provisioning;Copying;Verifying;Switching;Completed;Cancelled;Failed
	// +optional
	Phase string `json:"phase,omitempty"`
	// WorkflowID is the pgshard.workflows row driving the reshard.
	// +optional
	WorkflowID string `json:"workflowId,omitempty"`
	// +optional
	Targets []ReshardTargetStatus `json:"targets,omitempty"`
	// +optional
	JournalIDs []string `json:"journalIds,omitempty"`
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
