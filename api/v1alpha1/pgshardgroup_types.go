package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// GroupRollout is the rolling operation in flight on one group.
type GroupRollout struct {
	Phase string `json:"phase"`
	// +optional
	Member string `json:"member,omitempty"`
	// +optional
	Reason string `json:"reason,omitempty"`
	// Since is when the current step began; the hold timeout counts from it.
	// +optional
	Since *metav1.Time `json:"since,omitempty"`
}

// PgShardGroupSpec identifies the replication group this object mirrors.
type PgShardGroupSpec struct {
	ClusterRef string `json:"clusterRef"`
	// +kubebuilder:validation:Enum=catalog;shard
	Kind string `json:"kind"`
	// +optional
	ShardID *int `json:"shardId,omitempty"`
}

// PgShardGroupStatus is the observed state of a replication group.
type PgShardGroupStatus struct {
	// +optional
	Primary string `json:"primary,omitempty"`
	// +optional
	Epoch int64 `json:"epoch,omitempty"`
	// +optional
	Members []MemberStatus `json:"members,omitempty"`
	// +optional
	Rollout *GroupRollout `json:"rollout,omitempty"`
	// SettingsRestartPending is set once a postmaster-context setting
	// changed; it stays until every member restarted with the new settings.
	// +optional
	SettingsRestartPending bool `json:"settingsRestartPending,omitempty"`
}

// PgShardGroup is a status-only view of one catalog or shard group.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Kind",type=string,JSONPath=`.spec.kind`
// +kubebuilder:printcolumn:name="Primary",type=string,JSONPath=`.status.primary`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type PgShardGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec PgShardGroupSpec `json:"spec"`
	// +optional
	Status PgShardGroupStatus `json:"status,omitempty"`
}

// PgShardGroupList is a list of PgShardGroup.
// +kubebuilder:object:root=true
type PgShardGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PgShardGroup `json:"items"`
}

func init() {
	register(&PgShardGroup{}, &PgShardGroupList{})
}
