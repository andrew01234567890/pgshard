package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// PgShardReshardSpec requests a change in shard count.
type PgShardReshardSpec struct {
	ClusterName string `json:"clusterName"`
	// +kubebuilder:validation:Minimum=1
	TargetShards int `json:"targetShards"`
}

// ReshardProgress reports copy and catch-up progress.
type ReshardProgress struct {
	// +optional
	RowsCopied int64 `json:"rowsCopied,omitempty"`
	// +optional
	RowsTotal int64 `json:"rowsTotal,omitempty"`
	// +optional
	ReplicationLagBytes int64 `json:"replicationLagBytes,omitempty"`
}

// PgShardReshardStatus is the observed state of a reshard.
type PgShardReshardStatus struct {
	// +optional
	Phase string `json:"phase,omitempty"`
	// +optional
	JournalIDs []string `json:"journalIds,omitempty"`
	// +optional
	Progress ReshardProgress `json:"progress,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// PgShardReshard is a resharding run.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterName`
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
