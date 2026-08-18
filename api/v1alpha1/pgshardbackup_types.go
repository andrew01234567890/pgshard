package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ObjectStoreSpec locates a backup repository.
type ObjectStoreSpec struct {
	// +kubebuilder:validation:Enum=s3;azure;gcs;posix;sftp
	Type string `json:"type"`
	// +optional
	Bucket string `json:"bucket,omitempty"`
	// +optional
	Container string `json:"container,omitempty"`
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
	// +optional
	Region string `json:"region,omitempty"`
	// +optional
	Prefix string `json:"prefix,omitempty"`
	// +optional
	Credentials SecretRefSpec `json:"credentials,omitempty"`
	// +optional
	Encryption SecretRefSpec `json:"encryption,omitempty"`
}

// SecretRefSpec wraps an optional Secret reference.
type SecretRefSpec struct {
	// +optional
	SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`
}

// BackupSchedules holds cron expressions per backup type.
type BackupSchedules struct {
	// +optional
	Full string `json:"full,omitempty"`
	// +optional
	Differential string `json:"differential,omitempty"`
	// +optional
	Incremental string `json:"incremental,omitempty"`
}

// BackupRetention counts backups to keep per type.
type BackupRetention struct {
	// +kubebuilder:validation:Minimum=1
	// +optional
	Full int `json:"full,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +optional
	Differential int `json:"differential,omitempty"`
}

// PgShardBackupPolicySpec is the desired backup policy.
type PgShardBackupPolicySpec struct {
	ObjectStore ObjectStoreSpec `json:"objectStore"`
	// +optional
	Schedules BackupSchedules `json:"schedules,omitempty"`
	// +optional
	Retention BackupRetention `json:"retention,omitempty"`
}

// PgShardBackupPolicy defines where and when clusters are backed up.
// +kubebuilder:object:root=true
// +kubebuilder:printcolumn:name="Store",type=string,JSONPath=`.spec.objectStore.type`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type PgShardBackupPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec PgShardBackupPolicySpec `json:"spec"`
}

// PgShardBackupPolicyList is a list of PgShardBackupPolicy.
// +kubebuilder:object:root=true
type PgShardBackupPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PgShardBackupPolicy `json:"items"`
}

// PgShardBackupSpec requests one backup of a cluster.
type PgShardBackupSpec struct {
	ClusterName string `json:"clusterName"`
	// +kubebuilder:validation:Enum=full;differential;incremental
	// +kubebuilder:default=full
	// +optional
	Type string `json:"type,omitempty"`
}

// PgShardBackupStatus is the observed state of a backup.
type PgShardBackupStatus struct {
	// +optional
	Phase string `json:"phase,omitempty"`
	// +optional
	BackupID string `json:"backupId,omitempty"`
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// PgShardBackup is a single backup run.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterName`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type PgShardBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec PgShardBackupSpec `json:"spec"`
	// +optional
	Status PgShardBackupStatus `json:"status,omitempty"`
}

// PgShardBackupList is a list of PgShardBackup.
// +kubebuilder:object:root=true
type PgShardBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PgShardBackup `json:"items"`
}

// RestoreTarget selects the recovery point.
// +kubebuilder:validation:XValidation:rule="[has(self.time), has(self.lsn), has(self.name), has(self.xid), has(self.immediate) && self.immediate].filter(x, x).size() <= 1",message="at most one of target.time, target.lsn, target.name, target.xid, target.immediate may be set"
type RestoreTarget struct {
	// +optional
	Time *metav1.Time `json:"time,omitempty"`
	// +optional
	LSN *string `json:"lsn,omitempty"`
	// +optional
	Name *string `json:"name,omitempty"`
	// +optional
	XID *string `json:"xid,omitempty"`
	// +optional
	Immediate *bool `json:"immediate,omitempty"`
}

// PgShardRestoreSpec requests a restore.
type PgShardRestoreSpec struct {
	ClusterName string `json:"clusterName"`
	// +optional
	BackupID string `json:"backupId,omitempty"`
	// +optional
	Target RestoreTarget `json:"target,omitempty"`
	// +optional
	TargetTLI *int64 `json:"targetTLI,omitempty"`
	// +optional
	Exclusive bool `json:"exclusive,omitempty"`
}

// PgShardRestoreStatus is the observed state of a restore.
type PgShardRestoreStatus struct {
	// +optional
	Phase string `json:"phase,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// PgShardRestore is a restore run.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterName`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type PgShardRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec PgShardRestoreSpec `json:"spec"`
	// +optional
	Status PgShardRestoreStatus `json:"status,omitempty"`
}

// PgShardRestoreList is a list of PgShardRestore.
// +kubebuilder:object:root=true
type PgShardRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PgShardRestore `json:"items"`
}

func init() {
	register(
		&PgShardBackupPolicy{}, &PgShardBackupPolicyList{},
		&PgShardBackup{}, &PgShardBackupList{},
		&PgShardRestore{}, &PgShardRestoreList{},
	)
}
