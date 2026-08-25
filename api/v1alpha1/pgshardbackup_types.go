package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ObjectStoreSpec locates a backup repository.
type ObjectStoreSpec struct {
	// +kubebuilder:validation:Enum=s3;azure;gcs;posix;sftp
	Type string `json:"type"`
	// Bucket is the S3 or GCS bucket.
	// +optional
	Bucket string `json:"bucket,omitempty"`
	// Container is the Azure blob container.
	// +optional
	Container string `json:"container,omitempty"`
	// Endpoint overrides the store endpoint; an http:// or https:// URL or a
	// host[:port].
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
	// +optional
	Region string `json:"region,omitempty"`
	// Prefix is the path inside the store the repository lives under.
	// +optional
	Prefix string `json:"prefix,omitempty"`
	// URIStyle is host or path addressing for S3 and Azure.
	// +kubebuilder:validation:Enum=host;path
	// +optional
	URIStyle string `json:"uriStyle,omitempty"`
	// VerifyTLS defaults to true; false accepts self-signed store certificates.
	// +optional
	VerifyTLS *bool `json:"verifyTLS,omitempty"`
	// CredentialType selects how pgBackRest authenticates:
	// s3 shared|web-id|auto, azure shared|sas, gcs service|token|auto.
	// +kubebuilder:validation:Enum=shared;web-id;auto;sas;service;token
	// +optional
	CredentialType string `json:"credentialType,omitempty"`
	// Credentials names the Secret whose keys hold the store credentials:
	// s3 key/keySecret, azure account/key, gcs key.json, sftp privateKey.
	// +optional
	Credentials SecretRefSpec `json:"credentials,omitempty"`
	// Encryption names the Secret whose passphrase key encrypts the
	// repository with aes-256-cbc.
	// +optional
	Encryption SecretRefSpec `json:"encryption,omitempty"`
	// SFTP holds the host settings of an sftp store.
	// +optional
	SFTP *SFTPStoreSpec `json:"sftp,omitempty"`
}

// SFTPStoreSpec locates an sftp repository host.
type SFTPStoreSpec struct {
	Host string `json:"host"`
	User string `json:"user"`
	// +optional
	Port int `json:"port,omitempty"`
	// +kubebuilder:validation:Enum=strict;accept-new;fingerprint;none
	// +optional
	HostKeyCheck string `json:"hostKeyCheck,omitempty"`
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

// PgShardBackupPolicySpec is the desired backup policy. Clusters bind to it
// through spec.backup.policyRef.
type PgShardBackupPolicySpec struct {
	ObjectStore ObjectStoreSpec `json:"objectStore"`
	// +optional
	Schedules BackupSchedules `json:"schedules,omitempty"`
	// +optional
	Retention BackupRetention `json:"retention,omitempty"`
	// LogLevel is the pgBackRest log level (off, error, warn, info, detail,
	// debug, trace).
	// +kubebuilder:validation:Enum=off;error;warn;info;detail;debug;trace
	// +optional
	LogLevel string `json:"logLevel,omitempty"`
	// ProcessMax bounds pgBackRest parallelism per member.
	// +kubebuilder:validation:Minimum=1
	// +optional
	ProcessMax int `json:"processMax,omitempty"`
	// BarrierSchedule is a cron expression on which the operator asks each
	// bound cluster's controller for a certified barrier: a cluster-wide
	// restore point taken with writes paused and two-phase commits drained.
	// +optional
	BarrierSchedule string `json:"barrierSchedule,omitempty"`
	// ControllerEndpoint is the host:port of a cluster's Controller gRPC
	// service, with {cluster} and {namespace} substituted; the default is
	// {cluster}-controller.{namespace}.svc:15500.
	// +optional
	ControllerEndpoint string `json:"controllerEndpoint,omitempty"`
}

// ClusterBackupStatus is the backup health of one cluster bound to a policy.
type ClusterBackupStatus struct {
	Name string `json:"name"`
	// +optional
	LastFullTime *metav1.Time `json:"lastFullTime,omitempty"`
	// +optional
	LastDifferentialTime *metav1.Time `json:"lastDifferentialTime,omitempty"`
	// +optional
	LastIncrementalTime *metav1.Time `json:"lastIncrementalTime,omitempty"`
	Healthy             bool         `json:"healthy"`
	// +optional
	Message string `json:"message,omitempty"`
}

// PgShardBackupPolicyStatus is the observed state of a policy.
type PgShardBackupPolicyStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Clusters lists every cluster whose spec.backup.policyRef names this
	// policy with its last successful backups.
	// +optional
	Clusters []ClusterBackupStatus `json:"clusters,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// PgShardBackupPolicy defines where and when clusters are backed up.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Store",type=string,JSONPath=`.spec.objectStore.type`
// +kubebuilder:printcolumn:name="Healthy",type=string,JSONPath=`.status.conditions[?(@.type=="BackupHealthy")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type PgShardBackupPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec PgShardBackupPolicySpec `json:"spec"`
	// +optional
	Status PgShardBackupPolicyStatus `json:"status,omitempty"`
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

// Backup phases.
const (
	BackupPhasePending   = "Pending"
	BackupPhaseRunning   = "Running"
	BackupPhaseCompleted = "Completed"
	BackupPhaseFailed    = "Failed"
)

// GroupBackupStatus is the outcome of one group's pgBackRest backup.
type GroupBackupStatus struct {
	Group  string `json:"group"`
	Stanza string `json:"stanza"`
	// +optional
	BackupID string `json:"backupId,omitempty"`
	// +optional
	StartLSN string `json:"startLSN,omitempty"`
	// +optional
	StopLSN string `json:"stopLSN,omitempty"`
	// +optional
	WALStart string `json:"walStart,omitempty"`
	// +optional
	WALStop string `json:"walStop,omitempty"`
	// +optional
	SizeBytes int64 `json:"sizeBytes,omitempty"`
	// +optional
	RepoSizeBytes int64 `json:"repoSizeBytes,omitempty"`
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
	// +optional
	Duration string `json:"duration,omitempty"`
	// +optional
	Error string `json:"error,omitempty"`
}

// PgShardBackupStatus is the observed state of a backup.
type PgShardBackupStatus struct {
	// +optional
	Phase string `json:"phase,omitempty"`
	// BackupID is the catalog stanza's backup label once every group completed.
	// +optional
	BackupID string `json:"backupId,omitempty"`
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
	// +optional
	Groups []GroupBackupStatus `json:"groups,omitempty"`
	// +optional
	Error string `json:"error,omitempty"`
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
// +kubebuilder:validation:XValidation:rule="[has(self.time), has(self.lsn), has(self.name), has(self.xid), has(self.immediate) && self.immediate, has(self.barrier)].filter(x, x).size() <= 1",message="at most one of target.time, target.lsn, target.name, target.xid, target.immediate, target.barrier may be set"
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
	// Barrier names a certified barrier: every group recovers to the WAL
	// restore point pgshard-<barrier>, the new cluster stays write-fenced
	// until its prepared transactions are reconciled against the restored
	// decision log, and the restore fails on any contradiction.
	// +optional
	Barrier *string `json:"barrier,omitempty"`
}

// PgShardRestoreSpec requests a restore: a new cluster is created from the
// source cluster's repository and every group recovers to the same target.
// A barrier target is the only cluster-consistent one.
// +kubebuilder:validation:XValidation:rule="self.newClusterName != self.clusterName",message="newClusterName must differ from clusterName"
// +kubebuilder:validation:XValidation:rule="!(has(self.target) && (has(self.target.name) || has(self.target.xid) || has(self.target.barrier) || (has(self.target.immediate) && self.target.immediate))) || (has(self.backupId) && self.backupId.size() > 0)",message="target.name, target.xid, target.barrier and target.immediate require backupId"
type PgShardRestoreSpec struct {
	// ClusterName is the source cluster whose repository is restored from.
	ClusterName string `json:"clusterName"`
	// NewClusterName names the PgShardCluster the restore creates.
	// +kubebuilder:validation:MinLength=1
	NewClusterName string `json:"newClusterName"`
	// ClusterSpec is the spec of the new cluster; when unset the source
	// cluster's spec is copied. It must keep the source's shard count and
	// PostgreSQL major, and it must bind a backup policy that reaches the
	// source repository (the source policy when unset).
	// +optional
	ClusterSpec *PgShardClusterSpec `json:"clusterSpec,omitempty"`
	// BackupID pins the backup set: the name of a completed PgShardBackup of
	// the source cluster (each group restores its own set), or a raw
	// pgBackRest label applied to every group. Required for name, xid,
	// barrier and immediate targets (pick a backup taken before the
	// barrier); time and lsn targets select the set automatically.
	// +optional
	BackupID string `json:"backupId,omitempty"`
	// Target selects the recovery point; unset recovers to the end of the
	// archived WAL. The same target applies to every group.
	// +optional
	Target RestoreTarget `json:"target,omitempty"`
	// TargetTLI is the timeline to follow (recovery_target_timeline).
	// +kubebuilder:validation:Minimum=1
	// +optional
	TargetTLI *int64 `json:"targetTLI,omitempty"`
	// Exclusive stops recovery just before the target.
	// +optional
	Exclusive bool `json:"exclusive,omitempty"`
}

// Restore phases.
const (
	RestorePhasePending   = "Pending"
	RestorePhaseRestoring = "Restoring"
	// RestorePhaseReconciling: every group recovered to the barrier; the
	// prepared transactions are being finished against the decision log
	// while the cluster stays write-fenced.
	RestorePhaseReconciling = "Reconciling"
	RestorePhaseRecovered   = "Recovered"
	RestorePhaseFailed      = "Failed"
)

// RestoreReconciliationStatus is the outcome of finishing prepared
// transactions after a barrier restore.
type RestoreReconciliationStatus struct {
	// Decisions is the number of decision log rows applied.
	Decisions  int32 `json:"decisions"`
	Committed  int32 `json:"committed"`
	RolledBack int32 `json:"rolledBack"`
	// Contradictions lists "group: gid" pairs decided commit that the group
	// neither holds prepared nor committed; any entry fails the restore.
	// +optional
	Contradictions []string `json:"contradictions,omitempty"`
	// Unverifiable lists "group: gid" pairs decided commit that the group
	// cannot confirm or deny (its transaction id status is frozen, unrecorded
	// or in the future); any entry fails the restore exactly like a
	// contradiction, since the commit's presence cannot be proven.
	// +optional
	Unverifiable []string `json:"unverifiable,omitempty"`
	// Unfenced is true once the write fence of the new cluster was released.
	Unfenced bool `json:"unfenced"`
}

// ConditionPreparedTransactionsPending on a PgShardRestore is True when a
// recovered group still holds pgshard prepared transactions: a time, LSN,
// xid, immediate or name target is applied per group and is not
// cluster-consistent, so transactions prepared around it stay pending
// (locks held, vacuum horizon pinned) until an operator finishes them.
const ConditionPreparedTransactionsPending = "PreparedTransactionsPending"

// GroupRestoreStatus is the progress of one group of the new cluster.
type GroupRestoreStatus struct {
	Group string `json:"group"`
	// PreparedTransactions are the pgshard transaction ids the group still
	// holds prepared after recovery; only filled for non-barrier targets.
	// +optional
	PreparedTransactions []string `json:"preparedTransactions,omitempty"`
	// SourceStanza is the repository stanza the group restored from.
	SourceStanza string `json:"sourceStanza"`
	// +optional
	BackupID string `json:"backupId,omitempty"`
	// Timeline is the new primary's timeline once it left recovery.
	// +optional
	Timeline int64 `json:"timeline,omitempty"`
	// ReachedTarget is true once the group's primary finished recovery and
	// promoted; PostgreSQL refuses to promote before the target is reached.
	ReachedTarget bool `json:"reachedTarget"`
	// +optional
	Message string `json:"message,omitempty"`
}

// PgShardRestoreStatus is the observed state of a restore.
type PgShardRestoreStatus struct {
	// +optional
	Phase string `json:"phase,omitempty"`
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
	// +optional
	Groups []GroupRestoreStatus `json:"groups,omitempty"`
	// +optional
	Reconciliation *RestoreReconciliationStatus `json:"reconciliation,omitempty"`
	// +optional
	Error string `json:"error,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// PgShardRestore is a restore run.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Source",type=string,JSONPath=`.spec.clusterName`
// +kubebuilder:printcolumn:name="New",type=string,JSONPath=`.spec.newClusterName`
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
