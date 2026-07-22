package v1alpha1

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	PostgreSQLMajor18 = "18"
	MaximumShards     = 128
	MaximumDatabases  = 512
	// MaximumTotalRoutingRanges is derived from the two structural CRD bounds.
	// Keep the equality explicit so the API cannot admit more routes than the
	// bounded pgshard-catalog snapshot loader can hold.
	MaximumTotalRoutingRanges = MaximumDatabases * MaximumShards
	MaximumEndpointLength     = 2_048
	MaximumS3BucketLength     = 255
	MaximumS3RegionLength     = 128
	MaximumS3PrefixLength     = 1_024
	// MaximumClusterNameLength preserves the public API limit from the first
	// operator release. Longer workload identities are bounded independently.
	MaximumClusterNameLength = 50
	// maximumPostgreSQLWorkloadPrefixLength leaves room for the shard, member,
	// Pod ordinal, and role suffixes appended to PostgreSQL workload identities.
	maximumPostgreSQLWorkloadPrefixLength = 42
	postgresqlWorkloadDigestBytes         = 12

	DurabilitySynchronous  DurabilityMode = "Synchronous"
	DurabilityAsynchronous DurabilityMode = "Asynchronous"

	ScalingHPA   PoolerScalingMode = "HPA"
	ScalingFixed PoolerScalingMode = "Fixed"

	RepositoryS3         BackupRepositoryType = "S3"
	RepositoryFilesystem BackupRepositoryType = "Filesystem"

	DeletionRetain StorageDeletionPolicy = "Retain"
	DeletionDelete StorageDeletionPolicy = "Delete"
)

// PostgreSQLWorkloadPrefix returns the collision-resistant bounded prefix
// shared by every PostgreSQL workload and its coordination identities. Hash
// names at the boundary too so a longer name cannot alias an accepted literal
// 42-byte cluster name.
func PostgreSQLWorkloadPrefix(cluster string) string {
	if len(cluster) < maximumPostgreSQLWorkloadPrefixLength {
		return cluster
	}
	digest := sha256.Sum256([]byte(cluster))
	suffix := hex.EncodeToString(digest[:postgresqlWorkloadDigestBytes])
	return cluster[:maximumPostgreSQLWorkloadPrefixLength-len(suffix)-1] + "-" + suffix
}

// PostgreSQLShardStatefulSetName returns the stable role-neutral workload name.
func PostgreSQLShardStatefulSetName(cluster string, shard int32) string {
	return fmt.Sprintf("%s-shard-%04d", PostgreSQLWorkloadPrefix(cluster), shard)
}

// PostgreSQLWritableLeaseName returns the exact writable-term Lease name.
func PostgreSQLWritableLeaseName(cluster string, shard int32) string {
	return PostgreSQLShardStatefulSetName(cluster, shard) + "-term"
}

// PostgreSQLAgentServiceAccountName returns the exact writable agent identity.
func PostgreSQLAgentServiceAccountName(cluster string, shard int32) string {
	return PostgreSQLShardStatefulSetName(cluster, shard) + "-agent"
}

type DurabilityMode string
type PoolerScalingMode string
type BackupRepositoryType string
type StorageDeletionPolicy string

// PgShardClusterSpec describes one namespaced pgshard installation.
// +kubebuilder:validation:XValidation:rule="self.shards == oldSelf.shards",message="shards is immutable until physical cell transitions are implemented"
// +kubebuilder:validation:XValidation:rule="self.membersPerShard == oldSelf.membersPerShard",message="membersPerShard is immutable until membership transitions are implemented"
// +kubebuilder:validation:XValidation:rule="self.durability == oldSelf.durability",message="durability is immutable until replication-mode transitions are implemented"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.databases) ? !has(self.databases) || size(self.databases) == 0 : has(self.databases) && sets.equivalent(self.databases.map(database, database.name), oldSelf.databases.map(database, database.name))",message="databases is immutable until database lifecycle and online resharding are implemented"
type PgShardClusterSpec struct {
	// Shards is the number of physical PostgreSQL cells in the foundation API.
	// Each logical database maps its independently ordered hash ranges onto a
	// subset of these cells. The catalog remains on physical cell zero.
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

	// Databases declares immutable genesis database topologies. Database
	// lifecycle will move to PgShardDatabase without changing this placement
	// contract.
	// +kubebuilder:validation:MaxItems=512
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
	// StorageClassName is used for PostgreSQL data volumes.
	StorageClassName *string `json:"storageClassName,omitempty"`
	// DeletionPolicy controls PostgreSQL data PVC handling when the cluster is
	// deleted. Retain is the safe default; Delete must be selected explicitly.
	// +kubebuilder:validation:Enum=Retain;Delete
	// +kubebuilder:default=Retain
	DeletionPolicy StorageDeletionPolicy `json:"deletionPolicy,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="!(self.name in ['postgres', 'shardschema', 'template0', 'template1'])",message="database name is reserved by PostgreSQL or pgshard"
// +kubebuilder:validation:XValidation:rule="self == oldSelf || (!has(oldSelf.shards) && !has(oldSelf.cells) && has(self.shards) && has(self.cells) && self.shards == size(self.cells) && self.cells.all(cell, cell == self.cells.indexOf(cell)))",message="database topology is immutable except for exact materialization of legacy defaults"
type DatabaseTemplate struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`

	// Shards is this database's logical shard count. Zero is defaulted to the
	// number of explicitly selected cells, or to every cluster cell when cells
	// is also omitted.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=128
	Shards int32 `json:"shards,omitempty"`

	// Cells maps logical shard ordinal i to one exact physical cell ordinal.
	// Omitting it selects the first Shards cells. Reusing cells across different
	// databases is explicit shared-cell placement; cells within one database
	// must be unique.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=128
	// +kubebuilder:validation:items:Minimum=0
	// +kubebuilder:validation:items:Maximum=127
	Cells []int32 `json:"cells,omitempty"`
}

// ResolvedShardCount returns the database shard count after applying the
// admission default contract. Validation must still prove it is in range.
func (database DatabaseTemplate) ResolvedShardCount(clusterCells int32) int32 {
	if database.Shards != 0 {
		return database.Shards
	}
	if database.Cells != nil {
		return int32(len(database.Cells))
	}
	return clusterCells
}

// ResolvedCells returns an owned copy of the exact physical-cell placement
// after applying the admission default contract.
func (database DatabaseTemplate) ResolvedCells(clusterCells int32) []int32 {
	if database.Cells != nil {
		return append([]int32(nil), database.Cells...)
	}
	count := database.ResolvedShardCount(clusterCells)
	if count <= 0 || count > MaximumShards {
		return nil
	}
	cells := make([]int32, count)
	for ordinal := range cells {
		cells[ordinal] = int32(ordinal)
	}
	return cells
}

// DatabaseTopologySHA256 returns a canonical digest of every resolved immutable
// database name and ordered physical-cell placement.
func (spec PgShardClusterSpec) DatabaseTopologySHA256() string {
	databases := append([]DatabaseTemplate(nil), spec.Databases...)
	sort.Slice(databases, func(left, right int) bool {
		return databases[left].Name < databases[right].Name
	})
	hash := sha256.New()
	_, _ = hash.Write([]byte("pgshard-database-topology-v1\x00"))
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], uint32(len(databases)))
	_, _ = hash.Write(encoded[:])
	for _, database := range databases {
		binary.BigEndian.PutUint32(encoded[:], uint32(len(database.Name)))
		_, _ = hash.Write(encoded[:])
		_, _ = hash.Write([]byte(database.Name))
		cells := database.ResolvedCells(spec.Shards)
		binary.BigEndian.PutUint32(encoded[:], uint32(len(cells)))
		_, _ = hash.Write(encoded[:])
		for _, cell := range cells {
			binary.BigEndian.PutUint32(encoded[:], uint32(cell))
			_, _ = hash.Write(encoded[:])
		}
	}
	return hex.EncodeToString(hash.Sum(nil))
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
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=255
	Bucket string `json:"bucket"`
	// +kubebuilder:validation:MaxLength=2048
	Endpoint string `json:"endpoint,omitempty"`
	// +kubebuilder:validation:MaxLength=128
	Region string `json:"region,omitempty"`
	// +kubebuilder:validation:MaxLength=1024
	Prefix               string                      `json:"prefix,omitempty"`
	CredentialsSecretRef corev1.LocalObjectReference `json:"credentialsSecretRef"`
}

type FilesystemRepository struct {
	// +kubebuilder:validation:MaxLength=253
	PersistentVolumeClaimName string `json:"persistentVolumeClaimName"`
}

type ObservabilitySpec struct {
	// +kubebuilder:default=true
	Prometheus *bool `json:"prometheus,omitempty"`
	// +kubebuilder:default=false
	ServiceMonitor bool `json:"serviceMonitor,omitempty"`
	// +kubebuilder:validation:MaxLength=2048
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
	// each physical shard member. Missing or replaced children require an
	// explicit recovery; the controller never silently adopts or regenerates
	// them.
	// +listType=map
	// +listMapKey=shard
	// +listMapKey=member
	PostgreSQLBootstraps []PostgreSQLBootstrapStatus `json:"postgresqlBootstraps,omitempty"`
	// PostgreSQLWritableLeases binds every physical cell to the exact
	// operator-owned Kubernetes Lease used for writable-term coordination. A
	// missing or recreated Lease is a new coordination universe and requires
	// explicit recovery; the controller never silently adopts its replacement.
	// +listType=map
	// +listMapKey=shard
	PostgreSQLWritableLeases []PostgreSQLWritableLeaseStatus `json:"postgresqlWritableLeases,omitempty"`
	// PostgreSQLReplicationCredentials records one staged, immutable physical-
	// replication credential per shard. Multi-member workloads may consume a
	// credential only after its API UID and material digest are checkpointed.
	// Missing, replaced, or changed Secrets require explicit recovery.
	// +listType=map
	// +listMapKey=shard
	PostgreSQLReplicationCredentials []PostgreSQLReplicationCredentialStatus `json:"postgresqlReplicationCredentials,omitempty"`
	// PostgreSQLReplicationTLS records one staged replication CA and one server
	// certificate Secret per shard member. Multi-member workloads may reference
	// a shard's Secrets only after every member digest and the CA digest are
	// checkpointed. Missing, replaced, or changed Secrets require explicit
	// recovery.
	// +listType=map
	// +listMapKey=shard
	PostgreSQLReplicationTLS []PostgreSQLReplicationTLSStatus `json:"postgresqlReplicationTLS,omitempty"`
	// PostgreSQLConfiguration pins the exact immutable, content-addressed
	// PostgreSQL ConfigMap selected for a future shard-zero catalog
	// materialization attempt. It is recorded before candidate documents are
	// published so their execution bundle can bind both API incarnation and
	// canonical data bytes.
	PostgreSQLConfiguration *PostgreSQLConfigurationStatus `json:"postgresqlConfiguration,omitempty"`
	// PostgreSQLCatalogCandidates binds every shard-zero member to one immutable,
	// cluster-aware catalog bootstrap configuration. These ConfigMaps are not
	// mounted by the current non-serving workloads. Missing or recreated
	// checkpointed objects require explicit recovery.
	// +kubebuilder:validation:MaxItems=5
	// +listType=map
	// +listMapKey=member
	PostgreSQLCatalogCandidates []PostgreSQLCatalogCandidateStatus `json:"postgresqlCatalogCandidates,omitempty"`
	// CatalogAccess records the staged creation and API identity of the
	// catalog-only credential and TLS Secret. The operator first creates an empty
	// non-consumable Secret, checkpoints its UID, then atomically installs
	// immutable material and checkpoints its digests. A missing or replaced
	// checkpointed Secret requires explicit recovery.
	CatalogAccess *CatalogAccessStatus `json:"catalogAccess,omitempty"`
	// OperationWriterAccess records a separately projected, staged catalog
	// operation-writer credential. It is not mounted by any orchestrator until
	// a later connector slice is composed.
	OperationWriterAccess *OperationWriterAccessStatus `json:"operationWriterAccess,omitempty"`
	// CatalogActivation pins the one operator-created, initially empty carrier
	// for a future authenticated catalog materialization request. A missing or
	// replaced carrier is an explicit-recovery boundary and is never recreated.
	CatalogActivation *CatalogActivationCarrierStatus `json:"catalogActivation,omitempty"`
	// PostgreSQLMemberContracts records the reconciler-stamped full-contract
	// hash and monotonic security generation for each member StatefulSet's pod
	// template. The controller propagates the stamp to every pod it creates;
	// admission later validates a pod against its live owning parent's stamp.
	// The barrier/generation-bump logic is not yet implemented — the generation
	// is stamped at its current value only.
	// +listType=map
	// +listMapKey=shard
	// +listMapKey=member
	PostgreSQLMemberContracts []PostgreSQLMemberContractStatus `json:"postgresqlMemberContracts,omitempty"`
	// SupportingContracts records the reconciler-stamped full-contract hash and
	// security generation for each supporting workload class (pooler,
	// orchestrator).
	// +listType=map
	// +listMapKey=class
	SupportingContracts []SupportingContractStatus `json:"supportingContracts,omitempty"`
	// SupportingGenerations records the per-class compare-and-set state machine
	// that decides which ReplicaSet generation of a supporting workload may
	// create pods, and the security-generation barrier below which new creates
	// are revoked. It is sealed before the owning Deployment is mutated and
	// recomputed deterministically from live ReplicaSet UIDs on manager restart.
	// +listType=map
	// +listMapKey=class
	SupportingGenerations []SupportingGenerationStatus `json:"supportingGenerations,omitempty"`
	// IsolationReceipt is the durable, namespace-UID-bound activation state
	// machine that flips per-namespace isolation enforcement from the legacy
	// pre-activation behavior (INACTIVE) to full deny-all enforcement (ACTIVE).
	// It is absent until activation begins; while absent, admission behaves
	// exactly as before activation.
	IsolationReceipt *PostgreSQLIsolationReceipt `json:"isolationReceipt,omitempty"`
}

// IsolationPhase is the durable phase of a namespace's isolation activation.
// +kubebuilder:validation:Enum=INACTIVE;ACTIVATING_QUIESCE;ACTIVATING_RECREATE;ACTIVE
type IsolationPhase string

const (
	// IsolationInactive is the pre-activation phase: isolation is not enforced and
	// admission behaves exactly as it did before activation (stampless and
	// unclassified pods pass the legacy path).
	IsolationInactive IsolationPhase = "INACTIVE"
	// IsolationActivatingQuiesce freezes the namespace: every pod and workload
	// create is denied while the reconciler seals every protected parent and
	// drains in-flight creates.
	IsolationActivatingQuiesce IsolationPhase = "ACTIVATING_QUIESCE"
	// IsolationActivatingRecreate admits a create only if its controller-owner
	// parent matches a sealed parent, while the reconciler deletes and
	// controller-recreates every protected pod so each is authenticated at its
	// guarded create.
	IsolationActivatingRecreate IsolationPhase = "ACTIVATING_RECREATE"
	// IsolationActive is full enforcement: every pod must carry a valid stamp,
	// classify, and pass the full contract with digest pinning; any unknown,
	// stampless, or unclassified pod is denied.
	IsolationActive IsolationPhase = "ACTIVE"
)

// PostgreSQLIsolationReceipt is the durable per-namespace isolation activation
// record. It is bound to the namespace UID so a recreated namespace cannot
// inherit an activation, and it seals the exact parent identities admission
// trusts during recreation.
type PostgreSQLIsolationReceipt struct {
	// +kubebuilder:validation:MinLength=1
	NamespaceUID string         `json:"namespaceUID"`
	Phase        IsolationPhase `json:"phase"`
	// +kubebuilder:validation:Minimum=0
	SecurityGeneration int64 `json:"securityGeneration,omitempty"`
	// MinAcceptableSecurityGeneration is the isolation-level floor: once ACTIVE, a
	// pod stamped below it is denied. It composes with (never lowers) the
	// per-class SupportingGeneration barrier.
	// +kubebuilder:validation:Minimum=0
	MinAcceptableSecurityGeneration int64 `json:"minAcceptableSecurityGeneration,omitempty"`
	// +kubebuilder:validation:Pattern=`^([0-9a-f]{64})?$`
	ResidueProfileHash string `json:"residueProfileHash,omitempty"`
	// DispatchTupleHash binds the receipt to the exact dispatch-convergence proof
	// tuple {webhook-config resourceVersion, backend EndpointSlice addresses and
	// their resourceVersions}. Any change to the tuple during activation
	// invalidates the in-progress proof and forces re-enumeration + re-proof; the
	// receipt is never advanced under a stale tuple.
	// +kubebuilder:validation:Pattern=`^([0-9a-f]{64})?$`
	DispatchTupleHash string `json:"dispatchTupleHash,omitempty"`
	// SealedParents are the exact protected-parent identities admission accepts
	// as create parents during ACTIVATING_RECREATE.
	// +listType=atomic
	SealedParents []SealedParent `json:"sealedParents,omitempty"`
	ActivatedAt   metav1.Time    `json:"activatedAt,omitempty"`
}

// IsolationActivationAnnotation is the opt-in trigger for per-namespace isolation
// activation. It is absent by default, so a cluster never activates unless an
// operator explicitly requests it; existing clusters and the KIND smoke are
// unaffected. Activation additionally requires the build to permit it and the
// full preflight (supported minor, controller-identity, dispatch-convergence) to
// pass.
const IsolationActivationAnnotation = "pgshard.io/activate-isolation"

// IsolationActivationRequested is the annotation value that opts a cluster into
// activation.
const IsolationActivationRequested = "requested"

// SealedParent is one protected parent workload sealed into the isolation
// receipt at its exact API incarnation and contract hash.
type SealedParent struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
	// +kubebuilder:validation:MinLength=1
	UID             string `json:"uid"`
	ResourceVersion string `json:"resourceVersion,omitempty"`
	// +kubebuilder:validation:Pattern=`^([0-9a-f]{64})?$`
	ContractHash string `json:"contractHash,omitempty"`
}

// PostgreSQLMemberContractStatus binds one member StatefulSet's pod-template
// contract hash to its stable physical identity and the security generation it
// was stamped at.
type PostgreSQLMemberContractStatus struct {
	// +kubebuilder:validation:Minimum=0
	Shard int32 `json:"shard"`
	// +kubebuilder:validation:Minimum=0
	Member int32 `json:"member"`
	// +kubebuilder:validation:Enum=source;standby;single-member
	Class string `json:"class"`
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	ContractHash string `json:"contractHash"`
	// +kubebuilder:validation:Minimum=1
	SecurityGeneration int64 `json:"securityGeneration"`
}

// SupportingContractStatus binds one supporting workload class's pod-template
// contract hash to the security generation it was stamped at.
type SupportingContractStatus struct {
	// +kubebuilder:validation:Enum=pooler;orchestrator
	Class string `json:"class"`
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	ContractHash string `json:"contractHash"`
	// +kubebuilder:validation:Minimum=1
	SecurityGeneration int64 `json:"securityGeneration"`
}

// SupportingGenerationStatus is the sealed compare-and-set record for one
// supporting workload class. CurrentReplicaSetUID and PriorReplicaSetUID are the
// only ReplicaSet generations whose pods admission accepts; a security roll
// advances MinGenerationForNewCreates so a prior (lower) generation is denied for
// new creates the instant the barrier is persisted, before the prior ReplicaSet
// is drained. PriorReplicaSetUID is cleared only once the prior generation is
// proven fully converged (drained to zero live pods).
type SupportingGenerationStatus struct {
	// +kubebuilder:validation:Enum=pooler;orchestrator
	Class string `json:"class"`
	// DeploymentUID is the owning Deployment's UID; a change means the Deployment
	// was recreated and the record must be rebuilt from scratch.
	DeploymentUID string `json:"deploymentUID,omitempty"`
	// CurrentReplicaSetUID is the live ReplicaSet whose template carries
	// CurrentContractHash. Empty until the first Bind.
	CurrentReplicaSetUID string `json:"currentReplicaSetUID,omitempty"`
	// CurrentTemplateGeneration is the security generation the current template
	// was stamped at.
	// +kubebuilder:validation:Minimum=0
	CurrentTemplateGeneration int64 `json:"currentTemplateGeneration,omitempty"`
	// +kubebuilder:validation:Pattern=`^([0-9a-f]{64})?$`
	CurrentContractHash string `json:"currentContractHash,omitempty"`
	// PriorReplicaSetUID is the previous generation's ReplicaSet, still admissible
	// during a bounded rollout; cleared on convergence.
	PriorReplicaSetUID string `json:"priorReplicaSetUID,omitempty"`
	// +kubebuilder:validation:Pattern=`^([0-9a-f]{64})?$`
	PriorContractHash string `json:"priorContractHash,omitempty"`
	// MinGenerationForNewCreates is the security-generation floor: a pod stamped
	// below it is denied for new creates and, if it lands, is a late write to be
	// deleted before convergence.
	// +kubebuilder:validation:Minimum=0
	MinGenerationForNewCreates int64 `json:"minGenerationForNewCreates,omitempty"`
	// ConvergedGeneration is the highest security generation proven fully drained.
	// +kubebuilder:validation:Minimum=0
	ConvergedGeneration int64 `json:"convergedGeneration,omitempty"`
	// SealedAt is when this record was last authoritatively written; the prior
	// generation's drain timer is measured from it.
	SealedAt metav1.Time `json:"sealedAt,omitempty"`
}

// CatalogActivationCarrierStatus binds the activation carrier's deterministic
// name to one exact API incarnation.
type CatalogActivationCarrierStatus struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	UID types.UID `json:"uid"`
}

// PostgreSQLConfigurationStatus binds generated PostgreSQL inputs to one
// immutable ConfigMap incarnation. DataSHA256 covers the canonical sorted
// ConfigMap data representation used in the content-addressed object name.
type PostgreSQLConfigurationStatus struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	ConfigMapName string `json:"configMapName"`
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	ConfigMapUID types.UID `json:"configMapUID"`
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	DataSHA256 string `json:"dataSHA256"`
}

// OperationWriterAccessStatus binds the future orchestrator writer credential
// to one immutable Secret and to the catalog CA without exposing either value.
type OperationWriterAccessStatus struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	SecretName string `json:"secretName"`
	// SecretUID is empty only before the empty Secret identity is observed.
	SecretUID types.UID `json:"secretUID,omitempty"`
	// MaterialSHA256 binds the exact password and catalog CA projections.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	MaterialSHA256 string `json:"materialSHA256,omitempty"`
}

// PostgreSQLCatalogCandidateStatus pins one shard-zero member's immutable
// catalog-bootstrap configuration to its API identity and exact payload. The
// payload contains only references to already checkpointed immutable inputs;
// it grants no serving role or writable authority.
type PostgreSQLCatalogCandidateStatus struct {
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=4
	Member int32 `json:"member"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	ConfigMapName string `json:"configMapName"`
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	ConfigMapUID types.UID `json:"configMapUID"`
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	PayloadSHA256 string `json:"payloadSHA256"`
}

// PostgreSQLReplicationCredentialStatus binds one shard to a staged shared
// physical-replication password. SecretUID is checkpointed while the Secret is
// still empty; MaterialSHA256 appears only after the same UID is immutable.
type PostgreSQLReplicationCredentialStatus struct {
	// +kubebuilder:validation:Minimum=0
	Shard int32 `json:"shard"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	SecretName string `json:"secretName"`
	// SecretUID is empty only before the empty Secret identity is observed.
	SecretUID types.UID `json:"secretUID,omitempty"`
	// MaterialSHA256 binds the exact password projection to the checkpointed
	// creation result.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	MaterialSHA256 string `json:"materialSHA256,omitempty"`
}

// PostgreSQLReplicationTLSMemberStatus binds one shard member to a staged
// server-certificate Secret. SecretUID is checkpointed while the Secret is
// still empty; ServerSHA256 and NotAfter appear only after the same UID holds
// immutable material issued by the shard's checkpointed CA.
type PostgreSQLReplicationTLSMemberStatus struct {
	// Member is a stable physical identity, never a mutable PostgreSQL role.
	// +kubebuilder:validation:Minimum=0
	Member int32 `json:"member"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	SecretName string `json:"secretName"`
	// SecretUID is empty only before the empty Secret identity is observed.
	SecretUID types.UID `json:"secretUID,omitempty"`
	// ServerSHA256 binds the exact server certificate and private-key
	// projection to the checkpointed creation result.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	ServerSHA256 string `json:"serverSHA256,omitempty"`
	// NotAfter records the server certificate expiry.
	NotAfter metav1.Time `json:"notAfter,omitempty"`
}

// PostgreSQLReplicationTLSStatus binds one shard to a staged replication CA
// and its per-member server certificates. The CA digest is checkpointed only
// after every member Secret holds validated immutable material.
type PostgreSQLReplicationTLSStatus struct {
	// +kubebuilder:validation:Minimum=0
	Shard int32 `json:"shard"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	CASecretName string `json:"caSecretName"`
	// CASecretUID is empty only before the empty Secret identity is observed.
	CASecretUID types.UID `json:"caSecretUID,omitempty"`
	// CASHA256 binds the exact CA certificate projection to the checkpointed
	// creation result.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	CASHA256 string `json:"caSHA256,omitempty"`
	// RenewalDeadline is the moment the operator starts refusing this material
	// instead of pretending it can rotate certificates without a restart.
	RenewalDeadline metav1.Time `json:"renewalDeadline,omitempty"`
	// +listType=map
	// +listMapKey=member
	Members []PostgreSQLReplicationTLSMemberStatus `json:"members,omitempty"`
}

// PostgreSQLWritableLeaseStatus pins one physical cell's writable-term Lease
// to its API-assigned identity. The name is deterministic and role-neutral;
// LeaseUID distinguishes deletion and recreation under that same name.
type PostgreSQLWritableLeaseStatus struct {
	// +kubebuilder:validation:Minimum=0
	Shard int32 `json:"shard"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	LeaseName string    `json:"leaseName"`
	LeaseUID  types.UID `json:"leaseUID"`
}

// CatalogAccessStatus binds the cluster to one staged catalog access Secret.
// Its name contains an unpredictable suffix. SecretUID is checkpointed while
// the Secret is still empty; material digests appear only after the same UID is
// made immutable. Workloads require all fields, so neither intermediate state
// is consumable.
type CatalogAccessStatus struct {
	SecretName string `json:"secretName"`
	// SecretUID is empty only before the empty Secret identity is observed.
	SecretUID types.UID `json:"secretUID,omitempty"`
	// ClientSHA256 binds the pooler's password and CA projection to the
	// checkpointed creation result.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	ClientSHA256 string `json:"clientSHA256,omitempty"`
	// ServerSHA256 binds the PostgreSQL serving certificate and private-key
	// projection to the checkpointed creation result.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	ServerSHA256 string `json:"serverSHA256,omitempty"`
}

// ReplicationTransportPolicyServerTLSV1 marks a multi-member cluster whose
// physical replication was born strictly server-authenticated: the operator
// stages the replication CA and per-member server certificates and every
// standby client hop requires verify-full TLS.
const ReplicationTransportPolicyServerTLSV1 = "server-tls-v1"

// PostgreSQLBootstrapSpecStatus is the provisioned data-plane contract. The
// runtime is checkpointed before any credential or data volume is created so
// deleted workload objects cannot erase the selected process composition.
type PostgreSQLBootstrapSpecStatus struct {
	Shards          int32          `json:"shards"`
	MembersPerShard int32          `json:"membersPerShard"`
	Durability      DurabilityMode `json:"durability"`
	// +kubebuilder:validation:Enum=direct;agent-quarantine
	PostgreSQLRuntime string `json:"postgresqlRuntime,omitempty"`
	// ReplicationTransportPolicy is stamped only when the bootstrap contract is
	// first recorded. Clusters provisioned before replication TLS existed have
	// no marker and are deliberately left untouched: they receive no TLS
	// Secrets and no workload-template change, and their physical replication
	// stays cleartext until the cluster is recreated under a marked contract.
	// +kubebuilder:validation:Enum=server-tls-v1
	ReplicationTransportPolicy string `json:"replicationTransportPolicy,omitempty"`
	// DatabaseTopologySHA256 binds provisioned storage to the complete resolved
	// immutable logical-database genesis topology. It is omitted only on status
	// written by releases that predate database-scoped genesis.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	DatabaseTopologySHA256 string                `json:"databaseTopologySHA256,omitempty"`
	StorageSize            string                `json:"storageSize"`
	StorageClassName       *string               `json:"storageClassName,omitempty"`
	DeletionPolicy         StorageDeletionPolicy `json:"deletionPolicy"`
}

// PostgreSQLBootstrapStatus binds one physical shard member to randomly named,
// API-identified bootstrap resources. Names are durable creation intents; UIDs
// are filled only after the API server confirms each child. PVCFenceDetached is
// checkpointed only after the credential Secret has been detached from cluster
// garbage collection, making that exact Secret UID a durable owner for any
// outcome-unknown PVC create. After PVCUID is checkpointed, the controller
// protects and detaches that exact live PVC and anchors the Secret tombstone
// back to its UID. Retain finalization can instead record an absent
// uncheckpointed outcome as abandoned, after which no later outcome is
// retained. A workload cannot consume an incomplete, abandoned, or
// unstabilized record.
type PostgreSQLBootstrapStatus struct {
	// +kubebuilder:validation:Minimum=0
	Shard int32 `json:"shard"`
	// Member is a stable physical identity, never a mutable PostgreSQL role.
	// +kubebuilder:validation:Minimum=0
	Member     int32     `json:"member"`
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
