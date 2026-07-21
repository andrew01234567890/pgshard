package v1alpha1

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	CatalogActivationSuffix                      = "-catalog-activation"
	CatalogActivationRequestVersion              = "pgshard.catalog-activation-request.v1"
	CatalogActivationRequestDigestDomain         = "pgshard-catalog-activation-request-v1"
	CatalogActivationAcceptanceVersion           = "pgshard.catalog-activation-acceptance.v1"
	CatalogActivationPersistenceFsync            = "fsync"
	catalogActivationProcessIncarnationHexLength = 24
	catalogActivationManagedByLabel              = "app.kubernetes.io/managed-by"
	catalogActivationInstanceLabel               = "app.kubernetes.io/instance"
	catalogActivationComponentLabel              = "app.kubernetes.io/component"
	catalogActivationClusterLabel                = "pgshard.io/cluster"
	catalogActivationApplyOwnership              = "pgshard.io/apply-ownership"
	catalogActivationManagedByValue              = "pgshard-operator"
	catalogActivationApplyOwnershipVersion       = "v1"
)

// CatalogActivationObjectIdentity is an exact immutable Kubernetes object.
type CatalogActivationObjectIdentity struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_.:-]+$`
	UID types.UID `json:"uid"`
}

// CatalogActivationMaterialIdentity binds an immutable object and material.
type CatalogActivationMaterialIdentity struct {
	CatalogActivationObjectIdentity `json:",inline"`
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	MaterialSHA256 string `json:"materialSHA256"`
}

// CatalogActivationCatalogMaterialIdentity binds exact catalog TLS and client material.
type CatalogActivationCatalogMaterialIdentity struct {
	CatalogActivationObjectIdentity `json:",inline"`
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	ClientSHA256 string `json:"clientSHA256"`
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	ServerSHA256 string `json:"serverSHA256"`
}

// CatalogActivationCluster binds the exact PgShardCluster status snapshot.
type CatalogActivationCluster struct {
	CatalogActivationObjectIdentity `json:",inline"`
	// Namespace contains every namespaced object bound by the request.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Namespace string `json:"namespace"`
	// Generation is canonical unsigned decimal.
	// +kubebuilder:validation:MaxLength=20
	// +kubebuilder:validation:Pattern=`^(0|[1-9][0-9]{0,19})$`
	Generation string `json:"generation"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	ResourceVersion string `json:"resourceVersion"`
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	StatusSHA256 string `json:"statusSHA256"`
}

// CatalogActivationDispatcher binds the publisher Pod and elected term.
type CatalogActivationDispatcher struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	PodName string `json:"podName"`
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_.:-]+$`
	PodUID types.UID `json:"podUID"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	LeaseName string `json:"leaseName"`
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_.:-]+$`
	LeaseUID types.UID `json:"leaseUID"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	LeaseResourceVersion string `json:"leaseResourceVersion"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	LeaseHolder string `json:"leaseHolder"`
}

// CatalogActivationCandidate binds the selected immutable candidate document.
type CatalogActivationCandidate struct {
	CatalogActivationObjectIdentity `json:",inline"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	ResourceVersion string `json:"resourceVersion"`
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	PayloadSHA256 string `json:"payloadSHA256"`
}

// CatalogActivationBootstrap binds the target credential and data volume.
type CatalogActivationBootstrap struct {
	Secret CatalogActivationObjectIdentity `json:"secret"`
	PVC    CatalogActivationObjectIdentity `json:"pvc"`
}

// CatalogActivationWritableTerm binds the writable Lease and holder generation.
type CatalogActivationWritableTerm struct {
	CatalogActivationObjectIdentity `json:",inline"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	ResourceVersion string `json:"resourceVersion"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9._/-]+$`
	Holder string `json:"holder"`
	// +kubebuilder:validation:MaxLength=20
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]{0,19}$`
	Generation string `json:"generation"`
}

// CatalogActivationMaterials is the exact immutable materialization bundle.
type CatalogActivationMaterials struct {
	Replication             CatalogActivationMaterialIdentity        `json:"replication"`
	Catalog                 CatalogActivationCatalogMaterialIdentity `json:"catalog"`
	OperationWriter         CatalogActivationMaterialIdentity        `json:"operationWriter"`
	PostgreSQLConfiguration CatalogActivationMaterialIdentity        `json:"postgresqlConfiguration"`
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	MigrationSHA256 string `json:"migrationSHA256"`
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	GenesisSHA256 string `json:"genesisSHA256"`
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	PreflightSHA256 string `json:"preflightSHA256"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	ServingHBAVersion string `json:"servingHBAVersion"`
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	ServingHBASHA256 string `json:"servingHBASHA256"`
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	TargetTemplateSHA256 string `json:"targetTemplateSHA256"`
}

// CatalogActivationTargetFenceAcknowledgement binds the source fence proof.
type CatalogActivationTargetFenceAcknowledgement struct {
	// +kubebuilder:validation:MaxLength=20
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]{0,19}$`
	ObservedAtUnixMS string `json:"observedAtUnixMS"`
	// +kubebuilder:validation:MaxLength=20
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]{0,19}$`
	DeadlineBoottimeNS string `json:"deadlineBoottimeNS"`
	// +kubebuilder:validation:MaxLength=20
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]{0,19}$`
	RemainingValidityAtAckMS string `json:"remainingValidityAtAckMS"`
	// +kubebuilder:validation:MaxLength=20
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]{0,19}$`
	RemainingValidityAtReportMS string `json:"remainingValidityAtReportMS"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=4294967295
	ControlBackendPID int64 `json:"controlBackendPID"`
}

// CatalogActivationSource is the exact source incarnation and generation barrier.
type CatalogActivationSource struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	ClusterName string `json:"clusterName"`
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_.:-]+$`
	ClusterUID types.UID `json:"clusterUID"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	PodName string `json:"podName"`
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_.:-]+$`
	PodUID types.UID `json:"podUID"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=127
	Shard int32 `json:"shard"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=4
	Member int32 `json:"member"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	InstanceID string `json:"instanceID"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	BootID string `json:"bootID"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=4294967295
	PostmasterPID int64 `json:"postmasterPID"`
	// +kubebuilder:validation:MaxLength=20
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]{0,19}$`
	SystemIdentifier string `json:"systemIdentifier"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=4294967295
	Timeline int64 `json:"timeline"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=1024
	GenerationIdentity string `json:"generationIdentity"`
	// +kubebuilder:validation:MaxLength=20
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]{0,19}$`
	GenerationBarrierLSN       string                                      `json:"generationBarrierLSN"`
	TargetFenceAcknowledgement CatalogActivationTargetFenceAcknowledgement `json:"targetFenceAcknowledgement"`
}

// CatalogActivationRemoteApplyWitness is one complete standby apply proof.
type CatalogActivationRemoteApplyWitness struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	ClusterName string `json:"clusterName"`
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_.:-]+$`
	ClusterUID types.UID `json:"clusterUID"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	PodName string `json:"podName"`
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_.:-]+$`
	PodUID types.UID `json:"podUID"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=127
	Shard int32 `json:"shard"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=4
	Member int32 `json:"member"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	InstanceID string `json:"instanceID"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	BootID string `json:"bootID"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=4294967295
	PostmasterPID int64 `json:"postmasterPID"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	MemberSlotName string `json:"memberSlotName"`
	// +kubebuilder:validation:MaxLength=20
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]{0,19}$`
	SystemIdentifier string `json:"systemIdentifier"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=4294967295
	Timeline int64 `json:"timeline"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=1024
	GenerationIdentity string `json:"generationIdentity"`
	// +kubebuilder:validation:MaxLength=20
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]{0,19}$`
	GenerationBarrierLSN string `json:"generationBarrierLSN"`
	// +kubebuilder:validation:MaxLength=20
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]{0,19}$`
	ReceiveLSN string `json:"receiveLSN"`
	// +kubebuilder:validation:MaxLength=20
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]{0,19}$`
	ReplayLSN string `json:"replayLSN"`
}

// CatalogActivationRequest is one immutable, fully bound activation request.
// +kubebuilder:validation:XValidation:rule="self.carrier.name == self.cluster.name + '-catalog-activation'",message="carrier name must be the fixed cluster activation name"
// +kubebuilder:validation:XValidation:rule="self.dispatcher.leaseName == self.cluster.name + '-orch-lease'",message="dispatcher Lease must have the fixed cluster name"
// +kubebuilder:validation:XValidation:rule="self.dispatcher.leaseHolder.startsWith(self.dispatcher.podName + '/' + self.dispatcher.podUID + '/') && size(self.dispatcher.leaseHolder) == size(self.dispatcher.podName) + size(self.dispatcher.podUID) + 38",message="dispatcher Lease holder must bind the exact dispatcher Pod"
// +kubebuilder:validation:XValidation:rule="self.writableTerm.holder.startsWith(self.source.instanceID + '/' + self.source.podUID + '/') && size(self.writableTerm.holder) == size(self.source.instanceID) + size(self.source.podUID) + 26",message="writable Lease holder must bind the exact source process incarnation"
// +kubebuilder:validation:XValidation:rule="self.source.clusterName == self.cluster.name && self.source.clusterUID == self.cluster.uid && self.remoteApplyWitness.clusterName == self.cluster.name && self.remoteApplyWitness.clusterUID == self.cluster.uid",message="source and witness must bind the exact cluster identity"
// +kubebuilder:validation:XValidation:rule="self.source.shard == 0 && self.remoteApplyWitness.shard == 0 && self.source.member != self.remoteApplyWitness.member",message="activation requires distinct shard-zero source and witness members"
// +kubebuilder:validation:XValidation:rule="self.source.systemIdentifier == self.remoteApplyWitness.systemIdentifier && self.source.timeline == self.remoteApplyWitness.timeline && self.source.generationIdentity == self.remoteApplyWitness.generationIdentity && self.source.generationBarrierLSN == self.remoteApplyWitness.generationBarrierLSN",message="source and remote-apply witness must bind the same PostgreSQL generation"
type CatalogActivationRequest struct {
	// +kubebuilder:validation:Enum="pgshard.catalog-activation-request.v1"
	SchemaVersion      string                              `json:"schemaVersion"`
	Carrier            CatalogActivationObjectIdentity     `json:"carrier"`
	Cluster            CatalogActivationCluster            `json:"cluster"`
	Dispatcher         CatalogActivationDispatcher         `json:"dispatcher"`
	Candidate          CatalogActivationCandidate          `json:"candidate"`
	Bootstrap          CatalogActivationBootstrap          `json:"bootstrap"`
	WritableTerm       CatalogActivationWritableTerm       `json:"writableTerm"`
	Materials          CatalogActivationMaterials          `json:"materials"`
	Source             CatalogActivationSource             `json:"source"`
	RemoteApplyWitness CatalogActivationRemoteApplyWitness `json:"remoteApplyWitness"`
}

// PgShardCatalogActivationSpec is empty until a future orchestrator publishes
// one immutable request and its independently verifiable digest.
// +kubebuilder:validation:XValidation:rule="has(self.request) == has(self.requestSHA256)",message="request and requestSHA256 must be set together"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.request) || (has(self.request) && self.request == oldSelf.request && self.requestSHA256 == oldSelf.requestSHA256)",message="catalog activation request is set-once and immutable"
type PgShardCatalogActivationSpec struct {
	Request *CatalogActivationRequest `json:"request,omitempty"`
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	RequestSHA256 string `json:"requestSHA256,omitempty"`
}

// CatalogActivationAcceptance records only a durable exact-request acceptance.
type CatalogActivationAcceptance struct {
	// +kubebuilder:validation:Enum="pgshard.catalog-activation-acceptance.v1"
	SchemaVersion string `json:"schemaVersion"`
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_.:-]+$`
	CarrierUID types.UID `json:"carrierUID"`
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	RequestSHA256 string `json:"requestSHA256"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	TargetPodName string `json:"targetPodName"`
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_.:-]+$`
	TargetPodUID types.UID `json:"targetPodUID"`
	// Persistence is fsync only after the future consumer has durably installed
	// and re-read the exact request before acknowledging it.
	// +kubebuilder:validation:Enum=fsync
	Persistence string `json:"persistence"`
	// +kubebuilder:validation:MaxLength=20
	// +kubebuilder:validation:Pattern=`^(0|[1-9][0-9]{0,19})$`
	PersistedAtUnixMS string `json:"persistedAtUnixMS"`
}

// PgShardCatalogActivationStatus is empty until a future target agent records
// one immutable, fsync-backed acceptance.
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.acceptance) || (has(self.acceptance) && self.acceptance == oldSelf.acceptance)",message="catalog activation acceptance is set-once and immutable"
type PgShardCatalogActivationStatus struct {
	Acceptance *CatalogActivationAcceptance `json:"acceptance,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=pgsca
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.request.cluster.name`
// +kubebuilder:printcolumn:name="Requested",type=string,JSONPath=`.spec.requestSHA256`
// +kubebuilder:printcolumn:name="Accepted",type=string,JSONPath=`.status.acceptance.requestSHA256`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type PgShardCatalogActivation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              PgShardCatalogActivationSpec   `json:"spec,omitempty"`
	Status            PgShardCatalogActivationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type PgShardCatalogActivationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PgShardCatalogActivation `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PgShardCatalogActivation{}, &PgShardCatalogActivationList{})
}

// CatalogActivationName returns the fixed carrier name for one cluster.
func CatalogActivationName(cluster string) string { return cluster + CatalogActivationSuffix }

// EmptyCatalogActivation returns the exact operator-owned empty carrier.
func EmptyCatalogActivation(cluster *PgShardCluster) *PgShardCatalogActivation {
	controller := true
	blockDeletion := true
	return &PgShardCatalogActivation{ObjectMeta: metav1.ObjectMeta{
		Name:      CatalogActivationName(cluster.Name),
		Namespace: cluster.Namespace,
		Labels: map[string]string{
			"app.kubernetes.io/name":        "pgshard",
			catalogActivationManagedByLabel: catalogActivationManagedByValue,
			catalogActivationInstanceLabel:  cluster.Name,
			catalogActivationComponentLabel: "catalog-activation",
			catalogActivationClusterLabel:   cluster.Name,
		},
		Annotations: map[string]string{catalogActivationApplyOwnership: catalogActivationApplyOwnershipVersion},
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: GroupVersion.String(), Kind: "PgShardCluster", Name: cluster.Name, UID: cluster.UID,
			Controller: &controller, BlockOwnerDeletion: &blockDeletion,
		}},
	}}
}

// SHA256 returns the fixed-order, length-framed digest shared with pgshard-types.
func (request *CatalogActivationRequest) SHA256() (string, error) {
	if err := request.validate(); err != nil {
		return "", err
	}
	hash := sha256.New()
	writeActivationFrame(hash, CatalogActivationRequestDigestDomain)
	request.visitComponents(func(value string) { writeActivationFrame(hash, value) })
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (request *CatalogActivationRequest) validate() error {
	if request == nil || request.SchemaVersion != CatalogActivationRequestVersion {
		return fmt.Errorf("unsupported catalog activation request schema version")
	}
	texts := []string{
		request.Carrier.Name, request.Cluster.Name, request.Cluster.Namespace, request.Dispatcher.PodName, request.Dispatcher.LeaseName,
		request.Dispatcher.LeaseHolder, request.Candidate.Name, request.Bootstrap.Secret.Name, request.Bootstrap.PVC.Name,
		request.WritableTerm.Name, request.WritableTerm.Holder, request.Materials.Replication.Name, request.Materials.Catalog.Name,
		request.Materials.OperationWriter.Name, request.Materials.PostgreSQLConfiguration.Name, request.Materials.ServingHBAVersion,
		request.Source.ClusterName, request.Source.PodName, request.Source.InstanceID, request.Source.BootID,
		request.RemoteApplyWitness.ClusterName, request.RemoteApplyWitness.PodName, request.RemoteApplyWitness.InstanceID, request.RemoteApplyWitness.BootID,
		request.RemoteApplyWitness.MemberSlotName,
	}
	for _, value := range texts {
		if !validActivationText(value, 253) {
			return fmt.Errorf("catalog activation request contains an invalid bounded text field")
		}
	}
	for _, value := range []string{request.Cluster.ResourceVersion, request.Dispatcher.LeaseResourceVersion, request.Candidate.ResourceVersion, request.WritableTerm.ResourceVersion} {
		if !validActivationText(value, 256) {
			return fmt.Errorf("catalog activation request contains an invalid resource version")
		}
	}
	uids := []types.UID{
		request.Carrier.UID, request.Cluster.UID, request.Dispatcher.PodUID, request.Dispatcher.LeaseUID, request.Candidate.UID,
		request.Bootstrap.Secret.UID, request.Bootstrap.PVC.UID, request.WritableTerm.UID, request.Materials.Replication.UID,
		request.Materials.Catalog.UID, request.Materials.OperationWriter.UID, request.Materials.PostgreSQLConfiguration.UID,
		request.Source.ClusterUID, request.Source.PodUID, request.RemoteApplyWitness.ClusterUID, request.RemoteApplyWitness.PodUID,
	}
	for _, uid := range uids {
		if !validActivationUID(uid) {
			return fmt.Errorf("catalog activation request contains an invalid Kubernetes UID")
		}
	}
	digests := []string{
		request.Cluster.StatusSHA256, request.Candidate.PayloadSHA256, request.Materials.Replication.MaterialSHA256,
		request.Materials.Catalog.ClientSHA256, request.Materials.Catalog.ServerSHA256, request.Materials.OperationWriter.MaterialSHA256,
		request.Materials.PostgreSQLConfiguration.MaterialSHA256, request.Materials.MigrationSHA256, request.Materials.GenesisSHA256,
		request.Materials.PreflightSHA256, request.Materials.ServingHBASHA256, request.Materials.TargetTemplateSHA256,
	}
	for _, digest := range digests {
		if !validActivationDigest(digest) {
			return fmt.Errorf("catalog activation request contains an invalid SHA-256 digest")
		}
	}
	decimals := []string{
		request.Cluster.Generation, request.WritableTerm.Generation, request.Source.SystemIdentifier, request.Source.GenerationBarrierLSN,
		request.Source.TargetFenceAcknowledgement.ObservedAtUnixMS, request.Source.TargetFenceAcknowledgement.DeadlineBoottimeNS,
		request.Source.TargetFenceAcknowledgement.RemainingValidityAtAckMS, request.Source.TargetFenceAcknowledgement.RemainingValidityAtReportMS,
		request.RemoteApplyWitness.SystemIdentifier, request.RemoteApplyWitness.GenerationBarrierLSN,
		request.RemoteApplyWitness.ReceiveLSN, request.RemoteApplyWitness.ReplayLSN,
	}
	for _, decimal := range decimals {
		if !validActivationDecimal(decimal) {
			return fmt.Errorf("catalog activation request contains an invalid canonical decimal")
		}
	}
	expectedGenerationIdentity, err := request.expectedWritableGenerationIdentity()
	if err != nil {
		return err
	}
	systemIdentifier, _ := strconv.ParseUint(request.Source.SystemIdentifier, 10, 64)
	generationBarrierLSN, _ := strconv.ParseUint(request.Source.GenerationBarrierLSN, 10, 64)
	receiveLSN, _ := strconv.ParseUint(request.RemoteApplyWitness.ReceiveLSN, 10, 64)
	replayLSN, _ := strconv.ParseUint(request.RemoteApplyWitness.ReplayLSN, 10, 64)
	fenceObservedAt, _ := strconv.ParseUint(request.Source.TargetFenceAcknowledgement.ObservedAtUnixMS, 10, 64)
	fenceDeadline, _ := strconv.ParseUint(request.Source.TargetFenceAcknowledgement.DeadlineBoottimeNS, 10, 64)
	fenceRemainingAtAck, _ := strconv.ParseUint(request.Source.TargetFenceAcknowledgement.RemainingValidityAtAckMS, 10, 64)
	fenceRemainingAtReport, _ := strconv.ParseUint(request.Source.TargetFenceAcknowledgement.RemainingValidityAtReportMS, 10, 64)
	if request.Source.Shard < 0 || request.Source.Shard > 127 || request.RemoteApplyWitness.Shard < 0 || request.RemoteApplyWitness.Shard > 127 ||
		request.Source.Member < 0 || request.Source.Member > 4 || request.RemoteApplyWitness.Member < 0 || request.RemoteApplyWitness.Member > 4 ||
		request.Source.PostmasterPID < 1 || request.Source.PostmasterPID > 4294967295 || request.Source.Timeline < 1 || request.Source.Timeline > 4294967295 ||
		request.Source.TargetFenceAcknowledgement.ControlBackendPID < 1 || request.Source.TargetFenceAcknowledgement.ControlBackendPID > 4294967295 ||
		request.RemoteApplyWitness.PostmasterPID < 1 || request.RemoteApplyWitness.PostmasterPID > 4294967295 || request.RemoteApplyWitness.Timeline < 1 || request.RemoteApplyWitness.Timeline > 4294967295 {
		return fmt.Errorf("catalog activation request contains an invalid bounded integer")
	}
	if request.Carrier.Name != CatalogActivationName(request.Cluster.Name) ||
		request.Dispatcher.LeaseName != request.Cluster.Name+"-orch-lease" ||
		!catalogActivationDispatcherHolderBelongsToPod(request.Dispatcher.LeaseHolder, request.Dispatcher.PodName, request.Dispatcher.PodUID) ||
		request.WritableTerm.Name != PostgreSQLWritableLeaseName(request.Cluster.Name, 0) ||
		request.Source.ClusterName != request.Cluster.Name || request.Source.ClusterUID != request.Cluster.UID ||
		request.RemoteApplyWitness.ClusterName != request.Cluster.Name || request.RemoteApplyWitness.ClusterUID != request.Cluster.UID ||
		request.Source.Shard != 0 || request.RemoteApplyWitness.Shard != 0 || request.Source.Member == request.RemoteApplyWitness.Member ||
		request.Source.SystemIdentifier != request.RemoteApplyWitness.SystemIdentifier || request.Source.Timeline != request.RemoteApplyWitness.Timeline ||
		request.Source.GenerationIdentity != expectedGenerationIdentity || request.RemoteApplyWitness.GenerationIdentity != expectedGenerationIdentity ||
		!catalogActivationHolderBelongsToSource(request.WritableTerm.Holder, request.Source.InstanceID, request.Source.PodUID) ||
		request.Source.GenerationBarrierLSN != request.RemoteApplyWitness.GenerationBarrierLSN ||
		systemIdentifier == 0 || generationBarrierLSN == 0 || receiveLSN < generationBarrierLSN || replayLSN < generationBarrierLSN || receiveLSN < replayLSN ||
		fenceObservedAt == 0 || fenceDeadline == 0 || fenceRemainingAtAck == 0 || fenceRemainingAtReport == 0 || fenceRemainingAtReport > fenceRemainingAtAck {
		return fmt.Errorf("catalog activation request contains inconsistent topology bindings")
	}
	return nil
}

func catalogActivationDispatcherHolderBelongsToPod(holder, podName string, podUID types.UID) bool {
	parts := strings.Split(holder, "/")
	return len(parts) == 3 && parts[0] == podName && parts[1] == string(podUID) && validCatalogActivationUUIDV4(parts[2])
}

func validCatalogActivationUUIDV4(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' || value[14] != '4' || !strings.ContainsRune("89ab", rune(value[19])) {
		return false
	}
	for index, character := range []byte(value) {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			continue
		}
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func catalogActivationHolderBelongsToSource(holder, instanceID string, podUID types.UID) bool {
	parts := strings.Split(holder, "/")
	if len(parts) != 3 || parts[0] != instanceID || parts[1] != string(podUID) || len(parts[2]) != catalogActivationProcessIncarnationHexLength {
		return false
	}
	for _, value := range []byte(parts[2]) {
		if !(value >= '0' && value <= '9') && !(value >= 'a' && value <= 'f') {
			return false
		}
	}
	return true
}

func (request *CatalogActivationRequest) expectedWritableGenerationIdentity() (string, error) {
	term, err := strconv.ParseUint(request.WritableTerm.Generation, 10, 64)
	if err != nil || term == 0 {
		return "", fmt.Errorf("catalog activation request contains an invalid canonical decimal")
	}
	fields := []struct {
		value      string
		maximum    int
		allowSlash bool
	}{
		{request.Cluster.Name, 63, false},
		{string(request.Cluster.UID), 128, false},
		{request.Cluster.Namespace, 63, false},
		{request.WritableTerm.Name, 63, false},
		{string(request.WritableTerm.UID), 128, false},
		{request.WritableTerm.Holder, 128, true},
	}
	for _, field := range fields {
		if !validWritableGenerationField(field.value, field.maximum, field.allowSlash) {
			return "", fmt.Errorf("catalog activation request contains inconsistent topology bindings")
		}
	}
	return fmt.Sprintf(
		"format=1\ncluster_name=%s\ncluster_uid=%s\nshard=%d\nlease_namespace=%s\nlease_name=%s\nlease_uid=%s\nholder=%s\nterm=%d\n",
		request.Cluster.Name,
		request.Cluster.UID,
		request.Source.Shard,
		request.Cluster.Namespace,
		request.WritableTerm.Name,
		request.WritableTerm.UID,
		request.WritableTerm.Holder,
		term,
	), nil
}

func validWritableGenerationField(value string, maximum int, allowSlash bool) bool {
	if value == "" || len(value) > maximum {
		return false
	}
	for _, character := range []byte(value) {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '.' || character == '_' || character == '-' ||
			(allowSlash && character == '/') {
			continue
		}
		return false
	}
	return true
}

func (request *CatalogActivationRequest) visitComponents(visit func(string)) {
	for _, value := range []string{
		request.SchemaVersion, request.Carrier.Name, string(request.Carrier.UID), request.Cluster.Name, request.Cluster.Namespace, string(request.Cluster.UID),
		request.Cluster.Generation, request.Cluster.ResourceVersion, request.Cluster.StatusSHA256,
		request.Dispatcher.PodName, string(request.Dispatcher.PodUID), request.Dispatcher.LeaseName, string(request.Dispatcher.LeaseUID),
		request.Dispatcher.LeaseResourceVersion, request.Dispatcher.LeaseHolder,
		request.Candidate.Name, string(request.Candidate.UID), request.Candidate.ResourceVersion, request.Candidate.PayloadSHA256,
		request.Bootstrap.Secret.Name, string(request.Bootstrap.Secret.UID), request.Bootstrap.PVC.Name, string(request.Bootstrap.PVC.UID),
		request.WritableTerm.Name, string(request.WritableTerm.UID), request.WritableTerm.ResourceVersion, request.WritableTerm.Holder, request.WritableTerm.Generation,
		request.Materials.Replication.Name, string(request.Materials.Replication.UID), request.Materials.Replication.MaterialSHA256,
		request.Materials.Catalog.Name, string(request.Materials.Catalog.UID), request.Materials.Catalog.ClientSHA256, request.Materials.Catalog.ServerSHA256,
		request.Materials.OperationWriter.Name, string(request.Materials.OperationWriter.UID), request.Materials.OperationWriter.MaterialSHA256,
		request.Materials.PostgreSQLConfiguration.Name, string(request.Materials.PostgreSQLConfiguration.UID), request.Materials.PostgreSQLConfiguration.MaterialSHA256,
		request.Materials.MigrationSHA256, request.Materials.GenesisSHA256, request.Materials.PreflightSHA256,
		request.Materials.ServingHBAVersion, request.Materials.ServingHBASHA256, request.Materials.TargetTemplateSHA256,
		request.Source.ClusterName, string(request.Source.ClusterUID), request.Source.PodName, string(request.Source.PodUID), strconv.FormatInt(int64(request.Source.Shard), 10), strconv.FormatInt(int64(request.Source.Member), 10),
		request.Source.InstanceID, request.Source.BootID, strconv.FormatInt(request.Source.PostmasterPID, 10), request.Source.SystemIdentifier,
		strconv.FormatInt(request.Source.Timeline, 10), request.Source.GenerationIdentity, request.Source.GenerationBarrierLSN,
		request.Source.TargetFenceAcknowledgement.ObservedAtUnixMS, request.Source.TargetFenceAcknowledgement.DeadlineBoottimeNS,
		request.Source.TargetFenceAcknowledgement.RemainingValidityAtAckMS, request.Source.TargetFenceAcknowledgement.RemainingValidityAtReportMS,
		strconv.FormatInt(request.Source.TargetFenceAcknowledgement.ControlBackendPID, 10),
		request.RemoteApplyWitness.ClusterName, string(request.RemoteApplyWitness.ClusterUID), request.RemoteApplyWitness.PodName, string(request.RemoteApplyWitness.PodUID), strconv.FormatInt(int64(request.RemoteApplyWitness.Shard), 10),
		strconv.FormatInt(int64(request.RemoteApplyWitness.Member), 10), request.RemoteApplyWitness.InstanceID, request.RemoteApplyWitness.BootID,
		strconv.FormatInt(request.RemoteApplyWitness.PostmasterPID, 10), request.RemoteApplyWitness.MemberSlotName,
		request.RemoteApplyWitness.SystemIdentifier, strconv.FormatInt(request.RemoteApplyWitness.Timeline, 10),
		request.RemoteApplyWitness.GenerationIdentity, request.RemoteApplyWitness.GenerationBarrierLSN,
		request.RemoteApplyWitness.ReceiveLSN, request.RemoteApplyWitness.ReplayLSN,
	} {
		visit(value)
	}
}

func writeActivationFrame(hash interface{ Write([]byte) (int, error) }, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = hash.Write(length[:])
	_, _ = hash.Write([]byte(value))
}

func validActivationText(value string, maximum int) bool {
	if value == "" || len(value) > maximum {
		return false
	}
	for _, value := range []byte(value) {
		if value < '!' || value > '~' {
			return false
		}
	}
	return true
}

func validActivationUID(uid types.UID) bool {
	value := string(uid)
	if value == "" || len(value) > 128 {
		return false
	}
	for _, value := range []byte(value) {
		if !((value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') || (value >= '0' && value <= '9') || strings.ContainsRune("-_.:", rune(value))) {
			return false
		}
	}
	return true
}

func validActivationDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
}

func validActivationDecimal(value string) bool {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return false
	}
	for _, digit := range []byte(value) {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	_, err := strconv.ParseUint(value, 10, 64)
	return err == nil
}
