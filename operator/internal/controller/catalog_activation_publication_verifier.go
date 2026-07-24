package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"strconv"
	"strings"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/operator/internal/podfence"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	catalogActivationPublicationReadTimeout = 4 * time.Second
	catalogActivationStatusDigestDomain     = "pgshard-catalog-candidate-cluster-status-v1\x00"
	catalogCandidatePayloadKey              = "candidate.json"
	catalogActivationTLSMountPath           = "/etc/pgshard/catalog-tls"
	catalogActivationCAMountPath            = "/etc/pgshard/catalog-activation"
	catalogActivationCAFilePath             = catalogActivationCAMountPath + "/ca.crt"
	catalogActivationCAEnvironment          = "PGSHARD_CATALOG_ACTIVATION_CA_FILE"
	deploymentRevisionAnnotation            = "deployment.kubernetes.io/revision"
	deploymentRevisionHistoryAnnotation     = "deployment.kubernetes.io/revision-history"
	deploymentDesiredReplicasAnnotation     = "deployment.kubernetes.io/desired-replicas"
	deploymentMaxReplicasAnnotation         = "deployment.kubernetes.io/max-replicas"
	serviceAccountVolumePrefix              = "kube-api-access-"
	serviceAccountMountPath                 = "/var/run/secrets/kubernetes.io/serviceaccount"
)

// CatalogActivationPublicationVerifier validates a proposed request against
// authoritative API reads. It intentionally returns no proof object and keeps
// Secret material scoped to this one admission call.
type CatalogActivationPublicationVerifier struct {
	Reader client.Reader
	images owned.Images
}

// NewCatalogActivationPublicationVerifier captures the immutable controller
// image/runtime configuration used to render the authoritative desired
// workload snapshot for every admission call.
func NewCatalogActivationPublicationVerifier(reader client.Reader, images owned.Images) *CatalogActivationPublicationVerifier {
	return &CatalogActivationPublicationVerifier{Reader: reader, images: images}
}

// VerifyPublication implements v1alpha1.CatalogActivationPublicationVerifier.
// Reads are serial to keep failures deterministic and remain bounded by a
// deadline shorter than the webhook's five-second timeout.
func (verifier *CatalogActivationPublicationVerifier) VerifyPublication(ctx context.Context, oldActivation, newActivation *pgshardv1alpha1.PgShardCatalogActivation) error {
	if verifier == nil || verifier.Reader == nil {
		return fmt.Errorf("authoritative API reader is unavailable")
	}
	if oldActivation == nil || newActivation == nil || newActivation.Spec.Request == nil {
		return fmt.Errorf("catalog activation publication context is incomplete")
	}
	ctx, cancel := context.WithTimeout(ctx, catalogActivationPublicationReadTimeout)
	defer cancel()

	request := newActivation.Spec.Request
	cluster, rawStatus, err := verifier.readCluster(ctx, request)
	if err != nil {
		return err
	}
	if err := validateActivationClusterPublication(cluster, rawStatus, request); err != nil {
		return err
	}
	planned, err := renderActivationPlannedWorkloads(cluster, verifier.images)
	if err != nil {
		return err
	}
	if err := verifier.validateCarrier(ctx, cluster, oldActivation, newActivation, request); err != nil {
		return err
	}
	checkpoints, err := catalogActivationCheckpoints(cluster, request)
	if err != nil {
		return err
	}
	document, err := verifier.validateCandidate(ctx, cluster, checkpoints.candidate, request)
	if err != nil {
		return err
	}
	if err := validateRequestAgainstCandidate(document, request); err != nil {
		return err
	}
	if err := verifier.validateDispatcher(ctx, cluster, planned.dispatcher, request); err != nil {
		return err
	}
	if err := verifier.validateCoordinationLease(ctx, cluster, request); err != nil {
		return err
	}
	if err := verifier.validateSourceWorkload(ctx, cluster, planned, checkpoints.bootstrap, request); err != nil {
		return err
	}
	if err := verifier.validateWitnessWorkload(ctx, cluster, planned, request); err != nil {
		return err
	}
	if err := verifier.validateWritableLease(ctx, cluster, checkpoints.writable, request); err != nil {
		return err
	}
	if err := verifier.validateBootstrap(ctx, cluster, checkpoints.bootstrap, request); err != nil {
		return err
	}
	catalogCA, err := verifier.validateCatalogAccess(ctx, cluster, request)
	if err != nil {
		return err
	}
	defer clear(catalogCA)
	if err := verifier.validateReplication(ctx, cluster, checkpoints.replication, request); err != nil {
		return err
	}
	if err := verifier.validateOperationWriter(ctx, cluster, catalogCA, request); err != nil {
		return err
	}
	if err := verifier.validatePostgreSQLConfiguration(ctx, cluster, request); err != nil {
		return err
	}
	return verifier.validatePublicationFence(ctx, request)
}

func (verifier *CatalogActivationPublicationVerifier) readCluster(ctx context.Context, request *pgshardv1alpha1.CatalogActivationRequest) (*pgshardv1alpha1.PgShardCluster, any, error) {
	object := &unstructured.Unstructured{}
	object.SetGroupVersionKind(schema.GroupVersionKind{Group: pgshardv1alpha1.GroupVersion.Group, Version: pgshardv1alpha1.GroupVersion.Version, Kind: "PgShardCluster"})
	key := types.NamespacedName{Namespace: request.Cluster.Namespace, Name: request.Cluster.Name}
	if err := verifier.Reader.Get(ctx, key, object); err != nil {
		return nil, nil, fmt.Errorf("read live PgShardCluster %s/%s: %w", key.Namespace, key.Name, err)
	}
	rawStatus, ok, err := unstructured.NestedFieldCopy(object.Object, "status")
	if err != nil {
		return nil, nil, fmt.Errorf("copy live PgShardCluster %s/%s status: %w", key.Namespace, key.Name, err)
	}
	if !ok {
		return nil, nil, fmt.Errorf("live PgShardCluster %s/%s has no status", key.Namespace, key.Name)
	}
	cluster := &pgshardv1alpha1.PgShardCluster{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, cluster); err != nil {
		return nil, nil, fmt.Errorf("decode live PgShardCluster %s/%s: %w", key.Namespace, key.Name, err)
	}
	return cluster, rawStatus, nil
}

func validateActivationClusterPublication(cluster *pgshardv1alpha1.PgShardCluster, rawStatus any, request *pgshardv1alpha1.CatalogActivationRequest) error {
	if cluster.Name != request.Cluster.Name || cluster.Namespace != request.Cluster.Namespace || cluster.UID != request.Cluster.UID ||
		cluster.ResourceVersion != request.Cluster.ResourceVersion || strconv.FormatInt(cluster.Generation, 10) != request.Cluster.Generation ||
		cluster.DeletionTimestamp != nil {
		return fmt.Errorf("live PgShardCluster identity differs from the activation request")
	}
	if err := validateCatalogActivationStatusJSONValue(rawStatus); err != nil {
		return err
	}
	encoded, err := json.Marshal(rawStatus)
	if err != nil {
		return fmt.Errorf("encode live PgShardCluster status: %w", err)
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(catalogActivationStatusDigestDomain))
	_, _ = hash.Write(encoded)
	if hex.EncodeToString(hash.Sum(nil)) != request.Cluster.StatusSHA256 {
		return fmt.Errorf("live PgShardCluster status digest differs from the activation request")
	}
	if cluster.Status.ObservedGeneration != cluster.Generation || cluster.Status.PostgreSQLBootstrapSpec == nil ||
		cluster.Status.PostgreSQLBootstrapSpec.PostgreSQLRuntime != owned.PostgreSQLRuntimeAgentQuarantine.String() ||
		cluster.Spec.MembersPerShard < 3 {
		return fmt.Errorf("live PgShardCluster is not a reconciled multi-member agent-quarantine publication")
	}
	return validateBootstrapSpecStatus(cluster)
}

// validateCatalogActivationStatusJSONValue restricts the cross-language
// fingerprint domain to JSON values whose canonical representation is the
// same in Go and serde_json. In particular, floats are rejected even when
// their mathematical value is integral: Go encodes 1.0 as 1 while serde_json
// preserves 1.0.
func validateCatalogActivationStatusJSONValue(value any) error {
	switch typed := value.(type) {
	case nil, bool, string,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return nil
	case json.Number:
		if _, err := typed.Int64(); err == nil {
			return nil
		}
		if _, err := strconv.ParseUint(string(typed), 10, 64); err == nil {
			return nil
		}
		return fmt.Errorf("live PgShardCluster status contains a non-integer JSON number")
	case float32, float64:
		return fmt.Errorf("live PgShardCluster status contains a non-integer JSON number")
	case []any:
		for _, item := range typed {
			if err := validateCatalogActivationStatusJSONValue(item); err != nil {
				return err
			}
		}
		return nil
	case map[string]any:
		for _, item := range typed {
			if err := validateCatalogActivationStatusJSONValue(item); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("live PgShardCluster status contains unsupported JSON value %T", value)
	}
}

func (verifier *CatalogActivationPublicationVerifier) validateCarrier(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, oldActivation, newActivation *pgshardv1alpha1.PgShardCatalogActivation, request *pgshardv1alpha1.CatalogActivationRequest) error {
	live := &pgshardv1alpha1.PgShardCatalogActivation{}
	key := types.NamespacedName{Namespace: newActivation.Namespace, Name: newActivation.Name}
	if err := verifier.Reader.Get(ctx, key, live); err != nil {
		return fmt.Errorf("read live catalog activation carrier %s/%s: %w", key.Namespace, key.Name, err)
	}
	if live.UID != request.Carrier.UID || live.UID != oldActivation.UID || live.ResourceVersion != oldActivation.ResourceVersion ||
		live.DeletionTimestamp != nil || live.Spec.Request != nil || live.Spec.RequestSHA256 != "" || live.Status.Acceptance != nil ||
		!catalogActivationObjectMetadataEqual(live, pgshardv1alpha1.EmptyCatalogActivation(cluster)) ||
		!catalogActivationObjectMetadataEqual(newActivation, pgshardv1alpha1.EmptyCatalogActivation(cluster)) {
		return fmt.Errorf("live catalog activation carrier differs from the exact empty publication context")
	}
	return nil
}

func catalogActivationObjectMetadataEqual(current, expected *pgshardv1alpha1.PgShardCatalogActivation) bool {
	return current.Name == expected.Name && current.Namespace == expected.Namespace && current.GenerateName == "" &&
		maps.Equal(current.Labels, expected.Labels) && maps.Equal(current.Annotations, expected.Annotations) &&
		reflect.DeepEqual(current.OwnerReferences, expected.OwnerReferences) && len(current.Finalizers) == 0
}

type activationPublicationCheckpoints struct {
	bootstrap   pgshardv1alpha1.PostgreSQLBootstrapStatus
	writable    pgshardv1alpha1.PostgreSQLWritableLeaseStatus
	replication pgshardv1alpha1.PostgreSQLReplicationCredentialStatus
	candidate   pgshardv1alpha1.PostgreSQLCatalogCandidateStatus
}

type activationPlannedWorkloads struct {
	dispatcher *appsv1.Deployment
	members    map[string]*appsv1.StatefulSet
}

func renderActivationPlannedWorkloads(cluster *pgshardv1alpha1.PgShardCluster, images owned.Images) (*activationPlannedWorkloads, error) {
	if images.PostgreSQLRuntime != owned.PostgreSQLRuntimeAgentQuarantine {
		return nil, fmt.Errorf("catalog activation verifier is not configured for the exact agent-quarantine runtime")
	}
	objects, err := owned.Plan(cluster.DeepCopy(), images)
	if err != nil {
		return nil, fmt.Errorf("render exact catalog activation workloads: %w", err)
	}
	result := &activationPlannedWorkloads{members: make(map[string]*appsv1.StatefulSet)}
	for _, object := range objects {
		switch workload := object.(type) {
		case *appsv1.Deployment:
			if workload.Name == cluster.Name+owned.OrchestratorSuffix {
				if result.dispatcher != nil {
					return nil, fmt.Errorf("render exact catalog activation workloads: duplicate dispatcher Deployment")
				}
				result.dispatcher = workload.DeepCopy()
			}
		case *appsv1.StatefulSet:
			if workload.Labels[owned.ComponentLabel] == "postgresql" {
				if _, duplicate := result.members[workload.Name]; duplicate {
					return nil, fmt.Errorf("render exact catalog activation workloads: duplicate PostgreSQL StatefulSet %s", workload.Name)
				}
				result.members[workload.Name] = workload.DeepCopy()
			}
		}
	}
	if result.dispatcher == nil {
		return nil, fmt.Errorf("render exact catalog activation workloads: dispatcher Deployment is missing")
	}
	return result, nil
}

func catalogActivationCheckpoints(cluster *pgshardv1alpha1.PgShardCluster, request *pgshardv1alpha1.CatalogActivationRequest) (activationPublicationCheckpoints, error) {
	var result activationPublicationCheckpoints
	if request.Source.Shard != 0 || request.Source.Member != 0 || request.Source.PodName != owned.PostgreSQLMemberStatefulSetName(cluster.Name, 0, 0)+"-0" || request.Source.InstanceID != request.Source.PodName {
		return result, fmt.Errorf("activation source is not canonical shard-zero member zero")
	}
	foundBootstrap, foundWritable, foundReplication, foundCandidate := false, false, false, false
	for _, item := range cluster.Status.PostgreSQLBootstraps {
		if item.Shard == 0 && item.Member == 0 {
			if foundBootstrap {
				return result, fmt.Errorf("live PgShardCluster has a duplicate source bootstrap checkpoint")
			}
			result.bootstrap, foundBootstrap = item, true
		}
	}
	for _, item := range cluster.Status.PostgreSQLWritableLeases {
		if item.Shard == 0 {
			if foundWritable {
				return result, fmt.Errorf("live PgShardCluster has a duplicate writable Lease checkpoint")
			}
			result.writable, foundWritable = item, true
		}
	}
	for _, item := range cluster.Status.PostgreSQLReplicationCredentials {
		if item.Shard == 0 {
			if foundReplication {
				return result, fmt.Errorf("live PgShardCluster has a duplicate replication checkpoint")
			}
			result.replication, foundReplication = item, true
		}
	}
	for _, item := range cluster.Status.PostgreSQLCatalogCandidates {
		if item.Member == 0 {
			if foundCandidate {
				return result, fmt.Errorf("live PgShardCluster has a duplicate source candidate checkpoint")
			}
			result.candidate, foundCandidate = item, true
		}
	}
	if !foundBootstrap || !foundWritable || !foundReplication || !foundCandidate {
		return result, fmt.Errorf("live PgShardCluster lacks a complete source publication checkpoint set")
	}
	if !result.bootstrap.PVCFenceDetached || result.bootstrap.PVCCreationAbandoned || result.bootstrap.PVCStorageClassName == nil {
		return result, fmt.Errorf("live PgShardCluster source bootstrap checkpoint is not stabilized")
	}
	if request.Bootstrap.Secret.Name != result.bootstrap.SecretName || request.Bootstrap.Secret.UID != result.bootstrap.SecretUID ||
		request.Bootstrap.PVC.Name != result.bootstrap.PVCName || request.Bootstrap.PVC.UID != result.bootstrap.PVCUID ||
		request.WritableTerm.Name != result.writable.LeaseName || request.WritableTerm.UID != result.writable.LeaseUID ||
		request.Materials.Replication.Name != result.replication.SecretName || request.Materials.Replication.UID != result.replication.SecretUID || request.Materials.Replication.MaterialSHA256 != result.replication.MaterialSHA256 ||
		request.Candidate.Name != result.candidate.ConfigMapName || request.Candidate.UID != result.candidate.ConfigMapUID || request.Candidate.PayloadSHA256 != result.candidate.PayloadSHA256 {
		return result, fmt.Errorf("activation request differs from live PgShardCluster source checkpoints")
	}
	if cluster.Status.CatalogActivation == nil || cluster.Status.CatalogActivation.Name != request.Carrier.Name || cluster.Status.CatalogActivation.UID != request.Carrier.UID ||
		cluster.Status.CatalogAccess == nil || cluster.Status.CatalogAccess.SecretName != request.Materials.Catalog.Name || cluster.Status.CatalogAccess.SecretUID != request.Materials.Catalog.UID || cluster.Status.CatalogAccess.ClientSHA256 != request.Materials.Catalog.ClientSHA256 || cluster.Status.CatalogAccess.ServerSHA256 != request.Materials.Catalog.ServerSHA256 ||
		cluster.Status.OperationWriterAccess == nil || cluster.Status.OperationWriterAccess.SecretName != request.Materials.OperationWriter.Name || cluster.Status.OperationWriterAccess.SecretUID != request.Materials.OperationWriter.UID || cluster.Status.OperationWriterAccess.MaterialSHA256 != request.Materials.OperationWriter.MaterialSHA256 ||
		cluster.Status.PostgreSQLConfiguration == nil || cluster.Status.PostgreSQLConfiguration.ConfigMapName != request.Materials.PostgreSQLConfiguration.Name || cluster.Status.PostgreSQLConfiguration.ConfigMapUID != request.Materials.PostgreSQLConfiguration.UID || cluster.Status.PostgreSQLConfiguration.DataSHA256 != request.Materials.PostgreSQLConfiguration.MaterialSHA256 {
		return result, fmt.Errorf("activation request differs from live PgShardCluster material checkpoints")
	}
	return result, nil
}

type activationCandidateDocument struct {
	SchemaVersion    string                       `json:"schemaVersion"`
	ClusterObjectUID types.UID                    `json:"clusterObjectUID"`
	Shard            int32                        `json:"shard"`
	Member           int32                        `json:"member"`
	InstanceID       string                       `json:"instanceID"`
	Bootstrap        activationCandidateBootstrap `json:"bootstrap"`
	WritableLease    activationCandidateObject    `json:"writableLease"`
	Replication      activationCandidateMaterial  `json:"replicationCredential"`
	Catalog          activationCandidateCatalog   `json:"catalogAccess"`
	Materialization  activationCandidateBundle    `json:"materializationBundle"`
}

type activationCandidateObject struct {
	Name string    `json:"name"`
	UID  types.UID `json:"uid"`
}
type activationCandidateMaterial struct {
	activationCandidateObject
	MaterialSHA256 string `json:"materialSHA256"`
}
type activationCandidateCatalog struct {
	activationCandidateObject
	ClientSHA256 string `json:"clientSHA256"`
	ServerSHA256 string `json:"serverSHA256"`
}
type activationCandidateBootstrap struct {
	Secret activationCandidateObject `json:"secret"`
	PVC    activationCandidateObject `json:"pvc"`
}
type activationCandidateContent struct {
	SHA256 string `json:"sha256"`
}
type activationCandidatePolicy struct {
	Version string `json:"version"`
	SHA256  string `json:"sha256"`
}
type activationCandidateConfiguration struct {
	Name       string    `json:"name"`
	UID        types.UID `json:"uid"`
	DataSHA256 string    `json:"dataSHA256"`
}
type activationCandidateTarget struct {
	StatefulSetName   string `json:"statefulSetName"`
	PostgreSQLRuntime string `json:"postgresqlRuntime"`
	BootstrapHBAMode  string `json:"bootstrapHBAMode"`
	SHA256            string `json:"sha256"`
}
type activationCandidateBundle struct {
	PostgreSQLConfiguration   activationCandidateConfiguration `json:"postgresqlConfiguration"`
	ShardschemaMigration      activationCandidateContent       `json:"shardschemaMigration"`
	DatabaseGenesis           activationCandidateContent       `json:"databaseGenesis"`
	DatabaseTopologyPreflight activationCandidateContent       `json:"databaseTopologyPreflight"`
	CatalogAccess             activationCandidateCatalog       `json:"catalogAccess"`
	OperationWriterAccess     activationCandidateMaterial      `json:"operationWriterAccess"`
	ServingHBA                activationCandidatePolicy        `json:"servingHBA"`
	TargetPodTemplate         activationCandidateTarget        `json:"targetPodTemplate"`
}

func (verifier *CatalogActivationPublicationVerifier) validateCandidate(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, checkpoint pgshardv1alpha1.PostgreSQLCatalogCandidateStatus, request *pgshardv1alpha1.CatalogActivationRequest) (*activationCandidateDocument, error) {
	desired, err := owned.DesiredPostgreSQLCatalogCandidateConfigMaps(cluster)
	if err != nil {
		return nil, fmt.Errorf("render exact catalog candidate publication: %w", err)
	}
	if len(desired) == 0 {
		return nil, fmt.Errorf("render exact catalog candidate publication: no shard-zero candidates")
	}
	live := &corev1.ConfigMap{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: checkpoint.ConfigMapName}
	if err := verifier.Reader.Get(ctx, key, live); err != nil {
		return nil, fmt.Errorf("read live catalog candidate ConfigMap %s/%s: %w", key.Namespace, key.Name, err)
	}
	if live.UID != checkpoint.ConfigMapUID || live.ResourceVersion != request.Candidate.ResourceVersion || live.DeletionTimestamp != nil ||
		owned.PostgreSQLCatalogCandidatePayloadSHA256(live) != checkpoint.PayloadSHA256 {
		return nil, fmt.Errorf("live catalog candidate ConfigMap identity or digest differs from the activation request")
	}
	if err := validatePostgreSQLCatalogCandidateConfigMap(live, desired[0], cluster); err != nil {
		return nil, err
	}
	payload := live.Data[catalogCandidatePayloadKey]
	document := &activationCandidateDocument{}
	if err := json.Unmarshal([]byte(payload), document); err != nil {
		return nil, fmt.Errorf("decode live catalog candidate document: %w", err)
	}
	return document, nil
}

func validateRequestAgainstCandidate(document *activationCandidateDocument, request *pgshardv1alpha1.CatalogActivationRequest) error {
	if document.SchemaVersion != "pgshard.catalog-bootstrap-candidate.v1" || document.ClusterObjectUID != request.Cluster.UID || document.Shard != 0 || document.Member != request.Source.Member || document.InstanceID != request.Source.InstanceID ||
		document.Bootstrap.Secret.Name != request.Bootstrap.Secret.Name || document.Bootstrap.Secret.UID != request.Bootstrap.Secret.UID || document.Bootstrap.PVC.Name != request.Bootstrap.PVC.Name || document.Bootstrap.PVC.UID != request.Bootstrap.PVC.UID ||
		document.WritableLease.Name != request.WritableTerm.Name || document.WritableLease.UID != request.WritableTerm.UID ||
		document.Replication.Name != request.Materials.Replication.Name || document.Replication.UID != request.Materials.Replication.UID || document.Replication.MaterialSHA256 != request.Materials.Replication.MaterialSHA256 ||
		document.Catalog.Name != request.Materials.Catalog.Name || document.Catalog.UID != request.Materials.Catalog.UID || document.Catalog.ClientSHA256 != request.Materials.Catalog.ClientSHA256 || document.Catalog.ServerSHA256 != request.Materials.Catalog.ServerSHA256 ||
		document.Materialization.PostgreSQLConfiguration.Name != request.Materials.PostgreSQLConfiguration.Name || document.Materialization.PostgreSQLConfiguration.UID != request.Materials.PostgreSQLConfiguration.UID || document.Materialization.PostgreSQLConfiguration.DataSHA256 != request.Materials.PostgreSQLConfiguration.MaterialSHA256 ||
		document.Materialization.OperationWriterAccess.Name != request.Materials.OperationWriter.Name || document.Materialization.OperationWriterAccess.UID != request.Materials.OperationWriter.UID || document.Materialization.OperationWriterAccess.MaterialSHA256 != request.Materials.OperationWriter.MaterialSHA256 ||
		document.Materialization.CatalogAccess != document.Catalog || document.Materialization.ShardschemaMigration.SHA256 != request.Materials.MigrationSHA256 || document.Materialization.DatabaseGenesis.SHA256 != request.Materials.GenesisSHA256 || document.Materialization.DatabaseTopologyPreflight.SHA256 != request.Materials.PreflightSHA256 ||
		document.Materialization.ServingHBA.Version != request.Materials.ServingHBAVersion || document.Materialization.ServingHBA.SHA256 != request.Materials.ServingHBASHA256 || document.Materialization.TargetPodTemplate.StatefulSetName != owned.PostgreSQLMemberStatefulSetName(request.Cluster.Name, 0, 0) || document.Materialization.TargetPodTemplate.PostgreSQLRuntime != owned.PostgreSQLRuntimeAgentQuarantine.String() || document.Materialization.TargetPodTemplate.BootstrapHBAMode != "replication-bootstrap-primary" || document.Materialization.TargetPodTemplate.SHA256 != request.Materials.TargetTemplateSHA256 {
		return fmt.Errorf("activation request differs from the exact catalog candidate document")
	}
	return nil
}

func (verifier *CatalogActivationPublicationVerifier) validateDispatcher(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, planned *appsv1.Deployment, request *pgshardv1alpha1.CatalogActivationRequest) error {
	pod := &corev1.Pod{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: request.Dispatcher.PodName}
	if err := verifier.Reader.Get(ctx, key, pod); err != nil {
		return fmt.Errorf("read live dispatcher Pod %s/%s: %w", key.Namespace, key.Name, err)
	}
	if pod.UID != request.Dispatcher.PodUID || pod.DeletionTimestamp != nil || len(pod.Finalizers) != 0 || pod.Spec.ServiceAccountName != cluster.Name+owned.OrchestratorSuffix {
		return fmt.Errorf("live dispatcher Pod identity or operator-owned shape differs from the activation request")
	}
	replicaSetOwner := exactControllerOwner(pod.OwnerReferences, appsv1.SchemeGroupVersion.String(), "ReplicaSet")
	if replicaSetOwner == nil {
		return fmt.Errorf("live dispatcher Pod lacks its exact ReplicaSet owner")
	}
	replicaSet := &appsv1.ReplicaSet{}
	if err := verifier.Reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: replicaSetOwner.Name}, replicaSet); err != nil {
		return fmt.Errorf("read live dispatcher ReplicaSet %s/%s: %w", cluster.Namespace, replicaSetOwner.Name, err)
	}
	if replicaSet.UID != replicaSetOwner.UID || replicaSet.DeletionTimestamp != nil || len(replicaSet.Finalizers) != 0 {
		return fmt.Errorf("live dispatcher ReplicaSet differs from its Pod owner identity")
	}
	deploymentOwner := exactControllerOwner(replicaSet.OwnerReferences, appsv1.SchemeGroupVersion.String(), "Deployment")
	if deploymentOwner == nil || deploymentOwner.Name != cluster.Name+owned.OrchestratorSuffix {
		return fmt.Errorf("live dispatcher ReplicaSet lacks its canonical Deployment owner")
	}
	deployment := &appsv1.Deployment{}
	if err := verifier.Reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: deploymentOwner.Name}, deployment); err != nil {
		return fmt.Errorf("read live dispatcher Deployment %s/%s: %w", cluster.Namespace, deploymentOwner.Name, err)
	}
	if deployment.UID != deploymentOwner.UID || deployment.DeletionTimestamp != nil || len(deployment.Finalizers) != 0 ||
		deployment.Spec.Template.Spec.ServiceAccountName != cluster.Name+owned.OrchestratorSuffix {
		return fmt.Errorf("live dispatcher Deployment owner or Pod template is not canonical")
	}
	if !activationDeploymentMatchesPlan(planned, deployment) {
		return fmt.Errorf("live dispatcher Deployment differs from the exact configured workload plan")
	}
	if err := validateDispatcherControllerChain(cluster, deployment, replicaSet, pod); err != nil {
		return err
	}
	secretAllowances := activationDispatcherSecretAllowances(request)
	for _, workload := range []struct {
		name                         string
		spec                         corev1.PodSpec
		allowServiceAccountInjection bool
	}{{"dispatcher Deployment", deployment.Spec.Template.Spec, false}, {"dispatcher ReplicaSet", replicaSet.Spec.Template.Spec, false}, {"dispatcher Pod", pod.Spec, true}} {
		if err := validateActivationPodAuthorityBoundary(workload.name, workload.spec, secretAllowances, workload.allowServiceAccountInjection, false); err != nil {
			return err
		}
		if !activationContainerHasExactLiteralEnvironment(workload.spec, "orchestrator", catalogActivationCAEnvironment, catalogActivationCAFilePath) {
			return fmt.Errorf("%s lacks its exact catalog activation CA environment", workload.name)
		}
	}
	if !activationPodTemplateSpecMatches(deployment.Spec.Template.Spec, replicaSet.Spec.Template.Spec) {
		return fmt.Errorf("live dispatcher ReplicaSet security-relevant spec differs from its Deployment template")
	}
	if !activationDispatcherPodSpecMatches(replicaSet.Spec.Template.Spec, pod.Spec) {
		return fmt.Errorf("live dispatcher Pod security-relevant spec differs from its ReplicaSet template")
	}
	return nil
}

func validateDispatcherControllerChain(cluster *pgshardv1alpha1.PgShardCluster, deployment *appsv1.Deployment, replicaSet *appsv1.ReplicaSet, pod *corev1.Pod) error {
	selector := componentSelectorLabels(cluster.Name, "orchestrator", nil)
	revision := deployment.Annotations[deploymentRevisionAnnotation]
	if !canonicalPositiveDecimal(revision) || !exactOwnedMetadata(deployment.ObjectMeta, cluster, deployment.Name, "orchestrator", nil, map[string]string{deploymentRevisionAnnotation: revision}) ||
		deployment.Spec.Selector == nil || len(deployment.Spec.Selector.MatchExpressions) != 0 || !maps.Equal(deployment.Spec.Selector.MatchLabels, selector) ||
		!maps.Equal(deployment.Spec.Template.Labels, selector) || !canonicalPodTemplateMetadata(deployment.Spec.Template.ObjectMeta, nil) ||
		!exactDispatcherTemplateAnnotations(deployment.Spec.Template.Annotations) {
		return fmt.Errorf("live dispatcher Deployment metadata or revision is not canonical")
	}
	hash := replicaSet.Spec.Template.Labels[appsv1.DefaultDeploymentUniqueLabelKey]
	if hash == "" || len(validation.IsValidLabelValue(hash)) != 0 {
		return fmt.Errorf("live dispatcher ReplicaSet has an invalid Pod template hash")
	}
	expectedLabels := maps.Clone(selector)
	expectedLabels[appsv1.DefaultDeploymentUniqueLabelKey] = hash
	if replicaSet.Spec.Selector == nil || len(replicaSet.Spec.Selector.MatchExpressions) != 0 ||
		!maps.Equal(replicaSet.Spec.Selector.MatchLabels, expectedLabels) || !maps.Equal(replicaSet.Labels, expectedLabels) || !maps.Equal(replicaSet.Spec.Template.Labels, expectedLabels) ||
		replicaSet.Name != deployment.Name+"-"+hash || replicaSet.GenerateName != "" || !maps.Equal(replicaSet.Spec.Template.Annotations, deployment.Spec.Template.Annotations) || !canonicalPodTemplateMetadata(replicaSet.Spec.Template.ObjectMeta, nil) ||
		pod.GenerateName != replicaSet.Name+"-" || !strings.HasPrefix(pod.Name, pod.GenerateName) ||
		!maps.Equal(pod.Labels, expectedLabels) || !maps.Equal(pod.Annotations, replicaSet.Spec.Template.Annotations) {
		return fmt.Errorf("live dispatcher Deployment, ReplicaSet, and Pod metadata projection differs")
	}
	expectedAnnotations := map[string]string{
		owned.ApplyOwnershipAnnotation:      owned.ApplyOwnershipVersion,
		deploymentRevisionAnnotation:        revision,
		deploymentDesiredReplicasAnnotation: strconv.FormatInt(int64(ptrInt32(deployment.Spec.Replicas, 1)), 10),
		deploymentMaxReplicasAnnotation:     strconv.FormatInt(int64(deploymentMaxReplicas(deployment)), 10),
	}
	actualAnnotations := maps.Clone(replicaSet.Annotations)
	if history, ok := actualAnnotations[deploymentRevisionHistoryAnnotation]; ok {
		if !validDeploymentRevisionHistory(history, revision) {
			return fmt.Errorf("live dispatcher ReplicaSet revision history is not canonical")
		}
		delete(actualAnnotations, deploymentRevisionHistoryAnnotation)
	}
	if !maps.Equal(actualAnnotations, expectedAnnotations) {
		return fmt.Errorf("live dispatcher ReplicaSet annotations do not bind the current Deployment revision")
	}
	return nil
}

func canonicalPodTemplateMetadata(metadata metav1.ObjectMeta, finalizers []string) bool {
	return metadata.Name == "" && metadata.GenerateName == "" && metadata.Namespace == "" && len(metadata.OwnerReferences) == 0 && reflect.DeepEqual(metadata.Finalizers, finalizers) && metadata.DeletionTimestamp == nil
}

func exactDispatcherTemplateAnnotations(annotations map[string]string) bool {
	return len(annotations) == 1 && validCatalogAccessDigest(annotations[owned.ConfigHashAnnotation])
}

func canonicalPositiveDecimal(value string) bool {
	number, err := strconv.ParseUint(value, 10, 64)
	return err == nil && number > 0 && strconv.FormatUint(number, 10) == value
}

func validDeploymentRevisionHistory(value, current string) bool {
	if value == "" || len(value) > 2000 {
		return false
	}
	seen := map[string]struct{}{current: {}}
	for _, revision := range strings.Split(value, ",") {
		if !canonicalPositiveDecimal(revision) {
			return false
		}
		if _, duplicate := seen[revision]; duplicate {
			return false
		}
		seen[revision] = struct{}{}
	}
	return true
}

func ptrInt32(value *int32, fallback int32) int32 {
	if value == nil {
		return fallback
	}
	return *value
}

func deploymentMaxReplicas(deployment *appsv1.Deployment) int32 {
	desired := ptrInt32(deployment.Spec.Replicas, 1)
	if deployment.Spec.Strategy.Type != appsv1.RollingUpdateDeploymentStrategyType || deployment.Spec.Strategy.RollingUpdate == nil || deployment.Spec.Strategy.RollingUpdate.MaxSurge == nil {
		return desired
	}
	surge, err := intstr.GetScaledValueFromIntOrPercent(deployment.Spec.Strategy.RollingUpdate.MaxSurge, int(desired), true)
	if err != nil {
		return -1
	}
	return desired + int32(surge)
}

func activationDeploymentMatchesPlan(planned, live *appsv1.Deployment) bool {
	if planned == nil || live == nil || planned.Name != live.Name || planned.Namespace != live.Namespace {
		return false
	}
	expected := planned.DeepCopy()
	defaultActivationDeployment(expected)
	return apiequality.Semantic.DeepEqual(expected.Spec, live.Spec)
}

func activationStatefulSetMatchesPlan(planned, live *appsv1.StatefulSet) bool {
	if planned == nil || live == nil || planned.Name != live.Name || planned.Namespace != live.Namespace {
		return false
	}
	expected := planned.DeepCopy()
	defaultActivationStatefulSet(expected)
	return apiequality.Semantic.DeepEqual(expected.Spec, live.Spec)
}

// These defaults mirror the deterministic built-in API defaults that apply to
// controller Pod templates. Admission plugins such as LimitRange are not part
// of this allowlist and therefore remain fail closed.
func defaultActivationDeployment(deployment *appsv1.Deployment) {
	if deployment.Spec.Replicas == nil {
		deployment.Spec.Replicas = activationInt32(1)
	}
	if deployment.Spec.Strategy.Type == "" {
		deployment.Spec.Strategy.Type = appsv1.RollingUpdateDeploymentStrategyType
	}
	if deployment.Spec.Strategy.Type == appsv1.RollingUpdateDeploymentStrategyType {
		if deployment.Spec.Strategy.RollingUpdate == nil {
			deployment.Spec.Strategy.RollingUpdate = &appsv1.RollingUpdateDeployment{}
		}
		if deployment.Spec.Strategy.RollingUpdate.MaxUnavailable == nil {
			value := intstr.FromString("25%")
			deployment.Spec.Strategy.RollingUpdate.MaxUnavailable = &value
		}
		if deployment.Spec.Strategy.RollingUpdate.MaxSurge == nil {
			value := intstr.FromString("25%")
			deployment.Spec.Strategy.RollingUpdate.MaxSurge = &value
		}
	}
	if deployment.Spec.RevisionHistoryLimit == nil {
		deployment.Spec.RevisionHistoryLimit = activationInt32(10)
	}
	if deployment.Spec.ProgressDeadlineSeconds == nil {
		deployment.Spec.ProgressDeadlineSeconds = activationInt32(600)
	}
	defaultActivationPodTemplateSpec(&deployment.Spec.Template.Spec)
}

func defaultActivationStatefulSet(statefulSet *appsv1.StatefulSet) {
	if statefulSet.Spec.PodManagementPolicy == "" {
		statefulSet.Spec.PodManagementPolicy = appsv1.OrderedReadyPodManagement
	}
	if statefulSet.Spec.UpdateStrategy.Type == "" {
		statefulSet.Spec.UpdateStrategy.Type = appsv1.RollingUpdateStatefulSetStrategyType
		statefulSet.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{Partition: activationInt32(0)}
	}
	if statefulSet.Spec.UpdateStrategy.Type == appsv1.RollingUpdateStatefulSetStrategyType && statefulSet.Spec.UpdateStrategy.RollingUpdate != nil && statefulSet.Spec.UpdateStrategy.RollingUpdate.Partition == nil {
		statefulSet.Spec.UpdateStrategy.RollingUpdate.Partition = activationInt32(0)
	}
	if statefulSet.Spec.PersistentVolumeClaimRetentionPolicy == nil {
		statefulSet.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{}
	}
	if statefulSet.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted == "" {
		statefulSet.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted = appsv1.RetainPersistentVolumeClaimRetentionPolicyType
	}
	if statefulSet.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled == "" {
		statefulSet.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled = appsv1.RetainPersistentVolumeClaimRetentionPolicyType
	}
	if statefulSet.Spec.Replicas == nil {
		statefulSet.Spec.Replicas = activationInt32(1)
	}
	if statefulSet.Spec.RevisionHistoryLimit == nil {
		statefulSet.Spec.RevisionHistoryLimit = activationInt32(10)
	}
	defaultActivationPodTemplateSpec(&statefulSet.Spec.Template.Spec)
}

func defaultActivationPodTemplateSpec(spec *corev1.PodSpec) {
	if spec.DNSPolicy == "" {
		spec.DNSPolicy = corev1.DNSClusterFirst
	}
	if spec.RestartPolicy == "" {
		spec.RestartPolicy = corev1.RestartPolicyAlways
	}
	if spec.SecurityContext == nil {
		spec.SecurityContext = &corev1.PodSecurityContext{}
	}
	if spec.TerminationGracePeriodSeconds == nil {
		seconds := int64(corev1.DefaultTerminationGracePeriodSeconds)
		spec.TerminationGracePeriodSeconds = &seconds
	}
	if spec.SchedulerName == "" {
		spec.SchedulerName = corev1.DefaultSchedulerName
	}
	for index := range spec.Volumes {
		defaultActivationVolume(&spec.Volumes[index])
	}
	for index := range spec.InitContainers {
		defaultActivationContainer(&spec.InitContainers[index])
	}
	for index := range spec.Containers {
		defaultActivationContainer(&spec.Containers[index])
	}
	defaultActivationResourceList(spec.Overhead)
	if spec.Resources != nil {
		defaultActivationResourceList(spec.Resources.Limits)
		defaultActivationResourceList(spec.Resources.Requests)
	}
}

func defaultActivationVolume(volume *corev1.Volume) {
	if volume.Secret != nil && volume.Secret.DefaultMode == nil {
		volume.Secret.DefaultMode = activationInt32(corev1.SecretVolumeSourceDefaultMode)
	}
	if volume.ConfigMap != nil && volume.ConfigMap.DefaultMode == nil {
		volume.ConfigMap.DefaultMode = activationInt32(corev1.ConfigMapVolumeSourceDefaultMode)
	}
	if volume.DownwardAPI != nil {
		if volume.DownwardAPI.DefaultMode == nil {
			volume.DownwardAPI.DefaultMode = activationInt32(corev1.DownwardAPIVolumeSourceDefaultMode)
		}
		for index := range volume.DownwardAPI.Items {
			defaultActivationObjectFieldSelector(volume.DownwardAPI.Items[index].FieldRef)
		}
	}
	if volume.Projected != nil {
		if volume.Projected.DefaultMode == nil {
			volume.Projected.DefaultMode = activationInt32(corev1.ProjectedVolumeSourceDefaultMode)
		}
		for index := range volume.Projected.Sources {
			projection := &volume.Projected.Sources[index]
			if projection.ServiceAccountToken != nil && projection.ServiceAccountToken.ExpirationSeconds == nil {
				seconds := int64(3600)
				projection.ServiceAccountToken.ExpirationSeconds = &seconds
			}
			if projection.DownwardAPI != nil {
				for item := range projection.DownwardAPI.Items {
					defaultActivationObjectFieldSelector(projection.DownwardAPI.Items[item].FieldRef)
				}
			}
		}
	}
}

func defaultActivationContainer(container *corev1.Container) {
	if container.TerminationMessagePath == "" {
		container.TerminationMessagePath = corev1.TerminationMessagePathDefault
	}
	if container.TerminationMessagePolicy == "" {
		container.TerminationMessagePolicy = corev1.TerminationMessageReadFile
	}
	for index := range container.Ports {
		if container.Ports[index].Protocol == "" {
			container.Ports[index].Protocol = corev1.ProtocolTCP
		}
	}
	for index := range container.Env {
		if container.Env[index].ValueFrom != nil {
			defaultActivationObjectFieldSelector(container.Env[index].ValueFrom.FieldRef)
		}
	}
	defaultActivationResourceList(container.Resources.Limits)
	defaultActivationResourceList(container.Resources.Requests)
	defaultActivationProbe(container.LivenessProbe)
	defaultActivationProbe(container.ReadinessProbe)
	defaultActivationProbe(container.StartupProbe)
	if container.Lifecycle != nil {
		if container.Lifecycle.PostStart != nil {
			defaultActivationHTTPGet(container.Lifecycle.PostStart.HTTPGet)
		}
		if container.Lifecycle.PreStop != nil {
			defaultActivationHTTPGet(container.Lifecycle.PreStop.HTTPGet)
		}
	}
}

func defaultActivationResourceList(resources corev1.ResourceList) {
	for name, quantity := range resources {
		quantity.RoundUp(-3)
		resources[name] = quantity
	}
}

func defaultActivationProbe(probe *corev1.Probe) {
	if probe == nil {
		return
	}
	if probe.TimeoutSeconds == 0 {
		probe.TimeoutSeconds = 1
	}
	if probe.PeriodSeconds == 0 {
		probe.PeriodSeconds = 10
	}
	if probe.SuccessThreshold == 0 {
		probe.SuccessThreshold = 1
	}
	if probe.FailureThreshold == 0 {
		probe.FailureThreshold = 3
	}
	defaultActivationHTTPGet(probe.HTTPGet)
	if probe.GRPC != nil && probe.GRPC.Service == nil {
		probe.GRPC.Service = new(string)
	}
}

func defaultActivationHTTPGet(action *corev1.HTTPGetAction) {
	if action == nil {
		return
	}
	if action.Path == "" {
		action.Path = "/"
	}
	if action.Scheme == "" {
		action.Scheme = corev1.URISchemeHTTP
	}
}

func defaultActivationObjectFieldSelector(selector *corev1.ObjectFieldSelector) {
	if selector != nil && selector.APIVersion == "" {
		selector.APIVersion = "v1"
	}
}

func activationInt32(value int32) *int32 {
	return &value
}

func (verifier *CatalogActivationPublicationVerifier) validateCoordinationLease(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, request *pgshardv1alpha1.CatalogActivationRequest) error {
	lease := &coordinationv1.Lease{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: request.Dispatcher.LeaseName}
	if err := verifier.Reader.Get(ctx, key, lease); err != nil {
		return fmt.Errorf("read live orchestrator Lease %s/%s: %w", key.Namespace, key.Name, err)
	}
	if lease.UID != request.Dispatcher.LeaseUID || lease.ResourceVersion != request.Dispatcher.LeaseResourceVersion || lease.DeletionTimestamp != nil || len(lease.Finalizers) != 0 ||
		!exactOwnedMetadata(lease.ObjectMeta, cluster, request.Dispatcher.LeaseName, "orchestrator", nil, nil) || lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != request.Dispatcher.LeaseHolder ||
		lease.Spec.Strategy != nil || lease.Spec.PreferredHolder != nil {
		return fmt.Errorf("live orchestrator Lease identity, owner, or elected term differs from the activation request")
	}
	if err := validatePostgreSQLWritableLeaseRuntimeSpec(lease.Spec); err != nil {
		return fmt.Errorf("live orchestrator Lease runtime is invalid: %w", err)
	}
	return nil
}

func (verifier *CatalogActivationPublicationVerifier) validateSourceWorkload(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, planned *activationPlannedWorkloads, bootstrap pgshardv1alpha1.PostgreSQLBootstrapStatus, request *pgshardv1alpha1.CatalogActivationRequest) error {
	return verifier.validatePostgreSQLWorkload(ctx, cluster, planned, bootstrap.Member, request.Source.PodName, request.Source.PodUID, request.Source.BootID, true, request)
}

func (verifier *CatalogActivationPublicationVerifier) validateWitnessWorkload(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, planned *activationPlannedWorkloads, request *pgshardv1alpha1.CatalogActivationRequest) error {
	if request.RemoteApplyWitness.Shard != 0 || request.RemoteApplyWitness.Member <= 0 || request.RemoteApplyWitness.Member >= cluster.Spec.MembersPerShard || request.RemoteApplyWitness.InstanceID != request.RemoteApplyWitness.PodName {
		return fmt.Errorf("activation witness is not a canonical nonzero shard-zero member")
	}
	return verifier.validatePostgreSQLWorkload(ctx, cluster, planned, request.RemoteApplyWitness.Member, request.RemoteApplyWitness.PodName, request.RemoteApplyWitness.PodUID, request.RemoteApplyWitness.BootID, false, request)
}

func (verifier *CatalogActivationPublicationVerifier) validatePostgreSQLWorkload(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, planned *activationPlannedWorkloads, member int32, podName string, podUID types.UID, bootID string, source bool, request *pgshardv1alpha1.CatalogActivationRequest) error {
	statefulSetName := owned.PostgreSQLMemberStatefulSetName(cluster.Name, 0, member)
	if podName != statefulSetName+"-0" {
		return fmt.Errorf("catalog activation PostgreSQL Pod name is not canonical for member %d", member)
	}
	statefulSet := &appsv1.StatefulSet{}
	if err := verifier.Reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: statefulSetName}, statefulSet); err != nil {
		return fmt.Errorf("read live PostgreSQL member StatefulSet %s/%s: %w", cluster.Namespace, statefulSetName, err)
	}
	plannedStatefulSet := planned.members[statefulSetName]
	if plannedStatefulSet == nil || !activationStatefulSetMatchesPlan(plannedStatefulSet, statefulSet) {
		return fmt.Errorf("live PostgreSQL member StatefulSet %s differs from the exact configured workload plan", statefulSetName)
	}
	extra := map[string]string{owned.ShardLabel: "0000", owned.MemberLabel: fmt.Sprintf("%04d", member)}
	selector := componentSelectorLabels(cluster.Name, "postgresql", extra)
	templateLabels := maps.Clone(selector)
	templateLabels[owned.ManagedByLabel] = owned.ManagedByValue
	extraAnnotations := map[string]string{owned.PostgreSQLRuntimeAnnotation: owned.PostgreSQLRuntimeAgentQuarantine.String()}
	if source {
		durability, standbys := activationGenerationDurability(cluster)
		extraAnnotations[owned.PostgreSQLGenerationDurabilityAnnotation] = durability
		if standbys != "" {
			extraAnnotations[owned.PostgreSQLSynchronousStandbysAnnotation] = standbys
		}
	}
	expectedTemplateAnnotations := map[string]string{
		owned.PostgreSQLPodClusterUIDAnnotation: string(cluster.UID),
		owned.PostgreSQLRuntimeAnnotation:       owned.PostgreSQLRuntimeAgentQuarantine.String(),
	}
	if source {
		durability, standbys := activationGenerationDurability(cluster)
		expectedTemplateAnnotations[owned.ConfigHashAnnotation] = request.Materials.PostgreSQLConfiguration.MaterialSHA256
		expectedTemplateAnnotations["pgshard.io/shardschema-migration-sha256"] = request.Materials.MigrationSHA256
		expectedTemplateAnnotations[owned.PostgreSQLGenerationDurabilityAnnotation] = durability
		if standbys != "" {
			expectedTemplateAnnotations[owned.PostgreSQLSynchronousStandbysAnnotation] = standbys
		}
	}
	if statefulSet.DeletionTimestamp != nil || len(statefulSet.Finalizers) != 0 || !exactOwnedMetadata(statefulSet.ObjectMeta, cluster, statefulSetName, "postgresql", extra, extraAnnotations) ||
		statefulSet.Spec.Replicas == nil || *statefulSet.Spec.Replicas != 1 || statefulSet.Spec.UpdateStrategy.Type != appsv1.OnDeleteStatefulSetStrategyType || statefulSet.Spec.Selector == nil || !maps.Equal(statefulSet.Spec.Selector.MatchLabels, selector) ||
		statefulSet.Spec.ServiceName != owned.PostgreSQLShardStatefulSetName(cluster.Name, 0) || len(statefulSet.Spec.VolumeClaimTemplates) != 0 ||
		!maps.Equal(statefulSet.Spec.Template.Annotations, expectedTemplateAnnotations) || !maps.Equal(statefulSet.Spec.Template.Labels, templateLabels) || !canonicalPodTemplateMetadata(statefulSet.Spec.Template.ObjectMeta, []string{owned.PostgreSQLPodTerminationFinalizer}) {
		return fmt.Errorf("live PostgreSQL member StatefulSet %s is not the exact agent-quarantine publication", statefulSetName)
	}
	if statefulSet.Status.CurrentRevision == "" || statefulSet.Status.CurrentRevision != statefulSet.Status.UpdateRevision {
		return fmt.Errorf("live PostgreSQL member StatefulSet %s has no single current controller revision", statefulSetName)
	}
	secretAllowances := activationPostgreSQLSecretAllowances(request, source)
	if err := validateActivationPodAuthorityBoundary("PostgreSQL StatefulSet", statefulSet.Spec.Template.Spec, secretAllowances, false, source); err != nil {
		return err
	}
	pod := &corev1.Pod{}
	if err := verifier.Reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: podName}, pod); err != nil {
		return fmt.Errorf("read live PostgreSQL member Pod %s/%s: %w", cluster.Namespace, podName, err)
	}
	owner := exactControllerOwner(pod.OwnerReferences, appsv1.SchemeGroupVersion.String(), "StatefulSet")
	expectedServiceAccount := owned.PostgreSQLStandbyServiceAccountName(cluster.Name, 0)
	if source {
		expectedServiceAccount = owned.PostgreSQLAgentServiceAccountName(cluster.Name, 0)
	}
	if pod.UID != podUID || pod.DeletionTimestamp != nil || owner == nil || owner.Name != statefulSetName || owner.UID != statefulSet.UID ||
		!reflect.DeepEqual(pod.Finalizers, []string{owned.PostgreSQLPodTerminationFinalizer}) || !statefulSetPodMetadataMatches(statefulSet, pod, bootID) ||
		pod.Spec.ServiceAccountName != expectedServiceAccount || pod.Annotations[podfence.NodeUIDAnnotation] == "" || pod.Annotations[podfence.NodeBootIDAnnotation] != bootID {
		return fmt.Errorf("live PostgreSQL member Pod %s differs from its exact StatefulSet incarnation", podName)
	}
	if err := validateActivationPodAuthorityBoundary("PostgreSQL Pod", pod.Spec, secretAllowances, false, source); err != nil {
		return err
	}
	if !activationStatefulSetPodSpecMatches(statefulSet.Spec.Template.Spec, pod.Spec, podName, statefulSet.Spec.ServiceName) {
		return fmt.Errorf("live PostgreSQL member Pod %s security-relevant spec differs from its StatefulSet template", podName)
	}
	if source && !owned.IsCurrentPostgreSQLReplicationBootstrapSourcePod(pod) {
		return fmt.Errorf("live source Pod is not the current replication-bootstrap source shape")
	}
	if !source && (!owned.IsPostgreSQLReplicationStandbyPod(pod) || request.RemoteApplyWitness.MemberSlotName != fmt.Sprintf("pgshard_member_%04d", member)) {
		return fmt.Errorf("live witness Pod is not the canonical physical-standby shape")
	}
	if source && (!podSpecReferencesNamedPVC(pod.Spec, request.Bootstrap.PVC.Name) || !podSpecReferencesSecret(pod.Spec, request.Bootstrap.Secret.Name) || !podSpecReferencesSecret(pod.Spec, request.Materials.Replication.Name) || !podSpecReferencesSecret(pod.Spec, request.Materials.Catalog.Name) || !activationPodSpecReferencesConfigMap(pod.Spec, request.Materials.PostgreSQLConfiguration.Name)) {
		return fmt.Errorf("live source Pod projections differ from the activation request")
	}
	return nil
}

func (verifier *CatalogActivationPublicationVerifier) validateWritableLease(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, checkpoint pgshardv1alpha1.PostgreSQLWritableLeaseStatus, request *pgshardv1alpha1.CatalogActivationRequest) error {
	lease := &coordinationv1.Lease{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: checkpoint.LeaseName}
	if err := verifier.Reader.Get(ctx, key, lease); err != nil {
		return fmt.Errorf("read live PostgreSQL writable Lease %s/%s: %w", key.Namespace, key.Name, err)
	}
	if err := validatePostgreSQLWritableLeaseMetadata(lease, cluster, 0); err != nil {
		return err
	}
	if err := validatePostgreSQLWritableLeaseRuntimeSpec(lease.Spec); err != nil {
		return fmt.Errorf("live PostgreSQL writable Lease runtime is invalid: %w", err)
	}
	if lease.UID != request.WritableTerm.UID || lease.ResourceVersion != request.WritableTerm.ResourceVersion || lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != request.WritableTerm.Holder || lease.Spec.LeaseTransitions == nil || strconv.FormatInt(int64(*lease.Spec.LeaseTransitions), 10) != request.WritableTerm.Generation {
		return fmt.Errorf("live PostgreSQL writable Lease term differs from the activation request")
	}
	return nil
}

func (verifier *CatalogActivationPublicationVerifier) validateBootstrap(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, bootstrap pgshardv1alpha1.PostgreSQLBootstrapStatus, request *pgshardv1alpha1.CatalogActivationRequest) error {
	secret := &corev1.Secret{}
	if err := verifier.Reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.SecretName}, secret); err != nil {
		return fmt.Errorf("read live bootstrap Secret %s/%s: %w", cluster.Namespace, bootstrap.SecretName, err)
	}
	if secret.UID != request.Bootstrap.Secret.UID || secret.ResourceVersion == "" || secret.DeletionTimestamp != nil || len(secret.Finalizers) != 0 || len(secret.StringData) != 0 || !postgresqlCredentialIsDataAnchored(secret, bootstrap) {
		return fmt.Errorf("live bootstrap Secret identity or PVC owner differs from the activation request")
	}
	if err := validatePostgreSQLAuthSecret(secret, cluster, bootstrap, bootstrap.SecretName); err != nil {
		return err
	}
	expectedSecret := owned.PostgreSQLMemberAuthSecret(cluster, 0, 0, bootstrap.SecretName, nil)
	if secret.GenerateName != "" || !maps.Equal(secret.Labels, expectedSecret.Labels) || !maps.Equal(secret.Annotations, expectedSecret.Annotations) {
		return fmt.Errorf("live bootstrap Secret operator-owned metadata differs from its checkpoint")
	}
	claim := &corev1.PersistentVolumeClaim{}
	if err := verifier.Reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}, claim); err != nil {
		return fmt.Errorf("read live bootstrap PVC %s/%s: %w", cluster.Namespace, bootstrap.PVCName, err)
	}
	if claim.UID != request.Bootstrap.PVC.UID || claim.ResourceVersion == "" || claim.DeletionTimestamp != nil || len(claim.OwnerReferences) != 0 || !postgresqlDataPVCIsProtected(claim) {
		return fmt.Errorf("live bootstrap PVC identity, owner, or protection finalizer differs from the activation request")
	}
	expectedClaim := owned.PostgreSQLMemberDataPVC(cluster, 0, 0, bootstrap.PVCName, cluster.Spec.Storage.Size, bootstrap.PVCStorageClassName, bootstrap.SecretName, bootstrap.SecretUID)
	if claim.GenerateName != "" || !maps.Equal(claim.Labels, expectedClaim.Labels) || !reservedAnnotationsEqual(claim.Annotations, expectedClaim.Annotations) {
		return fmt.Errorf("live bootstrap PVC operator-owned metadata differs from its checkpoint")
	}
	return validatePostgreSQLDataPVC(claim, cluster, bootstrap, cluster.Spec.Storage.Size)
}

func (verifier *CatalogActivationPublicationVerifier) validateReplication(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, checkpoint pgshardv1alpha1.PostgreSQLReplicationCredentialStatus, request *pgshardv1alpha1.CatalogActivationRequest) error {
	secret := &corev1.Secret{}
	if err := verifier.Reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: checkpoint.SecretName}, secret); err != nil {
		return fmt.Errorf("read live replication Secret %s/%s: %w", cluster.Namespace, checkpoint.SecretName, err)
	}
	if secret.UID != request.Materials.Replication.UID || secret.ResourceVersion == "" || len(secret.StringData) != 0 {
		return fmt.Errorf("live replication Secret identity differs from the activation request")
	}
	return validateCheckpointedPostgreSQLReplicationCredential(secret, cluster, &checkpoint)
}

func (verifier *CatalogActivationPublicationVerifier) validateCatalogAccess(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, request *pgshardv1alpha1.CatalogActivationRequest) ([]byte, error) {
	secret := &corev1.Secret{}
	if err := verifier.Reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: request.Materials.Catalog.Name}, secret); err != nil {
		return nil, fmt.Errorf("read live catalog access Secret %s/%s: %w", cluster.Namespace, request.Materials.Catalog.Name, err)
	}
	recorded := cluster.Status.CatalogAccess
	if secret.UID != request.Materials.Catalog.UID || secret.ResourceVersion == "" || len(secret.StringData) != 0 || recorded == nil {
		return nil, fmt.Errorf("live catalog access Secret identity differs from the activation request")
	}
	if err := validateCheckpointedCatalogAccess(secret, cluster, recorded); err != nil {
		return nil, err
	}
	return append([]byte(nil), secret.Data[owned.CatalogCACertificateKey]...), nil
}

func (verifier *CatalogActivationPublicationVerifier) validateOperationWriter(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, catalogCA []byte, request *pgshardv1alpha1.CatalogActivationRequest) error {
	secret := &corev1.Secret{}
	if err := verifier.Reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: request.Materials.OperationWriter.Name}, secret); err != nil {
		return fmt.Errorf("read live operation-writer Secret %s/%s: %w", cluster.Namespace, request.Materials.OperationWriter.Name, err)
	}
	recorded := cluster.Status.OperationWriterAccess
	if secret.UID != request.Materials.OperationWriter.UID || secret.ResourceVersion == "" || len(secret.StringData) != 0 || recorded == nil {
		return fmt.Errorf("live operation-writer Secret identity differs from the activation request")
	}
	return validateCheckpointedOperationWriterAccess(secret, cluster, recorded, catalogCA)
}

func (verifier *CatalogActivationPublicationVerifier) validatePostgreSQLConfiguration(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, request *pgshardv1alpha1.CatalogActivationRequest) error {
	live := &corev1.ConfigMap{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: request.Materials.PostgreSQLConfiguration.Name}
	if err := verifier.Reader.Get(ctx, key, live); err != nil {
		return fmt.Errorf("read live PostgreSQL configuration ConfigMap %s/%s: %w", key.Namespace, key.Name, err)
	}
	desired, err := owned.DesiredPostgreSQLConfigurationConfigMap(cluster)
	if err != nil {
		return fmt.Errorf("render exact PostgreSQL configuration publication: %w", err)
	}
	if live.UID != request.Materials.PostgreSQLConfiguration.UID || live.ResourceVersion == "" || owned.PostgreSQLConfigurationDataSHA256(live) != request.Materials.PostgreSQLConfiguration.MaterialSHA256 {
		return fmt.Errorf("live PostgreSQL configuration identity or digest differs from the activation request")
	}
	return validatePostgreSQLConfigurationConfigMap(live, desired, cluster)
}

// validatePublicationFence repeats the three mutable observation roots after
// every referenced immutable object has been checked. This is not a global API
// transaction, but it prevents a cluster-status or elected-term change during
// the read bracket from being admitted as one coherent publication. Lease wall
// timestamps cannot prove the publisher's suspend-aware monotonic authority;
// the publisher must revalidate that local grant immediately before its CAS.
func (verifier *CatalogActivationPublicationVerifier) validatePublicationFence(ctx context.Context, request *pgshardv1alpha1.CatalogActivationRequest) error {
	cluster, rawStatus, err := verifier.readCluster(ctx, request)
	if err != nil {
		return fmt.Errorf("re-read publication fence: %w", err)
	}
	if err := validateActivationClusterPublication(cluster, rawStatus, request); err != nil {
		return fmt.Errorf("revalidate publication fence: %w", err)
	}
	for _, observation := range []struct {
		name string
		uid  types.UID
		rv   string
	}{
		{name: request.Dispatcher.LeaseName, uid: request.Dispatcher.LeaseUID, rv: request.Dispatcher.LeaseResourceVersion},
		{name: request.WritableTerm.Name, uid: request.WritableTerm.UID, rv: request.WritableTerm.ResourceVersion},
	} {
		lease := &coordinationv1.Lease{}
		if err := verifier.Reader.Get(ctx, types.NamespacedName{Namespace: request.Cluster.Namespace, Name: observation.name}, lease); err != nil {
			return fmt.Errorf("re-read live Lease %s/%s: %w", request.Cluster.Namespace, observation.name, err)
		}
		if lease.UID != observation.uid || lease.ResourceVersion != observation.rv || lease.DeletionTimestamp != nil {
			return fmt.Errorf("live Lease %s changed during activation publication verification", observation.name)
		}
	}
	return nil
}

func exactOwnedMetadata(metadata metav1.ObjectMeta, cluster *pgshardv1alpha1.PgShardCluster, name, component string, extraLabels, extraAnnotations map[string]string) bool {
	labels := map[string]string{
		"app.kubernetes.io/name": "pgshard", owned.ManagedByLabel: owned.ManagedByValue,
		owned.InstanceLabel: cluster.Name, owned.ComponentLabel: component, owned.ClusterLabel: cluster.Name,
	}
	maps.Copy(labels, extraLabels)
	annotations := map[string]string{owned.ApplyOwnershipAnnotation: owned.ApplyOwnershipVersion}
	maps.Copy(annotations, extraAnnotations)
	return metadata.Name == name && metadata.Namespace == cluster.Namespace && metadata.GenerateName == "" && maps.Equal(metadata.Labels, labels) && maps.Equal(metadata.Annotations, annotations) && exactClusterOwner(metadata.OwnerReferences, cluster) && len(metadata.Finalizers) == 0 && metadata.DeletionTimestamp == nil
}

func componentSelectorLabels(cluster, component string, extra map[string]string) map[string]string {
	expected := map[string]string{owned.ClusterLabel: cluster, owned.ComponentLabel: component}
	maps.Copy(expected, extra)
	return expected
}

func reservedAnnotationsEqual(actual, expected map[string]string) bool {
	for key, value := range expected {
		if actual[key] != value {
			return false
		}
	}
	for key := range actual {
		if strings.HasPrefix(key, "pgshard.io/") {
			if _, ok := expected[key]; !ok {
				return false
			}
		}
	}
	return true
}

func exactClusterOwner(owners []metav1.OwnerReference, cluster *pgshardv1alpha1.PgShardCluster) bool {
	owner := exactControllerOwner(owners, pgshardv1alpha1.GroupVersion.String(), "PgShardCluster")
	return owner != nil && owner.Name == cluster.Name && owner.UID == cluster.UID
}

func exactControllerOwner(owners []metav1.OwnerReference, apiVersion, kind string) *metav1.OwnerReference {
	if len(owners) != 1 {
		return nil
	}
	owner := &owners[0]
	if owner.APIVersion != apiVersion || owner.Kind != kind || owner.Name == "" || owner.UID == "" || owner.Controller == nil || !*owner.Controller || owner.BlockOwnerDeletion == nil || !*owner.BlockOwnerDeletion {
		return nil
	}
	return owner
}

func statefulSetPodMetadataMatches(statefulSet *appsv1.StatefulSet, pod *corev1.Pod, bootID string) bool {
	labels := maps.Clone(pod.Labels)
	knownLabels := map[string]string{
		appsv1.StatefulSetPodNameLabel:        pod.Name,
		appsv1.PodIndexLabel:                  "0",
		appsv1.ControllerRevisionHashLabelKey: statefulSet.Status.CurrentRevision,
	}
	for key, expected := range knownLabels {
		if labels[key] != expected {
			return false
		}
		delete(labels, key)
	}
	annotations := maps.Clone(pod.Annotations)
	if annotations[podfence.NodeUIDAnnotation] == "" || annotations[podfence.NodeBootIDAnnotation] != bootID {
		return false
	}
	delete(annotations, podfence.NodeUIDAnnotation)
	delete(annotations, podfence.NodeBootIDAnnotation)
	return maps.Equal(labels, statefulSet.Spec.Template.Labels) && maps.Equal(annotations, statefulSet.Spec.Template.Annotations)
}

func activationGenerationDurability(cluster *pgshardv1alpha1.PgShardCluster) (string, string) {
	if cluster.Spec.Durability == pgshardv1alpha1.DurabilityAsynchronous {
		return "local", ""
	}
	standbys := "pgshard_member_0001,pgshard_member_0002"
	if cluster.Spec.MembersPerShard == 5 {
		standbys += ",pgshard_member_0003,pgshard_member_0004"
	}
	return "remote-apply-any-one", standbys
}

type activationSecretProjection struct {
	source        corev1.SecretVolumeSource
	containerKind string
	containerName string
	mount         corev1.VolumeMount
}

func activationDispatcherSecretAllowances(request *pgshardv1alpha1.CatalogActivationRequest) map[string]activationSecretProjection {
	return map[string]activationSecretProjection{
		"catalog-activation-ca": {
			source: corev1.SecretVolumeSource{
				SecretName:  request.Materials.Catalog.Name,
				DefaultMode: activationMode0440(),
				Items: []corev1.KeyToPath{{
					Key: owned.CatalogCACertificateKey, Path: "ca.crt", Mode: activationMode0440(),
				}},
			},
			containerKind: "container",
			containerName: "orchestrator",
			mount:         corev1.VolumeMount{Name: "catalog-activation-ca", MountPath: catalogActivationCAMountPath, ReadOnly: true},
		},
	}
}

func activationContainerHasExactLiteralEnvironment(spec corev1.PodSpec, containerName, variableName, value string) bool {
	foundContainer := false
	foundVariable := false
	for _, container := range spec.Containers {
		if container.Name != containerName {
			continue
		}
		if foundContainer {
			return false
		}
		foundContainer = true
		for _, variable := range container.Env {
			if variable.Name != variableName {
				continue
			}
			if foundVariable || variable.Value != value || variable.ValueFrom != nil {
				return false
			}
			foundVariable = true
		}
	}
	return foundContainer && foundVariable
}

func activationPostgreSQLSecretAllowances(request *pgshardv1alpha1.CatalogActivationRequest, source bool) map[string]activationSecretProjection {
	bootstrapContainer := "bootstrap-standby"
	if source {
		bootstrapContainer = "bootstrap-postgresql"
	}
	result := map[string]activationSecretProjection{
		"replication-credential": {
			source: corev1.SecretVolumeSource{
				SecretName:  request.Materials.Replication.Name,
				DefaultMode: activationMode0440(),
				Items: []corev1.KeyToPath{{
					Key: owned.PostgreSQLReplicationPasswordKey, Path: "replication-password", Mode: activationMode0440(),
				}},
			},
			containerKind: "init container",
			containerName: bootstrapContainer,
			mount:         corev1.VolumeMount{Name: "replication-credential", MountPath: "/etc/pgshard/replication", ReadOnly: true},
		},
	}
	if source {
		result["bootstrap-secret"] = activationSecretProjection{
			source: corev1.SecretVolumeSource{
				SecretName: request.Bootstrap.Secret.Name, DefaultMode: activationMode0440(),
			},
			containerKind: "init container",
			containerName: "bootstrap-postgresql",
			mount:         corev1.VolumeMount{Name: "bootstrap-secret", MountPath: "/etc/pgshard/bootstrap", ReadOnly: true},
		}
		result["catalog-activation-tls"] = activationSecretProjection{
			source: corev1.SecretVolumeSource{
				SecretName:  request.Materials.Catalog.Name,
				DefaultMode: activationMode0440(),
				Items: []corev1.KeyToPath{
					{Key: owned.CatalogTLSCertificateKey, Path: "tls.crt", Mode: activationMode0440()},
					{Key: owned.CatalogTLSPrivateKeyKey, Path: "tls.key", Mode: activationMode0440()},
				},
			},
			containerKind: "container",
			containerName: "postgresql",
			mount:         corev1.VolumeMount{Name: "catalog-activation-tls", MountPath: catalogActivationTLSMountPath, ReadOnly: true},
		}
	}
	return result
}

func activationMode0440() *int32 {
	mode := int32(0o440)
	return &mode
}

func validateActivationPodSecretBoundary(context string, spec corev1.PodSpec, allowed map[string]activationSecretProjection) error {
	return validateActivationPodAuthorityBoundary(context, spec, allowed, false, false)
}

func validateActivationPodAuthorityBoundary(context string, spec corev1.PodSpec, allowed map[string]activationSecretProjection, allowServiceAccountInjection, allowPostgreSQLAgentToken bool) error {
	if len(spec.ImagePullSecrets) != 0 {
		return fmt.Errorf("%s Secret projection includes image-pull credentials", context)
	}
	if len(spec.EphemeralContainers) != 0 {
		return fmt.Errorf("%s Secret projection includes ephemeral containers", context)
	}
	seenNames := make(map[string]struct{}, len(spec.Volumes))
	seenSources := make(map[string]struct{}, len(allowed))
	for _, volume := range spec.Volumes {
		if _, duplicate := seenNames[volume.Name]; duplicate {
			return fmt.Errorf("%s Secret projection has duplicate volume %s", context, volume.Name)
		}
		seenNames[volume.Name] = struct{}{}
		if volume.Secret != nil {
			expected, ok := allowed[volume.Name]
			if !ok {
				return fmt.Errorf("%s Secret projection includes unauthorized direct Secret volume %s", context, volume.Name)
			}
			expectedSource := corev1.VolumeSource{Secret: expected.source.DeepCopy()}
			if !apiequality.Semantic.DeepEqual(volume.VolumeSource, expectedSource) {
				return fmt.Errorf("%s Secret projection for volume %s is not the exact key and mode allowlist", context, volume.Name)
			}
			seenSources[volume.Name] = struct{}{}
		}
		if volume.Projected != nil {
			for _, projection := range volume.Projected.Sources {
				if projection.Secret != nil {
					return fmt.Errorf("%s Secret projection includes unauthorized projected Secret volume %s", context, volume.Name)
				}
				if projection.ServiceAccountToken != nil || projection.PodCertificate != nil || projection.ClusterTrustBundle != nil {
					if (!allowServiceAccountInjection || !exactServiceAccountVolume(volume)) && (!allowPostgreSQLAgentToken || !exactPostgreSQLAgentTokenVolume(volume)) {
						return fmt.Errorf("%s authority projection includes unauthorized token or private-key volume %s", context, volume.Name)
					}
				}
			}
		}
		if activationVolumeHasSecretReference(volume.VolumeSource) {
			return fmt.Errorf("%s Secret projection includes unauthorized Secret reference in volume %s", context, volume.Name)
		}
	}
	for name := range allowed {
		if _, ok := seenSources[name]; !ok {
			return fmt.Errorf("%s Secret projection is missing exact volume %s", context, name)
		}
	}
	if allowPostgreSQLAgentToken && !exactPostgreSQLAgentTokenExposure(spec) {
		return fmt.Errorf("%s authority projection does not expose the exact PostgreSQL agent token", context)
	}

	mountCounts := make(map[string]int, len(allowed))
	for _, container := range spec.InitContainers {
		if err := validateActivationContainerSecrets(context, "init container", container.Name, container.Env, container.EnvFrom, container.VolumeMounts, container.VolumeDevices, allowed, mountCounts); err != nil {
			return err
		}
	}
	for _, container := range spec.Containers {
		if err := validateActivationContainerSecrets(context, "container", container.Name, container.Env, container.EnvFrom, container.VolumeMounts, container.VolumeDevices, allowed, mountCounts); err != nil {
			return err
		}
	}
	for name := range allowed {
		if mountCounts[name] != 1 {
			return fmt.Errorf("%s Secret projection volume %s must have exactly one authorized mount", context, name)
		}
	}
	return nil
}

func activationVolumeHasSecretReference(source corev1.VolumeSource) bool {
	return source.ISCSI != nil && source.ISCSI.SecretRef != nil ||
		source.RBD != nil && source.RBD.SecretRef != nil ||
		source.FlexVolume != nil && source.FlexVolume.SecretRef != nil ||
		source.Cinder != nil && source.Cinder.SecretRef != nil ||
		source.CephFS != nil && source.CephFS.SecretRef != nil ||
		source.AzureFile != nil && source.AzureFile.SecretName != "" ||
		source.ScaleIO != nil && source.ScaleIO.SecretRef != nil ||
		source.StorageOS != nil && source.StorageOS.SecretRef != nil ||
		source.CSI != nil && source.CSI.NodePublishSecretRef != nil
}

func validateActivationContainerSecrets(context, kind, name string, environment []corev1.EnvVar, environmentFrom []corev1.EnvFromSource, mounts []corev1.VolumeMount, devices []corev1.VolumeDevice, allowed map[string]activationSecretProjection, mountCounts map[string]int) error {
	for _, source := range environmentFrom {
		if source.SecretRef != nil {
			return fmt.Errorf("%s Secret projection includes Secret envFrom on %s %s", context, kind, name)
		}
	}
	for _, variable := range environment {
		if variable.ValueFrom != nil && variable.ValueFrom.SecretKeyRef != nil {
			return fmt.Errorf("%s Secret projection includes SecretKeyRef on %s %s", context, kind, name)
		}
	}
	for _, mount := range mounts {
		expected, ok := allowed[mount.Name]
		if !ok {
			continue
		}
		if expected.containerKind != kind || expected.containerName != name || !apiequality.Semantic.DeepEqual(mount, expected.mount) {
			return fmt.Errorf("%s Secret projection volume %s is mounted outside its exact least-authority container and path", context, mount.Name)
		}
		mountCounts[mount.Name]++
	}
	for _, device := range devices {
		if _, ok := allowed[device.Name]; ok {
			return fmt.Errorf("%s Secret projection volume %s is exposed as a block device", context, device.Name)
		}
	}
	return nil
}

// LimitRange mutation is deliberately unsupported. The only resource
// normalization below is the core API's exact request-from-corresponding-limit
// default for regular and init containers.
func activationPodTemplateSpecMatches(parent, child corev1.PodSpec) bool {
	return apiequality.Semantic.DeepEqual(&parent, &child)
}

func activationDispatcherPodSpecMatches(parent, child corev1.PodSpec) bool {
	normalized := child.DeepCopy()
	if !normalizeActivationContainerResourceDefaults(parent, normalized) || !removeExactServiceAccountInjection(parent, normalized) || !normalizeExactPodControllerDefaults(parent, normalized) {
		return false
	}
	return apiequality.Semantic.DeepEqual(&parent, normalized)
}

func activationStatefulSetPodSpecMatches(parent, child corev1.PodSpec, hostname, subdomain string) bool {
	normalized := child.DeepCopy()
	if normalized.Hostname != hostname || normalized.Subdomain != subdomain {
		return false
	}
	normalized.Hostname, normalized.Subdomain = "", ""
	if !normalizeActivationContainerResourceDefaults(parent, normalized) || !normalizeExactPodControllerDefaults(parent, normalized) {
		return false
	}
	return apiequality.Semantic.DeepEqual(&parent, normalized)
}

func normalizeActivationContainerResourceDefaults(parent corev1.PodSpec, child *corev1.PodSpec) bool {
	if len(parent.Containers) != len(child.Containers) || len(parent.InitContainers) != len(child.InitContainers) {
		return false
	}
	for index := range child.Containers {
		if parent.Containers[index].Name != child.Containers[index].Name || !normalizeActivationResourceDefaults(parent.Containers[index].Resources, &child.Containers[index].Resources) {
			return false
		}
	}
	for index := range child.InitContainers {
		if parent.InitContainers[index].Name != child.InitContainers[index].Name || !normalizeActivationResourceDefaults(parent.InitContainers[index].Resources, &child.InitContainers[index].Resources) {
			return false
		}
	}
	return true
}

func normalizeActivationResourceDefaults(parent corev1.ResourceRequirements, child *corev1.ResourceRequirements) bool {
	for name, limit := range parent.Limits {
		if _, explicit := parent.Requests[name]; explicit {
			continue
		}
		request, defaulted := child.Requests[name]
		if !defaulted || request.Cmp(limit) != 0 {
			continue
		}
		delete(child.Requests, name)
	}
	if parent.Requests == nil && len(child.Requests) == 0 {
		child.Requests = nil
	}
	return true
}

func removeExactServiceAccountInjection(parent corev1.PodSpec, child *corev1.PodSpec) bool {
	if parent.AutomountServiceAccountToken == nil || !*parent.AutomountServiceAccountToken {
		return false
	}
	if len(child.Volumes) != len(parent.Volumes)+1 || len(child.InitContainers) != len(parent.InitContainers) || len(child.Containers) != len(parent.Containers) || len(child.EphemeralContainers) != len(parent.EphemeralContainers) {
		return false
	}
	if len(parent.InitContainers) != 0 || len(parent.EphemeralContainers) != 0 {
		return false
	}
	volume := child.Volumes[len(child.Volumes)-1]
	if !exactServiceAccountVolume(volume) || len(child.Containers) != 1 || child.Containers[0].Name != "orchestrator" || len(child.Containers[0].VolumeMounts) != len(parent.Containers[0].VolumeMounts)+1 {
		return false
	}
	mount := child.Containers[0].VolumeMounts[len(child.Containers[0].VolumeMounts)-1]
	expectedMount := corev1.VolumeMount{Name: volume.Name, ReadOnly: true, MountPath: serviceAccountMountPath}
	if !apiequality.Semantic.DeepEqual(mount, expectedMount) {
		return false
	}
	child.Volumes = child.Volumes[:len(child.Volumes)-1]
	child.Containers[0].VolumeMounts = child.Containers[0].VolumeMounts[:len(child.Containers[0].VolumeMounts)-1]
	return true
}

func exactServiceAccountVolume(volume corev1.Volume) bool {
	if !strings.HasPrefix(volume.Name, serviceAccountVolumePrefix) || len(volume.Name) != len(serviceAccountVolumePrefix)+5 || len(validation.IsDNS1123Label(volume.Name)) != 0 {
		return false
	}
	mode, expiration := int32(0o644), int64(3607)
	expected := corev1.Volume{
		Name: volume.Name,
		VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
			DefaultMode: &mode,
			Sources: []corev1.VolumeProjection{
				{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token", ExpirationSeconds: &expiration}},
				{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"}, Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}},
				{DownwardAPI: &corev1.DownwardAPIProjection{Items: []corev1.DownwardAPIVolumeFile{{Path: "namespace", FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}}}},
			},
		}},
	}
	return apiequality.Semantic.DeepEqual(volume, expected)
}

func exactPostgreSQLAgentTokenVolume(volume corev1.Volume) bool {
	mode, expiration := int32(0o440), int64(600)
	expected := corev1.Volume{
		Name: "kubernetes-api",
		VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
			DefaultMode: &mode,
			Sources: []corev1.VolumeProjection{
				{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token", ExpirationSeconds: &expiration}},
				{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"}, Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}},
				{DownwardAPI: &corev1.DownwardAPIProjection{Items: []corev1.DownwardAPIVolumeFile{{Path: "namespace", FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}}}},
			},
		}},
	}
	return apiequality.Semantic.DeepEqual(volume, expected)
}

func exactPostgreSQLAgentTokenExposure(spec corev1.PodSpec) bool {
	volumeCount, mountCount := 0, 0
	expectedMount := corev1.VolumeMount{Name: "kubernetes-api", MountPath: serviceAccountMountPath, ReadOnly: true}
	for _, volume := range spec.Volumes {
		if volume.Name == "kubernetes-api" {
			volumeCount++
			if !exactPostgreSQLAgentTokenVolume(volume) {
				return false
			}
		}
	}
	for _, container := range spec.InitContainers {
		for _, mount := range container.VolumeMounts {
			if mount.Name == "kubernetes-api" {
				return false
			}
		}
	}
	for _, container := range spec.Containers {
		for _, mount := range container.VolumeMounts {
			if mount.Name != "kubernetes-api" {
				continue
			}
			if container.Name != "postgresql" || !apiequality.Semantic.DeepEqual(mount, expectedMount) {
				return false
			}
			mountCount++
		}
	}
	return volumeCount == 1 && mountCount == 1
}

func normalizeExactPodControllerDefaults(parent corev1.PodSpec, child *corev1.PodSpec) bool {
	if parent.NodeName == "" && child.NodeName != "" {
		child.NodeName = ""
	}
	if parent.DeprecatedServiceAccount == "" && child.DeprecatedServiceAccount == parent.ServiceAccountName {
		child.DeprecatedServiceAccount = ""
	}
	if parent.Priority == nil && child.Priority != nil && *child.Priority == 0 {
		child.Priority = nil
	}
	if parent.PreemptionPolicy == nil && child.PreemptionPolicy != nil && *child.PreemptionPolicy == corev1.PreemptLowerPriority {
		child.PreemptionPolicy = nil
	}
	if len(child.Tolerations) == len(parent.Tolerations)+2 &&
		apiequality.Semantic.DeepEqual(child.Tolerations[:len(parent.Tolerations)], parent.Tolerations) &&
		exactDefaultNoExecuteTolerations(child.Tolerations[len(parent.Tolerations):]) {
		child.Tolerations = child.Tolerations[:len(parent.Tolerations)]
	}
	return true
}

func exactDefaultNoExecuteTolerations(tolerations []corev1.Toleration) bool {
	seconds := int64(300)
	expected := []corev1.Toleration{
		{Key: "node.kubernetes.io/not-ready", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds},
		{Key: "node.kubernetes.io/unreachable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds},
	}
	return apiequality.Semantic.DeepEqual(tolerations, expected)
}

func podSpecReferencesNamedPVC(spec corev1.PodSpec, name string) bool {
	for _, volume := range spec.Volumes {
		if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == name {
			return true
		}
	}
	return false
}
func podSpecReferencesSecret(spec corev1.PodSpec, name string) bool {
	for _, volume := range spec.Volumes {
		if volume.Secret != nil && volume.Secret.SecretName == name {
			return true
		}
	}
	return false
}
func activationPodSpecReferencesConfigMap(spec corev1.PodSpec, name string) bool {
	for _, volume := range spec.Volumes {
		if volume.ConfigMap != nil && volume.ConfigMap.Name == name {
			return true
		}
	}
	return false
}
