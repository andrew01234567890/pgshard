package v1alpha1

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/andrew01234567890/pgshard/operator/internal/tuning"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	apivalidation "k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const maximumChangeStreams = 4

// PgShardClusterDefaulter supplies safety-oriented defaults.
type PgShardClusterDefaulter struct{}

func (*PgShardClusterDefaulter) Default(_ context.Context, cluster *PgShardCluster) error {
	if cluster.Spec.Shards == 0 {
		cluster.Spec.Shards = 1
	}
	if cluster.Spec.MembersPerShard == 0 {
		cluster.Spec.MembersPerShard = 3
	}
	if cluster.Spec.Durability == "" {
		cluster.Spec.Durability = DurabilitySynchronous
	}
	if cluster.Spec.PostgreSQL.Version == "" {
		cluster.Spec.PostgreSQL.Version = PostgreSQLMajor18
	}
	if cluster.Spec.Storage.DeletionPolicy == "" {
		cluster.Spec.Storage.DeletionPolicy = DeletionRetain
	}
	defaultScaling(&cluster.Spec.Pooler.Scaling)
	defaultService(&cluster.Spec.Services.ReadWrite)
	defaultService(&cluster.Spec.Services.ReadOnly)
	defaultService(&cluster.Spec.Services.Read)
	if cluster.Spec.Observability.Prometheus == nil {
		enabled := true
		cluster.Spec.Observability.Prometheus = &enabled
	}
	return nil
}

func defaultScaling(scaling *PoolerScaling) {
	if scaling.Mode == "" {
		scaling.Mode = ScalingHPA
	}
	if scaling.Mode == ScalingHPA {
		if scaling.HPA == nil {
			scaling.HPA = &HPAScaling{}
		}
		if scaling.HPA.MinReplicas == 0 {
			scaling.HPA.MinReplicas = 2
		}
		if scaling.HPA.MaxReplicas == 0 {
			scaling.HPA.MaxReplicas = 10
		}
		if scaling.HPA.TargetCPUUtilizationPercentage == 0 {
			scaling.HPA.TargetCPUUtilizationPercentage = 65
		}
	}
}

func defaultService(service *ServiceTemplate) {
	if service.Type == "" {
		service.Type = corev1.ServiceTypeClusterIP
	}
}

// PgShardClusterValidator applies validation that cannot be represented safely
// by OpenAPI alone.
type PgShardClusterValidator struct{}

func (v *PgShardClusterValidator) ValidateCreate(_ context.Context, cluster *PgShardCluster) (admission.Warnings, error) {
	return warningsFor(cluster), validateCluster(cluster)
}

func (v *PgShardClusterValidator) ValidateUpdate(_ context.Context, oldCluster, newCluster *PgShardCluster) (admission.Warnings, error) {
	if !newCluster.DeletionTimestamp.IsZero() {
		// A deleting object must always be able to shed finalizers, including if
		// it predates validation that its stored spec no longer satisfies.
		return warningsFor(newCluster), nil
	}
	allErrs := validateClusterFields(newCluster)
	if oldCluster.Spec.PostgreSQL.Version != newCluster.Spec.PostgreSQL.Version {
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "postgresql", "version"), newCluster.Spec.PostgreSQL.Version, "PostgreSQL major is immutable"))
	}
	if oldCluster.Spec.Shards != newCluster.Spec.Shards {
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "shards"), newCluster.Spec.Shards, "shards is immutable until online resharding is implemented"))
	}
	if oldCluster.Spec.MembersPerShard != newCluster.Spec.MembersPerShard {
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "membersPerShard"), newCluster.Spec.MembersPerShard, "membersPerShard is immutable until membership transitions are implemented"))
	}
	if oldCluster.Spec.Durability != newCluster.Spec.Durability {
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "durability"), newCluster.Spec.Durability, "durability is immutable until replication-mode transitions are implemented"))
	}
	if !equalOptionalString(oldCluster.Spec.Storage.StorageClassName, newCluster.Spec.Storage.StorageClassName) {
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "storage", "storageClassName"), newCluster.Spec.Storage.StorageClassName, "storage class is immutable after cluster creation"))
	}
	if !oldCluster.Spec.Storage.Size.Equal(newCluster.Spec.Storage.Size) {
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "storage", "size"), newCluster.Spec.Storage.Size.String(), "storage size is immutable until explicit PVC expansion is implemented"))
	}
	if oldCluster.Spec.Storage.DeletionPolicy != newCluster.Spec.Storage.DeletionPolicy {
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "storage", "deletionPolicy"), newCluster.Spec.Storage.DeletionPolicy, "deletion policy is immutable after cluster creation"))
	}
	return warningsFor(newCluster), invalidIfAny(newCluster.Name, allErrs)
}

func equalOptionalString(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func (*PgShardClusterValidator) ValidateDelete(_ context.Context, _ *PgShardCluster) (admission.Warnings, error) {
	return nil, nil
}

func warningsFor(cluster *PgShardCluster) admission.Warnings {
	if cluster.Spec.MembersPerShard == 1 {
		return admission.Warnings{"single-member topology has no standby or failover, and restarting its primary interrupts the shard"}
	}
	if cluster.Spec.Durability == DurabilityAsynchronous {
		return admission.Warnings{"asynchronous replication can lose acknowledged transactions during failover"}
	}
	return nil
}

func validateCluster(cluster *PgShardCluster) error {
	return invalidIfAny(cluster.Name, validateClusterFields(cluster))
}

// ValidateClusterForReconciliation defensively reapplies all admission safety
// invariants before any child resource is planned. Admission configuration can
// be temporarily absent and stored objects can predate newer validation.
func ValidateClusterForReconciliation(cluster *PgShardCluster) error {
	return validateCluster(cluster)
}

func invalidIfAny(name string, allErrs field.ErrorList) error {
	if len(allErrs) == 0 {
		return nil
	}
	return apierrors.NewInvalid(schema.GroupKind{Group: GroupVersion.Group, Kind: "PgShardCluster"}, name, allErrs)
}

func validateClusterFields(cluster *PgShardCluster) field.ErrorList {
	specPath := field.NewPath("spec")
	var allErrs field.ErrorList
	namePath := field.NewPath("metadata", "name")
	if messages := validation.IsDNS1123Label(cluster.Name); len(messages) != 0 {
		allErrs = append(allErrs, field.Invalid(namePath, cluster.Name, "must be a DNS-1123 label because it prefixes owned Services"))
	}
	if len(cluster.Name) > MaximumClusterNameLength {
		allErrs = append(allErrs, field.TooLong(namePath, cluster.Name, MaximumClusterNameLength))
	}
	if cluster.Spec.Shards < 1 {
		allErrs = append(allErrs, field.Invalid(specPath.Child("shards"), cluster.Spec.Shards, "must be at least 1"))
	}
	if cluster.Spec.Shards > MaximumShards {
		allErrs = append(allErrs, field.Invalid(specPath.Child("shards"), cluster.Spec.Shards, fmt.Sprintf("must not exceed %d", MaximumShards)))
	}
	if cluster.Spec.MembersPerShard != 1 && cluster.Spec.MembersPerShard != 3 && cluster.Spec.MembersPerShard != 5 {
		allErrs = append(allErrs, field.NotSupported(specPath.Child("membersPerShard"), cluster.Spec.MembersPerShard, []string{"1", "3", "5"}))
	}
	if cluster.Spec.Durability != DurabilitySynchronous && cluster.Spec.Durability != DurabilityAsynchronous {
		allErrs = append(allErrs, field.NotSupported(specPath.Child("durability"), cluster.Spec.Durability, []string{string(DurabilitySynchronous), string(DurabilityAsynchronous)}))
	}
	if cluster.Spec.Durability == DurabilitySynchronous && cluster.Spec.MembersPerShard < 3 {
		allErrs = append(allErrs, field.Invalid(specPath.Child("membersPerShard"), cluster.Spec.MembersPerShard, "synchronous durability requires at least 3 members per shard"))
	}
	if cluster.Spec.PostgreSQL.Version != PostgreSQLMajor18 {
		allErrs = append(allErrs, field.NotSupported(specPath.Child("postgresql", "version"), cluster.Spec.PostgreSQL.Version, []string{PostgreSQLMajor18}))
	}
	if cluster.Spec.Storage.Size.Cmp(resource.MustParse("4Gi")) < 0 {
		allErrs = append(allErrs, field.Invalid(specPath.Child("storage", "size"), cluster.Spec.Storage.Size.String(), "must be at least 4Gi"))
	}
	if storageClass := cluster.Spec.Storage.StorageClassName; storageClass != nil && *storageClass != "" {
		if err := ValidateObjectReferenceName(*storageClass); err != nil {
			allErrs = append(allErrs, field.Invalid(specPath.Child("storage", "storageClassName"), *storageClass, err.Error()))
		}
	}
	if cluster.Spec.Storage.DeletionPolicy != DeletionRetain && cluster.Spec.Storage.DeletionPolicy != DeletionDelete {
		allErrs = append(allErrs, field.NotSupported(specPath.Child("storage", "deletionPolicy"), cluster.Spec.Storage.DeletionPolicy, []string{string(DeletionRetain), string(DeletionDelete)}))
	}

	poolerMax, scalingErrs := validateScaling(cluster.Spec.Pooler.Scaling, specPath.Child("pooler", "scaling"))
	allErrs = append(allErrs, scalingErrs...)
	settingsForOverrides := map[string]string{}
	if poolerMax > 0 {
		result, err := tuning.Calculate(tuning.Input{
			Resources:            cluster.Spec.PostgreSQL.Resources,
			PoolerMaxReplicas:    poolerMax,
			MembersPerShard:      cluster.Spec.MembersPerShard,
			MaximumChangeStreams: maximumChangeStreams,
			SynchronousStandbys:  synchronousStandbys(cluster.Spec.Durability),
		})
		if err != nil {
			allErrs = append(allErrs, field.Invalid(specPath.Child("postgresql", "resources"), cluster.Spec.PostgreSQL.Resources, err.Error()))
		} else {
			settingsForOverrides = result.Settings
		}
	}
	if err := tuning.ApplyOverrides(settingsForOverrides, cluster.Spec.PostgreSQL.Parameters); err != nil {
		allErrs = append(allErrs, field.Invalid(specPath.Child("postgresql", "parameters"), cluster.Spec.PostgreSQL.Parameters, err.Error()))
	} else if _, tuningAvailable := settingsForOverrides["max_wal_size"]; tuningAvailable {
		if err := tuning.ValidateStorage(settingsForOverrides, cluster.Spec.Storage.Size); err != nil {
			allErrs = append(allErrs, field.Invalid(specPath.Child("postgresql", "parameters"), cluster.Spec.PostgreSQL.Parameters, err.Error()))
		}
	}

	allErrs = append(allErrs, validateDatabases(cluster.Spec.Databases, specPath.Child("databases"))...)
	allErrs = append(allErrs, validateServices(cluster.Spec.Services, specPath.Child("services"))...)
	allErrs = append(allErrs, validateBackup(cluster.Spec.Backup, specPath.Child("backup"))...)
	if cluster.Spec.Observability.ServiceMonitor && cluster.Spec.Observability.Prometheus != nil && !*cluster.Spec.Observability.Prometheus {
		allErrs = append(allErrs, field.Invalid(specPath.Child("observability", "serviceMonitor"), true, "requires Prometheus metrics to be enabled"))
	}
	if endpoint := cluster.Spec.Observability.OpenTelemetryEndpoint; endpoint != "" {
		if err := ValidateOpenTelemetryEndpoint(endpoint); err != nil {
			allErrs = append(allErrs, field.Invalid(specPath.Child("observability", "openTelemetryEndpoint"), endpoint, err.Error()))
		}
	}
	return allErrs
}

// ValidateOpenTelemetryEndpoint rejects endpoints that cannot be passed safely
// to the current runtime configuration or that could conceal credentials.
func ValidateOpenTelemetryEndpoint(value string) error {
	return ValidateCredentialFreeHTTPSEndpoint(value)
}

// ValidateCredentialFreeHTTPSEndpoint accepts only a concrete HTTP(S) origin
// or path and rejects URL components commonly abused to embed credentials.
func ValidateCredentialFreeHTTPSEndpoint(value string) error {
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("must not contain surrounding whitespace")
	}
	endpoint, err := url.Parse(value)
	if err != nil {
		return fmt.Errorf("must be a valid URL: %w", err)
	}
	if (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" {
		return fmt.Errorf("must be an HTTP(S) URL with a host")
	}
	if endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return fmt.Errorf("must not contain user information, a query string, or a fragment")
	}
	return nil
}

// ValidateObjectReferenceName applies the Kubernetes name grammar shared by
// namespaced Secrets and PersistentVolumeClaims.
func ValidateObjectReferenceName(value string) error {
	if messages := validation.IsDNS1123Subdomain(value); len(messages) != 0 {
		return fmt.Errorf("must be a valid Kubernetes object name: %s", messages[0])
	}
	return nil
}

func validateScaling(scaling PoolerScaling, path *field.Path) (int32, field.ErrorList) {
	switch scaling.Mode {
	case ScalingHPA:
		if scaling.HPA == nil {
			return 0, field.ErrorList{field.Required(path.Child("hpa"), "required when mode is HPA")}
		}
		var errs field.ErrorList
		if scaling.Fixed != nil {
			errs = append(errs, field.Forbidden(path.Child("fixed"), "must be absent when mode is HPA"))
		}
		if scaling.HPA.MinReplicas < 2 {
			errs = append(errs, field.Invalid(path.Child("hpa", "minReplicas"), scaling.HPA.MinReplicas, "must be at least 2"))
		}
		if scaling.HPA.MaxReplicas < scaling.HPA.MinReplicas {
			errs = append(errs, field.Invalid(path.Child("hpa", "maxReplicas"), scaling.HPA.MaxReplicas, "must be at least minReplicas"))
		}
		if scaling.HPA.MaxReplicas > 100 {
			errs = append(errs, field.Invalid(path.Child("hpa", "maxReplicas"), scaling.HPA.MaxReplicas, "must not exceed 100"))
		}
		if scaling.HPA.TargetCPUUtilizationPercentage < 1 || scaling.HPA.TargetCPUUtilizationPercentage > 100 {
			errs = append(errs, field.Invalid(path.Child("hpa", "targetCPUUtilizationPercentage"), scaling.HPA.TargetCPUUtilizationPercentage, "must be between 1 and 100"))
		}
		return scaling.HPA.MaxReplicas, errs
	case ScalingFixed:
		if scaling.Fixed == nil {
			return 0, field.ErrorList{field.Required(path.Child("fixed"), "required when mode is Fixed")}
		}
		var errs field.ErrorList
		if scaling.HPA != nil {
			errs = append(errs, field.Forbidden(path.Child("hpa"), "must be absent when mode is Fixed"))
		}
		if scaling.Fixed.Replicas < 1 {
			errs = append(errs, field.Invalid(path.Child("fixed", "replicas"), scaling.Fixed.Replicas, "must be at least 1"))
		}
		if scaling.Fixed.Replicas > 100 {
			errs = append(errs, field.Invalid(path.Child("fixed", "replicas"), scaling.Fixed.Replicas, "must not exceed 100"))
		}
		return scaling.Fixed.Replicas, errs
	default:
		return 0, field.ErrorList{field.NotSupported(path.Child("mode"), scaling.Mode, []string{string(ScalingHPA), string(ScalingFixed)})}
	}
}

func validateDatabases(databases []DatabaseTemplate, path *field.Path) field.ErrorList {
	seen := make(map[string]struct{}, len(databases))
	var errs field.ErrorList
	for i, database := range databases {
		itemPath := path.Index(i).Child("name")
		if messages := validation.IsDNS1123Label(database.Name); len(messages) != 0 {
			errs = append(errs, field.Invalid(itemPath, database.Name, messages[0]))
		}
		if _, exists := seen[database.Name]; exists {
			errs = append(errs, field.Duplicate(itemPath, database.Name))
		}
		seen[database.Name] = struct{}{}
	}
	return errs
}

func validateServices(services ServiceSet, path *field.Path) field.ErrorList {
	var errs field.ErrorList
	ordered := []struct {
		name    string
		service ServiceTemplate
	}{
		{name: "rw", service: services.ReadWrite},
		{name: "ro", service: services.ReadOnly},
		{name: "r", service: services.Read},
	}
	for _, item := range ordered {
		name, service := item.name, item.service
		errs = append(errs, apivalidation.ValidateAnnotations(service.Annotations, path.Child(name, "annotations"))...)
		switch service.Type {
		case corev1.ServiceTypeClusterIP, corev1.ServiceTypeNodePort, corev1.ServiceTypeLoadBalancer:
		default:
			errs = append(errs, field.NotSupported(path.Child(name, "type"), service.Type, []string{"ClusterIP", "NodePort", "LoadBalancer"}))
		}
	}
	return errs
}

func validateBackup(backup BackupSpec, path *field.Path) field.ErrorList {
	repository := backup.Repository
	switch repository.Type {
	case RepositoryS3:
		if repository.S3 == nil {
			return field.ErrorList{field.Required(path.Child("repository", "s3"), "required for an S3 repository")}
		}
		var errs field.ErrorList
		if repository.Filesystem != nil {
			errs = append(errs, field.Forbidden(path.Child("repository", "filesystem"), "must be absent for an S3 repository"))
		}
		if repository.S3.Bucket == "" {
			errs = append(errs, field.Required(path.Child("repository", "s3", "bucket"), "must not be empty"))
		}
		if repository.S3.CredentialsSecretRef.Name == "" {
			errs = append(errs, field.Required(path.Child("repository", "s3", "credentialsSecretRef", "name"), "must not be empty"))
		} else if err := ValidateObjectReferenceName(repository.S3.CredentialsSecretRef.Name); err != nil {
			errs = append(errs, field.Invalid(path.Child("repository", "s3", "credentialsSecretRef", "name"), repository.S3.CredentialsSecretRef.Name, err.Error()))
		}
		if repository.S3.Endpoint != "" {
			if err := ValidateCredentialFreeHTTPSEndpoint(repository.S3.Endpoint); err != nil {
				errs = append(errs, field.Invalid(path.Child("repository", "s3", "endpoint"), repository.S3.Endpoint, err.Error()))
			}
		}
		return errs
	case RepositoryFilesystem:
		if repository.Filesystem == nil {
			return field.ErrorList{field.Required(path.Child("repository", "filesystem"), "required for a filesystem repository")}
		}
		var errs field.ErrorList
		if repository.S3 != nil {
			errs = append(errs, field.Forbidden(path.Child("repository", "s3"), "must be absent for a filesystem repository"))
		}
		if repository.Filesystem.PersistentVolumeClaimName == "" {
			errs = append(errs, field.Required(path.Child("repository", "filesystem", "persistentVolumeClaimName"), "must not be empty"))
		} else if err := ValidateObjectReferenceName(repository.Filesystem.PersistentVolumeClaimName); err != nil {
			errs = append(errs, field.Invalid(path.Child("repository", "filesystem", "persistentVolumeClaimName"), repository.Filesystem.PersistentVolumeClaimName, err.Error()))
		}
		return errs
	default:
		return field.ErrorList{field.NotSupported(path.Child("repository", "type"), repository.Type, []string{string(RepositoryS3), string(RepositoryFilesystem)})}
	}
}

// ResolvedPostgreSQLStandby is one deterministic direct-standby role profile.
// PrimaryConninfo is intentionally absent because it will be written only by
// authenticated orchestration after the upstream identity and TLS material are
// known. Activation also requires collision-free reconciliation of managed
// slots retained from any earlier primary role.
// +kubebuilder:object:generate=false
type ResolvedPostgreSQLStandby struct {
	Ordinal          int32
	ApplicationName  string
	PhysicalSlotName string
	Settings         map[string]string
}

// ResolvedPostgreSQLPrimary is the role profile for one possible primary
// member. Its candidate list excludes that member's own ordinal.
// +kubebuilder:object:generate=false
type ResolvedPostgreSQLPrimary struct {
	Ordinal  int32
	Settings map[string]string
}

// ResolvedPostgreSQLConfiguration is the resource-derived PostgreSQL 18
// configuration plan. Common settings are safe on every member; exactly one
// role profile is activated by future bootstrap/orchestration code.
// +kubebuilder:object:generate=false
type ResolvedPostgreSQLConfiguration struct {
	Common                  map[string]string
	Primaries               []ResolvedPostgreSQLPrimary
	Standbys                []ResolvedPostgreSQLStandby
	ManagedLogicalConsumers int32
	PrimarySlotDemand       int32
	StandbySlotDemand       int32
	PromotionSlotDemand     int32
}

// ResolvedPostgreSQLSettings retains the original common-settings API for
// callers that do not need role-specific replication configuration.
func (cluster *PgShardCluster) ResolvedPostgreSQLSettings() (map[string]string, error) {
	configuration, err := cluster.ResolvedPostgreSQLConfiguration()
	if err != nil {
		return nil, err
	}
	return configuration.Common, nil
}

// ResolvedPostgreSQLConfiguration derives common, primary, and per-standby
// settings from the same validated resource budget.
func (cluster *PgShardCluster) ResolvedPostgreSQLConfiguration() (ResolvedPostgreSQLConfiguration, error) {
	poolerMax, errs := validateScaling(cluster.Spec.Pooler.Scaling, field.NewPath("spec", "pooler", "scaling"))
	if len(errs) != 0 {
		return ResolvedPostgreSQLConfiguration{}, fmt.Errorf("invalid pooler scaling: %s", errs.ToAggregate())
	}
	result, err := tuning.Calculate(tuning.Input{
		Resources:            cluster.Spec.PostgreSQL.Resources,
		PoolerMaxReplicas:    poolerMax,
		MembersPerShard:      cluster.Spec.MembersPerShard,
		MaximumChangeStreams: maximumChangeStreams,
		SynchronousStandbys:  synchronousStandbys(cluster.Spec.Durability),
	})
	if err != nil {
		return ResolvedPostgreSQLConfiguration{}, err
	}
	if err := tuning.ApplyOverrides(result.Settings, cluster.Spec.PostgreSQL.Parameters); err != nil {
		return ResolvedPostgreSQLConfiguration{}, err
	}
	if err := tuning.ValidateStorage(result.Settings, cluster.Spec.Storage.Size); err != nil {
		return ResolvedPostgreSQLConfiguration{}, err
	}
	primaries := make([]ResolvedPostgreSQLPrimary, 0, len(result.Primaries))
	for _, primary := range result.Primaries {
		primaries = append(primaries, ResolvedPostgreSQLPrimary{
			Ordinal:  primary.Ordinal,
			Settings: primary.Settings,
		})
	}
	standbys := make([]ResolvedPostgreSQLStandby, 0, len(result.Standbys))
	for _, standby := range result.Standbys {
		standbys = append(standbys, ResolvedPostgreSQLStandby{
			Ordinal:          standby.Ordinal,
			ApplicationName:  standby.ApplicationName,
			PhysicalSlotName: standby.PhysicalSlotName,
			Settings:         standby.Settings,
		})
	}
	return ResolvedPostgreSQLConfiguration{
		Common:                  result.Settings,
		Primaries:               primaries,
		Standbys:                standbys,
		ManagedLogicalConsumers: result.ManagedLogicalConsumers,
		PrimarySlotDemand:       result.PrimarySlotDemand,
		StandbySlotDemand:       result.StandbySlotDemand,
		PromotionSlotDemand:     result.PromotionSlotDemand,
	}, nil
}

func synchronousStandbys(durability DurabilityMode) int32 {
	if durability == DurabilitySynchronous {
		return 1
	}
	return 0
}

// +kubebuilder:webhook:path=/mutate-pgshard-io-v1alpha1-pgshardcluster,mutating=true,failurePolicy=fail,matchPolicy=Equivalent,sideEffects=None,timeoutSeconds=5,groups=pgshard.io,resources=pgshardclusters,verbs=create;update,versions=v1alpha1,name=mpgshardcluster.kb.io,admissionReviewVersions=v1
// +kubebuilder:webhook:path=/validate-pgshard-io-v1alpha1-pgshardcluster,mutating=false,failurePolicy=fail,matchPolicy=Equivalent,sideEffects=None,timeoutSeconds=5,groups=pgshard.io,resources=pgshardclusters,verbs=create;update,versions=v1alpha1,name=vpgshardcluster.kb.io,admissionReviewVersions=v1
