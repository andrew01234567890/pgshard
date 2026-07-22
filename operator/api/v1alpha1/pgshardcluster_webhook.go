package v1alpha1

import (
	"context"
	"fmt"
	"strings"

	"github.com/andrew01234567890/pgshard/operator/internal/tuning"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	apivalidation "k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const (
	maximumChangeStreams = 4
	// PodFencingChallengeAnnotation and PodFencingReceiptAnnotation are
	// controller/admission-owned metadata for direct PostgreSQL Pod fencing.
	PodFencingChallengeAnnotation = "pgshard.io/pod-fencing-challenge"
	PodFencingReceiptAnnotation   = "pgshard.io/pod-fencing-admission"
)

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
	for index := range cluster.Spec.Databases {
		database := &cluster.Spec.Databases[index]
		if database.Shards == 0 {
			database.Shards = database.ResolvedShardCount(cluster.Spec.Shards)
		}
		if database.Cells == nil {
			database.Cells = database.ResolvedCells(cluster.Spec.Shards)
		}
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

// PodFencingReceiptVerifier authenticates the final cluster handshake after all
// mutating admission has completed.
// +kubebuilder:object:generate=false
type PodFencingReceiptVerifier interface {
	Verify(context.Context, *PgShardCluster) (bool, error)
}

// PgShardClusterValidator applies validation that cannot be represented safely
// by OpenAPI alone.
// +kubebuilder:object:generate=false
type PgShardClusterValidator struct {
	FencingReceiptVerifier    PodFencingReceiptVerifier
	FencingControllerUsername string
	// NamespaceStateReader authoritatively reads a namespace's existing clusters
	// for the isolation-exclusivity gate: creating a SECOND PgShardCluster in a
	// namespace whose cluster holds an activating or active isolation receipt is
	// denied continuously at admission, not only by the point-in-time preflight
	// LIST. Optional; nil skips the gate (unit fixtures).
	NamespaceStateReader client.Reader
}

func (v *PgShardClusterValidator) ValidateCreate(ctx context.Context, cluster *PgShardCluster) (admission.Warnings, error) {
	allErrs := validateClusterFields(cluster)
	allErrs = append(allErrs, reservedPodFencingMetadataErrors(cluster)...)
	if v.NamespaceStateReader != nil {
		list := &PgShardClusterList{}
		if err := v.NamespaceStateReader.List(ctx, list, client.InNamespace(cluster.Namespace)); err != nil {
			return warningsFor(cluster), fmt.Errorf("read namespace clusters for isolation exclusivity: %w", err)
		}
		for i := range list.Items {
			existing := &list.Items[i]
			receipt := existing.Status.IsolationReceipt
			if existing.UID != cluster.UID && receipt != nil && receipt.Phase != "" && receipt.Phase != IsolationInactive {
				allErrs = append(allErrs, field.Forbidden(field.NewPath("metadata", "namespace"),
					fmt.Sprintf("namespace %s is isolation-%s for PgShardCluster %s; a second cluster may not be created there", cluster.Namespace, receipt.Phase, existing.Name)))
				break
			}
		}
	}
	return warningsFor(cluster), invalidIfAny(cluster.Name, allErrs)
}

func (v *PgShardClusterValidator) ValidateUpdate(ctx context.Context, oldCluster, newCluster *PgShardCluster) (admission.Warnings, error) {
	metadataErrs, requiresAttestation := reservedPodFencingMetadataUpdateErrors(oldCluster, newCluster)
	if requiresAttestation {
		attestationErrs, err := v.validatePodFencingMetadataAttestation(ctx, newCluster)
		if err != nil {
			return warningsFor(newCluster), err
		}
		metadataErrs = append(metadataErrs, attestationErrs...)
	}
	if !newCluster.DeletionTimestamp.IsZero() {
		// A deleting object must always be able to shed finalizers, including if
		// it predates validation that its stored spec no longer satisfies. The
		// authenticated fencing history must still remain byte-for-byte intact.
		return warningsFor(newCluster), invalidIfAny(newCluster.Name, metadataErrs)
	}
	allErrs := validateClusterFields(newCluster)
	allErrs = append(allErrs, metadataErrs...)
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
	if !databaseTemplatesEqual(oldCluster.Spec.Databases, newCluster.Spec.Databases, oldCluster.Spec.Shards, newCluster.Spec.Shards) {
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "databases"), newCluster.Spec.Databases, "databases is immutable until database lifecycle and online resharding are implemented"))
	}
	if !equalOptionalString(oldCluster.Spec.Storage.StorageClassName, newCluster.Spec.Storage.StorageClassName) {
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "storage", "storageClassName"), newCluster.Spec.Storage.StorageClassName, "storage class is immutable after cluster creation"))
	}
	if !oldCluster.Spec.Storage.Size.Equal(newCluster.Spec.Storage.Size) && !legacyStorageUpgrade(oldCluster.Spec.Storage.Size, newCluster.Spec.Storage.Size) {
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "storage", "size"), newCluster.Spec.Storage.Size.String(), "storage size is immutable until explicit PVC expansion is implemented"))
	}
	if oldCluster.Spec.Storage.DeletionPolicy != newCluster.Spec.Storage.DeletionPolicy {
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "storage", "deletionPolicy"), newCluster.Spec.Storage.DeletionPolicy, "deletion policy is immutable after cluster creation"))
	}
	return warningsFor(newCluster), invalidIfAny(newCluster.Name, allErrs)
}

func legacyStorageUpgrade(oldSize, newSize resource.Quantity) bool {
	minimum := resource.MustParse("4Gi")
	return oldSize.Cmp(minimum) < 0 && newSize.Cmp(minimum) >= 0
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

	allErrs = append(allErrs, validateDatabases(cluster.Spec.Databases, cluster.Spec.Shards, specPath.Child("databases"))...)
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

func reservedPodFencingMetadataErrors(cluster *PgShardCluster) field.ErrorList {
	annotationsPath := field.NewPath("metadata", "annotations")
	var allErrs field.ErrorList
	for _, key := range []string{PodFencingChallengeAnnotation, PodFencingReceiptAnnotation} {
		if _, exists := cluster.Annotations[key]; exists {
			allErrs = append(allErrs, field.Forbidden(annotationsPath.Key(key), "is reserved for the pgshard controller and admission webhook"))
		}
	}
	return allErrs
}

func reservedPodFencingMetadataUpdateErrors(oldCluster, newCluster *PgShardCluster) (field.ErrorList, bool) {
	annotationsPath := field.NewPath("metadata", "annotations")
	oldChallenge, oldHasChallenge := oldCluster.Annotations[PodFencingChallengeAnnotation]
	oldReceipt, oldHasReceipt := oldCluster.Annotations[PodFencingReceiptAnnotation]
	newChallenge, newHasChallenge := newCluster.Annotations[PodFencingChallengeAnnotation]
	newReceipt, newHasReceipt := newCluster.Annotations[PodFencingReceiptAnnotation]
	unchanged := oldHasChallenge == newHasChallenge && oldHasReceipt == newHasReceipt &&
		oldChallenge == newChallenge && oldReceipt == newReceipt

	if unchanged {
		return nil, false
	}
	if !newCluster.DeletionTimestamp.IsZero() {
		return field.ErrorList{field.Forbidden(annotationsPath, "controller-owned Pod fencing metadata is immutable during deletion")}, false
	}
	if !publishesManagedPostgreSQLPods(oldCluster) {
		return reservedPodFencingMetadataErrors(newCluster), false
	}
	if !newHasChallenge || !newHasReceipt || newChallenge == "" || newReceipt == "" {
		return field.ErrorList{field.Forbidden(annotationsPath, "Pod fencing challenge and receipt must be preserved or replaced by a non-empty admission attestation")}, false
	}
	return nil, true
}

func publishesManagedPostgreSQLPods(cluster *PgShardCluster) bool {
	if cluster.Spec.MembersPerShard == 1 {
		return true
	}
	return cluster.Status.PostgreSQLBootstrapSpec != nil &&
		cluster.Status.PostgreSQLBootstrapSpec.PostgreSQLRuntime == "agent-quarantine"
}

func (v *PgShardClusterValidator) validatePodFencingMetadataAttestation(ctx context.Context, cluster *PgShardCluster) (field.ErrorList, error) {
	if v.FencingReceiptVerifier == nil || v.FencingControllerUsername == "" {
		return nil, fmt.Errorf("Pod fencing metadata attestation is not configured")
	}
	request, err := admission.RequestFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("read Pod fencing admission request identity: %w", err)
	}
	annotationsPath := field.NewPath("metadata", "annotations")
	if request.UserInfo.Username != v.FencingControllerUsername {
		return field.ErrorList{field.Forbidden(annotationsPath, "Pod fencing metadata may only be established or repaired by the pgshard controller")}, nil
	}
	verified, err := v.FencingReceiptVerifier.Verify(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("verify final Pod fencing admission receipt: %w", err)
	}
	if !verified {
		return field.ErrorList{field.Forbidden(annotationsPath, "Pod fencing metadata does not carry a valid final admission receipt")}, nil
	}
	return nil, nil
}

// ValidateOpenTelemetryEndpoint rejects endpoints that cannot be passed safely
// to the current runtime configuration or that could conceal credentials.
func ValidateOpenTelemetryEndpoint(value string) error {
	return ValidateCredentialFreeHTTPSEndpoint(value)
}

// ValidateCredentialFreeHTTPSEndpoint accepts one deliberately narrow,
// portable HTTP(S) origin/path grammar shared with the Rust topology reader.
func ValidateCredentialFreeHTTPSEndpoint(value string) error {
	if value == "" {
		return fmt.Errorf("must not be empty")
	}
	if len(value) > MaximumEndpointLength {
		return fmt.Errorf("must not exceed %d bytes", MaximumEndpointLength)
	}
	for index := 0; index < len(value); index++ {
		if value[index] <= 0x20 || value[index] >= 0x7f || value[index] == '\\' {
			return fmt.Errorf("must contain only portable visible ASCII endpoint characters")
		}
	}
	if strings.ContainsAny(value, "@?#") {
		return fmt.Errorf("must not contain user information, a query delimiter, or a fragment delimiter")
	}
	remainder := ""
	switch {
	case strings.HasPrefix(value, "http://"):
		remainder = strings.TrimPrefix(value, "http://")
	case strings.HasPrefix(value, "https://"):
		remainder = strings.TrimPrefix(value, "https://")
	default:
		return fmt.Errorf("must use the lowercase http or https scheme")
	}
	authority, path := remainder, ""
	if pathStart := strings.IndexByte(remainder, '/'); pathStart >= 0 {
		authority, path = remainder[:pathStart], remainder[pathStart:]
	}
	if !validPortableHTTPAuthority(authority) {
		return fmt.Errorf("must contain a lowercase DNS or canonical IPv4 host and optional port 1 through 65535")
	}
	if !validPortableHTTPPath(path) {
		return fmt.Errorf("path must contain only nonempty unreserved segments and valid unreserved percent escapes")
	}
	return nil
}

func validPortableHTTPAuthority(authority string) bool {
	if authority == "" || strings.ContainsAny(authority, "[]") || strings.Count(authority, ":") > 1 {
		return false
	}
	host := authority
	if separator := strings.LastIndexByte(authority, ':'); separator >= 0 {
		host = authority[:separator]
		if !validCanonicalDecimal(authority[separator+1:], 65535, false) {
			return false
		}
	}
	if host == "" || len(host) > 253 {
		return false
	}
	numeric := true
	for index := 0; index < len(host); index++ {
		if (host[index] < '0' || host[index] > '9') && host[index] != '.' {
			numeric = false
			break
		}
	}
	if numeric {
		parts := strings.Split(host, ".")
		if len(parts) != 4 {
			return false
		}
		for _, part := range parts {
			if !validCanonicalDecimal(part, 255, true) {
				return false
			}
		}
		return true
	}
	labels := strings.Split(host, ".")
	if whatwgIPv4NumberSpelling(labels[len(labels)-1]) {
		return false
	}
	for _, label := range labels {
		if !validPortableDNSLabel(label) {
			return false
		}
	}
	return true
}

func whatwgIPv4NumberSpelling(label string) bool {
	digits := label
	base := byte(10)
	if strings.HasPrefix(label, "0x") {
		digits = label[2:]
		base = 16
	}
	for index := 0; index < len(digits); index++ {
		if digits[index] >= '0' && digits[index] <= '9' {
			continue
		}
		if base == 16 && digits[index] >= 'a' && digits[index] <= 'f' {
			continue
		}
		return false
	}
	return true
}

func validPortableDNSLabel(label string) bool {
	reservedIDNAPrefix := len(label) >= 4 && strings.EqualFold(label[:4], "xn--")
	if label == "" || len(label) > 63 || reservedIDNAPrefix || label[0] == '-' || label[len(label)-1] == '-' {
		return false
	}
	for index := 0; index < len(label); index++ {
		if (label[index] < 'a' || label[index] > 'z') && (label[index] < '0' || label[index] > '9') && label[index] != '-' {
			return false
		}
	}
	return true
}

func validCanonicalDecimal(value string, maximum uint32, allowZero bool) bool {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return false
	}
	var parsed uint32
	for index := 0; index < len(value); index++ {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
		parsed = parsed*10 + uint32(value[index]-'0')
		if parsed > maximum {
			return false
		}
	}
	return allowZero || parsed > 0
}

func validPortableHTTPPath(path string) bool {
	if path == "" || path == "/" {
		return true
	}
	if path[0] != '/' || path[len(path)-1] == '/' {
		return false
	}
	for _, segment := range strings.Split(path[1:], "/") {
		if segment == "" {
			return false
		}
		decoded := make([]byte, 0, len(segment))
		for index := 0; index < len(segment); {
			if segment[index] != '%' {
				if !portableHTTPUnreserved(segment[index]) {
					return false
				}
				decoded = append(decoded, segment[index])
				index++
				continue
			}
			if index+2 >= len(segment) {
				return false
			}
			high, highOK := hexadecimalNibble(segment[index+1])
			low, lowOK := hexadecimalNibble(segment[index+2])
			if !highOK || !lowOK {
				return false
			}
			decodedByte := high<<4 | low
			if !portableHTTPUnreserved(decodedByte) {
				return false
			}
			decoded = append(decoded, decodedByte)
			index += 3
		}
		if string(decoded) == "." || string(decoded) == ".." {
			return false
		}
	}
	return true
}

func portableHTTPUnreserved(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' || strings.ContainsRune("-._~", rune(value))
}

func hexadecimalNibble(value byte) (byte, bool) {
	switch {
	case value >= '0' && value <= '9':
		return value - '0', true
	case value >= 'a' && value <= 'f':
		return value - 'a' + 10, true
	case value >= 'A' && value <= 'F':
		return value - 'A' + 10, true
	default:
		return 0, false
	}
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

func validateDatabases(databases []DatabaseTemplate, clusterCells int32, path *field.Path) field.ErrorList {
	var errs field.ErrorList
	if len(databases) > MaximumDatabases {
		errs = append(errs, field.TooMany(path, len(databases), MaximumDatabases))
	}
	seen := make(map[string]struct{}, len(databases))
	var totalRoutingRanges int64
	for i, database := range databases {
		itemPath := path.Index(i)
		namePath := itemPath.Child("name")
		if messages := validation.IsDNS1123Label(database.Name); len(messages) != 0 {
			errs = append(errs, field.Invalid(namePath, database.Name, messages[0]))
		}
		switch database.Name {
		case "postgres", "shardschema", "template0", "template1":
			errs = append(errs, field.Invalid(namePath, database.Name, "is reserved by PostgreSQL or pgshard"))
		}
		if _, exists := seen[database.Name]; exists {
			errs = append(errs, field.Duplicate(namePath, database.Name))
		}
		seen[database.Name] = struct{}{}

		shards := database.ResolvedShardCount(clusterCells)
		cells := database.ResolvedCells(clusterCells)
		if shards > 0 {
			totalRoutingRanges += int64(shards)
		}
		if shards < 1 || shards > MaximumShards {
			errs = append(errs, field.Invalid(itemPath.Child("shards"), database.Shards, fmt.Sprintf("must resolve to between 1 and %d logical shards", MaximumShards)))
		}
		if shards > clusterCells {
			errs = append(errs, field.Invalid(itemPath.Child("shards"), shards, "cannot exceed the cluster's physical cell count"))
		}
		if len(cells) != int(shards) {
			errs = append(errs, field.Invalid(itemPath.Child("cells"), database.Cells, "must contain exactly one physical cell for every logical shard"))
		}
		seenCells := make(map[int32]struct{}, len(cells))
		for ordinal, cell := range cells {
			cellPath := itemPath.Child("cells").Index(ordinal)
			if cell < 0 || cell >= clusterCells {
				errs = append(errs, field.Invalid(cellPath, cell, fmt.Sprintf("must reference a physical cell in [0,%d)", clusterCells)))
			}
			if _, duplicate := seenCells[cell]; duplicate {
				errs = append(errs, field.Duplicate(cellPath, cell))
			}
			seenCells[cell] = struct{}{}
		}
	}
	if totalRoutingRanges > MaximumTotalRoutingRanges {
		errs = append(errs, field.TooMany(path, int(totalRoutingRanges), MaximumTotalRoutingRanges))
	}
	return errs
}

func databaseTemplatesEqual(left, right []DatabaseTemplate, leftCells, rightCells int32) bool {
	if len(left) != len(right) {
		return false
	}
	leftByName := make(map[string]DatabaseTemplate, len(left))
	for _, database := range left {
		leftByName[database.Name] = database
	}
	for _, database := range right {
		previous, exists := leftByName[database.Name]
		if !exists || previous.ResolvedShardCount(leftCells) != database.ResolvedShardCount(rightCells) {
			return false
		}
		previousPlacement := previous.ResolvedCells(leftCells)
		placement := database.ResolvedCells(rightCells)
		if len(previousPlacement) != len(placement) {
			return false
		}
		for ordinal := range placement {
			if previousPlacement[ordinal] != placement[ordinal] {
				return false
			}
		}
	}
	return true
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
		s3Path := path.Child("repository", "s3")
		if repository.Filesystem != nil {
			errs = append(errs, field.Forbidden(path.Child("repository", "filesystem"), "must be absent for an S3 repository"))
		}
		if repository.S3.Bucket == "" {
			errs = append(errs, field.Required(s3Path.Child("bucket"), "must not be empty"))
		} else if len(repository.S3.Bucket) > MaximumS3BucketLength {
			errs = append(errs, field.TooLong(s3Path.Child("bucket"), repository.S3.Bucket, MaximumS3BucketLength))
		}
		if len(repository.S3.Region) > MaximumS3RegionLength {
			errs = append(errs, field.TooLong(s3Path.Child("region"), repository.S3.Region, MaximumS3RegionLength))
		}
		if len(repository.S3.Prefix) > MaximumS3PrefixLength {
			errs = append(errs, field.TooLong(s3Path.Child("prefix"), repository.S3.Prefix, MaximumS3PrefixLength))
		}
		if repository.S3.CredentialsSecretRef.Name == "" {
			errs = append(errs, field.Required(s3Path.Child("credentialsSecretRef", "name"), "must not be empty"))
		} else if err := ValidateObjectReferenceName(repository.S3.CredentialsSecretRef.Name); err != nil {
			errs = append(errs, field.Invalid(s3Path.Child("credentialsSecretRef", "name"), repository.S3.CredentialsSecretRef.Name, err.Error()))
		}
		if repository.S3.Endpoint != "" {
			if err := ValidateCredentialFreeHTTPSEndpoint(repository.S3.Endpoint); err != nil {
				errs = append(errs, field.Invalid(s3Path.Child("endpoint"), repository.S3.Endpoint, err.Error()))
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

// +kubebuilder:webhook:path=/mutate-pgshard-io-v1alpha1-pgshardcluster,mutating=true,failurePolicy=fail,matchPolicy=Equivalent,sideEffects=None,timeoutSeconds=5,groups=pgshard.io,resources=pgshardclusters,verbs=create;update,versions=v1alpha1,name=mpgshardcluster.kb.io,admissionReviewVersions=v1,servicePort=9444
// +kubebuilder:webhook:path=/validate-pgshard-io-v1alpha1-pgshardcluster,mutating=false,failurePolicy=fail,matchPolicy=Equivalent,sideEffects=None,timeoutSeconds=5,groups=pgshard.io,resources=pgshardclusters,verbs=create;update,versions=v1alpha1,name=vpgshardcluster.kb.io,admissionReviewVersions=v1,servicePort=9444
