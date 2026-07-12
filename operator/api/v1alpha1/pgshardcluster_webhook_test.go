package v1alpha1

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func validCluster() *PgShardCluster {
	return &PgShardCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "valid"},
		Spec: PgShardClusterSpec{
			Shards:          2,
			MembersPerShard: 3,
			Durability:      DurabilitySynchronous,
			PostgreSQL: PostgreSQLSpec{
				Version: PostgreSQLMajor18,
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("2Gi")},
					Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")},
				},
			},
			Storage: StorageSpec{Size: resource.MustParse("10Gi")},
			Pooler:  PoolerSpec{Scaling: PoolerScaling{Mode: ScalingHPA, HPA: &HPAScaling{MinReplicas: 2, MaxReplicas: 10, TargetCPUUtilizationPercentage: 65}}},
			Services: ServiceSet{
				ReadWrite: ServiceTemplate{Type: corev1.ServiceTypeClusterIP},
				ReadOnly:  ServiceTemplate{Type: corev1.ServiceTypeClusterIP},
				Read:      ServiceTemplate{Type: corev1.ServiceTypeClusterIP},
			},
			Backup: BackupSpec{Repository: BackupRepository{Type: RepositoryFilesystem, Filesystem: &FilesystemRepository{PersistentVolumeClaimName: "backups"}}},
		},
	}
}

func TestDefaultsAreSafetyOriented(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	cluster.Spec.Shards = 0
	cluster.Spec.MembersPerShard = 0
	cluster.Spec.Durability = ""
	cluster.Spec.PostgreSQL.Version = ""
	cluster.Spec.Pooler.Scaling = PoolerScaling{}
	cluster.Spec.Services = ServiceSet{}
	cluster.Spec.Observability = ObservabilitySpec{}
	if err := (&PgShardClusterDefaulter{}).Default(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Spec.Shards != 1 || cluster.Spec.MembersPerShard != 3 || cluster.Spec.Durability != DurabilitySynchronous || cluster.Spec.PostgreSQL.Version != "18" {
		t.Fatalf("unexpected defaults: %#v", cluster.Spec)
	}
	if cluster.Spec.Pooler.Scaling.HPA == nil || cluster.Spec.Pooler.Scaling.HPA.MaxReplicas != 10 || cluster.Spec.Pooler.Scaling.HPA.TargetCPUUtilizationPercentage != 65 {
		t.Fatalf("unexpected HPA defaults: %#v", cluster.Spec.Pooler.Scaling)
	}
	if cluster.Spec.Observability.Prometheus == nil || !*cluster.Spec.Observability.Prometheus {
		t.Fatal("Prometheus must default on")
	}
	disabled := false
	cluster.Spec.Observability.Prometheus = &disabled
	if err := (&PgShardClusterDefaulter{}).Default(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if *cluster.Spec.Observability.Prometheus {
		t.Fatal("explicitly disabled Prometheus was overwritten")
	}
}

func TestValidationAcceptsSafeClusterAndResolvesTuning(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	if _, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	settings, err := cluster.ResolvedPostgreSQLSettings()
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"wal_level": "logical", "fsync": "on", "full_page_writes": "on", "synchronous_commit": "on"} {
		if settings[key] != want {
			t.Errorf("%s = %q, want %q", key, settings[key], want)
		}
	}
}

func TestValidationRejectsPostgreSQL17AndUnsafeOverride(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	cluster.Spec.PostgreSQL.Version = "17"
	cluster.Spec.PostgreSQL.Parameters = map[string]string{"fsync": "off"}
	_, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster)
	if err == nil {
		t.Fatal("expected validation error")
	}
	message := err.Error()
	if !strings.Contains(message, "supported values: \"18\"") || !strings.Contains(message, "fsync") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidationRequiresExplicitScalingUnionAndBackupUnion(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	cluster.Spec.Pooler.Scaling.Fixed = &FixedScaling{Replicas: 2}
	cluster.Spec.Backup.Repository.S3 = &S3Repository{Bucket: "also-set"}
	_, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "must be absent") {
		t.Fatalf("expected union validation failure, got %v", err)
	}
}

func TestAsynchronousModeWarnsWithoutDisablingLocalDurability(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	cluster.Spec.Durability = DurabilityAsynchronous
	warnings, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "lose acknowledged") {
		t.Fatalf("warnings = %#v", warnings)
	}
	settings, err := cluster.ResolvedPostgreSQLSettings()
	if err != nil {
		t.Fatal(err)
	}
	if settings["synchronous_commit"] != "on" || settings["fsync"] != "on" {
		t.Fatalf("local durability was weakened: %#v", settings)
	}
}

func TestValidationRejectsNamesAndShardCountsThatCannotBePlanned(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	cluster.Name = strings.Repeat("a", MaximumClusterNameLength+1)
	cluster.Spec.Shards = MaximumShards + 1
	_, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "Too long") || !strings.Contains(err.Error(), "must not exceed 128") {
		t.Fatalf("expected planning-bound validation errors, got %v", err)
	}
}

func TestValidationRejectsUnsafeOpenTelemetryEndpoints(t *testing.T) {
	t.Parallel()
	for _, endpoint := range []string{
		"file:///tmp/collector",
		"https://user:password@collector.example.com:4317",
		"https://collector.example.com:4317?token=secret",
		" collector.example.com:4317",
	} {
		cluster := validCluster()
		cluster.Spec.Observability.OpenTelemetryEndpoint = endpoint
		_, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster)
		if err == nil || !strings.Contains(err.Error(), "openTelemetryEndpoint") {
			t.Errorf("endpoint %q: expected validation error, got %v", endpoint, err)
		}
	}
	cluster := validCluster()
	cluster.Spec.Observability.OpenTelemetryEndpoint = "https://collector.example.com:4317/v1/traces"
	if _, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster); err != nil {
		t.Fatalf("safe endpoint rejected: %v", err)
	}
}

func TestValidationAllowsFinalizerRemovalFromDeletingLegacyObject(t *testing.T) {
	t.Parallel()
	oldCluster := validCluster()
	newCluster := oldCluster.DeepCopy()
	newCluster.Spec.Shards = MaximumShards + 1
	newCluster.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	if _, err := (&PgShardClusterValidator{}).ValidateUpdate(context.Background(), oldCluster, newCluster); err != nil {
		t.Fatalf("deleting legacy object cannot remove its finalizer: %v", err)
	}
}

func TestValidationChecksOverridesAgainstDerivedSettings(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	cluster.Spec.PostgreSQL.Parameters = map[string]string{"autovacuum_max_workers": "20"}
	_, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "autovacuum_max_workers") {
		t.Fatalf("override exceeding max_worker_processes was admitted: %v", err)
	}
}

func TestValidationRejectsUnsafeBackupReferencesAndEndpoints(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	cluster.Spec.Backup.Repository = BackupRepository{Type: RepositoryS3, S3: &S3Repository{
		Bucket: "backups", Endpoint: "https://user:password@minio.example.com?token=secret",
		CredentialsSecretRef: corev1.LocalObjectReference{Name: "Bad_Secret"},
	}}
	_, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "credentialsSecretRef") || !strings.Contains(err.Error(), "endpoint") {
		t.Fatalf("unsafe S3 configuration was admitted: %v", err)
	}
	cluster = validCluster()
	cluster.Spec.Backup.Repository.Filesystem.PersistentVolumeClaimName = "Bad_PVC"
	if _, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster); err == nil || !strings.Contains(err.Error(), "persistentVolumeClaimName") {
		t.Fatalf("invalid PVC reference was admitted: %v", err)
	}
}

func TestValidationRejectsInvalidServiceAnnotations(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	cluster.Spec.Services.ReadWrite.Annotations = map[string]string{"not a key": "value"}
	_, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "annotations") {
		t.Fatalf("invalid Service annotation was admitted: %v", err)
	}
}

func TestValidationRejectsInvalidStorageClassName(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	invalid := "BAD/NAME"
	cluster.Spec.Storage.StorageClassName = &invalid
	_, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "storageClassName") {
		t.Fatalf("invalid StorageClass name was admitted: %v", err)
	}
	empty := ""
	cluster.Spec.Storage.StorageClassName = &empty
	if _, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster); err != nil {
		t.Fatalf("explicit no-storage-class value was rejected: %v", err)
	}
}

func TestValidationKeepsStorageClassImmutable(t *testing.T) {
	t.Parallel()
	oldCluster := validCluster()
	oldClass := "fast"
	oldCluster.Spec.Storage.StorageClassName = &oldClass
	newCluster := oldCluster.DeepCopy()
	newClass := "slower"
	newCluster.Spec.Storage.StorageClassName = &newClass
	if _, err := (&PgShardClusterValidator{}).ValidateUpdate(context.Background(), oldCluster, newCluster); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("storage class update was admitted: %v", err)
	}
}
