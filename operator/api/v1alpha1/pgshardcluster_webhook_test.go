package v1alpha1

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func validCluster() *PgShardCluster {
	return &PgShardCluster{
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
