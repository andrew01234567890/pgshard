package v1alpha1

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const testFencingControllerUsername = "system:serviceaccount:pgshard-system:pgshard-controller-manager"

type fixedPodFencingReceiptVerifier struct {
	verified bool
	err      error
}

func (verifier fixedPodFencingReceiptVerifier) Verify(context.Context, *PgShardCluster) (bool, error) {
	return verifier.verified, verifier.err
}

func podFencingAdmissionContext(username string) context.Context {
	return admission.NewContextWithRequest(context.Background(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		UserInfo: authenticationv1.UserInfo{Username: username},
	}})
}

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
			Storage: StorageSpec{Size: resource.MustParse("10Gi"), DeletionPolicy: DeletionRetain},
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

func TestDatabaseTopologySHA256IsCanonicalAndPlacementSensitive(t *testing.T) {
	left := PgShardClusterSpec{
		Shards: 3,
		Databases: []DatabaseTemplate{
			{Name: "app", Shards: 2},
			{Name: "analytics", Cells: []int32{2}},
		},
	}
	right := PgShardClusterSpec{
		Shards: 3,
		Databases: []DatabaseTemplate{
			{Name: "analytics", Shards: 1, Cells: []int32{2}},
			{Name: "app", Shards: 2, Cells: []int32{0, 1}},
		},
	}
	if left.DatabaseTopologySHA256() != right.DatabaseTopologySHA256() {
		t.Fatal("equivalent resolved database topologies produced different digests")
	}
	if len(left.DatabaseTopologySHA256()) != sha256.Size*2 {
		t.Fatalf("database topology digest = %q", left.DatabaseTopologySHA256())
	}
	right.Databases[1].Cells = []int32{1, 0}
	if left.DatabaseTopologySHA256() == right.DatabaseTopologySHA256() {
		t.Fatal("ordered database placement change retained topology digest")
	}
}

func TestDefaultsAreSafetyOriented(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	cluster.Spec.Shards = 0
	cluster.Spec.MembersPerShard = 0
	cluster.Spec.Durability = ""
	cluster.Spec.PostgreSQL.Version = ""
	cluster.Spec.Storage.DeletionPolicy = ""
	cluster.Spec.Pooler.Scaling = PoolerScaling{}
	cluster.Spec.Services = ServiceSet{}
	cluster.Spec.Observability = ObservabilitySpec{}
	if err := (&PgShardClusterDefaulter{}).Default(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Spec.Shards != 1 || cluster.Spec.MembersPerShard != 3 || cluster.Spec.Durability != DurabilitySynchronous || cluster.Spec.PostgreSQL.Version != "18" || cluster.Spec.Storage.DeletionPolicy != DeletionRetain {
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

func TestDatabaseTopologyDefaultsAreExplicitAndDeterministic(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	cluster.Spec.Shards = 8
	cluster.Spec.Databases = []DatabaseTemplate{
		{Name: "all-cells"},
		{Name: "first-three", Shards: 3},
		{Name: "dedicated", Cells: []int32{5, 6, 7}},
	}
	if err := (&PgShardClusterDefaulter{}).Default(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	want := []DatabaseTemplate{
		{Name: "all-cells", Shards: 8, Cells: []int32{0, 1, 2, 3, 4, 5, 6, 7}},
		{Name: "first-three", Shards: 3, Cells: []int32{0, 1, 2}},
		{Name: "dedicated", Shards: 3, Cells: []int32{5, 6, 7}},
	}
	if !databaseTemplatesEqual(cluster.Spec.Databases, want, 8, 8) {
		t.Fatalf("database topology defaults = %#v, want %#v", cluster.Spec.Databases, want)
	}
	if _, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster); err != nil {
		t.Fatalf("defaulted topology was rejected: %v", err)
	}
}

func TestDatabaseTopologyDefaultingPreservesExplicitEmptyCellsForValidation(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	cluster.Spec.Databases = []DatabaseTemplate{{
		Name: "app", Shards: 1, Cells: []int32{},
	}}
	if err := (&PgShardClusterDefaulter{}).Default(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Spec.Databases[0].Cells == nil || len(cluster.Spec.Databases[0].Cells) != 0 {
		t.Fatalf("explicit empty cells were defaulted: %#v", cluster.Spec.Databases[0].Cells)
	}
	if _, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster); err == nil || !strings.Contains(err.Error(), "cells") {
		t.Fatalf("explicit empty cells were admitted: %v", err)
	}
}

func TestDatabaseTopologyValidationRejectsReservedNames(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"postgres", "shardschema", "template0", "template1"} {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cluster := validCluster()
			cluster.Spec.Databases = []DatabaseTemplate{{Name: name, Shards: 1, Cells: []int32{0}}}
			if _, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster); err == nil || !strings.Contains(err.Error(), "reserved") {
				t.Fatalf("reserved database name was admitted: %v", err)
			}
		})
	}
}

func TestDatabaseTopologyValidationBoundsTotalRoutingRanges(t *testing.T) {
	t.Parallel()
	if got, want := MaximumTotalRoutingRanges, MaximumDatabases*MaximumShards; got != want {
		t.Fatalf("maximum routing ranges = %d, want structural product %d", got, want)
	}
	cluster := validCluster()
	cluster.Spec.Shards = MaximumShards
	cluster.Spec.Databases = make([]DatabaseTemplate, MaximumTotalRoutingRanges/MaximumShards)
	for index := range cluster.Spec.Databases {
		cluster.Spec.Databases[index] = DatabaseTemplate{
			Name:   fmt.Sprintf("db-%04d", index),
			Shards: MaximumShards,
		}
	}
	if _, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster); err != nil {
		t.Fatalf("maximum total routing range count was rejected: %v", err)
	}

	cluster.Spec.Databases = append(cluster.Spec.Databases, DatabaseTemplate{
		Name:   "one-too-many",
		Shards: MaximumShards,
	})
	if _, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster); err == nil || !strings.Contains(err.Error(), "65536") {
		t.Fatalf("excess total routing ranges were admitted: %v", err)
	}
}

func TestDatabaseTopologyValidationAllowsSharedAndDisjointCells(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	cluster.Spec.Shards = 8
	cluster.Spec.Databases = []DatabaseTemplate{
		{Name: "a", Shards: 5, Cells: []int32{0, 1, 2, 3, 4}},
		{Name: "b-shared", Shards: 3, Cells: []int32{0, 1, 2}},
		{Name: "b-dedicated", Shards: 3, Cells: []int32{5, 6, 7}},
	}
	if _, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseTopologyValidationRejectsAmbiguousPlacement(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		database DatabaseTemplate
		field    string
	}{
		"count mismatch":  {database: DatabaseTemplate{Name: "app", Shards: 2, Cells: []int32{0}}, field: "cells"},
		"duplicate cell":  {database: DatabaseTemplate{Name: "app", Shards: 2, Cells: []int32{0, 0}}, field: "cells[1]"},
		"outside cluster": {database: DatabaseTemplate{Name: "app", Shards: 2, Cells: []int32{0, 2}}, field: "cells[1]"},
		"too many shards": {database: DatabaseTemplate{Name: "app", Shards: 3, Cells: []int32{0, 1, 2}}, field: "shards"},
	}
	for name, test := range tests {
		name, test := name, test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cluster := validCluster()
			cluster.Spec.Databases = []DatabaseTemplate{test.database}
			_, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster)
			if err == nil || !strings.Contains(err.Error(), test.field) {
				t.Fatalf("invalid database placement was admitted: %v", err)
			}
		})
	}
}

func TestDatabaseTopologyIsImmutableUntilLifecycleControllerExists(t *testing.T) {
	t.Parallel()
	oldCluster := validCluster()
	oldCluster.Spec.Databases = []DatabaseTemplate{
		{Name: "a", Shards: 2, Cells: []int32{0, 1}},
		{Name: "b", Shards: 1, Cells: []int32{0}},
	}

	reordered := oldCluster.DeepCopy()
	reordered.Spec.Databases[0], reordered.Spec.Databases[1] = reordered.Spec.Databases[1], reordered.Spec.Databases[0]
	if _, err := (&PgShardClusterValidator{}).ValidateUpdate(context.Background(), oldCluster, reordered); err != nil {
		t.Fatalf("map-list reordering changed database topology: %v", err)
	}

	materializedDefaults := validCluster()
	materializedDefaults.Spec.Databases = []DatabaseTemplate{
		{Name: "legacy"},
		{Name: "explicit", Shards: 1, Cells: []int32{0}},
	}
	defaulted := materializedDefaults.DeepCopy()
	if err := (&PgShardClusterDefaulter{}).Default(context.Background(), defaulted); err != nil {
		t.Fatal(err)
	}
	defaulted.Spec.Databases[0], defaulted.Spec.Databases[1] = defaulted.Spec.Databases[1], defaulted.Spec.Databases[0]
	if _, err := (&PgShardClusterValidator{}).ValidateUpdate(context.Background(), materializedDefaults, defaulted); err != nil {
		t.Fatalf("reordering map items while materializing equivalent defaults changed database topology: %v", err)
	}

	mutated := oldCluster.DeepCopy()
	mutated.Spec.Databases[1].Cells[0] = 1
	if _, err := (&PgShardClusterValidator{}).ValidateUpdate(context.Background(), oldCluster, mutated); err == nil || !strings.Contains(err.Error(), "databases is immutable") {
		t.Fatalf("database placement mutation was admitted: %v", err)
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
	configuration, err := cluster.ResolvedPostgreSQLConfiguration()
	if err != nil {
		t.Fatal(err)
	}
	if configuration.ManagedLogicalConsumers != 8 || configuration.PrimarySlotDemand != 10 || configuration.StandbySlotDemand != 16 || configuration.PromotionSlotDemand != 18 || configuration.Common["max_replication_slots"] != "20" {
		t.Fatalf("resolved slot configuration = %#v", configuration)
	}
	if len(configuration.Primaries) != 3 || configuration.Primaries[0].Settings["synchronous_standby_names"] != "'ANY 1 (pgshard_member_0001,pgshard_member_0002)'" || len(configuration.Standbys) != 3 {
		t.Fatalf("resolved role profiles = %#v", configuration)
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
	configuration, err := cluster.ResolvedPostgreSQLConfiguration()
	if err != nil {
		t.Fatal(err)
	}
	if configuration.Primaries[0].Settings["synchronous_standby_names"] != "''" || configuration.Primaries[0].Settings["synchronized_standby_slots"] == "''" {
		t.Fatalf("asynchronous role settings = %#v", configuration.Primaries)
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

func TestValidationAcceptsMaximumNameForSingleMemberPodIdentity(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	cluster.Name = strings.Repeat("a", MaximumClusterNameLength)
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = DurabilityAsynchronous
	if _, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster); err != nil {
		t.Fatalf("maximum safe cluster name was rejected: %v", err)
	}
}

func TestValidationRejectsUserSuppliedPodFencingMetadata(t *testing.T) {
	t.Parallel()
	validator := &PgShardClusterValidator{
		FencingReceiptVerifier:    fixedPodFencingReceiptVerifier{verified: true},
		FencingControllerUsername: testFencingControllerUsername,
	}
	controllerContext := podFencingAdmissionContext(testFencingControllerUsername)
	for _, members := range []int32{1, 3} {
		cluster := validCluster()
		cluster.Spec.MembersPerShard = members
		cluster.Spec.Durability = DurabilityAsynchronous
		cluster.Annotations = map[string]string{
			PodFencingChallengeAnnotation: "forged",
			PodFencingReceiptAnnotation:   "forged",
		}
		if _, err := validator.ValidateCreate(context.Background(), cluster); err == nil || !strings.Contains(err.Error(), "reserved for the pgshard controller") {
			t.Fatalf("membersPerShard=%d create error = %v", members, err)
		}
	}

	oldCluster := validCluster()
	newCluster := oldCluster.DeepCopy()
	newCluster.Annotations = map[string]string{PodFencingReceiptAnnotation: "forged"}
	if _, err := validator.ValidateUpdate(context.Background(), oldCluster, newCluster); err == nil || !strings.Contains(err.Error(), "reserved for the pgshard controller") {
		t.Fatalf("multi-member update error = %v", err)
	}
	newCluster = oldCluster.DeepCopy()
	newCluster.Status.PostgreSQLBootstrapSpec = &PostgreSQLBootstrapSpecStatus{
		MembersPerShard:   newCluster.Spec.MembersPerShard,
		PostgreSQLRuntime: "agent-quarantine",
	}
	newCluster.Annotations = map[string]string{
		PodFencingChallengeAnnotation: "controller-challenge",
		PodFencingReceiptAnnotation:   "admission-receipt",
	}
	if _, err := validator.ValidateUpdate(controllerContext, oldCluster, newCluster); err == nil || !strings.Contains(err.Error(), "reserved for the pgshard controller") {
		t.Fatalf("same-update runtime spoof accepted Pod fencing metadata: %v", err)
	}

	oldCluster = validCluster()
	oldCluster.Spec.MembersPerShard = 1
	oldCluster.Spec.Durability = DurabilityAsynchronous
	newCluster = oldCluster.DeepCopy()
	newCluster.Annotations = map[string]string{PodFencingReceiptAnnotation: "forged"}
	if _, err := validator.ValidateUpdate(context.Background(), oldCluster, newCluster); err == nil || !strings.Contains(err.Error(), "preserved or replaced") {
		t.Fatalf("receipt-only single-member update error = %v", err)
	}

	oldCluster.Annotations = nil
	newCluster = oldCluster.DeepCopy()
	newCluster.Annotations = map[string]string{
		PodFencingChallengeAnnotation: "controller-challenge",
		PodFencingReceiptAnnotation:   "admission-receipt",
	}
	if _, err := validator.ValidateUpdate(controllerContext, oldCluster, newCluster); err != nil {
		t.Fatalf("initial admission-attested metadata was rejected: %v", err)
	}
	if _, err := validator.ValidateUpdate(podFencingAdmissionContext("example-user"), oldCluster, newCluster); err == nil || !strings.Contains(err.Error(), "only be established or repaired by the pgshard controller") {
		t.Fatalf("user-established metadata error = %v", err)
	}

	agentMultiMember := validCluster()
	agentMultiMember.Status.PostgreSQLBootstrapSpec = &PostgreSQLBootstrapSpecStatus{
		MembersPerShard:   agentMultiMember.Spec.MembersPerShard,
		PostgreSQLRuntime: "agent-quarantine",
	}
	attestedAgentMultiMember := agentMultiMember.DeepCopy()
	attestedAgentMultiMember.Annotations = map[string]string{
		PodFencingChallengeAnnotation: "controller-multi-member-challenge",
		PodFencingReceiptAnnotation:   "admission-multi-member-receipt",
	}
	if _, err := validator.ValidateUpdate(controllerContext, agentMultiMember, attestedAgentMultiMember); err != nil {
		t.Fatalf("initial agent multi-member admission attestation was rejected: %v", err)
	}
	if _, err := validator.ValidateUpdate(podFencingAdmissionContext("example-user"), agentMultiMember, attestedAgentMultiMember); err == nil || !strings.Contains(err.Error(), "only be established or repaired by the pgshard controller") {
		t.Fatalf("user-established agent multi-member metadata error = %v", err)
	}

	oldCluster = newCluster.DeepCopy()
	newCluster = oldCluster.DeepCopy()
	newCluster.Annotations = nil
	if _, err := validator.ValidateUpdate(controllerContext, oldCluster, newCluster); err == nil || !strings.Contains(err.Error(), "preserved or replaced") {
		t.Fatalf("established metadata removal error = %v", err)
	}
	newCluster = oldCluster.DeepCopy()
	newCluster.Annotations[PodFencingChallengeAnnotation] = "replacement-challenge"
	newCluster.Annotations[PodFencingReceiptAnnotation] = "replacement-receipt"
	if _, err := validator.ValidateUpdate(controllerContext, oldCluster, newCluster); err != nil {
		t.Fatalf("controller repair with a valid final receipt was rejected: %v", err)
	}
	validator.FencingReceiptVerifier = fixedPodFencingReceiptVerifier{verified: false}
	if _, err := validator.ValidateUpdate(controllerContext, oldCluster, newCluster); err == nil || !strings.Contains(err.Error(), "valid final admission receipt") {
		t.Fatalf("invalid controller repair error = %v", err)
	}
	validator.FencingReceiptVerifier = fixedPodFencingReceiptVerifier{err: errors.New("key unavailable")}
	if _, err := validator.ValidateUpdate(controllerContext, oldCluster, newCluster); err == nil || !strings.Contains(err.Error(), "key unavailable") {
		t.Fatalf("unverifiable controller repair error = %v", err)
	}
	validator.FencingReceiptVerifier = fixedPodFencingReceiptVerifier{verified: true}

	deletionTime := metav1.Now()
	oldCluster.DeletionTimestamp = &deletionTime
	oldCluster.Finalizers = []string{"pgshard.io/test-finalizer"}
	newCluster = oldCluster.DeepCopy()
	newCluster.Finalizers = nil
	if _, err := validator.ValidateUpdate(context.Background(), oldCluster, newCluster); err != nil {
		t.Fatalf("deletion-time finalizer removal with preserved metadata was rejected: %v", err)
	}
	newCluster = oldCluster.DeepCopy()
	delete(newCluster.Annotations, PodFencingChallengeAnnotation)
	if _, err := validator.ValidateUpdate(context.Background(), oldCluster, newCluster); err == nil || !strings.Contains(err.Error(), "immutable during deletion") {
		t.Fatalf("deletion-time metadata removal error = %v", err)
	}
}

func TestValidationRejectsUnsafeStorageAndImmutableResize(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	cluster.Spec.Storage.Size = resource.MustParse("2Gi")
	_, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "at least 4Gi") {
		t.Fatalf("undersized storage was accepted: %v", err)
	}

	cluster = validCluster()
	cluster.Spec.PostgreSQL.Parameters = map[string]string{"max_wal_size": "4GB"}
	_, err = (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "one quarter") {
		t.Fatalf("unsafe WAL budget was accepted: %v", err)
	}

	oldCluster := validCluster()
	newCluster := oldCluster.DeepCopy()
	newCluster.Spec.Storage.Size = resource.MustParse("20Gi")
	_, err = (&PgShardClusterValidator{}).ValidateUpdate(context.Background(), oldCluster, newCluster)
	if err == nil || !strings.Contains(err.Error(), "immutable until explicit PVC expansion") {
		t.Fatalf("unsupported storage resize was accepted: %v", err)
	}

	oldCluster = validCluster()
	oldCluster.Spec.Storage.Size = resource.MustParse("2Gi")
	newCluster = oldCluster.DeepCopy()
	newCluster.Spec.Storage.Size = resource.MustParse("4Gi")
	if _, err = (&PgShardClusterValidator{}).ValidateUpdate(context.Background(), oldCluster, newCluster); err != nil {
		t.Fatalf("one-time legacy storage upgrade was rejected: %v", err)
	}
	newCluster.Spec.Storage.Size = resource.MustParse("3Gi")
	if _, err = (&PgShardClusterValidator{}).ValidateUpdate(context.Background(), oldCluster, newCluster); err == nil || !strings.Contains(err.Error(), "at least 4Gi") {
		t.Fatalf("undersized legacy storage update was accepted: %v", err)
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

func TestValidationBoundsRenderedTopologyStrings(t *testing.T) {
	t.Parallel()
	maximumEndpoint := func(host string, length int) string {
		prefix := "https://" + host + "/"
		return prefix + strings.Repeat("x", length-len(prefix))
	}
	validS3 := func() BackupRepository {
		return BackupRepository{Type: RepositoryS3, S3: &S3Repository{
			Bucket:               "backups",
			Endpoint:             "https://minio.example.com",
			Region:               "region",
			Prefix:               "prefix",
			CredentialsSecretRef: corev1.LocalObjectReference{Name: "backup-credentials"},
		}}
	}
	tests := map[string]struct {
		mutate func(*PgShardCluster)
		field  string
	}{
		"bucket": {
			mutate: func(cluster *PgShardCluster) {
				cluster.Spec.Backup.Repository = validS3()
				cluster.Spec.Backup.Repository.S3.Bucket = strings.Repeat("b", MaximumS3BucketLength+1)
			},
			field: "bucket",
		},
		"S3 endpoint": {
			mutate: func(cluster *PgShardCluster) {
				cluster.Spec.Backup.Repository = validS3()
				cluster.Spec.Backup.Repository.S3.Endpoint = maximumEndpoint("minio.example.com", MaximumEndpointLength+1)
			},
			field: "endpoint",
		},
		"region": {
			mutate: func(cluster *PgShardCluster) {
				cluster.Spec.Backup.Repository = validS3()
				cluster.Spec.Backup.Repository.S3.Region = strings.Repeat("r", MaximumS3RegionLength+1)
			},
			field: "region",
		},
		"prefix": {
			mutate: func(cluster *PgShardCluster) {
				cluster.Spec.Backup.Repository = validS3()
				cluster.Spec.Backup.Repository.S3.Prefix = strings.Repeat("p", MaximumS3PrefixLength+1)
			},
			field: "prefix",
		},
		"OpenTelemetry endpoint": {
			mutate: func(cluster *PgShardCluster) {
				cluster.Spec.Observability.OpenTelemetryEndpoint = maximumEndpoint("collector.example.com", MaximumEndpointLength+1)
			},
			field: "openTelemetryEndpoint",
		},
	}
	for name, test := range tests {
		name, test := name, test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cluster := validCluster()
			test.mutate(cluster)
			if _, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster); err == nil || !strings.Contains(err.Error(), test.field) {
				t.Fatalf("oversized %s was admitted: %v", test.field, err)
			}
		})
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

func TestValidationKeepsUnimplementedDataTransitionsImmutable(t *testing.T) {
	t.Parallel()
	tests := map[string]func(*PgShardCluster){
		"shards":          func(cluster *PgShardCluster) { cluster.Spec.Shards++ },
		"membersPerShard": func(cluster *PgShardCluster) { cluster.Spec.MembersPerShard = 5 },
		"durability":      func(cluster *PgShardCluster) { cluster.Spec.Durability = DurabilityAsynchronous },
		"deletionPolicy":  func(cluster *PgShardCluster) { cluster.Spec.Storage.DeletionPolicy = DeletionDelete },
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			oldCluster := validCluster()
			newCluster := oldCluster.DeepCopy()
			mutate(newCluster)
			if _, err := (&PgShardClusterValidator{}).ValidateUpdate(context.Background(), oldCluster, newCluster); err == nil || !strings.Contains(err.Error(), "immutable") {
				t.Fatalf("%s transition was admitted: %v", name, err)
			}
		})
	}
}
