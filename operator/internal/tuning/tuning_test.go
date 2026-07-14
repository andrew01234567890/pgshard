package tuning

import (
	"encoding/json"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

type replicationSlotNameContract struct {
	MemberPhysicalSlots []struct {
		MemberOrdinal int32  `json:"member_ordinal"`
		SlotName      string `json:"slot_name"`
	} `json:"member_physical_slots"`
}

func TestMemberReplicationNameMatchesSharedContract(t *testing.T) {
	t.Parallel()
	contents, err := os.ReadFile("../../../contracts/replication-slot-names.json")
	if err != nil {
		t.Fatal(err)
	}
	var contract replicationSlotNameContract
	if err := json.Unmarshal(contents, &contract); err != nil {
		t.Fatal(err)
	}
	if len(contract.MemberPhysicalSlots) == 0 {
		t.Fatal("shared replication-slot naming contract has no cases")
	}
	for _, test := range contract.MemberPhysicalSlots {
		if got := memberReplicationName(test.MemberOrdinal); got != test.SlotName {
			t.Errorf("member %d slot name = %q, want %q", test.MemberOrdinal, got, test.SlotName)
		}
	}
}

func resources(cpuRequest, cpuLimit, memoryRequest, memoryLimit string) corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(cpuRequest),
			corev1.ResourceMemory: resource.MustParse(memoryRequest),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(cpuLimit),
			corev1.ResourceMemory: resource.MustParse(memoryLimit),
		},
	}
}

func TestCalculateDeterministicSafeSettings(t *testing.T) {
	t.Parallel()
	got, err := Calculate(Input{
		Resources:            resources("2", "4", "4Gi", "8Gi"),
		PoolerMaxReplicas:    10,
		MembersPerShard:      3,
		MaximumChangeStreams: 4,
		SynchronousStandbys:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.MemoryBytes != 4*1024*1024*1024 || got.CPUMilli != 2000 {
		t.Fatalf("resource basis = memory %d cpu %d", got.MemoryBytes, got.CPUMilli)
	}
	want := map[string]string{
		"archive_mode":                    "off",
		"shared_buffers":                  "1024MB",
		"effective_cache_size":            "2867MB",
		"maintenance_work_mem":            "204MB",
		"work_mem":                        "5MB",
		"max_connections":                 "100",
		"max_prepared_transactions":       "48",
		"max_replication_slots":           "20",
		"max_wal_senders":                 "22",
		"max_worker_processes":            "12",
		"max_parallel_workers":            "2",
		"max_parallel_workers_per_gather": "1",
		"autovacuum_max_workers":          "3",
		"wal_level":                       "logical",
		"fsync":                           "on",
		"full_page_writes":                "on",
		"hot_standby":                     "on",
		"idle_replication_slot_timeout":   "0",
		"synchronous_commit":              "on",
	}
	for key, value := range want {
		if got.Settings[key] != value {
			t.Errorf("%s = %q, want %q", key, got.Settings[key], value)
		}
	}
	if got.ManagedLogicalConsumers != 8 || got.PrimarySlotDemand != 10 || got.StandbySlotDemand != 16 || got.PromotionSlotDemand != 18 {
		t.Fatalf("slot demand = consumers %d primary %d standby %d promotion %d", got.ManagedLogicalConsumers, got.PrimarySlotDemand, got.StandbySlotDemand, got.PromotionSlotDemand)
	}
	if len(got.Primaries) != 3 {
		t.Fatalf("primary profiles = %#v", got.Primaries)
	}
	wantCandidates := []string{
		"pgshard_member_0001,pgshard_member_0002",
		"pgshard_member_0000,pgshard_member_0002",
		"pgshard_member_0000,pgshard_member_0001",
	}
	for ordinal, primary := range got.Primaries {
		candidates := wantCandidates[ordinal]
		if primary.Ordinal != int32(ordinal) ||
			primary.Settings["synchronized_standby_slots"] != postgresqlString(candidates) ||
			primary.Settings["synchronous_standby_names"] != postgresqlString("ANY 1 ("+candidates+")") {
			t.Fatalf("primary profile %d = %#v", ordinal, primary)
		}
	}
	if len(got.Standbys) != 3 {
		t.Fatalf("standby profiles = %#v", got.Standbys)
	}
	for ordinal, standby := range got.Standbys {
		name := memberReplicationName(int32(ordinal))
		if standby.Ordinal != int32(ordinal) ||
			standby.ApplicationName != name ||
			standby.PhysicalSlotName != name ||
			standby.Settings["primary_slot_name"] != postgresqlString(name) ||
			standby.Settings["hot_standby"] != "on" ||
			standby.Settings["hot_standby_feedback"] != "on" ||
			standby.Settings["sync_replication_slots"] != "on" ||
			standby.Settings["wal_receiver_status_interval"] != "1s" {
			t.Fatalf("standby profile %d = %#v", ordinal, standby)
		}
	}
}

func TestCalculateSeparatesPrimaryAnchorsFromStandbyDecoders(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                string
		members             int32
		synchronousStandbys int32
		primaryDemand       int32
		standbyDemand       int32
		promotionDemand     int32
		maxSlots            int32
	}{
		{name: "single asynchronous member", members: 1, primaryDemand: 8, maxSlots: 10},
		{name: "three synchronous members", members: 3, synchronousStandbys: 1, primaryDemand: 10, standbyDemand: 16, promotionDemand: 18, maxSlots: 20},
		{name: "five synchronous members", members: 5, synchronousStandbys: 1, primaryDemand: 12, standbyDemand: 16, promotionDemand: 20, maxSlots: 22},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := Calculate(Input{
				Resources:            resources("2", "4", "4Gi", "8Gi"),
				PoolerMaxReplicas:    10,
				MembersPerShard:      test.members,
				MaximumChangeStreams: 4,
				SynchronousStandbys:  test.synchronousStandbys,
			})
			if err != nil {
				t.Fatal(err)
			}
			if got.PrimarySlotDemand != test.primaryDemand || got.StandbySlotDemand != test.standbyDemand || got.PromotionSlotDemand != test.promotionDemand || got.MaxReplicationSlots != test.maxSlots {
				t.Fatalf("slot demand = primary %d standby %d promotion %d max %d", got.PrimarySlotDemand, got.StandbySlotDemand, got.PromotionSlotDemand, got.MaxReplicationSlots)
			}
		})
	}
}

func TestCalculateRejectsImpossibleSynchronousStandbyCount(t *testing.T) {
	t.Parallel()
	for _, input := range []Input{
		{Resources: resources("1", "1", "2Gi", "2Gi"), PoolerMaxReplicas: 2, MembersPerShard: 1, SynchronousStandbys: 1},
		{Resources: resources("1", "1", "2Gi", "2Gi"), PoolerMaxReplicas: 2, MembersPerShard: 3, SynchronousStandbys: 2},
	} {
		if _, err := Calculate(input); err == nil {
			t.Fatalf("invalid synchronous standby count accepted: %#v", input)
		}
	}
}

func TestCalculateBoundsSlotCardinalityBeforeNarrowing(t *testing.T) {
	t.Parallel()
	for _, input := range []Input{
		{Resources: resources("1", "1", "2Gi", "2Gi"), PoolerMaxReplicas: 2, MembersPerShard: maximumMembersPerShard + 1},
		{Resources: resources("1", "1", "2Gi", "2Gi"), PoolerMaxReplicas: 2, MembersPerShard: 3, MaximumChangeStreams: maximumChangeStreams + 1},
	} {
		if _, err := Calculate(input); err == nil {
			t.Fatalf("unbounded slot cardinality accepted: %#v", input)
		}
	}
}

func TestCalculateRejectsMissingAndUnsafeResources(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		resources corev1.ResourceRequirements
	}{
		{name: "missing", resources: corev1.ResourceRequirements{}},
		{name: "limit below request", resources: resources("2", "1", "2Gi", "2Gi")},
		{name: "too little memory", resources: resources("1", "1", "512Mi", "512Mi")},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Calculate(Input{Resources: test.resources, PoolerMaxReplicas: 2, MembersPerShard: 3})
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestCalculateRejectsQuantitiesBeforeIntegerOverflow(t *testing.T) {
	t.Parallel()
	absurd := "9223372036854775807"
	_, err := Calculate(Input{
		Resources:            resources(absurd, absurd, absurd, absurd),
		PoolerMaxReplicas:    2,
		MembersPerShard:      3,
		MaximumChangeStreams: 4,
	})
	if err == nil {
		t.Fatal("overflowing Kubernetes quantities were accepted")
	}
}

func TestApplyOverridesRejectsOwnedSafetySettings(t *testing.T) {
	t.Parallel()
	settings := map[string]string{"fsync": "on"}
	if err := ApplyOverrides(settings, map[string]string{"fsync": "off"}); err == nil {
		t.Fatal("expected fsync override to be rejected")
	}
	if err := ApplyOverrides(settings, map[string]string{"max_wal_size": "4GB"}); err != nil {
		t.Fatalf("safe override rejected: %v", err)
	}
	if settings["max_wal_size"] != "4GB" {
		t.Fatal("safe override not applied")
	}
}

func TestApplyOverridesIsAtomicOnValidationFailure(t *testing.T) {
	t.Parallel()
	settings := map[string]string{"max_wal_size": "1GB"}
	err := ApplyOverrides(settings, map[string]string{
		"max_wal_size": "4GB",
		"wal_level":    "minimal",
	})
	if err == nil {
		t.Fatal("expected unsafe override to fail")
	}
	if settings["max_wal_size"] != "1GB" {
		t.Fatalf("settings were partially mutated: %#v", settings)
	}
}

func TestApplyOverridesRejectsConfigurationInjection(t *testing.T) {
	t.Parallel()
	settings := map[string]string{"fsync": "on"}
	err := ApplyOverrides(settings, map[string]string{
		"log_statement": "none\nfsync = off",
	})
	if err == nil {
		t.Fatal("expected multiline override to fail")
	}
	if _, exists := settings["log_statement"]; exists || settings["fsync"] != "on" {
		t.Fatalf("settings were mutated after injection attempt: %#v", settings)
	}
}

func TestApplyOverridesRejectsNonViableValues(t *testing.T) {
	t.Parallel()
	tests := map[string]map[string]string{
		"invalid enum":          {"log_statement": "everything"},
		"invalid float":         {"checkpoint_completion_target": "999"},
		"invalid integer":       {"default_statistics_target": "many"},
		"invalid duration":      {"checkpoint_timeout": "forever"},
		"too short duration":    {"checkpoint_timeout": "1s"},
		"invalid size":          {"max_wal_size": "garbage"},
		"too small size":        {"max_wal_size": "16MB"},
		"inverted wal sizes":    {"min_wal_size": "4GB", "max_wal_size": "1GB"},
		"min above default max": {"min_wal_size": "2GB"},
	}
	for name, overrides := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			settings := map[string]string{"max_worker_processes": "8", "min_wal_size": "80MB", "max_wal_size": "1GB"}
			if err := ApplyOverrides(settings, overrides); err == nil {
				t.Fatalf("unsafe overrides accepted: %#v", overrides)
			}
		})
	}
}

func TestApplyOverridesAcceptsBoundedValues(t *testing.T) {
	t.Parallel()
	settings := map[string]string{"max_worker_processes": "8", "min_wal_size": "80MB", "max_wal_size": "1GB"}
	overrides := map[string]string{
		"autovacuum_analyze_scale_factor": "0.05",
		"autovacuum_max_workers":          "4",
		"checkpoint_completion_target":    "0.9",
		"checkpoint_timeout":              "15min",
		"default_statistics_target":       "500",
		"effective_io_concurrency":        "200",
		"log_min_duration_statement":      "250",
		"log_statement":                   "ddl",
		"max_wal_size":                    "4GB",
		"min_wal_size":                    "1GB",
		"random_page_cost":                "1.1",
	}
	if err := ApplyOverrides(settings, overrides); err != nil {
		t.Fatalf("bounded overrides rejected: %v", err)
	}
}
