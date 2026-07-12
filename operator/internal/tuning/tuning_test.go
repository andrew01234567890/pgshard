package tuning

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

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
		"max_replication_slots":           "12",
		"max_wal_senders":                 "14",
		"max_worker_processes":            "12",
		"max_parallel_workers":            "2",
		"max_parallel_workers_per_gather": "1",
		"autovacuum_max_workers":          "3",
		"wal_level":                       "logical",
		"fsync":                           "on",
		"full_page_writes":                "on",
		"synchronous_commit":              "on",
	}
	for key, value := range want {
		if got.Settings[key] != value {
			t.Errorf("%s = %q, want %q", key, got.Settings[key], value)
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
