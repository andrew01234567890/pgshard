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
