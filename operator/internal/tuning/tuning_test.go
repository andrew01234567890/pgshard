package tuning

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
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
		"logical_decoding_work_mem":       "64MB",
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
		"listen_addresses":                "'*'",
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
	if err := ApplyOverrides(settings, map[string]string{"fsync": "off"}, 0); err == nil {
		t.Fatal("expected fsync override to be rejected")
	}
	// The generated value divides a budget share across every managed reorder
	// buffer, so a per-buffer override silently remultiplies it by the fleet.
	if err := ApplyOverrides(settings, map[string]string{"logical_decoding_work_mem": "64MB"}, 0); err == nil {
		t.Fatal("expected logical_decoding_work_mem override to be rejected")
	}
	if err := ApplyOverrides(settings, map[string]string{"max_wal_size": "4GB"}, 0); err != nil {
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
	}, 0)
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
	}, 0)
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
		"subnormal float":       {"checkpoint_completion_target": "1e-320"},
		"smallest subnormal":    {"autovacuum_vacuum_scale_factor": "5e-324"},
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
			if err := ApplyOverrides(settings, overrides, 0); err == nil {
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
	if err := ApplyOverrides(settings, overrides, 0); err != nil {
		t.Fatalf("bounded overrides rejected: %v", err)
	}
}

func TestApplyOverridesStoresTheParsedValueNotTheSpelling(t *testing.T) {
	t.Parallel()
	// Every spelling here is one strconv accepts, so it survives admission. The
	// stored value is what reaches postgresql.conf, and guc-file.l has to take
	// it as a single value token or the postmaster refuses to start.
	tests := map[string]struct {
		key   string
		value string
		want  string
	}{
		"hexadecimal float":   {key: "checkpoint_completion_target", value: "0x1p-1", want: "0.5"},
		"bare exponent":       {key: "random_page_cost", value: "1e1", want: "10"},
		"separated float":     {key: "random_page_cost", value: "1_0.5", want: "10.5"},
		"separated integer":   {key: "seq_page_cost", value: "1_0", want: "10"},
		"negative exponent":   {key: "autovacuum_vacuum_scale_factor", value: "1e-7", want: "0.0000001"},
		"leading point":       {key: "autovacuum_analyze_scale_factor", value: ".05", want: "0.05"},
		"trailing point":      {key: "seq_page_cost", value: "5.", want: "5"},
		"negative zero":       {key: "autovacuum_analyze_scale_factor", value: "-0.0", want: "-0"},
		"signed integer":      {key: "default_statistics_target", value: "+200", want: "200"},
		"zero padded integer": {key: "effective_io_concurrency", value: "0200", want: "200"},
		"signed duration":     {key: "checkpoint_timeout", value: "+15min", want: "15min"},
		"zero padded size":    {key: "max_wal_size", value: "004GB", want: "4GB"},
		"ordinary float":      {key: "checkpoint_completion_target", value: "0.9", want: "0.9"},
		"ordinary enum":       {key: "log_statement", value: "ddl", want: "ddl"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			settings := map[string]string{"max_worker_processes": "8", "min_wal_size": "80MB", "max_wal_size": "1GB"}
			if err := ApplyOverrides(settings, map[string]string{test.key: test.value}, 0); err != nil {
				t.Fatalf("%s = %q was refused, so it can no longer reach postgresql.conf: %v", test.key, test.value, err)
			}
			if got := settings[test.key]; got != test.want {
				t.Fatalf("%s = %q was stored as %q, want the parsed value %q", test.key, test.value, got, test.want)
			}
		})
	}
}

func TestValidateStorageBoundsCheckpointWAL(t *testing.T) {
	t.Parallel()
	settings := map[string]string{"max_wal_size": "1GB"}
	if err := ValidateStorage(settings, resource.MustParse("4Gi")); err != nil {
		t.Fatalf("safe WAL budget rejected: %v", err)
	}
	if err := ValidateStorage(settings, resource.MustParse("2Gi")); err == nil || !strings.Contains(err.Error(), "at least 4Gi") {
		t.Fatalf("undersized storage accepted: %v", err)
	}
	settings["max_wal_size"] = "2GB"
	if err := ValidateStorage(settings, resource.MustParse("4Gi")); err == nil || !strings.Contains(err.Error(), "one quarter") {
		t.Fatalf("oversized WAL budget accepted: %v", err)
	}
}

// The generated settings must fit the container's memory limit. The suite
// otherwise exercises only balanced core/memory ratios, which is why a fleet of
// autovacuum workers sized from cores alone, each inheriting an uncharged
// maintenance_work_mem, went unnoticed. A fleet of reorder buffers left at
// PostgreSQL's per-buffer default is the same failure below 4Gi. At and above
// 4Gi that default happens to fit its share, and the derived value is itself
// 64MB there, so what has to be caught at those shapes is the parameter going
// unemitted rather than any bound on its value.
func TestSkewedResourceRatiosStayInsideTheMemoryLimit(t *testing.T) {
	// The decoding fleet is fixed by admission rather than by the CR: the webhook
	// always passes its own maximumChangeStreams constant of 4, and Calculate adds
	// four operation slots, so every real cluster decodes with eight managed
	// consumers whatever its resources are. This package cannot import v1alpha1 to
	// say so -- v1alpha1 imports this package -- so the resolved topology is
	// restated here and exercised alongside the smallest one input validation
	// accepts. Each shape is its own subtest so a regression is reported wherever
	// it occurs rather than only at the first shape that trips.
	for _, topology := range []struct {
		name                                              string
		poolerMaxReplicas, membersPerShard, changeStreams int32
	}{
		{"minimum", 1, 1, 1},
		{"admission-resolved", 10, 3, 4},
	} {
		for _, shape := range []struct {
			cpu, memory string
		}{
			{"12", "1Gi"}, {"16", "1Gi"}, {"8", "2Gi"}, {"16", "2Gi"},
			{"4", "4Gi"}, {"16", "8Gi"}, {"1", "2Gi"}, {"2", "1Gi"},
		} {
			t.Run(fmt.Sprintf("%s/%score/%s", topology.name, shape.cpu, shape.memory), func(t *testing.T) {
				result, err := Calculate(Input{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(shape.cpu),
							corev1.ResourceMemory: resource.MustParse(shape.memory),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(shape.cpu),
							corev1.ResourceMemory: resource.MustParse(shape.memory),
						},
					},
					PoolerMaxReplicas:    topology.poolerMaxReplicas,
					MembersPerShard:      topology.membersPerShard,
					MaximumChangeStreams: topology.changeStreams,
				})
				if err != nil {
					t.Skipf("a shape this small is rejected outright, which is also safe: %v", err)
				}
				workMem := mebibytes(t, result.Settings["autovacuum_work_mem"])
				if workMem < minimumAutovacuumWorkMem/mib {
					t.Fatalf("autovacuum workers are starved at %dMB", workMem)
				}
				workers, err := strconv.ParseInt(result.Settings["autovacuum_max_workers"], 10, 64)
				if err != nil {
					t.Fatalf("autovacuum_max_workers is not an integer: %v", err)
				}
				shared := mebibytes(t, result.Settings["shared_buffers"])
				maintenance := mebibytes(t, result.Settings["maintenance_work_mem"])
				autovacuum := workers * workMem
				// The fleet has a budget share; exceeding it is what a worker count
				// derived from cores alone does, and the totals below can still look
				// affordable while it does.
				pot := (result.MemoryBytes-result.ReservedBytes)/mib - shared
				if ceiling := max64(pot/4, minimumAutovacuumWorkMem/mib); autovacuum > ceiling {
					t.Fatalf("%d autovacuum workers reach %dMB against a %dMB share", workers, autovacuum, ceiling)
				}
				// logical_decoding_work_mem is charged per reorder buffer and every
				// managed consumer's walsender holds one, so the fleet -- not one
				// buffer -- is what the budget has to cover. PostgreSQL spills these to
				// disk instead of raising an error, so nothing but the cgroup enforces
				// the total.
				consumers := int64(result.ManagedLogicalConsumers)
				if consumers < 1 {
					t.Fatalf("the shape reports %d logical consumers", consumers)
				}
				// Absence is the defect this replaced, and it is the only part of that
				// defect visible at every shape. An unemitted parameter leaves the
				// server on PostgreSQL's built-in 64MB, a default its own documentation
				// justifies by assuming few concurrent replication connections. From
				// 4Gi upward the derived value is itself 64MB, so no bound on the value
				// can tell a budgeted configuration from an inherited default there --
				// only the emission can.
				emitted, ok := result.Settings["logical_decoding_work_mem"]
				if !ok {
					t.Fatalf("no logical_decoding_work_mem is emitted, leaving %d reorder buffers on PostgreSQL's unbudgeted 64MB default", consumers)
				}
				decodingWorkMem := mebibytes(t, emitted)
				decoding := consumers * decodingWorkMem
				if ceiling := max64(pot/4, consumers); decoding > ceiling {
					t.Fatalf("%d logical decoding buffers reach %dMB against a %dMB share", consumers, decoding, ceiling)
				}
				// What follows budgets the resident commitments only: shared_buffers,
				// the whole autovacuum fleet, every managed reorder buffer, and one
				// concurrent manual maintenance operation, against the limit the cgroup
				// enforces.
				//
				// The work_mem fleet is deliberately not a term in it. work_mem is
				// sized as pot/(max_connections*4), so that fleet's own product is the
				// entire pot by construction -- the last assertion states exactly that
				// -- and the two quarter-shares are drawn from the same pot. Summing
				// all three exceeds the limit at every shape above the 1Gi minimum, so
				// such an invariant would bound nothing and would only make supported
				// shapes unrepresentable.
				//
				// The exclusion turns on what drives each fleet. A backend reaches
				// work_mem only while executing a query node whose input exceeds it,
				// those peaks are independent across clients, and the pooler bounds how
				// many clients there are. A reorder buffer is held for as long as its
				// replication connection lives, its occupancy is driven by the WAL
				// stream rather than by any client, and every managed consumer decodes
				// the same stream, so one large transaction pushes the whole fleet
				// toward its limit at once. For reorder buffers the fleet total is the
				// expected cost; for work_mem it is a ceiling no workload is sized to
				// reach.
				resident := shared + autovacuum + decoding + maintenance
				limit := result.MemoryBytes / mib
				t.Logf("limit=%dMB pot=%dMB resident=%dMB (shared=%d autovacuum=%d*%d decoding=%d*%d maintenance=%d)",
					limit, pot, resident, shared, workers, workMem, consumers, decodingWorkMem, maintenance)
				if resident >= limit {
					t.Fatalf("resident commitments reach %dMB (shared=%d autovacuum=%d*%d decoding=%d*%d maintenance=%d) of a %dMB limit before the postmaster, backends or worker slots",
						resident, shared, workers, workMem, consumers, decodingWorkMem, maintenance, limit)
				}
				// The claim that exclusion rests on, checked rather than asserted: the
				// transient fleet's ceiling is the whole pot and never more than it. A
				// change that broke this would make work_mem a claim the resident
				// budget above could no longer leave out.
				transient := int64(result.MaxConnections) * 4 * mebibytes(t, result.Settings["work_mem"])
				if transient > pot {
					t.Fatalf("%d backends running four operations of %s each reach %dMB against a %dMB pot",
						result.MaxConnections, result.Settings["work_mem"], transient, pot)
				}
			})
		}
	}
}

// formatMiB truncates, so a per-buffer share below a mebibyte would reach
// postgresql.conf as "0MB" -- under the parameter's own 64kB minimum, which
// makes it fatal at postmaster startup rather than merely small. What keeps the
// fleet itself inside its share at this cardinality is admission pinning the
// change stream count, not this arithmetic; the floor exists so that a fleet
// larger than its share still renders a value the server can start on.
func TestLogicalDecodingWorkMemNeverRendersBelowItsMinimum(t *testing.T) {
	t.Parallel()
	result, err := Calculate(Input{
		Resources:            resources("1", "1", "1Gi", "1Gi"),
		PoolerMaxReplicas:    1,
		MembersPerShard:      1,
		MaximumChangeStreams: maximumChangeStreams,
	})
	if err != nil {
		t.Fatalf("the smallest shape at the largest accepted stream count must be accepted: %v", err)
	}
	if share := result.LogicalDecodingBudgetBytes / int64(result.ManagedLogicalConsumers); share >= mib {
		t.Fatalf("this shape no longer drives the floor: %d consumers still afford %d bytes each",
			result.ManagedLogicalConsumers, share)
	}
	if got := mebibytes(t, result.Settings["logical_decoding_work_mem"]); got < 1 {
		t.Fatalf("logical_decoding_work_mem = %q, which PostgreSQL rejects at startup",
			result.Settings["logical_decoding_work_mem"])
	}
}

func mebibytes(t *testing.T, value string) int64 {
	t.Helper()
	parsed, err := strconv.ParseInt(strings.TrimSuffix(value, "MB"), 10, 64)
	if err != nil {
		t.Fatalf("setting %q is not a MB quantity: %v", value, err)
	}
	return parsed
}

// autovacuum_work_mem is sized for the generated worker count against a fixed
// share of the memory budget, and is not itself overridable. Raising the count
// therefore multiplies the ceiling the generation exists to bound, so an
// override may only lower it.
func TestAutovacuumWorkerOverrideCannotExceedTheMemoryBudget(t *testing.T) {
	result, err := Calculate(Input{
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("12"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("12"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
		},
		PoolerMaxReplicas: 1, MembersPerShard: 1, MaximumChangeStreams: 1,
	})
	if err != nil {
		t.Fatalf("a 12 core, 1Gi shape must be accepted: %v", err)
	}
	budgeted, err := strconv.ParseInt(result.Settings["autovacuum_max_workers"], 10, 64)
	if err != nil {
		t.Fatalf("autovacuum_max_workers is not an integer: %v", err)
	}
	// max_worker_processes is 52 for this shape, so the process-slot bound
	// alone would permit 20 — far past what the memory share can afford.
	if _, err := canonicalOverrideValue("autovacuum_max_workers", strconv.FormatInt(budgeted+1, 10), result.Settings, result.AutovacuumBudgetBytes); err == nil {
		t.Fatalf("an override of %d workers was accepted against a budget for %d", budgeted+1, budgeted)
	}
	if _, err := canonicalOverrideValue("autovacuum_max_workers", strconv.FormatInt(budgeted, 10), result.Settings, result.AutovacuumBudgetBytes); err != nil {
		t.Fatalf("the budgeted worker count was rejected: %v", err)
	}
	if _, err := canonicalOverrideValue("autovacuum_max_workers", "1", result.Settings, result.AutovacuumBudgetBytes); err != nil {
		t.Fatalf("lowering the worker count was rejected: %v", err)
	}
}
