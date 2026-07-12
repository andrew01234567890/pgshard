// Package tuning derives conservative PostgreSQL 18 settings from Kubernetes
// resources. It is pure so admission, reconciliation, and documentation can use
// exactly the same calculation.
package tuning

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const mib = int64(1024 * 1024)

var allowedOverrides = map[string]struct{}{
	"autovacuum_analyze_scale_factor": {},
	"autovacuum_max_workers":          {},
	"autovacuum_vacuum_cost_limit":    {},
	"autovacuum_vacuum_scale_factor":  {},
	"checkpoint_completion_target":    {},
	"checkpoint_timeout":              {},
	"default_statistics_target":       {},
	"effective_io_concurrency":        {},
	"log_min_duration_statement":      {},
	"log_statement":                   {},
	"max_wal_size":                    {},
	"min_wal_size":                    {},
	"random_page_cost":                {},
	"seq_page_cost":                   {},
}

var (
	maximumPostgreSQLCPU    = resourceQuantity(maximumPostgreSQLCPUText)
	maximumPostgreSQLMemory = resourceQuantity(maximumPostgreSQLMemoryText)
)

const (
	maximumPostgreSQLCPUText    = "1024"
	maximumPostgreSQLMemoryText = "16Ti"
)

type Input struct {
	Resources            corev1.ResourceRequirements
	PoolerMaxReplicas    int32
	MembersPerShard      int32
	MaximumChangeStreams int32
}

type Result struct {
	MemoryBytes             int64
	CPUMilli                int64
	ReservedBytes           int64
	MaxConnections          int32
	MaxPreparedTransactions int32
	MaxWALSenders           int32
	MaxReplicationSlots     int32
	Settings                map[string]string
}

func Calculate(in Input) (Result, error) {
	cpuRequest, ok := in.Resources.Requests[corev1.ResourceCPU]
	if !ok || cpuRequest.Sign() <= 0 {
		return Result{}, fmt.Errorf("postgresql.resources.requests.cpu must be positive")
	}
	cpuLimit, ok := in.Resources.Limits[corev1.ResourceCPU]
	if !ok || cpuLimit.Sign() <= 0 {
		return Result{}, fmt.Errorf("postgresql.resources.limits.cpu must be positive")
	}
	memRequest, ok := in.Resources.Requests[corev1.ResourceMemory]
	if !ok || memRequest.Sign() <= 0 {
		return Result{}, fmt.Errorf("postgresql.resources.requests.memory must be positive")
	}
	memLimit, ok := in.Resources.Limits[corev1.ResourceMemory]
	if !ok || memLimit.Sign() <= 0 {
		return Result{}, fmt.Errorf("postgresql.resources.limits.memory must be positive")
	}
	if cpuLimit.Cmp(cpuRequest) < 0 {
		return Result{}, fmt.Errorf("postgresql CPU limit must be at least its request")
	}
	if memLimit.Cmp(memRequest) < 0 {
		return Result{}, fmt.Errorf("postgresql memory limit must be at least its request")
	}
	if cpuRequest.Cmp(maximumPostgreSQLCPU) > 0 || cpuLimit.Cmp(maximumPostgreSQLCPU) > 0 {
		return Result{}, fmt.Errorf("postgresql CPU request and limit must not exceed %s cores", maximumPostgreSQLCPUText)
	}
	if memRequest.Cmp(maximumPostgreSQLMemory) > 0 || memLimit.Cmp(maximumPostgreSQLMemory) > 0 {
		return Result{}, fmt.Errorf("postgresql memory request and limit must not exceed %s", maximumPostgreSQLMemoryText)
	}

	memory := min64(memRequest.Value(), memLimit.Value())
	cpu := min64(cpuRequest.MilliValue(), cpuLimit.MilliValue())
	if memory < 1024*mib {
		return Result{}, fmt.Errorf("postgresql memory must be at least 1Gi")
	}
	if in.PoolerMaxReplicas < 1 {
		return Result{}, fmt.Errorf("pooler maximum replicas must be positive")
	}
	if in.PoolerMaxReplicas > 100 {
		return Result{}, fmt.Errorf("pooler maximum replicas must not exceed 100")
	}
	if in.MembersPerShard < 1 {
		return Result{}, fmt.Errorf("members per shard must be positive")
	}
	if in.MaximumChangeStreams < 0 {
		return Result{}, fmt.Errorf("maximum change streams cannot be negative")
	}

	shared := memory / 4
	effective := memory * 70 / 100
	reserved := max64(memory/5, 512*mib)

	connectionTarget := int64(in.PoolerMaxReplicas)*8 + 20
	connectionMemoryLimit := (memory - reserved - shared) / (4 * mib)
	maxConnections := clamp64(connectionTarget, 32, min64(500, connectionMemoryLimit))
	if maxConnections < 32 {
		return Result{}, fmt.Errorf("resources cannot safely provide the minimum backend connection budget")
	}

	available := memory - reserved - shared
	workMem := clamp64(available/(maxConnections*4), mib, 64*mib)
	maintenance := clamp64(memory/20, 64*mib, 1024*mib)
	cores := (cpu + 999) / 1000
	workerProcesses := max64(8, cores*4+4)
	parallelWorkers := max64(2, cores)
	parallelWorkersPerGather := clamp64((cores+1)/2, 1, 4)
	autovacuumWorkers := clamp64(cores+1, 3, 10)

	operationSlots := int64(4) // one each for backup, DDL, reshard, and recovery
	physicalSlots := int64(in.MembersPerShard - 1)
	logicalSlots := int64(in.MaximumChangeStreams) + operationSlots
	maxSlots := physicalSlots + logicalSlots + 2
	maxSenders := maxSlots + 2
	maxPrepared := max64(32, int64(in.PoolerMaxReplicas)*4) + 8

	settings := map[string]string{
		"archive_mode":                    "on",
		"autovacuum_max_workers":          strconv.FormatInt(autovacuumWorkers, 10),
		"effective_cache_size":            formatMiB(effective),
		"fsync":                           "on",
		"full_page_writes":                "on",
		"maintenance_work_mem":            formatMiB(maintenance),
		"max_connections":                 strconv.FormatInt(maxConnections, 10),
		"max_parallel_workers":            strconv.FormatInt(parallelWorkers, 10),
		"max_parallel_workers_per_gather": strconv.FormatInt(parallelWorkersPerGather, 10),
		"max_prepared_transactions":       strconv.FormatInt(maxPrepared, 10),
		"max_replication_slots":           strconv.FormatInt(maxSlots, 10),
		"max_wal_senders":                 strconv.FormatInt(maxSenders, 10),
		"max_wal_size":                    "1GB",
		"max_worker_processes":            strconv.FormatInt(workerProcesses, 10),
		"password_encryption":             "scram-sha-256",
		"min_wal_size":                    "80MB",
		"shared_buffers":                  formatMiB(shared),
		"synchronous_commit":              "on",
		"wal_level":                       "logical",
		"work_mem":                        formatMiB(workMem),
	}

	return Result{
		MemoryBytes:             memory,
		CPUMilli:                cpu,
		ReservedBytes:           reserved,
		MaxConnections:          int32(maxConnections),
		MaxPreparedTransactions: int32(maxPrepared),
		MaxWALSenders:           int32(maxSenders),
		MaxReplicationSlots:     int32(maxSlots),
		Settings:                settings,
	}, nil
}

func ApplyOverrides(settings map[string]string, overrides map[string]string) error {
	keys := make([]string, 0, len(overrides))
	for key := range overrides {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		normalized := strings.ToLower(strings.TrimSpace(key))
		if normalized != key {
			return fmt.Errorf("PostgreSQL parameter %q must be lower-case without surrounding whitespace", key)
		}
		if _, ok := allowedOverrides[key]; !ok {
			return fmt.Errorf("PostgreSQL parameter %q is not a safe operator override", key)
		}
		value := overrides[key]
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("PostgreSQL parameter %q cannot be empty", key)
		}
		if strings.ContainsAny(value, "\r\n\x00") {
			return fmt.Errorf("PostgreSQL parameter %q must be a single line without NUL bytes", key)
		}
		if err := validateOverrideValue(key, value, settings); err != nil {
			return fmt.Errorf("invalid PostgreSQL parameter %q: %w", key, err)
		}
	}
	minValue, hasMin := overrides["min_wal_size"]
	if !hasMin {
		minValue, hasMin = settings["min_wal_size"]
	}
	maxValue, hasMax := overrides["max_wal_size"]
	if !hasMax {
		maxValue, hasMax = settings["max_wal_size"]
	}
	if hasMin && hasMax {
		minBytes, minErr := parsePostgreSQLSize(minValue)
		maxBytes, maxErr := parsePostgreSQLSize(maxValue)
		if minErr == nil && maxErr == nil && minBytes > maxBytes {
			return fmt.Errorf("PostgreSQL parameter %q must not exceed max_wal_size", "min_wal_size")
		}
	}
	for _, key := range keys {
		settings[key] = overrides[key]
	}
	return nil
}

func validateOverrideValue(key, value string, settings map[string]string) error {
	switch key {
	case "autovacuum_analyze_scale_factor", "autovacuum_vacuum_scale_factor", "checkpoint_completion_target":
		return validateFloatRange(value, 0, 1)
	case "random_page_cost", "seq_page_cost":
		return validateFloatRange(value, 0.1, 100)
	case "autovacuum_max_workers":
		maximum := int64(20)
		if configured, err := strconv.ParseInt(settings["max_worker_processes"], 10, 64); err == nil && configured-4 < maximum {
			// Reserve processes for parallel work and logical replication.
			maximum = max64(1, configured-4)
		}
		return validateIntegerRange(value, 1, maximum)
	case "autovacuum_vacuum_cost_limit":
		return validateIntegerRange(value, 1, 10_000)
	case "default_statistics_target":
		return validateIntegerRange(value, 1, 10_000)
	case "effective_io_concurrency":
		return validateIntegerRange(value, 0, 1_000)
	case "log_min_duration_statement":
		return validateIntegerRange(value, -1, 3_600_000)
	case "log_statement":
		if value != "none" && value != "ddl" && value != "mod" && value != "all" {
			return fmt.Errorf("must be one of none, ddl, mod, or all")
		}
		return nil
	case "checkpoint_timeout":
		duration, err := parsePostgreSQLDuration(value)
		if err != nil {
			return err
		}
		if duration < 30*time.Second || duration > 24*time.Hour {
			return fmt.Errorf("must be between 30s and 24h")
		}
		return nil
	case "max_wal_size":
		bytes, err := parsePostgreSQLSize(value)
		if err != nil {
			return err
		}
		if bytes < 128*mib || bytes > 1024*1024*mib {
			return fmt.Errorf("must be between 128MB and 1TB")
		}
		return nil
	case "min_wal_size":
		bytes, err := parsePostgreSQLSize(value)
		if err != nil {
			return err
		}
		if bytes < 32*mib || bytes > 1024*1024*mib {
			return fmt.Errorf("must be between 32MB and 1TB")
		}
		return nil
	default:
		return fmt.Errorf("has no value validator")
	}
}

func validateFloatRange(value string, minimum, maximum float64) error {
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return fmt.Errorf("must be a finite number")
	}
	if parsed < minimum || parsed > maximum {
		return fmt.Errorf("must be between %g and %g", minimum, maximum)
	}
	return nil
}

func validateIntegerRange(value string, minimum, maximum int64) error {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fmt.Errorf("must be an integer")
	}
	if parsed < minimum || parsed > maximum {
		return fmt.Errorf("must be between %d and %d", minimum, maximum)
	}
	return nil
}

func parsePostgreSQLDuration(value string) (time.Duration, error) {
	units := []struct {
		suffix     string
		multiplier time.Duration
	}{
		{suffix: "min", multiplier: time.Minute},
		{suffix: "ms", multiplier: time.Millisecond},
		{suffix: "s", multiplier: time.Second},
		{suffix: "h", multiplier: time.Hour},
		{suffix: "d", multiplier: 24 * time.Hour},
	}
	for _, unit := range units {
		if !strings.HasSuffix(value, unit.suffix) {
			continue
		}
		amount, err := strconv.ParseInt(strings.TrimSuffix(value, unit.suffix), 10, 64)
		if err != nil || amount <= 0 || amount > int64(math.MaxInt64/time.Duration(unit.multiplier)) {
			return 0, fmt.Errorf("must use a positive integer with ms, s, min, h, or d")
		}
		return time.Duration(amount) * unit.multiplier, nil
	}
	return 0, fmt.Errorf("must use a positive integer with ms, s, min, h, or d")
}

func parsePostgreSQLSize(value string) (int64, error) {
	units := []struct {
		suffix     string
		multiplier int64
	}{
		{suffix: "TB", multiplier: 1024 * 1024 * mib},
		{suffix: "GB", multiplier: 1024 * mib},
		{suffix: "MB", multiplier: mib},
		{suffix: "kB", multiplier: 1024},
	}
	for _, unit := range units {
		if !strings.HasSuffix(value, unit.suffix) {
			continue
		}
		number := strings.TrimSuffix(value, unit.suffix)
		parsed, err := strconv.ParseInt(number, 10, 64)
		if err != nil || parsed <= 0 || parsed > math.MaxInt64/unit.multiplier {
			return 0, fmt.Errorf("must use a positive integer with kB, MB, GB, or TB")
		}
		return parsed * unit.multiplier, nil
	}
	return 0, fmt.Errorf("must use a positive integer with kB, MB, GB, or TB")
}

func formatMiB(bytes int64) string { return strconv.FormatInt(bytes/mib, 10) + "MB" }
func resourceQuantity(value string) resource.Quantity {
	return resource.MustParse(value)
}
func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
func clamp64(v, lo, hi int64) int64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
