// Package pgtune derives a PostgreSQL configuration from container resources
// and a workload profile, guarded by a memory-budget invariant.
package pgtune

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

// Binary byte units.
const (
	KiB = int64(1) << 10
	MiB = int64(1) << 20
	GiB = int64(1) << 30
)

// Profile selects the workload the settings favour.
type Profile string

// Supported profiles.
const (
	ProfileOLTP      Profile = "oltp"
	ProfileMixed     Profile = "mixed"
	ProfileAnalytics Profile = "analytics"
)

// Storage is the storage-class hint behind PGDATA.
type Storage string

// Supported storage hints.
const (
	StorageSSD     Storage = "ssd"
	StorageHDD     Storage = "hdd"
	StorageUnknown Storage = "unknown"
)

// Input describes the resources and expectations a shard member runs with.
type Input struct {
	// Major is the PostgreSQL major version (18 or 19); 0 means 18.
	Major         int
	CPUMillicores int64
	MemoryBytes   int64
	DiskBytes     int64
	Storage       Storage
	Profile       Profile
	// MaxBackends is the pooler's total server-connection budget across roles.
	MaxBackends  int
	LogicalSlots int
	Replicas     int
	// Overrides are applied last; keys on the unsafe list are rejected.
	Overrides map[string]string
}

// Setting is one derived GUC with the reason it holds its value.
type Setting struct {
	Name      string
	Value     string
	Reason    string
	Mandatory bool
}

// Settings is an ordered, name-unique list of derived settings.
type Settings []Setting

const (
	overheadBytes         = 256 * MiB
	sharedBuffersCap      = 16 * GiB
	maintenanceCap        = 2 * GiB
	logicalDecodingBudget = 64 * MiB
	walBuffers            = 16 * MiB
	workMemFloor          = 1 * MiB
	reservedConnections   = 8
	minMemory             = 512 * MiB
)

// ErrOverCommitted reports that the memory-budget invariant cannot be met.
var ErrOverCommitted = errors.New("pgtune: memory budget over-committed")

// ErrUnsafeOverride reports an override that would weaken durability or security.
var ErrUnsafeOverride = errors.New("pgtune: unsafe override")

// syncWorkersPerSub bounds the table-sync workers one subscription may take,
// so a single subscription cannot starve the others of the shared pool.
const syncWorkersPerSub = 2

// Derive computes the ordered settings for in or returns an error when the
// input is invalid, an override is unsafe, or the memory budget cannot hold.
func Derive(in Input) (Settings, error) {
	if err := validate(&in); err != nil {
		return nil, err
	}
	cpu := in.CPUMillicores / 1000
	if cpu < 1 {
		cpu = 1
	}
	mem := in.MemoryBytes
	maxConns := int64(in.MaxBackends + reservedConnections)
	autovacWorkers := clamp(cpu/2, 3, 10)

	sharedBuffers := alignMiB(min64(mem/4, sharedBuffersCap))
	maintenance := alignMiB(min64(maintenanceCap, mem/16))
	slots := int64(in.LogicalSlots)
	ldwm := logicalDecodingBudget
	if slots > 0 && ldwm*slots > mem/8 {
		ldwm = alignMiB(mem / 8 / slots)
	}
	fixed := sharedBuffers + maintenance*autovacWorkers + ldwm*slots + overheadBytes
	backendMem := mem - fixed
	workMem := backendMem / (int64(in.MaxBackends) * 4)
	workMemCap := 64 * MiB
	if in.Profile == ProfileAnalytics {
		workMemCap = 256 * MiB
	}
	workMem = alignMiB(min64(workMem, workMemCap))
	if workMem < workMemFloor {
		return nil, fmt.Errorf("%w: %s remain for %d backends after shared_buffers=%s maintenance=%s×%d decoding=%s×%d overhead=%s (work_mem would be below %s)",
			ErrOverCommitted, human(backendMem), in.MaxBackends, human(sharedBuffers), human(maintenance), autovacWorkers, human(ldwm), slots, human(overheadBytes), human(workMemFloor))
	}

	var s Settings
	add := func(name, value, reason string) { s = append(s, Setting{Name: name, Value: value, Reason: reason}) }
	must := func(name, value, reason string) {
		s = append(s, Setting{Name: name, Value: value, Reason: reason, Mandatory: true})
	}

	add("max_connections", itoa(maxConns), fmt.Sprintf("pooler backend budget %d plus %d reserved for superuser and the agent", in.MaxBackends, reservedConnections))
	add("shared_buffers", human(sharedBuffers), "25% of the memory limit, capped at 16GiB")
	add("effective_cache_size", human(alignMiB(mem*3/4)), "75% of the memory limit; the kernel page cache is counted")
	add("work_mem", human(workMem), fmt.Sprintf("(memory - shared_buffers - maintenance - decoding - overhead) / (%d backends × 4 sorts)", in.MaxBackends))
	add("maintenance_work_mem", human(maintenance), "min(2GiB, memory/16)")
	add("autovacuum_work_mem", human(maintenance), "same budget as maintenance_work_mem, one per autovacuum worker")
	add("logical_decoding_work_mem", human(ldwm), fmt.Sprintf("64MiB per decoding slot, budgeted for %d slots within memory/8", slots))
	add("wal_buffers", human(walBuffers), "16MiB is the ceiling PostgreSQL benefits from")
	add("huge_pages", "try", "use huge pages when the node offers them, never fail startup")

	add("io_method", "worker", "PostgreSQL 18 asynchronous I/O through worker processes")
	if in.Major >= 19 {
		add("io_max_workers", itoa(clamp(cpu/2, 3, 32)), "cpu/2 clamped to [3,32]; PostgreSQL 19 sizes the worker pool dynamically up to this")
	} else {
		add("io_workers", itoa(clamp(cpu/2, 3, 32)), "cpu/2 clamped to [3,32]")
	}
	switch in.Storage {
	case StorageHDD:
		add("effective_io_concurrency", "2", "spinning disks serve few concurrent requests")
		add("random_page_cost", "4", "hdd storage class: seeks are expensive")
	default:
		add("effective_io_concurrency", "200", "ssd (or unknown) storage class serves many concurrent requests")
		add("random_page_cost", "1.1", "ssd (or unknown) storage class: random reads cost almost as little as sequential")
	}

	maxWAL, walReason := 4*GiB, "default when the disk size is unknown"
	slotKeep, slotReason := 20*GiB, "default when the disk size is unknown"
	if in.DiskBytes > 0 {
		maxWAL = alignGiB(clamp(in.DiskBytes/10, 1*GiB, 64*GiB))
		walReason = "10% of the disk, clamped to [1GiB,64GiB]"
		slotKeep = alignGiB(clamp(in.DiskBytes/5, 4*GiB, 200*GiB))
		slotReason = "20% of the disk, clamped to [4GiB,200GiB]"
	}
	add("max_wal_size", human(maxWAL), walReason)
	add("min_wal_size", human(1*GiB), "keep recycled segments to avoid churn")
	add("checkpoint_completion_target", "0.9", "spread checkpoint writes over the interval")
	add("wal_compression", "zstd", "cheap CPU for smaller full-page images")

	add("autovacuum_max_workers", itoa(autovacWorkers), "cpu/2 clamped to [3,10]")
	if in.Profile == ProfileOLTP {
		add("autovacuum_vacuum_cost_limit", "1000", "oltp: vacuum steadily without starving foreground work")
	} else {
		add("autovacuum_vacuum_cost_limit", "2000", "mixed/analytics: vacuum aggressively between batches")
	}
	add("autovacuum_naptime", "15s", "wake often so small hot tables are vacuumed promptly")

	add("max_worker_processes", itoa(max64(max64(8, cpu*2), int64(in.LogicalSlots)*(1+syncWorkersPerSub)+12)),
		"max(8, cpu×2) but never below the logical replication workers plus room for parallel workers and extensions")
	add("max_parallel_workers", itoa(cpu), "one parallel worker per core")
	switch in.Profile {
	case ProfileAnalytics:
		add("max_parallel_workers_per_gather", itoa(max64(2, cpu/2)), "analytics: cpu/2 workers per gather")
	default:
		add("max_parallel_workers_per_gather", "2", "oltp/mixed: cap parallelism per query")
	}
	add("max_parallel_maintenance_workers", itoa(max64(1, cpu/4)), "cpu/4 for index builds and vacuum")
	if in.Profile == ProfileAnalytics {
		add("jit", "on", "analytics: long queries amortise compilation")
	} else {
		add("jit", "off", "oltp/mixed: compilation latency dominates short queries")
	}
	add("default_toast_compression", "lz4", "faster than pglz for the same class of data")

	add("log_checkpoints", "on", "checkpoint timing is the first thing to look at under I/O pressure")
	add("log_lock_waits", "on", "surface waits past deadlock_timeout")
	if in.Profile == ProfileOLTP {
		add("log_min_duration_statement", "1000ms", "oltp: a second is slow")
	} else {
		add("log_min_duration_statement", "5000ms", "mixed/analytics: batch queries are expected to run long")
	}
	add("shared_preload_libraries", "pg_stat_statements", "query statistics for the admin CLI")
	add("idle_in_transaction_session_timeout", "10min", "idle transactions pin xmin and hold locks")

	repl := int64(in.Replicas) + slots + 8
	must("wal_level", "logical", "logical decoding is required for resharding")
	must("max_replication_slots", itoa(repl), fmt.Sprintf("replicas %d + logical slots %d + 8 headroom", in.Replicas, slots))
	must("max_wal_senders", itoa(repl+2), "max_replication_slots + 2 for base backups")
	must("max_prepared_transactions", itoa(maxConns), "one prepared transaction per connection for two-phase commit")
	must("synchronous_commit", "on", "durability floor; the agent raises it via synchronous_standby_names")
	must("track_commit_timestamp", "on", "commit timestamps are needed for conflict resolution")
	// A reshard target subscribes to every source it takes range from, and
	// each subscription holds an apply worker for as long as it exists.
	// Table sync needs a worker on top of that, from the same pool. A merge
	// is where this bites: N sources into one target means N apply workers,
	// so with PostgreSQL's default of 4 the apply workers alone can exhaust
	// the pool and no table ever finishes its initial sync -- the copy stops
	// with no error, no lag and no retry. A split has one subscription per
	// target and so never noticed.
	logicalWorkers := max64(8, slots*(1+syncWorkersPerSub)+4)
	add("max_sync_workers_per_subscription", itoa(syncWorkersPerSub), "bounded so one subscription cannot take the whole worker pool")
	add("max_logical_replication_workers", itoa(logicalWorkers),
		fmt.Sprintf("%d subscriptions x (1 apply + %d sync) + 4 headroom", slots, syncWorkersPerSub))
	must("max_slot_wal_keep_size", human(slotKeep), slotReason)
	must("idle_replication_slot_timeout", "24h", "drop abandoned slots before they fill the disk")
	must("password_encryption", "scram-sha-256", "no md5 passwords")
	must("ssl", "on", "TLS between every process")

	if err := applyOverrides(&s, in.Overrides); err != nil {
		return nil, err
	}
	if err := checkBudget(s, mem, int64(in.LogicalSlots)); err != nil {
		return nil, err
	}
	return s, nil
}

func validate(in *Input) error {
	if in.MemoryBytes < minMemory {
		return fmt.Errorf("pgtune: memory %s below the %s minimum", human(in.MemoryBytes), human(minMemory))
	}
	if in.CPUMillicores <= 0 {
		return errors.New("pgtune: cpu must be positive")
	}
	if in.MaxBackends <= 0 {
		return errors.New("pgtune: max backends must be positive")
	}
	if in.LogicalSlots < 0 || in.Replicas < 0 || in.DiskBytes < 0 {
		return errors.New("pgtune: slots, replicas and disk must not be negative")
	}
	switch in.Profile {
	case ProfileOLTP, ProfileMixed, ProfileAnalytics:
	case "":
		in.Profile = ProfileOLTP
	default:
		return fmt.Errorf("pgtune: unknown profile %q", in.Profile)
	}
	switch in.Major {
	case 18, 19:
	case 0:
		in.Major = 18
	default:
		return fmt.Errorf("pgtune: unsupported PostgreSQL major %d", in.Major)
	}
	switch in.Storage {
	case StorageSSD, StorageHDD, StorageUnknown:
	case "":
		in.Storage = StorageUnknown
	default:
		return fmt.Errorf("pgtune: unknown storage class %q", in.Storage)
	}
	return nil
}

// checkBudget enforces the invariant on the final settings, after overrides:
// shared_buffers + max_backends×work_mem×4 + maintenance×autovacuum workers
// + decoding×slots + overhead <= memory.
func checkBudget(s Settings, mem, slots int64) error {
	get := func(name string) string {
		for _, x := range s {
			if x.Name == name {
				return x.Value
			}
		}
		return ""
	}
	sb, err1 := ParseBytes(get("shared_buffers"))
	wm, err2 := ParseBytes(get("work_mem"))
	mw, err3 := ParseBytes(get("maintenance_work_mem"))
	ld, err4 := ParseBytes(get("logical_decoding_work_mem"))
	conns, err5 := strconv.ParseInt(get("max_connections"), 10, 64)
	workers, err6 := strconv.ParseInt(get("autovacuum_max_workers"), 10, 64)
	if err := errors.Join(err1, err2, err3, err4, err5, err6); err != nil {
		return fmt.Errorf("pgtune: budget inputs: %w", err)
	}
	backends := conns - reservedConnections
	total := sb + backends*wm*4 + mw*workers + ld*slots + overheadBytes
	if total > mem {
		return fmt.Errorf("%w: shared_buffers %s + %d backends × work_mem %s × 4 + maintenance %s × %d + decoding %s × %d + overhead %s = %s > %s",
			ErrOverCommitted, human(sb), backends, human(wm), human(mw), workers, human(ld), slots, human(overheadBytes), human(total), human(mem))
	}
	return nil
}

func clamp(v, lo, hi int64) int64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
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

func alignMiB(b int64) int64 { return b / MiB * MiB }
func alignGiB(b int64) int64 { return b / GiB * GiB }
func itoa(v int64) string    { return strconv.FormatInt(v, 10) }

// human renders bytes in the largest binary unit that divides evenly, using
// PostgreSQL's unit names.
func human(b int64) string {
	switch {
	case b >= GiB && b%GiB == 0:
		return itoa(b/GiB) + "GB"
	case b >= MiB && b%MiB == 0:
		return itoa(b/MiB) + "MB"
	case b >= KiB && b%KiB == 0:
		return itoa(b/KiB) + "kB"
	}
	return itoa(b) + "B"
}

// ParseBytes parses a PostgreSQL memory value ("512MB", "1GB", "8kB", "1024").
func ParseBytes(v string) (int64, error) {
	v = strings.TrimSpace(v)
	units := []struct {
		suffix string
		mul    int64
	}{{"TB", 1 << 40}, {"GB", GiB}, {"MB", MiB}, {"kB", KiB}, {"B", 1}}
	for _, u := range units {
		if strings.HasSuffix(v, u.suffix) {
			n, err := strconv.ParseInt(strings.TrimSpace(strings.TrimSuffix(v, u.suffix)), 10, 64)
			if err != nil {
				return 0, fmt.Errorf("pgtune: bad memory value %q", v)
			}
			return n * u.mul, nil
		}
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("pgtune: bad memory value %q", v)
	}
	return n, nil
}

// Derived converts the settings into the API status form.
func (s Settings) Derived() []pgshardv1alpha1.DerivedSetting {
	out := make([]pgshardv1alpha1.DerivedSetting, 0, len(s))
	for _, x := range s {
		out = append(out, pgshardv1alpha1.DerivedSetting{Name: x.Name, Value: x.Value, Reason: x.Reason})
	}
	return out
}

// Render writes the settings as postgresql.conf text, sorted by name, with
// each value quoted the way PostgreSQL's parser expects.
func (s Settings) Render() string {
	sorted := append(Settings(nil), s...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	var b strings.Builder
	b.WriteString("# Derived by pgshard; do not edit. Reasons follow each setting.\n")
	for _, x := range sorted {
		fmt.Fprintf(&b, "%s = %s\t# %s\n", x.Name, Quote(x.Value), strings.ReplaceAll(x.Reason, "\n", " "))
	}
	return b.String()
}

// Quote returns v as a postgresql.conf value: bare when it is a number or a
// plain identifier-like token, single-quoted with doubled quotes otherwise.
func Quote(v string) string {
	if v != "" && isBareToken(v) {
		return v
	}
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}

func isBareToken(v string) bool {
	if _, err := strconv.ParseFloat(v, 64); err == nil {
		return true
	}
	for i, r := range v {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		case (r >= '0' && r <= '9') && i > 0:
		default:
			return false
		}
	}
	return true
}
