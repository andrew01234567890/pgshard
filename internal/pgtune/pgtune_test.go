package pgtune

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite golden files")

var sizes = []struct {
	name string
	cpu  int64
	mem  int64
}{
	{"2c-4Gi", 2000, 4 * GiB},
	{"4c-16Gi", 4000, 16 * GiB},
	{"16c-64Gi", 16000, 64 * GiB},
}

func baseInput(cpu, mem int64, p Profile) Input {
	return Input{CPUMillicores: cpu, MemoryBytes: mem, Storage: StorageSSD, Profile: p, MaxBackends: 100, LogicalSlots: 4, Replicas: 2}
}

func TestGolden(t *testing.T) {
	for _, sz := range sizes {
		for _, p := range []Profile{ProfileOLTP, ProfileMixed, ProfileAnalytics} {
			name := sz.name + "-" + string(p)
			t.Run(name, func(t *testing.T) {
				s, err := Derive(baseInput(sz.cpu, sz.mem, p))
				if err != nil {
					t.Fatal(err)
				}
				got := s.Render()
				path := filepath.Join("testdata", name+".conf")
				if *update {
					if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
						t.Fatal(err)
					}
				}
				want, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if string(want) != got {
					t.Fatalf("golden mismatch for %s (run with -update)\n%s", path, got)
				}
			})
		}
	}
}

func TestMajor19UsesDynamicIOWorkers(t *testing.T) {
	in := baseInput(8000, 16*GiB, ProfileOLTP)
	in.Major = 19
	s, err := Derive(in)
	if err != nil {
		t.Fatal(err)
	}
	if value(s, "io_max_workers") != "4" || value(s, "io_workers") != "" {
		t.Fatalf("io_max_workers=%q io_workers=%q", value(s, "io_max_workers"), value(s, "io_workers"))
	}
	in.Major = 18
	s, _ = Derive(in)
	if value(s, "io_workers") != "4" || value(s, "io_max_workers") != "" {
		t.Fatalf("io_workers=%q io_max_workers=%q", value(s, "io_workers"), value(s, "io_max_workers"))
	}
	in.Major = 17
	if _, err := Derive(in); err == nil {
		t.Fatal("major 17 accepted")
	}
}

func TestBudgetInvariantHoldsAcrossSizes(t *testing.T) {
	for _, sz := range sizes {
		for _, backends := range []int{10, 100, 500} {
			in := baseInput(sz.cpu, sz.mem, ProfileMixed)
			in.MaxBackends = backends
			s, err := Derive(in)
			if err != nil {
				if !errors.Is(err, ErrOverCommitted) {
					t.Fatalf("%s/%d: %v", sz.name, backends, err)
				}
				continue
			}
			if err := checkBudget(s, sz.mem, 4); err != nil {
				t.Fatalf("%s/%d: %v", sz.name, backends, err)
			}
			if err := checkBudget(s, sz.mem/2, 4); err == nil {
				t.Fatalf("%s/%d: budget accepted half the memory limit", sz.name, backends)
			}
		}
	}
}

func TestImpossibleInputErrors(t *testing.T) {
	in := baseInput(2000, 1*GiB, ProfileOLTP)
	in.MaxBackends = 5000
	_, err := Derive(in)
	if !errors.Is(err, ErrOverCommitted) {
		t.Fatalf("err = %v, want ErrOverCommitted", err)
	}
}

func TestOverrideCannotOverCommit(t *testing.T) {
	in := baseInput(4000, 16*GiB, ProfileOLTP)
	in.Overrides = map[string]string{"work_mem": "1GB"}
	_, err := Derive(in)
	if !errors.Is(err, ErrOverCommitted) {
		t.Fatalf("err = %v, want ErrOverCommitted", err)
	}
	in.Overrides = map[string]string{"shared_buffers": "15GB"}
	if _, err := Derive(in); !errors.Is(err, ErrOverCommitted) {
		t.Fatalf("shared_buffers override: err = %v", err)
	}
	in.Overrides = map[string]string{"work_mem": "40MB"}
	if _, err := Derive(in); !errors.Is(err, ErrOverCommitted) {
		t.Fatalf("work_mem 40MB fits once but not four times per backend: err = %v", err)
	}
	in.Overrides = map[string]string{"work_mem": "20MB"}
	if _, err := Derive(in); err != nil {
		t.Fatalf("work_mem 20MB should fit: %v", err)
	}
}

func TestEveryMandatorySettingIsOnTheUnsafeList(t *testing.T) {
	s, err := Derive(baseInput(2000, 4*GiB, ProfileOLTP))
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range s {
		if x.Mandatory && x.Name != "synchronous_commit" {
			if _, ok := unsafeKeys[x.Name]; !ok {
				t.Errorf("mandatory %s can be overridden", x.Name)
			}
		}
	}
}

func TestSafeOverrideApplied(t *testing.T) {
	in := baseInput(4000, 16*GiB, ProfileOLTP)
	in.Overrides = map[string]string{"work_mem": "8MB", "JIT": "on", "statement_timeout": "30s", "synchronous_commit": "remote_apply"}
	s, err := Derive(in)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]Setting{}
	for _, x := range s {
		if _, dup := got[x.Name]; dup {
			t.Fatalf("duplicate setting %s", x.Name)
		}
		got[x.Name] = x
	}
	for k, v := range map[string]string{"work_mem": "8MB", "jit": "on", "statement_timeout": "30s", "synchronous_commit": "remote_apply"} {
		if got[k].Value != v || got[k].Reason != "operator override" {
			t.Fatalf("%s = %+v", k, got[k])
		}
	}
	if !got["synchronous_commit"].Mandatory {
		t.Fatal("synchronous_commit lost its mandatory flag")
	}
}

func TestUnsafeOverridesRejectedPerKey(t *testing.T) {
	cases := map[string]string{
		"fsync": "off", "full_page_writes": "off", "wal_level": "replica", "max_prepared_transactions": "0",
		"ssl": "off", "synchronous_commit": "off", "data_checksums": "off", "password_encryption": "md5",
		"track_commit_timestamp": "off", "max_replication_slots": "1", "max_wal_senders": "1",
		"idle_replication_slot_timeout": "0", "max_slot_wal_keep_size": "-1", "listen_addresses": "localhost",
		"hba_file": "/x", "synchronous_standby_names": "", "primary_conninfo": "x", "include": "x",
		"FSYNC": "on", " ssl ": "on", "synchronous_commit ": "local",
	}
	for k, v := range cases {
		in := baseInput(4000, 16*GiB, ProfileOLTP)
		in.Overrides = map[string]string{k: v}
		_, err := Derive(in)
		if !errors.Is(err, ErrUnsafeOverride) {
			t.Errorf("%s=%s: err = %v, want ErrUnsafeOverride", k, v, err)
			continue
		}
		if !strings.Contains(err.Error(), strings.ToLower(strings.TrimSpace(k))) {
			t.Errorf("%s: error %q does not name the key", k, err)
		}
	}
	if len(cases) < len(unsafeKeys)/2 {
		t.Fatalf("test covers %d keys of %d", len(cases), len(unsafeKeys))
	}
	for _, k := range UnsafeKeys() {
		if _, ok := unsafeKeys[k]; !ok {
			t.Fatalf("UnsafeKeys returned %s", k)
		}
	}
	if len(UnsafeKeys()) != len(unsafeKeys) {
		t.Fatal("UnsafeKeys is incomplete")
	}
}

func TestEveryUnsafeKeyIsRejected(t *testing.T) {
	for _, k := range UnsafeKeys() {
		in := baseInput(4000, 16*GiB, ProfileOLTP)
		in.Overrides = map[string]string{k: "on"}
		if _, err := Derive(in); !errors.Is(err, ErrUnsafeOverride) {
			t.Errorf("%s: err = %v", k, err)
		}
	}
	in := baseInput(4000, 16*GiB, ProfileOLTP)
	in.Overrides = map[string]string{"synchronous_commit": "on"}
	if _, err := Derive(in); !errors.Is(err, ErrUnsafeOverride) {
		t.Errorf("synchronous_commit=on override should be rejected as not stronger: %v", err)
	}
}

func TestMandatorySettingsAlwaysEmitted(t *testing.T) {
	want := map[string]string{
		"wal_level": "logical", "max_replication_slots": "14", "max_wal_senders": "16", "max_prepared_transactions": "108",
		"synchronous_commit": "on", "track_commit_timestamp": "on", "max_slot_wal_keep_size": "20GB",
		"idle_replication_slot_timeout": "24h", "password_encryption": "scram-sha-256", "ssl": "on",
		"standard_conforming_strings": "on",
	}
	s, err := Derive(baseInput(2000, 4*GiB, ProfileAnalytics))
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, x := range s {
		v, ok := want[x.Name]
		if !ok {
			if x.Mandatory {
				t.Errorf("%s marked mandatory unexpectedly", x.Name)
			}
			continue
		}
		seen++
		if x.Value != v || !x.Mandatory {
			t.Errorf("%s = %q mandatory=%v, want %q mandatory", x.Name, x.Value, x.Mandatory, v)
		}
	}
	if seen != len(want) {
		t.Fatalf("saw %d mandatory settings, want %d", seen, len(want))
	}
}

func TestDiskDrivesWALSizes(t *testing.T) {
	in := baseInput(4000, 16*GiB, ProfileOLTP)
	in.DiskBytes = 100 * GiB
	s, err := Derive(in)
	if err != nil {
		t.Fatal(err)
	}
	if v := value(s, "max_wal_size"); v != "10GB" {
		t.Fatalf("max_wal_size = %s", v)
	}
	if v := value(s, "max_slot_wal_keep_size"); v != "20GB" {
		t.Fatalf("max_slot_wal_keep_size = %s", v)
	}
	in.DiskBytes = 5 * GiB
	s, err = Derive(in)
	if err != nil {
		t.Fatal(err)
	}
	if value(s, "max_wal_size") != "1GB" || value(s, "max_slot_wal_keep_size") != "4GB" {
		t.Fatalf("clamps: %s %s", value(s, "max_wal_size"), value(s, "max_slot_wal_keep_size"))
	}
}

func TestStorageAndProfileBranches(t *testing.T) {
	in := baseInput(16000, 64*GiB, ProfileAnalytics)
	in.Storage = StorageHDD
	s, err := Derive(in)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range map[string]string{
		"random_page_cost": "4", "effective_io_concurrency": "2", "jit": "on", "max_parallel_workers_per_gather": "8",
		"autovacuum_vacuum_cost_limit": "2000", "log_min_duration_statement": "5000ms", "io_workers": "8",
		"autovacuum_max_workers": "8", "max_worker_processes": "32", "max_parallel_maintenance_workers": "4",
		"shared_buffers": "16GB", "maintenance_work_mem": "2GB", "work_mem": "80MB",
	} {
		if got := value(s, k); got != v {
			t.Errorf("%s = %s, want %s", k, got, v)
		}
	}
}

func TestInvalidInput(t *testing.T) {
	for name, in := range map[string]Input{
		"no memory":  {CPUMillicores: 1000, MaxBackends: 1},
		"no cpu":     {MemoryBytes: GiB, MaxBackends: 1},
		"no backend": {CPUMillicores: 1000, MemoryBytes: GiB},
		"profile":    {CPUMillicores: 1000, MemoryBytes: GiB, MaxBackends: 1, Profile: "web"},
		"storage":    {CPUMillicores: 1000, MemoryBytes: GiB, MaxBackends: 1, Storage: "nvme"},
		"negative":   {CPUMillicores: 1000, MemoryBytes: GiB, MaxBackends: 1, Replicas: -1},
	} {
		if _, err := Derive(in); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	if _, err := Derive(Input{CPUMillicores: 500, MemoryBytes: GiB, MaxBackends: 1}); err != nil {
		t.Fatalf("defaults should apply: %v", err)
	}
}

func TestRenderSortedAndQuoted(t *testing.T) {
	s := Settings{
		{Name: "zeta", Value: "it's", Reason: "r1\nline2"},
		{Name: "alpha", Value: "1.5", Reason: "r2"},
		{Name: "mid", Value: "10min", Reason: "r3"},
		{Name: "bare", Value: "on_off", Reason: "r4"},
		{Name: "empty", Value: "", Reason: "r5"},
		{Name: "list", Value: "a,b", Reason: "r6"},
	}
	want := "# Derived by pgshard; do not edit. Reasons follow each setting.\n" +
		"alpha = 1.5\t# r2\n" +
		"bare = on_off\t# r4\n" +
		"empty = ''\t# r5\n" +
		"list = 'a,b'\t# r6\n" +
		"mid = '10min'\t# r3\n" +
		"zeta = 'it''s'\t# r1 line2\n"
	if got := s.Render(); got != want {
		t.Fatalf("got\n%s\nwant\n%s", got, want)
	}
	if s[0].Name != "zeta" {
		t.Fatal("Render mutated its receiver")
	}
}

func TestQuote(t *testing.T) {
	for in, want := range map[string]string{
		"on": "on", "108": "108", "0.9": "0.9", "-1": "-1", "1e3": "1e3", "lz4": "lz4",
		"16MB": "'16MB'", "scram-sha-256": "'scram-sha-256'", "": "''", "a b": "'a b'", "x'y": "'x''y'", "9lives": "'9lives'",
	} {
		if got := Quote(in); got != want {
			t.Errorf("Quote(%q) = %s, want %s", in, got, want)
		}
	}
}

func TestParseBytesAndHuman(t *testing.T) {
	for in, want := range map[string]int64{"1GB": GiB, "16MB": 16 * MiB, "8kB": 8 * KiB, "1024": 1024, "3B": 3, "2TB": 2 * (int64(1) << 40), "1kB\t": KiB} {
		got, err := ParseBytes(in)
		if err != nil || got != want {
			t.Errorf("ParseBytes(%q) = %d, %v", in, got, err)
		}
	}
	for _, bad := range []string{"", "1xB", "GB", "1.5GB"} {
		if _, err := ParseBytes(bad); err == nil {
			t.Errorf("ParseBytes(%q) accepted", bad)
		}
	}
	for in, want := range map[int64]string{GiB: "1GB", 1536 * MiB: "1536MB", 8 * KiB: "8kB", 3: "3B", 0: "0B"} {
		if got := human(in); got != want {
			t.Errorf("human(%d) = %s, want %s", in, got, want)
		}
	}
}

func TestDerivedJSONMatchesAPI(t *testing.T) {
	s, err := Derive(baseInput(2000, 4*GiB, ProfileOLTP))
	if err != nil {
		t.Fatal(err)
	}
	d := s.Derived()
	if len(d) != len(s) {
		t.Fatalf("len %d != %d", len(d), len(s))
	}
	raw, err := json.Marshal(d[0])
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf(`{"name":%q,"value":%q,"reason":%q}`, s[0].Name, s[0].Value, s[0].Reason)
	if string(raw) != want {
		t.Fatalf("json %s, want %s", raw, want)
	}
}

func value(s Settings, name string) string {
	for _, x := range s {
		if x.Name == name {
			return x.Value
		}
	}
	return ""
}

func TestDecodingTermBreaksBudget(t *testing.T) {
	in := baseInput(4000, 16*GiB, ProfileOLTP)
	in.LogicalSlots = 4
	s, err := Derive(in)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkBudget(s, 16*GiB, 4); err != nil {
		t.Fatalf("budget must hold for the derived slot count: %v", err)
	}
	if err := checkBudget(s, 16*GiB, 4096); !errors.Is(err, ErrOverCommitted) {
		t.Fatalf("decoding × slots alone must exceed memory: err = %v", err)
	}
	if err := checkBudget(s, 16*GiB, 0); err != nil {
		t.Fatalf("dropping the decoding term must be the only difference: %v", err)
	}

	small := baseInput(2000, 1*GiB, ProfileOLTP)
	small.MaxBackends = 60
	small.LogicalSlots = 0
	if _, err := Derive(small); err != nil {
		t.Fatalf("no slots must fit: %v", err)
	}
	small.LogicalSlots = 4
	if _, err := Derive(small); !errors.Is(err, ErrOverCommitted) {
		t.Fatalf("4 slots on 1GiB must overcommit: err = %v", err)
	}
}

// TestTheReservedConnectionsAreActuallyReserved: max_connections is sized as
// the pooler's budget plus headroom for the control plane, but headroom is
// only headroom if PostgreSQL is told to keep it. Without
// superuser_reserved_connections the default is 3 and the rest goes to
// whoever asks first -- and the control plane is what asks last, since the
// resolver reaches a shard when something has already gone wrong.
//
// The property is the relationship, not the number: non-superusers must get
// exactly the pooler's budget, whatever the budget is.
func TestTheReservedConnectionsAreActuallyReserved(t *testing.T) {
	for _, backends := range []int{10, 100, 500} {
		in := baseInput(4000, 16*GiB, ProfileMixed)
		in.MaxBackends = backends
		s, err := Derive(in)
		if err != nil {
			t.Fatalf("%d backends: %v", backends, err)
		}
		get := func(name string) int {
			t.Helper()
			for _, st := range s {
				if st.Name == name {
					n, cerr := strconv.Atoi(st.Value)
					if cerr != nil {
						t.Fatalf("%s = %q: %v", name, st.Value, cerr)
					}
					return n
				}
			}
			t.Fatalf("%s is not derived", name)
			return 0
		}
		maxConns, reserved := get("max_connections"), get("superuser_reserved_connections")
		if reserved >= maxConns {
			t.Fatalf("superuser_reserved_connections %d >= max_connections %d: PostgreSQL refuses to start", reserved, maxConns)
		}
		if got := maxConns - reserved; got != backends {
			t.Fatalf("non-superusers get %d connections, want the pooler budget %d (max_connections %d - reserved %d)",
				got, backends, maxConns, reserved)
		}
	}
}
