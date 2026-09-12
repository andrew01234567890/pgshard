package agent

import (
	"strings"
	"testing"
)

// TestSlotRetentionHasAFloorWithoutTuning.
//
// pgtune derives max_slot_wal_keep_size from the disk size, but Tuning
// returns NOTHING when spec.resources names no memory -- "without a budget
// nothing can be derived and the agent's fixed configuration stands alone".
// So an unbudgeted cluster got PostgreSQL's default of -1, unlimited
// retention, and a slot left for a member that no longer exists pinned WAL
// on a PRIMARY until pg_wal filled the disk. A failover that leaves a slot
// behind is enough; nothing else has to go wrong.
func TestSlotRetentionHasAFloorWithoutTuning(t *testing.T) {
	setting := func(conf, name string) string {
		t.Helper()
		for _, line := range strings.Split(conf, "\n") {
			if k, v, ok := strings.Cut(line, " = "); ok && strings.TrimSpace(k) == name {
				return strings.Trim(strings.TrimSpace(v), "'")
			}
		}
		return ""
	}

	// No tuning at all: the floor applies.
	bare := renderPostgresqlConf(&Config{Port: 5432}, false, false)
	if got := setting(bare, "max_slot_wal_keep_size"); got != "20GB" {
		t.Fatalf("an untuned cluster retains WAL without bound: max_slot_wal_keep_size = %q", got)
	}

	// A derived value wins: the floor must not override tuning, which knows
	// the disk size and this does not.
	tuned := renderPostgresqlConf(&Config{Port: 5432,
		Postgres: PostgresSettings{Parameters: map[string]string{"max_slot_wal_keep_size": "64GB"}}}, false, false)
	if got := setting(tuned, "max_slot_wal_keep_size"); got != "64GB" {
		t.Fatalf("the floor overrode a derived value: %q", got)
	}

	// idle_replication_slot_timeout is deliberately NOT floored: it
	// invalidates an inactive slot, and a change stream registered before
	// its consumer starts is inactive by design.
	if got := setting(bare, "idle_replication_slot_timeout"); got != "" {
		t.Fatalf("an idle-slot timeout was applied by default (%q); that discards a registered stream's position", got)
	}
}
