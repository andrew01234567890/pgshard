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

	// A derived value still wins, and this is the mechanism. pgtune's
	// settings do NOT arrive as parameters: the operator writes them to the
	// override file, which this file includes LAST, so whatever it names
	// beats the floor above.
	//
	// This used to be asserted by passing the derived value in as a
	// parameter -- a path the operator never takes. It sends only
	// spec.postgresql.parameters that way, and a user override of this key
	// is now refused, because it is on pgtune's unsafe list precisely for
	// being derived from the disk size (PGS-833).
	floorAt, includeAt := -1, -1
	for i, line := range strings.Split(bare, "\n") {
		switch {
		case strings.HasPrefix(line, "max_slot_wal_keep_size = "):
			floorAt = i
		case strings.HasPrefix(line, "include_if_exists = '"+overrideConf+"'"):
			includeAt = i
		}
	}
	if floorAt < 0 || includeAt < 0 {
		t.Fatalf("expected both the floor and the override include: floor=%d include=%d", floorAt, includeAt)
	}
	if includeAt < floorAt {
		t.Fatal("the override file is included before the floor, so the floor would override the derived value")
	}
	if got := setting(renderPostgresqlConf(&Config{Port: 5432,
		Postgres: PostgresSettings{Parameters: map[string]string{"max_slot_wal_keep_size": "64GB"}}}, false, false),
		"max_slot_wal_keep_size"); got != "20GB" {
		t.Fatalf("a user override of a derived key reached postgresql.conf: %q", got)
	}

	// idle_replication_slot_timeout is deliberately NOT floored: it
	// invalidates an inactive slot, and a change stream registered before
	// its consumer starts is inactive by design.
	if got := setting(bare, "idle_replication_slot_timeout"); got != "" {
		t.Fatalf("an idle-slot timeout was applied by default (%q); that discards a registered stream's position", got)
	}
}
