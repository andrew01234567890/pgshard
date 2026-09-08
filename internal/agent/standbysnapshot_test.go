package agent

import "testing"

// The record is WAL, so only a primary can write one, and only a primary
// holding a failover slot has anything waiting on it. Writing them
// anywhere else is WAL for nothing -- on an otherwise idle cluster it is
// the only WAL there is, and it would keep archive_timeout busy for no
// reader.
func TestOnlyAPrimaryHoldingFailoverSlotsWritesRunningXacts(t *testing.T) {
	for _, c := range []struct {
		what          string
		inRecovery    bool
		failoverSlots int
		want          bool
	}{
		{"a primary with a failover slot", false, 1, true},
		{"a primary with several", false, 4, true},
		{"a primary with none", false, 0, false},
		{"a standby, which cannot write WAL at all", true, 3, false},
		{"a standby with none", true, 0, false},
	} {
		if got := needsStandbySnapshot(c.inRecovery, c.failoverSlots); got != c.want {
			t.Errorf("%s: got %t, want %t", c.what, got, c.want)
		}
	}
}
