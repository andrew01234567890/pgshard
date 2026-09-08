package agent

import "testing"

// Each condition excludes a case where the record would be WAL that
// changes nothing: a standby cannot write WAL at all; a primary with no
// failover slot has nothing waiting on the record; and a failover slot
// with no consumer never advances its catalog_xmin however many records
// are written, because that only moves when a walsender decodes one and
// the consumer confirms past it.
func TestOnlyAPrimaryHoldingAnActiveFailoverSlotWritesRunningXacts(t *testing.T) {
	for _, c := range []struct {
		what                string
		inRecovery          bool
		activeFailoverSlots int
		want                bool
	}{
		{"a primary with an active failover slot", false, 1, true},
		{"a primary with several", false, 4, true},
		{"a primary whose failover slots have no consumer", false, 0, false},
		{"a standby, which cannot write WAL at all", true, 3, false},
		{"a standby with none", true, 0, false},
	} {
		if got := needsStandbySnapshot(c.inRecovery, c.activeFailoverSlots); got != c.want {
			t.Errorf("%s: got %t, want %t", c.what, got, c.want)
		}
	}
}
