package agent

import (
	"slices"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/twopc"
)

// TestAParticipantKeepsItsOwnTransactionID: the catalog stores participants
// and their transaction ids as two arrays paired by position, and this is
// the only place that pairing is read.
func TestAParticipantKeepsItsOwnTransactionID(t *testing.T) {
	cases := []struct {
		name string
		row  decisionRow
		want []twopc.Participation
	}{
		{"paired", decisionRow{Participants: []int32{0, 4}, XIDs: []string{"10", "20"}},
			[]twopc.Participation{{Shard: 0, XID: "10"}, {Shard: 4, XID: "20"}}},
		{"no ids recorded", decisionRow{Participants: []int32{0, 4}},
			[]twopc.Participation{{Shard: 0}, {Shard: 4}}},
		// Both arrays are written by one statement, so a length that does
		// not match means the row is not what it claims to be. Every id is
		// dropped rather than shifted onto the wrong shard: an unproven
		// commit fails the restore, a misattributed one corrupts it.
		{"short", decisionRow{Participants: []int32{0, 4}, XIDs: []string{"10"}},
			[]twopc.Participation{{Shard: 0}, {Shard: 4}}},
		{"long", decisionRow{Participants: []int32{0}, XIDs: []string{"10", "20"}},
			[]twopc.Participation{{Shard: 0}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := c.row.decision()
			if !slices.Equal(got.Participants, c.want) {
				t.Fatalf("participants %+v, want %+v", got.Participants, c.want)
			}
		})
	}
	if got := (decisionRow{State: "commit"}).decision().State; got != twopc.StateCommit {
		t.Fatalf("state %q", got)
	}
	if (decisionRow{State: "committed"}).decision().State.Known() {
		t.Fatal("a state the catalog should not hold was read as a known one")
	}
}
