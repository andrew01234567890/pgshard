package twopc

import (
	"context"
	"slices"
	"testing"

	"google.golang.org/protobuf/proto"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// TestADecisionSurvivesTheWire: the state and every participant's own
// transaction id come back as they went, and an absent id stays absent
// rather than becoming an empty one at some other shard's position.
func TestADecisionSurvivesTheWire(t *testing.T) {
	for _, d := range []Decision{
		{GID: "pgshard-1", State: StateCommit, Participants: []Participation{{Shard: 0, XID: "10"}, {Shard: 7, XID: "20"}}},
		{GID: "pgshard-2", State: StateAbort, Participants: []Participation{{Shard: 3}}},
		{GID: "pgshard-3", State: StatePreparing, Participants: []Participation{{Shard: 0}, {Shard: 1, XID: "30"}}},
		{GID: "pgshard-4", State: StateCommit},
	} {
		wire := DecisionToProto(d)
		got := DecisionFromProto(wire)
		if got.GID != d.GID || got.State != d.State || !slices.Equal(got.Participants, d.Participants) {
			t.Errorf("round trip of %+v gave %+v", d, got)
		}
		for i, p := range d.Participants {
			if want := p.XID != ""; wire.GetParticipants()[i].Xid != nil != want {
				t.Errorf("%s shard %d: xid presence %v, want %v", d.GID, p.Shard, wire.GetParticipants()[i].Xid != nil, want)
			}
		}
	}
}

// TestAnUnreadDecisionIsNotAnAbort: an unspecified state, or one from a
// coordinator this build is older than, must leave the transaction
// prepared. Reading either as an abort rolls back one participant of a
// transaction that committed everywhere else.
func TestAnUnreadDecisionIsNotAnAbort(t *testing.T) {
	for _, wire := range []pgshardv1.TransactionDecisionState{
		pgshardv1.TransactionDecisionState_TRANSACTION_DECISION_STATE_UNSPECIFIED,
		pgshardv1.TransactionDecisionState(99),
	} {
		d := DecisionFromProto(&pgshardv1.TransactionDecision{
			Gid: "pgshard-u", State: wire,
			Participants: []*pgshardv1.TransactionParticipant{{ShardId: 0, Xid: proto.String("10")}},
		})
		if d.State.Known() {
			t.Fatalf("state %d read as the known state %q", wire, d.State)
		}
		conn := &fakeConn{prepared: []string{"pgshard-u"}}
		out, err := Reconcile(context.Background(), ConnParticipant{conn}, 0, []Decision{d})
		if err != nil {
			t.Fatal(err)
		}
		if out.Committed != 0 || out.RolledBack != 0 || len(conn.ran) != 0 {
			t.Fatalf("state %d: outcome %+v ran %v", wire, out, conn.ran)
		}
		if want := []string{"pgshard-u"}; !slices.Equal(out.Unreadable, want) {
			t.Fatalf("state %d: unreadable %v, want %v", wire, out.Unreadable, want)
		}
		if !slices.Equal(conn.prepared, []string{"pgshard-u"}) {
			t.Fatalf("state %d: no longer prepared: %v", wire, conn.prepared)
		}
		if Contradictions(map[int32]Outcome{0: out}) == nil {
			t.Fatalf("state %d: an unreadable decision did not fail the reconciliation", wire)
		}
	}
}

// TestAStateThisBuildCannotSendTravelsAsUnspecified: a state that is not
// one of the enum's keeps its meaning off the wire -- unreadable -- rather
// than being flattened onto a neighbouring one.
func TestAStateThisBuildCannotSendTravelsAsUnspecified(t *testing.T) {
	wire := DecisionToProto(Decision{GID: "pgshard-f", State: "committed"})
	if got := wire.GetState(); got != pgshardv1.TransactionDecisionState_TRANSACTION_DECISION_STATE_UNSPECIFIED {
		t.Fatalf("state %s", got)
	}
	if DecisionFromProto(wire).State.Known() {
		t.Fatal("an unsendable state arrived as a known one")
	}
}
