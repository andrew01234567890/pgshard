package twopc

import (
	"fmt"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// The wire enum and the catalog's text are one mapping, kept in one place
// so a sender and a receiver cannot disagree about what a decision means.
var stateProto = map[State]pgshardv1.TransactionDecisionState{
	StatePreparing: pgshardv1.TransactionDecisionState_TRANSACTION_DECISION_STATE_PREPARING,
	StateCommit:    pgshardv1.TransactionDecisionState_TRANSACTION_DECISION_STATE_COMMIT,
	StateAbort:     pgshardv1.TransactionDecisionState_TRANSACTION_DECISION_STATE_ABORT,
}

// StateToProto maps a state onto the wire. A state this build does not
// recognise travels as unspecified, which the receiver refuses to act on.
func StateToProto(s State) pgshardv1.TransactionDecisionState {
	return stateProto[s]
}

// StateFromProto maps a wire state back. An unspecified or unknown value
// yields a state that is not Known, so Decide leaves the transaction
// prepared instead of guessing at it.
func StateFromProto(s pgshardv1.TransactionDecisionState) State {
	for state, wire := range stateProto {
		if wire == s {
			return state
		}
	}
	return State(fmt.Sprintf("unrecognised(%d)", int32(s)))
}

// DecisionToProto converts one decision log row for the wire.
func DecisionToProto(d Decision) *pgshardv1.TransactionDecision {
	out := &pgshardv1.TransactionDecision{Gid: d.GID, State: StateToProto(d.State)}
	for _, p := range d.Participants {
		part := &pgshardv1.TransactionParticipant{ShardId: p.Shard}
		if p.XID != "" {
			part.Xid = &p.XID
		}
		out.Participants = append(out.Participants, part)
	}
	return out
}

// DecisionFromProto converts one decision log row off the wire.
func DecisionFromProto(d *pgshardv1.TransactionDecision) Decision {
	out := Decision{GID: d.GetGid(), State: StateFromProto(d.GetState())}
	for _, p := range d.GetParticipants() {
		out.Participants = append(out.Participants, Participation{Shard: p.GetShardId(), XID: p.GetXid()})
	}
	return out
}
