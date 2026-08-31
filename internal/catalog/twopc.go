package catalog

import "time"

// DecisionHeartbeatInterval is how often a coordinator marks its preparing
// decision row alive, and MinPreparingTimeout is the shortest a resolver
// may wait before it calls that coordinator dead: MinPreparingBeats of
// them.
//
// The two live here, together, because they are one invariant split across
// two processes: the router beats, the controller's resolver decides that
// beats have stopped. Held apart as independent constants, a resolver
// timeout set within a beat or two of the interval aborts live coordinators
// whose beat was merely late -- safe, but a transaction the client was
// promised nothing had gone wrong with. The floor spans several missed
// beats, so only a coordinator that has genuinely stopped ages out.
const (
	DecisionHeartbeatInterval = 2 * time.Second
	MinPreparingBeats         = 4
	MinPreparingTimeout       = MinPreparingBeats * DecisionHeartbeatInterval
)
