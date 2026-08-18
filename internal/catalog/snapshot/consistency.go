package snapshot

import "sort"

// Consistency describes whether a shard set's effective map is stable.
type Consistency int

const (
	// Unknown means no snapshot has been observed yet.
	Unknown Consistency = iota
	// Consistent means every shard of the set is serving.
	Consistent
	// Inconsistent means at least one shard is migrating or fenced.
	Inconsistent
)

func (c Consistency) String() string {
	switch c {
	case Consistent:
		return "consistent"
	case Inconsistent:
		return "inconsistent"
	}
	return "unknown"
}

// Transition is one change of a shard set's consistency.
type Transition struct {
	ShardSet string
	From, To Consistency
	Blocking []ShardKey
}

// ConsistencyWatcher tracks the consistency of shard sets across snapshots.
type ConsistencyWatcher struct {
	state map[string]Consistency
}

// NewConsistencyWatcher returns an empty watcher.
func NewConsistencyWatcher() *ConsistencyWatcher {
	return &ConsistencyWatcher{state: map[string]Consistency{}}
}

// Blocking lists the shards of shardSet whose serving_state prevents a
// consistent view: migrating, fenced, or missing from shard_status.
func Blocking(s *Snapshot, shardSet string) []ShardKey {
	var out []ShardKey
	for _, r := range s.ShardSets[shardSet] {
		key := ShardKey{shardSet, r.ShardID}
		sv, ok := s.Serving[key]
		if !ok || sv.State == "migrating" || sv.State == "fenced" {
			out = append(out, key)
		}
	}
	return out
}

// State returns the last observed consistency of a shard set.
func (w *ConsistencyWatcher) State(shardSet string) Consistency { return w.state[shardSet] }

// Observe evaluates a snapshot and returns the transitions it caused, in
// shard-set name order.
func (w *ConsistencyWatcher) Observe(s *Snapshot) []Transition {
	var out []Transition
	for _, name := range sortedKeys(s.ShardSets) {
		blocking := Blocking(s, name)
		next := Consistent
		if len(blocking) > 0 {
			next = Inconsistent
		}
		if prev := w.state[name]; prev != next {
			out = append(out, Transition{ShardSet: name, From: prev, To: next, Blocking: blocking})
			w.state[name] = next
		}
	}
	return out
}

func sortedKeys(m map[string][]Range) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
