// Package snapshot gives the router an immutable, versioned view of the
// catalog: the effective shard map, serving primaries and table placement.
package snapshot

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// Range is one shard's slice of the int64 key space; both bounds are inclusive.
type Range struct {
	ShardID int32
	Start   int64
	End     int64
}

// ShardKey identifies a shard within a shard set.
type ShardKey struct {
	ShardSet string
	ShardID  int32
}

// Serving is the observed primary of one shard.
type Serving struct {
	PrimaryEndpoint string
	Epoch           int64
	State           string
}

// TableKey identifies a table within a logical database.
type TableKey struct {
	Database   string
	SchemaName string
	TableName  string
}

// Placement is the effective placement of a table.
type Placement struct {
	Placement  string
	ShardKey   string
	Generation int64
}

// Snapshot is an immutable view of the catalog taken in one transaction.
// Role verifiers are loaded separately (see Loader.LoadRoles) and never
// live in a Snapshot, so printing one can never leak credentials.
type Snapshot struct {
	ShardMapGeneration int64
	DesiredGeneration  int64
	ShardSets          map[string][]Range
	Serving            map[ShardKey]Serving
	Databases          map[string]catalog.Database
	Tables             map[TableKey]Placement
}

// Roles holds credential verifiers keyed by role name. Its String and
// GoString methods print only a count so a Roles can never leak into logs.
type Roles struct {
	verifiers map[string]string
}

// Verifier returns the SCRAM verifier of a role, if any.
func (r *Roles) Verifier(rolname string) (string, bool) {
	if r == nil {
		return "", false
	}
	v, ok := r.verifiers[rolname]
	return v, ok && v != ""
}

// String prints only the number of roles.
func (r *Roles) String() string { return fmt.Sprintf("roles(%d)", r.Len()) }

// GoString prints only the number of roles.
func (r *Roles) GoString() string { return r.String() }

// Len reports how many roles are held.
func (r *Roles) Len() int {
	if r == nil {
		return 0
	}
	return len(r.verifiers)
}

// ErrUnknownShardSet is returned by Locate for a shard set not in the snapshot.
var ErrUnknownShardSet = errors.New("snapshot: unknown shard set")

// ErrKeyUncovered is returned by Locate when no range owns the key.
var ErrKeyUncovered = errors.New("snapshot: keyspace id not covered by any range")

// Locate returns the shard owning keyspaceID within shardSet.
func (s *Snapshot) Locate(shardSet string, keyspaceID int64) (int32, error) {
	ranges, ok := s.ShardSets[shardSet]
	if !ok {
		return 0, fmt.Errorf("%w: %q", ErrUnknownShardSet, shardSet)
	}
	i := sort.Search(len(ranges), func(i int) bool { return ranges[i].End >= keyspaceID })
	if i == len(ranges) || ranges[i].Start > keyspaceID {
		return 0, fmt.Errorf("%w: %s/%d", ErrKeyUncovered, shardSet, keyspaceID)
	}
	return ranges[i].ShardID, nil
}

// String prints the generations and sizes only.
func (s *Snapshot) String() string {
	if s == nil {
		return "snapshot(nil)"
	}
	return fmt.Sprintf("snapshot(shard_map=%d desired=%d shard_sets=%d shards=%d databases=%d tables=%d)",
		s.ShardMapGeneration, s.DesiredGeneration, len(s.ShardSets), len(s.Serving), len(s.Databases), len(s.Tables))
}

// Generation is the shard-map generation a request was routed with.
type Generation int64

// Generation returns the shard-map generation the snapshot was routed with.
func (s *Snapshot) Generation() Generation { return Generation(s.ShardMapGeneration) }

// StaleGeneration describes a pooler response stamped with a different
// shard-map generation than the snapshot that produced the request.
type StaleGeneration struct {
	Routed, Observed Generation
}

func (e *StaleGeneration) Error() string {
	return fmt.Sprintf("snapshot: routed with shard map generation %d but pooler reported %d", e.Routed, e.Observed)
}

// CheckGeneration returns a *StaleGeneration error when observed differs
// from routed. A zero observed generation means the pooler did not report one
// and is accepted.
func CheckGeneration(routed, observed Generation) error {
	if observed == 0 || observed == routed {
		return nil
	}
	return &StaleGeneration{Routed: routed, Observed: observed}
}

func rangeFromCatalog(r catalog.ShardRange) Range {
	out := Range{ShardID: r.ShardID, Start: math.MinInt64, End: math.MaxInt64}
	if r.Lower != nil {
		out.Start = *r.Lower
	}
	if r.Upper != nil {
		out.End = *r.Upper - 1
	}
	return out
}
