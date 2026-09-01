// Package snapshot gives the router an immutable, versioned view of the
// catalog: the effective shard map, serving primaries and table placement.
package snapshot

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"maps"
	"math"
	"slices"
	"sort"
	"time"

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
	// Migrating is set on a source shard while a reshard cutover fences
	// its ranges: routers hold new writes, poolers refuse new PREPAREs.
	Migrating bool
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
	// SequenceColumns are the columns of a sharded table the router fills
	// from the catalog's global sequences.
	SequenceColumns []string
	// HiddenColumns are the working columns of an in-flight rewrite
	// migration; the router never lets clients see or name them.
	HiddenColumns []string
	// VisibleColumns is the client-visible column list of a table under
	// rewrite, recorded by the applier, in attribute order. Empty until
	// the applier records it (before any hidden column exists).
	VisibleColumns []string
	// Migrating is set while a placement workflow moves the table: routers
	// hold new writes to it until the swap publishes the new placement.
	Migrating bool
	// ReferenceChecked reports that the controller has inspected this
	// reference table's shards for the generation in force. False means the
	// inspection has not run, which is not the same as finding nothing.
	ReferenceChecked bool
	// ReferenceHazards is what that inspection found: everything a shard
	// would evaluate for itself, and so differently on every shard.
	ReferenceHazards []string
	// ShardKeyError says why this table's shard key cannot be routed by,
	// when the controller's inspection of the column on the shards found a
	// type whose equality does not match its hash. Empty when the key is
	// fine, and also when the inspection has not run: an unchecked table
	// is one nothing has found fault with.
	ShardKeyError string
	// ShardKeyType is the key column's type as the shards declare it,
	// typmod included, from the same inspection that sets ShardKeyError.
	// Empty when that inspection has not run for the generation in force,
	// in which case the router hashes the value the client sent unchanged
	// -- which is what it did before any type was recorded.
	ShardKeyType string
}

// IsPartial reports a view loaded by LoadServing: the generations and the
// serving rows, and nothing else. Its Tables and Databases are empty
// because they were never read, which is indistinguishable from a cluster
// that has none -- so a partial view must never be used to plan.
func (s *Snapshot) IsPartial() bool { return s != nil && s.Partial }

// MaxAge is how old a snapshot may be before the router serving it must
// stop rather than plan against a view of the catalog it can no longer
// trust. It is one fallback reload plus a margin, so a healthy router --
// which reloads every DefaultReloadInterval -- never trips it, and a
// router whose reloads are failing stops within one interval of the last
// one that worked.
//
// The online rewrite's settle window is the same quantity, and that is the
// point: the applier publishes the visible column list, waits, and then
// adds the hidden physical column. Any router still serving after the wait
// has reloaded inside it, because one that had not would have stopped.
// Tying both to the reload interval in one place also means shortening the
// interval for latency tightens the fail-closed bound with it, instead of
// silently making rewrites unsafe.
const MaxAge = DefaultReloadInterval + 5*time.Second

// Age is how long ago this view was read. A snapshot with no load time
// (one built by hand, as tests and fixtures do) is ageless.
func (s *Snapshot) Age(now time.Time) (time.Duration, bool) {
	if s == nil || s.LoadedAt.IsZero() {
		return 0, false
	}
	return now.Sub(s.LoadedAt), true
}

// Stale reports whether this view is too old to act on.
func (s *Snapshot) Stale(now time.Time) bool {
	age, ok := s.Age(now)
	return ok && age > MaxAge
}

// Snapshot is an immutable view of the catalog taken in one transaction.
// Role verifiers are loaded separately (see Loader.LoadRoles) and never
// live in a Snapshot, so printing one can never leak credentials.
type Snapshot struct {
	// LoadedAt is when this view was read from the catalog. A router that
	// cannot reload keeps serving the last good snapshot, so age is the
	// only thing that distinguishes a current view from a stale one.
	LoadedAt time.Time
	// Partial marks a view LoadServing produced: the generations and the
	// serving rows only. Everything else is empty because it was never
	// read, not because the cluster has none of it.
	Partial            bool
	ShardMapGeneration int64
	DesiredGeneration  int64
	// ServingSet names the shard set routers route user data by.
	ServingSet string
	ShardSets  map[string][]Range
	Serving    map[ShardKey]Serving
	Databases  map[string]catalog.Database
	Tables     map[TableKey]Placement
	// Sequences names the rows of pgshard.sequences, the global sequences
	// the router answers nextval() for.
	Sequences map[string]bool
	// WriteFence is set while the cluster pauses writes for a certified
	// restore point; routers hold new writes until it clears.
	WriteFence bool
	// migrating caches Migrating for a loaded snapshot, which the router
	// consults on every write. nil means it was not computed -- a snapshot
	// built directly rather than through Load -- and the scan still runs, so
	// a construction that misses this cannot silently report false.
	migrating *bool
	// rev fingerprints everything this snapshot says about the catalog, so
	// two reloads that read the same catalog are recognisable as such. Zero
	// means it was not computed, which SamePlanning reads as "assume they
	// differ".
	rev uint64
}

// SamePlanning reports whether b says the same thing about the catalog as
// a, so a statement planned against a needs no replanning against b.
//
// The watcher swaps a freshly built snapshot in on every reload, whether or
// not the catalog moved, so comparing snapshots by identity replanned every
// prepared statement of every session on its next Bind, every reload, for
// ever. Only a snapshot that was loaded carries a fingerprint; one built by
// hand, as tests and fixtures do, compares equal only to itself.
func SamePlanning(a, b *Snapshot) bool {
	if a == b {
		return true
	}
	if a == nil || b == nil || a.rev == 0 || b.rev == 0 {
		return false
	}
	return a.rev == b.rev
}

// index precomputes what the request path would otherwise recompute per
// statement. Load calls it; the fields are immutable afterwards.
func (s *Snapshot) index() {
	m := s.scanMigrating()
	s.migrating = &m
	s.rev = s.fingerprint()
}

// fingerprint hashes everything the snapshot says about the catalog. It
// covers more than the planner reads today on purpose: a field left out
// that a plan turns out to depend on is a stale plan, while one left in
// that nothing depends on costs a replan nobody notices. Only LoadedAt is
// excluded, because it is when the catalog was read rather than what it
// said.
func (s *Snapshot) fingerprint() uint64 {
	h := fnv.New64a()
	num := func(v int64) { _ = binary.Write(h, binary.LittleEndian, v) }
	str := func(v string) { num(int64(len(v))); _, _ = io.WriteString(h, v) }
	strs := func(vs []string) {
		num(int64(len(vs)))
		for _, v := range vs {
			str(v)
		}
	}
	flag := func(v bool) {
		if v {
			num(1)
		} else {
			num(0)
		}
	}
	num(s.ShardMapGeneration)
	num(s.DesiredGeneration)
	str(s.ServingSet)
	flag(s.WriteFence)

	for _, set := range slices.Sorted(maps.Keys(s.ShardSets)) {
		str(set)
		for _, r := range s.ShardSets[set] {
			num(int64(r.ShardID))
			num(r.Start)
			num(r.End)
		}
	}
	for _, k := range slices.SortedFunc(maps.Keys(s.Serving), compareShardKeys) {
		str(k.ShardSet)
		num(int64(k.ShardID))
		v := s.Serving[k]
		str(v.PrimaryEndpoint)
		num(v.Epoch)
		str(v.State)
		flag(v.Migrating)
	}
	for _, name := range slices.Sorted(maps.Keys(s.Databases)) {
		d := s.Databases[name]
		str(d.Name)
		str(d.DefaultPlacement)
		num(int64(d.HomeShard))
		num(d.DesiredGeneration)
	}
	for _, k := range slices.SortedFunc(maps.Keys(s.Tables), compareTableKeys) {
		str(k.Database)
		str(k.SchemaName)
		str(k.TableName)
		t := s.Tables[k]
		str(t.Placement)
		str(t.ShardKey)
		num(t.Generation)
		strs(t.SequenceColumns)
		strs(t.HiddenColumns)
		strs(t.VisibleColumns)
		flag(t.Migrating)
		flag(t.ReferenceChecked)
		strs(t.ReferenceHazards)
	}
	for _, name := range slices.Sorted(maps.Keys(s.Sequences)) {
		str(name)
		flag(s.Sequences[name])
	}
	return h.Sum64()
}

func compareShardKeys(a, b ShardKey) int {
	if c := cmp.Compare(a.ShardSet, b.ShardSet); c != 0 {
		return c
	}
	return cmp.Compare(a.ShardID, b.ShardID)
}

func compareTableKeys(a, b TableKey) int {
	if c := cmp.Compare(a.Database, b.Database); c != 0 {
		return c
	}
	if c := cmp.Compare(a.SchemaName, b.SchemaName); c != 0 {
		return c
	}
	return cmp.Compare(a.TableName, b.TableName)
}

func (s *Snapshot) scanMigrating() bool {
	for k, sv := range s.Serving {
		if k.ShardSet == s.ServingShardSet() && sv.Migrating {
			return true
		}
	}
	return false
}

// Resharding reports whether a non-serving shard set is being provisioned
// or copied into: statements logical replication cannot carry (TRUNCATE)
// are refused meanwhile.
func (s *Snapshot) Resharding() bool {
	for _, sv := range s.Serving {
		if sv.State == "provisioning" {
			return true
		}
	}
	return false
}

// ServingShardSet is ServingSet, or the default set when the snapshot was
// built without one.
func (s *Snapshot) ServingShardSet() string {
	if s.ServingSet == "" {
		return catalog.DefaultShardSet
	}
	return s.ServingSet
}

// Migrating reports whether any shard of the serving set is fenced by a
// reshard cutover; routers hold new writes meanwhile. The router asks this
// for every write, so a loaded snapshot answers from a precomputed flag
// rather than walking the whole serving map per statement.
func (s *Snapshot) Migrating() bool {
	if s.migrating != nil {
		return *s.migrating
	}
	return s.scanMigrating()
}

// TableMigrating reports whether any of keys is fenced by a placement
// workflow.
func (s *Snapshot) TableMigrating(keys []TableKey) bool {
	for _, k := range keys {
		if s.Tables[k].Migrating {
			return true
		}
	}
	return false
}

// RoleCred is one role's credential and login gates.
type RoleCred struct {
	Verifier   string
	CanLogin   bool
	ValidUntil *time.Time
	// ConnectionLimit is how many sessions the role may hold open at once;
	// -1 means unlimited, matching pg_roles.rolconnlimit.
	ConnectionLimit int32
}

// Roles holds credential verifiers keyed by role name. Its String and
// GoString methods print only a count so a Roles can never leak into logs.
type Roles struct {
	verifiers map[string]RoleCred
}

// Verifier returns the SCRAM verifier of a role, if any.
func (r *Roles) Verifier(rolname string) (string, bool) {
	c, ok := r.Cred(rolname)
	return c.Verifier, ok
}

// NewRoles builds a Roles from credentials that did not come from the
// catalog, for callers that assemble one themselves.
func NewRoles(creds map[string]RoleCred) *Roles {
	r := &Roles{verifiers: map[string]RoleCred{}}
	for name, c := range creds {
		r.verifiers[name] = c
	}
	return r
}

// Cred returns the credential record of a role with a non-empty verifier.
func (r *Roles) Cred(rolname string) (RoleCred, bool) {
	if r == nil {
		return RoleCred{}, false
	}
	c, ok := r.verifiers[rolname]
	if !ok || c.Verifier == "" {
		return RoleCred{}, false
	}
	return c, true
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
