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

// View is one row of pgshard.views: what a view projects, so the planner can
// route the view by its base table's placement instead of guessing.
type View struct {
	// Base is the single relation a simple view projects; zero for opaque.
	Base TableKey
	// Simple reports that Columns is a complete map of this view's output
	// columns to Base's, so the view can be routed. Opaque views are
	// recorded too, and refused.
	Simple bool
	// Columns maps each output column to the base column behind it. This is
	// what lets a shard key the view exposes under another name still be
	// recognised in a predicate.
	Columns map[string]string
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
	// ShardKeyChecked reports that the controller has recorded a verdict on
	// this table's shard key for the generation in force. False means the
	// inspection has not run, which is not the same as it finding nothing:
	// until it has, the key's type is unknown, and a type this router does
	// not know is one it cannot normalise the client's value to. Routing a
	// blank-padded character(n) key unnormalised sends the write to one
	// shard and every later lookup of it to another.
	ShardKeyChecked bool
	// ShardKeyError says why this table's shard key cannot be routed by,
	// when the controller's inspection of the column on the shards found a
	// type whose equality does not match its hash. Empty when the key is
	// fine, and also when the inspection has not run -- ShardKeyChecked is
	// what distinguishes those.
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
	// PGMajors is the PostgreSQL major each live shard set runs, from
	// pgshard.shard_sets.pg_major. Retired sets are left out, and so is a
	// set whose major was never stamped -- a cluster created before
	// upgrades existed says nothing here rather than claiming a default.
	// It is empty on a Partial view, which never reads it.
	PGMajors  map[string]int
	Serving   map[ShardKey]Serving
	Databases map[string]catalog.Database
	Tables    map[TableKey]Placement
	// Views are the routable views of pgshard.views, keyed the same way as
	// Tables. A view with no entry here is an undeclared relation and falls
	// to the database default placement, which for a view over a sharded
	// table is one shard's rows and no error -- so an entry, even an opaque
	// one, is what lets the router tell a view from a table it has never
	// heard of.
	Views map[TableKey]View
	// Sequences names the rows of pgshard.sequences, the global sequences
	// the router answers nextval() for.
	Sequences map[string]bool
	// ScalarFunctions names the non-built-in functions a scatter may
	// project, per database. A name is in it when pgshard.functions has a
	// scalar row for it and no aggregate row: an aggregate concatenated
	// across shards answers with one partial row per shard and no error,
	// and nothing in a parse tree tells the two apart.
	ScalarFunctions map[FunctionKey]bool
	// WriteFence is set while the cluster pauses writes for a certified
	// restore point; routers hold new writes until it clears.
	WriteFence bool
	// resharding and shardIDs cache what Resharding and ShardIDs would
	// otherwise recompute per statement. Both are derived from fields that
	// do not change after Load, so a snapshot answers them once.
	//
	// The slices in shardIDs are shared with every plan that asks for
	// them. Nothing may append to or reorder what ShardIDs returns: a plan
	// holds it for as long as it is cached, and a second plan would see
	// the first one's edit.
	resharding *bool
	shardIDs   map[string][]int32
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

// FunctionKey names a function within one database. The schema is not part
// of it: resolving an unqualified call against search_path is not something
// the router does, so a name that is a scalar in one schema and an aggregate
// in another is refused rather than guessed at.
type FunctionKey struct {
	Database string
	Name     string
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
	r := s.scanResharding()
	s.resharding = &r
	s.shardIDs = make(map[string][]int32, len(s.ShardSets))
	for set := range s.ShardSets {
		s.shardIDs[set] = s.scanShardIDs(set)
	}
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
	// The majors decide which grammar the cluster's SQL surface is, so a
	// plan made before one changed was made under a different rule.
	for _, set := range slices.Sorted(maps.Keys(s.PGMajors)) {
		str(set)
		num(int64(s.PGMajors[set]))
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
		// The planner reads all three: it refuses an unchecked or faulted
		// key and normalises the client's value by the recorded type. They
		// change without the generation moving, because the check publishes
		// its verdict on its own pass -- so a plan cached across that
		// publication is a plan made under the wrong rule.
		flag(t.ShardKeyChecked)
		str(t.ShardKeyError)
		str(t.ShardKeyType)
	}
	for _, name := range slices.Sorted(maps.Keys(s.Sequences)) {
		str(name)
		flag(s.Sequences[name])
	}
	// A declaration changes what a scatter may project, so a prepared
	// statement planned before it has to be replanned. Withdrawing one is
	// the case that matters: an operator who mis-declared an aggregate as a
	// scalar corrects it by adding the aggregate row, and every session
	// that had already prepared the scatter would otherwise go on
	// concatenating one partial answer per shard.
	for _, k := range slices.SortedFunc(maps.Keys(s.ScalarFunctions), compareFunctionKeys) {
		str(k.Database)
		str(k.Name)
		flag(s.ScalarFunctions[k])
	}
	return h.Sum64()
}

func compareShardKeys(a, b ShardKey) int {
	if c := cmp.Compare(a.ShardSet, b.ShardSet); c != 0 {
		return c
	}
	return cmp.Compare(a.ShardID, b.ShardID)
}

func compareFunctionKeys(a, b FunctionKey) int {
	if c := cmp.Compare(a.Database, b.Database); c != 0 {
		return c
	}
	return cmp.Compare(a.Name, b.Name)
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
	if s.resharding != nil {
		return *s.resharding
	}
	return s.scanResharding()
}

func (s *Snapshot) scanResharding() bool {
	for _, sv := range s.Serving {
		if sv.State == "provisioning" {
			return true
		}
	}
	return false
}

// ShardIDs is the shard ids of one shard set, ascending and without
// repeats. A plan over every shard -- a scatter, a reference write -- is
// this list, and it does not change for the life of a snapshot.
//
// The returned slice is shared. A caller that needs to change it must copy
// it first; see the note on shardIDs.
func (s *Snapshot) ShardIDs(set string) []int32 {
	if ids, ok := s.shardIDs[set]; ok {
		return ids
	}
	return s.scanShardIDs(set)
}

func (s *Snapshot) scanShardIDs(set string) []int32 {
	ranges := s.ShardSets[set]
	if len(ranges) == 0 {
		return nil
	}
	out := make([]int32, 0, len(ranges))
	for _, r := range ranges {
		out = append(out, r.ShardID)
	}
	// Sort then collapse, rather than a membership test per range: the
	// ranges of one set are one per shard today, but this runs for every
	// set on every catalog reload and a quadratic scan is a poor thing to
	// leave in the path of a cluster that grows.
	slices.Sort(out)
	return slices.Compact(out)
}

// ServingShardSet is ServingSet, or the default set when the snapshot was
// built without one.
func (s *Snapshot) ServingShardSet() string {
	if s.ServingSet == "" {
		return catalog.DefaultShardSet
	}
	return s.ServingSet
}

// ServingMajors is the PostgreSQL major of every live shard set, in
// ascending order. Sets whose major was never stamped contribute nothing:
// the answer is what the catalog knows, not a guess standing in for it.
func (s *Snapshot) ServingMajors() []int {
	if len(s.PGMajors) == 0 {
		return nil
	}
	majors := make([]int, 0, len(s.PGMajors))
	for _, m := range s.PGMajors {
		majors = append(majors, m)
	}
	slices.Sort(majors)
	return slices.Compact(majors)
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
	// catalogAccess is the roles the catalog server itself says hold a
	// control-plane role.
	catalogAccess map[string]bool
	// adminAccess is the subset of those that hold pgshard_admin rather
	// than only pgshard_reader. Reading the control plane and acting on
	// the cluster are different privileges, and pinning a session to one
	// shard is the second.
	adminAccess map[string]bool
}

// MayUseCatalog reports whether the role holds pgshard_admin or
// pgshard_reader on the catalog server -- directly, through another role,
// or by being a superuser.
func (r *Roles) MayUseCatalog(role string) bool {
	return r != nil && r.catalogAccess[role]
}

// MayAdminister reports whether the role holds pgshard_admin -- directly,
// through another role, or by being a superuser. pgshard_reader is not
// enough: a reader may read the control plane, and an administrator may act
// on the cluster.
func (r *Roles) MayAdminister(role string) bool {
	return r != nil && r.adminAccess[role]
}

// Verifier returns the SCRAM verifier of a role, if any.
func (r *Roles) Verifier(rolname string) (string, bool) {
	c, ok := r.Cred(rolname)
	return c.Verifier, ok
}

// NewRoles builds a Roles from credentials that did not come from the
// catalog, for callers that assemble one themselves.
func NewRoles(creds map[string]RoleCred) *Roles {
	return NewRolesWithCatalogAccess(creds, nil)
}

// NewRolesWithCatalogAccess is NewRoles plus the roles that may open a
// session on the catalog database.
func NewRolesWithCatalogAccess(creds map[string]RoleCred, catalogAccess []string) *Roles {
	return NewRolesWithAccess(creds, catalogAccess, nil)
}

// NewRolesWithAccess is NewRolesWithCatalogAccess plus the roles that hold
// pgshard_admin. Every administrator may also open a catalog session, so
// admins are added to both.
func NewRolesWithAccess(creds map[string]RoleCred, catalogAccess, admins []string) *Roles {
	r := &Roles{verifiers: map[string]RoleCred{}, catalogAccess: map[string]bool{}, adminAccess: map[string]bool{}}
	for name, c := range creds {
		r.verifiers[name] = c
	}
	for _, name := range catalogAccess {
		r.catalogAccess[name] = true
	}
	for _, name := range admins {
		r.adminAccess[name] = true
		r.catalogAccess[name] = true
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
