// Package plan decides which shard(s) a statement touches from the catalog
// snapshot: unsharded tables live on the database's home shard, reference
// tables on every shard, and sharded tables on the shard owning the hash of
// their shard key. Anything the router cannot execute on one shard today is
// refused with SQLSTATE 0A000 and a message naming the missing feature.
package plan

import (
	"fmt"
	"sort"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/placement"
)

// Kind is the routing shape of a statement.
type Kind int

const (
	// Unsharded statements go to the database's home shard.
	Unsharded Kind = iota
	// EqualUnique statements pin every sharded table to one key value.
	EqualUnique
	// In statements bound the shard keys to a list of values.
	In
	// Scatter statements read one sharded table without a key predicate.
	Scatter
	// Reference statements read only reference tables.
	Reference
	// Refuse marks a statement the router will not run; Plan.Err says why.
	Refuse
	// SessionLocal statements (SET, BEGIN, SHOW, ...) run wherever the session
	// currently is and never choose a shard.
	SessionLocal
)

var kindNames = [...]string{"Unsharded", "EqualUnique", "In", "Scatter", "Reference", "Refuse", "SessionLocal"}

func (k Kind) String() string {
	if int(k) < len(kindNames) {
		return kindNames[k]
	}
	return fmt.Sprintf("Kind(%d)", int(k))
}

// DefaultShardSet is the shard set every routable database lives in.
const DefaultShardSet = "default"

// StmtClass is what the planner learned about a statement that the session
// needs to track beyond routing.
type StmtClass struct {
	// SetGUC is set for a session-level SET/RESET; GUCName is the GUC (""
	// for RESET ALL).
	SetGUC  bool
	GUCName string
	// SearchPath is the schema list a session-level SET search_path names;
	// nil with GUCName "search_path" (or "" for RESET ALL) restores the
	// session default.
	SearchPath []string
}

// Plan is the routing decision for one statement.
type Plan struct {
	Kind Kind
	// Shards are the shard ids (in DefaultShardSet) the statement touches,
	// ascending. Empty while Deferred.
	Shards []int32
	// ShardKeyValues are the resolved key values (int64 or string), one per
	// key term, in statement order.
	ShardKeyValues []any
	// Generation is the shard map generation the plan was computed against.
	Generation int64
	// Deferred means at least one shard key is a bind parameter; Resolve
	// completes the plan once the parameters are known.
	Deferred bool
	Class    StmtClass
	// Err carries the refusal for Kind Refuse.
	Err error

	terms    []keyTerm
	multiRow bool
	touches  Kind
	home     int32
	set      string
	snap     *snapshot.Snapshot
}

// Session describes the connection the statement runs in.
type Session struct {
	Database  string
	HomeShard int32
	// SearchPath lists the schemas an unqualified table name is looked up
	// in; nil means {"public"}.
	SearchPath []string
	// ID spreads reference-table reads over shards.
	ID       uint64
	Snapshot *snapshot.Snapshot
}

// TypeHint is what the statement text says about a shard key parameter's
// type: an explicit cast such as $1::int8 or $1::text.
type TypeHint int

const (
	// HintNone leaves the type to the Bind message.
	HintNone TypeHint = iota
	// HintInt means the parameter was cast to an integer type.
	HintInt
	// HintText means the parameter was cast to a text type.
	HintText
)

// Params supplies bind-parameter values ($1 is index 1) to Resolve.
type Params interface {
	// ShardKey returns the value of parameter n as an int64 or a string.
	ShardKey(n int32, hint TypeHint) (any, error)
}

// ParamRef is a bind parameter used as a shard key.
type ParamRef struct {
	Number int32
	Hint   TypeHint
}

// keyTerm is one shard key predicate: the table's key equals one of the
// values (a constant) or one of the parameters.
type keyTerm struct {
	values []any
	params []ParamRef
	// list marks an IN list (Kind In) as opposed to a plain equality.
	list bool
}

// Resolve completes a deferred plan with parameter values. It is also safe
// on a plan that is not deferred and returns it unchanged.
func (p Plan) Resolve(params Params) (Plan, error) {
	if !p.Deferred {
		return p, nil
	}
	values := make([][]any, len(p.terms))
	for i, t := range p.terms {
		vals := append([]any(nil), t.values...)
		for _, ref := range t.params {
			v, err := params.ShardKey(ref.Number, ref.Hint)
			if err != nil {
				return refusal(fmt.Errorf("parameter $%d cannot be a shard key: %w", ref.Number, err),
					"shard keys are int8 or text values; cast an untyped parameter ($1::int8 or $1::text)")
			}
			vals = append(vals, v)
		}
		values[i] = vals
	}
	out := p
	out.Deferred = false
	if err := out.finish(values); err != nil {
		return refusalErr(err)
	}
	return out, nil
}

// finish turns per-term values into shards.
func (p *Plan) finish(values [][]any) error {
	var shards []int32
	p.ShardKeyValues = nil
	for _, vals := range values {
		var termShards []int32
		for _, v := range vals {
			id, err := placement.KeyspaceID(v)
			if err != nil {
				return pgwire.Errorf(pgwire.CodeFeatureNotSupported, "%v", err)
			}
			sh, err := p.snap.Locate(p.set, id)
			if err != nil {
				return pgwire.Errorf("57P03", "%v", err)
			}
			termShards = appendUnique(termShards, sh)
			p.ShardKeyValues = append(p.ShardKeyValues, v)
		}
		if shards == nil {
			shards = termShards
			continue
		}
		if !sameShards(shards, termShards) {
			if p.multiRow {
				return notYet("multi-row INSERT spanning shards is not available yet", "insert rows for one shard key value per statement")
			}
			return notYet("cross-shard join is not available yet", "join sharded tables only on equal shard keys")
		}
	}
	if p.touches == Unsharded && !sameShards(shards, []int32{p.home}) {
		return notYet("cross-shard join is not available yet", "unsharded tables live on the home shard; join them only with rows of that shard")
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i] < shards[j] })
	p.Shards = shards
	return nil
}

func appendUnique(s []int32, v int32) []int32 {
	for _, x := range s {
		if x == v {
			return s
		}
	}
	return append(s, v)
}

func sameShards(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for _, x := range a {
		found := false
		for _, y := range b {
			if x == y {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// notYet builds the 0A000 refusal every unsupported shape reports.
func notYet(msg, hint string) *pgwire.Error {
	err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "%s", msg)
	err.Hint = hint
	return err
}

func refuse(msg, hint string) (Plan, error) {
	err := notYet(msg, hint)
	return Plan{Kind: Refuse, Err: err}, err
}

func refusal(err error, hint string) (Plan, error) {
	e := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "%v", err)
	e.Hint = hint
	return Plan{Kind: Refuse, Err: e}, e
}

func refusalErr(err error) (Plan, error) { return Plan{Kind: Refuse, Err: err}, err }
