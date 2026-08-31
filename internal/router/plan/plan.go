// Package plan decides which shard(s) a statement touches from the catalog
// snapshot: unsharded tables live on the database's home shard, reference
// tables on every shard, and sharded tables on the shard owning the hash of
// their shard key. Anything the router cannot execute on one shard today is
// refused with SQLSTATE 0A000 and a message naming the missing feature.
package plan

import (
	"fmt"
	"slices"

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
	// MigrationKind statements are DDL/DCL the router queues as a migration
	// for the controller to apply on every target shard; Plan.Migration
	// says how.
	MigrationKind
)

var kindNames = [...]string{"Unsharded", "EqualUnique", "In", "Scatter", "Reference", "Refuse", "SessionLocal", "Migration"}

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
	// for RESET ALL) and GUCValue its first literal argument, when any.
	SetGUC   bool
	GUCName  string
	GUCValue string
	// SearchPath is the schema list a session-level SET search_path names;
	// nil with GUCName "search_path" (or "" for RESET ALL) restores the
	// session default.
	SearchPath []string
	// Write is set for every statement that is not a plain read: DML, DDL,
	// COPY, SELECT with a locking clause and anything the router does not
	// recognise. Session-local statements are never writes.
	Write bool
	// Txn is the transaction control statement this is, if any.
	Txn TxnKind
	// Chain marks COMMIT/ROLLBACK AND CHAIN.
	Chain bool
	// Savepoint names the savepoint of a SAVEPOINT, RELEASE or ROLLBACK TO.
	Savepoint string
	// Session is the session-state statement this is, if any.
	Session SessionKind
	// SessionName is the statement name of an SQL-level PREPARE or
	// DEALLOCATE; empty for DEALLOCATE ALL.
	SessionName string
}

// SessionKind classifies statements that create, drop or reset session
// state the router replays.
type SessionKind int

// Session-state statement kinds.
const (
	SessionNone SessionKind = iota
	// SessionPrepare is an SQL-level PREPARE name AS ....
	SessionPrepare
	// SessionDeallocate is DEALLOCATE name or DEALLOCATE ALL.
	SessionDeallocate
	// SessionDiscardAll is DISCARD ALL, which drops every setting and
	// prepared statement; DISCARD PLANS/SEQUENCES/TEMP are plain forwards.
	SessionDiscardAll
)

// TxnKind classifies transaction control statements.
type TxnKind int

// Transaction control statement kinds.
const (
	TxnNone TxnKind = iota
	TxnBegin
	TxnCommit
	TxnRollback
	TxnSavepoint
	TxnRelease
	TxnRollbackTo
)

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
	// Sequences is set for an INSERT into a sharded table whose registered
	// sequence columns the router fills; the executor runs Sequences.SQL
	// with the allocated values bound as extra parameters.
	Sequences *SequenceFill
	// NextVal names the global sequence a `SELECT nextval('...')` reads;
	// the router answers it without visiting a shard (Kind SessionLocal).
	NextVal string
	// Migration describes a DDL/DCL statement of Kind MigrationKind.
	Migration *Migration
	// Rewritten, when set, is the statement text the shards run instead of
	// the client's: stars and column lists expanded so the working column
	// of an online rewrite migration stays invisible.
	Rewritten string
	// Tables are the catalog tables the statement resolved, in statement
	// order; the executor holds writes while one of them is migrating.
	Tables []snapshot.TableKey

	// merge is how the executor combines shard streams when the plan runs
	// on more than one shard; mergeErr says why it cannot.
	merge    *Merge
	mergeErr error

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
	Snapshot   *snapshot.Snapshot
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
	first := true
	p.ShardKeyValues = nil
	for _, vals := range values {
		// Collected then sorted and compacted, rather than scanned for a
		// duplicate on every value: an IN list of N values over S shards
		// cost N*S comparisons here, and comparing two terms cost S*S
		// again, on every Bind of a prepared statement.
		termShards := make([]int32, 0, len(vals))
		for _, v := range vals {
			id, err := placement.KeyspaceID(v)
			if err != nil {
				return pgwire.Errorf(pgwire.CodeFeatureNotSupported, "%v", err)
			}
			sh, err := p.snap.Locate(p.set, id)
			if err != nil {
				return pgwire.Errorf("57P03", "%v", err)
			}
			termShards = append(termShards, sh)
			p.ShardKeyValues = append(p.ShardKeyValues, v)
		}
		slices.Sort(termShards)
		termShards = slices.Compact(termShards)
		if first {
			shards, first = termShards, false
			continue
		}
		if !slices.Equal(shards, termShards) {
			if p.multiRow {
				return notYet("multi-row INSERT spanning shards is not available yet", "insert rows for one shard key value per statement")
			}
			return notYet("cross-shard join is not available yet", "join sharded tables only on equal shard keys")
		}
	}
	if p.touches == Unsharded && !slices.Equal(shards, []int32{p.home}) {
		return notYet("cross-shard join is not available yet", "unsharded tables live on the home shard; join them only with rows of that shard")
	}
	// Already sorted and unique by construction; kept explicit because the
	// order is part of what callers rely on.
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

// notYet builds the 0A000 refusal every unsupported shape reports.
func notYet(msg, hint string) *pgwire.Error {
	err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "%s", msg)
	err.Hint = hint
	return err
}

// notDurable refuses a relation whose rows are acknowledged but not
// WAL-logged. PostgreSQL truncates an unlogged relation on crash recovery
// and a promoted standby has never held its rows, so a write to one is
// acknowledged and then gone -- which is the thing the cluster's durability
// floor exists to prevent. pgshard enforces synchronous_commit = on and
// refuses to let it be lowered; accepting a relation that opts out of WAL
// entirely would make that guarantee a matter of which table was written.
func notDurable(form string) *pgwire.Error {
	return notYet(form+" is not supported: an unlogged relation is emptied by crash recovery and its rows never reach a standby, so writes to it would be acknowledged and lost on a failover",
		"create the relation LOGGED; pgshard has no durability mode in which unlogged rows survive")
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
