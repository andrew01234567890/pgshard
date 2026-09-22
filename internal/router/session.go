package router

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/pooler"
	"github.com/andrew01234567890/pgshard/internal/router/plan"
)

const (
	codeConnectionFailure = "08006"
	maxIdentifierLen      = 63
	releaseTimeout        = 5 * time.Second
	releaseRetryDelay     = 250 * time.Millisecond
	releaseRetries        = 6
)

type prepared struct {
	sql   string
	oids  []uint32
	class StmtClass
	plan  plan.Plan
	// inferred are the parameter types the backend reported for a
	// Describe of this statement; they type shard key parameters the client
	// left undeclared.
	inferred []uint32
	// snap is the snapshot the plan was made against; a newer snapshot
	// replans the statement at Bind.
	snap *snapshot.Snapshot
}

// shardSQL is the text the shards run: the client's, or the sequence-filled
// rewrite of an INSERT.
func (p prepared) shardSQL() string {
	if p.plan.Sequences != nil {
		return p.plan.Sequences.SQL
	}
	if p.plan.Rewritten != "" {
		return p.plan.Rewritten
	}
	return p.sql
}

// shardOIDs are the parameter types declared to the shards.
func (p prepared) shardOIDs() []uint32 {
	if f := p.plan.Sequences; f != nil {
		return extendOIDs(p.oids, f.Base, len(f.Names))
	}
	return p.oids
}

// paramOIDs merges the declared and backend-inferred parameter types.
func (p prepared) paramOIDs() []uint32 {
	if len(p.inferred) == 0 {
		return p.oids
	}
	out := make([]uint32, len(p.inferred))
	copy(out, p.inferred)
	for i, oid := range p.oids {
		if oid != 0 && i < len(out) {
			out[i] = oid
		}
	}
	return out
}

// execItem is one statement a batch executed, for the transaction prelude.
type sqlPreparedStmt struct {
	name, sql string
	// homeDDL: executing it creates a relation on the home shard, so the
	// EXECUTE meets checkHomeDDL (PGS-975).
	homeDDL bool
}

type savepointMark struct {
	name   string
	staged int
}

type execItem struct {
	sql    string
	local  bool
	class  StmtClass
	tables []snapshot.TableKey
	// foreign marks an Execute of a portal this router did not bind -- a
	// cursor DECLAREd in SQL, which lives on the backend. It carries no
	// statement of ours, but it DOES produce a completion, and noteBatch
	// walks the batch's Executes by counting completions: without a place
	// held here, a cursor's completion shifted the count and made
	// noteBatch record a later statement that had not run. Its empty class
	// records nothing but the shard it ran on.
	foreign bool
}

type gucEntry struct {
	name  string
	sql   string
	value string
	// searchPath is the schema list a search_path entry set; nil restores
	// the startup default.
	searchPath []string
}

// Executor is the router's pgwire.Executor: one client session relayed to
// the pooler of the shard its current statement plans onto.
type Executor struct {
	r    *Router
	info pgwire.SessionInfo
	// sid names the session to its poolers. A Release of it that failed
	// may have left a pooler holding a backend under it, prepared
	// statements and session state included, which the next statement
	// would meet; renewSid then gives the session a name no pooler holds
	// anything under. Written on the session goroutine under cancelMu,
	// because cancelStatement reads it from another.
	sid         string
	renewals    uint64
	releaseLost atomic.Bool
	// ended is set, under cancelMu, once the session is over and its
	// name will not be used again.
	ended bool
	// lostOn records, under lostMu, the shards a Release of the session's
	// name failed on, and the name it failed for.
	lostMu sync.Mutex
	lostOn map[Shard]string
	home   Shard
	// shard is the shard the session's stream is (or will next be) on.
	shard Shard
	// latency and the shard counters belong to latencyOf, kept so a
	// statement does not pay to resolve them again.
	latencyOf  Shard
	latency    prometheus.Observer
	statements prometheus.Counter
	rows       prometheus.Counter
	errors     prometheus.Counter
	ident      *pgshardv1.UserIdentity

	ctx    context.Context
	cancel context.CancelFunc

	conn   *poolerStream
	pinned bool
	tx     pgwire.TxStatus
	// implicitTx marks the transaction this executor opened for a
	// multi-statement simple query, which the client never asked for and
	// cannot be told about.
	implicitTx bool
	lastTag    string
	txnEnded   bool
	// cancelSent dedupes cancel requests within one statement, and
	// cancelFor is the statement context it was reset for. The reset used
	// to happen on every pump, and a statement pumps more than once -- the
	// backend is acquired, the session state replayed, the transaction
	// prelude reopened, and only then the statement itself. A cancellation
	// observed either side of one of those boundaries therefore sent a
	// second Cancel, and the pooler cancels by session: the second one can
	// land on whatever that backend is running by the time it arrives.
	cancelSent atomic.Bool
	cancelFor  context.Context
	// statement numbers the pgwire message cancelFor belongs to, counting
	// up: a simple query gets one number, and so does each extended-protocol
	// message, so the numbers a batch's requests carry can skip. It travels
	// on every request, Cancel, Reserve and Release the session sends. A cancel is sent from other goroutines and can outlive
	// the statement it was fired for; the number is how the router, and
	// then the pooler, tell it from a cancel for the statement running now.
	// Written under cancelMu.
	statement atomic.Uint64
	// uncancellable is set once every participant of a two-phase commit
	// has PREPARED. Past that point a forwarded cancel has nothing
	// legitimate to interrupt: the participants are idle backends holding
	// prepared transactions, and both paths left -- COMMIT PREPARED after a
	// commit decision, ROLLBACK PREPARED after the resolver aborted first --
	// are ones a cancel must not reach. Cancelling either leaves the rows
	// prepared on that participant until the resolver's next pass, pinning
	// WAL and blocking slot creation, and for a commit the client has
	// already been told COMMIT and cannot read its own writes there.
	//
	// It used to be set only after the decision was recorded, which left
	// the decision-log write itself -- a catalog fsync and a synchronous
	// standby round trip -- as a window where the cancel was forwarded and
	// could land on the COMMIT PREPARED that followed.
	//
	// This narrows that window and does not close it. A cancel that read
	// the flag as false during PREPARE and is still in flight when it goes
	// up is delivered anyway: the pooler checks only that the backend is
	// still this session's, and PostgreSQL acts on the signal if it arrives
	// once COMMIT PREPARED is running. Closing it needs the pooler to scope
	// a cancel to a statement.
	//
	// It also suppresses the cancel that the decision deadline itself fires
	// through onCancel. For COMMIT PREPARED that was already the behaviour;
	// for a ROLLBACK PREPARED stuck behind a departed synchronous standby it
	// is new, and the pooler backend then waits for PostgreSQL rather than
	// being signalled. The router side stays bounded by the deadline plus
	// the cancel grace.
	uncancellable atomic.Bool

	// cancelMu guards cancelTo, the poolers a cancel must reach: the streams
	// this statement has run on. It is the only executor state another
	// goroutine reads. Resolving the pooler from e.shard instead raced the
	// session goroutine moving the session, and could send the cancel to the
	// shard it had just left -- while marking the statement cancelled, so the
	// right one was never asked. A stream also records the pooler it was
	// opened on for the same reason a release does: the endpoint may have
	// moved since.
	cancelMu sync.Mutex
	cancelTo []pgshardv1.PoolerClient

	gucs   []gucEntry
	staged []gucEntry
	// stagedMark is the staged length before the current extended batch.
	stagedMark int
	stmts      map[string]prepared
	// sqlPrepared are the SQL-level PREPAREd statements, in creation
	// order, replayed like named protocol statements.
	sqlPrepared []sqlPreparedStmt
	// savepoints marks the staged length at each open savepoint so
	// ROLLBACK TO drops the settings staged after it.
	savepoints []savepointMark
	// portals maps portal names to logical statement names.
	portals map[string]string
	// portalDDL holds the migration a portal was bound to, as its Bind
	// found the statement: a name parsed again before the Execute names
	// another statement by then.
	portalDDL map[string]*plan.Plan
	// portalRun is what a portal's statement, as its Bind found it, runs
	// -- for the same reason as portalDDL: a statement replaced after the
	// Bind, the unnamed one above all, no longer says what the portal runs.
	// Only the portal's own facts are kept. The SQL-level statement an
	// EXECUTE names is looked up when the portal runs, which is when
	// PostgreSQL looks it up too (PGS-975).
	portalRun map[string]portalRun

	batch       []*pgshardv1.ExecuteRequest
	batchStmts  []string
	batchFailed bool
	// erredSinceSync records an error answered since the last Sync or
	// simple query began, which pgwire skips to Sync after.
	erredSinceSync bool
	// clientReqs marks the staged requests the client sent, and
	// completions queues, in send order, the Parse, Bind and Close
	// completions the backend owes and whether each is the client's.
	clientReqs  map[*pgshardv1.ExecuteRequest]bool
	completions []owedCompletion
	// backendOpen marks a batch this session flushed to the backend but
	// has not synced. The backend is mid-batch and holding an implicit
	// transaction open, so the client's Sync still has to reach it even
	// though there is nothing left staged to send.
	backendOpen bool
	batchWriter pgwire.ResultWriter
	// batchTarget is the shard the current extended batch resolved to.
	batchTarget *Shard
	// batchScatter is the multi-shard plan the current batch is bound to,
	// and batchScatterStmt the one statement it may carry.
	batchScatter     *plan.Plan
	batchScatterStmt string
	batchExec        []execItem
	// batchDDL holds, for each Parse, Bind and Execute of the batch in
	// order, the migration its statement plans to as that message arrived,
	// or nil. Resolving names at Sync instead sees only the last statement
	// a name was parsed to, so every Execute of a pipeline would run it.
	batchDDL []*plan.Plan
	// batchInject maps a batch index to the requests sent right after it:
	// the search_path reapplication a staged RESET needs before the next
	// pipelined statement runs. hiddenExec flags, per Execute on the wire,
	// the ones whose responses are the router's own and not the client's.
	batchInject map[int][]*pgshardv1.ExecuteRequest
	hiddenExec  []bool
	// execDone counts the client's Executes in the batch being pumped whose
	// response ended -- in CommandComplete, EmptyQueryResponse or
	// PortalSuspended -- and so reached the client as having run.
	execDone int
	// batchBinds records the binds of the batch so the targets can be
	// aimed again when the shard map moves while the batch waits out a
	// write fence.
	batchBinds []batchBind
	// describes lists the statements a batch asked to Describe, in order,
	// so ParameterDescription replies can be attributed.
	describes []string
	// pendingDescribes is describes for the batch in flight.
	pendingDescribes []string

	// reported is the value of every GUC_REPORT setting the client has been
	// told, so a backend repeating one it already has -- which replaying
	// session state onto a new backend makes routine -- is not forwarded
	// again.
	reported map[string]string

	// txnPrelude holds the session-local statements (BEGIN, SET, ...) run
	// since the transaction opened while nothing has touched a shard yet,
	// so the transaction can still move to the shard of its first real
	// statement.
	txnPrelude []string
	txnTouched bool
	// txnOnBackend says a shard is holding this transaction open, and
	// txnPreFence whether the cluster's write pause went up after that
	// happened. PostgreSQL reads default_transaction_read_only at BEGIN, so
	// such a transaction can still write -- and a barrier's drain waits for
	// exactly these before it takes a restore point. Holding its statements
	// would hold the drain that is waiting for it.
	txnOnBackend bool
	txnPreFence  bool
	// txnRanDDL says the open transaction applied DDL on its own, in a
	// database that runs DDL transactions sequentially.
	txnRanDDL bool

	// stmtSnap is the snapshot the statement in flight was planned
	// against; nil between statements.
	stmtSnap *snapshot.Snapshot

	// parked holds the streams of the other shards an open transaction has
	// touched; wroteHere says whether the current shard was written to.
	// startupSearchPath is the search_path the client asked for at startup
	// (options=-c search_path=...); nil means the server default.
	startupSearchPath []string
	// unsent names the statements of the batch being placed, which no
	// backend has parsed yet.
	unsent    map[string]bool
	parked    map[Shard]*txnPart
	wroteHere bool
	gid       string
	gidSeq    uint64
	// scatterSeq numbers this session's scatter reads so their pooler
	// session ids are never reused.
	scatterSeq uint64
	// releasing holds, per shard, the completion of a Release the session
	// fired without waiting; the next stream on that shard waits for it so
	// Reserve never pins the backend the release is still cleaning.
	releasing map[Shard]chan struct{}
}

func newExecutor(r *Router, info pgwire.SessionInfo, home Shard) *Executor {
	ctx, cancel := context.WithCancel(context.Background())
	keys := info.Auth.SCRAM
	e := &Executor{
		r: r, info: info, sid: r.prefix + "-" + strconv.FormatUint(info.ID, 10), home: home, shard: home,
		ident: &pgshardv1.UserIdentity{Username: info.User,
			ScramClientKey: append([]byte(nil), keys.ClientKey...), ScramServerKey: append([]byte(nil), keys.ServerKey...)},
		ctx: ctx, cancel: cancel, tx: pgwire.TxIdle,
		stmts: map[string]prepared{}, portals: map[string]string{}, portalDDL: map[string]*plan.Plan{}, portalRun: map[string]portalRun{},
	}
	e.startupSearchPath = startupPath(info.Params)
	return e
}

// resetsSearchPath reports whether g restores the search_path default
// (RESET search_path, SET search_path TO DEFAULT, or RESET ALL).
func resetsSearchPath(g gucEntry) bool {
	return g.name == "" || (g.name == "search_path" && g.searchPath == nil)
}

// searchPathSQL renders the statement that applies path on a backend.
//
// Elements are quoted only where PostgreSQL would quote them. set_config
// stores the string VERBATIM -- check_search_path validates the syntax and
// nothing canonicalises it -- so quoting every element unconditionally made
// current_setting('search_path') report `"public_02_x"` where a direct
// PostgreSQL connection reports `public_02_x`. Anything comparing that
// string sees a different value through pgshard than off it. pgroll's
// dual-write triggers compare it exactly, to decide which column a write
// belongs to, so the quotes sent writes to the wrong column.
// recordedSearchPath renders the session's path the way a migration has to
// carry it, and empty when the session is on the default: a migration that
// names no path leaves the applier's own default alone, and a client that
// never set one has no opinion to record.
func (e *Executor) recordedSearchPath() string {
	path := e.searchPath()
	if slices.Equal(path, plan.DefaultSearchPath) {
		return ""
	}
	return searchPathValue(path)
}

// searchPathValue is the value half of searchPathSQL: each element quoted
// as PostgreSQL quotes it, joined the way PostgreSQL reports them.
func searchPathValue(path []string) string {
	quoted := make([]string, len(path))
	for i, s := range path {
		quoted[i] = quoteSearchPathElement(s)
	}
	return strings.Join(quoted, ", ")
}

func searchPathSQL(path []string) string {
	value := searchPathValue(path)
	return "SELECT set_config('search_path', '" + strings.ReplaceAll(value, "'", "''") + "', false)"
}

// quoteSearchPathElement quotes one search_path element the way PostgreSQL's
// quote_identifier does: bare when it is a valid unquoted identifier, quoted
// otherwise. "$user" is quoted by this rule, which is what PostgreSQL itself
// reports for the default path.
func quoteSearchPathElement(s string) string {
	if isBareIdentifier(s) {
		return s
	}
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// isBareIdentifier reports whether s needs no quoting, by PostgreSQL's rule
// in quote_identifier: [a-z_][a-z0-9_]* and not a keyword. Note that '$' is
// legal in an identifier but quote_identifier still quotes it -- checked
// against a live server, which reports "a$b_1" with quotes -- so it is not
// accepted here either.
func isBareIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c == '_':
		case i > 0 && c >= '0' && c <= '9':
		default:
			return false
		}
	}
	return !quotedKeywords[s]
}

// quotedKeywords are the keywords PostgreSQL quotes in an identifier
// position. Only the ones plausible as a schema name are listed: quoting one
// that did not need it is harmless for correctness here -- it round-trips --
// while failing to quote a reserved word would produce invalid SQL.
var quotedKeywords = map[string]bool{
	"all": true, "analyse": true, "analyze": true, "and": true, "any": true,
	"array": true, "as": true, "asc": true, "authorization": true, "between": true,
	"binary": true, "both": true, "case": true, "cast": true, "check": true,
	"collate": true, "column": true, "constraint": true, "create": true,
	"cross": true, "current_date": true, "current_role": true, "current_time": true,
	"current_timestamp": true, "current_user": true, "default": true,
	"deferrable": true, "desc": true, "distinct": true, "do": true, "else": true,
	"end": true, "except": true, "false": true, "for": true, "foreign": true,
	"freeze": true, "from": true, "full": true, "grant": true, "group": true,
	"having": true, "ilike": true, "in": true, "initially": true, "inner": true,
	"intersect": true, "into": true, "is": true, "isnull": true, "join": true,
	"leading": true, "left": true, "like": true, "limit": true, "localtime": true,
	"localtimestamp": true, "natural": true, "not": true, "notnull": true,
	"null": true, "offset": true, "on": true, "only": true, "or": true,
	"order": true, "outer": true, "overlaps": true, "placing": true,
	"primary": true, "references": true, "returning": true, "right": true,
	"select": true, "session_user": true, "similar": true, "some": true,
	"symmetric": true, "table": true, "then": true, "to": true, "trailing": true,
	"true": true, "union": true, "unique": true, "user": true, "using": true,
	"variadic": true, "verbose": true, "when": true, "where": true, "window": true,
	"with": true,
}

// startupSearchPath extracts search_path from a startup "options" parameter
// (-c search_path=a,b or --search_path=a,b); other options are left alone.
func startupSearchPath(options string) []string {
	if value, ok := startupOption(options, "search_path"); ok {
		return splitSearchPathValue(value)
	}
	return nil
}

// startupOption is the last value the startup options string gives setting
// name, in its -c name=value, -cname=value or --name=value forms.
func startupOption(options, name string) (value string, found bool) {
	fields := strings.Fields(options)
	for i := 0; i < len(fields); i++ {
		f := fields[i]
		switch {
		case f == "-c" && i+1 < len(fields):
			i++
			f = fields[i]
		case strings.HasPrefix(f, "-c"):
			f = f[2:]
		case strings.HasPrefix(f, "--"):
			f = f[2:]
		default:
			continue
		}
		key, v, ok := strings.Cut(f, "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(key), name) {
			continue
		}
		value, found = v, true
	}
	return value, found
}

// splitSearchPathValue splits one search_path VALUE into its elements, the
// way the backend does when it resolves a name.
func splitSearchPathValue(value string) []string {
	path := []string{}
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		// An UNQUOTED element is downcased, a quoted one is not,
		// because that is what the backend does with this value.
		//
		// A startup option stores the string raw, and PostgreSQL splits
		// it with SplitIdentifierString when it resolves a name, which
		// downcases every unquoted element. Keeping the case here made
		// the planner look up "MySchema" while the backend searched
		// myschema: the planner found nothing, fell through to the
		// database default, and sent a sharded table's statement to the
		// home shard -- where the table also exists, so the answer was
		// the home shard's rows and no error.
		//
		// Measured: PGOPTIONS="-c search_path=MySchema" gives
		// current_setting 'MySchema' and current_schemas '{myschema}'.
		//
		// NOT true of `SET search_path = 'MySchema'`, which PostgreSQL
		// stores already quoted and resolves with its case intact -- the
		// planner's own SET parsing matches that and is left alone.
		if quoted := strings.HasPrefix(part, `"`) && strings.HasSuffix(part, `"`) && len(part) >= 2; quoted {
			part = strings.ReplaceAll(part[1:len(part)-1], `""`, `"`)
		} else {
			part = downcaseIdentifier(part)
		}
		if part != "" {
			path = append(path, part)
		}
	}
	return path
}

// startupPath reads the session's startup search_path. A client may send it
// two ways and pgshard has to honour both: inside "options" as -c
// search_path=..., and as a startup PARAMETER of its own, which the protocol
// allows for any user-settable GUC and which Go drivers send for connection
// keys they do not recognise -- pgroll connects that way.
//
// Reading only "options" did not merely lose it for planning: nothing
// forwards a bare startup parameter to the backend either, so the setting
// was dropped entirely and the session silently ran under the default path.
// Unqualified names then resolved in the wrong schema, and for a sharded
// table that means the home shard's rows rather than an error.
//
// A parameter of its own wins over "options": it is the unambiguous form,
// and a client that sends both is asking for the more specific one.
func startupPath(params map[string]string) []string {
	if v, ok := params["search_path"]; ok {
		if path := splitSearchPathValue(v); len(path) > 0 {
			return path
		}
	}
	return startupSearchPath(params["options"])
}

// downcaseIdentifier lowercases an unquoted identifier the way PostgreSQL
// does: ASCII A-Z only. downcase_identifier (scansup.c) touches a high-bit
// byte only in a single-byte encoding, so under UTF-8 -- which is what a
// pgshard cluster runs -- anything above ASCII is left as it is. Using a
// Unicode-aware lowercase here would fold characters the backend does not.
func downcaseIdentifier(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}

// searchPath is the schema list in force for the next statement: the
// startup default, then every settled and staged SET/RESET in order.
func (e *Executor) searchPath() []string {
	path := e.startupSearchPath
	for _, list := range [][]gucEntry{e.gucs, e.staged} {
		for _, g := range list {
			switch g.name {
			case "":
				path = e.startupSearchPath
			case "search_path":
				path = g.searchPath
				if path == nil {
					path = e.startupSearchPath
				}
			}
		}
	}
	return path
}

// Home reports the home shard of the session's database in the serving
// shard set: a reshard cutover moves both the set and the home shard id.
//
// It reads the LIVE snapshot, which is right for a caller asking where the
// home shard is now and wrong for one deriving anything about a statement
// already planned. Those use homeAt with the statement's own snapshot.
func (e *Executor) Home() Shard { return e.homeAt(e.r.cfg.Snapshot()) }

// homeAt is Home against a snapshot the caller has already taken.
//
// The fallback to e.home is the session-start set, which after a cutover
// names a stale one, so it is reached only when snap does not carry the
// database at all -- a database dropped mid-session. Keeping it keyed to
// the caller's snapshot rather than the live one is the point: a statement
// planned against a snapshot that HAS the database must not fall back
// because a reload in between dropped it.
func (e *Executor) homeAt(snap *snapshot.Snapshot) Shard {
	if e.catalogSession() {
		return e.home
	}
	if snap != nil {
		if d, ok := snap.Databases[e.info.Database]; ok {
			return Shard{Set: snap.ServingShardSet(), ID: d.HomeShard}
		}
	}
	return e.home
}

// catalogSession reports whether the session fronts the catalog database,
// whose plans never depend on the shard map.
func (e *Executor) catalogSession() bool { return e.home.Set == CatalogShardSet }

// userSet is the shard set the session's user data lives in right now.
//
// A statement in flight reads it from the snapshot it was planned against:
// its shard ids name shards of that snapshot's set, and generation() stamps
// the epoch from the same one, so reading the live set could name a shard
// the stamp has no epoch for (PGS-962).
func (e *Executor) userSet() string {
	if e.stmtSnap != nil {
		return e.homeAt(e.stmtSnap).Set
	}
	return e.Home().Set
}

// Shard reports the shard the session's stream is on.
func (e *Executor) Shard() Shard { return e.shard }

// planSessionAt describes this session to the planner as of snap, a
// snapshot the caller has already taken, so planning and the generation
// stamped on what the plan sends come from the same one. Sessions on the
// catalog shard set see no table placement and plan everything onto their
// home shard.
func (e *Executor) planSessionAt(snap *snapshot.Snapshot) plan.Session {
	// homeAt(snap), not Home(): Home reads the live snapshot, and the
	// watcher swaps that pointer on every reload, so the plan was being
	// built from two different snapshots -- Snapshot pinned by the caller
	// and HomeShard from whatever had just landed. Pinning one and then
	// reading the other defeats the pinning for exactly one field.
	return plan.Session{Database: e.info.Database, HomeShard: e.homeAt(snap).ID, User: e.info.User,
		SearchPath: e.searchPath(), Snapshot: snap, PinnedShard: e.pinnedShard()}
}

// plan plans sql for this session. Sessions on the catalog database run
// DDL directly on it: the migration model covers user databases, whichever
// shard set serves them.
func (e *Executor) plan(ctx context.Context, sql string) (plan.Plan, error) {
	return e.planOp(ctx, sql, "simple")
}

// staleSnapshot refuses to plan against a catalog view the router can no
// longer trust. A failed reload leaves the previous snapshot in place and
// the router serving it, so nothing else distinguishes a current view from
// one whose reloads have been failing for an hour -- and an online rewrite
// relies on a router either having reloaded or having stopped, never on it
// quietly serving the view from before the column list was published.
func (e *Executor) staleSnapshot() error {
	if e.catalogSession() {
		return nil
	}
	snap := e.r.cfg.Snapshot()
	if !snap.Stale(e.r.now()) {
		return nil
	}
	age, _ := snap.Age(e.r.now())
	err := pgwire.Errorf(codeStaleGeneration, "this router last read the catalog %s ago and will not plan against it", age.Round(time.Second))
	err.Hint = "the router is failing to reload its catalog snapshot; retry, and check the catalog is reachable from it"
	return err
}

func (e *Executor) planOp(ctx context.Context, sql, opcode string) (plan.Plan, error) {
	return e.planOpAt(ctx, e.currentSnapshot(), sql, opcode)
}

// planOpAt is planOp against a snapshot the caller has already taken.
func (e *Executor) planOpAt(ctx context.Context, snap *snapshot.Snapshot, sql, opcode string) (plan.Plan, error) {
	if err := e.staleSnapshot(); err != nil {
		e.r.metrics.Refusals.WithLabelValues(codeStaleGeneration).Inc()
		return plan.Plan{}, err
	}
	// One snapshot for the statement: it is planned against this and
	// everything it sends is stamped from it. The watcher swaps the pointer
	// on every reload, so reading the live one again at send time could
	// stamp a generation the plan was never made under -- and a pooler
	// already at that generation admits it, which is the case the fence
	// exists to refuse.
	e.stmtSnap = snap
	pl, err := e.r.cfg.Planner.Plan(ctx, e.planSessionAt(e.stmtSnap), sql)
	if err == nil && pl.Kind == plan.MigrationKind && e.catalogSession() {
		pl.Kind, pl.Shards, pl.Migration = plan.Unsharded, []int32{e.home.ID}, nil
	}
	if err == nil && !e.catalogSession() {
		switch {
		case pl.HomeDDL, e.executesHomeDDL(pl.Class.Executes):
			err = e.checkHomeDDL(ctx)
		case pl.Kind == plan.MigrationKind:
			err = e.checkFanoutDDL(ctx)
		}
	}
	if err != nil {
		var perr *pgwire.Error
		if errors.As(err, &perr) {
			e.r.metrics.Refusals.WithLabelValues(perr.Code).Inc()
		}
		return pl, err
	}
	e.r.metrics.Queries.WithLabelValues(pl.Kind.String(), opcode).Inc()
	return pl, nil
}

// target turns a resolved plan into the one shard the executor can run it
// on, refusing what needs more than one shard.
func (e *Executor) target(pl plan.Plan) (Shard, error) {
	switch {
	case pl.Kind == plan.Refuse:
		return Shard{}, pl.Err
	case pl.Kind == plan.SessionLocal:
		return e.shard, nil
	case pl.Kind == plan.Reference && !pl.Class.Write && len(pl.Shards) == 0:
		return e.referenceTarget(), nil
	case pl.Deferred:
		return Shard{}, pgwire.Errorf(pgwire.CodeInternalError, "router: deferred plan was not resolved")
	case len(pl.Shards) != 1:
		return Shard{}, pgwire.Errorf(pgwire.CodeInternalError, "router: %s plan over %d shards has no single target", pl.Kind, len(pl.Shards))
	}
	return Shard{Set: e.userSet(), ID: pl.Shards[0]}, nil
}

// referenceTarget spreads reference reads across the serving shards, by
// session, so they do not all land on one.
//
// The choice is made here rather than in the plan because a plan that named
// the shard would depend on which session asked for it, which is the one
// thing keeping plans from being shared between sessions: a cached
// reference read would pin every session to whichever shard the first was
// given.
func (e *Executor) referenceTarget() Shard {
	set := e.userSet()
	var ids []int32
	if snap := e.r.cfg.Snapshot(); snap != nil {
		// The snapshot computed this list once when it was loaded; this
		// rebuilt it, deduplicated and sorted it, on every reference read.
		// Read-only here: the slice is shared with every plan that asks.
		ids = snap.ShardIDs(set)
	}
	if len(ids) == 0 {
		return e.Home()
	}
	// Inside a transaction, stay where the transaction already is. A
	// reference table is on EVERY shard, so reading it from the pinned one
	// is the same answer -- and choosing by session id instead forced a
	// shard switch for no gain: under REPEATABLE READ or SERIALIZABLE that
	// is refused outright with "cannot span shards", and under READ
	// COMMITTED it is a needless park, Reserve, SET LOCAL lock_timeout and
	// a hidden-writer probe at COMMIT.
	//
	// Only when the current shard really is one of this set's: a session
	// pinned elsewhere, or to a set that has moved, falls back to the
	// spread below.
	if e.tx != pgwire.TxIdle && e.shard.Set == set {
		for _, id := range ids {
			if id == e.shard.ID {
				return e.shard
			}
		}
	}
	return Shard{Set: set, ID: ids[e.info.ID%uint64(len(ids))]}
}

// moveTo points the session at target, dropping the stream on the previous
// shard. A transaction that already touched a shard cannot move.
func (e *Executor) moveTo(ctx context.Context, target Shard) error {
	if target == e.shard {
		return nil
	}
	if e.conn == nil && !e.pinned {
		e.shard = target
		return nil
	}
	if e.tx != pgwire.TxIdle && e.txnTouched {
		// switchPart runs SQL on a part the transaction has already used:
		// it repins it and replays the prelude. That happens before
		// withFailover ever sees the statement, so a flip landing there
		// went to the client as the participant's own 55000 -- the last
		// way one could, and the one a transfer alternating between two
		// shards hits every time round.
		return nameFenceInTxn(e.switchPart(ctx, target))
	}
	e.dropStream()
	e.shard = target
	return nil
}

// noteExecuted records what a completed statement did to the open
// transaction: session-local statements join the prelude, anything else
// pins the transaction to the current shard.
//
// DEALLOCATE does neither. PostgreSQL does not undo it on rollback, and
// noteSessionEffect already drops the statement from sqlPrepared, which a
// fresh backend is given before the prelude is replayed -- so replaying
// the DEALLOCATE as well ran it against a backend that never had the
// statement, and its 26000 failed the replay (PGS-959).
func (e *Executor) noteExecuted(sql string, local bool, class StmtClass) {
	if e.tx == pgwire.TxIdle {
		return
	}
	if class.Session == plan.SessionDeallocate {
		return
	}
	if local {
		e.txnPrelude = append(e.txnPrelude, sql)
		return
	}
	e.txnTouched = true
}

// TransactionStatus implements pgwire.Executor.
func (e *Executor) TransactionStatus() pgwire.TxStatus { return e.tx }

// physical maps a client statement name onto the per-session backend name.
func (e *Executor) physical(name string) string {
	if name == "" {
		return ""
	}
	p := "pgshard_" + strconv.FormatUint(e.info.ID, 10) + "_"
	if len(p)+len(name) > maxIdentifierLen {
		sum := sha256.Sum256([]byte(name))
		return p + "h" + hex.EncodeToString(sum[:12])
	}
	return p + name
}

func (e *Executor) needsPin() bool {
	if e.startupSearchPath != nil || len(e.gucs) > 0 || len(e.staged) > 0 || len(e.sqlPrepared) > 0 {
		return true
	}
	for name := range e.stmts {
		if name != "" {
			return true
		}
	}
	return false
}

// guard confines a panic in planning or execution to the calling session:
// it is logged, the session's shard stream is dropped and the client gets an
// XX000 error instead of the router process dying.
func (e *Executor) guard(op string, run func() error) (err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		e.r.cfg.Logger.Error("router: panic in session", "op", op, "session", e.sid, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
		e.failBatch()
		// Between Parse and Sync the pgwire layer skips to Sync, where the
		// failed batch is cleared; a simple query or Sync itself has no
		// batch left to skip.
		e.batchFailed = op == "Parse" || op == "Bind" || op == "Execute"
		e.dropStream()
		e.staged, e.stagedMark = nil, 0
		e.txnPrelude, e.txnTouched = nil, false
		e.txnOnBackend, e.txnPreFence = false, false
		e.txnRanDDL = false
		e.savepoints = nil
		// finishTxn is what lowers uncancellable, and a panic skips it. Left
		// up, every later cancel on this session would be swallowed: the
		// client waits out the cancel grace and gets 08006 instead of 57014,
		// for the rest of the session and with nothing logged. The cancel
		// targets are the dropped stream's, so they go too.
		e.uncancellable.Store(false)
		e.forgetCancelTargets()
		err = pgwire.Errorf(pgwire.CodeInternalError, "internal error while processing the statement; the session state was reset")
	}()
	err = e.nameFence(e.asWritePause(run()))
	if err != nil {
		e.erredSinceSync = true
	}
	return err
}

// nameFence is the last word on a pooler's fence refusal.
//
// Four separate paths were found returning one to the client unchanged --
// the statement path, the commit path, a wait that ran out, and rejoining
// a shard a transaction had already used -- and each was fixed where it
// was found, and the next measurement found another. The list was never
// the point. 55000 is object_not_in_prerequisite_state: a client can do
// nothing with it, and no path should be able to send it. Every public
// entry point goes through guard, so a path added later is covered by
// having been written at all.
//
// The declared reason only. A bare 55000 may be a rewrite in progress,
// which is not a fence and clears on its own, and rewriting that into
// "retry the transaction" would be a loop with nothing at the end of it.
// The paths above still answer first where they know more than this does
// -- whether output was sent, whether a transaction is open -- and this
// catches what reaches here regardless.
func (e *Executor) nameFence(err error) error {
	if poolerReason(err) != pgshardv1.Reason_REASON_STALE_GENERATION {
		return err
	}
	return failoverInTxnError()
}

// asWritePause reports a statement the cluster's own write pause stopped as
// that pause, rather than as the backend's read-only error.
//
// The pause is raised in two places that cannot be raised at once: the
// catalog fence, which routers see through their snapshot, and
// default_transaction_read_only on every group, which the barrier sets
// immediately afterwards. In the gap a router has not yet seen the fence,
// forwards a write, and the shard refuses it with 25006 -- so the client
// gets PostgreSQL's error for a pause pgshard took, instead of the
// retryable 57P03 the contract promises, and nothing tells it to retry.
//
// Translated while our own fence is raised, and for the buffering window
// after we last saw it: a barrier that is not waiting on anything is
// milliseconds long, so a statement it refused routinely returns after the
// pause has already been lifted, and a check that only looks at now would
// hand the client PostgreSQL's error for a pause that was ours.
//
// The one imprecision left
// is a session that asked for BEGIN READ ONLY during a pause: its write
// would have been refused either way, and it is told about the pause. A
// client cannot reach this by setting a GUC -- default_transaction_read_only
// and transaction_read_only are both refused by the planner.
func (e *Executor) asWritePause(err error) error {
	var pe *pgwire.Error
	if err == nil || !errors.As(err, &pe) || pe.Code != pgwire.CodeReadOnlySQLTransaction {
		return err
	}
	if !e.r.writeFenced(nil) && !e.r.sawWriteFenceRecently() {
		return err
	}
	return writeFenceError(e.r.migrating(), false)
}

// SimpleQuery implements pgwire.Executor.
func (e *Executor) SimpleQuery(ctx context.Context, sql string, w pgwire.ResultWriter) error {
	e.enterStatement(ctx)
	defer e.endStatement()
	e.erredSinceSync = false
	err := e.guard("SimpleQuery", func() error { return e.simpleQuery(ctx, sql, w) })
	e.failTxnTheBackendDidNotFail(ctx)
	return err
}

// failTxnTheBackendDidNotFail puts the client's transaction into the failed
// state after an error the router answered itself -- a refusal, a DDL
// PostgreSQL would have run, anything that never reached a shard.
//
// PostgreSQL fails the transaction block on every error: statements after
// it get 25P02 and a COMMIT rolls back. A shard's own error does that on
// the shard, and the relayed ReadyForQuery says so. A router error left the
// backend's transaction healthy, so "psql -1 -f" without ON_ERROR_STOP, or
// any client that logs an error and commits anyway, committed the
// statements around the one refused (PGS-976).
//
// The backend is failed rather than dropped, with a statement that raises
// on every participant: a savepoint taken before the error then recovers
// the transaction exactly as it does in PostgreSQL, which a dropped backend
// cannot. With no backend yet there is nothing to fail but the session's
// own state, which failTxn already answers for.
func (e *Executor) failTxnTheBackendDidNotFail(ctx context.Context) {
	erred := e.erredSinceSync
	e.erredSinceSync = false
	// A multi-shard transaction between shards has an idle current part
	// and is still the client's; an implicit one is pgwire's to roll back,
	// at once.
	inTxn := e.tx == pgwire.TxInBlock || (e.tx == pgwire.TxIdle && e.multiShardTxn())
	if !erred || !inTxn || e.implicitTx {
		return
	}
	if e.conn == nil && !e.multiShardTxn() {
		e.failTxn()
		return
	}
	ctx = context.WithoutCancel(ctx)
	var parts []*txnPart
	for _, p := range e.parts() {
		if p.ps != nil {
			parts = append(parts, p)
		}
	}
	e.each(parts, func(p *txnPart) error { return e.runOn(ctx, p, abortTxnSQL, discardWriter{}) })
	for _, p := range parts {
		if p.tx != pgwire.TxFailed {
			e.r.cfg.Logger.Warn("router: could not fail a transaction on its shard; dropping its backends",
				"session", e.sid, "shard", p.shard, "err", p.err)
			e.dropStream()
			e.failTxn()
			return
		}
	}
	e.tx = pgwire.TxFailed
}

const abortTxnSQL = "DO $pgshard$BEGIN RAISE EXCEPTION 'pgshard: an earlier statement of this transaction failed at the router'; END$pgshard$"

// endStatement forgets the snapshot the statement just ended was planned
// against. pump does it on a relayed ReadyForQuery, but a statement the
// router answers itself -- a refusal, EXPLAIN, nextval, a failed
// transaction's COMMIT -- relays none, and the snapshot then outlived it:
// a later Bind of a cached statement needs no re-plan, so nothing set it
// again, and that statement was stamped and targeted from an older map
// than the one it was checked against (PGS-962).
func (e *Executor) endStatement() { e.stmtSnap = nil }

// refuseQueryDuringABufferedBatch refuses a simple Query sent while an
// extended batch is still buffered, because the router would otherwise run
// them in the wrong order.
//
// The extended batch is staged until its Sync; a Query is executed at once.
// So a client that sends Parse/Bind/Execute and then a Query, without a
// Sync in between, has the Query run FIRST and its own earlier statement
// run afterwards. Measured: with an INSERT buffered and "select 1" sent
// after it, the select ran and the insert followed at the Sync. Replace
// the select with a CREATE TABLE and the buffered INSERT runs inside the
// transaction after the DDL, which is not what the client wrote (PGS-883).
//
// Refusing rather than flushing first. Flushing would preserve the order,
// but running a staged batch early is exactly what PGS-911 could not make
// safe: the router tracks transaction state from the relayed
// ReadyForQuery, which a Flush does not produce. Silent misordering is the
// worse failure of the two, and no mainstream driver sends this sequence --
// pgx, psycopg and the JDBC driver all Sync before leaving the extended
// protocol.
func (e *Executor) refuseQueryDuringABufferedBatch() error {
	if len(e.batch) == 0 {
		return nil
	}
	err := pgwire.Errorf(pgwire.CodeFeatureNotSupported,
		"a simple query is not available while an extended-protocol batch is still buffered: the batch runs at its Sync, so this statement would run before statements the client sent first")
	err.Hint = "send Sync to end the extended batch before the simple query"
	return err
}

// txnControlInBatch answers a transaction control statement inside a batch
// this executor opened a transaction for, the way PostgreSQL does
// (xact.c, BeginTransactionBlock and EndTransactionBlock in
// TBLOCK_IMPLICIT_INPROGRESS): a BEGIN adopts the transaction, so the
// statements before it commit or roll back with the client's own; a COMMIT
// or ROLLBACK ends it with a WARNING that no transaction was in progress,
// and pgwire opens a new one for the statements after it; a savepoint is
// an error, because there is no transaction block to hold one.
//
// implicitTx is set after BeginImplicit's own BEGIN and cleared before
// EndImplicit's COMMIT, so neither reaches this.
func (e *Executor) txnControlInBatch(ctx context.Context, class StmtClass, w pgwire.ResultWriter) (handled bool, err error) {
	if !e.implicitTx {
		return false, nil
	}
	switch class.Txn {
	case plan.TxnBegin:
		if !class.TxnModes {
			e.implicitTx = false
			return true, w.CommandComplete("BEGIN")
		}
		// Modes apply to a transaction that has not run anything yet, and
		// then ending the implicit one and running the client's BEGIN in
		// its place is the same transaction. Once a statement has run,
		// PostgreSQL itself refuses an isolation level, and the prelude
		// holding the router's own BEGIN may already be on a backend.
		if len(e.txnPrelude) <= 1 && !e.txnTouched && !e.txnRanDDL && !e.multiShardTxn() {
			return false, e.EndImplicit(ctx, false)
		}
		err := pgwire.Errorf(pgwire.CodeFeatureNotSupported,
			"BEGIN with transaction modes is not available after other statements of a multi-statement simple query")
		err.Hint = "put the BEGIN first in the batch, or send it as its own query"
		return true, err
	case plan.TxnCommit, plan.TxnRollback:
		verb := "COMMIT"
		if class.Txn == plan.TxnRollback {
			verb = "ROLLBACK"
		}
		if class.Chain {
			return true, pgwire.Errorf(codeNoActiveSQLTransaction, "%s AND CHAIN can only be used in transaction blocks", verb)
		}
		if err := w.Notice(noActiveTransactionWarning()); err != nil {
			return true, err
		}
		if err := e.EndImplicit(ctx, class.Txn == plan.TxnCommit); err != nil {
			return true, err
		}
		return true, w.CommandComplete(verb)
	case plan.TxnSavepoint:
		return true, pgwire.Errorf(codeNoActiveSQLTransaction, "SAVEPOINT can only be used in transaction blocks")
	case plan.TxnRelease:
		return true, pgwire.Errorf(codeNoActiveSQLTransaction, "RELEASE SAVEPOINT can only be used in transaction blocks")
	case plan.TxnRollbackTo:
		return true, pgwire.Errorf(codeNoActiveSQLTransaction, "ROLLBACK TO SAVEPOINT can only be used in transaction blocks")
	}
	return false, nil
}

// PostgreSQL's own words and SQLSTATE (xact.c: ERRCODE_NO_ACTIVE_SQL_TRANSACTION).
const codeNoActiveSQLTransaction = "25P01"

func noActiveTransactionWarning() *pgproto3.NoticeResponse {
	return &pgproto3.NoticeResponse{Severity: "WARNING", SeverityUnlocalized: "WARNING",
		Code: codeNoActiveSQLTransaction, Message: "there is no transaction in progress"}
}

// BeginImplicit implements pgwire.Executor: it opens the transaction a
// multi-statement simple query runs in. It is a BEGIN like any other, so
// the shard is pinned and the transaction escalates to two-phase commit the
// same way -- what differs is that the client never sent it, so nothing it
// answers reaches the client.
func (e *Executor) BeginImplicit(ctx context.Context) error {
	e.enterStatement(ctx)
	// Its error ends the batch there, with no Sync or simple query of the
	// router's to consume the flag, which would then fail the client's
	// next transaction.
	defer func() { e.erredSinceSync = false }()
	if err := e.guard("BeginImplicit", func() error {
		return e.simpleQuery(ctx, "BEGIN", discardWriter{})
	}); err != nil {
		return err
	}
	e.implicitTx = true
	return nil
}

// EndImplicit implements pgwire.Executor.
func (e *Executor) EndImplicit(ctx context.Context, commit bool) error {
	e.enterStatement(ctx)
	defer func() { e.erredSinceSync = false }()
	sql := "ROLLBACK"
	if commit {
		sql = "COMMIT"
	}
	// A BEGIN in the batch adopted the transaction: it is the client's
	// now, and so is ending it.
	if !e.implicitTx {
		return nil
	}
	e.implicitTx = false
	return e.guard("EndImplicit", func() error {
		return e.simpleQuery(ctx, sql, discardWriter{})
	})
}

func (e *Executor) simpleQuery(ctx context.Context, sql string, w pgwire.ResultWriter) error {
	if err := e.refuseQueryDuringABufferedBatch(); err != nil {
		return err
	}
	pl, err := e.plan(ctx, sql)
	if err != nil {
		return err
	}
	if handled, err := e.txnControlInBatch(ctx, pl.Class, w); handled || err != nil {
		return err
	}
	if pl.Explain != nil {
		return e.afterBatch(ctx, e.answerExplain(pl.Explain, true, true, w))
	}
	if err := checkFanoutMode(pl.Class); err != nil {
		return err
	}
	if err := e.checkShardPin(pl.Class); err != nil {
		return err
	}
	// The failed-transaction checks come before the fan-out ceiling on
	// purpose. A session whose transaction was killed needs to be told
	// that -- 25P02, end the transaction -- rather than that its query is
	// too wide, which is true but not the thing standing in its way.
	if handled, err := e.endFailedTxn(pl.Class, w); handled {
		return e.afterBatch(ctx, err)
	}
	if handled, err := e.endNoTxn(pl.Class, w); handled {
		return e.afterBatch(ctx, err)
	}
	if err := e.recoverFailedTxnToSavepoint(ctx, pl.Class, nil); err != nil {
		return e.afterBatch(ctx, err)
	}
	if err := e.refuseInFailedTransaction(pl.Class); err != nil {
		return e.afterBatch(ctx, err)
	}
	if err := e.checkFanout(pl); err != nil {
		return e.afterBatch(ctx, err)
	}
	if err := e.refuseShardStatementAfterDDL(pl); err != nil {
		return e.afterBatch(ctx, err)
	}
	if pl.Kind == plan.MigrationKind {
		return e.afterBatch(ctx, e.runMigration(ctx, pl, w))
	}
	if pl.NextVal != "" {
		if err := e.refuseSelfAnsweredInFailedTxn(); err != nil {
			return e.afterBatch(ctx, err)
		}
		return e.afterBatch(ctx, e.answerNextval(ctx, pl.NextVal, true, true, false, w))
	}
	if pl.Class.Write {
		before := e.currentSnapshot()
		if err := e.gateWrite(ctx, e.writeTarget(pl), pl.Tables); err != nil {
			return e.afterBatch(ctx, err)
		}
		if snap := e.currentSnapshot(); snap != before {
			// The shard map moved while the statement waited: plan it
			// against the map it will run on, and stamp it from that map
			// too. Keeping the old stamp let a pooler still serving the
			// old generation admit a plan made for the new one (PGS-951).
			e.stmtSnap = snap
			if pl, err = e.r.cfg.Planner.Plan(ctx, e.planSessionAt(snap), sql); err != nil {
				return err
			}
		}
	}
	if pl.Rewritten != "" {
		sql = pl.Rewritten
	}
	if isReferenceWrite(pl) {
		return e.afterBatch(ctx, e.referenceWrite(ctx, pl, []*pgshardv1.ExecuteRequest{simpleQuery(sql)}, w))
	}
	if multiShard(pl) {
		return e.afterBatch(ctx, e.scatterSimple(ctx, pl, sql, w))
	}
	if err := checkTransactionMode(pl.Class); err != nil {
		return err
	}
	if handled, err := e.txnControl(ctx, pl.Class, w); handled {
		return e.afterBatch(ctx, err)
	}
	if pl.Class.Session == plan.SessionDeallocate {
		if _, protocol := e.stmts[pl.Class.SessionName]; protocol && pl.Class.SessionName != "" {
			sql = "DEALLOCATE " + quoteIdent(e.physical(pl.Class.SessionName))
		}
	}
	reqs := []*pgshardv1.ExecuteRequest{simpleQuery(sql)}
	if fill := pl.Sequences; fill != nil {
		params, values, err := e.sequenceValues(ctx, fill)
		if err != nil {
			return err
		}
		if pl.Deferred {
			if pl, err = pl.Resolve(injectedParams{base: fill.Base, values: values}); err != nil {
				return err
			}
		}
		reqs = []*pgshardv1.ExecuteRequest{parseReq("", fill.SQL, extendOIDs(nil, fill.Base, len(fill.Names))),
			bindReq("", "", nil, params, nil), describeReq(pgwire.DescribePortal, ""), executeReq("", 0), syncReq()}
		w = noDataFilter{w}
	}
	target, err := e.target(pl)
	if err != nil {
		return err
	}
	if err := e.moveTo(ctx, target); err != nil {
		return err
	}
	return e.withFailover(ctx, w, func(cw pgwire.ResultWriter) error {
		if err := e.acquire(ctx, nil); err != nil {
			return err
		}
		if pl.Class.Write {
			if err := e.noteWrite(ctx); err != nil {
				return err
			}
		}
		if pl.Class.SetGUC || pl.Class.Session == plan.SessionPrepare {
			if err := e.ensurePinned(ctx); err != nil {
				return err
			}
		}
		for _, req := range reqs {
			if err := e.send(req); err != nil {
				return err
			}
		}
		err := e.pump(ctx, cw)
		if err == nil {
			e.noteExecuted(sql, pl.Kind == plan.SessionLocal && !pl.Class.RunsOnAShard, pl.Class)
		}
		if pl.Class.SetGUC && err == nil {
			g := gucEntry{name: pl.Class.GUCName, sql: sql, value: pl.Class.GUCValue, searchPath: pl.Class.SearchPath}
			e.staged = append(e.staged, g)
			if err := e.reapplyStartupSearchPath(ctx, g); err != nil {
				return err
			}
		}
		if err == nil {
			e.noteSessionEffect(pl.Class, sql)
		}
		return err
	})
}

// noteSessionEffect records what a completed statement did to the session
// state the router replays: savepoints scope staged settings, SQL PREPARE
// adds a replayed statement, DEALLOCATE and DISCARD ALL drop them.
func (e *Executor) noteSessionEffect(class StmtClass, sql string) {
	switch class.Txn {
	case plan.TxnSavepoint:
		e.savepoints = append(e.savepoints, savepointMark{name: class.Savepoint, staged: len(e.staged)})
	case plan.TxnRollbackTo:
		if i := e.savepointIndex(class.Savepoint); i >= 0 {
			e.staged = e.staged[:min(e.savepoints[i].staged, len(e.staged))]
			e.savepoints = e.savepoints[:i+1]
		}
	case plan.TxnRelease:
		if i := e.savepointIndex(class.Savepoint); i >= 0 {
			e.savepoints = e.savepoints[:i]
		}
	}
	switch class.Session {
	case plan.SessionPrepare:
		e.forgetSQLPrepared(class.SessionName)
		e.sqlPrepared = append(e.sqlPrepared, sqlPreparedStmt{name: class.SessionName, sql: sql, homeDDL: class.PreparesHomeDDL})
	case plan.SessionDeallocate:
		if class.SessionName == "" {
			e.sqlPrepared = nil
			e.forgetNamedStatements()
			return
		}
		e.forgetSQLPrepared(class.SessionName)
		delete(e.stmts, class.SessionName)
	case plan.SessionDiscardAll:
		e.gucs, e.staged, e.savepoints, e.sqlPrepared = nil, nil, nil, nil
		e.forgetNamedStatements()
	}
}

func (e *Executor) savepointIndex(name string) int {
	for i := len(e.savepoints) - 1; i >= 0; i-- {
		if e.savepoints[i].name == name {
			return i
		}
	}
	return -1
}

// executesHomeDDL reports that an SQL-level EXECUTE runs a statement whose
// execution creates a relation on the home shard. A PREPARE or DEALLOCATE of
// that name earlier in the batch in flight has not been recorded in
// sqlPrepared yet -- that happens once the batch's results are in -- so it
// is read from the batch first, latest first.
func (e *Executor) executesHomeDDL(name string) bool {
	if name == "" {
		return false
	}
	for i := len(e.batchExec) - 1; i >= 0; i-- {
		c := e.batchExec[i].class
		switch {
		case c.Session == plan.SessionPrepare && c.SessionName == name:
			return c.PreparesHomeDDL
		case c.Session == plan.SessionDeallocate && (c.SessionName == name || c.SessionName == ""),
			c.Session == plan.SessionDiscardAll:
			return false
		}
	}
	for _, p := range e.sqlPrepared {
		if p.name == name {
			return p.homeDDL
		}
	}
	return false
}

// portalRun is what a bound portal runs, as its Bind found its statement.
type portalRun struct {
	homeDDL  bool
	executes string
}

func (e *Executor) forgetSQLPrepared(name string) {
	e.sqlPrepared = slices.DeleteFunc(e.sqlPrepared, func(p sqlPreparedStmt) bool { return p.name == name })
}

func (e *Executor) forgetNamedStatements() {
	for name := range e.stmts {
		if name != "" {
			delete(e.stmts, name)
		}
	}
}

// withFailover runs one statement through run, buffering it while its shard
// fails over: a shard that is blocking in the snapshot is waited for before
// the first attempt, and a stale-generation refusal or a refused pooler
// connection is retried once after the snapshot moves, provided nothing has
// reached the client and no transaction is open (see decideFailover).
func (e *Executor) withFailover(ctx context.Context, w pgwire.ResultWriter, run func(pgwire.ResultWriter) error) error {
	inTxn := e.inClientTransaction()
	if e.r.blocking(e.shard) {
		switch decideFailover(true, inTxn, false, e.r.Buffered(e.shard), e.r.cfg.Buffering.PerShardCap) {
		case failoverFailTxn:
			e.dropStream()
			e.failTxn()
			return e.afterBatch(ctx, failoverInTxnError())
		case failoverRefuse:
			return e.afterBatch(ctx, e.bufferFull())
		case failoverWait:
			if ok, err := e.r.awaitConsistent(ctx, e.shard, false, e.r.cfg.Buffering.Window); err != nil {
				return e.afterBatch(ctx, err)
			} else if !ok {
				return e.afterBatch(ctx, pgwire.Errorf(codeConnectionFailure, "shard %s/%d has no serving primary", e.shard.Set, e.shard.ID))
			}
		}
	}
	cw := &countingWriter{w: w}
	err := run(cw)
	if e.writePauseRetryable(err, cw.wrote) {
		if rerr := e.reopenAfterWritePause(ctx); rerr != nil {
			err = rerr
		} else {
			e.r.cfg.Logger.Info("retrying statement after the cluster write pause", "session", e.sid, "shard", e.shard)
			cw.retrying()
			err = run(cw)
		}
	}
	err = e.namePauseThatCannotBeRetriedHere(err)
	switch decideFailover(isFailover(err), inTxn, cw.wrote, e.r.Buffered(e.shard), e.r.cfg.Buffering.PerShardCap) {
	case failoverFailTxn:
		e.dropStream()
		e.failTxn()
		err = failoverInTxnError()
	case failoverRefuse:
		err = e.bufferFull()
	case failoverWait:
		e.dropStream()
		ok, werr := e.r.awaitConsistent(ctx, e.shard, true, e.r.retryWindow(e.shard, err))
		switch {
		case werr != nil:
			err = werr
		case ok:
			e.r.cfg.Logger.Info("retrying statement after shard failover", "session", e.sid, "shard", e.shard)
			cw.retrying()
			err = run(cw)
		}
	}
	// A fence refusal must never reach the client as the pooler wrote it.
	// The wait can run out with the map still moving, and the one retry it
	// allows can meet the same flip, and both of those returned 55000 --
	// object_not_in_prerequisite_state, which names a state and no way out
	// of it. It is the router's own answer: nothing was written, nothing
	// committed, run it again. Only when no output has reached the client,
	// because after that "run it again" is advice the client cannot take.
	//
	// The declared reason, not the SQLSTATE. isStaleGeneration accepts a
	// bare 55000 for older poolers, and 55000 is also what a shard answers
	// while a rewrite is in progress -- a condition that clears on its own
	// and is not a fence. Turning that one into "retry the transaction"
	// would send a client round a loop with nothing at the end of it.
	//
	// Output already sent stops a retry, because output cannot be
	// replayed -- but naming the error is not retrying it. Inside a
	// transaction the whole thing is dead and retryable whatever reached
	// the client, so it is still told so. Outside one, a client that has
	// already consumed rows is left with the statement's own answer.
	if poolerReason(err) == pgshardv1.Reason_REASON_STALE_GENERATION && (!cw.wrote || inTxn) {
		err = failoverInTxnError()
	}
	return e.afterBatch(ctx, err)
}

// inClientTransaction reports whether the client is inside a transaction,
// which is not the same as this part being inside one. e.tx is the current
// part's status, and a transaction joining a shard it has not touched yet
// starts that part idle -- idle while the client is very much in a
// transaction, with writes already on another shard. Retry decisions are
// the client's contract, not one backend's, so they ask this.
func (e *Executor) inClientTransaction() bool {
	return e.tx != pgwire.TxIdle || e.multiShardTxn()
}

func (e *Executor) bufferFull() error { return bufferFullError(e.shard) }

// dropStream discards the pooler stream so the retry reacquires a backend
// from the refreshed endpoint and replays session state.
func (e *Executor) dropStream() {
	e.dropParked()
	e.pinned = false
	if e.conn == nil {
		e.tx = pgwire.TxIdle
		return
	}
	client := e.conn.client
	e.conn.abort()
	e.conn = nil
	// abort only cancels this side of the RPC. The pooler may still have
	// the session attached, and it refuses a second Execute stream while it
	// does; Release waits server-side for the old stream to detach, and
	// awaitRelease orders the next openStream behind it.
	e.releaseOn(client)
	e.tx = pgwire.TxIdle
}

// failTxn puts the session into PostgreSQL's failed-transaction state after
// a transaction was killed under it.
//
// dropStream leaves the session Idle, because that is what it is: there is
// no backend and no transaction. But the CLIENT still has one open, and
// telling it Idle is telling it the transaction ended cleanly. An
// application that retries the failed statement -- a statement-level 40001
// retry loop is the normal thing to write -- then sends COMMIT, which runs
// on a fresh backend as a no-op and answers with a COMMIT tag. pgx and JDBC
// both report success, and the work done before the failover is gone.
//
// PostgreSQL answers 'E' and 25P02 for every statement until the
// transaction is ended, which is what this makes the router do.
// It is called only where a transaction is open -- where decideFailover
// answered failoverFailTxn (failover.go:88-93), and where DDL a transaction
// ran on its own failed -- so there is no second condition to check here.
func (e *Executor) failTxn() { e.tx = pgwire.TxFailed }

// endFailedTxn ends a transaction that was killed under the client, without
// touching a shard: there is nothing left of it to end. COMMIT answers
// ROLLBACK, as PostgreSQL does for a commit in a failed transaction.
func (e *Executor) endFailedTxn(class StmtClass, w pgwire.ResultWriter) (bool, error) {
	if e.tx != pgwire.TxFailed || e.conn != nil {
		return false, nil
	}
	if class.Txn != plan.TxnCommit && class.Txn != plan.TxnRollback {
		return false, nil
	}
	e.finishTxn("ROLLBACK")
	return true, w.CommandComplete("ROLLBACK")
}

// endNoTxn answers a COMMIT or ROLLBACK sent when no transaction is in
// progress, the way PostgreSQL answers it: a warning, and the statement's
// own tag.
//
// It used to be routed to a shard, which is a round trip to learn what this
// session already knows. It is also the statement a client sends after its
// backend went away -- a retiring shard set takes its poolers with it, and
// poolerLost leaves the session idle with no transaction and nothing parked
// -- and routing it asked the shard that had just gone, so the client could
// not even end the transaction it had been thrown out of.
func (e *Executor) endNoTxn(class StmtClass, w pgwire.ResultWriter) (bool, error) {
	if e.tx != pgwire.TxIdle || e.conn != nil || e.multiShardTxn() {
		return false, nil
	}
	tag := ""
	switch class.Txn {
	case plan.TxnCommit:
		tag = "COMMIT"
	case plan.TxnRollback:
		tag = "ROLLBACK"
	default:
		return false, nil
	}
	if err := w.Notice(noActiveTransactionWarning()); err != nil {
		return true, err
	}
	return true, w.CommandComplete(tag)
}

// recoverFailedTxnToSavepoint reopens a failed transaction for a ROLLBACK
// TO SAVEPOINT, which PostgreSQL answers in the failed state -- it is how a
// client recovers a transaction instead of losing it.
//
// The router can only honour it where the transaction holds nothing on any
// shard, and then the prelude is the whole transaction: replaying it
// rebuilds the savepoint with everything before it, and the ROLLBACK TO
// runs on the backend afterwards and undoes what came after. That state is
// exactly what a failed sequential DDL leaves, because releaseUntouchedTxn
// gave the backend up before the migration was queued.
//
// A transaction a failover killed after it touched a shard keeps the 25P02:
// its work is gone, and answering ROLLBACK TO there would tell the client
// the statements before the savepoint survived when nothing did.
func (e *Executor) recoverFailedTxnToSavepoint(ctx context.Context, class StmtClass, fresh map[string]bool) error {
	if e.tx != pgwire.TxFailed || e.conn != nil || class.Txn != plan.TxnRollbackTo {
		return nil
	}
	if e.txnTouched || e.multiShardTxn() || len(e.txnPrelude) == 0 {
		return nil
	}
	if e.savepointIndex(class.Savepoint) < 0 {
		// 25P02, not PostgreSQL's 3B001 for an unknown savepoint, because
		// the router's record is not good enough to tell the two apart: a
		// batch that fails partway records NONE of the statements the
		// client already watched succeed, so a savepoint set in it is
		// missing here although it existed. Answering "does not exist"
		// would name the wrong problem in exactly the case where the
		// transaction is dead (PGS-959).
		return nil
	}
	e.tx = pgwire.TxIdle
	// The backend that opened the transaction is gone, and with it the
	// write-pause verdict of its BEGIN. The replayed BEGIN is a new one,
	// and pump takes the verdict again from it only if this is cleared: a
	// transaction re-opened under the pause cannot write, whatever the one
	// before it could (PGS-959).
	e.txnOnBackend, e.txnPreFence = false, false
	// fresh, not nil: the batch that carries the ROLLBACK TO parses its
	// own named statements, and a replay that parsed them here as well
	// would meet them again as the batch is forwarded -- PostgreSQL
	// refuses a second Parse of a live name with 42P05, which would fail
	// the recovery the client sent.
	if err := e.acquire(ctx, fresh); err != nil {
		// acquire only fails a transaction it can see, and it has just
		// been told there is none. Failed is what this session is if the
		// backend cannot be had: leaving it Idle would tell the client its
		// transaction ended cleanly.
		e.dropStream()
		e.failTxn()
		return err
	}
	return nil
}

// failedTxnBatch is endFailedTxn for the extended protocol: the COMMIT or
// ROLLBACK that ends a transaction whose backend is gone is answered here,
// without a shard, and answers ROLLBACK because that is what became of it.
//
// Without this the batch acquired a FRESH backend and ran the COMMIT on it.
// That backend had never heard of the transaction, so it answered a COMMIT
// tag, and the client -- pgx, JDBC, anything not using the simple protocol
// -- was told writes had landed that died with the old primary (PGS-957).
//
// Nothing else may reach a backend at all, and that is the rule rather than
// a list of message types: ACQUIRING one is itself the damage. pump relays
// the fresh backend's ReadyForQuery, so e.tx stops saying the transaction
// failed, and the COMMIT after it answers COMMIT. Probed: a Describe-only
// and a Close-only batch each reopened the whole defect that way, with no
// Parse, Bind or Execute in them for the checks above to see.
//
// Close is answered here instead, because it needs no backend: the physical
// statement it names died with the old one, and Close has already dropped
// the router's own record of it. Refusing it would break a driver clearing
// its statement cache on the way to the ROLLBACK it is about to send.
// The transaction ends on the EXECUTE of a COMMIT or ROLLBACK and on
// nothing else. Deciding it from the statement's presence anywhere in the
// batch let a Parse of a COMMIT -- which a driver sends on its own, and
// which PostgreSQL answers in an aborted transaction -- end the transaction
// before the client had executed anything. The session then read Idle, the
// client's real COMMIT was routed to a fresh backend, and the whole defect
// was back.
//
// A ROLLBACK TO executed before anything else is the one statement that
// may take a backend: recoverFailedTxnToSavepoint decides, as it does for
// the simple protocol, and a transaction it reopens runs the rest of the
// batch normally. The only Describe admitted is one of that portal, which
// the backend answers once the transaction is back.
func (e *Executor) failedTxnBatch(ctx context.Context, batch []*pgshardv1.ExecuteRequest, fresh map[string]bool, w pgwire.ResultWriter) (bool, error) {
	if e.tx != pgwire.TxFailed || e.conn != nil {
		return false, nil
	}
	ends, describedRollbackTo := false, false
	for _, req := range batch {
		switch r := req.Message.(type) {
		case *pgshardv1.ExecuteRequest_Parse, *pgshardv1.ExecuteRequest_Bind, *pgshardv1.ExecuteRequest_Close:
			// Answered below. A Parse or Bind that is still here is one
			// refuseStagedInFailedTransaction admitted, so it names a
			// COMMIT, a ROLLBACK or a ROLLBACK TO.
		case *pgshardv1.ExecuteRequest_Execute:
			if e.portalAnswersItself(r.Execute.Portal) {
				// explainBatch answers it further down without a backend.
				return false, nil
			}
			if class, ok := e.portalRollsBackToSavepoint(r.Execute.Portal); ok && !ends {
				return e.recoverBatchToSavepoint(ctx, class, fresh)
			}
			if !e.portalEndsTxn(r.Execute.Portal) {
				return true, failedTxnRefusal()
			}
			ends = true
		case *pgshardv1.ExecuteRequest_Describe:
			if _, ok := e.portalRollsBackToSavepoint(r.Describe.Name); ok && !ends && r.Describe.Kind == pgshardv1.Describe_KIND_PORTAL {
				describedRollbackTo = true
				continue
			}
			// PostgreSQL would answer one of a COMMIT, but a statement's
			// name here is the PHYSICAL one and the statement behind it
			// cannot be recovered to check -- and answering the wrong
			// shape is worse than refusing, because a driver caches it
			// for the life of the connection.
			return true, failedTxnRefusal()
		default:
			return true, failedTxnRefusal()
		}
	}
	if describedRollbackTo {
		// Described and never executed first: nothing reopened the
		// transaction, and there is no backend to answer the Describe.
		return true, failedTxnRefusal()
	}
	if ends {
		e.finishTxn("ROLLBACK")
	}
	// One walk, interleaved: every batch the router answers itself still
	// owes the client the ParseComplete and BindComplete a pooler would
	// have relayed, and they belong where the backend would have sent
	// them. Two passes put every completion ahead of every result.
	for _, req := range batch {
		var err error
		if _, ok := req.Message.(*pgshardv1.ExecuteRequest_Execute); ok {
			err = w.CommandComplete("ROLLBACK")
		} else {
			err = e.answerStagedCompletions(w, []*pgshardv1.ExecuteRequest{req})
		}
		if err != nil {
			return true, err
		}
	}
	return true, nil
}

// portalEndsTxn reports whether portal is bound to a COMMIT or a ROLLBACK.
// Membership is tested before the lookup: a portal that was never bound
// reads back as "" and would find the UNNAMED statement, so an Execute of
// an unbound portal could end the transaction on another statement's plan.
func (e *Executor) portalEndsTxn(portal string) bool {
	stmt, bound := e.portals[portal]
	if !bound {
		return false
	}
	st, ok := e.stmts[stmt]
	return ok && (st.plan.Class.Txn == plan.TxnCommit || st.plan.Class.Txn == plan.TxnRollback)
}

// portalRollsBackToSavepoint reports whether portal is bound to a ROLLBACK
// TO SAVEPOINT, and returns its class.
func (e *Executor) portalRollsBackToSavepoint(portal string) (StmtClass, bool) {
	stmt, bound := e.portals[portal]
	if !bound {
		return StmtClass{}, false
	}
	st, ok := e.stmts[stmt]
	return st.plan.Class, ok && st.plan.Class.Txn == plan.TxnRollbackTo
}

// recoverBatchToSavepoint answers failedTxnBatch for a batch whose first
// Execute is a ROLLBACK TO: handled, with the refusal, unless the
// transaction was reopened, and then the batch goes to the backend.
func (e *Executor) recoverBatchToSavepoint(ctx context.Context, class StmtClass, fresh map[string]bool) (bool, error) {
	if err := e.recoverFailedTxnToSavepoint(ctx, class, fresh); err != nil {
		return true, err
	}
	if e.tx == pgwire.TxFailed {
		return true, failedTxnRefusal()
	}
	return false, nil
}

// portalAnswersItself reports whether portal is bound to a statement the
// router answers without a backend, which stays available in a failed
// transaction.
func (e *Executor) portalAnswersItself(portal string) bool {
	stmt, bound := e.portals[portal]
	if !bound {
		return false
	}
	st, ok := e.stmts[stmt]
	return ok && st.plan.Explain != nil
}

func failedTxnRefusal() error {
	return pgwire.Errorf("25P02", "current transaction is aborted, commands ignored until end of transaction block")
}

// refuseInFailedTransaction answers what PostgreSQL answers while a
// transaction is in the failed state: nothing runs until it is ended.
//
// Also where the failed backend is still there, which would answer the
// same 25P02 -- but only for a statement sent to it. One routed to another
// shard moved the session there and ran, in a transaction PostgreSQL would
// have refused it in (PGS-978).
func (e *Executor) refuseInFailedTransaction(class StmtClass) error {
	if e.tx != pgwire.TxFailed {
		return nil
	}
	if class.Txn == plan.TxnCommit || class.Txn == plan.TxnRollback {
		return nil
	}
	// The backend recovers its own transaction to a savepoint.
	if e.conn != nil && class.Txn == plan.TxnRollbackTo {
		return nil
	}
	return pgwire.Errorf("25P02", "current transaction is aborted, commands ignored until end of transaction block")
}

// refuseStagedInFailedTransaction is refuseInFailedTransaction for a
// Parse, Bind or Execute, and also admits ROLLBACK TO, as PostgreSQL does
// at all three: whether the router can recover the transaction to that
// savepoint takes a backend, so failedTxnBatch decides it at Sync.
//
// A statement staged after a ROLLBACK TO in the same batch is admitted too
// where the backend is there: the recovery runs first at Sync, and the
// backend refuses what follows if it did not recover.
func (e *Executor) refuseStagedInFailedTransaction(class StmtClass) error {
	if class.Txn == plan.TxnRollbackTo {
		return nil
	}
	if e.conn != nil && e.batchRecovers() {
		return nil
	}
	return e.refuseInFailedTransaction(class)
}

func (e *Executor) batchRecovers() bool {
	for _, item := range e.batchExec {
		if item.class.Txn == plan.TxnRollbackTo {
			return true
		}
	}
	return false
}

// refuseSelfAnsweredInFailedTxn refuses nextval() over a global sequence in
// a transaction PostgreSQL has already failed.
//
// nextval() never reaches a backend that would refuse it, including on
// either protocol: the value comes
// from the router's own block of the global sequence. It would be handed
// out inside a transaction that cannot commit and never returned -- a gap
// in the sequence, which is allowed, opened at precisely the point where
// PostgreSQL would have done nothing at all.
//
// EXPLAIN (pgshard) is answered in a failed transaction on purpose and is
// not routed through here: it is how a user sees why the statement that
// failed was routed the way it was, without first losing the transaction.
func (e *Executor) refuseSelfAnsweredInFailedTxn() error {
	if e.tx != pgwire.TxFailed {
		return nil
	}
	return pgwire.Errorf("25P02", "current transaction is aborted, commands ignored until end of transaction block")
}

// releaseOn releases the session on the pooler that holds the current
// shard's backend, without waiting for it; acquire on the same shard waits
// through awaitRelease.
func (e *Executor) releaseOn(client pgshardv1.PoolerClient) {
	e.releaseOnShard(client, e.shard)
}

// releaseOnShard is releaseOn for a shard the session is not sitting on.
func (e *Executor) releaseOnShard(client pgshardv1.PoolerClient, sh Shard) {
	if client == nil {
		return
	}
	if e.releasing == nil {
		e.releasing = map[Shard]chan struct{}{}
	}
	done := make(chan struct{})
	prev := e.releasing[sh]
	e.releasing[sh] = done
	n, sid := e.statement.Load(), e.sid
	go func() {
		defer close(done)
		if prev != nil {
			<-prev
		}
		if err := releaseRPC(context.Background(), client, sid, n); err != nil {
			e.releaseFailed(sh, client, sid, n, err)
		}
	}()
}

// awaitRelease blocks until the async release of the current shard, if
// any, has finished.
func (e *Executor) awaitRelease(ctx context.Context) error {
	return e.awaitReleaseOf(ctx, e.shard)
}

func (e *Executor) awaitReleaseOf(ctx context.Context, sh Shard) error {
	done, ok := e.releasing[sh]
	if !ok {
		return nil
	}
	select {
	case <-done:
		delete(e.releasing, sh)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Parse implements pgwire.Executor: the message is buffered until Sync.
func (e *Executor) Parse(ctx context.Context, name, sql string, paramOIDs []uint32, w pgwire.ResultWriter) error {
	e.enterStatement(ctx)
	e.batchWriter = w
	return e.guard("Parse", func() error { return e.parse(ctx, name, sql, paramOIDs) })
}

func (e *Executor) parse(ctx context.Context, name, sql string, paramOIDs []uint32) error {
	if e.batchFailed {
		return nil
	}
	pl, err := e.planOp(ctx, sql, "parse")
	if err == nil && pl.Explain == nil {
		// Before the rest, as in simpleQuery: a session whose transaction
		// was killed needs to be told that, not that its statement is too
		// wide. PostgreSQL refuses at Parse, Bind and Execute alike
		// (postgres.c), so all three carry this.
		//
		// EXPLAIN (pgshard) is exempt for the reason
		// refuseSelfAnsweredInFailedTxn gives: it is how a user sees why
		// the statement that failed was routed the way it was, without
		// first losing the transaction. simpleQuery answers it before any
		// of these checks, and the extended protocol must not disagree.
		err = e.refuseStagedInFailedTransaction(pl.Class)
	}
	if err == nil {
		err = checkTransactionMode(pl.Class)
	}
	if err == nil {
		err = checkFanoutMode(pl.Class)
	}
	if err == nil {
		err = e.checkShardPin(pl.Class)
	}
	if err != nil {
		e.failBatch()
		return err
	}
	if !pl.Deferred && pl.Kind != plan.SessionLocal && pl.Kind != plan.MigrationKind {
		var err error
		if multiShard(pl) && !isReferenceWrite(pl) {
			_, err = pl.MultiShard()
		} else {
			err = e.aimBatch(pl, name)
		}
		if err != nil {
			e.failBatch()
			return err
		}
	}
	st := prepared{sql: sql, oids: paramOIDs, class: pl.Class, plan: pl, snap: e.currentSnapshot()}
	e.stmts[name] = st
	e.batchStmts = append(e.batchStmts, name)
	e.batch = append(e.batch, e.clientRequest(parseReq(e.physical(name), st.shardSQL(), st.shardOIDs())))
	e.batchDDL = append(e.batchDDL, migrationPlan(e.stmts, name))
	return nil
}

// multiShard reports whether a resolved plan runs on several shards.
func multiShard(pl plan.Plan) bool {
	return pl.Kind != plan.SessionLocal && pl.Kind != plan.Refuse && !pl.Deferred && len(pl.Shards) > 1
}

// aimBatch records the shard a resolved plan needs; one batch may only
// target one shard, or carry one multi-shard statement.
func (e *Executor) aimBatch(pl plan.Plan, stmt string) error {
	// The one place an extended-protocol statement's destinations are
	// final: parse comes here for a plan that needs no parameters, and
	// aimBound comes here after Bind has resolved the keys of one that
	// does. So the ceiling is checked on a plan that knows its shards.
	if err := e.checkFanout(pl); err != nil {
		return err
	}
	if multiShard(pl) {
		if _, err := pl.MultiShard(); err != nil && !isReferenceWrite(pl) {
			return err
		}
		if e.batchTarget != nil || e.batchScatter != nil && e.batchScatterStmt != stmt {
			return mixedBatchError()
		}
		e.batchScatter, e.batchScatterStmt = &pl, stmt
		return nil
	}
	target, err := e.target(pl)
	if err != nil {
		return err
	}
	if e.batchScatter != nil {
		return mixedBatchError()
	}
	if e.batchTarget != nil && *e.batchTarget != target {
		err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "statements of one batch target different shards (%s/%d and %s/%d)",
			e.batchTarget.Set, e.batchTarget.ID, target.Set, target.ID)
		err.Hint = "send a Sync between statements for different shards"
		return err
	}
	e.batchTarget = &target
	return nil
}

func mixedBatchError() error {
	err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "a multi-shard statement must be the only statement of its batch")
	err.Hint = "send a Sync before and after a statement that fans out to several shards"
	return err
}

func hasExecute(batch []*pgshardv1.ExecuteRequest) bool {
	for _, req := range batch {
		if _, ok := req.Message.(*pgshardv1.ExecuteRequest_Execute); ok {
			return true
		}
	}
	return false
}

// sequenceShape identifies the sequence rewrite of a plan, so a replan can
// tell whether the shards' prepared statement still matches.
func sequenceShape(pl plan.Plan) string {
	if pl.Sequences == nil {
		return ""
	}
	return pl.Sequences.SQL
}

// currentSnapshot is the snapshot plans of this session are made against;
// nil on the catalog shard set, whose plans never depend on one.
func (e *Executor) currentSnapshot() *snapshot.Snapshot {
	if e.catalogSession() {
		return nil
	}
	return e.r.cfg.Snapshot()
}

// Bind implements pgwire.Executor: a deferred plan is resolved here, once
// the shard key parameters are known. A statement prepared against an
// older snapshot is planned again first.
func (e *Executor) Bind(ctx context.Context, portal, statement string, paramFormats []int16, params [][]byte, resultFormats []int16, w pgwire.ResultWriter) error {
	e.enterStatement(ctx)
	e.batchWriter = w
	return e.guard("Bind", func() error { return e.bind(ctx, portal, statement, paramFormats, params, resultFormats) })
}

func (e *Executor) bind(ctx context.Context, portal, statement string, paramFormats []int16, params [][]byte, resultFormats []int16) error {
	if e.batchFailed {
		return nil
	}
	if err := e.replanStale(ctx, statement); err != nil {
		e.failBatch()
		return err
	}
	if st, ok := e.stmts[statement]; ok && st.plan.Explain == nil {
		if err := e.refuseStagedInFailedTransaction(st.plan.Class); err != nil {
			e.failBatch()
			return err
		}
	}
	if st, ok := e.stmts[statement]; ok && st.plan.Kind != plan.SessionLocal && st.plan.Kind != plan.MigrationKind {
		pl := st.plan
		var keys plan.Params = plan.BindParams{OIDs: st.paramOIDs(), Formats: paramFormats, Values: params}
		if fill := pl.Sequences; fill != nil {
			injected, values, err := e.sequenceValues(ctx, fill)
			if err != nil {
				e.failBatch()
				return err
			}
			for len(params) < fill.Base {
				params = append(params, nil)
			}
			keys = injectedParams{client: keys, base: fill.Base, values: values}
			paramFormats = extendFormats(paramFormats, fill.Base, len(fill.Names))
			params = append(params[:fill.Base:fill.Base], injected...)
		}
		e.batchBinds = append(e.batchBinds, batchBind{statement: statement, keys: keys})
		if err := e.aimBound(pl, statement, keys); err != nil {
			e.failBatch()
			return err
		}
	}
	e.portals[portal] = statement
	e.portalDDL[portal] = migrationPlan(e.stmts, statement)
	e.portalRun[portal] = portalRun{homeDDL: e.stmts[statement].plan.HomeDDL, executes: e.stmts[statement].plan.Class.Executes}
	e.batch = append(e.batch, e.clientRequest(bindReq(portal, e.physical(statement), paramFormats, params, resultFormats)))
	e.batchDDL = append(e.batchDDL, e.portalDDL[portal])
	return nil
}

// replanStale plans statement again when it was prepared against an older
// snapshot than the current one -- and refuses first if the snapshot it
// would plan against can no longer be trusted.
//
// The refusal has to be here rather than only in planOp. A router whose
// reloads are failing keeps the same snapshot, so the pointer comparison
// below finds nothing to replan and the statement runs on. Nothing else on
// the extended path consults the catalog again, so a statement prepared
// while the view was fresh could be bound and executed against a view that
// was hours old -- which is the case the whole staleness bound exists to
// prevent, and the one an online rewrite relies on: past MaxAge a router
// has either reloaded and hides the working column, or has stopped.
func (e *Executor) replanStale(ctx context.Context, statement string) error {
	return e.replanStaleAt(ctx, statement, e.currentSnapshot())
}

// replanStaleAt is replanStale against a snapshot the caller has already
// taken.
func (e *Executor) replanStaleAt(ctx context.Context, statement string, snap *snapshot.Snapshot) error {
	st, ok := e.stmts[statement]
	if !ok {
		return nil
	}
	if err := e.staleSnapshot(); err != nil {
		e.r.metrics.Refusals.WithLabelValues(codeStaleGeneration).Inc()
		return err
	}
	if snapshot.SamePlanning(st.snap, snap) {
		// Pinned even when nothing is replanned. Without it this path left
		// stmtSnap unset, so the statement was AIMED from the plan made
		// against st.snap while userSet and generation() read the live
		// snapshot: a reshard landing between this check and aimBound sent
		// the write to the old plan's shard id under the new set's
		// generation, which the fence cannot catch because the generation
		// is the current one.
		e.stmtSnap = snap
		return nil
	}
	pl, err := e.planOpAt(ctx, snap, st.sql, "parse")
	if err == nil && sequenceShape(pl) != sequenceShape(st.plan) {
		perr := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "the sequence columns of the table changed since statement %q was prepared", statement)
		perr.Hint = "prepare the statement again"
		err = perr
	}
	if err != nil {
		return err
	}
	st.plan, st.class, st.snap = pl, pl.Class, snap
	e.stmts[statement] = st
	return nil
}

// batchBind is one Bind of the batch in flight: what aimBound needs to
// target the statement again against a newer snapshot.
type batchBind struct {
	statement string
	keys      plan.Params
}

// aimBound resolves a deferred plan with the bound keys and aims the batch.
func (e *Executor) aimBound(pl plan.Plan, statement string, keys plan.Params) error {
	if pl.Deferred {
		var err error
		if pl, err = pl.Resolve(keys); err != nil {
			return err
		}
	}
	return e.aimBatch(pl, statement)
}

// reaim targets the batch again after the shard map moved while it waited
// out a write fence: every bound statement is planned against one current
// snapshot -- planOpAt stamps the batch from it -- and resolved with its
// recorded keys.
func (e *Executor) reaim(ctx context.Context, binds []batchBind) (*Shard, *plan.Plan, string, error) {
	e.batchTarget, e.batchScatter, e.batchScatterStmt = nil, nil, ""
	defer func() { e.batchTarget, e.batchScatter, e.batchScatterStmt = nil, nil, "" }()
	snap := e.currentSnapshot()
	for _, b := range binds {
		if err := e.replanStaleAt(ctx, b.statement, snap); err != nil {
			return nil, nil, "", err
		}
		st, ok := e.stmts[b.statement]
		if !ok || st.plan.Kind == plan.SessionLocal {
			continue
		}
		if err := e.aimBound(st.plan, b.statement, b.keys); err != nil {
			return nil, nil, "", err
		}
	}
	return e.batchTarget, e.batchScatter, e.batchScatterStmt, nil
}

// Describe implements pgwire.Executor.
func (e *Executor) Describe(_ context.Context, kind pgwire.DescribeKind, name string, w pgwire.ResultWriter) error {
	if e.batchFailed {
		return nil
	}
	e.batchWriter = w
	if kind == pgwire.DescribeStatement {
		e.describes = append(e.describes, name)
		// A Describe carries no keys and sets no target of its own, so a
		// batch that only describes stays on whatever shard the session
		// is on -- and since the replay stopped parsing statements of
		// OTHER shards there, that backend answers 26000 for a statement
		// the client prepared and never closed (PGS-967). Aim the batch
		// at the statement's own shard, where the replay will parse it.
		// Only when nothing else has aimed the batch: a Describe must not
		// fight the aim of a Parse or Bind beside it.
		if st, ok := e.stmts[name]; ok && e.batchTarget == nil && e.batchScatter == nil && !e.replayableHere(st.plan) {
			if err := e.aimBatch(st.plan, name); err != nil {
				e.failBatch()
				e.erredSinceSync = true
				return err
			}
		}
		name = e.physical(name)
	}
	e.batch = append(e.batch, describeReq(kind, name))
	return nil
}

// Execute implements pgwire.Executor.
func (e *Executor) Execute(ctx context.Context, portal string, maxRows int32, w pgwire.ResultWriter) error {
	return e.guard("Execute", func() error { return e.execute(ctx, portal, maxRows, w) })
}

func (e *Executor) execute(ctx context.Context, portal string, maxRows int32, w pgwire.ResultWriter) error {
	if e.batchFailed {
		return nil
	}
	e.batchWriter = w
	// A portal whose STATEMENT was closed still names it: Close of a
	// statement deletes e.stmts[name] and leaves e.portals pointing at it.
	// The lookup below then misses and every check it guards -- the
	// multi-shard rule, the shard pin, and refuseShardStatementAfterDDL --
	// is skipped, while the Execute is still appended to the batch. Probed:
	// the request reached a shard with no statement behind it, inside a
	// transaction that had already run DDL, which is the one thing that
	// guard exists to stop (PGS-883 item 2).
	//
	// PostgreSQL closes a statement's portals with it, so such a portal
	// cannot be executed there either; this says so rather than sending a
	// request whose answer would be the backend's confusion. Checked by
	// name, not through e.portals[portal], because a deleted entry reads
	// back as "" and would look up the UNNAMED statement -- executing one
	// portal's plan checks against another statement.
	stmt, bound := e.portals[portal]
	if bound {
		if _, live := e.stmts[stmt]; !live {
			e.failBatch()
			err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "portal %q cannot be executed: the prepared statement it was bound to has been closed", portal)
			err.Hint = "closing a prepared statement closes the portals bound to it; bind a new portal"
			return err
		}
	}
	// bound, not just the lookup: a portal this router never bound -- a
	// cursor DECLAREd in SQL lives on the backend and is named in no Bind
	// -- misses e.portals and reads back as "", the UNNAMED statement. The
	// Execute itself is forwarded either way and the backend answers the
	// real portal, but everything below acted on that unnamed statement:
	// measured on a real stack, a SET left unnamed and never executed was
	// recorded as session state and applied to the next backend the session
	// used (PGS-942). Not refused, because the portal does exist there.
	st, known := e.stmts[stmt]
	if !known || !bound {
		// Still forwarded; the backend answers its own portal. The place
		// is held so the completion it produces lines up.
		e.batchExec = append(e.batchExec, execItem{foreign: true})
	}
	if ok := known && bound; ok {
		if st.plan.Explain == nil {
			if err := e.refuseStagedInFailedTransaction(st.plan.Class); err != nil {
				e.failBatch()
				return err
			}
		}
		if multiShard(st.plan) && e.batchScatter == nil {
			e.failBatch()
			err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "a multi-shard portal must be bound and executed in the same batch")
			err.Hint = "send Bind and Execute before one Sync"
			return err
		}
		if st.class.SetGUC {
			// Again here, not only at Parse. A named statement can be
			// prepared while the session is idle and executed after BEGIN,
			// and the pin must not change under a transaction whichever
			// message carried it.
			if err := e.checkShardPin(st.class); err != nil {
				e.failBatch()
				return err
			}
			g := gucEntry{name: st.class.GUCName, sql: st.sql, value: st.class.GUCValue, searchPath: st.class.SearchPath}
			e.staged = append(e.staged, g)
			defer e.injectSearchPath(g)
		}
		if err := e.refuseShardStatementAfterDDL(st.plan); err != nil {
			e.failBatch()
			return err
		}
		// Checked where the portal RUNS, not only where its statement was
		// planned: a statement Parsed before a reshard started and
		// executed during it -- a driver's statement cache, a portal kept
		// across Syncs in a transaction, an EXECUTE of an SQL-level
		// statement PREPAREd earlier or earlier in this very batch -- is
		// otherwise never checked again, and creates its relation on the
		// source mid-copy (PGS-975).
		if run := e.portalRun[portal]; !e.catalogSession() && (run.homeDDL || e.executesHomeDDL(run.executes)) {
			if err := e.checkHomeDDL(ctx); err != nil {
				e.failBatch()
				return err
			}
		}
		// local drives noteExecuted, which is what sets txnTouched. An
		// EXECUTE or FETCH is SessionLocal but does touch the shard, so
		// calling it local left refuseDDLInTransaction believing the
		// transaction had run nothing there (PGS-883 item 6).
		e.batchExec = append(e.batchExec, execItem{sql: st.sql, local: st.plan.Kind == plan.SessionLocal && !st.plan.Class.RunsOnAShard, class: st.class, tables: st.plan.Tables})
	}
	e.batch = append(e.batch, executeReq(portal, maxRows))
	e.batchDDL = append(e.batchDDL, e.portalDDL[portal])
	return nil
}

// routerSearchPathStmt names the statement the router parses to reapply
// the startup search_path inside a batch; client statement names are
// namespaced by physical() and cannot collide with it.
const routerSearchPathStmt = "pgshard_search_path"

// injectSearchPath queues a set_config right after the Execute of a staged
// RESET, so a statement pipelined behind the RESET in the same Sync runs
// under the startup search_path the planner routed it with.
func (e *Executor) injectSearchPath(g gucEntry) {
	if e.startupSearchPath == nil || !resetsSearchPath(g) {
		return
	}
	if e.batchInject == nil {
		e.batchInject = map[int][]*pgshardv1.ExecuteRequest{}
	}
	e.batchInject[len(e.batch)-1] = []*pgshardv1.ExecuteRequest{
		parseReq(routerSearchPathStmt, searchPathSQL(e.startupSearchPath), nil),
		bindReq(routerSearchPathStmt, routerSearchPathStmt, nil, nil, nil),
		executeReq(routerSearchPathStmt, 0),
		closeReq(pgwire.DescribeStatement, routerSearchPathStmt),
	}
}

// Close implements pgwire.Executor.
func (e *Executor) Close(_ context.Context, kind pgwire.DescribeKind, name string, w pgwire.ResultWriter) error {
	if e.batchFailed {
		return nil
	}
	e.batchWriter = w
	if kind == pgwire.DescribeStatement {
		delete(e.stmts, name)
		name = e.physical(name)
	} else {
		delete(e.portals, name)
		delete(e.portalDDL, name)
		delete(e.portalRun, name)
	}
	e.batch = append(e.batch, e.clientRequest(closeReq(kind, name)))
	return nil
}

func (e *Executor) failBatch() {
	for _, name := range e.batchStmts {
		delete(e.stmts, name)
	}
	e.staged = e.staged[:e.stagedMark]
	e.batch, e.batchStmts, e.batchFailed, e.batchWriter, e.batchDDL = nil, nil, true, nil, nil
	e.batchTarget, e.batchExec, e.describes, e.batchBinds = nil, nil, nil, nil
	e.completions, e.clientReqs = nil, nil
	e.batchInject = nil
	e.batchScatter, e.batchScatterStmt = nil, ""
}

// Sync implements pgwire.Executor: it ships the buffered batch followed by
// Sync and relays every response.
func (e *Executor) Sync(ctx context.Context) error {
	e.enterStatement(ctx)
	defer e.endStatement()
	err := e.guard("Sync", func() error { return e.sync(ctx) })
	e.failTxnTheBackendDidNotFail(ctx)
	return err
}

// Flush answers a client's Flush: the extended batch staged so far runs and
// its responses reach the client, with no ReadyForQuery and the portals
// left open, so a pipelined client gets its rows before it sends Sync.
//
// Only a plain single-shard read batch takes this path. A scatter, an
// injected statement, a write, a transaction control statement and a
// session-effect statement each need the machinery Sync runs around them,
// and for those the Flush stays what it has always been -- nothing, with
// the client's answers arriving at Sync as before. That is not a
// regression, and a wrong guess here is a hung session.
func (e *Executor) Flush(ctx context.Context, w pgwire.ResultWriter) error {
	e.enterStatement(ctx)
	return e.guard("Flush", func() error { return e.flush(ctx, w) })
}

func (e *Executor) flush(ctx context.Context, w pgwire.ResultWriter) error {
	if e.batchFailed || len(e.batch) == 0 || e.batchScatter != nil {
		return nil
	}
	// flush is the OTHER place a staged batch reaches a pooler, and it
	// shipped without this guard: a Describe or a Close terminated by
	// Flush stages no Execute, so every condition below passed, acquire
	// opened a fresh backend, and its relayed ReadyForQuery cleared the
	// failed state before the client's Sync was even read. The COMMIT
	// after it answered COMMIT.
	//
	// Refused rather than left for Sync, because a Flush is what a
	// pipelined client BLOCKS on: declining silently here would hang it
	// until it sent a Sync it has no reason to send. pgwire reports this
	// and skips to Sync, which is what PostgreSQL does after an error in
	// an extended batch. A transaction-control statement never reaches
	// here anyway -- the loop below declines those.
	if e.tx == pgwire.TxFailed && e.conn == nil {
		return failedTxnRefusal()
	}
	for _, injected := range e.batchInject {
		if len(injected) > 0 {
			return nil
		}
	}
	for _, item := range e.batchExec {
		if item.class.Write || item.class.Txn != plan.TxnNone || item.class.Session != plan.SessionNone {
			return nil
		}
	}
	batch, executed := e.batch, e.batchExec
	if e.batchWriter != nil {
		w = e.batchWriter
	}
	target := e.shard
	if e.batchTarget != nil {
		target = *e.batchTarget
	}
	fresh := map[string]bool{}
	for _, name := range e.batchStmts {
		fresh[name] = true
	}
	// The messages are about to be sent, so the Sync that follows must not
	// send them again. The portals stay: that is the point of a Flush.
	e.batch, e.batchStmts, e.batchWriter, e.batchTarget, e.batchExec, e.batchBinds = nil, nil, nil, nil, nil, nil
	e.batchDDL = nil
	e.pendingDescribes, e.describes = e.describes, nil
	if w == nil {
		w = discardWriter{}
	}
	if err := e.moveTo(ctx, target); err != nil {
		return e.afterBatch(ctx, err)
	}
	if err := e.acquire(ctx, fresh); err != nil {
		return e.afterBatch(ctx, err)
	}
	// A flushed batch spans two client messages, so the backend has to be
	// the same one when the Sync arrives.
	if err := e.ensurePinned(ctx); err != nil {
		return e.afterBatch(ctx, err)
	}
	for _, req := range batch {
		if err := e.send(req); err != nil {
			return e.afterBatch(ctx, err)
		}
	}
	if err := e.send(flushReq()); err != nil {
		return e.afterBatch(ctx, err)
	}
	e.backendOpen = true
	e.execDone = 0
	err := e.pump(ctx, w)
	e.noteBatch(executed)
	return e.afterBatch(ctx, err)
}

// closeBackendBatch ends a batch this session flushed but never synced.
// The backend is mid-batch with an implicit transaction open, so its Sync
// has to be sent even though nothing is staged: without it the next
// statement joins a transaction the client believes ended.
func (e *Executor) closeBackendBatch(ctx context.Context) error {
	if !e.backendOpen {
		return nil
	}
	e.backendOpen = false
	if e.conn == nil {
		return nil
	}
	if err := e.send(syncReq()); err != nil {
		return e.afterBatch(ctx, err)
	}
	return e.afterBatch(ctx, e.pump(ctx, discardWriter{}))
}

func (e *Executor) sync(ctx context.Context) error {
	if e.batchFailed {
		e.batchFailed = false
		return e.closeBackendBatch(ctx)
	}
	batch, w, executed, binds, ddl := e.batch, e.batchWriter, e.batchExec, e.batchBinds, e.batchDDL
	target := e.shard
	if e.batchTarget != nil {
		target = *e.batchTarget
	}
	scatterPlan, scatterStmt := e.batchScatter, e.batchScatterStmt
	e.batchScatter, e.batchScatterStmt = nil, ""
	inject := e.batchInject
	e.batchInject = nil

	pin := false
	fresh := map[string]bool{}
	parsed := e.batchStmts
	for _, name := range parsed {
		pin = pin || name != ""
		fresh[name] = true
	}
	for _, item := range executed {
		pin = pin || item.class.Session == plan.SessionPrepare
	}
	e.batch, e.batchStmts, e.batchWriter, e.batchTarget, e.batchExec, e.batchBinds = nil, nil, nil, nil, nil, nil
	e.batchDDL = nil
	e.pendingDescribes, e.describes = e.describes, nil
	if len(batch) == 0 {
		return e.closeBackendBatch(ctx)
	}
	if w == nil {
		w = discardWriter{}
	}
	if handled, err := e.failedTxnBatch(ctx, batch, fresh, w); handled {
		return e.afterBatch(ctx, err)
	}
	for _, item := range executed {
		if item.class.Write {
			before := e.currentSnapshot()
			if err := e.gateWrite(ctx, target, item.tables); err != nil {
				e.staged = e.staged[:min(e.stagedMark, len(e.staged))]
				return e.afterBatch(ctx, err)
			}
			if e.currentSnapshot() != before && len(binds) > 0 {
				var batchTarget *Shard
				var err error
				if batchTarget, scatterPlan, scatterStmt, err = e.reaim(ctx, binds); err != nil {
					e.staged = e.staged[:min(e.stagedMark, len(e.staged))]
					return e.afterBatch(ctx, err)
				}
				if batchTarget != nil {
					target = *batchTarget
				}
			}
			break
		}
	}
	if scatterPlan != nil && isReferenceWrite(*scatterPlan) && !hasExecute(batch) {
		// Parse/Describe of a reference write is answered by the current
		// shard alone; the fan-out starts with Execute.
		scatterPlan = nil
	}
	if handled, err := e.nextvalBatch(ctx, batch, parsed, w); handled {
		e.staged = e.staged[:min(e.stagedMark, len(e.staged))]
		return e.afterBatch(ctx, err)
	}
	if handled, err := e.explainBatch(batch, parsed, w); handled {
		e.staged = e.staged[:min(e.stagedMark, len(e.staged))]
		return e.afterBatch(ctx, err)
	}
	if handled, err := e.migrationBatch(ctx, batch, ddl, w); handled {
		e.staged = e.staged[:min(e.stagedMark, len(e.staged))]
		return e.afterBatch(ctx, err)
	}
	if scatterPlan != nil {
		e.staged = e.staged[:min(e.stagedMark, len(e.staged))]
		if isReferenceWrite(*scatterPlan) {
			st, ok := e.stmts[scatterStmt]
			if !ok {
				return e.afterBatch(ctx, pgwire.Errorf("26000", "prepared statement %q does not exist", scatterStmt))
			}
			// shardSQL, not the client's text: a reference write on a
			// table under an online rewrite carries a column list the
			// client did not write, and RETURNING * is expanded to the
			// visible columns.
			if err := e.answerStagedCompletions(w, batch); err != nil {
				return e.afterBatch(ctx, err)
			}
			return e.afterBatch(ctx, e.referenceWrite(ctx, *scatterPlan, unnamedBatch(st.shardSQL(), st.shardOIDs(), batch), w))
		}
		if err := e.answerStagedCompletions(w, batch); err != nil {
			return e.afterBatch(ctx, err)
		}
		return e.afterBatch(ctx, e.scatterBatch(ctx, *scatterPlan, scatterStmt, batch, w))
	}
	if handled, err := e.txnControlBatch(ctx, batch, executed, w); handled {
		e.staged = e.staged[:min(e.stagedMark, len(e.staged))]
		return e.afterBatch(ctx, err)
	}
	e.unsent = fresh
	err := e.moveTo(ctx, target)
	e.unsent = nil
	if err != nil {
		e.staged = e.staged[:min(e.stagedMark, len(e.staged))]
		return e.afterBatch(ctx, err)
	}
	return e.withFailover(ctx, w, func(cw pgwire.ResultWriter) error {
		if err := e.acquire(ctx, fresh); err != nil {
			e.staged = e.staged[:min(e.stagedMark, len(e.staged))]
			return err
		}
		if pin || len(e.staged) > e.stagedMark {
			if err := e.ensurePinned(ctx); err != nil {
				e.staged = e.staged[:e.stagedMark]
				return err
			}
		}
		// A BEGIN earlier in this same batch opens the transaction the
		// write belongs to, even though it has not run yet, so the walk
		// tracks it rather than reading e.tx alone.
		inTxn := e.tx != pgwire.TxIdle
		for _, item := range executed {
			if item.class.Txn == plan.TxnBegin {
				inTxn = true
			}
			if item.class.Write {
				if err := e.noteWriteIn(ctx, inTxn); err != nil {
					e.staged = e.staged[:e.stagedMark]
					return err
				}
				break
			}
		}
		// A batch that Binds the unnamed statement without Parsing it is
		// binding one an earlier batch left behind. PostgreSQL keeps the
		// unnamed statement until something replaces it, but the router
		// does not pin for it and the pooler hands an unreserved session
		// whatever backend is free, having reset it -- so the Bind
		// reached a backend that had never seen the Parse and the client
		// got "prepared statement does not exist" for a sequence the
		// protocol allows. A driver doing a separate prepare round trip
		// produces exactly that sequence.
		//
		// Carried rather than pinned: re-parsing one statement costs a
		// message, while pinning would cost every such session its
		// transaction pooling, including the single-batch
		// Parse-Bind-Execute that works correctly today. The extra
		// ParseComplete is suppressed like any other request the router
		// sends on its own account: it is not in clientReqs, so the
		// completion queue records it as the router's and the pump does
		// not relay it.
		if st, ok := e.stmts[""]; ok && !fresh[""] && bindsUnnamed(batch) {
			if err := e.send(parseReq("", st.shardSQL(), st.shardOIDs())); err != nil {
				return err
			}
		}
		var hidden []bool
		for i, req := range batch {
			if err := e.send(req); err != nil {
				return err
			}
			if _, ok := req.Message.(*pgshardv1.ExecuteRequest_Execute); ok {
				hidden = append(hidden, false)
			}
			for _, inj := range inject[i] {
				if err := e.send(inj); err != nil {
					return err
				}
				if _, ok := inj.Message.(*pgshardv1.ExecuteRequest_Execute); ok {
					hidden = append(hidden, true)
				}
			}
		}
		if err := e.send(syncReq()); err != nil {
			return err
		}
		e.backendOpen = false
		e.hiddenExec = hidden
		e.execDone = 0
		err := e.pump(ctx, cw)
		e.hiddenExec = nil
		e.noteBatch(executed)
		return err
	})
}

// noteBatch records the statements of a pumped batch that ran: all of them
// when it succeeded, and the ones before its failure when it did not.
//
// Recording only a batch that succeeded in full lost what the client had
// already been told: the pump relays each CommandComplete as it arrives, so
// a batch whose last statement failed had shown the client its SAVEPOINT
// and its write succeed, and the router recorded neither -- the
// transaction read as untouched, with no savepoint (PGS-959). The
// statements after a failure never ran: PostgreSQL skips to the Sync.
func (e *Executor) noteBatch(executed []execItem) {
	for _, item := range executed[:min(e.execDone, len(executed))] {
		// A foreign item carries no statement and an empty class, so it
		// records no session effect -- and noteExecuted marks the
		// transaction as having touched this shard, which is true: the
		// backend ran its own portal.
		e.noteExecuted(item.sql, item.local, item.class)
		e.noteSessionEffect(item.class, item.sql)
	}
}

// bindsUnnamed reports whether the batch binds the unnamed statement.
func bindsUnnamed(batch []*pgshardv1.ExecuteRequest) bool {
	for _, req := range batch {
		if b, ok := req.Message.(*pgshardv1.ExecuteRequest_Bind); ok && b.Bind.GetStatement() == "" {
			return true
		}
	}
	return false
}

// reapplyStartupSearchPath keeps the executing backend's search_path in step
// with routing after g ran: a RESET restores the startup search_path on this
// session, while the backend — which never saw the startup options — would
// fall back to the server default the planner did not route with.
func (e *Executor) reapplyStartupSearchPath(ctx context.Context, g gucEntry) error {
	if e.startupSearchPath == nil || !resetsSearchPath(g) {
		return nil
	}
	if err := e.send(simpleQuery(searchPathSQL(e.startupSearchPath))); err != nil {
		return err
	}
	if err := e.pump(ctx, discardWriter{}); err != nil {
		return fmt.Errorf("router: reapplying the startup search_path: %w", err)
	}
	return nil
}

// afterBatch settles staged GUCs and releases the pinned backend when a
// transaction just ended.
func (e *Executor) afterBatch(ctx context.Context, err error) error {
	// A batch that ended holds no more completions, whether every one of
	// them arrived or the batch failed with some still owed. Carrying them
	// into the next batch would relay its first completion against this
	// batch's answer.
	e.completions, e.clientReqs = nil, nil
	if e.tx == pgwire.TxIdle {
		e.txnPrelude, e.txnTouched = nil, false
		e.txnOnBackend, e.txnPreFence = false, false
		e.txnRanDDL = false
		e.wroteHere, e.gid = false, ""
		e.savepoints = nil
		e.dropParked()
		switch {
		case err != nil || strings.HasPrefix(e.lastTag, "ROLLBACK"):
			e.staged = nil
		default:
			e.applyStaged()
		}
	}
	if e.txnEnded && e.pinned {
		e.txnEnded = false
		// The transaction's outcome is already on the wire. A cancel or
		// deadline that reached the statement after that, or a pooler that
		// cannot be reached for the release, must not report it as a
		// connection failure: 08006 tells a client the outcome is unknown.
		if rerr := e.release(context.WithoutCancel(ctx)); rerr != nil {
			e.releaseFailed(e.shard, nil, e.sid, e.statement.Load(), rerr)
		}
	}
	e.renewSid()
	e.txnEnded = false
	e.stagedMark = len(e.staged)
	return err
}

func (e *Executor) applyStaged() {
	for _, g := range e.staged {
		if g.name == "" {
			e.gucs = nil
			continue
		}
		kept := e.gucs[:0]
		for _, old := range e.gucs {
			if old.name != g.name {
				kept = append(kept, old)
			}
		}
		kept = append(kept, g)
		e.gucs = kept
	}
	e.staged = nil
}

// shardLatency is the latency observer of the session's current shard.
// Resolving it needed a formatted label and a lookup in the metric's label
// map, on a path that runs once per statement; a session stays on one
// shard for long stretches, so it is resolved when the shard changes.
func (e *Executor) shardLatency() prometheus.Observer {
	e.resolveShardMetrics()
	return e.latency
}

func (e *Executor) resolveShardMetrics() {
	if e.latency != nil && e.latencyOf == e.shard {
		return
	}
	e.latencyOf = e.shard
	label := e.shard.Set + "/" + strconv.FormatInt(int64(e.shard.ID), 10)
	e.latency = e.r.metrics.ShardLatency.WithLabelValues(label)
	e.statements = e.r.metrics.ShardStatements.WithLabelValues(label)
	e.rows = e.r.metrics.ShardRows.WithLabelValues(label)
	e.errors = e.r.metrics.ShardErrors.WithLabelValues(label)
}

// generation stamps a request with the shard map generation and primary
// epoch of the snapshot the statement in flight was planned against, or the
// live one when nothing is in flight (an out-of-band Sync or Close, and a
// catalog session, whose plans do not depend on a snapshot).
func (e *Executor) generation() *pgshardv1.Generation {
	if e.stmtSnap == nil {
		return e.r.cfg.Poolers.Generation(e.shard)
	}
	return &pgshardv1.Generation{
		ShardMapGeneration: uint64(e.stmtSnap.ShardMapGeneration),
		PrimaryEpoch:       uint64(e.stmtSnap.Serving[snapshot.ShardKey{ShardSet: e.shard.Set, ShardID: e.shard.ID}].Epoch),
	}
}

func (e *Executor) client() (pgshardv1.PoolerClient, error) { return e.r.cfg.Poolers.Client(e.shard) }

// acquire opens the pooler stream when needed; statements named in fresh
// are being parsed by the current batch and are not replayed.
func (e *Executor) acquire(ctx context.Context, fresh map[string]bool) error {
	if e.conn != nil {
		return nil
	}
	client, err := e.client()
	if err != nil {
		return err
	}
	if err := e.awaitRelease(ctx); err != nil {
		return err
	}
	e.renewSid()
	if err := e.refuseLostShard(e.shard); err != nil {
		if e.inClientTransaction() {
			// The transaction cannot go on, and has to be seen to have
			// failed: ending it as a failover does leaves ReadyForQuery
			// saying so, and a COMMIT answering ROLLBACK. Dropping only
			// the parked parts would leave the client, told to retry the
			// transaction, outside one -- where its COMMIT succeeds.
			e.dropStream()
			e.failTxn()
		}
		return err
	}
	ps, err := openStream(e.ctx, client)
	if err != nil {
		return e.poolerRefused(err)
	}
	e.conn = ps
	if e.needsPin() {
		if err := e.ensurePinned(ctx); err != nil {
			return err
		}
		if err := e.replay(ctx, fresh); err != nil {
			return err
		}
	}
	return e.replayPrelude(ctx)
}

// replayPrelude reopens a transaction that moved shards before touching
// any: the prelude runs on the new backend, which the pooler then holds.
func (e *Executor) replayPrelude(ctx context.Context) error {
	if len(e.txnPrelude) == 0 || e.tx != pgwire.TxIdle {
		return nil
	}
	for _, sql := range e.txnPrelude {
		if err := e.send(simpleQuery(sql)); err != nil {
			return err
		}
		if err := e.pump(ctx, discardWriter{}); err != nil {
			return fmt.Errorf("router: replaying transaction prelude: %w", err)
		}
	}
	// A RESET in the prelude restores the STARTUP search_path on this
	// session and the SERVER default on the backend, which is the whole
	// reason reapplyStartupSearchPath exists -- and the prelude does not
	// record what that sent, because it is not a statement the client
	// issued. Replaying the prelude alone therefore left the transaction
	// running with a search_path the planner had not routed with, and the
	// ROLLBACK that precedes the replay had already undone the original
	// reapply. Re-asserting the session's EFFECTIVE path covers both the
	// simple and the extended path, and is a no-op when the prelude ends
	// on a SET rather than a RESET.
	if e.startupSearchPath != nil {
		if err := e.send(simpleQuery(searchPathSQL(e.searchPath()))); err != nil {
			return err
		}
		if err := e.pump(ctx, discardWriter{}); err != nil {
			return fmt.Errorf("router: restoring the search_path after replaying the prelude: %w", err)
		}
	}
	return nil
}

func (e *Executor) ensurePinned(ctx context.Context) error {
	if e.pinned {
		return nil
	}
	client, err := e.client()
	if err != nil {
		return err
	}
	resp, err := client.Reserve(ctx, &pgshardv1.ReserveRequest{SessionId: e.sid, Generation: e.generation(), Statement: e.statement.Load()})
	if pe := resp.GetError(); pe != nil {
		// See scatter.go: a pooler from before the refusal moved to the
		// status channel.
		return toPgwireError(pe)
	}
	if err != nil {
		if pe := poolerRefusal(err); pe != nil {
			return toPgwireError(pe)
		}
		return e.poolerRefused(err)
	}
	e.pinned = true
	return nil
}

// replay re-establishes session GUCs and named prepared statements on a
// freshly pinned backend.
// sessionSettings is the ordered SQL that puts a fresh backend into this
// session's state: the startup search_path first, then every SET the
// session has run. Any backend the session's statements reach needs all of
// it, not a part - SET ROLE decides which grants and row-level security
// policies apply, and running somewhere that missed it means running as
// the login role instead.
//
// The backend never saw the client's startup options, so a startup
// search_path is applied first, and re-applied after every replayed RESET:
// on this session RESET restores the startup value, while on the backend
// it would restore the server default the planner did not route with.
func (e *Executor) sessionSettings() []string {
	var parts []string
	if e.startupSearchPath != nil {
		parts = append(parts, searchPathSQL(e.startupSearchPath))
	}
	for _, g := range e.gucs {
		parts = append(parts, strings.TrimRight(strings.TrimSpace(g.sql), ";"))
		if e.startupSearchPath != nil && resetsSearchPath(g) {
			parts = append(parts, searchPathSQL(e.startupSearchPath))
		}
	}
	return parts
}

func (e *Executor) replay(ctx context.Context, skip map[string]bool) error {
	parts := e.sessionSettings()
	if len(parts) > 0 {
		if err := e.send(simpleQuery(strings.Join(parts, "; "))); err != nil {
			return err
		}
		if err := e.pump(ctx, discardWriter{}); err != nil {
			return fmt.Errorf("router: replaying session settings: %w", err)
		}
	}
	for _, p := range e.sqlPrepared {
		if err := e.send(simpleQuery(p.sql)); err != nil {
			return err
		}
		if err := e.pump(ctx, discardWriter{}); err != nil {
			return fmt.Errorf("router: replaying prepared statement %q: %w", p.name, err)
		}
	}
	return e.replayStatements(ctx, skip)
}

// replayableHere reports whether a prepared statement can be parsed on the
// shard the session is on. A plan that resolves to one other shard cannot:
// its objects need not exist here. Anything unresolved -- a deferred plan
// waiting for its keys, a scatter, a session-local statement -- is replayed,
// because it either runs everywhere or runs wherever the session is.
func (e *Executor) replayableHere(pl plan.Plan) bool {
	if pl.Deferred || multiShard(pl) || pl.Kind == plan.SessionLocal {
		return true
	}
	target, err := e.target(pl)
	return err != nil || target == e.shard
}

// replayStatements parses the named statements the current backend lacks;
// skip names those it already has.
func (e *Executor) replayStatements(ctx context.Context, skip map[string]bool) error {
	n := 0
	for name, st := range e.stmts {
		if name == "" || skip[name] {
			continue
		}
		if !e.replayableHere(st.plan) {
			// A statement that belongs to another shard is not parsed
			// here. Its objects may not exist here at all -- a query over
			// a database's local_schemas lives on the home shard alone --
			// and PARSE resolves names, so replaying it failed the whole
			// replay and left the session unable to run anything: measured
			// with pgroll, where a cached SELECT pgroll.latest_version()
			// broke every later statement of the session once it moved
			// shard (PGS-882). It is parsed again when the session is on
			// the shard that has it, which is where it routes.
			continue
		}
		if err := e.send(parseReq(e.physical(name), st.shardSQL(), st.shardOIDs())); err != nil {
			return err
		}
		n++
	}
	if n == 0 {
		return nil
	}
	if err := e.send(syncReq()); err != nil {
		return err
	}
	if err := e.pump(ctx, discardWriter{}); err != nil {
		return fmt.Errorf("router: replaying prepared statements: %w", err)
	}
	return nil
}

func (e *Executor) send(req *pgshardv1.ExecuteRequest) error {
	if err := e.conn.send(req, e.sid, e.generation(), e.ident, e.backendDatabase(), e.statement.Load()); err != nil {
		return e.poolerLost(err)
	}
	e.noteCompletion(req)
	return nil
}

// backendDatabase is the database the pooler opens a backend on, which is
// not always the one the client named. The catalog is a SCHEMA inside an
// ordinary database, so `dbname=pgshard` -- the name the guide documents and
// the only way to edit the desired-state tables -- has no database of that
// name behind it, and asking for one gets PostgreSQL's 3D000. Planning still
// uses the client's name: it is the logical database, and only the backend
// needs the physical one.
func (e *Executor) backendDatabase() string {
	if e.catalogSession() {
		return e.r.cfg.CatalogPhysicalDatabase
	}
	return e.info.Database
}

// poolerLost drops the stream and reports 08006; the next statement
// reacquires a backend and replays session state.
func (e *Executor) poolerLost(cause error) error {
	e.dropParked()
	e.pinned = false
	if e.conn != nil {
		client := e.conn.client
		e.conn.abort()
		e.conn = nil
		e.releaseOn(client)
	}
	e.tx = pgwire.TxIdle
	e.staged, e.stagedMark = nil, 0
	if _, isPG := errors.AsType[*pgwire.Error](cause); isPG {
		return cause
	}
	if pe := tooLargeError("pooler stream", cause); pe != nil {
		return pe
	}
	return pgwire.Errorf(codeConnectionFailure, "pooler connection lost: %v", cause)
}

// poolerRefused is poolerLost for a connection that could not be opened at
// all: nothing was sent, so the statement is safe to retry after failover.
func (e *Executor) poolerRefused(cause error) error {
	err := e.poolerLost(cause)
	if status.Code(cause) == codes.Unavailable || attachRaced(cause) {
		return &refusedError{err}
	}
	return err
}

// attachRaced reports the pooler refusing a stream because the session it
// names is still attached. The release before a reacquire normally orders
// that away, but the release has its own timeout and can give up first, so
// the refusal has to stay retryable rather than reach the client as a dead
// connection.
func attachRaced(cause error) bool {
	if status.Code(cause) != codes.FailedPrecondition {
		return false
	}
	st := status.Convert(cause)
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetReason() == pooler.ReasonSessionAttached {
			return true
		}
	}
	// A pooler from before the structured reason only carries the text.
	// Kept as a fallback for a rolling upgrade, not as the contract: a
	// reworded sentence must not turn a race worth waiting through into a
	// dead connection reported to the client.
	return strings.Contains(st.Message(), "already has an Execute stream")
}

// refusedError marks a pooler that refused the connection before any
// statement was sent.
type refusedError struct{ error }

func (r *refusedError) Unwrap() error { return r.error }

// pump relays responses until ReadyForQuery. Errors from the backend are
// returned after the batch is drained so pgwire reports them itself.
func (e *Executor) pump(ctx context.Context, w pgwire.ResultWriter) error {
	start := time.Now()
	observer := e.shardLatency()
	e.statements.Inc()
	defer func() { observer.Observe(time.Since(start).Seconds()) }()
	var firstErr error
	n := e.beginStatement(ctx)
	onCancel := func() { e.cancelStatement(context.Background(), n) }
	for {
		resp, err := e.conn.recv(ctx, onCancel)
		if err != nil {
			return e.poolerLost(err)
		}
		var werr error
		switch m := resp.Message.(type) {
		case *pgshardv1.ExecuteResponse_RowDescription:
			werr = w.RowDescription(fieldDescriptions(m.RowDescription.Fields))
		case *pgshardv1.ExecuteResponse_DataRow:
			e.rows.Inc()
			if !e.hiddenNow() {
				werr = w.DataRow(rowValues(m.DataRow))
			}
		case *pgshardv1.ExecuteResponse_DataRows:
			for _, row := range m.DataRows.GetRows() {
				e.rows.Inc()
				if !e.hiddenNow() {
					if werr = w.DataRow(rowValues(row)); werr != nil {
						break
					}
				}
			}
		case *pgshardv1.ExecuteResponse_CommandComplete:
			if !e.popHidden() {
				e.execDone++
				e.lastTag = m.CommandComplete.Tag
				werr = w.CommandComplete(m.CommandComplete.Tag)
			}
		case *pgshardv1.ExecuteResponse_EmptyQuery:
			if !e.popHidden() {
				e.execDone++
				werr = w.EmptyQueryResponse()
			}
		case *pgshardv1.ExecuteResponse_FlushComplete:
			// The pooler's own marker that a Flush has been answered in
			// full. A Flush produces no ReadyForQuery, so this is where
			// the pump stops; the client is told nothing, which is what
			// Flush means on the wire.
			return firstErr
		case *pgshardv1.ExecuteResponse_PortalSuspended:
			// A suspended portal ends this Execute's response in place of
			// CommandComplete, so it advances the injected-Execute queue
			// the same way. Dropping it altogether left the client
			// believing the result set had ended, so a row-limited fetch
			// returned short with no error.
			if !e.popHidden() {
				e.execDone++
				werr = w.PortalSuspended()
			}
		case *pgshardv1.ExecuteResponse_Error:
			if firstErr == nil {
				e.errors.Inc()
				firstErr = toPgwireError(m.Error.GetError())
			}
		case *pgshardv1.ExecuteResponse_Notice:
			werr = w.Notice(toNotice(m.Notice.GetNotice()))
		case *pgshardv1.ExecuteResponse_Notification:
			n := m.Notification
			werr = w.Notification(&pgproto3.NotificationResponse{PID: n.GetPid(), Channel: n.GetChannel(), Payload: n.GetPayload()})
		case *pgshardv1.ExecuteResponse_ParameterDescription:
			oids := e.clientOIDs(m.ParameterDescription.ParamOids)
			e.inferParams(m.ParameterDescription.ParamOids)
			werr = w.ParameterDescription(oids)
		case *pgshardv1.ExecuteResponse_NoData:
			werr = w.NoData()
		case *pgshardv1.ExecuteResponse_CopyInResponse:
			werr = e.copyIn(w, m.CopyInResponse)
		case *pgshardv1.ExecuteResponse_CopyOutResponse:
			werr = w.CopyOut(byte(m.CopyOutResponse.Format), toUint16s(m.CopyOutResponse.ColumnFormats))
		case *pgshardv1.ExecuteResponse_CopyData:
			werr = w.CopyData(m.CopyData.Data)
		case *pgshardv1.ExecuteResponse_CopyDone:
			werr = w.CopyDone()
		case *pgshardv1.ExecuteResponse_ReadyForQuery:
			prev := e.tx
			e.tx = txStatus(m.ReadyForQuery.TxnStatus)
			// The statement is over; anything sent after it is stamped from
			// the live snapshot again.
			e.stmtSnap = nil
			if prev == pgwire.TxIdle && e.tx != pgwire.TxIdle && !e.txnOnBackend {
				e.txnOnBackend, e.txnPreFence = true, !e.r.writeFenced(nil)
			}
			if prev != pgwire.TxIdle && e.tx == pgwire.TxIdle {
				e.txnEnded = true
			}
			// After an error the backend skipped everything up to the
			// Sync, so what it still owes it will never send -- and
			// neither does the router: it relays completions, it does not
			// invent them (PGS-974). Answering them told the client that
			// statements after the failure had been parsed and bound,
			// which PostgreSQL never says.
			//
			// Only a batch that succeeded is owed anything at its end: a
			// pooler older than this router does not forward
			// CloseComplete, so a client that closed a statement would
			// wait for an answer that never comes. Answering the
			// stragglers at the end of the batch is later than PostgreSQL
			// would, and it is the difference between a mixed-version
			// rollout being slightly out of order and being hung.
			if firstErr != nil {
				e.completions = nil
			} else if werr := e.answerOwedCompletions(w); werr != nil {
				return werr
			}
			return firstErr
		case *pgshardv1.ExecuteResponse_ParameterStatus:
			werr = e.reportParameter(w, m.ParameterStatus.GetName(), m.ParameterStatus.GetValue())
		case *pgshardv1.ExecuteResponse_ParseComplete:
			if e.popCompletion() {
				werr = w.ParseComplete()
			}
		case *pgshardv1.ExecuteResponse_BindComplete:
			if e.popCompletion() {
				werr = w.BindComplete()
			}
		case *pgshardv1.ExecuteResponse_CloseComplete:
			if e.popCompletion() {
				werr = w.CloseComplete()
			}
		default:
			e.r.cfg.Logger.Warn("unexpected pooler response", "session", e.sid, "type", fmt.Sprintf("%T", resp.Message))
		}
		if werr != nil {
			return werr
		}
	}
}

// clientRequest marks a request as one the client sent, so that the
// completion the backend answers it with is relayed rather than swallowed.
// Every other Parse, Bind and Close in a batch is the router's own -- a
// re-Parse of the unnamed statement, a search_path reapplication, a
// scatter's per-shard copy -- and PostgreSQL's answer to it is not the
// client's to see.
func (e *Executor) clientRequest(req *pgshardv1.ExecuteRequest) *pgshardv1.ExecuteRequest {
	if e.clientReqs == nil {
		e.clientReqs = map[*pgshardv1.ExecuteRequest]bool{}
	}
	e.clientReqs[req] = true
	return req
}

// noteCompletion records, in send order, whether the completion req will be
// answered with belongs to the client. The queue is read by the pump as the
// completions arrive, which is what keeps them in the backend's order
// instead of the order the messages were received in.
func (e *Executor) noteCompletion(req *pgshardv1.ExecuteRequest) {
	owed := owedCompletion{client: e.clientReqs[req]}
	switch req.GetMessage().(type) {
	case *pgshardv1.ExecuteRequest_Parse:
		owed.write = pgwire.ResultWriter.ParseComplete
	case *pgshardv1.ExecuteRequest_Bind:
		owed.write = pgwire.ResultWriter.BindComplete
	case *pgshardv1.ExecuteRequest_Close:
		owed.write = pgwire.ResultWriter.CloseComplete
	default:
		return
	}
	e.completions = append(e.completions, owed)
}

// owedCompletion is one completion the backend owes: which message answers
// it, and whether the request was the client's, since the router's own
// Parses, Binds and Closes are answered too and those answers are not the
// client's to see.
type owedCompletion struct {
	write  func(pgwire.ResultWriter) error
	client bool
}

// popCompletion reports whether the completion that just arrived is the
// client's. An unexpected one -- a completion with nothing owed -- is not
// relayed: the router would otherwise pass on a message the client cannot
// place.
func (e *Executor) popCompletion() bool {
	if len(e.completions) == 0 {
		return false
	}
	c := e.completions[0]
	e.completions = e.completions[1:]
	return c.client
}

// answerStagedCompletions writes the Parse, Bind and Close completions the
// client is owed for a batch the router answers itself rather than sending
// to a pooler -- a scatter, a reference write, a migration, a multi-shard
// transaction control statement. Such a batch carries one statement (a
// multi-shard statement may not share a batch), so writing them in staged
// order, before the statement's own output, is the order PostgreSQL would
// have answered in.
func (e *Executor) answerStagedCompletions(w pgwire.ResultWriter, batch []*pgshardv1.ExecuteRequest) error {
	for _, req := range batch {
		if !e.clientReqs[req] {
			continue
		}
		var err error
		switch req.GetMessage().(type) {
		case *pgshardv1.ExecuteRequest_Parse:
			err = w.ParseComplete()
		case *pgshardv1.ExecuteRequest_Bind:
			err = w.BindComplete()
		case *pgshardv1.ExecuteRequest_Close:
			err = w.CloseComplete()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// answerOwedCompletions writes what the batch still owes the client when
// the batch has ended without the backend's answer for it.
func (e *Executor) answerOwedCompletions(w pgwire.ResultWriter) error {
	owed := e.completions
	e.completions = nil
	for _, c := range owed {
		if !c.client {
			continue
		}
		if err := c.write(w); err != nil {
			return err
		}
	}
	return nil
}

// hiddenNow reports whether the responses arriving belong to a request the
// router injected into the batch rather than to the client's.
func (e *Executor) hiddenNow() bool {
	return len(e.hiddenExec) > 0 && e.hiddenExec[0]
}

// popHidden advances past the Execute whose CommandComplete just arrived
// and reports whether it was an injected one.
func (e *Executor) popHidden() bool {
	if len(e.hiddenExec) == 0 {
		return false
	}
	h := e.hiddenExec[0]
	e.hiddenExec = e.hiddenExec[1:]
	return h
}

// clientOIDs strips the parameters the router injected for sequence values
// from the description of the statement being described.
func (e *Executor) clientOIDs(oids []uint32) []uint32 {
	if len(e.pendingDescribes) == 0 {
		return oids
	}
	st, ok := e.stmts[e.pendingDescribes[0]]
	if !ok || st.plan.Sequences == nil || len(oids) < st.plan.Sequences.Base {
		return oids
	}
	return oids[:st.plan.Sequences.Base]
}

// inferParams attributes a ParameterDescription to the next described
// statement of the batch.
func (e *Executor) inferParams(oids []uint32) {
	if len(e.pendingDescribes) == 0 {
		return
	}
	name := e.pendingDescribes[0]
	e.pendingDescribes = e.pendingDescribes[1:]
	if st, ok := e.stmts[name]; ok {
		st.inferred = append([]uint32(nil), oids...)
		e.stmts[name] = st
	}
}

// copyIn relays a COPY FROM STDIN: client chunks go to the pooler until the
// client ends the transfer.
func (e *Executor) copyIn(w pgwire.ResultWriter, resp *pgshardv1.CopyInResponse) error {
	in, err := w.CopyIn(byte(resp.Format), toUint16s(resp.ColumnFormats))
	if err != nil {
		return err
	}
	for {
		data, err := in.Next()
		switch {
		case err == nil:
			if err := e.send(copyDataReq(data)); err != nil {
				return err
			}
		case errors.Is(err, pgwire.ErrCopyFail):
			return e.send(copyFailReq("COPY terminated by client"))
		case errors.Is(err, io.EOF):
			return e.send(copyDoneReq())
		default:
			_ = e.send(copyFailReq("client connection lost"))
			return err
		}
	}
}

// beginStatement arms the backend cancel for a statement, once, and returns
// the statement's number. A statement pumps more than once -- the backend is
// acquired, the session state replayed, the transaction prelude reopened,
// and only then the statement runs -- and arming on each of those let one
// cancellation send a second Cancel. The context is an identity here, never
// waited on: it says which statement a pump belongs to.
func (e *Executor) beginStatement(ctx context.Context) uint64 {
	e.cancelMu.Lock()
	defer e.cancelMu.Unlock()
	n := e.enterStatementLocked(ctx)
	if e.conn != nil {
		e.noteCancelTargetLocked(e.conn.client)
	}
	return n
}

// enterStatement numbers the statement ctx belongs to. Every executor entry
// that can reach a pooler calls it first, so the requests a statement sends
// before its first pump already carry its number.
func (e *Executor) enterStatement(ctx context.Context) {
	e.cancelMu.Lock()
	defer e.cancelMu.Unlock()
	e.enterStatementLocked(ctx)
}

func (e *Executor) enterStatementLocked(ctx context.Context) uint64 {
	if e.cancelFor != ctx {
		e.cancelFor = ctx
		e.statement.Add(1)
		e.cancelSent.Store(false)
	}
	return e.statement.Load()
}

// cancelStatement asks the poolers to interrupt statement n. A cancel for a
// statement that has already ended is dropped here, and one that ends while
// the Cancel is on its way is dropped by the pooler, which has by then seen
// the next statement's number: without either, the cancel landed on
// whatever the session ran next.
func (e *Executor) cancelStatement(ctx context.Context, n uint64) {
	// A cancel that arrives once every participant has prepared has nothing
	// left to act on. Sending it anyway can interrupt the COMMIT PREPARED or
	// ROLLBACK PREPARED that follows.
	if e.uncancellable.Load() {
		return
	}
	e.cancelMu.Lock()
	// With no pooler to send to yet -- the statement's request is on its
	// way but its pump has not recorded the stream -- nothing is sent, and
	// the statement's one cancel must not be spent on it: the pump's own
	// cancel, a moment later, is the one that can reach the backend.
	if n != e.statement.Load() || len(e.cancelTo) == 0 || !e.cancelSent.CompareAndSwap(false, true) {
		e.cancelMu.Unlock()
		return
	}
	targets := append([]pgshardv1.PoolerClient(nil), e.cancelTo...)
	sid := e.sid
	e.cancelMu.Unlock()
	cctx, cancel := context.WithTimeout(ctx, releaseTimeout)
	defer cancel()
	for _, client := range targets {
		if _, err := client.Cancel(cctx, &pgshardv1.CancelRequest{SessionId: sid, Statement: n}); err != nil {
			e.r.cfg.Logger.Warn("cancel failed", "session", sid, "err", err)
		}
	}
}

// releaseFailed records that a Release of sid on shard sh failed. The pooler
// may still hold the backend under sid, and on its own would free it only
// after its reserve timeout, so the release is retried in the background;
// the session itself moves to a new name before it next reaches a pooler.
// client is the pooler the Release went to, or nil when there was none.
func (e *Executor) releaseFailed(sh Shard, client pgshardv1.PoolerClient, sid string, statement uint64, err error) {
	e.lostMu.Lock()
	if e.lostOn == nil {
		e.lostOn = map[Shard]string{}
	}
	e.lostOn[sh] = sid
	e.lostMu.Unlock()
	e.releaseLost.Store(true)
	e.r.cfg.Logger.Warn("releasing a pooler session failed; retrying it in the background under its old name", "session", sid, "shard", sh, "err", err)
	go e.retryRelease(sh, client, sid, statement)
}

// retryRelease releases sid again, to the pooler that refused it when that
// is known: a failover's new pooler answers a Release of a session it never
// had with success, while the old one keeps the backend.
//
// Not while the session still goes by sid. Until it is renamed -- a failure
// in the middle of a transaction waits for its end -- it may reserve the
// same shard again under sid in the same statement, and a late Release
// carrying that statement's number would end the new reservation.
func (e *Executor) retryRelease(sh Shard, client pgshardv1.PoolerClient, sid string, statement uint64) {
	delay := releaseRetryDelay
	for range releaseRetries {
		time.Sleep(delay)
		delay *= 2
		if e.goesBy(sid) {
			continue
		}
		c := client
		if c == nil {
			var err error
			if c, err = e.r.cfg.Poolers.Client(sh); err != nil {
				continue
			}
		}
		if releaseRPC(context.Background(), c, sid, statement) == nil {
			return
		}
	}
	e.r.cfg.Logger.Warn("gave up releasing a pooler session; the pooler frees it at its reserve timeout", "session", sid, "shard", sh)
}

func (e *Executor) goesBy(sid string) bool {
	e.cancelMu.Lock()
	defer e.cancelMu.Unlock()
	return !e.ended && e.sid == sid
}

// renewSid moves the session to a name no pooler holds anything under, once
// a Release of the current one failed and the session holds nothing under
// it itself.
func (e *Executor) renewSid() {
	if e.conn != nil || e.pinned || len(e.parked) > 0 || e.tx != pgwire.TxIdle || !e.releaseLost.CompareAndSwap(true, false) {
		return
	}
	e.lostMu.Lock()
	e.lostOn = nil
	e.lostMu.Unlock()
	e.renewals++
	e.cancelMu.Lock()
	e.sid = e.r.prefix + "-" + strconv.FormatUint(e.info.ID, 10) + "-" + strconv.FormatUint(e.renewals, 10)
	e.cancelMu.Unlock()
}

// refuseLostShard refuses to reach sh under the session's current name
// when a Release of that name on sh failed and the session could not be
// renamed since -- it is inside a transaction. The pooler may still hold
// the session there, and a stream or Reserve under the same name would
// attach to the backend the router believed it had given back: its
// prepared statements, and a transaction a dropped part had begun.
func (e *Executor) refuseLostShard(sh Shard) error {
	e.lostMu.Lock()
	lost, ok := e.lostOn[sh]
	e.lostMu.Unlock()
	if !ok || !e.goesBy(lost) {
		return nil
	}
	return &pgwire.Error{Severity: "ERROR", Code: codeRetryable,
		Message: fmt.Sprintf("the pooler session on shard %s/%d could not be released; retry the transaction", sh.Set, sh.ID),
		Detail:  "Reaching that shard again in this transaction could attach to the backend the release left behind."}
}

// release detaches the stream and returns the pinned backend to the pool.
func (e *Executor) release(ctx context.Context) error {
	if e.conn != nil {
		e.conn.close()
		e.conn = nil
	}
	e.pinned = false
	client, err := e.client()
	if err != nil {
		return err
	}
	return releaseRPC(ctx, client, e.sid, e.statement.Load())
}

func releaseRPC(ctx context.Context, client pgshardv1.PoolerClient, sid string, statement uint64) error {
	rctx, cancel := context.WithTimeout(ctx, releaseTimeout)
	defer cancel()
	// One channel. Release reports nothing in its response -- a session the
	// pooler does not know is already released -- so the only failure is the
	// call itself, and it arrives as a gRPC status. The branch that used to
	// read an embedded error could never run, which made the release path
	// look like it handled a case it did not.
	if _, err := client.Release(rctx, &pgshardv1.ReleaseRequest{SessionId: sid, Statement: statement}); err != nil {
		return pgwire.Errorf(codeConnectionFailure, "pooler release failed: %v", err)
	}
	return nil
}

// Release implements pgwire.Executor: the session is over.
func (e *Executor) Release() {
	e.r.forget(e)
	e.cancelMu.Lock()
	e.ended = true
	e.cancelMu.Unlock()
	e.dropParked()
	pinned := e.pinned
	if e.conn != nil {
		e.conn.close()
		e.conn = nil
	}
	if pinned {
		if client, err := e.client(); err == nil {
			// Unnumbered: the session is over, so there is no later
			// reservation this could be mistaken for.
			if err := releaseRPC(context.Background(), client, e.sid, 0); err != nil {
				e.releaseFailed(e.shard, client, e.sid, 0, err)
			}
		}
	}
	e.cancel()
	zero(e.ident.ScramClientKey)
	zero(e.ident.ScramServerKey)
}

func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

type discardWriter struct{}

func (discardWriter) RowDescription([]pgproto3.FieldDescription) error  { return nil }
func (discardWriter) DataRow([][]byte) error                            { return nil }
func (discardWriter) CommandComplete(string) error                      { return nil }
func (discardWriter) EmptyQueryResponse() error                         { return nil }
func (discardWriter) ParameterDescription([]uint32) error               { return nil }
func (discardWriter) NoData() error                                     { return nil }
func (discardWriter) PortalSuspended() error                            { return nil }
func (discardWriter) ParseComplete() error                              { return nil }
func (discardWriter) BindComplete() error                               { return nil }
func (discardWriter) CloseComplete() error                              { return nil }
func (discardWriter) Notice(*pgproto3.NoticeResponse) error             { return nil }
func (discardWriter) Notification(*pgproto3.NotificationResponse) error { return nil }
func (discardWriter) ParameterStatus(string, string) error              { return nil }
func (discardWriter) CopyIn(byte, []uint16) (pgwire.CopyInStream, error) {
	return nil, pgwire.Errorf(pgwire.CodeProtocolViolation, "unexpected COPY while replaying session state")
}
func (discardWriter) CopyOut(byte, []uint16) error { return nil }
func (discardWriter) CopyData([]byte) error        { return nil }
func (discardWriter) CopyDone() error              { return nil }

// reportParameter forwards a changed GUC_REPORT setting to the client.
//
// PostgreSQL sends one whenever such a setting changes, including when a
// SET LOCAL is undone by ROLLBACK or a savepoint is rolled back, and drivers
// read timestamps, intervals and escaped text according to what they were
// last told. A router that drops these leaves a driver parsing results by
// the values it was given at startup, on a backend that no longer holds
// them.
//
// Only changes reach the client. The router replays session state onto
// every backend it moves a session to, and a backend that reports back what
// the session already asked for is not news.
func (e *Executor) reportParameter(w pgwire.ResultWriter, name, value string) error {
	if name == "" {
		return nil
	}
	if e.reported == nil {
		e.reported = map[string]string{}
	}
	if old, ok := e.reported[name]; ok && old == value {
		return nil
	}
	e.reported[name] = value
	return w.ParameterStatus(name, value)
}

// noteCancelTarget records a pooler this session has work on, so a cancel
// reaches it. The list is the transaction's rather than the statement's: a
// multi-shard transaction leaves participants running on shards the current
// statement is not on, and every one of them has work to stop. It is
// cleared when the transaction ends, and a cancel that arrives for a pooler
// with nothing to cancel costs a round trip and no more.
func (e *Executor) noteCancelTarget(c pgshardv1.PoolerClient) {
	e.cancelMu.Lock()
	defer e.cancelMu.Unlock()
	e.noteCancelTargetLocked(c)
}

func (e *Executor) noteCancelTargetLocked(c pgshardv1.PoolerClient) {
	if c == nil {
		return
	}
	for _, have := range e.cancelTo {
		if have == c {
			return
		}
	}
	e.cancelTo = append(e.cancelTo, c)
}

// forgetCancelTargets drops the poolers of a transaction that is over. The
// next statement records the stream it runs on.
func (e *Executor) forgetCancelTargets() {
	e.cancelMu.Lock()
	e.cancelTo = nil
	e.cancelMu.Unlock()
}

// migrationPlan is the migration statement name plans to, or nil.
func migrationPlan(stmts map[string]prepared, name string) *plan.Plan {
	if st, ok := stmts[name]; ok && st.plan.Kind == plan.MigrationKind {
		pl := st.plan
		return &pl
	}
	return nil
}
