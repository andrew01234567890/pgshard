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
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/router/plan"
)

const (
	codeConnectionFailure = "08006"
	maxIdentifierLen      = 63
	releaseTimeout        = 5 * time.Second
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
type sqlPreparedStmt struct{ name, sql string }

type savepointMark struct {
	name   string
	staged int
}

type execItem struct {
	sql    string
	local  bool
	class  StmtClass
	tables []snapshot.TableKey
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
	sid  string
	home Shard
	// shard is the shard the session's stream is (or will next be) on.
	shard Shard
	ident *pgshardv1.UserIdentity

	ctx    context.Context
	cancel context.CancelFunc

	conn     *poolerStream
	pinned   bool
	tx       pgwire.TxStatus
	lastTag  string
	txnEnded bool
	// cancelSent dedupes cancel requests within one batch.
	cancelSent atomic.Bool

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

	batch       []*pgshardv1.ExecuteRequest
	batchStmts  []string
	batchFailed bool
	batchWriter pgwire.ResultWriter
	// batchTarget is the shard the current extended batch resolved to.
	batchTarget *Shard
	// batchScatter is the multi-shard plan the current batch is bound to,
	// and batchScatterStmt the one statement it may carry.
	batchScatter     *plan.Plan
	batchScatterStmt string
	batchExec        []execItem
	// batchInject maps a batch index to the requests sent right after it:
	// the search_path reapplication a staged RESET needs before the next
	// pipelined statement runs. hiddenExec flags, per Execute on the wire,
	// the ones whose responses are the router's own and not the client's.
	batchInject map[int][]*pgshardv1.ExecuteRequest
	hiddenExec  []bool
	// batchBinds records the binds of the batch so the targets can be
	// aimed again when the shard map moves while the batch waits out a
	// write fence.
	batchBinds []batchBind
	// describes lists the statements a batch asked to Describe, in order,
	// so ParameterDescription replies can be attributed.
	describes []string
	// pendingDescribes is describes for the batch in flight.
	pendingDescribes []string

	// txnPrelude holds the session-local statements (BEGIN, SET, ...) run
	// since the transaction opened while nothing has touched a shard yet,
	// so the transaction can still move to the shard of its first real
	// statement.
	txnPrelude []string
	txnTouched bool

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
		stmts: map[string]prepared{}, portals: map[string]string{},
	}
	e.startupSearchPath = startupSearchPath(info.Params["options"])
	return e
}

// resetsSearchPath reports whether g restores the search_path default
// (RESET search_path, SET search_path TO DEFAULT, or RESET ALL).
func resetsSearchPath(g gucEntry) bool {
	return g.name == "" || (g.name == "search_path" && g.searchPath == nil)
}

// searchPathSQL renders the statement that applies path on a backend.
func searchPathSQL(path []string) string {
	quoted := make([]string, len(path))
	for i, s := range path {
		quoted[i] = `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}
	value := strings.Join(quoted, ", ")
	return "SELECT set_config('search_path', '" + strings.ReplaceAll(value, "'", "''") + "', false)"
}

// startupSearchPath extracts search_path from a startup "options" parameter
// (-c search_path=a,b or --search_path=a,b); other options are left alone.
func startupSearchPath(options string) []string {
	fields := strings.Fields(options)
	var path []string
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
		name, value, ok := strings.Cut(f, "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(name), "search_path") {
			continue
		}
		path = []string{}
		for _, part := range strings.Split(value, ",") {
			part = strings.Trim(strings.TrimSpace(part), `"`)
			if part != "" {
				path = append(path, part)
			}
		}
	}
	return path
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
func (e *Executor) Home() Shard {
	if e.catalogSession() {
		return e.home
	}
	if snap := e.r.cfg.Snapshot(); snap != nil {
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
func (e *Executor) userSet() string { return e.Home().Set }

// Shard reports the shard the session's stream is on.
func (e *Executor) Shard() Shard { return e.shard }

// planSession describes this session to the planner. Sessions on the
// catalog shard set see no table placement and plan everything onto their
// home shard.
func (e *Executor) planSession() plan.Session {
	sess := plan.Session{Database: e.info.Database, HomeShard: e.Home().ID, ID: e.info.ID, SearchPath: e.searchPath()}
	if !e.catalogSession() {
		sess.Snapshot = e.r.cfg.Snapshot()
	}
	return sess
}

// plan plans sql for this session. Sessions on the catalog shard set run
// DDL directly on their home shard: the migration model covers the
// databases of the default shard set only.
func (e *Executor) plan(ctx context.Context, sql string) (plan.Plan, error) {
	return e.planOp(ctx, sql, "simple")
}

func (e *Executor) planOp(ctx context.Context, sql, opcode string) (plan.Plan, error) {
	pl, err := e.r.cfg.Planner.Plan(ctx, e.planSession(), sql)
	if err == nil && pl.Kind == plan.MigrationKind && e.home.Set != DefaultShardSet {
		pl.Kind, pl.Shards, pl.Migration = plan.Unsharded, []int32{e.home.ID}, nil
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
	case pl.Deferred:
		return Shard{}, pgwire.Errorf(pgwire.CodeInternalError, "router: deferred plan was not resolved")
	case len(pl.Shards) != 1:
		return Shard{}, pgwire.Errorf(pgwire.CodeInternalError, "router: %s plan over %d shards has no single target", pl.Kind, len(pl.Shards))
	}
	return Shard{Set: e.userSet(), ID: pl.Shards[0]}, nil
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
		return e.switchPart(ctx, target)
	}
	e.dropStream()
	e.shard = target
	return nil
}

// noteExecuted records what a completed statement did to the open
// transaction: session-local statements join the prelude, anything else
// pins the transaction to the current shard.
func (e *Executor) noteExecuted(sql string, local bool) {
	if e.tx == pgwire.TxIdle {
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
		err = pgwire.Errorf(pgwire.CodeInternalError, "internal error while processing the statement; the session state was reset")
	}()
	return run()
}

// SimpleQuery implements pgwire.Executor.
func (e *Executor) SimpleQuery(ctx context.Context, sql string, w pgwire.ResultWriter) error {
	return e.guard("SimpleQuery", func() error { return e.simpleQuery(ctx, sql, w) })
}

func (e *Executor) simpleQuery(ctx context.Context, sql string, w pgwire.ResultWriter) error {
	pl, err := e.plan(ctx, sql)
	if err != nil {
		return err
	}
	if pl.Kind == plan.MigrationKind {
		return e.afterBatch(ctx, e.runMigration(ctx, pl, w))
	}
	if pl.NextVal != "" {
		return e.afterBatch(ctx, e.answerNextval(ctx, pl.NextVal, true, true, false, w))
	}
	if pl.Class.Write {
		before := e.currentSnapshot()
		if err := e.gateWrite(ctx, pl.Tables); err != nil {
			return e.afterBatch(ctx, err)
		}
		if e.currentSnapshot() != before {
			// The shard map moved while the statement waited: plan it
			// against the map it will run on.
			if pl, err = e.r.cfg.Planner.Plan(ctx, e.planSession(), sql); err != nil {
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
			e.noteExecuted(sql, pl.Kind == plan.SessionLocal)
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
		e.sqlPrepared = append(e.sqlPrepared, sqlPreparedStmt{name: class.SessionName, sql: sql})
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
	inTxn := e.tx != pgwire.TxIdle
	if e.r.blocking(e.shard) {
		switch decideFailover(true, inTxn, false, e.r.Buffered(e.shard), e.r.cfg.Buffering.PerShardCap) {
		case failoverFailTxn:
			e.dropStream()
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
	switch decideFailover(isFailover(err), inTxn, cw.wrote, e.r.Buffered(e.shard), e.r.cfg.Buffering.PerShardCap) {
	case failoverFailTxn:
		e.dropStream()
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
			err = run(cw)
		}
	}
	return e.afterBatch(ctx, err)
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
	e.detachAsync(client)
	e.tx = pgwire.TxIdle
}

// detachAsync releases the session so the next stream cannot race the one
// just aborted. abort only cancels this side of the RPC; the pooler may
// still have the session attached, and it refuses a second Execute stream
// while it does. Release waits server-side for the old stream to detach,
// and awaitRelease orders the next openStream behind it.
func (e *Executor) detachAsync(client pgshardv1.PoolerClient) {
	e.releaseOn(client)
}

// releaseAsync returns the pinned backend of the current shard without
// waiting for the pooler; acquire on the same shard waits for it.
func (e *Executor) releaseAsync() {
	client, err := e.client()
	if err != nil {
		return
	}
	e.releaseOn(client)
}

// releaseOn releases the session on a specific pooler.
func (e *Executor) releaseOn(client pgshardv1.PoolerClient) {
	if client == nil {
		return
	}
	if e.releasing == nil {
		e.releasing = map[Shard]chan struct{}{}
	}
	done := make(chan struct{})
	prev := e.releasing[e.shard]
	e.releasing[e.shard] = done
	go func() {
		defer close(done)
		if prev != nil {
			<-prev
		}
		_ = releaseRPC(context.Background(), client, e.sid)
	}()
}

// awaitRelease blocks until the async release of the current shard, if
// any, has finished.
func (e *Executor) awaitRelease(ctx context.Context) error {
	done, ok := e.releasing[e.shard]
	if !ok {
		return nil
	}
	select {
	case <-done:
		delete(e.releasing, e.shard)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Parse implements pgwire.Executor: the message is buffered until Sync.
func (e *Executor) Parse(ctx context.Context, name, sql string, paramOIDs []uint32) error {
	return e.guard("Parse", func() error { return e.parse(ctx, name, sql, paramOIDs) })
}

func (e *Executor) parse(ctx context.Context, name, sql string, paramOIDs []uint32) error {
	if e.batchFailed {
		return nil
	}
	pl, err := e.planOp(ctx, sql, "parse")
	if err == nil {
		err = checkTransactionMode(pl.Class)
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
	e.batch = append(e.batch, parseReq(e.physical(name), st.shardSQL(), st.shardOIDs()))
	return nil
}

// multiShard reports whether a resolved plan runs on several shards.
func multiShard(pl plan.Plan) bool {
	return pl.Kind != plan.SessionLocal && pl.Kind != plan.Refuse && !pl.Deferred && len(pl.Shards) > 1
}

// aimBatch records the shard a resolved plan needs; one batch may only
// target one shard, or carry one multi-shard statement.
func (e *Executor) aimBatch(pl plan.Plan, stmt string) error {
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
func (e *Executor) Bind(ctx context.Context, portal, statement string, paramFormats []int16, params [][]byte, resultFormats []int16) error {
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
	e.batch = append(e.batch, bindReq(portal, e.physical(statement), paramFormats, params, resultFormats))
	return nil
}

// replanStale plans statement again when it was prepared against an older
// snapshot than the current one.
func (e *Executor) replanStale(ctx context.Context, statement string) error {
	st, ok := e.stmts[statement]
	if !ok || st.snap == e.currentSnapshot() {
		return nil
	}
	pl, err := e.planOp(ctx, st.sql, "parse")
	if err == nil && sequenceShape(pl) != sequenceShape(st.plan) {
		perr := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "the sequence columns of the table changed since statement %q was prepared", statement)
		perr.Hint = "prepare the statement again"
		err = perr
	}
	if err != nil {
		return err
	}
	st.plan, st.class, st.snap = pl, pl.Class, e.currentSnapshot()
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
// out a write fence: every bound statement is planned against the current
// snapshot and resolved with its recorded keys.
func (e *Executor) reaim(ctx context.Context, binds []batchBind) (*Shard, *plan.Plan, string, error) {
	e.batchTarget, e.batchScatter, e.batchScatterStmt = nil, nil, ""
	defer func() { e.batchTarget, e.batchScatter, e.batchScatterStmt = nil, nil, "" }()
	for _, b := range binds {
		if err := e.replanStale(ctx, b.statement); err != nil {
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
		name = e.physical(name)
	}
	e.batch = append(e.batch, describeReq(kind, name))
	return nil
}

// Execute implements pgwire.Executor.
func (e *Executor) Execute(_ context.Context, portal string, maxRows int32, w pgwire.ResultWriter) error {
	return e.guard("Execute", func() error { return e.execute(portal, maxRows, w) })
}

func (e *Executor) execute(portal string, maxRows int32, w pgwire.ResultWriter) error {
	if e.batchFailed {
		return nil
	}
	e.batchWriter = w
	if st, ok := e.stmts[e.portals[portal]]; ok {
		if multiShard(st.plan) && e.batchScatter == nil {
			e.failBatch()
			err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "a multi-shard portal must be bound and executed in the same batch")
			err.Hint = "send Bind and Execute before one Sync"
			return err
		}
		if st.class.SetGUC {
			g := gucEntry{name: st.class.GUCName, sql: st.sql, value: st.class.GUCValue, searchPath: st.class.SearchPath}
			e.staged = append(e.staged, g)
			defer e.injectSearchPath(g)
		}
		e.batchExec = append(e.batchExec, execItem{sql: st.sql, local: st.plan.Kind == plan.SessionLocal, class: st.class, tables: st.plan.Tables})
	}
	e.batch = append(e.batch, executeReq(portal, maxRows))
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
func (e *Executor) Close(_ context.Context, kind pgwire.DescribeKind, name string) error {
	if e.batchFailed {
		return nil
	}
	if kind == pgwire.DescribeStatement {
		delete(e.stmts, name)
		name = e.physical(name)
	} else {
		delete(e.portals, name)
	}
	e.batch = append(e.batch, closeReq(kind, name))
	return nil
}

func (e *Executor) failBatch() {
	for _, name := range e.batchStmts {
		delete(e.stmts, name)
	}
	e.staged = e.staged[:e.stagedMark]
	e.batch, e.batchStmts, e.batchFailed, e.batchWriter = nil, nil, true, nil
	e.batchTarget, e.batchExec, e.describes, e.batchBinds = nil, nil, nil, nil
	e.batchInject = nil
	e.batchScatter, e.batchScatterStmt = nil, ""
}

// Sync implements pgwire.Executor: it ships the buffered batch followed by
// Sync and relays every response.
func (e *Executor) Sync(ctx context.Context) error {
	return e.guard("Sync", func() error { return e.sync(ctx) })
}

func (e *Executor) sync(ctx context.Context) error {
	if e.batchFailed {
		e.batchFailed = false
		return nil
	}
	batch, w, executed, binds := e.batch, e.batchWriter, e.batchExec, e.batchBinds
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
	e.pendingDescribes, e.describes = e.describes, nil
	if len(batch) == 0 {
		return nil
	}
	if w == nil {
		w = discardWriter{}
	}
	for _, item := range executed {
		if item.class.Write {
			before := e.currentSnapshot()
			if err := e.gateWrite(ctx, item.tables); err != nil {
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
	if handled, err := e.migrationBatch(ctx, batch, parsed, w); handled {
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
			return e.afterBatch(ctx, e.referenceWrite(ctx, *scatterPlan, unnamedBatch(st.sql, st.oids, batch), w))
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
		for _, item := range executed {
			if item.class.Write {
				if err := e.noteWrite(ctx); err != nil {
					e.staged = e.staged[:e.stagedMark]
					return err
				}
				break
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
		e.hiddenExec = hidden
		err := e.pump(ctx, cw)
		e.hiddenExec = nil
		if err == nil {
			for _, item := range executed {
				e.noteExecuted(item.sql, item.local)
				e.noteSessionEffect(item.class, item.sql)
			}
		}
		return err
	})
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
	if e.tx == pgwire.TxIdle {
		e.txnPrelude, e.txnTouched = nil, false
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
		if rerr := e.release(ctx); rerr != nil && err == nil {
			err = rerr
		}
	}
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

func (e *Executor) generation() *pgshardv1.Generation { return e.r.cfg.Poolers.Generation(e.shard) }

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
	resp, err := client.Reserve(ctx, &pgshardv1.ReserveRequest{SessionId: e.sid, Generation: e.generation()})
	if err != nil {
		return e.poolerRefused(err)
	}
	if resp.Error != nil {
		return toPgwireError(resp.Error)
	}
	e.pinned = true
	return nil
}

// replay re-establishes session GUCs and named prepared statements on a
// freshly pinned backend.
func (e *Executor) replay(ctx context.Context, skip map[string]bool) error {
	var parts []string
	// The backend never saw the client's startup options, so a startup
	// search_path is applied first, and re-applied after every replayed
	// RESET: on this session RESET restores the startup value, while on
	// the backend it would restore the server default the planner did not
	// route with.
	if e.startupSearchPath != nil {
		parts = append(parts, searchPathSQL(e.startupSearchPath))
	}
	for _, g := range e.gucs {
		parts = append(parts, strings.TrimRight(strings.TrimSpace(g.sql), ";"))
		if e.startupSearchPath != nil && resetsSearchPath(g) {
			parts = append(parts, searchPathSQL(e.startupSearchPath))
		}
	}
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

// replayStatements parses the named statements the current backend lacks;
// skip names those it already has.
func (e *Executor) replayStatements(ctx context.Context, skip map[string]bool) error {
	n := 0
	for name, st := range e.stmts {
		if name == "" || skip[name] {
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
	if err := e.conn.send(req, e.sid, e.generation(), e.ident, e.info.Database); err != nil {
		return e.poolerLost(err)
	}
	return nil
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
		e.detachAsync(client)
	}
	e.tx = pgwire.TxIdle
	e.staged, e.stagedMark = nil, 0
	if _, isPG := errors.AsType[*pgwire.Error](cause); isPG {
		return cause
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
	return status.Code(cause) == codes.FailedPrecondition &&
		strings.Contains(status.Convert(cause).Message(), "already has an Execute stream")
}

// refusedError marks a pooler that refused the connection before any
// statement was sent.
type refusedError struct{ error }

func (r *refusedError) Unwrap() error { return r.error }

// pump relays responses until ReadyForQuery. Errors from the backend are
// returned after the batch is drained so pgwire reports them itself.
func (e *Executor) pump(ctx context.Context, w pgwire.ResultWriter) error {
	start := time.Now()
	defer func() {
		e.r.metrics.ShardLatency.WithLabelValues(fmt.Sprintf("%s/%d", e.shard.Set, e.shard.ID)).Observe(time.Since(start).Seconds())
	}()
	var firstErr error
	e.cancelSent.Store(false)
	onCancel := func() { e.cancelBackend(context.Background()) }
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
			if !e.hiddenNow() {
				werr = w.DataRow(rowValues(m.DataRow.Columns))
			}
		case *pgshardv1.ExecuteResponse_CommandComplete:
			if !e.popHidden() {
				e.lastTag = m.CommandComplete.Tag
				werr = w.CommandComplete(m.CommandComplete.Tag)
			}
		case *pgshardv1.ExecuteResponse_EmptyQuery:
			if !e.popHidden() {
				werr = w.EmptyQueryResponse()
			}
		case *pgshardv1.ExecuteResponse_Error:
			if firstErr == nil {
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
			if prev != pgwire.TxIdle && e.tx == pgwire.TxIdle {
				e.txnEnded = true
			}
			return firstErr
		case *pgshardv1.ExecuteResponse_ParseComplete, *pgshardv1.ExecuteResponse_BindComplete,
			*pgshardv1.ExecuteResponse_ParameterStatus:
		default:
			e.r.cfg.Logger.Warn("unexpected pooler response", "session", e.sid, "type", fmt.Sprintf("%T", resp.Message))
		}
		if werr != nil {
			return werr
		}
	}
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

// cancelBackend asks the pooler to interrupt the statement running for this
// session.
func (e *Executor) cancelBackend(ctx context.Context) {
	if !e.cancelSent.CompareAndSwap(false, true) {
		return
	}
	client, err := e.client()
	if err != nil {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, releaseTimeout)
	defer cancel()
	if _, err := client.Cancel(cctx, &pgshardv1.CancelRequest{SessionId: e.sid}); err != nil {
		e.r.cfg.Logger.Warn("cancel failed", "session", e.sid, "err", err)
	}
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
	return releaseRPC(ctx, client, e.sid)
}

func releaseRPC(ctx context.Context, client pgshardv1.PoolerClient, sid string) error {
	rctx, cancel := context.WithTimeout(ctx, releaseTimeout)
	defer cancel()
	resp, err := client.Release(rctx, &pgshardv1.ReleaseRequest{SessionId: sid})
	if err != nil {
		return pgwire.Errorf(codeConnectionFailure, "pooler release failed: %v", err)
	}
	if resp.Error != nil {
		return toPgwireError(resp.Error)
	}
	return nil
}

// Release implements pgwire.Executor: the session is over.
func (e *Executor) Release() {
	e.r.forget(e)
	e.dropParked()
	pinned := e.pinned
	if e.conn != nil {
		e.conn.close()
		e.conn = nil
	}
	if pinned {
		if client, err := e.client(); err == nil {
			_ = releaseRPC(context.Background(), client, e.sid)
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
func (discardWriter) Notice(*pgproto3.NoticeResponse) error             { return nil }
func (discardWriter) Notification(*pgproto3.NotificationResponse) error { return nil }
func (discardWriter) CopyIn(byte, []uint16) (pgwire.CopyInStream, error) {
	return nil, pgwire.Errorf(pgwire.CodeProtocolViolation, "unexpected COPY while replaying session state")
}
func (discardWriter) CopyOut(byte, []uint16) error { return nil }
func (discardWriter) CopyData([]byte) error        { return nil }
func (discardWriter) CopyDone() error              { return nil }
