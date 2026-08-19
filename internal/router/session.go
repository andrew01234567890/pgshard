package router

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
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
type execItem struct {
	sql   string
	local bool
}

type gucEntry struct {
	name string
	sql  string
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
	// portals maps portal names to logical statement names.
	portals map[string]string

	batch       []*pgshardv1.ExecuteRequest
	batchStmts  []string
	batchFailed bool
	batchWriter pgwire.ResultWriter
	// batchTarget is the shard the current extended batch resolved to.
	batchTarget *Shard
	batchExec   []execItem
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

	// startupSearchPath is the search_path the client asked for at startup
	// (options=-c search_path=...); nil means the server default.
	startupSearchPath []string
}

func newExecutor(r *Router, info pgwire.SessionInfo, home Shard) *Executor {
	ctx, cancel := context.WithCancel(context.Background())
	keys := info.Auth.SCRAM
	return &Executor{
		r: r, info: info, sid: r.prefix + "-" + strconv.FormatUint(info.ID, 10), home: home, shard: home,
		ident: &pgshardv1.UserIdentity{Username: info.User,
			ScramClientKey: append([]byte(nil), keys.ClientKey...), ScramServerKey: append([]byte(nil), keys.ServerKey...)},
		ctx: ctx, cancel: cancel, tx: pgwire.TxIdle,
		stmts: map[string]prepared{}, portals: map[string]string{},
		startupSearchPath: startupSearchPath(info.Params["options"]),
	}
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

// Home reports the home shard of the session's database.
func (e *Executor) Home() Shard { return e.home }

// Shard reports the shard the session's stream is on.
func (e *Executor) Shard() Shard { return e.shard }

// planSession describes this session to the planner. Sessions on the
// catalog shard set see no table placement and plan everything onto their
// home shard.
func (e *Executor) planSession() plan.Session {
	sess := plan.Session{Database: e.info.Database, HomeShard: e.home.ID, ID: e.info.ID, SearchPath: e.searchPath()}
	if e.home.Set == DefaultShardSet {
		sess.Snapshot = e.r.cfg.Snapshot()
	}
	return sess
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
		err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "scatter execution is not available yet (%s plan over %d shards)", pl.Kind, len(pl.Shards))
		err.Hint = "filter on one shard key value; scatter-gather execution is planned for M3.3"
		return Shard{}, err
	}
	return Shard{Set: e.home.Set, ID: pl.Shards[0]}, nil
}

// moveTo points the session at target, dropping the stream on the previous
// shard. A transaction that already touched a shard cannot move.
func (e *Executor) moveTo(target Shard) error {
	if target == e.shard {
		return nil
	}
	if e.conn == nil && !e.pinned {
		e.shard = target
		return nil
	}
	if e.tx != pgwire.TxIdle && e.txnTouched {
		err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "multi-shard transactions are not available yet: transaction is on shard %s/%d, statement needs shard %s/%d",
			e.shard.Set, e.shard.ID, target.Set, target.ID)
		err.Hint = "commit or roll back first; two-phase commit across shards is planned for M3.4"
		return err
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
	if len(e.gucs) > 0 || len(e.staged) > 0 {
		return true
	}
	for name := range e.stmts {
		if name != "" {
			return true
		}
	}
	return false
}

// SimpleQuery implements pgwire.Executor.
func (e *Executor) SimpleQuery(ctx context.Context, sql string, w pgwire.ResultWriter) error {
	pl, err := e.r.cfg.Planner.Plan(ctx, e.planSession(), sql)
	if err != nil {
		return err
	}
	target, err := e.target(pl)
	if err != nil {
		return err
	}
	if err := e.moveTo(target); err != nil {
		return err
	}
	return e.withFailover(ctx, w, func(cw pgwire.ResultWriter) error {
		if err := e.acquire(ctx, nil); err != nil {
			return err
		}
		if pl.Class.SetGUC {
			if err := e.ensurePinned(ctx); err != nil {
				return err
			}
		}
		if err := e.send(simpleQuery(sql)); err != nil {
			return err
		}
		err := e.pump(ctx, cw)
		if err == nil {
			e.noteExecuted(sql, pl.Kind == plan.SessionLocal)
		}
		if pl.Class.SetGUC && err == nil {
			e.staged = append(e.staged, gucEntry{name: pl.Class.GUCName, sql: sql, searchPath: pl.Class.SearchPath})
		}
		return err
	})
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
			if ok, err := e.r.awaitConsistent(ctx, e.shard, false); err != nil {
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
		ok, werr := e.r.awaitConsistent(ctx, e.shard, true)
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
	if e.conn != nil {
		e.conn.abort()
		e.conn = nil
	}
	if e.pinned {
		e.pinned = false
		e.releaseAsync()
	}
	e.tx = pgwire.TxIdle
}

// releaseAsync returns the pinned backend of the current shard without
// waiting for the pooler.
func (e *Executor) releaseAsync() {
	client, err := e.client()
	if err != nil {
		return
	}
	go func() { _ = releaseRPC(context.Background(), client, e.sid) }()
}

// Parse implements pgwire.Executor: the message is buffered until Sync.
func (e *Executor) Parse(ctx context.Context, name, sql string, paramOIDs []uint32) error {
	if e.batchFailed {
		return nil
	}
	pl, err := e.r.cfg.Planner.Plan(ctx, e.planSession(), sql)
	if err != nil {
		e.failBatch()
		return err
	}
	if !pl.Deferred && pl.Kind != plan.SessionLocal {
		if err := e.aimBatch(pl); err != nil {
			e.failBatch()
			return err
		}
	}
	e.stmts[name] = prepared{sql: sql, oids: paramOIDs, class: pl.Class, plan: pl, snap: e.currentSnapshot()}
	e.batchStmts = append(e.batchStmts, name)
	e.batch = append(e.batch, parseReq(e.physical(name), sql, paramOIDs))
	return nil
}

// aimBatch records the shard a resolved plan needs; one batch may only
// target one shard.
func (e *Executor) aimBatch(pl plan.Plan) error {
	target, err := e.target(pl)
	if err != nil {
		return err
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

// currentSnapshot is the snapshot plans of this session are made against;
// nil on the catalog shard set, whose plans never depend on one.
func (e *Executor) currentSnapshot() *snapshot.Snapshot {
	if e.home.Set != DefaultShardSet {
		return nil
	}
	return e.r.cfg.Snapshot()
}

// Bind implements pgwire.Executor: a deferred plan is resolved here, once
// the shard key parameters are known. A statement prepared against an
// older snapshot is planned again first.
func (e *Executor) Bind(ctx context.Context, portal, statement string, paramFormats []int16, params [][]byte, resultFormats []int16) error {
	if e.batchFailed {
		return nil
	}
	if st, ok := e.stmts[statement]; ok && st.snap != e.currentSnapshot() {
		pl, err := e.r.cfg.Planner.Plan(ctx, e.planSession(), st.sql)
		if err != nil {
			e.failBatch()
			return err
		}
		st.plan, st.class, st.snap = pl, pl.Class, e.currentSnapshot()
		e.stmts[statement] = st
	}
	if st, ok := e.stmts[statement]; ok && st.plan.Kind != plan.SessionLocal {
		pl := st.plan
		if pl.Deferred {
			var err error
			pl, err = pl.Resolve(plan.BindParams{OIDs: st.paramOIDs(), Formats: paramFormats, Values: params})
			if err != nil {
				e.failBatch()
				return err
			}
		}
		if err := e.aimBatch(pl); err != nil {
			e.failBatch()
			return err
		}
	}
	e.portals[portal] = statement
	e.batch = append(e.batch, bindReq(portal, e.physical(statement), paramFormats, params, resultFormats))
	return nil
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
	if e.batchFailed {
		return nil
	}
	e.batchWriter = w
	if st, ok := e.stmts[e.portals[portal]]; ok {
		if st.class.SetGUC {
			e.staged = append(e.staged, gucEntry{name: st.class.GUCName, sql: st.sql, searchPath: st.class.SearchPath})
		}
		e.batchExec = append(e.batchExec, execItem{sql: st.sql, local: st.plan.Kind == plan.SessionLocal})
	}
	e.batch = append(e.batch, executeReq(portal, maxRows))
	return nil
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
	e.batchTarget, e.batchExec, e.describes = nil, nil, nil
}

// Sync implements pgwire.Executor: it ships the buffered batch followed by
// Sync and relays every response.
func (e *Executor) Sync(ctx context.Context) error {
	if e.batchFailed {
		e.batchFailed = false
		return nil
	}
	batch, w, executed := e.batch, e.batchWriter, e.batchExec
	target := e.shard
	if e.batchTarget != nil {
		target = *e.batchTarget
	}
	pin := false
	fresh := map[string]bool{}
	for _, name := range e.batchStmts {
		pin = pin || name != ""
		fresh[name] = true
	}
	e.batch, e.batchStmts, e.batchWriter, e.batchTarget, e.batchExec = nil, nil, nil, nil, nil
	e.pendingDescribes, e.describes = e.describes, nil
	if len(batch) == 0 {
		return nil
	}
	if w == nil {
		w = discardWriter{}
	}
	if err := e.moveTo(target); err != nil {
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
		for _, req := range batch {
			if err := e.send(req); err != nil {
				return err
			}
		}
		if err := e.send(syncReq()); err != nil {
			return err
		}
		err := e.pump(ctx, cw)
		if err == nil {
			for _, item := range executed {
				e.noteExecuted(item.sql, item.local)
			}
		}
		return err
	})
}

// afterBatch settles staged GUCs and releases the pinned backend when a
// transaction just ended.
func (e *Executor) afterBatch(ctx context.Context, err error) error {
	if e.tx == pgwire.TxIdle {
		e.txnPrelude, e.txnTouched = nil, false
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
	if len(e.gucs) > 0 {
		parts := make([]string, len(e.gucs))
		for i, g := range e.gucs {
			parts[i] = strings.TrimRight(strings.TrimSpace(g.sql), ";")
		}
		if err := e.send(simpleQuery(strings.Join(parts, "; "))); err != nil {
			return err
		}
		if err := e.pump(ctx, discardWriter{}); err != nil {
			return fmt.Errorf("router: replaying session settings: %w", err)
		}
	}
	n := 0
	for name, st := range e.stmts {
		if name == "" || skip[name] {
			continue
		}
		if err := e.send(parseReq(e.physical(name), st.sql, st.oids)); err != nil {
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
	if err := e.conn.send(req, e.sid, e.generation(), e.ident); err != nil {
		return e.poolerLost(err)
	}
	return nil
}

// poolerLost drops the stream and reports 08006; the next statement
// reacquires a backend and replays session state.
func (e *Executor) poolerLost(cause error) error {
	if e.conn != nil {
		e.conn.abort()
		e.conn = nil
	}
	if e.pinned {
		e.pinned = false
		e.releaseAsync()
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
	if status.Code(cause) == codes.Unavailable {
		return &refusedError{err}
	}
	return err
}

// refusedError marks a pooler that refused the connection before any
// statement was sent.
type refusedError struct{ error }

func (r *refusedError) Unwrap() error { return r.error }

// pump relays responses until ReadyForQuery. Errors from the backend are
// returned after the batch is drained so pgwire reports them itself.
func (e *Executor) pump(ctx context.Context, w pgwire.ResultWriter) error {
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
			werr = w.DataRow(rowValues(m.DataRow.Columns))
		case *pgshardv1.ExecuteResponse_CommandComplete:
			e.lastTag = m.CommandComplete.Tag
			werr = w.CommandComplete(m.CommandComplete.Tag)
		case *pgshardv1.ExecuteResponse_EmptyQuery:
			werr = w.EmptyQueryResponse()
		case *pgshardv1.ExecuteResponse_Error:
			if firstErr == nil {
				firstErr = toPgwireError(m.Error.GetError())
			}
		case *pgshardv1.ExecuteResponse_Notice:
			werr = w.Notice(toNotice(m.Notice.GetNotice()))
		case *pgshardv1.ExecuteResponse_ParameterDescription:
			e.inferParams(m.ParameterDescription.ParamOids)
			werr = w.ParameterDescription(m.ParameterDescription.ParamOids)
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

func (discardWriter) RowDescription([]pgproto3.FieldDescription) error { return nil }
func (discardWriter) DataRow([][]byte) error                           { return nil }
func (discardWriter) CommandComplete(string) error                     { return nil }
func (discardWriter) EmptyQueryResponse() error                        { return nil }
func (discardWriter) ParameterDescription([]uint32) error              { return nil }
func (discardWriter) NoData() error                                    { return nil }
func (discardWriter) PortalSuspended() error                           { return nil }
func (discardWriter) Notice(*pgproto3.NoticeResponse) error            { return nil }
func (discardWriter) CopyIn(byte, []uint16) (pgwire.CopyInStream, error) {
	return nil, pgwire.Errorf(pgwire.CodeProtocolViolation, "unexpected COPY while replaying session state")
}
func (discardWriter) CopyOut(byte, []uint16) error { return nil }
func (discardWriter) CopyData([]byte) error        { return nil }
func (discardWriter) CopyDone() error              { return nil }
