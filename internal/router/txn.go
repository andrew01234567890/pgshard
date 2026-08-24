package router

import (
	"context"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgproto3"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/router/crashpoint"
	"github.com/andrew01234567890/pgshard/internal/router/plan"
)

const (
	// TransactionModeGUC selects how a transaction that writes to a second
	// shard is handled: "twopc" (default) escalates to two-phase commit,
	// "single" refuses the second writable shard.
	TransactionModeGUC = "pgshard.transaction_mode"

	txnModeTwoPC  = "twopc"
	txnModeSingle = "single"

	codeInDoubt         = "08007"
	codeTxnRollback     = "40000"
	codeInvalidParamVal = "22023"
)

// txnPart is one shard a transaction has touched: its stream and what it did.
type txnPart struct {
	shard  Shard
	ps     *poolerStream
	pinned bool
	tx     pgwire.TxStatus
	wrote  bool

	prepared bool
	tag      string
	err      error
	// known names the prepared statements the backend held when the part
	// was parked; statements prepared since are parsed on revival.
	known map[string]bool
}

// switchPart parks the stream of the current shard and makes target the
// current one, reviving its parked stream or leaving the session to open a
// fresh one (acquire replays the transaction prelude on it).
func (e *Executor) switchPart(ctx context.Context, target Shard) error {
	if e.catalogSession() {
		return pgwire.Errorf(pgwire.CodeFeatureNotSupported, "transactions on the catalog shard set cannot span shards")
	}
	if e.parked == nil {
		e.parked = map[Shard]*txnPart{}
	}
	known := make(map[string]bool, len(e.stmts))
	for name := range e.stmts {
		if !e.unsent[name] {
			known[name] = true
		}
	}
	e.parked[e.shard] = &txnPart{shard: e.shard, ps: e.conn, pinned: e.pinned, tx: e.tx, wrote: e.wroteHere, known: known}
	e.shard = target
	if p, ok := e.parked[target]; ok {
		delete(e.parked, target)
		e.conn, e.pinned, e.tx, e.wroteHere = p.ps, p.pinned, p.tx, p.wrote
		if e.conn != nil && e.needsPin() {
			if err := e.ensurePinned(ctx); err != nil {
				return err
			}
			return e.replayStatements(ctx, p.known)
		}
		return nil
	}
	e.conn, e.pinned, e.tx, e.wroteHere = nil, false, pgwire.TxIdle, false
	return nil
}

// current is the executor's active shard as a txnPart.
func (e *Executor) current() *txnPart {
	return &txnPart{shard: e.shard, ps: e.conn, pinned: e.pinned, tx: e.tx, wrote: e.wroteHere}
}

// parts lists every shard of the open transaction, current one included,
// by shard id.
func (e *Executor) parts() []*txnPart {
	out := []*txnPart{e.current()}
	for _, p := range e.parked {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].shard.ID < out[j].shard.ID })
	return out
}

func (e *Executor) multiShardTxn() bool { return len(e.parked) > 0 }

// transactionMode is the session's effective pgshard.transaction_mode.
func (e *Executor) transactionMode() string {
	mode := txnModeTwoPC
	for _, g := range e.gucs {
		if g.name == TransactionModeGUC {
			mode = txnModeFromSQL(g.sql, mode)
		}
	}
	for _, g := range e.staged {
		if g.name == TransactionModeGUC {
			mode = txnModeFromSQL(g.sql, mode)
		}
	}
	return mode
}

func txnModeFromSQL(sql, current string) string {
	if v := gucValueOf(sql); v != "" {
		return v
	}
	return current
}

// gucValueOf reads the value of a SET statement the session recorded.
func gucValueOf(sql string) string {
	s := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(sql), ";"))
	i := strings.LastIndexAny(s, "=")
	if j := strings.LastIndex(s, " to "); j > i {
		i = j + 3
	}
	if i < 0 {
		return ""
	}
	return strings.Trim(strings.TrimSpace(s[i+1:]), "'\"")
}

// checkTransactionMode validates a SET of pgshard.transaction_mode before
// it reaches a backend, which would accept any placeholder value.
func checkTransactionMode(class StmtClass) error {
	if !class.SetGUC || class.GUCName != TransactionModeGUC {
		return nil
	}
	switch strings.ToLower(class.GUCValue) {
	case txnModeTwoPC, txnModeSingle:
		return nil
	}
	err := pgwire.Errorf(codeInvalidParamVal, "invalid value for parameter %q: %q", TransactionModeGUC, class.GUCValue)
	err.Hint = "valid values are twopc and single"
	return err
}

// noteWrite records that the statement about to run on the current shard
// writes. The second writable shard of a transaction escalates it to
// two-phase commit, or is refused in single mode.
func (e *Executor) noteWrite(ctx context.Context) error {
	if e.tx == pgwire.TxIdle || e.wroteHere {
		return nil
	}
	var writers []*txnPart
	for _, p := range e.parked {
		if p.wrote {
			writers = append(writers, p)
		}
	}
	if len(writers) == 0 {
		e.wroteHere = true
		return nil
	}
	if e.transactionMode() == txnModeSingle {
		err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "transaction already writes to shard %s/%d and %s is single: statement needs shard %s/%d",
			writers[0].shard.Set, writers[0].shard.ID, TransactionModeGUC, e.shard.Set, e.shard.ID)
		err.Hint = "SET pgshard.transaction_mode = twopc to commit across shards with two-phase commit"
		return err
	}
	if e.r.cfg.Decisions == nil {
		return pgwire.Errorf(pgwire.CodeFeatureNotSupported, "two-phase commit is not available: the router has no decision log")
	}
	if e.gid == "" {
		for _, p := range append(writers, e.current()) {
			if err := e.checkPreparedCapacity(ctx, p); err != nil {
				return err
			}
		}
		e.gidSeq++
		e.gid = "pgshard-" + e.sid + "-" + strconv.FormatUint(e.gidSeq, 10)
	}
	e.wroteHere = true
	return nil
}

// checkPreparedCapacity refuses two-phase commit on a shard whose
// PostgreSQL has max_prepared_transactions = 0. The answer is cached per
// shard: the setting only changes with a server restart.
func (e *Executor) checkPreparedCapacity(ctx context.Context, p *txnPart) error {
	if ok, known := e.r.preparedCapacity(p.shard); known {
		if ok {
			return nil
		}
		return noPreparedCapacity(p.shard)
	}
	value, err := e.queryOne(ctx, p, "SHOW max_prepared_transactions")
	if err != nil {
		return err
	}
	n, _ := strconv.Atoi(value)
	e.r.setPreparedCapacity(p.shard, n > 0)
	if n <= 0 {
		return noPreparedCapacity(p.shard)
	}
	return nil
}

func noPreparedCapacity(sh Shard) error {
	err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "shard %s/%d has max_prepared_transactions = 0: it cannot take part in a multi-shard transaction", sh.Set, sh.ID)
	err.Hint = "set max_prepared_transactions on every shard, or keep the transaction's writes on one shard"
	return err
}

// queryOne runs a single-column, single-row query on p and returns the value.
func (e *Executor) queryOne(ctx context.Context, p *txnPart, sql string) (string, error) {
	cw := &captureWriter{}
	if err := e.runOn(ctx, p, sql, cw); err != nil {
		return "", err
	}
	if len(cw.rows) == 0 || len(cw.rows[0]) == 0 {
		return "", pgwire.Errorf(pgwire.CodeInternalError, "router: %s returned no rows", sql)
	}
	return string(cw.rows[0][0]), nil
}

// runOn sends one simple query on p's stream and drains the answer into w,
// updating p's transaction status. It touches no executor state, so several
// parts can run concurrently.
func (e *Executor) runOn(ctx context.Context, p *txnPart, sql string, w pgwire.ResultWriter) error {
	return e.runReqsOn(ctx, p, []*pgshardv1.ExecuteRequest{simpleQuery(sql)}, w)
}

// runReqsOn sends reqs (a simple query, or an extended batch ending in
// Sync) on p's stream and relays the answer into w until ReadyForQuery.
func (e *Executor) runReqsOn(ctx context.Context, p *txnPart, reqs []*pgshardv1.ExecuteRequest, w pgwire.ResultWriter) error {
	if p.ps == nil {
		return pgwire.Errorf(codeConnectionFailure, "shard %s/%d: no stream", p.shard.Set, p.shard.ID)
	}
	for _, req := range reqs {
		if err := p.ps.send(cloneRequest(req), e.sid, e.r.cfg.Poolers.Generation(p.shard), e.ident); err != nil {
			return pgwire.Errorf(codeConnectionFailure, "shard %s/%d: pooler connection lost: %v", p.shard.Set, p.shard.ID, err)
		}
	}
	var firstErr error
	for {
		resp, err := p.ps.recv(ctx, nil)
		if err != nil {
			return pgwire.Errorf(codeConnectionFailure, "shard %s/%d: pooler connection lost: %v", p.shard.Set, p.shard.ID, err)
		}
		var werr error
		switch m := resp.Message.(type) {
		case *pgshardv1.ExecuteResponse_RowDescription:
			werr = w.RowDescription(fieldDescriptions(m.RowDescription.Fields))
		case *pgshardv1.ExecuteResponse_DataRow:
			werr = w.DataRow(rowValues(m.DataRow.Columns))
		case *pgshardv1.ExecuteResponse_CommandComplete:
			p.tag = m.CommandComplete.Tag
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
			werr = w.ParameterDescription(m.ParameterDescription.ParamOids)
		case *pgshardv1.ExecuteResponse_NoData:
			werr = w.NoData()
		case *pgshardv1.ExecuteResponse_ReadyForQuery:
			p.tx = txStatus(m.ReadyForQuery.TxnStatus)
			return firstErr
		}
		if werr != nil {
			return werr
		}
	}
}

// each runs fn on every part concurrently and stores its outcome in p.err.
func (e *Executor) each(parts []*txnPart, fn func(*txnPart) error) {
	var wg sync.WaitGroup
	for _, p := range parts {
		wg.Add(1)
		go func(p *txnPart) {
			defer wg.Done()
			p.err = fn(p)
		}(p)
	}
	wg.Wait()
}

func firstError(parts []*txnPart) error {
	for _, p := range parts {
		if p.err != nil {
			return p.err
		}
	}
	return nil
}

// endTxn finishes a transaction that spans several shards. Only one
// writer: it commits or rolls back plainly and the others roll back. Several
// writers: two-phase commit through the decision log.
func (e *Executor) endTxn(ctx context.Context, commit bool, w pgwire.ResultWriter) error {
	if !e.multiShardTxn() {
		return pgwire.Errorf(pgwire.CodeInternalError, "router: endTxn on a single-shard transaction")
	}
	parts := e.parts()
	defer e.dropParked()
	if commit {
		if err := e.promoteHiddenWriters(ctx, parts); err != nil {
			e.each(parts, func(p *txnPart) error { return e.runOn(ctx, p, "ROLLBACK", discardWriter{}) })
			e.finishTxn("ROLLBACK")
			return err
		}
	}
	var writers, readers []*txnPart
	for _, p := range parts {
		if p.wrote {
			writers = append(writers, p)
		} else {
			readers = append(readers, p)
		}
	}
	if !commit || len(writers) <= 1 {
		final := e.current()
		if commit && len(writers) == 1 {
			final = writers[0]
		}
		var others []*txnPart
		for _, p := range parts {
			if p.shard != final.shard {
				others = append(others, p)
			}
		}
		e.each(others, func(p *txnPart) error { return e.runOn(ctx, p, "ROLLBACK", discardWriter{}) })
		sql := "ROLLBACK"
		if commit {
			sql = "COMMIT"
		}
		if err := e.runOn(ctx, final, sql, w); err != nil {
			e.finishTxn(final.tag)
			return err
		}
		e.finishTxn(final.tag)
		return firstError(others)
	}
	return e.twoPhaseCommit(ctx, writers, readers, w)
}

// hiddenWriteProbe asks a shard whether the transaction took a transaction
// id there, which a plain SELECT cannot do: only a write (or a function it
// called) does.
const hiddenWriteProbe = "SELECT pg_current_xact_id_if_assigned() IS NOT NULL"

// promoteHiddenWriters reclassifies parts the planner saw as readers whose
// backend nonetheless assigned a transaction id: a SELECT calling a function
// that writes must take part in two-phase commit instead of being rolled
// back behind a successful COMMIT. It applies the same gates as noteWrite.
func (e *Executor) promoteHiddenWriters(ctx context.Context, parts []*txnPart) error {
	var readers []*txnPart
	for _, p := range parts {
		if !p.wrote {
			readers = append(readers, p)
		}
	}
	if len(readers) == 0 {
		return nil
	}
	answers := make([]string, len(readers))
	e.each(readers, func(p *txnPart) error {
		v, err := e.queryOne(ctx, p, hiddenWriteProbe)
		answers[slices.Index(readers, p)] = v
		return err
	})
	if err := firstError(readers); err != nil {
		return err
	}
	var writers []*txnPart
	for _, p := range parts {
		if p.wrote {
			writers = append(writers, p)
		}
	}
	promoted := false
	for i, p := range readers {
		if answers[i] == "t" {
			p.wrote = true
			promoted = true
			writers = append(writers, p)
		}
	}
	if !promoted || len(writers) <= 1 {
		return nil
	}
	if e.transactionMode() == txnModeSingle {
		err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "transaction wrote on shard %s/%d through a function call while %s is single: it cannot commit with the write on shard %s/%d",
			writers[len(writers)-1].shard.Set, writers[len(writers)-1].shard.ID, TransactionModeGUC, writers[0].shard.Set, writers[0].shard.ID)
		err.Hint = "SET pgshard.transaction_mode = twopc to commit across shards with two-phase commit"
		return err
	}
	if e.r.cfg.Decisions == nil {
		return pgwire.Errorf(pgwire.CodeFeatureNotSupported, "two-phase commit is not available: the router has no decision log")
	}
	if e.gid == "" {
		for _, p := range writers {
			if err := e.checkPreparedCapacity(ctx, p); err != nil {
				return err
			}
		}
		e.gidSeq++
		e.gid = "pgshard-" + e.sid + "-" + strconv.FormatUint(e.gidSeq, 10)
	}
	return nil
}

// twoPhaseCommit is the coordinator: decision row, PREPARE everywhere, the
// atomic commit decision, COMMIT PREPARED everywhere, cleanup. The client
// hears success once the commit decision is durable; a COMMIT PREPARED that
// fails afterwards is finished by the resolver.
func (e *Executor) twoPhaseCommit(ctx context.Context, writers, readers []*txnPart, w pgwire.ResultWriter) error {
	gid := e.gid
	log := e.r.cfg.Decisions
	ids := make([]int32, len(writers))
	for i, p := range writers {
		ids[i] = p.shard.ID
	}
	if err := e.r.awaitWriteFence(ctx); err != nil {
		e.each(append(writers, readers...), func(p *txnPart) error { return e.runOn(ctx, p, "ROLLBACK", discardWriter{}) })
		e.finishTxn("ROLLBACK")
		return err
	}
	e.each(readers, func(p *txnPart) error { return e.runOn(ctx, p, "ROLLBACK", discardWriter{}) })
	xids := make([]string, len(writers))
	e.each(writers, func(p *txnPart) error {
		xid, err := e.queryOne(ctx, p, "SELECT pg_current_xact_id()::text")
		if err == nil {
			xids[slices.Index(writers, p)] = xid
		}
		return err
	})
	if err := firstError(writers); err != nil {
		e.each(writers, func(p *txnPart) error { return e.runOn(ctx, p, "ROLLBACK", discardWriter{}) })
		e.finishTxn("ROLLBACK")
		return err
	}
	if err := log.Begin(ctx, gid, ids, xids); err != nil {
		e.each(writers, func(p *txnPart) error { return e.runOn(ctx, p, "ROLLBACK", discardWriter{}) })
		e.finishTxn("ROLLBACK")
		return pgwire.Errorf(codeConnectionFailure, "two-phase commit: writing the decision log failed, transaction rolled back: %v", err)
	}
	crashpoint.Hit("before_prepare")
	e.each(writers, func(p *txnPart) error {
		err := e.runOn(ctx, p, "PREPARE TRANSACTION "+quoteLiteral(gid), discardWriter{})
		p.prepared = err == nil
		return err
	})
	if err := firstError(writers); err != nil {
		e.each(writers, func(p *txnPart) error {
			if p.prepared {
				return e.runOn(ctx, p, "ROLLBACK PREPARED "+quoteLiteral(gid), discardWriter{})
			}
			return e.runOn(ctx, p, "ROLLBACK", discardWriter{})
		})
		e.r.metrics.TwoPCAborts.Inc()
		if aerr := log.Abort(ctx, gid); aerr != nil {
			e.r.cfg.Logger.Warn("two-phase commit: recording abort failed; the resolver will finish it", "gid", gid, "err", aerr)
		} else if firstError(writers) == nil {
			_ = log.Delete(ctx, gid)
		}
		e.finishTxn("ROLLBACK")
		return err
	}
	crashpoint.Hit("after_prepare")
	decided, err := log.Commit(ctx, gid)
	if err != nil {
		e.r.inDoubt.Add(1)
		e.r.metrics.TwoPCInDoubt.Inc()
		e.finishTxn("")
		e.r.cfg.Logger.Warn("two-phase commit: decision unknown, participants left prepared for the resolver", "gid", gid, "err", err)
		perr := pgwire.Errorf(codeInDoubt, "two-phase commit: the outcome of transaction %s is unknown: %v", gid, err)
		perr.Hint = "the resolver commits or rolls back the prepared transaction once the decision log is reachable"
		return perr
	}
	if !decided {
		e.each(writers, func(p *txnPart) error {
			return e.runOn(ctx, p, "ROLLBACK PREPARED "+quoteLiteral(gid), discardWriter{})
		})
		e.finishTxn("ROLLBACK")
		e.r.metrics.TwoPCAborts.Inc()
		return pgwire.Errorf(codeTxnRollback, "two-phase commit: transaction %s was aborted by the resolver before it was decided", gid)
	}
	crashpoint.Hit("after_decision")
	e.r.metrics.TwoPCCommits.Inc()
	commitPrepared := func(p *txnPart) error {
		return e.runOn(ctx, p, "COMMIT PREPARED "+quoteLiteral(gid), discardWriter{})
	}
	writers[0].err = commitPrepared(writers[0])
	crashpoint.Hit("during_commit_prepared")
	e.each(writers[1:], commitPrepared)
	if err := firstError(writers); err != nil {
		e.r.inDoubt.Add(1)
		e.r.metrics.TwoPCInDoubt.Inc()
		e.r.cfg.Logger.Warn("two-phase commit: COMMIT PREPARED failed after the commit decision; the resolver will finish it", "gid", gid, "err", err)
	} else if err := log.Delete(ctx, gid); err != nil {
		e.r.cfg.Logger.Warn("two-phase commit: deleting the decision row failed", "gid", gid, "err", err)
	}
	e.finishTxn("COMMIT")
	return w.CommandComplete("COMMIT")
}

// finishTxn marks the multi-shard transaction over on the current stream.
func (e *Executor) finishTxn(tag string) {
	e.tx = pgwire.TxIdle
	e.txnEnded = true
	e.wroteHere = false
	e.gid = ""
	if tag != "" {
		e.lastTag = tag
	}
}

// dropParked closes every parked stream and returns their backends. The
// release waits for the pooler: the session may open a new stream on the
// same shard right away and must not meet its old backend.
func (e *Executor) dropParked() {
	for _, p := range e.parked {
		if p.ps != nil {
			p.ps.close()
		}
		if p.pinned {
			if client, err := e.r.cfg.Poolers.Client(p.shard); err == nil {
				if err := releaseRPC(context.Background(), client, e.sid); err != nil {
					e.r.cfg.Logger.Warn("releasing a transaction participant failed", "session", e.sid, "shard", p.shard, "err", err)
				}
			}
		}
	}
	e.parked = nil
}

// txnControl handles a transaction control statement while the transaction
// spans several shards; ok reports whether it was handled here.
func (e *Executor) txnControl(ctx context.Context, class StmtClass, w pgwire.ResultWriter) (handled bool, err error) {
	if !e.multiShardTxn() {
		return false, nil
	}
	switch class.Txn {
	case plan.TxnCommit, plan.TxnRollback:
		if class.Chain {
			return true, pgwire.Errorf(pgwire.CodeFeatureNotSupported, "COMMIT/ROLLBACK AND CHAIN is not available in a multi-shard transaction")
		}
		return true, e.endTxn(ctx, class.Txn == plan.TxnCommit, w)
	case plan.TxnSavepoint, plan.TxnRelease, plan.TxnRollbackTo:
		return true, pgwire.Errorf(pgwire.CodeFeatureNotSupported, "savepoints are not available once a transaction spans several shards")
	}
	return false, nil
}

// captureWriter keeps rows in memory.
type captureWriter struct {
	discardWriter
	rows [][][]byte
}

func (c *captureWriter) DataRow(values [][]byte) error {
	row := make([][]byte, len(values))
	for i, v := range values {
		row[i] = append([]byte(nil), v...)
	}
	c.rows = append(c.rows, row)
	return nil
}

func (c *captureWriter) RowDescription([]pgproto3.FieldDescription) error { return nil }

// txnControlBatch handles an extended-protocol batch that ends a
// multi-shard transaction: the batch may carry nothing but that statement.
func (e *Executor) txnControlBatch(ctx context.Context, batch []*pgshardv1.ExecuteRequest, executed []execItem, w pgwire.ResultWriter) (bool, error) {
	if !e.multiShardTxn() {
		return false, nil
	}
	var control *execItem
	for i := range executed {
		if executed[i].class.Txn != plan.TxnNone {
			control = &executed[i]
		}
	}
	if control == nil {
		return false, nil
	}
	if len(executed) != 1 {
		return true, pgwire.Errorf(pgwire.CodeFeatureNotSupported, "a transaction control statement must be the only statement of its batch in a multi-shard transaction")
	}
	for _, req := range batch {
		switch r := req.Message.(type) {
		case *pgshardv1.ExecuteRequest_Describe:
			if r.Describe.Kind == pgshardv1.Describe_KIND_STATEMENT {
				if err := w.ParameterDescription(nil); err != nil {
					return true, err
				}
			}
			if err := w.NoData(); err != nil {
				return true, err
			}
		case *pgshardv1.ExecuteRequest_Execute:
			if _, err := e.txnControl(ctx, control.class, w); err != nil {
				return true, err
			}
		}
	}
	return true, nil
}
