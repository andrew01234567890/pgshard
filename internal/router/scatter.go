package router

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/router/plan"
	"github.com/andrew01234567890/pgshard/internal/router/scatter"
)

// ScatterConfig bounds multi-shard reads.
type ScatterConfig struct {
	// MaxShards is the most shards one statement may fan out to; 0 means
	// every shard.
	MaxShards int
	// MaxStreams caps the shard streams open for multi-shard reads across
	// the router; 0 picks the default.
	MaxStreams int
	// MaxWait bounds how long a statement waits for scatter capacity
	// before it is refused; 0 picks the default, negative waits for ever.
	MaxWait time.Duration
}

const defaultScatterStreams = 4096

// defaultScatterWait bounds the wait for capacity. A statement that has
// waited this long is behind enough other work that failing it with a
// retryable error serves the client better than holding its session open
// for an answer that is not coming soon.
const defaultScatterWait = 30 * time.Second

func (c ScatterConfig) withDefaults() ScatterConfig {
	if c.MaxStreams <= 0 {
		c.MaxStreams = defaultScatterStreams
	}
	if c.MaxWait == 0 {
		c.MaxWait = defaultScatterWait
	}
	return c
}

// scatterSlots is the router-wide budget of open scatter streams.
type scatterSlots struct {
	mu   sync.Mutex
	cond *sync.Cond
	free int
	// wait bounds how long acquire blocks; zero or less waits for ever.
	wait time.Duration
	// now is overridable so a test can drive the deadline.
	now func() time.Time
}

func newScatterSlots(n int, wait time.Duration) *scatterSlots {
	s := &scatterSlots{free: n, wait: wait, now: time.Now}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// acquire takes n slots, waiting while the budget is exhausted.
//
// The wait is bounded. Without a bound a burst of wide scatters parks every
// later statement indefinitely, and a statement needing many slots can be
// overtaken for ever by smaller ones that take each slot as it is freed --
// so the client sees a session that has stopped answering rather than an
// error it could act on. 53300 is what the stream budget already refuses
// with, and it is retryable.
func (s *scatterSlots) acquire(ctx context.Context, n int) error {
	broadcast := func() {
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	}
	stop := context.AfterFunc(ctx, broadcast)
	defer stop()
	var deadline time.Time
	if s.wait > 0 {
		deadline = s.now().Add(s.wait)
		timer := time.AfterFunc(s.wait, broadcast)
		defer timer.Stop()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.free < n {
		if ctx.Err() != nil {
			return pgwire.Errorf("57014", "canceling statement while waiting for scatter capacity")
		}
		if !deadline.IsZero() && !s.now().Before(deadline) {
			err := pgwire.Errorf("53300", "waited %s for scatter capacity and %d of %d streams are still in use", s.wait, n, n)
			err.Hint = "retry the statement, narrow it to fewer shards, or raise the scatter stream budget"
			return err
		}
		s.cond.Wait()
	}
	s.free -= n
	return nil
}

func (s *scatterSlots) release(n int) {
	s.mu.Lock()
	s.free += n
	s.mu.Unlock()
	s.cond.Broadcast()
}

// scatterAllowed refuses multi-shard reads the session state cannot carry.
func (e *Executor) scatterAllowed(shards int) error {
	if e.tx != pgwire.TxIdle && e.txnTouched {
		err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "multi-shard read inside a transaction pinned to shard %s/%d is not available yet", e.shard.Set, e.shard.ID)
		err.Hint = "run the multi-shard SELECT outside the transaction, or filter on the transaction's shard key"
		return err
	}
	if e.strictIsolationInEffect() {
		return isolationRefusal()
	}
	if m := e.r.cfg.Scatter.MaxShards; m > 0 && shards > m {
		err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "statement fans out to %d shards, more than the router's limit of %d", shards, m)
		err.Hint = "filter on fewer shard key values or raise the scatter shard limit"
		return err
	}
	if shards > e.r.cfg.Scatter.MaxStreams {
		return pgwire.Errorf("53300", "statement needs %d shard streams, more than the router's scatter stream budget of %d", shards, e.r.cfg.Scatter.MaxStreams)
	}
	return nil
}

// strictIsolationInEffect reports whether the session has asked for
// REPEATABLE READ or SERIALIZABLE, whether by the transaction's own BEGIN
// or by a session default that every backend will pick up.
func (e *Executor) strictIsolationInEffect() bool {
	if e.tx != pgwire.TxIdle {
		for _, sql := range e.txnPrelude {
			if strictIsolation(sql) {
				return true
			}
		}
	}
	for _, g := range append(append([]gucEntry(nil), e.gucs...), e.staged...) {
		if (g.name == "default_transaction_isolation" || g.name == "session characteristics") && strictIsolation(g.sql) {
			return true
		}
	}
	return false
}

func strictIsolation(sql string) bool {
	s := strings.ToLower(sql)
	return strings.Contains(s, "repeatable read") || strings.Contains(s, "serializable")
}

func isolationRefusal() error {
	err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "multi-shard reads under REPEATABLE READ or SERIALIZABLE isolation are not available yet")
	err.Hint = "the shards take independent snapshots; use READ COMMITTED for multi-shard SELECT"
	return err
}

// scatterSimple runs a multi-shard simple-protocol SELECT.
func (e *Executor) scatterSimple(ctx context.Context, pl plan.Plan, sql string, w pgwire.ResultWriter) error {
	m, err := pl.MultiShard()
	if err != nil {
		return err
	}
	if m.ShardSQL != "" {
		sql = m.ShardSQL
	}
	return e.runScatter(ctx, pl.Shards, m, []*pgshardv1.ExecuteRequest{simpleQuery(sql)}, scatterOutput{execute: true, describe: true}, w, e.rewriting(pl))
}

// scatterBatch runs an extended-protocol batch bound to a multi-shard plan:
// the batch is rewritten onto the unnamed statement and portal so the
// shard backends, which the pooler returns to its pool afterwards, keep
// nothing of it.
func (e *Executor) scatterBatch(ctx context.Context, pl plan.Plan, stmt string, batch []*pgshardv1.ExecuteRequest, w pgwire.ResultWriter) error {
	m, err := pl.MultiShard()
	if err != nil {
		return err
	}
	st, ok := e.stmts[stmt]
	if !ok {
		return pgwire.Errorf("26000", "prepared statement %q does not exist", stmt)
	}
	// ShardSQL is derived from the same masked statement shardSQL returns,
	// so either carries the online-rewrite masking; st.sql is the client's
	// text and does not.
	sql := st.shardSQL()
	if m.ShardSQL != "" {
		sql = m.ShardSQL
	}
	var reqs []*pgshardv1.ExecuteRequest
	parsed, executes, described := false, false, false
	for _, req := range batch {
		switch r := req.Message.(type) {
		case *pgshardv1.ExecuteRequest_Parse:
			reqs = append(reqs, parseReq("", sql, r.Parse.ParamOids))
			parsed = true
		case *pgshardv1.ExecuteRequest_Bind:
			if !parsed {
				reqs = append(reqs, parseReq("", sql, st.oids))
				parsed = true
			}
			b := r.Bind
			reqs = append(reqs, &pgshardv1.ExecuteRequest{Message: &pgshardv1.ExecuteRequest_Bind{Bind: &pgshardv1.Bind{
				Params: b.Params, ParamFormats: b.ParamFormats, ResultFormats: b.ResultFormats}}})
			// The merge needs the row shape even when the client, holding a
			// cached description, does not ask for it.
			reqs = append(reqs, describeReq(pgwire.DescribePortal, ""))
		case *pgshardv1.ExecuteRequest_Describe:
			described = true
			if r.Describe.Kind == pgshardv1.Describe_KIND_STATEMENT {
				reqs = append(reqs, describeReq(pgwire.DescribeStatement, ""))
			}
		case *pgshardv1.ExecuteRequest_Execute:
			if r.Execute.MaxRows > 0 {
				err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "partial fetches (Execute with a row limit) from a multi-shard portal are not available yet")
				err.Hint = "execute the portal without a row limit, or filter on one shard key value"
				return err
			}
			if executes {
				return pgwire.Errorf(pgwire.CodeFeatureNotSupported, "a multi-shard portal can be executed once per batch")
			}
			executes = true
			reqs = append(reqs, executeReq("", 0))
		case *pgshardv1.ExecuteRequest_Close:
		}
	}
	reqs = append(reqs, syncReq())
	if !parsed {
		return nil
	}
	return e.runScatter(ctx, pl.Shards, m, reqs, scatterOutput{execute: executes, describe: described}, w, e.rewriting(pl))
}

// scatterOutput says which client-visible responses a scatter batch owes:
// a RowDescription (the client sent Describe) and rows plus CommandComplete
// (the client sent Execute).
type scatterOutput struct {
	execute  bool
	describe bool
}

// participant is one shard of a multi-shard read.
type participant struct {
	shard  Shard
	sid    string
	client pgshardv1.PoolerClient
	ps     *poolerStream

	// prelude holds the responses before the first row (ParseComplete,
	// ParameterDescription, RowDescription, ...); rows carries DataRows;
	// done closes after ReadyForQuery with the outcome in err.
	prelude []*pgshardv1.ExecuteResponse
	rowDesc *pgshardv1.RowDescription
	header  chan struct{}
	rows    chan [][]byte
	notices []*pgproto3.NoticeResponse
	// queued is the bytes of rows this participant has handed to the merge
	// and the merge has not taken yet; taken signals that it dropped.
	queued atomic.Int64
	taken  chan struct{}
	tag    string
	err    error
	done   chan struct{}
	stop   chan struct{}
	// reserved is set once the pooler pinned the backend so the session's
	// search_path could be applied; finish releases it.
	reserved bool
}

// scatterSetupConcurrency bounds how many participants are set up at
// once: enough that setup costs a round trip rather than one per shard,
// few enough that a wide fan-out does not open every stream at once.
const scatterSetupConcurrency = 32

// startParticipant opens a shard's stream, applies the session state and
// sends reqs. The participant is returned even when a later step failed,
// so the caller can finish the stream it opened.
func (e *Executor) startParticipant(ctx context.Context, sh Shard, seq, setup string, reqs []*pgshardv1.ExecuteRequest) (*participant, error) {
	if e.r.blocking(sh) {
		if ok, err := e.r.awaitConsistent(ctx, sh, false, e.r.cfg.Buffering.Window); err != nil {
			return nil, err
		} else if !ok {
			return nil, pgwire.Errorf(codeConnectionFailure, "shard %s/%d has no serving primary", sh.Set, sh.ID)
		}
	}
	client, err := e.r.cfg.Poolers.Client(sh)
	if err != nil {
		return nil, err
	}
	ps, err := openStream(e.ctx, client)
	if err != nil {
		return nil, pgwire.Errorf(codeConnectionFailure, "pooler of shard %s/%d refused the connection: %v", sh.Set, sh.ID, err)
	}
	p := &participant{shard: sh, sid: e.sid + "-x" + seq + "-" + strconv.FormatInt(int64(sh.ID), 10), client: client, ps: ps,
		header: make(chan struct{}), rows: make(chan [][]byte, 64), done: make(chan struct{}), stop: make(chan struct{}),
		taken: make(chan struct{}, 1)}
	gen := e.r.cfg.Poolers.Generation(sh)
	// A fresh scatter backend starts on the server defaults, so it gets
	// the same session state a routed backend is replayed: not just the
	// search_path the planner resolved relations under, but every SET
	// the session has run. SET ROLE in particular decides which grants
	// and row-level security policies apply, and a participant that
	// missed it would evaluate the query as the login role.
	if setup != "" {
		resp, rerr := client.Reserve(ctx, &pgshardv1.ReserveRequest{SessionId: p.sid, Generation: gen})
		if rerr != nil {
			return p, pgwire.Errorf(codeConnectionFailure, "pooler of shard %s/%d refused the connection: %v", sh.Set, sh.ID, rerr)
		}
		if resp.Error != nil {
			return p, toPgwireError(resp.Error)
		}
		p.reserved = true
		if err := ps.send(simpleQuery(setup), p.sid, gen, e.ident, e.info.Database); err != nil {
			return p, pgwire.Errorf(codeConnectionFailure, "pooler connection lost: %v", err)
		}
		if err := p.drain(ctx); err != nil {
			return p, err
		}
	}
	for _, req := range reqs {
		if err := ps.send(perShard(req), p.sid, gen, e.ident, e.info.Database); err != nil {
			return p, pgwire.Errorf(codeConnectionFailure, "pooler connection lost: %v", err)
		}
	}
	return p, nil
}

// runScatter merges a multi-shard read into w, waiting out a rewrite that
// is mid-cutover rather than refusing the read for its duration.
//
// A rewrite cuts over one shard at a time, so between the first shard's
// swap and the last the same column has a different type OID on different
// shards and a merge cannot describe the result. That window is short, and
// it is the same kind of window a cutover flip opens, which the router
// waits out rather than failing. It is bounded, not unbounded: a shard
// whose swap is blocked behind a lock can hold the window open, so past
// the buffering window the read is refused with the condition that says
// to retry, exactly as before.
//
// The retry is only safe because a shape mismatch is found before any of
// the merged output is written; the counting writer is what checks that
// rather than assuming it.
func (e *Executor) runScatter(ctx context.Context, shards []int32, m *plan.Merge, reqs []*pgshardv1.ExecuteRequest, out scatterOutput, w pgwire.ResultWriter, rewriting string) error {
	deadline := e.r.now().Add(e.r.cfg.Buffering.Window)
	waited := false
	for {
		cw := &countingWriter{w: w}
		err := e.scatterOnce(ctx, shards, m, reqs, out, cw, rewriting)
		if !isRewriteCutover(err) || cw.wrote || !e.r.now().Before(deadline) {
			if waited && err == nil {
				e.r.cfg.Logger.Info("multi-shard read served after waiting out a rewrite cutover", "session", e.sid, "table", rewriting)
			}
			return err
		}
		waited = true
		select {
		case <-time.After(e.r.cfg.Buffering.Poll):
		case <-ctx.Done():
			return err
		}
	}
}

// isRewriteCutover reports the refusal a scatter raises while a rewrite's
// shards disagree on the result shape.
func isRewriteCutover(err error) bool {
	var pe *pgwire.Error
	return errors.As(err, &pe) && pe.Code == codeRewriteInProgress
}

func (e *Executor) scatterOnce(ctx context.Context, shards []int32, m *plan.Merge, reqs []*pgshardv1.ExecuteRequest, out scatterOutput, w pgwire.ResultWriter, rewriting string) error {
	if err := e.scatterAllowed(len(shards)); err != nil {
		return err
	}
	if err := e.r.scatter.acquire(ctx, len(shards)); err != nil {
		return err
	}
	defer e.r.scatter.release(len(shards))
	e.r.metrics.ScatterFanout.Observe(float64(len(shards)))
	// Each scatter numbers its pooler sessions: releasing a reserved
	// participant is asynchronous, and a reused sid could be unpinned by
	// the previous scatter's late Release.
	e.scatterSeq++
	seq := strconv.FormatUint(e.scatterSeq, 10)
	// Every participant needs the same session state, so it is composed
	// once rather than per shard.
	settings := e.sessionSettings()
	if path := e.searchPath(); path != nil {
		// Pin the path the planner actually resolved relations under,
		// whatever the replayed sequence composed to.
		settings = append(settings, searchPathSQL(path))
	}
	setup := strings.Join(settings, "; ")

	parts := make([]*participant, len(shards))
	defer func() {
		for _, p := range parts {
			if p != nil {
				p.finish(e.r)
			}
		}
	}()
	// Setting a participant up costs a Reserve round trip and a session
	// state statement that has to be drained before the next one starts.
	// Serially that is one round trip per shard before any shard has begun
	// executing, so fan-out setup grew with the shard count instead of
	// staying the cost of the slowest shard.
	errs := make([]error, len(shards))
	var wg sync.WaitGroup
	sem := make(chan struct{}, scatterSetupConcurrency)
	for i, id := range shards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			// parts[i] is written before any step that can fail, so the
			// deferred cleanup finishes a participant whose setup broke
			// half way.
			parts[i], errs[i] = e.startParticipant(ctx, Shard{Set: e.userSet(), ID: id}, seq, setup, reqs)
		}()
	}
	wg.Wait()
	// Reported in shard order: which of several failing shards a scatter
	// blames should not depend on which goroutine lost the race.
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	// Any failure or client cancel interrupts every participant once.
	var cancelOnce sync.Once
	cancelAll := func() {
		cancelOnce.Do(func() {
			for _, p := range parts {
				go p.cancel(e.r)
			}
		})
	}
	stopWatch := context.AfterFunc(ctx, cancelAll)
	defer stopWatch()
	for _, p := range parts {
		go p.pump(cancelAll)
	}
	err := e.mergeScatter(parts, m, out, w, rewriting)
	if err != nil {
		cancelAll()
	}
	for _, p := range parts {
		p.stopRows()
		<-p.done
	}
	if err == nil {
		for _, p := range parts {
			if p.err != nil {
				err = p.err
				break
			}
		}
	}
	if err != nil {
		return err
	}
	for _, p := range parts {
		for _, n := range p.notices {
			if err := w.Notice(n); err != nil {
				return err
			}
		}
	}
	return nil
}

// perShard gives one shard its own ExecuteRequest header while sharing the
// payload. send() stamps the session id, generation, identity and database
// onto whatever it is handed, which is why each shard needs a struct of its
// own -- but it never touches Message, so the SQL, the parameters and the
// format vectors do not need copying. Deep-cloning them made the work of a
// scatter proportional to shards times protocol operations, for fields that
// are identical on every shard.
//
// Safe under the concurrent setup because the payload is only ever read:
// gRPC marshals it inside Send, and nothing on this path writes to it. The
// 32-shard fan-out test drives that sharing under the race detector, on
// the extended protocol, so the parameters and format vectors of one Bind
// really are marshalled by every participant at once.
func perShard(req *pgshardv1.ExecuteRequest) *pgshardv1.ExecuteRequest {
	return &pgshardv1.ExecuteRequest{Message: req.GetMessage()}
}

// mergeScatter waits for every participant's RowDescription, checks they
// agree, then streams the merged rows.
// rewriting names a table of this plan whose rewrite migration still has a
// working column, or "" when none has. A rewrite cuts over one shard at a
// time, so between the first shard's swap and the last the same column has
// a different type OID on different shards -- which is a defined stage of
// the migration, not the schema drift the mismatch otherwise means.
func (e *Executor) rewriting(pl plan.Plan) string {
	snap := e.currentSnapshot()
	if snap == nil {
		return ""
	}
	for _, k := range pl.Tables {
		if p, ok := snap.Tables[k]; ok && len(p.HiddenColumns) > 0 {
			return k.SchemaName + "." + k.TableName
		}
	}
	return ""
}

func (e *Executor) mergeScatter(parts []*participant, m *plan.Merge, out scatterOutput, w pgwire.ResultWriter, rewriting string) error {
	for _, p := range parts {
		<-p.header
	}
	first := parts[0]
	if first.rowDesc == nil {
		// The batch failed before producing a description (or only
		// parsed); relay what the first shard said and let the errors
		// surface.
		return e.relayPrelude(first.prelude, w)
	}
	for _, p := range parts[1:] {
		if err := sameDescription(first, p, rewriting); err != nil {
			return err
		}
	}
	if err := e.relayPrelude(first.prelude, w); err != nil {
		return err
	}
	fields := first.rowDesc.Fields
	if m.Hidden > len(fields) {
		return pgwire.Errorf(pgwire.CodeInternalError, "router: %d hidden sort columns but the shard row has %d", m.Hidden, len(fields))
	}
	if out.describe {
		if err := w.RowDescription(fieldDescriptions(fields[:len(fields)-m.Hidden])); err != nil {
			return err
		}
	}
	if !out.execute {
		return nil
	}
	cols := make([]scatter.Column, len(fields))
	for i, f := range fields {
		cols[i] = scatter.Column{TypeOID: f.TypeOid, Format: int16(f.Format)}
	}
	sources := make([]scatter.Source, len(parts))
	for i, p := range parts {
		sources[i] = p
	}
	n, err := scatter.Merge(m, cols, sources, w.DataRow)
	if err != nil {
		return err
	}
	for _, p := range parts {
		p.stopRows()
		<-p.done
		if p.err != nil {
			return p.err
		}
	}
	return w.CommandComplete("SELECT " + strconv.FormatInt(n, 10))
}

func (e *Executor) relayPrelude(msgs []*pgshardv1.ExecuteResponse, w pgwire.ResultWriter) error {
	for _, resp := range msgs {
		var err error
		switch m := resp.Message.(type) {
		case *pgshardv1.ExecuteResponse_ParameterDescription:
			e.inferParams(m.ParameterDescription.ParamOids)
			err = w.ParameterDescription(m.ParameterDescription.ParamOids)
		case *pgshardv1.ExecuteResponse_NoData:
			err = w.NoData()
		case *pgshardv1.ExecuteResponse_EmptyQuery:
			err = w.EmptyQueryResponse()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func sameDescription(a, b *participant, rewriting string) error {
	if b.rowDesc == nil {
		if b.err != nil {
			return b.err
		}
		return pgwire.Errorf(pgwire.CodeInternalError, "router: shard %s/%d returned no row description", b.shard.Set, b.shard.ID)
	}
	fa, fb := a.rowDesc.Fields, b.rowDesc.Fields
	if len(fa) != len(fb) {
		return descMismatch(a, b, fmt.Sprintf("%d vs %d columns", len(fa), len(fb)), rewriting)
	}
	for i := range fa {
		if fa[i].Name != fb[i].Name || fa[i].TypeOid != fb[i].TypeOid || fa[i].TypeModifier != fb[i].TypeModifier || fa[i].Format != fb[i].Format {
			return descMismatch(a, b, fmt.Sprintf("column %d is %s (oid %d) vs %s (oid %d)", i+1, fa[i].Name, fa[i].TypeOid, fb[i].Name, fb[i].TypeOid), rewriting)
		}
	}
	return nil
}

func descMismatch(a, b *participant, what, rewriting string) error {
	// Mid-rewrite this is the migration, not drift: a client told to align
	// the schema by hand would be told to undo the migration, and XX000
	// reads as a router bug rather than a stage to wait out.
	if rewriting != "" {
		err := pgwire.Errorf(codeRewriteInProgress, "table %s is being rewritten and its shards do not yet agree on the result shape: %s", rewriting, what)
		err.Hint = "the rewrite cuts over one shard at a time; retry once it has finished on every shard"
		return err
	}
	err := pgwire.Errorf("XX000", "shards %s/%d and %s/%d disagree on the result shape: %s", a.shard.Set, a.shard.ID, b.shard.Set, b.shard.ID, what)
	err.Hint = "the table definition differs between shards; align the schema on every shard"
	return err
}

// drain consumes the responses of the search_path application up to
// ReadyForQuery, surfacing any error the backend reported.
func (p *participant) drain(ctx context.Context) error {
	var failed error
	for {
		resp, err := p.ps.recv(ctx, nil)
		if err != nil {
			return pgwire.Errorf(codeConnectionFailure, "pooler connection lost: %v", err)
		}
		switch m := resp.Message.(type) {
		case *pgshardv1.ExecuteResponse_Error:
			if failed == nil {
				failed = toPgwireError(m.Error.GetError())
			}
		case *pgshardv1.ExecuteResponse_ReadyForQuery:
			return failed
		}
	}
}

// pump reads one participant's stream to ReadyForQuery, publishing the
// prelude, then rows, then the outcome. onError interrupts the others.
func (p *participant) pump(onError func()) {
	defer close(p.done)
	headerSent := false
	sendHeader := func() {
		if !headerSent {
			headerSent = true
			close(p.header)
		}
	}
	defer sendHeader()
	stopped := false
	for {
		resp, err := p.ps.recv(context.Background(), nil)
		if err != nil {
			p.err = pgwire.Errorf(codeConnectionFailure, "pooler connection lost: %v", err)
			onError()
			return
		}
		switch m := resp.Message.(type) {
		case *pgshardv1.ExecuteResponse_RowDescription:
			if p.rowDesc == nil {
				p.rowDesc = m.RowDescription
			}
			sendHeader()
		case *pgshardv1.ExecuteResponse_DataRow:
			if headerSent && !stopped {
				row := rowValues(m.DataRow)
				if !p.waitForRoom() {
					stopped = true
					break
				}
				p.queued.Add(rowBytes(row))
				select {
				case p.rows <- row:
				case <-p.stop:
					p.queued.Add(-rowBytes(row))
					stopped = true
				}
			}
		case *pgshardv1.ExecuteResponse_CommandComplete:
			p.tag = m.CommandComplete.Tag
			sendHeader()
			close(p.rows)
			stopped = true
		case *pgshardv1.ExecuteResponse_Error:
			if p.err == nil {
				p.err = toPgwireError(m.Error.GetError())
			}
			onError()
		case *pgshardv1.ExecuteResponse_Notice:
			p.notices = append(p.notices, toNotice(m.Notice.GetNotice()))
		case *pgshardv1.ExecuteResponse_ReadyForQuery:
			if !stopped {
				close(p.rows)
			}
			return
		case *pgshardv1.ExecuteResponse_CopyInResponse, *pgshardv1.ExecuteResponse_CopyOutResponse:
			p.err = pgwire.Errorf(pgwire.CodeFeatureNotSupported, "COPY cannot run on multiple shards")
			onError()
		default:
			if !headerSent {
				p.prelude = append(p.prelude, resp)
			}
		}
	}
}

// scatterRowBudget is what one participant may hold in rows the merge has
// not taken. The queue used to be bounded by row count alone, at 64: a
// row is anything from a few bytes to a megabyte, so a wide result held
// 64 megabytes per shard and a fan-out over a large topology multiplied
// that by the shard count. Bytes bound it either way, and the count bound
// stays as a second limit so a narrow result cannot queue without end.
const scatterRowBudget = 256 << 10

// waitForRoom blocks until this participant is under its byte budget, and
// reports whether the merge still wants rows. A participant is always let
// through when it holds nothing, so the merge can always reach a head row
// on every shard and an ordered merge cannot stall itself.
func (p *participant) waitForRoom() bool {
	for p.queued.Load() >= scatterRowBudget {
		select {
		case <-p.taken:
		case <-p.stop:
			return false
		}
	}
	return true
}

func rowBytes(row [][]byte) int64 {
	n := int64(0)
	for _, v := range row {
		n += int64(len(v)) + 8
	}
	return n
}

// Next implements scatter.Source over the participant's rows.
func (p *participant) Next() ([][]byte, bool, error) {
	row, ok := <-p.rows
	if !ok {
		return nil, false, p.err
	}
	p.queued.Add(-rowBytes(row))
	select {
	case p.taken <- struct{}{}:
	default:
	}
	return row, true, nil
}

// stopRows lets the pump discard the rows the merge no longer wants.
func (p *participant) stopRows() {
	select {
	case <-p.stop:
	default:
		close(p.stop)
	}
}

func (p *participant) cancel(r *Router) {
	cctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()
	if _, err := p.client.Cancel(cctx, &pgshardv1.CancelRequest{SessionId: p.sid}); err != nil {
		r.cfg.Logger.Warn("scatter cancel failed", "session", p.sid, "shard", p.shard, "err", err)
	}
}

// finish releases the participant's stream once its pump has ended and
// unpins the backend a search_path application reserved.
func (p *participant) finish(r *Router) {
	select {
	case <-p.done:
		p.ps.close()
	default:
		p.ps.abort()
	}
	if p.reserved {
		go p.release(r)
	}
}

func (p *participant) release(r *Router) {
	rctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()
	if _, err := p.client.Release(rctx, &pgshardv1.ReleaseRequest{SessionId: p.sid}); err != nil {
		r.cfg.Logger.Warn("scatter release failed", "session", p.sid, "shard", p.shard, "err", err)
	}
}
