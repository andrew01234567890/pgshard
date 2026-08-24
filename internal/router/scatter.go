package router

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgproto3"
	"google.golang.org/protobuf/proto"

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
}

const defaultScatterStreams = 4096

func (c ScatterConfig) withDefaults() ScatterConfig {
	if c.MaxStreams <= 0 {
		c.MaxStreams = defaultScatterStreams
	}
	return c
}

// scatterSlots is the router-wide budget of open scatter streams.
type scatterSlots struct {
	mu   sync.Mutex
	cond *sync.Cond
	free int
}

func newScatterSlots(n int) *scatterSlots {
	s := &scatterSlots{free: n}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// acquire takes n slots, waiting while the budget is exhausted.
func (s *scatterSlots) acquire(ctx context.Context, n int) error {
	stop := context.AfterFunc(ctx, func() {
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	})
	defer stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.free < n {
		if ctx.Err() != nil {
			return pgwire.Errorf("57014", "canceling statement while waiting for scatter capacity")
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
	if e.tx != pgwire.TxIdle {
		for _, sql := range e.txnPrelude {
			if strictIsolation(sql) {
				return isolationRefusal()
			}
		}
	}
	for _, g := range append(append([]gucEntry(nil), e.gucs...), e.staged...) {
		if (g.name == "default_transaction_isolation" || g.name == "session characteristics") && strictIsolation(g.sql) {
			return isolationRefusal()
		}
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
	return e.runScatter(ctx, pl.Shards, m, []*pgshardv1.ExecuteRequest{simpleQuery(sql)}, scatterOutput{execute: true, describe: true}, w)
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
	sql := st.sql
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
	return e.runScatter(ctx, pl.Shards, m, reqs, scatterOutput{execute: executes, describe: described}, w)
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
	tag     string
	err     error
	done    chan struct{}
	stop    chan struct{}
}

// runScatter opens one pooler stream per shard, sends reqs on each and
// merges the responses into w.
func (e *Executor) runScatter(ctx context.Context, shards []int32, m *plan.Merge, reqs []*pgshardv1.ExecuteRequest, out scatterOutput, w pgwire.ResultWriter) error {
	if err := e.scatterAllowed(len(shards)); err != nil {
		return err
	}
	if err := e.r.scatter.acquire(ctx, len(shards)); err != nil {
		return err
	}
	defer e.r.scatter.release(len(shards))
	parts := make([]*participant, 0, len(shards))
	defer func() {
		for _, p := range parts {
			p.finish()
		}
	}()
	for _, id := range shards {
		sh := Shard{Set: e.home.Set, ID: id}
		if e.r.blocking(sh) {
			if ok, err := e.r.awaitConsistent(ctx, sh, false, e.r.cfg.Buffering.Window); err != nil {
				return err
			} else if !ok {
				return pgwire.Errorf(codeConnectionFailure, "shard %s/%d has no serving primary", sh.Set, sh.ID)
			}
		}
		client, err := e.r.cfg.Poolers.Client(sh)
		if err != nil {
			return err
		}
		ps, err := openStream(e.ctx, client)
		if err != nil {
			return pgwire.Errorf(codeConnectionFailure, "pooler of shard %s/%d refused the connection: %v", sh.Set, sh.ID, err)
		}
		p := &participant{shard: sh, sid: e.sid + "-x" + strconv.FormatInt(int64(id), 10), client: client, ps: ps,
			header: make(chan struct{}), rows: make(chan [][]byte, 64), done: make(chan struct{}), stop: make(chan struct{})}
		parts = append(parts, p)
		gen := e.r.cfg.Poolers.Generation(sh)
		for _, req := range reqs {
			if err := ps.send(cloneRequest(req), p.sid, gen, e.ident); err != nil {
				return pgwire.Errorf(codeConnectionFailure, "pooler connection lost: %v", err)
			}
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
	err := e.mergeScatter(parts, m, out, w)
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

func cloneRequest(req *pgshardv1.ExecuteRequest) *pgshardv1.ExecuteRequest {
	return proto.Clone(req).(*pgshardv1.ExecuteRequest)
}

// mergeScatter waits for every participant's RowDescription, checks they
// agree, then streams the merged rows.
func (e *Executor) mergeScatter(parts []*participant, m *plan.Merge, out scatterOutput, w pgwire.ResultWriter) error {
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
		if err := sameDescription(first, p); err != nil {
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

func sameDescription(a, b *participant) error {
	if b.rowDesc == nil {
		if b.err != nil {
			return b.err
		}
		return pgwire.Errorf(pgwire.CodeInternalError, "router: shard %s/%d returned no row description", b.shard.Set, b.shard.ID)
	}
	fa, fb := a.rowDesc.Fields, b.rowDesc.Fields
	if len(fa) != len(fb) {
		return descMismatch(a, b, fmt.Sprintf("%d vs %d columns", len(fa), len(fb)))
	}
	for i := range fa {
		if fa[i].Name != fb[i].Name || fa[i].TypeOid != fb[i].TypeOid || fa[i].TypeModifier != fb[i].TypeModifier || fa[i].Format != fb[i].Format {
			return descMismatch(a, b, fmt.Sprintf("column %d is %s (oid %d) vs %s (oid %d)", i+1, fa[i].Name, fa[i].TypeOid, fb[i].Name, fb[i].TypeOid))
		}
	}
	return nil
}

func descMismatch(a, b *participant, what string) error {
	err := pgwire.Errorf("XX000", "shards %s/%d and %s/%d disagree on the result shape: %s", a.shard.Set, a.shard.ID, b.shard.Set, b.shard.ID, what)
	err.Hint = "the table definition differs between shards; align the schema on every shard"
	return err
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
				select {
				case p.rows <- rowValues(m.DataRow.Columns):
				case <-p.stop:
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

// Next implements scatter.Source over the participant's rows.
func (p *participant) Next() ([][]byte, bool, error) {
	row, ok := <-p.rows
	if !ok {
		return nil, false, p.err
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

// finish releases the participant's stream once its pump has ended.
func (p *participant) finish() {
	select {
	case <-p.done:
		p.ps.close()
	default:
		p.ps.abort()
	}
}
