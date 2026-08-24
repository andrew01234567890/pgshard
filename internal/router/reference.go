package router

import (
	"context"
	"encoding/binary"
	"strconv"

	"github.com/jackc/pgx/v5/pgproto3"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/router/plan"
)

// abortEverywhereSQL puts a backend whose statement succeeded into the
// aborted state when the same statement failed on another shard, so the
// transaction can only roll back.
const abortEverywhereSQL = "DO $$ BEGIN RAISE EXCEPTION 'pgshard: the statement failed on another shard, the transaction must roll back' USING ERRCODE = '40000'; END $$"

// isReferenceWrite reports a plan that writes a reference table on several
// shards; with one shard in the set it is an ordinary single-shard write.
func isReferenceWrite(pl plan.Plan) bool {
	return pl.Kind == plan.Reference && pl.Class.Write && !pl.Deferred && len(pl.Shards) > 1
}

// referenceWrite runs one statement (a simple query, or an extended batch
// ending in Sync) on every shard of pl inside the session's transaction.
// The rows and tag of the lowest shard reach the client. Outside a
// transaction the write is wrapped in one, committed with two-phase commit
// before the statement is reported complete.
func (e *Executor) referenceWrite(ctx context.Context, pl plan.Plan, reqs []*pgshardv1.ExecuteRequest, w pgwire.ResultWriter) error {
	if e.catalogSession() {
		return pgwire.Errorf(pgwire.CodeInternalError, "router: reference write on the catalog shard set")
	}
	implicit := e.tx == pgwire.TxIdle
	if err := e.acquire(ctx, nil); err != nil {
		return err
	}
	if implicit {
		if err := e.send(simpleQuery("BEGIN")); err != nil {
			return err
		}
		if err := e.pump(ctx, discardWriter{}); err != nil {
			return err
		}
		e.txnPrelude = append(e.txnPrelude, "BEGIN")
	}
	e.txnTouched = true
	for _, id := range pl.Shards {
		if err := e.moveTo(ctx, Shard{Set: e.userSet(), ID: id}); err != nil {
			return e.referenceFailed(ctx, implicit, err)
		}
		if err := e.acquire(ctx, nil); err != nil {
			return e.referenceFailed(ctx, implicit, err)
		}
		if err := e.noteWrite(ctx); err != nil {
			return e.referenceFailed(ctx, implicit, err)
		}
	}
	var targets []*txnPart
	for _, p := range e.parts() {
		for _, id := range pl.Shards {
			if p.shard.ID == id {
				targets = append(targets, p)
				break
			}
		}
	}
	e.each(targets, func(p *txnPart) error {
		var out pgwire.ResultWriter = discardWriter{}
		if p == targets[0] {
			out = w
		}
		return e.runReqsOn(ctx, p, reqs, out)
	})
	e.syncCurrent(targets)
	if err := firstError(targets); err != nil {
		e.each(targets, func(p *txnPart) error {
			if p.err == nil && p.tx == pgwire.TxInBlock {
				return e.runOn(ctx, p, abortEverywhereSQL, discardWriter{})
			}
			return nil
		})
		e.syncCurrent(targets)
		return e.referenceFailed(ctx, implicit, err)
	}
	if implicit {
		return e.endTxn(ctx, true, discardWriter{})
	}
	return nil
}

// syncCurrent copies the state of the current shard's part, run through
// runReqsOn, back onto the executor.
func (e *Executor) syncCurrent(parts []*txnPart) {
	for _, p := range parts {
		if p.shard == e.shard {
			e.tx = p.tx
			if p.tag != "" {
				e.lastTag = p.tag
			}
		}
	}
}

// referenceFailed rolls an implicit transaction back after a failure and
// hands the failure on.
func (e *Executor) referenceFailed(ctx context.Context, implicit bool, err error) error {
	if implicit && e.tx != pgwire.TxIdle {
		if e.multiShardTxn() {
			_ = e.endTxn(ctx, false, discardWriter{})
		} else if e.conn != nil {
			if serr := e.send(simpleQuery("ROLLBACK")); serr == nil {
				_ = e.pump(ctx, discardWriter{})
			}
		}
	}
	return err
}

// unnamedBatch rewrites an extended batch that carries one statement onto
// the unnamed statement and portal, with sql as its text and oids as its
// parameter types, so every shard parses it afresh. Close messages are
// dropped; the returned batch ends in Sync.
func unnamedBatch(sql string, oids []uint32, batch []*pgshardv1.ExecuteRequest) []*pgshardv1.ExecuteRequest {
	var reqs []*pgshardv1.ExecuteRequest
	parsed := false
	for _, req := range batch {
		switch r := req.Message.(type) {
		case *pgshardv1.ExecuteRequest_Parse:
			reqs = append(reqs, parseReq("", sql, oids))
			parsed = true
		case *pgshardv1.ExecuteRequest_Bind:
			if !parsed {
				reqs = append(reqs, parseReq("", sql, oids))
				parsed = true
			}
			b := r.Bind
			reqs = append(reqs, &pgshardv1.ExecuteRequest{Message: &pgshardv1.ExecuteRequest_Bind{Bind: &pgshardv1.Bind{
				Params: b.Params, ParamFormats: b.ParamFormats, ResultFormats: b.ResultFormats}}})
		case *pgshardv1.ExecuteRequest_Describe:
			kind := pgwire.DescribePortal
			if r.Describe.Kind == pgshardv1.Describe_KIND_STATEMENT {
				kind = pgwire.DescribeStatement
			}
			reqs = append(reqs, describeReq(kind, ""))
		case *pgshardv1.ExecuteRequest_Execute:
			reqs = append(reqs, executeReq("", r.Execute.MaxRows))
		}
	}
	return append(reqs, syncReq())
}

// nextvalBatch answers an extended batch whose statement is a nextval()
// over a global sequence; such a statement must be alone in its batch.
func (e *Executor) nextvalBatch(ctx context.Context, batch []*pgshardv1.ExecuteRequest, parsed []string, w pgwire.ResultWriter) (bool, error) {
	name := ""
	other := false
	binary := false
	stmtOf := func(portal string) string { return e.portals[portal] }
	for _, req := range batch {
		var stmt string
		switch r := req.Message.(type) {
		case *pgshardv1.ExecuteRequest_Parse:
			if len(parsed) == 0 {
				continue
			}
			stmt, parsed = parsed[0], parsed[1:]
		case *pgshardv1.ExecuteRequest_Bind:
			stmt = stmtOf(r.Bind.Portal)
			for _, f := range r.Bind.ResultFormats {
				binary = f == 1
			}
		case *pgshardv1.ExecuteRequest_Execute:
			stmt = stmtOf(r.Execute.Portal)
		default:
			continue
		}
		st, ok := e.stmts[stmt]
		switch {
		case ok && st.plan.NextVal != "":
			name = st.plan.NextVal
		default:
			other = true
		}
	}
	if name == "" {
		return false, nil
	}
	if other {
		err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "nextval() over a global sequence must be the only statement of its batch")
		err.Hint = "send a Sync before and after it"
		return true, err
	}
	for _, req := range batch {
		switch r := req.Message.(type) {
		case *pgshardv1.ExecuteRequest_Describe:
			if r.Describe.Kind == pgshardv1.Describe_KIND_STATEMENT {
				if err := w.ParameterDescription(nil); err != nil {
					return true, err
				}
			}
			if err := e.answerNextval(ctx, name, true, false, binary, w); err != nil {
				return true, err
			}
		case *pgshardv1.ExecuteRequest_Execute:
			if err := e.answerNextval(ctx, name, false, true, binary, w); err != nil {
				return true, err
			}
		}
	}
	return true, nil
}

// answerNextval serves `SELECT nextval('seq')` from the router's block of
// the global sequence: describe reports the row shape, execute the value.
func (e *Executor) answerNextval(ctx context.Context, name string, describe, execute bool, binary bool, w pgwire.ResultWriter) error {
	if e.r.cfg.Sequences == nil {
		return noSequenceAllocator()
	}
	if describe {
		if err := w.RowDescription([]pgproto3.FieldDescription{nextvalField(binary)}); err != nil {
			return err
		}
	}
	if !execute {
		return nil
	}
	vals, err := e.r.cfg.Sequences.Next(ctx, name, 1)
	if err != nil {
		return err
	}
	if err := w.DataRow([][]byte{encodeInt8(vals[0], binary)}); err != nil {
		return err
	}
	return w.CommandComplete("SELECT 1")
}

func noSequenceAllocator() error {
	err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "global sequences are not available: the router has no sequence allocator")
	err.Hint = "start the router with a catalog connection that may call pgshard.allocate_sequence_block"
	return err
}

func nextvalField(binary bool) pgproto3.FieldDescription {
	f := pgproto3.FieldDescription{Name: []byte("nextval"), DataTypeOID: 20, DataTypeSize: 8, TypeModifier: -1}
	if binary {
		f.Format = 1
	}
	return f
}

func encodeInt8(v int64, binary bool) []byte {
	if binary {
		var b [8]byte
		binaryBigEndianPut(b[:], uint64(v))
		return b[:]
	}
	return []byte(strconv.FormatInt(v, 10))
}

func binaryBigEndianPut(b []byte, v uint64) { binary.BigEndian.PutUint64(b, v) }

// sequenceValues allocates the values of a SequenceFill in parameter order,
// encoded as text bind parameters.
func (e *Executor) sequenceValues(ctx context.Context, fill *plan.SequenceFill) ([][]byte, []int64, error) {
	if e.r.cfg.Sequences == nil {
		return nil, nil, noSequenceAllocator()
	}
	counts := map[string]int{}
	for _, n := range fill.Names {
		counts[n]++
	}
	blocks := map[string][]int64{}
	for name, n := range counts {
		vals, err := e.r.cfg.Sequences.Next(ctx, name, n)
		if err != nil {
			return nil, nil, err
		}
		blocks[name] = vals
	}
	params := make([][]byte, len(fill.Names))
	values := make([]int64, len(fill.Names))
	for i, name := range fill.Names {
		values[i] = blocks[name][0]
		blocks[name] = blocks[name][1:]
		params[i] = []byte(strconv.FormatInt(values[i], 10))
	}
	return params, values, nil
}

// injectedParams is the Params view of a Bind whose trailing parameters the
// router filled: shard keys among them are the injected int64 values.
type injectedParams struct {
	client plan.Params
	base   int
	values []int64
}

func (p injectedParams) ShardKey(n int32, hint plan.TypeHint) (any, error) {
	if i := int(n) - 1 - p.base; i >= 0 && i < len(p.values) {
		return p.values[i], nil
	}
	if p.client == nil {
		return nil, pgwire.Errorf(pgwire.CodeInternalError, "router: parameter $%d has no value", n)
	}
	return p.client.ShardKey(n, hint)
}

// extendOIDs pads the client's parameter types to base and appends int8 for
// each injected sequence value.
func extendOIDs(oids []uint32, base, injected int) []uint32 {
	out := make([]uint32, base+injected)
	copy(out, oids)
	for i := base; i < base+injected; i++ {
		out[i] = 20
	}
	return out
}

// extendFormats keeps the client's parameter formats and marks the injected
// values as text.
func extendFormats(formats []int16, base, injected int) []int16 {
	if len(formats) == 0 || (len(formats) == 1 && formats[0] == 0) {
		return formats
	}
	out := make([]int16, base+injected)
	for i := 0; i < base; i++ {
		switch len(formats) {
		case 1:
			out[i] = formats[0]
		default:
			if i < len(formats) {
				out[i] = formats[i]
			}
		}
	}
	return out
}

// noDataFilter hides the NoData a Describe of a non-returning INSERT
// produces when the router runs a simple query as an extended batch.
type noDataFilter struct{ pgwire.ResultWriter }

func (noDataFilter) NoData() error { return nil }
