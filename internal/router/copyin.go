package router

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/router/copysplit"
	"github.com/andrew01234567890/pgshard/internal/router/plan"
)

// copyFlushBytes is how much of one shard's rows the router holds before it
// sends them on. Rows are routed one at a time, and a CopyData message per
// row would multiply the messages by the row count.
const copyFlushBytes = 64 << 10

// copyMaxLine bounds the one row the router holds while it waits for the
// row's end. PostgreSQL's own limit is a gigabyte; the router holds the row
// in memory on behalf of one client, so a stream that never ends a line
// must not grow it without bound.
var copyMaxLine = 64 << 20

// copyShardedIn runs COPY ... FROM STDIN into a sharded table.
//
// Every shard starts the same COPY inside one transaction of the router's,
// the client's rows are split and each goes to the shard its key hashes to,
// and the transaction commits across the shards like any multi-shard write:
// with two-phase commit, so a load that fails anywhere leaves nothing
// behind on any shard. A client that opened the transaction itself keeps
// it, and commits the rows with the rest of its work.
func (e *Executor) copyShardedIn(ctx context.Context, pl plan.Plan, sql string, w pgwire.ResultWriter) error {
	if e.catalogSession() {
		return pgwire.Errorf(pgwire.CodeInternalError, "router: sharded COPY on the catalog shard set")
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
	targetShards := make([]Shard, 0, len(pl.Shards))
	for _, id := range pl.Shards {
		targetShards = append(targetShards, Shard{Set: e.userSet(), ID: id})
	}
	e.openParts(ctx, targetShards)
	for _, sh := range targetShards {
		if err := e.moveTo(ctx, sh); err != nil {
			return e.referenceFailed(ctx, implicit, err)
		}
		if err := e.acquire(ctx, nil); err != nil {
			return e.referenceFailed(ctx, implicit, err)
		}
		if err := e.noteWrite(ctx); err != nil {
			return e.referenceFailed(ctx, implicit, err)
		}
	}
	parts := map[int32]*txnPart{}
	var targets []*txnPart
	for _, p := range e.parts() {
		for _, id := range pl.Shards {
			if p.shard.ID == id {
				parts[id] = p
				targets = append(targets, p)
				break
			}
		}
	}
	e.each(targets, func(p *txnPart) error { return e.startCopyOn(ctx, p, sql) })
	if err := firstError(targets); err != nil {
		return e.copyFailed(ctx, implicit, targets, err)
	}
	rows, err := e.relayCopyRows(pl, w, parts)
	if err != nil {
		return e.copyFailed(ctx, implicit, targets, err)
	}
	e.each(targets, func(p *txnPart) error {
		return e.runReqsOn(ctx, p, []*pgshardv1.ExecuteRequest{copyDoneReq()}, discardWriter{})
	})
	e.syncCurrent(targets)
	if err := firstError(targets); err != nil {
		return e.referenceFailed(ctx, implicit, err)
	}
	var total int64
	for _, p := range targets {
		n, perr := copyCount(p.tag)
		if perr != nil {
			return e.referenceFailed(ctx, implicit, perr)
		}
		total += n
	}
	// The shards counted what they stored; the router counted what it sent.
	// They differ only if a row was lost between the two, which is the one
	// thing a load must never report as success.
	if total != rows {
		err := pgwire.Errorf(pgwire.CodeInternalError, "router: COPY sent %d rows and the shards stored %d", rows, total)
		return e.referenceFailed(ctx, implicit, err)
	}
	if implicit {
		// Committed before the tag is sent: a client told COPY n must not
		// then learn that the commit failed. A shard set of one shard never
		// became a multi-shard transaction, and endTxn is only for those.
		if e.multiShardTxn() {
			if err := e.endTxn(ctx, true, discardWriter{}); err != nil {
				return err
			}
		} else {
			if err := e.send(simpleQuery("COMMIT")); err != nil {
				return e.referenceFailed(ctx, implicit, err)
			}
			if err := e.pump(ctx, discardWriter{}); err != nil {
				return e.referenceFailed(ctx, implicit, err)
			}
		}
	}
	e.lastTag = fmt.Sprintf("COPY %d", total)
	return w.CommandComplete(e.lastTag)
}

// startCopyOn sends the COPY to one shard and reads until the shard is
// ready for rows.
func (e *Executor) startCopyOn(ctx context.Context, p *txnPart, sql string) error {
	if err := e.sendOn(p, simpleQuery(sql)); err != nil {
		return err
	}
	var firstErr error
	for {
		resp, err := p.ps.recv(ctx, nil)
		if err != nil {
			return poolerTransportError(fmt.Sprintf("shard %s/%d", p.shard.Set, p.shard.ID), err)
		}
		switch m := resp.Message.(type) {
		case *pgshardv1.ExecuteResponse_CopyInResponse:
			if m.CopyInResponse.GetFormat() != 0 {
				return pgwire.Errorf(pgwire.CodeFeatureNotSupported, "COPY into a sharded table takes the text format only")
			}
			return nil
		case *pgshardv1.ExecuteResponse_Error:
			if firstErr == nil {
				firstErr = toPgwireError(m.Error.GetError())
			}
		case *pgshardv1.ExecuteResponse_ReadyForQuery:
			p.tx = txStatus(m.ReadyForQuery.TxnStatus)
			if firstErr == nil {
				firstErr = pgwire.Errorf(pgwire.CodeInternalError, "router: shard %s/%d answered the COPY without asking for rows", p.shard.Set, p.shard.ID)
			}
			return firstErr
		}
	}
}

// relayCopyRows reads the client's rows and sends each to its shard. It
// returns how many rows it sent.
func (e *Executor) relayCopyRows(pl plan.Plan, w pgwire.ResultWriter, parts map[int32]*txnPart) (int64, error) {
	formats := make([]uint16, pl.Copy.Columns)
	in, err := w.CopyIn(0, formats)
	if err != nil {
		return 0, err
	}
	split := copysplit.New(pl.Copy.KeyColumn, pl.Copy.Columns)
	pending := map[int32][]byte{}
	var rows int64
	flush := func(sh int32) error {
		if len(pending[sh]) == 0 {
			return nil
		}
		err := e.sendOn(parts[sh], copyDataReq(pending[sh]))
		pending[sh] = pending[sh][:0]
		return err
	}
	ended := false
	route := func() error {
		for {
			row, ok, err := split.Next()
			if err != nil {
				return pgwire.Errorf("22P04", "%v", err)
			}
			if !ok {
				return nil
			}
			if ended {
				continue
			}
			if row.Key == "" && !row.KeyIsNull && strings.TrimRight(string(row.Bytes), "\r\n") == `\.` {
				// PostgreSQL stops reading at the end-of-data marker and
				// ignores whatever follows it.
				ended = true
				continue
			}
			if row.KeyIsNull {
				return pgwire.Errorf("23502", "COPY row has a NULL shard key, which places it on no shard")
			}
			sh, err := pl.CopyShard(row.Key)
			if err != nil {
				return err
			}
			if parts[sh] == nil {
				return pgwire.Errorf(pgwire.CodeInternalError, "router: COPY row belongs to shard %d, which the COPY was not started on", sh)
			}
			pending[sh] = append(pending[sh], row.Bytes...)
			rows++
			if len(pending[sh]) >= copyFlushBytes {
				if err := flush(sh); err != nil {
					return err
				}
			}
		}
	}
	for {
		data, err := in.Next()
		switch {
		case err == nil:
			split.Write(data)
			if rerr := route(); rerr != nil {
				return 0, rerr
			}
			if len(split.Rest()) > copyMaxLine {
				return 0, pgwire.Errorf("54000", "COPY row is longer than the %d MiB the router holds for one row", copyMaxLine>>20)
			}
		case errors.Is(err, pgwire.ErrCopyFail):
			return 0, pgwire.Errorf("57014", "COPY from stdin failed: COPY terminated by client")
		case errors.Is(err, io.EOF):
			// A last row without its newline is still a row, as it is to
			// PostgreSQL.
			if rest := split.Rest(); len(rest) > 0 {
				split.Write([]byte("\n"))
				if rerr := route(); rerr != nil {
					return 0, rerr
				}
			}
			for sh := range pending {
				if ferr := flush(sh); ferr != nil {
					return 0, ferr
				}
			}
			return rows, nil
		default:
			return 0, err
		}
	}
}

// copyFailed ends the COPY on every shard it reached and the transaction
// with it, and hands the failure on.
func (e *Executor) copyFailed(ctx context.Context, implicit bool, targets []*txnPart, err error) error {
	e.each(targets, func(p *txnPart) error {
		if p.ps == nil || p.tx != pgwire.TxInBlock {
			return nil
		}
		_ = e.runReqsOn(ctx, p, []*pgshardv1.ExecuteRequest{copyFailReq("COPY aborted by the router")}, discardWriter{})
		return nil
	})
	e.syncCurrent(targets)
	return e.referenceFailed(ctx, implicit, err)
}

// sendOn sends one request on a transaction part's stream.
func (e *Executor) sendOn(p *txnPart, req *pgshardv1.ExecuteRequest) error {
	if p.ps == nil {
		return pgwire.Errorf(codeConnectionFailure, "shard %s/%d: no stream", p.shard.Set, p.shard.ID)
	}
	if err := p.ps.send(perShard(req), e.sid, e.r.cfg.Poolers.Generation(p.shard), e.ident, e.info.Database, e.statement.Load()); err != nil {
		return poolerTransportError(fmt.Sprintf("shard %s/%d", p.shard.Set, p.shard.ID), err)
	}
	return nil
}

func copyCount(tag string) (int64, error) {
	n, err := strconv.ParseInt(strings.TrimPrefix(tag, "COPY "), 10, 64)
	if err != nil || !strings.HasPrefix(tag, "COPY ") {
		return 0, pgwire.Errorf(pgwire.CodeInternalError, "router: a shard answered COPY with %q", tag)
	}
	return n, nil
}
