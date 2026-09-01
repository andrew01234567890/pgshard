package router

import (
	"context"
	"errors"
	"time"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/router/plan"
)

// codeReadOnlyTransaction is PostgreSQL's own answer when a shard is paused
// and something reached it anyway: the pause is
// default_transaction_read_only, and a transaction that began under it
// cannot write, whatever the router believed when it sent the statement.
const codeReadOnlyTransaction = "25006"

// codeWriteFence is cannot_connect_now: the cluster is pausing writes (for
// a certified restore point, or a reshard cutover fenced the serving
// shards) and the statement waited out the buffering window.
const codeWriteFence = "57P03"

func writeFenceError(migrating, table bool) error {
	if table {
		err := pgwire.Errorf(codeWriteFence, "write pause for a table placement change")
		err.Hint = "the pause ends when the table's new placement is published; retry the statement"
		return err
	}
	if migrating {
		err := pgwire.Errorf(codeWriteFence, "cluster write pause for a reshard cutover")
		err.Hint = "the pause ends when the new shard map is published; retry the statement"
		return err
	}
	err := pgwire.Errorf(codeWriteFence, "cluster write pause for a certified restore point")
	err.Hint = "the pause ends when the barrier's restore points are recorded; retry the statement"
	return err
}

func fenceBufferFullError() error {
	return pgwire.Errorf(codeBufferFull, "too many statements waiting for the cluster write pause to end")
}

// writeFenced reports whether the current snapshot pauses writes: the
// cluster-wide barrier fence, or a reshard cutover fencing the serving
// shards (migrating). Reads never wait.
func (r *Router) writeFenced(tables []snapshot.TableKey) bool {
	snap := r.cfg.Snapshot()
	fenced := snap != nil && (snap.WriteFence || snap.Migrating() || snap.TableMigrating(tables))
	if fenced {
		r.fenceSeen.Store(time.Now().UnixNano())
	}
	return fenced
}

// sawWriteFenceRecently reports that this router observed the cluster write
// pause within the buffering window. A certified barrier is milliseconds
// long once it is not waiting on anything, so a statement it refused can
// easily return after it has finished, and the pause is then invisible to a
// check that only looks at now.
func (r *Router) sawWriteFenceRecently() bool {
	seen := r.fenceSeen.Load()
	return seen != 0 && time.Since(time.Unix(0, seen)) < r.cfg.Buffering.Window
}

func (r *Router) tableMigrating(tables []snapshot.TableKey) bool {
	snap := r.cfg.Snapshot()
	return snap != nil && snap.TableMigrating(tables)
}

func (r *Router) migrating() bool {
	snap := r.cfg.Snapshot()
	return snap != nil && snap.Migrating()
}

// FenceWaiting reports how many statements are held by the write fence.
func (r *Router) FenceWaiting() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fenceWaiting
}

func (r *Router) reserveFenceSlot() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fenceWaiting >= r.cfg.Buffering.PerShardCap {
		return false
	}
	r.fenceWaiting++
	return true
}

func (r *Router) releaseFenceSlot() {
	r.mu.Lock()
	r.fenceWaiting--
	r.mu.Unlock()
}

// awaitWriteFence holds a new write while the cluster, or one of the tables
// the statement touches, is fenced, for at most the buffering window; a
// fence still up afterwards refuses the statement with 57P03. Reads never
// wait.
func (r *Router) awaitWriteFence(ctx context.Context, tables []snapshot.TableKey) error {
	if !r.writeFenced(tables) {
		return nil
	}
	if !r.reserveFenceSlot() {
		return fenceBufferFullError()
	}
	defer r.releaseFenceSlot()
	var changes <-chan snapshot.Change
	if r.cfg.Buffering.Changes != nil {
		ch, unsubscribe := r.cfg.Buffering.Changes()
		defer unsubscribe()
		changes = ch
	}
	deadline := time.NewTimer(r.cfg.Buffering.Window)
	defer deadline.Stop()
	poll := time.NewTicker(r.cfg.Buffering.Poll)
	defer poll.Stop()
	for {
		if !r.writeFenced(tables) {
			return nil
		}
		select {
		case <-changes:
		case <-poll.C:
		case <-deadline.C:
			if r.writeFenced(tables) {
				return writeFenceError(r.migrating(), r.tableMigrating(tables))
			}
			return nil
		case <-ctx.Done():
			return pgwire.Errorf(pgwire.CodeQueryCanceled, "canceling statement due to user request")
		}
	}
}

// gateWrite applies the write fence to a statement that writes, unless the
// session's transaction was already open before the fence: open
// transactions may finish, and holding their later statements would only
// make them linger -- in front of a barrier's drain, which is waiting for
// those same transactions to end.
func (e *Executor) gateWrite(ctx context.Context, target Shard, tables []snapshot.TableKey) error {
	if e.txnWrote() || e.txnPreFence {
		if e.holdsShard(target) || !e.r.writeFenced(tables) {
			return nil
		}
		// The exemption covers the shards this transaction is already on,
		// whose transactions began before the pause and may therefore still
		// write. A shard it has not reached yet would open its transaction
		// under the pause and refuse the statement with PostgreSQL's own
		// 25006, which tells the client nothing about retrying -- so the
		// answer is pgshard's, and it comes now rather than after a round
		// trip that cannot succeed.
		return writeFenceError(e.r.migrating(), e.r.tableMigrating(tables))
	}
	if !e.r.writeFenced(tables) {
		return nil
	}
	if err := e.releaseUntouchedTxn(ctx); err != nil {
		return err
	}
	err := e.r.awaitWriteFence(ctx, tables)
	if rerr := e.replayPrelude(ctx); rerr != nil && err == nil {
		err = rerr
	}
	return err
}

// releaseUntouchedTxn closes the backend's transaction before a write waits
// for the cluster's write pause, and is why that wait terminates. A
// transaction that has run nothing but its prelude holds no rows and no
// locks, but it does hold a backend inside a transaction -- and a barrier's
// drain waits for exactly that before it takes its restore points. Waiting
// while holding one waits for a pause that is waiting for us, until the
// buffering window runs out and the client is refused.
//
// The prelude survives, so gateWrite reopens the transaction once the pause
// lifts and the client sees the pause as latency. Reopening also matters
// for what runs next: PostgreSQL reads default_transaction_read_only at
// BEGIN, so a transaction opened during the pause would be refused its
// write even after the pause ended.
func (e *Executor) releaseUntouchedTxn(ctx context.Context) error {
	if e.tx == pgwire.TxIdle || e.txnTouched || e.conn == nil || len(e.parked) > 0 {
		return nil
	}
	if err := e.send(simpleQuery("ROLLBACK")); err != nil {
		return err
	}
	if err := e.pump(ctx, discardWriter{}); err != nil {
		return err
	}
	// The transaction is gone, including what this statement had already
	// recorded about it before it failed.
	e.txnEnded, e.txnOnBackend = false, false
	e.wroteHere, e.gid = false, ""
	return nil
}

// txnWrote reports whether the current transaction wrote on any shard.
func (e *Executor) txnWrote() bool {
	if e.tx == pgwire.TxIdle {
		return false
	}
	if e.wroteHere {
		return true
	}
	for _, p := range e.parked {
		if p.wrote {
			return true
		}
	}
	return false
}

// writeTarget is the shard a single-shard write will run on. A statement
// that fans out has no one target, and the session's current shard is the
// closest thing to one.
func (e *Executor) writeTarget(pl plan.Plan) Shard {
	if len(pl.Shards) == 1 {
		return Shard{Set: e.userSet(), ID: pl.Shards[0]}
	}
	return e.shard
}

// holdsShard reports that the open transaction already has a backend on s,
// so PostgreSQL read default_transaction_read_only for it before any pause
// that is up now.
func (e *Executor) holdsShard(s Shard) bool {
	if e.tx == pgwire.TxIdle {
		return false
	}
	if s == e.shard && (e.txnTouched || e.wroteHere) {
		return true
	}
	return e.parked[s] != nil
}

// writePauseRetryable reports that a statement failed only because a shard
// was paused for a barrier the router had not seen yet, on a transaction
// that has done nothing else and has told the client nothing.
//
// The window cannot be closed by looking harder: the pause is applied on
// the shards and the routers learn of it afterwards, so a transaction can
// always open in between. PostgreSQL reads default_transaction_read_only at
// BEGIN, so such a transaction stays unable to write even after the pause
// lifts -- an error the client can do nothing with, on a statement pgshard
// undertook to buffer.
// wroteHere is deliberately not consulted: it is set before the statement
// is sent, so it is true for the very statement that failed. What must not
// be retried is a transaction an earlier statement already put on a shard,
// and txnTouched is what says that.
func (e *Executor) writePauseRetryable(err error, wrote bool) bool {
	if err == nil || wrote || e.txnTouched {
		return false
	}
	pe, ok := errors.AsType[*pgwire.Error](err)
	return ok && pe.Code == codeReadOnlyTransaction
}

// namePauseThatCannotBeRetriedHere turns PostgreSQL's 25006 into pgshard's
// own answer for a write pause, when the router cannot retry the statement
// itself.
//
// A transaction that has already written to another shard cannot be given
// back and reopened, so a barrier landing between its statements reaches
// the client -- as "cannot execute INSERT in a read-only transaction",
// which says nothing about a cluster write pause and nothing about
// retrying. A client cannot tell it from a transaction it really did open
// read-only. The router already answers 57P03 with a retry hint for the
// pause it saw in time; this is the same event, and gets the same answer.
//
// Only when the router agrees a pause is on: a genuine read-only
// transaction still gets PostgreSQL's own error, which is the truthful one
// there.
func (e *Executor) namePauseThatCannotBeRetriedHere(err error) error {
	if err == nil {
		return nil
	}
	pe, ok := errors.AsType[*pgwire.Error](err)
	if !ok || pe.Code != codeReadOnlyTransaction {
		return err
	}
	if !e.r.writeFenced(nil) && !e.r.sawWriteFenceRecently() {
		return err
	}
	named := pgwire.Errorf(codeWriteFence, "cluster write pause reached a transaction that had already written")
	named.Hint = "the pause is lifted when the barrier's restore points are recorded, or the new shard map is published; retry the transaction"
	return named
}

// reopenAfterWritePause gives the transaction back, waits the pause out and
// opens it again, so the statement can run a second time on a backend that
// is allowed to write.
func (e *Executor) reopenAfterWritePause(ctx context.Context) error {
	if err := e.releaseUntouchedTxn(ctx); err != nil {
		return err
	}
	if err := e.r.awaitWriteFence(ctx, nil); err != nil {
		return err
	}
	return e.replayPrelude(ctx)
}
