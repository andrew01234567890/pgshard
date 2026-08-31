package router

import (
	"context"
	"time"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

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
	return snap != nil && (snap.WriteFence || snap.Migrating() || snap.TableMigrating(tables))
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
func (e *Executor) gateWrite(ctx context.Context, tables []snapshot.TableKey) error {
	if e.txnWrote() || e.txnPreFence {
		return nil
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
	e.txnEnded, e.txnOnBackend = false, false
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
