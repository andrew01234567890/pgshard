package router

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// SQLSTATEs used by failover buffering.
const (
	// codeStaleGeneration is what a pooler answers to a stale generation or
	// epoch stamp (object_not_in_prerequisite_state).
	codeStaleGeneration = "55000"
	// codeRewriteInProgress marks a condition the client should wait out
	// rather than a fault: the same SQLSTATE a fenced shard answers with.
	codeRewriteInProgress = "55000"
	// codeRetryTransaction is serialization_failure. It is the one code the
	// router uses to say a transaction did not commit anywhere and running
	// it again is safe, whatever ended it -- a failover mid-transaction, or
	// the resolver aborting a transaction whose coordinator it presumed
	// dead. Drivers' retry loops test for this exact value and nothing
	// broader, so a second code for a second cause is a retry that never
	// happens. The Detail says which cause it was.
	codeRetryTransaction = "40001"
	// codeBufferFull is too_many_connections: the per-shard buffer is full.
	codeBufferFull = "53300"
)

// Buffering tunes how statements are held while a shard fails over.
type Buffering struct {
	// Window bounds how long a statement waits for the shard to become
	// consistent again. Default 10s.
	Window time.Duration
	// TransportWindow bounds how long a statement waits after a pooler
	// refused the connection while the snapshot still shows the shard
	// serving: a transport fault rather than a failover. Default 1s.
	TransportWindow time.Duration
	// PerShardCap bounds statements waiting on one shard; the next one is
	// refused with 53300. Default 256.
	PerShardCap int
	// Changes subscribes to catalog snapshot changes so waiting statements
	// wake up as soon as the shard is consistent again. Nil polls.
	Changes func() (<-chan snapshot.Change, func())
	// Poll is the fallback wake-up interval. Default 200ms.
	Poll time.Duration
}

func (b Buffering) withDefaults() Buffering {
	if b.Window <= 0 {
		b.Window = 10 * time.Second
	}
	if b.TransportWindow <= 0 {
		b.TransportWindow = time.Second
	}
	if b.PerShardCap <= 0 {
		b.PerShardCap = 256
	}
	if b.Poll <= 0 {
		b.Poll = 200 * time.Millisecond
	}
	return b
}

// failoverAction is what to do with a statement that hit a failover.
type failoverAction int

const (
	// failoverPass reports the original error unchanged.
	failoverPass failoverAction = iota
	// failoverWait holds the statement until the shard is consistent, then
	// retries it once.
	failoverWait
	// failoverFailTxn refuses with 40001 because a transaction is open.
	failoverFailTxn
	// failoverRefuse refuses with 53300 because the shard buffer is full.
	failoverRefuse
)

// decideFailover is the buffering decision table. A statement is only
// transparently retried when the client has seen nothing of it yet and no
// transaction is open; inside a transaction the earlier statements cannot
// be replayed, so the client gets a retryable transaction error.
func decideFailover(trigger, inTxn, outputSent bool, buffered, capacity int) failoverAction {
	switch {
	case !trigger, outputSent:
		return failoverPass
	case inTxn:
		return failoverFailTxn
	case buffered >= capacity:
		return failoverRefuse
	}
	return failoverWait
}

// isFailover reports whether err is a stale-generation refusal from a
// pooler or a refused connection to it.
func isFailover(err error) bool { return isStaleGeneration(err) || isRefused(err) }

// isStaleGeneration reports a pooler refusing because the topology moved.
//
// A pooler that says so is believed; one that does not is judged by its
// SQLSTATE, as before. That fallback is not merely for older poolers: 55000
// is object_not_in_prerequisite_state, which a shard answers for a rewrite
// in progress and for conditions that are not a fence at all, so the code
// alone cannot tell them apart and never could.
func isStaleGeneration(err error) bool {
	switch poolerReason(err) {
	case pgshardv1.Reason_REASON_STALE_GENERATION:
		return true
	case pgshardv1.Reason_REASON_REWRITE_IN_PROGRESS:
		return false
	}
	pe, ok := errors.AsType[*pgwire.Error](err)
	return ok && pe.Code == codeStaleGeneration
}

func isRefused(err error) bool {
	_, refused := errors.AsType[*refusedError](err)
	return refused
}

// retryWindow picks how long a failed statement on sh waits before its
// retry. A refused pooler connection while the snapshot still shows the
// shard serving is a transport fault, not a failover, and gets the short
// transport window; a fence signal (stale generation, or a shard that is
// blocking in the snapshot) gets the full failover window.
func (r *Router) retryWindow(sh Shard, err error) time.Duration {
	if isRefused(err) && !isStaleGeneration(err) && !r.blocking(sh) {
		return r.cfg.Buffering.TransportWindow
	}
	return r.cfg.Buffering.Window
}

// blocking reports whether sh has no usable primary in the current
// snapshot: fenced, migrating, missing from shard_status or without an
// endpoint. Statically configured poolers are never blocking.
func (r *Router) blocking(sh Shard) bool {
	if _, static := r.cfg.Poolers.static[sh]; static {
		return false
	}
	snap := r.cfg.Snapshot()
	if snap == nil {
		return true
	}
	sv, ok := snap.Serving[snapshot.ShardKey{ShardSet: sh.Set, ShardID: sh.ID}]
	return !ok || sv.State == "fenced" || sv.State == "migrating" || sv.PrimaryEndpoint == ""
}

// reserveBuffer counts one waiting statement against sh's cap.
func (r *Router) reserveBuffer(sh Shard) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.buffered[sh] >= r.cfg.Buffering.PerShardCap {
		return false
	}
	r.buffered[sh]++
	return true
}

func (r *Router) releaseBuffer(sh Shard) {
	r.mu.Lock()
	r.buffered[sh]--
	r.mu.Unlock()
}

// Buffered reports how many statements are waiting on sh.
func (r *Router) Buffered(sh Shard) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buffered[sh]
}

// awaitConsistent holds the caller until sh stops blocking or the window
// elapses and reports whether the statement may be (re)tried. afterError
// additionally waits for a new snapshot, since the one that produced the
// error is by definition stale; if none arrives within the window the
// statement is still retried once, against whatever endpoint is current.
// window bounds the wait.
func (r *Router) awaitConsistent(ctx context.Context, sh Shard, afterError bool, window time.Duration) (bool, error) {
	if !r.reserveBuffer(sh) {
		return false, bufferFullError(sh)
	}
	r.metrics.BufferEvents.Inc()
	bufferStart := time.Now()
	defer func() {
		r.metrics.BufferSeconds.Observe(time.Since(bufferStart).Seconds())
		r.releaseBuffer(sh)
	}()
	var changes <-chan snapshot.Change
	if r.cfg.Buffering.Changes != nil {
		ch, unsubscribe := r.cfg.Buffering.Changes()
		defer unsubscribe()
		changes = ch
	}
	deadline := time.NewTimer(window)
	defer deadline.Stop()
	poll := time.NewTicker(r.cfg.Buffering.Poll)
	defer poll.Stop()
	stale := r.cfg.Snapshot()
	changed := !afterError
	for {
		if changed && !r.blocking(sh) {
			return true, nil
		}
		select {
		case <-changes:
			changed = true
		case <-poll.C:
			// shard_status edits that keep the generation pair are not
			// published as changes; a new snapshot pointer still counts.
			changed = changed || r.cfg.Snapshot() != stale
		case <-deadline.C:
			return !r.blocking(sh), nil
		case <-ctx.Done():
			return false, pgwire.Errorf(pgwire.CodeQueryCanceled, "canceling statement due to user request")
		}
	}
}

func bufferFullError(sh Shard) error {
	return pgwire.Errorf(codeBufferFull, "too many statements waiting for shard %s/%d to fail over", sh.Set, sh.ID)
}

func failoverInTxnError() error {
	return &pgwire.Error{Severity: "ERROR", Code: codeRetryTransaction,
		Message: "shard failover; retry the transaction",
		Detail:  "The shard serving this transaction changed primaries; statements already run in the transaction cannot be replayed.",
		Hint:    "Retry the whole transaction. Nothing it wrote was committed."}
}

// countingWriter records whether any protocol message reached the client;
// once one has, the statement can no longer be transparently retried.
type countingWriter struct {
	w     pgwire.ResultWriter
	wrote bool
}

func (c *countingWriter) RowDescription(f []pgproto3.FieldDescription) error {
	c.wrote = true
	return c.w.RowDescription(f)
}
func (c *countingWriter) DataRow(v [][]byte) error { c.wrote = true; return c.w.DataRow(v) }
func (c *countingWriter) CommandComplete(t string) error {
	c.wrote = true
	return c.w.CommandComplete(t)
}
func (c *countingWriter) EmptyQueryResponse() error { c.wrote = true; return c.w.EmptyQueryResponse() }
func (c *countingWriter) ParameterDescription(o []uint32) error {
	c.wrote = true
	return c.w.ParameterDescription(o)
}
func (c *countingWriter) NoData() error          { c.wrote = true; return c.w.NoData() }
func (c *countingWriter) PortalSuspended() error { c.wrote = true; return c.w.PortalSuspended() }
func (c *countingWriter) Notice(n *pgproto3.NoticeResponse) error {
	c.wrote = true
	return c.w.Notice(n)
}
func (c *countingWriter) Notification(n *pgproto3.NotificationResponse) error {
	c.wrote = true
	return c.w.Notification(n)
}

// ParameterStatus does not count as output. It carries no part of a result,
// and the executor suppresses one whose value the client already has, so a
// statement retried after it is still a statement that told the client
// nothing.
func (c *countingWriter) ParameterStatus(name, value string) error {
	return c.w.ParameterStatus(name, value)
}

func (c *countingWriter) CopyIn(f byte, cf []uint16) (pgwire.CopyInStream, error) {
	c.wrote = true
	return c.w.CopyIn(f, cf)
}
func (c *countingWriter) CopyOut(f byte, cf []uint16) error {
	c.wrote = true
	return c.w.CopyOut(f, cf)
}
func (c *countingWriter) CopyData(b []byte) error { c.wrote = true; return c.w.CopyData(b) }
func (c *countingWriter) CopyDone() error         { c.wrote = true; return c.w.CopyDone() }
