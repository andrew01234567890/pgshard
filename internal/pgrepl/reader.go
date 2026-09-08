package pgrepl

import (
	"context"
	"net"
	"sync"
	"time"
)

// Reader reads replication frames, bounding each wait with the connection's
// own read deadline instead of a context per frame.
//
// The shape it replaces is context.WithTimeout around every Receive. That
// is correct, but a change stream calls Receive once per WAL message, and
// each pass allocated a timer context, registered it on the stream's
// context, and had pgconn register a cancellation hook on top -- bookkeeping
// per row on a path whose whole job is to move rows. The deadline is the
// same mechanism underneath: pgconn's own cancellation works by setting one.
//
// A frame that does not arrive in time surfaces as a timeout error, which
// leaves the connection usable, so a caller can interleave standby status
// updates with idle waits exactly as before.
type Reader struct {
	c    *Conn
	ctx  context.Context
	nc   net.Conn
	done chan struct{}
	stop func() bool
	once sync.Once
}

// Reader returns a frame reader bound to ctx. Cancelling ctx interrupts a
// wait in progress. Close releases the hook and clears the deadline; the
// Conn stays usable afterwards. One reader at a time per Conn: two would
// each be setting the deadline the other is waiting on.
func (c *Conn) Reader(ctx context.Context) *Reader {
	r := &Reader{c: c, ctx: ctx, nc: c.pc.Conn(), done: make(chan struct{})}
	r.stop = context.AfterFunc(ctx, func() {
		// The read half only. The write half is what SendStandbyStatus
		// uses, and expiring it turns a cancel that races an ack into a
		// write failure the caller reports as an error -- when all that
		// happened is that its own context ended.
		_ = r.nc.SetReadDeadline(time.Now())
		close(r.done)
	})
	return r
}

// Next reads the next frame, waiting at most within for one to arrive.
func (r *Reader) Next(within time.Duration) (any, error) {
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	if err := r.nc.SetReadDeadline(time.Now().Add(within)); err != nil {
		return nil, err
	}
	// Checked again after setting the deadline, because this deadline can
	// overwrite the one the cancellation hook set. A context is marked
	// done before its hooks run, so a cancel that could have been
	// overwritten is one this check already sees; a cancel that arrives
	// later runs the hook, which shortens the deadline the read is by
	// then blocked on.
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	// context.Background() by identity: pgconn skips its context watcher
	// for exactly that value, which is the allocation this type exists to
	// avoid. The deadline above is what bounds the read.
	msg, err := r.c.Receive(context.Background())
	if err != nil && IsTimeout(err) {
		// A cancel reaches the read as a deadline, so report it as one:
		// the caller asked to be told about its own context, not about
		// the mechanism used to interrupt the socket.
		if cerr := r.ctx.Err(); cerr != nil {
			return nil, cerr
		}
	}
	return msg, err
}

// Close releases the cancellation hook and clears the read deadline. It may
// be called more than once.
func (r *Reader) Close() {
	// Once, because the second call cannot tell the two reasons stop
	// reports false apart: a hook that is running now, which must be
	// waited for, and a hook the first Close already cancelled, which
	// will never close done and so would be waited for forever.
	r.once.Do(func() {
		// The hook runs in its own goroutine, so a cancel already in
		// flight can set its deadline after this one clears it and leave
		// the connection unreadable for every later caller. Wait for it.
		// The context check keeps a Close on an uncancelled reader --
		// where the hook never runs -- from waiting on a channel nothing
		// will close.
		if !r.stop() && r.ctx.Err() != nil {
			<-r.done
		}
		_ = r.nc.SetReadDeadline(time.Time{})
	})
}
