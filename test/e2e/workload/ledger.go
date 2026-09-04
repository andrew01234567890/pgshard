// Package workload runs write workloads whose acknowledged commits can be
// checked after the fact.
//
// The distinction that matters here is between what was attempted and what
// was ACKNOWLEDGED. A chaos experiment kills things mid-write, so most
// statements in flight at that moment have no answer: they may have
// committed, they may not, and the client cannot tell. Only a statement
// that returned success is something the system promised to keep, and only
// those can be asserted about. A test that checked attempted writes would
// fail on a correct system; one that checked only liveness would pass on a
// system that came back having lost data.
package workload

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// AckedLedger appends rows to a ledger table through a caller-supplied
// executor, one goroutine per stream, and records for each stream the
// highest id whose INSERT was acknowledged.
type AckedLedger struct {
	// Streams are the shard-key values written under, one goroutine each.
	Streams []int64
	// Exec runs one statement. It must return a non-nil error unless the
	// statement is known to have committed.
	Exec func(ctx context.Context, sql string) error
	// Table is the ledger table; Batch is rows per statement.
	Table string
	Batch int
	// Retry is how long to wait after a failed statement. A workload run
	// through a failover spends most of its time here.
	Retry time.Duration
	// Pace is the wait between successful batches.
	Pace time.Duration

	acked []atomic.Int64
	// attempts counts statements that returned, successfully or not, so a
	// test can tell "the workload never ran" from "the workload ran and
	// everything failed" -- which look identical in the acked counts.
	attempts atomic.Int64
	failures atomic.Int64

	mu      sync.Mutex
	lastErr []string

	stop context.CancelFunc
	wg   sync.WaitGroup
}

func (l *AckedLedger) batch() int {
	if l.Batch <= 0 {
		return 10
	}
	return l.Batch
}

func (l *AckedLedger) note(i int, format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lastErr[i] = fmt.Sprintf(format, args...)
}

// InsertSQL is the statement for one batch, exported because the shape is
// part of what the workload guarantees: an explicit VALUES list, because
// the router routes an insert by its shard key and the key has to be
// readable from the statement rather than produced by a subquery.
//
// ON CONFLICT DO NOTHING makes a retry after an unanswered statement safe:
// the row may already be there from the attempt that never reported.
func InsertSQL(table string, stream, from, to int64) string {
	var rows strings.Builder
	for id := from; id <= to; id++ {
		if id > from {
			rows.WriteString(", ")
		}
		fmt.Fprintf(&rows, "(%d, %d, 1)", id, stream)
	}
	return "INSERT INTO " + table + " (id, tenant_id, amount) VALUES " + rows.String() + " ON CONFLICT DO NOTHING"
}

// Start begins writing and returns immediately.
func (l *AckedLedger) Start(ctx context.Context) {
	lctx, cancel := context.WithCancel(ctx)
	l.stop = cancel
	l.acked = make([]atomic.Int64, len(l.Streams))
	l.lastErr = make([]string, len(l.Streams))
	for i, stream := range l.Streams {
		l.wg.Add(1)
		go func() {
			defer l.wg.Done()
			next := int64(1)
			for lctx.Err() == nil {
				hi := next + int64(l.batch()) - 1
				err := l.Exec(lctx, InsertSQL(l.Table, stream, next, hi))
				l.attempts.Add(1)
				if err != nil {
					l.failures.Add(1)
					l.note(i, "insert [%d,%d]: %v", next, hi, err)
					sleep(lctx, l.Retry, 2*time.Second)
					continue
				}
				// Only now: the statement returned success, so every id
				// up to hi is a commit the system acknowledged and must
				// still have afterwards.
				l.acked[i].Store(hi)
				next = hi + 1
				sleep(lctx, l.Pace, 100*time.Millisecond)
			}
		}()
	}
}

func sleep(ctx context.Context, d, def time.Duration) {
	if d <= 0 {
		d = def
	}
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// Finish stops the workload and returns each stream's acknowledged
// high-water mark.
func (l *AckedLedger) Finish() []int64 {
	if l.stop != nil {
		l.stop()
	}
	l.wg.Wait()
	out := make([]int64, len(l.acked))
	for i := range l.acked {
		out[i] = l.acked[i].Load()
	}
	return out
}

// Acked is the current high-water per stream without stopping.
func (l *AckedLedger) Acked() []int64 {
	out := make([]int64, len(l.acked))
	for i := range l.acked {
		out[i] = l.acked[i].Load()
	}
	return out
}

// Attempts and Failures report what the workload actually did, so a test
// can distinguish a workload that never ran from one whose every write
// failed. Both leave the acked counts at zero.
func (l *AckedLedger) Attempts() int64 { return l.attempts.Load() }

// Failures counts attempts that returned an error.
func (l *AckedLedger) Failures() int64 { return l.failures.Load() }

// Why reports the last failure for each stream that has acknowledged
// nothing, for a test about to fail on a timeout.
func (l *AckedLedger) Why() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var b strings.Builder
	for i, stream := range l.Streams {
		if l.acked[i].Load() > 0 {
			continue
		}
		reason := l.lastErr[i]
		if reason == "" {
			reason = "no attempt completed"
		}
		fmt.Fprintf(&b, "\n  stream %d: %s", stream, reason)
	}
	if b.Len() == 0 {
		return ""
	}
	return "\nlast write failure per stream:" + b.String()
}
