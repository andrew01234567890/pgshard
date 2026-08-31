package pooler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// BenchmarkAcquireReleaseUncontended is the common case: one caller taking
// a backend and giving it back with nobody waiting. Every release used to
// close the shared wake channel and allocate a replacement, whether or not
// anyone was parked on it.
func BenchmarkAcquireReleaseUncontended(b *testing.B) {
	pg := newFakePG()
	p := newPool(PoolConfig{MaxBackends: 4, MaxPerRole: 4, AcquireTimeout: time.Second}, pg.dial)
	ctx := context.Background()
	got, err := p.Acquire(ctx, "db", "alice", nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	p.Release(got)
	b.ReportAllocs()
	for b.Loop() {
		c, err := p.Acquire(ctx, "db", "alice", nil, nil)
		if err != nil {
			b.Fatal(err)
		}
		p.Release(c)
	}
}

// BenchmarkAcquireReleaseContended measures what an uncontended benchmark
// cannot: the cost of one release when acquirers are parked on the budget.
//
// notify closes the shared wake channel, so every parked acquirer wakes,
// all but one lose the race and park again, and the release pays O(waiters)
// of scheduler and lock work plus a channel allocation. That is the half of
// PGS-187 still open, and a fix for it -- a direct handoff, or waking one
// eligible waiter -- has to be measured here rather than on the
// uncontended path, which it does not touch.
//
// The pool is deliberately full before the parked acquirers arrive, so each
// iteration is exactly: release one backend, wake everyone, one winner
// takes it, the rest park again.
func BenchmarkAcquireReleaseContended(b *testing.B) {
	for _, waiters := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("waiters=%d", waiters), func(b *testing.B) {
			pg := newFakePG()
			p := newPool(PoolConfig{MaxBackends: 1, MaxPerRole: 1, AcquireTimeout: 30 * time.Second}, pg.dial)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			held, err := p.Acquire(ctx, "db", "alice", nil, nil)
			if err != nil {
				b.Fatal(err)
			}

			// Park the waiters. Each one that wins hands the backend
			// straight back, so the steady state is one held backend and
			// `waiters` acquirers on the budget.
			var wg sync.WaitGroup
			for i := 0; i < waiters; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for ctx.Err() == nil {
						c, err := p.Acquire(ctx, "db", "alice", nil, nil)
						if err != nil {
							return
						}
						p.Release(c)
					}
				}()
			}
			// Let them reach the park before measuring; a waiter that has
			// not parked yet is not contention.
			for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
				p.mu.Lock()
				parked := len(p.budget)
				for _, rp := range p.roles {
					parked += len(rp.waiters)
				}
				p.mu.Unlock()
				if parked >= waiters {
					break
				}
				time.Sleep(time.Millisecond)
			}

			b.ReportAllocs()
			for b.Loop() {
				p.Release(held)
				var err error
				if held, err = p.Acquire(ctx, "db", "alice", nil, nil); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			cancel()
			wg.Wait()
			p.Release(held)
		})
	}
}
