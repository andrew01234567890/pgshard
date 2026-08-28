package pooler

import (
	"context"
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
