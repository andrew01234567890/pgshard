package vstream

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// countingMeter is a Meter that keeps running totals, so a test can watch
// what the gauges would show.
type countingMeter struct {
	mu       sync.Mutex
	bytes    int
	open     int
	maxBytes int
	exceeded map[string]int
}

func (m *countingMeter) BufferedBytes(delta int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bytes += delta
	if m.bytes > m.maxBytes {
		m.maxBytes = m.bytes
	}
}

func (m *countingMeter) OpenTransactions(delta int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.open += delta
}

func (m *countingMeter) TooLarge(bound string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.exceeded == nil {
		m.exceeded = map[string]int{}
	}
	m.exceeded[bound]++
}

func (m *countingMeter) read() (bytes, open int, exceeded map[string]int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]int{}
	for k, v := range m.exceeded {
		out[k] = v
	}
	return m.bytes, m.open, out
}

// TestTheBufferGaugesRiseAndReturnToZero is what makes the bound
// watchable: a stream that ends with TRANSACTION_TOO_LARGE has already
// cost the consumer its position, and a gauge is how anyone sees it
// coming. A gauge that only rises is worse than none, because it reads as
// a leak that is not there, so this checks both directions.
func TestTheBufferGaugesRiseAndReturnToZero(t *testing.T) {
	m := &countingMeter{}
	a := &assembler{relations: map[uint32]*relMeta{}, streamed: map[uint32]*unit{}, meter: m}

	if _, err := a.add(evRelation(1, "t", "id")); err != nil {
		t.Fatal(err)
	}
	if _, err := a.add(evBegin(1, 1_000_000)); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if _, err := a.add(evInsert(1, 0, strings.Repeat("x", 256))); err != nil {
			t.Fatal(err)
		}
	}
	if bytes, _, _ := m.read(); bytes <= 0 {
		t.Fatalf("an open transaction holding rows reported %d buffered bytes", bytes)
	}
	if _, err := a.add(evCommit(1_000_000, 1_000_001)); err != nil {
		t.Fatal(err)
	}
	bytes, open, _ := m.read()
	if bytes != 0 {
		t.Fatalf("the transaction committed and %d bytes are still counted", bytes)
	}
	if open != 0 {
		t.Fatalf("%d transactions still counted open", open)
	}
	if m.maxBytes <= 0 {
		t.Fatal("the gauge never rose, so it was measuring nothing")
	}
}

// TestTheTrippedBoundIsNamed: two bounds end a stream the same way, and
// which one tripped is the difference between "one transaction is too big"
// and "too many at once", which are different problems.
func TestTheTrippedBoundIsNamed(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*Server)
		feed  func(*harness)
		bound string
	}{
		{
			name:  "bytes",
			setup: func(s *Server) { s.MaxTransactionBytes = 4 << 10 },
			feed: func(h *harness) {
				evs := []*pgshardv1.ChangeEvent{evBegin(1, 1_000_000)}
				for range 20 {
					evs = append(evs, evInsert(1, 0, strings.Repeat("x", 512)))
				}
				h.pool[0].feed("plain", batch(100, evs...))
			},
			bound: "bytes",
		},
		{
			name:  "transactions",
			setup: func(s *Server) { s.MaxOpenTransactions = 2 },
			feed: func(h *harness) {
				var evs []*pgshardv1.ChangeEvent
				for xid := uint32(1); xid <= 6; xid++ {
					evs = append(evs,
						&pgshardv1.ChangeEvent{Xid: xid, Event: &pgshardv1.ChangeEvent_StreamStart_{
							StreamStart: &pgshardv1.ChangeEvent_StreamStart{Xid: xid, FirstSegment: true}}},
						evInsert(1, xid, "x"))
				}
				h.pool[0].feed("plain", batch(100, evs...))
			},
			bound: "transactions",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, 1)
			m := &countingMeter{}
			h.server.Meter = m
			tc.setup(h.server)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			st := h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain"})
			h.pool[0].feed("plain", batch(0, evRelation(1, "t", "id")))
			tc.feed(h)

			if got := recvUntilError(t, st, 5*time.Second); got == nil ||
				got.GetCode() != pgshardv1.VEvent_Error_CODE_TRANSACTION_TOO_LARGE {
				t.Fatalf("the stream did not end with TRANSACTION_TOO_LARGE: %v", got)
			}
			if _, _, exceeded := m.read(); exceeded[tc.bound] == 0 {
				t.Fatalf("the %s bound tripped and was not counted: %v", tc.bound, exceeded)
			}
		})
	}
}
