package vstream

import (
	"context"
	"strings"
	"testing"
	"time"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// TestATransactionThatDoesNotFitEndsTheStream: BufferUnits bounds
// transactions that are already assembled. Nothing bounded the one being
// assembled, so a single large transaction grew in the router until it ran
// out of memory -- and the router is not only serving this stream, so the
// SQL sessions on it go too.
//
// It ends the stream instead, with a code that says the position is still
// good. A consumer resumes from the last one it saw.
func TestATransactionThatDoesNotFitEndsTheStream(t *testing.T) {
	h := newHarness(t, 1)
	h.server.MaxTransactionBytes = 4 << 10
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain"})
	h.pool[0].feed("plain", batch(0, evRelation(1, "t", "id")))

	// One transaction that never commits, fed a row at a time past the
	// limit. Uncommitted, so no unit is ever handed to the merge and the
	// unit channel's bound never comes into it.
	row := strings.Repeat("x", 512)
	evs := []*pgshardv1.ChangeEvent{evBegin(1, 1_000_000)}
	for range 20 {
		evs = append(evs, evInsert(1, 0, row))
	}
	h.pool[0].feed("plain", batch(100, evs...))

	got := recvUntilError(t, st, 5*time.Second)
	if got == nil {
		t.Fatal("the stream did not end: an oversized transaction was buffered without limit")
	}
	if got.GetCode() != pgshardv1.VEvent_Error_CODE_TRANSACTION_TOO_LARGE {
		t.Fatalf("code %v, want TRANSACTION_TOO_LARGE", got.GetCode())
	}
	if !strings.Contains(got.GetMessage(), "exceed the") {
		t.Fatalf("the message must say what was exceeded: %q", got.GetMessage())
	}
}

// TestTooManyOpenTransactionsEndsTheStream: bytes alone would admit a very
// large number of small interleaved transactions, each with its own map
// entry and slices.
func TestTooManyOpenTransactionsEndsTheStream(t *testing.T) {
	h := newHarness(t, 1)
	h.server.MaxOpenTransactions = 4
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain"})
	h.pool[0].feed("plain", batch(0, evRelation(1, "t", "id")))

	var evs []*pgshardv1.ChangeEvent
	for xid := uint32(1); xid <= 10; xid++ {
		evs = append(evs,
			&pgshardv1.ChangeEvent{Xid: xid, Event: &pgshardv1.ChangeEvent_StreamStart_{StreamStart: &pgshardv1.ChangeEvent_StreamStart{Xid: xid, FirstSegment: true}}},
			evInsert(1, xid, "v"),
			&pgshardv1.ChangeEvent{Event: &pgshardv1.ChangeEvent_StreamStop_{StreamStop: &pgshardv1.ChangeEvent_StreamStop{}}})
	}
	h.pool[0].feed("plain", batch(100, evs...))

	got := recvUntilError(t, st, 5*time.Second)
	if got == nil {
		t.Fatal("the stream did not end: interleaved transactions were opened without limit")
	}
	if got.GetCode() != pgshardv1.VEvent_Error_CODE_TRANSACTION_TOO_LARGE {
		t.Fatalf("code %v, want TRANSACTION_TOO_LARGE", got.GetCode())
	}
}

// TestOrdinaryTransactionsDoNotTripTheLimit: the bound is released when a
// transaction commits, so a stream of ordinary ones stays well under it
// however many there are.
func TestOrdinaryTransactionsDoNotTripTheLimit(t *testing.T) {
	h := newHarness(t, 1)
	h.server.MaxTransactionBytes = 4 << 10
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain"})
	h.pool[0].feed("plain", batch(0, evRelation(1, "t", "id")))
	row := strings.Repeat("y", 512)
	for i := range 20 {
		h.pool[0].feed("plain", txn(1, uint32(i+1), int64(i+1)*1_000_000, uint64(100+i*8), row))
	}
	// Four events per transaction: Begin, Row, Commit, VGtid.
	got := recvN(t, st, 8, 5*time.Second)
	for _, ev := range got {
		if e := ev.GetError(); e != nil {
			t.Fatalf("committed transactions must release what they held: %v", e)
		}
	}
}

func recvUntilError(t *testing.T, st pgshardv1.VStream_StreamClient, d time.Duration) *pgshardv1.VEvent_Error {
	t.Helper()
	done := make(chan *pgshardv1.VEvent_Error, 1)
	go func() {
		for {
			ev, err := st.Recv()
			if err != nil {
				done <- nil
				return
			}
			if e := ev.GetError(); e != nil {
				done <- e
				return
			}
		}
	}()
	select {
	case e := <-done:
		return e
	case <-time.After(d):
		return nil
	}
}
