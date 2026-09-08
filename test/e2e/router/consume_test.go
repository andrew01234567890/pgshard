//go:build integration

package router

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// fakeStream hands out canned events and then does whatever a stalled or
// finished stream does.
type fakeStream struct {
	grpc.ClientStream
	events []*pgshardv1.VEvent
	mu     sync.Mutex
	i      int
	stall  bool
}

// asked is how many events have been requested, which is what makes the
// dropped-event case observable: the reader takes one more than the
// consumer used.
func (f *fakeStream) asked() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.i
}

func (f *fakeStream) Recv() (*pgshardv1.VEvent, error) {
	f.mu.Lock()
	if f.i < len(f.events) {
		f.i++
		ev := f.events[f.i-1]
		f.mu.Unlock()
		return ev, nil
	}
	f.mu.Unlock()
	if f.stall {
		select {} // never answers, which is the case under test
	}
	return nil, io.EOF
}
func (f *fakeStream) Send(*pgshardv1.VStreamRequest) error { return nil }
func (f *fakeStream) CloseSend() error                     { return nil }
func (f *fakeStream) Context() context.Context             { return context.Background() }
func (f *fakeStream) Header() (metadata.MD, error)         { return nil, nil }
func (f *fakeStream) Trailer() metadata.MD                 { return nil }
func (f *fakeStream) SendMsg(any) error                    { return nil }
func (f *fakeStream) RecvMsg(any) error                    { return nil }

func oneEvent() []*pgshardv1.VEvent {
	return []*pgshardv1.VEvent{{Event: &pgshardv1.VEvent_Vgtid{Vgtid: &pgshardv1.VEvent_VGtid{}}}}
}

// A stream that answers is read as before.
func TestStreamReaderPassesEventsThrough(t *testing.T) {
	r := newStreamReader(&fakeStream{events: oneEvent()})
	defer r.close()
	ev, err := r.next(10 * time.Second)
	if err != nil {
		t.Fatalf("an event that was delivered came back as %v", err)
	}
	if ev.GetVgtid() == nil {
		t.Fatalf("the wrong event came through: %v", ev)
	}
}

// A stream that stops answering is given up on, rather than being waited
// for until the 45-minute package timeout takes every other test with it.
func TestStreamReaderGivesUpOnAStalledStream(t *testing.T) {
	r := newStreamReader(&fakeStream{events: oneEvent(), stall: true})
	defer r.close()
	if _, err := r.next(10 * time.Second); err != nil {
		t.Fatalf("the first event should still arrive: %v", err)
	}
	start := time.Now()
	_, err := r.next(200 * time.Millisecond)
	if !errors.Is(err, errStreamStalled) {
		t.Fatalf("a stalled stream returned %v, want errStreamStalled", err)
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Fatalf("it waited %s before giving up", took)
	}
}

// The end of a stream is still the end, not a stall.
func TestStreamReaderReportsTheEndOfTheStream(t *testing.T) {
	r := newStreamReader(&fakeStream{events: oneEvent()})
	defer r.close()
	if _, err := r.next(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := r.next(10 * time.Second); !errors.Is(err, io.EOF) {
		t.Fatalf("the end of the stream came back as %v, want io.EOF", err)
	}
}

// Two consumes on ONE reader must not lose the event between them.
//
// consume returns while its reader is parked in a Recv whose event nothing
// is waiting for yet. A reader created per consume would drop that event,
// and the next consume would start one event late -- which arrives as a
// Row with no Begin, so the failover test fails rather than flakes. That
// test consumes twice from one stream, so this is its exact shape.
func TestTwoConsumesOnOneReaderLoseNothingBetweenThem(t *testing.T) {
	shard := &pgshardv1.ShardRef{ShardId: 1}
	row := func(id string) *pgshardv1.VEvent {
		return &pgshardv1.VEvent{Event: &pgshardv1.VEvent_Row_{Row: &pgshardv1.VEvent_Row{
			Shard: shard, Kind: pgshardv1.VEvent_Row_KIND_INSERT,
			New: &pgshardv1.VTuple{Columns: []*pgshardv1.VColumn{{}, {Value: []byte(id)}}}}}}
	}
	begin := &pgshardv1.VEvent{Event: &pgshardv1.VEvent_Begin_{Begin: &pgshardv1.VEvent_Begin{Shard: shard}}}
	commit := &pgshardv1.VEvent{Event: &pgshardv1.VEvent_Commit_{Commit: &pgshardv1.VEvent_Commit{}}}
	vgtid := &pgshardv1.VEvent{Event: &pgshardv1.VEvent_Vgtid{Vgtid: &pgshardv1.VEvent_VGtid{Position: &pgshardv1.VPosition{}}}}

	// Two whole transactions, each ended by the VGtid that stops a consume.
	fake := &fakeStream{events: []*pgshardv1.VEvent{
		begin, row("1"), commit, vgtid,
		begin, row("2"), commit, vgtid,
	}}
	r := newStreamReader(fake)
	defer r.close()

	first := consume(t, r, func(c *consumed) bool { return len(flatten(c)) >= 1 })
	if ids := flatten(first); len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("first consume got %v, want [1]", ids)
	}
	// Wait until the reader has taken the event AFTER the one that stopped
	// the first consume -- the fifth, the second transaction's Begin. That
	// is the event a per-consume reader drops, and waiting for it is what
	// makes this deterministic rather than a race the fix usually wins.
	deadline := time.Now().Add(10 * time.Second)
	for fake.asked() < 5 {
		if time.Now().After(deadline) {
			t.Fatalf("the reader never asked for the event after the one that stopped the consume (asked %d)", fake.asked())
		}
		time.Sleep(time.Millisecond)
	}
	second := consume(t, r, func(c *consumed) bool { return len(flatten(c)) >= 1 })
	if ids := flatten(second); len(ids) != 1 || ids[0] != 2 {
		t.Fatalf("second consume got %v, want [2]: the Begin between the two was dropped", ids)
	}
}
