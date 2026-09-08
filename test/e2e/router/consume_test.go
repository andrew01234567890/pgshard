//go:build integration

package router

import (
	"context"
	"errors"
	"io"
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
	i      int
	stall  bool
}

func (f *fakeStream) Recv() (*pgshardv1.VEvent, error) {
	if f.i < len(f.events) {
		f.i++
		return f.events[f.i-1], nil
	}
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
