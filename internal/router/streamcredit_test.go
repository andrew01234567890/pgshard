package router

import (
	"context"
	"testing"
	"time"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

func copyResponse(n int) *pgshardv1.ExecuteResponse {
	return &pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_CopyData{
		CopyData: &pgshardv1.CopyData{Data: make([]byte, n)}}}
}

func TestCreditsForChargeThePayloadAndNoMore(t *testing.T) {
	if got := creditsFor(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_CopyDone{}}); got != 1 {
		t.Fatalf("an empty message cost %d credits, want 1", got)
	}
	if got := creditsFor(copyResponse(streamCreditChunk * 3)); got != 3 {
		t.Fatalf("three chunks cost %d credits, want 3", got)
	}
	if got := creditsFor(copyResponse(streamCreditChunk * streamCredits * 10)); got != streamCredits {
		t.Fatalf("an oversized message cost %d credits, want the whole bound %d", got, streamCredits)
	}
	row := &pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_DataRow{
		DataRow: &pgshardv1.DataRow{Packed: [][]byte{make([]byte, streamCreditChunk*2)}}}}
	if got := creditsFor(row); got != 2 {
		t.Fatalf("a wide packed row cost %d credits, want 2", got)
	}
}

// countingStream answers every Recv with one full-bound message and counts
// how many times it was asked, which is what the read-ahead bound governs.
type countingStream struct {
	pgshardv1.Pooler_ExecuteClient
	ctx   context.Context
	calls chan struct{}
}

func (s *countingStream) Recv() (*pgshardv1.ExecuteResponse, error) {
	s.calls <- struct{}{}
	return copyResponse(streamCreditChunk * streamCredits), nil
}
func (s *countingStream) Context() context.Context { return s.ctx }

func TestReaderStopsReadingAheadWhenCreditIsSpent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Room for far more calls than the bound allows, so a test that fails
	// blocks on the assertion rather than on the channel.
	cs := &countingStream{ctx: ctx, calls: make(chan struct{}, 256)}
	ps := &poolerStream{stream: cs, cancel: cancel, recvc: make(chan recvResult, 64),
		credit: make(chan struct{}, streamCredits), done: make(chan struct{}), gone: make(chan struct{})}
	go ps.reader()
	defer func() { cancel(); <-ps.done }()

	// Nothing consumes the stream, so the reader may hold one message in
	// hand and one queued before the bound stops it. Without the bound it
	// would run until the 64-message queue filled.
	time.Sleep(200 * time.Millisecond)
	if got := len(cs.calls); got > 4 {
		t.Fatalf("the reader made %d calls with nothing consuming them; the byte bound should have stopped it after a couple", got)
	}
}
