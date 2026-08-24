package operator

import (
	"context"
	"testing"
	"time"
)

type blockingProber struct{ Prober }

func (blockingProber) PublishShardStatus(ctx context.Context, _ string, _ Group, _ int64, _ string) error {
	<-ctx.Done()
	return ctx.Err()
}

func (blockingProber) ProbeStandby(ctx context.Context, _ string) (StandbyState, error) {
	<-ctx.Done()
	return StandbyState{}, ctx.Err()
}

func TestBoundedProberNeverBlocksIndefinitely(t *testing.T) {
	b := boundedProber{Inner: blockingProber{}, Timeout: 50 * time.Millisecond}
	start := time.Now()
	if err := b.PublishShardStatus(context.Background(), "dsn", Group{}, 1, "ep"); err == nil {
		t.Fatal("want timeout error")
	}
	if _, err := b.ProbeStandby(context.Background(), "dsn"); err == nil {
		t.Fatal("want timeout error")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("calls blocked for %s", elapsed)
	}
}
