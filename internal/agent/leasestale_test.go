package agent

import (
	"context"
	"testing"
	"time"
)

// TestLiveFailsOnAStaleLease guards the case the self-fence cannot cover. A
// primary fences itself when it loses its lease, but that runs inside the
// agent's own renew goroutine: an agent that is frozen, wedged or thrashing
// never reaches it and keeps PostgreSQL writable while the operator promotes
// someone else. Failing liveness puts the deadline in the kubelet's hands.
func TestLiveFailsOnAStaleLease(t *testing.T) {
	stale := false
	p := &Probes{
		Health:        &fakeHealth{primary: true},
		LeaseStale:    func() bool { return stale },
		KubeReachable: func(context.Context) bool { return true },
	}

	if err := p.Live(context.Background()); err != nil {
		t.Fatalf("a primary holding its lease is live: %v", err)
	}
	stale = true
	if err := p.Live(context.Background()); err == nil {
		t.Fatal("a primary whose lease went unrenewed must not report live")
	}

	// A standby holds no primary lease and is unaffected.
	p.Health = &fakeHealth{primary: false}
	if err := p.Live(context.Background()); err != nil {
		t.Fatalf("a standby is always live: %v", err)
	}
}

// TestLeaseStaleUsesTheLeaseDuration checks the boundary: freshness is only
// lost once a whole lease duration has passed with no successful renewal.
func TestLeaseStaleUsesTheLeaseDuration(t *testing.T) {
	now := time.Unix(1000, 0)
	l := &Lease{duration: 15 * time.Second, now: func() time.Time { return now }}

	if l.Stale() {
		t.Fatal("a lease never acquired is not stale; the agent holds nothing yet")
	}
	l.markAcquired()
	if l.Stale() {
		t.Fatal("a lease just renewed is fresh")
	}
	now = now.Add(15 * time.Second)
	if l.Stale() {
		t.Fatal("exactly one duration is still within the lease")
	}
	now = now.Add(time.Second)
	if !l.Stale() {
		t.Fatal("past the duration with no renewal the lease is stale")
	}
}
