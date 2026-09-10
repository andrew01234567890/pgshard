package agent

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
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

// TestAReleasedLeaseStopsBeingStale.
//
// Release cleared the Kubernetes holder but never reset the acquired time,
// so Stale went true once the duration passed -- for ever, on a member that
// had deliberately GIVEN UP the lease.
//
// The liveness probe only consults Stale for a member that still READS as a
// primary, and a demote is exactly that window: standby.signal is not
// written until the end of Follow, so a member being demoted, rewound or
// recloned still looks like a primary while it works. It was then killed
// for holding a lease it no longer held -- and a reclone that had already
// cleared the data directory started again from nothing.
//
// Driven through Release rather than the helper it calls: the reset is only
// worth anything if the release path performs it.
func TestAReleasedLeaseStopsBeingStale(t *testing.T) {
	cs := fake.NewClientset()
	l := NewLeaseWithClient(cs.CoordinationV1().Leases("ns"), leaseCfg("pod-a"), slog.New(slog.DiscardHandler))
	now := time.Unix(1000, 0)
	l.now = func() time.Time { return now }
	ctx := context.Background()

	if err := l.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	now = now.Add(l.duration + time.Second)
	if !l.Stale() {
		t.Fatal("the premise of this test is a lease that has gone stale")
	}

	if err := l.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if l.Stale() {
		t.Fatal("a lease this member gave up is still reported stale; liveness kills it mid-demote for holding nothing")
	}

	// Taking it again starts the clock afresh rather than inheriting the
	// old one.
	if err := l.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	if l.Stale() {
		t.Fatal("a reacquired lease is fresh")
	}
}
