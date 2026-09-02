package agent

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// TestLeaseHoldOutlivesTheAgentLifecycleContext: shutdown is exactly when the
// lease matters most. Run cancels its context and only then stops PostgreSQL,
// with a budget of three times shutdownTimeout -- 90s at the defaults against
// a 15s lease. A hold bound to that context would stop renewing at the top of
// a shutdown that still has a minute of writable primary left in it, the
// lease would expire, and another member could promote under it.
//
// So the renewal deliberately runs on its own context and is ended by
// releaseLease, after postgres is down, rather than by the lifecycle context.
// This test exists because tying it to the lifecycle context is the obvious
// tidy-up and it is wrong.
func TestLeaseHoldOutlivesTheAgentLifecycleContext(t *testing.T) {
	cs := fake.NewClientset()
	var renewals atomic.Int32
	cs.PrependReactor("update", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
		renewals.Add(1)
		return false, nil, nil
	})
	cfg := leaseCfg("pod-a")
	cfg.Lease.Renew = Duration(time.Millisecond)
	log := slog.New(slog.DiscardHandler)
	lease := NewLeaseWithClient(cs.CoordinationV1().Leases("ns"), cfg, log)
	if err := lease.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	srv := NewServer(nil, nil, lease, log, func(error) {})
	lifecycle, cancel := context.WithCancel(context.Background())
	srv.bgCtx = lifecycle
	srv.mu.Lock()
	srv.startHold()
	srv.mu.Unlock()

	cancel()
	before := renewals.Load()
	deadline := time.Now().Add(2 * time.Second)
	for renewals.Load() <= before {
		if time.Now().After(deadline) {
			t.Fatal("renewal stopped when the lifecycle context was cancelled: a shutdown longer than the lease would drop it while postgres was still writable")
		}
		time.Sleep(time.Millisecond)
	}

	// And it does stop where the shutdown path stops it.
	srv.releaseLease(context.Background())
	stopped := renewals.Load()
	time.Sleep(20 * time.Millisecond)
	if n := renewals.Load(); n > stopped {
		t.Fatalf("releaseLease left the hold renewing: %d renewals after release", n-stopped)
	}
}
