package agent

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

func leaseCfg(pod string) *Config {
	c := testConfig()
	c.PodName = pod
	c.Lease = LeaseConfig{Enabled: true, Namespace: "ns"}
	c.applyDefaults()
	return c
}

func TestLeaseAcquireCreateRenewAndContention(t *testing.T) {
	cs := fake.NewClientset()
	client := cs.CoordinationV1().Leases("ns")
	log := slog.New(slog.DiscardHandler)
	a := NewLeaseWithClient(client, leaseCfg("pod-a"), log)
	b := NewLeaseWithClient(client, leaseCfg("pod-b"), log)
	ctx := context.Background()
	if err := a.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Acquire(ctx); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("b acquired a held lease: %v", err)
	}
	if err := a.Acquire(ctx); err != nil {
		t.Fatalf("holder cannot renew: %v", err)
	}
	l, _ := client.Get(ctx, "c1-s0-primary", metav1.GetOptions{})
	if *l.Spec.HolderIdentity != "pod-a" || *l.Spec.LeaseDurationSeconds != 15 || *l.Spec.LeaseTransitions != 1 {
		t.Fatalf("lease spec: %+v", l.Spec)
	}
	b.now = func() time.Time { return time.Now().Add(20 * time.Second) }
	if err := b.Acquire(ctx); err != nil {
		t.Fatalf("expired lease not taken: %v", err)
	}
	l, _ = client.Get(ctx, "c1-s0-primary", metav1.GetOptions{})
	if *l.Spec.HolderIdentity != "pod-b" || *l.Spec.LeaseTransitions != 2 {
		t.Fatalf("lease spec after takeover: %+v", l.Spec)
	}
	if err := a.Acquire(ctx); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("a re-took a lease held by b: %v", err)
	}
	if err := b.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.Acquire(ctx); err != nil {
		t.Fatalf("released lease not acquirable: %v", err)
	}
	if !a.Reachable(ctx) {
		t.Fatal("fake API should be reachable")
	}
}

func TestLeaseHoldReturnsWhenTakenOver(t *testing.T) {
	cs := fake.NewClientset()
	client := cs.CoordinationV1().Leases("ns")
	log := slog.New(slog.DiscardHandler)
	a := NewLeaseWithClient(client, leaseCfg("pod-a"), log)
	a.renew = 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := a.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- a.Hold(ctx) }()
	time.Sleep(30 * time.Millisecond)
	// The fake clientset applies any update regardless of resourceVersion, so a
	// renew landing between this Get and Update would silently reinstate pod-a
	// and the hold would never see the takeover.
	takeOver(ctx, t, client)
	select {
	case err := <-errc:
		if !errors.Is(err, ErrLeaseHeld) {
			t.Fatalf("hold returned %v", err)
		}
	case <-ctx.Done():
		t.Fatal("hold did not notice takeover")
	}
}

func TestLeaseHoldStopsCleanlyOnCancel(t *testing.T) {
	cs := fake.NewClientset()
	a := NewLeaseWithClient(cs.CoordinationV1().Leases("ns"), leaseCfg("pod-a"), slog.New(slog.DiscardHandler))
	a.renew = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	if err := a.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- a.Hold(ctx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-errc; err != nil {
		t.Fatalf("hold: %v", err)
	}
}

// TestLeaseAcquireRetriesOwnConflict: an optimistic-concurrency conflict on a
// lease we still hold (our hold loop or the operator's fence bumped the
// resourceVersion) is retried from a fresh read, not reported as a lost lease.
func TestLeaseAcquireRetriesOwnConflict(t *testing.T) {
	cs := fake.NewClientset()
	client := cs.CoordinationV1().Leases("ns")
	a := NewLeaseWithClient(client, leaseCfg("pod-a"), slog.New(slog.DiscardHandler))
	ctx := context.Background()
	if err := a.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	conflicts := 0
	cs.PrependReactor("update", "leases", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		if conflicts < 1 {
			conflicts++
			return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, "c1-s0-primary", errors.New("stale"))
		}
		return false, nil, nil
	})
	if err := a.Acquire(ctx); err != nil {
		t.Fatalf("a single conflict on our own lease must be retried, got %v", err)
	}
	if conflicts != 1 {
		t.Fatalf("conflict reactor fired %d times", conflicts)
	}
	// A persistent conflict is a transient error, never ErrLeaseHeld.
	cs.PrependReactor("update", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, "c1-s0-primary", errors.New("stale"))
	})
	if err := a.Acquire(ctx); err == nil || errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("persistent conflict must surface as transient, got %v", err)
	}
}

func takeOver(ctx context.Context, t *testing.T, client coordclient.LeaseInterface) {
	t.Helper()
	holder := "pod-b"
	for {
		l, err := client.Get(ctx, "c1-s0-primary", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		l.Spec.HolderIdentity = &holder
		now := metav1.NewMicroTime(time.Now())
		l.Spec.RenewTime = &now
		switch _, err := client.Update(ctx, l, metav1.UpdateOptions{}); {
		case err == nil:
			if got, _ := client.Get(ctx, "c1-s0-primary", metav1.GetOptions{}); got != nil &&
				ptr.Deref(got.Spec.HolderIdentity, "") == holder {
				return
			}
		case apierrors.IsConflict(err):
		default:
			t.Fatal(err)
		}
	}
}
