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
	// The fake clientset applies any update whatever resourceVersion it
	// carries, so a renew that read the lease before this write lands after
	// it and reinstates pod-a. Confirming the write stuck is not enough --
	// an in-flight renew can still overwrite it a moment later. The only
	// state that settles the question is Hold itself having returned, so
	// the takeover is reasserted until it does.
	// Well under the 15s lease duration on purpose. Past it Hold gives up
	// with "lease not renewed within ...", which wraps the same
	// ErrLeaseHeld -- so a generous deadline would let a Hold that merely
	// timed out pass as one that noticed the takeover.
	deadline := time.After(5 * time.Second)
	for {
		select {
		case err := <-errc:
			if !errors.Is(err, ErrLeaseHeld) {
				t.Fatalf("hold returned %v", err)
			}
			return
		case <-deadline:
			t.Fatal("hold did not notice the takeover")
		default:
		}
		takeOver(ctx, t, client)
		time.Sleep(2 * time.Millisecond)
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

// takeOver writes pod-b in as the holder once. It does not verify the write
// survived: only the caller's loop, which watches Hold, can tell that.
func takeOver(ctx context.Context, t *testing.T, client coordclient.LeaseInterface) {
	t.Helper()
	l, err := client.Get(ctx, "c1-s0-primary", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	holder := "pod-b"
	l.Spec.HolderIdentity = &holder
	now := metav1.NewMicroTime(time.Now())
	l.Spec.RenewTime = &now
	if _, err := client.Update(ctx, l, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(err) {
		t.Fatal(err)
	}
}

// TestLeaseIsLabelledWithItsCluster: a namespace can hold several clusters,
// and every other object pgshard creates says which one it belongs to. A
// Lease that does not can only be attributed by matching names, and a
// cluster called "a" prefixes "ab"'s objects.
func TestLeaseIsLabelledWithItsCluster(t *testing.T) {
	cs := fake.NewClientset()
	client := cs.CoordinationV1().Leases("ns")
	ctx := context.Background()
	l := NewLeaseWithClient(client, leaseCfg("pod-a"), slog.New(slog.DiscardHandler))
	if err := l.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := client.Get(ctx, "c1-s0-primary", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Labels[LabelCluster] != "c1" {
		t.Fatalf("labels %v, want %s=c1", got.Labels, LabelCluster)
	}
	// And a renewal keeps it: the label must not depend on which pass
	// created the object.
	if err := l.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	got, _ = client.Get(ctx, "c1-s0-primary", metav1.GetOptions{})
	if got.Labels[LabelCluster] != "c1" {
		t.Fatalf("after a renewal: labels %v", got.Labels)
	}
}
