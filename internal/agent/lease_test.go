package agent

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := a.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- a.Hold(ctx) }()
	time.Sleep(30 * time.Millisecond)
	l, _ := client.Get(ctx, "c1-s0-primary", metav1.GetOptions{})
	holder := "pod-b"
	l.Spec.HolderIdentity = &holder
	now := metav1.NewMicroTime(time.Now())
	l.Spec.RenewTime = &now
	if _, err := client.Update(ctx, l, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
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
