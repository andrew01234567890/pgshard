package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEpochStoreAcceptsOnlyIncreasingAndPersists(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenEpochStore(dir)
	if err != nil || s.Current() != 0 {
		t.Fatalf("open: %v cur=%d", err, s.Current())
	}
	if err := s.Accept(0); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("epoch 0 accepted: %v", err)
	}
	if err := s.Accept(5); err != nil {
		t.Fatal(err)
	}
	if err := s.Accept(5); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("equal epoch accepted: %v", err)
	}
	if err := s.Accept(4); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("lower epoch accepted: %v", err)
	}
	if s.Current() != 5 {
		t.Fatalf("cur=%d", s.Current())
	}
	b, err := os.ReadFile(filepath.Join(dir, "pgshard", "epoch"))
	if err != nil || string(b) != "5\n" {
		t.Fatalf("file=%q err=%v", b, err)
	}
	re, err := OpenEpochStore(dir)
	if err != nil || re.Current() != 5 {
		t.Fatalf("reopen: %v cur=%d", err, re.Current())
	}
	if err := re.Accept(6); err != nil {
		t.Fatal(err)
	}
}

func TestEpochStoreRejectsCorruptFile(t *testing.T) {
	dir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(dir, "pgshard"), 0o700)
	_ = os.WriteFile(filepath.Join(dir, "pgshard", "epoch"), []byte("nope"), 0o600)
	if _, err := OpenEpochStore(dir); err == nil {
		t.Fatal("expected error")
	}
}

func TestRequireCurrentAcceptsOnlyTheCurrentEpoch(t *testing.T) {
	s, err := OpenEpochStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Accept(3); err != nil {
		t.Fatal(err)
	}
	if err := s.RequireCurrent(3); err != nil {
		t.Fatalf("current epoch must be accepted: %v", err)
	}
	for _, e := range []uint64{0, 2, 4} {
		if err := s.RequireCurrent(e); !errors.Is(err, ErrStaleEpoch) {
			t.Fatalf("epoch %d must be rejected as stale, got %v", e, err)
		}
	}
	if s.Current() != 3 {
		t.Fatalf("RequireCurrent must not move the epoch, got %d", s.Current())
	}
}

// TestATermEndsWhenTheNextEpochIsAccepted: comparing the epoch and moving on
// only says the caller was current when it asked. An operation admitted by
// one controller can pause -- on a connection, on pgBackRest, on PostgreSQL
// -- and resume after a new term has begun, and then clear a fence that term
// raised, change replication slots, or finish a prepared transaction the new
// leader is deciding about. Work admitted by an epoch now runs under a
// context that ends with it.
func TestATermEndsWhenTheNextEpochIsAccepted(t *testing.T) {
	s, err := OpenEpochStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Accept(7); err != nil {
		t.Fatal(err)
	}

	ctx, release, err := s.Term(context.Background(), 7)
	if err != nil {
		t.Fatalf("the current epoch was refused: %v", err)
	}
	defer release()
	if ctx.Err() != nil {
		t.Fatalf("the admitted operation starts cancelled: %v", ctx.Err())
	}

	// A stale controller is refused outright, as before.
	if _, rel, err := s.Term(context.Background(), 6); !errors.Is(err, ErrStaleEpoch) {
		rel()
		t.Fatalf("a stale epoch was admitted: %v", err)
	}

	if err := s.Accept(8); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("an operation admitted by the old epoch is still running in the new term")
	}
	if _, rel, err := s.Term(context.Background(), 7); !errors.Is(err, ErrStaleEpoch) {
		rel()
		t.Fatalf("the old epoch is still current: %v", err)
	}

	// The new term admits its own work, and that work is not cancelled.
	next, releaseNext, err := s.Term(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseNext()
	if next.Err() != nil {
		t.Fatalf("the new term starts cancelled: %v", next.Err())
	}

	// Releasing ends the caller's context without touching the term.
	releaseNext()
	if next.Err() == nil {
		t.Fatal("release must end the operation's context")
	}
	if again, rel, err := s.Term(context.Background(), 8); err != nil || again.Err() != nil {
		t.Fatalf("the term did not survive one operation ending: %v %v", err, again.Err())
	} else {
		rel()
	}
}
