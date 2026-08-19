package agent

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
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
