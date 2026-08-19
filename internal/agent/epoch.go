package agent

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// ErrStaleEpoch is returned when a command's epoch does not exceed the last
// accepted one.
var ErrStaleEpoch = errors.New("stale epoch")

// EpochStore persists the last accepted fencing epoch under PGDATA/pgshard/epoch.
type EpochStore struct {
	mu   sync.Mutex
	path string
	cur  uint64
}

// OpenEpochStore loads the stored epoch, treating a missing file as 0.
func OpenEpochStore(pgdata string) (*EpochStore, error) {
	s := &EpochStore{path: filepath.Join(pgdata, "pgshard", "epoch")}
	b, err := os.ReadFile(s.path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return s, nil
	case err != nil:
		return nil, err
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("corrupt epoch file %s: %w", s.path, err)
	}
	s.cur = v
	return s, nil
}

// Current returns the last accepted epoch.
func (s *EpochStore) Current() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cur
}

// RequireCurrent accepts epoch only when it equals the last accepted epoch:
// same-term operations and idempotent retries pass, a stale controller is fenced.
func (s *EpochStore) RequireCurrent(epoch uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if epoch != s.cur {
		return fmt.Errorf("%w: got %d, current %d", ErrStaleEpoch, epoch, s.cur)
	}
	return nil
}

// Accept stores epoch if it is strictly greater than the current one; the
// write is fsynced before returning so a crash cannot roll the fence back.
func (s *EpochStore) Accept(epoch uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if epoch <= s.cur {
		return fmt.Errorf("%w: got %d, last accepted %d", ErrStaleEpoch, epoch, s.cur)
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	if err := writeFileSync(s.path, []byte(strconv.FormatUint(epoch, 10)+"\n")); err != nil {
		return err
	}
	s.cur = epoch
	return nil
}
