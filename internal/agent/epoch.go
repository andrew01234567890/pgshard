package agent

import (
	"context"
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
	// term is cancelled when a later epoch is accepted. An RPC that passed
	// the epoch check runs under a context derived from it, so work started
	// in one term does not continue into the next -- which is the whole
	// point of the check, and is not what comparing a number and moving on
	// achieves.
	term       context.Context
	termCancel context.CancelFunc
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
	return s.requireLocked(epoch)
}

func (s *EpochStore) requireLocked(epoch uint64) error {
	if epoch != s.cur {
		return fmt.Errorf("%w: got %d, current %d", ErrStaleEpoch, epoch, s.cur)
	}
	return nil
}

// Term is RequireCurrent plus the context the accepted operation must run
// under: a child of parent that is also cancelled when a later epoch is
// accepted.
//
// The check on its own only says the caller was current when it asked. An
// operation that passed it can pause -- on a connection, on pgBackRest, on
// PostgreSQL -- and resume after a new term has begun, and then clear a
// fence that term raised, change replication slots, or finish a prepared
// transaction the new leader is deciding about. The returned release must
// be called when the operation ends.
func (s *EpochStore) Term(parent context.Context, epoch uint64) (context.Context, func(), error) {
	s.mu.Lock()
	if err := s.requireLocked(epoch); err != nil {
		s.mu.Unlock()
		return nil, func() {}, err
	}
	s.ensureTermLocked()
	term := s.term
	s.mu.Unlock()

	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(term, cancel)
	return ctx, func() { stop(); cancel() }, nil
}

// ensureTermLocked creates the first term lazily, so a store loaded from
// disk does not need one until something is fenced against it.
func (s *EpochStore) ensureTermLocked() {
	if s.term == nil {
		s.term, s.termCancel = context.WithCancel(context.Background())
	}
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
	// Everything the old term admitted stops here, whether or not it has
	// noticed.
	if s.termCancel != nil {
		s.termCancel()
	}
	s.term, s.termCancel = context.WithCancel(context.Background())
	return nil
}
