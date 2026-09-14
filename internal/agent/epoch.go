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

// EpochStore persists the last accepted fencing epoch: in the file
// Config.EpochFile names, or under PGDATA/pgshard/epoch when that is unset.
type EpochStore struct {
	mu   sync.Mutex
	path string
	// legacy is the in-PGDATA file when path is somewhere else, and empty
	// when path IS the legacy file.
	legacy string
	cur    uint64
	// term is cancelled when a later epoch is accepted. An RPC that passed
	// the epoch check runs under a context derived from it, so work started
	// in one term does not continue into the next -- which is the whole
	// point of the check, and is not what comparing a number and moving on
	// achieves.
	term       context.Context
	termCancel context.CancelFunc
}

// OpenEpochStore loads the epoch from its legacy place inside PGDATA,
// treating a missing file as 0.
func OpenEpochStore(pgdata string) (*EpochStore, error) { return OpenEpochStoreAt(pgdata, "") }

// OpenEpochStoreAt loads the epoch from file, or from inside PGDATA when
// file is empty.
//
// Inside PGDATA is the wrong place for a fence, which is why file exists. A
// reclone empties PGDATA before pg_basebackup copies the primary's in --
// its epoch file included -- and pg_rewind replaces the file with the
// source's. The running agent keeps its epoch in memory, but one that dies
// part-way through a clone restarts with no file, loads 0, and treats any
// epoch at all as newer: a delayed Promote from a term long over included.
//
// Both files are read and the HIGHER wins, written through to the
// configured one. Taking the higher is the only safe direction, because
// every value in either file came from the group's single increasing epoch
// sequence, and a fence only ever needs to refuse more:
//
//   - an upgrading member's configured file does not exist yet, and its
//     epoch is in the legacy one;
//   - an agent rolled back to a binary that knows only the legacy file, then
//     forward again, has accepted epochs there that the configured file
//     never saw -- preferring the configured file would lower the fence and
//     admit a replay of an epoch it had already refused;
//   - a clone copies the primary's legacy file in, which is at least as high
//     as this member's own, and raising this member's fence to it is safe.
//
// Accept writes the legacy file too, for as long as it exists, so a
// rolled-back binary finds the current epoch.
func OpenEpochStoreAt(pgdata, file string) (*EpochStore, error) {
	legacy := filepath.Join(pgdata, "pgshard", "epoch")
	if file == "" || file == legacy {
		v, _, err := readEpochFile(legacy)
		if err != nil {
			return nil, err
		}
		return &EpochStore{path: legacy, cur: v}, nil
	}
	s := &EpochStore{path: file, legacy: legacy}
	configured, foundConfigured, err := readEpochFile(file)
	if err != nil {
		return nil, err
	}
	old, _, err := readEpochFile(legacy)
	if err != nil {
		return nil, err
	}
	s.cur = max(configured, old)
	if s.cur > configured || (!foundConfigured && s.cur > 0) {
		if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
			return nil, err
		}
		if err := writeFileSync(file, []byte(strconv.FormatUint(s.cur, 10)+"\n")); err != nil {
			return nil, fmt.Errorf("carry epoch %d into %s: %w", s.cur, file, err)
		}
	}
	return s, nil
}

func readEpochFile(path string) (uint64, bool, error) {
	b, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return 0, false, nil
	case err != nil:
		return 0, false, err
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("corrupt epoch file %s: %w", path, err)
	}
	return v, true, nil
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
	// The legacy file too, so a rolled-back binary finds the current epoch --
	// but only into an initialised data directory. Mid-clone PGDATA has just
	// been emptied and pg_basebackup refuses a directory that is not empty,
	// so writing there then would break the clone; PG_VERSION is absent
	// exactly then. Best effort beyond that: the configured file is the
	// fence, and this copy only spares a rolled-back binary a lower one.
	if s.legacy != "" {
		pgdata := filepath.Dir(filepath.Dir(s.legacy))
		if _, err := os.Stat(filepath.Join(pgdata, "PG_VERSION")); err == nil {
			if err := os.MkdirAll(filepath.Dir(s.legacy), 0o700); err == nil {
				_ = writeFileSync(s.legacy, []byte(strconv.FormatUint(epoch, 10)+"\n"))
			}
		}
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
