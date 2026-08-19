package router

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// BlockSource hands out disjoint value blocks of a global sequence.
type BlockSource interface {
	// AllocateBlock returns the inclusive bounds of a fresh block of name,
	// creating the sequence on first use.
	AllocateBlock(ctx context.Context, name string) (start, end int64, err error)
}

// PGBlockSource allocates through pgshard.allocate_sequence_block in the
// catalog, whose one-row UPDATE ... RETURNING makes concurrent routers'
// blocks disjoint.
type PGBlockSource struct {
	Pool *pgxpool.Pool
}

// AllocateBlock implements BlockSource.
func (s *PGBlockSource) AllocateBlock(ctx context.Context, name string) (start, end int64, err error) {
	err = s.Pool.QueryRow(ctx, `SELECT block_start, block_end FROM pgshard.allocate_sequence_block($1)`, name).Scan(&start, &end)
	return start, end, err
}

// SequenceAllocator serves sequence values from per-sequence blocks cached
// in the router; a block is fetched from the source when the previous one
// runs out and its unused remainder is never handed out again.
type SequenceAllocator struct {
	source BlockSource

	mu     sync.Mutex
	blocks map[string]*seqBlock
}

type seqBlock struct {
	mu   sync.Mutex
	next int64
	end  int64
}

// NewSequenceAllocator builds an allocator over source.
func NewSequenceAllocator(source BlockSource) *SequenceAllocator {
	return &SequenceAllocator{source: source, blocks: map[string]*seqBlock{}}
}

func (a *SequenceAllocator) block(name string) *seqBlock {
	a.mu.Lock()
	defer a.mu.Unlock()
	b, ok := a.blocks[name]
	if !ok {
		b = &seqBlock{next: 1, end: 0}
		a.blocks[name] = b
	}
	return b
}

// Next returns n fresh values of the named sequence, ascending.
func (a *SequenceAllocator) Next(ctx context.Context, name string, n int) ([]int64, error) {
	b := a.block(name)
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]int64, 0, n)
	for len(out) < n {
		if b.next > b.end {
			start, end, err := a.source.AllocateBlock(ctx, name)
			if err != nil {
				return nil, pgwire.Errorf(codeConnectionFailure, "allocating a block of sequence %s from the catalog failed: %v", name, err)
			}
			if end < start {
				return nil, pgwire.Errorf(pgwire.CodeInternalError, "sequence %s: catalog returned an empty block [%d, %d]", name, start, end)
			}
			b.next, b.end = start, end
		}
		out = append(out, b.next)
		b.next++
	}
	return out, nil
}
