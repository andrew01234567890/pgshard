package vstream

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/router"
)

// Topology is what the fan-in needs to know about the cluster: the shards
// of a set, the shard map generation, each shard's primary epoch and a
// pooler client for the shard's current primary.
type Topology interface {
	Shards(set string) []router.Shard
	Generation() uint64
	Epoch(sh router.Shard) uint64
	Client(sh router.Shard) (pgshardv1.PoolerClient, error)
}

// SnapshotTopology answers from the router's catalog snapshot and pooler
// resolver; endpoints follow shard_status.primary_endpoint, so a promotion
// is visible as a new epoch and a new pooler client.
type SnapshotTopology struct {
	Snapshot router.SnapshotFunc
	Poolers  *router.Poolers
}

// Shards lists the serving shards of set in id order.
func (t SnapshotTopology) Shards(set string) []router.Shard {
	s := t.Snapshot()
	if s == nil {
		return nil
	}
	var out []router.Shard
	for k := range s.Serving {
		if k.ShardSet == set {
			out = append(out, router.Shard{Set: k.ShardSet, ID: k.ShardID})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Generation returns the shard map generation.
func (t SnapshotTopology) Generation() uint64 {
	if s := t.Snapshot(); s != nil {
		return uint64(s.ShardMapGeneration)
	}
	return 0
}

// Epoch returns the primary epoch of sh.
func (t SnapshotTopology) Epoch(sh router.Shard) uint64 {
	if s := t.Snapshot(); s != nil {
		return uint64(s.Serving[snapshot.ShardKey{ShardSet: sh.Set, ShardID: sh.ID}].Epoch)
	}
	return 0
}

// Client returns the pooler client of sh's primary.
func (t SnapshotTopology) Client(sh router.Shard) (pgshardv1.PoolerClient, error) {
	return t.Poolers.Client(sh)
}

// Catalog reads stream definitions.
type Catalog interface {
	Lookup(ctx context.Context, name string) (catalog.Stream, error)
	List(ctx context.Context) ([]catalog.Stream, []catalog.StreamStatus, error)
}

// ErrUnknownStream is returned by Lookup for a stream that does not exist.
var ErrUnknownStream = errors.New("vstream: unknown stream")

// PGCatalog reads pgshard.streams and pgshard.stream_status.
type PGCatalog struct {
	Pool *pgxpool.Pool
}

// Lookup implements Catalog.
func (c PGCatalog) Lookup(ctx context.Context, name string) (catalog.Stream, error) {
	rows, err := c.Pool.Query(ctx, `SELECT name, database, two_phase, state, created_at FROM pgshard.streams WHERE name = $1`, name)
	if err != nil {
		return catalog.Stream{}, err
	}
	st, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByPos[catalog.Stream])
	if errors.Is(err, pgx.ErrNoRows) {
		return catalog.Stream{}, fmt.Errorf("%w: %q", ErrUnknownStream, name)
	}
	return st, err
}

// List implements Catalog.
func (c PGCatalog) List(ctx context.Context) ([]catalog.Stream, []catalog.StreamStatus, error) {
	streams, err := catalog.ListStreams(ctx, c.Pool)
	if err != nil {
		return nil, nil, err
	}
	status, err := catalog.ListStreamStatus(ctx, c.Pool, "")
	return streams, status, err
}
