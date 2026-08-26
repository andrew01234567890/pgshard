package catalog

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/placement"
)

// DefaultShardSet is the shard set the first generation of the shard map
// lives in; it is serving from the moment the catalog exists.
const DefaultShardSet = "default"

// Shard set states in pgshard.shard_sets.
const (
	ShardSetDesired      = "desired"
	ShardSetProvisioning = "provisioning"
	ShardSetServing      = "serving"
	ShardSetRetired      = "retired"
)

// ShardSet is a row of pgshard.shard_sets.
type ShardSet struct {
	Name              string
	Generation        int64
	State             string
	DesiredGeneration int64
	CreatedAt         time.Time
	UpdatedAt         time.Time
	// PGMajor is the PostgreSQL major the set's groups run; nil for sets
	// created before upgrades existed (treated as the cluster default).
	PGMajor *int
}

// ShardSetName names the shard set of one generation: "default" for the
// first, g<n> afterwards.
func ShardSetName(generation int64) string {
	if generation <= 1 {
		return DefaultShardSet
	}
	return fmt.Sprintf("g%d", generation)
}

// ListShardSets returns every shard set ordered by generation.
func ListShardSets(ctx context.Context, q Querier) ([]ShardSet, error) {
	rows, err := q.Query(ctx, `
		SELECT shard_set, generation, state, desired_generation, created_at, updated_at, pg_major
		FROM pgshard.shard_sets ORDER BY generation`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[ShardSet])
}

// Int8Range renders r as a PostgreSQL int8range literal; the bounds of the
// key space render as unbounded.
func Int8Range(r placement.Range) string {
	lo, hi := "", ""
	if r.Start != math.MinInt64 {
		lo = fmt.Sprint(r.Start)
	}
	if r.End != math.MaxInt64 {
		hi = fmt.Sprint(r.End + 1)
	}
	return "[" + lo + "," + hi + ")"
}

// ValidateShardRanges checks the key-space coverage and the rule that routing
// depends on: shard IDs are the position of their range in key order, because
// RangeSet is positional and drops the IDs.
func ValidateShardRanges(ranges []ShardRange) error {
	if err := RangeSet(ranges).Validate(); err != nil {
		return err
	}
	for i, r := range ranges {
		if int(r.ShardID) != i {
			return fmt.Errorf("shard IDs must be 0..N-1 in key order: position %d holds shard %d", i, r.ShardID)
		}
	}
	return nil
}

// RangeSet converts catalog rows (sorted by key space) to placement ranges.
func RangeSet(ranges []ShardRange) placement.RangeSet {
	out := make(placement.RangeSet, 0, len(ranges))
	for _, r := range ranges {
		start, end := int64(math.MinInt64), int64(math.MaxInt64)
		if r.Lower != nil {
			start = *r.Lower
		}
		if r.Upper != nil {
			end = *r.Upper - 1
		}
		out = append(out, placement.Range{Start: start, End: end})
	}
	return out
}

// MaterializeShardSet writes ranges as shard 0..n-1 of a shard set in state.
// The set must have no ranges yet; the default set's row (created by the
// schema) is adopted.
//
// major is stamped in the same statement rather than by a later call, because
// the controller decides whether a pending set is a major upgrade or a plain
// reshard from that field the first time it sees the set, and never revisits
// the decision. A set that appears without its major is taken for a reshard,
// and an upgrade that loses that race runs without any of its preconditions.
// Zero leaves it unset, for callers that genuinely have no major to record.
func MaterializeShardSet(ctx context.Context, tx pgx.Tx, name string, generation int64, state string, ranges placement.RangeSet, major int) error {
	if err := ranges.Validate(); err != nil {
		return err
	}
	var pgMajor *int
	if major > 0 {
		pgMajor = &major
	}
	if _, err := tx.Exec(ctx, `INSERT INTO pgshard.shard_sets (shard_set, generation, state, pg_major) VALUES ($1, $2, $3, $4)
		ON CONFLICT (shard_set) DO UPDATE SET state = EXCLUDED.state, pg_major = coalesce(EXCLUDED.pg_major, pgshard.shard_sets.pg_major)
		WHERE pgshard.shard_sets.generation = EXCLUDED.generation
		  AND NOT EXISTS (SELECT 1 FROM pgshard.shard_ranges WHERE shard_set = EXCLUDED.shard_set)`,
		name, generation, state, pgMajor); err != nil {
		return err
	}
	for i, r := range ranges {
		if _, err := tx.Exec(ctx, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ($1, $2, $3::int8range)`,
			name, i, Int8Range(r)); err != nil {
			return err
		}
	}
	return nil
}

// SetShardSetMajor stamps the PostgreSQL major a shard set's groups run.
func SetShardSetMajor(ctx context.Context, q Execer, name string, major int) error {
	_, err := q.Exec(ctx, `UPDATE pgshard.shard_sets SET pg_major = $2 WHERE shard_set = $1`, name, major)
	return err
}

// DropShardSet removes a shard set, its ranges and its status rows.
func DropShardSet(ctx context.Context, tx pgx.Tx, name string) error {
	for _, stmt := range []string{
		`DELETE FROM pgshard.shard_status WHERE shard_set = $1`,
		`DELETE FROM pgshard.shard_ranges WHERE shard_set = $1`,
		`DELETE FROM pgshard.shard_sets WHERE shard_set = $1`,
	} {
		if _, err := tx.Exec(ctx, stmt, name); err != nil {
			return err
		}
	}
	return nil
}

// ServingShardSet returns the name of the serving shard set with the highest
// generation: the map routers route by. Before any set is materialized it
// is DefaultShardSet.
func ServingShardSet(ctx context.Context, q Querier) (string, error) {
	rows, err := q.Query(ctx, `SELECT shard_set FROM pgshard.shard_sets WHERE state = $1 ORDER BY generation DESC LIMIT 1`, ShardSetServing)
	if err != nil {
		return "", err
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return "", err
	}
	if len(names) == 0 {
		return DefaultShardSet, nil
	}
	return names[0], nil
}
