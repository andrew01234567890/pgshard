package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/schemacopy"
)

// OwnerSchema and OwnerTable hold, in every user database of every shard,
// the keyspace range that shard owns (PGS-878). A check on each sharded
// table compares a written row's key hash against it, so a row that lands
// on a shard its key does not hash to -- a BEFORE trigger rewriting the
// key, a client writing to a shard directly -- is refused rather than
// stored where no lookup will find it.
//
// The range is per shard, not per schema: it must never travel with a
// schema copy (a reshard target would inherit its source's range) nor with
// a reshard's row stream. So the schema is left out of both, and each
// shard's row is written here.
const (
	OwnerSchema = schemacopy.OwnerSchema
	OwnerTable  = "owned_range"
)

// ownedRangeDDL creates the table. One row: singleton is its key and is
// always true, so a second row cannot exist to disagree with the first.
var ownedRangeDDL = []string{
	"CREATE SCHEMA IF NOT EXISTS " + OwnerSchema,
	fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
		singleton bool PRIMARY KEY DEFAULT true CHECK (singleton),
		shard_set text NOT NULL,
		shard_id  integer NOT NULL,
		lo        bigint NOT NULL,
		hi        bigint NOT NULL,
		CHECK (lo <= hi))`, OwnerSchema, OwnerTable),
}

// OwnedRanges keeps every shard's owned range in every user database: the
// serving set's shards, and a reshard's or upgrade's targets as soon as they
// have ranges, so a target holds its own range before it serves a write.
type OwnedRanges struct {
	Pool   *pgxpool.Pool
	Shards ShardDBDialer
	Logger *slog.Logger
}

func (o *OwnedRanges) logger() *slog.Logger {
	if o.Logger != nil {
		return o.Logger
	}
	return slog.Default()
}

// Run writes the ranges on a ticker while this process is the leader.
func (o *OwnedRanges) Run(ctx context.Context, interval time.Duration, leader func() bool) {
	runLoopStoppable(ctx, interval, leader, o.logger, "owned ranges", func(ctx context.Context) {
		if _, err := o.Pass(ctx); err != nil {
			o.logger().Warn("owned range pass failed", "err", err)
		}
	})
}

// Pass writes the owned range of every shard of every set that is serving
// or being provisioned, in every user database, and returns how many rows it
// changed. A shard it cannot reach is skipped and reported, not fatal: the
// next pass tries again, and one unreachable shard must not hold every
// other shard's range back.
func (o *OwnedRanges) Pass(ctx context.Context) (int, error) {
	rows, err := o.Pool.Query(ctx, `SELECT shard_set FROM pgshard.shard_sets WHERE state IN ($1, $2) ORDER BY generation`,
		catalog.ShardSetServing, catalog.ShardSetProvisioning)
	if err != nil {
		return 0, err
	}
	sets, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, err
	}
	dbs, err := catalog.ListDatabases(ctx, o.Pool)
	if err != nil {
		return 0, err
	}
	changed := 0
	var firstErr error
	for _, set := range sets {
		ranges, err := catalog.ListShardRanges(ctx, o.Pool, set)
		if err != nil {
			return changed, err
		}
		owned := catalog.RangeSet(ranges)
		for i, rg := range ranges {
			for _, db := range dbs {
				n, err := o.write(ctx, set, rg.ShardID, db.Name, owned[i].Start, owned[i].End)
				changed += n
				if err != nil {
					o.logger().Warn("owned range not written", "shard_set", set, "shard", rg.ShardID, "database", db.Name, "err", err)
					if firstErr == nil {
						firstErr = fmt.Errorf("shard %s/%d database %s: %w", set, rg.ShardID, db.Name, err)
					}
				}
			}
		}
	}
	return changed, firstErr
}

// write makes one database's row say [lo, hi] for shard id of set, and
// reports whether it changed anything.
func (o *OwnedRanges) write(ctx context.Context, set string, id int32, database string, lo, hi int64) (int, error) {
	conn, err := o.Shards.DialDatabase(ctx, set, id, database)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, stmt := range ownedRangeDDL {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return 0, err
		}
	}
	tag, err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.%s (shard_set, shard_id, lo, hi) VALUES ($1, $2, $3, $4)
		ON CONFLICT (singleton) DO UPDATE SET shard_set = EXCLUDED.shard_set, shard_id = EXCLUDED.shard_id, lo = EXCLUDED.lo, hi = EXCLUDED.hi
		WHERE (%[2]s.shard_set, %[2]s.shard_id, %[2]s.lo, %[2]s.hi) IS DISTINCT FROM (EXCLUDED.shard_set, EXCLUDED.shard_id, EXCLUDED.lo, EXCLUDED.hi)`,
		OwnerSchema, OwnerTable), set, id, lo, hi)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}
