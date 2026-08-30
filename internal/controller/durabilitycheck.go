package controller

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// protectedSettings are the settings a commit's durability depends on. The
// router refuses every statement that would change one, and pgtune fixes
// them in postgresql.conf, so drift here did not arrive through pgshard:
// it was set on the shard directly, and that is exactly why something has
// to look.
var protectedSettings = []string{"synchronous_commit", "fsync", "full_page_writes", "wal_level"}

// protectedWant is what each of them must read as. A value is compared, not
// merely reported, because "synchronous_commit = local" looks like a
// setting rather than a fault unless something knows which one is right.
var protectedWant = map[string]string{
	"synchronous_commit": "on",
	"fsync":              "on",
	"full_page_writes":   "on",
	"wal_level":          "logical",
}

// durabilityDriftSQL asks a shard two questions at once: what the setting
// reads as now, and whether a per-role or per-database entry will impose a
// different value on some future session. The second matters even when the
// first is correct -- ALTER ROLE app SET synchronous_commit = off leaves
// the controller's own connection reading "on".
const durabilityDriftSQL = `
SELECT format('%s is %s (source %s)', name, setting, source)
FROM pg_settings WHERE name = ANY($1) AND setting <> ($2::jsonb ->> name)
UNION ALL
SELECT format('%s is set to %s for %s', split_part(cfg, '=', 1), split_part(cfg, '=', 2),
	coalesce(nullif(concat_ws(' in database ', r.rolname, d.datname), ''), 'every role and database'))
FROM pg_db_role_setting s
	LEFT JOIN pg_roles r ON r.oid = s.setrole
	LEFT JOIN pg_database d ON d.oid = s.setdatabase,
	unnest(s.setconfig) AS cfg
WHERE split_part(cfg, '=', 1) = ANY($1)
ORDER BY 1`

// DurabilityCheck reports settings on the shards that would let a commit be
// acknowledged before its WAL is durable.
//
// The router already refuses every route to them that goes through pgshard:
// SET, set_config in any expression, ALTER ROLE ... SET, and ALTER DATABASE
// SET as a whole; CREATE FUNCTION and DO are refused outright, so a body
// the router cannot parse cannot be installed through it either. What
// remains is a connection made straight to a shard, which is a different
// finding with its own fix -- and until that lands, and after it for
// anything with the superuser password, the only defence is to look and
// say so.
type DurabilityCheck struct {
	Pool   *pgxpool.Pool
	Shards ShardDialer
	Logger *slog.Logger
}

func (d *DurabilityCheck) logger() *slog.Logger {
	if d.Logger != nil {
		return d.Logger
	}
	return slog.Default()
}

// Run re-audits on a ticker while this process is the leader.
func (d *DurabilityCheck) Run(ctx context.Context, interval time.Duration, leader func() bool) {
	runLoop(ctx, interval, leader, d.logger, "durability check", func(ctx context.Context) {
		if _, err := d.Pass(ctx); err != nil {
			d.logger().Warn("durability check pass failed", "err", err)
		}
	})
}

// Pass audits every shard of the serving set and returns what it found,
// one entry per shard that has drifted.
func (d *DurabilityCheck) Pass(ctx context.Context) (map[int32][]string, error) {
	var set string
	if err := d.Pool.QueryRow(ctx, `SELECT shard_set FROM pgshard.shard_sets WHERE state = $1 ORDER BY generation DESC LIMIT 1`,
		catalog.ShardSetServing).Scan(&set); err != nil {
		return nil, fmt.Errorf("serving shard set: %w", err)
	}
	ranges, err := catalog.ListShardRanges(ctx, d.Pool, set)
	if err != nil {
		return nil, err
	}
	drift := map[int32][]string{}
	for _, rg := range ranges {
		found, err := d.audit(ctx, set, rg.ShardID)
		if err != nil {
			// A shard that could not be asked has not been cleared: say so
			// and try again next pass rather than report a clean cluster.
			d.logger().Warn("shard not audited for durability drift", "shard", fmt.Sprintf("%s/%d", set, rg.ShardID), "err", err)
			continue
		}
		if len(found) > 0 {
			drift[rg.ShardID] = found
			d.logger().Error("durability settings on a shard do not match the cluster's floor",
				"shard", fmt.Sprintf("%s/%d", set, rg.ShardID), "drift", found)
		}
	}
	return drift, nil
}

func (d *DurabilityCheck) audit(ctx context.Context, set string, id int32) ([]string, error) {
	conn, err := d.Shards.Dial(ctx, set, id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(ctx) }()
	return durabilityDrift(ctx, conn)
}

func durabilityDrift(ctx context.Context, conn ShardConn) ([]string, error) {
	want := map[string]string{}
	for _, n := range protectedSettings {
		want[n] = protectedWant[n]
	}
	rows, err := conn.Query(ctx, durabilityDriftSQL, protectedSettings, mustJSON(want))
	if err != nil {
		return nil, err
	}
	found, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	slices.Sort(found)
	return found, nil
}
