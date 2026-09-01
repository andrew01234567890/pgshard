package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ShardKeyCheck asks a shard what type a table's shard key column really
// is, before the reconciler routes anything by it.
//
// The router hashes the key value the client sent; a row filter, a copy and
// a re-key hash the value the shard stored. For most types those are the
// same bytes. For a blank-padded character(n) they are not: PostgreSQL
// calls 'a' and 'a   ' equal, and the two hash to different shards. A table
// created through pgshard is refused at CREATE TABLE, and a table moved
// into place is refused by its placement workflow -- but a table that was
// already on the shards and is merely declared sharded in pgshard.tables
// passed through neither.
type ShardKeyCheck struct {
	Pool   *pgxpool.Pool
	Shards ShardDBDialer
	Logger *slog.Logger
}

func (c *ShardKeyCheck) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

// Run re-checks on a ticker while this process is the leader.
func (c *ShardKeyCheck) Run(ctx context.Context, interval time.Duration, leader func() bool) {
	runLoop(ctx, interval, leader, c.logger, "shard key check", func(ctx context.Context) {
		if _, err := c.Pass(ctx); err != nil {
			c.logger().Warn("shard key check pass failed", "err", err)
		}
	})
}

type uncheckedKey struct {
	Database   string
	SchemaName string
	TableName  string
	ShardKey   string
	Generation int64
}

// Pass checks every sharded table whose recorded check is older than its
// desired generation, and returns how many it published.
func (c *ShardKeyCheck) Pass(ctx context.Context) (int, error) {
	rows, err := c.Pool.Query(ctx, `
		SELECT t.database, t.schema_name, t.table_name, t.shard_key, t.desired_generation
		FROM pgshard.tables t
		LEFT JOIN pgshard.table_status s
		  ON s.database = t.database AND s.schema_name = t.schema_name AND s.table_name = t.table_name
		WHERE t.placement = 'sharded'
		  AND s.shard_key_checked_generation IS DISTINCT FROM t.desired_generation
		ORDER BY t.database, t.schema_name, t.table_name`)
	if err != nil {
		return 0, err
	}
	pending, err := pgx.CollectRows(rows, pgx.RowToStructByPos[uncheckedKey])
	if err != nil {
		return 0, err
	}
	if len(pending) == 0 {
		return 0, nil
	}
	var set string
	if err := c.Pool.QueryRow(ctx, `SELECT shard_set FROM pgshard.shard_sets WHERE state = $1 ORDER BY generation DESC LIMIT 1`, catalog.ShardSetServing).Scan(&set); err != nil {
		return 0, fmt.Errorf("serving shard set: %w", err)
	}
	ranges, err := catalog.ListShardRanges(ctx, c.Pool, set)
	if err != nil {
		return 0, err
	}
	published := 0
	for _, t := range pending {
		refusal, typ, err := c.inspect(ctx, set, ranges, t)
		if err != nil {
			// An unreachable shard must not publish a verdict for a table
			// nobody looked at. The row stays unchecked, which holds the
			// table out of the routing map, and the next pass tries again.
			c.logger().Warn("shard key not checked", "database", t.Database,
				"schema", t.SchemaName, "table", t.TableName, "err", err)
			continue
		}
		if err := c.publish(ctx, t, refusal, typ); err != nil {
			return published, err
		}
		if refusal != nil {
			c.logger().Warn("sharded table cannot be routed by its key", "database", t.Database,
				"schema", t.SchemaName, "table", t.TableName, "err", *refusal)
		}
		published++
	}
	return published, nil
}

func (c *ShardKeyCheck) publish(ctx context.Context, t uncheckedKey, refusal *string, typ *string) error {
	_, err := c.Pool.Exec(ctx, `
		INSERT INTO pgshard.table_status (database, schema_name, table_name,
			shard_key_checked_generation, shard_key_error, shard_key_type)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (database, schema_name, table_name) DO UPDATE
		SET shard_key_checked_generation = EXCLUDED.shard_key_checked_generation,
		    shard_key_error = EXCLUDED.shard_key_error,
		    shard_key_type = EXCLUDED.shard_key_type, updated_at = now()`,
		t.Database, t.SchemaName, t.TableName, t.Generation, refusal, typ)
	return err
}

// inspect asks every shard rather than one. The copies are meant to be
// identical, and a shard whose column type has drifted is exactly the case
// that would otherwise surface as rows on the wrong shard.
//
// A table no shard has yet is not a refusal: placement may be declared
// before the table exists, and the CREATE TABLE that follows goes through
// pgshard, which checks the type itself.
func (c *ShardKeyCheck) inspect(ctx context.Context, set string, ranges []catalog.ShardRange, t uncheckedKey) (*string, *string, error) {
	var agreed string
	var agreedShard int32
	for _, rg := range ranges {
		conn, err := c.Shards.DialDatabase(ctx, set, rg.ShardID, t.Database)
		if err != nil {
			return nil, nil, fmt.Errorf("shard %s/%d: %w", set, rg.ShardID, err)
		}
		typ, err := shardKeyType(ctx, conn, t.SchemaName, t.TableName, t.ShardKey)
		_ = conn.Close(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("shard %s/%d: %w", set, rg.ShardID, err)
		}
		if typ == "" {
			continue
		}
		if _, err := KeyHashExpr(t.ShardKey, typ); err != nil {
			msg := err.Error()
			return &msg, nil, nil
		}
		// Every shard passing on its own is not the same as the shards
		// agreeing. The router normalises by one recorded type, so a column
		// that is varchar(8) here and varchar(16) there truncates
		// differently depending on which shard the value came to rest on --
		// the drift this pass exists to catch.
		if agreed == "" {
			agreed, agreedShard = typ, rg.ShardID
			continue
		}
		if typ != agreed {
			msg := fmt.Sprintf("shard key %s has type %s on shard %d and %s on shard %d: the shards must agree before rows can be routed by it",
				t.ShardKey, agreed, agreedShard, typ, rg.ShardID)
			return &msg, nil, nil
		}
	}
	if agreed == "" {
		return nil, nil, nil
	}
	return nil, &agreed, nil
}

// shardKeyType returns the key column's SQL type on conn, or "" when the
// table or the column is not there.
func shardKeyType(ctx context.Context, conn ShardConn, schema, table, key string) (string, error) {
	rows, err := conn.Query(ctx, `
		SELECT pg_catalog.format_type(a.atttypid, a.atttypmod)
		FROM pg_attribute a
		WHERE a.attrelid = pg_catalog.to_regclass(pg_catalog.quote_ident($1) || '.' || pg_catalog.quote_ident($2))
		  AND a.attname = $3 AND a.attnum > 0 AND NOT a.attisdropped`, schema, table, key)
	if err != nil {
		return "", err
	}
	typ, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[string])
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return typ, err
}
