package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// GuardSchema holds the per-table check functions. Unlike OwnerSchema it is
// copied with a schema: the triggers that call these functions travel in a
// target's schema dump, and a trigger restores only once its function has.
const GuardSchema = "pgshard_guard"

// GuardTrigger is the name of the check on every sharded table.
const GuardTrigger = "pgshard_owns_row"

// guardFunction names a table's check function: one per table, since the
// key column and its hash differ, and hashed so that no schema and table
// name, however long, overflows an identifier.
func guardFunction(schema, table string) string {
	sum := sha256.Sum256([]byte(schema + "." + table))
	return "owns_" + hex.EncodeToString(sum[:])[:16]
}

// guardFunctionSQL renders the check for one sharded table (PGS-878). It
// runs AFTER the row is written, so it sees the row every BEFORE trigger has
// finished with, a user's that rewrote the key included, and refuses one
// whose key hashes outside the range this shard owns. An UPDATE that leaves
// the key alone is not rehashed: the row was placed when its key was last
// written, and only a new key can move it.
//
// It reads the range at run time from OwnerSchema, which is per shard and
// never copied. A shard with no range yet refuses the write rather than
// guessing; OwnedRanges writes every serving and provisioning shard's range.
//
// SECURITY DEFINER with a fixed search_path: the writing role needs no
// privilege on OwnerSchema, and cannot redirect what the check resolves.
func guardFunctionSQL(schema, table, key, typ string) (string, error) {
	hash, err := keyHashOf("NEW."+QuoteIdent(key), key, typ)
	if err != nil {
		return "", err
	}
	col := QuoteIdent(key)
	return fmt.Sprintf(`CREATE OR REPLACE FUNCTION %[1]s.%[2]s() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $pgshard_guard$
DECLARE
	owned record;
	h bigint;
BEGIN
	IF TG_OP = 'UPDATE' AND OLD.%[3]s IS NOT DISTINCT FROM NEW.%[3]s THEN
		RETURN NULL;
	END IF;
	-- Checked before it is read: the table is created on a shard with the
	-- range in it, and the schema copy never carries it, so a shard that
	-- has not been told its range has no table at all -- which would
	-- otherwise surface as the unhelpful 42P01.
	IF pg_catalog.to_regclass('%[4]s.%[5]s') IS NULL THEN
		owned := NULL;
	ELSE
		SELECT lo, hi INTO owned FROM %[4]s.%[5]s;
	END IF;
	IF owned IS NULL THEN
		RAISE EXCEPTION 'this shard does not know which keyspace range it owns yet, so a row of %%.%% cannot be checked', TG_TABLE_SCHEMA, TG_TABLE_NAME
			USING ERRCODE = '55000', HINT = 'retry shortly; pgshard records each shard''s range in %[4]s';
	END IF;
	h := %[6]s;
	IF h < owned.lo OR h > owned.hi THEN
		RAISE EXCEPTION 'row of %%.%% has shard key %%, which belongs to another shard', TG_TABLE_SCHEMA, TG_TABLE_NAME, NEW.%[3]s
			USING ERRCODE = '23514',
			DETAIL = 'the key hashes outside the range this shard owns, so no lookup of it would reach this shard',
			HINT = 'a BEFORE trigger that assigns the shard key, or a write made on a shard directly, puts rows where they cannot be found; set the shard key in the statement and send it through pgshard';
	END IF;
	RETURN NULL;
END
$pgshard_guard$`, QuoteIdent(GuardSchema), QuoteIdent(guardFunction(schema, table)), col, QuoteIdent(OwnerSchema), QuoteIdent(OwnerTable), hash), nil
}

// OwnerGuard installs the check on every shard of every effectively sharded
// table, once the shard key check has recorded the key's type for the
// table's current generation, and removes it from a table that is no longer
// sharded. owner_guard_generation records the generation installed.
type OwnerGuard struct {
	Pool   *pgxpool.Pool
	Shards ShardDBDialer
	Logger *slog.Logger
}

func (g *OwnerGuard) logger() *slog.Logger {
	if g.Logger != nil {
		return g.Logger
	}
	return slog.Default()
}

// Run installs and removes on a ticker while this process is the leader.
func (g *OwnerGuard) Run(ctx context.Context, interval time.Duration, leader func() bool) {
	runLoopStoppable(ctx, interval, leader, g.logger, "owner guard", func(ctx context.Context) {
		if _, err := g.Pass(ctx); err != nil {
			g.logger().Warn("owner guard pass failed", "err", err)
		}
	})
}

type guardedTable struct {
	Database   string
	SchemaName string
	TableName  string
	ShardKey   *string
	KeyType    *string
	Generation int64
	Sharded    bool
}

// Pass brings every table's check in line with its placement and returns how
// many tables it changed. A table some shard does not have yet, or cannot be
// reached on, is left for the next pass, and holds no other table back.
func (g *OwnerGuard) Pass(ctx context.Context) (int, error) {
	rows, err := g.Pool.Query(ctx, `
		SELECT database, schema_name, table_name, effective_shard_key, shard_key_type, effective_generation,
		       effective_placement IS NOT DISTINCT FROM 'sharded'
		FROM pgshard.table_status
		WHERE (effective_placement = 'sharded' AND effective_shard_key IS NOT NULL AND shard_key_type IS NOT NULL
		       AND shard_key_checked_generation = effective_generation AND shard_key_error IS NULL
		       AND owner_guard_generation IS DISTINCT FROM effective_generation)
		   OR (effective_placement IS DISTINCT FROM 'sharded' AND owner_guard_generation IS NOT NULL)
		ORDER BY database, schema_name, table_name`)
	if err != nil {
		return 0, err
	}
	pending, err := pgx.CollectRows(rows, pgx.RowToStructByPos[guardedTable])
	if err != nil {
		return 0, err
	}
	if len(pending) == 0 {
		return 0, nil
	}
	var set string
	if err := g.Pool.QueryRow(ctx, `SELECT shard_set FROM pgshard.shard_sets WHERE state = $1 ORDER BY generation DESC LIMIT 1`, catalog.ShardSetServing).Scan(&set); err != nil {
		return 0, fmt.Errorf("serving shard set: %w", err)
	}
	ranges, err := catalog.ListShardRanges(ctx, g.Pool, set)
	if err != nil {
		return 0, err
	}
	changed := 0
	var firstErr error
	for _, t := range pending {
		done, err := g.apply(ctx, set, ranges, t)
		if err != nil {
			g.logger().Warn("owner guard not applied", "database", t.Database, "table", t.SchemaName+"."+t.TableName, "err", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if !done {
			continue
		}
		var gen *int64
		if t.Sharded {
			gen = &t.Generation
		}
		// Compare-and-set on the generation read: a re-key that landed
		// while this pass ran has a check to install that this one is not.
		tag, err := g.Pool.Exec(ctx, `UPDATE pgshard.table_status SET owner_guard_generation = $4
			WHERE database = $1 AND schema_name = $2 AND table_name = $3 AND effective_generation = $5`,
			t.Database, t.SchemaName, t.TableName, gen, t.Generation)
		if err != nil {
			return changed, err
		}
		changed += int(tag.RowsAffected())
	}
	return changed, firstErr
}

// apply installs or removes one table's check on every shard, and reports
// whether every shard now agrees with its placement.
func (g *OwnerGuard) apply(ctx context.Context, set string, ranges []catalog.ShardRange, t guardedTable) (bool, error) {
	var fn string
	if t.Sharded {
		var err error
		if fn, err = guardFunctionSQL(t.SchemaName, t.TableName, *t.ShardKey, *t.KeyType); err != nil {
			return false, err
		}
	}
	owned := catalog.RangeSet(ranges)
	all := true
	for i, rg := range ranges {
		// The shard's range first: a check installed before the shard knows
		// what it owns refuses every write to the table until the owned
		// range pass reaches it, and that pass runs on its own timer.
		if t.Sharded {
			if _, err := writeOwnedRange(ctx, g.Shards, set, rg.ShardID, t.Database, owned[i].Start, owned[i].End); err != nil {
				return false, fmt.Errorf("shard %s/%d %s: owned range: %w", set, rg.ShardID, t.Database, err)
			}
		}
		ok, err := g.applyOn(ctx, set, rg.ShardID, t, fn)
		if err != nil {
			return false, fmt.Errorf("shard %s/%d %s.%s.%s: %w", set, rg.ShardID, t.Database, t.SchemaName, t.TableName, err)
		}
		all = all && ok
	}
	return all, nil
}

// applyOn does one shard, in one transaction, and reports false when the
// table is not there yet.
func (g *OwnerGuard) applyOn(ctx context.Context, set string, id int32, t guardedTable, fn string) (bool, error) {
	conn, err := g.Shards.DialDatabase(ctx, set, id, t.Database)
	if err != nil {
		return false, err
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, `SELECT pg_catalog.to_regclass(pg_catalog.quote_ident($1) || '.' || pg_catalog.quote_ident($2)) IS NOT NULL`, t.SchemaName, t.TableName)
	if err != nil {
		return false, err
	}
	exists, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[bool])
	if err != nil {
		return false, err
	}
	qualified := QuoteIdent(t.SchemaName) + "." + QuoteIdent(t.TableName)
	if !exists {
		// Nothing to guard, and nothing to remove. A sharded table no shard
		// has yet is not done: it will be created and must be guarded then.
		return !t.Sharded, nil
	}
	stmts := []string{"BEGIN", "DROP TRIGGER IF EXISTS " + GuardTrigger + " ON " + qualified}
	if t.Sharded {
		stmts = append(stmts,
			"CREATE SCHEMA IF NOT EXISTS "+QuoteIdent(GuardSchema),
			fn,
			"CREATE TRIGGER "+GuardTrigger+" AFTER INSERT OR UPDATE ON "+qualified+" FOR EACH ROW EXECUTE FUNCTION "+
				QuoteIdent(GuardSchema)+"."+QuoteIdent(guardFunction(t.SchemaName, t.TableName))+"()")
	} else {
		stmts = append(stmts, "DROP FUNCTION IF EXISTS "+QuoteIdent(GuardSchema)+"."+QuoteIdent(guardFunction(t.SchemaName, t.TableName))+"()")
	}
	stmts = append(stmts, "COMMIT")
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			_, _ = conn.Exec(ctx, "ROLLBACK")
			return false, err
		}
	}
	return true, nil
}
