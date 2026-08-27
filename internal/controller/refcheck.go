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

// deterministicNodes are the expression node types a reference table's
// default or generated expression may be built from. It is an allow list
// because the question is not "which nodes are known bad" but "which are
// proven to produce the same value on every shard": a node type this
// version of pgshard has never seen -- one PostgreSQL adds in a later
// major -- has to count as unsafe, not as fine by omission.
//
// SQLVALUEFUNCTION is the reason the list cannot be replaced by a
// volatility check alone: CURRENT_TIMESTAMP, CURRENT_DATE and CURRENT_USER
// parse to that node and carry no function OID at all, so nothing in
// pg_proc describes them.
var deterministicNodes = []string{
	"CONST", "VAR", "OPEXPR", "FUNCEXPR", "RELABELTYPE", "COERCEVIAIO",
	"ARRAYCOERCEEXPR", "ARRAYEXPR", "ROWEXPR", "COALESCEEXPR", "CASEEXPR",
	"CASEWHEN", "CASETESTEXPR", "BOOLEXPR", "NULLTEST", "BOOLEANTEST",
	"SCALARARRAYOPEXPR", "MINMAXEXPR", "COERCETODOMAIN", "COERCETODOMAINVALUE",
	"COLLATEEXPR", "FIELDSELECT", "FIELDSTORE", "SUBSCRIPTINGREF",
	"CONVERTROWTYPEEXPR", "NAMEDARGEXPR",
}

// referenceHazardSQL lists everything about one table that a shard would
// evaluate for itself. Volatility is read from pg_proc for the exact OID
// the stored expression tree resolved to, never matched by name: an
// overload, a function in another schema, and a cast the analyser inserted
// are all invisible to a name, and all of them are in the tree.
const referenceHazardSQL = `
WITH deflt AS (
	SELECT a.attname, a.attgenerated, ad.adbin
	FROM pg_attribute a JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
	WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped
), nodes AS (
	SELECT d.attname, d.attgenerated, m[1] AS label
	FROM deflt d CROSS JOIN LATERAL regexp_matches(d.adbin, '\{([A-Z]+)', 'g') m
), funcs AS (
	SELECT d.attname, d.attgenerated, p.proname, p.provolatile
	FROM deflt d CROSS JOIN LATERAL regexp_matches(d.adbin, ':(?:funcid|opfuncid|aggfnoid|winfnoid) (\d+)', 'g') m
	JOIN pg_proc p ON p.oid = m[1]::oid
)
SELECT DISTINCT h FROM (
	SELECT format('column %s is an identity column', quote_ident(attname)) AS h
	FROM pg_attribute WHERE attrelid = $1::regclass AND attidentity <> '' AND NOT attisdropped
	UNION ALL
	SELECT format('the %s of column %s uses %s, which is not a node proven deterministic',
		CASE WHEN attgenerated <> '' THEN 'generated expression' ELSE 'default' END,
		quote_ident(attname), label)
	FROM nodes WHERE label <> ALL ($2::text[])
	UNION ALL
	SELECT format('the %s of column %s calls %s(), which pg_proc marks %s',
		CASE WHEN attgenerated <> '' THEN 'generated expression' ELSE 'default' END,
		quote_ident(attname), proname,
		CASE provolatile WHEN 'v' THEN 'VOLATILE' ELSE 'STABLE' END)
	FROM funcs WHERE provolatile <> 'i'
	UNION ALL
	SELECT format('trigger %s fires on writes', quote_ident(tgname))
	FROM pg_trigger WHERE tgrelid = $1::regclass AND NOT tgisinternal
	UNION ALL
	SELECT format('rule %s rewrites writes', quote_ident(rulename))
	FROM pg_rewrite WHERE ev_class = $1::regclass AND rulename <> '_RETURN'
) x ORDER BY h`

// ReferenceCheck inspects each reference table on the shards and records
// what would make its replicas diverge. A write to a reference table runs
// on every shard, and 2PC makes them all commit -- not commit the same
// thing. The router plans from the statement alone and cannot see a
// default, a generated expression, a trigger or a rule, so the shards have
// to be asked and the answer published for routers to plan against.
type ReferenceCheck struct {
	Pool   *pgxpool.Pool
	Shards ShardDBDialer
	Logger *slog.Logger
}

func (r *ReferenceCheck) logger() *slog.Logger {
	if r.Logger != nil {
		return r.Logger
	}
	return slog.Default()
}

// Run re-inspects on a ticker while this process is the leader.
func (r *ReferenceCheck) Run(ctx context.Context, interval time.Duration, leader func() bool) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if leader != nil && !leader() {
			continue
		}
		if _, err := r.Pass(ctx); err != nil {
			r.logger().Warn("reference check pass failed", "err", err)
		}
	}
}

type referenceTable struct {
	Database   string
	SchemaName string
	TableName  string
	Generation int64
}

// Pass inspects every reference table whose recorded inspection is older
// than its effective generation, and returns how many it published.
func (r *ReferenceCheck) Pass(ctx context.Context) (int, error) {
	rows, err := r.Pool.Query(ctx, `
		SELECT database, schema_name, table_name, effective_generation
		FROM pgshard.table_status
		WHERE effective_placement = 'reference'
		  AND reference_checked_generation IS DISTINCT FROM effective_generation
		ORDER BY database, schema_name, table_name`)
	if err != nil {
		return 0, err
	}
	pending, err := pgx.CollectRows(rows, pgx.RowToStructByPos[referenceTable])
	if err != nil {
		return 0, err
	}
	if len(pending) == 0 {
		return 0, nil
	}
	var set string
	if err := r.Pool.QueryRow(ctx, `SELECT shard_set FROM pgshard.shard_sets WHERE state = $1 ORDER BY generation DESC LIMIT 1`, catalog.ShardSetServing).Scan(&set); err != nil {
		return 0, fmt.Errorf("serving shard set: %w", err)
	}
	ranges, err := catalog.ListShardRanges(ctx, r.Pool, set)
	if err != nil {
		return 0, err
	}
	published := 0
	for _, t := range pending {
		hazards, err := r.inspect(ctx, set, ranges, t)
		if err != nil {
			// One unreachable shard must not publish a clean result for a
			// table it never looked at; the row stays unchecked, which
			// routers already refuse, and the next pass tries again.
			r.logger().Warn("reference table not inspected", "database", t.Database,
				"schema", t.SchemaName, "table", t.TableName, "err", err)
			continue
		}
		if _, err := r.Pool.Exec(ctx, `
			UPDATE pgshard.table_status
			SET reference_hazards = $5, reference_checked_generation = $4, updated_at = now()
			WHERE database = $1 AND schema_name = $2 AND table_name = $3 AND effective_generation = $4`,
			t.Database, t.SchemaName, t.TableName, t.Generation, hazards); err != nil {
			return published, err
		}
		if len(hazards) > 0 {
			r.logger().Warn("reference table cannot be written safely", "database", t.Database,
				"schema", t.SchemaName, "table", t.TableName, "hazards", hazards)
		}
		published++
	}
	return published, nil
}

// inspect asks every shard, not just one. The tables are meant to be
// identical, and a shard whose copy has drifted -- a trigger left behind by
// a half-applied migration -- is exactly the case that would otherwise go
// unnoticed until the rows disagreed.
func (r *ReferenceCheck) inspect(ctx context.Context, set string, ranges []catalog.ShardRange, t referenceTable) ([]string, error) {
	hazards := []string{}
	for _, rg := range ranges {
		conn, err := r.Shards.DialDatabase(ctx, set, rg.ShardID, t.Database)
		if err != nil {
			return nil, fmt.Errorf("shard %s/%d: %w", set, rg.ShardID, err)
		}
		found, err := referenceHazards(ctx, conn, t.SchemaName, t.TableName)
		_ = conn.Close(ctx)
		if err != nil {
			return nil, fmt.Errorf("shard %s/%d: %w", set, rg.ShardID, err)
		}
		for _, h := range found {
			if !slices.Contains(hazards, h) {
				hazards = append(hazards, h)
			}
		}
	}
	slices.Sort(hazards)
	return hazards, nil
}

// referenceHazards runs the inspection on one shard. A table that is not
// there yet has nothing to report: the DDL has not reached this shard, and
// the generation it is checked under will not be published until it has.
func referenceHazards(ctx context.Context, conn ShardConn, schema, table string) ([]string, error) {
	qualified := QuoteIdent(schema) + "." + QuoteIdent(table)
	there, err := present(ctx, conn, qualified)
	if err != nil || !there {
		return nil, err
	}
	rows, err := conn.Query(ctx, referenceHazardSQL, qualified, deterministicNodes)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}
