// Package pgsequence carries sequence positions between two PostgreSQL
// clusters. Neither logical replication nor a base backup taken before the
// last nextval brings a sequence's position with it, so every cutover that
// moves serving from one cluster to another has to tell the new one where
// its sequences got to.
package pgsequence

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/jackc/pgx/v5"
)

// Conn is the part of a PostgreSQL connection this package uses. setval
// returns a row, so a single Query is enough for both directions.
type Conn interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Value is where one sequence has got to and the direction it advances in.
// The direction decides which of two values is the further on, both when
// merging sources and when refusing to move a target backwards.
type Value struct {
	At        int64
	Ascending bool
}

// Snapshot reads every called sequence outside excludeSchemas, keyed by
// quoted qualified name, at a position safely ahead of the one on disk.
//
// The headroom is what makes the position safe to hand to another cluster:
// a session may hold up to cache_size values and PostgreSQL pre-logs 32
// (SEQ_LOG_VALS) per fetch, so nextval calls that consume that headroom
// move neither pg_current_wal_lsn() nor the on-disk value. Without the
// bump, a writer that reached the old cluster between the snapshot and the
// flip could hand the new one values it will hand out again. The headroom
// is signed (greatest(cache_size, 32) * increment_by), computed in numeric
// so it cannot overflow bigint, and clamped to max_value (ascending) or
// min_value (descending) before the cast back. A CYCLE sequence clamps at
// its boundary rather than simulating the wrap: the carried value is never
// out of range, and values reused past a wrap are inherent to CYCLE, not
// introduced here.
func Snapshot(ctx context.Context, conn Conn, excludeSchemas []string) (map[string]Value, error) {
	rows, err := conn.Query(ctx, `SELECT quote_ident(schemaname) || '.' || quote_ident(sequencename),
			(CASE WHEN increment_by > 0
				THEN least(last_value::numeric + greatest(cache_size, 32)::numeric * increment_by::numeric, max_value::numeric)
				ELSE greatest(last_value::numeric + greatest(cache_size, 32)::numeric * increment_by::numeric, min_value::numeric)
			END)::bigint,
			increment_by > 0
		FROM pg_sequences WHERE last_value IS NOT NULL AND NOT (schemaname = ANY(coalesce($1::text[], '{}'))) ORDER BY 1`, excludeSchemas)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := map[string]Value{}
	for rows.Next() {
		var name string
		var v Value
		if err := rows.Scan(&name, &v.At, &v.Ascending); err != nil {
			return nil, err
		}
		values[name] = v
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return values, nil
}

// Merge folds from into into, keeping per sequence the value furthest on in
// its own direction. Several sources may hold the same sequence -- one per
// shard of the set being retired -- and only the furthest is safe.
func Merge(into, from map[string]Value) {
	for name, v := range from {
		if prev, ok := into[name]; ok {
			if v.Ascending {
				v.At = max(prev.At, v.At)
			} else {
				v.At = min(prev.At, v.At)
			}
		}
		into[name] = v
	}
}

// Apply moves each sequence conn holds to its carried value, never
// backwards, in name order so two runs of the same carry issue the same
// statements. A sequence the target does not have is skipped: it was never
// materialized there.
//
// Never backwards is not caution. A carry runs again at the swap, by which
// point the targets are serving and may have advanced past the sources on
// their own; an unconditional setval would hand those values out twice.
func Apply(ctx context.Context, conn Conn, values map[string]Value) error {
	const sql = `SELECT pg_catalog.setval(c.oid,
		CASE WHEN $3::bool
			THEN greatest(coalesce(pg_catalog.pg_sequence_last_value(c.oid), $2::bigint), $2::bigint)
			ELSE least(coalesce(pg_catalog.pg_sequence_last_value(c.oid), $2::bigint), $2::bigint)
		END, true)
		FROM pg_class c WHERE c.oid = to_regclass($1) AND c.relkind = 'S'`
	for _, name := range slices.Sorted(maps.Keys(values)) {
		rows, err := conn.Query(ctx, sql, name, values[name].At, values[name].Ascending)
		if err != nil {
			return fmt.Errorf("carry sequence %s: %w", name, err)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("carry sequence %s: %w", name, err)
		}
	}
	return nil
}
