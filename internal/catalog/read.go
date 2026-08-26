package catalog

import (
	"context"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Querier is satisfied by *pgx.Conn, pgx.Tx and *pgxpool.Pool.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Execer is satisfied by *pgx.Conn, pgx.Tx and *pgxpool.Pool.
type Execer interface {
	Querier
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Database is a row of pgshard.databases.
type Database struct {
	Name              string
	DefaultPlacement  string
	HomeShard         int32
	DesiredGeneration int64
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// Table is a row of pgshard.tables.
type Table struct {
	Database          string
	SchemaName        string
	TableName         string
	Placement         string
	ShardKey          *string
	HashVersion       int32
	DesiredGeneration int64
	UpdatedAt         time.Time
	// SequenceColumns are the columns the router fills from
	// pgshard.sequences on INSERT; nil when the table has none.
	SequenceColumns []string
}

// ShardRange is a row of pgshard.shard_ranges. Lower is inclusive, Upper is
// exclusive; a nil bound means the range is unbounded on that side.
type ShardRange struct {
	ShardSet          string
	ShardID           int32
	Lower             *int64
	Upper             *int64
	DesiredGeneration int64
	UpdatedAt         time.Time
}

// TableStatus is a row of pgshard.table_status.
type TableStatus struct {
	Database            string
	SchemaName          string
	TableName           string
	EffectivePlacement  *string
	EffectiveShardKey   *string
	EffectiveGeneration int64
	WorkflowID          *string
	Progress            []byte
	UpdatedAt           time.Time
	// Migrating marks a table whose writes routers hold while a placement
	// workflow swaps its shadow tables in.
	Migrating bool
}

// ShardStatus is a row of pgshard.shard_status.
type ShardStatus struct {
	ShardSet        string
	ShardID         int32
	GroupName       string
	ServingState    string
	PrimaryEpoch    int64
	PrimaryEndpoint *string
	ReplayLagBytes  *int64
	UpdatedAt       time.Time
	// Migrating marks a source shard whose ranges are write-fenced by a
	// reshard cutover.
	Migrating bool
}

// ListDatabases returns every desired database ordered by name.
func ListDatabases(ctx context.Context, q Querier) ([]Database, error) {
	rows, err := q.Query(ctx, `
		SELECT name, default_placement, home_shard, desired_generation, created_at, updated_at
		FROM pgshard.databases ORDER BY name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[Database])
}

// ListTables returns the desired tables of one database.
func ListTables(ctx context.Context, q Querier, database string) ([]Table, error) {
	rows, err := q.Query(ctx, `
		SELECT database, schema_name, table_name, placement, shard_key, hash_version, desired_generation, updated_at, sequence_columns
		FROM pgshard.tables WHERE database = $1 ORDER BY schema_name, table_name`, database)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[Table])
}

// ShardRangeLockTimeout bounds how long a caller waits for the shard ranges it
// is about to snapshot. A reconcile pass covers every shard set in one
// transaction, so blocking here holds up work unrelated to the set being
// locked; failing the pass and retrying on the next tick is cheaper than
// waiting out whoever holds the rows.
const ShardRangeLockTimeout = "5s"

// LockShardRangesOf share-locks the ranges of the named shard sets until the
// transaction ends. A caller about to create a workflow that will own these
// sets needs this: without it a concurrent edit can commit between the read and
// the workflow insert, and the ownership trigger, which only sees the workflow
// once it is committed, would let that edit through and freeze the divergence
// in.
//
// The sets are locked in name order, so two callers of this function cannot
// deadlock against each other. That is not a guarantee against every writer: a
// transaction that edits two shard sets in the opposite order can still form a
// cycle, which PostgreSQL breaks by aborting one side. The caller retries on a
// later pass, so the invariant holds either way.
func LockShardRangesOf(ctx context.Context, tx pgx.Tx, shardSets ...string) error {
	names := slices.Clone(shardSets)
	slices.Sort(names)
	names = slices.Compact(names)
	if len(names) == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '`+ShardRangeLockTimeout+`'`); err != nil {
		return err
	}
	for _, name := range names {
		if name == "" {
			continue
		}
		if _, err := tx.Exec(ctx, `SELECT 1 FROM pgshard.shard_ranges WHERE shard_set = $1 ORDER BY range FOR SHARE`, name); err != nil {
			return err
		}
	}
	return nil
}

// ListShardRanges returns the ranges of one shard set ordered by key space.
func ListShardRanges(ctx context.Context, q Querier, shardSet string) ([]ShardRange, error) {
	rows, err := q.Query(ctx, `
		SELECT shard_set, shard_id,
		       CASE WHEN lower_inf(range) THEN NULL ELSE lower(range) END,
		       CASE WHEN upper_inf(range) THEN NULL ELSE upper(range) END,
		       desired_generation, updated_at
		FROM pgshard.shard_ranges WHERE shard_set = $1 ORDER BY range`, shardSet)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[ShardRange])
}

// ListTableStatus returns the observed status of every table in one database.
func ListTableStatus(ctx context.Context, q Querier, database string) ([]TableStatus, error) {
	rows, err := q.Query(ctx, `
		SELECT database, schema_name, table_name, effective_placement, effective_shard_key,
		       effective_generation, workflow_id::text, progress, updated_at, migrating
		FROM pgshard.table_status WHERE database = $1 ORDER BY schema_name, table_name`, database)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[TableStatus])
}

// ListShardStatus returns the observed status of every shard in one shard set.
func ListShardStatus(ctx context.Context, q Querier, shardSet string) ([]ShardStatus, error) {
	rows, err := q.Query(ctx, `
		SELECT shard_set, shard_id, group_name, serving_state, primary_epoch,
		       primary_endpoint, replay_lag_bytes, updated_at, migrating
		FROM pgshard.shard_status WHERE shard_set = $1 ORDER BY shard_id`, shardSet)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[ShardStatus])
}

// ListAllTables returns every desired table ordered by database, schema and name.
func ListAllTables(ctx context.Context, q Querier) ([]Table, error) {
	rows, err := q.Query(ctx, `
		SELECT database, schema_name, table_name, placement, shard_key, hash_version, desired_generation, updated_at, sequence_columns
		FROM pgshard.tables ORDER BY database, schema_name, table_name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[Table])
}

// ListSequenceNames returns the names of every global sequence.
func ListSequenceNames(ctx context.Context, q Querier) ([]string, error) {
	rows, err := q.Query(ctx, `SELECT name FROM pgshard.sequences ORDER BY name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// ListAllShardRanges returns the ranges of every shard set ordered by set and key space.
func ListAllShardRanges(ctx context.Context, q Querier) ([]ShardRange, error) {
	rows, err := q.Query(ctx, `
		SELECT shard_set, shard_id,
		       CASE WHEN lower_inf(range) THEN NULL ELSE lower(range) END,
		       CASE WHEN upper_inf(range) THEN NULL ELSE upper(range) END,
		       desired_generation, updated_at
		FROM pgshard.shard_ranges ORDER BY shard_set, range`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[ShardRange])
}

// ListAllTableStatus returns the observed status of every table.
func ListAllTableStatus(ctx context.Context, q Querier) ([]TableStatus, error) {
	rows, err := q.Query(ctx, `
		SELECT database, schema_name, table_name, effective_placement, effective_shard_key,
		       effective_generation, workflow_id::text, progress, updated_at, migrating
		FROM pgshard.table_status ORDER BY database, schema_name, table_name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[TableStatus])
}

// ListAllShardStatus returns the observed status of every shard.
func ListAllShardStatus(ctx context.Context, q Querier) ([]ShardStatus, error) {
	rows, err := q.Query(ctx, `
		SELECT shard_set, shard_id, group_name, serving_state, primary_epoch,
		       primary_endpoint, replay_lag_bytes, updated_at, migrating
		FROM pgshard.shard_status ORDER BY shard_set, shard_id`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[ShardStatus])
}

// Generations returns the shard-map generation and the highest desired
// generation stamped on any desired-state row.
func Generations(ctx context.Context, q Querier) (shardMap, desired int64, err error) {
	rows, err := q.Query(ctx, `
		SELECT (SELECT generation FROM pgshard.shard_map_generation),
		       (SELECT coalesce(max(g), 0) FROM (
		            SELECT max(desired_generation) g FROM pgshard.databases
		            UNION ALL SELECT max(desired_generation) FROM pgshard.tables
		            UNION ALL SELECT max(desired_generation) FROM pgshard.shard_ranges
		            UNION ALL SELECT max(desired_generation) FROM pgshard.roles
		            UNION ALL SELECT max(desired_generation) FROM pgshard.grants) m)`)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, 0, rows.Err()
	}
	if err := rows.Scan(&shardMap, &desired); err != nil {
		return 0, 0, err
	}
	return shardMap, desired, rows.Err()
}

// WriteFence is the cluster-wide write pause routers observe.
type WriteFence struct {
	Active   bool
	Reason   string
	FencedAt *time.Time
}

// ReadWriteFence returns the current write fence.
func ReadWriteFence(ctx context.Context, q Querier) (WriteFence, error) {
	rows, err := q.Query(ctx, `SELECT write_fence, write_fence_reason, write_fenced_at FROM pgshard.shard_map_generation`)
	if err != nil {
		return WriteFence{}, err
	}
	return pgx.CollectExactlyOneRow(rows, pgx.RowToStructByPos[WriteFence])
}

// SetWriteFence raises or releases the write fence; the change notifies
// routers through ServingChannel.
func SetWriteFence(ctx context.Context, q Execer, active bool, reason string) error {
	_, err := q.Exec(ctx, `UPDATE pgshard.shard_map_generation
		SET write_fence = $1, write_fence_reason = CASE WHEN $1 THEN $2 ELSE '' END,
		    write_fenced_at = CASE WHEN $1 THEN now() ELSE NULL END, updated_at = now()`, active, reason)
	return err
}

// RaiseWriteFence raises the write fence and stamps it with owner, the token
// that ReleaseWriteFence must present to clear it.
func RaiseWriteFence(ctx context.Context, q Execer, reason, owner string) error {
	_, err := q.Exec(ctx, `UPDATE pgshard.shard_map_generation
		SET write_fence = true, write_fence_reason = $1, write_fence_owner = $2,
		    write_fenced_at = now(), updated_at = now()`, reason, owner)
	return err
}

// ReleaseWriteFence clears the write fence only if owner still holds it, so a
// barrier that lost its advisory-lock session cannot clear a fence a later
// barrier has since raised. It returns whether it cleared the fence.
func ReleaseWriteFence(ctx context.Context, q Execer, owner string) (bool, error) {
	tag, err := q.Exec(ctx, `UPDATE pgshard.shard_map_generation
		SET write_fence = false, write_fence_reason = '', write_fence_owner = '',
		    write_fenced_at = NULL, updated_at = now()
		WHERE write_fence_owner = $1`, owner)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}
