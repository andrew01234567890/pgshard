package catalog

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// Querier is satisfied by *pgx.Conn, pgx.Tx and *pgxpool.Pool.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
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
		SELECT database, schema_name, table_name, placement, shard_key, hash_version, desired_generation, updated_at
		FROM pgshard.tables WHERE database = $1 ORDER BY schema_name, table_name`, database)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[Table])
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
		       effective_generation, workflow_id::text, progress, updated_at
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
		       primary_endpoint, replay_lag_bytes, updated_at
		FROM pgshard.shard_status WHERE shard_set = $1 ORDER BY shard_id`, shardSet)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[ShardStatus])
}
