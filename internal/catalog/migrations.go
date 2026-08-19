package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Migration states in pgshard.migrations.
const (
	MigrationQueued   = "queued"
	MigrationRunning  = "running"
	MigrationComplete = "complete"
	MigrationFailed   = "failed"
)

// Per-shard migration states inside migrations.per_shard.
const (
	ShardPending  = "pending"
	ShardRunning  = "running"
	ShardRetrying = "retrying"
	ShardApplied  = "applied"
	ShardSkipped  = "skipped"
	ShardFailed   = "failed"
)

// MigrationObject names the object a migration creates or drops, so an
// applier resuming a shard step can tell whether it already ran.
type MigrationObject struct {
	// Kind is "relation", "schema", "type", "role" or "database".
	Kind   string `json:"kind,omitempty"`
	Schema string `json:"schema,omitempty"`
	Name   string `json:"name,omitempty"`
	// Expect is "present" or "absent".
	Expect string `json:"expect,omitempty"`
}

// MigrationMeta is the JSON of migrations.meta.
type MigrationMeta struct {
	Object MigrationObject `json:"object,omitempty"`
	// RunAs is the role the statement runs as on every shard (SET ROLE), so
	// ownership and privilege checks are the client's.
	RunAs string `json:"run_as,omitempty"`
	// Role, RoleOp ("create", "alter", "drop") and Verifier mirror role DDL
	// into pgshard.roles.
	Role     string `json:"role,omitempty"`
	RoleOp   string `json:"role_op,omitempty"`
	Verifier string `json:"verifier,omitempty"`
	// Database and DatabaseOp mirror CREATE/DROP DATABASE into
	// pgshard.databases.
	Database   string `json:"database,omitempty"`
	DatabaseOp string `json:"database_op,omitempty"`
}

// ShardMigration is one shard's entry in migrations.per_shard.
type ShardMigration struct {
	State    string `json:"state"`
	Attempts int    `json:"attempts,omitempty"`
	Error    string `json:"error,omitempty"`
	SQLState string `json:"sqlstate,omitempty"`
}

// DDLMigration is a row of pgshard.migrations.
type DDLMigration struct {
	ID        string
	Database  string
	Statement string
	Kind      string
	Strategy  string
	Scope     string
	HomeShard int32
	State     string
	Meta      MigrationMeta
	PerShard  map[string]ShardMigration
	Error     string
	CreatedAt time.Time
}

// RowQuerier is satisfied by *pgx.Conn, pgx.Tx and *pgxpool.Pool.
type RowQuerier interface {
	Querier
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// EnqueueMigration inserts m as queued and returns its id.
func EnqueueMigration(ctx context.Context, db RowQuerier, m DDLMigration) (string, error) {
	meta, err := json.Marshal(m.Meta)
	if err != nil {
		return "", err
	}
	var id string
	err = db.QueryRow(ctx, `INSERT INTO pgshard.migrations (id, database, statement, kind, strategy, scope, home_shard, meta, state)
		VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, 'queued') RETURNING id::text`,
		m.Database, m.Statement, m.Kind, m.Strategy, m.Scope, m.HomeShard, meta).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("catalog: enqueue migration: %w", err)
	}
	return id, nil
}

const migrationColumns = `id::text, database, statement, kind, strategy, scope, home_shard, state, meta, per_shard, coalesce(error, ''), created_at`

// LoadMigration reads one migration by id.
func LoadMigration(ctx context.Context, db RowQuerier, id string) (DDLMigration, error) {
	rows, err := db.Query(ctx, `SELECT `+migrationColumns+` FROM pgshard.migrations WHERE id = $1`, id)
	if err != nil {
		return DDLMigration{}, err
	}
	ms, err := collectMigrations(rows)
	if err != nil {
		return DDLMigration{}, err
	}
	if len(ms) == 0 {
		return DDLMigration{}, fmt.Errorf("catalog: migration %s: %w", id, pgx.ErrNoRows)
	}
	return ms[0], nil
}

// PendingMigrations lists queued and running migrations oldest first.
func PendingMigrations(ctx context.Context, db Querier) ([]DDLMigration, error) {
	rows, err := db.Query(ctx, `SELECT `+migrationColumns+` FROM pgshard.migrations WHERE state IN ('queued', 'running') ORDER BY created_at, id`)
	if err != nil {
		return nil, err
	}
	return collectMigrations(rows)
}

func collectMigrations(rows pgx.Rows) ([]DDLMigration, error) {
	defer rows.Close()
	var out []DDLMigration
	for rows.Next() {
		var m DDLMigration
		var meta, perShard []byte
		if err := rows.Scan(&m.ID, &m.Database, &m.Statement, &m.Kind, &m.Strategy, &m.Scope, &m.HomeShard, &m.State, &meta, &perShard, &m.Error, &m.CreatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(meta, &m.Meta); err != nil {
			return nil, fmt.Errorf("catalog: migration %s meta: %w", m.ID, err)
		}
		m.PerShard = map[string]ShardMigration{}
		if err := json.Unmarshal(perShard, &m.PerShard); err != nil {
			return nil, fmt.Errorf("catalog: migration %s per_shard: %w", m.ID, err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// SaveMigrationProgress writes the state, per-shard detail and error of m.
func SaveMigrationProgress(ctx context.Context, db RowQuerier, m DDLMigration) error {
	perShard, err := json.Marshal(m.PerShard)
	if err != nil {
		return err
	}
	var errText *string
	if m.Error != "" {
		errText = &m.Error
	}
	rows, err := db.Query(ctx, `UPDATE pgshard.migrations SET state = $2, per_shard = $3, error = $4, updated_at = now(),
		finished_at = CASE WHEN $2 IN ('complete', 'failed') THEN now() ELSE finished_at END WHERE id = $1`, m.ID, m.State, perShard, errText)
	if err != nil {
		return err
	}
	rows.Close()
	return rows.Err()
}
