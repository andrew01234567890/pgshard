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
	// Roles carries the desired-state delta of role, membership, grant and
	// setting statements.
	Roles *RoleChanges `json:"roles,omitempty"`
	// Steps is the ordered plan of a "multistep" migration; the applier
	// runs them per shard, each under its own lock_timeout retry.
	Steps []MigrationStep `json:"steps,omitempty"`
}

// MigrationStep is one statement of a multistep migration.
type MigrationStep struct {
	SQL string `json:"sql"`
	// Concurrent steps run outside a transaction (CONCURRENTLY forms).
	Concurrent bool `json:"concurrent,omitempty"`
	// Skip is the check under which the step is already done.
	Skip MigrationCheck `json:"skip,omitempty"`
	// Index names the index a CREATE INDEX CONCURRENTLY step builds; an
	// invalid leftover is dropped before the step runs and after it fails.
	Index string `json:"index,omitempty"`
	// OnFail runs after a hard failure of the step (drops the NOT VALID
	// constraint a failed VALIDATE leaves behind) so a re-run is clean.
	OnFail string `json:"on_fail,omitempty"`
}

// MigrationCheck is a catalog predicate on one shard.
type MigrationCheck struct {
	// Kind is "constraint" (exists), "constraint_valid", "notnull" (a
	// not-null constraint on the column exists), "notnull_valid",
	// "index_valid", "detached" (not a partition of Table
	// any more) or "detach_pending" (detached or its detach is pending).
	Kind   string `json:"kind,omitempty"`
	Schema string `json:"schema,omitempty"`
	Table  string `json:"table,omitempty"`
	// Name is the constraint, column, index or partition the check is on.
	Name string `json:"name,omitempty"`
}

// ShardMigration is one shard's entry in migrations.per_shard.
type ShardMigration struct {
	State    string `json:"state"`
	Attempts int    `json:"attempts,omitempty"`
	// Step is the index into meta.steps the shard is on (multistep).
	Step     int    `json:"step,omitempty"`
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
	// FinishedAt is set once the migration is complete or failed.
	FinishedAt *time.Time
}

// RowQuerier is satisfied by *pgx.Conn, pgx.Tx and *pgxpool.Pool.
type RowQuerier interface {
	Querier
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// StrategyMultistep is the strategy of a migration driven by meta.steps.
// The strategy column admits direct and concurrent only; a multistep
// migration is stored as direct with its steps in meta and read back as
// multistep, so the schema needs no change for it.
const StrategyMultistep = "multistep"

// EnqueueMigration inserts m as queued and returns its id.
func EnqueueMigration(ctx context.Context, db RowQuerier, m DDLMigration) (string, error) {
	meta, err := json.Marshal(m.Meta)
	if err != nil {
		return "", err
	}
	strategy := m.Strategy
	if strategy == StrategyMultistep {
		if len(m.Meta.Steps) == 0 {
			return "", fmt.Errorf("catalog: enqueue migration: a multistep migration needs steps")
		}
		strategy = "direct"
	}
	var id string
	err = db.QueryRow(ctx, `INSERT INTO pgshard.migrations (id, database, statement, kind, strategy, scope, home_shard, meta, state)
		VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, 'queued') RETURNING id::text`,
		m.Database, m.Statement, m.Kind, strategy, m.Scope, m.HomeShard, meta).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("catalog: enqueue migration: %w", err)
	}
	return id, nil
}

const migrationColumns = `id::text, database, statement, kind, strategy, scope, home_shard, state, meta, per_shard, coalesce(error, ''), created_at, finished_at`

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

// MigrationFilter narrows ListMigrations; empty fields match everything.
type MigrationFilter struct {
	Database string
	State    string
	Limit    int
	Offset   int
}

// ListMigrations returns the newest migrations matching f and the total count
// of matching rows, for paging.
func ListMigrations(ctx context.Context, db RowQuerier, f MigrationFilter) ([]DDLMigration, int, error) {
	where := `WHERE ($1 = '' OR database = $1) AND ($2 = '' OR state = $2)`
	var total int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM pgshard.migrations `+where, f.Database, f.State).Scan(&total); err != nil {
		return nil, 0, err
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	rows, err := db.Query(ctx, `SELECT `+migrationColumns+` FROM pgshard.migrations `+where+` ORDER BY created_at DESC, id DESC LIMIT $3 OFFSET $4`,
		f.Database, f.State, limit, f.Offset)
	if err != nil {
		return nil, 0, err
	}
	ms, err := collectMigrations(rows)
	return ms, total, err
}

// MigrationCounts is the number of migrations per state.
type MigrationCounts struct {
	Queued  int
	Running int
	Failed  int
}

// CountMigrations tallies queued, running and failed migrations.
func CountMigrations(ctx context.Context, db Querier) (MigrationCounts, error) {
	rows, err := db.Query(ctx, `SELECT state, count(*) FROM pgshard.migrations WHERE state IN ('queued', 'running', 'failed') GROUP BY state`)
	if err != nil {
		return MigrationCounts{}, err
	}
	defer rows.Close()
	var c MigrationCounts
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return MigrationCounts{}, err
		}
		switch state {
		case MigrationQueued:
			c.Queued = n
		case MigrationRunning:
			c.Running = n
		case MigrationFailed:
			c.Failed = n
		}
	}
	return c, rows.Err()
}

func collectMigrations(rows pgx.Rows) ([]DDLMigration, error) {
	defer rows.Close()
	var out []DDLMigration
	for rows.Next() {
		var m DDLMigration
		var meta, perShard []byte
		if err := rows.Scan(&m.ID, &m.Database, &m.Statement, &m.Kind, &m.Strategy, &m.Scope, &m.HomeShard, &m.State, &meta, &perShard, &m.Error, &m.CreatedAt, &m.FinishedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(meta, &m.Meta); err != nil {
			return nil, fmt.Errorf("catalog: migration %s meta: %w", m.ID, err)
		}
		if len(m.Meta.Steps) > 0 {
			m.Strategy = StrategyMultistep
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
