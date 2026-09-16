package catalog

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Operation kinds as pgshard.operation_blockers names them.
const (
	OperationDDL       = "ddl"
	OperationReshard   = "reshard"
	OperationUpgrade   = "upgrade"
	OperationPlacement = "table_placement"
)

// Why an operation waits for another.
const (
	BlockedByStarted = "started"
	BlockedByEarlier = "earlier"
)

// EnqueueLockKey is the pg_advisory_xact_lock key every deduplicating
// enqueue takes, so that two routers enqueueing the same statement at once
// cannot both find nothing to attach to and insert it twice.
const EnqueueLockKey int64 = 0x7067736861726451

// DefaultRetryWindow is how long after an identical migration completed a
// statement run again attaches to it instead of running again.
const DefaultRetryWindow = 10 * time.Minute

// RepeatableMigrationKinds are the kinds whose second run is the work
// again rather than a repeat of a statement already applied: running one
// of them is asking for its effect now, against the state now.
//
// A statement of any other kind that completed a moment ago stands for one
// sent again, which is what makes a client's retry after a timeout build
// an index once. One of these does not: ALTER SEQUENCE ... RESTART sent
// again after the application has consumed a few thousand values is asking
// for a restart from where the sequence is now, and answering it with the
// earlier one reports success for work that did not happen.
var RepeatableMigrationKinds = []string{"REINDEX", "VACUUM", "ALTER SEQUENCE"}

// ClusterScopedMigrationKinds are the migration kinds about the whole
// cluster rather than one database; pgshard.cluster_scoped_migration lists
// the same.
var ClusterScopedMigrationKinds = []string{"CREATE ROLE", "ALTER ROLE", "DROP ROLE", "GRANT ROLE", "REVOKE ROLE", "CREATE DATABASE", "DROP DATABASE"}

// Blocker is an operation another one waits for.
type Blocker struct {
	Kind   string `json:"kind"`
	ID     string `json:"id"`
	Reason string `json:"reason"`
}

// QueueSchema reports whether the catalog has the operation queue. A
// component newer than its catalog keeps the gates it had before.
func QueueSchema(ctx context.Context, q RowQuerier) (bool, error) {
	var ok bool
	err := q.QueryRow(ctx, `SELECT to_regprocedure('pgshard.operation_blockers(text,uuid)') IS NOT NULL`).Scan(&ok)
	return ok, err
}

// OperationBlockers lists what operation (kind, id) waits for, oldest
// first. It is empty for an operation that may start, has started or has
// finished.
func OperationBlockers(ctx context.Context, q Querier, kind, id string) ([]Blocker, error) {
	rows, err := q.Query(ctx, `SELECT blocker_kind, blocker_id::text, reason FROM pgshard.operation_blockers($1, $2)`, kind, id)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[Blocker])
}

// HomeDDLBlockers lists what a statement run directly on database's home
// shard would overlap.
func HomeDDLBlockers(ctx context.Context, q Querier, database string) ([]Blocker, error) {
	rows, err := q.Query(ctx, `SELECT blocker_kind, blocker_id::text, reason FROM pgshard.home_ddl_blockers($1)`, database)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[Blocker])
}

// MigrationDedupKey identifies m by everything the applier runs it with, so
// that the same statement sent again by the same role, against the same
// database and search_path, is recognised as the same migration. statement
// is the statement in a normalised form (the router deparses it), so that a
// retry differing only in whitespace, case or comments still matches.
//
// It is empty -- never deduplicated -- for a statement that carries a
// password or verifier: keys are stored, and a hash of a secret is one a
// reader could test guesses against.
func MigrationDedupKey(m DDLMigration, statement string) string {
	if m.Meta.Verifier != "" || strings.Contains(strings.ToLower(m.Statement), "password") ||
		strings.Contains(strings.ToLower(statement), "password") {
		return ""
	}
	meta := m.Meta
	meta.ShardSet, meta.Target = "", ""
	encoded, err := json.Marshal(meta)
	if err != nil {
		return ""
	}
	h := sha256.New()
	for _, part := range []string{m.Database, strings.TrimRight(strings.TrimSpace(statement), "; \t\r\n"), m.Kind, m.Strategy, m.Scope, string(encoded)} {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(part)))
		h.Write(n[:])
		h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// EnqueueResult is what a deduplicating enqueue did.
type EnqueueResult struct {
	ID string
	// Attached reports that an identical migration was already queued,
	// running or just completed, and ID is that one.
	Attached bool
	// State is the state of the migration ID named when it was chosen.
	State string
	// Deduplicated reports that the stored row carries a dedup key, so the
	// same statement sent again attaches to it rather than running twice.
	// It is the enqueue's answer rather than the caller's intent: a
	// catalog without the operation queue has nowhere to record a key, and
	// telling a client its retry will wait is how the retry builds the
	// index twice.
	Deduplicated bool
}

// EnqueueMigrationOnce enqueues m unless an identical migration (the same
// m.DedupKey) can stand for it: one still queued or running, or one that
// completed within window, provided no different migration arrived after
// it in the same scope -- otherwise a CREATE, DROP, CREATE sequence would
// collapse its last step into its first. A failed migration never stands for
// a new one, so a statement run again after a failure runs again.
func EnqueueMigrationOnce(ctx context.Context, db Beginner, m DDLMigration, window time.Duration) (EnqueueResult, error) {
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return EnqueueResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, EnqueueLockKey); err != nil {
		return EnqueueResult{}, err
	}
	if m.DedupKey != "" {
		var id, state string
		var arrival int64
		err := tx.QueryRow(ctx, `SELECT id::text, state, arrival FROM pgshard.migrations
			WHERE dedup_key = $1
			  AND (state IN ('queued', 'running')
			       OR (state = 'complete' AND finished_at >= now() - make_interval(secs => $2) AND kind <> ALL ($3)))
			ORDER BY arrival DESC LIMIT 1`, m.DedupKey, window.Seconds(), RepeatableMigrationKinds).Scan(&id, &state, &arrival)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
		case err != nil:
			return EnqueueResult{}, fmt.Errorf("catalog: enqueue migration: %w", err)
		default:
			var overtaken bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pgshard.migrations y
				WHERE y.arrival > $1 AND y.dedup_key IS DISTINCT FROM $2
				  AND (pgshard.cluster_scoped_migration($3) OR pgshard.cluster_scoped_migration(y.kind)
				       OR y.database = $4 OR y.meta->>'database' = $4))`,
				arrival, m.DedupKey, m.Kind, m.Database).Scan(&overtaken); err != nil {
				return EnqueueResult{}, fmt.Errorf("catalog: enqueue migration: %w", err)
			}
			if !overtaken {
				return EnqueueResult{ID: id, Attached: true, State: state, Deduplicated: true}, tx.Commit(ctx)
			}
		}
	}
	id, err := EnqueueMigration(ctx, tx, m)
	if err != nil {
		return EnqueueResult{}, err
	}
	return EnqueueResult{ID: id, State: MigrationQueued, Deduplicated: m.DedupKey != ""}, tx.Commit(ctx)
}

// HeartbeatApplier is the controller_heartbeat component the applier beats.
const HeartbeatApplier = "applier"

// BeatController records that component is alive under leadership term. A
// beat from a term that is no longer current writes nothing.
func BeatController(ctx context.Context, db Execer, component string, term int64) error {
	_, err := db.Exec(ctx, `INSERT INTO pgshard.controller_heartbeat (component, term, beat_at)
		SELECT $1, $2, now() WHERE $2 = (SELECT term FROM pgshard.leader_term)
		ON CONFLICT (component) DO UPDATE SET term = EXCLUDED.term, beat_at = EXCLUDED.beat_at`, component, term)
	return err
}

// ControllerHeartbeatAge is how long ago component last beat; found is
// false when it never has.
func ControllerHeartbeatAge(ctx context.Context, q RowQuerier, component string) (age time.Duration, found bool, err error) {
	var seconds float64
	err = q.QueryRow(ctx, `SELECT extract(epoch FROM now() - beat_at) FROM pgshard.controller_heartbeat WHERE component = $1`, component).Scan(&seconds)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return time.Duration(seconds * float64(time.Second)), true, nil
}

// QueueEntry is a row of pgshard.operation_queue_detail.
type QueueEntry struct {
	Position    int64      `json:"position"`
	Kind        string     `json:"kind"`
	ID          string     `json:"id"`
	Database    *string    `json:"database"`
	Command     string     `json:"command"`
	Statement   *string    `json:"statement,omitempty"`
	State       string     `json:"state"`
	Stage       *string    `json:"stage"`
	WaitingFor  *string    `json:"waiting_for"`
	Blockers    []Blocker  `json:"blockers"`
	Progress    *float64   `json:"progress"`
	ProgressBar *string    `json:"progress_bar"`
	Detail      *string    `json:"detail"`
	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	RetireAt    *time.Time `json:"retire_at"`
}

// QueueLimit caps how many entries one read of the queue returns. Every
// entry carries what it waits for, and a queue N deep behind one reshard
// has O(N^2) of those between them, so an unbounded read of a deep queue
// costs the catalog primary seconds of CPU and megabytes of JSON for a page
// nobody can read anyway. The head of the queue is what an operator needs.
const QueueLimit = 200

// ListOperationQueue reads the head of the operation queue in arrival
// order, with each migration's statement when detail is set
// (pgshard.operation_queue_detail, for administrators) and without it
// otherwise. total is how many entries there are, which is more than the
// rows returned when the queue is deeper than QueueLimit.
func ListOperationQueue(ctx context.Context, q RowQuerier, detail bool) (entries []QueueEntry, total int, err error) {
	statement, view := "NULL::text", "pgshard.operation_queue"
	if detail {
		statement, view = "statement", "pgshard.operation_queue_detail"
	}
	if err := q.QueryRow(ctx, `SELECT pgshard.operation_queue_depth()`).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := q.Query(ctx, `SELECT position, kind, id::text, database, command, `+statement+`, state, stage, waiting_for, blockers,
		progress::float8, progress_bar, detail, created_at, started_at, updated_at, retire_at
		FROM `+view+` ORDER BY position LIMIT `+strconv.Itoa(QueueLimit))
	if err != nil {
		return nil, 0, err
	}
	entries, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (QueueEntry, error) {
		var e QueueEntry
		var blockers []struct {
			Kind   string `json:"kind"`
			ID     string `json:"id"`
			Reason string `json:"reason"`
		}
		err := row.Scan(&e.Position, &e.Kind, &e.ID, &e.Database, &e.Command, &e.Statement, &e.State, &e.Stage, &e.WaitingFor, &blockers,
			&e.Progress, &e.ProgressBar, &e.Detail, &e.CreatedAt, &e.StartedAt, &e.UpdatedAt, &e.RetireAt)
		e.Blockers = make([]Blocker, 0, len(blockers))
		for _, b := range blockers {
			e.Blockers = append(e.Blockers, Blocker(b))
		}
		return e, err
	})
	return entries, total, err
}
