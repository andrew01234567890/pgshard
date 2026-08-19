package router

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DecisionLog is the durable record of every two-phase commit the router
// coordinates: a row is written before any participant prepares, and the
// commit/abort decision is one atomic row update.
type DecisionLog interface {
	// Begin records gid as preparing on the given participant shards.
	Begin(ctx context.Context, gid string, participants []int32) error
	// Commit decides commit; false means the row was no longer preparing
	// (the resolver aborted the transaction first).
	Commit(ctx context.Context, gid string) (bool, error)
	// Abort decides abort.
	Abort(ctx context.Context, gid string) error
	// Delete removes a fully applied decision.
	Delete(ctx context.Context, gid string) error
}

// PGDecisionLog stores decisions in pgshard.xact_decisions of the catalog.
type PGDecisionLog struct {
	Pool *pgxpool.Pool
}

// durable runs sql in its own transaction with synchronous commit forced on
// so the row is on disk (and on the catalog's synchronous standbys) before
// the coordinator proceeds.
func (l *PGDecisionLog) durable(ctx context.Context, sql string, args ...any) (int64, error) {
	var affected int64
	err := pgx.BeginFunc(ctx, l.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = on"); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, sql, args...)
		affected = tag.RowsAffected()
		return err
	})
	return affected, err
}

// Begin implements DecisionLog.
func (l *PGDecisionLog) Begin(ctx context.Context, gid string, participants []int32) error {
	_, err := l.durable(ctx, `INSERT INTO pgshard.xact_decisions (gid, state, participants) VALUES ($1, 'preparing', $2)`, gid, participants)
	return err
}

// Commit implements DecisionLog.
func (l *PGDecisionLog) Commit(ctx context.Context, gid string) (bool, error) {
	n, err := l.durable(ctx, `UPDATE pgshard.xact_decisions SET state = 'commit', decided_at = now() WHERE gid = $1 AND state = 'preparing'`, gid)
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// Abort implements DecisionLog.
func (l *PGDecisionLog) Abort(ctx context.Context, gid string) error {
	n, err := l.durable(ctx, `UPDATE pgshard.xact_decisions SET state = 'abort', decided_at = now() WHERE gid = $1 AND state = 'preparing'`, gid)
	if err != nil {
		return err
	}
	if n == 0 {
		var state string
		if qerr := l.Pool.QueryRow(ctx, `SELECT state FROM pgshard.xact_decisions WHERE gid = $1`, gid).Scan(&state); qerr == nil && state == "commit" {
			return fmt.Errorf("decision log: %s was already decided commit", gid)
		} else if qerr != nil && !errors.Is(qerr, pgx.ErrNoRows) {
			return qerr
		}
	}
	return nil
}

// Delete implements DecisionLog.
func (l *PGDecisionLog) Delete(ctx context.Context, gid string) error {
	_, err := l.Pool.Exec(ctx, `DELETE FROM pgshard.xact_decisions WHERE gid = $1`, gid)
	return err
}
