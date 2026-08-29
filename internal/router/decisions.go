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
	// Begin records gid as preparing on the given participant shards; xids
	// are the participants' transaction ids in the same order, which a
	// restore uses to tell a committed transaction from a lost one.
	Begin(ctx context.Context, gid string, participants []int32, xids []string) error
	// Heartbeat marks a preparing row's coordinator alive; the resolver
	// only aborts a preparing row whose heartbeat has gone stale.
	Heartbeat(ctx context.Context, gid string) error
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
// durable runs sql in a transaction that reaches disk before it returns,
// in one round trip. The decision log sits in front of every cross-shard
// commit, and a separate BEGIN, SET LOCAL, statement and COMMIT made that
// four round trips to the catalog of which three carried no information --
// so the catalog became a per-commit tax on every multi-shard write in the
// cluster. Pipelined, the four still execute in order and still abort
// together; only the waiting is gone.
func (l *PGDecisionLog) durable(ctx context.Context, sql string, args ...any) (int64, error) {
	b := &pgx.Batch{}
	b.Queue("BEGIN")
	// The decision has to be on disk before anything acts on it: a
	// coordinator that told a shard to commit and then lost the record
	// leaves a transaction nothing can resolve.
	b.Queue("SET LOCAL synchronous_commit = on")
	b.Queue(sql, args...)
	b.Queue("COMMIT")

	br := l.Pool.SendBatch(ctx, b)
	var affected int64
	var firstErr error
	for i := range 4 {
		tag, err := br.Exec()
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if i == 2 {
			affected = tag.RowsAffected()
		}
	}
	if err := br.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return affected, firstErr
}

// Begin implements DecisionLog.
func (l *PGDecisionLog) Begin(ctx context.Context, gid string, participants []int32, xids []string) error {
	_, err := l.durable(ctx, `INSERT INTO pgshard.xact_decisions (gid, state, participants, participant_xids) VALUES ($1, 'preparing', $2, $3)`, gid, participants, xids)
	return err
}

// Heartbeat implements DecisionLog. It needs no synchronous commit: a
// lost heartbeat costs at worst a spurious abort of a still-live
// coordinator, which the commit decision's atomic update turns into a
// clean rollback.
func (l *PGDecisionLog) Heartbeat(ctx context.Context, gid string) error {
	_, err := l.Pool.Exec(ctx, `UPDATE pgshard.xact_decisions SET heartbeat_at = now() WHERE gid = $1 AND state = 'preparing'`, gid)
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
