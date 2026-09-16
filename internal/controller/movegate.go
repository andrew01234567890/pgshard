package controller

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// MoveGateLockKey is the pg_advisory_xact_lock key under which a table
// placement starts and a reshard or upgrade starts its copy. The two must
// never overlap on the serving set, and each decides by counting the other:
// without one lock around the count and the state change, both could count
// nothing and both begin.
const MoveGateLockKey int64 = 0x7067736861726447

type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func lockMoveGate(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, MoveGateLockKey)
	return err
}

// activeCopies counts the reshards and upgrades a placement must wait for:
// every one with work left, since each will copy the serving set. It is the
// gate only on a catalog without the operation queue; with the queue,
// pgshard.operations decides. The two are not the same rule -- the queue
// also lets past a paused operation and one whose own pre-start step keeps
// failing, and applies the source-set test to upgrades too -- but where
// they differ this one counts more, never less.
//
// Not the row an in-place edit of pgshard.shard_ranges records. Nothing
// drives it -- it waits for someone to cancel it or reshard through
// spec.shards -- so counting it held every placement back for as long as it
// stood (PGS-866). Two things identify it, and both are needed. Its spec
// names no source set: the writers of a reshard spec are reconcile's record
// of an in-place edit, which names none, and reshard's own, which always
// does. And it carries no stage, because nothing has run it.
//
// The stage is what makes this safe, and it is the same thing 0058 reads.
// State would not do: a copy that has started keeps applying through its
// subscriptions whatever state the row is in, so a row past its first stage
// counts even when it is paused -- and a legacy row whose source set was
// only resolved at cutover would otherwise have been let past mid-copy.
func activeCopies(ctx context.Context, q rowQuerier) (int, error) {
	var n int
	err := q.QueryRow(ctx, `SELECT count(*) FROM pgshard.workflows
		WHERE kind = ANY($1) AND state = ANY($2)
		  AND NOT (kind = $3 AND NOT spec ? 'source_set'
		           AND coalesce(status->>'stage', '') = '')`,
		copyKinds, activeStates, KindReshard).Scan(&n)
	return n, err
}

// placementsHoldingTheSet counts the placements a copy must wait for: those
// past prepare, running or paused, and those still cleaning up after a
// cancel. A placement still preparing holds nothing yet and waits for the
// copy itself, so counting it would leave each waiting for the other --
// including one paused while it was preparing.
func placementsHoldingTheSet(ctx context.Context, q rowQuerier) (int, error) {
	var n int
	err := q.QueryRow(ctx, `SELECT count(*) FROM pgshard.workflows
		WHERE kind = $1 AND coalesce(status->>'stage', '') <> ALL($4)
		  AND (state = ANY($2) OR status->>'stage' = $3)`,
		KindTablePlacement, []string{StateRunning, StatePaused}, StageCancelling, []string{"", StagePlacementPreparing}).Scan(&n)
	return n, err
}
