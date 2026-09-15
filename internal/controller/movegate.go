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
// every one with work left, since each will copy the serving set.
func activeCopies(ctx context.Context, q rowQuerier) (int, error) {
	var n int
	err := q.QueryRow(ctx, `SELECT count(*) FROM pgshard.workflows WHERE kind = ANY($1) AND state = ANY($2)`, copyKinds, activeStates).Scan(&n)
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
