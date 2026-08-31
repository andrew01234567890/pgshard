package controller

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// errNotOwner ends a pass that no longer owns its workflow. It is not a
// failure: another replica has taken the workflow over and is driving it.
var errNotOwner = errors.New("controller: another replica owns this workflow")

// DefaultOwnerLease is how long a claim stands without being refreshed. A
// pass refreshes its claim every tick, so the lease only has to outlive one
// pass; anything shorter lets a peer steal a workflow that is still being
// driven, and anything much longer strands a workflow whose owner died.
const DefaultOwnerLease = 5 * time.Minute

var (
	replicaOnce sync.Once
	replicaID   string
	replicaErr  error
)

// replicaToken identifies this process for the life of the process, so a
// replica that keeps leadership keeps its workflows across passes.
func replicaToken() (string, error) {
	replicaOnce.Do(func() { replicaID, replicaErr = fenceOwner() })
	return replicaID, replicaErr
}

// claimWorkflow takes ownership of a workflow for one replica and reports
// whether it holds it. Every write the pass makes carries the claim.
//
// Leadership is checked between ticks, so a replica that loses the advisory
// lock during a pass -- and a copy or placement pass is long -- would run
// that pass to the end against shards a new leader is already driving.
// Ownership is checked on every write instead: the pass stops at its next
// write, and a claim can only move to another replica once the current
// owner has stopped refreshing it.
func claimWorkflow(ctx context.Context, pool *pgxpool.Pool, me, id string, lease time.Duration) (string, bool, error) {
	if me == "" {
		var err error
		if me, err = replicaToken(); err != nil {
			return "", false, err
		}
	}
	if lease <= 0 {
		lease = DefaultOwnerLease
	}
	tag, err := pool.Exec(ctx, `UPDATE pgshard.workflows SET owner = $2, owned_at = now()
		WHERE id = $1::uuid AND (owner IS NULL OR owner = $2 OR owned_at < now() - $3::interval)`,
		id, me, lease)
	if err != nil {
		return "", false, err
	}
	return me, tag.RowsAffected() == 1, nil
}

// holdClaim confirms the claim before a step's side effects and refreshes
// its lease. A step can outrun the lease -- copying one source shard is a
// single step over a whole table -- so a pass that only claimed at its
// start would be stealable while it is still writing; refreshing here keeps
// the lease alive exactly as long as the pass keeps making progress.
//
// A workflow that was taken over or deleted under the pass matches nothing,
// which stops the pass at the step in flight rather than a stage later.
func holdClaim(ctx context.Context, pool *pgxpool.Pool, id, owner string) error {
	if owner == "" {
		return nil
	}
	tag, err := pool.Exec(ctx, `UPDATE pgshard.workflows SET owned_at = now() WHERE id = $1::uuid AND owner = $2`, id, owner)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return errNotOwner
	}
	return nil
}

// holdClaimTx is holdClaim inside a transaction, so a publish that makes a
// new placement effective either carries the claim or does not happen.
func holdClaimTx(ctx context.Context, tx pgx.Tx, id, owner string) error {
	if owner == "" {
		return nil
	}
	tag, err := tx.Exec(ctx, `UPDATE pgshard.workflows SET owned_at = now() WHERE id = $1::uuid AND owner = $2`, id, owner)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return errNotOwner
	}
	return nil
}

// ownedExec runs a status write that only applies while this pass still owns
// the workflow. A write that matches nothing means the workflow was taken
// over, which ends the pass without an error.
func ownedExec(ctx context.Context, pool *pgxpool.Pool, owner string, sql string, args ...any) error {
	tag, err := pool.Exec(ctx, sql, args...)
	if err != nil {
		return err
	}
	if owner != "" && tag.RowsAffected() == 0 {
		return errNotOwner
	}
	return nil
}

// nullIfEmpty renders an unclaimed owner as SQL NULL, so a caller that owns
// nothing writes as before.
func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
