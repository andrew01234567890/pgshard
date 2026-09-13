package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// WritePauseSweep gives back a write pause whose workflow is gone.
//
// A cutover past its swap step has set default_transaction_read_only = on
// via ALTER SYSTEM on every source primary, and every ordinary exit gives it
// back: the swap lifts it on success and on each retry, the fatal and abort
// paths lift it, an unwind lifts it, and a controller that dies mid-swap
// resumes the step and reaches one of those. An operator deleting the
// pgshard.workflows row directly does not. Nothing is left to run the
// release, ALTER SYSTEM survives a restart, and the sources refuse every
// writing transaction with 25006 -- with no hint, because as far as
// PostgreSQL is concerned nothing is wrong.
//
// The sweep is keyed on shard_status.write_paused_by, which pauseSet claims
// before it raises the pause, and NOT on migrating_by: the range fence is
// also raised by workflows that never pause, and resetting
// default_transaction_read_only on a shard pgshard did not pause would undo
// an operator's own maintenance setting.
type WritePauseSweep struct {
	Pool   CatalogDB
	Shards ShardDialer
	Logger *slog.Logger
}

// CatalogDB is the catalog access a sweep needs, so what it decides can be
// tested without a database. *pgxpool.Pool satisfies it.
type CatalogDB interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func (s *WritePauseSweep) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

// Run sweeps on a ticker while this process is the leader.
func (s *WritePauseSweep) Run(ctx context.Context, interval time.Duration, leader func() bool) {
	runLoop(ctx, interval, leader, s.logger, "write pause sweep", func(ctx context.Context) {
		if _, err := s.Pass(ctx); err != nil {
			s.logger().Warn("write pause sweep failed", "err", err)
		}
	})
}

// Pass lifts every orphaned pause and returns how many shards it made
// writable again.
func (s *WritePauseSweep) Pass(ctx context.Context) (int, error) {
	orphans, err := s.orphans(ctx)
	if err != nil {
		return 0, err
	}
	freed := 0
	for _, sh := range orphans {
		// The shard first, the claim second. A crash between them leaves a
		// claim on a shard that is already writable, which the next pass
		// resets again for nothing; the other order would lose the record
		// of a pause that is still on.
		if err := s.unpause(ctx, sh); err != nil {
			return freed, fmt.Errorf("shard %s/%d: %w", sh.Set, sh.ID, err)
		}
		if _, err := s.Pool.Exec(ctx, `UPDATE pgshard.shard_status SET write_paused_by = NULL, updated_at = now()
			WHERE shard_set = $1 AND shard_id = $2`, sh.Set, sh.ID); err != nil {
			return freed, err
		}
		freed++
		s.logger().Warn("lifted a write pause whose workflow no longer exists; the source was refusing writes with 25006",
			"shard_set", sh.Set, "shard_id", sh.ID)
	}
	return freed, nil
}

func (s *WritePauseSweep) orphans(ctx context.Context) ([]ShardRef, error) {
	// A workflow that is gone OR finished. Deleting the row is the case
	// this was written for, but a terminal workflow is the same fact: the
	// pause lives inside a single swap attempt, so a completed, failed or
	// cancelled workflow cannot still be relying on one, and any exit that
	// ends a workflow without releasing leaves a claim exactly like a
	// deletion does.
	rows, err := s.Pool.Query(ctx, `SELECT shard_set, shard_id FROM pgshard.shard_status s
		WHERE write_paused_by IS NOT NULL
		  AND NOT EXISTS (SELECT 1 FROM pgshard.workflows w
		                  WHERE w.id = s.write_paused_by
		                    AND w.state NOT IN ('completed', 'failed', 'cancelled'))
		ORDER BY shard_set, shard_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ShardRef
	for rows.Next() {
		var sh ShardRef
		if err := rows.Scan(&sh.Set, &sh.ID); err != nil {
			return nil, err
		}
		out = append(out, sh)
	}
	return out, rows.Err()
}

func (s *WritePauseSweep) unpause(ctx context.Context, sh ShardRef) error {
	conn, err := s.Shards.Dial(ctx, sh.Set, sh.ID)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	// ALTER SYSTEM cannot run inside a transaction block, so it and the
	// reload go as separate statements.
	if _, err := conn.Exec(ctx, `ALTER SYSTEM RESET default_transaction_read_only`); err != nil {
		return err
	}
	_, err = conn.Exec(ctx, `SELECT pg_reload_conf()`)
	return err
}
