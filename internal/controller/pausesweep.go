package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// WritePauseSweep gives back a write pause whose workflow is gone.
//
// A cutover past its quiesce step has set default_transaction_read_only = on
// via ALTER SYSTEM on every source primary, and every ordinary exit gives it
// back: quiesce lifts it whenever it has to wait, the swap lifts it once
// forward replication is off, the fatal and abort paths lift it, and a
// controller that dies in between resumes the switch and reaches one of
// those. A switch that waits at its flip or swap keeps the pause for as long
// as it waits, which is the one it is for. An operator deleting the
// pgshard.workflows row directly does not. Nothing is left to run the
// release, ALTER SYSTEM survives a restart, and the sources refuse every
// writing transaction with 25006 -- with no hint, because as far as
// PostgreSQL is concerned nothing is wrong.
//
// The sweep is keyed on shard_status.write_paused_by, which pauseSetClaimed
// writes before it raises a pause it means to lift again. Two things it is
// deliberately not keyed on: migrating_by, because the range fence is also
// raised by workflows that never pause; and the setting itself, because an
// operator pausing a shard for their own maintenance, and the permanent
// pause a retired set carries, both look exactly like a stuck one.
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
	runLoopStoppable(ctx, interval, leader, s.logger, "write pause sweep", func(ctx context.Context) {
		if _, err := s.Pass(ctx); err != nil {
			s.logger().Warn("write pause sweep failed", "err", err)
		}
		if _, err := s.Reassert(ctx); err != nil {
			s.logger().Warn("retirement pause sweep failed", "err", err)
		}
	})
}

// liveWorkflowNamesSet is true while a workflow that has not ended names the
// set in column col as its target or its source: a switch that has flipped
// keeps its retired source writable for reverse replication until it
// completes. A pending workflow has created nothing that could need it, and
// an in-place range edit stays pending for good. The source is read from
// the cutover status too, where the copier records the one it resolved.
func liveWorkflowNamesSet(col string) string {
	return `EXISTS (SELECT 1 FROM pgshard.workflows w
		WHERE w.state NOT IN ('pending', 'completed', 'failed', 'cancelled')
		  AND ` + col + ` IN (w.spec->>'shard_set', w.spec->>'source_set', w.status->'cutover'->>'source_set'))`
}

// Reassert puts the retirement pause back on every primary of a retired set
// that has lost it, and returns how many it paused.
//
// Complete raises that pause once and nothing else ever did: a barrier that
// resumed the set after a rollback retired it mid-run, or one from before
// barriers stopped listing retired sets, left it writable for good, and a
// client connected straight to it had writes acknowledged by a primary
// nothing reads from again.
//
// Only a set nothing may still need writable: no claimed pause on the shard,
// no live workflow naming the set, and no enabled subscription on the
// primary -- a pause would fail its apply with 25006 and hold WAL on its
// publisher.
func (s *WritePauseSweep) Reassert(ctx context.Context) (int, error) {
	rows, err := s.Pool.Query(ctx, `SELECT st.shard_set, st.shard_id FROM pgshard.shard_status st
		JOIN pgshard.shard_sets ss ON ss.shard_set = st.shard_set
		WHERE ss.state = 'retired' AND st.write_paused_by IS NULL
		  AND NOT `+liveWorkflowNamesSet("st.shard_set")+`
		ORDER BY st.shard_set, st.shard_id`)
	if err != nil {
		return 0, err
	}
	retired, err := pgx.CollectRows(rows, pgx.RowToStructByPos[ShardRef])
	if err != nil {
		return 0, err
	}
	paused := 0
	var errs []error
	for _, sh := range retired {
		did, err := s.repause(ctx, sh)
		if err != nil {
			errs = append(errs, fmt.Errorf("shard %s/%d: %w", sh.Set, sh.ID, err))
			continue
		}
		if did {
			paused++
			s.logger().Warn("put the retirement write pause back on a retired shard that had lost it",
				"shard_set", sh.Set, "shard_id", sh.ID)
		}
	}
	return paused, errors.Join(errs...)
}

func (s *WritePauseSweep) repause(ctx context.Context, sh ShardRef) (bool, error) {
	conn, err := s.Shards.Dial(ctx, sh.Set, sh.ID)
	if err != nil {
		// Not an error to report on every tick: a retired set's pods are
		// deleted before its catalog rows, and a primary nothing can reach
		// takes no writes either.
		s.logger().Debug("retired shard unreachable; its retirement pause is not checked", "shard_set", sh.Set, "shard_id", sh.ID, "err", err)
		return false, nil
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, `SELECT current_setting('default_transaction_read_only') <> 'on'
		AND NOT EXISTS (SELECT 1 FROM pg_subscription WHERE subenabled)`)
	if err != nil {
		return false, err
	}
	lost, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[bool])
	if err != nil || !lost {
		return false, err
	}
	if _, err := conn.Exec(ctx, `ALTER SYSTEM SET default_transaction_read_only = on`); err != nil {
		return false, err
	}
	_, err = conn.Exec(ctx, `SELECT pg_reload_conf()`)
	return err == nil, err
}

// Pass lifts every orphaned pause and returns how many shards it made
// writable again.
func (s *WritePauseSweep) Pass(ctx context.Context) (int, error) {
	orphans, err := s.orphans(ctx)
	if err != nil {
		return 0, err
	}
	freed := 0
	var errs []error
	for _, sh := range orphans {
		// The shard first, the claim second. A crash between them leaves a
		// claim on a shard that is already writable, which the next pass
		// resets again for nothing; the other order would lose the record
		// of a pause that is still on.
		if err := s.unpause(ctx, sh); err != nil {
			// Collected, not returned: a shard_status row outlives the
			// pods of a set that was deleted, so one unreachable shard can
			// hold a claim no pass will ever clear. Stopping at it would
			// leave every orphan that sorts after it paused for as long as
			// that row exists.
			errs = append(errs, fmt.Errorf("shard %s/%d: %w", sh.Set, sh.ID, err))
			continue
		}
		if _, err := s.Pool.Exec(ctx, `UPDATE pgshard.shard_status SET write_paused_by = NULL, write_paused_at = NULL, updated_at = now()
			WHERE shard_set = $1 AND shard_id = $2`, sh.Set, sh.ID); err != nil {
			errs = append(errs, err)
			continue
		}
		freed++
		s.logger().Warn("lifted a write pause whose workflow is gone or finished; the shard was refusing writes with 25006",
			"shard_set", sh.Set, "shard_id", sh.ID)
	}
	return freed, errors.Join(errs...)
}

func (s *WritePauseSweep) orphans(ctx context.Context) ([]ShardRef, error) {
	// A workflow that is gone OR finished. Deleting the row is the case
	// this was written for, but a terminal workflow is the same fact: a
	// claimed pause is one a workflow intends to lift, so a completed,
	// failed or cancelled workflow cannot still be relying on one, and any
	// exit that ends a workflow without releasing leaves a claim exactly
	// like a deletion does.
	//
	// Only CLAIMED pauses. The retirement pause Complete raises on a set
	// that will never serve again is deliberate, permanent and unclaimed,
	// and lifting it would hand a retired primary back to any client still
	// connected straight to it -- writes acknowledged by a shard nothing
	// reads from again.
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
