package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// GIDPrefix starts every transaction identifier the router coordinates.
const GIDPrefix = "pgshard-"

// decisionShardSet is the shard set decision participants live in: the
// router only escalates transactions of routable databases.
const decisionShardSet = "default"

// DefaultPreparingTimeout is how long a decision row may stay preparing
// before the resolver decides abort for it: the router that owned it is
// presumed dead.
const DefaultPreparingTimeout = 10 * time.Second

// ShardConn is a connection to one shard's primary with the privileges to
// see and finish prepared transactions.
type ShardConn interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconnTag, error)
	Close(ctx context.Context) error
}

type pgconnTag interface{ RowsAffected() int64 }

// ShardDialer opens a connection to a shard's primary.
type ShardDialer interface {
	Dial(ctx context.Context, shardSet string, shardID int32) (ShardConn, error)
}

// ShardRef names one shard.
type ShardRef struct {
	Set string
	ID  int32
}

// Resolver finishes in-doubt two-phase commits from the decision log and
// rolls back prepared transactions no decision row claims. Every step is
// idempotent and safe to run concurrently with a coordinating router.
type Resolver struct {
	Pool   *pgxpool.Pool
	Shards ShardDialer
	Logger *slog.Logger
	// PreparingTimeout overrides DefaultPreparingTimeout.
	PreparingTimeout time.Duration
	// Now overrides the clock in tests.
	Now func() time.Time
}

// Outcome counts one resolution pass.
type Outcome struct {
	Committed  int
	RolledBack int
	Unresolved int
}

type decision struct {
	GID          string
	State        string
	Participants []int32
	CreatedAt    time.Time
}

// Resolve runs one pass; shardSet "" means every shard set.
func (r *Resolver) Resolve(ctx context.Context, shardSet string) (Outcome, error) {
	logger := r.Logger
	if logger == nil {
		logger = slog.Default()
	}
	var out Outcome
	rows, err := r.Pool.Query(ctx, `SELECT gid, state, participants, created_at FROM pgshard.xact_decisions ORDER BY created_at`)
	if err != nil {
		return out, fmt.Errorf("resolver: decisions: %w", err)
	}
	decisions, err := pgx.CollectRows(rows, pgx.RowToStructByPos[decision])
	if err != nil {
		return out, fmt.Errorf("resolver: decisions: %w", err)
	}
	for _, d := range decisions {
		if shardSet != "" && shardSet != decisionShardSet {
			break
		}
		if err := r.resolveDecision(ctx, d, &out); err != nil {
			out.Unresolved++
			logger.Warn("resolver: transaction left in doubt", "gid", d.GID, "state", d.State, "err", err)
		}
	}
	shards, err := r.listShards(ctx, shardSet)
	if err != nil {
		return out, err
	}
	for _, sh := range shards {
		if err := r.sweepOrphans(ctx, sh, &out); err != nil {
			out.Unresolved++
			logger.Warn("resolver: orphan sweep failed", "shard", fmt.Sprintf("%s/%d", sh.Set, sh.ID), "err", err)
		}
	}
	return out, nil
}

func (r *Resolver) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *Resolver) preparingTimeout() time.Duration {
	if r.PreparingTimeout > 0 {
		return r.PreparingTimeout
	}
	return DefaultPreparingTimeout
}

// resolveDecision finishes one decision row. A preparing row older than the
// timeout belongs to a dead router that never decided: abort is safe
// because commit was never recorded. Commit rows are committed on every
// participant that still holds the prepared transaction; abort rows are
// rolled back; finished rows are deleted.
func (r *Resolver) resolveDecision(ctx context.Context, d decision, out *Outcome) error {
	if d.State == "preparing" {
		if r.now().Sub(d.CreatedAt) < r.preparingTimeout() {
			return nil
		}
		tag, err := r.Pool.Exec(ctx, `UPDATE pgshard.xact_decisions SET state = 'abort', decided_at = now() WHERE gid = $1 AND state = 'preparing'`, d.GID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			if err := r.Pool.QueryRow(ctx, `SELECT state FROM pgshard.xact_decisions WHERE gid = $1`, d.GID).Scan(&d.State); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return nil
				}
				return err
			}
		} else {
			d.State = "abort"
		}
	}
	verb := "ROLLBACK PREPARED"
	if d.State == "commit" {
		verb = "COMMIT PREPARED"
	}
	for _, id := range d.Participants {
		if err := r.finishOn(ctx, ShardRef{Set: decisionShardSet, ID: id}, verb, d.GID); err != nil {
			return err
		}
	}
	if _, err := r.Pool.Exec(ctx, `DELETE FROM pgshard.xact_decisions WHERE gid = $1 AND state = $2`, d.GID, d.State); err != nil {
		return err
	}
	if d.State == "commit" {
		out.Committed++
	} else {
		out.RolledBack++
	}
	return nil
}

// finishOn runs verb on gid at sh when the shard still holds it prepared.
func (r *Resolver) finishOn(ctx context.Context, sh ShardRef, verb, gid string) error {
	conn, err := r.Shards.Dial(ctx, sh.Set, sh.ID)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, `SELECT 1 FROM pg_prepared_xacts WHERE gid = $1`, gid)
	if err != nil {
		return err
	}
	held, err := pgx.CollectRows(rows, pgx.RowTo[int])
	if err != nil {
		return err
	}
	if len(held) == 0 {
		return nil
	}
	_, err = conn.Exec(ctx, verb+" "+quoteLiteral(gid))
	return err
}

// sweepOrphans rolls back router-coordinated prepared transactions on sh
// that no decision row claims, and finishes those whose row was decided
// meanwhile. A gid whose row says commit is never rolled back.
func (r *Resolver) sweepOrphans(ctx context.Context, sh ShardRef, out *Outcome) error {
	conn, err := r.Shards.Dial(ctx, sh.Set, sh.ID)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, `SELECT gid FROM pg_prepared_xacts WHERE gid LIKE $1 ORDER BY prepared`, GIDPrefix+"%")
	if err != nil {
		return err
	}
	gids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	for _, gid := range gids {
		var state string
		err := r.Pool.QueryRow(ctx, `SELECT state FROM pgshard.xact_decisions WHERE gid = $1`, gid).Scan(&state)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			state = "abort"
		case err != nil:
			return err
		}
		switch state {
		case "commit":
			if _, err := conn.Exec(ctx, "COMMIT PREPARED "+quoteLiteral(gid)); err != nil {
				return err
			}
			out.Committed++
		case "abort":
			if _, err := conn.Exec(ctx, "ROLLBACK PREPARED "+quoteLiteral(gid)); err != nil {
				return err
			}
			out.RolledBack++
		}
	}
	return nil
}

func (r *Resolver) listShards(ctx context.Context, shardSet string) ([]ShardRef, error) {
	rows, err := r.Pool.Query(ctx, `SELECT shard_set, shard_id FROM pgshard.shard_status WHERE ($1 = '' OR shard_set = $1) ORDER BY shard_set, shard_id`, shardSet)
	if err != nil {
		return nil, fmt.Errorf("resolver: shards: %w", err)
	}
	refs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[ShardRef])
	if err != nil {
		return nil, fmt.Errorf("resolver: shards: %w", err)
	}
	return refs, nil
}

// Run resolves every interval until ctx ends.
func (r *Resolver) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if _, err := r.Resolve(ctx, ""); err != nil && r.Logger != nil {
			r.Logger.Warn("resolver pass failed", "err", err)
		}
	}
}

func quoteLiteral(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// PgxShardDialer opens pgx connections from a DSN per shard: explicit
// entries first, then Template with {set}, {id} and {group} (the
// shard_status group name) substituted.
type PgxShardDialer struct {
	Pool     *pgxpool.Pool
	DSNs     map[ShardRef]string
	Template string
}

// Dial implements ShardDialer.
func (d *PgxShardDialer) Dial(ctx context.Context, shardSet string, shardID int32) (ShardConn, error) {
	dsn, ok := d.DSNs[ShardRef{Set: shardSet, ID: shardID}]
	if !ok {
		if d.Template == "" {
			return nil, fmt.Errorf("no DSN for shard %s/%d", shardSet, shardID)
		}
		var group string
		if err := d.Pool.QueryRow(ctx, `SELECT group_name FROM pgshard.shard_status WHERE shard_set = $1 AND shard_id = $2`, shardSet, shardID).Scan(&group); err != nil {
			return nil, fmt.Errorf("shard %s/%d: %w", shardSet, shardID, err)
		}
		dsn = strings.NewReplacer("{set}", shardSet, "{id}", fmt.Sprint(shardID), "{group}", group).Replace(d.Template)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("shard %s/%d: %w", shardSet, shardID, err)
	}
	return pgxShardConn{conn}, nil
}

type pgxShardConn struct{ *pgx.Conn }

func (c pgxShardConn) Exec(ctx context.Context, sql string, args ...any) (pgconnTag, error) {
	return c.Conn.Exec(ctx, sql, args...)
}
