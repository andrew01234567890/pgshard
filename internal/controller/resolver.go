package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/metrics"
)

// GIDPrefix starts every transaction identifier the router coordinates.
const GIDPrefix = "pgshard-"

// DefaultPreparingTimeout is how long a preparing decision row may go
// without a coordinator heartbeat before the resolver decides abort for
// it: the router that owned it is presumed dead. Live coordinators beat
// far more often than this, so only a dead one ages out.
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

// ShardDBDialer opens a connection to one database of a shard's primary.
type ShardDBDialer interface {
	ShardDialer
	DialDatabase(ctx context.Context, shardSet string, shardID int32, database string) (ShardConn, error)
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
	// Metrics counts resolved transactions; nil disables it.
	Metrics *metrics.Controller
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
	// LastAlive is the later of the row's creation and its coordinator's
	// last heartbeat.
	LastAlive time.Time
}

// holder is one place a prepared transaction sits: a shard's primary and
// the database it was prepared in.
type holder struct {
	Shard    ShardRef
	Database string
}

// Resolve runs one pass; shardSet "" means every shard set.
func (r *Resolver) Resolve(ctx context.Context, shardSet string) (Outcome, error) {
	logger := r.Logger
	if logger == nil {
		logger = slog.Default()
	}
	var out Outcome
	rows, err := r.Pool.Query(ctx, `SELECT gid, state, participants, greatest(created_at, heartbeat_at) FROM pgshard.xact_decisions ORDER BY created_at`)
	if err != nil {
		return out, fmt.Errorf("resolver: decisions: %w", err)
	}
	decisions, err := pgx.CollectRows(rows, pgx.RowToStructByPos[decision])
	if err != nil {
		return out, fmt.Errorf("resolver: decisions: %w", err)
	}
	shards, err := r.listShards(ctx, shardSet)
	if err != nil {
		return out, err
	}
	holders, scanErrs := r.scanPrepared(ctx, shards)
	// A decision may only be deleted once every shard of the whole current
	// topology was searched: a participant can sit on a group its shard id
	// no longer maps to after a reshard.
	complete := shardSet == "" && len(scanErrs) == 0
	for _, d := range decisions {
		if err := r.resolveDecision(ctx, d, holders, complete, &out); err != nil {
			out.Unresolved++
			logger.Warn("resolver: transaction left in doubt", "gid", d.GID, "state", d.State, "err", err)
		}
	}
	for sh, err := range scanErrs {
		out.Unresolved++
		logger.Warn("resolver: prepared-transaction scan failed", "shard", fmt.Sprintf("%s/%d", sh.Set, sh.ID), "err", err)
	}
	if err := r.sweepOrphans(ctx, holders, &out); err != nil {
		out.Unresolved++
		logger.Warn("resolver: orphan sweep failed", "err", err)
	}
	return out, nil
}

// scanPrepared searches every shard's pg_prepared_xacts for
// router-coordinated gids and returns where each is held, with the scan
// errors per unreachable shard.
func (r *Resolver) scanPrepared(ctx context.Context, shards []ShardRef) (map[string][]holder, map[ShardRef]error) {
	holders := map[string][]holder{}
	scanErrs := map[ShardRef]error{}
	for _, sh := range shards {
		gids, err := r.listPrepared(ctx, sh)
		if err != nil {
			scanErrs[sh] = err
			continue
		}
		for gid, db := range gids {
			holders[gid] = append(holders[gid], holder{Shard: sh, Database: db})
		}
	}
	return holders, scanErrs
}

// listPrepared reads sh's pg_prepared_xacts: gid to database. The view is
// cluster-wide, so one connection sees every database's entries.
func (r *Resolver) listPrepared(ctx context.Context, sh ShardRef) (map[string]string, error) {
	conn, err := r.Shards.Dial(ctx, sh.Set, sh.ID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, `SELECT gid, database FROM pg_prepared_xacts WHERE gid LIKE $1 ORDER BY prepared`, GIDPrefix+"%")
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for rows.Next() {
		var gid, db string
		if err := rows.Scan(&gid, &db); err != nil {
			return nil, err
		}
		out[gid] = db
	}
	return out, rows.Err()
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
// shard that holds the prepared transaction — searched across the whole
// topology, never trusted to the recorded participant list, which a
// reshard can leave pointing at groups that no longer hold it. The row is
// deleted only once every shard was searched and none still holds the gid.
func (r *Resolver) resolveDecision(ctx context.Context, d decision, holders map[string][]holder, complete bool, out *Outcome) error {
	if d.State == "preparing" {
		if r.now().Sub(d.LastAlive) < r.preparingTimeout() {
			return nil
		}
		// The staleness check re-runs inside the UPDATE against the same
		// cutoff: a coordinator heartbeat landing after the scan snapshot
		// makes it match zero rows instead of aborting a live transaction.
		cutoff := r.now().Add(-r.preparingTimeout())
		tag, err := r.Pool.Exec(ctx, `UPDATE pgshard.xact_decisions SET state = 'abort', decided_at = now() WHERE gid = $1 AND state = 'preparing' AND greatest(created_at, heartbeat_at) <= $2`, d.GID, cutoff)
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
			if d.State == "preparing" {
				return nil
			}
		} else {
			d.State = "abort"
		}
	}
	for len(holders[d.GID]) > 0 {
		h := holders[d.GID][0]
		if err := r.finishOn(ctx, h, d.State == "commit", d.GID); err != nil {
			return err
		}
		holders[d.GID] = holders[d.GID][1:]
	}
	delete(holders, d.GID)
	if !complete {
		return errors.New("not every shard could be searched for the prepared transaction: keeping the decision")
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

// finishOn commits or rolls back gid where h holds it. PostgreSQL only
// finishes a prepared transaction from the database it was prepared in, so
// the connection targets h's database, not the DSN's default one.
func (r *Resolver) finishOn(ctx context.Context, h holder, commit bool, gid string) error {
	var conn ShardConn
	var err error
	if d, ok := r.Shards.(ShardDBDialer); ok {
		conn, err = d.DialDatabase(ctx, h.Shard.Set, h.Shard.ID, h.Database)
	} else {
		conn, err = r.Shards.Dial(ctx, h.Shard.Set, h.Shard.ID)
	}
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	verb := "ROLLBACK PREPARED"
	if commit {
		verb = "COMMIT PREPARED"
	}
	_, err = conn.Exec(ctx, verb+" "+quoteLiteral(gid))
	return err
}

// sweepOrphans finishes the remaining prepared transactions no decision
// pass handled: by the decision row's state when one exists (checked at
// sweep time, so a commit-decided gid is never rolled back), rolled back
// when no row claims the gid — the coordinator writes the row before any
// participant prepares, so a rowless prepared transaction is an orphan.
func (r *Resolver) sweepOrphans(ctx context.Context, holders map[string][]holder, out *Outcome) error {
	gids := make([]string, 0, len(holders))
	for gid := range holders {
		gids = append(gids, gid)
	}
	slices.Sort(gids)
	var errs []error
	for _, gid := range gids {
		var state string
		err := r.Pool.QueryRow(ctx, `SELECT state FROM pgshard.xact_decisions WHERE gid = $1`, gid).Scan(&state)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			state = "abort"
		case err != nil:
			errs = append(errs, fmt.Errorf("%s: %w", gid, err))
			continue
		}
		if state == "preparing" {
			continue
		}
		for _, h := range holders[gid] {
			err := r.finishOn(ctx, h, state == "commit", gid)
			switch {
			// The live coordinator can finish gid and delete its row between
			// the scan snapshot and this sweep: a gone prepared transaction
			// is already resolved, not a failure of this pass.
			case isGonePreparedXact(err):
				continue
			case err != nil:
				errs = append(errs, fmt.Errorf("%s on %s/%d: %w", gid, h.Shard.Set, h.Shard.ID, err))
				continue
			}
			if state == "commit" {
				out.Committed++
			} else {
				out.RolledBack++
			}
		}
	}
	return errors.Join(errs...)
}

func isGonePreparedXact(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UndefinedObject {
		return true
	}
	return strings.Contains(err.Error(), "does not exist")
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

// Run resolves in-doubt transactions on every tick while this replica is
// the leader. Only the leader may: a pass commits and rolls back prepared
// transactions on every group.
func (r *Resolver) Run(ctx context.Context, interval time.Duration, leader func() bool) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if leader != nil && !leader() {
			continue
		}
		out, err := r.Resolve(ctx, "")
		if err != nil && r.Logger != nil {
			r.Logger.Warn("resolver pass failed", "err", err)
		}
		if r.Metrics != nil {
			r.Metrics.ResolvedTxns.WithLabelValues("committed").Add(float64(out.Committed))
			r.Metrics.ResolvedTxns.WithLabelValues("rolled_back").Add(float64(out.RolledBack))
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
	return d.DialDatabase(ctx, shardSet, shardID, "")
}

func (d *PgxShardDialer) dsn(ctx context.Context, shardSet string, shardID int32) (string, error) {
	if dsn, ok := d.DSNs[ShardRef{Set: shardSet, ID: shardID}]; ok {
		return dsn, nil
	}
	if d.Template == "" {
		return "", fmt.Errorf("no DSN for shard %s/%d", shardSet, shardID)
	}
	group, err := GroupName(ctx, d.Pool, shardSet, shardID)
	if err != nil {
		return "", err
	}
	return ExpandShardTemplate(d.Template, shardSet, shardID, group, ""), nil
}

// GroupName reads the shard_status group name of one shard.
func GroupName(ctx context.Context, pool *pgxpool.Pool, shardSet string, shardID int32) (string, error) {
	var group string
	if err := pool.QueryRow(ctx, `SELECT group_name FROM pgshard.shard_status WHERE shard_set = $1 AND shard_id = $2`, shardSet, shardID).Scan(&group); err != nil {
		return "", fmt.Errorf("shard %s/%d: %w", shardSet, shardID, err)
	}
	return group, nil
}

// ExpandShardTemplate substitutes {set}, {id}, {group} and {db} in a DSN
// template.
func ExpandShardTemplate(template, shardSet string, shardID int32, group, database string) string {
	return strings.NewReplacer("{set}", shardSet, "{id}", fmt.Sprint(shardID), "{group}", group, "{db}", database).Replace(template)
}

type pgxShardConn struct{ *pgx.Conn }

func (c pgxShardConn) Exec(ctx context.Context, sql string, args ...any) (pgconnTag, error) {
	return c.Conn.Exec(ctx, sql, args...)
}
