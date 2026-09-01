package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/placement"
)

// DefaultCutoverLockTimeout bounds each SHARE lock of the sweep.
const DefaultCutoverLockTimeout = 5 * time.Second

// JournalSchema and JournalTable name the resharding journal in every user
// database: the row every source writes at the point of no return.
const (
	JournalSchema = "pgshard_journal"
	JournalTable  = "resharding_journal"
)

// ReversePublicationName names the publication on a target that carries the
// sharded rows of one source's ranges back to it.
func ReversePublicationName(generation int64, source int32) string {
	return fmt.Sprintf("pgshard_reshard_g%d_rev_s%d", generation, source)
}

// ReverseHomePublicationName names the publication on the home target that
// carries reference and unsharded tables back to the home source.
func ReverseHomePublicationName(generation int64) string {
	return fmt.Sprintf("pgshard_reshard_g%d_rev_home", generation)
}

// ReverseSubscriptionName names the subscription (and slot on the target) of
// one (source, target) pair of the reverse direction.
func ReverseSubscriptionName(generation int64, source, target int32) string {
	return fmt.Sprintf("pgshard_reshard_g%d_rev_s%d_t%d", generation, source, target)
}

// pgCutover implements cutoverOps against the catalog and the shards.
type pgCutover struct {
	c         *Copier
	wf        *copyWorkflow
	srcSet    string
	srcIDs    []int32
	srcRanges placement.RangeSet
	dbs       []dbPlan
}

// driveCutover builds the PostgreSQL ops of one workflow and advances it.
func (c *Copier) driveCutover(ctx context.Context, wf *copyWorkflow) (bool, error) {
	if err := holdClaim(ctx, c.Pool, wf.id, wf.owner); err != nil {
		return false, err
	}
	if _, _, err := c.pinSource(ctx, wf); err != nil {
		return false, err
	}
	ops, err := c.pgCutover(ctx, wf)
	if err != nil {
		return false, err
	}
	return c.cutover(ctx, wf, ops)
}

func (c *Copier) pgCutover(ctx context.Context, wf *copyWorkflow) (*pgCutover, error) {
	ranges, err := catalog.ListShardRanges(ctx, c.Pool, wf.sourceSet())
	if err != nil {
		return nil, err
	}
	if len(ranges) == 0 {
		return nil, fmt.Errorf("source shard set %s has no ranges", wf.sourceSet())
	}
	ops := &pgCutover{c: c, wf: wf, srcSet: wf.sourceSet(), srcRanges: catalog.RangeSet(ranges)}
	for _, r := range ranges {
		ops.srcIDs = append(ops.srcIDs, r.ShardID)
	}
	if ops.dbs, err = c.databases(ctx); err != nil {
		return nil, err
	}
	return ops, nil
}

func (o *pgCutover) lockTimeout() time.Duration {
	if o.c.LockTimeout > 0 {
		return o.c.LockTimeout
	}
	return DefaultCutoverLockTimeout
}

func (o *pgCutover) forwardPattern(t int32) string {
	return fmt.Sprintf("pgshard\\_reshard\\_g%d\\_t%d\\_%%", o.wf.gen, t)
}

func (o *pgCutover) reversePattern(s int32) string {
	return fmt.Sprintf("pgshard\\_reshard\\_g%d\\_rev\\_s%d\\_%%", o.wf.gen, s)
}

// GateOpen: every table ready, lag under threshold, no paused subscription
// and an apply worker alive behind every forward subscription.
func (o *pgCutover) GateOpen(ctx context.Context) (bool, string, error) {
	progress, byTarget, _, err := o.c.observe(ctx, o.wf, o.srcSet, o.srcIDs, o.dbs)
	if err != nil {
		return false, "", err
	}
	o.wf.copy.Progress, o.wf.copy.Targets = progress, byTarget
	if !progress.CaughtUp(o.c.lagBytes()) {
		return false, progress.Describe(), nil
	}
	if progress.Paused > 0 {
		return false, fmt.Sprintf("%d subscriptions paused by the throttle", progress.Paused), nil
	}
	var stalled []string
	for _, db := range o.dbs {
		for _, t := range o.wf.ids {
			conn, err := o.c.Shards.DialDatabase(ctx, o.wf.set, t, db.name)
			if err != nil {
				return false, "", err
			}
			rows, err := conn.Query(ctx, `SELECT s.subname FROM pg_subscription s
				LEFT JOIN pg_stat_subscription st ON st.subid = s.oid AND st.relid IS NULL
				WHERE s.subname LIKE $1 AND (NOT s.subenabled OR st.pid IS NULL) ORDER BY 1`, o.forwardPattern(t))
			if err == nil {
				var names []string
				names, err = pgx.CollectRows(rows, pgx.RowTo[string])
				for _, n := range names {
					stalled = append(stalled, fmt.Sprintf("%s/%d %s", db.name, t, n))
				}
			}
			_ = conn.Close(ctx)
			if err != nil {
				return false, "", err
			}
		}
	}
	if len(stalled) > 0 {
		return false, "subscriptions without an apply worker: " + strings.Join(stalled, ", "), nil
	}
	return true, "", nil
}

// Fence marks every source shard migrating and takes the DDL lock of every
// database.
func (o *pgCutover) Fence(ctx context.Context) error {
	// Take the DDL locks first and commit them on their own. They stop new
	// migrations from starting, which is what lets the running ones drain;
	// taking them in the same transaction as the write fence would roll them
	// back on every wait below and let a fresh migration in each time.
	if _, err := o.c.Pool.Exec(ctx, `INSERT INTO pgshard.workflow_locks (kind, key, workflow_id)
		SELECT 'ddl', name, $1::uuid FROM pgshard.databases ON CONFLICT (kind, key) DO NOTHING`, o.wf.id); err != nil {
		return err
	}
	// A lock only holds back migrations that have not started. One already
	// running keeps applying -- abandoning it half-way is worse than
	// finishing it -- so fencing now would split it across both sets: its
	// per-shard progress describes the set it was planned against while its
	// remaining shards would be applied against the other. Wait for it, and
	// wait before raising the write fence rather than after, so the pause
	// clients see does not include the migration.
	rows, err := o.c.Pool.Query(ctx, `SELECT DISTINCT database FROM pgshard.migrations WHERE state = 'running' ORDER BY database`)
	if err != nil {
		return err
	}
	running, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	if len(running) > 0 {
		return retryf("a migration is still applying on %s; fencing now would split it across both sets", strings.Join(running, ", "))
	}
	tx, err := o.c.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Claim the fence rather than just setting it: a second cutover of the
	// same source must not join a fence it did not raise, because either
	// one releasing would open writes the other still believes are held.
	if _, err := tx.Exec(ctx, `UPDATE pgshard.shard_status SET migrating = true, migrating_by = $2::uuid, updated_at = now()
		WHERE shard_set = $1 AND (NOT migrating OR migrating_by = $2::uuid)`, o.srcSet, o.wf.id); err != nil {
		return err
	}
	var heldByOther int
	if err := tx.QueryRow(ctx, `SELECT count(*)::int FROM pgshard.shard_status
		WHERE shard_set = $1 AND migrating AND migrating_by IS DISTINCT FROM $2::uuid`, o.srcSet, o.wf.id).Scan(&heldByOther); err != nil {
		return err
	}
	if heldByOther > 0 {
		return fmt.Errorf("another cutover holds the write fence on %s", o.srcSet)
	}
	return tx.Commit(ctx)
}

// Drain runs the resolver against every source and lists what remains.
func (o *pgCutover) Drain(ctx context.Context) ([]string, error) {
	var remaining []string
	for _, s := range o.srcIDs {
		gids, err := o.c.preparedOn(ctx, o.srcSet, s)
		if err != nil {
			return nil, err
		}
		for _, g := range gids {
			remaining = append(remaining, fmt.Sprintf("%s/%d:%s", o.srcSet, s, g))
		}
	}
	return remaining, nil
}

// Sweep takes a SHARE lock on every user table of every source database in
// one short transaction per database: it returns once no write is in
// flight, or errRetry when one outlasts the lock timeout.
func (o *pgCutover) Sweep(ctx context.Context) error {
	for _, db := range o.dbs {
		for _, s := range o.srcIDs {
			conn, err := o.c.Shards.DialDatabase(ctx, o.srcSet, s, db.name)
			if err != nil {
				return err
			}
			err = o.sweepOne(ctx, conn)
			_ = conn.Close(ctx)
			if err != nil {
				return fmt.Errorf("sweep of %s on %s/%d: %w", db.name, o.srcSet, s, err)
			}
		}
	}
	return nil
}

func (o *pgCutover) sweepOne(ctx context.Context, conn ShardConn) error {
	tables, err := listTables(ctx, conn)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return nil
	}
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		names = append(names, QuoteIdent(t.schema)+"."+QuoteIdent(t.name))
	}
	if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
		return err
	}
	_, err = conn.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", o.lockTimeout().Milliseconds()))
	if err == nil {
		_, err = conn.Exec(ctx, "LOCK TABLE "+strings.Join(names, ", ")+" IN SHARE MODE")
	}
	if err != nil {
		_, _ = conn.Exec(ctx, "ROLLBACK")
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "55P03" {
			return retryf("lock timeout: a write is still in flight")
		}
		return err
	}
	_, err = conn.Exec(ctx, "COMMIT")
	return err
}

// Positions reads pg_current_wal_lsn() of every source.
func (o *pgCutover) Positions(ctx context.Context) (map[string]int64, error) {
	out := map[string]int64{}
	for _, s := range o.srcIDs {
		lsn, err := o.currentLSN(ctx, o.srcSet, s)
		if err != nil {
			return nil, err
		}
		out[fmt.Sprint(s)] = lsn
	}
	return out, nil
}

func (o *pgCutover) currentLSN(ctx context.Context, set string, id int32) (int64, error) {
	conn, err := o.c.Shards.Dial(ctx, set, id)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, `SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint`)
	if err != nil {
		return 0, err
	}
	return pgx.CollectExactlyOneRow(rows, pgx.RowTo[int64])
}

// CaughtUp: every forward subscription confirmed a flush position at or
// past its source's. The check reads the publisher slot's
// confirmed_flush_lsn: it survives apply-worker restarts (unlike the
// in-memory pg_stat_subscription.latest_end_lsn) and still advances over
// keepalives on an idle source.
func (o *pgCutover) CaughtUp(ctx context.Context, positions map[string]int64) (bool, string, error) {
	behind := nothingToCompare(o.srcSet, o.srcIDs, o.wf.ids, positions)
	for _, s := range o.srcIDs {
		want, ok := positions[fmt.Sprint(s)]
		if !ok || want <= 0 {
			continue
		}
		flushed, err := slotFlushPositions(ctx, o.c.Shards, o.srcSet, s,
			fmt.Sprintf("pgshard\\_reshard\\_g%d\\_t%%\\_s%d", o.wf.gen, s))
		if err != nil {
			return false, "", err
		}
		expected := make([]string, 0, len(o.wf.ids))
		for _, t := range o.wf.ids {
			expected = append(expected, SubscriptionName(o.wf.gen, t, s))
		}
		behind = append(behind, slotsBehind(expected, flushed, want, fmt.Sprintf("%s/%d", o.srcSet, s))...)
	}
	if len(behind) > 0 {
		return false, "subscriptions behind the source position: " + strings.Join(behind, ", "), nil
	}
	return true, "", nil
}

// nothingToCompare reports the ways the caught-up question can have no
// content: no sources, no targets, or a source with no recorded position.
// Each of them made the check return "caught up" while comparing nothing,
// which is how a switch proceeds onto a target that never received the
// rows. A missing position is the sharpest of the three, because the map
// lookup yields zero and every slot is at or past zero.
func nothingToCompare(srcSet string, srcIDs, targetIDs []int32, positions map[string]int64) []string {
	var out []string
	if len(srcIDs) == 0 {
		out = append(out, "no source shards to compare")
	}
	if len(targetIDs) == 0 {
		out = append(out, "no target shards to compare")
	}
	for _, s := range srcIDs {
		if want, ok := positions[fmt.Sprint(s)]; !ok || want <= 0 {
			out = append(out, fmt.Sprintf("%s/%d has no recorded source position", srcSet, s))
		}
	}
	return out
}

// slotsBehind lists the expected publisher slots that are missing,
// unconfirmed, or behind want. Every expected slot must be present with a
// non-NULL confirmed_flush_lsn at or past want: a slot the query did not
// return means the subscription (or its slot) is gone, not that nothing is
// left to apply, so its absence must never read as caught-up.
func slotsBehind(expected []string, flushed map[string]int64, want int64, at string) []string {
	var out []string
	for _, name := range expected {
		switch v, ok := flushed[name]; {
		case !ok:
			out = append(out, fmt.Sprintf("%s %s missing", at, name))
		case v < 0:
			out = append(out, fmt.Sprintf("%s %s has no confirmed flush position", at, name))
		case v < want:
			out = append(out, fmt.Sprintf("%s %s at %d of %d", at, name, v, want))
		}
	}
	return out
}

// slotFlushPositions reads confirmed_flush_lsn of every replication slot
// on the shard whose name matches pattern.
func slotFlushPositions(ctx context.Context, dialer ShardDialer, set string, id int32, pattern string) (map[string]int64, error) {
	conn, err := dialer.Dial(ctx, set, id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, `SELECT slot_name, coalesce((confirmed_flush_lsn - '0/0'::pg_lsn)::bigint, -1)
		FROM pg_replication_slots WHERE slot_name LIKE $1 ORDER BY 1`, pattern)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var name string
		var v int64
		if err := rows.Scan(&name, &v); err != nil {
			return nil, err
		}
		out[name] = v
	}
	return out, rows.Err()
}

// rowDigest is one table slice's row count and two independent
// combinations of its per-row text hashes.
type rowDigest struct {
	Rows int64
	Hash int64
	// XOR is the same per-row hashes combined over a different algebra,
	// so a substitution that preserves the sum still has to preserve this.
	XOR int64
}

func (d rowDigest) add(o rowDigest) rowDigest {
	return rowDigest{Rows: d.Rows + o.Rows, Hash: d.Hash + o.Hash, XOR: d.XOR ^ o.XOR}
}

// fenceRemaining is how long the current fence may still be held. It
// reports false when there is no fence to bound -- nothing has been fenced
// yet, or the switch is past the journal, where there is no going back and
// so no deadline to enforce.
func (o *pgCutover) fenceRemaining() (time.Duration, bool) {
	if o.wf == nil || o.wf.cutover.FencedAt == nil || o.wf.cutover.JournalID != "" {
		return 0, false
	}
	left := o.c.cutoverTimeout() - o.c.now().Sub(*o.wf.cutover.FencedAt)
	if left <= 0 {
		// Already over: give the scan a moment rather than zero, so the
		// step fails on the deadline check that follows it instead of on a
		// context that was dead before the query was sent.
		return time.Millisecond, true
	}
	return left, true
}

// digest is what verification compares, and it decides whether a cutover
// proceeds -- so what it costs to fool matters.
//
// hashtext is 32 bits, which at a few million rows makes an accidental
// collision unremarkable, and a sum is commutative and additive: any two
// rows swapped for two others of the same total pass unnoticed. Both
// together mean a target missing or holding wrong rows could verify clean,
// which is the worst answer this can give -- it is the one that lets the
// cutover continue.
//
// So: 64 bits instead of 32, and a second aggregate over a different
// algebra. Rows that sum the same generally do not XOR the same, so
// defeating both at once is a much harder accident than defeating either.
// It is still a digest and not a row-by-row comparison; that is the
// resumable VDiff phase in PGS-478, and this is what the fenced check can
// afford until it exists.
func digest(ctx context.Context, conn ShardConn, schema, name, filter string) (rowDigest, error) {
	where := ""
	if filter != "" {
		where = " WHERE (" + filter + ")"
	}
	// Hashed once per row rather than once per aggregate.
	//
	// The sum is taken over the low 31 bits of each hash, not the whole
	// 64: sum(bigint) is numeric in PostgreSQL and a full-width sum runs
	// past int64 after a few million rows, which failed the scan rather
	// than the comparison -- a verification that errors instead of
	// answering. Masking bounds the total at rows x 2^31, which no table
	// this runs on comes near. The sum loses width; bit_xor still carries
	// all 64 bits, and it is the pair that makes a substitution hard.
	sql := fmt.Sprintf(`SELECT count(*), coalesce(sum(h & 2147483647), 0), coalesce(bit_xor(h), 0)
		FROM (SELECT hashtextextended(t::text, 0) AS h FROM %s.%s t%s) s`,
		QuoteIdent(schema), QuoteIdent(name), where)
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return rowDigest{}, err
	}
	return pgx.CollectExactlyOneRow(rows, pgx.RowToStructByPos[rowDigest])
}

// verifyDetail explains a mismatch. The prediction is the sources restricted to
// one target's range, while got is everything that target holds, so the two can
// differ either because the target carries rows that are not its own or because
// the two sides disagree about the range they share. inRange, the target
// measured inside its own range, separates them.
func verifyDetail(got, want, inRange rowDigest) string {
	foreign := got.Rows - inRange.Rows
	switch {
	case inRange == want:
		return fmt.Sprintf("; within its range the target matches, so the difference is %d row(s) it holds outside its range", foreign)
	case foreign != 0:
		return fmt.Sprintf("; within its range the target has %d rows hash %d, and holds %d row(s) outside its range", inRange.Rows, inRange.Hash, foreign)
	default:
		return "; every row the target holds is within its range, so the two sides disagree about that range"
	}
}

// Verify compares, per table and target, what the sources predict for the
// target's ranges with what the target holds. It runs under the fence
// after the targets caught up, so both sides are still.
func (o *pgCutover) Verify(ctx context.Context) (VerifyReport, error) {
	report := VerifyReport{CheckedAt: o.c.now()}
	// The digests are full scans and they run with writes already fenced,
	// so the scan is the write outage. Bounding the pass by what is left of
	// the fence means an oversized table cancels the query and aborts the
	// switch, rather than holding writes for as long as the scan takes.
	// A cancelled scan is a retry; a fence held open is an outage.
	if left, ok := o.fenceRemaining(); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, left)
		defer cancel()
	}
	for _, db := range o.dbs {
		expected := map[string]map[int32]rowDigest{}
		hashes := map[string]string{}
		add := func(table string, t int32, d rowDigest) {
			if expected[table] == nil {
				expected[table] = map[int32]rowDigest{}
			}
			expected[table][t] = expected[table][t].add(d)
		}
		for _, s := range o.srcIDs {
			conn, err := o.c.Shards.DialDatabase(ctx, o.srcSet, s, db.name)
			if err != nil {
				return report, err
			}
			err = func() error {
				for _, tb := range db.sharded {
					key := tb.SchemaName + "." + tb.TableName
					if hashes[key] == "" {
						st, err := describeTable(ctx, conn, tb.SchemaName, tb.TableName, *tb.ShardKey)
						if errors.Is(err, pgx.ErrNoRows) {
							continue
						}
						if err != nil {
							return err
						}
						if hashes[key], err = KeyHashExpr(*tb.ShardKey, st.keyType); err != nil {
							return fatal("%s: %w", tb.TableName, err)
						}
					}
					for i, t := range o.wf.ids {
						d, err := digest(ctx, conn, tb.SchemaName, tb.TableName, RangeFilter(hashes[key], o.wf.ranges[i]))
						if err != nil {
							return err
						}
						add(key, t, d)
					}
				}
				if s != db.home {
					return nil
				}
				shardedNames := map[string]bool{}
				for _, tb := range db.sharded {
					shardedNames[tb.SchemaName+"."+tb.TableName] = true
				}
				refNames := map[string]bool{}
				for _, tb := range db.reference {
					refNames[tb.SchemaName+"."+tb.TableName] = true
				}
				others, err := listTables(ctx, conn)
				if err != nil {
					return err
				}
				homeTarget := o.wf.ids[HomeTarget(o.wf.ranges)]
				for _, ot := range others {
					key := ot.schema + "." + ot.name
					if shardedNames[key] || ot.schema == JournalSchema {
						continue
					}
					d, err := digest(ctx, conn, ot.schema, ot.name, "")
					if err != nil {
						return err
					}
					if refNames[key] {
						for _, t := range o.wf.ids {
							add(key, t, d)
						}
					} else {
						add(key, homeTarget, d)
					}
				}
				return nil
			}()
			_ = conn.Close(ctx)
			if err != nil {
				return report, fmt.Errorf("verify %s on %s/%d: %w", db.name, o.srcSet, s, err)
			}
		}
		for i, t := range o.wf.ids {
			conn, err := o.c.Shards.DialDatabase(ctx, o.wf.set, t, db.name)
			if err != nil {
				return report, err
			}
			err = func() error {
				for _, key := range sortedKeys(expected) {
					want, ok := expected[key][t]
					if !ok {
						continue
					}
					schema, name, _ := strings.Cut(key, ".")
					got, err := digest(ctx, conn, schema, name, "")
					if err != nil {
						return err
					}
					report.Tables++
					report.Rows += got.Rows
					if got != want {
						// The prediction is the sources restricted to this
						// target's range, while the count above is everything
						// the target holds. Measuring the target inside its own
						// range too separates a target carrying rows that are
						// not its own from the two sides genuinely disagreeing
						// about the range they share.
						detail := ""
						if hashes[key] != "" {
							inRange, err := digest(ctx, conn, schema, name, RangeFilter(hashes[key], o.wf.ranges[i]))
							if err != nil {
								return err
							}
							detail = verifyDetail(got, want, inRange)
						}
						// Verify runs under the fence so both sides are
						// still, and a target holding MORE than its sources
						// predict cannot happen while that is true --
						// replication only carries rows the sources have.
						// So read the sources again: if the prediction has
						// moved, they were writing during the verify and
						// the fence was not holding, which is a different
						// bug from the two sides genuinely disagreeing.
						detail += o.sourcesMovedSince(ctx, db, schema, name, hashes[key], i, want)
						report.Mismatches = append(report.Mismatches, fmt.Sprintf("%s.%s on %s/%d: %d rows hash %d/%d, sources predict %d rows hash %d/%d%s",
							db.name, key, o.wf.set, t, got.Rows, got.Hash, got.XOR, want.Rows, want.Hash, want.XOR, detail))
					}
				}
				return nil
			}()
			_ = conn.Close(ctx)
			if err != nil {
				return report, fmt.Errorf("verify %s on %s/%d: %w", db.name, o.wf.set, t, err)
			}
		}
	}
	return report, nil
}

// sourcesMovedSince re-reads the prediction for one table and target after a
// mismatch, and says whether it changed. It runs only on the failing path,
// where one more read is worth having: a prediction that moves between the
// first read and this one is a fence that did not hold, and a prediction
// that stands is the target and its sources genuinely disagreeing. The
// difference decides which bug is being looked at, and the status of a
// failed cutover is all anyone gets afterwards.
func (o *pgCutover) sourcesMovedSince(ctx context.Context, db dbPlan, schema, name, hash string, i int, was rowDigest) string {
	if hash == "" {
		return ""
	}
	var now rowDigest
	for _, s := range o.srcIDs {
		conn, err := o.c.Shards.DialDatabase(ctx, o.srcSet, s, db.name)
		if err != nil {
			return fmt.Sprintf("; the sources could not be re-read (%v), so whether they moved during the verify is unknown", err)
		}
		d, err := digest(ctx, conn, schema, name, RangeFilter(hash, o.wf.ranges[i]))
		_ = conn.Close(ctx)
		if err != nil {
			return fmt.Sprintf("; the sources could not be re-read (%v), so whether they moved during the verify is unknown", err)
		}
		now = now.add(d)
	}
	return sourcesMovedMessage(now, was)
}

// sourcesMovedMessage names which of the two the re-read showed.
func sourcesMovedMessage(now, was rowDigest) string {
	if now == was {
		return "; the sources read the same again, so they were still: the target and its sources disagree"
	}
	return fmt.Sprintf("; the sources now predict %d rows hash %d, so they were still being written during the verify and the fence did not hold", now.Rows, now.Hash)
}

// Reverse creates, per database, a publication per source on every target
// (the source's ranges) plus the home publication on the home target, and a
// disabled subscription per (source, target) pair on every source.
func (o *pgCutover) Reverse(ctx context.Context) error {
	homeTarget := o.wf.ids[HomeTarget(o.wf.ranges)]
	for _, db := range o.dbs {
		for _, t := range o.wf.ids {
			conn, err := o.c.Shards.DialDatabase(ctx, o.wf.set, t, db.name)
			if err != nil {
				return err
			}
			err = o.reversePublishOn(ctx, conn, db, t == homeTarget)
			_ = conn.Close(ctx)
			if err != nil {
				return fmt.Errorf("reverse publications of %s on %s/%d: %w", db.name, o.wf.set, t, err)
			}
		}
		for _, s := range o.srcIDs {
			conn, err := o.c.Shards.DialDatabase(ctx, o.srcSet, s, db.name)
			if err != nil {
				return err
			}
			err = o.reverseSubscribeOn(ctx, conn, db, s, homeTarget)
			_ = conn.Close(ctx)
			if err != nil {
				return fmt.Errorf("reverse subscriptions of %s on %s/%d: %w", db.name, o.srcSet, s, err)
			}
		}
	}
	return nil
}

// schemaFingerprintSQL hashes the user-visible structure of one database:
// columns, constraints and indexes outside the system and pgshard schemas.
// It is only ever compared with an earlier hash of the SAME set, so the two
// sides of an upgrade never have to render identically across majors.
const schemaFingerprintSQL = `SELECT coalesce(md5(string_agg(line, E'\n' ORDER BY line)), '')
FROM (
    SELECT 'col ' || n.nspname || ' ' || c.relname || ' ' || c.relkind::text || ' ' || a.attname
        || ' ' || format_type(a.atttypid, a.atttypmod) || ' ' || a.attnotnull::text AS line
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
     WHERE c.relkind IN ('r', 'p', 'm', 'v')
       AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pgshard')
       AND n.nspname NOT LIKE 'pg\_toast%'
    UNION ALL
    SELECT 'con ' || n.nspname || ' ' || r.relname || ' ' || k.conname || ' ' || pg_get_constraintdef(k.oid)
      FROM pg_constraint k
      JOIN pg_class r ON r.oid = k.conrelid
      JOIN pg_namespace n ON n.oid = r.relnamespace
     WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pgshard')
    UNION ALL
    SELECT 'idx ' || schemaname || ' ' || indexname || ' ' || indexdef
      FROM pg_indexes
     WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pgshard')
) parts`

// scalarString runs a query that returns exactly one text value.
func scalarString(ctx context.Context, conn ShardConn, sql string) (string, error) {
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", err
		}
		return "", errors.New("no row")
	}
	var out string
	if err := rows.Scan(&out); err != nil {
		return "", err
	}
	rows.Close()
	return out, rows.Err()
}

// schemaKey names one fingerprint: a database on one shard of one set.
func schemaKey(set string, id int32, db string) string {
	return fmt.Sprintf("%s/%d/%s", set, id, db)
}

// SchemaFingerprints hashes every database on both sets. Logical replication
// does not carry DDL, so an ALTER applied after the switch reaches only the
// serving set; comparing these at rollback is what stops the sources being
// switched back structurally stale.
func (o *pgCutover) SchemaFingerprints(ctx context.Context) (map[string]string, error) {
	out := map[string]string{}
	sets := []struct {
		name string
		ids  []int32
	}{{o.srcSet, o.srcIDs}, {o.wf.set, o.wf.ids}}
	for _, set := range sets {
		for _, db := range o.dbs {
			for _, id := range set.ids {
				conn, err := o.c.Shards.DialDatabase(ctx, set.name, id, db.name)
				if err != nil {
					return nil, err
				}
				fp, err := scalarString(ctx, conn, schemaFingerprintSQL)
				_ = conn.Close(ctx)
				if err != nil {
					return nil, fmt.Errorf("schema fingerprint of %s on %s/%d: %w", db.name, set.name, id, err)
				}
				out[schemaKey(set.name, id, db.name)] = fp
			}
		}
	}
	return out, nil
}

// schemaDrift lists the databases whose structure has changed since the
// switch. A key that was never recorded is not drift: runs switched before
// fingerprints were captured have nothing to compare against.
func (o *pgCutover) schemaDrift(ctx context.Context) ([]string, error) {
	want := o.wf.cutover.Schema
	if len(want) == 0 {
		return nil, nil
	}
	now, err := o.SchemaFingerprints(ctx)
	if err != nil {
		return nil, err
	}
	var drifted []string
	for key, before := range want {
		after, ok := now[key]
		if !ok {
			continue
		}
		if after != before {
			drifted = append(drifted, key)
		}
	}
	sort.Strings(drifted)
	return drifted, nil
}

func (o *pgCutover) reversePublishOn(ctx context.Context, conn ShardConn, db dbPlan, home bool) error {
	existing, err := existingPublications(ctx, conn, o.wf.gen)
	if err != nil {
		return err
	}
	want := map[string][]PublishedTable{}
	var sharded []struct {
		table       catalog.Table
		hash        string
		partitioned bool
	}
	for _, tb := range db.sharded {
		st, err := describeTable(ctx, conn, tb.SchemaName, tb.TableName, *tb.ShardKey)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		hash, err := KeyHashExpr(*tb.ShardKey, st.keyType)
		if err != nil {
			return fatal("%s: %w", tb.TableName, err)
		}
		if !st.replIdentOK {
			if err := setReplicaIdentityFull(ctx, conn, tb.SchemaName, tb.TableName); err != nil {
				return err
			}
		}
		sharded = append(sharded, struct {
			table       catalog.Table
			hash        string
			partitioned bool
		}{tb, hash, st.partitioned})
	}
	for i, s := range o.srcIDs {
		var tables []PublishedTable
		for _, sp := range sharded {
			tables = append(tables, PublishedTable{Schema: sp.table.SchemaName, Name: sp.table.TableName, Filter: RangeFilter(sp.hash, o.srcRanges[i]), Partitioned: sp.partitioned})
		}
		want[ReversePublicationName(o.wf.gen, s)] = tables
	}
	if home {
		shardedNames := map[string]bool{}
		for _, tb := range db.sharded {
			shardedNames[tb.SchemaName+"."+tb.TableName] = true
		}
		others, err := listTables(ctx, conn)
		if err != nil {
			return err
		}
		var tables []PublishedTable
		for _, ot := range others {
			if shardedNames[ot.schema+"."+ot.name] || ot.schema == JournalSchema {
				continue
			}
			if !ot.replIdentOK {
				if err := setReplicaIdentityFull(ctx, conn, ot.schema, ot.name); err != nil {
					return err
				}
			}
			tables = append(tables, PublishedTable{Schema: ot.schema, Name: ot.name, Partitioned: ot.partitioned})
		}
		want[ReverseHomePublicationName(o.wf.gen)] = tables
	}
	for _, name := range sortedKeys(want) {
		if existing[name] {
			continue
		}
		if _, err := conn.Exec(ctx, CreatePublicationSQL(name, want[name])); err != nil {
			return err
		}
	}
	return nil
}

func (o *pgCutover) reverseSubscribeOn(ctx context.Context, conn ShardConn, db dbPlan, s, homeTarget int32) error {
	rows, err := conn.Query(ctx, `SELECT subname FROM pg_subscription WHERE subname LIKE $1`, o.reversePattern(s))
	if err != nil {
		return err
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	existing := map[string]bool{}
	for _, n := range names {
		existing[n] = true
	}
	for _, t := range o.wf.ids {
		name := ReverseSubscriptionName(o.wf.gen, s, t)
		if existing[name] {
			continue
		}
		pubs := []string{ReversePublicationName(o.wf.gen, s)}
		if s == db.home && t == homeTarget {
			pubs = append(pubs, ReverseHomePublicationName(o.wf.gen))
		}
		conninfo, err := o.c.SourceConnInfo(ctx, ShardRef{Set: o.wf.set, ID: t}, db.name)
		if err != nil {
			return err
		}
		cctx, cancel := context.WithTimeout(ctx, createSubscriptionTimeout)
		_, err = conn.Exec(cctx, CreateReverseSubscriptionSQL(name, conninfo, pubs, SubscriptionOptions{Slot: name, Failover: !o.c.SlotFailoverDisabled}))
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

// CreateReverseSubscriptionSQL renders the reverse subscription: no initial
// copy, a slot created now, disabled until the flip, and only rows the
// publisher originated (origin = none) so forwarded rows never loop back.
func CreateReverseSubscriptionSQL(name, conninfo string, publications []string, opts SubscriptionOptions) string {
	pubs := make([]string, 0, len(publications))
	for _, p := range publications {
		pubs = append(pubs, QuoteIdent(p))
	}
	with := []string{"copy_data = false", "create_slot = true", "enabled = false", "streaming = parallel", "two_phase = false", "origin = none",
		"slot_name = " + quoteLiteral(opts.Slot)}
	if opts.Failover {
		with = append(with, "failover = true")
	}
	return fmt.Sprintf("CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s WITH (%s)",
		QuoteIdent(name), quoteLiteral(conninfo), strings.Join(pubs, ", "), strings.Join(with, ", "))
}

// journalDDL creates the journal table of a user database.
var journalDDL = []string{
	"CREATE SCHEMA IF NOT EXISTS " + JournalSchema,
	fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
		id uuid NOT NULL, generation bigint NOT NULL, source_shard integer NOT NULL,
		participants integer[] NOT NULL, targets jsonb NOT NULL DEFAULT '{}'::jsonb,
		created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (id, source_shard))`, JournalSchema, JournalTable),
}

// Journal writes the journal row into every user database of every source
// and into the catalog. Its id is allocated before the first attempt, so a
// repeated step finds its rows in place.
func (o *pgCutover) Journal(ctx context.Context, id string) error {
	targets := map[string]int64{}
	for _, t := range o.wf.ids {
		lsn, err := o.currentLSN(ctx, o.wf.set, t)
		if err != nil {
			return err
		}
		targets[fmt.Sprint(t)] = lsn
	}
	for _, db := range o.dbs {
		for _, s := range o.srcIDs {
			conn, err := o.c.Shards.DialDatabase(ctx, o.srcSet, s, db.name)
			if err != nil {
				return err
			}
			err = func() error {
				for _, ddl := range journalDDL {
					if _, err := conn.Exec(ctx, ddl); err != nil {
						return err
					}
				}
				// The targets are updated rather than left alone: a flip
				// that found the sources had advanced rewinds to re-carry
				// the sequences and comes back through here, and the
				// journal has to describe the attempt that actually
				// flipped. Leaving the first attempt's positions in place
				// starts a consumer that repositions off the journal
				// before the cutover it is repositioning to.
				_, err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.%s (id, generation, source_shard, participants, targets)
					VALUES ($1::uuid, $2, $3, $4, $5::jsonb)
					ON CONFLICT (id, source_shard) DO UPDATE SET targets = EXCLUDED.targets`, JournalSchema, JournalTable),
					id, o.wf.gen, s, o.srcIDs, mustJSON(targets))
				return err
			}()
			_ = conn.Close(ctx)
			if err != nil {
				return fmt.Errorf("journal of %s on %s/%d: %w", db.name, o.srcSet, s, err)
			}
		}
	}
	tx, err := o.c.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `INSERT INTO pgshard.resharding_journal (id, generation, shard_set, participants, targets)
		VALUES ($1::uuid, $2, $3, $4, $5::jsonb)
		ON CONFLICT (id) DO UPDATE SET targets = EXCLUDED.targets`, id, o.wf.gen, o.wf.set, o.srcIDs, mustJSON(targets)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE pgshard.workflows SET journal_ids = array_append(array_remove(journal_ids, $2), $2) WHERE id = $1::uuid`, o.wf.id, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Flip publishes the new map in one catalog transaction: targets serving,
// sources retired, the target set serving and published, every database's
// home shard moved to the home target, and the shard-map generation bumped
// so poolers refuse stale routing. A flip that already happened is skipped.
func (o *pgCutover) Flip(ctx context.Context, _ string) error {
	tx, err := o.c.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var state string
	if err := tx.QueryRow(ctx, `SELECT state FROM pgshard.shard_sets WHERE shard_set = $1 FOR UPDATE`, o.wf.set).Scan(&state); err != nil {
		return err
	}
	if state == catalog.ShardSetServing {
		return nil
	}
	// The workflow froze its source when it was created. Retiring a set
	// that is no longer the only one serving would publish a second serving
	// set and drop whatever was committed to the other one.
	if err := o.stillSoleServing(ctx, tx, o.srcSet); err != nil {
		return err
	}
	homeTarget := o.wf.ids[HomeTarget(o.wf.ranges)]
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{`UPDATE pgshard.shard_status SET serving_state = $2, migrating = false, migrating_by = NULL, updated_at = now() WHERE shard_set = $1`, []any{o.wf.set, ServingServing}},
		{`UPDATE pgshard.shard_status SET serving_state = $2, updated_at = now() WHERE shard_set = $1`, []any{o.srcSet, ServingRetired}},
		{`UPDATE pgshard.shard_sets SET state = $2, updated_at = now() WHERE shard_set = $1`, []any{o.wf.set, catalog.ShardSetServing}},
		{`UPDATE pgshard.shard_sets SET state = $2, updated_at = now() WHERE shard_set = $1`, []any{o.srcSet, catalog.ShardSetRetired}},
		{`INSERT INTO pgshard.serving (shard_set, generation)
			SELECT $1, coalesce(max(desired_generation), 0) FROM pgshard.shard_ranges WHERE shard_set = $1
			ON CONFLICT (shard_set) DO UPDATE SET generation = EXCLUDED.generation, published_at = now()`, []any{o.wf.set}},
		{`UPDATE pgshard.databases SET home_shard = $1 WHERE home_shard <> $1`, []any{homeTarget}},
		{`UPDATE pgshard.shard_map_generation SET generation = generation + 1, updated_at = now()`, nil},
	} {
		if _, err := tx.Exec(ctx, q.sql, q.args...); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// stillSoleServing refuses to retire a set on behalf of a workflow whose
// idea of what is serving is out of date: another workflow flipped
// underneath it, and publishing on top would leave two serving sets and
// drop whatever was committed to the other one.
func (o *pgCutover) stillSoleServing(ctx context.Context, tx pgx.Tx, set string) error {
	rows, err := tx.Query(ctx, `SELECT shard_set FROM pgshard.shard_sets WHERE state = $1 ORDER BY shard_set`, catalog.ShardSetServing)
	if err != nil {
		return err
	}
	serving, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	if len(serving) == 1 && serving[0] == set {
		return nil
	}
	if !slices.Contains(serving, set) {
		// Retired sets are never returned to serving, so this switch can
		// never publish on top of one: it is over, not waiting.
		return sourceRetired("%s no longer serves; another workflow published %v while this one was switching", set, serving)
	}
	return fmt.Errorf("%s is no longer the only serving shard set (serving: %v)", set, serving)
}

// Swap freezes the forward direction (disable; dropped on complete) and
// starts the reverse one.
// DisableForward stops the forward subscriptions once the targets hold
// everything the sources have.
func (o *pgCutover) DisableForward(ctx context.Context) error {
	for _, db := range o.dbs {
		for _, t := range o.wf.ids {
			conn, err := o.c.Shards.DialDatabase(ctx, o.wf.set, t, db.name)
			if err != nil {
				return err
			}
			err = alterSubscriptions(ctx, conn, o.forwardPattern(t), true, "DISABLE")
			_ = conn.Close(ctx)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// EnableReverse starts the reverse subscriptions, which keep the sources
// current for a rollback. Their apply workers write to the sources, so this
// runs after the write pause is lifted.
func (o *pgCutover) EnableReverse(ctx context.Context) error {
	for _, db := range o.dbs {
		for _, s := range o.srcIDs {
			conn, err := o.c.Shards.DialDatabase(ctx, o.srcSet, s, db.name)
			if err != nil {
				return err
			}
			err = alterSubscriptions(ctx, conn, o.reversePattern(s), false, "ENABLE")
			_ = conn.Close(ctx)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// PauseSources flips default_transaction_read_only on every source primary,
// so a router that has not yet seen the fence cannot commit to one while
// the swap decides the targets are caught up. Reads, and the replication
// the swap itself depends on, are unaffected.
//
// Pausing is confirmed on a fresh backend: the reload is asynchronous, and
// a pause that is not yet in force is the same as no pause at all.
func (o *pgCutover) PauseSources(ctx context.Context, pause bool) error {
	return o.pauseSet(ctx, o.srcSet, o.srcIDs, pause)
}

// pauseSet flips default_transaction_read_only on every primary of a set.
func (o *pgCutover) pauseSet(ctx context.Context, set string, ids []int32, pause bool) error {
	stmt := `ALTER SYSTEM RESET default_transaction_read_only`
	if pause {
		stmt = `ALTER SYSTEM SET default_transaction_read_only = on`
	}
	for _, s := range ids {
		if err := o.setReadOnly(ctx, set, s, stmt, pause); err != nil {
			return fmt.Errorf("write pause on %s/%d: %w", set, s, err)
		}
	}
	return nil
}

// drainWriters waits until no client backend on the set still holds a
// write transaction. default_transaction_read_only is read when a
// transaction starts, so one that began before the pause can still commit
// after it; the pause alone stops new writers, not the ones already in
// flight.
func (o *pgCutover) drainWriters(ctx context.Context, set string, ids []int32) error {
	deadline := o.c.now().Add(writerDrainTimeout)
	for {
		busy, err := o.writingBackends(ctx, set, ids)
		if err != nil {
			return err
		}
		if busy == 0 {
			return nil
		}
		if o.c.now().After(deadline) {
			return fmt.Errorf("%d write transactions on %s still open after %s", busy, set, writerDrainTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// writingBackends counts client backends holding a transaction that has
// written. A transaction is assigned an xid the first time it writes, so
// backend_xid is exactly the question; readers and the replication workers
// carrying the rollback are not counted.
func (o *pgCutover) writingBackends(ctx context.Context, set string, ids []int32) (int, error) {
	total := 0
	for _, s := range ids {
		conn, err := o.c.Shards.Dial(ctx, set, s)
		if err != nil {
			return 0, err
		}
		rows, err := conn.Query(ctx, `SELECT count(*)::int FROM pg_stat_activity
			WHERE backend_xid IS NOT NULL AND backend_type = 'client backend' AND pid <> pg_backend_pid()`)
		var n int
		if err == nil {
			n, err = pgx.CollectExactlyOneRow(rows, pgx.RowTo[int])
		}
		_ = conn.Close(ctx)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

// writerDrainTimeout bounds the wait for in-flight write transactions to
// end once new ones are refused.
const writerDrainTimeout = 30 * time.Second

func (o *pgCutover) setReadOnly(ctx context.Context, set string, s int32, stmt string, pause bool) error {
	conn, err := o.c.Shards.Dial(ctx, set, s)
	if err != nil {
		return err
	}
	// ALTER SYSTEM cannot run inside a transaction block, so it and the
	// reload go as separate statements.
	_, err = conn.Exec(ctx, stmt)
	if err == nil {
		_, err = conn.Exec(ctx, `SELECT pg_reload_conf()`)
	}
	_ = conn.Close(ctx)
	if err != nil || !pause {
		return err
	}
	deadline := o.c.now().Add(pauseConfirmTimeout)
	for {
		on, err := o.refusesWrites(ctx, set, s)
		if err != nil {
			return err
		}
		if on {
			return nil
		}
		if o.c.now().After(deadline) {
			return fmt.Errorf("did not start refusing writes within %s", pauseConfirmTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// sourceRefusesWrites asks a backend started after the reload, since an
// existing one may have been running before the setting changed.
func (o *pgCutover) refusesWrites(ctx context.Context, set string, s int32) (bool, error) {
	conn, err := o.c.Shards.Dial(ctx, set, s)
	if err != nil {
		return false, err
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, `SELECT current_setting('default_transaction_read_only') = 'on'`)
	if err != nil {
		return false, err
	}
	return pgx.CollectExactlyOneRow(rows, pgx.RowTo[bool])
}

// alterSubscriptions applies verb to the subscriptions matching pattern
// whose enabled flag equals enabled.
func alterSubscriptions(ctx context.Context, conn ShardConn, pattern string, enabled bool, verb string) error {
	rows, err := conn.Query(ctx, `SELECT subname FROM pg_subscription WHERE subname LIKE $1 AND subenabled = $2 ORDER BY 1`, pattern, enabled)
	if err != nil {
		return err
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	for _, n := range names {
		if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER SUBSCRIPTION %s %s", QuoteIdent(n), verb)); err != nil {
			return err
		}
	}
	return nil
}

// Release drops the range fence and the DDL locks.
func (o *pgCutover) Release(ctx context.Context) error {
	tx, err := o.c.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Only the workflow that raised the fence may lift it.
	if _, err := tx.Exec(ctx, `UPDATE pgshard.shard_status SET migrating = false, migrating_by = NULL, updated_at = now()
		WHERE shard_set = $1 AND migrating AND migrating_by = $2::uuid`, o.srcSet, o.wf.id); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM pgshard.workflow_locks WHERE workflow_id = $1::uuid`, o.wf.id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Unwind undoes what a started cutover left on the SOURCES, and nothing else:
// it lifts this workflow's write fence, clears its lock rows, and drops the
// reverse subscriptions it created. It never touches the targets, so it works
// when they are gone -- which is exactly when it is needed, because Complete
// requires them and a cancelled reshard is usually one whose targets have been
// deleted. Sources that are themselves unreachable are skipped, as Complete
// does: their objects went with them.
func (o *pgCutover) Unwind(ctx context.Context) error {
	for _, db := range o.dbs {
		for _, s := range o.srcIDs {
			conn, err := o.c.Shards.DialDatabase(ctx, o.srcSet, s, db.name)
			if err != nil {
				o.c.logger().Info("reshard cancel: source unreachable, skipping its reverse subscriptions", "workflow", o.wf.id, "source", s, "err", err)
				continue
			}
			err = dropSubscriptionsLike(ctx, conn, o.reversePattern(s))
			_ = conn.Close(ctx)
			if err != nil {
				return err
			}
		}
	}
	// Last, so a failure above leaves the fence in place rather than opening
	// writes onto a source whose reverse replication is still attached.
	return o.Release(ctx)
}

// Complete drops the reverse subscriptions on the sources, the reverse
// slots and publications on the targets, the frozen forward subscriptions
// on the targets and the forward slots and publications on the sources, and
// unpublishes the retired set. Sources that are already gone are skipped.
// DropJournal removes this run's journal rows from every source database.
// A source that cannot be reached is skipped and reported: its rows point
// at a set that will never serve, but the alternative is a workflow that
// cannot end while one source is down.
func (o *pgCutover) DropJournal(ctx context.Context, id string) error {
	if id == "" {
		return nil
	}
	for _, db := range o.dbs {
		for _, src := range o.srcIDs {
			conn, err := o.c.Shards.DialDatabase(ctx, o.srcSet, src, db.name)
			if err != nil {
				o.c.logger().Info("abandoned switch: source unreachable, its journal rows stay", "workflow", o.wf.id, "source", src, "err", err)
				continue
			}
			_, err = conn.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.%s WHERE id = $1::uuid`, JournalSchema, JournalTable), id)
			_ = conn.Close(ctx)
			if err != nil && !missingObject(err) {
				return err
			}
		}
	}
	return nil
}

func (o *pgCutover) Complete(ctx context.Context) error {
	for _, db := range o.dbs {
		for _, s := range o.srcIDs {
			conn, err := o.c.Shards.DialDatabase(ctx, o.srcSet, s, db.name)
			if err != nil {
				o.c.logger().Info("reshard complete: source unreachable, skipping its reverse subscriptions", "workflow", o.wf.id, "source", s, "err", err)
				continue
			}
			err = dropSubscriptionsLike(ctx, conn, o.reversePattern(s))
			_ = conn.Close(ctx)
			if err != nil {
				return err
			}
		}
		for _, t := range o.wf.ids {
			conn, err := o.c.Shards.DialDatabase(ctx, o.wf.set, t, db.name)
			if err != nil {
				return err
			}
			err = dropSubscriptionsLike(ctx, conn, o.forwardPattern(t))
			if err == nil {
				err = dropPublications(ctx, conn, o.wf.gen)
			}
			_ = conn.Close(ctx)
			if err != nil {
				return err
			}
		}
	}
	for _, t := range o.wf.ids {
		conn, err := o.c.Shards.Dial(ctx, o.wf.set, t)
		if err != nil {
			return err
		}
		err = dropSlots(ctx, conn, o.wf.gen)
		_ = conn.Close(ctx)
		if err != nil {
			return err
		}
	}
	for _, s := range o.srcIDs {
		conn, err := o.c.Shards.Dial(ctx, o.srcSet, s)
		if err != nil {
			o.c.logger().Info("reshard complete: source unreachable, skipping its slots and publications", "workflow", o.wf.id, "source", s, "err", err)
			continue
		}
		err = dropSlots(ctx, conn, o.wf.gen)
		_ = conn.Close(ctx)
		if err != nil {
			return err
		}
		for _, db := range o.dbs {
			conn, err := o.c.Shards.DialDatabase(ctx, o.srcSet, s, db.name)
			if err != nil {
				return err
			}
			err = dropPublications(ctx, conn, o.wf.gen)
			_ = conn.Close(ctx)
			if err != nil {
				return err
			}
		}
	}
	if _, err := o.c.Pool.Exec(ctx, `DELETE FROM pgshard.serving WHERE shard_set = $1`, o.srcSet); err != nil {
		return err
	}
	// The retired set now has no replication in either direction and will
	// never serve again, but its pods stay up for the retirement window
	// and its -rw Service still answers. A client connected straight to it
	// -- which is not the supported path, but is reachable -- would have
	// its writes acknowledged by a primary nothing reads from again, and
	// lose them at deletion with no error anywhere. Refusing is the
	// difference between losing data and being told no.
	//
	// Which set that is has to be read, not assumed. A run that was rolled
	// back completes with the roles the other way round: its source set is
	// serving again and its targets are the retired ones. Pausing the
	// workflow's source set unconditionally made the live set read-only,
	// and the next upgrade could not so much as create a publication on
	// it.
	set, ids := o.srcSet, o.srcIDs
	serving, err := o.setIsServing(ctx, set)
	if err != nil {
		o.c.logger().Info("reshard complete: serving state unreadable, leaving the sets writable", "workflow", o.wf.id, "err", err)
		return nil
	}
	if serving {
		set, ids = o.wf.set, o.wf.ids
		if serving, err := o.setIsServing(ctx, set); err != nil || serving {
			// Neither is retired, or the answer is not readable. Making a
			// serving set read-only is worse than leaving a retired one
			// writable, so do neither.
			return nil
		}
	}
	// Best effort, and last: a set that cannot be reached is already
	// beyond reach of a client too, and a retirement that has otherwise
	// finished must not be undone by it.
	if err := o.pauseSet(ctx, set, ids, true); err != nil {
		o.c.logger().Info("reshard complete: retired set not made read-only", "workflow", o.wf.id, "set", set, "err", err)
	}
	return nil
}

// setIsServing reports whether a shard set is the one routing goes to.
func (o *pgCutover) setIsServing(ctx context.Context, set string) (bool, error) {
	var state string
	if err := o.c.Pool.QueryRow(ctx, `SELECT state FROM pgshard.shard_sets WHERE shard_set = $1`, set).Scan(&state); err != nil {
		return false, err
	}
	return state == catalog.ShardSetServing, nil
}
