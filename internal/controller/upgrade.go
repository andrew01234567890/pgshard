package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/pgsequence"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// upgradePreconditions runs the automatic checks of docs/upgrade.md before an
// upgrade workflow starts copying. A violation fails the workflow with a
// message that names every failed check.
func (c *Copier) upgradePreconditions(ctx context.Context, wf *copyWorkflow, srcSet string, srcIDs []int32, dbs []dbPlan) error {
	if len(wf.ids) == 0 || len(srcIDs) == 0 {
		return fatal("upgrade %s: no shards to check", wf.set)
	}
	var failures []string
	// Every source shard and every database, against every target shard.
	// Checking one pair passed preflight on a cluster whose other shard
	// had the extension and whose other database did not -- and the
	// upgrade then failed during schema restore, which is after the
	// target groups have been provisioned and is the expensive place to
	// find out. Extensions are per-database objects; availability is per
	// installation, so an extension has to be available on all of the
	// target shards, not the one that happened to be first.
	available, err := c.commonTargetExtensions(ctx, wf.set, wf.ids)
	if err != nil {
		return err
	}
	for _, db := range dbs {
		for _, s := range srcIDs {
			installed, err := c.installedExtensions(ctx, srcSet, s, db.name)
			if err != nil {
				return err
			}
			if missing := MissingExtensions(installed, available); len(missing) > 0 {
				failures = append(failures, fmt.Sprintf("extensions missing on the target major for %s on %s/%d: %s",
					db.name, srcSet, s, strings.Join(missing, ", ")))
			}
		}
	}
	for _, db := range dbs {
		for _, s := range srcIDs {
			n, err := c.largeObjectCount(ctx, srcSet, s, db.name)
			if err != nil {
				return err
			}
			if n > 0 {
				failures = append(failures, fmt.Sprintf("%d large objects in %s on %s/%d: logical replication does not carry pg_largeobject, use the offline strategy", n, db.name, srcSet, s))
			}
		}
	}
	var placements int
	if err := c.Pool.QueryRow(ctx, `SELECT count(*) FROM pgshard.workflows WHERE kind = $1 AND state = ANY($2)`, KindTablePlacement, activeStates).Scan(&placements); err != nil {
		return err
	}
	if placements > 0 {
		failures = append(failures, fmt.Sprintf("%d table placement workflow(s) in flight", placements))
	}
	if len(failures) > 0 {
		return fatal("upgrade preconditions failed: %s", strings.Join(failures, "; "))
	}
	return nil
}

// commonTargetExtensions is what every target shard can offer: an
// extension available on one of them and not another would install on some
// shards and fail on the rest, which is the same outcome as missing.
func (c *Copier) commonTargetExtensions(ctx context.Context, tgtSet string, tgtIDs []int32) ([]string, error) {
	var common []string
	for i, id := range tgtIDs {
		available, err := c.availableExtensions(ctx, tgtSet, id)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			common = available
			continue
		}
		common = intersect(common, available)
	}
	return common, nil
}

// intersect returns the names present in both, keeping a's order.
func intersect(a, b []string) []string {
	have := make(map[string]bool, len(b))
	for _, x := range b {
		have[x] = true
	}
	var out []string
	for _, x := range a {
		if have[x] {
			out = append(out, x)
		}
	}
	return out
}

// MissingExtensions returns the installed extensions absent from available,
// sorted; plpgsql and pgcrypto ship with every supported major and are
// checked like any other name.
func MissingExtensions(installed, available []string) []string {
	have := map[string]bool{}
	for _, a := range available {
		have[a] = true
	}
	var missing []string
	for _, e := range installed {
		if !have[e] && !slices.Contains(missing, e) {
			missing = append(missing, e)
		}
	}
	slices.Sort(missing)
	return missing
}

// installedExtensions lists one database's extensions. They are
// per-database objects, so the default database's list says nothing about
// the others.
func (c *Copier) installedExtensions(ctx context.Context, set string, id int32, database string) ([]string, error) {
	conn, err := c.Shards.DialDatabase(ctx, set, id, database)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, `SELECT DISTINCT extname FROM pg_extension ORDER BY 1`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func (c *Copier) availableExtensions(ctx context.Context, set string, id int32) ([]string, error) {
	conn, err := c.Shards.Dial(ctx, set, id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, `SELECT name FROM pg_available_extensions ORDER BY 1`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func (c *Copier) largeObjectCount(ctx context.Context, set string, id int32, database string) (int64, error) {
	conn, err := c.Shards.DialDatabase(ctx, set, id, database)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, `SELECT count(*) FROM pg_largeobject_metadata`)
	if err != nil {
		return 0, err
	}
	return pgx.CollectExactlyOneRow(rows, pgx.RowTo[int64])
}

// Sequences carries every user-database sequence position from the sources
// to the targets: max(last_value) across the sources, setval with is_called
// on every target that holds the sequence. It runs inside the fence, after
// the sweep, so no source nextval can race it.
func (o *pgCutover) Sequences(ctx context.Context) (string, error) {
	return o.syncSequences(ctx, o.srcSet, o.srcIDs, o.wf.set, o.wf.ids)
}

// SequenceFingerprint reads the sources without writing anything, so the
// flip can ask whether a sequence advanced since the carry.
func (o *pgCutover) SequenceFingerprint(ctx context.Context) (string, error) {
	values, err := o.sourceSequences(ctx, o.srcSet, o.srcIDs)
	if err != nil {
		return "", err
	}
	return sequenceFingerprint(values), nil
}

// sourceSequences is the merged sequence position of every source, per
// database.
func (o *pgCutover) sourceSequences(ctx context.Context, fromSet string, fromIDs []int32) (map[string]map[string]pgsequence.Value, error) {
	out := map[string]map[string]pgsequence.Value{}
	for _, db := range o.dbs {
		values := map[string]pgsequence.Value{}
		for _, s := range fromIDs {
			conn, err := o.c.Shards.DialDatabase(ctx, fromSet, s, db.name)
			if err != nil {
				return nil, err
			}
			from, err := pgsequence.Snapshot(ctx, conn, []string{"pgshard", JournalSchema})
			_ = conn.Close(ctx)
			if err != nil {
				return nil, fmt.Errorf("sequences of %s on %s/%d: %w", db.name, fromSet, s, err)
			}
			pgsequence.Merge(values, from)
		}
		out[db.name] = values
	}
	return out, nil
}

// sequenceFingerprint renders a sequence snapshot as one comparable string.
// Every field the carry would apply is in it, so a value that would be
// carried differently is a fingerprint that differs.
func sequenceFingerprint(values map[string]map[string]pgsequence.Value) string {
	h := sha256.New()
	for _, db := range slices.Sorted(maps.Keys(values)) {
		fmt.Fprintf(h, "%s\n", db)
		for _, name := range slices.Sorted(maps.Keys(values[db])) {
			v := values[db][name]
			fmt.Fprintf(h, "%s=%d:%t\n", name, v.At, v.Ascending)
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (o *pgCutover) syncSequences(ctx context.Context, fromSet string, fromIDs []int32, toSet string, toIDs []int32) (string, error) {
	all, err := o.sourceSequences(ctx, fromSet, fromIDs)
	if err != nil {
		return "", err
	}
	for _, db := range o.dbs {
		values := all[db.name]
		if len(values) == 0 {
			continue
		}
		for _, t := range toIDs {
			conn, err := o.c.Shards.DialDatabase(ctx, toSet, t, db.name)
			if err != nil {
				return "", err
			}
			err = pgsequence.Apply(ctx, conn, values)
			_ = conn.Close(ctx)
			if err != nil {
				return "", fmt.Errorf("sequences of %s on %s/%d: %w", db.name, toSet, t, err)
			}
		}
	}
	return sequenceFingerprint(all), nil
}

// Rollback returns serving to the source set of a switched run. It is
// idempotent and retried: fence both sets, wait until every reverse
// subscription passed the targets' positions (errRetry while behind), carry
// the sequences back, flip the serving map to the sources and release. A
// run whose sources already serve only releases the fences.
func (o *pgCutover) Rollback(ctx context.Context) error {
	var state string
	if err := o.c.Pool.QueryRow(ctx, `SELECT state FROM pgshard.shard_sets WHERE shard_set = $1`, o.srcSet).Scan(&state); err != nil {
		return err
	}
	if state == catalog.ShardSetServing {
		return o.releaseRollback(ctx)
	}
	// Claim the fence on both sets, as the forward cutover does: a fence
	// with no owner is one any workflow can drop, and the release below
	// only lifts what this workflow raised.
	if _, err := o.c.Pool.Exec(ctx, `UPDATE pgshard.shard_status SET migrating = true, migrating_by = $2::uuid, updated_at = now()
		WHERE shard_set = ANY($1) AND (NOT migrating OR migrating_by = $2::uuid)`, []string{o.srcSet, o.wf.set}, o.wf.id); err != nil {
		return err
	}
	var heldByOther int
	if err := o.c.Pool.QueryRow(ctx, `SELECT count(*)::int FROM pgshard.shard_status
		WHERE shard_set = ANY($1) AND migrating AND migrating_by IS DISTINCT FROM $2::uuid`, []string{o.srcSet, o.wf.set}, o.wf.id).Scan(&heldByOther); err != nil {
		return err
	}
	if heldByOther > 0 {
		return fmt.Errorf("another cutover holds the write fence on %s or %s", o.srcSet, o.wf.set)
	}
	// Logical replication carries no DDL, so anything applied since the
	// switch reached only the set that was serving. Switching back to a
	// structurally stale set either fails on reverse apply or quietly drops
	// the change, so refuse rather than guess which.
	drifted, err := o.schemaDrift(ctx)
	if err != nil {
		return err
	}
	if len(drifted) > 0 {
		return fmt.Errorf("schema changed since the switch on %s; rollback would restore a set that never received it, so it needs reconciling by hand", strings.Join(drifted, ", "))
	}
	// The metadata fence stops routers that have seen it, and a router
	// that has not can still commit on the set being rolled away from --
	// after which Complete drops the reverse replication that would have
	// carried the row back. So the targets stop taking new writing
	// transactions, and the ones already open are waited out: the setting
	// is read when a transaction starts, so the pause alone does not end
	// a writer that began before it.
	if err := o.pauseSet(ctx, o.wf.set, o.wf.ids, true); err != nil {
		return err
	}
	defer func() { _ = o.pauseSet(ctx, o.wf.set, o.wf.ids, false) }()
	if err := o.drainWriters(ctx, o.wf.set, o.wf.ids); err != nil {
		return err
	}
	positions := map[int32]int64{}
	for _, t := range o.wf.ids {
		lsn, err := o.currentLSN(ctx, o.wf.set, t)
		if err != nil {
			return err
		}
		positions[t] = lsn
	}
	behind, err := o.reverseBehind(ctx, positions)
	if err != nil {
		return err
	}
	if len(behind) > 0 {
		return retryf("reverse subscriptions behind the target position: %s", strings.Join(behind, ", "))
	}
	if _, err := o.syncSequences(ctx, o.wf.set, o.wf.ids, o.srcSet, o.srcIDs); err != nil {
		return err
	}
	if err := o.flipBack(ctx); err != nil {
		return err
	}
	return o.releaseRollback(ctx)
}

// reverseBehind lists the reverse subscriptions whose confirmed flush
// position has not passed the recorded position of the target they
// subscribe to. The rollback path has no verify backstop, so it reads the
// publisher slot's confirmed_flush_lsn on the targets, which survives
// apply-worker restarts and advances over keepalives.
func (o *pgCutover) reverseBehind(ctx context.Context, positions map[int32]int64) ([]string, error) {
	var behind []string
	for _, t := range o.wf.ids {
		flushed, err := slotFlushPositions(ctx, o.c.Shards, o.wf.set, t,
			fmt.Sprintf("pgshard\\_reshard\\_g%d\\_rev\\_s%%\\_t%d", o.wf.gen, t))
		if err != nil {
			return nil, err
		}
		expected := make([]string, 0, len(o.srcIDs))
		for _, s := range o.srcIDs {
			expected = append(expected, ReverseSubscriptionName(o.wf.gen, s, t))
		}
		behind = append(behind, slotsBehind(expected, flushed, positions[t], fmt.Sprintf("%s/%d", o.wf.set, t))...)
	}
	return behind, nil
}

// flipBack mirrors Flip: sources serving, targets retired, the serving row
// back on the source set, every database's home shard back on the home
// source, and the map generation bumped.
func (o *pgCutover) flipBack(ctx context.Context) error {
	tx, err := o.c.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var state string
	if err := tx.QueryRow(ctx, `SELECT state FROM pgshard.shard_sets WHERE shard_set = $1 FOR UPDATE`, o.srcSet).Scan(&state); err != nil {
		return err
	}
	if state == catalog.ShardSetServing {
		return nil
	}
	// The rollback restores the source and retires the target, so it has
	// the same stale-view hazard as the flip: if another workflow has since
	// flipped, its set is the one serving and restoring this one on top
	// would leave two.
	if err := o.stillSoleServing(ctx, tx, o.wf.set); err != nil {
		return err
	}
	homeSource := o.srcIDs[HomeTarget(o.srcRanges)]
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{`UPDATE pgshard.shard_status SET serving_state = $2, updated_at = now() WHERE shard_set = $1`, []any{o.srcSet, ServingServing}},
		{`UPDATE pgshard.shard_status SET serving_state = $2, updated_at = now() WHERE shard_set = $1`, []any{o.wf.set, ServingRetired}},
		{`UPDATE pgshard.shard_sets SET state = $2, updated_at = now() WHERE shard_set = $1`, []any{o.srcSet, catalog.ShardSetServing}},
		{`UPDATE pgshard.shard_sets SET state = $2, updated_at = now() WHERE shard_set = $1`, []any{o.wf.set, catalog.ShardSetRetired}},
		{`INSERT INTO pgshard.serving (shard_set, generation)
			SELECT $1, coalesce(max(desired_generation), 0) FROM pgshard.shard_ranges WHERE shard_set = $1
			ON CONFLICT (shard_set) DO UPDATE SET generation = EXCLUDED.generation, published_at = now()`, []any{o.srcSet}},
		{`DELETE FROM pgshard.serving WHERE shard_set = $1`, []any{o.wf.set}},
		{`UPDATE pgshard.databases SET home_shard = $1 WHERE home_shard <> $1`, []any{homeSource}},
		{`UPDATE pgshard.shard_map_generation SET generation = generation + 1, updated_at = now()`, nil},
	} {
		if _, err := tx.Exec(ctx, q.sql, q.args...); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (o *pgCutover) releaseRollback(ctx context.Context) error {
	tx, err := o.c.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Only the fence this workflow raised: another cutover's fence on the
	// same set is not ours to lift.
	if _, err := tx.Exec(ctx, `UPDATE pgshard.shard_status SET migrating = false, migrating_by = NULL, updated_at = now()
		WHERE shard_set = ANY($1) AND migrating AND migrating_by = $2::uuid`, []string{o.srcSet, o.wf.set}, o.wf.id); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM pgshard.workflow_locks WHERE workflow_id = $1::uuid`, o.wf.id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
