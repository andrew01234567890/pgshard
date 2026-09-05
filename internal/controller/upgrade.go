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
	available, disagreed, err := c.commonTargetExtensions(ctx, wf.set, wf.ids)
	if err != nil {
		return err
	}
	if len(disagreed) > 0 {
		failures = append(failures, fmt.Sprintf("target shards disagree on the default version of: %s", strings.Join(disagreed, ", ")))
	}
	for _, db := range dbs {
		for _, s := range srcIDs {
			installed, err := c.installedExtensions(ctx, srcSet, s, db.name)
			if err != nil {
				return err
			}
			if bad := UnsupportedExtensions(installed, available); len(bad) > 0 {
				failures = append(failures, fmt.Sprintf("extensions the target major cannot carry for %s on %s/%d: %s",
					db.name, srcSet, s, strings.Join(bad, "; ")))
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

// The two catalogue queries the precheck runs, named so a test can run the
// same SQL against a real pair of majors rather than a paraphrase of it.
const (
	installedExtensionsSQL = `SELECT extname, extversion FROM pg_extension ORDER BY 1`

	// The update paths are the target's, because they come from the target
	// major's extension scripts, and only paths TO the default matter: that
	// is the version a restore installs.
	availableExtensionsSQL = `
		SELECT a.name, a.default_version,
		       ARRAY(SELECT p.source FROM pg_extension_update_paths(a.name) p
		              WHERE p.target = a.default_version AND p.path IS NOT NULL)
		  FROM pg_available_extensions a
		 ORDER BY 1`
)

// InstalledExtension is one extension as a source database has it.
type InstalledExtension struct {
	Name    string
	Version string
}

// TargetExtension is what the target major will do with an extension name.
//
// Default is what the target installs, because pg_dump emits CREATE EXTENSION
// with no version: the restored schema gets the TARGET's default whatever the
// source had. ReachableFrom is every source version PostgreSQL declares an
// update path from, to that default.
type TargetExtension struct {
	Default       string
	ReachableFrom map[string]bool
}

// commonTargetExtensions is what every target shard can offer, and the names
// the targets disagree about.
//
// An extension available on one target shard and not another would install on
// some and fail on the rest, which is the same outcome as missing. One whose
// DEFAULT VERSION differs between target shards is worse than either, because
// the same restore would produce different schemas per shard, so it is
// reported under its own name rather than folded into "missing".
func (c *Copier) commonTargetExtensions(ctx context.Context, tgtSet string, tgtIDs []int32) (map[string]TargetExtension, []string, error) {
	perShard := make([]map[string]TargetExtension, 0, len(tgtIDs))
	for _, id := range tgtIDs {
		available, err := c.availableExtensions(ctx, tgtSet, id)
		if err != nil {
			return nil, nil, err
		}
		perShard = append(perShard, available)
	}
	common, disagreed := MergeTargetExtensions(perShard)
	return common, disagreed, nil
}

// MergeTargetExtensions folds what each target shard offers into what all of
// them offer, and lists the names they disagree about.
func MergeTargetExtensions(perShard []map[string]TargetExtension) (map[string]TargetExtension, []string) {
	common := map[string]TargetExtension{}
	disagreed := map[string]bool{}
	for i, available := range perShard {
		if i == 0 {
			maps.Copy(common, available)
			continue
		}
		for name, have := range common {
			next, ok := available[name]
			if !ok {
				delete(common, name)
				continue
			}
			if next.Default != have.Default {
				disagreed[name] = true
				delete(common, name)
				continue
			}
			have.ReachableFrom = intersectSet(have.ReachableFrom, next.ReachableFrom)
			common[name] = have
		}
	}
	return common, sortedKeys(disagreed)
}

func intersectSet(a, b map[string]bool) map[string]bool {
	out := map[string]bool{}
	for k := range a {
		if b[k] {
			out[k] = true
		}
	}
	return out
}

// UnsupportedExtensions returns one message per installed extension the
// target major cannot carry, sorted by extension name.
//
// Two things can be wrong, and they are different failures:
//
// ABSENT. The name is not available on the target at all, so the restore's
// CREATE EXTENSION fails outright.
//
// UNREACHABLE. The name is available but at a different default version, and
// PostgreSQL declares no update path from the source's version to it. That
// matters because pg_dump emits CREATE EXTENSION without a version, so the
// target installs its own default whatever the source had -- and an update
// path is PostgreSQL's own statement that the newer version can carry the
// older one's objects forward. Where it says nothing, we are guessing.
//
// What is deliberately NOT done here is comparing the two version strings.
// extversion is opaque and PostgreSQL does not order it -- "1.11" against
// "1.9" is the obvious trap and "2.1-beta" has no defined position at all --
// which is exactly why update paths exist. A check that ordered them would be
// a guess that looked principled, and a preflight that refuses upgrades which
// would have worked is worse than one that misses an upgrade that fails.
//
// Measured against this project's own images before writing this: of the 46
// extensions PostgreSQL 18.6 offers, five have a different default on 19beta3
// (btree_gin, btree_gist, pg_buffercache, pg_stat_statements, postgres_fdw),
// and PostgreSQL declares an update path for every one of the five. So the
// ordinary 18-to-19 upgrade passes, and the check fails closed only where
// PostgreSQL itself says there is no route.
func UnsupportedExtensions(installed []InstalledExtension, target map[string]TargetExtension) []string {
	seen := map[string]bool{}
	var out []string
	for _, e := range installed {
		if seen[e.Name] {
			continue
		}
		seen[e.Name] = true
		t, ok := target[e.Name]
		switch {
		case !ok:
			out = append(out, e.Name+" is not available on the target major")
		case t.Default == e.Version, t.ReachableFrom[e.Version]:
		default:
			out = append(out, fmt.Sprintf("%s is installed at %s and the target major installs %s, with no update path between them",
				e.Name, e.Version, t.Default))
		}
	}
	slices.Sort(out)
	return out
}

// installedExtensions lists one database's extensions and their versions.
// They are per-database objects, so the default database's list says nothing
// about the others.
func (c *Copier) installedExtensions(ctx context.Context, set string, id int32, database string) ([]InstalledExtension, error) {
	conn, err := c.Shards.DialDatabase(ctx, set, id, database)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, installedExtensionsSQL)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[InstalledExtension])
}

// availableExtensions is what one target shard offers, with the versions its
// default is reachable from. The update paths are read from the TARGET,
// because they come from the target major's extension scripts.
func (c *Copier) availableExtensions(ctx context.Context, set string, id int32) (map[string]TargetExtension, error) {
	conn, err := c.Shards.Dial(ctx, set, id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, availableExtensionsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]TargetExtension{}
	for rows.Next() {
		var name, def string
		var from []string
		if err := rows.Scan(&name, &def, &from); err != nil {
			return nil, err
		}
		reach := make(map[string]bool, len(from))
		for _, v := range from {
			reach[v] = true
		}
		out[name] = TargetExtension{Default: def, ReachableFrom: reach}
	}
	return out, rows.Err()
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
			fmt.Sprintf("pgshard\\_reshard\\_g%d\\_rev\\_s%%\\_t%d\\_%%", o.wf.gen, t))
		if err != nil {
			return nil, err
		}
		// One slot per (database, source), for the same reason CaughtUp
		// enumerates per database: the reverse slots are per-database too.
		expected := make([]string, 0, len(o.srcIDs)*len(o.dbs))
		for _, db := range o.dbs {
			for _, s := range o.srcIDs {
				expected = append(expected, ReverseSlotName(o.wf.gen, s, t, db.name))
			}
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
