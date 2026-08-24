package controller

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// upgradePreconditions runs the automatic checks of plan §3.11 before an
// upgrade workflow starts copying. A violation fails the workflow with a
// message that names every failed check.
func (c *Copier) upgradePreconditions(ctx context.Context, wf *copyWorkflow, srcSet string, srcIDs []int32, dbs []dbPlan) error {
	if len(wf.ids) == 0 || len(srcIDs) == 0 {
		return fatal("upgrade %s: no shards to check", wf.set)
	}
	var failures []string
	missing, err := c.missingTargetExtensions(ctx, srcSet, srcIDs[0], wf.set, wf.ids[0])
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		failures = append(failures, "extensions missing on the target major: "+strings.Join(missing, ", "))
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

// missingTargetExtensions compares pg_available_extensions: every extension
// installed anywhere on the source shard must be available on the target.
func (c *Copier) missingTargetExtensions(ctx context.Context, srcSet string, src int32, tgtSet string, tgt int32) ([]string, error) {
	installed, err := c.installedExtensions(ctx, srcSet, src)
	if err != nil {
		return nil, err
	}
	available, err := c.availableExtensions(ctx, tgtSet, tgt)
	if err != nil {
		return nil, err
	}
	return MissingExtensions(installed, available), nil
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

func (c *Copier) installedExtensions(ctx context.Context, set string, id int32) ([]string, error) {
	conn, err := c.Shards.Dial(ctx, set, id)
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
func (o *pgCutover) Sequences(ctx context.Context) error {
	return o.syncSequences(ctx, o.srcSet, o.srcIDs, o.wf.set, o.wf.ids)
}

func (o *pgCutover) syncSequences(ctx context.Context, fromSet string, fromIDs []int32, toSet string, toIDs []int32) error {
	for _, db := range o.dbs {
		values := map[string]int64{}
		for _, s := range fromIDs {
			conn, err := o.c.Shards.DialDatabase(ctx, fromSet, s, db.name)
			if err != nil {
				return err
			}
			err = collectSequences(ctx, conn, values)
			_ = conn.Close(ctx)
			if err != nil {
				return fmt.Errorf("sequences of %s on %s/%d: %w", db.name, fromSet, s, err)
			}
		}
		if len(values) == 0 {
			continue
		}
		for _, t := range toIDs {
			conn, err := o.c.Shards.DialDatabase(ctx, toSet, t, db.name)
			if err != nil {
				return err
			}
			err = applySequences(ctx, conn, values)
			_ = conn.Close(ctx)
			if err != nil {
				return fmt.Errorf("sequences of %s on %s/%d: %w", db.name, toSet, t, err)
			}
		}
	}
	return nil
}

// collectSequences merges the called sequences of conn into values, keeping
// the maximum last_value per qualified name.
func collectSequences(ctx context.Context, conn ShardConn, values map[string]int64) error {
	rows, err := conn.Query(ctx, `SELECT quote_ident(schemaname) || '.' || quote_ident(sequencename), last_value
		FROM pg_sequences WHERE last_value IS NOT NULL AND schemaname NOT IN ('pgshard', $1) ORDER BY 1`, JournalSchema)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var v int64
		if err := rows.Scan(&name, &v); err != nil {
			return err
		}
		values[name] = max(values[name], v)
	}
	return rows.Err()
}

// applySequences sets every sequence conn holds to the recorded value; a
// sequence the target does not have (never materialized) is skipped.
func applySequences(ctx context.Context, conn ShardConn, values map[string]int64) error {
	for _, name := range sortedKeys(values) {
		if _, err := conn.Exec(ctx, `SELECT pg_catalog.setval(oid, $2, true) FROM pg_class WHERE oid = to_regclass($1) AND relkind = 'S'`, name, values[name]); err != nil {
			return err
		}
	}
	return nil
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
	if _, err := o.c.Pool.Exec(ctx, `UPDATE pgshard.shard_status SET migrating = true, updated_at = now()
		WHERE shard_set = ANY($1) AND NOT migrating`, []string{o.srcSet, o.wf.set}); err != nil {
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
	// A router that missed the fence may have written on the new-major set
	// after the positions were read; the flip back only happens once the
	// targets stood still through a whole reverse catch-up, so that write
	// is replicated back instead of lost.
	for _, t := range o.wf.ids {
		lsn, err := o.currentLSN(ctx, o.wf.set, t)
		if err != nil {
			return err
		}
		if lsn != positions[t] {
			return retryf("target %s/%d advanced from %d to %d during the rollback catch-up", o.wf.set, t, positions[t], lsn)
		}
	}
	if err := o.syncSequences(ctx, o.wf.set, o.wf.ids, o.srcSet, o.srcIDs); err != nil {
		return err
	}
	if err := o.flipBack(ctx); err != nil {
		return err
	}
	return o.releaseRollback(ctx)
}

// reverseBehind lists the reverse subscriptions whose apply position has
// not passed the recorded position of the target they subscribe to.
func (o *pgCutover) reverseBehind(ctx context.Context, positions map[int32]int64) ([]string, error) {
	var behind []string
	for _, db := range o.dbs {
		for _, s := range o.srcIDs {
			conn, err := o.c.Shards.DialDatabase(ctx, o.srcSet, s, db.name)
			if err != nil {
				return nil, err
			}
			rows, err := conn.Query(ctx, `SELECT s.subname, coalesce((st.latest_end_lsn - '0/0'::pg_lsn)::bigint, -1)
				FROM pg_subscription s LEFT JOIN pg_stat_subscription st ON st.subid = s.oid AND st.relid IS NULL
				WHERE s.subname LIKE $1 AND s.subenabled ORDER BY 1`, o.reversePattern(s))
			if err == nil {
				err = func() error {
					defer rows.Close()
					for rows.Next() {
						var name string
						var applied int64
						if err := rows.Scan(&name, &applied); err != nil {
							return err
						}
						var gen int64
						var src, tgt int32
						if _, err := fmt.Sscanf(name, "pgshard_reshard_g%d_rev_s%d_t%d", &gen, &src, &tgt); err != nil {
							continue
						}
						if applied < positions[tgt] {
							behind = append(behind, fmt.Sprintf("%s/%d %s at %d of %d", db.name, s, name, applied, positions[tgt]))
						}
					}
					return rows.Err()
				}()
			}
			_ = conn.Close(ctx)
			if err != nil {
				return nil, err
			}
		}
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
	if _, err := tx.Exec(ctx, `UPDATE pgshard.shard_status SET migrating = false, updated_at = now()
		WHERE shard_set = ANY($1) AND migrating`, []string{o.srcSet, o.wf.set}); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM pgshard.workflow_locks WHERE workflow_id = $1::uuid`, o.wf.id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
