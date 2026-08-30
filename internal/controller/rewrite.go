package controller

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
)

// Rewrite phases, tracked per shard in ShardMigration.Step.
const (
	rewriteAdd = iota
	rewriteTrigger
	rewriteBackfill
	rewriteCutover
	rewriteValidate
	rewriteDone
)

// DefaultRewriteBatch bounds one backfill batch.
const DefaultRewriteBatch = 1000

// DefaultRewriteSettle is how long the applier waits after publishing the
// visible column list before adding the hidden column, so every router has
// reloaded its snapshot and hides the column from the first moment. It
// must cover the snapshot watcher's fallback reload: a router whose LISTEN
// dropped only notices the column list on its periodic reload, and a
// shorter settle would let its SELECT * leak the hidden column.
//
// It is snapshot.MaxAge rather than that sum written out again, and the
// two being the same quantity is what makes the wait a guarantee instead
// of a hope: a router whose reloads are failing keeps its old snapshot and
// would never see the column list however long the wait, so past MaxAge it
// stops serving. After this settle every router still answering has
// reloaded inside it.
const DefaultRewriteSettle = snapshot.MaxAge

// driveRewrite runs an online rewrite migration: per phase across every
// shard, so no shard cuts over before all shards finished their backfill.
// A failure before any cutover reverts every shard, leaving the table
// untouched; a failure during cutover is reported with the shards left on
// each side.
func (a *Applier) driveRewrite(ctx context.Context, logger *slog.Logger, m *catalog.DDLMigration) error {
	rw := m.Meta.Rewrite
	if err := a.recordRewriteColumns(ctx, m); err != nil {
		return a.failRewrite(ctx, logger, m, false, fmt.Sprintf("recording the column list: %v", err))
	}
	shards := sortedShardKeys(m.PerShard)
	for phase := rewriteAdd; phase < rewriteDone; phase++ {
		for _, key := range shards {
			s := m.PerShard[key]
			if s.State == catalog.ShardFailed || s.Step > phase {
				continue
			}
			if a.lostLeadership() {
				return errNotLeader
			}
			id := shardID(key)
			s = a.retrying(ctx, logger, m, key, id, s, func() (string, error) {
				return a.rewritePhase(ctx, m, id, phase)
			})
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if s.State == catalog.ShardApplied && phase < rewriteDone-1 {
				s.State = catalog.ShardRunning
			}
			s.Step = phase + 1
			m.PerShard[key] = s
			if err := a.Store.Save(ctx, *m); err != nil {
				return err
			}
			if s.State == catalog.ShardFailed {
				return a.failRewrite(ctx, logger, m, phase >= rewriteCutover,
					fmt.Sprintf("shard %s: %s", key, s.Error))
			}
		}
	}
	m.State, m.Error = catalog.MigrationComplete, ""
	if err := a.Store.Save(ctx, *m); err != nil {
		return err
	}
	logger.Info("rewrite migration finished", "table", rw.Table, "column", rw.Column)
	return nil
}

func shardID(key string) int32 {
	var id int32
	_, _ = fmt.Sscanf(key, "%d", &id)
	return id
}

// failRewrite marks the migration failed. Unless a shard already cut over,
// every shard is reverted so the table is left as it was.
func (a *Applier) failRewrite(ctx context.Context, logger *slog.Logger, m *catalog.DDLMigration, cutoverStarted bool, msg string) error {
	m.State, m.Error = catalog.MigrationFailed, msg
	if cutoverStarted {
		m.Error += "; some shards may already be cut over (schema is DEGRADED until resolved)"
	}
	for _, key := range sortedShardKeys(m.PerShard) {
		s := m.PerShard[key]
		if s.State != catalog.ShardFailed {
			s.State = catalog.ShardFailed
			if s.Error == "" {
				s.Error = "reverted: another shard failed"
			}
		}
		m.PerShard[key] = s
		if s.Step > rewriteCutover {
			continue
		}
		if err := a.revertRewrite(ctx, m, shardID(key)); err != nil {
			logger.Warn("rewrite revert failed", "shard", key, "err", err)
		}
	}
	if err := a.Store.Save(ctx, *m); err != nil {
		return err
	}
	logger.Warn("rewrite migration failed", "error", m.Error)
	return nil
}

// rewriteColumnDependents lists the objects an ALTER TABLE ... DROP COLUMN of
// the target column would cascade away or be blocked by, which the OID-
// preserving rewrite cutover does not recreate: indexes (including expression
// and partial indexes and PRIMARY KEY/UNIQUE/EXCLUSION constraints), CHECK and
// foreign-key constraints, inbound foreign keys, generated columns, extended
// statistics, views/rules, and RLS policies, plus identity columns whose owned
// sequence the drop would take with it. It is a general pg_depend sweep over
// everything that depends on the column, excluding only what the cutover itself
// restores (the column's own default and NOT NULL). Any result means the
// rewrite must be refused. Applies to the type form only (the add form creates
// a new column and drops nothing).
func rewriteColumnDependents(ctx context.Context, conn ShardConn, schema, table, column string) ([]string, error) {
	rows, err := conn.Query(ctx, `WITH t AS (
  SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relname = $1 AND ($2 = '' AND n.nspname = ANY (current_schemas(false)) OR n.nspname = $2)
  ORDER BY CASE WHEN $2 = '' THEN array_position(current_schemas(false), n.nspname::text) ELSE 0 END
  LIMIT 1),
col AS (SELECT a.attnum, a.attidentity FROM pg_attribute a, t WHERE a.attrelid = t.oid AND a.attname = $3 AND NOT a.attisdropped)
SELECT label FROM (
  SELECT 'identity column ' || $3 AS label FROM col WHERE col.attidentity <> ''
  UNION ALL
  SELECT 'column privileges on ' || $3 FROM col, t WHERE EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = t.oid AND a.attnum = col.attnum AND a.attacl IS NOT NULL)
  UNION ALL
  SELECT 'security label on ' || $3 FROM col, t WHERE EXISTS (SELECT 1 FROM pg_seclabel sl WHERE sl.classoid = 'pg_class'::regclass AND sl.objoid = t.oid AND sl.objsubid = col.attnum)
  UNION ALL
  SELECT CASE d.classid
      WHEN 'pg_class'::regclass THEN (SELECT CASE relkind WHEN 'i' THEN 'index ' WHEN 'v' THEN 'view ' WHEN 'm' THEN 'materialized view ' WHEN 'S' THEN 'sequence ' ELSE 'relation ' END || relname FROM pg_class WHERE oid = d.objid)
      WHEN 'pg_constraint'::regclass THEN 'constraint ' || (SELECT conname FROM pg_constraint WHERE oid = d.objid)
      WHEN 'pg_attrdef'::regclass THEN 'generated column ' || (SELECT a2.attname FROM pg_attrdef ad JOIN pg_attribute a2 ON a2.attrelid = ad.adrelid AND a2.attnum = ad.adnum WHERE ad.oid = d.objid)
      WHEN 'pg_statistic_ext'::regclass THEN 'statistics ' || (SELECT stxname FROM pg_statistic_ext WHERE oid = d.objid)
      WHEN 'pg_rewrite'::regclass THEN 'view/rule ' || (SELECT c2.relname FROM pg_rewrite r JOIN pg_class c2 ON c2.oid = r.ev_class WHERE r.oid = d.objid)
      WHEN 'pg_policy'::regclass THEN 'policy ' || (SELECT polname FROM pg_policy WHERE oid = d.objid)
      WHEN 'pg_trigger'::regclass THEN 'trigger ' || (SELECT tgname FROM pg_trigger WHERE oid = d.objid)
      WHEN 'pg_publication_rel'::regclass THEN 'publication column list'
      ELSE d.classid::regclass::text
    END AS label
  FROM pg_depend d, t, col
  WHERE d.refclassid = 'pg_class'::regclass AND d.refobjid = t.oid AND d.refobjsubid = col.attnum
    AND d.deptype IN ('a','n','i')
    AND NOT (d.classid = 'pg_attrdef'::regclass AND EXISTS (SELECT 1 FROM pg_attrdef ad WHERE ad.oid = d.objid AND ad.adrelid = t.oid AND ad.adnum = col.attnum))
    AND NOT (d.classid = 'pg_constraint'::regclass AND EXISTS (SELECT 1 FROM pg_constraint con WHERE con.oid = d.objid AND con.contype = 'n' AND con.convalidated AND NOT con.connoinherit AND con.conrelid = t.oid AND con.conkey = ARRAY[col.attnum]::int2[]))
    AND NOT (d.classid = 'pg_class'::regclass AND EXISTS (SELECT 1 FROM pg_constraint con WHERE con.conindid = d.objid))
) x WHERE label IS NOT NULL ORDER BY 1`, table, schema, column)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// recordRewriteColumns publishes the table's visible column list into the
// migration meta so routers hide the working column, then waits for
// snapshots to reload before any hidden column exists.
func (a *Applier) recordRewriteColumns(ctx context.Context, m *catalog.DDLMigration) error {
	rw := m.Meta.Rewrite
	if len(rw.Columns) > 0 {
		return nil
	}
	keys := sortedShardKeys(m.PerShard)
	if len(keys) == 0 {
		return fmt.Errorf("no target shards")
	}
	conn, err := a.prepare(ctx, m, keys[0], shardID(keys[0]), m.Database)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	// Fail closed: the OID-preserving cutover drops the old column, whose
	// DROP silently cascades away dependent indexes/constraints/statistics/
	// generated columns and loses column privileges and identity that the
	// cutover never recreates. Refuse the rewrite before any schema change if
	// the target column carries such dependents on ANY shard (the add form
	// has no old column to drop).
	if !rw.Add && rw.Column != "" {
		for _, key := range keys {
			dc, derr := a.prepare(ctx, m, key, shardID(key), m.Database)
			if derr != nil {
				return derr
			}
			deps, derr := rewriteColumnDependents(ctx, dc, rw.Schema, rw.Table, rw.Column)
			_ = dc.Close(context.WithoutCancel(ctx))
			if derr != nil {
				return derr
			}
			if len(deps) > 0 {
				return fmt.Errorf("online rewrite of column %q on %s cannot proceed (shard %s): it has dependent objects the cutover would drop or lose without recreating: %s", rw.Column, rw.Table, key, strings.Join(deps, ", "))
			}
		}
	}
	rows, err := conn.Query(ctx, `SELECT a.attname FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relname = $1 AND ($2 = '' AND n.nspname = ANY (current_schemas(false)) OR n.nspname = $2)
		AND a.attnum > 0 AND NOT a.attisdropped AND a.attname NOT LIKE $3 ORDER BY a.attnum`,
		rw.Table, rw.Schema, hiddenLike())
	if err != nil {
		return err
	}
	cols, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return fmt.Errorf("table %q has no columns on shard %s", rw.Table, keys[0])
	}
	rw.Columns = cols
	if err := a.Store.SaveMeta(ctx, m.ID, m.Meta); err != nil {
		return err
	}
	settle := a.RewriteSettle
	if settle == 0 {
		settle = DefaultRewriteSettle
	}
	if settle > 0 {
		if err := a.sleep(ctx, settle); err != nil {
			return err
		}
	}
	return nil
}

// rewritePhase runs one phase of the rewrite on one shard.
func (a *Applier) rewritePhase(ctx context.Context, m *catalog.DDLMigration, id int32, phase int) (string, error) {
	defer a.releaseDDLRole(ctx, m, id, m.Meta.RunAs)
	conn, err := a.prepare(ctx, m, shardKey(id), id, m.Database)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	rw, mid := m.Meta.Rewrite, m.ID
	table := qualified(rw.Schema, rw.Table)
	hidden := rw.HiddenColumn(mid)
	switch phase {
	case rewriteAdd:
		_, err = conn.Exec(ctx, "ALTER TABLE "+table+" ADD COLUMN IF NOT EXISTS "+ident(hidden)+" "+rw.NewType)
	case rewriteTrigger:
		for _, sql := range rewriteTriggerSQL(rw, mid) {
			if _, err = conn.Exec(ctx, sql); err != nil {
				break
			}
		}
	case rewriteBackfill:
		err = a.backfill(ctx, conn, rw, mid)
	case rewriteCutover:
		return a.cutover(ctx, conn, rw, mid)
	case rewriteValidate:
		err = validateNotNull(ctx, conn, rw, mid)
	}
	if err != nil {
		return "", err
	}
	return catalog.ShardApplied, nil
}

func ident(s string) string { return pgx.Identifier{s}.Sanitize() }

// hiddenLike is the LIKE pattern matching rewrite artifacts, with the
// prefix underscores escaped.
func hiddenLike() string { return strings.ReplaceAll(catalog.HiddenPrefix, "_", `\_`) + "%" }

// rewriteTriggerSQL builds the dual-write trigger: the type form keeps the
// working column in sync on INSERT and UPDATE via the USING expression;
// the add form applies the volatile DEFAULT on INSERT only.
func rewriteTriggerSQL(rw *catalog.RewriteChange, mid string) []string {
	table := qualified(rw.Schema, rw.Table)
	trig := rw.TriggerName(mid)
	fn := qualified(rw.Schema, trig)
	hidden := ident(rw.HiddenColumn(mid))
	var body, events string
	if rw.Add {
		body = "NEW." + hidden + " := (" + rw.Default + ");"
		events = "INSERT"
	} else {
		body = "NEW." + hidden + " := (SELECT (" + rw.Using + ") FROM (SELECT (NEW).*) AS pgshard_row);"
		events = "INSERT OR UPDATE"
	}
	return []string{
		"CREATE OR REPLACE FUNCTION " + fn + "() RETURNS trigger LANGUAGE plpgsql AS $pgshard$ BEGIN " + body + " RETURN NEW; END $pgshard$",
		"DROP TRIGGER IF EXISTS " + ident(trig) + " ON " + table,
		"CREATE TRIGGER " + ident(trig) + " BEFORE " + events + " ON " + table + " FOR EACH ROW EXECUTE FUNCTION " + fn + "()",
	}
}

// backfillPredicate is the rows a backfill still has to convert.
func backfillPredicate(rw *catalog.RewriteChange, hidden string) string {
	if rw.Add {
		return ident(hidden) + " IS NULL"
	}
	return ident(hidden) + " IS DISTINCT FROM (" + rw.Using + ")"
}

func backfillExpr(rw *catalog.RewriteChange) string {
	if rw.Add {
		return rw.Default
	}
	return rw.Using
}

// backfill converts existing rows in batches, one committed batch at a
// time: keyset-paginated over a single-column primary key when the table
// has one, ctid-batched otherwise. Each batch reports the last key it
// selected and the next starts there, so no batch rescans what an earlier
// one converted; the loop ends only when a scan of the whole table finds
// nothing left to convert.
func (a *Applier) backfill(ctx context.Context, conn ShardConn, rw *catalog.RewriteChange, mid string) error {
	table := qualified(rw.Schema, rw.Table)
	hidden := rw.HiddenColumn(mid)
	batch := rw.BatchSize
	if batch <= 0 {
		batch = DefaultRewriteBatch
	}
	rows, err := conn.Query(ctx, `SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
		WHERE i.indisprimary AND i.indrelid = ($1)::regclass`, table)
	if err != nil {
		return err
	}
	pks, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	pred := backfillPredicate(rw, hidden)
	set := ident(hidden) + " = (" + backfillExpr(rw) + ")"
	// A primary key is ordered by its own index, so batches follow it and
	// none can be skipped. ctid has no index and sorting by it would read
	// and sort every remaining row on every batch -- the cost this cursor
	// exists to avoid -- so the ctid batches take the rows the scan reaches
	// first. A row a batch passes over is not lost: the scan of the whole
	// table at the end finds it and the loop resumes from there.
	cursor, order, cast := "ctid", "", "tid"
	if len(pks) == 1 {
		cursor = ident(pks[0])
		order = " ORDER BY " + cursor
		if cast, err = cursorType(ctx, conn, table, pks[0]); err != nil {
			return err
		}
	}
	// The bound is a statement of its own rather than "OR $1 IS NULL" in
	// one: an OR over the cursor hides the range from the planner, which
	// then scans from the head of the table anyway. The UPDATE takes the
	// batch's keys as an array for the same reason -- joined against the
	// CTE it is planned as a hash join over a sequential scan of the whole
	// table, once per batch.
	pass := func(where string) string {
		return "WITH batch AS (SELECT " + cursor + " FROM " + table + " WHERE " + pred + where + order +
			" LIMIT " + fmt.Sprint(batch) + "), upd AS (UPDATE " + table + " t SET " + set +
			" WHERE t." + cursor + " = ANY (ARRAY(SELECT " + cursor + " FROM batch)))" +
			" SELECT max(" + cursor + ")::text AS at FROM batch"
	}
	// The batch reports the last key it selected, not how many rows it
	// changed: a concurrent DELETE or primary-key change can remove a
	// selected row before the UPDATE reaches it, so a short batch does not
	// mean the table is done. A key that survives its own batch does mean
	// the conversion cannot finish -- a volatile DEFAULT yielding NULL or
	// an unstable USING keeps rows matching pred however often they are
	// updated.
	head, from := pass(""), pass(" AND "+cursor+" >= ($1)::"+cast)
	rest := "SELECT " + cursor + "::text AS at FROM " + table + " WHERE " + pred + " LIMIT 1"
	var at *string
	for {
		var rows pgx.Rows
		var err error
		if at == nil {
			rows, err = conn.Query(ctx, head)
		} else {
			rows, err = conn.Query(ctx, from, *at)
		}
		if err != nil {
			return err
		}
		next, err := pgx.CollectRows(rows, pgx.RowTo[*string])
		if err != nil {
			return err
		}
		if len(next) == 1 && next[0] != nil {
			if at != nil && *next[0] == *at {
				return fmt.Errorf("backfill of %s is not converging: rows keep matching %q after being updated (a volatile DEFAULT returning NULL or an unstable USING expression cannot be backfilled); fix the expression and run the migration again", table, pred)
			}
			at = next[0]
			continue
		}
		if at == nil {
			return nil
		}
		// Nothing left from the cursor on. Read the whole table once to
		// find anything the batches passed over, and resume there.
		rows, err = conn.Query(ctx, rest)
		if err != nil {
			return err
		}
		left, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		if len(left) == 0 {
			return nil
		}
		if left[0] == *at {
			return fmt.Errorf("backfill of %s is not converging: rows keep matching %q after being updated (a volatile DEFAULT returning NULL or an unstable USING expression cannot be backfilled); fix the expression and run the migration again", table, pred)
		}
		at = &left[0]
	}
}

// cursorType names a column's type as SQL text, for casting a cursor read
// back as text into the type it is compared against.
func cursorType(ctx context.Context, conn ShardConn, table, column string) (string, error) {
	rows, err := conn.Query(ctx, `SELECT pg_catalog.format_type(atttypid, atttypmod) FROM pg_attribute
		WHERE attrelid = ($1)::regclass AND attname = $2`, table, column)
	if err != nil {
		return "", err
	}
	types, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return "", err
	}
	if len(types) != 1 {
		return "", fmt.Errorf("column %q of %s has no type", column, table)
	}
	return types[0], nil
}

// cutover swaps the working column in for the old one in one transaction
// under the session's lock_timeout: drop the trigger, drop the old column,
// rename, restore the default and queue a NOT NULL for validation.
func (a *Applier) cutover(ctx context.Context, conn ShardConn, rw *catalog.RewriteChange, mid string) (string, error) {
	table := qualified(rw.Schema, rw.Table)
	hidden := rw.HiddenColumn(mid)
	trig := rw.TriggerName(mid)
	hiddenExists, err := columnExists(ctx, conn, rw, hidden)
	if err != nil {
		return "", err
	}
	if !hiddenExists {
		// A previous attempt committed the cutover.
		return catalog.ShardApplied, nil
	}
	var stmts []string
	stmts = append(stmts, "DROP TRIGGER IF EXISTS "+ident(trig)+" ON "+table)
	if rw.Add {
		stmts = append(stmts, "ALTER TABLE "+table+" RENAME COLUMN "+ident(hidden)+" TO "+ident(rw.Column))
		if rw.Default != "" {
			stmts = append(stmts, "ALTER TABLE "+table+" ALTER COLUMN "+ident(rw.Column)+" SET DEFAULT ("+rw.Default+")")
		}
	} else {
		// A dependent added during the (possibly long) backfill would be
		// cascaded away by the DROP below; re-check under the cutover lock so
		// the guarantee holds at cutover, not just at preflight.
		deps, derr := rewriteColumnDependents(ctx, conn, rw.Schema, rw.Table, rw.Column)
		if derr != nil {
			return "", derr
		}
		if len(deps) > 0 {
			return "", fmt.Errorf("column %q gained dependent objects during the rewrite that the cutover would drop without recreating: %s", rw.Column, strings.Join(deps, ", "))
		}
		oldDefault, notNull, err := columnFacts(ctx, conn, rw, rw.Column)
		if err != nil {
			return "", err
		}
		stmts = append(stmts,
			"ALTER TABLE "+table+" DROP COLUMN "+ident(rw.Column),
			"ALTER TABLE "+table+" RENAME COLUMN "+ident(hidden)+" TO "+ident(rw.Column))
		if oldDefault != "" {
			stmts = append(stmts, "ALTER TABLE "+table+" ALTER COLUMN "+ident(rw.Column)+" SET DEFAULT CAST(("+oldDefault+") AS "+rw.NewType+")")
		}
		if notNull {
			stmts = append(stmts, "ALTER TABLE "+table+" ADD CONSTRAINT "+ident(notNullName(rw, mid))+" NOT NULL "+ident(rw.Column)+" NOT VALID")
		}
	}
	if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
		return "", err
	}
	for _, sql := range stmts {
		if _, err := conn.Exec(ctx, sql); err != nil {
			_, _ = conn.Exec(context.WithoutCancel(ctx), "ROLLBACK")
			return "", err
		}
	}
	if _, err := conn.Exec(ctx, "COMMIT"); err != nil {
		return "", err
	}
	_, _ = conn.Exec(context.WithoutCancel(ctx), "DROP FUNCTION IF EXISTS "+qualified(rw.Schema, trig)+"()")
	return catalog.ShardApplied, nil
}

func notNullName(rw *catalog.RewriteChange, mid string) string {
	return catalog.HiddenPrefix + "nn_" + rw.Column + "_" + shortMigrationID(mid)
}

func shortMigrationID(id string) string {
	return strings.ReplaceAll(id, "-", "")[:8]
}

// validateNotNull validates the NOT NULL constraint the cutover added, so
// the long scan runs outside the cutover's exclusive lock.
func validateNotNull(ctx context.Context, conn ShardConn, rw *catalog.RewriteChange, mid string) error {
	if rw.Add {
		return nil
	}
	table := qualified(rw.Schema, rw.Table)
	name := notNullName(rw, mid)
	rows, err := conn.Query(ctx, `SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = $1 AND NOT convalidated)`, name)
	if err != nil {
		return err
	}
	pending, err := pgx.CollectOneRow(rows, pgx.RowTo[bool])
	if err != nil {
		return err
	}
	if !pending {
		return nil
	}
	_, err = conn.Exec(ctx, "ALTER TABLE "+table+" VALIDATE CONSTRAINT "+ident(name))
	return err
}

func columnExists(ctx context.Context, conn ShardConn, rw *catalog.RewriteChange, col string) (bool, error) {
	rows, err := conn.Query(ctx, `SELECT EXISTS (SELECT 1 FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relname = $1 AND ($2 = '' AND n.nspname = ANY (current_schemas(false)) OR n.nspname = $2)
		AND a.attname = $3 AND a.attnum > 0 AND NOT a.attisdropped)`, rw.Table, rw.Schema, col)
	if err != nil {
		return false, err
	}
	return pgx.CollectOneRow(rows, pgx.RowTo[bool])
}

// columnFacts reads the default expression and NOT NULL of a column.
func columnFacts(ctx context.Context, conn ShardConn, rw *catalog.RewriteChange, col string) (string, bool, error) {
	rows, err := conn.Query(ctx, `SELECT coalesce(pg_get_expr(d.adbin, d.adrelid), ''), a.attnotnull
		FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		WHERE c.relname = $1 AND ($2 = '' AND n.nspname = ANY (current_schemas(false)) OR n.nspname = $2)
		AND a.attname = $3 AND a.attnum > 0 AND NOT a.attisdropped`, rw.Table, rw.Schema, col)
	if err != nil {
		return "", false, err
	}
	type facts struct {
		Default string
		NotNull bool
	}
	f, err := pgx.CollectOneRow(rows, pgx.RowToStructByPos[facts])
	if err != nil {
		return "", false, err
	}
	return f.Default, f.NotNull, nil
}

// revertRewrite removes the working column, trigger and function of a
// rewrite from one shard, leaving the old column intact.
func (a *Applier) revertRewrite(ctx context.Context, m *catalog.DDLMigration, id int32) error {
	ctx = context.WithoutCancel(ctx)
	set, serr := a.shardSet(ctx)
	if serr != nil {
		return serr
	}
	conn, err := a.Shards.DialDatabase(ctx, set, id, m.Database)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	rw := m.Meta.Rewrite
	table := qualified(rw.Schema, rw.Table)
	trig := rw.TriggerName(m.ID)
	for _, sql := range []string{
		"DROP TRIGGER IF EXISTS " + ident(trig) + " ON " + table,
		"DROP FUNCTION IF EXISTS " + qualified(rw.Schema, trig) + "()",
		"ALTER TABLE " + table + " DROP COLUMN IF EXISTS " + ident(rw.HiddenColumn(m.ID)),
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			return err
		}
	}
	return nil
}

// SweepRewriteArtifacts drops leftover rewrite triggers, functions and
// working columns on every shard. It runs only while no rewrite migration
// is queued or running, so anything matching the prefix is garbage from a
// crashed or failed migration.
func (a *Applier) SweepRewriteArtifacts(ctx context.Context) error {
	dbs, err := a.Store.Databases(ctx)
	if err != nil {
		return err
	}
	set, serr := a.shardSet(ctx)
	if serr != nil {
		return serr
	}
	ids, err := a.Store.Shards(ctx, set)
	if err != nil {
		return err
	}
	for _, id := range ids {
		for _, db := range dbs {
			if err := a.sweepShard(ctx, id, db); err != nil {
				a.logger().Warn("rewrite artifact sweep failed", "shard", id, "database", db, "err", err)
			}
		}
	}
	return nil
}

func (a *Applier) sweepShard(ctx context.Context, id int32, db string) error {
	set, serr := a.shardSet(ctx)
	if serr != nil {
		return serr
	}
	conn, err := a.Shards.DialDatabase(ctx, set, id, db)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	like := hiddenLike()
	rows, err := conn.Query(ctx, `SELECT format('DROP TRIGGER IF EXISTS %I ON %I.%I', t.tgname, n.nspname, c.relname)
		FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE NOT t.tgisinternal AND t.tgname LIKE $1
		UNION ALL
		SELECT format('DROP FUNCTION IF EXISTS %I.%I()', n.nspname, p.proname)
		FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE p.proname LIKE $1
		UNION ALL
		SELECT format('ALTER TABLE %I.%I DROP COLUMN IF EXISTS %I', n.nspname, c.relname, a.attname)
		FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped AND a.attname LIKE $1`, like)
	if err != nil {
		return err
	}
	drops, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	for _, sql := range drops {
		if _, err := conn.Exec(ctx, sql); err != nil {
			return err
		}
	}
	return nil
}

// repackStep repacks the table of a repack migration on one shard: REPACK
// (CONCURRENTLY) on PostgreSQL 19+, the client's statement (VACUUM FULL,
// which locks the table) on 18.
func (a *Applier) repackStep(ctx context.Context, conn ShardConn, m *catalog.DDLMigration) error {
	rows, err := conn.Query(ctx, `SELECT current_setting('server_version_num')::int`)
	if err != nil {
		return err
	}
	version, err := pgx.CollectOneRow(rows, pgx.RowTo[int])
	if err != nil {
		return err
	}
	if version >= 190000 {
		_, err = conn.Exec(ctx, "REPACK (CONCURRENTLY) "+qualified(m.Meta.Object.Schema, m.Meta.Object.Name))
		return err
	}
	_, err = conn.Exec(ctx, m.Statement)
	return err
}
