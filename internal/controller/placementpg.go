package controller

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
)

// describe reads the table's shape from the first source shard and checks
// the constraints the new placement needs: a primary key (rows are applied
// by it), the new shard key present, and covered by the primary key or a
// unique constraint when the table becomes sharded.
func (p *Placer) describe(ctx context.Context, wf *placementWorkflow) error {
	sources := wf.from.Sources()
	conn, err := p.Shards.DialDatabase(ctx, wf.st.SourceSet, sources[0], wf.spec.Database)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	cols, err := tableColumns(ctx, conn, wf.spec.SchemaName, wf.spec.TableName)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return fatal("table %s does not exist on %s/%d", wf.spec.table(), wf.st.SourceSet, sources[0])
	}
	pk, err := primaryKey(ctx, conn, wf.spec.SchemaName, wf.spec.TableName)
	if err != nil {
		return err
	}
	if len(pk) == 0 {
		return fatal("table %s has no primary key; placement workflows apply rows by primary key", wf.spec.table())
	}
	wf.st.Columns, wf.st.PK = nil, pk
	for _, c := range cols {
		wf.st.Columns = append(wf.st.Columns, c.name)
	}
	if wf.spec.To.Placement == "sharded" {
		key := wf.spec.To.key()
		i := slices.IndexFunc(cols, func(c tableColumn) bool { return c.name == key })
		if i < 0 {
			return fatal("shard key column %q does not exist on table %s", key, wf.spec.table())
		}
		if _, err := KeyHashExpr(key, cols[i].typ); err != nil {
			return fatal("%s: %w", wf.spec.table(), err)
		}
		covered, err := keyCoveredByUnique(ctx, conn, wf.spec.SchemaName, wf.spec.TableName, key)
		if err != nil {
			return err
		}
		if !covered {
			return fatal("shard key %q of table %s must be part of the primary key or a unique constraint", key, wf.spec.table())
		}
		wf.st.KeyType = cols[i].typ
	} else if wf.spec.From.Placement == "sharded" {
		i := slices.IndexFunc(cols, func(c tableColumn) bool { return c.name == wf.spec.From.key() })
		if i >= 0 {
			wf.st.KeyType = cols[i].typ
		}
	}
	wf.st.Sources, wf.st.Targets, wf.st.Holders = wf.from.Sources(), wf.rt.Holders(), wf.from.Holders()
	return p.load(ctx, wf)
}

type tableColumn struct {
	name, typ, def string
	notNull        bool
	identity       string
}

func tableColumns(ctx context.Context, conn ShardConn, schema, name string) ([]tableColumn, error) {
	rows, err := conn.Query(ctx, `SELECT a.attname, format_type(a.atttypid, a.atttypmod), coalesce(pg_get_expr(d.adbin, d.adrelid), ''), a.attnotnull, a.attidentity::text
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum`, schema, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []tableColumn
	for rows.Next() {
		var c tableColumn
		if err := rows.Scan(&c.name, &c.typ, &c.def, &c.notNull, &c.identity); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func primaryKey(ctx context.Context, conn ShardConn, schema, name string) ([]string, error) {
	rows, err := conn.Query(ctx, `SELECT a.attname FROM pg_index i
		JOIN pg_class c ON c.oid = i.indrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN unnest(i.indkey) WITH ORDINALITY k(attnum, ord) ON true
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
		WHERE n.nspname = $1 AND c.relname = $2 AND i.indisprimary ORDER BY k.ord`, schema, name)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func keyCoveredByUnique(ctx context.Context, conn ShardConn, schema, name, key string) (bool, error) {
	rows, err := conn.Query(ctx, `SELECT EXISTS (SELECT 1 FROM pg_index i
		JOIN pg_class c ON c.oid = i.indrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY (i.indkey)
		WHERE n.nspname = $1 AND c.relname = $2 AND (i.indisprimary OR i.indisunique) AND a.attname = $3)`, schema, name, key)
	if err != nil {
		return false, err
	}
	return pgx.CollectExactlyOneRow(rows, pgx.RowTo[bool])
}

func tableExists(ctx context.Context, conn ShardConn, schema, name string) (bool, error) {
	rows, err := conn.Query(ctx, `SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'r')`, schema, name)
	if err != nil {
		return false, err
	}
	return pgx.CollectExactlyOneRow(rows, pgx.RowTo[bool])
}

// ensureShadows creates the shadow table on every shard of the new
// placement: LIKE the local table when the shard has one, else from the
// definition read on a source.
func (p *Placer) ensureShadows(ctx context.Context, wf *placementWorkflow) error {
	var ddl []string
	for _, t := range wf.rt.Holders() {
		conn, err := p.Shards.DialDatabase(ctx, wf.st.SourceSet, t, wf.spec.Database)
		if err != nil {
			return err
		}
		err = func() error {
			exists, err := tableExists(ctx, conn, wf.spec.SchemaName, wf.shadow())
			if err != nil || exists {
				return err
			}
			local, err := tableExists(ctx, conn, wf.spec.SchemaName, wf.spec.TableName)
			if err != nil {
				return err
			}
			if local {
				_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (LIKE %s INCLUDING ALL)", wf.shape.qualified(wf.shadow()), wf.shape.qualified(wf.spec.TableName)))
				return err
			}
			if ddl == nil {
				if ddl, err = p.shadowDDL(ctx, wf); err != nil {
					return err
				}
			}
			for _, stmt := range ddl {
				if _, err := conn.Exec(ctx, stmt); err != nil {
					return fmt.Errorf("%s: %w", stmt, err)
				}
			}
			return nil
		}()
		_ = conn.Close(ctx)
		if err != nil {
			return fmt.Errorf("shadow table on %s/%d: %w", wf.st.SourceSet, t, err)
		}
	}
	return nil
}

var nextvalRE = regexp.MustCompile(`nextval\('([^']+)'::regclass\)`)

// shadowDDL renders the shadow table from the source table's catalog
// entries: columns with defaults and identity, constraints, and the other
// indexes, all named after the shadow table.
func (p *Placer) shadowDDL(ctx context.Context, wf *placementWorkflow) ([]string, error) {
	conn, err := p.Shards.DialDatabase(ctx, wf.st.SourceSet, wf.from.Sources()[0], wf.spec.Database)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(ctx) }()
	cols, err := tableColumns(ctx, conn, wf.spec.SchemaName, wf.spec.TableName)
	if err != nil {
		return nil, err
	}
	var out []string
	var defs []string
	for _, c := range cols {
		d := QuoteIdent(c.name) + " " + c.typ
		if c.notNull {
			d += " NOT NULL"
		}
		switch c.identity {
		case "a":
			d += " GENERATED ALWAYS AS IDENTITY"
		case "d":
			d += " GENERATED BY DEFAULT AS IDENTITY"
		default:
			if c.def != "" {
				for _, m := range nextvalRE.FindAllStringSubmatch(c.def, -1) {
					out = append(out, "CREATE SEQUENCE IF NOT EXISTS "+m[1])
				}
				d += " DEFAULT " + c.def
			}
		}
		defs = append(defs, d)
	}
	out = append(out, fmt.Sprintf("CREATE TABLE %s (%s)", wf.shape.qualified(wf.shadow()), strings.Join(defs, ", ")))
	rows, err := conn.Query(ctx, `SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint
		WHERE conrelid = $1::regclass AND contype IN ('p', 'u', 'c') ORDER BY contype, conname`, wf.shape.qualified(wf.spec.TableName))
	if err != nil {
		return nil, err
	}
	cons, err := pgx.CollectRows(rows, pgx.RowToStructByPos[struct{ Name, Def string }])
	if err != nil {
		return nil, err
	}
	for _, c := range cons {
		out = append(out, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s", wf.shape.qualified(wf.shadow()), QuoteIdent(shadowName(c.Name, wf.spec.TableName, wf.shadow())), c.Def))
	}
	rows, err = conn.Query(ctx, `SELECT c.relname, pg_get_indexdef(i.indexrelid) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
		WHERE i.indrelid = $1::regclass AND NOT EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conindid = i.indexrelid)
		ORDER BY c.relname`, wf.shape.qualified(wf.spec.TableName))
	if err != nil {
		return nil, err
	}
	idx, err := pgx.CollectRows(rows, pgx.RowToStructByPos[struct{ Name, Def string }])
	if err != nil {
		return nil, err
	}
	for _, i := range idx {
		def := strings.Replace(i.Def, " ON "+wf.shape.qualified(wf.spec.TableName)+" ", " ON "+wf.shape.qualified(wf.shadow())+" ", 1)
		def = strings.Replace(def, " ON "+wf.spec.SchemaName+"."+wf.spec.TableName+" ", " ON "+wf.shape.qualified(wf.shadow())+" ", 1)
		def = strings.Replace(def, " INDEX "+QuoteIdent(i.Name)+" ", " INDEX "+QuoteIdent(shadowName(i.Name, wf.spec.TableName, wf.shadow()))+" ", 1)
		def = strings.Replace(def, " INDEX "+i.Name+" ", " INDEX "+QuoteIdent(shadowName(i.Name, wf.spec.TableName, wf.shadow()))+" ", 1)
		out = append(out, def)
	}
	return out, nil
}

// shadowName renames an index or constraint of the table for its shadow the
// way LIKE INCLUDING ALL does: the table name inside it becomes the shadow's.
func shadowName(name, table, shadow string) string {
	if strings.HasPrefix(name, table) {
		return shadow + strings.TrimPrefix(name, table)
	}
	return shadow + "_" + name
}

// copyAll runs the initial copy of every source not copied yet: a slot
// and publication first (so every change since is replayed by the
// catch-up), then a keyset walk under one REPEATABLE READ snapshot whose
// rows are upserted into the shadow tables by the new placement.
func (p *Placer) copyAll(ctx context.Context, wf *placementWorkflow) error {
	for _, s := range wf.from.Sources() {
		if wf.st.Copied[fmt.Sprint(s)] {
			continue
		}
		if err := p.copySource(ctx, wf, s); err != nil {
			return fmt.Errorf("copy of %s from %s/%d: %w", wf.spec.table(), wf.st.SourceSet, s, err)
		}
		wf.st.Copied[fmt.Sprint(s)] = true
		if err := p.save(ctx, wf, fmt.Sprintf("copied %s from %s/%d", wf.spec.table(), wf.st.SourceSet, s)); err != nil {
			return err
		}
	}
	return nil
}

func (p *Placer) copySource(ctx context.Context, wf *placementWorkflow, s int32) error {
	conn, err := p.Shards.DialDatabase(ctx, wf.st.SourceSet, s, wf.spec.Database)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	if err := p.ensureReplication(ctx, wf, conn, s); err != nil {
		return err
	}
	targets, err := p.targetConns(ctx, wf)
	if err != nil {
		return err
	}
	defer targets.close(ctx)
	cols, err := tableColumns(ctx, conn, wf.spec.SchemaName, wf.spec.TableName)
	if err != nil {
		return err
	}
	typeOf := map[string]string{}
	var selectCols []string
	for _, c := range cols {
		typeOf[c.name] = c.typ
		selectCols = append(selectCols, QuoteIdent(c.name)+"::text")
	}
	if !slices.Equal(wf.st.Columns, colNames(cols)) {
		return fatal("columns of %s on %s/%d differ from the recorded shape", wf.spec.table(), wf.st.SourceSet, s)
	}
	// The ORDER BY and the keyset bound name the table's columns through
	// the alias: a bare name would pick the ::text output column and sort
	// numbers as text.
	var pkCols []string
	for _, k := range wf.st.PK {
		pkCols = append(pkCols, "src."+QuoteIdent(k))
	}
	pkIdx := wf.shape.pkIndexes()
	if _, err := conn.Exec(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"); err != nil {
		return err
	}
	defer func() { _, _ = conn.Exec(ctx, "ROLLBACK") }()
	var last *Tuple
	for {
		sql := fmt.Sprintf("SELECT %s FROM %s AS src", strings.Join(selectCols, ", "), wf.shape.qualified(wf.spec.TableName))
		if last != nil {
			var bounds []string
			for _, i := range pkIdx {
				bounds = append(bounds, QuoteLiteral(last.Values[i])+"::"+typeOf[wf.st.Columns[i]])
			}
			sql += fmt.Sprintf(" WHERE (%s) > (%s)", strings.Join(pkCols, ", "), strings.Join(bounds, ", "))
		}
		sql += fmt.Sprintf(" ORDER BY %s LIMIT %d", strings.Join(pkCols, ", "), p.copyBatch())
		rows, err := conn.Query(ctx, sql)
		if err != nil {
			return err
		}
		batch, err := collectTuples(rows, len(cols))
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		byTarget := map[int32][]*Tuple{}
		for _, t := range batch {
			dests, err := wf.rt.Route(t.Values)
			if err != nil {
				return fatal("%w", err)
			}
			for _, d := range dests {
				byTarget[d] = append(byTarget[d], t)
			}
		}
		for _, d := range sortedInt32Keys(byTarget) {
			for _, stmt := range wf.shape.UpsertSQL(wf.shadow(), byTarget[d]) {
				if _, err := targets[d].Exec(ctx, stmt); err != nil {
					return fmt.Errorf("target %s/%d: %w", wf.st.SourceSet, d, err)
				}
			}
		}
		last = batch[len(batch)-1]
	}
}

func colNames(cols []tableColumn) []string {
	var out []string
	for _, c := range cols {
		out = append(out, c.name)
	}
	return out
}

func collectTuples(rows pgx.Rows, n int) ([]*Tuple, error) {
	defer rows.Close()
	var out []*Tuple
	for rows.Next() {
		vals := make([]*string, n)
		ptrs := make([]any, n)
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		out = append(out, &Tuple{Values: vals, Unchanged: make([]bool, n)})
	}
	return out, rows.Err()
}

type targetConns map[int32]ShardConn

func (t targetConns) close(ctx context.Context) {
	for _, c := range t {
		_ = c.Close(ctx)
	}
}

func (p *Placer) targetConns(ctx context.Context, wf *placementWorkflow) (targetConns, error) {
	out := targetConns{}
	for _, t := range wf.rt.Holders() {
		conn, err := p.Shards.DialDatabase(ctx, wf.st.SourceSet, t, wf.spec.Database)
		if err != nil {
			out.close(ctx)
			return nil, err
		}
		out[t] = conn
	}
	return out, nil
}

// ensureReplication creates the publication and the pgoutput slot of one
// source and gives the table a full replica identity so updates carry the
// old row (the old shard of a moved row is derived from it).
func (p *Placer) ensureReplication(ctx context.Context, wf *placementWorkflow, conn ShardConn, s int32) error {
	var ident string
	rows, err := conn.Query(ctx, `SELECT relreplident::text FROM pg_class WHERE oid = $1::regclass`, wf.shape.qualified(wf.spec.TableName))
	if err != nil {
		return err
	}
	if ident, err = pgx.CollectExactlyOneRow(rows, pgx.RowTo[string]); err != nil {
		return err
	}
	if ident != "f" {
		if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", wf.shape.qualified(wf.spec.TableName))); err != nil {
			return err
		}
		if !slices.Contains(wf.st.ReplicaIdentityFull, s) {
			wf.st.ReplicaIdentityFull = append(wf.st.ReplicaIdentityFull, s)
		}
	}
	rows, err = conn.Query(ctx, `SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`, wf.publicationName())
	if err != nil {
		return err
	}
	exists, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[bool])
	if err != nil {
		return err
	}
	if !exists {
		if _, err := conn.Exec(ctx, CreatePublicationSQL(wf.publicationName(), []PublishedTable{{Schema: wf.spec.SchemaName, Name: wf.spec.TableName}})); err != nil {
			return err
		}
	}
	rows, err = conn.Query(ctx, `SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, wf.slotName(s))
	if err != nil {
		return err
	}
	if exists, err = pgx.CollectExactlyOneRow(rows, pgx.RowTo[bool]); err != nil {
		return err
	}
	if !exists {
		if _, err := conn.Exec(ctx, `SELECT pg_create_logical_replication_slot($1, 'pgoutput')`, wf.slotName(s)); err != nil {
			return err
		}
	}
	return nil
}

// catchUp applies the pending changes of every source slot to the shadow
// tables. It returns the largest remaining slot lag and how many row
// changes it applied; drain keeps reading until every slot is empty.
func (p *Placer) catchUp(ctx context.Context, wf *placementWorkflow, drain bool) (lag int64, applied int, err error) {
	targets, err := p.targetConns(ctx, wf)
	if err != nil {
		return 0, 0, err
	}
	defer targets.close(ctx)
	for _, s := range wf.from.Sources() {
		conn, err := p.Shards.DialDatabase(ctx, wf.st.SourceSet, s, wf.spec.Database)
		if err != nil {
			return 0, 0, err
		}
		l, n, err := p.catchUpSource(ctx, wf, conn, targets, s, drain)
		_ = conn.Close(ctx)
		if err != nil {
			return 0, 0, fmt.Errorf("catch-up of %s from %s/%d: %w", wf.spec.table(), wf.st.SourceSet, s, err)
		}
		lag = max(lag, l)
		applied += n
		wf.st.Applied[fmt.Sprint(s)] += int64(n)
	}
	return lag, applied, nil
}

const peekChanges = 2000

func (p *Placer) catchUpSource(ctx context.Context, wf *placementWorkflow, conn ShardConn, targets targetConns, s int32, drain bool) (int64, int, error) {
	dec := NewDecoder()
	applied := 0
	limit := peekChanges
	for round := 0; ; round++ {
		rows, err := conn.Query(ctx, `SELECT lsn::text, data FROM pg_logical_slot_peek_binary_changes($1, NULL, $2, 'proto_version', '1', 'publication_names', $3)`,
			wf.slotName(s), limit, wf.publicationName())
		if err != nil {
			return 0, applied, err
		}
		msgs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[struct {
			LSN  string
			Data []byte
		}])
		if err != nil {
			return 0, applied, err
		}
		var pending []applyOp
		var commitLSN string
		n := 0
		for _, m := range msgs {
			c, committed, err := dec.Decode(m.Data)
			if err != nil {
				return 0, applied, err
			}
			if committed {
				if err := applyOps(ctx, targets, pending); err != nil {
					return 0, applied, err
				}
				applied += n
				pending, n, commitLSN = nil, 0, m.LSN
				continue
			}
			if c == nil || c.Relation.Schema != wf.spec.SchemaName || c.Relation.Name != wf.spec.TableName {
				continue
			}
			if !slices.Equal(c.Relation.Columns, wf.st.Columns) {
				return 0, applied, fatal("columns of %s changed during the workflow (%v)", wf.spec.table(), c.Relation.Columns)
			}
			ops, err := routeChange(wf.rt, wf.shape, wf.shadow(), c)
			if err != nil {
				return 0, applied, fatal("%w", err)
			}
			pending = append(pending, ops...)
			n++
		}
		switch {
		case commitLSN != "":
			if _, err := conn.Exec(ctx, `SELECT pg_replication_slot_advance($1, $2::pg_lsn)`, wf.slotName(s), commitLSN); err != nil {
				return 0, applied, err
			}
		case len(msgs) == 0:
			// Nothing decodes between the slot and the end of WAL: a
			// transaction that commits later is decoded in full from its
			// start, so the slot may follow the WAL end.
			if _, err := conn.Exec(ctx, `SELECT pg_replication_slot_advance($1, pg_current_wal_lsn())`, wf.slotName(s)); err != nil {
				return 0, applied, err
			}
		case len(msgs) >= limit:
			limit *= 4
			continue
		}
		lag, err := slotLag(ctx, conn, wf.slotName(s))
		if err != nil {
			return 0, applied, err
		}
		if len(msgs) < limit || !drain {
			return lag, applied, nil
		}
	}
}

func applyOps(ctx context.Context, targets targetConns, ops []applyOp) error {
	for _, op := range ops {
		conn, ok := targets[op.shard]
		if !ok {
			return fmt.Errorf("no connection to target shard %d", op.shard)
		}
		if _, err := conn.Exec(ctx, op.sql); err != nil {
			return fmt.Errorf("target %d: %w", op.shard, err)
		}
	}
	return nil
}

func slotLag(ctx context.Context, conn ShardConn, slot string) (int64, error) {
	rows, err := conn.Query(ctx, `SELECT (pg_current_wal_lsn() - confirmed_flush_lsn)::bigint FROM pg_replication_slots WHERE slot_name = $1`, slot)
	if err != nil {
		return 0, err
	}
	lag, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[int64])
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("slot %s vanished", slot)
	}
	return max(lag, 0), err
}

// fence raises the table-scoped write pause routers observe.
func (p *Placer) fence(ctx context.Context, wf *placementWorkflow) error {
	_, err := p.Pool.Exec(ctx, `UPDATE pgshard.table_status SET migrating = true, workflow_id = $4::uuid, updated_at = now()
		WHERE database = $1 AND schema_name = $2 AND table_name = $3 AND NOT migrating`, wf.spec.Database, wf.spec.SchemaName, wf.spec.TableName, wf.id)
	return err
}

func (p *Placer) releaseFence(ctx context.Context, wf *placementWorkflow) error {
	_, err := p.Pool.Exec(ctx, `UPDATE pgshard.table_status SET migrating = false, updated_at = now()
		WHERE database = $1 AND schema_name = $2 AND table_name = $3 AND migrating`, wf.spec.Database, wf.spec.SchemaName, wf.spec.TableName)
	return err
}

func (p *Placer) unlock(ctx context.Context, wf *placementWorkflow) error {
	_, err := p.Pool.Exec(ctx, `DELETE FROM pgshard.workflow_locks WHERE workflow_id = $1::uuid`, wf.id)
	return err
}

// swapAll renames the tables on every serving shard in one transaction per
// shard: the shadow becomes the table where the new placement holds it,
// the previous table becomes <table>__pgshard_old wherever it existed.
// Sequences owned by columns of the old table move to the new one so the
// old table's drop does not take them along.
func (p *Placer) swapAll(ctx context.Context, wf *placementWorkflow) error {
	for _, t := range wf.rt.ids {
		conn, err := p.Shards.DialDatabase(ctx, wf.st.SourceSet, t, wf.spec.Database)
		if err != nil {
			return err
		}
		err = p.swapOn(ctx, wf, conn, slices.Contains(wf.rt.Holders(), t))
		_ = conn.Close(ctx)
		if err != nil {
			return fmt.Errorf("swap on %s/%d: %w", wf.st.SourceSet, t, err)
		}
	}
	return nil
}

func (p *Placer) swapOn(ctx context.Context, wf *placementWorkflow, conn ShardConn, holder bool) error {
	table, shadow, old := wf.shape.qualified(wf.spec.TableName), wf.shape.qualified(wf.shadow()), wf.shape.qualified(wf.old())
	if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
		return err
	}
	defer func() { _, _ = conn.Exec(ctx, "ROLLBACK") }()
	hasShadow, err := tableExists(ctx, conn, wf.spec.SchemaName, wf.shadow())
	if err != nil {
		return err
	}
	hasTable, err := tableExists(ctx, conn, wf.spec.SchemaName, wf.spec.TableName)
	if err != nil {
		return err
	}
	hasOld, err := tableExists(ctx, conn, wf.spec.SchemaName, wf.old())
	if err != nil {
		return err
	}
	if holder && !hasShadow {
		if !hasTable {
			return fatal("neither %s nor its shadow exists", table)
		}
		return nil
	}
	if !holder && !hasTable {
		return nil
	}
	if hasTable {
		if hasOld {
			if _, err := conn.Exec(ctx, "DROP TABLE "+old); err != nil {
				return err
			}
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s RENAME TO %s", table, QuoteIdent(wf.old()))); err != nil {
			return err
		}
	}
	if holder {
		if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s RENAME TO %s", shadow, QuoteIdent(wf.spec.TableName))); err != nil {
			return err
		}
		if hasTable {
			if err := moveOwnedSequences(ctx, conn, wf.spec.SchemaName, wf.old(), wf.spec.TableName); err != nil {
				return err
			}
		}
	}
	_, err = conn.Exec(ctx, "COMMIT")
	return err
}

func moveOwnedSequences(ctx context.Context, conn ShardConn, schema, from, to string) error {
	rows, err := conn.Query(ctx, `SELECT s.oid::regclass::text, a.attname FROM pg_depend d
		JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S'
		JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
		WHERE d.refobjid = $1::regclass AND d.deptype = 'a'`, QuoteIdent(schema)+"."+QuoteIdent(from))
	if err != nil {
		return err
	}
	seqs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[struct{ Seq, Col string }])
	if err != nil {
		return err
	}
	for _, s := range seqs {
		if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER SEQUENCE %s OWNED BY %s.%s.%s", s.Seq, QuoteIdent(schema), QuoteIdent(to), QuoteIdent(s.Col))); err != nil {
			return err
		}
	}
	return nil
}

// publish flips the effective placement in one catalog transaction: the
// table's status row, the fence, the lock and the shard map generation.
func (p *Placer) publish(ctx context.Context, wf *placementWorkflow) error {
	if wf.st.SwappedAt != nil {
		return nil
	}
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `UPDATE pgshard.table_status
		SET effective_placement = $4, effective_shard_key = $5, effective_generation = $6, migrating = false, updated_at = now()
		WHERE database = $1 AND schema_name = $2 AND table_name = $3`,
		wf.spec.Database, wf.spec.SchemaName, wf.spec.TableName, wf.spec.To.Placement, wf.spec.To.ShardKey, wf.spec.DesiredGeneration); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM pgshard.workflow_locks WHERE workflow_id = $1::uuid`, wf.id); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE pgshard.shard_map_generation SET generation = generation + 1, updated_at = now()`); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	now := p.now()
	wf.st.SwappedAt = &now
	if wf.st.FencedAt != nil {
		wf.st.PauseMS = now.Sub(*wf.st.FencedAt).Milliseconds()
	}
	return nil
}

// dropReplication drops the slots and publications of the run on every
// source; a cancelled run also restores the replica identity it changed
// (a completed one dropped that table).
func (p *Placer) dropReplication(ctx context.Context, wf *placementWorkflow) error {
	for _, s := range wf.from.Sources() {
		conn, err := p.Shards.DialDatabase(ctx, wf.st.SourceSet, s, wf.spec.Database)
		if err != nil {
			return err
		}
		err = func() error {
			if _, err := conn.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1`, wf.slotName(s)); err != nil {
				return err
			}
			if _, err := conn.Exec(ctx, "DROP PUBLICATION IF EXISTS "+QuoteIdent(wf.publicationName())); err != nil {
				return err
			}
			if wf.stage == StageCancelling && slices.Contains(wf.st.ReplicaIdentityFull, s) {
				if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY DEFAULT", wf.shape.qualified(wf.spec.TableName))); err != nil {
					return err
				}
			}
			return nil
		}()
		_ = conn.Close(ctx)
		if err != nil {
			return fmt.Errorf("replication objects on %s/%d: %w", wf.st.SourceSet, s, err)
		}
	}
	return nil
}

// dropOld drops the previous tables and gives the new table's indexes and
// constraints their final names.
func (p *Placer) dropOld(ctx context.Context, wf *placementWorkflow) error {
	for _, t := range wf.rt.ids {
		conn, err := p.Shards.DialDatabase(ctx, wf.st.SourceSet, t, wf.spec.Database)
		if err != nil {
			return err
		}
		err = func() error {
			if _, err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+wf.shape.qualified(wf.old())); err != nil {
				return err
			}
			if !slices.Contains(wf.rt.Holders(), t) {
				return nil
			}
			return renameShadowIndexes(ctx, conn, wf)
		}()
		_ = conn.Close(ctx)
		if err != nil {
			return fmt.Errorf("retire on %s/%d: %w", wf.st.SourceSet, t, err)
		}
	}
	return nil
}

func renameShadowIndexes(ctx context.Context, conn ShardConn, wf *placementWorkflow) error {
	table := wf.shape.qualified(wf.spec.TableName)
	rows, err := conn.Query(ctx, `SELECT conname FROM pg_constraint WHERE conrelid = $1::regclass AND conname LIKE $2 ORDER BY conname`, table, "%"+ShadowSuffix+"%")
	if err != nil {
		return err
	}
	cons, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	for _, name := range cons {
		if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s RENAME CONSTRAINT %s TO %s", table, QuoteIdent(name), QuoteIdent(strings.Replace(name, ShadowSuffix, "", 1)))); err != nil {
			return err
		}
	}
	rows, err = conn.Query(ctx, `SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
		WHERE i.indrelid = $1::regclass AND c.relname LIKE $2 AND NOT EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conindid = c.oid) ORDER BY c.relname`, table, "%"+ShadowSuffix+"%")
	if err != nil {
		return err
	}
	idx, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	for _, name := range idx {
		if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER INDEX %s.%s RENAME TO %s", QuoteIdent(wf.spec.SchemaName), QuoteIdent(name), QuoteIdent(strings.Replace(name, ShadowSuffix, "", 1)))); err != nil {
			return err
		}
	}
	return nil
}

func sortedInt32Keys[V any](m map[int32]V) []int32 {
	out := make([]int32, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
