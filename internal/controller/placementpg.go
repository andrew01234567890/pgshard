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
	// The shadow table is rebuilt from columns, constraints and indexes only:
	// neither CREATE TABLE LIKE nor the remote DDL carries row-level security
	// policies, triggers or foreign keys, and the swap would silently drop
	// them (and leave inbound foreign keys bound to the retired table's OID).
	// Refuse rather than lose enforcement. Every source is checked, not just
	// the first: DDL is meant to be identical across shards, but a feature
	// present on any one of them would still be lost there.
	for _, src := range sources {
		sconn := conn
		if src != sources[0] {
			c2, derr := p.Shards.DialDatabase(ctx, wf.st.SourceSet, src, wf.spec.Database)
			if derr != nil {
				return derr
			}
			sconn = c2
		}
		unsupported, uerr := unsupportedTableFeatures(ctx, sconn, wf.spec.SchemaName, wf.spec.TableName)
		if src != sources[0] {
			_ = sconn.Close(ctx)
		}
		if uerr != nil {
			return uerr
		}
		if len(unsupported) > 0 {
			return fatal("table %s on %s/%d has features a placement move cannot yet preserve (%s); drop them before moving or keep the table where it is",
				wf.spec.table(), wf.st.SourceSet, src, strings.Join(unsupported, ", "))
		}
	}
	comment, err := tableComment(ctx, conn, wf.spec.SchemaName, wf.spec.TableName)
	if err != nil {
		return err
	}
	wf.st.TableComment = comment
	// A column defaulting to a sequence in another schema cannot be moved
	// safely: the target's rebuilt sequence would not be advanced past the
	// copied rows (it is not owned, so pg_get_serial_sequence cannot find it),
	// and such a sequence is typically shared, so per-shard copies collide.
	// Refuse rather than silently reissue identifiers.
	for _, c := range cols {
		for _, m := range nextvalRE.FindAllStringSubmatch(c.def, -1) {
			same, serr := sequenceInSchema(ctx, conn, m[1], wf.spec.SchemaName)
			if serr != nil {
				return serr
			}
			if !same {
				return fatal("column %q of table %s defaults to a sequence in another schema (%s); moving such a table is not supported", c.name, wf.spec.table(), m[1])
			}
		}
	}
	// A generated column cannot key the move: it is not part of the copied
	// row shape (it is recomputed on the target), so routing by it or
	// applying rows by a primary key that contains it has nothing to read.
	// Refuse up front rather than fail per row deep in the copy.
	copied := copiedColumns(cols)
	for _, k := range pk {
		if !slices.Contains(colNames(copied), k) {
			return fatal("primary key column %q of table %s is a generated column; placement workflows cannot apply rows by it", k, wf.spec.table())
		}
	}
	if wf.spec.To.Placement == "sharded" && !slices.Contains(colNames(copied), wf.spec.To.key()) {
		if slices.IndexFunc(cols, func(c tableColumn) bool { return c.name == wf.spec.To.key() }) >= 0 {
			return fatal("shard key column %q of table %s is a generated column; rows cannot be routed by it", wf.spec.To.key(), wf.spec.table())
		}
	}
	wf.st.Columns, wf.st.Identity, wf.st.PK = nil, nil, pk
	for _, c := range copied {
		wf.st.Columns = append(wf.st.Columns, c.name)
		wf.st.Identity = append(wf.st.Identity, c.identity)
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
		uncovered, err := uniqueConstraintsMissingKey(ctx, conn, wf.spec.SchemaName, wf.spec.TableName, key)
		if err != nil {
			return err
		}
		if len(uncovered) > 0 {
			return fatal("shard key %q of table %s is absent from unique/exclusion constraint(s) %s; every global uniqueness key must contain the shard key",
				key, wf.spec.table(), strings.Join(uncovered, ", "))
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
	// generated is attgenerated: "s" stored, "v" virtual, "" for a plain column.
	generated string
	// collate is the qualified collation name when it differs from the
	// type's default, "" otherwise.
	collate string
}

func tableColumns(ctx context.Context, conn ShardConn, schema, name string) ([]tableColumn, error) {
	rows, err := conn.Query(ctx, `SELECT a.attname, format_type(a.atttypid, a.atttypmod), coalesce(pg_get_expr(d.adbin, d.adrelid), ''), a.attnotnull, a.attidentity::text, a.attgenerated::text,
		CASE WHEN a.attcollation <> 0 AND a.attcollation <> ty.typcollation THEN format('%I.%I', cn.nspname, co.collname) ELSE '' END
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_type ty ON ty.oid = a.atttypid
		LEFT JOIN pg_collation co ON co.oid = a.attcollation
		LEFT JOIN pg_namespace cn ON cn.oid = co.collnamespace
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
		if err := rows.Scan(&c.name, &c.typ, &c.def, &c.notNull, &c.identity, &c.generated, &c.collate); err != nil {
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

// uniqueConstraintsMissingKey returns the names of every unique or exclusion
// index on the table that cannot stay globally enforceable once rows split
// across shards, so sharding must be refused. A unique index is safe only when
// the shard key is one of its key columns (ord <= indnkeyatts, never an
// INCLUDE column) AND that column uses a deterministic collation and the
// default operator class: index equality must match pgshard's raw-hash
// distribution equality, or two "equal" values could hash to different shards.
// Exclusion constraints (including PG18 temporal WITHOUT OVERLAPS keys, whose
// index is indisexclusion) are refused outright: their targets are not yet
// recreated on shadow shards and their operators are not verified here.
// Expression and partial unique indexes are reported too (fail closed).
func uniqueConstraintsMissingKey(ctx context.Context, conn ShardConn, schema, name, key string) ([]string, error) {
	rows, err := conn.Query(ctx, `SELECT ix.relname FROM pg_index i
		JOIN pg_class c ON c.oid = i.indrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_class ix ON ix.oid = i.indexrelid
		WHERE n.nspname = $1 AND c.relname = $2 AND (i.indisunique OR i.indisexclusion)
		AND NOT (
			NOT i.indisexclusion
			AND EXISTS (
				SELECT 1 FROM unnest(i.indkey) WITH ORDINALITY k(attnum, ord)
				JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
				WHERE k.ord <= i.indnkeyatts AND a.attname = $3
				AND NOT EXISTS (
					SELECT 1 FROM unnest(i.indcollation::oid[]) WITH ORDINALITY ic(coll, cord)
					JOIN pg_collation cl ON cl.oid = ic.coll
					WHERE ic.cord = k.ord AND NOT cl.collisdeterministic)
				AND EXISTS (
					SELECT 1 FROM unnest(i.indclass::oid[]) WITH ORDINALITY icl(cls, clord)
					JOIN pg_opclass oc ON oc.oid = icl.cls
					WHERE icl.clord = k.ord AND oc.opcdefault)))
		ORDER BY ix.relname`, schema, name, key)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// unsupportedTableFeatures lists what a placement move cannot yet preserve on
// the table. The shadow is rebuilt from columns, constraints and indexes, so
// everything below would be silently lost at the swap: row-level security
// (an enabled-but-policy-less table denies all today and would allow all
// after), the owner and table/column privileges (an outage for application
// roles, or a privilege leak), user triggers, foreign keys in either direction
// (inbound ones would keep pointing at the retired table's OID), rewrite
// rules, inheritance/partition membership, user publications (downstream
// subscribers would silently stop receiving), and a non-default replica
// identity (the shadow is created with DEFAULT, so downstream logical
// replication of UPDATE/DELETE would break after the move).
func unsupportedTableFeatures(ctx context.Context, conn ShardConn, schema, name string) ([]string, error) {
	rows, err := conn.Query(ctx, `WITH t AS (
			SELECT c.oid, c.relowner, c.relacl, c.relrowsecurity, c.relforcerowsecurity, c.relreplident
			FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname = $2)
		SELECT f FROM (
			SELECT 'row-level security policy ' || polname AS f FROM pg_policy, t WHERE polrelid = t.oid
			UNION ALL SELECT 'replica identity ' || CASE t.relreplident WHEN 'f' THEN 'FULL' WHEN 'i' THEN 'USING INDEX' ELSE 'NOTHING' END
				FROM t WHERE t.relreplident <> 'd'
			UNION ALL SELECT 'row-level security enabled' FROM t WHERE t.relrowsecurity OR t.relforcerowsecurity
			UNION ALL SELECT 'owner ' || pg_get_userbyid(t.relowner) FROM t
				WHERE t.relowner <> (SELECT oid FROM pg_roles WHERE rolname = current_user)
			UNION ALL SELECT 'table privileges' FROM t WHERE t.relacl IS NOT NULL
			UNION ALL SELECT 'column privileges on ' || attname FROM pg_attribute, t
				WHERE attrelid = t.oid AND attnum > 0 AND NOT attisdropped AND attacl IS NOT NULL
			UNION ALL SELECT 'trigger ' || tgname FROM pg_trigger, t WHERE tgrelid = t.oid AND NOT tgisinternal
			UNION ALL SELECT 'foreign key ' || conname FROM pg_constraint, t
				WHERE contype = 'f' AND (conrelid = t.oid OR confrelid = t.oid)
			UNION ALL SELECT 'rule ' || rulename FROM pg_rewrite, t WHERE ev_class = t.oid AND rulename <> '_RETURN'
			UNION ALL SELECT 'inheritance/partition membership' FROM pg_inherits, t
				WHERE inhrelid = t.oid OR inhparent = t.oid
			UNION ALL SELECT 'publication ' || p.pubname FROM pg_publication_rel pr
				JOIN pg_publication p ON p.oid = pr.prpubid, t WHERE pr.prrelid = t.oid
		) x ORDER BY f`, schema, name)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func tableExists(ctx context.Context, conn ShardConn, schema, name string) (bool, error) {
	rows, err := conn.Query(ctx, `SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'r')`, schema, name)
	if err != nil {
		return false, err
	}
	return pgx.CollectExactlyOneRow(rows, pgx.RowTo[bool])
}

// placementMarker is the table comment that stamps a shadow or retired table as
// this workflow's own artifact. Including the workflow id makes it unguessable,
// so a user table that merely shares the __pgshard_new / __pgshard_old name (or
// even carries the bare prefix) is never adopted, overwritten or dropped.
func (wf *placementWorkflow) placementMarker() string {
	return "pgshard:placement:" + wf.id
}

// ensureShadows creates the shadow table on every shard of the new
// placement: LIKE the local table when the shard has one, else from the
// definition read on a source.

func markPlacementArtifact(ctx context.Context, conn ShardConn, schema, name, marker string) error {
	_, err := conn.Exec(ctx, fmt.Sprintf("COMMENT ON TABLE %s.%s IS %s",
		QuoteIdent(schema), QuoteIdent(name), QuoteLiteral(&marker)))
	return err
}

// isPlacementArtifact reports whether schema.name carries this workflow's exact
// marker. A missing table, a user table, and an artifact left by a different
// workflow all return false (the latter must be resolved by an operator rather
// than clobbered).
func isPlacementArtifact(ctx context.Context, conn ShardConn, schema, name, marker string) (bool, error) {
	rows, err := conn.Query(ctx, `SELECT obj_description((quote_ident($1) || '.' || quote_ident($2))::regclass, 'pg_class')`, schema, name)
	if err != nil {
		return false, err
	}
	comment, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[*string])
	if err != nil {
		return false, err
	}
	return comment != nil && *comment == marker, nil
}

// tableComment returns the table's comment, or nil when it has none.
func tableComment(ctx context.Context, conn ShardConn, schema, name string) (*string, error) {
	rows, err := conn.Query(ctx, `SELECT obj_description((quote_ident($1) || '.' || quote_ident($2))::regclass, 'pg_class')`, schema, name)
	if err != nil {
		return nil, err
	}
	return pgx.CollectExactlyOneRow(rows, pgx.RowTo[*string])
}

// restoreComment sets (or clears) a table's comment, used to put the user's
// original comment back after the shadow that carried the pgshard marker is
// renamed into place.
func restoreComment(ctx context.Context, conn ShardConn, schema, name string, comment *string) error {
	lit := "NULL"
	if comment != nil {
		// quoteLiteralE, not QuoteLiteral: under standard_conforming_strings a
		// plain '...' keeps backslashes literal, so a comment with a backslash
		// must use the E'...' form to round-trip unchanged.
		lit = quoteLiteralE(comment)
	}
	_, err := conn.Exec(ctx, fmt.Sprintf("COMMENT ON TABLE %s.%s IS %s", QuoteIdent(schema), QuoteIdent(name), lit))
	return err
}

// dropArtifactTable drops schema.name only if it exists and carries this
// workflow's marker, so a same-named user table is never dropped. The lock,
// marker re-check and DROP run in one transaction so a concurrent rename
// cannot slip an unmarked table under the name between the check and the drop.
func dropArtifactTable(ctx context.Context, conn ShardConn, schema, name, marker string) error {
	exists, err := tableExists(ctx, conn, schema, name)
	if err != nil || !exists {
		return err
	}
	if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
		return err
	}
	defer func() { _, _ = conn.Exec(ctx, "ROLLBACK") }()
	qual := QuoteIdent(schema) + "." + QuoteIdent(name)
	if _, err := conn.Exec(ctx, "LOCK TABLE "+qual+" IN ACCESS EXCLUSIVE MODE"); err != nil {
		return err
	}
	ours, err := isPlacementArtifact(ctx, conn, schema, name, marker)
	if err != nil || !ours {
		return err
	}
	if _, err := conn.Exec(ctx, "DROP TABLE "+qual); err != nil {
		return err
	}
	_, err = conn.Exec(ctx, "COMMIT")
	return err
}

func (p *Placer) ensureShadows(ctx context.Context, wf *placementWorkflow) error {
	var ddl []string
	for _, t := range wf.rt.Holders() {
		conn, err := p.Shards.DialDatabase(ctx, wf.st.SourceSet, t, wf.spec.Database)
		if err != nil {
			return err
		}
		marker := wf.placementMarker()
		err = func() error {
			exists, err := tableExists(ctx, conn, wf.spec.SchemaName, wf.shadow())
			if err != nil {
				return err
			}
			if exists {
				// Adopt a shadow only if a prior pass of THIS workflow created
				// it; a same-named user table, or an artifact left by another
				// workflow, does not match the marker and must not be written
				// into.
				ours, err := isPlacementArtifact(ctx, conn, wf.spec.SchemaName, wf.shadow(), marker)
				if err != nil {
					return err
				}
				if !ours {
					return fatal("a table named %s already exists and is not this workflow's shadow; rename it before sharding %s", wf.shape.qualified(wf.shadow()), wf.spec.table())
				}
				return nil
			}
			local, err := tableExists(ctx, conn, wf.spec.SchemaName, wf.spec.TableName)
			if err != nil {
				return err
			}
			// Create and mark the shadow in one transaction so a crash never
			// leaves an unmarked pgshard shadow that a retry would reject and
			// cancellation would refuse to clean up.
			var stmts []string
			if local {
				stmts = []string{fmt.Sprintf("CREATE TABLE %s (LIKE %s INCLUDING ALL)", wf.shape.qualified(wf.shadow()), wf.shape.qualified(wf.spec.TableName))}
			} else {
				if ddl == nil {
					if ddl, err = p.shadowDDL(ctx, wf); err != nil {
						return err
					}
				}
				stmts = ddl
			}
			if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
				return err
			}
			defer func() { _, _ = conn.Exec(ctx, "ROLLBACK") }()
			for _, stmt := range stmts {
				if _, err := conn.Exec(ctx, stmt); err != nil {
					return fmt.Errorf("%s: %w", stmt, err)
				}
			}
			if err := markPlacementArtifact(ctx, conn, wf.spec.SchemaName, wf.shadow(), marker); err != nil {
				return err
			}
			_, err = conn.Exec(ctx, "COMMIT")
			return err
		}()
		_ = conn.Close(ctx)
		if err != nil {
			return fmt.Errorf("shadow table on %s/%d: %w", wf.st.SourceSet, t, err)
		}
	}
	return nil
}

var nextvalRE = regexp.MustCompile(`nextval\('([^']+)'::regclass\)`)

// identitySequence returns the regclass text of the sequence owned by an
// identity column, or "" if the column is not an identity column.
func identitySequence(ctx context.Context, conn ShardConn, schema, table, column string) (string, error) {
	rows, err := conn.Query(ctx, `SELECT pg_get_serial_sequence($1, $2)`, QuoteIdent(schema)+"."+QuoteIdent(table), column)
	if err != nil {
		return "", err
	}
	seq, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[*string])
	if err != nil || seq == nil {
		return "", err
	}
	return *seq, nil
}

// sequenceInSchema reports whether the sequence at seqRegclass lives in schema.
func sequenceInSchema(ctx context.Context, conn ShardConn, seqRegclass, schema string) (bool, error) {
	rows, err := conn.Query(ctx, `SELECT n.nspname = $2 FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = $1::regclass`, seqRegclass, schema)
	if err != nil {
		return false, err
	}
	return pgx.CollectExactlyOneRow(rows, pgx.RowTo[bool])
}

// sequenceOptionsClause renders the non-default options of the sequence at
// seqRegclass so a recreated sequence keeps the same increment, bounds, cache
// and cycle behaviour instead of silently falling back to the defaults.
func sequenceOptionsClause(ctx context.Context, conn ShardConn, seqRegclass string) (string, error) {
	var inc, minV, maxV, start, cache int64
	var cycle bool
	rows, err := conn.Query(ctx, `SELECT seqincrement, seqmin, seqmax, seqstart, seqcache, seqcycle
		FROM pg_sequence WHERE seqrelid = $1::regclass`, seqRegclass)
	if err != nil {
		return "", err
	}
	found, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (struct{}, error) {
		return struct{}{}, r.Scan(&inc, &minV, &maxV, &start, &cache, &cycle)
	})
	if err != nil || len(found) == 0 {
		return "", err
	}
	clause := fmt.Sprintf("INCREMENT BY %d MINVALUE %d MAXVALUE %d START WITH %d CACHE %d", inc, minV, maxV, start, cache)
	if cycle {
		clause += " CYCLE"
	} else {
		clause += " NO CYCLE"
	}
	return clause, nil
}

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
	type ownedSeq struct{ seq, col string }
	var owned []ownedSeq
	for _, c := range cols {
		d := QuoteIdent(c.name) + " " + c.typ
		if c.collate != "" {
			d += " COLLATE " + c.collate
		}
		if c.notNull {
			d += " NOT NULL"
		}
		if c.generated != "" {
			// A generated column carries its expression in pg_attrdef like a
			// default; render it as the generated column it is, never as a
			// DEFAULT the copy would then try to insert into.
			d += " GENERATED ALWAYS AS (" + c.def + ")"
			if c.generated == "v" {
				d += " VIRTUAL"
			} else {
				d += " STORED"
			}
			defs = append(defs, d)
			continue
		}
		switch c.identity {
		case "a", "d":
			kind := "ALWAYS"
			if c.identity == "d" {
				kind = "BY DEFAULT"
			}
			// Reproduce the identity sequence's own options (increment, bounds,
			// cache, cycle) so the moved table keeps the same generator.
			seq, oerr := identitySequence(ctx, conn, wf.spec.SchemaName, wf.spec.TableName, c.name)
			if oerr != nil {
				return nil, oerr
			}
			d += " GENERATED " + kind + " AS IDENTITY"
			if seq != "" {
				opts, oerr := sequenceOptionsClause(ctx, conn, seq)
				if oerr != nil {
					return nil, oerr
				}
				if opts != "" {
					d += " (" + opts + ")"
				}
			}
		default:
			if c.def != "" {
				for _, m := range nextvalRE.FindAllStringSubmatch(c.def, -1) {
					opts, oerr := sequenceOptionsClause(ctx, conn, m[1])
					if oerr != nil {
						return nil, oerr
					}
					stmt := "CREATE SEQUENCE IF NOT EXISTS " + m[1]
					if opts != "" {
						stmt += " " + opts
					}
					out = append(out, stmt)
					owned = append(owned, ownedSeq{m[1], c.name})
				}
				d += " DEFAULT " + c.def
			}
		}
		defs = append(defs, d)
	}
	out = append(out, fmt.Sprintf("CREATE TABLE %s (%s)", wf.shape.qualified(wf.shadow()), strings.Join(defs, ", ")))
	// Tie each freshly created serial sequence to its column so it is dropped
	// with the table and, crucially, is discoverable by pg_get_serial_sequence
	// when the sequence is advanced past the copied rows at swap time.
	// PostgreSQL requires an owned sequence to share the table's schema, so a
	// column defaulting to a sequence in another schema is left unowned rather
	// than failing the whole shadow build.
	for _, o := range owned {
		sameSchema, oerr := sequenceInSchema(ctx, conn, o.seq, wf.spec.SchemaName)
		if oerr != nil {
			return nil, oerr
		}
		if !sameSchema {
			continue
		}
		out = append(out, fmt.Sprintf("ALTER SEQUENCE %s OWNED BY %s.%s", o.seq, wf.shape.qualified(wf.shadow()), QuoteIdent(o.col)))
	}
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
	cols = copiedColumns(cols)
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
			sql += fmt.Sprintf(" WHERE (%s) > (%s)", strings.Join(pkCols, ", "), strings.Join(keysetBounds(last, pkIdx, wf.st.Columns, typeOf), ", "))
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

// keysetBounds renders the typed literals of the resume bound. The values
// arrive as text; quoteLiteralE keeps a backslash a backslash under
// standard_conforming_strings=on, or the bound lands past rows and skips
// them.
func keysetBounds(last *Tuple, pkIdx []int, columns []string, typeOf map[string]string) []string {
	var bounds []string
	for _, i := range pkIdx {
		bounds = append(bounds, quoteLiteralE(last.Values[i])+"::"+typeOf[columns[i]])
	}
	return bounds
}

// copiedColumns drops generated columns: they are computed on the target
// from the copied columns, are never inserted, and pgoutput omits them from
// the relation message by default.
func copiedColumns(cols []tableColumn) []tableColumn {
	out := make([]tableColumn, 0, len(cols))
	for _, c := range cols {
		if c.generated == "" {
			out = append(out, c)
		}
	}
	return out
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
	if _, err := p.Pool.Exec(ctx, `UPDATE pgshard.table_status SET migrating = true, workflow_id = $4::uuid, updated_at = now()
		WHERE database = $1 AND schema_name = $2 AND table_name = $3 AND NOT migrating`, wf.spec.Database, wf.spec.SchemaName, wf.spec.TableName, wf.id); err != nil {
		return err
	}
	return nil
}

// fenceShards arms the database fence on every shard. The catalog flag only
// asks routers to hold writes; a router that has not read it yet is refused
// by the shard itself. It goes on after the drain and immediately before
// the first swap, so writers that were already in flight finish normally
// instead of being broken mid-transaction, and it stays on until the new
// placement is published.
func (p *Placer) fenceShards(ctx context.Context, wf *placementWorkflow) error {
	return p.eachShard(ctx, wf, func(ctx context.Context, conn ShardConn) error {
		return fenceTables(ctx, conn, wf.spec.SchemaName, wf.shape.qualified(wf.spec.TableName), wf.shape.qualified(wf.shadow()))
	})
}

// releaseShardFence drops the database fence from every shard. It is
// best-effort per shard so one unreachable shard cannot strand the rest.
func (p *Placer) releaseShardFence(ctx context.Context, wf *placementWorkflow) error {
	return p.eachShard(ctx, wf, func(ctx context.Context, conn ShardConn) error {
		return unfenceTables(ctx, conn, wf.shape.qualified(wf.spec.TableName), wf.shape.qualified(wf.shadow()), wf.shape.qualified(wf.old()))
	})
}

// eachShard runs fn on every shard the workflow touches.
func (p *Placer) eachShard(ctx context.Context, wf *placementWorkflow, fn func(context.Context, ShardConn) error) error {
	for _, t := range wf.rt.ids {
		conn, err := p.Shards.DialDatabase(ctx, wf.st.SourceSet, t, wf.spec.Database)
		if err != nil {
			return err
		}
		err = fn(ctx, conn)
		_ = conn.Close(ctx)
		if err != nil {
			return fmt.Errorf("shard %s/%d: %w", wf.st.SourceSet, t, err)
		}
	}
	return nil
}

func (p *Placer) releaseFence(ctx context.Context, wf *placementWorkflow) error {
	if err := p.releaseShardFence(ctx, wf); err != nil {
		return err
	}
	_, err := p.Pool.Exec(ctx, `UPDATE pgshard.table_status SET migrating = false, updated_at = now()
		WHERE database = $1 AND schema_name = $2 AND table_name = $3 AND migrating`, wf.spec.Database, wf.spec.SchemaName, wf.spec.TableName)
	return err
}

func (p *Placer) unlock(ctx context.Context, wf *placementWorkflow) error {
	_, err := p.Pool.Exec(ctx, `DELETE FROM pgshard.workflow_locks WHERE workflow_id = $1::uuid`, wf.id)
	return err
}

// verifyPlacement compares what the sources hold with what the shadow
// tables received, before the swap makes the shadows live. Every reference
// holder must match the source exactly; under any other placement the
// shadow slices must add up to the source. It runs under the fence after
// the drain, so both sides are still. Shadows live on the new placement's
// holders. A holder whose shadow is missing fails the verification closed
// unless the durable swap marker (placementState.Swapped, persisted before
// the first rename on that shard) covers it: only that marker proves the
// swap began after a verification that already passed, so a shadow lost to
// anything else can never publish a stale placement.
func (p *Placer) verifyPlacement(ctx context.Context, wf *placementWorkflow) error {
	targets := map[int32]rowDigest{}
	for _, t := range wf.rt.Holders() {
		conn, err := p.Shards.DialDatabase(ctx, wf.st.SourceSet, t, wf.spec.Database)
		if err != nil {
			return err
		}
		hasShadow, err := tableExists(ctx, conn, wf.spec.SchemaName, wf.shadow())
		if err == nil && !hasShadow {
			_ = conn.Close(ctx)
			if slices.Contains(wf.st.Swapped, t) {
				return nil
			}
			return fatal("shadow of %s missing on shard %d before the swap began", wf.spec.table(), t)
		}
		var d rowDigest
		if err == nil {
			d, err = digest(ctx, conn, wf.spec.SchemaName, wf.shadow(), "")
		}
		_ = conn.Close(ctx)
		if err != nil {
			return err
		}
		targets[t] = d
	}
	var src rowDigest
	for _, s := range wf.from.Sources() {
		conn, err := p.Shards.DialDatabase(ctx, wf.st.SourceSet, s, wf.spec.Database)
		if err != nil {
			return err
		}
		d, err := digest(ctx, conn, wf.spec.SchemaName, wf.spec.TableName, "")
		_ = conn.Close(ctx)
		if err != nil {
			return err
		}
		src = src.add(d)
	}
	if mismatches := placementMismatches(wf.spec.To.Placement, src, targets); len(mismatches) > 0 {
		return fatal("placement verification of %s failed: %s", wf.spec.table(), strings.Join(mismatches, "; "))
	}
	return nil
}

func placementMismatches(placement string, src rowDigest, targets map[int32]rowDigest) []string {
	var out []string
	if placement == "reference" {
		for _, t := range sortedInt32Keys(targets) {
			if targets[t] != src {
				out = append(out, fmt.Sprintf("shard %d holds %d rows hash %d, source %d rows hash %d", t, targets[t].Rows, targets[t].Hash, src.Rows, src.Hash))
			}
		}
		return out
	}
	var sum rowDigest
	for _, t := range sortedInt32Keys(targets) {
		sum = sum.add(targets[t])
	}
	if sum != src {
		out = append(out, fmt.Sprintf("targets hold %d rows hash %d, sources %d rows hash %d", sum.Rows, sum.Hash, src.Rows, src.Hash))
	}
	return out
}

// swapAll renames the tables on every serving shard in one transaction per
// shard: the shadow becomes the table where the new placement holds it,
// the previous table becomes <table>__pgshard_old wherever it existed.
// Sequences owned by columns of the old table move to the new one so the
// old table's drop does not take them along.
func (p *Placer) swapAll(ctx context.Context, wf *placementWorkflow) error {
	for _, t := range wf.rt.ids {
		slot := ""
		if slices.Contains(wf.from.Sources(), t) {
			slot = wf.slotName(t)
		}
		resumed := slices.Contains(wf.st.Swapped, t)
		if !resumed {
			wf.st.Swapped = append(wf.st.Swapped, t)
			if err := p.save(ctx, wf, fmt.Sprintf("swapping on shard %d", t)); err != nil {
				return err
			}
		}
		for attempt := 0; ; attempt++ {
			conn, err := p.Shards.DialDatabase(ctx, wf.st.SourceSet, t, wf.spec.Database)
			if err != nil {
				return err
			}
			err = p.swapOn(ctx, wf, conn, slices.Contains(wf.rt.Holders(), t), slot, resumed)
			_ = conn.Close(ctx)
			if errors.Is(err, errSwapLagged) {
				if attempt >= 5 {
					return fmt.Errorf("swap on %s/%d: %w after %d catch-up passes", wf.st.SourceSet, t, err, attempt)
				}
				if _, _, cerr := p.catchUp(ctx, wf, true); cerr != nil {
					return cerr
				}
				continue
			}
			if err != nil {
				return fmt.Errorf("swap on %s/%d: %w", wf.st.SourceSet, t, err)
			}
			break
		}
	}
	return nil
}

// errSwapLagged: the slot still held changes for the table when the swap
// transaction took its lock; the caller applies them and retries.
var errSwapLagged = errors.New("replication slot still holds changes under the swap lock")

func (p *Placer) swapOn(ctx context.Context, wf *placementWorkflow, conn ShardConn, holder bool, slot string, resumed bool) error {
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
		// Only the durable marker proves the live table is the renamed
		// shadow of an interrupted swap; without it the table is the old
		// data and the shadow is lost, so the swap must not proceed.
		if !resumed {
			return fatal("shadow of %s missing before the swap began", table)
		}
		return nil
	}
	if !holder && !hasTable {
		return nil
	}
	if hasTable {
		if _, err := conn.Exec(ctx, "LOCK TABLE "+table+" IN ACCESS EXCLUSIVE MODE"); err != nil {
			return err
		}
		// The renamed table keeps its OID, so the publication would keep
		// streaming it: under the lock (no writer can slip in any more)
		// the slot must hold nothing for this table before the rename.
		if slot != "" {
			rows, err := conn.Query(ctx, `SELECT count(*) FROM pg_logical_slot_peek_binary_changes($1, NULL, NULL, 'proto_version', '1', 'publication_names', $2)`,
				slot, wf.publicationName())
			if err != nil {
				return err
			}
			pending, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[int64])
			if err != nil {
				return err
			}
			if pending > 0 {
				return fmt.Errorf("%w: %d pending message(s)", errSwapLagged, pending)
			}
		}
		if hasOld {
			// Lock before the marker check so a concurrent rename cannot swap
			// an unmarked table under the name between the check and the drop.
			if _, err := conn.Exec(ctx, "LOCK TABLE "+old+" IN ACCESS EXCLUSIVE MODE"); err != nil {
				return err
			}
			ours, err := isPlacementArtifact(ctx, conn, wf.spec.SchemaName, wf.old(), wf.placementMarker())
			if err != nil {
				return err
			}
			if !ours {
				return fatal("a table named %s already exists and is not a pgshard retired table; rename it before sharding %s", old, wf.spec.table())
			}
			if _, err := conn.Exec(ctx, "DROP TABLE "+old); err != nil {
				return err
			}
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s RENAME TO %s", table, QuoteIdent(wf.old()))); err != nil {
			return err
		}
		// Tag the retired table so a later drop-old pass (or an interrupted
		// retry) only ever drops pgshard's own renamed original.
		if err := markPlacementArtifact(ctx, conn, wf.spec.SchemaName, wf.old(), wf.placementMarker()); err != nil {
			return err
		}
	}
	if holder {
		if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s RENAME TO %s", shadow, QuoteIdent(wf.spec.TableName))); err != nil {
			return err
		}
		// Clear the pgshard marker and restore the original table's comment,
		// which LIKE INCLUDING ALL copied onto the shadow before marking.
		if err := restoreComment(ctx, conn, wf.spec.SchemaName, wf.spec.TableName, wf.st.TableComment); err != nil {
			return err
		}
		if hasTable {
			if err := moveOwnedSequences(ctx, conn, wf.spec.SchemaName, wf.old(), wf.spec.TableName); err != nil {
				return err
			}
		}
		// The shadow's serial/identity sequences start fresh, so advance
		// each to the greatest value already copied onto this shard; without
		// it the next implicit insert reuses a copied identifier.
		if err := advanceSequences(ctx, conn, wf.spec.SchemaName, wf.spec.TableName); err != nil {
			return err
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

// advanceSequences sets every serial or identity sequence backing a column of
// the table to the greatest value present, so a later implicit insert does not
// reuse an identifier that was copied in. GREATEST with the current last_value
// never moves a shared sequence backwards.
func advanceSequences(ctx context.Context, conn ShardConn, schema, table string) error {
	qual := QuoteIdent(schema) + "." + QuoteIdent(table)
	rows, err := conn.Query(ctx, `SELECT a.attname, pg_get_serial_sequence($1, a.attname)
		FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $2 AND c.relname = $3 AND a.attnum > 0 AND NOT a.attisdropped
		AND pg_get_serial_sequence($1, a.attname) IS NOT NULL`, qual, schema, table)
	if err != nil {
		return err
	}
	cols, err := pgx.CollectRows(rows, pgx.RowToStructByPos[struct{ Col, Seq string }])
	if err != nil {
		return err
	}
	for _, c := range cols {
		// Ascending sequences advance to the max copied value, descending to
		// the min; GREATEST/LEAST with last_value never moves a shared
		// sequence the wrong way. An empty shard advances nothing (the WHERE
		// yields no row), so a fresh sequence keeps its start value.
		if _, err := conn.Exec(ctx, fmt.Sprintf(
			`SELECT setval(%[1]s::regclass,
				CASE WHEN sq.seqincrement > 0
					THEN GREATEST(x.v, (SELECT last_value FROM %[4]s))
					ELSE LEAST(x.v, (SELECT last_value FROM %[4]s)) END, true)
			FROM pg_sequence sq,
				LATERAL (SELECT CASE WHEN sq.seqincrement > 0 THEN max(%[2]s) ELSE min(%[2]s) END AS v FROM %[3]s) x
			WHERE sq.seqrelid = %[1]s::regclass AND x.v IS NOT NULL`,
			QuoteLiteral(&c.Seq), QuoteIdent(c.Col), qual, c.Seq)); err != nil {
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
			if err := dropArtifactTable(ctx, conn, wf.spec.SchemaName, wf.old(), wf.placementMarker()); err != nil {
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
