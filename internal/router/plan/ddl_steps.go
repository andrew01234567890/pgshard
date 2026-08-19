package plan

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"

	"github.com/andrew01234567890/pgshard/internal/pgparser"
	"github.com/andrew01234567890/pgshard/internal/pgparser/pg18/pgquerypb"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// Check kinds the applier evaluates before a step (see catalog.MigrationCheck).
const (
	CheckConstraint      = "constraint"
	CheckConstraintValid = "constraint_valid"
	CheckNotNull         = "notnull"
	CheckNotNullValid    = "notnull_valid"
	CheckIndexValid      = "index_valid"
	CheckDetachPending   = "detach_pending"
	CheckDetached        = "detached"
)

// deparse clones the statement being planned, lets edit change it and
// returns its SQL.
func (w *walker) deparse(edit func(*pgquerypb.Node)) (string, error) {
	clone := proto.Clone(w.raw).(*pgquerypb.RawStmt)
	edit(clone.GetStmt())
	sql, err := pgparser.Deparse(&pgquerypb.ParseResult{Version: w.parseVersion(), Stmts: []*pgquerypb.RawStmt{clone}})
	if err != nil {
		return "", pgwire.Errorf(pgwire.CodeInternalError, "rewriting the statement: %v", err)
	}
	return sql, nil
}

// alterSteps turns the constraint-, index- and partition-taking forms of
// ALTER TABLE into a multistep plan; nil means the statement runs as is.
func (w *walker) alterSteps(a *pgquerypb.AlterTableStmt, r *rel) ([]Step, error) {
	if len(a.GetCmds()) > 1 {
		for _, n := range a.GetCmds() {
			if multistepCmd(n.GetAlterTableCmd()) {
				return nil, notYet("ALTER TABLE with several actions of which one is applied online in steps is not available yet",
					"run the constraint, NOT NULL or DETACH PARTITION action as its own ALTER TABLE statement")
			}
		}
		return nil, nil
	}
	var steps []Step
	for _, n := range a.GetCmds() {
		c := n.GetAlterTableCmd()
		var s []Step
		var err error
		switch c.GetSubtype() {
		case pgquerypb.AlterTableType_AT_AddConstraint:
			s, err = w.constraintSteps(a, c.GetDef().GetConstraint(), r)
		case pgquerypb.AlterTableType_AT_SetNotNull:
			s = notNullSteps(a.GetRelation(), c.GetName())
		case pgquerypb.AlterTableType_AT_DetachPartition:
			if !c.GetDef().GetPartitionCmd().GetConcurrent() {
				s = detachSteps(a.GetRelation(), c.GetDef().GetPartitionCmd().GetName())
			}
		}
		if err != nil {
			return nil, err
		}
		steps = append(steps, s...)
	}
	return steps, nil
}

func multistepCmd(c *pgquerypb.AlterTableCmd) bool {
	switch c.GetSubtype() {
	case pgquerypb.AlterTableType_AT_SetNotNull:
		return true
	case pgquerypb.AlterTableType_AT_DetachPartition:
		return !c.GetDef().GetPartitionCmd().GetConcurrent()
	case pgquerypb.AlterTableType_AT_AddConstraint:
		cs := c.GetDef().GetConstraint()
		switch cs.GetContype() {
		case pgquerypb.ConstrType_CONSTR_CHECK, pgquerypb.ConstrType_CONSTR_FOREIGN:
			return !cs.GetSkipValidation()
		case pgquerypb.ConstrType_CONSTR_PRIMARY, pgquerypb.ConstrType_CONSTR_UNIQUE:
			return cs.GetIndexname() == "" && len(cs.GetKeys()) > 0
		}
	}
	return false
}

func (w *walker) constraintSteps(a *pgquerypb.AlterTableStmt, cs *pgquerypb.Constraint, r *rel) ([]Step, error) {
	rv := a.GetRelation()
	switch cs.GetContype() {
	case pgquerypb.ConstrType_CONSTR_CHECK:
		if cs.GetSkipValidation() {
			return nil, nil
		}
		return w.validatedSteps(rv, cs, "check")
	case pgquerypb.ConstrType_CONSTR_FOREIGN:
		if err := w.checkColocated(cs, r); err != nil {
			return nil, err
		}
		if cs.GetSkipValidation() {
			return nil, nil
		}
		return w.validatedSteps(rv, cs, "fkey")
	case pgquerypb.ConstrType_CONSTR_PRIMARY, pgquerypb.ConstrType_CONSTR_UNIQUE:
		if cs.GetIndexname() != "" || len(cs.GetKeys()) == 0 || cs.GetWithoutOverlaps() || len(cs.GetOptions()) > 0 || cs.GetIndexspace() != "" {
			return nil, nil
		}
		return uniqueSteps(rv, cs), nil
	}
	return nil, nil
}

// validatedSteps adds a CHECK or FOREIGN KEY constraint NOT VALID
// (short AccessExclusive) and validates it (ShareUpdateExclusive).
func (w *walker) validatedSteps(rv *pgquerypb.RangeVar, cs *pgquerypb.Constraint, suffix string) ([]Step, error) {
	name := cs.GetConname()
	if name == "" {
		cols := stringList(cs.GetFkAttrs())
		if cs.GetContype() == pgquerypb.ConstrType_CONSTR_CHECK {
			cols = columnRefs(cs.GetRawExpr())
		}
		name = autoName(rv.GetRelname(), cols, suffix)
	}
	add, err := w.deparse(func(n *pgquerypb.Node) {
		c := n.GetAlterTableStmt().GetCmds()[0].GetAlterTableCmd().GetDef().GetConstraint()
		c.Conname, c.SkipValidation = name, true
	})
	if err != nil {
		return nil, err
	}
	table := tableName(rv)
	check := Check{Schema: rv.GetSchemaname(), Table: rv.GetRelname(), Name: name}
	return []Step{
		{SQL: add, Skip: withKind(check, CheckConstraint)},
		{SQL: "ALTER TABLE " + table + " VALIDATE CONSTRAINT " + quoteIdent(name), Skip: withKind(check, CheckConstraintValid),
			OnFail: "ALTER TABLE " + table + " DROP CONSTRAINT IF EXISTS " + quoteIdent(name)},
	}, nil
}

// notNullSteps sets a column NOT NULL through a NOT VALID not-null
// constraint that is then validated (PostgreSQL 18+).
func notNullSteps(rv *pgquerypb.RangeVar, col string) []Step {
	name := autoName(rv.GetRelname(), []string{col}, "not_null")
	table := tableName(rv)
	check := Check{Schema: rv.GetSchemaname(), Table: rv.GetRelname(), Name: col}
	return []Step{
		{SQL: "ALTER TABLE " + table + " ADD CONSTRAINT " + quoteIdent(name) + " NOT NULL " + quoteIdent(col) + " NOT VALID", Skip: withKind(check, CheckNotNull)},
		{SQL: "ALTER TABLE " + table + " VALIDATE CONSTRAINT " + quoteIdent(name), Skip: withKind(check, CheckNotNullValid),
			OnFail: "ALTER TABLE " + table + " DROP CONSTRAINT IF EXISTS " + quoteIdent(name)},
	}
}

// uniqueSteps builds the unique index concurrently and attaches it as the
// PRIMARY KEY or UNIQUE constraint; a primary key first makes its columns
// NOT NULL in steps so the attach does not scan the table.
func uniqueSteps(rv *pgquerypb.RangeVar, cs *pgquerypb.Constraint) []Step {
	cols := stringList(cs.GetKeys())
	primary := cs.GetContype() == pgquerypb.ConstrType_CONSTR_PRIMARY
	name := cs.GetConname()
	if name == "" {
		if primary {
			name = autoName(rv.GetRelname(), nil, "pkey")
		} else {
			name = autoName(rv.GetRelname(), cols, "key")
		}
	}
	var steps []Step
	if primary {
		for _, col := range cols {
			steps = append(steps, notNullSteps(rv, col)...)
		}
	}
	table := tableName(rv)
	create := "CREATE UNIQUE INDEX CONCURRENTLY " + quoteIdent(name) + " ON " + table + " (" + quoteList(cols) + ")"
	if inc := stringList(cs.GetIncluding()); len(inc) > 0 {
		create += " INCLUDE (" + quoteList(inc) + ")"
	}
	if cs.GetNullsNotDistinct() {
		create += " NULLS NOT DISTINCT"
	}
	kind := "UNIQUE"
	if primary {
		kind = "PRIMARY KEY"
	}
	attach := "ALTER TABLE " + table + " ADD CONSTRAINT " + quoteIdent(name) + " " + kind + " USING INDEX " + quoteIdent(name)
	if cs.GetDeferrable() {
		attach += " DEFERRABLE"
		if cs.GetInitdeferred() {
			attach += " INITIALLY DEFERRED"
		}
	}
	check := Check{Schema: rv.GetSchemaname(), Table: rv.GetRelname(), Name: name}
	return append(steps,
		Step{SQL: create, Concurrent: true, Index: name, Skip: withKind(check, CheckIndexValid)},
		Step{SQL: attach, Skip: withKind(check, CheckConstraint)})
}

// detachSteps detaches a partition concurrently and finalizes a detach a
// crash left pending.
func detachSteps(rv, part *pgquerypb.RangeVar) []Step {
	table, p := tableName(rv), tableName(part)
	check := Check{Schema: rv.GetSchemaname(), Table: rv.GetRelname(), Name: part.GetRelname()}
	return []Step{
		{SQL: "ALTER TABLE " + table + " DETACH PARTITION " + p + " CONCURRENTLY", Concurrent: true, Skip: withKind(check, CheckDetachPending)},
		{SQL: "ALTER TABLE " + table + " DETACH PARTITION " + p + " FINALIZE", Skip: withKind(check, CheckDetached)},
	}
}

// checkColocated refuses a foreign key whose referenced rows may live on
// another shard than the referencing row.
func (w *walker) checkColocated(cs *pgquerypb.Constraint, r *rel) error {
	ref, err := w.lookup(cs.GetPktable())
	if err != nil {
		return err
	}
	fromKind, toKind := placeUnsharded, placeUnsharded
	if r != nil {
		fromKind = r.kind
	}
	if ref != nil {
		toKind = ref.kind
	}
	refuse := func(why string) error {
		return notYet("cross-shard foreign key: "+why, "reference a reference table, or a sharded table through the shard key with the same distribution")
	}
	switch fromKind {
	case placeUnsharded:
		if toKind == placeSharded {
			return refuse("an unsharded table cannot reference sharded table \"" + ref.name + "\"")
		}
	case placeReference:
		if toKind != placeReference {
			return refuse("reference table \"" + r.name + "\" can only reference another reference table")
		}
	case placeSharded:
		switch toKind {
		case placeReference:
		case placeSharded:
			fk, pk := stringList(cs.GetFkAttrs()), stringList(cs.GetPkAttrs())
			at := -1
			for i, c := range fk {
				if c == r.shardKey {
					at = i
				}
			}
			if at < 0 || at >= len(pk) || pk[at] != ref.shardKey {
				return refuse("the foreign key from sharded table \"" + r.name + "\" must map its shard key \"" + r.shardKey + "\" onto the shard key \"" + ref.shardKey + "\" of \"" + ref.name + "\"")
			}
		default:
			return refuse("sharded table \"" + r.name + "\" cannot reference unsharded table \"" + ref.name + "\"")
		}
	}
	return nil
}

func withKind(c Check, kind string) Check {
	c.Kind = kind
	return c
}

func tableName(rv *pgquerypb.RangeVar) string {
	if rv.GetSchemaname() != "" {
		return pgx.Identifier{rv.GetSchemaname(), rv.GetRelname()}.Sanitize()
	}
	return quoteIdent(rv.GetRelname())
}

func quoteIdent(s string) string { return pgx.Identifier{s}.Sanitize() }

func quoteList(cols []string) string {
	q := make([]string, len(cols))
	for i, c := range cols {
		q[i] = quoteIdent(c)
	}
	return strings.Join(q, ", ")
}

// columnRefs lists the column names an expression mentions, in order.
func columnRefs(n *pgquerypb.Node) []string {
	var out []string
	visit(n, func(x *pgquerypb.Node) bool {
		if cr := x.GetColumnRef(); cr != nil {
			fields := stringList(cr.GetFields())
			if len(fields) > 0 && !contains(out, fields[len(fields)-1]) {
				out = append(out, fields[len(fields)-1])
			}
		}
		return true
	})
	return out
}

// autoName is PostgreSQL's <table>_<cols>_<suffix> shape, deterministic
// and at most 63 bytes: an over-long name keeps a prefix and an 8-hex hash
// of the full name.
func autoName(table string, cols []string, suffix string) string {
	base := table
	if len(cols) > 0 {
		base += "_" + strings.Join(cols, "_")
	}
	full := base + "_" + suffix
	if len(full) <= 63 {
		return full
	}
	sum := sha256.Sum256([]byte(full))
	h := hex.EncodeToString(sum[:4])
	keep := 63 - len(suffix) - len(h) - 2
	prefix := base
	for len(prefix) > keep {
		_, size := lastRune(prefix)
		prefix = prefix[:len(prefix)-size]
	}
	return prefix + "_" + h + "_" + suffix
}

func lastRune(s string) (rune, int) {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i]&0xC0 != 0x80 {
			r := []rune(s[i:])
			return r[0], len(s) - i
		}
	}
	return 0, 1
}
