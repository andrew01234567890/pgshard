package plan

import (
	"strings"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/pgparser"
	"github.com/andrew01234567890/pgshard/internal/pgparser/pg18/pgquerypb"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"google.golang.org/protobuf/proto"
)

// SequenceFill is how an INSERT into a sharded table gets the values of its
// registered sequence columns: the statement is rewritten so every missing,
// DEFAULT or nextval() value becomes a bind parameter beyond the client's
// own, and the executor binds values it allocated from the catalog.
type SequenceFill struct {
	// SQL is the rewritten statement text.
	SQL string
	// Base is the number of parameters the client's text uses ($1..$Base).
	Base int
	// Names lists the global sequence of each injected parameter, in order:
	// parameter $(Base+1+i) takes a value of Names[i].
	Names []string
}

// SequenceName is the pgshard.sequences row of a registered table column.
func SequenceName(database, schema, table, column string) string {
	return database + "." + schema + "." + table + "." + column
}

// immutableFuncs are the built-ins a write replicated to every shard of a
// reference table may call: given the same arguments each one returns the
// same answer on every shard, in any session, at any time.
//
// This is an allow list on purpose. PostgreSQL's volatility is a catalog
// property and anyone can define a VOLATILE function, so a deny list of
// known-bad names cannot be complete: uuid_generate_v4() from uuid-ossp,
// or any user-defined function, would simply not be on it and would commit
// a different value on every shard. Anything not proven immutable here is
// refused, and the client is told to compute the value itself.
var immutableFuncs = map[string]bool{
	// text
	"lower": true, "upper": true, "initcap": true, "char_length": true,
	"character_length": true, "octet_length": true, "bit_length": true,
	"trim": true, "btrim": true, "ltrim": true, "rtrim": true, "lpad": true, "rpad": true,
	"substr": true, "substring": true, "left": true, "right": true, "reverse": true,
	"replace": true, "translate": true, "repeat": true, "split_part": true, "strpos": true, "position": true, "starts_with": true,
	"md5": true, "sha224": true, "sha256": true, "sha384": true, "sha512": true,
	"encode": true, "decode": true, "quote_ident": true, "ascii": true, "chr": true,
	// numeric
	"abs": true, "ceil": true, "ceiling": true, "floor": true, "round": true, "trunc": true,
	"sign": true, "mod": true, "div": true, "power": true, "sqrt": true, "cbrt": true,
	"exp": true, "ln": true, "log": true, "log10": true, "width_bucket": true,
	// json, arrays and misc structural helpers
	"array_length": true, "array_lower": true, "array_upper": true, "cardinality": true,
	"array_position": true, "array_remove": true, "array_replace": true, "string_to_array": true, "unnest": true,
	"jsonb_extract_path": true, "jsonb_extract_path_text": true, "jsonb_array_length": true,
	// deterministic date arithmetic on values the statement supplies
	"make_date": true, "make_time": true,
	"make_interval": true, "make_timestamp": true,
}

// specialDateWords are the inputs PostgreSQL's date and time types resolve
// when they are parsed rather than when they are written: each shard reads
// its own clock, so a reference row built from one differs everywhere.
var specialDateWords = map[string]bool{
	"now": true, "today": true, "tomorrow": true, "yesterday": true, "allballs": true,
}

// dateTimeTypes are the types those words mean something to.
var dateTimeTypes = map[string]bool{
	"date": true, "time": true, "timetz": true, "timestamp": true, "timestamptz": true,
	"interval": true, "abstime": true,
}

// builtinName reports whether a dotted name refers to something in
// pg_catalog: either unqualified, or qualified with pg_catalog itself.
// Anything else belongs to somebody's own schema, whatever it is called.
func builtinName(names []string) bool {
	// Case-sensitive on purpose: "PG_CATALOG" in double quotes is a
	// different schema from pg_catalog, and treating them alike would admit
	// anything somebody chose to put in it.
	return len(names) == 1 || (len(names) > 1 && names[len(names)-2] == "pg_catalog")
}

// unorderedPick names a construct that chooses which rows to use without
// fully determining which ones, "" if there is none. Running the same
// statement on every shard is only safe when every shard picks the same
// rows, and LIMIT without a total order, TABLESAMPLE and DISTINCT ON do not
// promise that - no function is involved, so the volatility check cannot
// see them.
func unorderedPick(node *pgquerypb.Node) string {
	found := ""
	visit(node, func(n *pgquerypb.Node) bool {
		if found != "" {
			return false
		}
		sel := n.GetSelectStmt()
		if sel == nil {
			return true
		}
		switch {
		case sel.GetLimitCount() != nil || sel.GetLimitOffset() != nil:
			found = "LIMIT or OFFSET"
		case len(sel.GetDistinctClause()) > 0:
			found = "DISTINCT ON"
		}
		if found != "" {
			return false
		}
		for _, from := range sel.GetFromClause() {
			if from.GetRangeTableSample() != nil {
				found = "TABLESAMPLE"
				return false
			}
		}
		return true
	})
	return found
}

// nonImmutableCall names the first call under node that is not proven
// immutable, "" if every call is. Callers use it to refuse a statement that
// must evaluate identically on every shard.
func nonImmutableCall(node *pgquerypb.Node) string {
	found := ""
	visit(node, func(n *pgquerypb.Node) bool {
		if found != "" {
			return false
		}
		switch x := n.GetNode().(type) {
		case *pgquerypb.Node_FuncCall:
			names := stringList(x.FuncCall.GetFuncname())
			if len(names) == 0 {
				return true
			}
			name := strings.ToLower(names[len(names)-1])
			if !builtinName(names) || !immutableFuncs[name] {
				found = name
				return false
			}
		case *pgquerypb.Node_SqlvalueFunction:
			// current_date, current_timestamp, current_user and friends:
			// none of them is immutable.
			found = strings.ToLower(strings.TrimPrefix(x.SqlvalueFunction.GetOp().String(), "SVFOP_"))
			return false
		case *pgquerypb.Node_AExpr:
			// An operator is a function with punctuation for a name, and
			// one in somebody's schema can be backed by anything.
			if names := stringList(x.AExpr.GetName()); len(names) > 0 && !builtinName(names) {
				found = "operator " + strings.Join(names, ".")
				return false
			}
		case *pgquerypb.Node_SetToDefault:
			// DEFAULT stands for whatever the column's default expression
			// evaluates to on each shard, which the statement does not say.
			found = "DEFAULT"
			return false
		case *pgquerypb.Node_TypeCast:
			names := stringList(x.TypeCast.GetTypeName().GetNames())
			if len(names) == 0 {
				return true
			}
			typ := strings.ToLower(names[len(names)-1])
			if !builtinName(names) {
				// A user-defined cast runs a function of its author's
				// choosing.
				found = "cast to " + strings.Join(names, ".")
				return false
			}
			// 'now'::timestamptz and friends read the clock when the shard
			// parses them, so each shard gets its own answer.
			if dateTimeTypes[typ] {
				if lit := x.TypeCast.GetArg().GetAConst().GetSval().GetSval(); lit != "" &&
					specialDateWords[strings.ToLower(strings.TrimSpace(lit))] {
					found = "'" + strings.ToLower(strings.TrimSpace(lit)) + "'::" + typ
					return false
				}
			}
		}
		return true
	})
	return found
}

// maxParam is the highest $n the statement uses.
func maxParam(node *pgquerypb.Node) int {
	n := 0
	visit(node, func(x *pgquerypb.Node) bool {
		if p := x.GetParamRef(); p != nil && int(p.GetNumber()) > n {
			n = int(p.GetNumber())
		}
		return true
	})
	return n
}

// fillable reports a DEFAULT keyword or a nextval(...) call: the shapes a
// sequence column value is filled in for.
func fillable(node *pgquerypb.Node) bool {
	switch x := node.GetNode().(type) {
	case *pgquerypb.Node_SetToDefault:
		return true
	case *pgquerypb.Node_FuncCall:
		names := stringList(x.FuncCall.GetFuncname())
		return len(names) > 0 && strings.EqualFold(names[len(names)-1], "nextval")
	}
	return false
}

// rewriteInsert plans the sequence values of an INSERT ... VALUES into r:
// for every registered sequence column that is absent from the column
// list, or given as DEFAULT or nextval(), a parameter is injected. It
// returns nil when the statement supplies every sequence column itself.
// injected maps each column to the parameter number of each VALUES row
// (by row index) that received one.
func (w *walker) rewriteInsert(s *pgquerypb.InsertStmt, r *rel) (*SequenceFill, map[string]map[int]int32, error) {
	sel := s.GetSelectStmt().GetSelectStmt()
	rows := sel.GetValuesLists()
	if len(r.seqCols) == 0 || len(rows) == 0 {
		return nil, nil, nil
	}
	cols := s.GetCols()
	colIdx := map[string]int{}
	for i, c := range cols {
		colIdx[c.GetResTarget().GetName()] = i
	}
	type slot struct {
		col   string
		index int // column index in the (possibly extended) column list
	}
	var slots []slot
	next := len(cols)
	for _, col := range r.seqCols {
		idx, present := colIdx[col]
		if !present {
			slots = append(slots, slot{col, next})
			next++
			continue
		}
		for _, row := range rows {
			items := row.GetList().GetItems()
			if idx < len(items) && fillable(items[idx]) {
				slots = append(slots, slot{col, idx})
				break
			}
		}
	}
	if len(slots) == 0 {
		return nil, nil, nil
	}
	tree := proto.Clone(w.tree).(*pgquerypb.ParseResult)
	ins := tree.GetStmts()[0].GetStmt().GetInsertStmt()
	fill := &SequenceFill{Base: maxParam(w.tree.(*pgquerypb.ParseResult).GetStmts()[0].GetStmt())}
	injected := map[string]map[int]int32{}
	param := int32(fill.Base)
	for _, sl := range slots {
		if sl.index >= len(ins.Cols) {
			ins.Cols = append(ins.Cols, &pgquerypb.Node{Node: &pgquerypb.Node_ResTarget{ResTarget: &pgquerypb.ResTarget{Name: sl.col}}})
		}
	}
	for rowIdx, row := range ins.GetSelectStmt().GetSelectStmt().GetValuesLists() {
		items := row.GetList().GetItems()
		if len(items) != len(cols) {
			return nil, nil, notYet("INSERT into \""+r.name+"\" has a VALUES row with a different number of values than columns", "")
		}
		for _, sl := range slots {
			if sl.index < len(items) && !fillable(items[sl.index]) {
				continue
			}
			param++
			ref := &pgquerypb.Node{Node: &pgquerypb.Node_ParamRef{ParamRef: &pgquerypb.ParamRef{Number: param}}}
			if sl.index < len(items) {
				items[sl.index] = ref
			} else {
				items = append(items, ref)
			}
			fill.Names = append(fill.Names, SequenceName(w.sess.Database, r.schema, r.name, sl.col))
			if injected[sl.col] == nil {
				injected[sl.col] = map[int]int32{}
			}
			injected[sl.col][rowIdx] = param
		}
		row.GetList().Items = items
	}
	if len(fill.Names) == 0 {
		return nil, nil, nil
	}
	out, err := pgparser.Deparse(tree)
	if err != nil {
		return nil, nil, pgwire.Errorf(pgwire.CodeInternalError, "router: deparse of the rewritten INSERT failed: %v", err)
	}
	fill.SQL = out
	return fill, injected, nil
}

// sequenceRefusal refuses the sequence functions whose answer pgshard
// cannot give truthfully.
//
// A global sequence is a catalog counter the router allocates from; the
// per-shard sequence objects the DDL fanned out are not it. currval and
// setval naming a registered global sequence therefore reach an unrelated
// physical counter -- the answer looks ordinary and is about a different
// sequence. lastval has no name at all: it means "the sequence nextval
// last touched in this session", which for a global sequence was the
// router's counter and not the backend's, and the router cannot tell from
// the statement which one is meant.
//
// So they are refused rather than answered wrongly, which is the whole of
// this until a global sequence is a real catalog object with its own
// currval and lastval state. A currval or setval over a sequence that is
// not registered is left alone: that one is an ordinary PostgreSQL
// sequence living on one shard, and it means there what it says.
func (w *walker) sequenceRefusal(root *pgquerypb.Node) error {
	claimed := w.claimedNextval(root)
	var refusal error
	visit(root, func(n *pgquerypb.Node) bool {
		if refusal != nil {
			return false
		}
		fc := n.GetFuncCall()
		if fc == nil {
			return true
		}
		names := stringList(fc.GetFuncname())
		if len(names) == 0 || !builtinName(names) {
			return true
		}
		switch strings.ToLower(names[len(names)-1]) {
		case "lastval":
			refusal = notYet("lastval() is not available through the router: it names whichever sequence nextval last touched, and for a global sequence that was the router's counter rather than this backend's",
				"use currval('<sequence>') on a sequence that is not global, or keep the value the INSERT ... RETURNING gave you")
		case "currval", "setval":
			if seq := w.sequenceArg(fc); seq != "" {
				refusal = notYet(strings.ToLower(names[len(names)-1])+"() on the global sequence "+seq+" is not available through the router: the value it would read or set belongs to one shard's own sequence object, not to the counter the router allocates from",
					"keep the value the INSERT ... RETURNING gave you; global sequence state is not stored on a shard")
			}
		case "nextval":
			// The dangerous one, and the reason this case exists at all:
			// currval only reads the wrong counter, while an unclaimed
			// nextval allocates from it. Two shards would hand out the same
			// numbers from a sequence declared global, so the duplicates
			// arrive as a primary key violation or as two rows that should
			// never have shared an id.
			if claimed[fc] {
				break
			}
			if seq := w.sequenceArg(fc); seq != "" {
				refusal = notYet("nextval() on the global sequence "+seq+" is not available in this statement: the router allocates from the global counter only for `SELECT nextval(...)` on its own and for the sequence columns of an INSERT, and anywhere else the value would come from one shard's own sequence object",
					"select the value first with `SELECT nextval('"+seq+"')` and bind it, or let the INSERT fill the column")
			}
		}
		return refusal == nil
	})
	return refusal
}

// sequenceArg resolves a sequence function's first argument to a registered
// global sequence name, or "" when it names none.
func (w *walker) sequenceArg(fc *pgquerypb.FuncCall) string {
	if len(fc.GetArgs()) == 0 {
		return ""
	}
	return w.registeredSequence(fc.GetArgs()[0])
}

// nextvalName recognises `SELECT nextval('<name>')` over a registered global
// sequence and returns the sequence's catalog name; "" for anything else.
func (w *walker) nextvalName(s *pgquerypb.SelectStmt) string {
	if len(s.GetFromClause()) != 0 || len(s.GetTargetList()) != 1 || s.GetWhereClause() != nil || s.GetWithClause() != nil ||
		s.GetOp() != pgquerypb.SetOperation_SETOP_NONE || len(s.GetValuesLists()) != 0 {
		return ""
	}
	fc := s.GetTargetList()[0].GetResTarget().GetVal().GetFuncCall()
	if fc == nil || len(fc.GetArgs()) != 1 {
		return ""
	}
	names := stringList(fc.GetFuncname())
	if len(names) == 0 || !strings.EqualFold(names[len(names)-1], "nextval") {
		return ""
	}
	return w.registeredSequence(fc.GetArgs()[0])
}

// registeredSequence resolves a sequence-naming argument -- a literal, or a
// cast of one -- to a registered global sequence's catalog name.
func (w *walker) registeredSequence(arg *pgquerypb.Node) string {
	if tc := arg.GetTypeCast(); tc != nil {
		arg = tc.GetArg()
	}
	lit := arg.GetAConst().GetSval().GetSval()
	if lit == "" || w.sess.Snapshot == nil {
		return ""
	}
	if w.sess.Snapshot.Sequences[lit] {
		return lit
	}
	parts := strings.Split(lit, ".")
	for i := range parts {
		parts[i] = strings.Trim(parts[i], `"`)
	}
	var schemas []string
	var table, col string
	switch len(parts) {
	case 2:
		table, col = parts[0], parts[1]
		schemas = w.sess.SearchPath
		if len(schemas) == 0 {
			schemas = []string{"public"}
		}
	case 3:
		schemas, table, col = []string{parts[0]}, parts[1], parts[2]
	default:
		return ""
	}
	for _, schema := range schemas {
		pl, ok := w.sess.Snapshot.Tables[snapshot.TableKey{Database: w.sess.Database, SchemaName: schema, TableName: table}]
		if !ok {
			continue
		}
		if contains(pl.SequenceColumns, col) {
			return SequenceName(w.sess.Database, schema, table, col)
		}
		return ""
	}
	return ""
}

// claimedNextval collects the nextval() calls the router answers itself, so
// the refusal above lets exactly those through: the whole statement being
// `SELECT nextval(...)`, and a value directly in an INSERT's VALUES, which
// is where the sequence fill substitutes an allocated parameter.
//
// The VALUES case is claimed by position rather than by column, because the
// columns a fill claims are known only once the relation is resolved, which
// happens after this runs. A nextval in a VALUES position for a column that
// is not a registered sequence column is therefore still forwarded to a
// shard.
func (w *walker) claimedNextval(root *pgquerypb.Node) map[*pgquerypb.FuncCall]bool {
	claimed := map[*pgquerypb.FuncCall]bool{}
	switch n := root.GetNode().(type) {
	case *pgquerypb.Node_SelectStmt:
		// Exactly the shape the planner answers, not merely one that looks
		// like it: `SELECT nextval('g') FROM t` is a scatter over t and the
		// router does not allocate for it.
		if w.nextvalName(n.SelectStmt) != "" {
			claimed[n.SelectStmt.GetTargetList()[0].GetResTarget().GetVal().GetFuncCall()] = true
		}
	case *pgquerypb.Node_InsertStmt:
		for _, row := range n.InsertStmt.GetSelectStmt().GetSelectStmt().GetValuesLists() {
			for _, item := range row.GetList().GetItems() {
				if fc := item.GetFuncCall(); fc != nil {
					claimed[fc] = true
				}
			}
		}
	}
	return claimed
}

// registeredSequenceObject reports the registered global sequence whose
// serial a sequence OBJECT backs, or "" for one that backs none.
//
// The two are named differently and that is the whole difficulty.
// pgshard registers a global sequence as database.schema.table.column;
// PostgreSQL names the physical sequence a serial creates
// <table>_<column>_seq. So ALTER SEQUENCE orders_id_seq names a per-shard
// object, and fanning it out resets every shard's own counter while the
// counter the router actually allocates from carries on untouched. The
// statement reports success and changes nothing that matters.
//
// Derived rather than looked up, because the router has the registered
// columns and not the shards' catalogs. A name PostgreSQL truncated to 63
// characters will not match and the statement is fanned out as before:
// missing one is the safe direction, since the outcome is the behaviour
// that exists today rather than a refusal of something unrelated.
// refuseRegisteredSequenceObject refuses a statement naming the per-shard
// sequence behind a registered global sequence, when acting on it would
// either destroy something or defeat the guard above.
//
// Not every statement over it: a GRANT or an OWNER change on the physical
// sequence is inert for a registered global sequence -- the router
// allocates from the catalog and nothing reads that sequence -- and
// refusing what costs nothing would break the migration tools that grant
// over every object they find. What is refused is what does not come back:
// a DROP takes the column's default with it under CASCADE, and a RENAME or
// a SET SCHEMA moves the name this guard is derived from, so the next
// ALTER SEQUENCE would be fanned out unnoticed.
func (w *walker) refuseRegisteredSequenceObject(verb string, rv *pgquerypb.RangeVar) error {
	seq := w.registeredSequenceObject(rv)
	if seq == "" {
		return nil
	}
	return notYet(verb+" on "+rv.GetRelname()+" is not available through the router: it is the per-shard sequence behind the global sequence "+seq+
		", and the router allocates from the catalog rather than from it",
		"drop the column's registration in pgshard.tables first if the global sequence is really going away")
}

func (w *walker) registeredSequenceObject(rv *pgquerypb.RangeVar) string {
	if rv == nil || w.sess.Snapshot == nil || rv.GetRelname() == "" {
		return ""
	}
	schemas := []string{rv.GetSchemaname()}
	if rv.GetSchemaname() == "" {
		schemas = w.sess.SearchPath
		if len(schemas) == 0 {
			schemas = []string{"public"}
		}
	}
	for name := range w.sess.Snapshot.Sequences {
		parts := strings.Split(name, ".")
		if len(parts) != 4 || parts[0] != w.sess.Database {
			continue
		}
		schema, table, col := parts[1], parts[2], parts[3]
		if !contains(schemas, schema) {
			continue
		}
		if table+"_"+col+"_seq" == rv.GetRelname() {
			return name
		}
	}
	return ""
}
