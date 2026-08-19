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

// volatileFuncs are the functions whose value differs between shards (or
// between calls) and so cannot appear in a statement replicated to every
// shard of a reference table.
var volatileFuncs = map[string]bool{
	"now": true, "clock_timestamp": true, "statement_timestamp": true, "transaction_timestamp": true,
	"timeofday": true, "random": true, "random_normal": true, "gen_random_uuid": true, "uuidv4": true, "uuidv7": true,
	"nextval": true, "currval": true, "lastval": true, "setval": true, "pg_backend_pid": true,
	"txid_current": true, "pg_current_xact_id": true, "pg_current_wal_lsn": true, "inet_server_addr": true,
	"current_date": true, "current_time": true, "current_timestamp": true, "localtime": true, "localtimestamp": true,
}

// volatileCall names the first volatile function call under node, "" if none.
func volatileCall(node *pgquerypb.Node) string {
	found := ""
	visit(node, func(n *pgquerypb.Node) bool {
		if found != "" {
			return false
		}
		switch x := n.GetNode().(type) {
		case *pgquerypb.Node_FuncCall:
			names := stringList(x.FuncCall.GetFuncname())
			if len(names) > 0 && volatileFuncs[strings.ToLower(names[len(names)-1])] {
				found = names[len(names)-1]
				return false
			}
		case *pgquerypb.Node_SqlvalueFunction:
			switch x.SqlvalueFunction.GetOp() {
			case pgquerypb.SQLValueFunctionOp_SVFOP_CURRENT_DATE, pgquerypb.SQLValueFunctionOp_SVFOP_CURRENT_TIME,
				pgquerypb.SQLValueFunctionOp_SVFOP_CURRENT_TIME_N, pgquerypb.SQLValueFunctionOp_SVFOP_CURRENT_TIMESTAMP,
				pgquerypb.SQLValueFunctionOp_SVFOP_CURRENT_TIMESTAMP_N, pgquerypb.SQLValueFunctionOp_SVFOP_LOCALTIME,
				pgquerypb.SQLValueFunctionOp_SVFOP_LOCALTIME_N, pgquerypb.SQLValueFunctionOp_SVFOP_LOCALTIMESTAMP,
				pgquerypb.SQLValueFunctionOp_SVFOP_LOCALTIMESTAMP_N:
				found = strings.ToLower(strings.TrimPrefix(x.SqlvalueFunction.GetOp().String(), "SVFOP_"))
				return false
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
	arg := fc.GetArgs()[0]
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
