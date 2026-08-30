package plan

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/pgparser"
	"github.com/andrew01234567890/pgshard/internal/pgparser/pg18/pgquerypb"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"google.golang.org/protobuf/proto"
)

// Planner plans statements against catalog snapshots.
type Planner struct {
	parser *pgparser.Parser
}

// New builds a Planner with a bounded parse cache.
func New() *Planner { return NewWithMetrics(nil) }

// NewWithMetrics builds a Planner whose parse cache reports to m.
func NewWithMetrics(m pgparser.Metrics) *Planner {
	return &Planner{parser: pgparser.New(pgparser.Options{CacheEntries: 4096, CacheBytes: 32 << 20, Metrics: m})}
}

const cursorOptHold = 0x0020

// Plan parses sql and resolves the shards it touches for sess. Text the
// bound grammar rejects is refused outright: forwarding it would run it on
// the home shard alone, silently skipping the shards a newer server's
// grammar would have targeted.
func (p *Planner) Plan(ctx context.Context, sess Session, sql string) (Plan, error) {
	return p.plan(ctx, sess, sql, false)
}

// plan resolves sql. masked is set on the second pass over a statement the
// online-rewrite masking rewrote, which exists so everything derived from
// the statement -- the merge spec and its ShardSQL as much as the text a
// single shard receives -- comes from the same masked tree. Without it the
// merge spec was built from the client's text and the masking applied
// afterwards, so a scatter sent the shards the star form and the working
// column came back to the client.
func (p *Planner) plan(ctx context.Context, sess Session, sql string, masked bool) (Plan, error) {
	res, err := p.parser.Parse(ctx, sql)
	if err != nil {
		var perr *pgparser.Error
		if errors.As(err, &perr) {
			e := pgwire.Errorf(perr.SQLState, "%s", perr.Message)
			if perr.SQLState == pgparser.SyntaxErrorSQLState {
				e.Hint = "the router only forwards SQL the PostgreSQL 18 grammar accepts"
			}
			return Plan{Kind: Refuse, Err: e}, e
		}
		return Plan{}, err
	}
	if len(res.Stmts) == 0 {
		return sess.session(), nil
	}
	if len(res.Stmts) > 1 {
		return refuse("multi-statement queries are not supported through the router", "send one statement per query")
	}
	raw, ok := res.Stmts[0].RawStmt.(*pgquerypb.RawStmt)
	if !ok {
		return sess.unsharded(), nil
	}
	pl := &Plan{Generation: sess.generation(), home: sess.HomeShard, set: sess.shardSet(), snap: sess.Snapshot}
	if err := classify(raw.GetStmt(), &pl.Class); err != nil {
		return refusalErr(err)
	}
	if rw := readWriteRewrite(raw.GetStmt()); rw != "" {
		pl.Rewritten = rw
	}
	// Once per distinct statement, not once per execution: the scan walks
	// every field of every node by reflection and depends on the tree
	// alone, so the parse cache's result is where the answer belongs.
	scan := res.Memo(func() any { return scanStatement(raw.GetStmt()) }).(preScan)
	if scan.setConfigErr != nil {
		return refusalErr(scan.setConfigErr)
	}
	w := &walker{sess: sess, plan: pl, tree: res.Tree, root: raw.GetStmt(), raw: raw, sql: sql, hiddenName: scan.hiddenName}
	if err := w.statement(raw.GetStmt()); err != nil {
		return refusalErr(err)
	}
	if err := w.hideRewriteColumns(); err != nil {
		return refusalErr(err)
	}
	if pl.Rewritten != "" && !masked {
		// The masked text names its columns, so the second pass finds
		// nothing left to mask and cannot recurse again.
		out, err := p.plan(ctx, sess, pl.Rewritten, true)
		if err != nil {
			return out, err
		}
		out.Rewritten = pl.Rewritten
		return out, nil
	}
	pl.Class.Write = pl.Kind != SessionLocal && (w.stmt != "SELECT" || w.locking)
	if pl.merge != nil && raw.GetStmt().GetSelectStmt() == nil {
		pl.merge, pl.mergeErr = nil, notYet("only a plain SELECT can run on multiple shards", "filter on one shard key value")
		if pl.Kind == Scatter {
			return refusalErr(pl.mergeErr)
		}
	}
	return *pl, nil
}

func (s Session) generation() int64 {
	if s.Snapshot == nil {
		return 0
	}
	return s.Snapshot.ShardMapGeneration
}

// shardSet is the shard set plans locate keys in: the serving set of the
// snapshot, or DefaultShardSet without one.
func (s Session) shardSet() string {
	if s.Snapshot == nil {
		return DefaultShardSet
	}
	return s.Snapshot.ServingShardSet()
}

func (s Session) unsharded() Plan {
	return Plan{Kind: Unsharded, Shards: []int32{s.HomeShard}, Generation: s.generation(), Class: StmtClass{Write: true}}
}

func (s Session) session() Plan { return Plan{Kind: SessionLocal, Generation: s.generation()} }

// classify picks up the session-level facts the executor tracks and the
// refusals that do not depend on the catalog.
func classify(node *pgquerypb.Node, c *StmtClass) error {
	switch n := node.GetNode().(type) {
	case *pgquerypb.Node_UpdateStmt:
		// pg_catalog.pg_settings is an updatable view whose rule rewrites the
		// UPDATE into set_config(name, setting, false), so writing to it is a
		// SET by another route. Refuse it for protected durability GUCs.
		if targetsPgSettings(n.UpdateStmt.GetRelation()) {
			err := pgwire.Errorf(pgwire.CodeInsufficientPrivilege, "updating pg_settings is not permitted through pgshard")
			err.Hint = "durability settings are fixed by the cluster to keep commits recoverable across failover"
			return err
		}
	case *pgquerypb.Node_ListenStmt:
		return notYet("LISTEN is not supported through the router", "")
	case *pgquerypb.Node_NotifyStmt:
		return notYet("NOTIFY is not supported through the router", "")
	case *pgquerypb.Node_UnlistenStmt:
		return notYet("UNLISTEN is not supported through the router", "")
	case *pgquerypb.Node_DeclareCursorStmt:
		if n.DeclareCursorStmt.GetOptions()&cursorOptHold != 0 {
			return notYet("WITH HOLD cursors are not supported through the router", "")
		}
	case *pgquerypb.Node_CreateStmt:
		switch n.CreateStmt.GetRelation().GetRelpersistence() {
		case "t":
			return notYet("temporary tables are not supported through the router", "")
		case "u":
			return notDurable("CREATE UNLOGGED TABLE")
		}
	case *pgquerypb.Node_CreateTableAsStmt:
		// The same node carries CREATE MATERIALIZED VIEW and SELECT INTO.
		form := "CREATE UNLOGGED TABLE AS"
		if n.CreateTableAsStmt.GetObjtype() == pgquerypb.ObjectType_OBJECT_MATVIEW {
			form = "CREATE UNLOGGED MATERIALIZED VIEW"
		}
		switch n.CreateTableAsStmt.GetInto().GetRel().GetRelpersistence() {
		case "t":
			return notYet("temporary tables are not supported through the router", "")
		case "u":
			return notDurable(form)
		}
	case *pgquerypb.Node_CreateSeqStmt:
		switch n.CreateSeqStmt.GetSequence().GetRelpersistence() {
		case "t":
			return notYet("temporary sequences are not supported through the router", "")
		case "u":
			return notDurable("CREATE UNLOGGED SEQUENCE")
		}
	case *pgquerypb.Node_TransactionStmt:
		t := n.TransactionStmt
		c.Chain = t.GetChain()
		switch t.GetKind() {
		case pgquerypb.TransactionStmtKind_TRANS_STMT_BEGIN, pgquerypb.TransactionStmtKind_TRANS_STMT_START:
			c.Txn = TxnBegin
		case pgquerypb.TransactionStmtKind_TRANS_STMT_COMMIT:
			c.Txn = TxnCommit
		case pgquerypb.TransactionStmtKind_TRANS_STMT_ROLLBACK:
			c.Txn = TxnRollback
		case pgquerypb.TransactionStmtKind_TRANS_STMT_SAVEPOINT:
			c.Txn, c.Savepoint = TxnSavepoint, t.GetSavepointName()
		case pgquerypb.TransactionStmtKind_TRANS_STMT_RELEASE:
			c.Txn, c.Savepoint = TxnRelease, t.GetSavepointName()
		case pgquerypb.TransactionStmtKind_TRANS_STMT_ROLLBACK_TO:
			c.Txn, c.Savepoint = TxnRollbackTo, t.GetSavepointName()
		case pgquerypb.TransactionStmtKind_TRANS_STMT_PREPARE, pgquerypb.TransactionStmtKind_TRANS_STMT_COMMIT_PREPARED,
			pgquerypb.TransactionStmtKind_TRANS_STMT_ROLLBACK_PREPARED:
			return notYet("PREPARE TRANSACTION, COMMIT PREPARED and ROLLBACK PREPARED are reserved for the router's transaction coordinator",
				"use COMMIT; the router runs two-phase commit itself when a transaction writes to several shards")
		}
	case *pgquerypb.Node_PrepareStmt:
		c.Session, c.SessionName = SessionPrepare, n.PrepareStmt.GetName()
	case *pgquerypb.Node_DeallocateStmt:
		c.Session, c.SessionName = SessionDeallocate, n.DeallocateStmt.GetName()
	case *pgquerypb.Node_DiscardStmt:
		if n.DiscardStmt.GetTarget() == pgquerypb.DiscardMode_DISCARD_ALL {
			c.Session = SessionDiscardAll
		}
	case *pgquerypb.Node_VariableSetStmt:
		s := n.VariableSetStmt
		if s.GetIsLocal() {
			if setsProtectedValue(s.GetKind()) {
				if err := refuseProtectedGUC(s.GetName()); err != nil {
					return err
				}
			}
			if strings.EqualFold(s.GetName(), "search_path") {
				return notYet("SET LOCAL search_path is not available yet", "use SET search_path or schema-qualify table names")
			}
			return nil
		}
		if setsProtectedValue(s.GetKind()) {
			if err := refuseProtectedGUC(s.GetName()); err != nil {
				return err
			}
		}
		switch s.GetKind() {
		case pgquerypb.VariableSetKind_VAR_SET_VALUE, pgquerypb.VariableSetKind_VAR_SET_DEFAULT,
			pgquerypb.VariableSetKind_VAR_SET_CURRENT, pgquerypb.VariableSetKind_VAR_RESET:
			c.SetGUC, c.GUCName = true, strings.ToLower(s.GetName())
			if args := s.GetArgs(); len(args) == 1 {
				c.GUCValue = args[0].GetAConst().GetSval().GetSval()
			}
			if c.GUCName == "search_path" {
				if s.GetKind() == pgquerypb.VariableSetKind_VAR_SET_CURRENT {
					return notYet("SET search_path FROM CURRENT is not available yet", "")
				}
				if s.GetKind() == pgquerypb.VariableSetKind_VAR_SET_VALUE {
					c.SearchPath = searchPathArgs(s.GetArgs())
					if len(c.SearchPath) == 0 {
						return notYet("SET search_path with an unrecognised argument list is not available yet", "use a comma-separated list of schema names")
					}
				}
			}
		case pgquerypb.VariableSetKind_VAR_RESET_ALL:
			c.SetGUC, c.GUCName = true, ""
		case pgquerypb.VariableSetKind_VAR_SET_MULTI:
			if strings.EqualFold(s.GetName(), "SESSION CHARACTERISTICS") {
				c.SetGUC, c.GUCName = true, "session characteristics"
			}
		}
	}
	return nil
}

// protectedDurabilityGUCs are settings the router never lets a client change:
// weakening them would let a transaction be acknowledged before its WAL is
// durable, which silently breaks the durability that failover candidate
// selection assumes. The safe value is forced as the server default instead.
var protectedDurabilityGUCs = map[string]bool{
	"synchronous_commit": true,
	// The barrier pauses writes with default_transaction_read_only, so a
	// client that can turn either of these off writes straight through a
	// pause taken to make a cluster-consistent restore point -- and the
	// router replays client SETs onto every shard session it opens, so one
	// override follows the session everywhere. BEGIN READ ONLY still gives
	// a client a read-only transaction; only overriding the cluster's own
	// setting is refused.
	"default_transaction_read_only": true,
	"transaction_read_only":         true,
}

// refuseProtectedSetConfig refuses set_config('<protected>', ...) anywhere in a
// statement, the expression-level equivalent of SET. A non-constant setting
// name cannot be checked at plan time, so it fails closed.
// preScan makes one pass over the statement for the two checks that each
// used to walk the whole tree on their own. The walk is protobuf
// reflection over every field of every node and is the dominant cost of
// planning -- two passes cost twice that, for answers one pass can carry.
//
// Both answers are returned rather than reported here, so each is still
// raised where it was: the set_config refusal before the walker exists,
// the hidden-name one from hideRewriteColumns. A statement that violates
// both reports the same error it did before.
type preScan struct {
	setConfigErr error
	hiddenName   string
}

func scanStatement(root *pgquerypb.Node) preScan {
	var out preScan
	visit(root, func(n *pgquerypb.Node) bool {
		if cr := n.GetColumnRef(); cr != nil && out.hiddenName == "" {
			for _, f := range stringList(cr.GetFields()) {
				if strings.HasPrefix(f, catalog.HiddenPrefix) {
					out.hiddenName = f
					break
				}
			}
		}
		if rt := n.GetResTarget(); rt != nil && out.hiddenName == "" && strings.HasPrefix(rt.GetName(), catalog.HiddenPrefix) {
			out.hiddenName = rt.GetName()
		}
		if out.setConfigErr == nil {
			out.setConfigErr = setConfigRefusal(n)
		}
		return true
	})
	return out
}

// setConfigRefusal reports why this node may not run, or nil.
func setConfigRefusal(n *pgquerypb.Node) error {
	fc := n.GetFuncCall()
	if fc == nil {
		return nil
	}
	names := stringList(fc.GetFuncname())
	last := len(names) - 1
	if last < 0 || !strings.EqualFold(names[last], "set_config") {
		return nil
	}
	// The durability built-in is pg_catalog.set_config, reachable
	// unqualified or with a pg_catalog schema part (optionally preceded by
	// the current-database name). A set_config in any other schema is a
	// different function. Match on the parse tree, never the SQL text:
	// U&"..." escapes and database qualification hide it from a substring
	// check while still resolving to the built-in.
	if last >= 1 && !strings.EqualFold(names[last-1], "pg_catalog") {
		return nil
	}
	args := fc.GetArgs()
	if len(args) == 0 {
		return nil
	}
	name := args[0].GetAConst().GetSval().GetSval()
	if name == "" {
		err := pgwire.Errorf(pgwire.CodeInsufficientPrivilege, "set_config with a non-constant setting name is not permitted through pgshard")
		err.Hint = "durability settings are fixed by the cluster to keep commits recoverable across failover"
		return err
	}
	return refuseProtectedGUC(name)
}

// targetsPgSettings reports whether a range var names pg_catalog.pg_settings.
func targetsPgSettings(rv *pgquerypb.RangeVar) bool {
	if rv == nil || !strings.EqualFold(rv.GetRelname(), "pg_settings") {
		return false
	}
	sc := rv.GetSchemaname()
	return sc == "" || strings.EqualFold(sc, "pg_catalog")
}

// setsProtectedValue reports whether a SET kind assigns an explicit value (as
// opposed to VAR_SET_DEFAULT/VAR_RESET, which restore the forced-safe server
// default and are always allowed for a protected GUC).
func setsProtectedValue(kind pgquerypb.VariableSetKind) bool {
	return kind == pgquerypb.VariableSetKind_VAR_SET_VALUE ||
		kind == pgquerypb.VariableSetKind_VAR_SET_CURRENT
}

// refuseProtectedGUC returns a refusal error if name is a protected durability
// GUC, and nil otherwise. It is applied to SET, SET LOCAL and RESET of a named
// setting; RESET ALL (empty name) is allowed because it restores the forced
// server default.
// clientGUCs are the pgshard settings a client may set for itself.
var clientGUCs = map[string]bool{
	"pgshard.ddl_async":        true,
	"pgshard.transaction_mode": true,
}

func refuseProtectedGUC(name string) error {
	// pgshard's own namespace is closed except for the settings that are
	// deliberately client-facing: the control plane uses it to tell a shard
	// that a session is its own, and the placement write fence reads one of
	// them, so a client able to set it would exempt itself from a fence
	// that exists to refuse it. Closed by default so a control-plane
	// setting added later is refused without anyone remembering to.
	if lower := strings.ToLower(name); strings.HasPrefix(lower, "pgshard.") && !clientGUCs[lower] {
		err := pgwire.Errorf(pgwire.CodeInsufficientPrivilege, "changing %s is not permitted through pgshard", strings.ToLower(name))
		err.Hint = "settings under the pgshard namespace belong to the control plane"
		return err
	}
	if protectedDurabilityGUCs[strings.ToLower(name)] {
		err := pgwire.Errorf(pgwire.CodeInsufficientPrivilege, "changing %s is not permitted through pgshard", strings.ToLower(name))
		err.Hint = "durability and write-pause settings are fixed by the cluster; use BEGIN READ ONLY for a read-only transaction"
		return err
	}
	return nil
}

// refuseReadWriteOverride refuses the transaction-level spelling of what
// protectedDurabilityGUCs already refuses as a setting: BEGIN READ WRITE,
// START TRANSACTION READ WRITE and SET SESSION CHARACTERISTICS AS
// TRANSACTION READ WRITE all turn transaction_read_only off, which is how
// the barrier pauses writes for a certified restore point. A client may
// still make itself more restrictive -- READ ONLY is untouched -- and a
// plain BEGIN gives a writable transaction whenever the cluster is not
// pausing.
// readWriteRewrite neutralises a transaction that declares itself READ
// WRITE. The barrier pauses writes with default_transaction_read_only, which
// catches what the planner does not class as a write -- a volatile function,
// a set_config -- and the setting form of the override is already refused;
// BEGIN READ WRITE, START TRANSACTION READ WRITE, SET TRANSACTION READ WRITE
// and SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE turn the same
// GUC off through the grammar.
//
// Refusing them would break ordinary clients: pgjdbc sends SET SESSION
// CHARACTERISTICS AS TRANSACTION READ WRITE whenever an application calls
// setReadOnly(false), which a pool does on every connection it hands back.
// So the mode is dropped instead and the cluster's own default put back:
// with no pause running the session is read-write exactly as it asked, and
// during one it stays paused. READ ONLY is left alone -- a session may make
// itself more restrictive, never less.
//
// The rewritten text is planned again, so a form this builds wrongly fails
// as a parse error here rather than reaching a shard.
func readWriteRewrite(node *pgquerypb.Node) string {
	switch {
	case node.GetTransactionStmt() != nil:
		t := node.GetTransactionStmt()
		kind := t.GetKind()
		if kind != pgquerypb.TransactionStmtKind_TRANS_STMT_BEGIN && kind != pgquerypb.TransactionStmtKind_TRANS_STMT_START {
			return ""
		}
		modes, dropped := transactionModes(t.GetOptions())
		if !dropped {
			return ""
		}
		head := "BEGIN"
		if kind == pgquerypb.TransactionStmtKind_TRANS_STMT_START {
			head = "START TRANSACTION"
		}
		return strings.TrimSpace(head + " " + strings.Join(modes, " "))
	case node.GetVariableSetStmt() != nil:
		s := node.GetVariableSetStmt()
		if s.GetKind() != pgquerypb.VariableSetKind_VAR_SET_MULTI {
			return ""
		}
		session := strings.EqualFold(s.GetName(), "SESSION CHARACTERISTICS")
		if !session && !strings.EqualFold(s.GetName(), "TRANSACTION") {
			return ""
		}
		modes, dropped := transactionModes(s.GetArgs())
		if !dropped {
			return ""
		}
		local := ""
		if s.GetIsLocal() {
			local = "LOCAL "
		}
		if len(modes) == 0 {
			// Nothing left to say: put the cluster's own value back, which
			// is off while nothing is pausing and on while something is.
			guc := "transaction_read_only"
			if session {
				guc = "default_transaction_read_only"
			}
			return "SET " + local + guc + " = DEFAULT"
		}
		head := "SET " + local + "TRANSACTION "
		if session {
			head = "SET " + local + "SESSION CHARACTERISTICS AS TRANSACTION "
		}
		return head + strings.Join(modes, " ")
	}
	return ""
}

// transactionModes renders the transaction modes of a BEGIN or SET
// TRANSACTION, dropping a READ WRITE and reporting that it did. The
// grammar carries READ WRITE as the integer 0 and READ ONLY as 1, and an
// argument that is neither counts as READ WRITE: a mode that could turn the
// pause off must not survive because it was spelled oddly.
func transactionModes(options []*pgquerypb.Node) (modes []string, dropped bool) {
	for _, o := range options {
		d := o.GetDefElem()
		if d == nil {
			continue
		}
		switch strings.ToLower(d.GetDefname()) {
		case "transaction_read_only":
			if v := d.GetArg().GetAConst(); v != nil && v.GetIval().GetIval() == 1 {
				modes = append(modes, "READ ONLY")
				continue
			}
			dropped = true
		case "transaction_deferrable":
			if v := d.GetArg().GetAConst(); v != nil && v.GetIval().GetIval() == 1 {
				modes = append(modes, "DEFERRABLE")
				continue
			}
			modes = append(modes, "NOT DEFERRABLE")
		case "transaction_isolation":
			level := d.GetArg().GetAConst().GetSval().GetSval()
			if level == "" {
				continue
			}
			modes = append(modes, "ISOLATION LEVEL "+strings.ToUpper(level))
		}
	}
	return modes, dropped
}

// searchPathArgs turns the arguments of SET search_path into a schema
// list, splitting comma-separated string values the way PostgreSQL does
// (identifiers arrive already case-folded from the parser).
// A parse failure yields an empty, non-nil list: nothing resolves rather
// than everything resolving in the default schemas.
func searchPathArgs(args []*pgquerypb.Node) []string {
	out := []string{}
	for _, a := range args {
		c := a.GetAConst()
		if c == nil {
			continue
		}
		var raw string
		switch v := c.GetVal().(type) {
		case *pgquerypb.A_Const_Sval:
			raw = v.Sval.GetSval()
		default:
			continue
		}
		for _, part := range strings.Split(raw, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if strings.HasPrefix(part, `"`) && strings.HasSuffix(part, `"`) && len(part) >= 2 {
				part = strings.ReplaceAll(part[1:len(part)-1], `""`, `"`)
			}
			out = append(out, part)
		}
	}
	return out
}

// placementKind is what the catalog says about one relation.
type placementKind int

const (
	placeUnsharded placementKind = iota
	placeSharded
	placeReference
)

// rel is one relation reference in the statement.
type rel struct {
	alias    string
	name     string
	schema   string
	kind     placementKind
	shardKey string
	// seqCols are the registered sequence columns of a sharded table.
	seqCols []string
	// hidden and visible are the migration working columns and the
	// client-visible column list of a table under an online rewrite.
	hidden  []string
	visible []string
	// refDeclared marks a reference table that has a row in pgshard.tables,
	// and so a status row the controller inspects. A table that is a
	// reference table only because its database defaults to that placement
	// has neither, and is not gated here.
	refDeclared bool
	// refChecked and refHazards carry the controller's inspection: whether
	// it has run for the generation in force, and what it found that a
	// shard would evaluate for itself.
	refChecked bool
	refHazards []string
	// terms are the key predicates found for this relation.
	terms []keyTerm
	// scatter marks a sharded relation without any key predicate.
	group *rel
}

func (r *rel) root() *rel {
	for r.group != nil {
		r = r.group
	}
	return r
}

// walker collects relations and their key terms across every query level.
type walker struct {
	sess Session
	plan *Plan
	// hiddenName is the first migration working column the statement names,
	// found by the single pre-scan rather than by a walk of its own.
	hiddenName string
	rels       []*rel
	ctes       map[string]bool
	// features of the outermost SELECT that a scatter cannot carry yet.
	scatterBlockers []string
	nested          bool
	stmt            string
	// outerQuals is set while walking the ON clause of an outer join: a key
	// literal there filters only the inner side, so it must not pin the query.
	outerQuals bool
	// locking marks a SELECT with FOR UPDATE/SHARE, which holds row locks
	// until the transaction ends and so counts as a write participant.
	locking bool
	tree    proto.Message
	// outer is the outermost SELECT, the one a multi-shard merge is built for.
	outer *pgquerypb.SelectStmt
	// target is the relation an INSERT, UPDATE or DELETE writes.
	target *rel
	// root is the statement being planned; raw wraps it and sql is its text.
	root *pgquerypb.Node
	raw  *pgquerypb.RawStmt
	sql  string
}

func (w *walker) lookup(rv *pgquerypb.RangeVar) (*rel, error) {
	name := rv.GetRelname()
	if rv.GetSchemaname() == "" && w.ctes[name] {
		return nil, nil
	}
	r := &rel{name: name, alias: rv.GetAlias().GetAliasname()}
	if r.alias == "" {
		r.alias = name
	}
	if rv.GetCatalogname() != "" && rv.GetCatalogname() != w.sess.Database {
		return nil, pgwire.Errorf("0A000", "cross-database references are not implemented: %q", rv.GetCatalogname())
	}
	schemas := w.sess.SearchPath
	if rv.GetSchemaname() != "" {
		schemas = []string{rv.GetSchemaname()}
	} else if len(schemas) == 0 {
		schemas = []string{"public"}
	}
	snap := w.sess.Snapshot
	r.schema = schemas[0]
	for _, schema := range schemas {
		if schema == "pg_catalog" || schema == "information_schema" || schema == "pg_temp" {
			// System schemas never hold pgshard tables. An explicit qualifier
			// means a catalog relation on the home shard; inside a search path
			// they are simply skipped so later schemas are still searched.
			if rv.GetSchemaname() != "" {
				return r, nil
			}
			continue
		}
		if snap == nil {
			continue
		}
		key := snapshot.TableKey{Database: w.sess.Database, SchemaName: schema, TableName: name}
		pl, ok := snap.Tables[key]
		if !ok {
			continue
		}
		w.plan.Tables = append(w.plan.Tables, key)
		r.schema = schema
		switch pl.Placement {
		case "sharded":
			r.kind, r.shardKey, r.seqCols = placeSharded, pl.ShardKey, pl.SequenceColumns
		case "reference":
			r.kind, r.refDeclared, r.refChecked, r.refHazards = placeReference, true, pl.ReferenceChecked, pl.ReferenceHazards
		}
		r.hidden, r.visible = pl.HiddenColumns, pl.VisibleColumns
		return r, nil
	}
	if snap != nil {
		switch snap.Databases[w.sess.Database].DefaultPlacement {
		case "reference":
			// Undeclared, but a reference table all the same: it is
			// replicated to every shard and diverges in the same ways.
			// refDeclared says the inspection can reach it, which it now
			// can -- the controller sweeps the tables of a
			// reference-default database that have no catalog row -- so a
			// write waits for that answer rather than going out unchecked.
			r.kind, r.refDeclared = placeReference, true
		case "sharded":
			return nil, notYet("table \""+name+"\" is not declared in the catalog and the database defaults to sharded placement",
				"declare the table in pgshard.tables with its shard key, or set the database default placement to unsharded")
		}
	}
	return r, nil
}

func (w *walker) add(r *rel) {
	if r != nil {
		w.rels = append(w.rels, r)
	}
}

// statement dispatches on the statement type.
func (w *walker) statement(node *pgquerypb.Node) error {
	switch n := node.GetNode().(type) {
	case *pgquerypb.Node_SelectStmt:
		w.stmt = "SELECT"
		if name := w.nextvalName(n.SelectStmt); name != "" {
			w.plan.Kind, w.plan.Shards, w.plan.NextVal = SessionLocal, nil, name
			return nil
		}
		if err := w.selectStmt(n.SelectStmt); err != nil {
			return err
		}
		return w.finishRead()
	case *pgquerypb.Node_InsertStmt:
		w.stmt = "INSERT"
		return w.insert(n.InsertStmt)
	case *pgquerypb.Node_UpdateStmt:
		w.stmt = "UPDATE"
		return w.update(n.UpdateStmt)
	case *pgquerypb.Node_DeleteStmt:
		w.stmt = "DELETE"
		return w.delete(n.DeleteStmt)
	case *pgquerypb.Node_ExplainStmt:
		return w.statement(n.ExplainStmt.GetQuery())
	case *pgquerypb.Node_DeclareCursorStmt:
		return w.statement(n.DeclareCursorStmt.GetQuery())
	case *pgquerypb.Node_PrepareStmt:
		if err := w.statement(n.PrepareStmt.GetQuery()); err != nil {
			return err
		}
		if w.plan.Kind != Unsharded {
			return notYet("SQL-level PREPARE touching sharded or reference tables is not available yet",
				"use protocol-level prepared statements ($1 bind parameters)")
		}
		return nil
	case *pgquerypb.Node_CopyStmt:
		if q := n.CopyStmt.GetQuery(); q != nil {
			return w.statement(q)
		}
		r, err := w.lookup(n.CopyStmt.GetRelation())
		if err != nil {
			return err
		}
		if r.kind != placeUnsharded {
			return notYet("COPY on sharded and reference tables is not available yet", "COPY through the router works for unsharded tables only")
		}
		return w.unshardedOnly()
	case *pgquerypb.Node_CreateStmt:
		return w.createTable(n.CreateStmt)
	case *pgquerypb.Node_IndexStmt:
		return w.createIndex(n.IndexStmt)
	case *pgquerypb.Node_AlterTableStmt:
		return w.alterTable(n.AlterTableStmt)
	case *pgquerypb.Node_RenameStmt:
		return w.rename(n.RenameStmt)
	case *pgquerypb.Node_ViewStmt:
		return w.createView(n.ViewStmt)
	case *pgquerypb.Node_CreateTableAsStmt:
		return w.derived(n.CreateTableAsStmt.GetInto().GetRel(), n.CreateTableAsStmt.GetQuery(), "CREATE TABLE AS")
	case *pgquerypb.Node_TruncateStmt:
		if w.sess.Snapshot != nil && w.sess.Snapshot.Resharding() {
			return notYet("TRUNCATE is not available while a reshard is active: the copy streams row changes only",
				"DELETE the rows, or wait for the reshard to complete")
		}
		return w.maintenanceList("TRUNCATE", n.TruncateStmt.GetRelations())
	case *pgquerypb.Node_LockStmt:
		return w.maintenanceList("LOCK TABLE", n.LockStmt.GetRelations())
	case *pgquerypb.Node_VacuumStmt:
		return w.vacuum(n.VacuumStmt)
	case *pgquerypb.Node_CreateSchemaStmt:
		return w.migration(Migration{Kind: "CREATE SCHEMA", Scope: ScopeAll,
			Object: ObjectRef{Kind: "schema", Name: n.CreateSchemaStmt.GetSchemaname(), Expect: objectPresent}})
	case *pgquerypb.Node_CreateSeqStmt:
		return w.migration(Migration{Kind: "CREATE SEQUENCE", Scope: ScopeAll, Object: relationRef(n.CreateSeqStmt.GetSequence(), objectPresent)})
	case *pgquerypb.Node_AlterSeqStmt:
		return w.migration(Migration{Kind: "ALTER SEQUENCE", Scope: ScopeAll})
	case *pgquerypb.Node_CompositeTypeStmt, *pgquerypb.Node_CreateEnumStmt, *pgquerypb.Node_CreateRangeStmt:
		return w.migration(Migration{Kind: "CREATE TYPE", Scope: ScopeAll})
	case *pgquerypb.Node_AlterEnumStmt:
		return w.migration(Migration{Kind: "ALTER TYPE", Scope: ScopeAll})
	case *pgquerypb.Node_ReindexStmt:
		return w.reindex(n.ReindexStmt)
	case *pgquerypb.Node_GrantStmt:
		return w.grant(n.GrantStmt)
	case *pgquerypb.Node_GrantRoleStmt:
		return w.grantRole(n.GrantRoleStmt)
	case *pgquerypb.Node_AlterDefaultPrivilegesStmt:
		return notYet("ALTER DEFAULT PRIVILEGES is not available through the router",
			"default ACLs are recorded per creating role and schema; GRANT on the objects after creating them")
	case *pgquerypb.Node_ReassignOwnedStmt:
		return refuseOwned("REASSIGN OWNED")
	case *pgquerypb.Node_DropOwnedStmt:
		return refuseOwned("DROP OWNED")
	case *pgquerypb.Node_CreateRoleStmt:
		return w.createRole(w.raw, n.CreateRoleStmt)
	case *pgquerypb.Node_AlterRoleStmt:
		return w.alterRole(w.raw, n.AlterRoleStmt)
	case *pgquerypb.Node_AlterRoleSetStmt:
		return w.alterRoleSet(n.AlterRoleSetStmt)
	case *pgquerypb.Node_DropRoleStmt:
		return w.dropRole(n.DropRoleStmt)
	case *pgquerypb.Node_CreatedbStmt:
		return w.createDatabase(n.CreatedbStmt)
	case *pgquerypb.Node_DropdbStmt:
		return w.dropDatabase(n.DropdbStmt)
	case *pgquerypb.Node_AlterObjectSchemaStmt:
		return w.alterObject("ALTER TABLE SET SCHEMA", n.AlterObjectSchemaStmt.GetObjectType(), n.AlterObjectSchemaStmt.GetRelation())
	case *pgquerypb.Node_AlterOwnerStmt:
		return w.alterObject("ALTER TABLE OWNER", n.AlterOwnerStmt.GetObjectType(), n.AlterOwnerStmt.GetRelation())
	case *pgquerypb.Node_DropStmt:
		return w.drop(n.DropStmt)
	case *pgquerypb.Node_TransactionStmt, *pgquerypb.Node_VariableSetStmt, *pgquerypb.Node_VariableShowStmt,
		*pgquerypb.Node_DiscardStmt, *pgquerypb.Node_DeallocateStmt, *pgquerypb.Node_ClosePortalStmt,
		*pgquerypb.Node_FetchStmt, *pgquerypb.Node_ExecuteStmt, *pgquerypb.Node_CheckPointStmt, *pgquerypb.Node_ConstraintsSetStmt:
		w.plan.Kind = SessionLocal
		w.plan.Shards = nil
		return nil
	}
	// Fail closed: a statement shape the planner does not recognise could
	// write, and routing it to the home shard would run it on one shard
	// silently. Everything the router supports is listed above.
	return notYet(statementName(node)+" is not supported through the router", "")
}

// statementName renders the human-readable name of an unrecognised
// statement node for the refusal message.
func statementName(node *pgquerypb.Node) string {
	inner := node.GetNode()
	if inner == nil {
		return "an empty statement"
	}
	name := strings.TrimPrefix(fmt.Sprintf("%T", inner), "*pgquerypb.Node_")
	name = strings.TrimSuffix(name, "Stmt")
	var out []byte
	for i := 0; i < len(name); i++ {
		c := name[i]
		if i > 0 && c >= 'A' && c <= 'Z' {
			out = append(out, ' ')
		}
		out = append(out, c)
	}
	name = strings.ToUpper(string(out))
	r := strings.NewReplacer("MAT VIEW", "MATERIALIZED VIEW", "SEC LABEL", "SECURITY LABEL", "SEQ ", "SEQUENCE ", "TRIG ", "TRIGGER ")
	return r.Replace(name)
}

// unshardedOnly pins the plan to the home shard.
func (w *walker) unshardedOnly() error {
	w.plan.Kind, w.plan.Shards = Unsharded, []int32{w.sess.HomeShard}
	return nil
}

func (w *walker) maintenanceList(what string, nodes []*pgquerypb.Node) error {
	for _, n := range nodes {
		if err := w.maintenance(what, n.GetRangeVar()); err != nil {
			return err
		}
	}
	return nil
}

// maintenance lets TRUNCATE, LOCK and VACUUM of unsharded tables through to
// the home shard and refuses them for tables that live on every shard.
func (w *walker) maintenance(what string, rv *pgquerypb.RangeVar) error {
	if rv == nil {
		return w.unshardedOnly()
	}
	r, err := w.lookup(rv)
	if err != nil {
		return err
	}
	if r != nil && r.kind != placeUnsharded {
		return notYet(what+" on sharded and reference tables is not available yet: \""+r.name+"\" lives on every shard",
			"run it on every shard through the operator")
	}
	return w.unshardedOnly()
}

// derived handles CREATE TABLE AS on the home shard: the query must itself
// be home-shard only.
func (w *walker) derived(rv *pgquerypb.RangeVar, query *pgquerypb.Node, what string) error {
	r, err := w.lookup(rv)
	if err != nil {
		return err
	}
	if r != nil && r.kind != placeUnsharded {
		return notYet(what+" cannot create the sharded or reference table \""+r.name+"\"",
			"CREATE TABLE it through the migration model, then INSERT ... SELECT the rows")
	}
	if err := w.statement(query); err != nil {
		return err
	}
	if w.plan.Kind != Unsharded {
		return notYet(what+" over sharded or reference tables is not available yet",
			"the object would exist on the home shard only; create it through the operator on every shard")
	}
	return nil
}

func keyConstraintError(r *rel, cols string) error {
	return notYet("primary key or unique constraint ("+cols+") on sharded table \""+r.name+"\" must include the shard key \""+r.shardKey+"\"",
		"uniqueness is enforced per shard; include \""+r.shardKey+"\" in the constraint")
}

func isUniqueConstraint(c *pgquerypb.Constraint) bool {
	return c.GetContype() == pgquerypb.ConstrType_CONSTR_PRIMARY || c.GetContype() == pgquerypb.ConstrType_CONSTR_UNIQUE
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func stringList(nodes []*pgquerypb.Node) []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if s := n.GetString_(); s != nil {
			out = append(out, s.GetSval())
		}
	}
	return out
}

// selectStmt walks one SELECT (and its set-operation arms, CTEs and
// subqueries), recording relations and key predicates.
func (w *walker) selectStmt(s *pgquerypb.SelectStmt) error {
	if err := w.with(s.GetWithClause()); err != nil {
		return err
	}
	if s.GetOp() != pgquerypb.SetOperation_SETOP_NONE && s.GetOp() != pgquerypb.SetOperation_SET_OPERATION_UNDEFINED {
		w.blocker("set operations")
		w.nested = true
		if err := w.selectStmt(s.GetLarg()); err != nil {
			return err
		}
		return w.selectStmt(s.GetRarg())
	}
	if len(s.GetLockingClause()) > 0 {
		w.locking = true
	}
	if !w.nested {
		w.outer = s
		w.outerFeatures(s)
	}
	scope := len(w.rels)
	for _, item := range s.GetFromClause() {
		if err := w.fromItem(item); err != nil {
			return err
		}
	}
	local := w.rels[scope:]
	if err := w.where(s.GetWhereClause(), local); err != nil {
		return err
	}
	if err := w.exprs(s.GetTargetList()); err != nil {
		return err
	}
	if err := w.expr(s.GetHavingClause()); err != nil {
		return err
	}
	for _, v := range s.GetValuesLists() {
		if err := w.expr(v); err != nil {
			return err
		}
	}
	return nil
}

func (w *walker) blocker(name string) {
	if !contains(w.scatterBlockers, name) {
		w.scatterBlockers = append(w.scatterBlockers, name)
	}
}

func (w *walker) outerFeatures(s *pgquerypb.SelectStmt) {
	if len(s.GetWindowClause()) > 0 {
		w.blocker("window functions")
	}
	for _, t := range s.GetTargetList() {
		if hasWindow(t) {
			w.blocker("window functions")
			break
		}
	}
	if len(s.GetLockingClause()) > 0 {
		w.blocker("FOR UPDATE/SHARE")
	}
	if s.GetIntoClause() != nil {
		w.blocker("SELECT INTO")
	}
}

func hasWindow(node *pgquerypb.Node) bool {
	found := false
	visit(node, func(n *pgquerypb.Node) bool {
		if fc := n.GetFuncCall(); fc != nil && fc.GetOver() != nil {
			found = true
		}
		return !found
	})
	return found
}

var aggregateNames = map[string]bool{
	"count": true, "sum": true, "avg": true, "min": true, "max": true, "array_agg": true, "string_agg": true,
	"bool_and": true, "bool_or": true, "every": true, "json_agg": true, "jsonb_agg": true, "json_object_agg": true,
	"jsonb_object_agg": true, "stddev": true, "stddev_pop": true, "stddev_samp": true, "variance": true,
	"var_pop": true, "var_samp": true, "bit_and": true, "bit_or": true, "bit_xor": true, "xmlagg": true,
	"percentile_cont": true, "percentile_disc": true, "mode": true, "rank": true, "dense_rank": true,
	"row_number": true, "first_value": true, "last_value": true, "lag": true, "lead": true, "ntile": true,
	"cume_dist": true, "percent_rank": true, "nth_value": true, "range_agg": true, "range_intersect_agg": true,
	"any_value": true, "corr": true, "covar_pop": true, "covar_samp": true, "regr_avgx": true, "regr_avgy": true,
	"regr_count": true, "regr_intercept": true, "regr_r2": true, "regr_slope": true, "regr_sxx": true,
	"regr_sxy": true, "regr_syy": true,
}

// hasStar reports whether the expression expands to an unknown number of
// output columns.
func hasStar(node *pgquerypb.Node) bool {
	found := false
	visit(node, func(n *pgquerypb.Node) bool {
		if n.GetAStar() != nil {
			found = true
		}
		return !found
	})
	return found
}

func hasAggregate(node *pgquerypb.Node) bool {
	found := false
	visit(node, func(n *pgquerypb.Node) bool {
		fc := n.GetFuncCall()
		if fc == nil {
			return !found
		}
		names := stringList(fc.GetFuncname())
		if fc.GetAggStar() || fc.GetAggDistinct() || fc.GetAggFilter() != nil || len(fc.GetAggOrder()) > 0 || fc.GetOver() != nil ||
			(len(names) > 0 && aggregateNames[strings.ToLower(names[len(names)-1])]) {
			found = true
		}
		return !found
	})
	return found
}

func (w *walker) with(wc *pgquerypb.WithClause) error {
	for _, c := range wc.GetCtes() {
		cte := c.GetCommonTableExpr()
		if cte == nil {
			continue
		}
		if w.ctes == nil {
			w.ctes = map[string]bool{}
		}
		w.ctes[cte.GetCtename()] = true
		w.blocker("common table expressions")
		if err := w.nestedStatement(cte.GetCtequery()); err != nil {
			return err
		}
	}
	return nil
}

// nestedStatement plans a subquery, CTE body or writable CTE within the
// current plan.
func (w *walker) nestedStatement(node *pgquerypb.Node) error {
	prev := w.nested
	w.nested = true
	defer func() { w.nested = prev }()
	switch n := node.GetNode().(type) {
	case *pgquerypb.Node_SelectStmt:
		return w.selectStmt(n.SelectStmt)
	case *pgquerypb.Node_InsertStmt, *pgquerypb.Node_UpdateStmt, *pgquerypb.Node_DeleteStmt:
		return notYet("data-modifying statements in WITH are not available yet", "run the modification as its own statement")
	}
	return nil
}

func (w *walker) fromItem(node *pgquerypb.Node) error {
	switch n := node.GetNode().(type) {
	case *pgquerypb.Node_RangeVar:
		r, err := w.lookup(n.RangeVar)
		if err != nil {
			return err
		}
		w.add(r)
	case *pgquerypb.Node_JoinExpr:
		w.blocker("joins")
		scope := len(w.rels)
		if err := w.fromItem(n.JoinExpr.GetLarg()); err != nil {
			return err
		}
		if err := w.fromItem(n.JoinExpr.GetRarg()); err != nil {
			return err
		}
		if len(n.JoinExpr.GetUsingClause()) > 0 || n.JoinExpr.GetIsNatural() {
			w.joinUsing(n.JoinExpr, w.rels[scope:])
		}
		if n.JoinExpr.GetJointype() != pgquerypb.JoinType_JOIN_INNER {
			prev := w.outerQuals
			w.outerQuals = true
			defer func() { w.outerQuals = prev }()
		}
		return w.where(n.JoinExpr.GetQuals(), w.rels[scope:])
	case *pgquerypb.Node_RangeSubselect:
		w.blocker("subqueries")
		return w.nestedStatement(n.RangeSubselect.GetSubquery())
	case *pgquerypb.Node_RangeFunction:
		w.blocker("function scans")
		for _, f := range n.RangeFunction.GetFunctions() {
			if err := w.expr(f); err != nil {
				return err
			}
		}
	}
	return nil
}

// joinUsing unifies the shard keys of tables joined with USING (key) or
// NATURAL when both sides expose the key column.
func (w *walker) joinUsing(j *pgquerypb.JoinExpr, rels []*rel) {
	using := stringList(j.GetUsingClause())
	var sharded []*rel
	for _, r := range rels {
		if r.kind == placeSharded && (j.GetIsNatural() || contains(using, r.shardKey)) {
			sharded = append(sharded, r)
		}
	}
	for i := 1; i < len(sharded); i++ {
		if sharded[i].shardKey == sharded[0].shardKey {
			unify(sharded[0], sharded[i])
		}
	}
}

func unify(a, b *rel) {
	ra, rb := a.root(), b.root()
	if ra != rb {
		rb.group = ra
	}
}

// exprs walks expressions for subqueries.
func (w *walker) exprs(nodes []*pgquerypb.Node) error {
	for _, n := range nodes {
		if err := w.expr(n); err != nil {
			return err
		}
	}
	return nil
}

// expr walks an expression tree for sublinks whose relations count too.
func (w *walker) expr(node *pgquerypb.Node) error {
	var err error
	visit(node, func(n *pgquerypb.Node) bool {
		if err != nil {
			return false
		}
		if sl := n.GetSubLink(); sl != nil {
			w.blocker("subqueries")
			err = w.nestedStatement(sl.GetSubselect())
			return false
		}
		return true
	})
	return err
}

// where extracts key predicates from a conjunction for the relations in
// scope; the rest of the expression is walked for subqueries.
func (w *walker) where(node *pgquerypb.Node, scope []*rel) error {
	if node == nil {
		return nil
	}
	for _, conj := range conjuncts(node) {
		if err := w.conjunct(conj, scope); err != nil {
			return err
		}
	}
	return nil
}

func conjuncts(node *pgquerypb.Node) []*pgquerypb.Node {
	if b := node.GetBoolExpr(); b != nil && b.GetBoolop() == pgquerypb.BoolExprType_AND_EXPR {
		var out []*pgquerypb.Node
		for _, a := range b.GetArgs() {
			out = append(out, conjuncts(a)...)
		}
		return out
	}
	return []*pgquerypb.Node{node}
}

func (w *walker) conjunct(node *pgquerypb.Node, scope []*rel) error {
	ae := node.GetAExpr()
	if ae == nil {
		return w.expr(node)
	}
	op := strings.Join(stringList(ae.GetName()), ".")
	switch {
	case ae.GetKind() == pgquerypb.A_Expr_Kind_AEXPR_OP && op == "=":
		if err := w.refuseCastKey(ae.GetLexpr(), scope); err != nil {
			return err
		}
		if err := w.refuseCastKey(ae.GetRexpr(), scope); err != nil {
			return err
		}
		l, lok := w.keyColumn(ae.GetLexpr(), scope)
		r, rok := w.keyColumn(ae.GetRexpr(), scope)
		switch {
		case lok && rok:
			if l.shardKey == r.shardKey {
				unify(l, r)
			}
			return nil
		case lok && !w.outerQuals:
			return w.term(l, ae.GetRexpr(), false)
		case rok && !w.outerQuals:
			return w.term(r, ae.GetLexpr(), false)
		}
	case ae.GetKind() == pgquerypb.A_Expr_Kind_AEXPR_IN && op == "=" && !w.outerQuals:
		if err := w.refuseCastKey(ae.GetLexpr(), scope); err != nil {
			return err
		}
		if l, ok := w.keyColumn(ae.GetLexpr(), scope); ok {
			return w.term(l, ae.GetRexpr(), true)
		}
	}
	return w.expr(node)
}

// term records value(s) for a key; expressions that are neither constants
// nor parameters are ignored (the predicate then does not route).
func (w *walker) term(r *rel, value *pgquerypb.Node, list bool) error {
	t := keyTerm{list: list}
	items := []*pgquerypb.Node{value}
	if list {
		if l := value.GetList(); l != nil {
			items = l.GetItems()
		} else {
			return w.expr(value)
		}
	}
	for _, it := range items {
		item, ok, err := constOrParam(it)
		if err != nil {
			return err
		}
		if !ok {
			return w.expr(value)
		}
		if item.param != 0 {
			t.params = append(t.params, ParamRef{Number: item.param, Hint: item.hint})
		} else {
			t.values = append(t.values, item.value)
		}
	}
	r.terms = append(r.terms, t)
	return nil
}

// keyColumn reports whether expr is a reference to a sharded relation's
// shard key among the relations in scope.
func (w *walker) keyColumn(expr *pgquerypb.Node, scope []*rel) (*rel, bool) {
	cr := expr.GetColumnRef()
	if cr == nil {
		return nil, false
	}
	fields := stringList(cr.GetFields())
	if len(fields) == 0 || len(fields) != len(cr.GetFields()) {
		return nil, false
	}
	col := fields[len(fields)-1]
	var qual string
	if len(fields) >= 2 {
		qual = fields[len(fields)-2]
	}
	var match *rel
	for _, r := range scope {
		if r.kind != placeSharded || r.shardKey != col {
			continue
		}
		if qual != "" && qual != r.alias {
			continue
		}
		// An unqualified column that several relations expose is ambiguous
		// unless USING/NATURAL merged them into one key.
		if match != nil && match.root() != r.root() {
			return nil, false
		}
		if match == nil {
			match = r
		}
	}
	return match, match != nil
}

// refuseCastKey rejects a cast applied to the shard key column: the router
// hashes the literal's type, so `tenant_id::text = '7'::text` would route
// to the text hash of '7' while the row lives under the int8 hash.
func (w *walker) refuseCastKey(expr *pgquerypb.Node, scope []*rel) error {
	tc := expr.GetTypeCast()
	if tc == nil {
		return nil
	}
	for inner := tc.GetArg(); inner != nil; {
		if r, ok := w.keyColumn(inner, scope); ok {
			return notYet("shard key column "+r.shardKey+" is compared through a cast",
				"compare the bare column and cast the literal instead")
		}
		next := inner.GetTypeCast()
		if next == nil {
			break
		}
		inner = next.GetArg()
	}
	return nil
}

// keyItem is one shard key operand: a typed literal or a parameter.
type keyItem struct {
	value any
	param int32
	hint  TypeHint
}

// constOrParam extracts a shard-key literal (int64 or string) or a
// parameter from an expression. ok is false for anything else; err reports
// a literal whose type is ambiguous.
func constOrParam(node *pgquerypb.Node) (item keyItem, ok bool, err error) {
	item, ok = literal(node)
	if !ok {
		return keyItem{}, false, nil
	}
	if s, isString := item.value.(string); isString && item.hint == HintNone {
		if _, err := parseInt(strings.TrimSpace(s)); err == nil {
			return keyItem{}, false, notYet("shard key literal '"+s+"' is untyped and looks numeric",
				"cast it: '"+s+"'::int8 or '"+s+"'::text")
		}
	}
	return item, true, nil
}

// literal reads a constant or parameter, applying casts; hint records the
// cast a string literal or parameter carries.
func literal(node *pgquerypb.Node) (keyItem, bool) {
	switch n := node.GetNode().(type) {
	case *pgquerypb.Node_ParamRef:
		return keyItem{param: n.ParamRef.GetNumber()}, true
	case *pgquerypb.Node_TypeCast:
		inner, ok := literal(n.TypeCast.GetArg())
		if !ok {
			return keyItem{}, false
		}
		return castItem(inner, n.TypeCast.GetTypeName())
	case *pgquerypb.Node_AConst:
		switch v := n.AConst.GetVal().(type) {
		case *pgquerypb.A_Const_Ival:
			return keyItem{value: int64(v.Ival.GetIval()), hint: HintInt}, true
		case *pgquerypb.A_Const_Fval:
			if i, err := parseInt(v.Fval.GetFval()); err == nil {
				return keyItem{value: i, hint: HintInt}, true
			}
		case *pgquerypb.A_Const_Sval:
			return keyItem{value: v.Sval.GetSval()}, true
		}
	}
	return keyItem{}, false
}

func castItem(item keyItem, tn *pgquerypb.TypeName) (keyItem, bool) {
	names := stringList(tn.GetNames())
	if len(names) == 0 {
		return keyItem{}, false
	}
	var hint TypeHint
	switch strings.ToLower(names[len(names)-1]) {
	case "int8", "int4", "int2", "bigint", "integer", "int", "smallint":
		hint = HintInt
	case "text", "varchar", "name":
		hint = HintText
	default:
		// bpchar/char/character are refused too: their equality ignores
		// trailing spaces, so a raw-byte hash of the operand may not match
		// the shard the row was placed on.
		return keyItem{}, false
	}
	if item.param != 0 {
		item.hint = hint
		return item, true
	}
	switch x := item.value.(type) {
	case int64:
		if hint == HintInt {
			return item, true
		}
	case string:
		if hint == HintText {
			item.hint = HintText
			return item, true
		}
		if i, err := parseInt(strings.TrimSpace(x)); err == nil {
			return keyItem{value: i, hint: HintInt}, true
		}
	}
	return keyItem{}, false
}

// finishRead decides the plan for a SELECT after every relation was seen.
func (w *walker) finishRead() error { return w.decide(false) }

// decide computes the plan from the collected relations.
func (w *walker) decide(write bool) error {
	p := w.plan
	var sharded, unsharded, reference int
	for _, r := range w.rels {
		switch r.kind {
		case placeSharded:
			sharded++
		case placeReference:
			reference++
		default:
			unsharded++
		}
	}
	if write && w.target != nil && w.target.kind == placeReference {
		return w.referenceWrite(sharded + unsharded)
	}
	if sharded == 0 {
		if unsharded == 0 && reference > 0 {
			p.Kind, p.Shards = Reference, []int32{w.referenceShard()}
			return nil
		}
		p.Kind, p.Shards = Unsharded, []int32{w.sess.HomeShard}
		return nil
	}
	// Share terms across unified relations, then require every sharded
	// relation to have at least one term.
	groups := map[*rel][]keyTerm{}
	for _, r := range w.rels {
		if r.kind == placeSharded {
			groups[r.root()] = append(groups[r.root()], r.terms...)
		}
	}
	var scatter []*rel
	for _, r := range w.rels {
		if r.kind == placeSharded && len(groups[r.root()]) == 0 {
			scatter = append(scatter, r)
		}
	}
	if len(scatter) > 0 {
		return w.scatter(write, sharded+unsharded+reference)
	}
	if !write {
		w.mergeSpec()
	}
	p.touches = Unsharded
	if unsharded == 0 {
		p.touches = EqualUnique
	}
	p.Kind = EqualUnique
	seen := map[*rel]bool{}
	for _, r := range w.rels {
		if r.kind != placeSharded || seen[r.root()] {
			continue
		}
		seen[r.root()] = true
		for _, t := range groups[r.root()] {
			if t.list {
				p.Kind = In
			}
			if len(t.params) > 0 {
				p.Deferred = true
			}
			p.terms = append(p.terms, t)
		}
	}
	if p.Deferred {
		return nil
	}
	values := make([][]any, len(p.terms))
	for i, t := range p.terms {
		values[i] = t.values
	}
	return p.finish(values)
}

// referenceWrite plans a write to a reference table onto every shard of
// the set: the same statement runs on each, so nothing in it may read
// rows that live on one shard only or evaluate differently per shard.
func (w *walker) referenceWrite(otherRels int) error {
	p := w.plan
	if otherRels > 0 {
		return notYet("a write to reference table \""+w.target.name+"\" cannot read sharded or unsharded tables",
			"the statement runs on every shard; a sharded or unsharded table is present on one shard only")
	}
	if pick := unorderedPick(w.root); pick != "" {
		return notYet("a write to reference table \""+w.target.name+"\" cannot use "+pick+": each shard would choose its own rows",
			"select the rows the write should use with a fully ordered, deterministic condition")
	}
	// An unqualified name is only the built-in while pg_catalog is searched
	// first, which it is by default. A session that puts its own schema
	// ahead of it can shadow any of them, so the proof no longer holds.
	// pg_catalog is searched first unless the session explicitly puts it
	// later, and only then can another schema shadow a built-in name.
	for i, schema := range w.sess.SearchPath {
		if !strings.EqualFold(schema, "pg_catalog") || i == 0 {
			continue
		}
		return notYet("a write to reference table \""+w.target.name+"\" cannot run with "+w.sess.SearchPath[0]+" searched before pg_catalog: an unqualified function name could resolve to that schema instead of the built-in",
			"reset search_path, or qualify the reference write's functions with pg_catalog")
	}
	// The statement is only half of it: a default, a generated expression,
	// an identity column, a trigger or a rule never appears in the parse
	// tree and is evaluated by each shard for itself, so an INSERT naming
	// no such column still writes a different row on every shard. Only the
	// shards can answer that, and the controller publishes what they said.
	if w.target.refDeclared && !w.target.refChecked {
		return notYet("a write to reference table \""+w.target.name+"\" cannot be planned until its shards have been inspected",
			"the controller records what a reference table evaluates per shard; retry once it has run")
	}
	if len(w.target.refHazards) > 0 {
		return notYet("a write to reference table \""+w.target.name+"\" would not write the same row on every shard: "+strings.Join(w.target.refHazards, "; "),
			"a reference table is replicated to every shard, so every value it stores must be fixed by the statement")
	}
	if fn := nonImmutableCall(w.root); fn != "" {
		return notYet("a write to reference table \""+w.target.name+"\" cannot call "+fn+"(): the statement runs on every shard and only a function proven to return the same answer everywhere may take part",
			"compute the value in the client and pass it as a literal or parameter")
	}
	p.Kind, p.Shards = Reference, w.allShards()
	if len(p.Shards) == 0 {
		p.Shards = []int32{w.sess.HomeShard}
	}
	return nil
}

// scatter refuses or, for a plain single-table read, produces a Scatter plan.
func (w *walker) scatter(write bool, rels int) error {
	if write {
		return notYet("scatter "+w.stmt+" without a shard key predicate is not available yet",
			"add WHERE <shard key> = ... or IN (...); this will fan out once multi-shard writes land")
	}
	if rels > 1 {
		return notYet("cross-shard join is not available yet",
			"join sharded tables on equal shard keys and filter on one key value")
	}
	p := w.plan
	w.mergeSpec()
	if p.mergeErr != nil {
		return p.mergeErr
	}
	p.Kind = Scatter
	p.Shards = w.allShards()
	return nil
}

// mergeSpec records how a single-table read merges across shards, or why
// it cannot.
func (w *walker) mergeSpec() {
	p := w.plan
	if w.outer == nil || len(w.rels) != 1 || w.rels[0].kind != placeSharded {
		p.mergeErr = notYet("cross-shard join is not available yet", "join sharded tables on equal shard keys and filter on one key value")
		return
	}
	p.merge, p.mergeErr = buildMerge(w.tree, w.outer, w.rels[0].shardKey, w.scatterBlockers)
}

func (w *walker) allShards() []int32 {
	var out []int32
	if w.sess.Snapshot != nil {
		for _, r := range w.sess.Snapshot.ShardSets[w.sess.shardSet()] {
			out = appendUnique(out, r.ShardID)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// referenceShard spreads reference reads across the shard set by session.
func (w *walker) referenceShard() int32 {
	all := w.allShards()
	if len(all) == 0 {
		return w.sess.HomeShard
	}
	return all[w.sess.ID%uint64(len(all))]
}

func (w *walker) insert(s *pgquerypb.InsertStmt) error {
	if err := w.with(s.GetWithClause()); err != nil {
		return err
	}
	r, err := w.lookup(s.GetRelation())
	if err != nil {
		return err
	}
	if r == nil {
		return notYet("INSERT into a CTE is not supported", "")
	}
	w.add(r)
	w.target = r
	sel := s.GetSelectStmt().GetSelectStmt()
	if r.kind == placeSharded {
		if len(s.GetCols()) == 0 {
			return notYet("insert requires the shard key: column \""+r.shardKey+"\" of \""+r.name+"\" is not in the column list",
				"list the columns explicitly and include \""+r.shardKey+"\"")
		}
		if sel == nil || len(sel.GetValuesLists()) == 0 {
			return notYet("INSERT ... SELECT into a sharded table is not available yet", "insert with VALUES so each row's shard key is visible")
		}
		fill, injected, err := w.rewriteInsert(s, r)
		if err != nil {
			return err
		}
		w.plan.Sequences = fill
		keyIdx := -1
		for i, c := range s.GetCols() {
			if c.GetResTarget().GetName() == r.shardKey {
				keyIdx = i
			}
		}
		keyParams := injected[r.shardKey]
		if keyIdx < 0 && len(keyParams) == 0 {
			return notYet("insert requires the shard key: column \""+r.shardKey+"\" of \""+r.name+"\" is not in the column list",
				"list the columns explicitly and include \""+r.shardKey+"\"")
		}
		for i, row := range sel.GetValuesLists() {
			if param, ok := keyParams[i]; ok {
				r.terms = append(r.terms, keyTerm{params: []ParamRef{{Number: param, Hint: HintInt}}})
				continue
			}
			if keyIdx < 0 {
				return notYet("insert requires the shard key: VALUES row has fewer values than columns", "")
			}
			items := row.GetList().GetItems()
			if keyIdx >= len(items) {
				return notYet("insert requires the shard key: VALUES row has fewer values than columns", "")
			}
			item, ok, err := constOrParam(items[keyIdx])
			if err != nil {
				return err
			}
			if !ok {
				return notYet("shard key of an INSERT must be a constant or a parameter",
					"expressions, DEFAULT and NULL cannot be routed")
			}
			var t keyTerm
			if item.param != 0 {
				t.params = []ParamRef{{Number: item.param, Hint: item.hint}}
			} else {
				t.values = []any{item.value}
			}
			r.terms = append(r.terms, t)
		}
		w.plan.multiRow = len(sel.GetValuesLists()) > 1
		if oc := s.GetOnConflictClause(); oc != nil {
			for _, t := range oc.GetTargetList() {
				if t.GetResTarget().GetName() == r.shardKey {
					return notYet("shard key is immutable: ON CONFLICT DO UPDATE cannot set \""+r.shardKey+"\"", "")
				}
			}
		}
	} else if sel != nil && len(sel.GetValuesLists()) == 0 {
		if err := w.nestedStatement(s.GetSelectStmt()); err != nil {
			return err
		}
	}
	if sel != nil {
		for _, row := range sel.GetValuesLists() {
			if err := w.expr(row); err != nil {
				return err
			}
		}
	}
	return w.decide(true)
}

func (w *walker) update(s *pgquerypb.UpdateStmt) error {
	if err := w.with(s.GetWithClause()); err != nil {
		return err
	}
	r, err := w.lookup(s.GetRelation())
	if err != nil {
		return err
	}
	if r == nil {
		return notYet("UPDATE of a CTE is not supported", "")
	}
	w.add(r)
	w.target = r
	if r.kind == placeSharded {
		for _, t := range s.GetTargetList() {
			if t.GetResTarget().GetName() == r.shardKey {
				return notYet("shard key is immutable: UPDATE cannot set \""+r.shardKey+"\" of \""+r.name+"\"",
					"delete the row and insert it with the new key")
			}
		}
	}
	scope := len(w.rels) - 1
	for _, item := range s.GetFromClause() {
		w.blocker("joins")
		if err := w.fromItem(item); err != nil {
			return err
		}
	}
	if err := w.where(s.GetWhereClause(), w.rels[scope:]); err != nil {
		return err
	}
	if err := w.exprs(s.GetTargetList()); err != nil {
		return err
	}
	return w.decide(true)
}

func (w *walker) delete(s *pgquerypb.DeleteStmt) error {
	if err := w.with(s.GetWithClause()); err != nil {
		return err
	}
	r, err := w.lookup(s.GetRelation())
	if err != nil {
		return err
	}
	if r == nil {
		return notYet("DELETE from a CTE is not supported", "")
	}
	w.add(r)
	w.target = r
	scope := len(w.rels) - 1
	for _, item := range s.GetUsingClause() {
		w.blocker("joins")
		if err := w.fromItem(item); err != nil {
			return err
		}
	}
	if err := w.where(s.GetWhereClause(), w.rels[scope:]); err != nil {
		return err
	}
	return w.decide(true)
}
