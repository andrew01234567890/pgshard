package policy

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// deliberate names the returns that embed an error ON PURPOSE, with the
// reason. The rule has a legitimate other half -- a gRPC status for a
// whole-RPC failure, an embedded error only for a PER-ITEM or PARTIAL
// outcome -- and these are that half.
//
// Listed rather than pattern-matched, so each one is a decision somebody
// made and a new embedding has to be argued for here rather than merely
// written.
//
// Keyed by the enclosing function, not by line. Line numbers were the point
// once -- a line that moved forced a second look at whether the reason still
// held -- but in practice they move when something ELSE in the file gains a
// few lines, and the second look has nothing to look at. Twice in one day
// that failed make test on a rule the change never touched, which teaches
// people to update the number without reading the reason: the opposite of
// what the list is for. A function that is renamed, or a return that moves
// to a different function, still trips it, and those are the changes where
// the reason really does need re-reading.
//
// The suffix on a repeat is its order within the function.
var deliberate = map[string]string{
	// The agent's Status is a REPORT. A role it cannot read is reported as
	// an error rather than as primary, because the operator promotes and
	// fences on this answer -- the RPC succeeded and the report says the
	// role is unknown.
	"internal/agent/server.go (*Server).Status":   "Status reports a role it could not read",
	"internal/agent/server.go (*Server).Status#2": "Status could not connect; the report says so",
	"internal/agent/server.go (*Server).Status#3": "Status could not read the LSN; the report says so",

	// pgBackRest's own output is the only diagnostic a failed backup has,
	// and the backup reconciler writes it into the group's status. One
	// channel per RPC, so the failures with nothing to carry go the same
	// way as the one that has.
	"internal/agent/backupops.go (*Server).Backup":   "Backup: fenced at the wrong epoch, same channel as the failure that carries a log",
	"internal/agent/backupops.go (*Server).Backup#2": "Backup: unknown type, same channel",
	"internal/agent/backupops.go (*Server).Backup#3": "Backup failed and its log is the diagnostic",
	"internal/agent/backupops.go (*Server).Verify":   "Verify could not start",
	"internal/agent/backupops.go (*Server).Verify#2": "Verify ran and found a problem: the finding is the result",

	// Partial outcomes, which is exactly what the embedded channel is for.
	"internal/controller/server.go (*Server).ResolveTransactions": "ResolveTransactions returns counts AND what stopped it",
	"internal/controller/streamadmin.go (*Server).CreateStream":   "CreateStream returns the slots it made AND the error that stopped it",
	"internal/router/vstream/server.go (*Server).Create":          "Create forwards those partial slots; losing them loses WAL-retaining slots nobody knows to drop",
}

// TestNoRPCReturnsAnErrorInAnOKResponse enforces the rule PGS-393 settled:
// a whole-RPC failure travels as a gRPC status with the pgshardv1.Error
// attached as a detail, and an Error inside a response body is only for a
// per-item or partial outcome.
//
// The rule was arrived at four times before it was written down -- the
// agent's promotions, CreateBarrier's execution failures, Pooler.Reserve's
// three refusals, and VStream.Create and Drop -- each found by reading, one
// at a time, months apart. Each had the same consequence: a mutation that
// did not happen counts as OK to every interceptor, retry policy and metric
// that reads only the status.
//
// It looks for a block that both sets an Error on a value and returns that
// value with a nil error. Same block on purpose: a function-final
// "return resp, nil" is the SUCCESS path even when an earlier branch set an
// error, and flagging it would make the rule noise. Both shapes count --
// the value built with the Error in place, and the value assigned into --
// because the defect has both and the first version of this test saw only
// the literal at the return, so the agent's "resp.Error = ...; return resp,
// nil" was invisible to a rule this file claims to enforce.
func TestNoRPCReturnsAnErrorInAnOKResponse(t *testing.T) {
	root := filepath.Join("..", "..")
	seen := map[string]bool{}
	var found []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "bin", "gen":
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return nil // not ours to police; the build catches it
		}
		rel, _ := filepath.Rel(root, path)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			// Counted per function so a second embedding in the same one
			// can be listed separately; the order is source order, which
			// ast.Inspect walks deterministically.
			n := 0
			lines := map[token.Pos]bool{}
			ast.Inspect(fn, func(node ast.Node) bool {
				var list []ast.Stmt
				switch b := node.(type) {
				case *ast.BlockStmt:
					list = b.List
				case *ast.CaseClause:
					// A case body is a statement list without being a
					// block, and the agent's Status embeds inside one.
					list = b.Body
				default:
					return true
				}
				for _, pos := range embeddedErrorReturns(list) {
					if lines[pos] {
						continue
					}
					lines[pos] = true
					n++
					at := rel + " " + funcName(fn)
					if n > 1 {
						at += "#" + strconv.Itoa(n)
					}
					if seen[at] {
						continue
					}
					seen[at] = true
					if _, ok := deliberate[at]; !ok {
						found = append(found, at+" (line "+strconv.Itoa(fset.Position(pos).Line)+")")
					}
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 0 {
		t.Fatalf("these return a response carrying an Error with a nil status, so a failure that did not happen looks like a successful RPC:\n  %s\n"+
			"Return a gRPC status instead, with the Error attached as a detail (see internal/pooler/stream.go refusalStatus). "+
			"If the outcome really is partial -- some items succeeded and the caller needs both -- add it to the deliberate list above with the reason.",
			strings.Join(found, "\n  "))
	}
	// The list is documentation, so it must not rot: an entry naming a line
	// that no longer embeds anything is a reason nobody is reading.
	for at := range deliberate {
		if !seen[at] {
			t.Errorf("deliberate lists %s, which no longer returns an embedded error: remove it", at)
		}
	}
}

// funcName is the receiver-qualified name, so two methods of different
// types in one file do not collide.
func funcName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	var b strings.Builder
	b.WriteString("(")
	switch t := fn.Recv.List[0].Type.(type) {
	case *ast.StarExpr:
		b.WriteString("*")
		if id, ok := t.X.(*ast.Ident); ok {
			b.WriteString(id.Name)
		}
	case *ast.Ident:
		b.WriteString(t.Name)
	}
	b.WriteString(").")
	b.WriteString(fn.Name.Name)
	return b.String()
}

// embeddedErrorReturns finds, within ONE statement list, the returns of
// (value, nil) whose value got its Error in that same list.
func embeddedErrorReturns(list []ast.Stmt) []token.Pos {
	carries := map[string]bool{}
	var out []token.Pos
	for _, stmt := range list {
		switch s := stmt.(type) {
		case *ast.AssignStmt:
			for i, lhs := range s.Lhs {
				// x.Error = ...
				if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "Error" {
					if id, ok := sel.X.(*ast.Ident); ok {
						carries[id.Name] = true
					}
					continue
				}
				// x := &T{Error: ...}
				if id, ok := lhs.(*ast.Ident); ok && i < len(s.Rhs) && setsErrorField(s.Rhs[i]) {
					carries[id.Name] = true
				}
			}
		case *ast.IfStmt:
			// A branch that sets the Error and FALLS THROUGH leaves it on
			// the value the shared return below hands back, which is how
			// the controller's partial outcomes are written. A branch that
			// sets it and RETURNS cannot reach that return, so it must not
			// mark the variable -- otherwise every success return in a
			// function with an early error path is flagged.
			for _, id := range fallthroughErrorSetters(s) {
				carries[id] = true
			}
		case *ast.ReturnStmt:
			if len(s.Results) != 2 || !isNilIdent(s.Results[1]) {
				continue
			}
			if id, ok := s.Results[0].(*ast.Ident); ok {
				if carries[id.Name] {
					out = append(out, s.Pos())
				}
				continue
			}
			if setsErrorField(s.Results[0]) {
				out = append(out, s.Pos())
			}
		}
	}
	return out
}

// fallthroughErrorSetters names the variables an if statement sets an Error
// on in a branch that does not return.
func fallthroughErrorSetters(s *ast.IfStmt) []string {
	var out []string
	branch := func(list []ast.Stmt) {
		// Whether the branch returns is decided BEFORE anything is
		// collected. Deciding it while walking recorded the assignment and
		// only then met the return, so every early-error branch marked its
		// variable and the function's SUCCESS return was flagged.
		for _, st := range list {
			if _, isReturn := st.(*ast.ReturnStmt); isReturn {
				return // this branch cannot reach the enclosing return
			}
		}
		for _, st := range list {
			as, ok := st.(*ast.AssignStmt)
			if !ok {
				continue
			}
			for _, lhs := range as.Lhs {
				sel, ok := lhs.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Error" {
					continue
				}
				if id, ok := sel.X.(*ast.Ident); ok {
					out = append(out, id.Name)
				}
			}
		}
	}
	if s.Body != nil {
		branch(s.Body.List)
	}
	if els, ok := s.Else.(*ast.BlockStmt); ok {
		branch(els.List)
	}
	return out
}

// setsErrorField reports whether e is a composite literal (or a pointer to
// one) that sets a field named Error.
func setsErrorField(e ast.Expr) bool {
	if u, ok := e.(*ast.UnaryExpr); ok {
		e = u.X
	}
	lit, ok := e.(*ast.CompositeLit)
	if !ok {
		return false
	}
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		if id, ok := kv.Key.(*ast.Ident); ok && id.Name == "Error" {
			return true
		}
	}
	return false
}

func isNilIdent(e ast.Expr) bool {
	id, ok := e.(*ast.Ident)
	return ok && id.Name == "nil"
}
