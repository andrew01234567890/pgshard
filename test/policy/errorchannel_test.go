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

// TestNoRPCReturnsAnErrorInAnOKResponse enforces the rule PGS-393 settled:
// a whole-RPC failure travels as a gRPC status with the pgshardv1.Error
// attached as a detail, and an Error inside a response body is only for a
// per-item or partial outcome.
//
// The rule was arrived at four times before it was written down. The agent
// returned failed promotions with an OK status; the controller's
// CreateBarrier did the same for execution failures; the pooler's Reserve
// did it for the same three refusals its own Ack already reported as a
// status; and VStream.Create and Drop forwarded the controller's embedded
// error. Each was found by reading, one at a time, and the consequence is
// the same every time: a mutation that did not happen counts as OK to every
// interceptor, retry policy and metric that reads only the status, and only
// a caller that also reads the body can tell.
//
// So this looks for the shape rather than trusting the next reviewer to
// remember: a return of a composite literal that sets an Error field,
// paired with a nil error.
//
// It is deliberately narrow. It does not judge which code an RPC returns,
// and it says nothing about a response that carries per-item errors
// alongside a nil error -- those are the legitimate case, and they set
// their errors on a nested element rather than on the response itself.
func TestNoRPCReturnsAnErrorInAnOKResponse(t *testing.T) {
	root := filepath.Join("..", "..")
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
		ast.Inspect(file, func(n ast.Node) bool {
			ret, ok := n.(*ast.ReturnStmt)
			if !ok || len(ret.Results) != 2 || !isNilIdent(ret.Results[1]) {
				return true
			}
			if !setsErrorField(ret.Results[0]) {
				return true
			}
			pos := fset.Position(ret.Pos())
			rel, _ := filepath.Rel(root, pos.Filename)
			found = append(found, rel+":"+strconv.Itoa(pos.Line))
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 0 {
		t.Fatalf("these return a response carrying an Error with a nil status, so a failure that did not happen looks like a successful RPC:\n  %s\n"+
			"Return a gRPC status instead, with the Error attached as a detail; see internal/pooler/stream.go refusalStatus.",
			strings.Join(found, "\n  "))
	}
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
