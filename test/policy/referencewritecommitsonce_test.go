package policy

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// TestAReferenceWriteCommitsThroughTheSharedTransactionPath.
//
// A reference write touches every shard, which makes it the most expensive
// write in the system and the most tempting thing to make cheaper. The
// cheap version is to commit each shard as it finishes, and it is exactly
// what pgshard's nearest competitor does -- PlanetScale's Neki documents
// that its reference-table writes "commit independently, so a commit can
// succeed on some shards and fail on another".
//
// pgshard commits them through endTxn, which raises two-phase commit as
// soon as a second writable shard joins, so the copies of a reference table
// cannot diverge (ADR 16). Diverged copies are silent: a reference table
// exists so that a join can be answered locally on any shard, so copies
// that disagree make the same join return different answers depending on
// where it ran, and nothing reports it.
//
// That property lives in one call, and a later edit can replace it with a
// per-shard COMMIT without any test going red -- every reference-write test
// would still pass, because they assert the rows, and the rows are the same
// right up until a shard fails. So the call is asserted here.
func TestAReferenceWriteCommitsThroughTheSharedTransactionPath(t *testing.T) {
	const file = "../../internal/router/reference.go"
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	fn := findFunc(f, "referenceWrite")
	if fn == nil {
		t.Fatalf("%s no longer declares referenceWrite; if the reference-write path moved, move this test with it", file)
	}
	commits, literals := false, []string{}
	ast.Inspect(fn, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "endTxn" {
			commits = true
		}
		if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
			if s := strings.ToUpper(strings.Trim(lit.Value, `"`)); s == "COMMIT" || strings.HasPrefix(s, "COMMIT ") {
				literals = append(literals, lit.Value)
			}
		}
		return true
	})
	if !commits {
		t.Error("referenceWrite no longer commits through endTxn: a reference write that commits each shard on its own can leave the copies disagreeing, which nothing reports (ADR 16)")
	}
	if len(literals) > 0 {
		t.Errorf("referenceWrite issues %v directly; the commit belongs to endTxn, which raises two-phase commit once a second writable shard joins", literals)
	}
}

// findFunc returns the declaration of name, method or function.
func findFunc(f *ast.File, name string) *ast.FuncDecl {
	for _, d := range f.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok && fn.Name.Name == name {
			return fn
		}
	}
	return nil
}
