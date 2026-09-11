package policy

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// TestTheAgentServesItsProbesBeforeItBuildsTheDataDirectory.
//
// The kubelet's startup probe is a budget, not a question: it fails until
// something answers and then the container is killed. The agent used to
// build the data directory first and start its HTTP listener afterwards, so
// a clone longer than that budget failed the probe for its whole duration
// and was killed -- and the restart cleared the directory and began again,
// for ever, on any shard large enough.
//
// The fix is an ORDER, and an order is exactly the kind of thing a later
// edit undoes without noticing: the probe handlers keep working either way,
// the tests for them keep passing, and nothing is visibly wrong until a
// cluster is big enough for a clone to outrun ten minutes. So the order is
// asserted here rather than left to be rediscovered.
func TestTheAgentServesItsProbesBeforeItBuildsTheDataDirectory(t *testing.T) {
	const file = "../../internal/agent/run.go"
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var serve, bootstrap token.Pos
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		x, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		switch {
		case x.Name == "httpSrv" && sel.Sel.Name == "Serve":
			serve = call.Pos()
		case x.Name == "inst" && sel.Sel.Name == "Bootstrap":
			bootstrap = call.Pos()
		}
		return true
	})
	if !serve.IsValid() {
		t.Fatal("no httpSrv.Serve in run.go: the probe listener has been renamed or removed, and this rule can no longer see it")
	}
	if !bootstrap.IsValid() {
		t.Fatal("no inst.Bootstrap in run.go: this rule can no longer see when the data directory is built")
	}
	if serve > bootstrap {
		t.Fatalf("run.go serves the probes at %s, after Bootstrap at %s: a clone longer than the startup probe's budget will be killed and started again from an empty data directory",
			fset.Position(serve), fset.Position(bootstrap))
	}
}
