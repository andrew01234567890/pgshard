package policy

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// TestOnlyClaimHoldersFinishTheirPass keeps one rule visible: a controller
// loop that takes a workflow CLAIM must use runLoopHoldingClaims, and every
// other loop must use runLoopStoppable.
//
// Nothing releases a claim -- claimWorkflow admits a successor only once
// owned_at is DefaultOwnerLease old, and no site sets owner back to NULL.
// So abandoning a claim-holding pass when the term drops leaves the claim,
// and any write pause it raised, standing for five minutes; letting it
// finish clears it inside the cutover's own timeout. The opposite mistake
// is quieter: a loop that holds no claim and uses runLoop simply goes on
// writing beside the new leader, which is what this rule exists to stop.
//
// Both directions are checked, because the file that gets this wrong is
// the one somebody adds later.
func TestOnlyClaimHoldersFinishTheirPass(t *testing.T) {
	dir := filepath.Join("..", "..", "internal", "controller")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var finishes, stops []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") || e.Name() == "loop.go" {
			continue
		}
		path := filepath.Join(dir, e.Name())
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		fset := token.NewFileSet()
		file, perr := parser.ParseFile(fset, path, src, 0)
		if perr != nil {
			continue // not ours to police; the build catches it
		}
		var usesRunLoop, usesStoppable bool
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if id, ok := call.Fun.(*ast.Ident); ok {
				switch id.Name {
				case "runLoopHoldingClaims":
					usesRunLoop = true
				case "runLoopStoppable":
					usesStoppable = true
				}
			}
			return true
		})
		claims := strings.Contains(string(src), "claimWorkflow(")
		switch {
		case usesRunLoop && !claims:
			finishes = append(finishes, e.Name())
		case usesStoppable && claims:
			stops = append(stops, e.Name())
		}
	}
	slices.Sort(finishes)
	slices.Sort(stops)
	if len(finishes) > 0 {
		t.Errorf("these hold no workflow claim but use runLoopHoldingClaims, so a demoted leader keeps working beside the new one: %s\n"+
			"Use runLoopStoppable, or say here why this loop must finish.", strings.Join(finishes, ", "))
	}
	if len(stops) > 0 {
		t.Errorf("these take a workflow claim but use runLoopStoppable: %s\n"+
			"Nothing releases a claim, so an abandoned pass leaves it -- and any write pause it raised -- standing for DefaultOwnerLease.", strings.Join(stops, ", "))
	}
}
