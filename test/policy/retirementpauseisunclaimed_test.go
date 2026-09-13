package policy

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// TestTheRetirementPauseIsNotClaimed.
//
// Completing a reshard makes the RETIRED set read-only for good. Its pods
// stay up for the retirement window and its -rw Service still answers, so a
// client connected straight to it -- not the supported path, but reachable
// -- would have writes acknowledged by a primary nothing reads from again,
// and lose them at deletion with no error anywhere.
//
// That pause belongs to nobody and is never lifted, so it must not be
// claimed in shard_status.write_paused_by. WritePauseSweep lifts a claimed
// pause whose workflow is gone or has finished, and Complete is the last
// thing every terminal path does: a claim there would be an orphan within
// seconds, and the sweep would hand the retired set back.
//
// The two calls differ by one word. Nothing else goes red if they are
// swapped -- the rows are identical, every cutover test still passes, and
// the damage is only visible to a client nobody is watching -- so the
// distinction is asserted here.
func TestTheRetirementPauseIsNotClaimed(t *testing.T) {
	const file = "../../internal/controller/cutoverpg.go"
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	fn := findFunc(f, "Complete")
	if fn == nil {
		t.Fatalf("%s no longer declares Complete; if the retirement path moved, move this test with it", file)
	}
	var claimed, plain bool
	ast.Inspect(fn, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch sel.Sel.Name {
		case "pauseSetClaimed":
			claimed = true
		case "pauseSet":
			plain = true
		}
		return true
	})
	if claimed {
		t.Error("Complete claims the retirement pause: the sweep lifts a claimed pause whose workflow has finished, and Complete runs on every terminal path, so the retired set would be handed back writable within seconds of retirement")
	}
	if !plain {
		t.Error("Complete no longer pauses the retired set: a retired primary that still accepts writes acknowledges them and loses them at deletion, with no error anywhere")
	}
}
