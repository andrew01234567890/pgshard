package router

import (
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// TestSearchPathArrivesAsAStartupParameterToo.
//
// A client may send search_path two ways: inside "options" as
// -c search_path=..., or as a startup PARAMETER of its own, which the
// protocol allows for any user-settable GUC and which Go drivers send for
// connection keys they do not recognise. pgroll connects the second way.
//
// Reading only "options" did not merely lose it for planning: nothing
// forwards a bare startup parameter to the backend either, so the setting
// was dropped entirely and the session ran under the default path. For a
// sharded table that means the home shard's rows rather than an error.
func TestSearchPathArrivesAsAStartupParameterToo(t *testing.T) {
	for _, c := range []struct {
		name   string
		params map[string]string
		want   []string
	}{
		{"parameter of its own", map[string]string{"search_path": "public_02_x, public"}, []string{"public_02_x", "public"}},
		{"options still works", map[string]string{"options": "-c search_path=audit,public"}, []string{"audit", "public"}},
		{"neither", map[string]string{}, nil},
		// The unambiguous form wins: a client sending both is asking for
		// the more specific one.
		{"parameter beats options", map[string]string{
			"search_path": "chosen",
			"options":     "-c search_path=ignored",
		}, []string{"chosen"}},
		// The parameter is split and case-folded exactly like the option,
		// or the planner would look up a name the backend cannot find.
		{"unquoted is downcased", map[string]string{"search_path": "MySchema"}, []string{"myschema"}},
		{"quoted keeps its case", map[string]string{"search_path": `"MySchema"`}, []string{"MySchema"}},
		// An empty parameter is not a path; fall back rather than pinning
		// the session to nothing.
		{"empty parameter falls back", map[string]string{
			"search_path": "",
			"options":     "-c search_path=audit",
		}, []string{"audit"}},
	} {
		got := startupPath(c.params)
		if strings.Join(got, ",") != strings.Join(c.want, ",") {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

// TestTheExecutorReadsTheStartupParameterPath: the parsing above is only
// useful if the session actually consults it. Asserting the helper alone
// cannot catch the caller being wired to the old one -- which is exactly
// what a mutation of the call site showed.
func TestTheExecutorReadsTheStartupParameterPath(t *testing.T) {
	h := newShardedHarness(t)
	shard := Shard{Set: DefaultShardSet, ID: 0}
	e := newExecutor(h.r, pgwire.SessionInfo{ID: 1, Database: "app", User: "app",
		Auth:   &pgwire.AuthResult{SCRAM: &pgwire.SCRAMKeys{}},
		Params: map[string]string{"search_path": "public_02_x, public"},
	}, shard)
	if got := strings.Join(e.startupSearchPath, ","); got != "public_02_x,public" {
		t.Fatalf("the session ignored a search_path sent as a startup parameter: %q", got)
	}
}
