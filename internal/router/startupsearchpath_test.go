package router

import (
	"slices"
	"testing"
)

// TestAStartupSearchPathIsDowncasedTheWayTheBackendDoesIt.
//
// A startup option stores the search_path string RAW, and PostgreSQL splits
// it with SplitIdentifierString when it resolves a name -- which downcases
// every unquoted element. Keeping the case here made the planner look up
// "MySchema" while the backend searched myschema: the planner found nothing
// in the snapshot, fell through to the database default, and sent a sharded
// table's statement to the home shard. The table exists there too, so the
// client got the home shard's rows and no error at all.
//
// Measured on PostgreSQL 18, PGOPTIONS="-c search_path=MySchema":
//
//	current_setting('search_path') = MySchema
//	current_schemas(false)         = {myschema}
//
// A quoted element keeps its case, because SplitIdentifierString leaves a
// quoted identifier alone -- and that asymmetry is the whole rule.
func TestAStartupSearchPathIsDowncasedTheWayTheBackendDoesIt(t *testing.T) {
	for _, c := range []struct {
		options string
		want    []string
	}{
		{"-c search_path=MySchema", []string{"myschema"}},
		{"-c search_path=MixedCase,Public", []string{"mixedcase", "public"}},
		{"-csearch_path=AUDIT", []string{"audit"}},
		{"--search_path=Audit", []string{"audit"}},
		// Quoted stays exactly as written: this is how a client reaches a
		// schema that really is spelled with capitals.
		{`-c search_path="MySchema"`, []string{"MySchema"}},
		{`-c search_path="MySchema",other`, []string{"MySchema", "other"}},
		// Already lowercase, and unrelated options, are unaffected.
		{"-c search_path=audit,public", []string{"audit", "public"}},
		{"-c statement_timeout=5s", nil},
		{"", nil},
		// ONLY ASCII is folded, which is what PostgreSQL does under UTF-8:
		// downcase_identifier touches a high-bit byte in a single-byte
		// encoding alone. Measured with both schemas present --
		// PGOPTIONS="-c search_path=ÉCOLE" resolves current_schemas to
		// {École}, not {ÉCOLE} and not {école}.
		{"-c search_path=ÉCOLE", []string{"École"}},
	} {
		got := startupSearchPath(c.options)
		if !slices.Equal(got, c.want) {
			t.Errorf("startupSearchPath(%q) = %q, want %q", c.options, got, c.want)
		}
	}
}
