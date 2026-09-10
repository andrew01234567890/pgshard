package router

import "testing"

// TestSearchPathIsQuotedTheWayPostgreSQLQuotesIt.
//
// set_config stores the string VERBATIM -- check_search_path validates the
// syntax and nothing canonicalises it -- so quoting every element made
// current_setting('search_path') report `"public_02_x"` where a direct
// PostgreSQL connection reports `public_02_x`. Anything that compares that
// string sees a different value through pgshard than off it; pgroll's
// dual-write triggers compare it exactly to decide which column a write
// belongs to, so the quotes sent writes to the wrong column.
//
// The expectations here are PostgreSQL's own output, checked against a live
// server: SET search_path = "public_02_x" reports public_02_x, and the
// default path reports "$user", public.
func TestSearchPathIsQuotedTheWayPostgreSQLQuotesIt(t *testing.T) {
	for _, c := range []struct {
		path []string
		want string
	}{
		{[]string{"public"}, "public"},
		{[]string{"public_02_x"}, "public_02_x"},
		{[]string{"public_02_add_column", "public"}, "public_02_add_column, public"},
		{[]string{"$user", "public"}, `"$user", public`},
		// PostgreSQL quotes this even though $ is legal in an identifier:
		// quote_identifier accepts only [a-z_][a-z0-9_]*. Checked live.
		{[]string{"a$b_1"}, `"a$b_1"`},
		// Not bare identifiers, so PostgreSQL quotes them too.
		{[]string{"Weird Name"}, `"Weird Name"`},
		{[]string{"MixedCase"}, `"MixedCase"`},
		{[]string{"1leading"}, `"1leading"`},
		{[]string{"has\"quote"}, `"has""quote"`},
		{[]string{"select"}, `"select"`},
		{[]string{""}, `""`},
	} {
		got := searchPathSQL(c.path)
		want := "SELECT set_config('search_path', '" + replaceQuotes(c.want) + "', false)"
		if got != want {
			t.Errorf("path %q\n got %s\nwant %s", c.path, got, want)
		}
	}
}

func replaceQuotes(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			out = append(out, '\'', '\'')
			continue
		}
		out = append(out, s[i])
	}
	return string(out)
}
