package catalog

import (
	"errors"
	"testing"
)

// TestAPrivilegeIsAWordNotAStatement: privileges are rendered into a GRANT
// by concatenation, and the row they come from can be written directly by
// an administrator who is deliberately not a superuser on the shards. So
// anything that is not a privilege of the object kind is refused before it
// reaches a statement.
func TestAPrivilegeIsAWordNotAStatement(t *testing.T) {
	for _, ok := range []struct {
		kind, column string
		privs        []string
	}{
		{"table", "", []string{"SELECT", "insert", "ALL"}},
		{"table", "note", []string{"UPDATE", "REFERENCES"}},
		{"sequence", "", []string{"USAGE"}},
		{"database", "", []string{"CONNECT", "TEMP"}},
		{"function", "", []string{"EXECUTE"}},
		{"parameter", "", []string{"SET", "ALTER SYSTEM"}},
	} {
		if err := CheckPrivileges(ok.kind, ok.column, ok.privs); err != nil {
			t.Errorf("%s %v was refused: %v", ok.kind, ok.privs, err)
		}
	}

	for _, bad := range []struct {
		name, kind, column string
		privs              []string
	}{
		{"punctuation ends the statement", "table", "", []string{"SELECT, x FROM y --"}},
		{"a semicolon starts another", "table", "", []string{"SELECT; RESET ALL"}},
		{"quotes", "table", "", []string{`SELECT" TO "postgres`}},
		{"a privilege of another kind", "sequence", "", []string{"TRUNCATE"}},
		{"a column grant takes column privileges", "table", "note", []string{"TRUNCATE"}},
		{"not a privilege at all", "schema", "", []string{"SUPERUSER"}},
	} {
		if err := CheckPrivileges(bad.kind, bad.column, bad.privs); !errors.Is(err, ErrUnknownPrivilege) {
			t.Errorf("%s: %v on %s returned %v, want ErrUnknownPrivilege", bad.name, bad.privs, bad.kind, err)
		}
	}
}
