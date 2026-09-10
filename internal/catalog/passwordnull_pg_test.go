package catalog

import (
	"context"
	"testing"
)

// TestPasswordNullActuallyRemovesTheVerifier.
//
// The router terminates SCRAM against the verifier in pgshard.roles, so a
// verifier left behind is a password that still works. ALTER ROLE ...
// PASSWORD NULL emitted no catalog statement at all -- an empty verifier is
// what a statement with no PASSWORD clause produces too -- and even when one
// was emitted the upsert preserved the existing value. An administrator
// revoking a credential was told it had worked and it had not.
func TestPasswordNullActuallyRemovesTheVerifier(t *testing.T) {
	requireDocker(t)
	ctx := context.Background()
	conn := connect(t, startPostgres(t, candidateImages[0]))
	if err := Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	run := func(sts []Statement) {
		t.Helper()
		for _, s := range sts {
			if _, err := conn.Exec(ctx, s.SQL, s.Args...); err != nil {
				t.Fatalf("%s: %v", s.SQL, err)
			}
		}
	}
	verifier := func() *string {
		t.Helper()
		var v *string
		if err := conn.QueryRow(ctx, `SELECT verifier FROM pgshard.roles WHERE rolname = 'analyst'`).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}

	run(RoleMirrorStatements("app", MigrationMeta{Role: "analyst", RoleOp: "create", Verifier: "SCRAM-SHA-256$4096:c2FsdA==$c3RvcmVk:c2VydmVy"}))
	if v := verifier(); v == nil || *v == "" {
		t.Fatal("the premise of this test is a role that has a verifier")
	}

	// A statement that never mentioned PASSWORD must leave it alone.
	no := false
	run(RoleMirrorStatements("app", MigrationMeta{Role: "analyst", RoleOp: "alter",
		Roles: &RoleChanges{Attributes: &RoleAttributes{CreateDB: &no}}}))
	if v := verifier(); v == nil {
		t.Fatal("an ALTER that did not mention PASSWORD removed the verifier")
	}

	// PASSWORD NULL must remove it.
	run(RoleMirrorStatements("app", MigrationMeta{Role: "analyst", RoleOp: "alter", ClearVerifier: true}))
	if v := verifier(); v != nil {
		t.Fatalf("the revoked password is still usable: verifier is %q", *v)
	}
}
