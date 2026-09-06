package plan

import (
	"context"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
)

// TestASharedTableInTheUsersOwnSchemaIsFound: PostgreSQL's default search
// path is "$user", public, and the router advertises exactly that at
// startup -- but the planner defaulted to public alone and never expanded
// "$user" even when a session set it explicitly. A declared sharded table in
// the schema named after the login role (the PostgreSQL convention: role
// app owns schema app) was therefore resolved by the backend and missed by
// the planner, which routed the statement as undeclared: SELECTs answered
// from the home shard only, INSERTs stored there whatever the key.
func TestASharedTableInTheUsersOwnSchemaIsFound(t *testing.T) {
	snap := fixture(t)
	snap.Tables[snapshot.TableKey{Database: fixtureDB, SchemaName: "app", TableName: "invoices"}] =
		snapshot.Placement{Placement: "sharded", ShardKey: "tenant_id", ShardKeyChecked: true, Generation: 3}

	cases := []struct {
		name    string
		user    string
		path    []string
		sharded bool
	}{
		{"default path, role owns its schema", "app", nil, true},
		{`explicit "$user", public`, "app", []string{UserSchema, "public"}, true},
		{"a role with no schema of its own falls through", "nobody", nil, false},
		{"no user to name a schema after", "", []string{UserSchema}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sess := session(snap)
			sess.User, sess.SearchPath = c.user, c.path
			pl, err := New().Plan(context.Background(), sess, "SELECT * FROM invoices WHERE tenant_id = 5")
			if err != nil {
				t.Fatal(err)
			}
			// A sharded table resolves to the one shard holding the key; an
			// unresolved one is treated as undeclared and goes to the home
			// shard whatever the key says.
			if got := pl.Kind == EqualUnique; got != c.sharded {
				t.Fatalf("kind %v (shards %v); sharded=%v, want %v", pl.Kind, pl.Shards, got, c.sharded)
			}
		})
	}
}
