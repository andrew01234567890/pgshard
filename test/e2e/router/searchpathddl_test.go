//go:build integration

package router

import (
	"context"
	"testing"
)

// TestDDLRunsUnderTheClientsSearchPath.
//
// A migration used to carry only the role it runs as. The applier set
// lock_timeout and SET ROLE and nothing else, so the statement was resolved
// under the DDL role's default path rather than the client's -- and an
// unqualified CREATE TABLE from a client that had set search_path landed in
// public, with no error and no sign that anything was wrong. The client then
// could not find its own table.
func TestDDLRunsUnderTheClientsSearchPath(t *testing.T) {
	s := startStack(t)
	ctx := context.Background()
	conn := s.connect(t)
	for _, sql := range []string{
		`CREATE SCHEMA app2`,
		`SET search_path TO app2`,
		`CREATE TABLE widgets (id int primary key)`,
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	var schema string
	if err := conn.QueryRow(ctx, `SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'widgets'`).Scan(&schema); err != nil {
		t.Fatalf("the table the client created is nowhere: %v", err)
	}
	if schema != "app2" {
		t.Fatalf("the table landed in %q, not the schema the client's search_path named", schema)
	}

	// And the client can use it by the name it created it under, which is
	// the part a user notices.
	if _, err := conn.Exec(ctx, `INSERT INTO widgets (id) VALUES (1)`); err != nil {
		t.Fatalf("the client cannot use the table it just created: %v", err)
	}
}
