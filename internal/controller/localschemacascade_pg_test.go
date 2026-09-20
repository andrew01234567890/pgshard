package controller

import (
	"context"
	"strings"
	"testing"
)

// TestALocalSchemaIsNotDroppedOutFromUnderWhatDependsOnIt (PGS-970): the
// cleanup that removes a database's local schemas from a target dropped them
// with CASCADE, and what depends on a schema's contents need not be in the
// schema. A sharded table's CHECK calling a function there went with it, on
// that target alone -- so the shard accepted rows every other shard rejects,
// and the cleanup reported success.
func TestALocalSchemaIsNotDroppedOutFromUnderWhatDependsOnIt(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	conn := f.app(0)
	mustExec(t, conn, `CREATE SCHEMA helper`)
	mustExec(t, conn, `CREATE FUNCTION helper.positive(int) RETURNS bool LANGUAGE sql IMMUTABLE AS 'SELECT $1 > 0'`)
	mustExec(t, conn, `CREATE TABLE amounts (id bigint PRIMARY KEY, amount int CHECK (helper.positive(amount)))`)

	// What the cleanup is asked to do, on a target that is not home.
	err := (&Copier{Shards: f.placer.Shards}).dropLocalSchemas(ctx, "default", 0, dbPlan{name: "app", localSchemas: []string{"helper"}})
	if err == nil {
		t.Fatal("the cleanup dropped a schema a sharded table's CHECK depends on; that shard now accepts rows the others reject")
	}
	if !strings.Contains(err.Error(), "amounts") {
		t.Errorf("the refusal does not name what depends on the schema: %v", err)
	}

	// The constraint is still there, and still refuses.
	if _, err := conn.Exec(ctx, `INSERT INTO amounts (id, amount) VALUES (1, -5)`); err == nil {
		t.Error("the CHECK is gone: a negative amount was accepted")
	}

	// And a local schema nothing outside it points at is still removed.
	mustExec(t, conn, `CREATE SCHEMA tool`)
	mustExec(t, conn, `CREATE FUNCTION tool.noop() RETURNS void LANGUAGE sql AS 'SELECT'`)
	if err := (&Copier{Shards: f.placer.Shards}).dropLocalSchemas(ctx, "default", 0, dbPlan{name: "app", localSchemas: []string{"tool"}}); err != nil {
		t.Fatalf("a local schema nothing depends on must still be removed: %v", err)
	}
	if queryOne[bool](t, conn, `SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'tool')`) {
		t.Error("the schema nothing depends on was not removed")
	}
}
