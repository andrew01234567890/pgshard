package router

import (
	"context"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// TestAStatementOfAnotherShardIsNotReplayedHere (PGS-882): replayStatements
// parsed every named statement on whatever backend the session acquired.
// PARSE resolves names, so a statement whose objects live on one shard --
// anything over a database's local_schemas, which live on the home shard
// alone -- failed there, and the failure took the whole replay with it: the
// session could then run nothing at all. Measured with pgroll, where a
// cached SELECT pgroll.latest_version() broke every later statement once the
// session moved shard.
func TestAStatementOfAnotherShardIsNotReplayedHere(t *testing.T) {
	h := newShardedHarness(t)
	ctx := context.Background()
	keys := &pgwire.SCRAMKeys{ClientKey: make([]byte, 32), ServerKey: make([]byte, 32)}
	e := newExecutor(h.r, pgwire.SessionInfo{ID: 1, Database: "app", User: "app", Auth: &pgwire.AuthResult{SCRAM: keys}}, Shard{Set: DefaultShardSet, ID: 0})

	a, b := h.twoTenants(t)
	shardA, shardB := h.shardOf(t, a), h.shardOf(t, b)
	if shardA == shardB {
		t.Fatal("the two tenants must be on different shards, or this test proves nothing")
	}
	keyed, err := e.plan(ctx, "select id from orders where tenant_id = "+itoa64(a))
	if err != nil {
		t.Fatal(err)
	}
	scatter, err := e.plan(ctx, "select id from orders")
	if err != nil {
		t.Fatal(err)
	}

	e.shard = Shard{Set: DefaultShardSet, ID: int32(shardA)}
	if !e.replayableHere(keyed) {
		t.Error("a statement of this shard must be replayed here")
	}
	e.shard = Shard{Set: DefaultShardSet, ID: int32(shardB)}
	if e.replayableHere(keyed) {
		t.Error("a statement of another shard must not be parsed here: its objects need not exist")
	}
	if !e.replayableHere(scatter) {
		t.Error("a statement that runs on every shard must be replayed here")
	}
}
