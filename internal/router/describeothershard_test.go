package router

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// TestDescribingAStatementOfAnotherShardIsAnswered (PGS-967): the replay was
// narrowed so a statement whose plan resolves to another shard is not parsed
// on this backend -- its objects need not exist there. Parse, Bind and
// Execute all route to the statement's own shard, so they were unaffected;
// a Describe-only batch sets no target, so it stayed on whatever shard the
// session was on and answered 26000 for a statement the client had
// prepared and never closed.
func TestDescribingAStatementOfAnotherShardIsAnswered(t *testing.T) {
	h := newShardedHarness(t)
	s := newExtendedSession(t, h.dsn())
	a, b := h.twoTenants(t)
	if h.shardOf(t, a) == h.shardOf(t, b) {
		t.Fatal("the two tenants must be on different shards, or this test proves nothing")
	}

	if code, _, _ := s.send("prepare for one shard",
		&pgproto3.Parse{Name: "s", Query: "select * from orders where tenant_id = " + itoa64(a)},
		&pgproto3.Sync{}); code != "" {
		t.Fatalf("preparing: %s", code)
	}
	// A statement of the OTHER shard, which moves the session there.
	if code, _, _ := s.send("run on the other shard",
		&pgproto3.Parse{Query: "insert into orders (tenant_id, id) values (" + itoa64(b) + ", 1)"},
		&pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{}); code != "" {
		t.Fatalf("running on the other shard: %s", code)
	}

	code, _, _ := s.send("describe the prepared statement", &pgproto3.Describe{ObjectType: 'S', Name: "s"}, &pgproto3.Sync{})
	if code != "" {
		t.Fatalf("Describe of a statement prepared for another shard answered %s; the client prepared it and never closed it", code)
	}
	if want := []string{"ParameterDescription", "RowDescription", "ReadyForQuery"}; len(s.seen) != len(want) {
		t.Fatalf("Describe answered %v, want %v", s.seen, want)
	}
}
