package plan

import (
	"context"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/placement"
)

// uuidFixture adds a table sharded on a uuid column, which the controller's
// key inspection accepts (KeyHashExpr has a uuid case) and therefore
// publishes with ShardKeyType "uuid".
func uuidFixture(t *testing.T) *snapshot.Snapshot {
	t.Helper()
	s := fixture(t)
	s.Tables[snapshot.TableKey{Database: fixtureDB, SchemaName: "public", TableName: "events_by_uuid"}] =
		snapshot.Placement{Placement: "sharded", ShardKey: "id", ShardKeyType: "uuid", ShardKeyChecked: true, Generation: 3}
	return s
}

// TestAUUIDLiteralRoutesToTheUUIDHashShard.
//
// The controller moves a uuid-keyed row by uuid_hash_extended over its
// sixteen bytes. The router used to hash the 36 characters with
// hashtextextended, so it placed and found rows at a keyspace position the
// copy never sent them to -- after a reshard the row existed and the router
// looked on a different shard, with no error anywhere.
func TestAUUIDLiteralRoutesToTheUUIDHashShard(t *testing.T) {
	snap := uuidFixture(t)
	for _, u := range []string{
		"0189d0f4-4c1a-7c2e-9c1a-2f1b3c4d5e6f",
		"11111111-2222-3333-4444-555555555555",
		"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
		// The spellings PostgreSQL accepts have to route to the same place
		// as the canonical one: they are the same value.
		"0189D0F4-4C1A-7C2E-9C1A-2F1B3C4D5E6F",
		"{0189d0f4-4c1a-7c2e-9c1a-2f1b3c4d5e6f}",
		"0189d0f44c1a7c2e9c1a2f1b3c4d5e6f",
	} {
		b, ok := placement.ParseUUID(u)
		if !ok {
			t.Fatalf("%q did not parse", u)
		}
		id, err := placement.KeyspaceID(b)
		if err != nil {
			t.Fatal(err)
		}
		want := snap.ShardSets[DefaultShardSet]
		var wantShard int32 = -1
		for _, r := range want {
			if id >= r.Start && id < r.End {
				wantShard = r.ShardID
			}
		}
		for _, sql := range []string{
			"select * from events_by_uuid where id = '" + u + "'",
			"select * from events_by_uuid where id = '" + u + "'::uuid",
			"delete from events_by_uuid where id = '" + u + "'",
		} {
			p, err := New().Plan(context.Background(), session(snap), sql)
			if err != nil {
				t.Errorf("%s: %v", sql, err)
				continue
			}
			if len(p.Shards) != 1 || p.Shards[0] != wantShard {
				t.Errorf("%s: routed to %v, want the uuid_hash_extended shard %d", sql, p.Shards, wantShard)
			}
		}
	}
}

// A uuid parameter routes the same way, by either wire format. Before this
// the text format hashed the characters and the binary format was refused
// outright as an unsupported shard key.
func TestAUUIDParameterRoutesToTheUUIDHashShard(t *testing.T) {
	const u = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
	b, ok := placement.ParseUUID(u)
	if !ok {
		t.Fatal("fixture uuid did not parse")
	}
	for _, c := range []struct {
		name   string
		oid    uint32
		hint   TypeHint
		format int16
		raw    []byte
	}{
		{"declared uuid, text format", 2950, HintNone, 0, []byte(u)},
		{"declared uuid, binary format", 2950, HintNone, 1, b[:]},
		{"undeclared, cast to uuid in the statement", 0, HintUUID, 0, []byte(u)},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := DecodeShardKey(c.oid, c.hint, c.format, c.raw)
			if err != nil {
				t.Fatal(err)
			}
			if got != any(b) {
				t.Fatalf("decoded %#v, want the sixteen bytes %#v", got, b)
			}
		})
	}
}

// An unparseable uuid is left as it was rather than hashed from invented
// bytes: PostgreSQL rejects the statement for invalid syntax, so where it
// routed first does not matter, and inventing a value would be worse.
func TestAnInvalidUUIDIsNotInvented(t *testing.T) {
	if v := normaliseKey("not-a-uuid", "uuid"); v != any("not-a-uuid") {
		t.Fatalf("normaliseKey turned an invalid uuid into %#v", v)
	}
	if _, err := DecodeShardKey(2950, HintNone, 0, []byte("not-a-uuid")); err == nil {
		t.Fatal("an invalid uuid parameter must be an error, not a guess")
	}
}
