package plan

import (
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/placement"
)

const fixtureDB = "app"

// fixture is four shards, an unsharded table, a reference table and two
// sharded tables (int8 key tenant_id, text key slug).
func fixture(t testing.TB) *snapshot.Snapshot {
	t.Helper()
	ranges, err := placement.Split(4)
	if err != nil {
		t.Fatal(err)
	}
	s := &snapshot.Snapshot{
		ShardMapGeneration: 11,
		ShardSets:          map[string][]snapshot.Range{},
		Databases:          map[string]catalog.Database{fixtureDB: {Name: fixtureDB, DefaultPlacement: "unsharded", HomeShard: 0}},
		Tables:             map[snapshot.TableKey]snapshot.Placement{},
	}
	for i, r := range ranges {
		s.ShardSets[DefaultShardSet] = append(s.ShardSets[DefaultShardSet], snapshot.Range{ShardID: int32(i), Start: r.Start, End: r.End})
	}
	tbl := func(name, placement, key string) {
		s.Tables[snapshot.TableKey{Database: fixtureDB, SchemaName: "public", TableName: name}] = snapshot.Placement{Placement: placement, ShardKey: key, Generation: 3, ReferenceChecked: placement == "reference"}
	}
	tbl("items", "unsharded", "")
	tbl("regions", "reference", "")
	tbl("orders", "sharded", "tenant_id")
	tbl("order_lines", "sharded", "tenant_id")
	tbl("docs", "sharded", "slug")
	s.Tables[snapshot.TableKey{Database: fixtureDB, SchemaName: "public", TableName: "tickets"}] = snapshot.Placement{Placement: "sharded", ShardKey: "tenant_id", SequenceColumns: []string{"id"}}
	s.Tables[snapshot.TableKey{Database: fixtureDB, SchemaName: "public", TableName: "events"}] = snapshot.Placement{Placement: "sharded", ShardKey: "event_id", SequenceColumns: []string{"event_id"}}
	// A registered global sequence, named the way the catalog names one:
	// database.schema.table.column. The physical sequence behind it is
	// PostgreSQL's tickets_id_seq, which is what an ALTER SEQUENCE would
	// name.
	s.Sequences = map[string]bool{"invoice_numbers": true, fixtureDB + ".public.tickets.id": true}
	s.Tables[snapshot.TableKey{Database: fixtureDB, SchemaName: "audit", TableName: "events"}] = snapshot.Placement{Placement: "sharded", ShardKey: "tenant_id"}
	return s
}

func shardOf(t testing.TB, snap *snapshot.Snapshot, v any) int32 {
	t.Helper()
	id, err := placement.KeyspaceID(v)
	if err != nil {
		t.Fatal(err)
	}
	sh, err := snap.Locate(DefaultShardSet, id)
	if err != nil {
		t.Fatal(err)
	}
	return sh
}

func session(snap *snapshot.Snapshot) Session {
	return Session{Database: fixtureDB, HomeShard: 0, Snapshot: snap}
}
