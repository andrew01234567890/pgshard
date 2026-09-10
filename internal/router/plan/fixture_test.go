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
		s.Tables[snapshot.TableKey{Database: fixtureDB, SchemaName: "public", TableName: name}] = snapshot.Placement{Placement: placement, ShardKey: key, Generation: 3, ReferenceChecked: placement == "reference", ShardKeyChecked: placement == "sharded"}
	}
	tbl("items", "unsharded", "")
	tbl("regions", "reference", "")
	tbl("orders", "sharded", "tenant_id")
	tbl("order_lines", "sharded", "tenant_id")
	tbl("docs", "sharded", "slug")
	s.Tables[snapshot.TableKey{Database: fixtureDB, SchemaName: "public", TableName: "tickets"}] = snapshot.Placement{Placement: "sharded", ShardKey: "tenant_id", ShardKeyChecked: true, SequenceColumns: []string{"id"}}
	s.Tables[snapshot.TableKey{Database: fixtureDB, SchemaName: "public", TableName: "events"}] = snapshot.Placement{Placement: "sharded", ShardKey: "event_id", ShardKeyChecked: true, SequenceColumns: []string{"event_id"}}
	// A registered global sequence, named the way the catalog names one:
	// database.schema.table.column. The physical sequence behind it is
	// PostgreSQL's tickets_id_seq, which is what an ALTER SEQUENCE would
	// name.
	// Views, as a versioned-schema migration produces them: a projection of
	// one table, with the shard key exposed under ANOTHER NAME. Routing has
	// to see through the alias or every read of the view scatters.
	s.Views = map[snapshot.TableKey]snapshot.View{
		{Database: fixtureDB, SchemaName: "public", TableName: "orders_v2"}: {
			Base:    snapshot.TableKey{Database: fixtureDB, SchemaName: "public", TableName: "orders"},
			Simple:  true,
			Columns: map[string]string{"tenant": "tenant_id", "sku": "sku"},
		},
		// A view whose query pgshard cannot map. It is recorded so the
		// router knows it is a VIEW and refuses it, rather than taking it
		// for a table it has never heard of and falling back to a
		// placement.
		{Database: fixtureDB, SchemaName: "public", TableName: "orders_agg"}: {},
		// A view over an unsharded table: routed to the home shard by the
		// base table's placement, like the table itself.
		{Database: fixtureDB, SchemaName: "public", TableName: "items_v"}: {
			Base:    snapshot.TableKey{Database: fixtureDB, SchemaName: "public", TableName: "items"},
			Simple:  true,
			Columns: map[string]string{"id": "id", "name": "name"},
		},
	}
	s.Sequences = map[string]bool{"invoice_numbers": true, fixtureDB + ".public.tickets.id": true}
	s.Tables[snapshot.TableKey{Database: fixtureDB, SchemaName: "audit", TableName: "events"}] = snapshot.Placement{Placement: "sharded", ShardKey: "tenant_id", ShardKeyChecked: true}
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
