package plan

import (
	"context"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
)

// A sharded table becomes effective as soon as the reconciler sees it
// declared (controller/reconcile.go writes effective_generation from
// desired_generation), while the verdict on its key column arrives on the
// shard-key check's own later pass. In between the router knows the table
// is sharded and does not know what its key column is.
//
// Hashing an unnormalised value there would put the INSERT on one shard and
// every later lookup of the same key on another, silently. But normaliseKey
// only ever changes a string -- character(n) trimmed, over-length character
// varying(n) truncated -- so only a string key is at risk, and only that is
// refused. Refusing every key would take a table with an integer key out of
// service for a window that cannot hurt it.
func TestAnUncheckedTextShardKeyIsNotRoutable(t *testing.T) {
	s := fixture(t)
	key := snapshot.TableKey{Database: fixtureDB, SchemaName: "public", TableName: "codes"}
	s.Tables[key] = snapshot.Placement{Placement: "sharded", ShardKey: "code", Generation: 3}

	for _, sql := range []string{
		"select * from codes where code = 'abc'",
		"insert into codes (code) values ('abc')",
		"update codes set label = 'x' where code = 'abc'",
		"delete from codes where code = 'abc'",
	} {
		t.Run(sql, func(t *testing.T) {
			// Reads too: a read of an unnormalised key goes to the shard
			// the row is not on, which reports no rows rather than an
			// error.
			_, err := New().Plan(context.Background(), session(s), sql)
			if err == nil {
				t.Fatal("planned a table whose shard key has not been checked")
			}
			if !strings.Contains(err.Error(), "has not been checked") {
				t.Fatalf("refusal does not say why: %v", err)
			}
		})
	}

	// The refusal is of the window, not of the table: the same table with a
	// verdict recorded plans normally.
	checked := s.Tables[key]
	checked.ShardKeyChecked, checked.ShardKeyType = true, "text"
	s.Tables[key] = checked
	if _, err := New().Plan(context.Background(), session(s), "select * from codes where code = 'abc'"); err != nil {
		t.Fatalf("a checked key must plan: %v", err)
	}
}

// The other half of the rule, and the reason it is worth being narrow: an
// integer key hashes as it came whatever the column turns out to be, so a
// table keyed by one stays in service while its verdict is outstanding.
func TestAnUncheckedIntegerShardKeyStillRoutes(t *testing.T) {
	s := fixture(t)
	key := snapshot.TableKey{Database: fixtureDB, SchemaName: "public", TableName: "tickets"}
	pl := s.Tables[key]
	pl.ShardKeyChecked, pl.ShardKeyType = false, ""
	s.Tables[key] = pl
	if _, err := New().Plan(context.Background(), session(s), "select * from tickets where tenant_id = 7"); err != nil {
		t.Fatalf("an integer key must still route while unchecked: %v", err)
	}
}
