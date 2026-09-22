package schemacopy

import (
	"slices"
	"testing"
)

// TestASchemaCopyLeavesTheOwnedRangeBehind (PGS-878): each shard records the
// keyspace range it owns in OwnerSchema. A reshard target materialises its
// schema from a source, and one that copied the source's range would refuse
// every row it is meant to hold.
func TestASchemaCopyLeavesTheOwnedRangeBehind(t *testing.T) {
	if !slices.Contains(DumpArgs("dbname=x"), "--exclude-schema="+OwnerSchema) {
		t.Fatalf("DumpArgs %v copies the source's owned range", DumpArgs("dbname=x"))
	}
}
