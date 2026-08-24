package plan

import (
	"context"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
)

// TestTruncateRefusedWhileResharding: a provisioning shard set (the copy
// phase streams row changes only) turns every TRUNCATE into a refusal,
// including the one on the unsharded table that is otherwise allowed.
func TestTruncateRefusedWhileResharding(t *testing.T) {
	snap := fixture(t)
	p := New()
	if _, err := p.Plan(context.Background(), session(snap), "truncate items"); err != nil {
		t.Fatalf("truncate before reshard: %v", err)
	}
	snap.Serving = map[snapshot.ShardKey]snapshot.Serving{{ShardSet: "g2", ShardID: 0}: {State: "provisioning"}}
	if !snap.Resharding() {
		t.Fatal("snapshot must report the reshard")
	}
	for _, sql := range []string{"truncate items", "truncate orders", "truncate items, regions"} {
		pl, err := p.Plan(context.Background(), session(snap), sql)
		checkRefusal(t, pl, err, "TRUNCATE is not available while a reshard is active")
	}
	if _, err := p.Plan(context.Background(), session(snap), "delete from items"); err != nil {
		t.Fatalf("delete during reshard: %v", err)
	}
	snap.Serving[snapshot.ShardKey{ShardSet: "g2", ShardID: 0}] = snapshot.Serving{State: "serving"}
	if snap.Resharding() {
		t.Fatal("a serving set is not a reshard")
	}
}
