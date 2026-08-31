package controller

import (
	"testing"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// TestEveryWorkflowKindHasAProtoName: a kind the controller creates but the
// map omits reports as UNSPECIFIED over the API and cannot be filtered for.
// An upgrade ran normally and was invisible to anyone asking the controller
// what was running, which is the shape of failure a map like this has: the
// work happens, only the reporting is wrong, so nothing fails loudly.
func TestEveryWorkflowKindHasAProtoName(t *testing.T) {
	for _, kind := range []string{KindReshard, KindUpgrade, KindTablePlacement} {
		got, ok := kindToProto[kind]
		if !ok {
			t.Errorf("catalog kind %q has no proto name", kind)
			continue
		}
		if got == pgshardv1.WorkflowKind_WORKFLOW_KIND_UNSPECIFIED {
			t.Errorf("catalog kind %q maps to UNSPECIFIED", kind)
		}
	}

	// And the mapping is one-to-one: two kinds sharing a name would make
	// a filter return the wrong workflows rather than none.
	seen := map[pgshardv1.WorkflowKind]string{}
	for kind, proto := range kindToProto {
		if other, dup := seen[proto]; dup {
			t.Errorf("kinds %q and %q both map to %v", other, kind, proto)
		}
		seen[proto] = kind
	}

	// lookupKey is what turns an API filter back into a catalog kind, so a
	// caller can actually ask for upgrades.
	if k, ok := lookupKey(kindToProto, pgshardv1.WorkflowKind_WORKFLOW_KIND_UPGRADE); !ok || k != KindUpgrade {
		t.Errorf("filtering by UPGRADE resolves to %q ok=%v, want %q", k, ok, KindUpgrade)
	}
}
