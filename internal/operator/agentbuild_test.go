package operator

import (
	"context"
	"testing"
)

// TestThePrimarysBuildReachesStatus: during a rolling upgrade the members
// of one cluster run different binaries for a while, and nothing said
// which. The agent now reports its build and the primary's reaches the
// cluster's status, so a roll is visible in the object rather than only in
// the images an operator goes looking for.
func TestThePrimarysBuildReachesStatus(t *testing.T) {
	r := &ClusterReconciler{}
	g := Group{Cluster: "c", Kind: "shard", ShardID: 0, Replicas: 2}
	obs := groupObservation{
		group:        g,
		primaryBuild: "v1.2.3 (abc1234, 2026-09-01)",
		state:        groupState{primary: g.MemberName(0), pvcs: map[string]string{}},
	}
	got := r.finishGroup(context.Background(), nil, g, obs, map[string]*memberInfo{})
	if len(got.members) != 2 {
		t.Fatalf("members %+v", got.members)
	}
	if got.members[0].Build != obs.primaryBuild {
		t.Fatalf("primary build %q, want %q", got.members[0].Build, obs.primaryBuild)
	}
	// Only the primary: the operator makes one Status call a pass, and
	// claiming a build for members it never asked would be an invention.
	if got.members[1].Build != "" {
		t.Fatalf("standby build %q, want empty: it was never asked", got.members[1].Build)
	}
}

// TestAnOlderAgentSaysNothing: the field is absent from an agent that
// predates it, and an empty build is the honest answer rather than a
// guess.
func TestAnOlderAgentSaysNothing(t *testing.T) {
	r := &ClusterReconciler{}
	g := Group{Cluster: "c", Kind: "shard", ShardID: 0, Replicas: 1}
	obs := groupObservation{group: g, state: groupState{primary: g.MemberName(0), pvcs: map[string]string{}}}
	got := r.finishGroup(context.Background(), nil, g, obs, map[string]*memberInfo{})
	if got.members[0].Build != "" {
		t.Fatalf("build %q, want empty", got.members[0].Build)
	}
}
