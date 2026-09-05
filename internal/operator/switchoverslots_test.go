package operator

import (
	"context"
	"errors"
	"strings"
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

// A planned switchover may refuse over slots where an emergency failover
// only prefers: the failover has to promote someone, the switchover can
// wait. Losing a logical slot the primary holds costs a reshard's reverse
// replication or a stream's resumable position, which is not a price for
// avoiding a wait.
func TestSlotsLostBySwitchover(t *testing.T) {
	g := Group{Cluster: "c", Kind: "shard", Replicas: 3}
	members := map[string]*memberInfo{
		"c-shard-0-0": {name: "c-shard-0-0", ip: "10.0.0.10"},
		"c-shard-0-1": {name: "c-shard-0-1", ip: "10.0.0.11"},
	}
	for _, tc := range []struct {
		name    string
		primary LogicalSlots
		target  LogicalSlots
		fail    bool
		want    string
	}{
		// The common cluster: no reshard, no stream, nothing to lose.
		{name: "no logical slots at all", want: ""},
		{name: "target holds them all",
			primary: LogicalSlots{All: []string{"reshard_1", "stream_a"}},
			target:  LogicalSlots{All: []string{"reshard_1", "stream_a"}, Ready: []string{"stream_a", "reshard_1"}}},
		{name: "one not synchronised yet",
			primary: LogicalSlots{All: []string{"reshard_1", "stream_a"}},
			target:  LogicalSlots{All: []string{"reshard_1", "stream_a"}, Ready: []string{"reshard_1"}},
			want:    "stream_a"},
		// Present but not ready is the case a count cannot see: the slot
		// exists on the target and would not work after promotion.
		{name: "present but temporary or invalidated",
			primary: LogicalSlots{All: []string{"reshard_1"}},
			target:  LogicalSlots{All: []string{"reshard_1"}},
			want:    "reshard_1"},
		// A member with the same NUMBER of ready slots, but not the same
		// ones. Counting would call this equal.
		{name: "same count, different slots",
			primary: LogicalSlots{All: []string{"reshard_1"}},
			target:  LogicalSlots{All: []string{"other"}, Ready: []string{"other"}},
			want:    "reshard_1"},
		{name: "target did not answer",
			primary: LogicalSlots{All: []string{"reshard_1"}},
			fail:    true,
			want:    "reshard_1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fa := &fakeAgents{status: map[string]AgentStatus{}, errs: map[string]error{}}
			fa.setSlots(agentAddr("10.0.0.10"), tc.primary)
			if tc.fail {
				fa.set("10.0.0.11", AgentStatus{}, errors.New("connection refused"))
			} else {
				fa.setSlots(agentAddr("10.0.0.11"), tc.target)
			}
			r := &ClusterReconciler{Agents: fa}
			got := strings.Join(r.slotsLostBySwitchover(context.Background(), g, "c-shard-0-0", "c-shard-0-1", members), ",")
			if got != tc.want {
				t.Fatalf("missing %q, want %q", got, tc.want)
			}
		})
	}
}

// The decision point itself: a switchover that would lose a slot holds,
// and holds without asking for the switchover it refused.
func TestAPlannedSwitchoverHoldsRatherThanLoseASlot(t *testing.T) {
	r, fp, fa, c := healthyCluster(t, "swslot")
	g := Groups(c)[1]
	primary, target := g.MemberName(0), g.MemberName(1)
	fa.setSlots(agentAddr(podIP(1, 0)), LogicalSlots{All: []string{"reshard_1"}})
	fa.setSlots(agentAddr(podIP(1, 1)), LogicalSlots{})
	fa.setSlots(agentAddr(podIP(1, 2)), LogicalSlots{})
	fp.standbys[podIP(1, 1)] = StandbyState{InRecovery: true, FlushLSN: 900}
	fp.standbys[podIP(1, 2)] = StandbyState{InRecovery: true, FlushLSN: 800}

	obs := &groupObservation{group: g, state: groupState{primary: primary,
		syncSet: map[string]bool{target: true, g.MemberName(2): true}}}
	members := map[string]*memberInfo{}
	for i := range 3 {
		members[g.MemberName(i)] = &memberInfo{name: g.MemberName(i), ip: podIP(1, i)}
	}
	if err := r.stepAwayFromPrimary(context.Background(), c, g, obs, members, "pw"); err != nil {
		t.Fatal(err)
	}
	if obs.rollout == nil || obs.rollout.Phase != pgshardv1alpha1.RolloutPhaseHeld ||
		!strings.Contains(obs.rollout.Reason, "reshard_1") {
		t.Fatalf("rollout %+v", obs.rollout)
	}
	get(t, "swslot", c)
	if v := c.Annotations[AnnotationSwitchover]; v != "" {
		t.Fatalf("a refused switchover must not be requested anyway: %q", v)
	}

	// Once the slot is synchronised there, it proceeds.
	fa.setSlots(agentAddr(podIP(1, 1)), LogicalSlots{All: []string{"reshard_1"}, Ready: []string{"reshard_1"}})
	obs.rollout = nil
	if err := r.stepAwayFromPrimary(context.Background(), c, g, obs, members, "pw"); err != nil {
		t.Fatal(err)
	}
	if obs.rollout == nil || obs.rollout.Phase != pgshardv1alpha1.RolloutPhaseSwitchover {
		t.Fatalf("rollout %+v", obs.rollout)
	}
	get(t, "swslot", c)
	if c.Annotations[AnnotationSwitchover] != target {
		t.Fatalf("switchover annotation %q, want %q", c.Annotations[AnnotationSwitchover], target)
	}
}
