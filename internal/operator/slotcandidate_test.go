package operator

import (
	"context"
	"errors"
	"testing"
)

// TestSlotsBreakATieAndNeverBeatData: a reshard's subscription, the
// reverse replication that makes a cutover reversible and a change
// stream's resumable position each live in a logical slot on the primary.
// Between two members holding the same data, promoting the one that keeps
// more of them costs less. Between two holding different data it is not a
// question: the one with more acknowledged commits wins, whatever it
// costs in workflows, because the alternative is losing a commit somebody
// was told was durable.
func TestSlotsBreakATieAndNeverBeatData(t *testing.T) {
	for _, tc := range []struct {
		name    string
		members []memberView
		want    string
	}{
		{
			name: "same LSN, more slots wins",
			members: []memberView{
				{Name: "a", Reachable: true, InRecovery: true, FlushLSN: 100, ReadySlots: 0},
				{Name: "b", Reachable: true, InRecovery: true, FlushLSN: 100, ReadySlots: 3},
			},
			want: "b",
		},
		{
			name: "fewer slots but more data still wins",
			members: []memberView{
				{Name: "a", Reachable: true, InRecovery: true, FlushLSN: 200, ReadySlots: 0},
				{Name: "b", Reachable: true, InRecovery: true, FlushLSN: 100, ReadySlots: 9},
			},
			want: "a",
		},
		{
			name: "same LSN and same slots still breaks by name",
			members: []memberView{
				{Name: "b", Reachable: true, InRecovery: true, FlushLSN: 100, ReadySlots: 2},
				{Name: "a", Reachable: true, InRecovery: true, FlushLSN: 100, ReadySlots: 2},
			},
			want: "a",
		},
		{
			name: "a member that could not report its slots is not excluded",
			members: []memberView{
				{Name: "a", Reachable: true, InRecovery: true, FlushLSN: 300, ReadySlots: 0},
				{Name: "b", Reachable: true, InRecovery: true, FlushLSN: 100, ReadySlots: 5},
			},
			want: "a",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := chooseCandidate(tc.members, "", "", 1)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("chose %q, want %q", got, tc.want)
			}
		})
	}
}

// TestAnExplicitTargetStillOutranksSlotCount: a switchover names its
// target, and a slot count is not a reason to promote something else.
func TestAnExplicitTargetStillOutranksSlotCount(t *testing.T) {
	members := []memberView{
		{Name: "a", Reachable: true, InRecovery: true, FlushLSN: 100, ReadySlots: 0},
		{Name: "b", Reachable: true, InRecovery: true, FlushLSN: 100, ReadySlots: 4},
	}
	got, err := chooseCandidate(members, "", "a", 1)
	if err != nil {
		t.Fatal(err)
	}
	if got != "a" {
		t.Fatalf("chose %q, want the preferred member %q", got, "a")
	}
}

// TestAFailoverPrefersTheStandbyKeepingTheSlots wires the tie-break
// through a real failover: two standbys flushed to the same LSN, one
// holding the slots a reshard and a stream depend on.
func TestAFailoverPrefersTheStandbyKeepingTheSlots(t *testing.T) {
	r, fp, fa, c := healthyCluster(t, "slotfo")

	deletePod(t, "slotfo-shard-0-0")
	fa.set(podIP(1, 0), AgentStatus{}, errors.New("connection refused"))
	// Identical data: only the slots separate them.
	fp.standbys[podIP(1, 1)] = StandbyState{InRecovery: true, FlushLSN: 500}
	fp.standbys[podIP(1, 2)] = StandbyState{InRecovery: true, FlushLSN: 500}
	fa.setReady(agentAddr(podIP(1, 1)), 0)
	fa.setReady(agentAddr(podIP(1, 2)), 3)

	reconcile(t, r, c)
	want := agentAddr(podIP(1, 2)) + ":1:slotfo-shard-0-2"
	if len(fa.promotes) != 1 || fa.promotes[0] != want {
		t.Fatalf("promoted %v, want the standby holding the slots (%s)", fa.promotes, want)
	}
}

// TestSlotCountingSurvivesAReconcilerWithNoAgents: a rollout decision can
// be taken from probes alone, with no agent client configured. Asking one
// that is not there for a slot count is a panic in the failover path,
// which is the one path that must not have any.
func TestSlotCountingSurvivesAReconcilerWithNoAgents(t *testing.T) {
	r := &ClusterReconciler{}
	if n := r.readySlots(context.Background(), Group{Cluster: "c", Kind: "shard", Replicas: 3}, "c-shard-0-1", "10.0.0.1"); n != 0 {
		t.Fatalf("counted %d slots with no agent client", n)
	}
}
