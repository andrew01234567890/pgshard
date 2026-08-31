package operator

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

func placementOf(t *testing.T, p pgshardv1alpha1.PlacementSpec) corev1.PodSpec {
	t.Helper()
	var spec corev1.PodSpec
	applyPlacement(&spec, p, map[string]string{LabelCluster: "c", LabelGroup: "c-shard-0"})
	return spec
}

// TestMembersAreKeptOffOneAnothersNodes: replica counts promise nothing on
// their own. Without a rule, a primary and both of its synchronous
// standbys may share one node, and losing that node loses every failover
// candidate a three-replica cluster was supposed to have.
func TestMembersAreKeptOffOneAnothersNodes(t *testing.T) {
	spec := placementOf(t, pgshardv1alpha1.PlacementSpec{SpreadNodes: "preferred", SpreadZones: "preferred"})
	anti := spec.Affinity.PodAntiAffinity
	if anti == nil || len(anti.PreferredDuringSchedulingIgnoredDuringExecution) != 1 {
		t.Fatalf("preferred spread must produce one preferred term: %+v", spec.Affinity)
	}
	if len(anti.RequiredDuringSchedulingIgnoredDuringExecution) != 0 {
		t.Fatal("preferred must not schedule-block")
	}
	if got := anti.PreferredDuringSchedulingIgnoredDuringExecution[0].PodAffinityTerm.TopologyKey; got != hostnameKey {
		t.Fatalf("topology key %q", got)
	}
	// Zones spread evenly but never block: nodes without a zone label all
	// count as one domain, so refusing there would refuse everywhere.
	if len(spec.TopologySpreadConstraints) != 1 {
		t.Fatalf("constraints %+v", spec.TopologySpreadConstraints)
	}
	if c := spec.TopologySpreadConstraints[0]; c.MaxSkew != 1 || c.WhenUnsatisfiable != corev1.ScheduleAnyway {
		t.Fatalf("zone spread %+v", c)
	}

	required := placementOf(t, pgshardv1alpha1.PlacementSpec{SpreadNodes: "required", SpreadZones: "required"})
	if n := len(required.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution); n != 1 {
		t.Fatalf("required spread must schedule-block: %d terms", n)
	}
	if got := required.TopologySpreadConstraints[0].WhenUnsatisfiable; got != corev1.DoNotSchedule {
		t.Fatalf("required zone spread = %v", got)
	}

	none := placementOf(t, pgshardv1alpha1.PlacementSpec{SpreadNodes: "none", SpreadZones: "none"})
	if none.Affinity != nil || none.TopologySpreadConstraints != nil {
		t.Fatalf("none must generate nothing: %+v", none)
	}
}

// TestAnExplicitAffinityReplacesTheGeneratedOne: a generated rule silently
// ANDed with an operator's own is how a pod becomes unschedulable for a
// reason nobody wrote down.
func TestAnExplicitAffinityReplacesTheGeneratedOne(t *testing.T) {
	own := &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{}}
	spec := placementOf(t, pgshardv1alpha1.PlacementSpec{SpreadNodes: "required", Affinity: own})
	if spec.Affinity != own {
		t.Fatalf("affinity %+v, want the one that was set", spec.Affinity)
	}
	if spec.TopologySpreadConstraints != nil {
		t.Fatal("an explicit affinity turns the generated spreading off entirely")
	}
}

// TestNodeSelectorAndTolerationsReachThePod: the plainest half, and the
// one an operator reaches for first on a tainted or heterogeneous cluster.
func TestNodeSelectorAndTolerationsReachThePod(t *testing.T) {
	tol := []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "pgshard"}}
	spec := placementOf(t, pgshardv1alpha1.PlacementSpec{
		NodeSelector: map[string]string{"disk": "nvme"}, Tolerations: tol})
	if spec.NodeSelector["disk"] != "nvme" || len(spec.Tolerations) != 1 {
		t.Fatalf("selector %v tolerations %v", spec.NodeSelector, spec.Tolerations)
	}
}

// TestACrowdedGroupIsReported: preferred spreading lets the scheduler
// co-locate when it must, so the cluster has to say when it did. Silence
// would leave the replica count implying a resilience the cluster has not
// got -- the same illusion, one step later.
func TestACrowdedGroupIsReported(t *testing.T) {
	if got := topologyMessage("c-shard-0", []string{"n1", "n2", "n3"}); got != "" {
		t.Fatalf("three members on three nodes is not degraded: %q", got)
	}
	if got := topologyMessage("c-shard-0", nil); got != "" {
		t.Fatalf("no observed members is not degraded: %q", got)
	}
	got := topologyMessage("c-shard-0", []string{"n1", "n1", "n2"})
	if got != "c-shard-0: 2 members on n1" {
		t.Fatalf("message %q", got)
	}
	// Sorted, so the message does not change between passes that learned
	// the same thing.
	if got := topologyMessage("g", []string{"b", "b", "a", "a"}); got != "g: 2 members on a, 2 members on b" {
		t.Fatalf("message %q", got)
	}
}
