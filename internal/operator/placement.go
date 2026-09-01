package operator

import (
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

// applyPlacement puts a cluster's placement rules on a pod spec. match is
// the label set whose pods should be kept apart -- one group's members, or
// the routers -- which is also what the spread rules count.
func applyPlacement(spec *corev1.PodSpec, p pgshardv1alpha1.PlacementSpec, match map[string]string) {
	spec.NodeSelector = p.NodeSelector
	spec.Tolerations = p.Tolerations
	if p.Affinity != nil {
		// Set deliberately, so it replaces rather than merges: a generated
		// rule silently ANDed with an explicit one is how a pod ends up
		// unschedulable for a reason nobody wrote down.
		spec.Affinity = p.Affinity
		return
	}
	selector := &metav1.LabelSelector{MatchLabels: match}
	if anti := nodeAntiAffinity(p.SpreadNodes, selector); anti != nil {
		spec.Affinity = &corev1.Affinity{PodAntiAffinity: anti}
	}
	if zones := zoneSpread(p, selector); zones != nil {
		spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{*zones}
	}
}

// hostnameKey is the node label every Kubernetes node carries; zones are
// optional, hostnames are not.
const hostnameKey = "kubernetes.io/hostname"

func nodeAntiAffinity(mode string, selector *metav1.LabelSelector) *corev1.PodAntiAffinity {
	term := corev1.PodAffinityTerm{LabelSelector: selector, TopologyKey: hostnameKey}
	switch mode {
	case "required":
		return &corev1.PodAntiAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{term}}
	case "preferred":
		// One term, so the weight is only ever compared against itself.
		return &corev1.PodAntiAffinity{PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{Weight: 100, PodAffinityTerm: term}}}
	}
	return nil
}

func zoneSpread(p pgshardv1alpha1.PlacementSpec, selector *metav1.LabelSelector) *corev1.TopologySpreadConstraint {
	key := p.ZoneKey
	if key == "" {
		key = "topology.kubernetes.io/zone"
	}
	c := corev1.TopologySpreadConstraint{MaxSkew: 1, TopologyKey: key, LabelSelector: selector}
	switch p.SpreadZones {
	case "required":
		c.WhenUnsatisfiable = corev1.DoNotSchedule
	case "preferred":
		// Nodes with no zone label count as one domain, so on a cluster
		// without zones this constraint is satisfied by anything. That is
		// the reason it schedules anyway rather than refusing.
		c.WhenUnsatisfiable = corev1.ScheduleAnyway
	default:
		return nil
	}
	return &c
}

// sharedNodes reports the nodes holding more than one member of the same
// group, with how many. Empty means every member of every group landed on
// a node of its own.
//
// This is what a preferred spread owes the operator. The scheduler is
// allowed to co-locate when it must, and without this the cluster would go
// on reporting three replicas while a single machine held all three.
func sharedNodes(nodes []string) map[string]int {
	count := map[string]int{}
	for _, n := range nodes {
		count[n]++
	}
	for n, c := range count {
		if c < 2 {
			delete(count, n)
		}
	}
	if len(count) == 0 {
		return nil
	}
	return count
}

// topologyMessage describes a group whose members share nodes, or "" when
// they do not.
func topologyMessage(group string, nodes []string) string {
	shared := sharedNodes(nodes)
	if shared == nil {
		return ""
	}
	names := make([]string, 0, len(shared))
	for n := range shared {
		names = append(names, n)
	}
	slices.Sort(names)
	parts := make([]string, 0, len(names))
	for _, n := range names {
		parts = append(parts, fmt.Sprintf("%d members on %s", shared[n], n))
	}
	return group + ": " + strings.Join(parts, ", ")
}
