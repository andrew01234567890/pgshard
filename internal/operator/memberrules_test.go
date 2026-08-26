package operator

import (
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

// TestMemberRulesCoverReshardTargets guards the RBAC a reshard target needs:
// its agent holds a primary Lease from the moment it starts, long before its
// generation serves, so a name-scoped rule listing only the serving groups
// leaves it unable to renew and the pod crash-looping with "primary cannot
// start without the lease".
func TestMemberRulesCoverReshardTargets(t *testing.T) {
	c := &pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{Name: "rs"}}
	c.Status.EffectiveShards = 1
	c.Status.Reshard = &pgshardv1alpha1.ClusterReshardStatus{Name: "rs-reshard-g2", ShardSet: "g2", Generation: 2, Shards: 2}

	var named []string
	for _, rule := range MemberRules(c) {
		if slices.Contains(rule.Verbs, "update") {
			named = append(named, rule.ResourceNames...)
		}
	}

	for _, g := range TargetGroups(c) {
		if !slices.Contains(named, g.LeaseName()) {
			t.Errorf("target %s cannot renew its primary lease %s; named leases are %v", g.Name(), g.LeaseName(), named)
		}
	}
	for _, g := range Groups(c) {
		if !slices.Contains(named, g.LeaseName()) {
			t.Errorf("serving group %s lost its lease permission: %v", g.Name(), named)
		}
	}
	if len(named) != len(slices.Compact(slices.Clone(named))) {
		t.Errorf("lease names must not repeat: %v", named)
	}
}
