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

// TestMemberRulesCoverRetiredSource guards the other end of a cutover: once the
// serving generation moves on, the old source keeps running for the rollback
// window and keeps holding its primary Lease. Dropping it from the rule stops
// the old primary renewing and takes down the set that is still being copied
// from.
func TestMemberRulesCoverRetiredSource(t *testing.T) {
	c := &pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{Name: "rs"}}
	// The cutover has flipped: generation 2 serves, and generation 1 is kept
	// running for the rollback window.
	c.Status.EffectiveShards = 2
	c.Status.ServingGeneration = 2
	c.Status.Reshard = &pgshardv1alpha1.ClusterReshardStatus{
		Name: "rs-reshard-g2", ShardSet: "g2", Generation: 2, Shards: 2,
		RetiredShardSet: "default", RetiredGeneration: 1, RetiredShards: 1,
	}

	var named []string
	for _, rule := range MemberRules(c) {
		if slices.Contains(rule.Verbs, "update") {
			named = append(named, rule.ResourceNames...)
		}
	}

	retired := RetiredGroups(c)
	if len(retired) == 0 {
		t.Fatal("the fixture must produce a retired group")
	}
	for _, g := range retired {
		// The retired group must not coincide with a serving one, or this
		// proves nothing.
		for _, s := range Groups(c) {
			if s.LeaseName() == g.LeaseName() {
				t.Fatalf("fixture does not isolate the retired group: %s also serves", g.LeaseName())
			}
		}
		if !slices.Contains(named, g.LeaseName()) {
			t.Errorf("retired source %s cannot renew its primary lease %s; named leases are %v", g.Name(), g.LeaseName(), named)
		}
	}
}

// TestMemberRulesCoverAGenerationLeftBehindByARollback: the serving, target
// and retired groups are all read out of one reshard record, which
// describes one cutover. A cluster that switched, rolled back and switched
// again is running a generation none of them names -- and its agent then
// exits with "primary cannot start without the lease" and crash-loops,
// while the cluster still reports Ready.
func TestMemberRulesCoverAGenerationLeftBehindByARollback(t *testing.T) {
	c := &pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{Name: "up"}}
	// Generation 1 served, generation 2 took over and was rolled back, and
	// generation 3 now serves with 2 retired. Generation 1's groups are
	// still running: nothing has retired them.
	c.Status.EffectiveShards = 1
	c.Status.ServingGeneration = 3
	c.Status.Reshard = &pgshardv1alpha1.ClusterReshardStatus{
		Name: "up-reshard-g3", ShardSet: "g3", Generation: 3, Shards: 1,
		RetiredShardSet: "g2", RetiredGeneration: 2, RetiredShards: 1,
	}

	var named []string
	for _, rule := range MemberRules(c) {
		if slices.Contains(rule.Verbs, "update") {
			named = append(named, rule.ResourceNames...)
		}
	}
	left := Group{Cluster: "up", Kind: "shard", ShardID: 0, Generation: 1}
	if !slices.Contains(named, left.LeaseName()) {
		t.Errorf("a generation left running by a rollback cannot renew %s; named leases are %v", left.LeaseName(), named)
	}
	if cat := (Group{Cluster: "up", Kind: "catalog", Generation: 1}); !slices.Contains(named, cat.LeaseName()) {
		t.Errorf("the first catalog generation lost its lease permission: %v", named)
	}
	// Everything the record does describe stays covered.
	for _, g := range append(Groups(c), RetiredGroups(c)...) {
		if !slices.Contains(named, g.LeaseName()) {
			t.Errorf("group %s lost its lease permission: %v", g.Name(), named)
		}
	}
	if len(named) != len(slices.Compact(slices.Clone(named))) {
		t.Errorf("lease names must not repeat: %v", named)
	}
}
