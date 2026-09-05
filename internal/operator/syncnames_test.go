package operator

import (
	"strings"
	"testing"
)

// TestAMemberIsNeverInItsOwnSynchronousStandbyNames: the per-member config
// carried one synchronous_standby_names computed around the group's
// designated primary, and Promote rewrites postgresql.conf from that config
// (and truncates postgresql.auto.conf, so the operator's ALTER SYSTEM value
// goes with it). A promoted member therefore waited on a list containing its
// own name, which no walsender matches: on a two-member shard the list was
// only its own name and every commit blocked until the operator's next probe
// pass.
func TestAMemberIsNeverInItsOwnSynchronousStandbyNames(t *testing.T) {
	for _, replicas := range []int{2, 3} {
		c := newCluster("demo")
		c.Spec.ReplicasPerShard = replicas
		c.Spec.Durability.MinSyncStandbys = 1
		g := Groups(c)[1]
		primary := g.MemberName(0)
		tpl := Template(c, g, nil, nil)
		for i := range replicas {
			member := g.MemberName(i)
			got := agentConfig(c, g, member, primary, tpl, false, false).Postgres.SynchronousStandbyNames
			if strings.Contains(got, `"`+member+`"`) {
				t.Errorf("replicas=%d member %s waits for itself: %s", replicas, member, got)
			}
			for j := range replicas {
				if peer := g.MemberName(j); peer != member && !strings.Contains(got, `"`+peer+`"`) {
					t.Errorf("replicas=%d member %s does not list %s: %s", replicas, member, peer, got)
				}
			}
		}
	}
}
