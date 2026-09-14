package operator

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

// TestGroupTimersAreKeptPerNamespace (PGS-848): the operator watches every
// namespace, and two clusters of one name in different namespaces have groups
// of one prefix. The failover delay, the switchover catch-up wait and the
// re-promotion pacing were keyed by that prefix alone, so a pass over the
// healthy cluster cleared the unhealthy one's clock and its dead primary was
// never failed over.
func TestGroupTimersAreKeptPerNamespace(t *testing.T) {
	now := time.Now()
	r := &ClusterReconciler{Now: func() time.Time { return now }}
	down := &pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "team-a"}}
	up := &pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "team-b"}}
	g := Group{Cluster: "orders", Kind: "shard", ShardID: 0}

	r.unhealthyFor(down, g, true)
	now = now.Add(5 * time.Second)
	r.unhealthyFor(up, g, false)
	now = now.Add(5 * time.Second)
	if got := r.unhealthyFor(down, g, true); got != 10*time.Second {
		t.Fatalf("unhealthy for %s after a healthy pass over the same-named cluster in another namespace, want 10s", got)
	}

	r.switchoverWaiting(down, g, true)
	now = now.Add(5 * time.Second)
	r.switchoverWaiting(up, g, false)
	if got := r.switchoverWaiting(down, g, true); got != 5*time.Second {
		t.Fatalf("switchover waiting %s after the other namespace's cluster stopped waiting, want 5s", got)
	}

	if !r.repromoteDue(down, g) {
		t.Fatal("the first re-promotion must be due")
	}
	if !r.repromoteDue(up, g) {
		t.Fatal("a re-promotion in one namespace must not delay the same-named cluster's in another")
	}
}
