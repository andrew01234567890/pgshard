package resources

import (
	"strings"
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
)

// The single-member catalog-access assertions used to panic. They are now
// typed errors so a reconcile bug degrades to a typed failure rather than
// crashing the manager (they are on an admission-reachable code path in later
// steps).

func TestPostgreSQLShardStatefulSetReturnsErrorWithoutCatalogCheckpoints(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	_, err := postgresqlShardStatefulSet(cluster, 0, DevelopmentImages(), "secret", "pvc", "config", "hash", nil, nil, pgshardv1alpha1.PostgreSQLWritableLeaseStatus{})
	if err == nil || !strings.Contains(err.Error(), "incomplete catalog access checkpoints") {
		t.Fatalf("missing catalog checkpoints error = %v", err)
	}
}

func TestPoolerDeploymentReturnsErrorWithoutCatalogCheckpoint(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	cluster.Spec.MembersPerShard = 1
	cluster.Spec.Durability = pgshardv1alpha1.DurabilityAsynchronous
	_, err := poolerDeployment(cluster, DevelopmentImages().Pooler, "hash", nil)
	if err == nil || !strings.Contains(err.Error(), "no catalog access checkpoint") {
		t.Fatalf("missing pooler catalog checkpoint error = %v", err)
	}
}
