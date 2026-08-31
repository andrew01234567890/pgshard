package admin

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/operator"
)

// clustersOf names the clusters an object belongs to. The second return is
// false when the object carries no cluster identity at all, which is not
// the same as belonging to none: a group whose label is missing is a
// question this cannot answer, and answering "not yours" would stop an
// admin refreshing on its own cluster's changes.
func clustersOf(obj client.Object) ([]string, bool) {
	switch o := obj.(type) {
	case *pgshardv1alpha1.PgShardCluster:
		return []string{o.Name}, true
	case *pgshardv1alpha1.PgShardBackup:
		return []string{o.Spec.ClusterName}, o.Spec.ClusterName != ""
	case *pgshardv1alpha1.PgShardReshard:
		return []string{o.Spec.ClusterName}, o.Spec.ClusterName != ""
	case *pgshardv1alpha1.PgShardRestore:
		// Both ends: the cluster whose repository is read, and the one the
		// restore creates. A restore building cluster A is A's business
		// even while A does not exist yet.
		names := []string{}
		if o.Spec.ClusterName != "" {
			names = append(names, o.Spec.ClusterName)
		}
		if o.Spec.NewClusterName != "" {
			names = append(names, o.Spec.NewClusterName)
		}
		return names, len(names) > 0
	case *pgshardv1alpha1.PgShardGroup:
		name := o.Labels[operator.LabelCluster]
		return []string{name}, name != ""
	case *corev1.Pod:
		name := o.Labels[operator.LabelCluster]
		return []string{name}, name != ""
	}
	return nil, false
}

// inScope reports whether an admin scoped to cluster should be told about a
// change to obj. An empty scope is the whole namespace.
//
// It fails open, deliberately and in two places: an object whose owner
// cannot be determined, and a kind this does not know, both notify. A tick
// an admin did not need costs a re-render; a suppressed one leaves a page
// showing a cluster's state as it was, with nothing to say it is stale,
// which is the worse failure of the two by some distance.
func inScope(cluster string, obj client.Object) bool {
	if cluster == "" {
		return true
	}
	names, known := clustersOf(obj)
	if !known {
		return true
	}
	for _, name := range names {
		if name == cluster {
			return true
		}
	}
	return false
}

// policyInScope answers for a PgShardBackupPolicy, which names no cluster:
// clusters bind to a policy, not the other way round, so the only way to
// know is to read the one this admin serves. A policy is a rare object and
// this runs on its changes alone.
func policyInScope(ctx context.Context, c client.Client, cluster string, p *pgshardv1alpha1.PgShardBackupPolicy) bool {
	if cluster == "" {
		return true
	}
	var owner pgshardv1alpha1.PgShardCluster
	if err := c.Get(ctx, client.ObjectKey{Namespace: p.Namespace, Name: cluster}, &owner); err != nil {
		// Unreadable for any reason, including the cluster not existing
		// yet: notify rather than decide on what is not known.
		return true
	}
	return owner.Spec.Backup.PolicyRef == p.Name
}
