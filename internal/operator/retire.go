package operator

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

// extraMembers are the members a lowered replica count no longer describes:
// pods or claims whose ordinal is at or above the group's current Replicas,
// oldest ordinal last so the highest goes first.
//
// They are found by label rather than by name, because MemberNames() only
// ever returns the members the group is supposed to have. A member the spec
// has stopped describing is invisible to every other part of the reconcile
// -- which is the whole reason lowering used to be refused.
func (r *ClusterReconciler) extraMembers(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group) ([]string, error) {
	sel := client.MatchingLabels(g.Labels())
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(c.Namespace), sel); err != nil {
		return nil, err
	}
	extra := map[string]bool{}
	note := func(name, ordinal string) {
		n, err := strconv.Atoi(ordinal)
		if err != nil || n < g.Replicas || name == "" {
			return
		}
		extra[name] = true
	}
	for i := range pods.Items {
		note(pods.Items[i].Name, pods.Items[i].Labels[LabelOrdinal])
	}
	// A claim counts only when the spec says to reclaim it. Under Retain --
	// the default -- the claim is kept on purpose, so treating it as work
	// left to do makes the member extra for ever: its pod is already gone,
	// every pass picks it again as the highest ordinal, and no lower
	// ordinal is ever reached. That stalls a lowering half way.
	if storageOf(c, g).ReclaimRetiredClaims == pgshardv1alpha1.ReclaimDelete {
		var claims corev1.PersistentVolumeClaimList
		if err := r.List(ctx, &claims, client.InNamespace(c.Namespace), sel); err != nil {
			return nil, err
		}
		for i := range claims.Items {
			note(claims.Items[i].Labels[LabelMember], claims.Items[i].Labels[LabelOrdinal])
		}
	}
	out := make([]string, 0, len(extra))
	for name := range extra {
		out = append(out, name)
	}
	// Highest ordinal first: it is the one furthest from the group the spec
	// now describes, and retiring downwards keeps the remaining members a
	// contiguous 0..n-1 at every step.
	sort.Sort(sort.Reverse(sort.StringSlice(out)))
	return out, nil
}

// retireMember takes one member out of a group: its slot on the primary,
// then its pod, then its claim if the spec says to reclaim it.
//
// The order is the point. synchronous_standby_names is rewritten from
// MemberNames() on every pass, so a lowered count has already removed this
// member from it before anything here runs -- and stepRetire refuses to act
// until the primary confirms that, because deleting a member the primary is
// still waiting for acknowledgements from would stall every commit.
func (r *ClusterReconciler) retireMember(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, obs *groupObservation, members map[string]*memberInfo, password, name string) error {
	log := logf.FromContext(ctx).WithValues("group", g.Name(), "member", name)
	if primary := members[obs.state.primary]; primary != nil && primary.ip != "" {
		var want []string
		for _, n := range g.MemberNames() {
			if n != obs.state.primary {
				want = append(want, SlotName(n))
			}
		}
		// Dropping it before the pod goes means the primary stops keeping
		// WAL for a standby that is never coming back. A slot left behind
		// pins WAL until the disk fills, which is the failure this ticket's
		// refusal was protecting against.
		if err := r.Prober.EnsureSlots(ctx, HostDSN(primary.ip, password), want, SlotName(name)); err != nil {
			return fmt.Errorf("drop the slot of %s: %w", name, err)
		}
	}
	// Read the pod rather than taking it from members: that map is keyed by
	// MemberNames(), which is exactly the set this member is no longer in.
	// A retirement has to reach the members the spec has stopped
	// describing, which is the whole difficulty of it.
	var pod corev1.Pod
	err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: name}, &pod)
	switch {
	case apierrors.IsNotFound(err):
		return r.reclaimClaims(ctx, c, g, name)
	case err != nil:
		return err
	case pod.DeletionTimestamp != nil:
		// Still terminating. Its claim is not touched until it is gone, so
		// nothing deletes a volume out from under a running PostgreSQL.
		return nil
	}
	if err := r.Delete(ctx, &pod); client.IgnoreNotFound(err) != nil {
		return err
	}
	log.Info("retiring member: deleting pod")
	return nil
}

// reclaimClaims deletes the retired member's claims when the spec says to,
// and otherwise leaves them labelled for whoever wants them.
func (r *ClusterReconciler) reclaimClaims(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, name string) error {
	log := logf.FromContext(ctx).WithValues("group", g.Name(), "member", name)
	var claims corev1.PersistentVolumeClaimList
	if err := r.List(ctx, &claims, client.InNamespace(c.Namespace), client.MatchingLabels{LabelMember: name}); err != nil {
		return err
	}
	if storageOf(c, g).ReclaimRetiredClaims != pgshardv1alpha1.ReclaimDelete {
		return nil
	}
	for i := range claims.Items {
		if claims.Items[i].DeletionTimestamp != nil {
			continue
		}
		if err := r.Delete(ctx, &claims.Items[i]); client.IgnoreNotFound(err) != nil && !apierrors.IsNotFound(err) {
			return err
		}
		log.Info("member retired; deleting its claim", "claim", claims.Items[i].Name)
	}
	return nil
}

// storageOf is the storage spec that governs a group: the catalog has its
// own, every shard shares the cluster's.
func storageOf(c *pgshardv1alpha1.PgShardCluster, g Group) pgshardv1alpha1.StorageSpec {
	if g.Kind == "catalog" {
		return c.Spec.Catalog.Storage
	}
	return c.Spec.Storage
}

// memberIsPrimary reports whether the member's own pod still carries the
// primary role. Asked of the pod because a member being retired is outside
// the group's member set, so nothing else in the reconcile describes it.
func (r *ClusterReconciler) memberIsPrimary(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, name string) (bool, error) {
	var pod corev1.Pod
	switch err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: name}, &pod); {
	case apierrors.IsNotFound(err):
		return false, nil
	case err != nil:
		return false, err
	}
	return pod.Labels[LabelRole] == RolePrimary, nil
}
