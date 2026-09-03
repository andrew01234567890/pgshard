package operator

import (
	"context"
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

// TestReplicaCountsMayGrowButNotShrink is containment, not a feature.
// Lowering a replica count stops the operator rendering the removed
// members and does nothing else: their pods keep running, their PVCs
// stay, their slots stay, and they stay in synchronous_standby_names --
// so a commit can still be acknowledged by a member the cluster has
// stopped managing. Refusing the edit is the honest answer until
// something drains and retires them.
func TestReplicaCountsMayGrowButNotShrink(t *testing.T) {
	requireEnvtest(t)
	ctx := context.Background()
	c := newCluster("shrink")
	c.Spec.ReplicasPerShard = 3
	c.Spec.Catalog.Replicas = 3
	if err := k8sClient.Create(ctx, c); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name  string
		apply func(*pgshardv1alpha1.PgShardCluster)
		want  string
	}{
		{"lowering the shard replicas", func(cl *pgshardv1alpha1.PgShardCluster) { cl.Spec.ReplicasPerShard = 2 },
			"replicasPerShard cannot be lowered"},
		{"lowering the catalog replicas", func(cl *pgshardv1alpha1.PgShardCluster) { cl.Spec.Catalog.Replicas = 1 },
			"catalog.replicas cannot be lowered"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cl pgshardv1alpha1.PgShardCluster
			get(t, c.Name, &cl)
			tc.apply(&cl)
			err := k8sClient.Update(ctx, &cl)
			if err == nil {
				t.Fatal("the edit was accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("unexpected rejection: %v", err)
			}
			// The message has to say why, because the operator cannot do
			// it rather than because it is not allowed.
			if !strings.Contains(err.Error(), "is left running") {
				t.Fatalf("the rejection does not say what would happen: %v", err)
			}
		})
	}

	for _, tc := range []struct {
		name  string
		apply func(*pgshardv1alpha1.PgShardCluster)
	}{
		{"raising the shard replicas", func(cl *pgshardv1alpha1.PgShardCluster) { cl.Spec.ReplicasPerShard = 5 }},
		{"raising the catalog replicas", func(cl *pgshardv1alpha1.PgShardCluster) { cl.Spec.Catalog.Replicas = 5 }},
		{"leaving them alone", func(*pgshardv1alpha1.PgShardCluster) {}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cl pgshardv1alpha1.PgShardCluster
			get(t, c.Name, &cl)
			tc.apply(&cl)
			if err := k8sClient.Update(ctx, &cl); err != nil {
				t.Fatalf("%s must be accepted: %v", tc.name, err)
			}
		})
	}
}

// TestADesignatedPrimaryOutsideTheMemberSetIsNotDereferenced covers the
// panic PGS-466 reported. It is already guarded -- loadState designates a
// member that exists, and the observation treats a missing primary as an
// unhealthy one -- and this holds that guard in place, because the failure
// it prevents is a crash loop over the whole cluster rather than one group.
func TestADesignatedPrimaryOutsideTheMemberSetIsNotDereferenced(t *testing.T) {
	r, fp, c := setup(t, "outside")
	bringUp(t, r, fp, c)
	ctx := context.Background()
	g := Groups(c)[len(Groups(c))-1]

	pg := r.Renderer.PgShardGroup(c, g)
	if err := r.Get(ctx, client.ObjectKeyFromObject(pg), pg); err != nil {
		t.Fatal(err)
	}
	pg.Status.Primary = g.Prefix() + "-9"
	if err := r.Status().Update(ctx, pg); err != nil {
		t.Fatal(err)
	}
	st, err := r.loadState(ctx, c, g)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(g.MemberNames(), st.primary) {
		t.Fatalf("loadState designated %q, which is not a member", st.primary)
	}
	reconcile(t, r, c)
}
