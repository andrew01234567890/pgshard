package admin

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/operator"
)

func meta(name string, labels map[string]string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name, Namespace: "default", Labels: labels}
}

func ofA() map[string]string { return map[string]string{operator.LabelCluster: "a"} }
func ofB() map[string]string { return map[string]string{operator.LabelCluster: "b"} }

// TestAnAdminIsToldAboutItsOwnClusterOnly: every kind the admin watches,
// asserted for both clusters rather than assumed from the one that was
// easy to reach.
func TestAnAdminIsToldAboutItsOwnClusterOnly(t *testing.T) {
	for _, c := range []struct {
		name string
		obj  client.Object
		want bool
	}{
		{"its own cluster", &pgshardv1alpha1.PgShardCluster{ObjectMeta: meta("a", nil)}, true},
		{"another cluster", &pgshardv1alpha1.PgShardCluster{ObjectMeta: meta("b", nil)}, false},

		{"its own group", &pgshardv1alpha1.PgShardGroup{ObjectMeta: meta("a-shard-0", ofA())}, true},
		{"another cluster's group", &pgshardv1alpha1.PgShardGroup{ObjectMeta: meta("b-shard-0", ofB())}, false},

		{"its own pod", &corev1.Pod{ObjectMeta: meta("a-shard-0-1", ofA())}, true},
		{"another cluster's pod", &corev1.Pod{ObjectMeta: meta("b-shard-0-1", ofB())}, false},

		{"its own backup", &pgshardv1alpha1.PgShardBackup{ObjectMeta: meta("nightly-1", nil),
			Spec: pgshardv1alpha1.PgShardBackupSpec{ClusterName: "a"}}, true},
		{"another cluster's backup", &pgshardv1alpha1.PgShardBackup{ObjectMeta: meta("nightly-1", nil),
			Spec: pgshardv1alpha1.PgShardBackupSpec{ClusterName: "b"}}, false},

		{"its own reshard", &pgshardv1alpha1.PgShardReshard{ObjectMeta: meta("grow", nil),
			Spec: pgshardv1alpha1.PgShardReshardSpec{ClusterName: "a"}}, true},
		{"another cluster's reshard", &pgshardv1alpha1.PgShardReshard{ObjectMeta: meta("grow", nil),
			Spec: pgshardv1alpha1.PgShardReshardSpec{ClusterName: "b"}}, false},

		// A restore names two clusters and both are its business: the one
		// whose repository is read, and the one it is building.
		{"a restore from its cluster", &pgshardv1alpha1.PgShardRestore{ObjectMeta: meta("r", nil),
			Spec: pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "a", NewClusterName: "c"}}, true},
		{"a restore creating its cluster", &pgshardv1alpha1.PgShardRestore{ObjectMeta: meta("r", nil),
			Spec: pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "b", NewClusterName: "a"}}, true},
		{"a restore between other clusters", &pgshardv1alpha1.PgShardRestore{ObjectMeta: meta("r", nil),
			Spec: pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "b", NewClusterName: "c"}}, false},

		// Suppressing a real update is the worse failure, so anything this
		// cannot place still gets through.
		{"a group with no cluster label", &pgshardv1alpha1.PgShardGroup{ObjectMeta: meta("orphan", nil)}, true},
		{"a backup naming no cluster", &pgshardv1alpha1.PgShardBackup{ObjectMeta: meta("b", nil)}, true},
		{"a kind this does not know", &corev1.Secret{ObjectMeta: meta("s", nil)}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := inScope("a", c.obj); got != c.want {
				t.Fatalf("inScope(a, %T %s) = %v, want %v", c.obj, c.obj.GetName(), got, c.want)
			}
			// An admin serving the whole namespace is told about everything.
			if !inScope("", c.obj) {
				t.Fatal("an unscoped admin must be told about every change")
			}
		})
	}
}

// TestAPolicyIsPlacedByTheClusterThatBindsToIt: a policy names no cluster --
// clusters bind to it -- so the only way to place one is to read the
// cluster this admin serves.
func TestAPolicyIsPlacedByTheClusterThatBindsToIt(t *testing.T) {
	scheme, err := operator.NewScheme()
	if err != nil {
		t.Fatal(err)
	}
	cluster := func(name, policy string) *pgshardv1alpha1.PgShardCluster {
		c := &pgshardv1alpha1.PgShardCluster{ObjectMeta: meta(name, nil)}
		c.Spec.Backup.PolicyRef = policy
		return c
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster("a", "nightly"), cluster("b", "weekly")).Build()
	ctx := context.Background()
	mine := &pgshardv1alpha1.PgShardBackupPolicy{ObjectMeta: meta("nightly", nil)}
	theirs := &pgshardv1alpha1.PgShardBackupPolicy{ObjectMeta: meta("weekly", nil)}

	if !policyInScope(ctx, c, "a", mine) {
		t.Fatal("the policy this cluster binds to must reach it")
	}
	if policyInScope(ctx, c, "a", theirs) {
		t.Fatal("a policy only another cluster binds to must not")
	}
	if !policyInScope(ctx, c, "", theirs) {
		t.Fatal("an unscoped admin must be told about every policy")
	}
	// The cluster may not exist yet, and a policy created before it is one
	// this admin will want. Not knowing is not a reason to go quiet.
	if !policyInScope(ctx, c, "notyet", mine) {
		t.Fatal("a policy must reach an admin whose cluster cannot be read")
	}
}
