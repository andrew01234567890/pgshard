package operator

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/placement"
)

// A reshard record's name is derived from the cluster and the target
// generation, so anyone who can create a PgShardReshard in the namespace can
// put an object there before the operator does. ensureReshardRecord took
// AlreadyExists for success, so it adopted whatever was there -- and later
// code reads Spec.Mode back, which is how a pre-claimed record turns a
// reshard into an upgrade (PGS-388).
func adoptFixture(t *testing.T) (*ClusterReconciler, *pgshardv1alpha1.PgShardCluster, *ShardSetInfo, *ShardSetInfo) {
	t.Helper()
	c := boundCluster("demo")
	serving := &ShardSetInfo{Name: "default", Generation: 3, PGMajor: 18,
		Ranges: placement.RangeSet{{Start: -9223372036854775808, End: 0}}}
	pending := &ShardSetInfo{Name: "default", Generation: 4, PGMajor: 18,
		Ranges: placement.RangeSet{{Start: -9223372036854775808, End: -1}, {Start: 0, End: 9223372036854775807}}}
	r := &ClusterReconciler{Client: fakeClient(t, c)}
	return r, c, serving, pending
}

func TestAPreClaimedReshardRecordIsRefused(t *testing.T) {
	ctx := context.Background()
	r, c, serving, pending := adoptFixture(t)

	// Somebody else's object, sitting on the name the operator is about to
	// use, asking for an upgrade to another major.
	impostor := &pgshardv1alpha1.PgShardReshard{
		ObjectMeta: metav1.ObjectMeta{Name: ReshardName(c.Name, pending.Generation), Namespace: c.Namespace},
		Spec: pgshardv1alpha1.PgShardReshardSpec{
			ClusterName: c.Name, Mode: pgshardv1alpha1.ReshardModeUpgrade, TargetMajor: 19,
			TargetShardSet: "default", TargetGeneration: pending.Generation,
		},
	}
	if err := r.Create(ctx, impostor); err != nil {
		t.Fatal(err)
	}
	err := r.ensureReshardRecord(ctx, c, serving, pending, "spec")
	if err == nil {
		t.Fatal("the operator adopted a record it did not create")
	}
	if !strings.Contains(err.Error(), "not controlled by cluster") {
		t.Fatalf("refusal must say why: %v", err)
	}
	// And it must not have been rewritten into something the operator would
	// run: refusing is the point, not repairing.
	var after pgshardv1alpha1.PgShardReshard
	if gerr := r.Get(ctx, client.ObjectKeyFromObject(impostor), &after); gerr != nil {
		t.Fatal(gerr)
	}
	if after.Spec.Mode != pgshardv1alpha1.ReshardModeUpgrade {
		t.Fatalf("the impostor was modified: %+v", after.Spec)
	}
}

// An edited record the operator DOES own is refused too. It is either a hand
// edit or a bug, and a reshard is not a thing to start on a guess about
// which.
func TestAnOwnedReshardRecordWithADifferentSpecIsRefused(t *testing.T) {
	ctx := context.Background()
	r, c, serving, pending := adoptFixture(t)

	if err := r.ensureReshardRecord(ctx, c, serving, pending, "spec"); err != nil {
		t.Fatalf("first create: %v", err)
	}
	// Idempotent while nothing has changed: the reconciler runs again and
	// again, and adopting its own unchanged record is the normal path.
	if err := r.ensureReshardRecord(ctx, c, serving, pending, "spec"); err != nil {
		t.Fatalf("re-adopting an identical record must succeed: %v", err)
	}

	var rec pgshardv1alpha1.PgShardReshard
	if err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: ReshardName(c.Name, pending.Generation)}, &rec); err != nil {
		t.Fatal(err)
	}
	rec.Spec.Mode = pgshardv1alpha1.ReshardModeUpgrade
	rec.Spec.TargetMajor = 19
	if err := r.Update(ctx, &rec); err != nil {
		t.Fatal(err)
	}
	err := r.ensureReshardRecord(ctx, c, serving, pending, "spec")
	if err == nil {
		t.Fatal("an edited record was adopted")
	}
	if !strings.Contains(err.Error(), "different spec") {
		t.Fatalf("refusal must say why: %v", err)
	}
}
