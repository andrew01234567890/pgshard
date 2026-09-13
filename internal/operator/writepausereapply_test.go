package operator

import (
	"context"
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func reapplyFixture(t *testing.T) (*ClusterReconciler, *pgshardv1alpha1.PgShardCluster, Group, *fakeProber) {
	t.Helper()
	one := 1
	fp := &fakeProber{pausedDSN: map[string]bool{}, pauseClaimed: map[string]bool{}}
	c := &pgshardv1alpha1.PgShardCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns"},
		Spec:       pgshardv1alpha1.PgShardClusterSpec{Shards: &one, ReplicasPerShard: 1},
	}
	r := &ClusterReconciler{Prober: fp}
	var g Group
	for _, gr := range Groups(c) {
		if gr.Kind == "shard" {
			g = gr
			break
		}
	}
	if g.Kind != "shard" {
		t.Fatal("fixture has no shard group")
	}
	return r, c, g, fp
}

// PGS-737. A configuration reload rewrites postgresql.auto.conf, where the
// write pause lives, so a rollout pass during a cutover leaves the sources
// writable. reapplyWritePause is the thing that puts a lost pause back --
// but it read only the barrier's cluster-wide fence, and a cutover does not
// raise that. It records its intent per shard, in
// shard_status.write_paused_by.
//
// So the cutover case had no owner at all: the sources took writes for the
// rest of the cutover, and a write landing after the forward subscriptions
// are dropped is acknowledged and lost.
func TestACutoverPauseIsReapplied(t *testing.T) {
	r, c, g, fp := reapplyFixture(t)
	fp.fenced = false // no barrier anywhere near this
	fp.pauseClaimed[g.ShardSet()+"/0"] = true

	if err := r.reapplyWritePause(context.Background(), c, g, "shard-dsn", PrimaryState{WritesPaused: false}, "pw"); err != nil {
		t.Fatal(err)
	}
	if len(fp.paused) != 1 || fp.paused[0] != "shard-dsn" {
		t.Fatalf("paused %v; a cutover holds a claim on this shard and the primary is taking writes", fp.paused)
	}
}

// And the pause is not reapplied to a shard nobody is holding, because
// reapplying one nothing asked for makes a healthy primary read-only.
func TestAnUnclaimedShardIsLeftWritable(t *testing.T) {
	r, c, g, fp := reapplyFixture(t)
	fp.fenced = false

	if err := r.reapplyWritePause(context.Background(), c, g, "shard-dsn", PrimaryState{WritesPaused: false}, "pw"); err != nil {
		t.Fatal(err)
	}
	if len(fp.paused) != 0 {
		t.Fatalf("paused %v with no fence and no claim", fp.paused)
	}
}

// A primary already refusing writes is left alone whoever holds it: the
// early return on WritesPaused comes before either question is asked.
func TestAPausedPrimaryIsNotPausedAgain(t *testing.T) {
	r, c, g, fp := reapplyFixture(t)
	fp.pauseClaimed[g.ShardSet()+"/0"] = true

	if err := r.reapplyWritePause(context.Background(), c, g, "shard-dsn", PrimaryState{WritesPaused: true}, "pw"); err != nil {
		t.Fatal(err)
	}
	if len(fp.paused) != 0 {
		t.Fatalf("paused %v; it was already refusing writes", fp.paused)
	}
}
