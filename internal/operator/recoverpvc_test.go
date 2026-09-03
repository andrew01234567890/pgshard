package operator

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

func memberPod(namespace, name, claim string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "postgres", Image: "pg"}},
			Volumes: []corev1.Volume{
				{Name: "config"},
				{Name: "data", VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim}}},
			},
		},
	}
}

// TestAGroupThatLostItsStatusKeepsTheVolumeItIsRunningOn is the storage
// rebuild case: the rebuilt claim carries a -v2 suffix, so a member whose
// status is gone would otherwise fall back to the claim named after it -
// the pre-rebuild volume, on the old storage class, holding data as stale
// as the rebuild is old.
func TestAGroupThatLostItsStatusKeepsTheVolumeItIsRunningOn(t *testing.T) {
	c := newCluster("rebuilt")
	g := Group{Cluster: c.Name, Kind: "shard", ShardID: 0, Replicas: 3}
	names := g.MemberNames()
	cl := fakeClient(t,
		memberPod(c.Namespace, names[0], names[0]),
		memberPod(c.Namespace, names[1], names[1]+"-v2"),
	)
	r := &ClusterReconciler{Client: cl}
	st := groupState{syncSet: map[string]bool{}, pvcs: map[string]string{}}
	for _, n := range names {
		st.pvcs[n] = n
	}
	pg := &pgshardv1alpha1.PgShardGroup{}
	if err := r.recoverPVCs(context.Background(), c, g, pg, st); err != nil {
		t.Fatal(err)
	}
	if got := st.pvcs[names[1]]; got != names[1]+"-v2" {
		t.Fatalf("member on a rebuilt volume recovered %q, want %q", got, names[1]+"-v2")
	}
	if got := st.pvcs[names[0]]; got != names[0] {
		t.Fatalf("member on its original volume recovered %q, want %q", got, names[0])
	}
	if got := st.pvcs[names[2]]; got != names[2] {
		t.Fatalf("member with no pod at all recovered %q, want the default %q", got, names[2])
	}
}

// TestStatusStillWinsWhenItHasAVolume keeps the recovery a fallback: a
// status that names a claim is the record of a rebuild in progress, and a
// pod still running on the old volume must not overwrite it.
func TestStatusStillWinsWhenItHasAVolume(t *testing.T) {
	c := newCluster("inflight")
	g := Group{Cluster: c.Name, Kind: "shard", ShardID: 0, Replicas: 3}
	names := g.MemberNames()
	cl := fakeClient(t, memberPod(c.Namespace, names[1], names[1]))
	r := &ClusterReconciler{Client: cl}
	st := groupState{syncSet: map[string]bool{}, pvcs: map[string]string{names[1]: names[1] + "-v2"}}
	pg := &pgshardv1alpha1.PgShardGroup{Status: pgshardv1alpha1.PgShardGroupStatus{
		Members: []pgshardv1alpha1.MemberStatus{{Name: names[1], PVC: names[1] + "-v2"}}}}
	if err := r.recoverPVCs(context.Background(), c, g, pg, st); err != nil {
		t.Fatal(err)
	}
	if got := st.pvcs[names[1]]; got != names[1]+"-v2" {
		t.Fatalf("the pod overwrote a claim the status had already recorded: %q", got)
	}
}

func memberPVC(namespace, group, member, name string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: namespace,
		Labels: map[string]string{LabelCluster: "gone", LabelGroup: group, LabelMember: member}}}
}

// TestWithNoPodLeftTheNewestClaimIsTheOne covers the worse case: the
// status is gone and so is the pod, so there is nothing running to ask.
// The claims themselves record the rebuilds, and they only move forward.
func TestWithNoPodLeftTheNewestClaimIsTheOne(t *testing.T) {
	c := newCluster("gone")
	g := Group{Cluster: c.Name, Kind: "shard", ShardID: 0, Replicas: 3}
	names := g.MemberNames()
	cl := fakeClient(t,
		memberPVC(c.Namespace, g.Name(), names[0], names[0]),
		memberPVC(c.Namespace, g.Name(), names[1], names[1]),
		memberPVC(c.Namespace, g.Name(), names[1], names[1]+"-v2"),
		memberPVC(c.Namespace, g.Name(), names[1], names[1]+"-v10"),
		memberPVC(c.Namespace, g.Name(), names[1], names[1]+"-v3"),
	)
	r := &ClusterReconciler{Client: cl}
	st := groupState{syncSet: map[string]bool{}, pvcs: map[string]string{}}
	for _, n := range names {
		st.pvcs[n] = n
	}
	if err := r.recoverPVCs(context.Background(), c, g, &pgshardv1alpha1.PgShardGroup{}, st); err != nil {
		t.Fatal(err)
	}
	// -v10 and not -v3: the suffix is a number, and comparing it as text
	// would put -v3 last and mount a volume two rebuilds old.
	if got := st.pvcs[names[1]]; got != names[1]+"-v10" {
		t.Fatalf("recovered %q, want %q", got, names[1]+"-v10")
	}
	if got := st.pvcs[names[0]]; got != names[0] {
		t.Fatalf("a member rebuilt never recovered %q, want %q", got, names[0])
	}
	if got := st.pvcs[names[2]]; got != names[2] {
		t.Fatalf("a member with no claim at all recovered %q, want the default %q", got, names[2])
	}
}
