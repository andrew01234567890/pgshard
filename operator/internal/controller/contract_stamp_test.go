package controller

import (
	"context"
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func memberStatefulSetForContract(cluster *pgshardv1alpha1.PgShardCluster, shard, member int32) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: cluster.Namespace,
			Name:      owned.PostgreSQLMemberStatefulSetName(cluster.Name, shard, member),
			Labels: map[string]string{
				owned.ComponentLabel: "postgresql",
				owned.ShardLabel:     "0000",
				owned.MemberLabel:    "0001",
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{owned.ComponentLabel: "postgresql"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "postgresql", Image: "img@sha256:" + repeatByte('a', 64)}}},
			},
		},
	}
}

func poolerDeploymentForContract(cluster *pgshardv1alpha1.PgShardCluster) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: cluster.Namespace,
			Name:      cluster.Name + "-pooler",
			Labels:    map[string]string{owned.ComponentLabel: "pooler"},
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{owned.ComponentLabel: "pooler"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "pooler", Image: "img@sha256:" + repeatByte('b', 64)}}},
			},
		},
	}
}

func repeatByte(c byte, n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = c
	}
	return string(out)
}

func TestStampPlanContractsStampsTemplatesAndRecordsStatus(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.MembersPerShard = 3
	base := newFakeClient(t, cluster)
	reconciler := developmentReconciler(base, base)
	current := getCluster(t, ctx, base, cluster)

	member := memberStatefulSetForContract(current, 0, 1)
	pooler := poolerDeploymentForContract(current)
	plan := []client.Object{member, pooler}

	if err := reconciler.stampPlanContracts(ctx, current, plan); err != nil {
		t.Fatal(err)
	}

	// The controller stamps the pod TEMPLATE, which Kubernetes then propagates
	// to every pod the StatefulSet/Deployment controller creates.
	for name, template := range map[string]metav1.ObjectMeta{
		"member": member.Spec.Template.ObjectMeta,
		"pooler": pooler.Spec.Template.ObjectMeta,
	} {
		hash := template.Annotations[owned.PodContractHashAnnotation]
		gen := template.Annotations[owned.PodSecurityGenerationAnnotation]
		if len(hash) != 64 || gen != "1" {
			t.Fatalf("%s template not stamped: hash=%q gen=%q", name, hash, gen)
		}
	}

	// The member stamp must equal a fresh recompute over the stamped template
	// (self-consistency) and be bound to the standby class + identity.
	wantMemberHash, err := owned.ComputeContractStamp(owned.ClassStandby, string(current.UID), 0, 1, 1, &member.Spec.Template)
	if err != nil {
		t.Fatal(err)
	}
	if member.Spec.Template.Annotations[owned.PodContractHashAnnotation] != wantMemberHash {
		t.Fatal("stamped member hash is not self-consistent with a recompute")
	}

	after := getCluster(t, ctx, base, cluster)
	if len(after.Status.PostgreSQLMemberContracts) != 1 {
		t.Fatalf("member contracts = %#v", after.Status.PostgreSQLMemberContracts)
	}
	memberContract := after.Status.PostgreSQLMemberContracts[0]
	if memberContract.Shard != 0 || memberContract.Member != 1 || memberContract.Class != string(owned.ClassStandby) ||
		memberContract.ContractHash != wantMemberHash || memberContract.SecurityGeneration != 1 {
		t.Fatalf("recorded member contract = %#v", memberContract)
	}
	if len(after.Status.SupportingContracts) != 1 || after.Status.SupportingContracts[0].Class != string(owned.ClassPooler) ||
		len(after.Status.SupportingContracts[0].ContractHash) != 64 || after.Status.SupportingContracts[0].SecurityGeneration != 1 {
		t.Fatalf("recorded supporting contracts = %#v", after.Status.SupportingContracts)
	}
}

func TestStampPlanContractsIsIdempotentAndPreservesGeneration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.MembersPerShard = 3
	base := newFakeClient(t, cluster)
	reconciler := developmentReconciler(base, base)
	current := getCluster(t, ctx, base, cluster)

	plan := []client.Object{memberStatefulSetForContract(current, 0, 1), poolerDeploymentForContract(current)}
	if err := reconciler.stampPlanContracts(ctx, current, plan); err != nil {
		t.Fatal(err)
	}
	first := getCluster(t, ctx, base, cluster)
	firstRV := first.ResourceVersion

	// A second stamp over an equivalent plan must not bump the generation and,
	// because the recorded contracts are unchanged, must not write status.
	current = getCluster(t, ctx, base, cluster)
	plan = []client.Object{memberStatefulSetForContract(current, 0, 1), poolerDeploymentForContract(current)}
	if err := reconciler.stampPlanContracts(ctx, current, plan); err != nil {
		t.Fatal(err)
	}
	second := getCluster(t, ctx, base, cluster)
	if second.ResourceVersion != firstRV {
		t.Fatalf("idempotent stamp rewrote status: rv %s -> %s", firstRV, second.ResourceVersion)
	}
	if second.Status.PostgreSQLMemberContracts[0].SecurityGeneration != 1 {
		t.Fatalf("generation drifted: %#v", second.Status.PostgreSQLMemberContracts[0])
	}
}
