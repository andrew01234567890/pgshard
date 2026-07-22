package controller

import (
	"context"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/operator/internal/podfence"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func identityProbeProber(t *testing.T, objects ...client.Object) *serverControllerIdentityProber {
	t.Helper()
	fakeClient := newFakeClient(t, objects...)
	prober := NewServerControllerIdentityProber(fakeClient, testProbeIdentities(), podfence.NewIdentityObservationStore())
	prober.cleanupTimeout = 50 * time.Millisecond
	return prober
}

func testProbeIdentities() podfence.ControllerIdentities {
	return podfence.ControllerIdentities{
		Operator:                          "system:serviceaccount:pgshard-system:pgshard-controller-manager",
		StatefulSetController:             "system:serviceaccount:kube-system:statefulset-controller",
		ReplicaSetController:              "system:serviceaccount:kube-system:replicaset-controller",
		DeploymentController:              "system:serviceaccount:kube-system:deployment-controller",
		HorizontalPodAutoscalerController: "system:serviceaccount:kube-system:horizontal-pod-autoscaler",
	}
}

func lingeringProbePod(name, token string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: genTestNamespace,
			Labels:    map[string]string{identityProbeLabel: "deploy-" + token},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "probe", Image: identityProbeImage}}},
	}
}

func TestProbeArtifactsAbsentDetectsLingeringDependents(t *testing.T) {
	t.Parallel()
	pod := lingeringProbePod("probe-dep-0", "tok")
	prober := identityProbeProber(t, isolationNamespace("ns-uid"), pod)

	// A dependent pod carrying the probe token label means cleanup is NOT complete.
	absent, err := prober.probeArtifactsAbsent(context.Background(), genTestNamespace, "tok", nil)
	if err != nil {
		t.Fatal(err)
	}
	if absent {
		t.Fatal("a lingering dependent probe pod was reported absent")
	}

	// Once it is gone, cleanup is confirmed complete.
	if err := prober.client.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	absent, err = prober.probeArtifactsAbsent(context.Background(), genTestNamespace, "tok", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !absent {
		t.Fatal("probe artifacts were not reported absent after the dependent was deleted")
	}
}

func TestCleanupProbeWaitsForAbsenceAndTimesOut(t *testing.T) {
	t.Parallel()
	// A dependent pod the named-object deletes do not remove (it has no owner in
	// the fake client) forces the absence poll to time out — proving cleanup
	// WAITS for absence rather than returning immediately after issuing deletes.
	lingering := lingeringProbePod("orphan-dep", "tok")
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "pgshard-idprobe-sts-tok", Namespace: genTestNamespace}}
	prober := identityProbeProber(t, isolationNamespace("ns-uid"), sts, lingering)

	err := prober.cleanupProbe(context.Background(), genTestNamespace, "tok", []client.Object{sts})
	if err == nil {
		t.Fatal("cleanup returned success while a dependent probe pod still lingered")
	}
	// The named object it CAN delete is gone (delete was issued), and it still
	// reported failure because the dependent never cleared.
	remaining := &appsv1.StatefulSet{}
	if getErr := prober.client.Get(context.Background(), client.ObjectKey{Namespace: genTestNamespace, Name: sts.Name}, remaining); getErr == nil {
		t.Fatal("cleanup did not delete the named probe object")
	}
}
