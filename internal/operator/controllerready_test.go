package operator

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func deployment(namespace, name string, replicas, ready int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Status:     appsv1.DeploymentStatus{Replicas: replicas, ReadyReplicas: ready},
	}
}

func TestDeploymentReady(t *testing.T) {
	r := &ClusterReconciler{Client: fakeClient(t,
		deployment("ns", "up", 1, 1),
		deployment("ns", "down", 2, 0),
	)}
	ctx := context.Background()

	if ok, msg := r.deploymentReady(ctx, "ns", "up"); !ok || msg != "" {
		t.Errorf("ready deployment: %v %q", ok, msg)
	}
	if ok, msg := r.deploymentReady(ctx, "ns", "down"); ok || !strings.Contains(msg, "0 ready replica(s) of 2") {
		t.Errorf("unready deployment: %v %q", ok, msg)
	}
	if ok, msg := r.deploymentReady(ctx, "ns", "absent"); ok || !strings.Contains(msg, "does not exist yet") {
		t.Errorf("missing deployment: %v %q", ok, msg)
	}
}
