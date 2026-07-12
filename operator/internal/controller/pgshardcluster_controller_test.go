package controller

import (
	"context"
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReconcileReportsFoundationAsNotReady(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := pgshardv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cluster := &pgshardv1alpha1.PgShardCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", Generation: 7},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(cluster).Build()
	reconciler := &PgShardClusterReconciler{Client: client}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test", Namespace: "default"}}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}

	got := &pgshardv1alpha1.PgShardCluster{}
	if err := client.Get(context.Background(), request.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != "Pending" || got.Status.ObservedGeneration != 7 {
		t.Fatalf("status = %#v", got.Status)
	}
	if len(got.Status.Conditions) != 1 || got.Status.Conditions[0].Status != metav1.ConditionFalse || got.Status.Conditions[0].Reason != "FoundationOnly" {
		t.Fatalf("conditions = %#v", got.Status.Conditions)
	}

	// A steady-state reconcile must not write or change the transition time.
	transition := got.Status.Conditions[0].LastTransitionTime
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), request.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if !got.Status.Conditions[0].LastTransitionTime.Equal(&transition) {
		t.Fatal("steady-state reconcile changed the transition time")
	}
}
