// Package controller contains Kubernetes reconcilers for pgshard APIs.
package controller

import (
	"context"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const readyCondition = "Ready"

// PgShardClusterReconciler establishes truthful status for the API foundation.
// Workload reconciliation is intentionally deferred to the next implementation
// slice; this controller must never claim that an unobserved data plane is ready.
type PgShardClusterReconciler struct {
	client.Client
}

// +kubebuilder:rbac:groups=pgshard.io,resources=pgshardclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=pgshard.io,resources=pgshardclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pgshard.io,resources=pgshardclusters/finalizers,verbs=update

func (r *PgShardClusterReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	cluster := &pgshardv1alpha1.PgShardCluster{}
	if err := r.Get(ctx, request.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	condition := metav1.Condition{
		Type:               readyCondition,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: cluster.Generation,
		Reason:             "FoundationOnly",
		Message:            "the Milestone 1 API foundation is installed; PostgreSQL workload reconciliation is not implemented yet",
	}
	current := meta.FindStatusCondition(cluster.Status.Conditions, readyCondition)
	if cluster.Status.ObservedGeneration == cluster.Generation &&
		cluster.Status.Phase == "Pending" &&
		current != nil && current.Status == condition.Status &&
		current.Reason == condition.Reason && current.Message == condition.Message &&
		current.ObservedGeneration == condition.ObservedGeneration {
		return ctrl.Result{}, nil
	}

	cluster.Status.ObservedGeneration = cluster.Generation
	cluster.Status.Phase = "Pending"
	meta.SetStatusCondition(&cluster.Status.Conditions, condition)
	if err := r.Status().Update(ctx, cluster); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *PgShardClusterReconciler) SetupWithManager(manager ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(manager).
		For(&pgshardv1alpha1.PgShardCluster{}).
		Named("pgshardcluster").
		Complete(r)
}
