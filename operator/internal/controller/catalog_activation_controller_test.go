package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func activationFoundationCluster() *pgshardv1alpha1.PgShardCluster {
	return &pgshardv1alpha1.PgShardCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "activation", Namespace: "database", UID: "cluster-uid", Generation: 1},
		Spec:       pgshardv1alpha1.PgShardClusterSpec{MembersPerShard: 3},
		Status: pgshardv1alpha1.PgShardClusterStatus{PostgreSQLBootstrapSpec: &pgshardv1alpha1.PostgreSQLBootstrapSpecStatus{
			PostgreSQLRuntime: "agent-quarantine",
		}},
	}
}

func TestCatalogActivationCarrierCreationIsEmptyCheckpointedAndNeverRecreated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := activationFoundationCluster()
	kubeClient := newFakeClient(t, cluster)
	reconciler := &PgShardClusterReconciler{Client: kubeClient, APIReader: kubeClient}

	if err := reconciler.ensureCatalogActivationCarrier(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	checkpointed := getCluster(t, ctx, kubeClient, cluster)
	if checkpointed.Status.CatalogActivation == nil || checkpointed.Status.CatalogActivation.Name != "activation-catalog-activation" || checkpointed.Status.CatalogActivation.UID == "" {
		t.Fatalf("catalog activation checkpoint = %#v", checkpointed.Status.CatalogActivation)
	}
	carrier := &pgshardv1alpha1.PgShardCatalogActivation{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: "activation-catalog-activation"}
	if err := kubeClient.Get(ctx, key, carrier); err != nil {
		t.Fatal(err)
	}
	if carrier.Spec.Request != nil || carrier.Spec.RequestSHA256 != "" || carrier.Status.Acceptance != nil {
		t.Fatalf("operator populated inert carrier: %#v", carrier)
	}
	if err := kubeClient.Delete(ctx, carrier); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ensureCatalogActivationCarrier(ctx, checkpointed); err == nil || !strings.Contains(err.Error(), "explicit recovery is required") {
		t.Fatalf("missing checkpointed carrier error = %v", err)
	}
	replacement := pgshardv1alpha1.EmptyCatalogActivation(checkpointed)
	if err := kubeClient.Create(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	if replacement.UID == checkpointed.Status.CatalogActivation.UID {
		t.Fatal("fake API reused the deleted carrier UID")
	}
	if err := reconciler.ensureCatalogActivationCarrier(ctx, checkpointed); err == nil || !strings.Contains(err.Error(), "expected recorded UID") {
		t.Fatalf("replacement carrier error = %v", err)
	}
}

func TestCatalogActivationCarrierResolvesOutcomeUnknownCreate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := activationFoundationCluster()
	base := newFakeClient(t, cluster)
	createLost := errors.New("injected response loss")
	writeClient := interceptedClient(t, base, interceptor.Funcs{Create: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, options ...client.CreateOption) error {
		if _, ok := object.(*pgshardv1alpha1.PgShardCatalogActivation); !ok {
			return kubeClient.Create(ctx, object, options...)
		}
		if err := kubeClient.Create(ctx, object, options...); err != nil {
			return err
		}
		return createLost
	}})
	reconciler := &PgShardClusterReconciler{Client: writeClient, APIReader: base}
	if err := reconciler.ensureCatalogActivationCarrier(ctx, cluster); err != nil {
		t.Fatalf("outcome-unknown create did not converge: %v", err)
	}
	checkpointed := getCluster(t, ctx, base, cluster)
	if checkpointed.Status.CatalogActivation == nil || checkpointed.Status.CatalogActivation.UID == "" {
		t.Fatalf("outcome-unknown carrier was not checkpointed: %#v", checkpointed.Status.CatalogActivation)
	}
}

func TestCatalogActivationCarrierDoesNotRecreateAfterCheckpointResponseLossAndStaleCache(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	staleCluster := activationFoundationCluster()
	authoritativeCluster := staleCluster.DeepCopy()
	authoritativeCluster.Status.CatalogActivation = &pgshardv1alpha1.CatalogActivationCarrierStatus{
		Name: "activation-catalog-activation",
		UID:  "deleted-carrier-uid",
	}
	staleCache := newFakeClient(t, staleCluster)
	authoritativeAPI := newFakeClient(t, authoritativeCluster)
	createAttempted := false
	writeClient := interceptedClient(t, staleCache, interceptor.Funcs{Create: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, options ...client.CreateOption) error {
		if _, ok := object.(*pgshardv1alpha1.PgShardCatalogActivation); ok {
			createAttempted = true
		}
		return kubeClient.Create(ctx, object, options...)
	}})
	reconciler := &PgShardClusterReconciler{Client: writeClient, APIReader: authoritativeAPI}
	err := reconciler.ensureCatalogActivationCarrier(ctx, staleCluster)
	if err == nil || !strings.Contains(err.Error(), "explicit recovery is required") {
		t.Fatalf("missing checkpointed carrier error = %v", err)
	}
	if createAttempted {
		t.Fatal("stale cached status recreated a carrier after its UID checkpoint")
	}
	if staleCluster.Status.CatalogActivation == nil || staleCluster.Status.CatalogActivation.UID != "deleted-carrier-uid" {
		t.Fatalf("caller did not retain authoritative carrier checkpoint: %#v", staleCluster.Status.CatalogActivation)
	}
}

func TestCatalogActivationCarrierRejectsConflictingUncheckpointedObject(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := activationFoundationCluster()
	kubeClient := newFakeClient(t, cluster)
	carrier := pgshardv1alpha1.EmptyCatalogActivation(cluster)
	carrier.Spec.RequestSHA256 = strings.Repeat("0", 64)
	if err := kubeClient.Create(ctx, carrier); err != nil {
		t.Fatal(err)
	}
	reconciler := &PgShardClusterReconciler{Client: kubeClient, APIReader: kubeClient}
	if err := reconciler.ensureCatalogActivationCarrier(ctx, cluster); err == nil || !strings.Contains(err.Error(), "is not empty") {
		t.Fatalf("conflicting carrier error = %v", err)
	}
}
