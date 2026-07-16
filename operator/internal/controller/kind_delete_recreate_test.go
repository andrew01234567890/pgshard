package controller

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestKINDDeletionWaitsForPVCBeforeSameNameRecreate(t *testing.T) {
	if os.Getenv("PGSHARD_KIND_E2E") != "true" {
		t.Skip("set PGSHARD_KIND_E2E=true against a disposable KIND cluster")
	}
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := pgshardv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kubeClient, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pgshard-delete-recreate-%d", os.Getpid())}}
	if err := kubeClient.Create(ctx, namespace); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		cleanupReconciler := &PgShardClusterReconciler{Client: kubeClient, APIReader: kubeClient}
		request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: namespace.Name, Name: "example"}}
		current := &pgshardv1alpha1.PgShardCluster{}
		if err := kubeClient.Get(cleanupCtx, request.NamespacedName, current); err == nil {
			if err := kubeClient.Delete(cleanupCtx, current); err != nil && !apierrors.IsNotFound(err) {
				t.Errorf("delete cleanup cluster: %v", err)
			} else {
				waitForClusterDeletion(t, cleanupCtx, kubeClient, cleanupReconciler, request)
			}
		} else if !apierrors.IsNotFound(err) {
			t.Errorf("read cleanup cluster: %v", err)
		}
		if err := kubeClient.Delete(cleanupCtx, namespace); err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("delete cleanup namespace: %v", err)
		}
		if err := wait.PollUntilContextTimeout(cleanupCtx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
			err := kubeClient.Get(ctx, client.ObjectKeyFromObject(namespace), &corev1.Namespace{})
			return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
		}); err != nil {
			t.Errorf("wait for cleanup namespace deletion: %v", err)
		}
	})

	cluster := validCluster()
	cluster.Namespace = namespace.Name
	cluster.UID = ""
	cluster.ResourceVersion = ""
	cluster.Generation = 0
	if err := kubeClient.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}}
	reconciler := &PgShardClusterReconciler{Client: kubeClient, APIReader: kubeClient}
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	current := &pgshardv1alpha1.PgShardCluster{}
	if err := kubeClient.Get(ctx, request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	oldClusterUID := current.UID
	oldPVC := waitForEtcdPVC(t, ctx, kubeClient, cluster.Namespace, cluster.Name)
	if !metav1.IsControlledBy(oldPVC, current) {
		t.Fatalf("PVC owner = %#v, want cluster UID %s", oldPVC.OwnerReferences, current.UID)
	}
	oldPVCUID := oldPVC.UID

	if err := kubeClient.Delete(ctx, current); err != nil {
		t.Fatal(err)
	}
	waitForClusterDeletion(t, ctx, kubeClient, reconciler, request)
	missing := &corev1.PersistentVolumeClaim{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(oldPVC), missing); !apierrors.IsNotFound(err) {
		t.Fatalf("old PVC still exists after finalizer completion: %v", err)
	}

	replacement := validCluster()
	replacement.Namespace = namespace.Name
	replacement.UID = ""
	replacement.ResourceVersion = ""
	replacement.Generation = 0
	if err := kubeClient.Create(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Get(ctx, request.NamespacedName, replacement); err != nil {
		t.Fatal(err)
	}
	if replacement.UID == oldClusterUID {
		t.Fatalf("replacement reused cluster UID %s", replacement.UID)
	}
	newPVC := waitForEtcdPVC(t, ctx, kubeClient, replacement.Namespace, replacement.Name)
	if newPVC.UID == oldPVCUID {
		t.Fatalf("replacement reused stale PVC UID %s", newPVC.UID)
	}
	if !metav1.IsControlledBy(newPVC, replacement) {
		t.Fatalf("replacement PVC owner = %#v, want cluster UID %s", newPVC.OwnerReferences, replacement.UID)
	}
}

func waitForEtcdPVC(t *testing.T, ctx context.Context, kubeClient client.Client, namespace, cluster string) *corev1.PersistentVolumeClaim {
	t.Helper()
	key := types.NamespacedName{Namespace: namespace, Name: "data-" + cluster + "-etcd-0"}
	claim := &corev1.PersistentVolumeClaim{}
	err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		err := kubeClient.Get(ctx, key, claim)
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return err == nil, err
	})
	if err != nil {
		t.Fatalf("wait for etcd PVC %s: %v", key, err)
	}
	return claim
}

func waitForClusterDeletion(
	t *testing.T,
	ctx context.Context,
	kubeClient client.Client,
	reconciler *PgShardClusterReconciler,
	request ctrl.Request,
) {
	t.Helper()
	err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		if _, err := reconciler.Reconcile(ctx, request); err != nil {
			return false, err
		}
		cluster := &pgshardv1alpha1.PgShardCluster{}
		err := kubeClient.Get(ctx, request.NamespacedName, cluster)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	})
	if err != nil {
		t.Fatalf("wait for supervised cluster deletion: %v", err)
	}
}
