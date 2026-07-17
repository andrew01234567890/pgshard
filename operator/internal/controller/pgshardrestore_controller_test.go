package controller

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"reflect"
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	restorepreflight "github.com/andrew01234567890/pgshard/operator/internal/restore"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

type fixedRestoreDestinationResolver struct {
	destination restorepreflight.AuthoritativeDestination
	err         error
}

func (resolver fixedRestoreDestinationResolver) ResolveDestination(context.Context, string, string) (restorepreflight.AuthoritativeDestination, error) {
	return resolver.destination, resolver.err
}

func TestRestoreTopologyMismatchChangesOnlyRestoreStatus(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	restore, keySecret := signedRestore(t, restoreTestTopology(5), restoreTestTopology(3))
	// Omission is only a caller input; the resolver still proves that the live
	// destination exists with a mismatched topology.
	restore.Spec.DestinationTopology = nil
	sentinelConfig := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "sentinel", Namespace: restore.Namespace}, Data: map[string]string{"unchanged": "true"}}
	sentinelPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "sentinel-data", Namespace: restore.Namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("4Gi"),
			}},
		},
	}
	base := newFakeClient(t, restore, keySecret, sentinelConfig, sentinelPVC)
	beforeConfig := &corev1.ConfigMap{}
	beforePVC := &corev1.PersistentVolumeClaim{}
	if err := base.Get(ctx, client.ObjectKeyFromObject(sentinelConfig), beforeConfig); err != nil {
		t.Fatal(err)
	}
	if err := base.Get(ctx, client.ObjectKeyFromObject(sentinelPVC), beforePVC); err != nil {
		t.Fatal(err)
	}

	forbiddenWrites := make([]string, 0)
	statusUpdates := 0
	writeClient := interceptedClient(t, base, interceptor.Funcs{
		Create: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, options ...client.CreateOption) error {
			forbiddenWrites = append(forbiddenWrites, "create "+object.GetObjectKind().GroupVersionKind().Kind+"/"+object.GetName())
			return kubeClient.Create(ctx, object, options...)
		},
		Update: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, options ...client.UpdateOption) error {
			forbiddenWrites = append(forbiddenWrites, "update "+object.GetObjectKind().GroupVersionKind().Kind+"/"+object.GetName())
			return kubeClient.Update(ctx, object, options...)
		},
		Patch: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, patch client.Patch, options ...client.PatchOption) error {
			forbiddenWrites = append(forbiddenWrites, "patch "+object.GetObjectKind().GroupVersionKind().Kind+"/"+object.GetName())
			return kubeClient.Patch(ctx, object, patch, options...)
		},
		Delete: func(ctx context.Context, kubeClient client.WithWatch, object client.Object, options ...client.DeleteOption) error {
			forbiddenWrites = append(forbiddenWrites, "delete "+object.GetObjectKind().GroupVersionKind().Kind+"/"+object.GetName())
			return kubeClient.Delete(ctx, object, options...)
		},
		SubResourceUpdate: func(ctx context.Context, kubeClient client.Client, subresource string, object client.Object, options ...client.SubResourceUpdateOption) error {
			if subresource == "status" {
				if _, ok := object.(*pgshardv1alpha1.PgShardRestore); ok {
					statusUpdates++
					return kubeClient.SubResource(subresource).Update(ctx, object, options...)
				}
			}
			forbiddenWrites = append(forbiddenWrites, "subresource "+subresource+"/"+object.GetName())
			return kubeClient.SubResource(subresource).Update(ctx, object, options...)
		},
	})

	reconciler := &PgShardRestoreReconciler{
		Client:              writeClient,
		APIReader:           base,
		DestinationResolver: fixedRestoreDestinationResolver{destination: restorepreflight.ExistingDestination(restoreTestTopology(3))},
	}
	if result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(restore)}); err != nil || result != (ctrl.Result{}) {
		t.Fatalf("mismatch reconciliation = result %#v, error %v", result, err)
	}
	if len(forbiddenWrites) != 0 || statusUpdates != 1 {
		t.Fatalf("mismatch writes = forbidden %#v, status updates %d", forbiddenWrites, statusUpdates)
	}

	got := &pgshardv1alpha1.PgShardRestore{}
	if err := base.Get(ctx, client.ObjectKeyFromObject(restore), got); err != nil {
		t.Fatal(err)
	}
	assertRestoreCondition(t, got, restorePreflightCondition, metav1.ConditionFalse, "RestoreTopologyMismatch")
	assertRestoreCondition(t, got, restoreReadyCondition, metav1.ConditionFalse, "RestoreTopologyMismatch")
	if got.Status.Phase != pgshardv1alpha1.RestorePhaseRejected || got.Status.TopologySHA256 == "" || got.Status.DestinationTopologySHA256 == "" || got.Status.ManifestSHA256 == "" || got.Status.VerificationKeyUID != keySecret.UID {
		t.Fatalf("mismatch status = %#v", got.Status)
	}

	afterConfig := &corev1.ConfigMap{}
	afterPVC := &corev1.PersistentVolumeClaim{}
	if err := base.Get(ctx, client.ObjectKeyFromObject(sentinelConfig), afterConfig); err != nil {
		t.Fatal(err)
	}
	if err := base.Get(ctx, client.ObjectKeyFromObject(sentinelPVC), afterPVC); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(beforeConfig, afterConfig) || !reflect.DeepEqual(beforePVC, afterPVC) {
		t.Fatalf("mismatch mutated target sentinels: config %t pvc %t", reflect.DeepEqual(beforeConfig, afterConfig), reflect.DeepEqual(beforePVC, afterPVC))
	}
}

func TestRestorePreflightPassesWithoutEnablingExecution(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	restore, keySecret := signedRestore(t, restoreTestTopology(5), restoreTestTopology(5))
	base := newFakeClient(t, restore, keySecret)
	reconciler := &PgShardRestoreReconciler{
		Client:              base,
		APIReader:           base,
		DestinationResolver: fixedRestoreDestinationResolver{destination: restorepreflight.ExistingDestination(restoreTestTopology(5))},
	}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(restore)}
	if result, err := reconciler.Reconcile(ctx, request); err != nil || result != (ctrl.Result{}) {
		t.Fatalf("exact reconciliation = result %#v, error %v", result, err)
	}
	got := &pgshardv1alpha1.PgShardRestore{}
	if err := base.Get(ctx, request.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	assertRestoreCondition(t, got, restorePreflightCondition, metav1.ConditionTrue, "AuthoritativeDestinationVerified")
	assertRestoreCondition(t, got, restoreReadyCondition, metav1.ConditionFalse, "RestoreExecutionUnavailable")
	if got.Status.Phase != pgshardv1alpha1.RestorePhasePreflightPassed || got.Status.ManifestSHA256 == "" || got.Status.TopologySHA256 == "" || got.Status.DestinationTopologySHA256 != got.Status.TopologySHA256 || got.Status.VerificationKeyUID != keySecret.UID {
		t.Fatalf("exact status = %#v", got.Status)
	}

	statusUpdates := 0
	idempotentClient := interceptedClient(t, base, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, kubeClient client.Client, subresource string, object client.Object, options ...client.SubResourceUpdateOption) error {
			statusUpdates++
			return kubeClient.SubResource(subresource).Update(ctx, object, options...)
		},
	})
	if _, err := (&PgShardRestoreReconciler{
		Client:              idempotentClient,
		APIReader:           base,
		DestinationResolver: fixedRestoreDestinationResolver{destination: restorepreflight.ExistingDestination(restoreTestTopology(5))},
	}).Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	if statusUpdates != 0 {
		t.Fatalf("idempotent preflight wrote status %d times", statusUpdates)
	}
}

func TestRestorePreflightWaitsForAuthoritativeDestinationEvidence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	restore, keySecret := signedRestore(t, restoreTestTopology(5), restoreTestTopology(5))
	restore.Spec.DestinationTopology = nil
	base := newFakeClient(t, restore, keySecret)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(restore)}
	if result, err := (&PgShardRestoreReconciler{Client: base, APIReader: base}).Reconcile(ctx, request); err != nil || result != (ctrl.Result{}) {
		t.Fatalf("unresolved destination reconciliation = result %#v, error %v", result, err)
	}
	got := &pgshardv1alpha1.PgShardRestore{}
	if err := base.Get(ctx, request.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	assertRestoreCondition(t, got, restorePreflightCondition, metav1.ConditionUnknown, "DestinationTopologyResolverUnavailable")
	assertRestoreCondition(t, got, restoreReadyCondition, metav1.ConditionFalse, "DestinationTopologyResolverUnavailable")
	if got.Status.Phase != pgshardv1alpha1.RestorePhasePending || got.Status.ManifestSHA256 == "" || got.Status.TopologySHA256 == "" || got.Status.DestinationTopologySHA256 != "" || got.Status.VerificationKeyUID != keySecret.UID {
		t.Fatalf("unresolved destination status = %#v", got.Status)
	}
}

func TestRestorePreflightChecksSignatureBeforeMismatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	restore, keySecret := signedRestore(t, restoreTestTopology(5), restoreTestTopology(3))
	restore.Spec.ManifestSignature = base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))
	base := newFakeClient(t, restore, keySecret)
	if _, err := (&PgShardRestoreReconciler{Client: base, APIReader: base}).Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(restore)}); err != nil {
		t.Fatal(err)
	}
	got := &pgshardv1alpha1.PgShardRestore{}
	if err := base.Get(ctx, client.ObjectKeyFromObject(restore), got); err != nil {
		t.Fatal(err)
	}
	assertRestoreCondition(t, got, restorePreflightCondition, metav1.ConditionFalse, "BackupManifestSignatureInvalid")
	if got.Status.TopologySHA256 != "" {
		t.Fatalf("unverified topology reached status: %#v", got.Status)
	}
}

func TestRestoreVerificationKeyMustBeExactAndImmutable(t *testing.T) {
	t.Parallel()
	restore, keySecret := signedRestore(t, restoreTestTopology(1), restoreTestTopology(1))
	_ = restore
	for _, test := range []struct {
		name   string
		mutate func(*corev1.Secret)
	}{
		{name: "mutable", mutate: func(secret *corev1.Secret) { secret.Immutable = nil }},
		{name: "wrong type", mutate: func(secret *corev1.Secret) { secret.Type = corev1.SecretTypeTLS }},
		{name: "extra key", mutate: func(secret *corev1.Secret) { secret.Data["extra"] = []byte("value") }},
		{name: "encoded key", mutate: func(secret *corev1.Secret) {
			secret.Data[restorepreflight.VerificationKeyDataKey] = []byte(base64.StdEncoding.EncodeToString(make([]byte, 32)))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			candidate := keySecret.DeepCopy()
			test.mutate(candidate)
			if _, err := restoreVerificationKey(candidate); err == nil {
				t.Fatalf("invalid verification key was accepted: %#v", candidate)
			}
		})
	}
}

func signedRestore(t *testing.T, manifestTopology, destinationTopology pgshardv1alpha1.RestoreTopology) (*pgshardv1alpha1.PgShardRestore, *corev1.Secret) {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	for index := range seed {
		seed[index] = byte(index + 1)
	}
	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	manifest := pgshardv1alpha1.RestoreManifest{
		ManifestVersion: 1,
		BackupSetID:     "backup-a-2026-07-17",
		SourceDatabase:  "A",
		Topology:        manifestTopology,
	}
	payload, err := restorepreflight.CanonicalManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	immutable := true
	keySecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "backup-verification-key", Namespace: "database", UID: "backup-key-uid"},
		Type:       corev1.SecretTypeOpaque,
		Immutable:  &immutable,
		Data:       map[string][]byte{restorepreflight.VerificationKeyDataKey: publicKey},
	}
	restore := &pgshardv1alpha1.PgShardRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "restore-a-as-b", Namespace: "database", UID: "restore-uid", Generation: 1},
		Spec: pgshardv1alpha1.PgShardRestoreSpec{
			Manifest:                 manifest,
			ManifestSignature:        base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, payload)),
			VerificationKeySecretRef: corev1.LocalObjectReference{Name: keySecret.Name},
			DestinationDatabase:      "B",
			DestinationTopology:      &destinationTopology,
		},
	}
	return restore, keySecret
}

func restoreTestTopology(shards int) pgshardv1alpha1.RestoreTopology {
	boundaries := map[int][]string{
		1: {"0", "18446744073709551616"},
		3: {"0", "6148914691236517205", "12297829382473034410", "18446744073709551616"},
		5: {"0", "3689348814741910323", "7378697629483820646", "11068046444225730969", "14757395258967641292", "18446744073709551616"},
	}[shards]
	if boundaries == nil {
		panic(fmt.Sprintf("unsupported test shard count %d", shards))
	}
	ranges := make([]pgshardv1alpha1.RestoreShardRange, shards)
	for index := range ranges {
		ranges[index] = pgshardv1alpha1.RestoreShardRange{Ordinal: int32(index), Start: boundaries[index], End: boundaries[index+1]}
	}
	return pgshardv1alpha1.RestoreTopology{
		PostgreSQLMajor: "18", HashVersion: 1, HashSeed: "7", ShardCount: int32(shards), Shards: ranges,
	}
}

func assertRestoreCondition(t *testing.T, restore *pgshardv1alpha1.PgShardRestore, conditionType string, status metav1.ConditionStatus, reason string) {
	t.Helper()
	condition := meta.FindStatusCondition(restore.Status.Conditions, conditionType)
	if condition == nil || condition.Status != status || condition.Reason != reason {
		t.Fatalf("restore condition %s = %#v, want status %s reason %s", conditionType, condition, status, reason)
	}
}
