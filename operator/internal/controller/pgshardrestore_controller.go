package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	restorepreflight "github.com/andrew01234567890/pgshard/operator/internal/restore"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	restorePreflightCondition = "PreflightPassed"
	restoreReadyCondition     = "Ready"
	restoreKeyRetryDelay      = 15 * time.Second
)

// PgShardRestoreReconciler performs only signed, mutation-free restore
// preflight. Physical restore, logical import, and activation remain disabled.
type PgShardRestoreReconciler struct {
	Client              restoreStatusClient
	APIReader           client.Reader
	DestinationResolver RestoreDestinationResolver
}

type restoreStatusClient interface {
	client.Reader
	Status() client.SubResourceWriter
}

// RestoreDestinationResolver returns catalog-derived proof of destination
// absence or its exact live routing topology.
type RestoreDestinationResolver interface {
	ResolveDestination(context.Context, string, string) (restorepreflight.AuthoritativeDestination, error)
}

// +kubebuilder:rbac:groups=pgshard.io,resources=pgshardrestores,verbs=get;list;watch
// +kubebuilder:rbac:groups=pgshard.io,resources=pgshardrestores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get

func (r *PgShardRestoreReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	restore := &pgshardv1alpha1.PgShardRestore{}
	if r.Client == nil {
		return ctrl.Result{}, fmt.Errorf("restore status client is required")
	}
	if err := r.Client.Get(ctx, request.NamespacedName, restore); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !restore.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	if r.APIReader == nil {
		return ctrl.Result{}, fmt.Errorf("authoritative API reader is required for restore verification keys")
	}

	secretName := restore.Spec.VerificationKeySecretRef.Name
	if errors := validation.IsDNS1123Subdomain(secretName); len(errors) != 0 {
		return ctrl.Result{}, r.reject(ctx, restore, "VerificationKeyInvalid", "verification key Secret name is invalid", "", "", "", "")
	}
	keySecret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: restore.Namespace, Name: secretName}
	if err := r.APIReader.Get(ctx, key, keySecret); err != nil {
		if apierrors.IsNotFound(err) {
			if statusErr := r.waiting(ctx, restore, "VerificationKeyUnavailable", "verification key Secret does not exist"); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
			return ctrl.Result{RequeueAfter: restoreKeyRetryDelay}, nil
		}
		return ctrl.Result{}, fmt.Errorf("read restore verification key Secret %s: %w", secretName, err)
	}
	publicKey, err := restoreVerificationKey(keySecret)
	if err != nil {
		return ctrl.Result{}, r.reject(ctx, restore, "VerificationKeyInvalid", err.Error(), "", "", "", "")
	}

	result, err := restorepreflight.Preflight(
		restore.Spec.Manifest,
		restore.Spec.ManifestSignature,
		publicKey,
		restore.Spec.DestinationDatabase,
		restore.Spec.DestinationTopology,
	)
	if err != nil {
		var mismatch *restorepreflight.RestoreTopologyMismatchError
		var signature *restorepreflight.SignatureError
		var manifest *restorepreflight.InvalidManifestError
		var destination *restorepreflight.InvalidDestinationError
		switch {
		case errors.As(err, &mismatch):
			return ctrl.Result{}, r.reject(ctx, restore, "RestoreTopologyMismatch", mismatch.Error(), mismatch.ManifestSHA256, mismatch.ManifestTopologyFingerprint, mismatch.DestinationTopologyFingerprint, keySecret.UID)
		case errors.As(err, &signature):
			return ctrl.Result{}, r.reject(ctx, restore, "BackupManifestSignatureInvalid", signature.Error(), "", "", "", keySecret.UID)
		case errors.As(err, &manifest):
			return ctrl.Result{}, r.reject(ctx, restore, "BackupManifestInvalid", manifest.Error(), "", "", "", keySecret.UID)
		case errors.As(err, &destination):
			return ctrl.Result{}, r.reject(ctx, restore, "RestoreRequestInvalid", destination.Error(), "", "", "", keySecret.UID)
		default:
			return ctrl.Result{}, fmt.Errorf("restore preflight failed: %w", err)
		}
	}
	if r.DestinationResolver == nil {
		return ctrl.Result{}, r.destinationUnverified(ctx, restore, result, keySecret.UID, "DestinationTopologyResolverUnavailable", "authoritative destination topology resolution is not implemented")
	}
	authoritative, err := r.DestinationResolver.ResolveDestination(ctx, restore.Namespace, restore.Spec.DestinationDatabase)
	if err != nil {
		if statusErr := r.destinationUnverified(ctx, restore, result, keySecret.UID, "DestinationTopologyUnavailable", "authoritative destination topology is temporarily unavailable"); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: restoreKeyRetryDelay}, nil
	}
	destinationFingerprint, err := restorepreflight.VerifyAuthoritativeDestination(restore.Spec.Manifest, result, authoritative)
	if err != nil {
		var mismatch *restorepreflight.RestoreTopologyMismatchError
		var destination *restorepreflight.InvalidDestinationError
		switch {
		case errors.As(err, &mismatch):
			return ctrl.Result{}, r.reject(ctx, restore, "RestoreTopologyMismatch", mismatch.Error(), mismatch.ManifestSHA256, mismatch.ManifestTopologyFingerprint, mismatch.DestinationTopologyFingerprint, keySecret.UID)
		case errors.As(err, &destination):
			return ctrl.Result{}, r.reject(ctx, restore, "AuthoritativeDestinationInvalid", destination.Error(), result.ManifestSHA256, result.TopologySHA256, "", keySecret.UID)
		default:
			return ctrl.Result{}, fmt.Errorf("verify authoritative restore destination: %w", err)
		}
	}
	return ctrl.Result{}, r.preflightPassed(ctx, restore, result, destinationFingerprint, keySecret.UID)
}

func restoreVerificationKey(secret *corev1.Secret) ([]byte, error) {
	if secret.UID == "" {
		return nil, fmt.Errorf("verification key Secret %s has no API-assigned UID", secret.Name)
	}
	if secret.DeletionTimestamp != nil || secret.Type != corev1.SecretTypeOpaque || secret.Immutable == nil || !*secret.Immutable {
		return nil, fmt.Errorf("verification key Secret %s must be an immutable, non-deleting Opaque Secret", secret.Name)
	}
	if len(secret.Data) != 1 || len(secret.Data[restorepreflight.VerificationKeyDataKey]) != 32 {
		return nil, fmt.Errorf("verification key Secret %s must contain only %s with exactly 32 raw bytes", secret.Name, restorepreflight.VerificationKeyDataKey)
	}
	return append([]byte(nil), secret.Data[restorepreflight.VerificationKeyDataKey]...), nil
}

func (r *PgShardRestoreReconciler) waiting(ctx context.Context, restore *pgshardv1alpha1.PgShardRestore, reason, message string) error {
	status := restore.Status
	status.ObservedGeneration = restore.Generation
	status.Phase = pgshardv1alpha1.RestorePhasePending
	status.ManifestSHA256 = ""
	status.TopologySHA256 = ""
	status.DestinationTopologySHA256 = ""
	status.VerificationKeyUID = ""
	setRestoreCondition(&status.Conditions, restorePreflightCondition, metav1.ConditionUnknown, reason, message, restore.Generation)
	setRestoreCondition(&status.Conditions, restoreReadyCondition, metav1.ConditionFalse, reason, message, restore.Generation)
	return r.updateRestoreStatus(ctx, restore, status)
}

func (r *PgShardRestoreReconciler) reject(ctx context.Context, restore *pgshardv1alpha1.PgShardRestore, reason, message, manifestSHA256, topologySHA256, destinationTopologySHA256 string, keyUID types.UID) error {
	status := restore.Status
	status.ObservedGeneration = restore.Generation
	status.Phase = pgshardv1alpha1.RestorePhaseRejected
	status.ManifestSHA256 = manifestSHA256
	status.TopologySHA256 = topologySHA256
	status.DestinationTopologySHA256 = destinationTopologySHA256
	status.VerificationKeyUID = keyUID
	setRestoreCondition(&status.Conditions, restorePreflightCondition, metav1.ConditionFalse, reason, message, restore.Generation)
	setRestoreCondition(&status.Conditions, restoreReadyCondition, metav1.ConditionFalse, reason, message, restore.Generation)
	return r.updateRestoreStatus(ctx, restore, status)
}

func (r *PgShardRestoreReconciler) destinationUnverified(ctx context.Context, restore *pgshardv1alpha1.PgShardRestore, result *restorepreflight.PreflightResult, keyUID types.UID, reason, message string) error {
	status := restore.Status
	status.ObservedGeneration = restore.Generation
	status.Phase = pgshardv1alpha1.RestorePhasePending
	status.ManifestSHA256 = result.ManifestSHA256
	status.TopologySHA256 = result.TopologySHA256
	status.DestinationTopologySHA256 = ""
	status.VerificationKeyUID = keyUID
	setRestoreCondition(&status.Conditions, restorePreflightCondition, metav1.ConditionUnknown, reason, message, restore.Generation)
	setRestoreCondition(&status.Conditions, restoreReadyCondition, metav1.ConditionFalse, reason, message, restore.Generation)
	return r.updateRestoreStatus(ctx, restore, status)
}

func (r *PgShardRestoreReconciler) preflightPassed(ctx context.Context, restore *pgshardv1alpha1.PgShardRestore, result *restorepreflight.PreflightResult, destinationTopologySHA256 string, keyUID types.UID) error {
	status := restore.Status
	status.ObservedGeneration = restore.Generation
	status.Phase = pgshardv1alpha1.RestorePhasePreflightPassed
	status.ManifestSHA256 = result.ManifestSHA256
	status.TopologySHA256 = result.TopologySHA256
	status.DestinationTopologySHA256 = destinationTopologySHA256
	status.VerificationKeyUID = keyUID
	setRestoreCondition(&status.Conditions, restorePreflightCondition, metav1.ConditionTrue, "AuthoritativeDestinationVerified", "signed backup manifest and authoritative destination state are verified", restore.Generation)
	setRestoreCondition(&status.Conditions, restoreReadyCondition, metav1.ConditionFalse, "RestoreExecutionUnavailable", "restore materialization is not implemented", restore.Generation)
	return r.updateRestoreStatus(ctx, restore, status)
}

func setRestoreCondition(conditions *[]metav1.Condition, conditionType string, status metav1.ConditionStatus, reason, message string, generation int64) {
	meta.SetStatusCondition(conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		ObservedGeneration: generation,
		Reason:             reason,
		Message:            message,
	})
}

func (r *PgShardRestoreReconciler) updateRestoreStatus(ctx context.Context, restore *pgshardv1alpha1.PgShardRestore, desired pgshardv1alpha1.PgShardRestoreStatus) error {
	if reflect.DeepEqual(restore.Status, desired) {
		return nil
	}
	restore.Status = desired
	if err := r.Client.Status().Update(ctx, restore); err != nil {
		return fmt.Errorf("update PgShardRestore status: %w", err)
	}
	return nil
}

func (r *PgShardRestoreReconciler) SetupWithManager(manager ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(manager).
		For(&pgshardv1alpha1.PgShardRestore{}).
		Named("pgshardrestore").
		Complete(r)
}
