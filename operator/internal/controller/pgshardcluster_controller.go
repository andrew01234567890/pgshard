// Package controller contains Kubernetes reconcilers for pgshard APIs.
package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/operator/internal/podfence"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	readyCondition                    = "Ready"
	reconciledCondition               = "ResourcesReconciled"
	supportingAvailableCondition      = "SupportingWorkloadsAvailable"
	postgresqlAvailableCondition      = "PostgreSQLPrimariesAvailable"
	transportSecurityCondition        = "TransportSecurityReady"
	resourceFinalizer                 = owned.ClusterResourceFinalizer
	hpaScaleFieldManager              = "pgshard-hpa-scale"
	ownershipMigrationManager         = "pgshard-ownership-migration"
	retryDelay                        = 15 * time.Second
	bootstrapIntegrityInterval        = 30 * time.Second
	postgresqlPasswordBytes           = 32
	bootstrapNameRandomBytes          = 16
	fencingChallengeRandomBytes       = 16
	defaultPodFencingKeyNamespace     = "pgshard-system"
	defaultPodFencingKeySecret        = "pgshard-webhook-fencing-key"
	defaultPodFencingKeyData          = "hmac.key"
	defaultPodFencingAnchorSecret     = "pgshard-webhook-ca"
	defaultPodFencingAnchorAnnotation = "pgshard.io/pod-fencing-key-sha256"
)

// PgShardClusterReconciler owns safe supporting resources and single-member
// PostgreSQL primaries while failing closed on unavailable HA and SQL routing.
// Ready is never inferred merely from desired objects existing; availability
// comes from observed workload status.
type PgShardClusterReconciler struct {
	client.Client
	// APIReader bypasses the informer cache for ownership migration, HPA presence
	// gates, storage-class selection, replica handoff, deletion-finalizer absence
	// proofs, and post-apply workload status.
	// Writes and plan reconciliation continue through Client.
	APIReader            client.Reader
	Images               owned.Images
	PodFencingReceiptKey podfence.SecretReceiptKeyRef
}

// +kubebuilder:rbac:groups=pgshard.io,resources=pgshardclusters,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=pgshard.io,resources=pgshardclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pgshard.io,resources=pgshardclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps;persistentvolumeclaims;services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;patch;delete
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;create;update;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=list

func (r *PgShardClusterReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	cluster := &pgshardv1alpha1.PgShardCluster{}
	if err := r.Get(ctx, request.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	releasedPodFence, err := r.releaseTerminatedPostgreSQLPodFences(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("release terminated PostgreSQL Pod fence: %w", err)
	}
	if releasedPodFence {
		return ctrl.Result{Requeue: true}, nil
	}
	if !cluster.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(cluster, resourceFinalizer) {
			return ctrl.Result{}, nil
		}
		deletionPolicy, err := provisionedStorageDeletionPolicy(cluster)
		if err != nil {
			return ctrl.Result{}, err
		}
		// The protected data-PVC name must remain reserved until every workload
		// that can mount it is gone. This prevents a same-name late create from
		// ever reaching the PostgreSQL bootstrap path.
		if err := r.verifyPostgreSQLPodTerminationFences(ctx, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("verify PostgreSQL Pod termination fences before pruning workloads: %w", err)
		}
		remaining, err := r.prune(ctx, cluster, nil, true)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("prune resources before finalizing PostgreSQL data: %w", err)
		}
		if remaining {
			return ctrl.Result{RequeueAfter: retryDelay}, nil
		}
		resolvingData, err := r.resolvePostgreSQLPVCOutcomesForFinalization(ctx, cluster, deletionPolicy)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("resolve PostgreSQL data outcomes during cluster deletion: %w", err)
		}
		if resolvingData {
			return ctrl.Result{Requeue: true}, nil
		}
		deletingFence, err := r.deletePostgreSQLCredentialFences(ctx, cluster, deletionPolicy)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("delete PostgreSQL PVC creation fences during cluster deletion: %w", err)
		}
		if deletingFence {
			return ctrl.Result{RequeueAfter: retryDelay}, nil
		}
		deletingPods, err := r.deletePostgreSQLPodsForFinalization(ctx, cluster)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("stop PostgreSQL Pods before finalizing data: %w", err)
		}
		if deletingPods {
			return ctrl.Result{RequeueAfter: retryDelay}, nil
		}
		if deletionPolicy == pgshardv1alpha1.DeletionRetain {
			retained, err := r.retainPostgreSQLPVCs(ctx, cluster)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("retain PostgreSQL data during cluster deletion: %w", err)
			}
			if retained {
				return ctrl.Result{Requeue: true}, nil
			}
		} else {
			deleting, err := r.deletePostgreSQLPVCs(ctx, cluster)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("delete PostgreSQL data during cluster deletion: %w", err)
			}
			if deleting {
				return ctrl.Result{RequeueAfter: retryDelay}, nil
			}
		}
		controllerutil.RemoveFinalizer(cluster, resourceFinalizer)
		if err := r.Update(ctx, cluster); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}
	images := r.Images
	if images == (owned.Images{}) {
		images = owned.DefaultImages()
	}
	if err := pgshardv1alpha1.ValidateClusterForReconciliation(cluster); err != nil {
		statusErr := r.reportFailure(ctx, cluster, "PlanInvalid", fmt.Sprintf("cannot safely plan owned resources: %v", err))
		return ctrl.Result{}, errors.Join(err, statusErr)
	}
	if err := owned.ValidateImagesForCluster(cluster, images); err != nil {
		statusErr := r.reportFailure(ctx, cluster, "PlanInvalid", fmt.Sprintf("cannot safely plan owned resources: %v", err))
		return ctrl.Result{}, errors.Join(err, statusErr)
	}
	if err := r.verifyPostgreSQLPodFencingNamespace(ctx, cluster); err != nil {
		statusErr := r.reportFailure(ctx, cluster, "PodFencingUnavailable", fmt.Sprintf("PostgreSQL Pod fencing is unavailable: %v", err))
		return ctrl.Result{}, errors.Join(err, statusErr)
	}
	fencingReady, err := r.ensurePostgreSQLPodFencingHandshake(ctx, cluster)
	if err != nil {
		statusErr := r.reportFailure(ctx, cluster, "PodFencingUnavailable", fmt.Sprintf("PostgreSQL Pod fencing admission handshake failed: %v", err))
		return ctrl.Result{}, errors.Join(err, statusErr)
	}
	if !fencingReady {
		return ctrl.Result{Requeue: true}, nil
	}
	if !controllerutil.ContainsFinalizer(cluster, resourceFinalizer) {
		controllerutil.AddFinalizer(cluster, resourceFinalizer)
		if err := r.Update(ctx, cluster); err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := r.ensurePostgreSQLBootstrap(ctx, cluster); err != nil {
		statusErr := r.reportFailure(ctx, cluster, "BootstrapReconcileFailed", fmt.Sprintf("PostgreSQL bootstrap reconciliation failed: %v", err))
		return ctrl.Result{}, errors.Join(err, statusErr)
	}
	plan, err := owned.Plan(cluster, images)
	if err != nil {
		statusErr := r.reportFailure(ctx, cluster, "PlanInvalid", fmt.Sprintf("cannot safely plan owned resources: %v", err))
		return ctrl.Result{}, errors.Join(err, statusErr)
	}
	states, err := r.preflightOwnership(ctx, cluster, plan)
	if err != nil {
		statusErr := r.reportFailure(ctx, cluster, "ReconcileFailed", fmt.Sprintf("owned resource reconciliation failed: %v", err))
		return ctrl.Result{}, errors.Join(err, statusErr)
	}
	staleHPA, err := r.ownedHPAForFixedTransition(ctx, cluster)
	if err != nil {
		statusErr := r.reportFailure(ctx, cluster, "ReconcileFailed", fmt.Sprintf("pooler scaling transition failed: %v", err))
		return ctrl.Result{}, errors.Join(err, statusErr)
	}
	if staleHPA != nil {
		uid := staleHPA.UID
		resourceVersion := staleHPA.ResourceVersion
		if err := r.Delete(ctx, staleHPA, client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("delete HPA before fixed scaling: %w", err)
		}
		if err := r.reportScalingTransition(ctx, cluster); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	if err := r.applyPlan(ctx, cluster, plan, states); err != nil {
		statusErr := r.reportFailure(ctx, cluster, "ReconcileFailed", fmt.Sprintf("owned resource reconciliation failed: %v", err))
		return ctrl.Result{}, errors.Join(err, statusErr)
	}

	available, message, err := r.supportingWorkloadsAvailable(ctx, cluster)
	if err != nil {
		statusErr := r.reportFailure(ctx, cluster, "ObservationFailed", fmt.Sprintf("cannot observe supporting workloads: %v", err))
		return ctrl.Result{}, errors.Join(err, statusErr)
	}
	postgresqlAvailable, postgresqlReason, postgresqlMessage, err := r.postgresqlWorkloadsAvailable(ctx, cluster)
	if err != nil {
		statusErr := r.reportFailure(ctx, cluster, "ObservationFailed", fmt.Sprintf("cannot observe PostgreSQL workloads: %v", err))
		return ctrl.Result{}, errors.Join(err, statusErr)
	}
	if err := r.reportSuccess(ctx, cluster, available, message, postgresqlAvailable, postgresqlReason, postgresqlMessage); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	if !available {
		return ctrl.Result{RequeueAfter: retryDelay}, nil
	}
	if cluster.Spec.MembersPerShard == 1 {
		return ctrl.Result{RequeueAfter: bootstrapIntegrityInterval}, nil
	}
	return ctrl.Result{}, nil
}

func (r *PgShardClusterReconciler) verifyPostgreSQLPodFencingNamespace(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) error {
	if cluster.Spec.MembersPerShard != 1 {
		return nil
	}
	namespace := &corev1.Namespace{}
	if err := r.authoritativeReader().Get(ctx, types.NamespacedName{Name: cluster.Namespace}, namespace); err != nil {
		return fmt.Errorf("read namespace %s before PostgreSQL creation: %w", cluster.Namespace, err)
	}
	if namespace.DeletionTimestamp != nil {
		return fmt.Errorf("namespace %s is deleting", cluster.Namespace)
	}
	if namespace.Labels[podfence.NamespaceLabel] != podfence.NamespaceLabelValue {
		return fmt.Errorf("namespace %s must be labelled %s=%s before PostgreSQL creation", cluster.Namespace, podfence.NamespaceLabel, podfence.NamespaceLabelValue)
	}
	return nil
}

func (r *PgShardClusterReconciler) ensurePostgreSQLPodFencingHandshake(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (bool, error) {
	if cluster.Spec.MembersPerShard != 1 {
		return true, nil
	}
	verified, err := r.podFencingHandshakeCodec().Verify(ctx, cluster)
	if err != nil {
		return false, fmt.Errorf("verify Pod fencing admission receipt: %w", err)
	}
	if verified {
		return true, nil
	}
	random := make([]byte, fencingChallengeRandomBytes)
	if _, err := rand.Read(random); err != nil {
		return false, fmt.Errorf("generate Pod fencing admission challenge: %w", err)
	}
	before := cluster.DeepCopy()
	if cluster.Annotations == nil {
		cluster.Annotations = make(map[string]string, 1)
	}
	cluster.Annotations[podfence.HandshakeChallengeAnnotation] = hex.EncodeToString(random)
	delete(cluster.Annotations, podfence.HandshakeReceiptAnnotation)
	if err := r.Patch(ctx, cluster, client.MergeFrom(before), client.FieldOwner(owned.ManagedByValue)); err != nil {
		return false, fmt.Errorf("request Pod fencing admission handshake: %w", err)
	}
	return false, nil
}

func (r *PgShardClusterReconciler) podFencingHandshakeCodec() *podfence.HandshakeCodec {
	ref := r.PodFencingReceiptKey
	secret := ref.Secret
	if secret.Namespace == "" {
		secret.Namespace = defaultPodFencingKeyNamespace
	}
	if secret.Name == "" {
		secret.Name = defaultPodFencingKeySecret
	}
	dataKey := ref.DataKey
	if dataKey == "" {
		dataKey = defaultPodFencingKeyData
	}
	anchor := ref.AnchorSecret
	if anchor.Namespace == "" {
		anchor.Namespace = defaultPodFencingKeyNamespace
	}
	if anchor.Name == "" {
		anchor.Name = defaultPodFencingAnchorSecret
	}
	anchorAnnotation := ref.AnchorAnnotation
	if anchorAnnotation == "" {
		anchorAnnotation = defaultPodFencingAnchorAnnotation
	}
	return podfence.NewSecretHandshakeCodec(r.authoritativeReader(), podfence.SecretReceiptKeyRef{
		Secret: secret, DataKey: dataKey, AnchorSecret: anchor, AnchorAnnotation: anchorAnnotation,
	})
}

func (r *PgShardClusterReconciler) ensurePostgreSQLBootstrap(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) error {
	if cluster.Status.PostgreSQLBootstrapSpec == nil {
		if cluster.Spec.MembersPerShard != 1 {
			return nil
		}
		cluster.Status.PostgreSQLBootstrapSpec = bootstrapSpecStatus(cluster)
		if err := r.Status().Update(ctx, cluster); err != nil {
			return fmt.Errorf("checkpoint PostgreSQL provisioned spec: %w", err)
		}
	}
	if err := validateBootstrapSpecStatus(cluster); err != nil {
		return err
	}
	if cluster.Spec.MembersPerShard != 1 {
		return nil
	}

	reader := r.authoritativeReader()
	storageClassName, err := resolvePostgreSQLStorageClass(ctx, reader, cluster)
	if err != nil {
		return err
	}
	indices := make(map[int32]int, len(cluster.Status.PostgreSQLBootstraps))
	resourceNames := make(map[string]struct{}, 2*len(cluster.Status.PostgreSQLBootstraps))
	resourceUIDs := make(map[types.UID]struct{}, 2*len(cluster.Status.PostgreSQLBootstraps))
	for index, bootstrap := range cluster.Status.PostgreSQLBootstraps {
		if bootstrap.Shard < 0 || bootstrap.Shard >= cluster.Spec.Shards || bootstrap.SecretName == "" || bootstrap.PVCName == "" || bootstrap.PVCStorageClassName == nil {
			return fmt.Errorf("recorded PostgreSQL bootstrap for shard %d is invalid", bootstrap.Shard)
		}
		if !optionalStringsEqual(bootstrap.PVCStorageClassName, storageClassName) {
			return fmt.Errorf("recorded PostgreSQL storage class for shard %d differs from the provisioned creation intent", bootstrap.Shard)
		}
		if bootstrap.SecretUID == "" && (bootstrap.PVCFenceDetached || bootstrap.PVCUID != "") {
			return fmt.Errorf("recorded PostgreSQL bootstrap for shard %d identifies a data creation fence before its credential", bootstrap.Shard)
		}
		if bootstrap.PVCUID != "" && !bootstrap.PVCFenceDetached {
			return fmt.Errorf("recorded PostgreSQL bootstrap for shard %d identifies data before its creation fence was detached", bootstrap.Shard)
		}
		if bootstrap.PVCCreationAbandoned && (bootstrap.SecretUID == "" || !bootstrap.PVCFenceDetached || bootstrap.PVCUID != "") {
			return fmt.Errorf("recorded PostgreSQL bootstrap for shard %d has an invalid abandoned PVC creation outcome", bootstrap.Shard)
		}
		if _, duplicate := indices[bootstrap.Shard]; duplicate {
			return fmt.Errorf("recorded PostgreSQL bootstrap for shard %d is duplicated", bootstrap.Shard)
		}
		for _, name := range []string{bootstrap.SecretName, bootstrap.PVCName} {
			if _, duplicate := resourceNames[name]; duplicate {
				return fmt.Errorf("recorded PostgreSQL bootstrap resource name %s is duplicated", name)
			}
			resourceNames[name] = struct{}{}
		}
		for _, uid := range []types.UID{bootstrap.SecretUID, bootstrap.PVCUID} {
			if uid == "" {
				continue
			}
			if _, duplicate := resourceUIDs[uid]; duplicate {
				return fmt.Errorf("recorded PostgreSQL bootstrap resource UID %s is duplicated", uid)
			}
			resourceUIDs[uid] = struct{}{}
		}
		indices[bootstrap.Shard] = index
	}

	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		index, exists := indices[shard]
		if !exists {
			secretName, err := randomBootstrapName(owned.PostgreSQLAuthSecretPrefix(cluster.Name, shard))
			if err != nil {
				return err
			}
			pvcName, err := randomBootstrapName(owned.PostgreSQLPrimaryDataPVCPrefix(cluster.Name, shard))
			if err != nil {
				return err
			}
			cluster.Status.PostgreSQLBootstraps = append(cluster.Status.PostgreSQLBootstraps, pgshardv1alpha1.PostgreSQLBootstrapStatus{
				Shard: shard, SecretName: secretName, PVCName: pvcName, PVCStorageClassName: copyOptionalString(storageClassName),
			})
			sort.Slice(cluster.Status.PostgreSQLBootstraps, func(left, right int) bool {
				return cluster.Status.PostgreSQLBootstraps[left].Shard < cluster.Status.PostgreSQLBootstraps[right].Shard
			})
			if err := r.Status().Update(ctx, cluster); err != nil {
				return fmt.Errorf("checkpoint PostgreSQL bootstrap creation intent for shard %d: %w", shard, err)
			}
			for candidate := range cluster.Status.PostgreSQLBootstraps {
				if cluster.Status.PostgreSQLBootstraps[candidate].Shard == shard {
					index = candidate
					break
				}
			}
		}

		bootstrap := &cluster.Status.PostgreSQLBootstraps[index]
		secret, err := r.ensurePostgreSQLAuthSecret(ctx, reader, cluster, shard, *bootstrap)
		if err != nil {
			return err
		}
		if bootstrap.SecretUID != "" && secret.UID != bootstrap.SecretUID {
			return fmt.Errorf("credential Secret %s has UID %s, expected recorded UID %s", bootstrap.SecretName, secret.UID, bootstrap.SecretUID)
		}
		if err := validatePostgreSQLAuthSecret(secret, cluster, shard, bootstrap.SecretName); err != nil {
			return err
		}
		if bootstrap.SecretUID == "" {
			if !postgresqlCredentialIsClusterFenced(secret, cluster) {
				return fmt.Errorf("uncheckpointed credential Secret %s is not controlled by the exact cluster", secret.Name)
			}
			bootstrap.SecretUID = secret.UID
			if err := r.Status().Update(ctx, cluster); err != nil {
				return fmt.Errorf("checkpoint PostgreSQL credential identity for shard %d: %w", shard, err)
			}
			// Status updates decode the API response back into cluster and may
			// replace the slice backing array. Never retain an element pointer
			// across that boundary.
			bootstrap = &cluster.Status.PostgreSQLBootstraps[index]
		}
		if !bootstrap.PVCFenceDetached {
			if err := r.detachPostgreSQLCredentialFence(ctx, secret, cluster); err != nil {
				return fmt.Errorf("detach PostgreSQL PVC creation fence %s: %w", secret.Name, err)
			}
			bootstrap.PVCFenceDetached = true
			if err := r.Status().Update(ctx, cluster); err != nil {
				return fmt.Errorf("checkpoint detached PostgreSQL PVC creation fence for shard %d: %w", shard, err)
			}
			bootstrap = &cluster.Status.PostgreSQLBootstraps[index]
		}
		if bootstrap.PVCUID == "" && len(secret.OwnerReferences) != 0 {
			return fmt.Errorf("PostgreSQL PVC creation fence %s is still garbage-collection-owned", secret.Name)
		}
		if bootstrap.PVCUID != "" && len(secret.OwnerReferences) != 0 && !postgresqlCredentialIsDataAnchored(secret, *bootstrap) {
			return fmt.Errorf("PostgreSQL credential tombstone %s has an unexpected garbage-collection owner", secret.Name)
		}

		storageSize, err := provisionedPostgreSQLStorageSize(cluster)
		if err != nil {
			return err
		}
		claim, err := r.ensurePostgreSQLDataPVC(ctx, reader, cluster, shard, *bootstrap, storageSize)
		if err != nil {
			return err
		}
		if bootstrap.PVCUID != "" && claim.UID != bootstrap.PVCUID {
			return fmt.Errorf("PostgreSQL data PVC %s has UID %s, expected recorded UID %s", bootstrap.PVCName, claim.UID, bootstrap.PVCUID)
		}
		if err := validatePostgreSQLDataPVC(claim, cluster, shard, *bootstrap, storageSize); err != nil {
			return err
		}
		if bootstrap.PVCUID == "" {
			if !postgresqlDataPVCIsCreationFenced(claim, *bootstrap) {
				return fmt.Errorf("PostgreSQL data PVC %s lost its credential creation fence before its API UID was checkpointed", bootstrap.PVCName)
			}
			bootstrap.PVCUID = claim.UID
			if err := r.Status().Update(ctx, cluster); err != nil {
				return fmt.Errorf("checkpoint PostgreSQL data identity for shard %d: %w", shard, err)
			}
			bootstrap = &cluster.Status.PostgreSQLBootstraps[index]
		}
		if err := r.stabilizePostgreSQLDataFence(ctx, secret, claim, *bootstrap); err != nil {
			return fmt.Errorf("stabilize PostgreSQL data PVC %s: %w", claim.Name, err)
		}
	}
	return nil
}

func resolvePostgreSQLStorageClass(ctx context.Context, reader client.Reader, cluster *pgshardv1alpha1.PgShardCluster) (*string, error) {
	if cluster.Spec.Storage.StorageClassName != nil {
		return copyOptionalString(cluster.Spec.Storage.StorageClassName), nil
	}

	var recorded *string
	for _, bootstrap := range cluster.Status.PostgreSQLBootstraps {
		if bootstrap.PVCStorageClassName == nil {
			return nil, fmt.Errorf("recorded PostgreSQL bootstrap for shard %d has no resolved storage class", bootstrap.Shard)
		}
		if recorded == nil {
			recorded = copyOptionalString(bootstrap.PVCStorageClassName)
			continue
		}
		if !optionalStringsEqual(recorded, bootstrap.PVCStorageClassName) {
			return nil, fmt.Errorf("recorded PostgreSQL bootstraps disagree on the resolved storage class")
		}
	}
	if recorded != nil {
		return recorded, nil
	}

	classes := &storagev1.StorageClassList{}
	if err := reader.List(ctx, classes); err != nil {
		return nil, fmt.Errorf("list StorageClasses before PostgreSQL data creation: %w", err)
	}
	defaults := make([]storagev1.StorageClass, 0, len(classes.Items))
	for _, storageClass := range classes.Items {
		if isDefaultStorageClass(storageClass.Annotations) {
			defaults = append(defaults, storageClass)
		}
	}
	if len(defaults) == 0 {
		return nil, fmt.Errorf("no default StorageClass is available; set spec.storage.storageClassName explicitly")
	}
	// Match Kubernetes's persistent-volume controller: newest default wins,
	// with ascending name as a deterministic tie-breaker.
	sort.Slice(defaults, func(left, right int) bool {
		leftCreated := defaults[left].CreationTimestamp.UnixNano()
		rightCreated := defaults[right].CreationTimestamp.UnixNano()
		if leftCreated == rightCreated {
			return defaults[left].Name < defaults[right].Name
		}
		return leftCreated > rightCreated
	})
	selected := defaults[0].Name
	return &selected, nil
}

func isDefaultStorageClass(annotations map[string]string) bool {
	return annotations["storageclass.kubernetes.io/is-default-class"] == "true" ||
		annotations["storageclass.beta.kubernetes.io/is-default-class"] == "true"
}

func bootstrapSpecStatus(cluster *pgshardv1alpha1.PgShardCluster) *pgshardv1alpha1.PostgreSQLBootstrapSpecStatus {
	return &pgshardv1alpha1.PostgreSQLBootstrapSpecStatus{
		Shards: cluster.Spec.Shards, MembersPerShard: cluster.Spec.MembersPerShard, Durability: cluster.Spec.Durability,
		StorageSize: cluster.Spec.Storage.Size.String(), StorageClassName: copyOptionalString(cluster.Spec.Storage.StorageClassName), DeletionPolicy: storageDeletionPolicy(cluster),
	}
}

func validateBootstrapSpecStatus(cluster *pgshardv1alpha1.PgShardCluster) error {
	recorded := cluster.Status.PostgreSQLBootstrapSpec
	wanted := bootstrapSpecStatus(cluster)
	if recorded.Shards != wanted.Shards || recorded.MembersPerShard != wanted.MembersPerShard || recorded.Durability != wanted.Durability || recorded.StorageSize != wanted.StorageSize || !optionalStringsEqual(recorded.StorageClassName, wanted.StorageClassName) || recorded.DeletionPolicy != wanted.DeletionPolicy {
		return fmt.Errorf("current topology or storage differs from the provisioned PostgreSQL bootstrap spec; an explicit transition is required")
	}
	return nil
}

func (r *PgShardClusterReconciler) ensurePostgreSQLAuthSecret(ctx context.Context, reader client.Reader, cluster *pgshardv1alpha1.PgShardCluster, shard int32, bootstrap pgshardv1alpha1.PostgreSQLBootstrapStatus) (*corev1.Secret, error) {
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.SecretName}
	if err := reader.Get(ctx, key, secret); err == nil {
		return secret, nil
	} else if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("read credential Secret %s: %w", bootstrap.SecretName, err)
	} else if bootstrap.SecretUID != "" {
		return nil, fmt.Errorf("credential Secret %s with recorded UID %s is missing; explicit recovery is required", bootstrap.SecretName, bootstrap.SecretUID)
	}

	random := make([]byte, postgresqlPasswordBytes)
	if _, err := rand.Read(random); err != nil {
		return nil, fmt.Errorf("generate PostgreSQL credential for shard %d: %w", shard, err)
	}
	password := make([]byte, hex.EncodedLen(len(random)))
	hex.Encode(password, random)
	desired := owned.PostgreSQLAuthSecret(cluster, shard, bootstrap.SecretName, password)
	if err := r.Create(ctx, desired, client.FieldOwner(owned.ManagedByValue)); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("create credential Secret %s: %w", bootstrap.SecretName, err)
		}
		if err := reader.Get(ctx, key, secret); err != nil {
			return nil, fmt.Errorf("read concurrently created credential Secret %s: %w", bootstrap.SecretName, err)
		}
		return secret, nil
	}
	if desired.UID == "" {
		return nil, fmt.Errorf("created credential Secret %s has no API-assigned UID", bootstrap.SecretName)
	}
	return desired, nil
}

func (r *PgShardClusterReconciler) ensurePostgreSQLDataPVC(ctx context.Context, reader client.Reader, cluster *pgshardv1alpha1.PgShardCluster, shard int32, bootstrap pgshardv1alpha1.PostgreSQLBootstrapStatus, storageSize resource.Quantity) (*corev1.PersistentVolumeClaim, error) {
	claim := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}
	if err := reader.Get(ctx, key, claim); err == nil {
		if claim.DeletionTimestamp != nil {
			return nil, fmt.Errorf("PostgreSQL data PVC %s with recorded UID %s is deleting; restore is required", bootstrap.PVCName, bootstrap.PVCUID)
		}
		return claim, nil
	} else if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("read PostgreSQL data PVC %s: %w", bootstrap.PVCName, err)
	} else if bootstrap.PVCUID != "" {
		return nil, fmt.Errorf("PostgreSQL data PVC %s with recorded UID %s is missing; restore is required", bootstrap.PVCName, bootstrap.PVCUID)
	}

	if bootstrap.SecretUID == "" || !bootstrap.PVCFenceDetached {
		return nil, fmt.Errorf("PostgreSQL data PVC %s cannot be created before its credential fence is detached", bootstrap.PVCName)
	}
	desired := owned.PostgreSQLPrimaryDataPVC(cluster, shard, bootstrap.PVCName, storageSize, bootstrap.PVCStorageClassName, bootstrap.SecretName, bootstrap.SecretUID)
	if err := r.Create(ctx, desired, client.FieldOwner(owned.ManagedByValue)); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("create PostgreSQL data PVC %s: %w", bootstrap.PVCName, err)
		}
		if err := reader.Get(ctx, key, claim); err != nil {
			return nil, fmt.Errorf("read concurrently created PostgreSQL data PVC %s: %w", bootstrap.PVCName, err)
		}
		return claim, nil
	}
	if desired.UID == "" {
		return nil, fmt.Errorf("created PostgreSQL data PVC %s has no API-assigned UID", bootstrap.PVCName)
	}
	return desired, nil
}

func randomBootstrapName(prefix string) (string, error) {
	random := make([]byte, bootstrapNameRandomBytes)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("generate bootstrap resource name: %w", err)
	}
	return prefix + hex.EncodeToString(random), nil
}

func validatePostgreSQLAuthSecret(secret *corev1.Secret, cluster *pgshardv1alpha1.PgShardCluster, shard int32, expectedName string) error {
	if secret.Annotations[owned.PostgreSQLBootstrapClusterUIDAnnotation] != string(cluster.UID) {
		return fmt.Errorf("credential Secret is not bound to PgShardCluster UID %s", cluster.UID)
	}
	if secret.Name != expectedName || secret.DeletionTimestamp != nil || secret.Labels[owned.ClusterLabel] != cluster.Name || secret.Labels[owned.ComponentLabel] != "postgresql" || secret.Labels[owned.ShardLabel] != fmt.Sprintf("%04d", shard) {
		return fmt.Errorf("credential Secret metadata does not match shard %d", shard)
	}
	if secret.Type != corev1.SecretTypeOpaque || secret.Immutable == nil || !*secret.Immutable {
		return fmt.Errorf("credential Secret must be immutable and have type Opaque")
	}
	if len(secret.Data) != 1 {
		return fmt.Errorf("credential Secret has an unexpected key set")
	}
	password, ok := secret.Data[owned.PostgreSQLPasswordKey]
	if !ok || len(password) != hex.EncodedLen(postgresqlPasswordBytes) {
		return fmt.Errorf("credential Secret password has an invalid shape")
	}
	decoded := make([]byte, postgresqlPasswordBytes)
	if _, err := hex.Decode(decoded, password); err != nil {
		return fmt.Errorf("credential Secret password is not canonical hexadecimal")
	}
	return nil
}

func validatePostgreSQLDataPVC(claim *corev1.PersistentVolumeClaim, cluster *pgshardv1alpha1.PgShardCluster, shard int32, bootstrap pgshardv1alpha1.PostgreSQLBootstrapStatus, storageSize resource.Quantity) error {
	return validatePostgreSQLDataPVCState(claim, cluster, shard, bootstrap, storageSize, false)
}

func validatePostgreSQLDataPVCForFinalization(claim *corev1.PersistentVolumeClaim, cluster *pgshardv1alpha1.PgShardCluster, shard int32, bootstrap pgshardv1alpha1.PostgreSQLBootstrapStatus, storageSize resource.Quantity) error {
	return validatePostgreSQLDataPVCState(claim, cluster, shard, bootstrap, storageSize, true)
}

func validatePostgreSQLDataPVCState(claim *corev1.PersistentVolumeClaim, cluster *pgshardv1alpha1.PgShardCluster, shard int32, bootstrap pgshardv1alpha1.PostgreSQLBootstrapStatus, storageSize resource.Quantity, allowDeleting bool) error {
	if claim.Annotations[owned.PostgreSQLDataClusterUIDAnnotation] != string(cluster.UID) {
		return fmt.Errorf("PostgreSQL data PVC is not bound to PgShardCluster UID %s", cluster.UID)
	}
	if claim.Name != bootstrap.PVCName || (!allowDeleting && claim.DeletionTimestamp != nil) || claim.Labels[owned.ClusterLabel] != cluster.Name || claim.Labels[owned.ComponentLabel] != "postgresql" || claim.Labels[owned.ShardLabel] != fmt.Sprintf("%04d", shard) || claim.Labels[owned.RoleLabel] != "primary" || claim.Labels[owned.MemberLabel] != "0000" {
		return fmt.Errorf("PostgreSQL data PVC metadata does not match shard %d", shard)
	}
	if len(claim.Spec.AccessModes) != 1 || claim.Spec.AccessModes[0] != corev1.ReadWriteOnce || claim.Spec.Selector != nil || claim.Spec.DataSource != nil || claim.Spec.DataSourceRef != nil {
		return fmt.Errorf("PostgreSQL data PVC %s has an unexpected access or data-source contract", claim.Name)
	}
	if claim.Spec.VolumeMode != nil && *claim.Spec.VolumeMode != corev1.PersistentVolumeFilesystem {
		return fmt.Errorf("PostgreSQL data PVC %s is not a filesystem volume", claim.Name)
	}
	requested, ok := claim.Spec.Resources.Requests[corev1.ResourceStorage]
	if !ok || requested.Cmp(storageSize) != 0 {
		return fmt.Errorf("PostgreSQL data PVC %s capacity differs from the provisioned size", claim.Name)
	}
	if bootstrap.PVCStorageClassName == nil || !optionalStringsEqual(claim.Spec.StorageClassName, bootstrap.PVCStorageClassName) {
		return fmt.Errorf("PostgreSQL data PVC %s storage class differs from its recorded API value", claim.Name)
	}
	return nil
}

func postgresqlCredentialIsClusterFenced(secret *corev1.Secret, cluster *pgshardv1alpha1.PgShardCluster) bool {
	if len(secret.OwnerReferences) != 1 {
		return false
	}
	owner := secret.OwnerReferences[0]
	return owner.APIVersion == pgshardv1alpha1.GroupVersion.String() &&
		owner.Kind == "PgShardCluster" &&
		owner.Name == cluster.Name &&
		owner.UID == cluster.UID &&
		owner.Controller != nil && *owner.Controller &&
		owner.BlockOwnerDeletion != nil && *owner.BlockOwnerDeletion
}

func postgresqlDataPVCIsCreationFenced(claim *corev1.PersistentVolumeClaim, bootstrap pgshardv1alpha1.PostgreSQLBootstrapStatus) bool {
	if len(claim.OwnerReferences) != 1 {
		return false
	}
	owner := claim.OwnerReferences[0]
	return bootstrap.SecretUID != "" && bootstrap.PVCFenceDetached &&
		owner.APIVersion == corev1.SchemeGroupVersion.String() &&
		owner.Kind == "Secret" &&
		owner.Name == bootstrap.SecretName &&
		owner.UID == bootstrap.SecretUID &&
		owner.Controller != nil && *owner.Controller &&
		owner.BlockOwnerDeletion != nil && *owner.BlockOwnerDeletion
}

func postgresqlCredentialIsDataAnchored(secret *corev1.Secret, bootstrap pgshardv1alpha1.PostgreSQLBootstrapStatus) bool {
	if len(secret.OwnerReferences) != 1 {
		return false
	}
	owner := secret.OwnerReferences[0]
	return bootstrap.PVCUID != "" &&
		owner.APIVersion == corev1.SchemeGroupVersion.String() &&
		owner.Kind == "PersistentVolumeClaim" &&
		owner.Name == bootstrap.PVCName &&
		owner.UID == bootstrap.PVCUID &&
		owner.Controller != nil && *owner.Controller &&
		owner.BlockOwnerDeletion != nil && *owner.BlockOwnerDeletion
}

func postgresqlDataPVCIsProtected(claim *corev1.PersistentVolumeClaim) bool {
	return slices.Contains(claim.Finalizers, owned.PostgreSQLDataProtectionFinalizer)
}

func (r *PgShardClusterReconciler) stabilizePostgreSQLDataFence(ctx context.Context, secret *corev1.Secret, claim *corev1.PersistentVolumeClaim, bootstrap pgshardv1alpha1.PostgreSQLBootstrapStatus) error {
	if secret.UID != bootstrap.SecretUID || claim.UID != bootstrap.PVCUID {
		return fmt.Errorf("resource identity differs from the recorded Secret or PVC UID")
	}
	if !postgresqlDataPVCIsProtected(claim) {
		controllerutil.AddFinalizer(claim, owned.PostgreSQLDataProtectionFinalizer)
		if err := r.Update(ctx, claim, client.FieldOwner(owned.ManagedByValue)); err != nil {
			return fmt.Errorf("protect exact PostgreSQL data PVC: %w", err)
		}
	}
	if len(claim.OwnerReferences) != 0 {
		if !postgresqlDataPVCIsCreationFenced(claim, bootstrap) {
			return fmt.Errorf("data PVC has an unexpected garbage-collection owner")
		}
		claim.OwnerReferences = nil
		if err := r.Update(ctx, claim, client.FieldOwner(owned.ManagedByValue)); err != nil {
			return fmt.Errorf("detach exact PostgreSQL data PVC from its creation fence: %w", err)
		}
	}
	if !postgresqlDataPVCIsProtected(claim) || len(claim.OwnerReferences) != 0 {
		return fmt.Errorf("data PVC is not independently protected and ownerless")
	}
	if postgresqlCredentialIsDataAnchored(secret, bootstrap) {
		return nil
	}
	if len(secret.OwnerReferences) != 0 {
		return fmt.Errorf("credential tombstone has an unexpected garbage-collection owner")
	}
	controller := true
	blockDeletion := true
	secret.OwnerReferences = []metav1.OwnerReference{{
		APIVersion:         corev1.SchemeGroupVersion.String(),
		Kind:               "PersistentVolumeClaim",
		Name:               bootstrap.PVCName,
		UID:                bootstrap.PVCUID,
		Controller:         &controller,
		BlockOwnerDeletion: &blockDeletion,
	}}
	if err := r.Update(ctx, secret, client.FieldOwner(owned.ManagedByValue)); err != nil {
		return fmt.Errorf("anchor credential tombstone to exact PostgreSQL data PVC: %w", err)
	}
	return nil
}

func (r *PgShardClusterReconciler) detachPostgreSQLCredentialFence(ctx context.Context, secret *corev1.Secret, cluster *pgshardv1alpha1.PgShardCluster) error {
	if len(secret.OwnerReferences) == 0 {
		return nil
	}
	if !postgresqlCredentialIsClusterFenced(secret, cluster) {
		return fmt.Errorf("Secret does not have the exact PgShardCluster owner")
	}
	secret.OwnerReferences = nil
	return r.Update(ctx, secret, client.FieldOwner(owned.ManagedByValue))
}

func copyOptionalString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func optionalStringsEqual(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func (r *PgShardClusterReconciler) applyPlan(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, plan []client.Object, states map[string]ownershipState) error {
	for _, desired := range plan {
		if err := r.applyObject(ctx, cluster, desired, states[owned.Key(desired)]); err != nil {
			return fmt.Errorf("apply %T %s/%s: %w", desired, desired.GetNamespace(), desired.GetName(), err)
		}
	}
	if _, err := r.prune(ctx, cluster, plan, false); err != nil {
		return fmt.Errorf("prune stale resources: %w", err)
	}
	return nil
}

type ownershipState struct {
	exists bool
	object client.Object
}

func (r *PgShardClusterReconciler) preflightOwnership(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, plan []client.Object) (map[string]ownershipState, error) {
	states := make(map[string]ownershipState, len(plan))
	for _, desired := range plan {
		existing := desired.DeepCopyObject().(client.Object)
		if err := r.Get(ctx, client.ObjectKeyFromObject(desired), existing); apierrors.IsNotFound(err) {
			states[owned.Key(desired)] = ownershipState{}
			continue
		} else if err != nil {
			return nil, fmt.Errorf("check ownership of %T %s/%s: %w", desired, desired.GetNamespace(), desired.GetName(), err)
		}
		if !metav1.IsControlledBy(existing, cluster) {
			return nil, fmt.Errorf("resource collision: existing %T %s/%s is not controlled by PgShardCluster UID %s", existing, existing.GetNamespace(), existing.GetName(), cluster.UID)
		}
		states[owned.Key(desired)] = ownershipState{exists: true, object: existing}
	}
	return states, nil
}

func (r *PgShardClusterReconciler) applyObject(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, desired client.Object, state ownershipState) error {
	desiredDeployment, isDeployment := desired.(*appsv1.Deployment)
	isPoolerDeployment := isDeployment && desiredDeployment.Name == cluster.Name+owned.PoolerSuffix
	isHPAPooler := isPoolerDeployment && desiredDeployment.Spec.Replicas == nil
	created := false
	if !state.exists {
		create := desired.DeepCopyObject().(client.Object)
		removeApplyOwnershipMarker(create)
		if deployment, ok := create.(*appsv1.Deployment); ok && isHPAPooler {
			replicas := poolerMinimum(cluster)
			deployment.Spec.Replicas = &replicas
		}
		if err := r.Create(ctx, create, client.FieldOwner(owned.ManagedByValue)); err != nil {
			if apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("resource appeared after ownership preflight; refusing to adopt it")
			}
			return err
		}
		state = ownershipState{exists: true, object: create}
		created = true
	}

	gvk, err := objectGVK(desired)
	if err != nil {
		return err
	}
	if !created && !applyOwnershipMigrationComplete(state.object) {
		legacyDesired := desired.DeepCopyObject().(client.Object)
		removeApplyOwnershipMarker(legacyDesired)
		aligned, err := r.alignLegacyOwnedFields(ctx, state.object, legacyDesired, isPoolerDeployment)
		if err != nil {
			return err
		}
		state.object = aligned
	}
	if created || !hasApplyOwnership(state.object, owned.ManagedByValue) {
		// Kubernetes names all pre-existing fields "before-first-apply" when
		// the first apply follows a create or untracked update. Establish the
		// apply field set without publishing the completed-migration marker, then
		// remove synthetic and legacy co-owners before the authoritative apply.
		bootstrap := desired.DeepCopyObject().(client.Object)
		removeApplyOwnershipMarker(bootstrap)
		bootstrap.GetObjectKind().SetGroupVersionKind(gvk)
		bootstrap.SetUID(state.object.GetUID())
		if err := r.Patch(ctx, bootstrap, client.Apply, client.FieldOwner(owned.ManagedByValue), client.ForceOwnership); err != nil {
			return fmt.Errorf("bootstrap server-side apply ownership: %w", err)
		}
		state.object = bootstrap
	}
	migrated, err := r.migrateApplyOwnership(ctx, state.object)
	if err != nil {
		return err
	}
	state.object = migrated

	if isHPAPooler {
		if err := r.handoffPoolerReplicas(ctx, cluster, desiredDeployment, state.object.GetUID(), gvk); err != nil {
			return err
		}
	}
	desired = desired.DeepCopyObject().(client.Object)
	desired.GetObjectKind().SetGroupVersionKind(gvk)
	desired.SetUID(state.object.GetUID())
	if err := r.Patch(ctx, desired, client.Apply, client.FieldOwner(owned.ManagedByValue), client.ForceOwnership); err != nil {
		return err
	}
	if isPoolerDeployment && !isHPAPooler {
		return r.relinquishPoolerScaleOwnership(ctx, desiredDeployment, state.object.GetUID(), gvk)
	}
	return nil
}

func removeApplyOwnershipMarker(object client.Object) {
	annotations := maps.Clone(object.GetAnnotations())
	delete(annotations, owned.ApplyOwnershipAnnotation)
	if len(annotations) == 0 {
		annotations = nil
	}
	object.SetAnnotations(annotations)
}

func (r *PgShardClusterReconciler) authoritativeReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *PgShardClusterReconciler) alignLegacyOwnedFields(ctx context.Context, current, desired client.Object, allowLegacyHPAScale bool) (client.Object, error) {
	const maxAttempts = 4
	originalUID := current.GetUID()
	authoritative := current.DeepCopyObject().(client.Object)
	if err := r.authoritativeReader().Get(ctx, client.ObjectKeyFromObject(current), authoritative); err != nil {
		return nil, fmt.Errorf("read authoritative legacy fields before alignment: %w", err)
	}
	if authoritative.GetUID() != originalUID {
		return nil, fmt.Errorf("resource was replaced before legacy field alignment")
	}
	current = authoritative
	var lastConflict error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if applyOwnershipMigrationComplete(current) {
			return current, nil
		}
		if hasUnrelatedTopLevelApplyOwnership(current, allowLegacyHPAScale) {
			return nil, fmt.Errorf("cannot safely align legacy fields while another top-level Apply manager is present")
		}
		aligned, err := legacyAlignedObject(current, desired)
		if err != nil {
			return nil, err
		}
		if err := r.Update(ctx, aligned, client.FieldOwner(ownershipMigrationManager)); err == nil {
			return aligned, nil
		} else if !apierrors.IsConflict(err) {
			return nil, fmt.Errorf("align legacy operator-owned fields: %w", err)
		} else {
			lastConflict = err
		}

		fresh := current.DeepCopyObject().(client.Object)
		if err := r.authoritativeReader().Get(ctx, client.ObjectKeyFromObject(current), fresh); err != nil {
			return nil, fmt.Errorf("reload legacy fields after conflict: %w", err)
		}
		if fresh.GetUID() != originalUID {
			return nil, fmt.Errorf("resource was replaced during legacy field alignment")
		}
		current = fresh
	}
	return nil, fmt.Errorf("align legacy operator-owned fields after %d conflicts: %w", maxAttempts, lastConflict)
}

func hasUnrelatedTopLevelApplyOwnership(object client.Object, allowHPAScale bool) bool {
	for _, entry := range object.GetManagedFields() {
		if entry.Operation != metav1.ManagedFieldsOperationApply || entry.Subresource != "" || entry.Manager == owned.ManagedByValue {
			continue
		}
		if allowHPAScale && entry.Manager == hpaScaleFieldManager {
			continue
		}
		return true
	}
	return false
}

func legacyAlignedObject(current, desired client.Object) (client.Object, error) {
	aligned := current.DeepCopyObject().(client.Object)
	aligned.SetLabels(maps.Clone(desired.GetLabels()))
	aligned.SetAnnotations(maps.Clone(desired.GetAnnotations()))
	aligned.SetOwnerReferences(append([]metav1.OwnerReference(nil), desired.GetOwnerReferences()...))

	switch wanted := desired.(type) {
	case *corev1.ConfigMap:
		got, ok := aligned.(*corev1.ConfigMap)
		if !ok {
			return nil, fmt.Errorf("legacy object type %T does not match desired ConfigMap", current)
		}
		got.Data = maps.Clone(wanted.Data)
		got.BinaryData = maps.Clone(wanted.BinaryData)
		got.Immutable = nil
		if wanted.Immutable != nil {
			immutable := *wanted.Immutable
			got.Immutable = &immutable
		}
	case *corev1.Service:
		got, ok := aligned.(*corev1.Service)
		if !ok {
			return nil, fmt.Errorf("legacy object type %T does not match desired Service", current)
		}
		ports := wanted.Spec.DeepCopy().Ports
		if wanted.Spec.Type == corev1.ServiceTypeNodePort || wanted.Spec.Type == corev1.ServiceTypeLoadBalancer {
			for index := range ports {
				if ports[index].NodePort != 0 {
					continue
				}
				for _, existing := range got.Spec.Ports {
					if existing.Name == ports[index].Name && existing.Protocol == ports[index].Protocol && existing.Port == ports[index].Port {
						ports[index].NodePort = existing.NodePort
						break
					}
				}
			}
		}
		got.Spec.Type = wanted.Spec.Type
		got.Spec.Selector = maps.Clone(wanted.Spec.Selector)
		got.Spec.Ports = ports
		got.Spec.PublishNotReadyAddresses = wanted.Spec.PublishNotReadyAddresses
	case *appsv1.Deployment:
		got, ok := aligned.(*appsv1.Deployment)
		if !ok {
			return nil, fmt.Errorf("legacy object type %T does not match desired Deployment", current)
		}
		replicas := got.Spec.Replicas
		got.Spec = *wanted.Spec.DeepCopy()
		if wanted.Spec.Replicas == nil {
			got.Spec.Replicas = replicas
		}
	case *appsv1.StatefulSet:
		got, ok := aligned.(*appsv1.StatefulSet)
		if !ok {
			return nil, fmt.Errorf("legacy object type %T does not match desired StatefulSet", current)
		}
		got.Spec = *wanted.Spec.DeepCopy()
	case *autoscalingv2.HorizontalPodAutoscaler:
		got, ok := aligned.(*autoscalingv2.HorizontalPodAutoscaler)
		if !ok {
			return nil, fmt.Errorf("legacy object type %T does not match desired HorizontalPodAutoscaler", current)
		}
		got.Spec = *wanted.Spec.DeepCopy()
	case *networkingv1.NetworkPolicy:
		got, ok := aligned.(*networkingv1.NetworkPolicy)
		if !ok {
			return nil, fmt.Errorf("legacy object type %T does not match desired NetworkPolicy", current)
		}
		got.Spec = *wanted.Spec.DeepCopy()
	case *policyv1.PodDisruptionBudget:
		got, ok := aligned.(*policyv1.PodDisruptionBudget)
		if !ok {
			return nil, fmt.Errorf("legacy object type %T does not match desired PodDisruptionBudget", current)
		}
		got.Spec = *wanted.Spec.DeepCopy()
	default:
		return nil, fmt.Errorf("unsupported legacy planned object type %T", desired)
	}
	return aligned, nil
}

func (r *PgShardClusterReconciler) handoffPoolerReplicas(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, desired *appsv1.Deployment, expectedUID types.UID, gvk schema.GroupVersionKind) error {
	const maxAttempts = 4
	var lastConflict error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		current := &appsv1.Deployment{}
		if err := r.authoritativeReader().Get(ctx, client.ObjectKeyFromObject(desired), current); err != nil {
			return fmt.Errorf("read authoritative pooler replicas before HPA handoff: %w", err)
		}
		if current.UID != expectedUID {
			return fmt.Errorf("pooler Deployment was replaced before HPA handoff")
		}
		if hasExactReplicaApplyOwnership(current, hpaScaleFieldManager) {
			return nil
		}
		replicas := poolerMinimum(cluster)
		if current.Spec.Replicas != nil {
			replicas = *current.Spec.Replicas
		}
		metadata := map[string]any{
			"name":      desired.Name,
			"namespace": desired.Namespace,
			"uid":       string(current.UID),
		}
		if current.ResourceVersion != "" {
			metadata["resourceVersion"] = current.ResourceVersion
		}
		handoff := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": gvk.GroupVersion().String(),
			"kind":       gvk.Kind,
			"metadata":   metadata,
			"spec":       map[string]any{"replicas": int64(replicas)},
		}}
		if err := r.Patch(ctx, handoff, client.Apply, client.FieldOwner(hpaScaleFieldManager), client.ForceOwnership); err == nil {
			return nil
		} else if !apierrors.IsConflict(err) {
			return fmt.Errorf("hand off pooler replicas to HPA: %w", err)
		} else {
			lastConflict = err
		}
	}
	return fmt.Errorf("hand off pooler replicas after %d conflicts: %w", maxAttempts, lastConflict)
}

func hasExactReplicaApplyOwnership(object client.Object, manager string) bool {
	found := false
	for _, entry := range object.GetManagedFields() {
		if entry.Manager != manager || entry.Operation != metav1.ManagedFieldsOperationApply || entry.Subresource != "" {
			continue
		}
		if found || entry.FieldsV1 == nil {
			return false
		}
		found = true
		var root map[string]json.RawMessage
		if err := json.Unmarshal(entry.FieldsV1.Raw, &root); err != nil || len(root) != 1 {
			return false
		}
		var spec map[string]json.RawMessage
		if err := json.Unmarshal(root["f:spec"], &spec); err != nil || len(spec) != 1 {
			return false
		}
		var replicas map[string]json.RawMessage
		if err := json.Unmarshal(spec["f:replicas"], &replicas); err != nil || replicas == nil || len(replicas) != 0 {
			return false
		}
	}
	return found
}

func (r *PgShardClusterReconciler) relinquishPoolerScaleOwnership(ctx context.Context, desired *appsv1.Deployment, expectedUID types.UID, gvk schema.GroupVersionKind) error {
	const maxAttempts = 4
	if desired.Spec.Replicas == nil {
		return fmt.Errorf("fixed-scale pooler Deployment has no desired replicas")
	}
	var lastRetry error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		current := &appsv1.Deployment{}
		if err := r.authoritativeReader().Get(ctx, client.ObjectKeyFromObject(desired), current); err != nil {
			return fmt.Errorf("read authoritative pooler ownership before fixed-scale handoff: %w", err)
		}
		if current.UID != expectedUID {
			return fmt.Errorf("pooler Deployment was replaced before fixed-scale handoff")
		}
		if current.Spec.Replicas == nil || *current.Spec.Replicas != *desired.Spec.Replicas || !hasReplicaApplyOwnership(current, owned.ManagedByValue) {
			reclaim := desired.DeepCopy()
			reclaim.GetObjectKind().SetGroupVersionKind(gvk)
			reclaim.UID = current.UID
			reclaim.ResourceVersion = current.ResourceVersion
			if err := r.Patch(ctx, reclaim, client.Apply, client.FieldOwner(owned.ManagedByValue), client.ForceOwnership); err == nil {
				lastRetry = errors.New("fixed pooler replicas were not yet authoritative after re-apply")
				continue
			} else if !apierrors.IsConflict(err) {
				return fmt.Errorf("reclaim fixed pooler replicas before HPA-manager relinquishment: %w", err)
			} else {
				lastRetry = err
				continue
			}
		}
		if !hasApplyOwnership(current, hpaScaleFieldManager) {
			return nil
		}
		metadata := map[string]any{
			"name":      desired.Name,
			"namespace": desired.Namespace,
			"uid":       string(current.UID),
		}
		if current.ResourceVersion != "" {
			metadata["resourceVersion"] = current.ResourceVersion
		}
		relinquish := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": gvk.GroupVersion().String(),
			"kind":       gvk.Kind,
			"metadata":   metadata,
		}}
		if err := r.Patch(ctx, relinquish, client.Apply, client.FieldOwner(hpaScaleFieldManager), client.ForceOwnership); err == nil {
			return nil
		} else if !apierrors.IsConflict(err) {
			return fmt.Errorf("relinquish pooler replicas from HPA manager: %w", err)
		} else {
			lastRetry = err
		}
	}
	return fmt.Errorf("stabilize fixed pooler replicas and relinquish HPA ownership after %d attempts: %w", maxAttempts, lastRetry)
}

func hasApplyOwnership(object client.Object, manager string) bool {
	for _, entry := range object.GetManagedFields() {
		if entry.Manager == manager && entry.Operation == metav1.ManagedFieldsOperationApply && entry.Subresource == "" {
			return true
		}
	}
	return false
}

func hasReplicaApplyOwnership(object client.Object, manager string) bool {
	for _, entry := range object.GetManagedFields() {
		if entry.Manager != manager || entry.Operation != metav1.ManagedFieldsOperationApply || entry.Subresource != "" || entry.FieldsV1 == nil {
			continue
		}
		var root map[string]json.RawMessage
		if err := json.Unmarshal(entry.FieldsV1.Raw, &root); err != nil {
			continue
		}
		var spec map[string]json.RawMessage
		if err := json.Unmarshal(root["f:spec"], &spec); err != nil {
			continue
		}
		var replicas map[string]json.RawMessage
		if err := json.Unmarshal(spec["f:replicas"], &replicas); err == nil && replicas != nil && len(replicas) == 0 {
			return true
		}
	}
	return false
}

func applyOwnershipMigrationComplete(object metav1.Object) bool {
	if object.GetAnnotations()[owned.ApplyOwnershipAnnotation] != owned.ApplyOwnershipVersion {
		return false
	}
	for _, entry := range object.GetManagedFields() {
		if entry.Manager != owned.ManagedByValue || entry.Operation != metav1.ManagedFieldsOperationApply || entry.Subresource != "" || entry.FieldsV1 == nil {
			continue
		}
		var root map[string]json.RawMessage
		if err := json.Unmarshal(entry.FieldsV1.Raw, &root); err != nil {
			continue
		}
		var metadata map[string]json.RawMessage
		if err := json.Unmarshal(root["f:metadata"], &metadata); err != nil {
			continue
		}
		var annotations map[string]json.RawMessage
		if err := json.Unmarshal(metadata["f:annotations"], &annotations); err != nil {
			continue
		}
		var marker map[string]json.RawMessage
		if err := json.Unmarshal(annotations["f:"+owned.ApplyOwnershipAnnotation], &marker); err == nil && marker != nil && len(marker) == 0 {
			return true
		}
	}
	return false
}

func (r *PgShardClusterReconciler) migrateApplyOwnership(ctx context.Context, object client.Object) (client.Object, error) {
	const maxAttempts = 4
	originalUID := object.GetUID()
	current := object
	var lastConflict error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		managedFields := current.GetManagedFields()
		filtered := make([]metav1.ManagedFieldsEntry, 0, len(managedFields))
		removed := false
		migrationComplete := applyOwnershipMigrationComplete(current)
		for _, entry := range managedFields {
			// Kubernetes derives an omitted Update field manager from the client
			// user agent, so pre-SSA releases did not leave one stable manager name.
			// Before the durable marker exists, the type-aware alignment above has
			// reset the operator-controlled fields and every top-level Update field
			// set belongs to that legacy whole-object ownership era. Once the marker
			// and Apply field set both exist, preserve every unrelated later manager.
			legacyOwner := !migrationComplete || entry.Manager == owned.ManagedByValue || entry.Manager == ownershipMigrationManager || entry.Manager == "before-first-apply"
			if entry.Subresource == "" && entry.Operation == metav1.ManagedFieldsOperationUpdate && legacyOwner {
				removed = true
				continue
			}
			filtered = append(filtered, entry)
		}
		if !removed {
			return current, nil
		}
		if len(filtered) == 0 {
			// A singleton empty entry is the Kubernetes API's explicit request to
			// reset managed fields; an empty slice can disappear under omitempty.
			filtered = []metav1.ManagedFieldsEntry{{}}
		}
		migrated := current.DeepCopyObject().(client.Object)
		migrated.SetManagedFields(filtered)
		if err := r.Update(ctx, migrated, client.FieldOwner(ownershipMigrationManager)); err == nil {
			return migrated, nil
		} else if !apierrors.IsConflict(err) {
			return nil, fmt.Errorf("migrate create-time field ownership: %w", err)
		} else {
			lastConflict = err
		}

		fresh := current.DeepCopyObject().(client.Object)
		if err := r.authoritativeReader().Get(ctx, client.ObjectKeyFromObject(current), fresh); err != nil {
			return nil, fmt.Errorf("reload field ownership after conflict: %w", err)
		}
		if fresh.GetUID() != originalUID {
			return nil, fmt.Errorf("resource was replaced during field-ownership migration")
		}
		current = fresh
	}
	return nil, fmt.Errorf("migrate create-time field ownership after %d conflicts: %w", maxAttempts, lastConflict)
}

func (r *PgShardClusterReconciler) ownedHPAForFixedTransition(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (*autoscalingv2.HorizontalPodAutoscaler, error) {
	if cluster.Spec.Pooler.Scaling.Mode != pgshardv1alpha1.ScalingFixed {
		return nil, nil
	}
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + owned.PoolerSuffix}
	if err := r.authoritativeReader().Get(ctx, key, hpa); apierrors.IsNotFound(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	if !metav1.IsControlledBy(hpa, cluster) {
		return nil, fmt.Errorf("resource collision: existing HPA %s/%s is not controlled by PgShardCluster UID %s", hpa.Namespace, hpa.Name, cluster.UID)
	}
	return hpa, nil
}

func objectGVK(object client.Object) (schema.GroupVersionKind, error) {
	switch object.(type) {
	case *corev1.ConfigMap:
		return corev1.SchemeGroupVersion.WithKind("ConfigMap"), nil
	case *corev1.Service:
		return corev1.SchemeGroupVersion.WithKind("Service"), nil
	case *appsv1.Deployment:
		return appsv1.SchemeGroupVersion.WithKind("Deployment"), nil
	case *appsv1.StatefulSet:
		return appsv1.SchemeGroupVersion.WithKind("StatefulSet"), nil
	case *autoscalingv2.HorizontalPodAutoscaler:
		return autoscalingv2.SchemeGroupVersion.WithKind("HorizontalPodAutoscaler"), nil
	case *networkingv1.NetworkPolicy:
		return networkingv1.SchemeGroupVersion.WithKind("NetworkPolicy"), nil
	case *policyv1.PodDisruptionBudget:
		return policyv1.SchemeGroupVersion.WithKind("PodDisruptionBudget"), nil
	default:
		return schema.GroupVersionKind{}, fmt.Errorf("unsupported planned object type %T", object)
	}
}

func storageDeletionPolicy(cluster *pgshardv1alpha1.PgShardCluster) pgshardv1alpha1.StorageDeletionPolicy {
	if cluster.Spec.Storage.DeletionPolicy == "" {
		return pgshardv1alpha1.DeletionRetain
	}
	return cluster.Spec.Storage.DeletionPolicy
}

func provisionedStorageDeletionPolicy(cluster *pgshardv1alpha1.PgShardCluster) (pgshardv1alpha1.StorageDeletionPolicy, error) {
	if cluster.Status.PostgreSQLBootstrapSpec == nil {
		// A delete can race the first post-finalizer reconcile. No PostgreSQL
		// child exists before this snapshot, and retaining is the only safe
		// default if admission was bypassed.
		return pgshardv1alpha1.DeletionRetain, nil
	}
	policy := cluster.Status.PostgreSQLBootstrapSpec.DeletionPolicy
	if policy != pgshardv1alpha1.DeletionRetain && policy != pgshardv1alpha1.DeletionDelete {
		return "", fmt.Errorf("recorded PostgreSQL deletion policy %q is invalid", policy)
	}
	return policy, nil
}

func provisionedPostgreSQLStorageSize(cluster *pgshardv1alpha1.PgShardCluster) (resource.Quantity, error) {
	if cluster.Status.PostgreSQLBootstrapSpec == nil {
		return resource.Quantity{}, fmt.Errorf("PostgreSQL provisioning snapshot is missing")
	}
	storageSize, err := resource.ParseQuantity(cluster.Status.PostgreSQLBootstrapSpec.StorageSize)
	if err != nil || storageSize.Sign() <= 0 {
		return resource.Quantity{}, fmt.Errorf("recorded PostgreSQL storage size %q is invalid", cluster.Status.PostgreSQLBootstrapSpec.StorageSize)
	}
	return storageSize, nil
}

// resolvePostgreSQLPVCOutcomesForFinalization closes every possible PVC-create
// outcome while the credential fence is still present. Retain claims are made
// ownerless and protected, but remain protected until the credential and Pod
// absence barriers have completed.
func (r *PgShardClusterReconciler) resolvePostgreSQLPVCOutcomesForFinalization(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, deletionPolicy pgshardv1alpha1.StorageDeletionPolicy) (bool, error) {
	if r.APIReader == nil {
		return false, fmt.Errorf("authoritative API reader is required for data-outcome resolution")
	}
	if len(cluster.Status.PostgreSQLBootstraps) == 0 {
		return false, nil
	}
	storageSize, err := provisionedPostgreSQLStorageSize(cluster)
	if err != nil {
		return false, err
	}
	for index := range cluster.Status.PostgreSQLBootstraps {
		bootstrap := &cluster.Status.PostgreSQLBootstraps[index]
		if bootstrap.PVCName == "" {
			continue
		}
		claim, err := r.resolvePostgreSQLDataPVCForFinalization(ctx, cluster, *bootstrap)
		if err != nil {
			return false, err
		}
		if claim == nil {
			if deletionPolicy == pgshardv1alpha1.DeletionRetain && bootstrap.PVCUID == "" && bootstrap.SecretUID != "" && bootstrap.PVCFenceDetached && !bootstrap.PVCCreationAbandoned {
				unavailable, unavailableErr := namespaceUnavailableForCreate(ctx, r.APIReader, cluster.Namespace)
				if unavailableErr != nil {
					return false, unavailableErr
				}
				if !unavailable {
					bootstrap.PVCCreationAbandoned = true
					if err := r.Status().Update(ctx, cluster); err != nil {
						return false, fmt.Errorf("checkpoint abandoned PostgreSQL data creation for shard %d: %w", bootstrap.Shard, err)
					}
					return true, nil
				}
			}
			continue
		}
		if bootstrap.PVCCreationAbandoned || (bootstrap.PVCUID != "" && claim.UID != bootstrap.PVCUID) {
			return r.deleteLatePostgreSQLDataPVC(ctx, cluster, *bootstrap, claim, storageSize)
		}
		if err := validatePostgreSQLDataPVCForFinalization(claim, cluster, bootstrap.Shard, *bootstrap, storageSize); err != nil {
			return false, err
		}
		if bootstrap.PVCUID == "" {
			if !postgresqlDataPVCIsCreationFenced(claim, *bootstrap) {
				return false, fmt.Errorf("PostgreSQL data PVC %s has an unexpected owner before its API UID was checkpointed", bootstrap.PVCName)
			}
			bootstrap.PVCUID = claim.UID
			if err := r.Status().Update(ctx, cluster); err != nil {
				return false, fmt.Errorf("checkpoint PostgreSQL data identity for shard %d during finalization: %w", bootstrap.Shard, err)
			}
			return true, nil
		}
		if claim.DeletionTimestamp != nil {
			if len(claim.OwnerReferences) != 0 && !postgresqlDataPVCIsCreationFenced(claim, *bootstrap) {
				return false, fmt.Errorf("deleting PostgreSQL data PVC %s has an unexpected owner", claim.Name)
			}
			continue
		}
		stable := len(claim.OwnerReferences) == 0 && postgresqlDataPVCIsProtected(claim)
		retained := deletionPolicy == pgshardv1alpha1.DeletionRetain &&
			len(claim.OwnerReferences) == 0 && !postgresqlDataPVCIsProtected(claim) &&
			claim.Annotations[owned.RetainedFromAnnotation] == cluster.Namespace+"/"+cluster.Name
		creationFenced := postgresqlDataPVCIsCreationFenced(claim, *bootstrap)
		if stable || retained {
			continue
		}
		if !creationFenced {
			return false, fmt.Errorf("PostgreSQL data PVC %s is neither protected steady data nor exact creation-fenced data", claim.Name)
		}
		if deletionPolicy == pgshardv1alpha1.DeletionDelete {
			continue
		}
		claim.OwnerReferences = nil
		controllerutil.AddFinalizer(claim, owned.PostgreSQLDataProtectionFinalizer)
		if err := r.Update(ctx, claim, client.FieldOwner(owned.ManagedByValue)); err != nil {
			return false, fmt.Errorf("protect PostgreSQL data PVC %s before deleting its credential fence: %w", claim.Name, err)
		}
		return true, nil
	}
	return false, nil
}

func (r *PgShardClusterReconciler) retainPostgreSQLPVCs(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (bool, error) {
	if r.APIReader == nil {
		return false, fmt.Errorf("authoritative API reader is required for data retention")
	}
	if len(cluster.Status.PostgreSQLBootstraps) == 0 {
		return false, nil
	}
	storageSize, err := provisionedPostgreSQLStorageSize(cluster)
	if err != nil {
		return false, err
	}
	changed := false
	for index := range cluster.Status.PostgreSQLBootstraps {
		bootstrap := &cluster.Status.PostgreSQLBootstraps[index]
		if bootstrap.PVCName == "" {
			continue
		}
		claim, err := r.resolvePostgreSQLDataPVCForFinalization(ctx, cluster, *bootstrap)
		if err != nil {
			return false, err
		}
		if claim == nil {
			if bootstrap.PVCUID == "" && bootstrap.SecretUID != "" && bootstrap.PVCFenceDetached && !bootstrap.PVCCreationAbandoned {
				unavailable, unavailableErr := namespaceUnavailableForCreate(ctx, r.APIReader, cluster.Namespace)
				if unavailableErr != nil {
					return false, unavailableErr
				}
				if !unavailable {
					bootstrap.PVCCreationAbandoned = true
					if err := r.Status().Update(ctx, cluster); err != nil {
						return false, fmt.Errorf("checkpoint abandoned PostgreSQL data creation for shard %d: %w", bootstrap.Shard, err)
					}
					return true, nil
				}
			}
			continue
		}
		if bootstrap.PVCCreationAbandoned {
			return r.deleteLatePostgreSQLDataPVC(ctx, cluster, *bootstrap, claim, storageSize)
		}
		if bootstrap.PVCUID != "" && claim.UID != bootstrap.PVCUID {
			return r.deleteLatePostgreSQLDataPVC(ctx, cluster, *bootstrap, claim, storageSize)
		}
		if claim.DeletionTimestamp != nil {
			if bootstrap.PVCUID == "" && !postgresqlDataPVCIsCreationFenced(claim, *bootstrap) {
				return false, fmt.Errorf("refuse to release uncheckpointed deleting PostgreSQL data PVC %s without its exact credential creation fence", claim.Name)
			}
			if err := validatePostgreSQLDataPVCForFinalization(claim, cluster, bootstrap.Shard, *bootstrap, storageSize); err != nil {
				return false, err
			}
			if postgresqlDataPVCIsProtected(claim) {
				controllerutil.RemoveFinalizer(claim, owned.PostgreSQLDataProtectionFinalizer)
				if err := r.Update(ctx, claim, client.FieldOwner(owned.ManagedByValue)); err != nil && !apierrors.IsNotFound(err) {
					return false, fmt.Errorf("release explicitly deleting PostgreSQL data PVC %s: %w", claim.Name, err)
				}
			}
			// A Delete request is irreversible. Workloads were pruned before this
			// function, so release only our finalizer and wait for authoritative
			// absence instead of misrepresenting the claim as retained.
			return true, nil
		}
		if err := validatePostgreSQLDataPVC(claim, cluster, bootstrap.Shard, *bootstrap, storageSize); err != nil {
			return false, err
		}
		if bootstrap.PVCUID == "" {
			if !postgresqlDataPVCIsCreationFenced(claim, *bootstrap) {
				return false, fmt.Errorf("retained PostgreSQL data PVC %s has an unexpected owner before its API UID was checkpointed", bootstrap.PVCName)
			}
			bootstrap.PVCUID = claim.UID
			if err := r.Status().Update(ctx, cluster); err != nil {
				return false, fmt.Errorf("checkpoint retained PostgreSQL data identity for shard %d: %w", bootstrap.Shard, err)
			}
			return true, nil
		}
		annotations := maps.Clone(claim.Annotations)
		if annotations == nil {
			annotations = make(map[string]string, 1)
		}
		retainedFrom := cluster.Namespace + "/" + cluster.Name
		ownershipDetached := len(claim.OwnerReferences) == 0
		protected := postgresqlDataPVCIsProtected(claim)
		if annotations[owned.RetainedFromAnnotation] == retainedFrom && ownershipDetached && !protected {
			continue
		}
		annotations[owned.RetainedFromAnnotation] = retainedFrom
		claim.Annotations = annotations
		if !ownershipDetached {
			if !postgresqlDataPVCIsCreationFenced(claim, *bootstrap) {
				return false, fmt.Errorf("PostgreSQL data PVC %s does not have the exact credential creation fence", claim.Name)
			}
			claim.OwnerReferences = nil
		}
		controllerutil.RemoveFinalizer(claim, owned.PostgreSQLDataProtectionFinalizer)
		if err := r.Update(ctx, claim, client.FieldOwner(owned.ManagedByValue)); err != nil {
			return false, fmt.Errorf("detach, release, and mark retained PostgreSQL data PVC %s: %w", claim.Name, err)
		}
		changed = true
	}
	return changed, nil
}

func (r *PgShardClusterReconciler) deletePostgreSQLPVCs(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (bool, error) {
	if r.APIReader == nil {
		return false, fmt.Errorf("authoritative API reader is required for data deletion")
	}
	if len(cluster.Status.PostgreSQLBootstraps) == 0 {
		return false, nil
	}
	storageSize, err := provisionedPostgreSQLStorageSize(cluster)
	if err != nil {
		return false, err
	}
	for index := range cluster.Status.PostgreSQLBootstraps {
		bootstrap := &cluster.Status.PostgreSQLBootstraps[index]
		if bootstrap.PVCName == "" {
			continue
		}
		claim, err := r.resolvePostgreSQLDataPVCForFinalization(ctx, cluster, *bootstrap)
		if err != nil {
			return false, err
		}
		if claim == nil {
			continue
		}
		if bootstrap.PVCUID != "" && claim.UID != bootstrap.PVCUID {
			return r.deleteLatePostgreSQLDataPVC(ctx, cluster, *bootstrap, claim, storageSize)
		}
		if claim.DeletionTimestamp != nil {
			if bootstrap.PVCUID == "" && !postgresqlDataPVCIsCreationFenced(claim, *bootstrap) {
				return false, fmt.Errorf("refuse to release uncheckpointed deleting PostgreSQL data PVC %s without its exact credential creation fence", claim.Name)
			}
			if err := validatePostgreSQLDataPVCForFinalization(claim, cluster, bootstrap.Shard, *bootstrap, storageSize); err != nil {
				return false, err
			}
			if postgresqlDataPVCIsProtected(claim) {
				controllerutil.RemoveFinalizer(claim, owned.PostgreSQLDataProtectionFinalizer)
				if err := r.Update(ctx, claim, client.FieldOwner(owned.ManagedByValue)); err != nil && !apierrors.IsNotFound(err) {
					return false, fmt.Errorf("release deletable PostgreSQL data PVC %s: %w", claim.Name, err)
				}
			}
			return true, nil
		}
		if err := validatePostgreSQLDataPVC(claim, cluster, bootstrap.Shard, *bootstrap, storageSize); err != nil {
			return false, err
		}
		if bootstrap.PVCUID == "" {
			if !postgresqlDataPVCIsCreationFenced(claim, *bootstrap) {
				return false, fmt.Errorf("Delete-policy PostgreSQL data PVC %s has an unexpected owner before its API UID was checkpointed", claim.Name)
			}
			bootstrap.PVCUID = claim.UID
			if err := r.Status().Update(ctx, cluster); err != nil {
				return false, fmt.Errorf("checkpoint deletable PostgreSQL data identity for shard %d: %w", bootstrap.Shard, err)
			}
			return true, nil
		}
		stable := len(claim.OwnerReferences) == 0 && postgresqlDataPVCIsProtected(claim)
		if !stable && !postgresqlDataPVCIsCreationFenced(claim, *bootstrap) {
			return false, fmt.Errorf("Delete-policy PostgreSQL data PVC %s is neither protected steady data nor exact creation-fenced data", claim.Name)
		}
		uid := bootstrap.PVCUID
		resourceVersion := claim.ResourceVersion
		if err := r.Delete(ctx, claim, client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}); err != nil && !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("delete PostgreSQL data PVC %s with recorded UID %s: %w", bootstrap.PVCName, bootstrap.PVCUID, err)
		}
		return true, nil
	}
	return false, nil
}

func (r *PgShardClusterReconciler) deleteLatePostgreSQLDataPVC(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, bootstrap pgshardv1alpha1.PostgreSQLBootstrapStatus, claim *corev1.PersistentVolumeClaim, storageSize resource.Quantity) (bool, error) {
	if !postgresqlDataPVCIsCreationFenced(claim, bootstrap) {
		return false, fmt.Errorf("PostgreSQL data PVC %s has UID %s, expected recorded UID %s, and is not controlled by the exact credential tombstone", bootstrap.PVCName, claim.UID, bootstrap.PVCUID)
	}
	if postgresqlDataPVCIsProtected(claim) {
		return false, fmt.Errorf("late PostgreSQL data PVC %s unexpectedly carries the protected-data finalizer", claim.Name)
	}
	if claim.DeletionTimestamp != nil {
		return true, nil
	}
	if err := validatePostgreSQLDataPVC(claim, cluster, bootstrap.Shard, bootstrap, storageSize); err != nil {
		return false, fmt.Errorf("validate late PostgreSQL data PVC %s before deletion: %w", claim.Name, err)
	}
	uid := claim.UID
	resourceVersion := claim.ResourceVersion
	if err := r.Delete(ctx, claim, client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}); err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("delete late PostgreSQL data PVC %s with UID %s: %w", claim.Name, claim.UID, err)
	}
	return true, nil
}

func (r *PgShardClusterReconciler) resolvePostgreSQLDataPVCForFinalization(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, bootstrap pgshardv1alpha1.PostgreSQLBootstrapStatus) (*corev1.PersistentVolumeClaim, error) {
	claim := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.PVCName}
	err := r.APIReader.Get(ctx, key, claim)
	if err == nil {
		if bootstrap.SecretUID == "" || !bootstrap.PVCFenceDetached {
			return nil, fmt.Errorf("PostgreSQL data PVC %s exists before its credential creation fence was detached", bootstrap.PVCName)
		}
		return claim, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("read PostgreSQL data PVC %s: %w", bootstrap.PVCName, err)
	}
	// Finalization never creates storage. A visible uncheckpointed claim is
	// resolved by its caller; an absent one is either known-gone or may still
	// arrive carrying the detached Secret UID. Retain durably abandons that
	// intent before deleting the Secret, and Delete relies on the same owner
	// fence, so a delayed create cannot become replacement data.
	return nil, nil
}

func (r *PgShardClusterReconciler) deletePostgreSQLCredentialFences(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, deletionPolicy pgshardv1alpha1.StorageDeletionPolicy) (bool, error) {
	if r.APIReader == nil {
		return false, fmt.Errorf("authoritative API reader is required for credential-fence deletion")
	}
	for index := range cluster.Status.PostgreSQLBootstraps {
		bootstrap := &cluster.Status.PostgreSQLBootstraps[index]
		if bootstrap.SecretName == "" {
			continue
		}
		secret := &corev1.Secret{}
		key := types.NamespacedName{Namespace: cluster.Namespace, Name: bootstrap.SecretName}
		if err := r.APIReader.Get(ctx, key, secret); apierrors.IsNotFound(err) {
			if deletionPolicy == pgshardv1alpha1.DeletionRetain && bootstrap.PVCFenceDetached && bootstrap.PVCUID == "" && !bootstrap.PVCCreationAbandoned {
				unavailable, unavailableErr := namespaceUnavailableForCreate(ctx, r.APIReader, cluster.Namespace)
				if unavailableErr != nil {
					return false, unavailableErr
				}
				if unavailable {
					continue
				}
				return false, fmt.Errorf("credential creation fence %s disappeared before its possible PVC outcome was retained", bootstrap.SecretName)
			}
			continue
		} else if err != nil {
			return false, fmt.Errorf("read PostgreSQL credential creation fence %s: %w", bootstrap.SecretName, err)
		}
		if bootstrap.SecretUID == "" {
			if secret.DeletionTimestamp != nil {
				return true, nil
			}
			if err := validatePostgreSQLAuthSecret(secret, cluster, bootstrap.Shard, bootstrap.SecretName); err != nil {
				return false, err
			}
			if !postgresqlCredentialIsClusterFenced(secret, cluster) {
				return false, fmt.Errorf("uncheckpointed credential creation fence %s is not controlled by the exact cluster", bootstrap.SecretName)
			}
			bootstrap.SecretUID = secret.UID
			if err := r.Status().Update(ctx, cluster); err != nil {
				return false, fmt.Errorf("checkpoint credential creation fence identity for shard %d during finalization: %w", bootstrap.Shard, err)
			}
			return true, nil
		}
		if secret.UID != bootstrap.SecretUID {
			if !postgresqlCredentialIsClusterFenced(secret, cluster) {
				return false, fmt.Errorf("credential creation fence %s has UID %s, expected recorded UID %s, and is not controlled by the exact deleting cluster", bootstrap.SecretName, secret.UID, bootstrap.SecretUID)
			}
			if secret.DeletionTimestamp != nil {
				return true, nil
			}
			if err := validatePostgreSQLAuthSecret(secret, cluster, bootstrap.Shard, bootstrap.SecretName); err != nil {
				return false, fmt.Errorf("validate late PostgreSQL credential fence %s before deletion: %w", secret.Name, err)
			}
			uid := secret.UID
			resourceVersion := secret.ResourceVersion
			if err := r.Delete(ctx, secret, client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}); err != nil && !apierrors.IsNotFound(err) {
				return false, fmt.Errorf("delete late PostgreSQL credential fence %s with UID %s: %w", secret.Name, secret.UID, err)
			}
			return true, nil
		}
		if deletionPolicy == pgshardv1alpha1.DeletionRetain && bootstrap.PVCFenceDetached && bootstrap.PVCUID == "" && !bootstrap.PVCCreationAbandoned {
			unavailable, unavailableErr := namespaceUnavailableForCreate(ctx, r.APIReader, cluster.Namespace)
			if unavailableErr != nil {
				return false, unavailableErr
			}
			if unavailable {
				continue
			}
			return false, fmt.Errorf("credential creation fence %s cannot be deleted before its possible PVC outcome is resolved", bootstrap.SecretName)
		}
		if err := validatePostgreSQLAuthSecret(secret, cluster, bootstrap.Shard, bootstrap.SecretName); err != nil && secret.DeletionTimestamp == nil {
			return false, err
		}
		if secret.DeletionTimestamp != nil {
			return true, nil
		}
		if !bootstrap.PVCFenceDetached {
			if len(secret.OwnerReferences) != 0 && !postgresqlCredentialIsClusterFenced(secret, cluster) {
				return false, fmt.Errorf("credential creation fence %s has an unexpected owner before detachment", secret.Name)
			}
		} else if bootstrap.PVCUID == "" {
			if len(secret.OwnerReferences) != 0 {
				return false, fmt.Errorf("credential creation fence %s is not detached while its PVC outcome is unresolved", secret.Name)
			}
		} else if len(secret.OwnerReferences) != 0 && !postgresqlCredentialIsDataAnchored(secret, *bootstrap) {
			return false, fmt.Errorf("credential tombstone %s is not anchored to the exact recorded PVC", secret.Name)
		}
		uid := bootstrap.SecretUID
		resourceVersion := secret.ResourceVersion
		if err := r.Delete(ctx, secret, client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}); err != nil && !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("delete PostgreSQL credential creation fence %s with recorded UID %s: %w", bootstrap.SecretName, bootstrap.SecretUID, err)
		}
		return true, nil
	}
	return false, nil
}

// releaseTerminatedPostgreSQLPodFences removes the operator's Pod finalizer
// only for a Pod that was never assigned or after admission has attached a
// durable termination receipt to an authenticated terminal-phase update from
// the exact binding-time kubelet. PodGC's control-plane-authored Failed phase
// does not satisfy that proof.
func (r *PgShardClusterReconciler) releaseTerminatedPostgreSQLPodFences(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (bool, error) {
	if len(cluster.Status.PostgreSQLBootstraps) == 0 {
		return false, nil
	}
	pods := &corev1.PodList{}
	if err := r.authoritativeReader().List(ctx, pods,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{owned.ClusterLabel: cluster.Name, owned.ComponentLabel: "postgresql"},
	); err != nil {
		return false, fmt.Errorf("list PostgreSQL Pods while releasing termination fences: %w", err)
	}
	released := false
	for index := range pods.Items {
		pod := &pods.Items[index]
		if !controllerutil.ContainsFinalizer(pod, owned.PostgreSQLPodTerminationFinalizer) || pod.DeletionTimestamp == nil {
			continue
		}
		safe, err := r.podHasSafeTerminationEvidence(ctx, pod)
		if err != nil {
			return false, fmt.Errorf("verify PostgreSQL Pod %s termination receipt: %w", pod.Name, err)
		}
		if !safe {
			continue
		}
		matching, err := postgreSQLDataBootstrapIndexForPod(pod, cluster)
		if err != nil {
			return false, err
		}
		if matching < 0 {
			continue
		}
		bootstrap := cluster.Status.PostgreSQLBootstraps[matching]
		if err := validatePostgreSQLFinalizationPod(pod, cluster, bootstrap); err != nil {
			return false, err
		}
		original := pod.DeepCopy()
		controllerutil.RemoveFinalizer(pod, owned.PostgreSQLPodTerminationFinalizer)
		patch := client.MergeFromWithOptions(original, client.MergeFromWithOptimisticLock{})
		if err := r.Patch(ctx, pod, patch); err != nil && !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("release terminal PostgreSQL Pod %s with UID %s: %w", pod.Name, pod.UID, err)
		}
		released = true
	}
	return released, nil
}

// verifyPostgreSQLPodTerminationFences runs before any workload controller is
// deleted. Every live Pod that mounts recorded PGDATA must be the exact managed
// Pod and must already carry the finalizer that preserves authenticated process
// termination evidence. A safely terminated Pod whose finalizer was just
// released will disappear before the PVC barrier can complete.
func (r *PgShardClusterReconciler) verifyPostgreSQLPodTerminationFences(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) error {
	if r.APIReader == nil {
		return fmt.Errorf("authoritative API reader is required for PostgreSQL Pod termination-fence verification")
	}
	pods := &corev1.PodList{}
	if err := r.APIReader.List(ctx, pods, client.InNamespace(cluster.Namespace)); err != nil {
		return fmt.Errorf("list PostgreSQL Pods before workload pruning: %w", err)
	}
	for index := range pods.Items {
		pod := &pods.Items[index]
		matching, err := postgreSQLDataBootstrapIndexForPod(pod, cluster)
		if err != nil {
			return err
		}
		if matching < 0 {
			continue
		}
		bootstrap := cluster.Status.PostgreSQLBootstraps[matching]
		if err := validatePostgreSQLFinalizationPod(pod, cluster, bootstrap); err != nil {
			return err
		}
		if controllerutil.ContainsFinalizer(pod, owned.PostgreSQLPodTerminationFinalizer) {
			continue
		}
		if pod.DeletionTimestamp != nil {
			safe, err := r.podHasSafeTerminationEvidence(ctx, pod)
			if err != nil {
				return fmt.Errorf("verify PostgreSQL Pod %s termination receipt: %w", pod.Name, err)
			}
			if safe {
				continue
			}
		}
		return fmt.Errorf("PostgreSQL Pod %s with UID %s lacks its termination finalizer", pod.Name, pod.UID)
	}
	return nil
}

// deletePostgreSQLPodsForFinalization establishes an authoritative
// PGDATA-Pod-absence barrier after every bootstrap Secret is absent. Credential
// clients do not own storage and their sessions are severed by the authenticated
// PostgreSQL process proof. Any Pod that mounts the protected PVC remains in the
// barrier regardless of phase or identity.
func (r *PgShardClusterReconciler) deletePostgreSQLPodsForFinalization(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (bool, error) {
	if r.APIReader == nil {
		return false, fmt.Errorf("authoritative API reader is required for PostgreSQL Pod finalization")
	}
	pods := &corev1.PodList{}
	if err := r.APIReader.List(ctx, pods, client.InNamespace(cluster.Namespace)); err != nil {
		return false, fmt.Errorf("list PostgreSQL Pods during finalization: %w", err)
	}
	pending := false
	for index := range pods.Items {
		pod := &pods.Items[index]
		matching, err := postgreSQLDataBootstrapIndexForPod(pod, cluster)
		if err != nil {
			return false, err
		}
		if matching < 0 {
			continue
		}
		pending = true
		bootstrap := cluster.Status.PostgreSQLBootstraps[matching]
		if err := validatePostgreSQLFinalizationPod(pod, cluster, bootstrap); err != nil {
			return false, err
		}
		if !controllerutil.ContainsFinalizer(pod, owned.PostgreSQLPodTerminationFinalizer) {
			if pod.DeletionTimestamp != nil {
				safe, err := r.podHasSafeTerminationEvidence(ctx, pod)
				if err != nil {
					return false, fmt.Errorf("verify PostgreSQL Pod %s termination receipt: %w", pod.Name, err)
				}
				if safe {
					continue
				}
			}
			return false, fmt.Errorf("PostgreSQL Pod %s with UID %s lacks its termination finalizer", pod.Name, pod.UID)
		}
		if pod.DeletionTimestamp != nil {
			continue
		}
		uid := pod.UID
		resourceVersion := pod.ResourceVersion
		if err := r.Delete(ctx, pod, client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}); err != nil && !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("delete PostgreSQL Pod %s with UID %s during finalization: %w", pod.Name, pod.UID, err)
		}
	}
	return pending, nil
}

func postgreSQLDataBootstrapIndexForPod(pod *corev1.Pod, cluster *pgshardv1alpha1.PgShardCluster) (int, error) {
	matching := -1
	for bootstrapIndex := range cluster.Status.PostgreSQLBootstraps {
		bootstrap := cluster.Status.PostgreSQLBootstraps[bootstrapIndex]
		if !podSpecReferencesPostgreSQLDataPVC(pod.Spec, bootstrap.PVCName) {
			continue
		}
		if matching >= 0 {
			return -1, fmt.Errorf("Pod %s references PostgreSQL data PVCs for multiple shards", pod.Name)
		}
		matching = bootstrapIndex
	}
	return matching, nil
}

func podSpecReferencesPostgreSQLDataPVC(spec corev1.PodSpec, pvcName string) bool {
	for _, volume := range spec.Volumes {
		if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == pvcName {
			return true
		}
	}
	return false
}

func podHasTerminalPhase(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}

func (r *PgShardClusterReconciler) podHasSafeTerminationEvidence(ctx context.Context, pod *corev1.Pod) (bool, error) {
	if pod.Spec.NodeName == "" {
		return true, nil
	}
	return r.podFencingHandshakeCodec().VerifyTermination(ctx, pod)
}

func validatePostgreSQLFinalizationPod(pod *corev1.Pod, cluster *pgshardv1alpha1.PgShardCluster, bootstrap pgshardv1alpha1.PostgreSQLBootstrapStatus) error {
	expectedStatefulSet := owned.PostgreSQLPrimaryStatefulSetName(cluster.Name, bootstrap.Shard)
	owner := metav1.GetControllerOf(pod)
	if pod.Name != expectedStatefulSet+"-0" ||
		pod.Annotations[owned.PostgreSQLPodClusterUIDAnnotation] != string(cluster.UID) ||
		pod.Labels[owned.ClusterLabel] != cluster.Name ||
		pod.Labels[owned.ComponentLabel] != "postgresql" ||
		pod.Labels[owned.ShardLabel] != fmt.Sprintf("%04d", bootstrap.Shard) ||
		pod.Labels[owned.RoleLabel] != "primary" ||
		pod.Labels[owned.MemberLabel] != "0000" ||
		owner == nil || owner.APIVersion != appsv1.SchemeGroupVersion.String() || owner.Kind != "StatefulSet" || owner.Name != expectedStatefulSet || owner.UID == "" {
		return fmt.Errorf("Pod %s references protected PostgreSQL data but is not the exact shard %d StatefulSet Pod", pod.Name, bootstrap.Shard)
	}
	return nil
}

func namespaceUnavailableForCreate(ctx context.Context, reader client.Reader, name string) (bool, error) {
	namespace := &corev1.Namespace{}
	if err := reader.Get(ctx, types.NamespacedName{Name: name}, namespace); apierrors.IsNotFound(err) {
		return true, nil
	} else if err != nil {
		return false, fmt.Errorf("read namespace %s before resolving PostgreSQL data creation intent: %w", name, err)
	}
	return namespace.DeletionTimestamp != nil, nil
}

func (r *PgShardClusterReconciler) prune(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, plan []client.Object, includePVCs bool) (bool, error) {
	desired := make(map[string]struct{}, len(plan))
	for _, object := range plan {
		desired[owned.Key(object)] = struct{}{}
	}

	var existing []client.Object
	reader := client.Reader(r.Client)
	if includePVCs {
		if r.APIReader == nil {
			return false, fmt.Errorf("authoritative API reader is required for deletion finalization")
		}
		reader = r.APIReader
	}
	listOptions := []client.ListOption{client.InNamespace(cluster.Namespace)}
	lists := []client.ObjectList{
		&corev1.ConfigMapList{},
		&corev1.ServiceList{},
		&appsv1.DeploymentList{},
		&appsv1.StatefulSetList{},
		&autoscalingv2.HorizontalPodAutoscalerList{},
		&networkingv1.NetworkPolicyList{},
		&policyv1.PodDisruptionBudgetList{},
	}
	if includePVCs {
		lists = append(lists, &corev1.PersistentVolumeClaimList{})
	}
	for _, list := range lists {
		if err := reader.List(ctx, list, listOptions...); err != nil {
			return false, fmt.Errorf("list %T: %w", list, err)
		}
		existing = append(existing, listObjects(list)...)
	}

	stale := make([]client.Object, 0)
	for _, object := range existing {
		if includePVCs && isRecordedPostgreSQLDataPVC(cluster, object) {
			// Data-policy finalization handles these exact names and UIDs after
			// workload pruning, including legacy or externally altered ownership.
			continue
		}
		if !metav1.IsControlledBy(object, cluster) {
			continue
		}
		if _, keep := desired[owned.Key(object)]; !keep {
			if !includePVCs && retainPostgreSQLConfigurationDuringRollout(cluster, object, existing) {
				continue
			}
			stale = append(stale, object)
		}
	}
	sort.Slice(stale, func(i, j int) bool { return owned.Key(stale[i]) < owned.Key(stale[j]) })
	for _, object := range stale {
		uid := object.GetUID()
		resourceVersion := object.GetResourceVersion()
		preconditions := client.Preconditions{UID: &uid}
		deleteOptions := []client.DeleteOption{preconditions}
		if !includePVCs {
			preconditions.ResourceVersion = &resourceVersion
			deleteOptions[0] = preconditions
		} else {
			deleteOptions = append(deleteOptions, client.PropagationPolicy(metav1.DeletePropagationForeground))
		}
		if err := r.Delete(ctx, object, deleteOptions...); err != nil && !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("delete %T %s/%s: %w", object, object.GetNamespace(), object.GetName(), err)
		}
	}
	return len(stale) > 0, nil
}

func isRecordedPostgreSQLDataPVC(cluster *pgshardv1alpha1.PgShardCluster, object client.Object) bool {
	if _, ok := object.(*corev1.PersistentVolumeClaim); !ok || object.GetNamespace() != cluster.Namespace {
		return false
	}
	for _, bootstrap := range cluster.Status.PostgreSQLBootstraps {
		if bootstrap.PVCName != "" && object.GetName() == bootstrap.PVCName {
			return true
		}
	}
	return false
}

func retainPostgreSQLConfigurationDuringRollout(cluster *pgshardv1alpha1.PgShardCluster, object client.Object, existing []client.Object) bool {
	configuration, ok := object.(*corev1.ConfigMap)
	if !ok || !strings.HasPrefix(configuration.Name, cluster.Name+owned.PostgreSQLConfigSuffix+"-") {
		return false
	}
	for _, candidate := range existing {
		if !metav1.IsControlledBy(candidate, cluster) {
			continue
		}
		switch workload := candidate.(type) {
		case *appsv1.Deployment:
			if !podSpecReferencesPostgreSQLConfiguration(workload.Spec.Template.Spec) {
				continue
			}
			if podSpecReferencesConfigMap(workload.Spec.Template.Spec, configuration.Name) || !deploymentRolloutComplete(workload) {
				return true
			}
		case *appsv1.StatefulSet:
			if !podSpecReferencesPostgreSQLConfiguration(workload.Spec.Template.Spec) {
				continue
			}
			if podSpecReferencesConfigMap(workload.Spec.Template.Spec, configuration.Name) || !statefulSetRolloutComplete(workload) {
				return true
			}
		}
	}
	return false
}

func podSpecReferencesPostgreSQLConfiguration(spec corev1.PodSpec) bool {
	for _, volume := range spec.Volumes {
		if volume.ConfigMap != nil && strings.Contains(volume.ConfigMap.Name, owned.PostgreSQLConfigSuffix+"-") {
			return true
		}
	}
	return false
}

func podSpecReferencesConfigMap(spec corev1.PodSpec, name string) bool {
	for _, volume := range spec.Volumes {
		if volume.ConfigMap != nil && volume.ConfigMap.Name == name {
			return true
		}
	}
	return false
}

func deploymentRolloutComplete(deployment *appsv1.Deployment) bool {
	desired := int32(1)
	if deployment.Spec.Replicas != nil {
		desired = *deployment.Spec.Replicas
	}
	return workloadGenerationObserved(deployment.Generation, deployment.Status.ObservedGeneration) &&
		deployment.Status.Replicas == desired && deployment.Status.UpdatedReplicas == desired
}

func statefulSetRolloutComplete(statefulSet *appsv1.StatefulSet) bool {
	desired := int32(1)
	if statefulSet.Spec.Replicas != nil {
		desired = *statefulSet.Spec.Replicas
	}
	updated := workloadGenerationObserved(statefulSet.Generation, statefulSet.Status.ObservedGeneration) &&
		statefulSet.Status.Replicas == desired && statefulSet.Status.UpdatedReplicas == desired
	if statefulSet.Spec.UpdateStrategy.Type == appsv1.OnDeleteStatefulSetStrategyType {
		return updated
	}
	return updated && statefulSet.Status.CurrentRevision != "" && statefulSet.Status.CurrentRevision == statefulSet.Status.UpdateRevision
}

func listObjects(list client.ObjectList) []client.Object {
	var result []client.Object
	switch list := list.(type) {
	case *corev1.ConfigMapList:
		for index := range list.Items {
			result = append(result, &list.Items[index])
		}
	case *corev1.ServiceList:
		for index := range list.Items {
			result = append(result, &list.Items[index])
		}
	case *corev1.PersistentVolumeClaimList:
		for index := range list.Items {
			result = append(result, &list.Items[index])
		}
	case *appsv1.DeploymentList:
		for index := range list.Items {
			result = append(result, &list.Items[index])
		}
	case *appsv1.StatefulSetList:
		for index := range list.Items {
			result = append(result, &list.Items[index])
		}
	case *autoscalingv2.HorizontalPodAutoscalerList:
		for index := range list.Items {
			result = append(result, &list.Items[index])
		}
	case *networkingv1.NetworkPolicyList:
		for index := range list.Items {
			result = append(result, &list.Items[index])
		}
	case *policyv1.PodDisruptionBudgetList:
		for index := range list.Items {
			result = append(result, &list.Items[index])
		}
	}
	return result
}

func (r *PgShardClusterReconciler) supportingWorkloadsAvailable(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (bool, string, error) {
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	etcd := &appsv1.StatefulSet{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + owned.EtcdSuffix}, etcd); err != nil {
		return false, "", err
	}
	orchestrator := &appsv1.Deployment{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + owned.OrchestratorSuffix}, orchestrator); err != nil {
		return false, "", err
	}
	pooler := &appsv1.Deployment{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + owned.PoolerSuffix}, pooler); err != nil {
		return false, "", err
	}

	etcdWanted := int32(0)
	if etcd.Spec.Replicas != nil {
		etcdWanted = *etcd.Spec.Replicas
	}
	orchestratorWanted := int32(0)
	if orchestrator.Spec.Replicas != nil {
		orchestratorWanted = *orchestrator.Spec.Replicas
	}
	poolerWanted := poolerMinimum(cluster)
	if pooler.Spec.Replicas != nil && *pooler.Spec.Replicas > poolerWanted {
		poolerWanted = *pooler.Spec.Replicas
	}
	etcdReady := workloadGenerationObserved(etcd.Generation, etcd.Status.ObservedGeneration) && etcd.Status.ReadyReplicas >= etcdWanted && etcd.Status.UpdatedReplicas >= etcdWanted
	orchestratorReady := workloadGenerationObserved(orchestrator.Generation, orchestrator.Status.ObservedGeneration) && orchestrator.Status.AvailableReplicas >= orchestratorWanted && orchestrator.Status.UpdatedReplicas >= orchestratorWanted
	poolerReady := workloadGenerationObserved(pooler.Generation, pooler.Status.ObservedGeneration) && pooler.Status.AvailableReplicas >= poolerWanted && pooler.Status.UpdatedReplicas >= poolerWanted
	autoscalingReady := true
	autoscalingMessage := "fixed scaling selected"
	if cluster.Spec.Pooler.Scaling.Mode == pgshardv1alpha1.ScalingHPA {
		hpa := &autoscalingv2.HorizontalPodAutoscaler{}
		if err := reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + owned.PoolerSuffix}, hpa); err != nil {
			return false, "", err
		}
		observed := hpa.Status.ObservedGeneration != nil && *hpa.Status.ObservedGeneration >= hpa.Generation
		autoscalingReady = observed && hpaConditionTrue(hpa, autoscalingv2.AbleToScale) && hpaConditionTrue(hpa, autoscalingv2.ScalingActive)
		autoscalingMessage = fmt.Sprintf("HPA active=%t", autoscalingReady)
	}
	message := fmt.Sprintf("etcd %d/%d, orchestrator %d/%d, pooler %d/%d replicas available; %s", etcd.Status.ReadyReplicas, etcdWanted, orchestrator.Status.AvailableReplicas, orchestratorWanted, pooler.Status.AvailableReplicas, poolerWanted, autoscalingMessage)
	return etcdReady && orchestratorReady && poolerReady && autoscalingReady, message, nil
}

func (r *PgShardClusterReconciler) postgresqlWorkloadsAvailable(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (bool, string, string, error) {
	if cluster.Spec.MembersPerShard != 1 {
		return false, "PostgreSQLHAUnavailable", "three- and five-member PostgreSQL lifecycle remains disabled until bootstrap, replication, fencing, promotion, and recovery are implemented", nil
	}
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	ready := int32(0)
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		statefulSet := &appsv1.StatefulSet{}
		key := types.NamespacedName{Namespace: cluster.Namespace, Name: owned.PostgreSQLPrimaryStatefulSetName(cluster.Name, shard)}
		if err := reader.Get(ctx, key, statefulSet); apierrors.IsNotFound(err) {
			continue
		} else if err != nil {
			return false, "", "", err
		}
		pod := &corev1.Pod{}
		podKey := types.NamespacedName{Namespace: cluster.Namespace, Name: key.Name + "-0"}
		if err := reader.Get(ctx, podKey, pod); apierrors.IsNotFound(err) {
			continue
		} else if err != nil {
			return false, "", "", err
		}
		if !podfence.IsManagedPostgreSQLPod(pod) {
			return false, "PostgreSQLPodFencingUnavailable", fmt.Sprintf("PostgreSQL Pod %s does not preserve its operator identity and termination finalizer", pod.Name), nil
		}
		if pod.Spec.NodeName != "" && (pod.Annotations[podfence.NodeUIDAnnotation] == "" || pod.Annotations[podfence.NodeBootIDAnnotation] == "") {
			return false, "PostgreSQLPodFencingUnavailable", fmt.Sprintf("PostgreSQL Pod %s was assigned without binding-time Node UID and boot ID; deletion remains fail closed", pod.Name), nil
		}
		if workloadGenerationObserved(statefulSet.Generation, statefulSet.Status.ObservedGeneration) && statefulSet.Status.ReadyReplicas >= 1 && statefulSet.Status.AvailableReplicas >= 1 && statefulSet.Status.UpdatedReplicas >= 1 && pod.Spec.NodeName != "" {
			ready++
		}
	}
	message := fmt.Sprintf("single-member PostgreSQL primaries %d/%d ready; this mode has no standby, promotion, or zero-downtime restart guarantee", ready, cluster.Spec.Shards)
	if ready != cluster.Spec.Shards {
		return false, "PostgreSQLPrimariesProgressing", message, nil
	}
	return true, "SingleMemberPrimariesAvailable", message, nil
}

func hpaConditionTrue(hpa *autoscalingv2.HorizontalPodAutoscaler, conditionType autoscalingv2.HorizontalPodAutoscalerConditionType) bool {
	for _, condition := range hpa.Status.Conditions {
		if condition.Type == conditionType {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func workloadGenerationObserved(generation, observed int64) bool {
	return generation == 0 || observed >= generation
}

func poolerMinimum(cluster *pgshardv1alpha1.PgShardCluster) int32 {
	if cluster.Spec.Pooler.Scaling.Mode == pgshardv1alpha1.ScalingFixed {
		return cluster.Spec.Pooler.Scaling.Fixed.Replicas
	}
	return cluster.Spec.Pooler.Scaling.HPA.MinReplicas
}

func (r *PgShardClusterReconciler) reportSuccess(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, available bool, availabilityMessage string, postgresqlAvailable bool, postgresqlReason, postgresqlMessage string) error {
	status := metav1.ConditionFalse
	reason := "SupportingWorkloadsProgressing"
	phase := "Reconciling"
	if available {
		status = metav1.ConditionTrue
		reason = "SupportingWorkloadsAvailable"
		phase = "Pending"
	}
	postgresqlStatus := metav1.ConditionFalse
	if postgresqlAvailable {
		postgresqlStatus = metav1.ConditionTrue
	}
	readyReason := "PostgreSQLHAUnavailable"
	readyMessage := "PostgreSQL Pods are intentionally absent until bootstrap, replication, fencing, promotion, and recovery are implemented"
	if cluster.Spec.MembersPerShard == 1 {
		readyReason = "DataPlaneUnavailable"
		readyMessage = "single-member PostgreSQL primaries are supported, but SQL routing and high-availability failover are not implemented"
	}
	conditions := []metav1.Condition{
		{
			Type:               reconciledCondition,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: cluster.Generation,
			Reason:             "ResourcesApplied",
			Message:            "the deterministic supporting-resource plan is applied and stale owned resources are pruned",
		},
		{
			Type:               supportingAvailableCondition,
			Status:             status,
			ObservedGeneration: cluster.Generation,
			Reason:             reason,
			Message:            availabilityMessage,
		},
		{
			Type:               postgresqlAvailableCondition,
			Status:             postgresqlStatus,
			ObservedGeneration: cluster.Generation,
			Reason:             postgresqlReason,
			Message:            postgresqlMessage,
		},
		{
			Type:               readyCondition,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: cluster.Generation,
			Reason:             readyReason,
			Message:            readyMessage,
		},
		{
			Type:               transportSecurityCondition,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: cluster.Generation,
			Reason:             "TransportTLSUnavailable",
			Message:            "etcd client/peer and PostgreSQL shard traffic lack authenticated TLS; ingress NetworkPolicies provide isolation only",
		},
	}
	return r.updateStatus(ctx, cluster, cluster.Generation, phase, conditions)
}

func (r *PgShardClusterReconciler) reportFailure(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, reason, message string) error {
	conditions := []metav1.Condition{
		{
			Type:               reconciledCondition,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: cluster.Generation,
			Reason:             reason,
			Message:            message,
		},
		{
			Type:               supportingAvailableCondition,
			Status:             metav1.ConditionUnknown,
			ObservedGeneration: cluster.Generation,
			Reason:             "ObservationStale",
			Message:            "supporting workload availability is not current because resource reconciliation did not complete",
		},
		{
			Type:               postgresqlAvailableCondition,
			Status:             metav1.ConditionUnknown,
			ObservedGeneration: cluster.Generation,
			Reason:             "ObservationStale",
			Message:            "PostgreSQL workload availability is not current because resource reconciliation did not complete",
		},
		{
			Type:               readyCondition,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: cluster.Generation,
			Reason:             reason,
			Message:            message,
		},
		{
			Type:               transportSecurityCondition,
			Status:             metav1.ConditionUnknown,
			ObservedGeneration: cluster.Generation,
			Reason:             "TransportSecurityUnobserved",
			Message:            "transport isolation could not be observed because resource reconciliation did not complete",
		},
	}
	// ObservedGeneration is intentionally not advanced when the plan was not
	// fully applied and pruned.
	return r.updateStatus(ctx, cluster, cluster.Status.ObservedGeneration, "Degraded", conditions)
}

func (r *PgShardClusterReconciler) reportScalingTransition(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) error {
	conditions := []metav1.Condition{
		{
			Type:               reconciledCondition,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: cluster.Generation,
			Reason:             "PoolerScalingTransition",
			Message:            "the HPA was deleted; fixed replicas will be claimed only after its absence is observed",
		},
		{
			Type:               supportingAvailableCondition,
			Status:             metav1.ConditionUnknown,
			ObservedGeneration: cluster.Generation,
			Reason:             "PoolerScalingTransition",
			Message:            "supporting workload availability is not evaluated during the pooler scaling handoff",
		},
		{
			Type:               postgresqlAvailableCondition,
			Status:             metav1.ConditionUnknown,
			ObservedGeneration: cluster.Generation,
			Reason:             "PoolerScalingTransition",
			Message:            "PostgreSQL workload availability is not evaluated during the pooler scaling handoff",
		},
		{
			Type:               readyCondition,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: cluster.Generation,
			Reason:             "DataPlaneUnavailable",
			Message:            "the data plane is not ready while the pooler scaling handoff is in progress",
		},
		{
			Type:               transportSecurityCondition,
			Status:             metav1.ConditionUnknown,
			ObservedGeneration: cluster.Generation,
			Reason:             "PoolerScalingTransition",
			Message:            "transport resources are not evaluated during the pooler scaling handoff",
		},
	}
	return r.updateStatus(ctx, cluster, cluster.Status.ObservedGeneration, "Reconciling", conditions)
}

func (r *PgShardClusterReconciler) updateStatus(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster, observedGeneration int64, phase string, conditions []metav1.Condition) error {
	before := cluster.DeepCopy().Status
	cluster.Status.ObservedGeneration = observedGeneration
	cluster.Status.Phase = phase
	for _, condition := range conditions {
		meta.SetStatusCondition(&cluster.Status.Conditions, condition)
	}
	if statusesEqual(before, cluster.Status) {
		return nil
	}
	return r.Status().Update(ctx, cluster)
}

func statusesEqual(left, right pgshardv1alpha1.PgShardClusterStatus) bool {
	if left.ObservedGeneration != right.ObservedGeneration || left.Phase != right.Phase || len(left.Conditions) != len(right.Conditions) || !bootstrapSpecsEqual(left.PostgreSQLBootstrapSpec, right.PostgreSQLBootstrapSpec) || !slices.EqualFunc(left.PostgreSQLBootstraps, right.PostgreSQLBootstraps, postgreSQLBootstrapsEqual) {
		return false
	}
	for index := range left.Conditions {
		if left.Conditions[index] != right.Conditions[index] {
			return false
		}
	}
	return true
}

func bootstrapSpecsEqual(left, right *pgshardv1alpha1.PostgreSQLBootstrapSpecStatus) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Shards == right.Shards && left.MembersPerShard == right.MembersPerShard && left.Durability == right.Durability && left.StorageSize == right.StorageSize && optionalStringsEqual(left.StorageClassName, right.StorageClassName) && left.DeletionPolicy == right.DeletionPolicy
}

func postgreSQLBootstrapsEqual(left, right pgshardv1alpha1.PostgreSQLBootstrapStatus) bool {
	return left.Shard == right.Shard && left.SecretName == right.SecretName && left.SecretUID == right.SecretUID && left.PVCFenceDetached == right.PVCFenceDetached && left.PVCName == right.PVCName && left.PVCUID == right.PVCUID && left.PVCCreationAbandoned == right.PVCCreationAbandoned && optionalStringsEqual(left.PVCStorageClassName, right.PVCStorageClassName)
}

func (r *PgShardClusterReconciler) SetupWithManager(manager ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(manager).
		For(&pgshardv1alpha1.PgShardCluster{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&appsv1.Deployment{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Named("pgshardcluster").
		Complete(r)
}
