package operator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

const (
	requeueNotReady = 10 * time.Second
	requeueReady    = 30 * time.Second
)

// ClusterReconciler reconciles PgShardCluster objects into groups of pods.
type ClusterReconciler struct {
	client.Client
	Renderer Renderer
	Prober   Prober
}

// SetupWithManager registers the reconciler and its watches.
func (r *ClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pgshardv1alpha1.PgShardCluster{}).
		Owns(&pgshardv1alpha1.PgShardGroup{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Secret{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Named("pgshardcluster").
		Complete(r)
}

// groupObservation is what one reconcile pass learned about a group.
type groupObservation struct {
	group        Group
	podsRunning  int
	podsReady    int
	primaryOK    bool
	primaryErr   string
	streaming    map[string]bool
	syncApplied  bool
	members      []pgshardv1alpha1.MemberStatus
	replicasWant int
}

func (o groupObservation) streamingCount() int {
	n := 0
	for _, ok := range o.streaming {
		if ok {
			n++
		}
	}
	return n
}

func (o groupObservation) ready() bool {
	return o.podsRunning == o.group.Replicas && o.primaryOK && o.streamingCount() >= o.replicasWant
}

// Reconcile drives one PgShardCluster to its desired state.
func (r *ClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	var cluster pgshardv1alpha1.PgShardCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !cluster.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	password, err := r.ensureSecret(ctx, &cluster)
	if err != nil {
		return ctrl.Result{}, err
	}

	var observations []groupObservation
	for _, g := range Groups(&cluster) {
		obs, err := r.reconcileGroup(ctx, &cluster, g, password)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("group %s: %w", g.Name(), err)
		}
		observations = append(observations, obs)
	}

	catalogReady := r.reconcileCatalogSchema(ctx, &cluster, observations[0], password)
	if err := r.updateStatus(ctx, &cluster, observations, catalogReady); err != nil {
		return ctrl.Result{}, err
	}
	for _, o := range observations {
		if !o.ready() {
			log.V(1).Info("group not ready", "group", o.group.Name(), "running", o.podsRunning, "streaming", o.streamingCount(), "primaryOK", o.primaryOK)
			return ctrl.Result{RequeueAfter: requeueNotReady}, nil
		}
	}
	return ctrl.Result{RequeueAfter: requeueReady}, nil
}

func (r *ClusterReconciler) ensureSecret(ctx context.Context, c *pgshardv1alpha1.PgShardCluster) (string, error) {
	var sec corev1.Secret
	key := types.NamespacedName{Namespace: c.Namespace, Name: SecretName(c.Name)}
	err := r.Get(ctx, key, &sec)
	if err == nil {
		if pw, ok := sec.Data[secretKey]; ok && len(pw) > 0 {
			return string(pw), nil
		}
		return "", fmt.Errorf("secret %s has no %q key", key.Name, secretKey)
	}
	if !apierrors.IsNotFound(err) {
		return "", err
	}
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	pw := hex.EncodeToString(buf)
	sec = corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace, Labels: map[string]string{LabelCluster: c.Name}},
		Type:       corev1.SecretTypeBasicAuth,
		StringData: map[string]string{"username": superuserName, secretKey: pw},
	}
	if err := controllerutil.SetControllerReference(c, &sec, r.Scheme()); err != nil {
		return "", err
	}
	if err := r.Create(ctx, &sec); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return r.ensureSecret(ctx, c)
		}
		return "", err
	}
	return pw, nil
}

// ensureOwned creates obj if absent. Existing objects are left as they are:
// pods and PVCs are immutable in the ways that matter, and the remaining
// objects only change when the spec does (handled by mutate on the update path).
func (r *ClusterReconciler) ensureOwned(ctx context.Context, owner client.Object, obj client.Object, mutate func() error) error {
	if err := controllerutil.SetControllerReference(owner, obj, r.Scheme()); err != nil {
		return err
	}
	if mutate == nil {
		err := r.Create(ctx, obj)
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return err
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, obj, func() error {
		if err := controllerutil.SetControllerReference(owner, obj, r.Scheme()); err != nil {
			return err
		}
		return mutate()
	})
	return err
}

func (r *ClusterReconciler) reconcileGroup(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, password string) (groupObservation, error) {
	obs := groupObservation{group: g, streaming: map[string]bool{}, replicasWant: g.Replicas - 1}

	pg := r.Renderer.PgShardGroup(c, g)
	if err := r.ensureOwned(ctx, c, pg, nil); err != nil {
		return obs, err
	}
	desiredCM := r.Renderer.ConfigMap(c, g)
	cm := &corev1.ConfigMap{ObjectMeta: desiredCM.ObjectMeta}
	if err := r.ensureOwned(ctx, c, cm, func() error {
		cm.Labels = desiredCM.Labels
		cm.Data = desiredCM.Data
		return nil
	}); err != nil {
		return obs, err
	}
	for _, desired := range r.Renderer.Services(c, g) {
		svc := &corev1.Service{ObjectMeta: desired.ObjectMeta}
		if err := r.ensureOwned(ctx, c, svc, func() error {
			svc.Labels = desired.Labels
			svc.Spec.Selector = desired.Spec.Selector
			svc.Spec.Ports = desired.Spec.Ports
			if desired.Spec.ClusterIP == corev1.ClusterIPNone {
				svc.Spec.ClusterIP = corev1.ClusterIPNone
				svc.Spec.PublishNotReadyAddresses = true
			}
			return nil
		}); err != nil {
			return obs, err
		}
	}
	for _, desired := range r.Renderer.PDBs(c, g) {
		pdb := &policyv1.PodDisruptionBudget{ObjectMeta: desired.ObjectMeta}
		if err := r.ensureOwned(ctx, c, pdb, func() error {
			pdb.Labels = desired.Labels
			pdb.Spec = desired.Spec
			return nil
		}); err != nil {
			return obs, err
		}
	}

	for i := 0; i < g.Replicas; i++ {
		if err := r.ensureOwned(ctx, c, r.Renderer.PVC(c, g, i), nil); err != nil {
			return obs, err
		}
		pod, err := r.ensurePod(ctx, c, g, i)
		if err != nil {
			return obs, err
		}
		member := pgshardv1alpha1.MemberStatus{Name: g.MemberName(i), Role: pod.Labels[LabelRole]}
		if pod.Status.Phase == corev1.PodRunning {
			obs.podsRunning++
		}
		if podReady(pod) {
			obs.podsReady++
			member.Ready = true
		}
		obs.members = append(obs.members, member)
	}

	dsn := DSN(g.ServiceRW(), c.Namespace, password)
	state, err := r.Prober.Probe(ctx, dsn)
	if err != nil {
		obs.primaryErr = err.Error()
		return obs, nil
	}
	obs.primaryOK = true
	for i := 1; i < g.Replicas; i++ {
		if name := g.MemberName(i); state.Streaming[name] {
			obs.streaming[name] = true
		}
	}
	want := SyncStandbyNames(g, c.Spec.Durability.MinSyncStandbys, obs.streaming)
	if want != state.SyncStandbyNames {
		if err := r.Prober.SetSyncStandbyNames(ctx, dsn, want); err != nil {
			obs.primaryErr = "set synchronous_standby_names: " + err.Error()
			return obs, nil
		}
	}
	obs.syncApplied = true
	return obs, nil
}

func (r *ClusterReconciler) ensurePod(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, ordinal int) (*corev1.Pod, error) {
	desired := r.Renderer.Pod(c, g, ordinal)
	var pod corev1.Pod
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), &pod)
	switch {
	case apierrors.IsNotFound(err):
		if err := r.ensureOwned(ctx, c, desired, nil); err != nil {
			return nil, err
		}
		return desired, nil
	case err != nil:
		return nil, err
	}
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		if err := r.Delete(ctx, &pod); client.IgnoreNotFound(err) != nil {
			return nil, err
		}
	}
	return &pod, nil
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (r *ClusterReconciler) reconcileCatalogSchema(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, catalog groupObservation, password string) metav1.Condition {
	cond := metav1.Condition{Type: ConditionCatalogReady, Status: metav1.ConditionFalse, ObservedGeneration: c.Generation}
	if !catalog.ready() {
		cond.Reason = "CatalogGroupNotReady"
		cond.Message = "catalog group is not ready"
		return cond
	}
	if err := r.Prober.MigrateCatalog(ctx, DSN(catalog.group.ServiceRW(), c.Namespace, password)); err != nil {
		cond.Reason = "MigrationFailed"
		cond.Message = err.Error()
		return cond
	}
	cond.Status = metav1.ConditionTrue
	cond.Reason = "Migrated"
	cond.Message = "catalog schema is current"
	return cond
}

func (r *ClusterReconciler) updateStatus(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, obs []groupObservation, catalogReady metav1.Condition) error {
	ready := true
	primaryOK := true
	replOK := true
	var msg string
	var shards []pgshardv1alpha1.ShardStatus
	for _, o := range obs {
		if !o.ready() {
			ready = false
			if msg == "" {
				msg = fmt.Sprintf("group %s: %d/%d pods running, primary ok=%t (%s), %d/%d replicas streaming",
					o.group.Name(), o.podsRunning, o.group.Replicas, o.primaryOK, o.primaryErr, o.streamingCount(), o.replicasWant)
			}
		}
		if !o.primaryOK {
			primaryOK = false
		}
		if o.streamingCount() < o.replicasWant {
			replOK = false
		}
		if err := r.updateGroupStatus(ctx, c, o); err != nil {
			return err
		}
		if o.group.Kind == "shard" {
			shards = append(shards, pgshardv1alpha1.ShardStatus{
				ID: o.group.ShardID, Primary: o.group.MemberName(0), Epoch: 0, Members: o.members,
			})
		}
	}
	if ready && catalogReady.Status != metav1.ConditionTrue {
		ready = false
		msg = catalogReady.Message
	}
	if ready {
		msg = "all groups have a primary accepting SQL and every replica streaming"
	}
	base := c.DeepCopy()
	set := func(t string, ok bool, reason, message string) {
		st := metav1.ConditionFalse
		if ok {
			st = metav1.ConditionTrue
		}
		meta.SetStatusCondition(&c.Status.Conditions, metav1.Condition{
			Type: t, Status: st, Reason: reason, Message: message, ObservedGeneration: c.Generation,
		})
	}
	set(pgshardv1alpha1.ConditionReady, ready, boolReason(ready, "Ready", "NotReady"), msg)
	set(pgshardv1alpha1.ConditionPrimaryHealthy, primaryOK, boolReason(primaryOK, "AcceptingSQL", "ProbeFailed"), "")
	set(pgshardv1alpha1.ConditionReplicationHealthy, replOK, boolReason(replOK, "AllStreaming", "ReplicasMissing"), "")
	set(pgshardv1alpha1.ConditionProgressing, !ready, boolReason(!ready, "Reconciling", "Stable"), "")
	meta.SetStatusCondition(&c.Status.Conditions, catalogReady)
	c.Status.ObservedGeneration = c.Generation
	c.Status.Shards = shards
	return r.Status().Patch(ctx, c, client.MergeFrom(base))
}

func boolReason(ok bool, yes, no string) string {
	if ok {
		return yes
	}
	return no
}

func (r *ClusterReconciler) updateGroupStatus(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, o groupObservation) error {
	var pg pgshardv1alpha1.PgShardGroup
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: o.group.Prefix()}, &pg); err != nil {
		return err
	}
	base := pg.DeepCopy()
	pg.Status.Primary = ""
	if o.primaryOK {
		pg.Status.Primary = o.group.MemberName(0)
	}
	pg.Status.Epoch = 0
	pg.Status.Members = o.members
	for i := range pg.Status.Members {
		if i > 0 {
			pg.Status.Members[i].Ready = pg.Status.Members[i].Ready && o.streaming[pg.Status.Members[i].Name]
		}
	}
	return r.Status().Patch(ctx, &pg, client.MergeFrom(base))
}
