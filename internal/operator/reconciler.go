package operator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
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
	requeueFailover = 2 * time.Second
)

// ClusterReconciler reconciles PgShardCluster objects into groups of pods
// and is the single failover decision maker for every group.
type ClusterReconciler struct {
	client.Client
	Renderer Renderer
	Prober   Prober
	Agents   AgentClient
	// FailoverDelay overrides DefaultFailoverDelay; PollInterval the
	// quiesce poll; Now the clock. Zero values mean the defaults.
	FailoverDelay  time.Duration
	PollInterval   time.Duration
	QuiesceTimeout time.Duration
	Now            func() time.Time

	mu             sync.Mutex
	unhealthySince map[string]time.Time
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
		Owns(&corev1.ServiceAccount{}).
		Owns(&rbacv1.Role{}).
		Owns(&rbacv1.RoleBinding{}).
		Owns(&coordinationv1.Lease{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Named("pgshardcluster").
		Complete(r)
}

// groupState is the persistent failover state of a group, read from and
// written to the PgShardGroup status.
type groupState struct {
	primary string
	epoch   int64
	// syncSet holds the standbys that were streaming at the last healthy
	// observation: the members an acknowledged commit may only exist on.
	syncSet map[string]bool
}

// memberInfo is one member's pod as observed this pass.
type memberInfo struct {
	name    string
	pod     *corev1.Pod
	running bool
	ready   bool
	ip      string
}

// groupObservation is what one reconcile pass learned about a group.
type groupObservation struct {
	group        Group
	state        groupState
	podsRunning  int
	podsReady    int
	primaryOK    bool
	primaryErr   string
	streaming    map[string]bool
	syncApplied  bool
	members      []pgshardv1alpha1.MemberStatus
	replicasWant int
	// failing is set while the primary is unhealthy or a failover is pending.
	failing bool
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
	if err := r.ensureMemberRBAC(ctx, &cluster); err != nil {
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

	catalogReady := r.reconcileCatalogSchema(ctx, &cluster, observations, password)
	if err := r.updateStatus(ctx, &cluster, observations, catalogReady); err != nil {
		return ctrl.Result{}, err
	}
	requeue := requeueReady
	for _, o := range observations {
		if o.failing {
			return ctrl.Result{RequeueAfter: requeueFailover}, nil
		}
		if !o.ready() {
			log.V(1).Info("group not ready", "group", o.group.Name(), "running", o.podsRunning, "streaming", o.streamingCount(), "primaryOK", o.primaryOK)
			requeue = requeueNotReady
		}
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
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

func (r *ClusterReconciler) ensureMemberRBAC(ctx context.Context, c *pgshardv1alpha1.PgShardCluster) error {
	dSA, dRole, dRB := r.Renderer.MemberRBAC(c)
	sa := &corev1.ServiceAccount{ObjectMeta: dSA.ObjectMeta}
	if err := r.ensureOwned(ctx, c, sa, func() error { sa.Labels = dSA.Labels; return nil }); err != nil {
		return err
	}
	role := &rbacv1.Role{ObjectMeta: dRole.ObjectMeta}
	if err := r.ensureOwned(ctx, c, role, func() error { role.Labels = dRole.Labels; role.Rules = dRole.Rules; return nil }); err != nil {
		return err
	}
	rb := &rbacv1.RoleBinding{ObjectMeta: dRB.ObjectMeta}
	return r.ensureOwned(ctx, c, rb, func() error {
		rb.Labels = dRB.Labels
		rb.Subjects = dRB.Subjects
		if rb.RoleRef.Name == "" {
			rb.RoleRef = dRB.RoleRef
		}
		return nil
	})
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

// loadState reads the group's designated primary and epoch, designating
// member 0 at epoch 0 for a new group.
func (r *ClusterReconciler) loadState(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group) (groupState, error) {
	pg := r.Renderer.PgShardGroup(c, g)
	if err := r.ensureOwned(ctx, c, pg, nil); err != nil {
		return groupState{}, err
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(pg), pg); err != nil {
		return groupState{}, err
	}
	st := groupState{primary: pg.Status.Primary, epoch: pg.Status.Epoch, syncSet: map[string]bool{}}
	for _, m := range pg.Status.Members {
		if m.Name != st.primary && m.Ready {
			st.syncSet[m.Name] = true
		}
	}
	if st.primary == "" {
		st.primary = g.MemberName(0)
		base := pg.DeepCopy()
		pg.Status.Primary = st.primary
		pg.Status.Epoch = 0
		if err := r.Status().Patch(ctx, pg, client.MergeFrom(base)); err != nil {
			return groupState{}, err
		}
	}
	return st, nil
}

func (r *ClusterReconciler) ensureConfigMap(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, primary string) error {
	desired := r.Renderer.ConfigMap(c, g, primary)
	cm := &corev1.ConfigMap{ObjectMeta: desired.ObjectMeta}
	return r.ensureOwned(ctx, c, cm, func() error {
		cm.Labels = desired.Labels
		cm.Data = desired.Data
		return nil
	})
}

func (r *ClusterReconciler) reconcileGroup(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, password string) (groupObservation, error) {
	obs := groupObservation{group: g, streaming: map[string]bool{}, replicasWant: g.Replicas - 1}

	state, err := r.loadState(ctx, c, g)
	if err != nil {
		return obs, err
	}
	obs.state = state
	if err := r.ensureConfigMap(ctx, c, g, state.primary); err != nil {
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

	members := map[string]*memberInfo{}
	for i := 0; i < g.Replicas; i++ {
		if err := r.ensureOwned(ctx, c, r.Renderer.PVC(c, g, i), nil); err != nil {
			return obs, err
		}
		name := g.MemberName(i)
		// A missing primary pod is a failover trigger, not something to
		// recreate: a fresh pod would take the Lease back and no promotion
		// would happen. It is recreated as a replica once the group moved on,
		// or as the primary when no candidate exists.
		createIfMissing := name != state.primary || len(state.syncSet) == 0
		m, err := r.observePod(ctx, c, g, i, state.primary, createIfMissing)
		if err != nil {
			return obs, err
		}
		members[name] = m
	}

	if target := c.Annotations[AnnotationSwitchover]; target != "" && g.HasMember(target) {
		return r.switchover(ctx, c, g, obs, members, password, target)
	}

	primary := members[state.primary]
	var st AgentStatus
	stErr := errors.New("pod missing")
	switch {
	case primary.pod != nil && primary.ip != "":
		st, stErr = r.Agents.Status(ctx, agentAddr(primary.ip))
	case primary.pod != nil:
		stErr = errors.New("pod has no IP yet")
	}
	healthy := primaryHealthy(primary.pod, primary.ready, st, stErr)
	unhealthyFor := r.unhealthyFor(g.Prefix(), !healthy)
	if !healthy {
		obs.failing = true
		obs.primaryErr = "primary unhealthy"
		if stErr != nil {
			obs.primaryErr += ": " + stErr.Error()
		}
		if unhealthyFor >= r.failoverDelay() {
			state, err = r.failover(ctx, c, g, state, members, password, "")
			obs.state = state
			if errors.Is(err, errNoCandidate) {
				logf.FromContext(ctx).Info("primary unhealthy but no candidate; keeping the current primary", "group", g.Name(), "primary", state.primary)
				r.unhealthyFor(g.Prefix(), false)
				if primary.pod == nil {
					if _, err := r.observePod(ctx, c, g, ordinalOf(g, state.primary), state.primary, true); err != nil {
						return obs, err
					}
				}
				return r.finishGroup(ctx, c, g, obs, members), nil
			}
			if err != nil {
				return obs, err
			}
			if err := r.ensureConfigMap(ctx, c, g, state.primary); err != nil {
				return obs, err
			}
		}
		return r.finishGroup(ctx, c, g, obs, members), nil
	}

	state, err = r.converge(ctx, c, g, state, members, password)
	obs.state = state
	if err != nil {
		return obs, err
	}
	obs = r.finishGroup(ctx, c, g, obs, members)

	dsn := DSN(g.ServiceRW(), c.Namespace, password)
	pstate, err := r.Prober.Probe(ctx, dsn)
	if err != nil {
		obs.primaryErr = err.Error()
		return obs, nil
	}
	obs.primaryOK = true
	var slots []string
	for _, name := range g.MemberNames() {
		if name == state.primary {
			continue
		}
		slots = append(slots, SlotName(name))
		if pstate.Streaming[name] {
			obs.streaming[name] = true
		}
	}
	if err := r.Prober.EnsureSlots(ctx, dsn, slots, SlotName(state.primary)); err != nil {
		obs.primaryErr = "ensure slots: " + err.Error()
		return obs, nil
	}
	want := SyncStandbyNames(g, state.primary, c.Spec.Durability.MinSyncStandbys, obs.streaming)
	if want != pstate.SyncStandbyNames {
		if err := r.Prober.SetSyncStandbyNames(ctx, dsn, want); err != nil {
			obs.primaryErr = "set synchronous_standby_names: " + err.Error()
			return obs, nil
		}
	}
	obs.syncApplied = true
	return obs, nil
}

func ordinalOf(g Group, member string) int {
	for i, m := range g.MemberNames() {
		if m == member {
			return i
		}
	}
	return -1
}

// finishGroup fills the pod counters and member statuses from members.
func (r *ClusterReconciler) finishGroup(_ context.Context, _ *pgshardv1alpha1.PgShardCluster, g Group, obs groupObservation, members map[string]*memberInfo) groupObservation {
	obs.podsRunning, obs.podsReady, obs.members = 0, 0, nil
	for _, name := range g.MemberNames() {
		m := members[name]
		ms := pgshardv1alpha1.MemberStatus{Name: name}
		if m != nil && m.pod != nil {
			ms.Role = m.pod.Labels[LabelRole]
			if m.running {
				obs.podsRunning++
			}
			if m.ready {
				obs.podsReady++
				ms.Ready = true
			}
		}
		obs.members = append(obs.members, ms)
	}
	return obs
}

// switchover runs a planned failover to target: it stops the current primary
// by deleting its pod (the agent shuts postgres down and releases the Lease),
// then promotes target through the ordinary failover path.
func (r *ClusterReconciler) switchover(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, obs groupObservation, members map[string]*memberInfo, password, target string) (groupObservation, error) {
	log := logf.FromContext(ctx).WithValues("group", g.Name(), "target", target)
	obs.failing = true
	state := obs.state
	clearAnnotation := func() error {
		base := c.DeepCopy()
		delete(c.Annotations, AnnotationSwitchover)
		return r.Patch(ctx, c, client.MergeFrom(base))
	}
	if target == state.primary {
		log.Info("switchover target is already the primary")
		return obs, clearAnnotation()
	}
	if !state.syncSet[target] {
		log.Info("switchover refused: target was not a streaming standby at the last observation")
		return obs, clearAnnotation()
	}
	if old := members[state.primary]; old != nil && old.pod != nil {
		if err := r.patchRole(ctx, old.pod, RoleUnhealthy); err != nil {
			return obs, err
		}
		if err := r.Delete(ctx, old.pod); client.IgnoreNotFound(err) != nil {
			return obs, err
		}
		if err := r.waitPodGone(ctx, old.pod); err != nil {
			return obs, err
		}
		old.pod, old.ip, old.ready, old.running = nil, "", false, false
	}
	state, err := r.failover(ctx, c, g, state, members, password, target)
	obs.state = state
	if err != nil {
		return obs, err
	}
	if err := r.ensureConfigMap(ctx, c, g, state.primary); err != nil {
		return obs, err
	}
	obs = r.finishGroup(ctx, c, g, obs, members)
	return obs, clearAnnotation()
}

func (r *ClusterReconciler) waitPodGone(ctx context.Context, pod *corev1.Pod) error {
	deadline := r.now().Add(r.quiesceTimeout())
	for {
		var cur corev1.Pod
		err := r.Get(ctx, client.ObjectKeyFromObject(pod), &cur)
		if apierrors.IsNotFound(err) || (err == nil && cur.UID != pod.UID) {
			return nil
		}
		if err != nil {
			return err
		}
		if r.now().After(deadline) {
			return fmt.Errorf("pod %s still present after %s", pod.Name, r.quiesceTimeout())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.pollInterval()):
		}
	}
}

// observePod fetches member ordinal's pod, creating it when absent and
// createIfMissing is set. Finished pods are deleted so they come back.
func (r *ClusterReconciler) observePod(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, ordinal int, primary string, createIfMissing bool) (*memberInfo, error) {
	name := g.MemberName(ordinal)
	role := RoleReplica
	if name == primary {
		role = RolePrimary
	}
	m := &memberInfo{name: name}
	var pod corev1.Pod
	err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: name}, &pod)
	switch {
	case apierrors.IsNotFound(err):
		if !createIfMissing {
			return m, nil
		}
		desired := r.Renderer.Pod(c, g, ordinal, role)
		if err := r.ensureOwned(ctx, c, desired, nil); err != nil {
			return nil, err
		}
		m.pod = desired
		return m, nil
	case err != nil:
		return nil, err
	}
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		if err := r.Delete(ctx, &pod); client.IgnoreNotFound(err) != nil {
			return nil, err
		}
	}
	m.pod = &pod
	m.running = pod.Status.Phase == corev1.PodRunning
	m.ready = podReady(&pod)
	m.ip = pod.Status.PodIP
	return m, nil
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (r *ClusterReconciler) reconcileCatalogSchema(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, obs []groupObservation, password string) metav1.Condition {
	cond := metav1.Condition{Type: ConditionCatalogReady, Status: metav1.ConditionFalse, ObservedGeneration: c.Generation}
	catalog := obs[0]
	if !catalog.ready() {
		cond.Reason = "CatalogGroupNotReady"
		cond.Message = "catalog group is not ready"
		return cond
	}
	dsn := DSN(catalog.group.ServiceRW(), c.Namespace, password)
	if err := r.Prober.MigrateCatalog(ctx, dsn); err != nil {
		cond.Reason = "MigrationFailed"
		cond.Message = err.Error()
		return cond
	}
	for _, o := range obs[1:] {
		if err := r.Prober.PublishShardStatus(ctx, dsn, o.group.ShardID, o.group.Name(), o.state.epoch, r.memberEndpoint(c, o.group, o.state.primary)); err != nil {
			cond.Reason = "PublishFailed"
			cond.Message = err.Error()
			return cond
		}
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
		members, err := r.updateGroupStatus(ctx, c, o)
		if err != nil {
			return err
		}
		if o.group.Kind == "shard" {
			shards = append(shards, pgshardv1alpha1.ShardStatus{
				ID: o.group.ShardID, Primary: o.state.primary, Epoch: o.state.epoch, Members: members,
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

// updateGroupStatus writes the designated primary, epoch and members. While
// the primary probe fails the standbys' Ready flags are carried over from
// the last healthy pass: they are the sync set failover selects from.
func (r *ClusterReconciler) updateGroupStatus(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, o groupObservation) ([]pgshardv1alpha1.MemberStatus, error) {
	var pg pgshardv1alpha1.PgShardGroup
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: o.group.Prefix()}, &pg); err != nil {
		return nil, err
	}
	base := pg.DeepCopy()
	previous := map[string]bool{}
	for _, m := range pg.Status.Members {
		previous[m.Name] = m.Ready
	}
	pg.Status.Primary = o.state.primary
	pg.Status.Epoch = o.state.epoch
	pg.Status.Members = o.members
	for i := range pg.Status.Members {
		m := &pg.Status.Members[i]
		if m.Name == o.state.primary {
			continue
		}
		if o.primaryOK {
			m.Ready = m.Ready && o.streaming[m.Name]
		} else {
			m.Ready = previous[m.Name] && m.Name != base.Status.Primary
		}
	}
	return pg.Status.Members, r.Status().Patch(ctx, &pg, client.MergeFrom(base))
}
