package operator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/agentauth"
	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/pgparser"
	"github.com/andrew01234567890/pgshard/internal/pgtune"

	"github.com/andrew01234567890/pgshard/internal/metrics"
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
	// AgentTLS is how each member's agent must be dialled, filled from the
	// pods this reconciler observes and read by the agent client. Nil means
	// every agent is plaintext.
	AgentTLS *AgentTLSModes
	// FailoverDelay overrides DefaultFailoverDelay; PollInterval the
	// quiesce poll; Now the clock. Zero values mean the defaults.
	FailoverDelay  time.Duration
	PollInterval   time.Duration
	QuiesceTimeout time.Duration
	// PodFenceTimeout bounds the wait for the old primary's Pod to go
	// before a successor is promoted; zero means DefaultPodFenceTimeout.
	PodFenceTimeout time.Duration
	RolloutTimeout  time.Duration
	Now             func() time.Time
	// Metrics counts failovers and rolling-update progress; nil disables it.
	Metrics *metrics.Operator

	mu             sync.Mutex
	unhealthySince map[string]time.Time
	lastRepromote  map[string]time.Time
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
		Owns(&networkingv1.NetworkPolicy{}).
		Owns(&appsv1.Deployment{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
		Owns(&pgshardv1alpha1.PgShardReshard{}).
		Watches(&pgshardv1alpha1.PgShardBackupPolicy{}, handler.EnqueueRequestsFromMapFunc(r.policyToClusters)).
		Watches(&pgshardv1alpha1.PgShardBackup{}, handler.EnqueueRequestsFromMapFunc(backupToCluster)).
		Named("pgshardcluster").
		Complete(r)
}

// groupState is the failover state of a group. The PgShardGroup status is
// where it is kept and written back to; the group Lease carries the same
// primary and epoch, and loadState reconstructs from it when the status
// has been lost.
type groupState struct {
	primary string
	epoch   int64
	// syncSet holds the standbys that were streaming at the last healthy
	// observation: the members an acknowledged commit may only exist on.
	syncSet map[string]bool
	// pvcs maps each member to the claim its data directory lives on.
	pvcs map[string]string
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
	// nodes names the node each running member landed on, in member order.
	// A group with two members on one node has fewer failure domains than
	// replicas, whatever the replica count says.
	nodes []string
	// primaryAgentMTLS is whether the primary's agent was STARTED requiring
	// mutual TLS. Published to the catalog for the controller, which dials
	// this shard's primary and cannot read pod annotations itself.
	primaryAgentMTLS bool
	// primaryBuild is what the primary's agent says it is. Only the
	// primary's: that is the one Status call this pass already makes, and
	// asking every member would be an RPC per member per reconcile for
	// something that changes at the pace of a rollout.
	primaryBuild string
	// failing is set while the primary is unhealthy or a failover is pending.
	failing bool
	// writesPaused is the primary refusing writes: the observable effect of
	// a raised catalog write fence, and what a barrier or a cutover leaves
	// behind if it dies mid-flight.
	writesPaused bool
	// template is the desired member template; tuning the derived settings
	// behind it (tuningErr when they could not be derived).
	template  MemberTemplate
	tuning    pgtune.Settings
	tuningErr error
	// rollout is the rolling step in flight or held, nil when idle.
	rollout *pgshardv1alpha1.GroupRollout
	// repoReady is true when the repository holds a completed backup, so
	// members that must be rebuilt restore from it instead of the primary.
	repoReady bool
	// policy is the backup policy bound to the cluster, nil when none.
	policy *pgshardv1alpha1.PgShardBackupPolicy
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
	if cluster.Spec.UnsafeSingleReplica {
		log.Info("unsafeSingleReplica is set: groups may run without synchronous standbys or failover candidates; unsupported for production")
	}

	password, err := r.ensureSecret(ctx, &cluster)
	if err != nil {
		return ctrl.Result{}, err
	}
	derived, err := agentauth.Token(password)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("agent auth token: %w", err)
	}
	agentToken, err := r.ensureAgentSecret(ctx, &cluster)
	if err != nil {
		return ctrl.Result{}, err
	}
	// Both, so one operator reaches an agent that has been rolled onto the
	// cluster's own token and one that has not. The server accepts a call
	// presenting either. REMOVE the derived one with the agent's acceptance
	// of it -- PGS-572.
	ctx = agentauth.WithTokens(ctx, agentToken, derived)
	if err := r.reconcilePKI(ctx, &cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("internal certificates: %w", err)
	}
	if err := r.ensureMemberRBAC(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureNetworkPolicy(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}

	policy, repoReady, backupCond, err := r.backupState(ctx, &cluster)
	if err != nil {
		return ctrl.Result{}, err
	}
	// Before any group: every member pod mounts this Secret for its pooler,
	// and a pod whose Secret does not exist never starts. Creating it on the
	// catalog-ready path instead deadlocked -- the catalog group could not
	// become ready because its own pooler was waiting for the Secret that
	// readiness was going to create. Nothing here needs the catalog: it is a
	// generated password, and the role it belongs to is given that password
	// later, once there is a catalog to give it to.
	if _, err := r.ensureRouterSecret(ctx, &cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("router secret: %w", err)
	}
	catalogGroup := Groups(&cluster)[0]
	catalogObs, err := r.reconcileGroup(ctx, &cluster, catalogGroup, password, policy, repoReady)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("group %s: %w", catalogGroup.Name(), err)
	}
	catalogReady, dsn := r.reconcileCatalogSchema(ctx, &cluster, catalogObs, password)
	var plan reshardPlan
	if catalogReady.Status == metav1.ConditionTrue {
		plan, err = r.reconcileReshard(ctx, &cluster, dsn)
		if err != nil {
			log.Error(err, "reshard reconciliation failed; groups keep reconciling")
			plan.cond = metav1.Condition{Type: pgshardv1alpha1.ConditionResharding, Status: metav1.ConditionUnknown, Reason: "Error", Message: err.Error(), ObservedGeneration: cluster.Generation}
		}
	} else {
		plan.cond = metav1.Condition{Type: pgshardv1alpha1.ConditionResharding, Status: metav1.ConditionUnknown, Reason: "CatalogNotReady", Message: catalogReady.Message, ObservedGeneration: cluster.Generation}
	}
	observations := []groupObservation{catalogObs}
	shardObs, err := r.reconcileGroups(ctx, &cluster, Groups(&cluster)[1:], password, policy, repoReady)
	if err != nil {
		return ctrl.Result{}, err
	}
	observations = append(observations, shardObs...)
	if catalogReady.Status == metav1.ConditionTrue {
		if err := r.spreadBootstrapVerifier(ctx, &cluster, dsn, observations, password); err != nil {
			log.Error(err, "bootstrap verifier not yet published to every group; retrying")
		}
	}

	// A target group over the provisioning budget only reconciles once it
	// has started, so which groups run is decided before they run: every
	// started group, then as many fresh ones as the budget still has room
	// for once the started ones that came back not ready are counted.
	var targets []groupObservation
	if budget := ProvisionBudget(&cluster); budget > 0 {
		var started, fresh []Group
		for _, g := range TargetGroups(&cluster) {
			ok, err := r.groupStarted(ctx, &cluster, g)
			if err != nil {
				return ctrl.Result{}, err
			}
			if ok {
				started = append(started, g)
			} else {
				fresh = append(fresh, g)
			}
		}
		obs, err := r.reconcileGroups(ctx, &cluster, started, password, policy, repoReady)
		if err != nil {
			return ctrl.Result{}, err
		}
		inflight := 0
		for _, o := range obs {
			if !o.ready() {
				inflight++
			}
		}
		if room := budget - inflight; room > 0 && len(fresh) > 0 {
			more, err := r.reconcileGroups(ctx, &cluster, fresh[:min(room, len(fresh))], password, policy, repoReady)
			if err != nil {
				return ctrl.Result{}, err
			}
			obs = append(obs, more...)
		}
		targets = inGroupOrder(TargetGroups(&cluster), obs)
	} else {
		var err error
		targets, err = r.reconcileGroups(ctx, &cluster, TargetGroups(&cluster), password, policy, repoReady)
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	retired, err := r.reconcileGroups(ctx, &cluster, RetiredGroups(&cluster), password, policy, repoReady)
	if err != nil {
		return ctrl.Result{}, err
	}

	if catalogReady.Status == metav1.ConditionTrue {
		if err := r.ensureCatalogEndpoint(ctx, &cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("catalog endpoint: %w", err)
		}
		catObs, err := r.reconcileCatalogUpgrade(ctx, &cluster, dsn, password, policy, repoReady)
		if err != nil {
			log.Error(err, "catalog upgrade reconciliation failed; groups keep reconciling")
		}
		retired = append(retired, catObs...)
	}

	if err := r.reconcileController(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileAdmin(ctx, &cluster); err != nil {
		// The admin is an accessory: a credential somebody pre-created
		// with the wrong key, or an image that will not pull, must not
		// stop the router, the shard status and the conditions from being
		// reconciled.
		log.Error(err, "admin reconciliation failed; the rest of the cluster keeps reconciling")
	}
	if err := r.reconcileRouter(ctx, &cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("router: %w", err)
	}
	if catalogReady.Status == metav1.ConditionTrue {
		published := r.publishShardStatus(ctx, &cluster, dsn, slices.Concat(observations[1:], targets, retired))
		// Publishing succeeded says nothing about the schema, and it is the
		// schema this condition reports: a deferral has to survive it.
		if published.Status == metav1.ConditionTrue {
			published.Reason, published.Message = catalogReady.Reason, catalogReady.Message
		}
		catalogReady = published
	}
	if err := r.updateReshardStatus(ctx, plan, targets); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.updateStatus(ctx, &cluster, observations, catalogReady, backupCond, plan.cond, plan.placements); err != nil {
		return ctrl.Result{}, err
	}
	requeue := requeueReady
	for _, o := range slices.Concat(observations, targets, retired) {
		if o.failing {
			return ctrl.Result{RequeueAfter: requeueFailover}, nil
		}
		if !o.ready() || o.rollout != nil {
			log.V(1).Info("group not settled", "group", o.group.Name(), "running", o.podsRunning, "streaming", o.streamingCount(), "primaryOK", o.primaryOK)
			requeue = requeueNotReady
		}
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// adoptSecret makes c the owner of an unowned secret. One that already has
// a controller is left alone, whoever it is: taking an object from another
// owner is how a deletion elsewhere removes a live cluster's credential.
func (r *ClusterReconciler) adoptSecret(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, sec *corev1.Secret) error {
	if metav1.GetControllerOf(sec) != nil {
		return nil
	}
	base := sec.DeepCopy()
	if err := controllerutil.SetControllerReference(c, sec, r.Scheme()); err != nil {
		return err
	}
	return r.Patch(ctx, sec, client.MergeFrom(base))
}

func (r *ClusterReconciler) ensureSecret(ctx context.Context, c *pgshardv1alpha1.PgShardCluster) (string, error) {
	var sec corev1.Secret
	key := types.NamespacedName{Namespace: c.Namespace, Name: SecretName(c.Name)}
	err := r.Get(ctx, key, &sec)
	if err == nil {
		pw, ok := sec.Data[secretKey]
		if !ok || len(pw) == 0 {
			return "", fmt.Errorf("secret %s has no %q key", key.Name, secretKey)
		}
		// A secret copied by a restore is created before the cluster
		// exists, so it cannot be owned at that point. Adopt it now, so
		// deleting the cluster takes its credential with it instead of
		// leaving one behind for the next cluster of the same name.
		if err := r.adoptSecret(ctx, c, &sec); err != nil {
			return "", err
		}
		return string(pw), nil
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

// ensureRouterSecret generates the router's catalog password. Like the
// admin's, it is the cluster's own and separate from the superuser's: the
// router terminates untrusted client connections, and the superuser
// password is also the seed of the agent control-plane token.
func (r *ClusterReconciler) ensureRouterSecret(ctx context.Context, c *pgshardv1alpha1.PgShardCluster) (string, error) {
	key := types.NamespacedName{Namespace: c.Namespace, Name: RouterSecretName(c.Name)}
	var sec corev1.Secret
	err := r.Get(ctx, key, &sec)
	if err == nil {
		if pw := sec.Data[secretKey]; len(pw) > 0 {
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
		StringData: map[string]string{"username": catalog.RouterRole, secretKey: pw},
	}
	if err := controllerutil.SetControllerReference(c, &sec, r.Scheme()); err != nil {
		return "", err
	}
	if err := r.Create(ctx, &sec); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return r.ensureRouterSecret(ctx, c)
		}
		return "", err
	}
	return pw, nil
}

// ensureAgentSecret generates the control-plane token the member agents
// accept. It is the cluster's own rather than something derived from the
// superuser password: anything holding that password used to hold the token
// that unlocks Promote, Demote, Rewind and Reclone on every member, and
// rotating either silently rotated the other.
func (r *ClusterReconciler) ensureAgentSecret(ctx context.Context, c *pgshardv1alpha1.PgShardCluster) (string, error) {
	key := types.NamespacedName{Namespace: c.Namespace, Name: AgentSecretName(c.Name)}
	var sec corev1.Secret
	err := r.Get(ctx, key, &sec)
	if err == nil {
		if tok := sec.Data[agentTokenKey]; len(tok) > 0 {
			return strings.TrimSpace(string(tok)), nil
		}
		return "", fmt.Errorf("secret %s has no %q key", key.Name, agentTokenKey)
	}
	if !apierrors.IsNotFound(err) {
		return "", err
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	token := hex.EncodeToString(buf)
	sec = corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace, Labels: map[string]string{LabelCluster: c.Name}},
		StringData: map[string]string{agentTokenKey: token},
	}
	if err := controllerutil.SetControllerReference(c, &sec, r.Scheme()); err != nil {
		return "", err
	}
	if err := r.Create(ctx, &sec); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return r.ensureAgentSecret(ctx, c)
		}
		return "", err
	}
	return token, nil
}

// ensureAdminSecret generates the credential the admin API requires. It is
// the cluster's own, not the superuser's: reading the admin is not being
// able to write to PostgreSQL, and a token that could do both would make the
// weaker surface the way to the stronger one.
func (r *ClusterReconciler) ensureAdminSecret(ctx context.Context, c *pgshardv1alpha1.PgShardCluster) (string, error) {
	key := types.NamespacedName{Namespace: c.Namespace, Name: AdminSecretName(c.Name)}
	var sec corev1.Secret
	err := r.Get(ctx, key, &sec)
	if err == nil {
		// A Secret somebody else made is likely to follow the basic-auth
		// convention, whose key is password. Take either, and tell the
		// caller which so the mount can name the file the admin expects.
		for _, k := range []string{adminSecretKey, "password"} {
			if len(sec.Data[k]) > 0 {
				return k, nil
			}
		}
		return "", fmt.Errorf("secret %s has neither a %q nor a \"password\" key", key.Name, adminSecretKey)
	}
	if !apierrors.IsNotFound(err) {
		return "", err
	}
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	sec = corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace, Labels: map[string]string{LabelCluster: c.Name}},
		Type:       corev1.SecretTypeBasicAuth,
		StringData: map[string]string{"username": "admin", adminSecretKey: hex.EncodeToString(buf)},
	}
	if err := controllerutil.SetControllerReference(c, &sec, r.Scheme()); err != nil {
		return "", err
	}
	if err := r.Create(ctx, &sec); err != nil && !apierrors.IsAlreadyExists(err) {
		return "", err
	}
	return adminSecretKey, nil
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

// ensureNetworkPolicy renders the member policy while it is enabled and
// removes it when it is turned off: a policy left behind keeps enforcing.
func (r *ClusterReconciler) ensureNetworkPolicy(ctx context.Context, c *pgshardv1alpha1.PgShardCluster) error {
	desired := r.Renderer.MemberNetworkPolicy(c)
	if !c.Spec.NetworkPolicy.Enabled {
		// Only ours: a policy of the same name that somebody wrote by hand
		// is not this operator's to remove, and the default spec would
		// remove it on every pass.
		var existing networkingv1.NetworkPolicy
		if err := r.Get(ctx, client.ObjectKeyFromObject(desired), &existing); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if ref := metav1.GetControllerOf(&existing); ref == nil || ref.UID != c.UID {
			return nil
		}
		err := r.Delete(ctx, &existing)
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	np := &networkingv1.NetworkPolicy{ObjectMeta: desired.ObjectMeta}
	return r.ensureOwned(ctx, c, np, func() error {
		np.Labels = desired.Labels
		np.Spec = desired.Spec
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

// leaseState reads the primary and epoch the fencing protocol last
// published on the group Lease. The Lease is written before any promotion
// and outlives the PgShardGroup, so it is what says who the primary is
// when the status does not.
func (r *ClusterReconciler) leaseState(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group) (string, int64) {
	var lease coordinationv1.Lease
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: g.LeaseName()}, &lease); err != nil {
		return "", 0
	}
	epoch, err := strconv.ParseInt(lease.Annotations[AnnotationPrimaryEpoch], 10, 64)
	if err != nil {
		epoch = 0
	}
	return lease.Annotations[AnnotationPrimary], epoch
}

// loadState reads the group's designated primary and epoch. The
// PgShardGroup status is where they are kept, but it is not the only
// record of them and it is losable -- an etcd restore, a deleted object, a
// status-schema migration -- so a group whose status says nothing is
// reconstructed from the group Lease before member 0 is designated at
// epoch 0. Designating member 0 while another member holds the fence and
// the data is how a promotion loses every commit the two differ by.
func (r *ClusterReconciler) loadState(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group) (groupState, error) {
	pg := r.Renderer.PgShardGroup(c, g)
	if err := r.ensureOwned(ctx, c, pg, nil); err != nil {
		return groupState{}, err
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(pg), pg); err != nil {
		return groupState{}, err
	}
	if pg.Spec.NonServing != g.NonServing {
		base := pg.DeepCopy()
		pg.Spec.NonServing = g.NonServing
		if err := r.Patch(ctx, pg, client.MergeFrom(base)); err != nil {
			return groupState{}, err
		}
	}
	st := groupState{primary: pg.Status.Primary, epoch: pg.Status.Epoch, syncSet: map[string]bool{}, pvcs: map[string]string{}}
	for _, name := range g.MemberNames() {
		st.pvcs[name] = name
	}
	for _, m := range pg.Status.Members {
		if m.Name != st.primary && m.Ready {
			st.syncSet[m.Name] = true
		}
		if m.PVC != "" {
			st.pvcs[m.Name] = m.PVC
		}
	}
	if err := r.recoverPVCs(ctx, c, g, pg, st); err != nil {
		return groupState{}, err
	}
	if len(st.syncSet) == 0 {
		// status is rebuilt from what a pass observes, so it comes back on
		// its own while there is a primary to observe. During an outage
		// there is not, and an empty synchronous set refuses every
		// failover -- safely, but the cluster stays down. The Lease
		// remembers it for exactly that case.
		for _, name := range rememberedSyncSet(g, pg.Annotations[AnnotationSyncSet]) {
			st.syncSet[name] = true
		}
	}
	if len(st.syncSet) == 0 {
		// The group object itself can be gone, not just its status. The
		// Lease is a separate object and outlives it -- when there is one:
		// it is created by the first fence, so a cluster that has never
		// failed over has none.
		for _, name := range r.leaseSyncSet(ctx, c, g) {
			st.syncSet[name] = true
		}
	}
	// A designated primary outside the member set is not an oracle to be
	// obeyed: scaling replicasPerShard below the ordinal of a primary that a
	// failover promoted leaves status.primary naming a member that no longer
	// exists, and status.primary is externally editable besides. Designate a
	// member that does exist and let the ordinary converge path promote it.
	// The epoch is NOT reset here, unlike the never-designated case below: it
	// fences writes against a primary that may still be running, so it may
	// only ever increase.
	stale := st.primary != "" && !slices.Contains(g.MemberNames(), st.primary)
	if stale || st.primary == "" {
		fenced, fencedEpoch := r.leaseState(ctx, c, g)
		switch {
		case st.primary == "" && slices.Contains(g.MemberNames(), fenced):
			// The fence names a member that still exists: it holds the
			// data a promotion would otherwise discard, and its epoch is
			// the one the poolers are refusing writes below.
			st.primary, st.epoch = fenced, max(st.epoch, fencedEpoch)
		default:
			st.primary = g.MemberName(0)
			// The epoch fences writes against a primary that may still be
			// running, so it only ever increases: it is left alone for a
			// designated primary that has gone out of the member set, and
			// for a group the fence remembers, and reset only where
			// nothing has ever been promoted.
			if !stale {
				st.epoch = max(st.epoch, fencedEpoch)
			}
		}
		base := pg.DeepCopy()
		pg.Status.Primary, pg.Status.Epoch = st.primary, st.epoch
		if err := r.Status().Patch(ctx, pg, client.MergeFrom(base)); err != nil {
			return groupState{}, err
		}
	}
	return st, nil
}

// recordSyncSet keeps the Lease's memory of the synchronous set current.
// It writes only when the set changed: a Lease rewritten every pass for
// every group is a write per group per pass for a value that rarely moves.
func (r *ClusterReconciler) recordSyncSet(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, primary string, streaming map[string]bool) error {
	var names []string
	for _, name := range g.MemberNames() {
		if name != primary && streaming[name] {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		// Nothing observed streaming is not evidence that nothing is: a
		// pass that could not reach the members must not erase what the
		// last one knew.
		return nil
	}
	want := strings.Join(names, ",")
	pg := r.Renderer.PgShardGroup(c, g)
	if err := r.Get(ctx, client.ObjectKeyFromObject(pg), pg); err == nil && pg.Annotations[AnnotationSyncSet] != want {
		base := pg.DeepCopy()
		if pg.Annotations == nil {
			pg.Annotations = map[string]string{}
		}
		pg.Annotations[AnnotationSyncSet] = want
		if err := r.Patch(ctx, pg, client.MergeFrom(base)); err != nil {
			return err
		}
	} else if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	var lease coordinationv1.Lease
	key := types.NamespacedName{Namespace: c.Namespace, Name: g.LeaseName()}
	if err := r.Get(ctx, key, &lease); err != nil {
		return client.IgnoreNotFound(err)
	}
	if lease.Annotations[AnnotationSyncSet] == want {
		return nil
	}
	base := lease.DeepCopy()
	if lease.Annotations == nil {
		lease.Annotations = map[string]string{}
	}
	lease.Annotations[AnnotationSyncSet] = want
	return r.Patch(ctx, &lease, client.MergeFrom(base))
}

// rememberedSyncSet reads a recorded set, keeping only members that still
// exist: a set scaled down leaves names behind, and a name that is not a
// member is not a candidate for anything.
func rememberedSyncSet(g Group, recorded string) []string {
	var out []string
	for _, name := range strings.Split(recorded, ",") {
		if name != "" && slices.Contains(g.MemberNames(), name) {
			out = append(out, name)
		}
	}
	return out
}

// leaseSyncSet is the synchronous set the Lease remembers.
func (r *ClusterReconciler) leaseSyncSet(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group) []string {
	var lease coordinationv1.Lease
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: g.LeaseName()}, &lease); err != nil {
		return nil
	}
	return rememberedSyncSet(g, lease.Annotations[AnnotationSyncSet])
}

// recoverPVCs fills in the claim of any member whose status did not name
// one, by reading what that member's pod actually mounts.
//
// The default is the claim named after the member, and after a storage
// rebuild that is the wrong one: the rebuilt claim gains a -v<n> suffix,
// so a group that lost its status would mount the pre-rebuild volume -
// the old storage class, and data as stale as the rebuild is old. The
// running pod is the authority on where its data directory is, which is
// exactly the evidence status was standing in for.
func (r *ClusterReconciler) recoverPVCs(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, pg *pgshardv1alpha1.PgShardGroup, st groupState) error {
	known := map[string]bool{}
	for _, m := range pg.Status.Members {
		if m.PVC != "" {
			known[m.Name] = true
		}
	}
	var claimed corev1.PersistentVolumeClaimList
	if err := r.List(ctx, &claimed, client.InNamespace(c.Namespace),
		client.MatchingLabels{LabelCluster: c.Name, LabelGroup: g.Name()}); err != nil {
		return err
	}
	for _, name := range g.MemberNames() {
		if known[name] {
			continue
		}
		var pod corev1.Pod
		err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: name}, &pod)
		if err == nil {
			if claim := dataClaim(&pod); claim != "" {
				st.pvcs[name] = claim
				continue
			}
		} else if !apierrors.IsNotFound(err) {
			return err
		}
		// No pod to ask. The claims themselves say which volumes the
		// member has had; rebuilds only ever move forward, so the newest
		// is the one holding its data.
		if claim := newestClaim(claimed.Items, name); claim != "" {
			st.pvcs[name] = claim
		}
	}
	return nil
}

// newestClaim is the member's most recent claim: rebuilds rename forward,
// member then member-v2 then member-v3, so the highest suffix is the last
// one the member was rebuilt onto.
func newestClaim(claims []corev1.PersistentVolumeClaim, member string) string {
	best, bestV := "", -1
	for _, pvc := range claims {
		if pvc.Labels[LabelMember] != member {
			continue
		}
		v := 1
		if rest, ok := strings.CutPrefix(pvc.Name, member+"-v"); ok {
			n, err := strconv.Atoi(rest)
			if err != nil {
				continue
			}
			v = n
		} else if pvc.Name != member {
			continue
		}
		if v > bestV {
			best, bestV = pvc.Name, v
		}
	}
	return best
}

// dataClaim is the claim a member's pod mounts as its data directory.
func dataClaim(pod *corev1.Pod) string {
	for _, v := range pod.Spec.Volumes {
		if v.Name == "data" && v.PersistentVolumeClaim != nil {
			return v.PersistentVolumeClaim.ClaimName
		}
	}
	return ""
}

func (r *ClusterReconciler) ensureConfigMap(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, primary string, tuning pgtune.Settings, pol *pgshardv1alpha1.PgShardBackupPolicy, repoReady bool) error {
	desired := r.Renderer.ConfigMap(c, g, primary, tuning, pol, repoReady)
	cm := &corev1.ConfigMap{ObjectMeta: desired.ObjectMeta}
	return r.ensureOwned(ctx, c, cm, func() error {
		cm.Labels = desired.Labels
		cm.Data = desired.Data
		return nil
	})
}

func (r *ClusterReconciler) reconcileGroup(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, password string, pol *pgshardv1alpha1.PgShardBackupPolicy, repoReady bool) (groupObservation, error) {
	obs := groupObservation{group: g, streaming: map[string]bool{}, replicasWant: g.Replicas - 1, policy: pol, repoReady: repoReady}

	state, err := r.loadState(ctx, c, g)
	if err != nil {
		return obs, err
	}
	obs.state = state
	obs.tuning, obs.tuningErr = Tuning(c, g)
	obs.template = Template(c, g, obs.tuning, pol)
	if sum, err := r.internalTLSChecksum(ctx, c); err != nil {
		return obs, err
	} else if sum != "" {
		obs.template.InternalTLS += ":" + sum
	}
	if err := r.ensureSettings(ctx, c, g, &obs, password); err != nil {
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
		name := g.MemberName(i)
		if err := r.ensureOwned(ctx, c, r.Renderer.PVC(c, g, i, state.pvcs[name]), nil); err != nil {
			return obs, err
		}
		// A missing primary pod is a failover trigger, not something to
		// recreate: a fresh pod would take the Lease back and no promotion
		// would happen. It is recreated as a replica once the group moved on,
		// or as the primary when no candidate exists.
		createIfMissing := name != state.primary || len(state.syncSet) == 0
		m, err := r.observePod(ctx, c, g, i, state, obs.template, createIfMissing)
		if err != nil {
			return obs, err
		}
		members[name] = m
		if m.pod != nil && m.pod.Spec.NodeName != "" {
			obs.nodes = append(obs.nodes, m.pod.Spec.NodeName)
		}
	}

	if target := c.Annotations[AnnotationSwitchover]; target != "" && g.HasMember(target) {
		return r.switchover(ctx, c, g, obs, members, password, target)
	}

	primary := members[state.primary]
	if primary == nil {
		// loadState keeps the designated primary inside the member set, so
		// reaching here means the pod has not been observed this pass. Treat
		// it as an unhealthy primary, which is what every neighbouring
		// function does, rather than dereferencing nil and panicking the
		// whole cluster's reconcile.
		primary = &memberInfo{}
	}
	var st AgentStatus
	stErr := errors.New("pod missing")
	switch {
	case primary.pod != nil && primary.ip != "":
		st, stErr = r.Agents.Status(ctx, agentAddr(primary.ip))
	case primary.pod != nil:
		stErr = errors.New("pod has no IP yet")
	}
	obs.primaryBuild = st.Build
	// Taken from the pod, not the spec: the controller dials this shard's
	// primary and cannot see pods, so the catalog has to carry what this
	// member is RUNNING rather than what the cluster now asks for.
	obs.primaryAgentMTLS = primary.pod != nil && primary.pod.Annotations[AnnotationAgentMTLS] == "true"

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
					if _, err := r.observePod(ctx, c, g, ordinalOf(g, state.primary), state, obs.template, true); err != nil {
						return obs, err
					}
				}
				return r.finishGroup(ctx, c, g, obs, members), nil
			}
			if err != nil {
				return obs, err
			}
			if err := r.ensureConfigMap(ctx, c, g, state.primary, obs.tuning, obs.policy, obs.repoReady); err != nil {
				return obs, err
			}
		}
		return r.finishGroup(ctx, c, g, obs, members), nil
	}

	state, refused, err := r.converge(ctx, c, g, state, members, password)
	obs.state = state
	if err != nil {
		return obs, err
	}
	obs = r.finishGroup(ctx, c, g, obs, members)
	if refused != "" {
		obs.failing, obs.primaryErr = true, refused
		return obs, nil
	}

	dsn := DSN(g.ServiceRW(), c.Namespace, password)
	pstate, err := r.Prober.Probe(ctx, dsn)
	if err != nil {
		obs.primaryErr = err.Error()
		return obs, nil
	}
	obs.primaryOK = true
	obs.writesPaused = pstate.WritesPaused
	// Reported, never fatal to the pass: the fence lives in the catalog
	// group, which is itself rebuilt member by member on a storage-class
	// change, and a group that stopped rolling out because it could not
	// read the catalog for a moment would be waiting on the very thing it
	// is waiting for. A pause that does not get reapplied still fails the
	// barrier's certification, which is what it did before it was
	// reapplied at all.
	if err := r.reapplyWritePause(ctx, c, g, dsn, pstate, password); err != nil {
		logf.FromContext(ctx).Info("could not reapply the write pause; continuing", "group", g.Name(), "err", err)
	}
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
	if err := r.recordSyncSet(ctx, c, g, state.primary, obs.streaming); err != nil {
		logf.FromContext(ctx).Info("could not record the synchronous set on the lease; continuing", "group", g.Name(), "err", err)
	}
	if primary.ip != "" {
		if _, err := r.Agents.SetSynchronizedStandbySlots(ctx, agentAddr(primary.ip), SynchronizedStandbySlots(g, state.primary, c.Spec.Durability.MinSyncStandbys, obs.streaming)); err != nil {
			obs.primaryErr = "set synchronized_standby_slots: " + err.Error()
			return obs, nil
		}
	}
	if err := r.rollout(ctx, c, g, &obs, members, password); err != nil {
		return obs, fmt.Errorf("rollout: %w", err)
	}
	return obs, nil
}

// ensureSettings classifies a settings change against the primary before
// the ConfigMap is rewritten, so the group knows whether the change needs a
// restart; when the primary cannot answer the ConfigMap is left as it is.
func (r *ClusterReconciler) ensureSettings(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, obs *groupObservation, password string) error {
	primaryIP := ""
	var pod corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: obs.state.primary}, &pod); err == nil {
		primaryIP = pod.Status.PodIP
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	restart, known, err := r.classifySettingsChange(ctx, c, g, obs.template, primaryIP, password)
	if err != nil {
		return err
	}
	if !known {
		return nil
	}
	if restart {
		if err := r.patchGroupStatus(ctx, c, g, func(pg *pgshardv1alpha1.PgShardGroup) { pg.Status.SettingsRestartPending = true }); err != nil {
			return err
		}
	}
	return r.ensureConfigMap(ctx, c, g, obs.state.primary, obs.tuning, obs.policy, obs.repoReady)
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
// reapplyWritePause puts back the barrier's pause on a primary that lost
// it. The pause is an ALTER SYSTEM, so it lives in postgresql.auto.conf,
// which the agent rewrites on bootstrap, promotion and restore: a primary
// that crashed and came back, or one promoted mid-barrier, serves writes
// again while the barrier believes every shard is holding still. The
// durable statement of that intent is the catalog write fence, and this
// makes the primary match it.
//
// Only the pause is reapplied. Lifting it belongs to the barrier that
// raised the fence -- or, if that barrier died, to the recovery pass that
// reads the still-raised fence and resumes the shards it left paused.
func (r *ClusterReconciler) reapplyWritePause(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, dsn string, pstate PrimaryState, password string) error {
	if g.Kind != "shard" || pstate.WritesPaused {
		return nil
	}
	fenced, err := r.Prober.WriteFenced(ctx, DSN(Groups(c)[0].ServiceRW(), c.Namespace, password))
	if err != nil {
		return fmt.Errorf("read the write fence: %w", err)
	}
	if !fenced {
		return nil
	}
	if err := r.Prober.PauseWrites(ctx, dsn); err != nil {
		return fmt.Errorf("reapply the write pause: %w", err)
	}
	logf.FromContext(ctx).Info("write fence is raised and the primary was accepting writes; pause reapplied", "group", g.Name())
	return nil
}

func (r *ClusterReconciler) finishGroup(_ context.Context, _ *pgshardv1alpha1.PgShardCluster, g Group, obs groupObservation, members map[string]*memberInfo) groupObservation {
	obs.podsRunning, obs.podsReady, obs.members = 0, 0, nil
	for _, name := range g.MemberNames() {
		m := members[name]
		ms := pgshardv1alpha1.MemberStatus{Name: name, PVC: obs.state.pvcs[name]}
		if name == obs.state.primary {
			ms.Build = obs.primaryBuild
		}
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
	if err := r.ensureConfigMap(ctx, c, g, state.primary, obs.tuning, obs.policy, obs.repoReady); err != nil {
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
func (r *ClusterReconciler) observePod(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group, ordinal int, state groupState, tpl MemberTemplate, createIfMissing bool) (*memberInfo, error) {
	name := g.MemberName(ordinal)
	role := RoleReplica
	if name == state.primary {
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
		desired := r.Renderer.Pod(c, g, ordinal, role, state.pvcs[name], tpl)
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
	// Recorded before anything dials this member: the annotation says what
	// its agent was started requiring, and during a rollout that differs
	// from what the spec now asks for.
	r.AgentTLS.Set(agentAddr(m.ip), pod.Annotations[AnnotationAgentMTLS] == "true")
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

// The router terminates SCRAM against the verifier in pgshard.roles and
// forwards the recovered key to a backend, so a group whose own initdb salted
// the role differently refuses that key with 28P01 -- after the router has
// already accepted the client. Give every group the published verifier.
//
// EVERY group, the catalog included. It was the shard groups only, and the
// catalog is the one this is most visible on: the pgshard database routes to
// the catalog set, so the first login the getting-started guide documents
// goes there and nowhere else. It failed while every shard was reachable.
func (r *ClusterReconciler) spreadBootstrapVerifier(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, catalogDSN string, groups []groupObservation, password string) error {
	verifier, err := r.Prober.BootstrapVerifier(ctx, catalogDSN, superuserName)
	if err != nil {
		return fmt.Errorf("read published verifier: %w", err)
	}
	if verifier == "" {
		return nil
	}
	var errs []error
	for _, o := range groups {
		if !o.primaryOK {
			continue
		}
		dsn := DSN(o.group.ServiceRW(), c.Namespace, password)
		if err := r.Prober.AdoptBootstrapVerifier(ctx, dsn, superuserName, verifier); err != nil {
			errs = append(errs, fmt.Errorf("group %s: %w", o.group.Name(), err))
		}
	}
	return errors.Join(errs...)
}

func (r *ClusterReconciler) reconcileCatalogSchema(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, catalog groupObservation, password string) (metav1.Condition, string) {
	cond := metav1.Condition{Type: ConditionCatalogReady, Status: metav1.ConditionFalse, ObservedGeneration: c.Generation}
	if !catalog.ready() {
		cond.Reason = "CatalogGroupNotReady"
		cond.Message = "catalog group is not ready"
		return cond, ""
	}
	dsn := DSN(catalog.group.ServiceRW(), c.Namespace, password)
	// The router's password is not part of the schema and is not
	// replicated, so it is applied on its own: a catalog just switched to
	// has the role from its migration and no password for it.
	setRouterCredential := func() error {
		pw, err := r.ensureRouterSecret(ctx, c)
		if err != nil {
			return err
		}
		return r.Prober.SetRouterPassword(ctx, dsn, pw)
	}
	// The router authenticates against pgshard.roles and the migrations
	// leave it empty, so without this nobody can reach the cluster through
	// the router to create the first role -- including the operator's own
	// generated superuser credential, which is the only one that exists.
	seedBootstrapRole := func() error {
		return r.Prober.SeedBootstrapRole(ctx, dsn, superuserName, password)
	}
	if up := c.Status.CatalogUpgrade; up != nil {
		// Both catalog publications are FOR TABLES IN SCHEMA pgshard, which
		// includes pgshard.schema_migrations, and logical replication
		// carries no DDL. So a migration applied to whichever catalog is
		// serving replicates its ledger row to the other one while the
		// ALTER and CREATE stay behind, and that catalog then skips DDL it
		// believes it has already applied.
		//
		// After the cutover the stream runs new to old, so the group kept
		// for rollback ends up structurally behind and believing
		// otherwise. Before it, the stream runs old to new and the group
		// about to serve does. Neither direction is safe, so the whole
		// upgrade is the window, not just its retirement.
		cond.Status = metav1.ConditionTrue
		cond.Reason = "MigrationDeferred"
		cond.Message = "catalog schema migrations wait while a catalog upgrade is in flight; they run when it finishes"
		if err := setRouterCredential(); err != nil {
			// Not fatal here: the catalog is serving, and refusing to
			// reconcile the rest of the cluster over a credential the next
			// pass can set again helps nobody.
			cond.Message += "; the router credential could not be applied: " + err.Error()
		}
		if err := seedBootstrapRole(); err != nil {
			cond.Message += "; the bootstrap role could not be published: " + err.Error()
		}
		return cond, dsn
	}
	if err := r.Prober.MigrateCatalog(ctx, dsn); err != nil {
		cond.Reason = "MigrationFailed"
		cond.Message = err.Error()
		return cond, ""
	}
	// After the migration, which is what creates the role.
	if err := setRouterCredential(); err != nil {
		cond.Reason = "RouterCredential"
		cond.Message = err.Error()
		return cond, ""
	}
	// Also after the migration: pgshard.roles does not exist before it.
	if err := seedBootstrapRole(); err != nil {
		cond.Reason = "BootstrapRole"
		cond.Message = err.Error()
		return cond, ""
	}
	cond.Status = metav1.ConditionTrue
	cond.Reason = "Migrated"
	cond.Message = "catalog schema is current"
	return cond, dsn
}

// publishShardStatus upserts the fence of every shard group into the
// catalog; target groups stay provisioning until their set is serving.
func (r *ClusterReconciler) publishShardStatus(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, dsn string, shards []groupObservation) metav1.Condition {
	cond := metav1.Condition{Type: ConditionCatalogReady, Status: metav1.ConditionTrue, Reason: "Migrated", Message: "catalog schema is current", ObservedGeneration: c.Generation}
	rows := make([]ShardStatus, len(shards))
	for i, o := range shards {
		rows[i] = ShardStatus{Group: o.group, Epoch: o.state.epoch,
			Endpoint: r.memberEndpoint(c, o.group, o.state.primary), AgentMTLS: o.primaryAgentMTLS}
	}
	if err := r.Prober.PublishShardStatus(ctx, dsn, rows); err != nil {
		cond.Status = metav1.ConditionFalse
		cond.Reason = "PublishFailed"
		cond.Message = err.Error()
	}
	return cond
}

func (r *ClusterReconciler) updateStatus(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, obs []groupObservation, catalogReady, backupCond, reshardCond metav1.Condition, placements []pgshardv1alpha1.ClusterPlacementWorkflowStatus) error {
	ready := true
	primaryOK := true
	replOK := true
	fenced, servingShards, pausedShards := false, 0, 0
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
		if o.group.Kind == "shard" {
			servingShards++
			if o.writesPaused {
				fenced, pausedShards = true, pausedShards+1
			}
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
	var crowded []string
	for _, o := range obs {
		if m := topologyMessage(o.group.Name(), o.nodes); m != "" {
			crowded = append(crowded, m)
		}
	}
	// True is the bad state here, as with Degraded: a cluster says when it
	// has fewer failure domains than replicas rather than leaving the
	// replica count to imply otherwise.
	set(pgshardv1alpha1.ConditionTopologyDegraded, len(crowded) > 0,
		boolReason(len(crowded) > 0, "MembersShareNodes", "MembersOnDistinctNodes"),
		strings.Join(crowded, "; "))
	// Fenced and ServingWrites were declared and documented alongside the
	// two below and were likewise never set. Both are read off what the
	// pass already probed rather than a fresh catalog read: a primary
	// refusing writes is the observable effect of a raised fence, and it is
	// the state a barrier or a cutover leaves behind if it dies mid-flight.
	servingWrites := primaryOK && !fenced && servingShards > 0
	set(pgshardv1alpha1.ConditionFenced, fenced,
		boolReason(fenced, "WritesPaused", "Unfenced"),
		fmt.Sprintf("%d/%d shard primaries refusing writes", pausedShards, servingShards))
	set(pgshardv1alpha1.ConditionServingWrites, servingWrites,
		boolReason(servingWrites, "Serving", "NotServing"), "")
	// Both of these were declared in the API and documented as conditions a
	// cluster reports, and neither was ever set: anybody waiting on
	// RouterReady waited for something that never arrived. ControllerReady
	// matters more than it looks -- every other condition here can be True
	// while nothing resolves in-doubt transactions or applies queued DDL,
	// because that work belongs to a process none of them describe.
	routerReady, routerMsg := r.deploymentReady(ctx, c.Namespace, RouterName(c.Name))
	set(pgshardv1alpha1.ConditionRouterReady, routerReady, boolReason(routerReady, "Ready", "NotReady"), routerMsg)
	controllerReady, controllerMsg := r.deploymentReady(ctx, c.Namespace, ControllerName(c.Name))
	set(pgshardv1alpha1.ConditionControllerReady, controllerReady, boolReason(controllerReady, "Ready", "NotReady"), controllerMsg)
	meta.SetStatusCondition(&c.Status.Conditions, catalogReady)
	meta.SetStatusCondition(&c.Status.Conditions, backupCond)
	meta.SetStatusCondition(&c.Status.Conditions, reshardCond)
	r.setRolloutStatus(c, obs, set)
	// The routers parse with one grammar, chosen when the binary was
	// built. A cluster whose groups have all moved to a newer major is
	// running that major and offering the older one's SQL, and the upgrade
	// that got it there reported success -- correctly, because the data
	// moved. Saying so here is the difference between a documented
	// limitation and a statement that is refused for no visible reason.
	behind := c.Status.ServingPGMajor > pgparser.Major
	set(pgshardv1alpha1.ConditionSQLSurfaceBehindServers, behind,
		boolReason(behind, "GrammarOlderThanServers", "GrammarMatchesServers"),
		sqlSurfaceMessage(c.Status.ServingPGMajor))
	c.Status.ObservedGeneration = c.Generation
	c.Status.Shards = shards
	c.Status.PlacementWorkflows = placements
	return r.Status().Patch(ctx, c, client.MergeFrom(base))
}

// deploymentReady reports whether a Deployment has a ready replica, and
// says why when it has not. A missing Deployment is not ready rather than
// an error: the pass that creates it runs before the pass that reports on
// it, and a reconcile that failed here would never get to the creating.
func (r *ClusterReconciler) deploymentReady(ctx context.Context, namespace, name string) (bool, string) {
	var dep appsv1.Deployment
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &dep); err != nil {
		if apierrors.IsNotFound(err) {
			return false, name + " does not exist yet"
		}
		return false, "reading " + name + ": " + err.Error()
	}
	if dep.Status.ReadyReplicas > 0 {
		return true, ""
	}
	return false, fmt.Sprintf("%s has %d ready replica(s) of %d", name, dep.Status.ReadyReplicas, dep.Status.Replicas)
}

// sqlSurfaceMessage says what a client actually gets, in both directions,
// because "the routers parse PostgreSQL 18" is only interesting next to
// what the servers are.
func sqlSurfaceMessage(serving int) string {
	if serving <= 0 || serving == pgparser.Major {
		return fmt.Sprintf("routers parse PostgreSQL %d and the shards run it", pgparser.Major)
	}
	if serving < pgparser.Major {
		return fmt.Sprintf("routers parse PostgreSQL %d while the shards run %d; syntax the shards would refuse is refused here first", pgparser.Major, serving)
	}
	return fmt.Sprintf("the shards run PostgreSQL %d but the routers parse %d: %d-only syntax is refused and server_version reports %d. The upgrade moved the data; the SQL surface follows a router build that parses %d",
		serving, pgparser.Major, serving, pgparser.Major, serving)
}

// setRolloutStatus summarises the groups' rolling steps into status.rollout
// and the RolloutInProgress, Degraded and TuningApplied conditions.
func (r *ClusterReconciler) setRolloutStatus(c *pgshardv1alpha1.PgShardCluster, obs []groupObservation, set func(t string, ok bool, reason, message string)) {
	rollout := pgshardv1alpha1.RolloutStatus{Phase: pgshardv1alpha1.RolloutPhaseIdle, LastRestartToken: c.Status.Rollout.LastRestartToken}
	held := ""
	tuningErr := ""
	var tuned []pgshardv1alpha1.DerivedSetting
	for _, o := range obs {
		if o.rollout != nil && rollout.Phase == pgshardv1alpha1.RolloutPhaseIdle {
			rollout.Phase, rollout.Member, rollout.Reason = o.rollout.Phase, o.rollout.Member, o.group.Name()+": "+o.rollout.Reason
		}
		if o.rollout != nil && o.rollout.Phase == pgshardv1alpha1.RolloutPhaseHeld && held == "" {
			held = o.group.Name() + ": " + o.rollout.Reason
		}
		if o.tuningErr != nil && tuningErr == "" {
			tuningErr = o.group.Name() + ": " + o.tuningErr.Error()
		}
		if o.group.Kind == "shard" || tuned == nil {
			tuned = o.tuning.Derived()
		}
	}
	if rollout.Phase == pgshardv1alpha1.RolloutPhaseIdle {
		rollout.LastRestartToken = c.Annotations[AnnotationRestart]
	}
	c.Status.Rollout = rollout
	c.Status.Tuning.Derived = tuned
	inProgress := rollout.Phase != pgshardv1alpha1.RolloutPhaseIdle
	set(pgshardv1alpha1.ConditionRolloutInProgress, inProgress, boolReason(inProgress, "Rolling", "Idle"), rollout.Reason)
	set(pgshardv1alpha1.ConditionDegraded, held != "", boolReason(held != "", "RolloutHeld", "Healthy"), held)
	switch {
	case tuningErr != "":
		set(pgshardv1alpha1.ConditionTuningApplied, false, "DeriveFailed", tuningErr)
	case len(tuned) == 0:
		set(pgshardv1alpha1.ConditionTuningApplied, false, "NoMemoryBudget", "spec.resources sets no memory; nothing was derived")
	default:
		set(pgshardv1alpha1.ConditionTuningApplied, true, "Derived", fmt.Sprintf("%d settings derived from the pod resources", len(tuned)))
	}
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
		// A group deleted later in the same pass (a rolled-back catalog
		// upgrade target) has no record left to update.
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
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

// ProvisionBudget is spec.upgrade.maxParallelGroups when an upgrade run is
// provisioning targets: how many new-major groups may be brought up at
// once. Zero means unbounded (topology reshards, or the field unset). The
// cutover itself still flips the whole set at once (docs/upgrade.md).
func ProvisionBudget(c *pgshardv1alpha1.PgShardCluster) int {
	rs := c.Status.Reshard
	if rs == nil || rs.PGMajor == 0 || rs.PGMajor == c.Status.ServingPGMajor {
		return 0
	}
	return c.Spec.Upgrade.MaxParallelGroups
}

// inGroupOrder sorts observations the way groups lists them, so which
// groups the budget admitted does not change the order a pass reports
// them in.
func inGroupOrder(groups []Group, obs []groupObservation) []groupObservation {
	at := map[string]groupObservation{}
	for _, o := range obs {
		at[o.group.Name()] = o
	}
	out := make([]groupObservation, 0, len(obs))
	for _, g := range groups {
		if o, ok := at[g.Name()]; ok {
			out = append(out, o)
		}
	}
	return out
}

// groupConcurrency bounds how many groups reconcile at once. A group's
// pass is mostly waiting -- Kubernetes reads and writes, an agent RPC and
// a few PostgreSQL round trips -- so serially a pass cost the sum over
// every group of the cluster. On a large topology that ran past the
// requeue interval, and a primary failure in the last group was not
// noticed until the walk reached it.
const groupConcurrency = 16

// reconcileGroups reconciles groups concurrently and returns their
// observations in the order given. The first error in that same order is
// returned: which of several failing groups a pass blames should not
// depend on which goroutine lost the race.
func (r *ClusterReconciler) reconcileGroups(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, groups []Group, password string, pol *pgshardv1alpha1.PgShardBackupPolicy, repoReady bool) ([]groupObservation, error) {
	if len(groups) == 0 {
		return nil, nil
	}
	// A switchover writes to the cluster object the other groups are
	// reading, and it is one deliberate operation on one group, so it is
	// not worth making every other read of the object synchronise.
	if target := c.Annotations[AnnotationSwitchover]; target != "" {
		out := make([]groupObservation, 0, len(groups))
		for _, g := range groups {
			obs, err := r.reconcileGroup(ctx, c, g, password, pol, repoReady)
			if err != nil {
				return nil, fmt.Errorf("group %s: %w", g.Name(), err)
			}
			out = append(out, obs)
		}
		return out, nil
	}
	out := make([]groupObservation, len(groups))
	errs := make([]error, len(groups))
	var wg sync.WaitGroup
	sem := make(chan struct{}, groupConcurrency)
	for i, g := range groups {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			obs, err := r.reconcileGroup(ctx, c, g, password, pol, repoReady)
			out[i], errs[i] = obs, err
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			return nil, fmt.Errorf("group %s: %w", groups[i].Name(), err)
		}
	}
	return out, nil
}

// groupStarted reports whether a target group's PgShardGroup record
// already exists: a started group keeps reconciling even over the
// provisioning budget, so it can finish.
func (r *ClusterReconciler) groupStarted(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group) (bool, error) {
	pg := r.Renderer.PgShardGroup(c, g)
	err := r.Get(ctx, client.ObjectKeyFromObject(pg), pg)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	return err == nil, err
}
