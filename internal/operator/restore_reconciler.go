package operator

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/agent/backup"
)

const restorePollInterval = 5 * time.Second

// RestoreReconciler turns a PgShardRestore into a new PgShardCluster whose
// primaries bootstrap from the source cluster's repository, then follows
// the recovery until every group promoted on a new timeline.
type RestoreReconciler struct {
	client.Client
	// APIReader is an uncached reader used to confirm a cluster is truly
	// gone before failing a restore, since the cached client can briefly
	// miss a just-created cluster. nil skips the confirmation (unit tests).
	APIReader client.Reader
	Agents    AgentClient
	// TwoPC finishes prepared transactions and lifts the write fence after a
	// barrier restore; nil fails barrier restores with a clear reason.
	TwoPC TwoPCAgentClient
	// Barriers answers whether a named barrier was certified, asked of the
	// live source cluster before the restore starts. nil skips the check.
	Barriers BarrierCertifier
	Now      func() time.Time
}

// BarrierCertifier reports whether a barrier of that name was certified.
type BarrierCertifier interface {
	CertifiedBarrier(ctx context.Context, dsn, password, name string) (bool, error)
}

// SetupWithManager registers the reconciler; clusters created by a restore
// requeue it through their label.
func (r *RestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pgshardv1alpha1.PgShardRestore{}).
		Watches(&pgshardv1alpha1.PgShardCluster{}, handler.EnqueueRequestsFromMapFunc(clusterToRestore)).
		Named("pgshardrestore").
		Complete(r)
}

func clusterToRestore(_ context.Context, obj client.Object) []ctrl.Request {
	name := obj.GetLabels()[LabelRestoredFrom]
	if name == "" {
		return nil
	}
	return []ctrl.Request{{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: name}}}
}

func (r *RestoreReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// Reconcile drives one PgShardRestore through Pending, Restoring and a
// terminal phase.
func (r *RestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var rs pgshardv1alpha1.PgShardRestore
	if err := r.Get(ctx, req.NamespacedName, &rs); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !rs.DeletionTimestamp.IsZero() || rs.Status.Phase == pgshardv1alpha1.RestorePhaseRecovered || rs.Status.Phase == pgshardv1alpha1.RestorePhaseFailed {
		return ctrl.Result{}, nil
	}
	if rs.Spec.NewClusterName == "" || rs.Spec.NewClusterName == rs.Spec.ClusterName {
		return ctrl.Result{}, r.fail(ctx, &rs, "spec.newClusterName must be set and differ from spec.clusterName")
	}
	target, err := restoreTargetOptions(&rs.Spec)
	if err != nil {
		return ctrl.Result{}, r.fail(ctx, &rs, err.Error())
	}
	// The new cluster's secret is a copy of the source's, so either yields
	// the token the restored agents expect.
	ctx = withClusterAgentToken(ctx, r.Client, rs.Namespace, rs.Spec.ClusterName)
	var newCluster pgshardv1alpha1.PgShardCluster
	err = r.Get(ctx, types.NamespacedName{Namespace: rs.Namespace, Name: rs.Spec.NewClusterName}, &newCluster)
	switch {
	case apierrors.IsNotFound(err):
		if rs.Status.Phase == pgshardv1alpha1.RestorePhaseRestoring {
			// The cached client can lag behind the API server just after the
			// cluster is created; confirm with an uncached read before
			// declaring it gone, and requeue if it is actually still there.
			if r.APIReader != nil {
				if err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: rs.Namespace, Name: rs.Spec.NewClusterName}, &newCluster); err == nil {
					return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
				} else if !apierrors.IsNotFound(err) {
					return ctrl.Result{}, err
				}
			}
			return ctrl.Result{}, r.fail(ctx, &rs, fmt.Sprintf("cluster %q disappeared during the restore", rs.Spec.NewClusterName))
		}
		return r.create(ctx, &rs, target)
	case err != nil:
		return ctrl.Result{}, err
	case newCluster.Labels[LabelRestoredFrom] != rs.Name:
		return ctrl.Result{}, r.fail(ctx, &rs, fmt.Sprintf("cluster %q already exists and was not created by this restore", rs.Spec.NewClusterName))
	}
	return r.observe(ctx, &rs, &newCluster)
}

// backupIDs resolves spec.backupId to per-group pgbackrest labels: the
// groups of a completed PgShardBackup of the source cluster, or the raw
// label for every group when no such object exists.
func (r *RestoreReconciler) backupIDs(ctx context.Context, rs *pgshardv1alpha1.PgShardRestore, source *pgshardv1alpha1.PgShardCluster) (map[string]string, error) {
	if rs.Spec.BackupID == "" {
		return nil, nil
	}
	var b pgshardv1alpha1.PgShardBackup
	err := r.Get(ctx, types.NamespacedName{Namespace: rs.Namespace, Name: rs.Spec.BackupID}, &b)
	if apierrors.IsNotFound(err) {
		out := map[string]string{}
		for _, g := range Groups(source) {
			out[g.Name()] = rs.Spec.BackupID
		}
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	if b.Spec.ClusterName != source.Name {
		return nil, fmt.Errorf("backup %s belongs to cluster %s, not %s", b.Name, b.Spec.ClusterName, source.Name)
	}
	if b.Status.Phase != pgshardv1alpha1.BackupPhaseCompleted {
		return nil, fmt.Errorf("backup %s is %s, not Completed", b.Name, firstNonEmpty(b.Status.Phase, "pending"))
	}
	out := map[string]string{}
	for _, g := range b.Status.Groups {
		out[g.Group] = g.BackupID
	}
	for _, g := range Groups(source) {
		if out[g.Name()] == "" {
			return nil, fmt.Errorf("backup %s has no set for group %s", b.Name, g.Name())
		}
	}
	return out, nil
}

func (r *RestoreReconciler) create(ctx context.Context, rs *pgshardv1alpha1.PgShardRestore, target backup.RestoreOptions) (ctrl.Result, error) {
	var source pgshardv1alpha1.PgShardCluster
	if err := r.Get(ctx, types.NamespacedName{Namespace: rs.Namespace, Name: rs.Spec.ClusterName}, &source); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.fail(ctx, rs, fmt.Sprintf("source cluster %q not found", rs.Spec.ClusterName))
		}
		return ctrl.Result{}, err
	}
	ids, err := r.backupIDs(ctx, rs, &source)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.fail(ctx, rs, err.Error())
	}
	spec := source.Spec.DeepCopy()
	if rs.Spec.ClusterSpec != nil {
		spec = rs.Spec.ClusterSpec.DeepCopy()
	}
	if spec.Backup.PolicyRef == "" {
		spec.Backup.PolicyRef = source.Spec.Backup.PolicyRef
	}
	if spec.Backup.PolicyRef == "" {
		return ctrl.Result{}, r.fail(ctx, rs, "the new cluster needs spec.backup.policyRef to reach the repository")
	}
	newCluster := &pgshardv1alpha1.PgShardCluster{
		ObjectMeta: metav1.ObjectMeta{Name: rs.Spec.NewClusterName, Namespace: rs.Namespace},
		Spec:       *spec,
	}
	if got, want := len(Groups(newCluster)), len(Groups(&source)); got != want || spec.PostgreSQL.Major != source.Spec.PostgreSQL.Major {
		return ctrl.Result{}, r.fail(ctx, rs, fmt.Sprintf("the new cluster must keep the source's %d groups and PostgreSQL %d", want, source.Spec.PostgreSQL.Major))
	}
	// A barrier that failed certification still left its physical restore
	// point on every group, so restoring to it succeeds and silently lands
	// the cluster on a point that is not two-phase-consistent. Ask the live
	// source, which is the only place the answer is knowable.
	if rs.Spec.Target.Barrier != nil && r.Barriers != nil {
		name := *rs.Spec.Target.Barrier
		password, perr := r.superuserPassword(ctx, &source)
		if perr != nil {
			return ctrl.Result{}, r.fail(ctx, rs, fmt.Sprintf("cannot read %s's superuser secret to confirm barrier %q: %v", source.Name, name, perr))
		}
		ok, cerr := r.Barriers.CertifiedBarrier(ctx, CatalogDSN(&source), password, BarrierRestorePoint(name))
		if cerr != nil {
			return ctrl.Result{}, r.fail(ctx, rs, fmt.Sprintf("cannot confirm barrier %q is certified on %s: %v", name, source.Name, cerr))
		}
		if !ok {
			return ctrl.Result{}, r.fail(ctx, rs, fmt.Sprintf("barrier %q is not certified on %s; restoring to it would land on a point that is not two-phase consistent", name, source.Name))
		}
	}
	src := RestoreSource{
		SourceCluster: source.Name, Major: source.Spec.PostgreSQL.Major, Restore: rs.Name, BackupIDs: ids,
		Type: target.Type, Target: target.Target, TargetTLI: target.TargetTLI, Exclusive: target.Exclusive,
	}
	newCluster.Labels = map[string]string{LabelRestoredFrom: rs.Name}
	newCluster.Annotations = map[string]string{AnnotationRestoreSource: src.Encode()}
	if err := r.copySuperuserSecret(ctx, &source, newCluster.Name); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Create(ctx, newCluster); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return ctrl.Result{RequeueAfter: restorePollInterval}, nil
		}
		return ctrl.Result{}, err
	}
	logf.FromContext(ctx).Info("created cluster from repository", "cluster", newCluster.Name, "source", source.Name, "target", target.String())
	base := rs.DeepCopy()
	rs.Status.Phase = pgshardv1alpha1.RestorePhaseRestoring
	rs.Status.StartedAt = ptrTime(r.now())
	rs.Status.Error = ""
	rs.Status.Groups = nil
	for _, g := range Groups(newCluster) {
		rs.Status.Groups = append(rs.Status.Groups, pgshardv1alpha1.GroupRestoreStatus{Group: g.Name(), SourceStanza: src.Stanza(g), BackupID: ids[g.Name()]})
	}
	meta.SetStatusCondition(&rs.Status.Conditions, metav1.Condition{Type: "Progressing", Status: metav1.ConditionTrue, Reason: "Restoring",
		Message: fmt.Sprintf("cluster %s restoring from %s (%s)", newCluster.Name, source.Name, target.String()), ObservedGeneration: rs.Generation})
	if err := r.Status().Patch(ctx, rs, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: restorePollInterval}, nil
}

// superuserPassword reads the source cluster's superuser password. The
// operator process holds no credentials for an arbitrary cluster, so
// anything it connects to has to be authenticated from that cluster's own
// secret.
func (r *RestoreReconciler) superuserPassword(ctx context.Context, source *pgshardv1alpha1.PgShardCluster) (string, error) {
	var sec corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: source.Namespace, Name: SecretName(source.Name)}, &sec); err != nil {
		return "", err
	}
	pw, ok := sec.Data[secretKey]
	if !ok || len(pw) == 0 {
		return "", fmt.Errorf("secret %s has no %q key", SecretName(source.Name), secretKey)
	}
	return string(pw), nil
}

// copySuperuserSecret gives the new cluster the source's superuser password:
// the restored catalog carries the source's roles, so a freshly generated
// password would lock the agent out of its own instance.
func (r *RestoreReconciler) copySuperuserSecret(ctx context.Context, source *pgshardv1alpha1.PgShardCluster, newName string) error {
	var src corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: source.Namespace, Name: SecretName(source.Name)}, &src); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("source cluster %s has no superuser secret %s yet", source.Name, SecretName(source.Name))
		}
		return err
	}
	dst := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: SecretName(newName), Namespace: source.Namespace, Labels: map[string]string{LabelCluster: newName}},
		Type: src.Type, Data: src.Data}
	if err := r.Create(ctx, dst); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// observe refreshes the per-group progress from the new cluster and settles
// the phase once every primary left recovery and the cluster is Ready.
func (r *RestoreReconciler) observe(ctx context.Context, rs *pgshardv1alpha1.PgShardRestore, c *pgshardv1alpha1.PgShardCluster) (ctrl.Result, error) {
	src, _ := RestoreSourceOf(c)
	base := rs.DeepCopy()
	rs.Status.Phase = pgshardv1alpha1.RestorePhaseRestoring
	if rs.Status.StartedAt == nil {
		rs.Status.StartedAt = ptrTime(r.now())
	}
	all := true
	var failed string
	var groups []pgshardv1alpha1.GroupRestoreStatus
	for _, g := range Groups(c) {
		st := pgshardv1alpha1.GroupRestoreStatus{Group: g.Name(), SourceStanza: src.Stanza(g), BackupID: src.BackupIDs[g.Name()]}
		for _, prev := range rs.Status.Groups {
			if prev.Group == g.Name() {
				st = prev
			}
		}
		st.Message = ""
		reached, tl, msg, err := r.groupProgress(ctx, c, g)
		if err != nil {
			return ctrl.Result{}, err
		}
		if reached {
			st.ReachedTarget = true
			st.Timeline = tl
		} else {
			all = false
			st.Message = msg
			if isCrashLoop(msg) && failed == "" {
				failed = fmt.Sprintf("group %s: %s", g.Name(), msg)
			}
		}
		groups = append(groups, st)
	}
	rs.Status.Groups = groups
	ready := meta.IsStatusConditionTrue(c.Status.Conditions, pgshardv1alpha1.ConditionReady)
	var reconcileErr error
	switch {
	case failed != "":
		rs.Status.Phase = pgshardv1alpha1.RestorePhaseFailed
		rs.Status.Error = failed
	case r.now().Sub(rs.Status.StartedAt.Time) > restoreTimeout:
		rs.Status.Phase = pgshardv1alpha1.RestorePhaseFailed
		rs.Status.Error = fmt.Sprintf("cluster %s did not recover within %s", c.Name, restoreTimeout)
	case all && ready && isBarrierRestore(rs):
		rs.Status.Phase = pgshardv1alpha1.RestorePhaseReconciling
		rs.Status.Error = ""
		reconcileErr = r.reconcileTwoPhase(ctx, rs, c)
	case all && ready:
		rs.Status.Phase = pgshardv1alpha1.RestorePhaseRecovered
		rs.Status.Error = ""
		r.reportPrepared(ctx, rs, c)
	}
	inProgress := rs.Status.Phase == pgshardv1alpha1.RestorePhaseRestoring || rs.Status.Phase == pgshardv1alpha1.RestorePhaseReconciling
	if !inProgress {
		rs.Status.CompletedAt = ptrTime(r.now())
	}
	msg := fmt.Sprintf("cluster %s: %d/%d groups recovered, ready=%v", c.Name, countReached(groups), len(groups), ready)
	switch {
	case rs.Status.Error != "":
		msg = rs.Status.Error
	case rs.Status.Phase == pgshardv1alpha1.RestorePhaseReconciling && reconcileErr != nil:
		msg = fmt.Sprintf("cluster %s recovered to the barrier; reconciling prepared transactions: %v", c.Name, reconcileErr)
	case rs.Status.Phase == pgshardv1alpha1.RestorePhaseRecovered && rs.Status.Reconciliation != nil:
		msg = fmt.Sprintf("cluster %s recovered to the barrier and unfenced: %d committed, %d rolled back", c.Name, rs.Status.Reconciliation.Committed, rs.Status.Reconciliation.RolledBack)
	}
	meta.SetStatusCondition(&rs.Status.Conditions, metav1.Condition{Type: "Progressing", Status: boolCondition(inProgress),
		Reason: rs.Status.Phase, Message: msg, ObservedGeneration: rs.Generation})
	if err := r.Status().Patch(ctx, rs, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, err
	}
	if rs.Status.Phase == pgshardv1alpha1.RestorePhaseRecovered {
		if err := r.clearRestoreSource(ctx, c); err != nil {
			return ctrl.Result{}, err
		}
	}
	if reconcileErr != nil {
		return ctrl.Result{}, reconcileErr
	}
	if !inProgress {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: restorePollInterval}, nil
}

// reportPrepared surfaces the pgshard prepared transactions each group
// still holds after a non-barrier restore as the
// PreparedTransactionsPending condition. Such a target is applied per
// group and is not cluster-consistent, so the operator only reports; it
// never finishes them without a decision log.
func (r *RestoreReconciler) reportPrepared(ctx context.Context, rs *pgshardv1alpha1.PgShardRestore, c *pgshardv1alpha1.PgShardCluster) {
	cond := metav1.Condition{Type: pgshardv1alpha1.ConditionPreparedTransactionsPending, Status: metav1.ConditionFalse, Reason: "NonePending",
		Message: "no pgshard prepared transactions are left on the recovered groups", ObservedGeneration: rs.Generation}
	var pending, problems []string
	for i, g := range Groups(c) {
		rs.Status.Groups[i].PreparedTransactions = nil
		if r.TwoPC == nil {
			problems = append(problems, g.Name()+": no two-phase agent client")
			continue
		}
		addr, _, err := r.primaryAgent(ctx, c, g)
		if err == nil {
			var prepared map[string]string
			prepared, err = r.TwoPC.ListPrepared(ctx, addr)
			for _, gid := range slices.Sorted(maps.Keys(prepared)) {
				rs.Status.Groups[i].PreparedTransactions = append(rs.Status.Groups[i].PreparedTransactions, gid)
				pending = append(pending, g.Name()+": "+gid)
			}
		}
		if err != nil {
			problems = append(problems, g.Name()+": "+err.Error())
		}
	}
	switch {
	case len(pending) > 0:
		cond.Status, cond.Reason = metav1.ConditionTrue, "PreparedTransactionsPending"
		cond.Message = fmt.Sprintf("%d pgshard prepared transaction(s) left by the per-group target, finish them by hand: %s", len(pending), strings.Join(pending, "; "))
		if len(problems) > 0 {
			cond.Message += "; not checked: " + strings.Join(problems, "; ")
		}
	case len(problems) > 0:
		cond.Status, cond.Reason = metav1.ConditionUnknown, "CheckFailed"
		cond.Message = "could not list prepared transactions: " + strings.Join(problems, "; ")
	}
	meta.SetStatusCondition(&rs.Status.Conditions, cond)
}

// clearRestoreSource drops the restore annotation once the cluster has
// recovered, so a member that later bootstraps with an empty PGDATA does
// not restore the source's old data over the group again.
func (r *RestoreReconciler) clearRestoreSource(ctx context.Context, c *pgshardv1alpha1.PgShardCluster) error {
	if _, ok := c.Annotations[AnnotationRestoreSource]; !ok {
		return nil
	}
	base := c.DeepCopy()
	delete(c.Annotations, AnnotationRestoreSource)
	if err := r.Patch(ctx, c, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("clear restore source of cluster %s: %w", c.Name, err)
	}
	return nil
}

// reconcileTwoPhase finishes the new cluster's prepared transactions
// against its restored decision log and, when nothing contradicts it,
// releases the write fence the barrier left in the restored catalog. It
// sets the phase to Recovered or Failed on rs; an error means a step could
// not run yet and the caller retries with the phase still Reconciling.
func (r *RestoreReconciler) reconcileTwoPhase(ctx context.Context, rs *pgshardv1alpha1.PgShardRestore, c *pgshardv1alpha1.PgShardCluster) error {
	if r.TwoPC == nil {
		rs.Status.Phase = pgshardv1alpha1.RestorePhaseFailed
		rs.Status.Error = "barrier restore needs the two-phase agent client; the new cluster stays fenced"
		return nil
	}
	groups := Groups(c)
	catalogAddr, catalogEpoch, err := r.primaryAgent(ctx, c, groups[0])
	if err != nil {
		return err
	}
	decisions, err := r.TwoPC.ListTransactionDecisions(ctx, catalogAddr)
	if err != nil {
		return fmt.Errorf("catalog decision log: %w", err)
	}
	st := &pgshardv1alpha1.RestoreReconciliationStatus{Decisions: int32(len(decisions))}
	for _, g := range groups[1:] {
		addr, epoch, err := r.primaryAgent(ctx, c, g)
		if err != nil {
			return err
		}
		out, err := r.TwoPC.ReconcilePrepared(ctx, addr, epoch, int32(g.ShardID), decisions)
		if err != nil {
			return fmt.Errorf("group %s: %w", g.Name(), err)
		}
		st.Committed += int32(out.Committed)
		st.RolledBack += int32(out.RolledBack)
		for _, gid := range out.Contradictions {
			st.Contradictions = append(st.Contradictions, g.Name()+": "+gid)
		}
		for _, gid := range out.Unverifiable {
			st.Unverifiable = append(st.Unverifiable, g.Name()+": "+gid)
		}
	}
	rs.Status.Reconciliation = st
	if blockers := append(append([]string{}, st.Contradictions...), st.Unverifiable...); len(blockers) > 0 {
		rs.Status.Phase = pgshardv1alpha1.RestorePhaseFailed
		rs.Status.Error = fmt.Sprintf("two-phase reconciliation found %d unresolved commit(s), the cluster stays fenced: %s", len(blockers), strings.Join(blockers, "; "))
		return nil
	}
	if err := r.TwoPC.SetWriteFence(ctx, catalogAddr, catalogEpoch, false, ""); err != nil {
		return fmt.Errorf("release write fence: %w", err)
	}
	st.Unfenced = true
	rs.Status.Phase = pgshardv1alpha1.RestorePhaseRecovered
	rs.Status.Error = ""
	logf.FromContext(ctx).Info("barrier restore reconciled and unfenced", "cluster", c.Name, "committed", st.Committed, "rolledBack", st.RolledBack)
	return nil
}

// primaryAgent returns the agent address and epoch of a group's primary.
func (r *RestoreReconciler) primaryAgent(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group) (string, uint64, error) {
	var pg pgshardv1alpha1.PgShardGroup
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: g.Prefix()}, &pg); err != nil {
		return "", 0, err
	}
	var pod corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: pg.Status.Primary}, &pod); err != nil {
		return "", 0, err
	}
	addr := agentAddr(pod.Status.PodIP)
	st, err := r.Agents.Status(ctx, addr)
	if err != nil {
		return "", 0, fmt.Errorf("group %s primary %s: %w", g.Name(), pod.Name, err)
	}
	return addr, st.Epoch, nil
}

const crashLoopPrefix = "primary pod is crash looping"

func isCrashLoop(msg string) bool {
	return len(msg) >= len(crashLoopPrefix) && msg[:len(crashLoopPrefix)] == crashLoopPrefix
}

// groupProgress reports whether the group's primary left recovery, its
// timeline, and otherwise why not.
func (r *RestoreReconciler) groupProgress(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group) (bool, int64, string, error) {
	var pg pgshardv1alpha1.PgShardGroup
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: g.Prefix()}, &pg); err != nil {
		if apierrors.IsNotFound(err) {
			return false, 0, "group not created yet", nil
		}
		return false, 0, "", err
	}
	if pg.Status.Primary == "" {
		return false, 0, "group has no primary yet", nil
	}
	var pod corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: pg.Status.Primary}, &pod); err != nil {
		if apierrors.IsNotFound(err) {
			return false, 0, fmt.Sprintf("primary %s has no pod yet", pg.Status.Primary), nil
		}
		return false, 0, "", err
	}
	if reason := crashLoopReason(&pod); reason != "" {
		return false, 0, fmt.Sprintf("%s: %s", crashLoopPrefix, reason), nil
	}
	if pod.Status.PodIP == "" || !podReady(&pod) {
		return false, 0, fmt.Sprintf("primary %s is restoring; not ready yet", pod.Name), nil
	}
	st, err := r.Agents.Status(ctx, agentAddr(pod.Status.PodIP))
	if err != nil {
		return false, 0, fmt.Sprintf("primary %s: %s", pod.Name, err.Error()), nil
	}
	if !st.Running || !st.Primary {
		return false, 0, fmt.Sprintf("primary %s is still in recovery", pod.Name), nil
	}
	return true, int64(st.Timeline), "", nil
}

// crashLoopReason names the postgres container's crash loop when the pod
// has restarted repeatedly; a restore whose WAL ends before the target
// makes the agent exit at every start.
func crashLoopReason(pod *corev1.Pod) string {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name != "postgres" || cs.RestartCount < 2 {
			continue
		}
		if w := cs.State.Waiting; w != nil && w.Reason == "CrashLoopBackOff" {
			return fmt.Sprintf("%s restarted %d times; check its logs", pod.Name, cs.RestartCount)
		}
	}
	return ""
}

func countReached(groups []pgshardv1alpha1.GroupRestoreStatus) int {
	n := 0
	for _, g := range groups {
		if g.ReachedTarget {
			n++
		}
	}
	return n
}

func boolCondition(b bool) metav1.ConditionStatus {
	if b {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

func (r *RestoreReconciler) fail(ctx context.Context, rs *pgshardv1alpha1.PgShardRestore, msg string) error {
	base := rs.DeepCopy()
	rs.Status.Phase = pgshardv1alpha1.RestorePhaseFailed
	rs.Status.Error = msg
	rs.Status.CompletedAt = ptrTime(r.now())
	meta.SetStatusCondition(&rs.Status.Conditions, metav1.Condition{Type: "Progressing", Status: metav1.ConditionFalse, Reason: "Failed", Message: msg, ObservedGeneration: rs.Generation})
	return r.Status().Patch(ctx, rs, client.MergeFrom(base))
}
