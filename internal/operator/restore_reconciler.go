package operator

import (
	"context"
	"fmt"
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
	Agents AgentClient
	Now    func() time.Time
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
	var newCluster pgshardv1alpha1.PgShardCluster
	err = r.Get(ctx, types.NamespacedName{Namespace: rs.Namespace, Name: rs.Spec.NewClusterName}, &newCluster)
	switch {
	case apierrors.IsNotFound(err):
		if rs.Status.Phase == pgshardv1alpha1.RestorePhaseRestoring {
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
	switch {
	case failed != "":
		rs.Status.Phase = pgshardv1alpha1.RestorePhaseFailed
		rs.Status.Error = failed
	case r.now().Sub(rs.Status.StartedAt.Time) > restoreTimeout:
		rs.Status.Phase = pgshardv1alpha1.RestorePhaseFailed
		rs.Status.Error = fmt.Sprintf("cluster %s did not recover within %s", c.Name, restoreTimeout)
	case all && ready:
		rs.Status.Phase = pgshardv1alpha1.RestorePhaseRecovered
		rs.Status.Error = ""
	}
	if rs.Status.Phase != pgshardv1alpha1.RestorePhaseRestoring {
		rs.Status.CompletedAt = ptrTime(r.now())
	}
	msg := fmt.Sprintf("cluster %s: %d/%d groups recovered, ready=%v", c.Name, countReached(groups), len(groups), ready)
	if rs.Status.Error != "" {
		msg = rs.Status.Error
	}
	meta.SetStatusCondition(&rs.Status.Conditions, metav1.Condition{Type: "Progressing", Status: boolCondition(rs.Status.Phase == pgshardv1alpha1.RestorePhaseRestoring),
		Reason: rs.Status.Phase, Message: msg, ObservedGeneration: rs.Generation})
	if err := r.Status().Patch(ctx, rs, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, err
	}
	if rs.Status.Phase != pgshardv1alpha1.RestorePhaseRestoring {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: restorePollInterval}, nil
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
