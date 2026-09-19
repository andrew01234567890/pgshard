package operator

import (
	"bytes"
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

// catalogUpgradeRestoreWait is how often a barrier restore asks again whether
// the source's catalog upgrade has finished.
const catalogUpgradeRestoreWait = 30 * time.Second

// BarrierCertifier reports whether a barrier of that name was certified,
// which groups it holds a restore point on and when it was recorded, and
// lifts the fence a restored catalog came back holding.
type BarrierCertifier interface {
	CertifiedBarrier(ctx context.Context, dsn, password, name string) (BarrierRecord, error)
	ClearWriteFenceAfterRestore(ctx context.Context, dsn, password string) error
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
	// Both clusters, because this pass reaches agents of both: it lists and
	// reconciles prepared transactions on the source, and polls the
	// primaries of the new one. Their agent tokens are unrelated --
	// ensureAgentSecret generates a fresh random one per cluster and
	// nothing copies it -- so a single token reaches only half of them.
	//
	// This used to say the new cluster's secret was a copy of the source's.
	// It is not; only the superuser Secret is copied, which is why the
	// restore authenticated at all while a token derived from that password
	// was still accepted (PGS-572).
	ctx = withClusterAgentTokens(ctx, r.Client, rs.Namespace, rs.Spec.ClusterName, rs.Spec.NewClusterName)
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
	//
	// Not while the source's catalog is switching generations: the groups
	// this restore recovers are read from the cluster's status and the
	// barrier from its catalog Service, and during the cutover and a
	// rollback the two can name different generations for a pass. The
	// retiring stage that follows one does not wait: it lasts the whole
	// retirement window, and the barrier is read at the group's own address
	// rather than the stable endpoint, so it does not depend on the
	// endpoint having caught up.
	if up := source.Status.CatalogUpgrade; rs.Spec.Target.Barrier != nil && r.Barriers != nil && up != nil &&
		(up.Stage == CatalogUpgradeCutover || up.RollbackRequested || up.RollbackStarted) {
		base := rs.DeepCopy()
		meta.SetStatusCondition(&rs.Status.Conditions, metav1.Condition{Type: "Progressing", Status: metav1.ConditionFalse, Reason: "WaitingForCatalogUpgrade",
			Message:            fmt.Sprintf("waiting for %s's catalog to finish switching generations before judging barrier %q", source.Name, *rs.Spec.Target.Barrier),
			ObservedGeneration: rs.Generation})
		if err := r.Status().Patch(ctx, rs, client.MergeFrom(base)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: catalogUpgradeRestoreWait}, nil
	}
	if rs.Spec.Target.Barrier != nil && r.Barriers != nil {
		name := *rs.Spec.Target.Barrier
		password, perr := r.superuserPassword(ctx, &source)
		if perr != nil {
			return ctrl.Result{}, r.fail(ctx, rs, fmt.Sprintf("cannot read %s's superuser secret to confirm barrier %q: %v", source.Name, name, perr))
		}
		// pgshard.restore_points is keyed by the barrier's own name; the
		// pgshard- prefix belongs to the WAL restore point the recovery
		// target names, not to the catalog row.
		// The group's own address, not the stable endpoint: the endpoint
		// moves after the status that names the new generation, so for a
		// window after a cutover the stable one answers as the catalog the
		// restore is NOT going to recover (PGS-932).
		rec, cerr := r.Barriers.CertifiedBarrier(ctx, CatalogGroupDSN(&source), password, name)
		if cerr != nil {
			return ctrl.Result{}, r.fail(ctx, rs, fmt.Sprintf("cannot confirm barrier %q is certified on %s: %v", name, source.Name, cerr))
		}
		if !rec.Certified {
			return ctrl.Result{}, r.fail(ctx, rs, fmt.Sprintf("barrier %q is not certified on %s; restoring to it would land on a point that is not two-phase consistent", name, source.Name))
		}
		// Certified says the barrier held on the groups it was taken on,
		// not on the groups this restore recovers: those are the source's
		// serving groups NOW. One without the restore point runs recovery
		// to the end of its WAL, fails with "recovery ended before
		// configured recovery target was reached", and the restore only
		// finds out after it has created the cluster and watched that
		// member crash-loop.
		// A cluster built by a restore carries its source's
		// pgshard.restore_points rows, but archives to stanzas of its own
		// that begin with that restore. A barrier recorded before the restore
		// that built this cluster completed was taken on the cluster it was
		// restored from -- a restore to the end of the archive keeps
		// replaying that cluster's WAL after this one's object exists -- and
		// its restore point is in that cluster's repository, not this one's.
		cut, err := InheritedBarrierCutoffOf(ctx, r.Client, &source)
		if err != nil {
			return ctrl.Result{}, err
		}
		if refusal := cut.Refusal(name, rec.CreatedAt); refusal != "" {
			return ctrl.Result{}, r.fail(ctx, rs, refusal)
		}
		refusal, berr := backupAfterBarrier(ctx, r.Client, rs, name, rec)
		if berr != nil {
			return ctrl.Result{}, berr
		}
		if refusal != "" {
			return ctrl.Result{}, r.fail(ctx, rs, refusal)
		}
		// A manifest from before the catalog identifier was recorded names
		// "catalog" and says nothing about which catalog system that was. On
		// a cluster whose catalog has been rebuilt by a major upgrade it is
		// the barrier most likely to be wrong: the rows travelled with the
		// catalog, while the group a restore recovers was initdb'd since and
		// holds none of its restore points. Treated as covering the catalog
		// it recovers the new group to a name only the old stanza has, which
		// ends in "recovery ended before configured recovery target was
		// reached" and a crash-looping member -- the late failure this whole
		// gate exists to turn into an up-front refusal.
		if !rec.CatalogIdentified && CatalogGeneration(&source) > 1 {
			rec.Groups = slices.DeleteFunc(rec.Groups, func(g string) bool { return g == "catalog" })
			delete(rec.LSNs, "catalog")
		}
		if missing := groupsWithoutBarrier(Groups(&source), rec.Groups); len(missing) > 0 {
			return ctrl.Result{}, r.fail(ctx, rs, fmt.Sprintf("barrier %q on %s has no restore point on group(s) %s, which this restore would recover to it; take a new barrier that covers every serving group",
				name, source.Name, strings.Join(missing, ", ")))
		}
	}
	// The group count and major are already checked equal above, so the
	// groups line up one for one; only their names differ, and only when
	// the source carried a generation the new cluster has no status for.
	sourceGroups := map[string]string{}
	newGroups, srcGroups := Groups(newCluster), Groups(&source)
	for i := range newGroups {
		if newGroups[i].Name() != srcGroups[i].Name() {
			sourceGroups[newGroups[i].Name()] = srcGroups[i].Name()
		}
	}
	src := RestoreSource{
		SourceCluster: source.Name, Major: source.Spec.PostgreSQL.Major, Restore: rs.Name, BackupIDs: ids,
		SourceGroups: sourceGroups,
		Type:         target.Type, Target: target.Target, TargetTLI: target.TargetTLI, Exclusive: target.Exclusive,
	}
	newCluster.Labels = map[string]string{LabelRestoredFrom: rs.Name}
	newCluster.Annotations = map[string]string{AnnotationRestoreSource: src.Encode()}
	createdSecret, err := r.copySuperuserSecret(ctx, rs, &source, newCluster.Name)
	if err != nil {
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.fail(ctx, rs, err.Error())
	}
	if err := r.Create(ctx, newCluster); err != nil {
		// The credential this pass wrote belongs to a cluster that does
		// not exist, so leaving it behind would poison the next attempt
		// with a copy nothing owns. One created by an earlier pass is not
		// touched: it is already this restore's own.
		if createdSecret {
			sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: SecretName(newCluster.Name), Namespace: newCluster.Namespace}}
			if derr := r.Delete(ctx, sec); client.IgnoreNotFound(derr) != nil {
				logf.FromContext(ctx).Error(derr, "removing the copied superuser secret after a failed cluster create")
			}
		}
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
		rs.Status.Groups = append(rs.Status.Groups, pgshardv1alpha1.GroupRestoreStatus{Group: g.Name(), SourceStanza: src.Stanza(g), BackupID: ids[src.SourceGroup(g)]})
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
// copySuperuserSecret puts the source's credential where the restored
// cluster will look for it, and reports whether this pass created it.
//
// A secret already sitting at that name is not evidence of anything: it may
// be left over from a deleted cluster, or belong to one that still exists.
// Adopting it silently gives the restored cluster a credential that does
// not match what the restored catalog holds, which locks its own agents and
// routers out of it. So a collision is only reused when the object proves
// it is this restore's own earlier copy, carrying the same password.
func (r *RestoreReconciler) copySuperuserSecret(ctx context.Context, rs *pgshardv1alpha1.PgShardRestore, source *pgshardv1alpha1.PgShardCluster, newName string) (bool, error) {
	var src corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: source.Namespace, Name: SecretName(source.Name)}, &src); err != nil {
		if apierrors.IsNotFound(err) {
			return false, fmt.Errorf("source cluster %s has no superuser secret %s yet", source.Name, SecretName(source.Name))
		}
		return false, err
	}
	dst := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: SecretName(newName), Namespace: source.Namespace,
		Labels: map[string]string{LabelCluster: newName},
		Annotations: map[string]string{
			AnnotationRestoreUID:       string(rs.UID),
			AnnotationRestoreSourceUID: string(source.UID),
		}},
		Type: src.Type, Data: src.Data}
	err := r.Create(ctx, dst)
	if err == nil {
		return true, nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return false, err
	}
	var existing corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: source.Namespace, Name: SecretName(newName)}, &existing); err != nil {
		return false, err
	}
	switch {
	// An unstamped object cannot be this restore's own copy, and an
	// unidentified restore cannot claim one.
	case rs.UID == "" || existing.Annotations[AnnotationRestoreUID] != string(rs.UID):
		return false, fmt.Errorf("secret %s already exists and was not created by this restore; remove it or restore into another name", SecretName(newName))
	case existing.Annotations[AnnotationRestoreSourceUID] != string(source.UID):
		return false, fmt.Errorf("secret %s carries a credential from another source cluster; remove it or restore into another name", SecretName(newName))
	case !bytes.Equal(existing.Data[secretKey], src.Data[secretKey]):
		return false, fmt.Errorf("secret %s holds a different password than %s; the restored cluster would lock its own agents out", SecretName(newName), SecretName(source.Name))
	}
	return false, nil
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
		// On the cluster too, because that is what outlives this object:
		// a barrier recorded before recovery ended belongs to the cluster
		// this one was restored from, and the check that refuses it needs
		// the time long after anyone has deleted the PgShardRestore.
		if c.Annotations == nil || c.Annotations[AnnotationRestoreCompletedAt] == "" {
			stamped := c.DeepCopy()
			if stamped.Annotations == nil {
				stamped.Annotations = map[string]string{}
			}
			stamped.Annotations[AnnotationRestoreCompletedAt] = rs.Status.CompletedAt.UTC().Format(time.RFC3339)
			if err := r.Patch(ctx, stamped, client.MergeFrom(c)); err != nil {
				return ctrl.Result{}, err
			}
			c = stamped
		}
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
	// Clearing comes BEFORE the phase is written, because Reconcile returns
	// immediately for a Recovered restore. Clearing afterwards meant one
	// failed patch -- or a crash in the gap -- left the restore-source
	// annotation on the cluster with nothing that would ever retry it, and
	// a later primary bootstrap with an empty PGDATA acts on that
	// annotation: it would restore the ORIGINAL source backup over the
	// cluster that has been running since. Clearing is idempotent, so
	// doing it first costs nothing when the patch below then fails.
	if rs.Status.Phase == pgshardv1alpha1.RestorePhaseRecovered {
		if err := r.clearRestoreSource(ctx, c); err != nil {
			return ctrl.Result{}, err
		}
	}
	meta.SetStatusCondition(&rs.Status.Conditions, metav1.Condition{Type: "Progressing", Status: boolCondition(inProgress),
		Reason: rs.Status.Phase, Message: msg, ObservedGeneration: rs.Generation})
	if err := r.Status().Patch(ctx, rs, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, err
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
	catalogAddr, _, err := r.primaryAgent(ctx, c, groups[0])
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
		for _, gid := range out.Unreadable {
			st.Unreadable = append(st.Unreadable, g.Name()+": "+gid)
		}
	}
	rs.Status.Reconciliation = st
	blockers := append(append([]string{}, st.Contradictions...), st.Unverifiable...)
	blockers = append(blockers, st.Unreadable...)
	if len(blockers) > 0 {
		rs.Status.Phase = pgshardv1alpha1.RestorePhaseFailed
		rs.Status.Error = fmt.Sprintf("two-phase reconciliation found %d transaction(s) it could not resolve, the cluster stays fenced: %s", len(blockers), strings.Join(blockers, "; "))
		return nil
	}
	// Straight to the catalog, not through the agent RPC: a catalog
	// restored to a certified barrier comes back holding that barrier's
	// fence owner -- the restore point is taken while the fence is up --
	// and the agent's SetWriteFence refuses to touch a fence it does not
	// own, which would leave the restored cluster fenced for good.
	password, perr := r.superuserPassword(ctx, c)
	if perr != nil {
		return fmt.Errorf("read superuser secret to release the write fence: %w", perr)
	}
	if err := r.Barriers.ClearWriteFenceAfterRestore(ctx, CatalogDSN(c), password); err != nil {
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

// InheritedBarrierCutoff is where a restored cluster's inherited barriers
// end: those recorded before Built, plus inheritedBarrierMargin, belong to
// the cluster it was restored from. The restore reconciler refuses them and
// the admin UI marks them, from this one value, so the two cannot disagree
// about which barriers are restorable (PGS-961).
type InheritedBarrierCutoff struct {
	Restore, Cluster string
	Built            time.Time
}

// InheritedBarrierCutoffOf returns the cut-off of cluster, or nil for a
// cluster no restore built.
func InheritedBarrierCutoffOf(ctx context.Context, c client.Reader, cluster *pgshardv1alpha1.PgShardCluster) (*InheritedBarrierCutoff, error) {
	from := cluster.Labels[LabelRestoredFrom]
	if from == "" {
		return nil, nil
	}
	// The cluster's own stamp first: the PgShardRestore is a one-shot
	// object operators delete, and without it the cut-off falls back to the
	// cluster's creation time -- which is BEFORE recovery ended, so every
	// barrier inherited in that window is accepted again. That is the
	// window this check exists for.
	built := cluster.CreationTimestamp.Time
	if at, err := time.Parse(time.RFC3339, cluster.Annotations[AnnotationRestoreCompletedAt]); err == nil {
		built = at
	} else {
		var buildingRestore pgshardv1alpha1.PgShardRestore
		gerr := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: from}, &buildingRestore)
		switch {
		case gerr == nil:
			if buildingRestore.Spec.NewClusterName == cluster.Name && buildingRestore.Status.CompletedAt != nil {
				built = buildingRestore.Status.CompletedAt.Time
			}
		case !apierrors.IsNotFound(gerr):
			// Deleted is an answer; unreachable is not. Falling through on
			// an API failure would loosen the cut-off for as long as the
			// failure lasted.
			return nil, gerr
		}
	}
	return &InheritedBarrierCutoff{Restore: from, Cluster: cluster.Name, Built: built}, nil
}

// Refusal is why a restore to barrier, recorded at recorded, is refused as
// inherited, or "" when it is not.
func (cut *InheritedBarrierCutoff) Refusal(barrier string, recorded time.Time) string {
	if cut == nil || !recorded.Before(cut.Built.Add(inheritedBarrierMargin)) {
		return ""
	}
	return inheritedBarrierRefusal(barrier, recorded, cut.Restore, cut.Cluster, cut.Built)
}

// inheritedBarrierMargin moves the inherited-barrier cut-off FORWARD, so a
// barrier recorded shortly after the restore that built its cluster finished
// is refused as well. The two times come from different clocks -- the
// barrier's from the source database, the cut-off from the operator or the
// API server at second granularity -- and cannot be ordered closer than the
// skew between them.
//
// Erring towards refusing is the owner's choice (PGS-933). Its cost is not
// "retake it": a barrier refused here is refused for good, and a new one is a
// different, later point in time -- that barrier's own moment cannot be
// restored to. What keeps it narrow is backupAfterBarrier, which already
// refuses a barrier with no backup ending before it, and on a freshly
// restored cluster that backup normally takes longer than the margin. Erring
// the other way lets an inherited barrier through, and that restore fails
// later and further from its cause, after it has created the cluster and
// watched a member crash-loop on a restore point its repository does not hold.
const inheritedBarrierMargin = time.Minute

// inheritedBarrierRefusal says which side of the cut-off the barrier fell on,
// because the two are different claims. Before it, the barrier was taken
// while the cluster was still replaying the one it was restored from. Within
// the margin after it, all the code knows is that the clocks cannot tell --
// and saying "before" there would be false.
func inheritedBarrierRefusal(barrier string, recorded time.Time, restore, cluster string, built time.Time) string {
	rec, cut := recorded.UTC().Format(time.RFC3339), built.UTC().Format(time.RFC3339)
	clocks := "The two times come from different clocks -- the barrier's from the source database, the cut-off from the operator -- and cannot be ordered closer than the clock skew between them"
	if recorded.Before(built) {
		return fmt.Sprintf("barrier %q was recorded at %s, before PgShardRestore %s finished building %s at %s: it belongs to the cluster %s was restored from, whose repository holds its restore point; take a new barrier on %s. %s, so if they are within a few seconds this may be skew instead, and a barrier taken after the restore really did finish is the one to retake",
			barrier, rec, restore, cluster, cut, cluster, cluster, clocks)
	}
	return fmt.Sprintf("barrier %q was recorded at %s, within %.0f seconds after PgShardRestore %s finished building %s at %s. %s, so a barrier this close may still belong to the cluster %s was restored from, and it is refused rather than risk a restore that fails later on a restore point this cluster's repository does not hold. Take a new barrier on %s: one recorded at least %.0f seconds after the restore is accepted",
		barrier, rec, inheritedBarrierMargin.Seconds(), restore, cluster, cut, clocks, cluster, cluster, inheritedBarrierMargin.Seconds())
}

func (r *RestoreReconciler) fail(ctx context.Context, rs *pgshardv1alpha1.PgShardRestore, msg string) error {
	base := rs.DeepCopy()
	rs.Status.Phase = pgshardv1alpha1.RestorePhaseFailed
	rs.Status.Error = msg
	rs.Status.CompletedAt = ptrTime(r.now())
	meta.SetStatusCondition(&rs.Status.Conditions, metav1.Condition{Type: "Progressing", Status: metav1.ConditionFalse, Reason: "Failed", Message: msg, ObservedGeneration: rs.Generation})
	return r.Status().Patch(ctx, rs, client.MergeFrom(base))
}

// groupsWithoutBarrier names the groups of want that a barrier's manifest
// has no restore point for. The manifest keys a shard group by the name
// shard_status carries, which is Group.Name(), and the catalog as "catalog"
// whatever its generation.
// backupAfterBarrier explains why the restore's base backup cannot reach the
// barrier, or returns "". Recovery replays forward from where a backup
// ended, so a backup of a group that ended after the barrier's restore point
// on that group has already passed it: recovery never stops at the barrier,
// and the member fails late. Only a backup the restore names as a
// PgShardBackup is known group by group.
func backupAfterBarrier(ctx context.Context, c client.Client, rs *pgshardv1alpha1.PgShardRestore, barrier string, rec BarrierRecord) (string, error) {
	if rs.Spec.BackupID == "" {
		return "", nil
	}
	var b pgshardv1alpha1.PgShardBackup
	if err := c.Get(ctx, types.NamespacedName{Namespace: rs.Namespace, Name: rs.Spec.BackupID}, &b); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", err
	}
	var late []string
	for _, g := range b.Status.Groups {
		key := g.Group
		if strings.HasPrefix(key, "catalog") {
			key = "catalog"
		}
		point, ok := rec.LSNs[key]
		if !ok || g.StopLSN == "" {
			continue
		}
		stop, err := backup.ParseLSN(g.StopLSN)
		if err != nil || stop <= point {
			continue
		}
		late = append(late, fmt.Sprintf("%s (backup ends at %s, the restore point is at %s)", g.Group, g.StopLSN, formatLSN(point)))
	}
	if len(late) == 0 {
		return "", nil
	}
	return fmt.Sprintf("backup %s ended after barrier %q on group(s) %s, so recovery from it cannot stop at the barrier; use a backup taken before the barrier",
		b.Name, barrier, strings.Join(late, ", ")), nil
}

func groupsWithoutBarrier(want []Group, recorded []string) []string {
	var missing []string
	for _, g := range want {
		key := g.Name()
		if g.Kind == "catalog" {
			key = "catalog"
		}
		if !slices.Contains(recorded, key) {
			missing = append(missing, g.Name())
		}
	}
	return missing
}
