package operator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/agent/backup"
)

const (
	backupPollInterval = 5 * time.Second
	// backupRunTimeout bounds one PgShardBackup across all groups.
	backupRunTimeout = 12 * time.Hour
	// ConditionRetentionApplied reports whether expire ran after the backup.
	ConditionRetentionApplied = "RetentionApplied"
)

// BackupReconciler executes PgShardBackup objects against the primaries of
// the cluster's groups, one group after another.
type BackupReconciler struct {
	client.Client
	Agents BackupAgentClient
	Now    func() time.Time

	mu   sync.Mutex
	runs map[types.UID]*backupRun
	// base is the manager context; runs are bound to it.
	base context.Context
}

type backupRun struct {
	done   chan struct{}
	groups []pgshardv1alpha1.GroupBackupStatus
	// expireErr collects retention failures; they do not fail the backup.
	expireErr error
	// expireSkipped marks a run that did not reach retention because a
	// group failed or the run was cancelled.
	expireSkipped bool
	// cancel ends the worker. A backup whose record is deleted has nothing
	// left to report to, and it holds an agent and a repository for as long
	// as it runs -- long enough for a replacement to start against the same
	// stanza beside it.
	cancel context.CancelFunc
	// key is what the record was called, so a reconcile that finds it gone
	// -- and so has no UID -- can still find the worker.
	key types.NamespacedName
}

// SetupWithManager registers the reconciler and its background runner.
func (r *BackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.Add(r); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&pgshardv1alpha1.PgShardBackup{}).
		Named("pgshardbackup").
		Complete(r)
}

// Start records the manager context; it returns when ctx ends.
func (r *BackupReconciler) Start(ctx context.Context) error {
	r.mu.Lock()
	r.base = ctx
	r.mu.Unlock()
	<-ctx.Done()
	return nil
}

// NeedLeaderElection keeps runs on the leader only.
func (r *BackupReconciler) NeedLeaderElection() bool { return true }

func (r *BackupReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// backupTypeArg maps the CRD type to the pgbackrest type.
func backupTypeArg(t string) (string, error) {
	switch t {
	case "", "full":
		return string(backup.Full), nil
	case "differential":
		return string(backup.Diff), nil
	case "incremental":
		return string(backup.Incr), nil
	}
	return "", fmt.Errorf("unknown backup type %q", t)
}

// Reconcile drives one PgShardBackup through Pending, Running and a
// terminal phase.
func (r *BackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	var b pgshardv1alpha1.PgShardBackup
	if err := r.Get(ctx, req.NamespacedName, &b); err != nil {
		if apierrors.IsNotFound(err) {
			// Deleted between the event and this read: there is no UID to
			// look the worker up by, which is how one used to be left
			// running with nothing to report to.
			if !r.stopByName(req.NamespacedName) {
				return ctrl.Result{RequeueAfter: backupPollInterval}, nil
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !b.DeletionTimestamp.IsZero() || b.Status.Phase == pgshardv1alpha1.BackupPhaseCompleted || b.Status.Phase == pgshardv1alpha1.BackupPhaseFailed {
		if !r.stop(b.UID) {
			return ctrl.Result{RequeueAfter: backupPollInterval}, nil
		}
		return ctrl.Result{}, nil
	}
	if run := r.run(b.UID); run != nil {
		select {
		case <-run.done:
			return ctrl.Result{}, r.finish(ctx, &b, run)
		default:
			return ctrl.Result{RequeueAfter: backupPollInterval}, nil
		}
	}
	if b.Status.Phase == pgshardv1alpha1.BackupPhaseRunning {
		return ctrl.Result{}, r.fail(ctx, &b, "operator restarted while the backup was running; create a new PgShardBackup")
	}
	if _, err := backupTypeArg(b.Spec.Type); err != nil {
		return ctrl.Result{}, r.fail(ctx, &b, err.Error())
	}
	var cluster pgshardv1alpha1.PgShardCluster
	if err := r.Get(ctx, types.NamespacedName{Namespace: b.Namespace, Name: b.Spec.ClusterName}, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.fail(ctx, &b, fmt.Sprintf("cluster %q not found", b.Spec.ClusterName))
		}
		return ctrl.Result{}, err
	}
	ctx = withClusterAgentToken(ctx, r.Client, cluster.Namespace, cluster.Name)
	pol, err := findBackupPolicy(ctx, r.Client, &cluster)
	if errors.Is(err, ErrBackupPolicyMissing) {
		return ctrl.Result{}, r.fail(ctx, &b, err.Error())
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if pol == nil {
		return ctrl.Result{}, r.fail(ctx, &b, fmt.Sprintf("cluster %q has no spec.backup.policyRef", b.Spec.ClusterName))
	}
	targets, pending, err := r.primaries(ctx, &cluster)
	if err != nil {
		return ctrl.Result{}, err
	}
	if pending == "" {
		pending, err = r.otherRunning(ctx, &b)
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	if pending != "" {
		if b.Status.Phase != pgshardv1alpha1.BackupPhasePending {
			if err := r.setPhase(ctx, &b, pgshardv1alpha1.BackupPhasePending, pending); err != nil {
				return ctrl.Result{}, err
			}
		}
		log.Info("backup waiting for primaries", "reason", pending)
		return ctrl.Result{RequeueAfter: backupPollInterval}, nil
	}
	if err := r.start(ctx, &b, &cluster, pol, targets); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: backupPollInterval}, nil
}

// otherRunning names another backup of the same cluster that is running;
// pgbackrest holds one backup lock per stanza, so runs are serialised here.
func (r *BackupReconciler) otherRunning(ctx context.Context, b *pgshardv1alpha1.PgShardBackup) (string, error) {
	others, err := backupsOfCluster(ctx, r.Client, b.Namespace, b.Spec.ClusterName)
	if err != nil {
		return "", err
	}
	for _, o := range others {
		if o.UID != b.UID && o.Status.Phase == pgshardv1alpha1.BackupPhaseRunning {
			return fmt.Sprintf("backup %s of cluster %s is still running", o.Name, b.Spec.ClusterName), nil
		}
	}
	return "", nil
}

// backupTarget is one group's primary agent.
type backupTarget struct {
	group  Group
	stanza string
	addr   string
}

// primaries resolves every group's primary agent address; pending names the
// first group that has no reachable primary yet.
func (r *BackupReconciler) primaries(ctx context.Context, c *pgshardv1alpha1.PgShardCluster) ([]backupTarget, string, error) {
	return primariesOf(ctx, r.Client, c)
}

// primariesOf resolves every group's primary agent for c. It is a function
// rather than a method because the scheduler needs the same answer for
// verification and holds no reconciler.
func primariesOf(ctx context.Context, k client.Client, c *pgshardv1alpha1.PgShardCluster) ([]backupTarget, string, error) {
	var out []backupTarget
	for _, g := range Groups(c) {
		var pg pgshardv1alpha1.PgShardGroup
		if err := k.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: g.Prefix()}, &pg); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, fmt.Sprintf("group %s not created yet", g.Name()), nil
			}
			return nil, "", err
		}
		if pg.Status.Primary == "" {
			return nil, fmt.Sprintf("group %s has no primary yet", g.Name()), nil
		}
		var pod corev1.Pod
		if err := k.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: pg.Status.Primary}, &pod); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, fmt.Sprintf("primary %s of group %s has no pod", pg.Status.Primary, g.Name()), nil
			}
			return nil, "", err
		}
		if !podReady(&pod) || pod.Status.PodIP == "" {
			return nil, fmt.Sprintf("primary %s of group %s is not ready", pg.Status.Primary, g.Name()), nil
		}
		out = append(out, backupTarget{group: g, stanza: backup.StanzaName(c.Name, g.Name(), MajorFor(c, g)), addr: agentAddr(pod.Status.PodIP)})
	}
	return out, "", nil
}

func (r *BackupReconciler) run(uid types.UID) *backupRun {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.runs[uid]
}

func (r *BackupReconciler) forget(uid types.UID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.runs, uid)
}

// stop cancels the run for uid and reports whether it has finished. The
// entry is kept until it has: a worker still running is still holding an
// agent and a repository, and forgetting it here is what let a replacement
// start beside it.
func (r *BackupReconciler) stop(uid types.UID) bool {
	r.mu.Lock()
	run := r.runs[uid]
	r.mu.Unlock()
	if run == nil {
		return true
	}
	if run.cancel != nil {
		run.cancel()
	}
	select {
	case <-run.done:
		r.forget(uid)
		return true
	default:
		return false
	}
}

// stopByName is stop for a record that is already gone, so the caller has
// its name but not its UID.
func (r *BackupReconciler) stopByName(key types.NamespacedName) bool {
	r.mu.Lock()
	var uid types.UID
	for u, run := range r.runs {
		if run.key == key {
			uid = u
			break
		}
	}
	r.mu.Unlock()
	if uid == "" {
		return true
	}
	return r.stop(uid)
}

func (r *BackupReconciler) start(ctx context.Context, b *pgshardv1alpha1.PgShardBackup, c *pgshardv1alpha1.PgShardCluster, pol *pgshardv1alpha1.PgShardBackupPolicy, targets []backupTarget) error {
	typ, _ := backupTypeArg(b.Spec.Type)
	base := b.DeepCopy()
	b.Status.Phase = pgshardv1alpha1.BackupPhaseRunning
	b.Status.ClusterName, b.Status.Type = b.Spec.ClusterName, b.Spec.Type
	b.Status.Policy, b.Status.PolicyUID = pol.Name, string(pol.UID)
	b.Status.StartedAt = ptrTime(r.now())
	b.Status.Error = ""
	b.Status.Groups = nil
	for _, t := range targets {
		b.Status.Groups = append(b.Status.Groups, pgshardv1alpha1.GroupBackupStatus{Group: t.group.Name(), Stanza: t.stanza})
	}
	meta.SetStatusCondition(&b.Status.Conditions, metav1.Condition{Type: "Progressing", Status: metav1.ConditionTrue, Reason: "Running",
		Message: fmt.Sprintf("%s backup of %d groups via policy %s", typ, len(targets), pol.Name), ObservedGeneration: b.Generation})
	if err := r.Status().Patch(ctx, b, client.MergeFrom(base)); err != nil {
		return err
	}
	r.mu.Lock()
	baseCtx := r.base
	r.mu.Unlock()
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	// The cancel belongs to the run, not to the goroutine: a record that is
	// deleted while its backup is going has to be able to end it.
	runCtx, cancel := context.WithTimeout(baseCtx, backupRunTimeout)
	run := &backupRun{done: make(chan struct{}), cancel: cancel,
		key: types.NamespacedName{Namespace: b.Namespace, Name: b.Name}}
	r.mu.Lock()
	if r.runs == nil {
		r.runs = map[types.UID]*backupRun{}
	}
	r.runs[b.UID] = run
	r.mu.Unlock()
	log := logf.FromContext(ctx).WithValues("backup", b.Name, "cluster", c.Name)
	go func() {
		defer close(run.done)
		defer cancel()
		runCtx := withClusterAgentToken(runCtx, r.Client, c.Namespace, c.Name)
		failed := false
		for _, t := range targets {
			// A cancelled run has nothing left to try; a failed GROUP does.
			if runCtx.Err() != nil {
				break
			}
			st := pgshardv1alpha1.GroupBackupStatus{Group: t.group.Name(), Stanza: t.stanza, StartedAt: ptrTime(r.now())}
			res, err := r.Agents.Backup(runCtx, t.addr, typ)
			st.CompletedAt = ptrTime(r.now())
			st.Duration = st.CompletedAt.Sub(st.StartedAt.Time).Round(time.Second).String()
			if err != nil {
				// Every other group is still attempted. Each has its own
				// stanza and its own repository, so one group's failure
				// says nothing about the next -- and abandoning the run
				// left every group after the failure with whatever backup
				// it had from the previous run, which is exactly the
				// recovery-point window a backup is taken to close.
				st.Error = err.Error()
				failed = true
				log.Error(err, "group backup failed; continuing with the remaining groups",
					"group", t.group.Name(), "log", strings.Join(res.Log, "\n"))
				run.groups = append(run.groups, st)
				continue
			}
			st.BackupID = res.Label
			st.StartLSN = formatLSN(res.StartLSN)
			st.StopLSN = formatLSN(res.StopLSN)
			st.WALStart = res.ArchiveStart
			st.WALStop = res.ArchiveStop
			st.SizeBytes = int64(res.SizeBytes)
			st.RepoSizeBytes = int64(res.RepoBytes)
			run.groups = append(run.groups, st)
			log.Info("group backup completed", "group", t.group.Name(), "label", res.Label)
		}
		// Only now, and only if every group succeeded: a group that
		// expires as soon as its own backup lands can retire the set the
		// last complete cluster backup depends on while a later group is
		// still running, and a failure there leaves nothing restorable
		// cluster-wide. Retention is what the early return used to buy,
		// and it is the one thing that must still wait for the whole run.
		if failed || runCtx.Err() != nil {
			run.expireSkipped = true
			return
		}
		for _, t := range targets {
			if err := r.Agents.Expire(runCtx, t.addr); err != nil {
				run.expireErr = errors.Join(run.expireErr, fmt.Errorf("group %s: %w", t.group.Name(), err))
			}
		}
	}()
	return nil
}

func (r *BackupReconciler) finish(ctx context.Context, b *pgshardv1alpha1.PgShardBackup, run *backupRun) error {
	base := b.DeepCopy()
	b.Status.Groups = run.groups
	b.Status.CompletedAt = ptrTime(r.now())
	b.Status.Phase = pgshardv1alpha1.BackupPhaseCompleted
	b.Status.Error = ""
	// Every failed group, not the last one seen: a run now continues past a
	// failure, so naming one hides the others from whoever has to decide
	// what is restorable.
	var failures []string
	for _, g := range run.groups {
		if g.Error != "" {
			failures = append(failures, fmt.Sprintf("group %s: %s", g.Group, g.Error))
		}
	}
	if len(failures) > 0 {
		b.Status.Phase = pgshardv1alpha1.BackupPhaseFailed
		b.Status.Error = strings.Join(failures, "; ")
	}
	if b.Status.Phase == pgshardv1alpha1.BackupPhaseCompleted && len(run.groups) > 0 {
		b.Status.BackupID = run.groups[0].BackupID
	}
	meta.SetStatusCondition(&b.Status.Conditions, metav1.Condition{Type: "Progressing", Status: metav1.ConditionFalse, Reason: b.Status.Phase,
		Message: firstNonEmpty(b.Status.Error, "all groups backed up"), ObservedGeneration: b.Generation})
	retention := metav1.Condition{Type: ConditionRetentionApplied, Status: metav1.ConditionTrue, Reason: "Expired", Message: "pgbackrest expire ran on every group", ObservedGeneration: b.Generation}
	switch {
	case run.expireSkipped:
		retention.Status = metav1.ConditionFalse
		retention.Reason = "RunIncomplete"
		retention.Message = "retention was not applied: expiring on a group whose backup landed can retire the set a group that failed still depends on"
	case run.expireErr != nil:
		retention.Status = metav1.ConditionFalse
		retention.Reason = "ExpireFailed"
		retention.Message = run.expireErr.Error()
	}
	meta.SetStatusCondition(&b.Status.Conditions, retention)
	if err := r.Status().Patch(ctx, b, client.MergeFrom(base)); err != nil {
		return err
	}
	r.forget(b.UID)
	return nil
}

func (r *BackupReconciler) fail(ctx context.Context, b *pgshardv1alpha1.PgShardBackup, msg string) error {
	base := b.DeepCopy()
	b.Status.Phase = pgshardv1alpha1.BackupPhaseFailed
	b.Status.Error = msg
	b.Status.CompletedAt = ptrTime(r.now())
	meta.SetStatusCondition(&b.Status.Conditions, metav1.Condition{Type: "Progressing", Status: metav1.ConditionFalse, Reason: "Failed", Message: msg, ObservedGeneration: b.Generation})
	return r.Status().Patch(ctx, b, client.MergeFrom(base))
}

func (r *BackupReconciler) setPhase(ctx context.Context, b *pgshardv1alpha1.PgShardBackup, phase, msg string) error {
	base := b.DeepCopy()
	b.Status.Phase = phase
	meta.SetStatusCondition(&b.Status.Conditions, metav1.Condition{Type: "Progressing", Status: metav1.ConditionFalse, Reason: phase, Message: msg, ObservedGeneration: b.Generation})
	return r.Status().Patch(ctx, b, client.MergeFrom(base))
}

func ptrTime(t time.Time) *metav1.Time { mt := metav1.NewTime(t); return &mt }

func formatLSN(v uint64) string { return fmt.Sprintf("%X/%X", v>>32, uint32(v)) }

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
