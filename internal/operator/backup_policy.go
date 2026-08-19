package operator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
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
)

// Policy conditions.
const (
	ConditionPolicyValid = "Valid"
	policyRequeue        = time.Minute
)

// cronParser accepts the standard five fields plus @descriptors.
var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// ParseSchedule validates one cron expression.
func ParseSchedule(expr string) (cron.Schedule, error) { return cronParser.Parse(expr) }

// scheduleTypes lists the CRD backup types with their cron expression.
func scheduleTypes(s pgshardv1alpha1.BackupSchedules) map[string]string {
	out := map[string]string{}
	if s.Full != "" {
		out["full"] = s.Full
	}
	if s.Differential != "" {
		out["differential"] = s.Differential
	}
	if s.Incremental != "" {
		out["incremental"] = s.Incremental
	}
	return out
}

// BackupPolicyReconciler validates policies, keeps their schedules armed and
// reports BackupHealthy from the completed PgShardBackups.
type BackupPolicyReconciler struct {
	client.Client
	Scheduler *BackupScheduler
	Now       func() time.Time
}

// SetupWithManager registers the reconciler and the scheduler.
func (r *BackupPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Scheduler == nil {
		r.Scheduler = NewBackupScheduler(mgr.GetClient())
	}
	if r.Scheduler.Barriers == nil {
		r.Scheduler.Barriers = GRPCBarrierClient{}
	}
	if err := mgr.Add(r.Scheduler); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&pgshardv1alpha1.PgShardBackupPolicy{}).
		Watches(&pgshardv1alpha1.PgShardCluster{}, handler.EnqueueRequestsFromMapFunc(clusterToPolicy)).
		Watches(&pgshardv1alpha1.PgShardBackup{}, handler.EnqueueRequestsFromMapFunc(r.backupToPolicy)).
		Named("pgshardbackuppolicy").
		Complete(r)
}

func clusterToPolicy(_ context.Context, obj client.Object) []ctrl.Request {
	c, ok := obj.(*pgshardv1alpha1.PgShardCluster)
	if !ok || c.Spec.Backup.PolicyRef == "" {
		return nil
	}
	return []ctrl.Request{{NamespacedName: client.ObjectKey{Namespace: c.Namespace, Name: c.Spec.Backup.PolicyRef}}}
}

func (r *BackupPolicyReconciler) backupToPolicy(ctx context.Context, obj client.Object) []ctrl.Request {
	b, ok := obj.(*pgshardv1alpha1.PgShardBackup)
	if !ok {
		return nil
	}
	var c pgshardv1alpha1.PgShardCluster
	if err := r.Get(ctx, client.ObjectKey{Namespace: b.Namespace, Name: b.Spec.ClusterName}, &c); err != nil {
		return nil
	}
	return clusterToPolicy(ctx, &c)
}

func (r *BackupPolicyReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// Reconcile validates the policy, syncs its schedules and refreshes status.
func (r *BackupPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var pol pgshardv1alpha1.PgShardBackupPolicy
	if err := r.Get(ctx, req.NamespacedName, &pol); err != nil {
		if apierrors.IsNotFound(err) {
			r.Scheduler.Remove(req.NamespacedName)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !pol.DeletionTimestamp.IsZero() {
		r.Scheduler.Remove(req.NamespacedName)
		return ctrl.Result{}, nil
	}
	base := pol.DeepCopy()
	pol.Status.ObservedGeneration = pol.Generation
	valid := metav1.Condition{Type: ConditionPolicyValid, Status: metav1.ConditionTrue, Reason: "Valid", Message: "policy accepted", ObservedGeneration: pol.Generation}
	if err := r.validate(&pol); err != nil {
		valid.Status = metav1.ConditionFalse
		valid.Reason = "Invalid"
		valid.Message = err.Error()
		r.Scheduler.Remove(req.NamespacedName)
	} else if err := r.Scheduler.Set(req.NamespacedName, pol.Spec.Schedules); err != nil {
		valid.Status = metav1.ConditionFalse
		valid.Reason = "InvalidSchedule"
		valid.Message = err.Error()
	} else if err := r.Scheduler.SetBarrier(req.NamespacedName, pol.Spec.BarrierSchedule); err != nil {
		valid.Status = metav1.ConditionFalse
		valid.Reason = "InvalidSchedule"
		valid.Message = err.Error()
	}
	meta.SetStatusCondition(&pol.Status.Conditions, valid)

	clusters, err := clustersOfPolicy(ctx, r.Client, req.NamespacedName)
	if err != nil {
		return ctrl.Result{}, err
	}
	pol.Status.Clusters = nil
	healthy := metav1.Condition{Type: pgshardv1alpha1.ConditionBackupHealthy, Status: metav1.ConditionTrue, Reason: "Current", ObservedGeneration: pol.Generation}
	var unhealthy []string
	for i := range clusters {
		c := &clusters[i]
		backups, err := backupsOfCluster(ctx, r.Client, c.Namespace, c.Name)
		if err != nil {
			return ctrl.Result{}, err
		}
		last := lastSuccessful(backups)
		h := BackupHealth(r.now(), pol.Spec.Schedules, last)
		cs := pgshardv1alpha1.ClusterBackupStatus{Name: c.Name, LastFullTime: last["full"], LastDifferentialTime: last["differential"], LastIncrementalTime: last["incremental"],
			Healthy: h.Status == metav1.ConditionTrue, Message: h.Message}
		pol.Status.Clusters = append(pol.Status.Clusters, cs)
		if !cs.Healthy {
			unhealthy = append(unhealthy, c.Name+": "+h.Message)
		}
	}
	switch {
	case len(clusters) == 0:
		healthy.Status, healthy.Reason, healthy.Message = metav1.ConditionFalse, "NoClusters", "no PgShardCluster references this policy"
	case len(unhealthy) > 0:
		healthy.Status, healthy.Reason, healthy.Message = metav1.ConditionFalse, "Overdue", strings.Join(unhealthy, "; ")
	default:
		healthy.Message = fmt.Sprintf("%d cluster(s) current", len(clusters))
	}
	meta.SetStatusCondition(&pol.Status.Conditions, healthy)
	if err := r.Status().Patch(ctx, &pol, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: policyRequeue}, nil
}

func (r *BackupPolicyReconciler) validate(pol *pgshardv1alpha1.PgShardBackupPolicy) error {
	probe := &pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster"}, Spec: pgshardv1alpha1.PgShardClusterSpec{PostgreSQL: pgshardv1alpha1.PostgreSQLSpec{Major: 18}}}
	if err := BackupSettings(probe, Groups(probe)[0], &pol.Spec).WithDefaults().Validate(); err != nil {
		return err
	}
	for typ, expr := range scheduleTypes(pol.Spec.Schedules) {
		if _, err := ParseSchedule(expr); err != nil {
			return fmt.Errorf("schedules.%s %q: %w", typ, expr, err)
		}
	}
	if pol.Spec.BarrierSchedule != "" {
		if _, err := ParseSchedule(pol.Spec.BarrierSchedule); err != nil {
			return fmt.Errorf("barrierSchedule %q: %w", pol.Spec.BarrierSchedule, err)
		}
	}
	return nil
}

// lastSuccessful returns the newest completion time per backup type.
func lastSuccessful(backups []pgshardv1alpha1.PgShardBackup) map[string]*metav1.Time {
	out := map[string]*metav1.Time{}
	for i := range backups {
		b := &backups[i]
		if b.Status.Phase != pgshardv1alpha1.BackupPhaseCompleted || b.Status.CompletedAt == nil {
			continue
		}
		typ := b.Spec.Type
		if typ == "" {
			typ = "full"
		}
		if cur := out[typ]; cur == nil || b.Status.CompletedAt.After(cur.Time) {
			out[typ] = b.Status.CompletedAt
		}
	}
	return out
}

// BackupHealth derives the BackupHealthy condition: every scheduled type
// must have a success no older than one full period past its due time; a
// coarser type's success satisfies a finer one (a full counts as an incr).
// Without schedules any completed backup counts as healthy.
func BackupHealth(now time.Time, schedules pgshardv1alpha1.BackupSchedules, last map[string]*metav1.Time) metav1.Condition {
	newest := func(types ...string) (time.Time, bool) {
		var t time.Time
		ok := false
		for _, ty := range types {
			if v := last[ty]; v != nil && v.After(t) {
				t, ok = v.Time, true
			}
		}
		return t, ok
	}
	covers := map[string][]string{
		"full":         {"full"},
		"differential": {"full", "differential"},
		"incremental":  {"full", "differential", "incremental"},
	}
	cond := metav1.Condition{Type: pgshardv1alpha1.ConditionBackupHealthy, Status: metav1.ConditionTrue, Reason: "Current"}
	sched := scheduleTypes(schedules)
	if len(sched) == 0 {
		if _, ok := newest("full", "differential", "incremental"); ok {
			cond.Reason = "Unscheduled"
			cond.Message = "no schedules; a completed backup exists"
			return cond
		}
		cond.Status = metav1.ConditionFalse
		cond.Reason = "NoBackups"
		cond.Message = "no completed backup and no schedules"
		return cond
	}
	var problems []string
	types := make([]string, 0, len(sched))
	for t := range sched {
		types = append(types, t)
	}
	sort.Strings(types)
	for _, typ := range types {
		s, err := ParseSchedule(sched[typ])
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s schedule invalid", typ))
			continue
		}
		lastOK, ok := newest(covers[typ]...)
		if !ok {
			problems = append(problems, fmt.Sprintf("no completed %s backup yet", typ))
			continue
		}
		due := s.Next(lastOK)
		if graceEnd := s.Next(due); !graceEnd.After(now) {
			problems = append(problems, fmt.Sprintf("%s backup overdue: last success %s, due %s", typ, lastOK.UTC().Format(time.RFC3339), due.UTC().Format(time.RFC3339)))
		}
	}
	if len(problems) > 0 {
		cond.Status = metav1.ConditionFalse
		cond.Reason = "Overdue"
		cond.Message = strings.Join(problems, "; ")
		return cond
	}
	cond.Message = "scheduled backups are current"
	return cond
}

// BackupScheduler fires PgShardBackup objects and controller barriers from
// policy cron schedules.
type BackupScheduler struct {
	client client.Client
	cron   *cron.Cron
	now    func() time.Time
	// Barriers asks controllers for barriers; nil disables barrier ticks.
	Barriers BarrierClient

	mu      sync.Mutex
	entries map[types.NamespacedName][]cron.EntryID
	specs   map[types.NamespacedName]pgshardv1alpha1.BackupSchedules
	// barriers holds the barrier entry and cron expression per policy.
	barriers map[types.NamespacedName]barrierEntry
}

type barrierEntry struct {
	id   cron.EntryID
	expr string
}

// NewBackupScheduler builds a scheduler that creates backups through cl.
func NewBackupScheduler(cl client.Client) *BackupScheduler {
	return &BackupScheduler{client: cl, cron: cron.New(cron.WithParser(cronParser), cron.WithLocation(time.UTC)), now: time.Now,
		entries: map[types.NamespacedName][]cron.EntryID{}, specs: map[types.NamespacedName]pgshardv1alpha1.BackupSchedules{},
		barriers: map[types.NamespacedName]barrierEntry{}}
}

// SetBarrier arms (or, with "", disarms) the barrier schedule of a policy;
// an unchanged expression keeps its entry.
func (s *BackupScheduler) SetBarrier(key types.NamespacedName, expr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.barriers[key]; ok && cur.expr == expr {
		return nil
	}
	s.removeBarrierLocked(key)
	if expr == "" {
		return nil
	}
	id, err := s.cron.AddFunc(expr, func() { s.fireBarrier(key) })
	if err != nil {
		return fmt.Errorf("barrierSchedule %q: %w", expr, err)
	}
	s.barriers[key] = barrierEntry{id: id, expr: expr}
	return nil
}

func (s *BackupScheduler) removeBarrierLocked(key types.NamespacedName) {
	if e, ok := s.barriers[key]; ok {
		s.cron.Remove(e.id)
		delete(s.barriers, key)
	}
}

// BarrierArmed reports whether a policy has a barrier schedule armed.
func (s *BackupScheduler) BarrierArmed(key types.NamespacedName) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.barriers[key]
	return ok
}

func (s *BackupScheduler) fireBarrier(key types.NamespacedName) {
	ctx, cancel := context.WithTimeout(context.Background(), barrierRPCTimeout+30*time.Second)
	defer cancel()
	if err := s.FireBarrier(ctx, key); err != nil {
		logf.Log.WithName("backup-scheduler").Error(err, "scheduled barrier failed", "policy", key.String())
	}
}

// FireBarrier asks the controller of every cluster bound to the policy for
// a barrier named after the tick. Clusters are taken one after another so
// their write pauses never overlap; one failure does not stop the others.
func (s *BackupScheduler) FireBarrier(ctx context.Context, key types.NamespacedName) error {
	if s.Barriers == nil {
		return errors.New("no barrier client configured")
	}
	var pol pgshardv1alpha1.PgShardBackupPolicy
	if err := s.client.Get(ctx, key, &pol); err != nil {
		return client.IgnoreNotFound(err)
	}
	clusters, err := clustersOfPolicy(ctx, s.client, key)
	if err != nil {
		return err
	}
	at := s.now()
	var errs []error
	for i := range clusters {
		c := &clusters[i]
		addr := ControllerEndpoint(pol.Spec.ControllerEndpoint, c.Name, c.Namespace)
		name := ScheduledBarrierName(key.Name, c.Name, at)
		if err := s.Barriers.CreateBarrier(ctx, addr, name); err != nil {
			errs = append(errs, fmt.Errorf("cluster %s (%s): barrier %s: %w", c.Name, addr, name, err))
			continue
		}
		logf.Log.WithName("backup-scheduler").Info("certified barrier recorded", "policy", key.String(), "cluster", c.Name, "barrier", name)
	}
	return errors.Join(errs...)
}

// Start runs the cron loop until ctx ends.
func (s *BackupScheduler) Start(ctx context.Context) error {
	s.cron.Start()
	<-ctx.Done()
	<-s.cron.Stop().Done()
	return nil
}

// NeedLeaderElection keeps one scheduler per operator deployment.
func (s *BackupScheduler) NeedLeaderElection() bool { return true }

// Set replaces the schedules of a policy; unchanged schedules keep their
// entries so a reconcile does not reset the next fire time.
func (s *BackupScheduler) Set(key types.NamespacedName, schedules pgshardv1alpha1.BackupSchedules) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.specs[key]; ok && cur == schedules {
		return nil
	}
	s.removeLocked(key)
	types := scheduleTypes(schedules)
	names := make([]string, 0, len(types))
	for t := range types {
		names = append(names, t)
	}
	sort.Strings(names)
	var ids []cron.EntryID
	for _, typ := range names {
		typ := typ
		id, err := s.cron.AddFunc(types[typ], func() { s.fire(key, typ) })
		if err != nil {
			for _, id := range ids {
				s.cron.Remove(id)
			}
			return fmt.Errorf("schedules.%s %q: %w", typ, types[typ], err)
		}
		ids = append(ids, id)
	}
	s.entries[key] = ids
	s.specs[key] = schedules
	return nil
}

// Remove drops every schedule of a policy.
func (s *BackupScheduler) Remove(key types.NamespacedName) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removeLocked(key)
}

func (s *BackupScheduler) removeLocked(key types.NamespacedName) {
	for _, id := range s.entries[key] {
		s.cron.Remove(id)
	}
	delete(s.entries, key)
	delete(s.specs, key)
	s.removeBarrierLocked(key)
}

// Entries reports how many cron entries a policy has armed.
func (s *BackupScheduler) Entries(key types.NamespacedName) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries[key])
}

func (s *BackupScheduler) fire(key types.NamespacedName, typ string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.Fire(ctx, key, typ); err != nil {
		logf.Log.WithName("backup-scheduler").Error(err, "scheduled backup not created", "policy", key.String(), "type", typ)
	}
}

// Fire creates one PgShardBackup per cluster bound to the policy for a
// schedule tick, skipping clusters whose scheduled backup is still pending
// or running.
func (s *BackupScheduler) Fire(ctx context.Context, key types.NamespacedName, typ string) error {
	var pol pgshardv1alpha1.PgShardBackupPolicy
	if err := s.client.Get(ctx, key, &pol); err != nil {
		return client.IgnoreNotFound(err)
	}
	clusters, err := clustersOfPolicy(ctx, s.client, key)
	if err != nil {
		return err
	}
	var backups pgshardv1alpha1.PgShardBackupList
	if err := s.client.List(ctx, &backups, client.InNamespace(key.Namespace), client.MatchingLabels{LabelBackupPolicy: key.Name}); err != nil {
		return err
	}
	inProgress := map[string]string{}
	for _, b := range backups.Items {
		if b.Status.Phase == "" || b.Status.Phase == pgshardv1alpha1.BackupPhasePending || b.Status.Phase == pgshardv1alpha1.BackupPhaseRunning {
			inProgress[b.Spec.ClusterName] = b.Name
		}
	}
	var errs []error
	for i := range clusters {
		c := &clusters[i]
		if name, busy := inProgress[c.Name]; busy {
			logf.Log.WithName("backup-scheduler").Info("skipping tick; a backup is still in progress", "policy", key.String(), "cluster", c.Name, "inProgress", name)
			continue
		}
		b := &pgshardv1alpha1.PgShardBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ScheduledBackupName(key.Name, c.Name, typ, s.now()),
				Namespace: key.Namespace,
				Labels:    map[string]string{LabelBackupPolicy: key.Name, LabelBackupType: typ, LabelCluster: c.Name},
			},
			Spec: pgshardv1alpha1.PgShardBackupSpec{ClusterName: c.Name, Type: typ},
		}
		if err := controllerutil.SetControllerReference(&pol, b, s.client.Scheme()); err != nil {
			return err
		}
		if err := s.client.Create(ctx, b); err != nil && !apierrors.IsAlreadyExists(err) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// ScheduledBackupName names a scheduled backup after its policy, cluster,
// type and tick.
func ScheduledBackupName(policy, cluster, typ string, at time.Time) string {
	short := map[string]string{"full": "full", "differential": "diff", "incremental": "incr"}[typ]
	return fmt.Sprintf("%s-%s-%s-%s", policy, cluster, short, at.UTC().Format("20060102-1504"))
}
