package operator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/placement"
)

// LabelReshardSource on a PgShardReshard tells whether spec.shards or a
// catalog edit created the pending shard set; only spec-sourced runs are
// cancelled by reverting spec.shards.
const (
	LabelReshardSource   = "pgshard.io/reshard-source"
	ReshardSourceSpec    = "spec"
	ReshardSourceCatalog = "catalog"
)

// ReshardName names the PgShardReshard record of one target generation.
func ReshardName(cluster string, generation int64) string {
	return fmt.Sprintf("%s-reshard-g%d", cluster, generation)
}

// reshardPlan is what reconcileReshard decided for this pass.
type reshardPlan struct {
	cond       metav1.Condition
	placements []pgshardv1alpha1.ClusterPlacementWorkflowStatus
	pending    *ShardSetInfo
	workflow   WorkflowInfo
	record     *pgshardv1alpha1.PgShardReshard
}

// reconcileReshard keeps the catalog's shard sets, the cluster spec and the
// PgShardReshard record in step. It materializes the serving set on first
// contact, turns a spec.shards change into a pending set, adopts pending
// sets created by SQL, and cancels a spec-sourced run when spec.shards is
// reverted while targets are still provisioning. status.effectiveShards and
// status.reshard are patched before returning so the group loop sees them.
// cancellableOnRevert reports whether a reshard in this phase can still be
// undone by lowering spec.shards back to the serving count.
//
// Everything up to the journal is undoable by design; the journal is a step
// of the switch, so Switching is the first phase that is not. Verifying is
// in the list because that is the phase a run reports while it waits at an
// opt-in pause before the write switch -- leaving it out meant a reshard
// paused before switchWrites could not be cancelled by reverting
// spec.shards, which is the one thing that pause exists to allow.
//
// Being before the switch is also what makes the DropShardSet that follows
// safe: it deletes the set's pgshard.shard_status rows, and those rows are
// how the resolver finds shards to search for prepared transactions. A set
// that never took client writes holds none, so removing them hides
// nothing. Past the switch the old set is retired rather than dropped --
// its rows stay, and stay visible to the resolver.
func cancellableOnRevert(phase string) bool {
	switch phase {
	case pgshardv1alpha1.ReshardPhasePending, pgshardv1alpha1.ReshardPhaseProvisioning,
		pgshardv1alpha1.ReshardPhaseCopying, pgshardv1alpha1.ReshardPhaseVerifying:
		return true
	}
	return false
}

func (r *ClusterReconciler) reconcileReshard(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, dsn string) (reshardPlan, error) {
	log := logf.FromContext(ctx)
	plan := reshardPlan{cond: metav1.Condition{Type: pgshardv1alpha1.ConditionResharding, Status: metav1.ConditionFalse, Reason: "Idle", Message: "", ObservedGeneration: c.Generation}}
	base := c.DeepCopy()
	placements, err := r.Prober.PlacementWorkflows(ctx, dsn)
	if err != nil {
		return plan, fmt.Errorf("placement workflows: %w", err)
	}
	for _, w := range placements {
		plan.placements = append(plan.placements, pgshardv1alpha1.ClusterPlacementWorkflowStatus{
			WorkflowID: w.ID, Table: w.Table, From: w.From, To: w.To, State: w.State, Phase: w.Stage, Message: w.Message, PauseMS: w.PauseMS})
	}
	sets, err := r.Prober.ShardSets(ctx, dsn)
	if err != nil {
		return plan, fmt.Errorf("shard sets: %w", err)
	}
	var serving, pending, retired *ShardSetInfo
	var maxGen int64
	for i := range sets {
		s := &sets[i]
		maxGen = max(maxGen, s.Generation)
		switch s.State {
		case catalog.ShardSetServing:
			if serving == nil || s.Generation > serving.Generation {
				serving = s
			}
		case catalog.ShardSetDesired, catalog.ShardSetProvisioning:
			if pending == nil {
				pending = s
			}
		case catalog.ShardSetRetired:
			if retired == nil || s.Generation > retired.Generation {
				retired = s
			}
		}
	}
	if serving == nil || len(serving.Ranges) == 0 {
		n := ServingShards(c)
		ranges, err := placement.Split(n)
		if err != nil {
			return plan, err
		}
		gen := int64(1)
		if serving != nil {
			gen = serving.Generation
		}
		if err := r.Prober.MaterializeShardSet(ctx, dsn, catalog.ShardSetName(gen), gen, catalog.ShardSetServing, ranges, c.Spec.PostgreSQL.Major); err != nil {
			return plan, fmt.Errorf("materialize serving shard set: %w", err)
		}
		if err := r.Prober.SetShardSetMajor(ctx, dsn, catalog.ShardSetName(gen), c.Spec.PostgreSQL.Major); err != nil {
			return plan, fmt.Errorf("stamp serving shard set major: %w", err)
		}
		log.Info("materialized serving shard set", "shards", n, "major", c.Spec.PostgreSQL.Major)
		serving = &ShardSetInfo{Name: catalog.ShardSetName(gen), Generation: gen, State: catalog.ShardSetServing, Ranges: ranges, PGMajor: c.Spec.PostgreSQL.Major}
		maxGen = max(maxGen, gen)
	}
	effective := len(serving.Ranges)
	c.Status.EffectiveShards = effective
	c.Status.ServingGeneration = serving.Generation
	// Captured while the spec still describes this set. Once the spec
	// names a newer major the serving set has not reached, this keeps the
	// image those groups were actually built with.
	if serving.PGMajor == c.Spec.PostgreSQL.Major || c.Status.ServingPGImage == "" {
		c.Status.ServingPGImage = Image(c)
	}
	c.Status.ServingPGMajor = serving.PGMajor
	want := c.Spec.Shards

	if pending == nil && retired != nil {
		done, err := r.reconcileRetirement(ctx, c, base, dsn, serving, retired, &plan)
		if err != nil || !done {
			return plan, err
		}
	}
	if pending == nil && want != nil && *want != effective {
		gen := maxGen + 1
		ranges, err := placement.Split(*want)
		if err != nil {
			return plan, err
		}
		name := catalog.ShardSetName(gen)
		if err := r.Prober.MaterializeShardSet(ctx, dsn, name, gen, catalog.ShardSetDesired, ranges, serving.PGMajor); err != nil {
			return plan, fmt.Errorf("materialize pending shard set: %w", err)
		}
		if serving.PGMajor != 0 {
			if err := r.Prober.SetShardSetMajor(ctx, dsn, name, serving.PGMajor); err != nil {
				return plan, fmt.Errorf("stamp pending shard set major: %w", err)
			}
		}
		log.Info("materialized pending shard set", "set", name, "shards", *want)
		pending = &ShardSetInfo{Name: name, Generation: gen, State: catalog.ShardSetDesired, Ranges: ranges, PGMajor: serving.PGMajor}
		if err := r.ensureReshardRecord(ctx, c, serving, pending, ReshardSourceSpec); err != nil {
			return plan, err
		}
	}

	if pending == nil && (want == nil || *want == effective) && UpgradeRequested(c, serving) {
		if blockers := UpgradeBlockers(c, nil, plan.placements); len(blockers) > 0 {
			plan.cond.Status = metav1.ConditionTrue
			plan.cond.Reason = "UpgradeBlocked"
			plan.cond.Message = fmt.Sprintf("upgrade to major %d blocked: %s", c.Spec.PostgreSQL.Major, strings.Join(blockers, "; "))
			log.Info("upgrade blocked", "blockers", blockers)
		} else {
			gen := maxGen + 1
			name := catalog.ShardSetName(gen)
			if err := r.Prober.MaterializeShardSet(ctx, dsn, name, gen, catalog.ShardSetDesired, serving.Ranges, c.Spec.PostgreSQL.Major); err != nil {
				return plan, fmt.Errorf("materialize upgrade shard set: %w", err)
			}
			if err := r.Prober.SetShardSetMajor(ctx, dsn, name, c.Spec.PostgreSQL.Major); err != nil {
				return plan, fmt.Errorf("stamp upgrade shard set major: %w", err)
			}
			log.Info("materialized upgrade shard set", "set", name, "from", serving.PGMajor, "to", c.Spec.PostgreSQL.Major)
			pending = &ShardSetInfo{Name: name, Generation: gen, State: catalog.ShardSetDesired, Ranges: serving.Ranges, PGMajor: c.Spec.PostgreSQL.Major}
			if err := r.ensureReshardRecord(ctx, c, serving, pending, ReshardSourceSpec); err != nil {
				return plan, err
			}
		}
	}

	if pending == nil {
		if prev := c.Status.Reshard; prev != nil && prev.ShardSet != serving.Name {
			log.Info("pending shard set vanished from the catalog; tearing targets down", "set", prev.ShardSet)
			if err := r.deleteTargetGroups(ctx, c, prev.ShardSet); err != nil {
				return plan, err
			}
			record := &pgshardv1alpha1.PgShardReshard{}
			if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: prev.Name}, record); err == nil {
				if err := r.patchReshardStatus(ctx, record, func(st *pgshardv1alpha1.PgShardReshardStatus) {
					st.Phase = pgshardv1alpha1.ReshardPhaseCancelled
					st.Message = "shard set " + prev.ShardSet + " removed from the catalog; target groups deleted"
					st.Targets = nil
				}); err != nil {
					return plan, err
				}
			} else if !apierrors.IsNotFound(err) {
				return plan, err
			}
			plan.cond.Reason = "Cancelled"
			plan.cond.Message = "reshard " + prev.Name + " cancelled: shard set removed"
		}
		c.Status.Reshard = nil
		return plan, r.patchClusterStatus(ctx, c, base)
	}

	if err := r.ensureReshardRecord(ctx, c, serving, pending, ReshardSourceCatalog); err != nil {
		return plan, err
	}
	record := &pgshardv1alpha1.PgShardReshard{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: ReshardName(c.Name, pending.Generation)}, record); err != nil {
		return plan, err
	}
	wf, err := r.mirrorCutoverSpec(ctx, c, dsn, pending.Name, record)
	if err != nil {
		return plan, err
	}
	phase := reshardPhase(wf)
	specSourced := record.Labels[LabelReshardSource] == ReshardSourceSpec
	// An upgrade run keeps the shard count, so spec.shards matching the
	// serving count is its normal state, not a revert; upgrades are undone
	// by the rollback annotation (or lowering spec.postgresql.major before
	// provisioning finished, which drops the pending set itself).
	revert := want != nil && *want == effective && specSourced && record.Spec.Mode != pgshardv1alpha1.ReshardModeUpgrade

	switch {
	case revert && cancellableOnRevert(phase):
		log.Info("cancelling reshard: spec.shards reverted", "set", pending.Name)
		if err := r.Prober.DropShardSet(ctx, dsn, pending.Name); err != nil {
			return plan, fmt.Errorf("drop pending shard set: %w", err)
		}
		if err := r.deleteTargetGroups(ctx, c, pending.Name); err != nil {
			return plan, err
		}
		if err := r.patchReshardStatus(ctx, record, func(st *pgshardv1alpha1.PgShardReshardStatus) {
			st.Phase = pgshardv1alpha1.ReshardPhaseCancelled
			st.Message = "spec.shards reverted to the serving shard count; target groups deleted"
			st.Targets = nil
		}); err != nil {
			return plan, err
		}
		c.Status.Reshard = nil
		plan.cond.Reason = "Cancelled"
		plan.cond.Message = "reshard " + record.Name + " cancelled"
		return plan, r.patchClusterStatus(ctx, c, base)
	case want != nil && *want != effective && *want != len(pending.Ranges):
		plan.cond.Status = metav1.ConditionTrue
		plan.cond.Reason = "ReshardActive"
		plan.cond.Message = fmt.Sprintf("spec.shards=%d refused: reshard %s to %d shards is %s; wait for it or revert spec.shards to %d",
			*want, record.Name, len(pending.Ranges), phase, effective)
	default:
		plan.cond.Status = metav1.ConditionTrue
		plan.cond.Reason = phase
		plan.cond.Message = fmt.Sprintf("reshard %s: %d -> %d shards", record.Name, effective, len(pending.Ranges))
	}
	c.Status.Reshard = &pgshardv1alpha1.ClusterReshardStatus{
		Name: record.Name, ShardSet: pending.Name, Generation: pending.Generation, Shards: len(pending.Ranges), Phase: phase,
		PGMajor: pending.PGMajor, PGImage: generationImage(c, pending.PGMajor),
	}
	plan.pending, plan.workflow, plan.record = pending, wf, record
	return plan, r.patchClusterStatus(ctx, c, base)
}

// generationImage is the image a set of the given major runs: the spec's
// when the spec describes that major, and otherwise whatever was captured
// for the set already serving. A cluster that never carried a custom image
// gets an empty string and ImageFor falls back to the public default, as
// it always did.
func generationImage(c *pgshardv1alpha1.PgShardCluster, major int) string {
	if major == 0 || major == c.Spec.PostgreSQL.Major {
		return Image(c)
	}
	if c.Status.ServingPGImage != "" {
		return c.Status.ServingPGImage
	}
	return c.Status.CatalogPGImage
}

func (r *ClusterReconciler) patchClusterStatus(ctx context.Context, c, base *pgshardv1alpha1.PgShardCluster) error {
	if c.Status.EffectiveShards == base.Status.EffectiveShards && c.Status.ServingGeneration == base.Status.ServingGeneration &&
		c.Status.ServingPGMajor == base.Status.ServingPGMajor && c.Status.ServingPGImage == base.Status.ServingPGImage &&
		equalReshard(c.Status.Reshard, base.Status.Reshard) {
		return nil
	}
	return r.Status().Patch(ctx, c, client.MergeFrom(base))
}

func equalReshard(a, b *pgshardv1alpha1.ClusterReshardStatus) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// mirrorCutoverSpec reads the workflow of set and, when it exists, writes
// spec.resharding and the record's proceed annotation into its spec.
func (r *ClusterReconciler) mirrorCutoverSpec(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, dsn, set string, record *pgshardv1alpha1.PgShardReshard) (WorkflowInfo, error) {
	wf, err := r.Prober.ReshardWorkflow(ctx, dsn, set)
	if err != nil {
		return wf, fmt.Errorf("reshard workflow: %w", err)
	}
	if wf.ID == "" {
		return wf, nil
	}
	var proceed []string
	for _, p := range strings.Split(record.Annotations[pgshardv1alpha1.AnnotationProceed], ",") {
		if p = strings.TrimSpace(p); p != "" {
			proceed = append(proceed, p)
		}
	}
	var retire int64
	if d := c.Spec.Resharding.RetireOldGroupsAfter; d != nil {
		retire = int64(d.Seconds())
	}
	if err := r.Prober.SetReshardCutoverSpec(ctx, dsn, wf.ID, c.Spec.Resharding.PauseBefore, proceed, retire); err != nil {
		return wf, fmt.Errorf("mirror cutover spec: %w", err)
	}
	if record.Spec.Mode == pgshardv1alpha1.ReshardModeUpgrade && record.Annotations[pgshardv1alpha1.AnnotationUpgrade] == pgshardv1alpha1.UpgradeActionRollback {
		if err := r.Prober.SetWorkflowRollback(ctx, dsn, wf.ID); err != nil {
			return wf, fmt.Errorf("mirror upgrade rollback: %w", err)
		}
	}
	return wf, nil
}

// reconcileRetirement handles the window after the write switch: the old
// set is retired in the catalog and its groups stay up for reverse
// replication until the workflow completes, then they are deleted. It
// reports done=false when the pass is fully handled here.
func (r *ClusterReconciler) reconcileRetirement(ctx context.Context, c, base *pgshardv1alpha1.PgShardCluster, dsn string, serving, retired *ShardSetInfo, plan *reshardPlan) (bool, error) {
	log := logf.FromContext(ctx)
	record := &pgshardv1alpha1.PgShardReshard{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: ReshardName(c.Name, serving.Generation)}, record); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	if record.Status.Phase == pgshardv1alpha1.ReshardPhaseCompleted {
		return true, nil
	}
	wf, err := r.mirrorCutoverSpec(ctx, c, dsn, serving.Name, record)
	if err != nil {
		return false, err
	}
	phase := reshardPhase(wf)
	if phase == pgshardv1alpha1.ReshardPhaseCompleted {
		log.Info("reshard completed; deleting retired groups", "set", retired.Name)
		if err := r.deleteTargetGroups(ctx, c, retired.Name); err != nil {
			return false, err
		}
		if err := r.patchReshardStatus(ctx, record, func(st *pgshardv1alpha1.PgShardReshardStatus) {
			st.Phase = phase
			st.WorkflowID = wf.ID
			st.CutoverPause = cutoverPause(wf)
			st.Message = "completed: old groups of " + retired.Name + " deleted"
			meta.SetStatusCondition(&st.Conditions, metav1.Condition{Type: pgshardv1alpha1.ReshardConditionSwitched, Status: metav1.ConditionTrue, Reason: "Completed", Message: wf.Message, ObservedGeneration: record.Generation})
		}); err != nil {
			return false, err
		}
		c.Status.Reshard = nil
		plan.cond.Reason = "Completed"
		plan.cond.Message = "reshard " + record.Name + " completed"
		return false, r.patchClusterStatus(ctx, c, base)
	}
	plan.cond.Status = metav1.ConditionTrue
	plan.cond.Reason = phase
	plan.cond.Message = fmt.Sprintf("reshard %s: writes switched to %s; %s retiring", record.Name, serving.Name, retired.Name)
	c.Status.Reshard = &pgshardv1alpha1.ClusterReshardStatus{
		Name: record.Name, ShardSet: serving.Name, Generation: serving.Generation, Shards: len(serving.Ranges), Phase: phase,
		PGMajor: serving.PGMajor, PGImage: generationImage(c, serving.PGMajor),
		RetiredShardSet: retired.Name, RetiredGeneration: retired.Generation, RetiredShards: len(retired.Ranges),
		RetiredPGMajor: retired.PGMajor, RetiredPGImage: generationImage(c, retired.PGMajor),
	}
	plan.workflow, plan.record = wf, record
	return false, r.patchClusterStatus(ctx, c, base)
}

func cutoverPause(wf WorkflowInfo) *metav1.Duration {
	if wf.CutoverPauseMS <= 0 {
		return nil
	}
	return &metav1.Duration{Duration: time.Duration(wf.CutoverPauseMS) * time.Millisecond}
}

// reshardPhase maps the catalog workflow onto the record's phase.
func reshardPhase(wf WorkflowInfo) string {
	switch {
	case wf.ID == "":
		return pgshardv1alpha1.ReshardPhasePending
	case wf.State == "provisioning":
		return pgshardv1alpha1.ReshardPhaseProvisioning
	case wf.State == "cancelled":
		return pgshardv1alpha1.ReshardPhaseCancelled
	case wf.State == "failed":
		return pgshardv1alpha1.ReshardPhaseFailed
	case wf.State == "completed":
		return pgshardv1alpha1.ReshardPhaseCompleted
	case wf.Stage == "ready_for_copy", wf.Stage == "copying", wf.Stage == "catch_up_done":
		return pgshardv1alpha1.ReshardPhaseCopying
	case wf.Stage == "awaiting_switch_writes":
		return pgshardv1alpha1.ReshardPhaseVerifying
	case wf.Stage == "switching", wf.Stage == "rolling_back":
		// A rollback is a switch being undone, so the set is live and the
		// phase must not be one that lets it be dropped. Unmapped, this
		// stage fell through to Provisioning, which is droppable.
		return pgshardv1alpha1.ReshardPhaseSwitching
	case wf.Stage == "switched", wf.Stage == "completing":
		return pgshardv1alpha1.ReshardPhaseCompleting
	}
	return pgshardv1alpha1.ReshardPhaseProvisioning
}

func (r *ClusterReconciler) ensureReshardRecord(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, serving, pending *ShardSetInfo, source string) error {
	ranges := make([]pgshardv1alpha1.ReshardRange, 0, len(pending.Ranges))
	for i, rg := range pending.Ranges {
		ranges = append(ranges, pgshardv1alpha1.ReshardRange{ShardID: i, RangeStart: rg.Start, RangeEnd: rg.End})
	}
	rec := &pgshardv1alpha1.PgShardReshard{ObjectMeta: metav1.ObjectMeta{
		Name: ReshardName(c.Name, pending.Generation), Namespace: c.Namespace,
		Labels: map[string]string{LabelCluster: c.Name, LabelShardSet: pending.Name, LabelReshardSource: source},
	}}
	rec.Spec = pgshardv1alpha1.PgShardReshardSpec{
		ClusterName: c.Name, FromGeneration: serving.Generation, TargetGeneration: pending.Generation,
		TargetShardSet: pending.Name, TargetShards: len(pending.Ranges), TargetRanges: ranges,
		Mode: pgshardv1alpha1.ReshardModeReshard,
	}
	if pending.PGMajor != 0 && pending.PGMajor != serving.PGMajor {
		rec.Spec.Mode = pgshardv1alpha1.ReshardModeUpgrade
		rec.Spec.TargetMajor = pending.PGMajor
	}
	if err := controllerutil.SetControllerReference(c, rec, r.Scheme()); err != nil {
		return err
	}
	err := r.Create(ctx, rec)
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (r *ClusterReconciler) patchReshardStatus(ctx context.Context, rec *pgshardv1alpha1.PgShardReshard, mutate func(*pgshardv1alpha1.PgShardReshardStatus)) error {
	base := rec.DeepCopy()
	mutate(&rec.Status)
	return r.Status().Patch(ctx, rec, client.MergeFrom(base))
}

// updateReshardStatus reports target readiness and the workflow on the record.
func (r *ClusterReconciler) updateReshardStatus(ctx context.Context, plan reshardPlan, targets []groupObservation) error {
	if plan.record == nil {
		return nil
	}
	return r.patchReshardStatus(ctx, plan.record, func(st *pgshardv1alpha1.PgShardReshardStatus) {
		st.Phase = reshardPhase(plan.workflow)
		st.WorkflowID = plan.workflow.ID
		st.JournalIDs = plan.workflow.JournalIDs
		st.CutoverPause = cutoverPause(plan.workflow)
		if plan.pending == nil {
			meta.SetStatusCondition(&st.Conditions, metav1.Condition{Type: pgshardv1alpha1.ReshardConditionSwitched, Status: metav1.ConditionTrue, Reason: st.Phase, Message: plan.workflow.Message, ObservedGeneration: plan.record.Generation})
			st.Message = st.Phase + ": " + plan.workflow.Message
			return
		}
		st.Targets = st.Targets[:0]
		ready := 0
		for _, o := range targets {
			ok := o.ready()
			if ok {
				ready++
			}
			st.Targets = append(st.Targets, pgshardv1alpha1.ReshardTargetStatus{ShardID: o.group.ShardID, Group: o.group.Name(), Ready: ok, Primary: o.state.primary})
		}
		sort.Slice(st.Targets, func(i, j int) bool { return st.Targets[i].ShardID < st.Targets[j].ShardID })
		allReady := ready == len(targets) && len(targets) > 0
		set := func(t string, ok bool, reason, msg string) {
			s := metav1.ConditionFalse
			if ok {
				s = metav1.ConditionTrue
			}
			meta.SetStatusCondition(&st.Conditions, metav1.Condition{Type: t, Status: s, Reason: reason, Message: msg, ObservedGeneration: plan.record.Generation})
		}
		set(pgshardv1alpha1.ReshardConditionTargetsReady, allReady, boolReason(allReady, "AllReady", "Provisioning"), fmt.Sprintf("%d/%d target groups ready", ready, len(targets)))
		set(pgshardv1alpha1.ReshardConditionWorkflow, plan.workflow.ID != "", boolReason(plan.workflow.ID != "", "Created", "Waiting"), plan.workflow.ID)
		st.Message = fmt.Sprintf("%s: %d/%d target groups ready", st.Phase, ready, len(targets))
		if plan.workflow.Stage != "" {
			st.Message += "; workflow stage " + plan.workflow.Stage
		}
		if plan.workflow.Message != "" {
			st.Message += "; " + plan.workflow.Message
		}
	})
}

// deleteTargetGroups removes every object of the target groups of set.
func (r *ClusterReconciler) deleteTargetGroups(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, set string) error {
	sel := client.MatchingLabels{LabelCluster: c.Name, LabelShardSet: set}
	ns := client.InNamespace(c.Namespace)
	for _, obj := range []client.Object{&corev1.Pod{}, &corev1.PersistentVolumeClaim{}, &corev1.Service{}, &corev1.ConfigMap{},
		&policyv1.PodDisruptionBudget{}, &pgshardv1alpha1.PgShardGroup{}} {
		if err := r.DeleteAllOf(ctx, obj, ns, sel); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete targets of %s: %w", set, err)
		}
	}
	// Leases are created by the agents without the group labels.
	for _, g := range TargetGroups(c) {
		lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: g.LeaseName(), Namespace: c.Namespace}}
		if err := r.Delete(ctx, lease); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
