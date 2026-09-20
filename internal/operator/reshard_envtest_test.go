package operator

import (
	"context"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/controller"
	"github.com/andrew01234567890/pgshard/internal/placement"
)

// bringUp drives a fresh cluster to Ready with the catalog migrated.
func bringUp(t *testing.T, r *ClusterReconciler, fp *fakeProber, c *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	reconcile(t, r, c)
	markPodsRunning(t, c)
	fp.err = nil
	fp.streaming = map[string]bool{}
	for _, g := range Groups(c) {
		for i := 1; i < g.Replicas; i++ {
			fp.streaming[g.MemberName(i)] = true
		}
	}
	reconcile(t, r, c)
	if cond := condition(t, c.Name, ConditionCatalogReady); cond.Status != metav1.ConditionTrue {
		t.Fatalf("CatalogReady: %+v", cond)
	}
}

func markTargetsRunning(t *testing.T, fp *fakeProber, c *pgshardv1alpha1.PgShardCluster) {
	t.Helper()
	for gi, g := range TargetGroups(c) {
		for i := 0; i < g.Replicas; i++ {
			markPodRunning(t, g.MemberName(i), podIP(10+gi, i))
			if i > 0 {
				fp.mu.Lock()
				fp.streaming[g.MemberName(i)] = true
				fp.mu.Unlock()
			}
		}
	}
}

func TestReshardProvisionsNonServingTargets(t *testing.T) {
	r, fp, c := setup(t, "rs")
	bringUp(t, r, fp, c)

	def, ok := fp.shardSet(catalog.DefaultShardSet)
	if !ok || def.State != catalog.ShardSetServing || len(def.Ranges) != 1 || def.Ranges[0].Start != math.MinInt64 || def.Ranges[0].End != math.MaxInt64 {
		t.Fatalf("serving set must be materialized as one full range: %+v", def)
	}
	get(t, "rs", c)
	if c.Status.EffectiveShards != 1 || c.Status.Reshard != nil {
		t.Fatalf("status after bring-up: effective=%d reshard=%+v", c.Status.EffectiveShards, c.Status.Reshard)
	}

	two := 2
	c.Spec.Shards = &two
	if err := k8sClient.Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, c)

	pending, ok := fp.shardSet("g2")
	if !ok || pending.State != catalog.ShardSetDesired || len(pending.Ranges) != 2 || pending.Ranges[0].End+1 != pending.Ranges[1].Start {
		t.Fatalf("pending set g2: %+v", pending)
	}
	var rec pgshardv1alpha1.PgShardReshard
	get(t, "rs-reshard-g2", &rec)
	ownedBy(t, &rec, c)
	if rec.Spec.FromGeneration != 1 || rec.Spec.TargetGeneration != 2 || rec.Spec.TargetShards != 2 || rec.Spec.TargetShardSet != "g2" ||
		len(rec.Spec.TargetRanges) != 2 || rec.Labels[LabelReshardSource] != ReshardSourceSpec {
		t.Fatalf("record spec: %+v labels=%v", rec.Spec, rec.Labels)
	}
	if rec.Status.Phase != pgshardv1alpha1.ReshardPhasePending || len(rec.Status.Targets) != 2 || rec.Status.Targets[0].Ready {
		t.Fatalf("record status: %+v", rec.Status)
	}
	get(t, "rs", c)
	if c.Status.EffectiveShards != 1 || c.Status.Reshard == nil || c.Status.Reshard.Shards != 2 || c.Status.Reshard.ShardSet != "g2" {
		t.Fatalf("cluster reshard status: %+v", c.Status.Reshard)
	}
	if cond := condition(t, "rs", pgshardv1alpha1.ConditionResharding); cond.Status != metav1.ConditionTrue {
		t.Fatalf("Resharding condition: %+v", cond)
	}
	for _, name := range []string{"rs-shard-0-g2", "rs-shard-1-g2"} {
		var pg pgshardv1alpha1.PgShardGroup
		get(t, name, &pg)
		if !pg.Spec.NonServing || pg.Spec.ShardSet != "g2" || pg.Spec.Kind != "shard" {
			t.Errorf("target group %s spec: %+v", name, pg.Spec)
		}
		var pod corev1.Pod
		get(t, name+"-0", &pod)
		if pod.Labels[LabelShardSet] != "g2" {
			t.Errorf("target pod labels: %v", pod.Labels)
		}
		var cm corev1.ConfigMap
		get(t, name+"-config", &cm)
		if !strings.Contains(cm.Data[name+"-0.json"], `"nonServing": true`) {
			t.Errorf("target agent config must be non-serving:\n%s", cm.Data[name+"-0.json"])
		}
	}
	var pg pgshardv1alpha1.PgShardGroup
	get(t, "rs-shard-0", &pg)
	if pg.Spec.NonServing || pg.Spec.ShardSet != catalog.DefaultShardSet {
		t.Errorf("serving group must stay serving: %+v", pg.Spec)
	}
	if len(c.Status.Shards) != 1 {
		t.Errorf("status.shards must list serving shards only: %+v", c.Status.Shards)
	}

	fp.mu.Lock()
	fp.workflows = map[string]WorkflowInfo{"g2": {ID: "wf-1", State: "provisioning", Stage: "provisioning"}}
	fp.mu.Unlock()
	fp.setShardSetState("g2", catalog.ShardSetProvisioning)
	markTargetsRunning(t, fp, c)
	reconcile(t, r, c)
	get(t, "rs-reshard-g2", &rec)
	if rec.Status.Phase != pgshardv1alpha1.ReshardPhaseProvisioning || rec.Status.WorkflowID != "wf-1" {
		t.Fatalf("record after workflow: %+v", rec.Status)
	}
	if cond := meta.FindStatusCondition(rec.Status.Conditions, pgshardv1alpha1.ReshardConditionTargetsReady); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("TargetsReady: %+v targets=%+v", cond, rec.Status.Targets)
	}
	fp.mu.Lock()
	servingPublished := fp.endpoints["default/shard-0"]
	targetPublished := fp.endpoints["g2/shard-0-g2"]
	fp.mu.Unlock()
	if servingPublished == "" || !strings.HasPrefix(targetPublished, "rs-shard-0-g2-0.") {
		t.Fatalf("published endpoints: %v", fp.endpoints)
	}

	three := 3
	get(t, "rs", c)
	c.Spec.Shards = &three
	if err := k8sClient.Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, c)
	if cond := condition(t, "rs", pgshardv1alpha1.ConditionResharding); cond.Reason != "ReshardActive" || !strings.Contains(cond.Message, "refused") {
		t.Fatalf("second change must be refused: %+v", cond)
	}
	if _, ok := fp.shardSet("g3"); ok {
		t.Fatal("a refused change must not materialize another set")
	}

	one := 1
	get(t, "rs", c)
	c.Spec.Shards = &one
	if err := k8sClient.Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, c)
	if _, ok := fp.shardSet("g2"); ok {
		t.Fatal("cancel must drop the pending set")
	}
	get(t, "rs-reshard-g2", &rec)
	if rec.Status.Phase != pgshardv1alpha1.ReshardPhaseCancelled {
		t.Fatalf("record must be Cancelled: %+v", rec.Status)
	}
	for _, name := range []string{"rs-shard-0-g2", "rs-shard-1-g2"} {
		err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, &pgshardv1alpha1.PgShardGroup{})
		if !apierrors.IsNotFound(err) {
			t.Errorf("target group %s must be deleted: %v", name, err)
		}
		var pods corev1.PodList
		if err := k8sClient.List(context.Background(), &pods, client.InNamespace("default"), client.MatchingLabels{LabelGroup: "shard-0-g2"}); err != nil || len(pods.Items) != 0 {
			t.Errorf("target pods must be deleted: %d %v", len(pods.Items), err)
		}
	}
	get(t, "rs", c)
	if c.Status.Reshard != nil || c.Status.EffectiveShards != 1 {
		t.Fatalf("cluster status after cancel: %+v", c.Status)
	}
	get(t, "rs-shard-0", &pg)
	if pg.Spec.NonServing {
		t.Error("serving group untouched by cancel")
	}
}

func TestReshardAdoptsCatalogEditedShardSet(t *testing.T) {
	r, fp, c := setup(t, "rsql")
	bringUp(t, r, fp, c)
	def, _ := fp.shardSet(catalog.DefaultShardSet)
	split := def.Ranges
	fp.mu.Lock()
	fp.shardSets = append(fp.shardSets, ShardSetInfo{Name: "g2", Generation: 2, State: catalog.ShardSetDesired, Ranges: splitInHalf(split[0])})
	fp.mu.Unlock()
	reconcile(t, r, c)
	var rec pgshardv1alpha1.PgShardReshard
	get(t, "rsql-reshard-g2", &rec)
	if rec.Labels[LabelReshardSource] != ReshardSourceCatalog || rec.Spec.TargetShards != 2 {
		t.Fatalf("record: %+v labels=%v", rec.Spec, rec.Labels)
	}
	get(t, "rsql-shard-1-g2", &pgshardv1alpha1.PgShardGroup{})
	reconcile(t, r, c)
	if _, ok := fp.shardSet("g2"); !ok {
		t.Fatal("a catalog-sourced set must not be cancelled by an unchanged spec.shards")
	}
	get(t, "rsql", c)
	markTargetsRunning(t, fp, c)
	reconcile(t, r, c)
	get(t, "rsql-reshard-g2", &rec)
	if cond := meta.FindStatusCondition(rec.Status.Conditions, pgshardv1alpha1.ReshardConditionTargetsReady); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("TargetsReady: %+v", rec.Status)
	}

	_ = fp.DropShardSet(context.Background(), "", "g2")
	reconcile(t, r, c)
	get(t, "rsql-reshard-g2", &rec)
	if rec.Status.Phase != pgshardv1alpha1.ReshardPhaseCancelled {
		t.Fatalf("deleting the set in SQL must cancel: %+v", rec.Status)
	}
	err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "rsql-shard-1-g2"}, &pgshardv1alpha1.PgShardGroup{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("targets must be deleted: %v", err)
	}
	get(t, "rsql", c)
	if c.Status.Reshard != nil {
		t.Fatalf("status.reshard must clear: %+v", c.Status.Reshard)
	}
}

// splitInHalf splits one range at zero, which is the midpoint of the whole
// int8 keyspace and so the split a 1 -> 2 reshard produces.
func splitInHalf(r placement.Range) placement.RangeSet {
	return placement.RangeSet{{Start: r.Start, End: -1}, {Start: 0, End: r.End}}
}

func TestReshardRetiresOldGroupsAfterSwitch(t *testing.T) {
	r, fp, c := setup(t, "rsw")
	bringUp(t, r, fp, c)
	get(t, "rsw", c)
	two := 2
	c.Spec.Shards = &two
	c.Spec.Resharding.PauseBefore = "switchWrites"
	if err := k8sClient.Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, c)
	fp.mu.Lock()
	fp.workflows = map[string]WorkflowInfo{"g2": {ID: "wf-2", State: "running", Stage: "awaiting_switch_writes"}}
	fp.mu.Unlock()
	fp.setShardSetState("g2", catalog.ShardSetProvisioning)
	markTargetsRunning(t, fp, c)
	reconcile(t, r, c)
	var rec pgshardv1alpha1.PgShardReshard
	get(t, "rsw-reshard-g2", &rec)
	if rec.Status.Phase != pgshardv1alpha1.ReshardPhaseVerifying {
		t.Fatalf("phase at the gate: %+v", rec.Status)
	}
	fp.mu.Lock()
	last := fp.cutoverSpecs[len(fp.cutoverSpecs)-1]
	fp.mu.Unlock()
	if want := "wf-2:switchWrites::" + controller.DefaultRetireAfter.String(); last != want {
		t.Fatalf("mirrored spec: %q, want the retirement window mirrored from controller.DefaultRetireAfter", last)
	}

	// A plain reshard can ask to be rolled back. The controller's rollback
	// names no kind -- it triggers on spec.Rollback at StageSwitched -- but
	// the operator used to mirror the request only for upgrade-mode runs, so
	// an ordinary reshard had the machinery and no handle, leaving a hand
	// edit of pgshard.workflows as the only route during an incident.
	rec.Annotations = map[string]string{pgshardv1alpha1.AnnotationRollback: pgshardv1alpha1.UpgradeActionRollback}
	if err := k8sClient.Update(context.Background(), &rec); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, c)
	fp.mu.Lock()
	rolled := append([]string(nil), fp.rollbacks...)
	fp.mu.Unlock()
	if !slices.Contains(rolled, "wf-2") {
		t.Errorf("a reshard asking for rollback did not reach the workflow; mirrored %v", rolled)
	}

	rec.Annotations = map[string]string{pgshardv1alpha1.AnnotationProceed: "switchWrites, complete"}
	if err := k8sClient.Update(context.Background(), &rec); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, c)
	fp.mu.Lock()
	last = fp.cutoverSpecs[len(fp.cutoverSpecs)-1]
	fp.mu.Unlock()
	if want := "wf-2:switchWrites:switchWrites+complete:" + controller.DefaultRetireAfter.String(); last != want {
		t.Fatalf("proceed annotation must reach the workflow: %q", last)
	}
	// No window at all is mirrored as one, not as the default (PGS-901).
	get(t, "rsw", c)
	c.Spec.Resharding.RetireOldGroupsAfter = &metav1.Duration{}
	if err := k8sClient.Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, c)
	fp.mu.Lock()
	last = fp.cutoverSpecs[len(fp.cutoverSpecs)-1]
	fp.mu.Unlock()
	if last != "wf-2:switchWrites:switchWrites+complete:0s" {
		t.Fatalf("retireOldGroupsAfter: 0 mirrored as %q", last)
	}

	fp.setShardSetState(catalog.DefaultShardSet, catalog.ShardSetRetired)
	fp.setShardSetState("g2", catalog.ShardSetServing)
	fp.mu.Lock()
	fp.workflows["g2"] = WorkflowInfo{ID: "wf-2", State: "running", Stage: "switched", Message: "old groups retire in 24h", CutoverPauseMS: 800}
	fp.mu.Unlock()
	reconcile(t, r, c)
	reconcile(t, r, c)
	get(t, "rsw", c)
	if c.Status.ServingGeneration != 2 || c.Status.EffectiveShards != 2 || c.Status.Reshard == nil ||
		c.Status.Reshard.RetiredShardSet != "default" || c.Status.Reshard.RetiredShards != 1 || c.Status.Reshard.Phase != pgshardv1alpha1.ReshardPhaseCompleting {
		t.Fatalf("cluster status after the switch: gen=%d effective=%d reshard=%+v", c.Status.ServingGeneration, c.Status.EffectiveShards, c.Status.Reshard)
	}
	if got := len(Groups(c)); got != 3 {
		t.Fatalf("serving groups after the switch: %d", got)
	}
	for _, name := range []string{"rsw-shard-0-g2", "rsw-shard-1-g2", "rsw-shard-0"} {
		var pg pgshardv1alpha1.PgShardGroup
		get(t, name, &pg)
		if pg.Spec.NonServing {
			t.Errorf("%s must serve after the switch: %+v", name, pg.Spec)
		}
	}
	get(t, "rsw-reshard-g2", &rec)
	if rec.Status.Phase != pgshardv1alpha1.ReshardPhaseCompleting || rec.Status.CutoverPause == nil || rec.Status.CutoverPause.Duration != 800*time.Millisecond {
		t.Fatalf("record after the switch: %+v", rec.Status)
	}
	if cond := meta.FindStatusCondition(rec.Status.Conditions, pgshardv1alpha1.ReshardConditionSwitched); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("WritesSwitched: %+v", cond)
	}
	if len(c.Status.Shards) != 2 {
		t.Errorf("status.shards must list the new serving shards: %+v", c.Status.Shards)
	}

	// Transactions still open on the old primaries hold the retirement
	// (PGS-927): deleting a primary fast-shuts it down, which ends them.
	fp.mu.Lock()
	fp.workflows["g2"] = WorkflowInfo{ID: "wf-2", State: "completed", Stage: "completed", CutoverPauseMS: 800}
	fp.openTxns = 2
	fp.mu.Unlock()
	now := time.Now()
	r.Now = func() time.Time { return now }
	reconcile(t, r, c)
	reconcile(t, r, c)
	get(t, "rsw-shard-0", &pgshardv1alpha1.PgShardGroup{})
	get(t, "rsw-reshard-g2", &rec)
	if rec.Annotations[annotationRetireDrainStarted] == "" {
		t.Fatal("the drain's start is not recorded, so an operator restart would begin the bound again")
	}
	get(t, "rsw", c)
	if cond := meta.FindStatusCondition(c.Status.Conditions, pgshardv1alpha1.ConditionResharding); cond == nil || cond.Reason != "Draining" {
		t.Fatalf("a drain in progress is not reported: %+v", cond)
	}
	// The bound is a bound: open transactions do not hold the old groups
	// past it.
	now = now.Add(retireDrainBound)
	reconcile(t, r, c)
	reconcile(t, r, c)
	get(t, "rsw-reshard-g2", &rec)
	if rec.Status.Phase != pgshardv1alpha1.ReshardPhaseCompleted || rec.Status.CutoverPause == nil {
		t.Fatalf("record after completion: %+v", rec.Status)
	}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "rsw-shard-0"}, &pgshardv1alpha1.PgShardGroup{}); !apierrors.IsNotFound(err) {
		t.Errorf("retired group must be deleted: %v", err)
	}
	var pods corev1.PodList
	if err := k8sClient.List(context.Background(), &pods, client.InNamespace("default"), client.MatchingLabels{LabelCluster: "rsw", LabelGroup: "shard-0"}); err != nil || len(pods.Items) != 0 {
		t.Errorf("retired pods must be deleted: %d %v", len(pods.Items), err)
	}
	get(t, "rsw", c)
	if c.Status.Reshard != nil || c.Status.ServingGeneration != 2 || c.Status.EffectiveShards != 2 {
		t.Fatalf("cluster status after completion: %+v", c.Status)
	}
	reconcile(t, r, c)
	get(t, "rsw-shard-0-g2", &pgshardv1alpha1.PgShardGroup{})
}

// TestRevertingShardsClearsAFailedReshard (PGS-876): a failed reshard kept
// its target set, and routers refuse DDL while a reshard's set exists, so
// every DDL statement was refused until someone edited the catalog by hand.
// A failed run never served, so reverting spec.shards drops it and deletes
// its groups, as it does for a run still copying.
func TestRevertingShardsClearsAFailedReshard(t *testing.T) {
	r, fp, c := setup(t, "rsf")
	bringUp(t, r, fp, c)
	get(t, "rsf", c)
	two := 2
	c.Spec.Shards = &two
	if err := k8sClient.Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, c)
	fp.mu.Lock()
	fp.workflows = map[string]WorkflowInfo{"g2": {ID: "wf-f", State: "failed", Stage: "failed", Message: "copy of app.public.orders failed"}}
	fp.mu.Unlock()
	fp.setShardSetState("g2", catalog.ShardSetProvisioning)
	reconcile(t, r, c)
	var rec pgshardv1alpha1.PgShardReshard
	get(t, "rsf-reshard-g2", &rec)
	if rec.Status.Phase != pgshardv1alpha1.ReshardPhaseFailed {
		t.Fatalf("record before the revert: %+v", rec.Status)
	}
	if _, ok := fp.shardSet("g2"); !ok {
		t.Fatal("a failed reshard's set went away before anything asked for it")
	}

	one := 1
	get(t, "rsf", c)
	c.Spec.Shards = &one
	if err := k8sClient.Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, c)
	if _, ok := fp.shardSet("g2"); ok {
		t.Fatal("reverting spec.shards left the failed reshard's set in place")
	}
	get(t, "rsf-reshard-g2", &rec)
	if rec.Status.Phase != pgshardv1alpha1.ReshardPhaseCancelled || !strings.Contains(rec.Status.Message, "after the reshard failed (copy of app.public.orders failed)") {
		t.Fatalf("record after the revert: %+v", rec.Status)
	}
	for _, name := range []string{"rsf-shard-0-g2", "rsf-shard-1-g2"} {
		if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, &pgshardv1alpha1.PgShardGroup{}); !apierrors.IsNotFound(err) {
			t.Errorf("target group %s must be deleted: %v", name, err)
		}
	}
	get(t, "rsf", c)
	if c.Status.Reshard != nil || c.Status.EffectiveShards != 1 {
		t.Fatalf("cluster status after the revert: %+v", c.Status)
	}

	// The next reshard gets the next generation. The dropped set left no
	// shard_sets row, and reusing g2 met the cancelled record of that name,
	// which the operator refused to adopt, pass after pass.
	three := 3
	c.Spec.Shards = &three
	if err := k8sClient.Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, c)
	if next, ok := fp.shardSet("g3"); !ok || len(next.Ranges) != 3 {
		t.Fatalf("the next reshard's set: %+v (present %v), want g3 with 3 ranges", next, ok)
	}
	if _, ok := fp.shardSet("g2"); ok {
		t.Fatal("the next reshard reused the dropped generation")
	}
	var next pgshardv1alpha1.PgShardReshard
	get(t, "rsf-reshard-g3", &next)
	if next.Spec.TargetShards != 3 {
		t.Fatalf("the next reshard's record: %+v", next.Spec)
	}
}

// TestARerunAfterARollbackRetiresItsRealSource (PGS-964): an upgrade to g2
// rolled back to default, then run again as g3. When g3 completed, both
// default (its real source) and g2 (the set rolled back from) were retired,
// and retirement took the newest, g2 -- so default's groups stayed up for
// good, because on every later pass g3's record was already Completed.
func TestARerunAfterARollbackRetiresItsRealSource(t *testing.T) {
	r, fp, c := setup(t, "rrb")
	bringUp(t, r, fp, c)
	ctx := context.Background()
	one, err := placement.Split(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := fp.MaterializeShardSet(ctx, "", "g2", 2, catalog.ShardSetRetired, one, 18); err != nil {
		t.Fatal(err)
	}
	if err := fp.MaterializeShardSet(ctx, "", "g3", 3, catalog.ShardSetServing, one, 19); err != nil {
		t.Fatal(err)
	}
	fp.setShardSetState(catalog.DefaultShardSet, catalog.ShardSetRetired)
	// What stands for each set's groups: deleteTargetGroups selects by the
	// shard-set label, and a ConfigMap is one of the kinds it deletes.
	for _, set := range []string{catalog.DefaultShardSet, "g2"} {
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "rrb-marker-" + set, Namespace: "default",
			Labels: map[string]string{LabelCluster: "rrb", LabelShardSet: set}}}
		if err := k8sClient.Create(ctx, cm); err != nil {
			t.Fatal(err)
		}
	}
	rec := &pgshardv1alpha1.PgShardReshard{ObjectMeta: metav1.ObjectMeta{Name: ReshardName("rrb", 3), Namespace: "default",
		Labels: map[string]string{LabelCluster: "rrb"}},
		Spec: pgshardv1alpha1.PgShardReshardSpec{ClusterName: "rrb", FromGeneration: 1, TargetGeneration: 3, TargetShardSet: "g3", TargetShards: 1,
			TargetRanges: []pgshardv1alpha1.ReshardRange{{ShardID: 0}}, Mode: pgshardv1alpha1.ReshardModeUpgrade, TargetMajor: 19}}
	if err := k8sClient.Create(ctx, rec); err != nil {
		t.Fatal(err)
	}
	// Inside the retirement window the set being retired -- the one the
	// drain waits on and the status names -- is the run's source.
	fp.mu.Lock()
	fp.workflows = map[string]WorkflowInfo{"g3": {ID: "wf-3", State: "running", Stage: "switched", SourceSet: catalog.DefaultShardSet}}
	fp.mu.Unlock()
	reconcile(t, r, c)
	get(t, "rrb", c)
	if rs := c.Status.Reshard; rs == nil || rs.RetiredShardSet != catalog.DefaultShardSet {
		t.Fatalf("the retiring set is %+v, want the run's source %s rather than the newer rolled-back g2", rs, catalog.DefaultShardSet)
	}

	fp.mu.Lock()
	fp.workflows["g3"] = WorkflowInfo{ID: "wf-3", State: "completed", Stage: "completed", SourceSet: catalog.DefaultShardSet}
	// g2's own run is still going -- a rollback retires its target before
	// it has released the pause and dropped replication -- so g2's groups
	// are still in use and must survive, while default's go.
	fp.workflows["g2"] = WorkflowInfo{ID: "wf-2", State: "running", Stage: "rolling_back"}
	fp.mu.Unlock()
	reconcile(t, r, c)
	reconcile(t, r, c)
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "rrb-marker-g2"}, &corev1.ConfigMap{}); err != nil {
		t.Errorf("g2's groups were deleted while its own run was still going: %v", err)
	}

	// Once that run ends, g2 goes too.
	fp.mu.Lock()
	fp.workflows["g2"] = WorkflowInfo{ID: "wf-2", State: "cancelled", Stage: "rolled_back"}
	fp.mu.Unlock()
	reconcile(t, r, c)
	reconcile(t, r, c)

	for _, set := range []string{catalog.DefaultShardSet, "g2"} {
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "rrb-marker-" + set}, &corev1.ConfigMap{})
		if !apierrors.IsNotFound(err) {
			t.Errorf("the groups of retired set %s are still there after g3 completed (%v)", set, err)
		}
	}
	sets, _ := fp.ShardSets(ctx, "")
	for _, s := range sets {
		if s.State == catalog.ShardSetRetired {
			t.Errorf("retired set %s is still in the catalog", s.Name)
		}
	}
}
