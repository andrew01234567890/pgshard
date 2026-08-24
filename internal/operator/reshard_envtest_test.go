package operator

import (
	"context"
	"math"
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
	fp.shardSets = append(fp.shardSets, ShardSetInfo{Name: "g2", Generation: 2, State: catalog.ShardSetDesired, Ranges: splitAt(split[0], 0)})
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

func splitAt(r placement.Range, at int64) placement.RangeSet {
	return placement.RangeSet{{Start: r.Start, End: at - 1}, {Start: at, End: r.End}}
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
	if last != "wf-2:switchWrites::86400" {
		t.Fatalf("mirrored spec: %q", last)
	}

	rec.Annotations = map[string]string{pgshardv1alpha1.AnnotationProceed: "switchWrites, complete"}
	if err := k8sClient.Update(context.Background(), &rec); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, c)
	fp.mu.Lock()
	last = fp.cutoverSpecs[len(fp.cutoverSpecs)-1]
	fp.mu.Unlock()
	if last != "wf-2:switchWrites:switchWrites+complete:86400" {
		t.Fatalf("proceed annotation must reach the workflow: %q", last)
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

	fp.mu.Lock()
	fp.workflows["g2"] = WorkflowInfo{ID: "wf-2", State: "completed", Stage: "completed", CutoverPauseMS: 800}
	fp.mu.Unlock()
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
