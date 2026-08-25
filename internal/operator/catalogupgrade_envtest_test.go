package operator

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/catalog"
)

func (f *fakeProber) setShardSetMajor(name string, major int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.shardSets {
		if f.shardSets[i].Name == name {
			f.shardSets[i].PGMajor = major
		}
	}
}

func getCluster(t *testing.T, name string) *pgshardv1alpha1.PgShardCluster {
	t.Helper()
	var c pgshardv1alpha1.PgShardCluster
	get(t, name, &c)
	return &c
}

// startCatalogUpgrade drives a healthy 18 cluster whose shard set already
// reached 19 into a catalog upgrade and returns the refreshed cluster.
func startCatalogUpgrade(t *testing.T, r *ClusterReconciler, fp *fakeProber, c *pgshardv1alpha1.PgShardCluster) *pgshardv1alpha1.PgShardCluster {
	t.Helper()
	bringUp(t, r, fp, c)
	fp.setShardSetMajor(catalog.DefaultShardSet, 19)
	base := c.DeepCopy()
	c.Spec.PostgreSQL.Major = 19
	if err := k8sClient.Patch(context.Background(), c, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, c)
	cur := getCluster(t, c.Name)
	up := cur.Status.CatalogUpgrade
	if up == nil || up.Stage != CatalogUpgradeProvisioning || up.FromMajor != 18 || up.ToMajor != 19 || up.Generation != 2 {
		t.Fatalf("catalog upgrade not started: %+v", up)
	}
	reconcile(t, r, c)
	g := *CatalogTargetGroup(cur)
	if g.Name() != "catalog-g2" {
		t.Fatalf("target group %s, want catalog-g2", g.Name())
	}
	for i := 0; i < g.Replicas; i++ {
		markPodRunning(t, g.MemberName(i), podIP(7, i))
		if i > 0 {
			fp.mu.Lock()
			fp.streaming[g.MemberName(i)] = true
			fp.mu.Unlock()
		}
	}
	return cur
}

func catalogStage(t *testing.T, name string) string {
	t.Helper()
	cur := getCluster(t, name)
	if cur.Status.CatalogUpgrade == nil {
		return ""
	}
	return cur.Status.CatalogUpgrade.Stage
}

func TestCatalogUpgradeBlueGreenRepointsRouterEndpoint(t *testing.T) {
	r, fp, c := setup(t, "cu")
	cur := startCatalogUpgrade(t, r, fp, c)
	base := cur.DeepCopy()
	cur.Spec.Resharding.RetireOldGroupsAfter = &metav1.Duration{Duration: 0}
	if err := k8sClient.Patch(context.Background(), cur, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}

	reconcile(t, r, c)
	if got := catalogStage(t, c.Name); got != CatalogUpgradeCopying {
		t.Fatalf("stage %s, want copying", got)
	}
	reconcile(t, r, c)
	if got := catalogStage(t, c.Name); got != CatalogUpgradeCatchingUp {
		t.Fatalf("stage %s, want catching_up", got)
	}
	fp.mu.Lock()
	if len(fp.catalogCopies) == 0 || fp.catalogCopies[0] != "cu-catalog-rw.default.svc>cu-catalog-g2-rw.default.svc" {
		t.Fatalf("catalog copies %v", fp.catalogCopies)
	}
	fp.catalogLag = "42 bytes of WAL behind"
	fp.mu.Unlock()
	reconcile(t, r, c)
	cur = getCluster(t, c.Name)
	if cur.Status.CatalogUpgrade.Stage != CatalogUpgradeCatchingUp || cur.Status.CatalogUpgrade.Message != "42 bytes of WAL behind" {
		t.Fatalf("lagging upgrade: %+v", cur.Status.CatalogUpgrade)
	}
	fp.mu.Lock()
	fp.catalogLag = ""
	fp.mu.Unlock()
	reconcile(t, r, c)
	if got := catalogStage(t, c.Name); got != CatalogUpgradeCutover {
		t.Fatalf("stage %s, want cutover", got)
	}
	reconcile(t, r, c)
	cur = getCluster(t, c.Name)
	if cur.Status.CatalogGeneration != 2 || cur.Status.CatalogPGMajor != 19 {
		t.Fatalf("catalog not flipped: gen=%d major=%d", cur.Status.CatalogGeneration, cur.Status.CatalogPGMajor)
	}
	if cur.Status.CatalogUpgrade.Stage != CatalogUpgradeRetiring {
		t.Fatalf("stage %s, want retiring", cur.Status.CatalogUpgrade.Stage)
	}
	var svc corev1.Service
	get(t, "cu-catalog-rw", &svc)
	if svc.Spec.Selector[LabelGroup] != "catalog-g2" || svc.Spec.Selector[LabelRole] != RolePrimary {
		t.Fatalf("stable catalog endpoint selector %v must point at catalog-g2's primary", svc.Spec.Selector)
	}
	if dsn := CatalogDSN(cur); dsn != "host=cu-catalog-rw.default.svc port=5432 user=postgres dbname=postgres" {
		t.Fatalf("router catalog DSN changed across the flip: %s", dsn)
	}

	// RetireOldGroupsAfter is unset, so the next pass deletes the old group.
	reconcile(t, r, c)
	cur = getCluster(t, c.Name)
	if cur.Status.CatalogUpgrade != nil {
		t.Fatalf("upgrade not finished: %+v", cur.Status.CatalogUpgrade)
	}
	var pod corev1.Pod
	err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "cu-catalog-0"}, &pod)
	if err == nil && pod.DeletionTimestamp == nil {
		t.Fatal("old catalog pod cu-catalog-0 must be deleted")
	} else if err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
	get(t, "cu-catalog-rw", &svc)
	if svc.Spec.Selector[LabelGroup] != "catalog-g2" {
		t.Fatalf("stable endpoint lost after retirement: %v", svc.Spec.Selector)
	}
}

func TestCatalogUpgradeRollbackBeforeRetirement(t *testing.T) {
	r, fp, c := setup(t, "cr")
	cur := startCatalogUpgrade(t, r, fp, c)
	base := cur.DeepCopy()
	cur.Spec.Resharding.RetireOldGroupsAfter = &metav1.Duration{Duration: time.Hour}
	if err := k8sClient.Patch(context.Background(), cur, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4 && catalogStage(t, c.Name) != CatalogUpgradeRetiring; i++ {
		reconcile(t, r, c)
	}
	if got := catalogStage(t, c.Name); got != CatalogUpgradeRetiring {
		t.Fatalf("stage %s, want retiring", got)
	}

	cur = getCluster(t, c.Name)
	base = cur.DeepCopy()
	if cur.Annotations == nil {
		cur.Annotations = map[string]string{}
	}
	cur.Annotations[pgshardv1alpha1.AnnotationCatalogUpgrade] = pgshardv1alpha1.UpgradeActionRollback
	if err := k8sClient.Patch(context.Background(), cur, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, c)
	cur = getCluster(t, c.Name)
	if cur.Status.CatalogUpgrade != nil {
		t.Fatalf("rollback must clear the upgrade: %+v", cur.Status.CatalogUpgrade)
	}
	if cur.Status.CatalogGeneration != 1 || cur.Status.CatalogPGMajor != 18 {
		t.Fatalf("rollback must restore generation 1 major 18: gen=%d major=%d", cur.Status.CatalogGeneration, cur.Status.CatalogPGMajor)
	}
	fp.mu.Lock()
	releases := len(fp.catalogReleases)
	rollbacks := append([]string(nil), fp.catalogRollbacks...)
	fp.mu.Unlock()
	if releases == 0 {
		t.Fatal("rollback must release the old catalog's fence")
	}
	if len(rollbacks) != 1 || rollbacks[0] != "cr-catalog-g2-rw.default.svc>cr-catalog-rw.default.svc" {
		t.Fatalf("rollback must replay the new catalog back into the old one: %v", rollbacks)
	}
	var pod corev1.Pod
	err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "cr-catalog-g2-0"}, &pod)
	if err == nil && pod.DeletionTimestamp == nil {
		t.Fatal("new-major catalog pod must be deleted on rollback")
	} else if err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
}

// TestCatalogRollbackKeepsTheEndpointUntilTheReplaySucceeds: everything the
// new catalog accepted after the cutover - roles, topology, workflows, 2PC
// decisions - only exists there, so a rollback that cannot replay it must
// leave the endpoint alone rather than serve a catalog missing those writes.
func TestCatalogRollbackKeepsTheEndpointUntilTheReplaySucceeds(t *testing.T) {
	r, fp, c := setup(t, "ck")
	cur := startCatalogUpgrade(t, r, fp, c)
	base := cur.DeepCopy()
	cur.Spec.Resharding.RetireOldGroupsAfter = &metav1.Duration{Duration: time.Hour}
	if err := k8sClient.Patch(context.Background(), cur, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4 && catalogStage(t, c.Name) != CatalogUpgradeRetiring; i++ {
		reconcile(t, r, c)
	}
	if got := catalogStage(t, c.Name); got != CatalogUpgradeRetiring {
		t.Fatalf("stage %s, want retiring", got)
	}
	fp.mu.Lock()
	fp.catalogRollbackErr = "reverse subscription is behind"
	fp.mu.Unlock()

	cur = getCluster(t, c.Name)
	base = cur.DeepCopy()
	if cur.Annotations == nil {
		cur.Annotations = map[string]string{}
	}
	cur.Annotations[pgshardv1alpha1.AnnotationCatalogUpgrade] = pgshardv1alpha1.UpgradeActionRollback
	if err := k8sClient.Patch(context.Background(), cur, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, c)
	cur = getCluster(t, c.Name)
	if cur.Status.CatalogUpgrade == nil {
		t.Fatal("a rollback that could not replay must stay in progress")
	}
	if !strings.Contains(cur.Status.CatalogUpgrade.Message, "reverse subscription is behind") {
		t.Fatalf("message %q must report why the rollback did not proceed", cur.Status.CatalogUpgrade.Message)
	}
	if cur.Status.CatalogGeneration != 2 || cur.Status.CatalogPGMajor != 19 {
		t.Fatalf("catalog must keep serving the new group: gen=%d major=%d", cur.Status.CatalogGeneration, cur.Status.CatalogPGMajor)
	}
	var pod corev1.Pod
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "ck-catalog-g2-0"}, &pod); err != nil {
		t.Fatalf("new-major catalog pod must survive a failed rollback: %v", err)
	}
	if pod.DeletionTimestamp != nil {
		t.Fatal("new-major catalog pod must not be deleted by a failed rollback")
	}

	// Once the replay succeeds the rollback completes as usual.
	fp.mu.Lock()
	fp.catalogRollbackErr = ""
	fp.mu.Unlock()
	reconcile(t, r, c)
	cur = getCluster(t, c.Name)
	if cur.Status.CatalogUpgrade != nil {
		t.Fatalf("rollback must clear the upgrade once the replay succeeds: %+v", cur.Status.CatalogUpgrade)
	}
	if cur.Status.CatalogGeneration != 1 || cur.Status.CatalogPGMajor != 18 {
		t.Fatalf("rollback must restore generation 1 major 18: gen=%d major=%d", cur.Status.CatalogGeneration, cur.Status.CatalogPGMajor)
	}
}

func TestUpgradeProvisioningHonorsMaxParallelGroups(t *testing.T) {
	r, fp, c := setup(t, "mp")
	// Pods carry only the unqualified group label, which the target set
	// shares with other clusters' tests; without GC in envtest they must
	// go explicitly.
	t.Cleanup(func() {
		_ = k8sClient.DeleteAllOf(context.Background(), &corev1.Pod{},
			client.InNamespace("default"), client.MatchingLabels{LabelCluster: "mp"})
	})
	base := c.DeepCopy()
	two := 2
	c.Spec.Shards = &two
	c.Spec.Upgrade.MaxParallelGroups = 1
	if err := k8sClient.Patch(context.Background(), c, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	bringUp(t, r, fp, c)
	fp.setShardSetMajor(catalog.DefaultShardSet, 18)
	base = getCluster(t, c.Name).DeepCopy()
	cur := base.DeepCopy()
	cur.Spec.PostgreSQL.Major = 19
	if err := k8sClient.Patch(context.Background(), cur, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, c)
	waitEnv(t, func() bool {
		cs := getCluster(t, c.Name)
		if cs.Status.Reshard == nil {
			t.Logf("status: serving=%d spec=%d conds=%+v sets=%+v", cs.Status.ServingPGMajor, cs.Spec.PostgreSQL.Major, cs.Status.Conditions, fp.shardSets)
		}
		return cs.Status.Reshard != nil && cs.Status.Reshard.PGMajor == 19
	}, func() { reconcile(t, r, c) }, "upgrade run recorded")
	reconcile(t, r, c)
	var pg pgshardv1alpha1.PgShardGroup
	get(t, "mp-shard-0-g2", &pg)
	err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "mp-shard-1-g2"}, &pg)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("second target must wait for the provisioning budget: %v", err)
	}

	// Once the first target group is ready the next one starts.
	tg := TargetGroups(getCluster(t, c.Name))[0]
	for i := 0; i < tg.Replicas; i++ {
		markPodRunning(t, tg.MemberName(i), podIP(8, i))
		if i > 0 {
			fp.mu.Lock()
			fp.streaming[tg.MemberName(i)] = true
			fp.mu.Unlock()
		}
	}
	fp.mu.Lock()
	fp.endpoints = map[string]string{}
	fp.mu.Unlock()
	reconcile(t, r, c)
	reconcile(t, r, c)
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "mp-shard-1-g2"}, &pg); err != nil {
		t.Fatalf("second target must start once the first is ready: %v", err)
	}
}

func waitEnv(t *testing.T, cond func() bool, step func(), what string) {
	t.Helper()
	for i := 0; i < 10; i++ {
		if cond() {
			return
		}
		step()
	}
	if !cond() {
		t.Fatalf("gave up waiting for %s", what)
	}
}

// TestCatalogUpgradeSteadyStatePatchesStatusOnlyOnChange: a retiring
// upgrade waiting out the retention window must not patch the cluster
// status on every pass.
func TestCatalogUpgradeSteadyStatePatchesStatusOnlyOnChange(t *testing.T) {
	r, fp, c := setup(t, "cu-steady")
	cur := startCatalogUpgrade(t, r, fp, c)
	base := cur.DeepCopy()
	cur.Spec.Resharding.RetireOldGroupsAfter = &metav1.Duration{Duration: time.Hour}
	if err := k8sClient.Patch(context.Background(), cur, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8 && catalogStage(t, c.Name) != CatalogUpgradeRetiring; i++ {
		reconcile(t, r, c)
	}
	if got := catalogStage(t, c.Name); got != CatalogUpgradeRetiring {
		t.Fatalf("stage %s, want retiring", got)
	}

	patches := 0
	r2 := &ClusterReconciler{Client: statusPatchCounter{Client: k8sClient, patches: &patches},
		Renderer: r.Renderer, Prober: r.Prober, Agents: r.Agents,
		FailoverDelay: r.FailoverDelay, PollInterval: r.PollInterval, QuiesceTimeout: r.QuiesceTimeout}
	pass := func() {
		t.Helper()
		fresh := getCluster(t, c.Name)
		if _, err := r2.reconcileCatalogUpgrade(context.Background(), fresh, CatalogDSN(fresh), "pw", nil, false); err != nil {
			t.Fatal(err)
		}
	}
	pass()
	settled := patches
	pass()
	pass()
	if patches != settled {
		t.Fatalf("steady retiring passes patched the cluster status %d more time(s)", patches-settled)
	}
}

type statusPatchCounter struct {
	client.Client
	patches *int
}

func (c statusPatchCounter) Status() client.SubResourceWriter {
	return countingStatusWriter{c.Client.Status(), c.patches}
}

type countingStatusWriter struct {
	client.SubResourceWriter
	patches *int
}

func (w countingStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if _, ok := obj.(*pgshardv1alpha1.PgShardCluster); ok {
		*w.patches++
	}
	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}
