package operator

import (
	"context"
	"slices"
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
	// The old end must NOT be the stable cr-catalog-rw endpoint: after the
	// cutover that name selects the new group, so a rollback addressed
	// through it would talk to the new catalog on both connections and
	// replay nothing.
	if len(rollbacks) != 1 || rollbacks[0] != "cr-catalog-g2-rw.default.svc>cr-catalog-g1-rw.default.svc" {
		t.Fatalf("rollback must replay the new catalog back into the old one: %v", rollbacks)
	}
	var gen1 corev1.Service
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "cr-catalog-g1-rw"}, &gen1); err != nil {
		t.Fatalf("the old generation needs an address of its own: %v", err)
	}
	if gen1.Spec.Selector[LabelGroup] == "catalog-g2" {
		t.Fatalf("the generation-1 Service must not select the new group: %v", gen1.Spec.Selector)
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

// TestAbandonedCatalogRollbackPutsTheCatalogBack: a rollback fences the
// catalog that is serving before it replays. If the request is withdrawn
// while that is in flight, retirement must lift the fence again instead of
// completing the upgrade on a catalog nobody can write to.
func TestAbandonedCatalogRollbackPutsTheCatalogBack(t *testing.T) {
	r, fp, c := setup(t, "ca")
	cur := startCatalogUpgrade(t, r, fp, c)
	base := cur.DeepCopy()
	cur.Spec.Resharding.RetireOldGroupsAfter = &metav1.Duration{Duration: time.Hour}
	if err := k8sClient.Patch(context.Background(), cur, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4 && catalogStage(t, c.Name) != CatalogUpgradeRetiring; i++ {
		reconcile(t, r, c)
	}
	fp.mu.Lock()
	fp.catalogRollbackErr = "still draining"
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
	if cur = getCluster(t, c.Name); cur.Status.CatalogUpgrade == nil || !cur.Status.CatalogUpgrade.RollbackStarted {
		t.Fatal("a rollback that fenced the serving catalog must record that it started")
	}

	// The operator withdraws the request while the replay is still stuck.
	base = cur.DeepCopy()
	delete(cur.Annotations, pgshardv1alpha1.AnnotationCatalogUpgrade)
	if err := k8sClient.Patch(context.Background(), cur, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, c)
	cur = getCluster(t, c.Name)
	if cur.Status.CatalogUpgrade == nil || cur.Status.CatalogUpgrade.RollbackStarted {
		t.Fatalf("abandoning the rollback must clear the started flag: %+v", cur.Status.CatalogUpgrade)
	}
	fp.mu.Lock()
	releases := append([]string(nil), fp.catalogReleases...)
	disables := append([]string(nil), fp.catalogRollbackDisables...)
	fp.mu.Unlock()
	if len(releases) == 0 || releases[len(releases)-1] != "ca-catalog-g2-rw.default.svc" {
		t.Fatalf("the serving catalog must be unfenced again: %v", releases)
	}
	if len(disables) != 1 || disables[0] != "ca-catalog-g1-rw.default.svc" {
		t.Fatalf("the reverse stream must be stopped on the old group: %v", disables)
	}
	if cur.Status.CatalogGeneration != 2 || cur.Status.CatalogPGMajor != 19 {
		t.Fatalf("the catalog must still be the new group: gen=%d major=%d", cur.Status.CatalogGeneration, cur.Status.CatalogPGMajor)
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

// TestCatalogRollbackRecordsTheRestoredCatalogBeforeItMovesAnything: the
// rollback repoints the stable Service and deletes the new catalog group. If
// status still named that group when the operator died, the next pass sent
// schema reconciliation at a fenced catalog and, once the delete had run,
// rebuilt an empty group of that generation and pointed the Service at it.
// So the restored generation has to be durable before anything moves.
func TestCatalogRollbackRecordsTheRestoredCatalogBeforeItMovesAnything(t *testing.T) {
	r, fp, c := setup(t, "cb")
	cur := startCatalogUpgrade(t, r, fp, c)
	base := cur.DeepCopy()
	cur.Spec.Resharding.RetireOldGroupsAfter = &metav1.Duration{Duration: time.Hour}
	if err := k8sClient.Patch(context.Background(), cur, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4 && catalogStage(t, c.Name) != CatalogUpgradeRetiring; i++ {
		reconcile(t, r, c)
	}

	// ReleaseCatalog runs after the endpoint has moved and before the new
	// group is deleted: what the API server holds at that moment is what an
	// operator that died there would leave behind.
	var atRelease *pgshardv1alpha1.PgShardCluster
	fp.mu.Lock()
	fp.onRelease = func() { atRelease = getCluster(t, c.Name) }
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

	if atRelease == nil {
		t.Fatal("the rollback never reached the release, so this proves nothing")
	}
	if atRelease.Status.CatalogGeneration != 1 || atRelease.Status.CatalogPGMajor != 18 {
		t.Errorf("status named generation %d (major %d) while the rollback was moving the endpoint; want the restored generation 1 (major 18)",
			atRelease.Status.CatalogGeneration, atRelease.Status.CatalogPGMajor)
	}
	if up := atRelease.Status.CatalogUpgrade; up == nil || !up.RollbackStarted {
		t.Errorf("the rollback must be recorded as started before it moves anything: %+v", atRelease.Status.CatalogUpgrade)
	}
}

// TestCatalogMigrationsWaitForTheRollbackWindowToClose: a catalog schema
// migration applied while the previous catalog is still kept for rollback
// cannot be rolled back with the data. Logical replication carries no DDL,
// so the old catalog would take the bookkeeping row that says it is migrated
// while the DDL stayed behind, and rows in a table the migration created
// would not replicate at all.
func TestCatalogMigrationsWaitForTheRollbackWindowToClose(t *testing.T) {
	r, fp, c := setup(t, "cm")
	cur := startCatalogUpgrade(t, r, fp, c)
	base := cur.DeepCopy()
	cur.Spec.Resharding.RetireOldGroupsAfter = &metav1.Duration{Duration: time.Hour}
	if err := k8sClient.Patch(context.Background(), cur, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4 && catalogStage(t, c.Name) != CatalogUpgradeRetiring; i++ {
		reconcile(t, r, c)
	}
	if catalogStage(t, c.Name) != CatalogUpgradeRetiring {
		t.Fatalf("the upgrade never reached retiring: %q", catalogStage(t, c.Name))
	}

	fp.mu.Lock()
	fp.migrated = 0
	fp.mu.Unlock()
	reconcile(t, r, c)
	fp.mu.Lock()
	during := fp.migrated
	fp.mu.Unlock()
	if during != 0 {
		t.Errorf("the catalog was migrated %d time(s) while the old one was still kept for rollback", during)
	}
	if cond := condition(t, c.Name, ConditionCatalogReady); cond.Reason != "MigrationDeferred" {
		t.Errorf("CatalogReady reason = %q, want the deferral said out loud", cond.Reason)
	}

	// Once the window closes and the upgrade is done, migrations resume.
	cur = getCluster(t, c.Name)
	base = cur.DeepCopy()
	cur.Spec.Resharding.RetireOldGroupsAfter = &metav1.Duration{Duration: 0}
	if err := k8sClient.Patch(context.Background(), cur, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4 && getCluster(t, c.Name).Status.CatalogUpgrade != nil; i++ {
		reconcile(t, r, c)
	}
	if up := getCluster(t, c.Name).Status.CatalogUpgrade; up != nil {
		t.Fatalf("the upgrade never retired: %+v", up)
	}
	fp.mu.Lock()
	fp.migrated = 0
	fp.mu.Unlock()
	reconcile(t, r, c)
	fp.mu.Lock()
	after := fp.migrated
	fp.mu.Unlock()
	if after == 0 {
		t.Error("migrations must resume once the previous catalog is retired")
	}
}

// TestCatalogUpgradeGivesTheNewCatalogTheRouterPassword: roles are not
// logically replicated, so the new catalog has the router's role from its
// own migration and no password for it. If nothing set one, every router
// would be refused the moment the endpoint moved to it.
func TestCatalogUpgradeGivesTheNewCatalogTheRouterPassword(t *testing.T) {
	r, fp, c := setup(t, "cp")
	startCatalogUpgrade(t, r, fp, c)
	for i := 0; i < 3 && catalogStage(t, c.Name) == CatalogUpgradeProvisioning; i++ {
		reconcile(t, r, c)
	}

	var sec corev1.Secret
	get(t, RouterSecretName(c.Name), &sec)
	want := "cp-catalog-g2-rw.default.svc=" + string(sec.Data["password"])

	fp.mu.Lock()
	applied := append([]string(nil), fp.routerPasswords...)
	fp.mu.Unlock()
	if !slices.Contains(applied, want) {
		t.Errorf("the new catalog was never given the router password: %v", applied)
	}
}

// TestNoCatalogMigrationWhileAnUpgradeIsInFlight: both catalog
// publications are FOR TABLES IN SCHEMA pgshard, which includes
// pgshard.schema_migrations, and logical replication carries no DDL. A
// migration applied to whichever catalog is serving therefore replicates
// its ledger row to the other while the ALTER and CREATE stay behind, and
// that catalog then skips DDL it believes it has already applied.
//
// The deferral used to cover the retirement window alone, where the stream
// runs new to old. Before the cutover it runs old to new and the group
// about to serve inherits the same lie, so the window is the whole
// upgrade.
func TestNoCatalogMigrationWhileAnUpgradeIsInFlight(t *testing.T) {
	r, fp, c := setup(t, "nomig")
	startCatalogUpgrade(t, r, fp, c)

	serving := "nomig-catalog-rw.default.svc"
	migratedServing := func() int {
		fp.mu.Lock()
		defer fp.mu.Unlock()
		n := 0
		for _, d := range fp.migratedDSNs {
			if d == serving {
				n++
			}
		}
		return n
	}
	before := migratedServing()

	// Every stage up to and including retirement, not just the last one.
	for _, want := range []string{CatalogUpgradeCopying, CatalogUpgradeCatchingUp, CatalogUpgradeCutover} {
		reconcile(t, r, c)
		if got := catalogStage(t, c.Name); got != want {
			t.Fatalf("stage %s, want %s", got, want)
		}
		if got := migratedServing(); got != before {
			t.Fatalf("at stage %s the serving catalog was migrated (%d then %d)", want, before, got)
		}
	}

	// The new catalog is still migrated: that is its setup, and it happens
	// before any copy carries a ledger row to it.
	fp.mu.Lock()
	target := 0
	for _, d := range fp.migratedDSNs {
		if d == "nomig-catalog-g2-rw.default.svc" {
			target++
		}
	}
	fp.mu.Unlock()
	if target == 0 {
		t.Error("the new-major catalog was never migrated, so it has no schema to copy into")
	}

	// And the condition says why, rather than looking like a failure.
	cur := getCluster(t, c.Name)
	var cond *metav1.Condition
	for i := range cur.Status.Conditions {
		if cur.Status.Conditions[i].Type == ConditionCatalogReady {
			cond = &cur.Status.Conditions[i]
		}
	}
	if cond == nil || cond.Reason != "MigrationDeferred" || cond.Status != metav1.ConditionTrue {
		t.Fatalf("catalog condition %+v", cond)
	}
}
