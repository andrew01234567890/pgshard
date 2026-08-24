package operator

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

func upgradedShardsCluster() *pgshardv1alpha1.PgShardCluster {
	return &pgshardv1alpha1.PgShardCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec:       pgshardv1alpha1.PgShardClusterSpec{PostgreSQL: pgshardv1alpha1.PostgreSQLSpec{Major: 19}},
		Status:     pgshardv1alpha1.PgShardClusterStatus{ServingPGMajor: 19, CatalogPGMajor: 18},
	}
}

func TestCatalogUpgradeRequestedOnlyAfterShardsFinish(t *testing.T) {
	c := upgradedShardsCluster()
	if !CatalogUpgradeRequested(c) {
		t.Fatal("catalog behind the spec with shards done must request an upgrade")
	}
	c.Status.ServingPGMajor = 18
	if CatalogUpgradeRequested(c) {
		t.Fatal("shards still on the old major: the catalog goes last")
	}
	c.Status.ServingPGMajor = 19
	c.Status.Reshard = &pgshardv1alpha1.ClusterReshardStatus{Name: "r"}
	if CatalogUpgradeRequested(c) {
		t.Fatal("a reshard in flight must hold the catalog upgrade")
	}
	c.Status.Reshard = nil
	c.Spec.Upgrade.Strategy = UpgradeStrategyOffline
	if CatalogUpgradeRequested(c) {
		t.Fatal("the offline strategy never triggers the blue/green catalog path")
	}
	c.Spec.Upgrade.Strategy = ""
	c.Status.CatalogPGMajor = 0
	if CatalogUpgradeRequested(c) {
		t.Fatal("an unprobed catalog major must not trigger")
	}
	c.Status.CatalogPGMajor = 19
	if CatalogUpgradeRequested(c) {
		t.Fatal("a catalog already on the target major must not trigger")
	}
}

func TestCatalogUpgradeBlockersNameEveryHold(t *testing.T) {
	c := upgradedShardsCluster()
	c.Status.ServingPGMajor = 18
	c.Status.Reshard = &pgshardv1alpha1.ClusterReshardStatus{Name: "c-reshard-g2"}
	c.Status.PlacementWorkflows = []pgshardv1alpha1.ClusterPlacementWorkflowStatus{{WorkflowID: "w1", State: "running"}}
	blockers := CatalogUpgradeBlockers(c)
	if len(blockers) != 3 {
		t.Fatalf("blockers: %v", blockers)
	}
}

func TestCatalogGroupNamingAndTargets(t *testing.T) {
	c := upgradedShardsCluster()
	if got := Groups(c)[0].Name(); got != "catalog" {
		t.Fatalf("generation 1 catalog group named %s", got)
	}
	c.Status.CatalogGeneration = 3
	if got := Groups(c)[0].Name(); got != "catalog-g3" {
		t.Fatalf("generation 3 catalog group named %s", got)
	}
	if got := Groups(c)[0].ServiceRW(); got != "c-catalog-g3-rw" {
		t.Fatalf("own service %s", got)
	}
	if got := CatalogServiceRW(c.Name); got != "c-catalog-rw" {
		t.Fatalf("stable endpoint %s", got)
	}
	c.Status.CatalogGeneration = 1
	c.Status.CatalogUpgrade = &pgshardv1alpha1.ClusterCatalogUpgradeStatus{FromMajor: 18, ToMajor: 19, Generation: 2}
	tg := CatalogTargetGroup(c)
	if tg == nil || tg.Name() != "catalog-g2" || !tg.NonServing || tg.PGMajor != 19 {
		t.Fatalf("target group %+v", tg)
	}
	c.Status.CatalogGeneration = 2
	if CatalogTargetGroup(c) != nil {
		t.Fatal("the flipped generation is no longer a target")
	}
	c.Status.CatalogUpgrade.RetiredGeneration = 1
	c.Status.CatalogUpgrade.RetiredMajor = 18
	rg := RetiredCatalogGroup(c)
	if rg == nil || rg.Name() != "catalog" || !rg.Retired || rg.PGMajor != 18 {
		t.Fatalf("retired group %+v", rg)
	}
}

func TestRetiredCatalogGroupKeepsStableServiceUntouched(t *testing.T) {
	c := upgradedShardsCluster()
	c.Status.CatalogGeneration = 2
	c.Status.CatalogUpgrade = &pgshardv1alpha1.ClusterCatalogUpgradeStatus{FromMajor: 18, ToMajor: 19, Generation: 2, RetiredGeneration: 1, RetiredMajor: 18}
	rg := *RetiredCatalogGroup(c)
	var names []string
	for _, svc := range (Renderer{}).Services(c, rg) {
		names = append(names, svc.Name)
	}
	for _, n := range names {
		if n == CatalogServiceRW(c.Name) {
			t.Fatalf("retired catalog group must not render the stable endpoint: %v", names)
		}
	}
	svc := (Renderer{}).CatalogEndpointService(c, Groups(c)[0])
	if svc.Name != "c-catalog-rw" || svc.Spec.Selector[LabelGroup] != "catalog-g2" || svc.Spec.Selector[LabelRole] != RolePrimary {
		t.Fatalf("stable endpoint %s selector %v", svc.Name, svc.Spec.Selector)
	}
}

func TestProvisionBudgetOnlyDuringUpgrades(t *testing.T) {
	c := upgradedShardsCluster()
	c.Spec.Upgrade.MaxParallelGroups = 2
	if got := ProvisionBudget(c); got != 0 {
		t.Fatalf("no run in flight: budget %d", got)
	}
	c.Status.Reshard = &pgshardv1alpha1.ClusterReshardStatus{Name: "r", PGMajor: 0}
	if got := ProvisionBudget(c); got != 0 {
		t.Fatalf("topology reshard: budget %d", got)
	}
	c.Status.Reshard.PGMajor = c.Status.ServingPGMajor
	if got := ProvisionBudget(c); got != 0 {
		t.Fatalf("same-major run: budget %d", got)
	}
	c.Status.ServingPGMajor = 18
	c.Status.Reshard.PGMajor = 19
	if got := ProvisionBudget(c); got != 2 {
		t.Fatalf("upgrade run: budget %d", got)
	}
}
