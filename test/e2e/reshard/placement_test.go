//go:build e2e

package reshard

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/test/e2e"
)

// TestTablePlacementRekey changes the shard key of a registered table on a
// running cluster: the controller's placement workflow builds the shadow
// table, swaps it in under a table-scoped write pause, the catalog
// publishes the new key, and the cluster status reports the run.
func TestTablePlacementRekey(t *testing.T) {
	c := e2e.NewCluster(t)
	c.GatherOnFailure(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	root := repoRoot(t)
	major := env("PG_MAJOR", "18")

	deployOperator(ctx, t, c, root, env("OPERATOR_IMAGE", "pgshard-operator:e2e"))
	manifest := clusterManifest(major, os.Getenv("PGSHARD_POSTGRES_IMAGE"), 1)
	if err := c.Apply(ctx, manifest); err != nil {
		t.Fatal(err)
	}
	if err := c.Apply(ctx, clientManifest(memberImage(major))); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if t.Failed() {
			gatherNamespace(ctx, c)
		}
		if err := c.Delete(ctx, manifest); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
	if err := waitCondition(ctx, c, "Ready", 12*time.Minute); err != nil {
		gatherNamespace(ctx, c)
		t.Fatal(err)
	}
	if err := waitCondition(ctx, c, "CatalogReady", 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitPodsReady(ctx, testNamespace, "app="+clientPod, 3*time.Minute); err != nil {
		t.Fatal(err)
	}
	seedApp(ctx, t, c)
	if err := c.Apply(ctx, controllerManifest(env("CONTROLLER_IMAGE", "pgshard-controller:e2e"))); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitPodsReady(ctx, testNamespace, "app="+clusterName+"-controller", 3*time.Minute); err != nil {
		t.Fatal(err)
	}
	waitFor(ctx, t, c, "orders effective", 2*time.Minute, func() bool {
		return catalogSQL(ctx, t, c, "SELECT coalesce(effective_shard_key, '') FROM pgshard.table_status WHERE table_name = 'orders'") == "tenant_id"
	})

	catalogSQL(ctx, t, c, "UPDATE pgshard.tables SET shard_key = 'id' WHERE table_name = 'orders'")
	waitFor(ctx, t, c, "placement workflow created", 2*time.Minute, func() bool {
		return catalogSQL(ctx, t, c, "SELECT count(*) FROM pgshard.workflows WHERE kind = 'table_placement'") == "1"
	})
	if _, err := shardSQL(ctx, c, "shard-0", "INSERT INTO orders (tenant_id, note) SELECT g * 7919 + 13, 'during' FROM generate_series(1001, 1200) g"); err != nil {
		t.Fatal(err)
	}
	placementState := func() string {
		return "\nplacement workflow: " + catalogSQL(ctx, t, c,
			"SELECT state || ' stage=' || coalesce(status->>'stage', '') || ' message=' || coalesce(status->>'message', '') || ' error=' || coalesce(error, '') FROM pgshard.workflows WHERE kind = 'table_placement'")
	}
	waitForWhy(ctx, t, c, "placement workflow completed", 10*time.Minute, placementState, func() bool {
		got := catalogSQL(ctx, t, c, "SELECT state || ':' || coalesce(status->>'stage', '') FROM pgshard.workflows WHERE kind = 'table_placement'")
		if strings.HasPrefix(got, "failed") {
			t.Fatalf("placement workflow failed: %s", catalogSQL(ctx, t, c, "SELECT coalesce(error, '') || ' ' || status::text FROM pgshard.workflows WHERE kind = 'table_placement'"))
		}
		return got == "completed:completed"
	})
	if got := catalogSQL(ctx, t, c, "SELECT effective_placement || ':' || effective_shard_key || ':' || migrating FROM pgshard.table_status WHERE table_name = 'orders'"); got != "sharded:id:false" {
		t.Fatalf("table_status after the re-key: %q", got)
	}
	if got, err := shardSQL(ctx, c, "shard-0", "SELECT count(*) FROM orders"); err != nil || got != "1200" {
		t.Fatalf("orders after the re-key: %q %v", got, err)
	}
	if got, err := shardSQL(ctx, c, "shard-0", "SELECT count(*) FROM pg_tables WHERE tablename LIKE 'orders%'"); err != nil || got != "1" {
		t.Fatalf("tables named orders* after the re-key: %q %v", got, err)
	}
	if got, err := shardSQL(ctx, c, "shard-0", "SELECT count(*) FROM pg_indexes WHERE tablename = 'orders' AND indexname IN ('orders_pkey', 'orders_note_idx')"); err != nil || got != "2" {
		t.Fatalf("index names after the re-key: %q %v", got, err)
	}
	if got, err := psql(ctx, c, clusterName+"-shard-0-rw", "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'pgshard\\_place\\_%'"); err != nil || got != "0" {
		t.Errorf("placement slots left: %q %v", got, err)
	}
	if got := catalogSQL(ctx, t, c, "SELECT count(*) FROM pgshard.workflow_locks"); got != "0" {
		t.Errorf("workflow locks left: %q", got)
	}
	waitFor(ctx, t, c, "cluster status reports the run", 3*time.Minute, func() bool {
		return jsonpath(ctx, c, "pgshardcluster", clusterName, "{.status.placementWorkflows[0].state}/{.status.placementWorkflows[0].to}") == "completed/sharded(id)"
	})
	if got := jsonpath(ctx, c, "pgshardcluster", clusterName, "{.status.placementWorkflows[0].table}"); got != appDatabase+".public.orders" {
		t.Errorf("status table: %q", got)
	}
}
