//go:build e2e

package backup

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/controller"
	"github.com/andrew01234567890/pgshard/test/e2e"
)

// runBarrierRestore takes a certified barrier on the cluster (the barrier
// state machine runs in the test process over port-forwards to the group
// primaries, standing in for the controller) and restores it into a new
// cluster with target.barrier: every group recovers to the barrier's
// restore point, the operator reconciles prepared transactions against the
// restored decision log and lifts the write fence the barrier left behind.
func runBarrierRestore(ctx context.Context, t *testing.T, c *e2e.Cluster, s store, major, backupName string) {
	cluster := s.name
	primaries := map[string]string{}
	for _, g := range []string{"catalog", "shard-0"} {
		primaries[g] = jsonpath(ctx, t, c, "pgshardgroup", cluster+"-"+g, "{.status.primary}")
	}
	psqlOn := func(pod, sql string) string {
		t.Helper()
		out, err := c.Kubectl(ctx, nil, "-n", testNamespace, "exec", pod, "-c", "postgres", "--", "env", pgpassEnv, "psql", "-h", "/tmp", "-U", "postgres", "-tAc", sql)
		if err != nil {
			t.Fatalf("%s: %v\n%s", pod, err, out)
		}
		return strings.TrimSpace(out)
	}
	for _, pod := range primaries {
		psqlOn(pod, "CREATE TABLE barrier_e2e (id int, note text); INSERT INTO barrier_e2e SELECT i, 'before' FROM generate_series(1, 100) i")
	}

	pw, err := c.Kubectl(ctx, nil, "-n", testNamespace, "get", "secret", cluster+"-superuser", "-o", "jsonpath={.data.password}")
	if err != nil {
		t.Fatal(err)
	}
	password, err := base64.StdEncoding.DecodeString(strings.TrimSpace(pw))
	if err != nil {
		t.Fatal(err)
	}
	dsnFor := func(group string) (string, func()) {
		t.Helper()
		addr, stop, err := c.PortForward(ctx, testNamespace, cluster+"-"+group+"-rw", 5432)
		if err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("postgres://postgres:%s@%s/postgres?sslmode=disable", string(password), strings.TrimPrefix(addr, "http://")), stop
	}
	catalogDSN, stopCatalog := dsnFor("catalog")
	defer stopCatalog()
	shardDSN, stopShard := dsnFor("shard-0")
	defer stopShard()
	pool, err := pgxpool.New(ctx, catalogDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	dialer := &controller.PgxShardDialer{Pool: pool, DSNs: map[controller.ShardRef]string{{Set: "default", ID: 0}: shardDSN}}
	barrier := &controller.Barrier{Store: &controller.PGBarrierStore{Pool: pool}, Groups: &controller.SQLBarrierGroups{Pool: pool, Shards: dialer},
		Resolver: &controller.Resolver{Pool: pool, Shards: dialer}, ArchiveTimeout: 5 * time.Minute}
	rp, err := barrier.Run(ctx, "e2e-"+cluster)
	if err != nil {
		t.Fatalf("barrier: %v", err)
	}
	if !rp.Certified || len(rp.Groups) != 2 {
		t.Fatalf("barrier %+v", rp)
	}
	t.Logf("%s: certified barrier %s: %+v", cluster, rp.Name, rp.Groups)
	var fenced bool
	if err := pool.QueryRow(ctx, `SELECT write_fence FROM pgshard.shard_map_generation`).Scan(&fenced); err != nil || fenced {
		t.Fatalf("fence after the barrier: %v %v", fenced, err)
	}
	for _, pod := range primaries {
		psqlOn(pod, "INSERT INTO barrier_e2e SELECT i, 'after' FROM generate_series(101, 200) i; SELECT pg_switch_wal()")
	}

	image := os.Getenv("PGSHARD_POSTGRES_IMAGE")
	name := cluster + "-barrier"
	if err := c.Apply(ctx, restoreManifest(cluster, name, major, image, 3, `
  backupId: `+backupName+`
  target:
    barrier: e2e-`+cluster)); err != nil {
		t.Fatal(err)
	}
	r := waitRestore(ctx, t, c, name, 25*time.Minute)
	if r.Status.Reconciliation == nil || !r.Status.Reconciliation.Unfenced || len(r.Status.Reconciliation.Contradictions) != 0 {
		t.Fatalf("reconciliation %+v", r.Status.Reconciliation)
	}
	if got := jsonpath(ctx, t, c, "pgshardcluster", name, `{.status.conditions[?(@.type=="Ready")].status}`); got != "True" {
		t.Fatalf("%s Ready=%q", name, got)
	}
	for _, g := range []string{"catalog", "shard-0"} {
		pod := jsonpath(ctx, t, c, "pgshardgroup", name+"-"+g, "{.status.primary}")
		if got := psqlOn(pod, "SELECT count(*) || ':' || count(*) FILTER (WHERE note = 'before') FROM barrier_e2e"); got != "100:100" {
			t.Fatalf("%s %s: rows %s, want 100 rows from before the barrier", name, g, got)
		}
		if got := psqlOn(pod, "SELECT count(*) FROM pg_prepared_xacts"); got != "0" {
			t.Fatalf("%s %s: %s prepared transactions", name, g, got)
		}
		if got := psqlOn(pod, "SELECT pg_is_in_recovery()"); got != "f" {
			t.Fatalf("%s %s primary in recovery", name, g)
		}
	}
	catalogPod := jsonpath(ctx, t, c, "pgshardgroup", name+"-catalog", "{.status.primary}")
	if got := psqlOn(catalogPod, "SELECT write_fence FROM pgshard.shard_map_generation"); got != "f" {
		t.Fatalf("restored catalog still fenced: %s", got)
	}
	if got := psqlOn(catalogPod, "SELECT count(*) FROM pgshard.restore_points WHERE name = 'e2e-"+cluster+"'"); got != "0" {
		t.Fatalf("the restore point row is written after the barrier and must not be in the restored catalog: %s", got)
	}
	var restored pgshardv1alpha1.PgShardRestore
	if err := getJSON(ctx, c, "pgshardrestore", name, &restored); err != nil || restored.Status.Phase != pgshardv1alpha1.RestorePhaseRecovered {
		t.Fatalf("restore %+v %v", restored.Status, err)
	}
	if _, err := c.Kubectl(ctx, nil, "-n", testNamespace, "delete", "pgshardcluster", name, "--wait=true", "--timeout=4m"); err != nil {
		t.Fatal(err)
	}
}
