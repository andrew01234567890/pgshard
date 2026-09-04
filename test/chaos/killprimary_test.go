//go:build chaos

package chaos

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/test/e2e"
	"github.com/andrew01234567890/pgshard/test/e2e/workload"
)

const (
	killNamespace = "pgshard-chaos-kill"
	killCluster   = "kp"
	killClientPod = "psql-client"
	ledgerTable   = "ledger"
)

var killTenants = []int64{11, 22, 33}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate source file")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// killClusterManifest differs from the e2e suites' clusters in the one way
// that matters here: this is a real HA cluster rather than a
// unsafeSingleReplica one. A single-replica group cannot fail over, and a
// kill-primary experiment against one would only ever measure how long a
// rebuild takes. The CRD enforces the same thing -- replicasPerShard and
// catalog.replicas below 3 are refused for HA -- so the shape is not a
// choice this test gets to make.
func killClusterManifest(major, image string) string {
	img := ""
	if image != "" {
		img = "    image: " + image + "\n"
	}
	return fmt.Sprintf(`
apiVersion: v1
kind: Namespace
metadata:
  name: %[1]s
---
apiVersion: pgshard.io/v1alpha1
kind: PgShardCluster
metadata:
  name: %[2]s
  namespace: %[1]s
spec:
  internalTLS:
    insecure: true
  postgresql:
    major: %[3]s
%[4]s  catalog:
    replicas: 3
    storage:
      size: 512Mi
  shards: 1
  replicasPerShard: 3
  storage:
    size: 512Mi
  resources:
    requests:
      cpu: 50m
      memory: 128Mi
`, killNamespace, killCluster, major, img)
}

func killClientManifest(image string) string {
	return fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %[1]s
  namespace: %[2]s
  labels:
    app: %[1]s
spec:
  securityContext:
    runAsUser: 999
    runAsGroup: 999
  containers:
    - name: psql
      image: %[3]s
      command: ["sleep", "infinity"]
      env:
        - name: PGPASSWORD
          valueFrom:
            secretKeyRef:
              name: %[4]s-superuser
              key: password
        - name: PGCONNECT_TIMEOUT
          value: "5"
`, killClientPod, killNamespace, image, killCluster)
}

// killPrimaryChaos kills whichever pod currently wears the primary label
// for shard 0. Selecting by ROLE rather than by pod name is deliberate:
// the name is only known at apply time, and a manifest naming one would
// silently miss after any earlier promotion.
func killPrimaryChaos() string {
	return fmt.Sprintf(`
apiVersion: chaos-mesh.org/v1alpha1
kind: PodChaos
metadata:
  name: kill-shard-primary
  namespace: %[1]s
spec:
  action: pod-kill
  mode: one
  selector:
    namespaces:
      - %[1]s
    labelSelectors:
      pgshard.io/cluster: %[2]s
      pgshard.io/group: shard-0
      pgshard.io/role: primary
`, killNamespace, killCluster)
}

func kpsql(ctx context.Context, c *e2e.Cluster, host, sql string) (string, error) {
	out, err := c.Kubectl(ctx, nil, "-n", killNamespace, "exec", killClientPod, "--",
		"psql", "-h", host, "-U", "postgres", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-tAc", sql)
	return strings.TrimSpace(out), err
}

func kpsqlDB(ctx context.Context, c *e2e.Cluster, host, db, sql string) (string, error) {
	out, err := c.Kubectl(ctx, nil, "-n", killNamespace, "exec", killClientPod, "--",
		"psql", "-h", host, "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1", "-tAc", sql)
	return strings.TrimSpace(out), err
}

func primaryPod(ctx context.Context, c *e2e.Cluster) string {
	out, err := c.Kubectl(ctx, nil, "-n", killNamespace, "get", "pod",
		"-l", "pgshard.io/cluster="+killCluster+",pgshard.io/group=shard-0,pgshard.io/role=primary",
		"-o", "jsonpath={.items[*].metadata.name}")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

func waitUntil(ctx context.Context, t *testing.T, what string, timeout time.Duration, why func() string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s after %s%s", what, timeout, why())
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context ended waiting for %s%s", what, why())
		case <-time.After(3 * time.Second):
		}
	}
}

const (
	ledgerRole     = "ledger_writer"
	ledgerPassword = "ledger-pw"
	appDatabase    = "app"
)

func kpRouterSQL(ctx context.Context, c *e2e.Cluster, sql string) error {
	_, err := c.Kubectl(ctx, nil, "-n", killNamespace, "exec", killClientPod, "--",
		"env", "PGPASSWORD="+ledgerPassword,
		"psql", "-h", killCluster+"-router", "-U", ledgerRole, "-d", appDatabase,
		"-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=verbose", "-tAc", sql)
	return err
}

func kpCatalogSQL(ctx context.Context, t *testing.T, c *e2e.Cluster, sql string) string {
	t.Helper()
	out, err := kpsql(ctx, c, killCluster+"-catalog-rw", sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return out
}

func kpDeployOperator(ctx context.Context, t *testing.T, c *e2e.Cluster, root, image string) {
	t.Helper()
	for _, f := range []string{"config/crd/bases", "config/manager/namespace.yaml", "config/rbac"} {
		if _, err := c.Kubectl(ctx, nil, "apply", "-f", filepath.Join(root, f)); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile(filepath.Join(root, "config/manager/manager.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	manifest := strings.Replace(string(raw), "image: ghcr.io/andrew01234567890/pgshard-operator:latest", "image: "+image, 1)
	extra := "            - --admin-image=" + env("ADMIN_IMAGE", "pgshard-admin:e2e") + "\n"
	extra += "            - --controller-image=" + env("CONTROLLER_IMAGE", "pgshard-controller:e2e") + "\n"
	if img := os.Getenv("ROUTER_IMAGE"); img != "" {
		extra += "            - --router-image=" + img + "\n"
	}
	manifest = strings.Replace(manifest, "            - run\n", "            - run\n"+extra, 1)
	if err := c.Apply(ctx, manifest); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		_ = c.Delete(ctx, manifest)
		for _, f := range []string{"config/rbac", "config/manager/namespace.yaml", "config/crd/bases"} {
			_, _ = c.Kubectl(ctx, nil, "delete", "--ignore-not-found", "--wait=true", "-f", filepath.Join(root, f))
		}
	})
	if err := c.WaitPodsReady(ctx, e2e.SystemNamespace, "app.kubernetes.io/name=pgshard-operator", 3*time.Minute); err != nil {
		t.Fatal(err)
	}
}

// TestKillPrimaryLosesNoAcknowledgedCommit is the chaos suite's first real
// experiment, and the shape the rest are meant to follow.
//
// The assertion is about the INVARIANT, not about liveness. A test that
// waited for the pods to come back would pass against a cluster that came
// back having lost data, which is the failure worth catching and the one a
// recovery check cannot see. So the workload records what the system
// ACKNOWLEDGED, the fault is injected while it writes, and afterwards every
// acknowledged row must still exist exactly once.
//
// Two guards keep it from passing vacuously, because a chaos test that
// injects nothing looks exactly like one whose system survived:
//   - the primary pod name must CHANGE, proving the kill landed and a
//     promotion followed rather than the pod being restarted in place;
//   - the workload must acknowledge writes AFTER the failover as well as
//     before, proving it wrote through the event rather than stopping at it.
func TestKillPrimaryLosesNoAcknowledgedCommit(t *testing.T) {
	c := e2e.NewCluster(t)
	c.GatherOnFailure(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	root := repoRoot(t)
	major := env("PG_MAJOR", "18")
	kpDeployOperator(ctx, t, c, root, env("OPERATOR_IMAGE", "pgshard-operator:e2e"))

	// A namespace still terminating from an earlier run refuses new
	// content in it, so the apply fails with Forbidden rather than
	// waiting. Deleting and waiting first makes a re-run behave the same
	// as a first run, which matters most when the previous run is the one
	// that just failed.
	freshNamespace(ctx, t, c)

	manifest := killClusterManifest(major, env("PGSHARD_POSTGRES_IMAGE",
		"ghcr.io/andrew01234567890/pgshard-postgres:"+major))
	if err := c.Apply(ctx, manifest); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		_ = c.Delete(ctx, manifest)
	})
	if err := c.WaitPodsReady(ctx, killNamespace, "pgshard.io/cluster="+killCluster, 12*time.Minute); err != nil {
		t.Fatalf("cluster never became ready: %v\n%s", err, c.Summary(ctx, killNamespace))
	}
	if err := c.Apply(ctx, killClientManifest(env("PGSHARD_POSTGRES_IMAGE",
		"ghcr.io/andrew01234567890/pgshard-postgres:"+major))); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitPodsReady(ctx, killNamespace, "app="+killClientPod, 3*time.Minute); err != nil {
		t.Fatalf("client pod not ready: %v", err)
	}

	// Seed: the ledger lives on shard 0 and is registered sharded by
	// tenant_id, so the router routes each stream by its key.
	if _, err := kpsql(ctx, c, killCluster+"-shard-0-rw", "CREATE DATABASE "+appDatabase); err != nil {
		t.Fatal(err)
	}
	if _, err := kpsqlDB(ctx, c, killCluster+"-shard-0-rw", appDatabase,
		"CREATE TABLE "+ledgerTable+" (id bigint NOT NULL, tenant_id bigint NOT NULL, amount int NOT NULL, PRIMARY KEY (tenant_id, id))"); err != nil {
		t.Fatal(err)
	}
	if _, err := kpsql(ctx, c, killCluster+"-catalog-rw",
		"SET password_encryption = 'scram-sha-256'; CREATE ROLE "+ledgerRole+" LOGIN PASSWORD '"+ledgerPassword+"'"); err != nil {
		t.Fatal(err)
	}
	verifier, err := kpsql(ctx, c, killCluster+"-catalog-rw",
		"SELECT rolpassword FROM pg_authid WHERE rolname = '"+ledgerRole+"'")
	if err != nil {
		t.Fatal(err)
	}
	// Order matters: pgshard.grants and pgshard.tables both carry a
	// foreign key to pgshard.databases, so the database is registered
	// before anything refers to it.
	kpCatalogSQL(ctx, t, c, "INSERT INTO pgshard.databases (name, default_placement, home_shard) VALUES ('"+appDatabase+"', 'unsharded', 0)")
	kpCatalogSQL(ctx, t, c, "INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES "+
		"('"+appDatabase+"', 'public', '"+ledgerTable+"', 'sharded', 'tenant_id')")
	kpCatalogSQL(ctx, t, c, "INSERT INTO pgshard.roles (rolname, verifier, login) VALUES ('"+ledgerRole+"', '"+verifier+"', true)")
	kpCatalogSQL(ctx, t, c, "INSERT INTO pgshard.grants (rolname, database, object_kind, object_schema, object_name, privileges) VALUES "+
		"('"+ledgerRole+"', '"+appDatabase+"', 'database', '', '"+appDatabase+"', ARRAY['CONNECT']), "+
		"('"+ledgerRole+"', '"+appDatabase+"', 'schema', '', 'public', ARRAY['USAGE']), "+
		"('"+ledgerRole+"', '"+appDatabase+"', 'table', 'public', '"+ledgerTable+"', ARRAY['SELECT','INSERT'])")

	load := &workload.AckedLedger{
		Streams: killTenants,
		Table:   ledgerTable,
		Batch:   5,
		Retry:   2 * time.Second,
		Pace:    500 * time.Millisecond,
		Exec:    func(ctx context.Context, sql string) error { return kpRouterSQL(ctx, c, sql) },
	}
	load.Start(ctx)
	defer load.Finish()

	waitUntil(ctx, t, "the first acknowledged writes on every stream", 5*time.Minute, load.Why, func() bool {
		for _, n := range load.Acked() {
			if n == 0 {
				return false
			}
		}
		return true
	})
	before := load.Acked()
	victim := primaryPod(ctx, c)
	if victim == "" {
		t.Fatal("shard-0 has no pod labelled primary; nothing to kill")
	}
	t.Logf("acknowledged before the kill: %v; primary is %s", before, victim)

	chaosManifest := killPrimaryChaos()
	if err := c.Apply(ctx, chaosManifest); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_ = c.Delete(ctx, chaosManifest)
	})

	// The fault landed and a promotion followed: a different pod wears the
	// primary label. Waiting for "a primary exists" would be satisfied by
	// the original pod never having been killed at all.
	waitUntil(ctx, t, "a different pod to wear the primary label", 10*time.Minute,
		func() string { return "\nstill primary: " + primaryPod(ctx, c) + "\n" + c.Summary(ctx, killNamespace) },
		func() bool {
			now := primaryPod(ctx, c)
			return now != "" && now != victim
		})
	t.Logf("promoted: %s -> %s", victim, primaryPod(ctx, c))

	// The workload wrote THROUGH the failover, not just up to it.
	waitUntil(ctx, t, "acknowledged writes after the failover", 10*time.Minute, load.Why, func() bool {
		now := load.Acked()
		for i := range now {
			if now[i] <= before[i] {
				return false
			}
		}
		return true
	})

	acked := load.Finish()
	t.Logf("acknowledged in total: %v", acked)
	if load.Attempts() == 0 {
		t.Fatal("the workload made no attempts; the run proves nothing")
	}

	// One group is swept because this cluster has one shard and never
	// reshards, so shard-0 is the only place a ledger row can be. An
	// experiment that moves ranges must sweep every group instead --
	// uniqueness is per shard, so a row on its owner AND somewhere else
	// is invisible from the owner alone. workload.Counter is shaped for
	// that; this call site is the narrow case, not the general one.
	vs, err := workload.Verify(ctx, killTenants, acked, func(ctx context.Context, stream, high int64) (workload.Counted, error) {
		out, err := kpsqlDB(ctx, c, killCluster+"-shard-0-rw", appDatabase, fmt.Sprintf(
			`SELECT count(*) FILTER (WHERE id <= %d) || ' ' || count(DISTINCT id) FILTER (WHERE id <= %d)
			   FROM `+ledgerTable+` WHERE tenant_id = %d`, high, high, stream))
		if err != nil {
			return workload.Counted{}, err
		}
		var rows, distinct int64
		if _, serr := fmt.Sscanf(out, "%d %d", &rows, &distinct); serr != nil {
			return workload.Counted{}, fmt.Errorf("unexpected count %q: %w", out, serr)
		}
		return workload.Counted{Rows: rows, Distinct: distinct, High: high}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range vs {
		t.Errorf("invariant violated: %s", v)
	}
}

// freshNamespace removes the test namespace and waits for it to be gone.
func freshNamespace(ctx context.Context, t *testing.T, c *e2e.Cluster) {
	t.Helper()
	if _, err := c.Kubectl(ctx, nil, "delete", "namespace", killNamespace, "--ignore-not-found", "--wait=true", "--timeout=5m"); err != nil {
		t.Logf("deleting %s: %v", killNamespace, err)
	}
	waitUntil(ctx, t, "the previous namespace to be gone", 5*time.Minute,
		func() string { return "" },
		func() bool {
			out, err := c.Kubectl(ctx, nil, "get", "namespace", killNamespace, "-o", "jsonpath={.status.phase}")
			return err != nil || strings.TrimSpace(out) == ""
		})
}
