//go:build e2e

package upgrade

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/test/e2e"
)

const (
	testNamespace = "pgshard-e2e-upgrade"
	clusterName   = "up"
	clientPod     = "psql-client"
	appDatabase   = "app"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate source file")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// clusterManifest renders the cluster at major with a short retirement
// window so the upgrade completes within the suite.
func clusterManifest(major int, retire string) string {
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
  postgresql:
    major: %[3]d
  catalog:
    replicas: 1
    storage:
      size: 512Mi
  shards: 1
  replicasPerShard: 1
  unsafeSingleReplica: true
  storage:
    size: 512Mi
  resharding:
    retireOldGroupsAfter: %[4]s
  resources:
    requests:
      cpu: 50m
      memory: 128Mi
`, testNamespace, clusterName, major, retire)
}

func clientManifest(image string) string {
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
`, clientPod, testNamespace, image, clusterName)
}

func controllerManifest(image string) string {
	return fmt.Sprintf(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[1]s-controller
  namespace: %[2]s
  labels:
    app: %[1]s-controller
spec:
  replicas: 1
  selector:
    matchLabels:
      app: %[1]s-controller
  template:
    metadata:
      labels:
        app: %[1]s-controller
    spec:
      containers:
        - name: controller
          image: %[3]s
          imagePullPolicy: IfNotPresent
          args:
            - run
            - --listen=
            - --catalog-dsn=postgres://postgres:$(PGPASSWORD)@%[1]s-catalog-rw.%[2]s.svc:5432/postgres?sslmode=disable
            - --shard-dsn-template=postgres://postgres:$(PGPASSWORD)@%[1]s-{group}-rw.%[2]s.svc:5432/postgres?sslmode=disable
            - --subscription-dsn-template=host=%[1]s-{group}-rw.%[2]s.svc port=5432 user=postgres password=$(PGPASSWORD) dbname={db} sslmode=disable
            - --reconcile-interval=5s
            - --copy-interval=5s
            - --resolve-interval=5s
          env:
            - name: PGPASSWORD
              valueFrom:
                secretKeyRef:
                  name: %[1]s-superuser
                  key: password
`, clusterName, testNamespace, image)
}

func memberImage(major int) string {
	return fmt.Sprintf("ghcr.io/andrew01234567890/pgshard-postgres:%d", major)
}

func deployOperator(ctx context.Context, t *testing.T, c *e2e.Cluster, root, image string) {
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
	extraArgs := "            - --admin-image=" + env("ADMIN_IMAGE", "pgshard-admin:e2e") + "\n"
	if img := os.Getenv("ROUTER_IMAGE"); img != "" {
		extraArgs += "            - --router-image=" + img + "\n"
	}
	manifest = strings.Replace(manifest, "            - run\n", "            - run\n"+extraArgs, 1)
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

func psql(ctx context.Context, c *e2e.Cluster, host, db, sql string) (string, error) {
	out, err := c.Kubectl(ctx, nil, "-n", testNamespace, "exec", clientPod, "--",
		"psql", "-h", host, "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1", "-tAc", sql)
	return strings.TrimSpace(out), err
}

func catalogSQL(ctx context.Context, t *testing.T, c *e2e.Cluster, sql string) string {
	t.Helper()
	out, err := psql(ctx, c, clusterName+"-catalog-rw", "postgres", sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return out
}

func waitFor(ctx context.Context, t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(3 * time.Second):
		}
	}
}

func jsonpath(ctx context.Context, c *e2e.Cluster, kind, name, path string) string {
	out, err := c.Kubectl(ctx, nil, "-n", testNamespace, "get", kind, name, "-o", "jsonpath="+path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

func gatherNamespace(ctx context.Context, c *e2e.Cluster) {
	dir := filepath.Join(c.Artifacts, "upgrade-"+testNamespace)
	_ = os.MkdirAll(dir, 0o755)
	save := func(file string, args ...string) {
		out, err := c.Kubectl(ctx, nil, args...)
		if err != nil {
			out += "\n# error: " + err.Error() + "\n"
		}
		_ = os.WriteFile(filepath.Join(dir, file), []byte(out), 0o644)
	}
	save("cluster.yaml", "-n", testNamespace, "get", "pgshardclusters,pgshardgroups,pgshardreshards,pods,pvc,svc", "-o", "yaml")
	save("events.txt", "-n", testNamespace, "get", "events", "--sort-by=.lastTimestamp")
	pods, err := c.Kubectl(ctx, nil, "-n", testNamespace, "get", "pods", "-o", "name")
	if err == nil {
		for _, p := range strings.Fields(pods) {
			save("logs-"+strings.TrimPrefix(p, "pod/")+".txt", "-n", testNamespace, "logs", "--all-containers", p)
		}
	}
	save("operator-logs.txt", "-n", e2e.SystemNamespace, "logs", "-l", "app.kubernetes.io/name=pgshard-operator", "--tail=-1")
}

// servingShardGroup resolves the group name currently serving shard 0.
func servingShardGroup(ctx context.Context, c *e2e.Cluster) string {
	out, err := psql(ctx, c, clusterName+"-catalog-rw", "postgres",
		`SELECT s.shard_set || ':' || (SELECT max(generation) FROM pgshard.shard_sets ss WHERE ss.shard_set = s.shard_set) FROM pgshard.serving s LIMIT 1`)
	if err != nil || out == "" {
		return ""
	}
	set, gen, ok := strings.Cut(out, ":")
	if !ok {
		return ""
	}
	_ = set
	if gen == "1" {
		return "shard-0"
	}
	return "shard-0-g" + gen
}

// ledger is a background writer that appends acknowledged, uniquely
// numbered rows to the app ledger through the serving primary, retrying
// through fences and failovers. Every acknowledged id must survive the
// upgrade exactly once.
type ledger struct {
	c       *e2e.Cluster
	acked   atomic.Int64
	retries atomic.Int64
	stopped chan struct{}
	stop    context.CancelFunc
	wg      sync.WaitGroup
}

func startLedger(ctx context.Context, t *testing.T, c *e2e.Cluster) *ledger {
	t.Helper()
	lctx, cancel := context.WithCancel(ctx)
	l := &ledger{c: c, stopped: make(chan struct{}), stop: cancel}
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		defer close(l.stopped)
		next := int64(1)
		for lctx.Err() == nil {
			group := servingShardGroup(lctx, c)
			if group == "" {
				time.Sleep(time.Second)
				continue
			}
			hi := next + 24
			sql := fmt.Sprintf(`INSERT INTO ledger (id, tenant_id, amount) SELECT g, 4242, 1 FROM generate_series(%d, %d) g ON CONFLICT DO NOTHING`, next, hi)
			if _, err := psql(lctx, c, clusterName+"-"+group+"-rw", appDatabase, sql); err != nil {
				l.retries.Add(1)
				time.Sleep(2 * time.Second)
				continue
			}
			l.acked.Store(hi)
			next = hi + 1
			time.Sleep(500 * time.Millisecond)
		}
	}()
	return l
}

// finish stops the writer and returns the highest acknowledged id.
func (l *ledger) finish() int64 {
	l.stop()
	l.wg.Wait()
	return l.acked.Load()
}

// verify asserts the ledger oracle on the serving group: every
// acknowledged id present exactly once, nothing lost, nothing duplicated.
func (l *ledger) verify(ctx context.Context, t *testing.T, acked int64) {
	t.Helper()
	group := servingShardGroup(ctx, l.c)
	if group == "" {
		t.Fatal("no serving group to verify against")
	}
	got, err := psql(ctx, l.c, clusterName+"-"+group+"-rw", appDatabase,
		fmt.Sprintf(`SELECT count(*) FILTER (WHERE id <= %d) || '/' || count(DISTINCT id) FILTER (WHERE id <= %d) FROM ledger`, acked, acked))
	if err != nil {
		t.Fatalf("ledger verify: %v", err)
	}
	want := fmt.Sprintf("%d/%d", acked, acked)
	if got != want {
		t.Fatalf("ledger oracle on %s: %s, want %s (acked writes lost or duplicated)", group, got, want)
	}
}

func seedLedger(ctx context.Context, t *testing.T, c *e2e.Cluster) {
	t.Helper()
	if _, err := psql(ctx, c, clusterName+"-shard-0-rw", "postgres", "CREATE DATABASE "+appDatabase); err != nil {
		t.Fatal(err)
	}
	if _, err := psql(ctx, c, clusterName+"-shard-0-rw", appDatabase,
		"CREATE TABLE ledger (id bigint NOT NULL, tenant_id bigint NOT NULL, amount int NOT NULL, PRIMARY KEY (tenant_id, id))"); err != nil {
		t.Fatal(err)
	}
	catalogSQL(ctx, t, c, "INSERT INTO pgshard.databases (name, default_placement, home_shard) VALUES ('"+appDatabase+"', 'unsharded', 0)")
	catalogSQL(ctx, t, c, "INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('"+appDatabase+"', 'public', 'ledger', 'sharded', 'tenant_id')")
}

// bringUpCluster deploys the operator, the cluster on pg18, the client pod
// and the controller, and seeds the ledger.
func bringUpCluster(ctx context.Context, t *testing.T, c *e2e.Cluster, retire string) {
	t.Helper()
	root := repoRoot(t)
	deployOperator(ctx, t, c, root, env("OPERATOR_IMAGE", "pgshard-operator:e2e"))
	manifest := clusterManifest(18, retire)
	if err := c.Apply(ctx, manifest); err != nil {
		t.Fatal(err)
	}
	if err := c.Apply(ctx, clientManifest(memberImage(18))); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if t.Failed() {
			gatherNamespace(ctx, c)
		}
		if err := c.Delete(ctx, clusterManifest(19, retire)); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
	if _, err := c.Kubectl(ctx, nil, "-n", testNamespace, "wait", "--for=condition=Ready", "pgshardcluster/"+clusterName, "--timeout=12m"); err != nil {
		gatherNamespace(ctx, c)
		t.Fatal(err)
	}
	if err := c.WaitPodsReady(ctx, testNamespace, "app="+clientPod, 3*time.Minute); err != nil {
		t.Fatal(err)
	}
	seedLedger(ctx, t, c)
	if err := c.Apply(ctx, controllerManifest(env("CONTROLLER_IMAGE", "pgshard-controller:e2e"))); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitPodsReady(ctx, testNamespace, "app="+clusterName+"-controller", 3*time.Minute); err != nil {
		t.Fatal(err)
	}
	waitFor(ctx, t, "serving shard set stamped major 18", 3*time.Minute, func() bool {
		return catalogSQL(ctx, t, c, "SELECT coalesce(max(pg_major), 0) FROM pgshard.shard_sets WHERE state = 'serving'") == "18"
	})
}

func upgradeWorkflowState(ctx context.Context, t *testing.T, c *e2e.Cluster, set string) string {
	t.Helper()
	return catalogSQL(ctx, t, c,
		"SELECT coalesce(string_agg(state || ':' || (status->>'stage'), ','), '') FROM pgshard.workflows WHERE kind = 'upgrade' AND spec->>'shard_set' = '"+set+"'")
}
