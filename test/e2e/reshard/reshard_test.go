//go:build e2e

package reshard

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
)

const (
	testNamespace = "pgshard-e2e-reshard"
	clusterName   = "rs"
	clientPod     = "psql-client"
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

func clusterManifest(major, image string, shards int) string {
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
  postgresql:
    major: %[3]s
%[4]s  catalog:
    replicas: 3
    storage:
      size: 512Mi
  shards: %[5]d
  replicasPerShard: 3
  storage:
    size: 512Mi
  resources:
    requests:
      cpu: 50m
      memory: 128Mi
`, testNamespace, clusterName, major, img, shards)
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

func memberImage(major string) string {
	if img := os.Getenv("PGSHARD_POSTGRES_IMAGE"); img != "" {
		return img
	}
	return "ghcr.io/andrew01234567890/pgshard-postgres:" + major
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

func psql(ctx context.Context, c *e2e.Cluster, host, sql string) (string, error) {
	out, err := c.Kubectl(ctx, nil, "-n", testNamespace, "exec", clientPod, "--",
		"psql", "-h", host, "-U", "postgres", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-tAc", sql)
	return strings.TrimSpace(out), err
}

func catalogSQL(ctx context.Context, t *testing.T, c *e2e.Cluster, sql string) string {
	t.Helper()
	out, err := psql(ctx, c, clusterName+"-catalog-rw", sql)
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

func waitCondition(ctx context.Context, c *e2e.Cluster, cond string, timeout time.Duration) error {
	_, err := c.Kubectl(ctx, nil, "-n", testNamespace, "wait", "--for=condition="+cond, "pgshardcluster/"+clusterName, "--timeout="+timeout.String())
	return err
}

func jsonpath(ctx context.Context, c *e2e.Cluster, kind, name, path string) string {
	out, err := c.Kubectl(ctx, nil, "-n", testNamespace, "get", kind, name, "-o", "jsonpath="+path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

func count(ctx context.Context, t *testing.T, c *e2e.Cluster, kind, selector string) int {
	t.Helper()
	out, err := c.Kubectl(ctx, nil, "-n", testNamespace, "get", kind, "-l", selector, "-o", "name")
	if err != nil {
		t.Fatal(err)
	}
	return len(strings.Fields(out))
}

func gatherNamespace(ctx context.Context, c *e2e.Cluster) {
	dir := filepath.Join(c.Artifacts, "reshard-"+testNamespace)
	_ = os.MkdirAll(dir, 0o755)
	save := func(file string, args ...string) {
		out, err := c.Kubectl(ctx, nil, args...)
		if err != nil {
			out += "\n# error: " + err.Error() + "\n"
		}
		_ = os.WriteFile(filepath.Join(dir, file), []byte(out), 0o644)
	}
	save("cluster.yaml", "-n", testNamespace, "get", "pgshardclusters,pgshardgroups,pgshardreshards,pods,pvc,svc,pdb", "-o", "yaml")
	save("events.txt", "-n", testNamespace, "get", "events", "--sort-by=.lastTimestamp")
	pods, err := c.Kubectl(ctx, nil, "-n", testNamespace, "get", "pods", "-o", "name")
	if err == nil {
		for _, p := range strings.Fields(pods) {
			save("logs-"+strings.TrimPrefix(p, "pod/")+".txt", "-n", testNamespace, "logs", "--all-containers", p)
		}
	}
	save("operator-logs.txt", "-n", e2e.SystemNamespace, "logs", "-l", "app.kubernetes.io/name=pgshard-operator", "--tail=-1")
}

// TestReshardProvisionsTargetsAndCancels grows a one-shard cluster to two
// shards, checks the targets come up non-serving while routing stays on
// the old shard, and reverts spec.shards to tear them down again.
func TestReshardProvisionsTargetsAndCancels(t *testing.T) {
	c := e2e.NewCluster(t)
	c.GatherOnFailure(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
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

	waitFor(ctx, t, "serving shard set materialized", 2*time.Minute, func() bool {
		return jsonpath(ctx, c, "pgshardcluster", clusterName, "{.status.effectiveShards}") == "1"
	})
	if got := catalogSQL(ctx, t, c, "SELECT shard_set || ':' || generation || ':' || state FROM pgshard.shard_sets ORDER BY generation"); got != "default:1:serving" {
		t.Fatalf("shard_sets after bring-up: %q", got)
	}
	if got := catalogSQL(ctx, t, c, "SELECT count(*) FROM pgshard.shard_ranges WHERE shard_set = 'default'"); got != "1" {
		t.Fatalf("default ranges: %q", got)
	}

	if err := c.Apply(ctx, clusterManifest(major, os.Getenv("PGSHARD_POSTGRES_IMAGE"), 2)); err != nil {
		t.Fatal(err)
	}
	record := clusterName + "-reshard-g2"
	waitFor(ctx, t, "PgShardReshard record", 3*time.Minute, func() bool {
		return jsonpath(ctx, c, "pgshardreshard", record, "{.spec.targetShards}") == "2"
	})
	if got := jsonpath(ctx, c, "pgshardreshard", record, "{.spec.fromGeneration}/{.spec.targetGeneration}/{.spec.targetShardSet}"); got != "1/2/g2" {
		t.Fatalf("record spec: %q", got)
	}
	sel := "pgshard.io/cluster=" + clusterName + ",pgshard.io/shard-set=g2"
	waitFor(ctx, t, "two non-serving target groups", 3*time.Minute, func() bool {
		return count(ctx, t, c, "pgshardgroups", sel) == 2
	})
	for _, g := range []string{"shard-0-g2", "shard-1-g2"} {
		if got := jsonpath(ctx, c, "pgshardgroup", clusterName+"-"+g, "{.spec.nonServing}/{.spec.shardSet}"); got != "true/g2" {
			t.Errorf("target group %s: %q", g, got)
		}
	}
	// The kind suite runs no pgshard-controller, so the record stays Pending
	// (no workflow) unless one is deployed alongside; both are accepted.
	waitFor(ctx, t, "targets Ready", 15*time.Minute, func() bool {
		phase := jsonpath(ctx, c, "pgshardreshard", record, "{.status.phase}")
		ready := jsonpath(ctx, c, "pgshardreshard", record, `{.status.conditions[?(@.type=="TargetsReady")].status}`)
		return ready == "True" && (phase == "Pending" || phase == "Provisioning" || phase == "Copying")
	})
	if n := count(ctx, t, c, "pods", sel); n != 6 {
		t.Errorf("target pods: got %d want 6", n)
	}
	if got := catalogSQL(ctx, t, c, "SELECT string_agg(shard_set || ':' || shard_id || ':' || serving_state, ',' ORDER BY shard_set, shard_id) FROM pgshard.shard_status"); got != "default:0:serving,g2:0:provisioning,g2:1:provisioning" {
		t.Fatalf("shard_status must keep routing on the old shard only: %q", got)
	}
	if got := catalogSQL(ctx, t, c, "SELECT string_agg(shard_set, ',' ORDER BY shard_set) FROM pgshard.serving"); got != "default" {
		t.Fatalf("serving sets: %q", got)
	}
	if got := catalogSQL(ctx, t, c, "SELECT coalesce(string_agg(state || ':' || (status->>'stage'), ','), '') FROM pgshard.workflows WHERE kind = 'reshard' AND spec->>'shard_set' = 'g2'"); got != "" && got != "running:ready_for_copy" && got != "provisioning:provisioning" {
		t.Fatalf("reshard workflow: %q", got)
	}
	if got, err := psql(ctx, c, clusterName+"-shard-0-g2-rw", "SHOW archive_mode"); err != nil || got != "off" {
		t.Errorf("target archive_mode: %q %v", got, err)
	}
	if got := jsonpath(ctx, c, "pgshardcluster", clusterName, "{.status.effectiveShards}/{.status.reshard.shards}"); got != "1/2" {
		t.Errorf("cluster status: %q", got)
	}

	if err := c.Apply(ctx, clusterManifest(major, os.Getenv("PGSHARD_POSTGRES_IMAGE"), 1)); err != nil {
		t.Fatal(err)
	}
	waitFor(ctx, t, "reshard cancelled", 5*time.Minute, func() bool {
		return jsonpath(ctx, c, "pgshardreshard", record, "{.status.phase}") == "Cancelled"
	})
	waitFor(ctx, t, "target groups deleted", 5*time.Minute, func() bool {
		return count(ctx, t, c, "pgshardgroups", sel) == 0 && count(ctx, t, c, "pods", sel) == 0
	})
	if got := catalogSQL(ctx, t, c, "SELECT count(*) FROM pgshard.shard_sets WHERE shard_set = 'g2'"); got != "0" {
		t.Fatalf("pending set must be dropped: %q", got)
	}
	if got := catalogSQL(ctx, t, c, "SELECT string_agg(shard_set || ':' || shard_id || ':' || serving_state, ',') FROM pgshard.shard_status"); got != "default:0:serving" {
		t.Fatalf("shard_status after cancel: %q", got)
	}
	if err := waitCondition(ctx, c, "Ready", 5*time.Minute); err != nil {
		t.Fatal(err)
	}
}
