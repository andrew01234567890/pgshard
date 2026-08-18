//go:build e2e

package operator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/test/e2e"
)

const (
	testNamespace = "pgshard-e2e-operator"
	clusterName   = "demo"
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

func clusterManifest(major string, image string) string {
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
  shards: 1
  replicasPerShard: 3
  storage:
    size: 512Mi
  resources:
    requests:
      cpu: 50m
      memory: 128Mi
`, testNamespace, clusterName, major, img)
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
	out, err := c.Kubectl(ctx, nil, "-n", testNamespace, "exec", clusterName+"-catalog-0", "--", "psql", "-h", host, "-U", "postgres", "-d", "postgres", "-tAc", sql)
	return strings.TrimSpace(out), err
}

func waitCondition(ctx context.Context, c *e2e.Cluster, cond string, timeout time.Duration) error {
	_, err := c.Kubectl(ctx, nil, "-n", testNamespace, "wait", "--for=condition="+cond, "pgshardcluster/"+clusterName, "--timeout="+timeout.String())
	return err
}

func jsonpath(ctx context.Context, t *testing.T, c *e2e.Cluster, kind, name, path string) string {
	t.Helper()
	out, err := c.Kubectl(ctx, nil, "-n", testNamespace, "get", kind, name, "-o", "jsonpath="+path)
	if err != nil {
		t.Fatal(err)
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

func TestOperatorProvisionsCatalogAndShard(t *testing.T) {
	c := e2e.NewCluster(t)
	c.GatherOnFailure(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	root := repoRoot(t)
	major := env("PG_MAJOR", "18")

	deployOperator(ctx, t, c, root, env("OPERATOR_IMAGE", "pgshard-operator:e2e"))

	manifest := clusterManifest(major, os.Getenv("PGSHARD_POSTGRES_IMAGE"))
	if err := c.Apply(ctx, manifest); err != nil {
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

	sel := "pgshard.io/cluster=" + clusterName
	if n := count(ctx, t, c, "pods", sel); n != 6 {
		t.Errorf("pods: got %d want 6", n)
	}
	if n := count(ctx, t, c, "pvc", sel); n != 6 {
		t.Errorf("pvcs: got %d want 6", n)
	}
	if n := count(ctx, t, c, "svc", sel); n != 6 {
		t.Errorf("services: got %d want 6", n)
	}
	if n := count(ctx, t, c, "pdb", sel); n != 4 {
		t.Errorf("pdbs: got %d want 4", n)
	}
	if n := count(ctx, t, c, "pgshardgroups", sel); n != 2 {
		t.Errorf("groups: got %d want 2", n)
	}

	for _, group := range []string{"catalog", "shard-0"} {
		rw := clusterName + "-" + group + "-rw"
		if out, err := psql(ctx, c, rw, "SELECT 1"); err != nil || out != "1" {
			t.Errorf("SELECT 1 via %s: %q %v", rw, out, err)
		}
		if out, err := psql(ctx, c, rw, "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'"); err != nil || out != "2" {
			t.Errorf("streaming replicas on %s: %q %v", rw, out, err)
		}
		if out, err := psql(ctx, c, rw, "SHOW synchronous_standby_names"); err != nil || !strings.HasPrefix(out, "ANY 1 (") {
			t.Errorf("synchronous_standby_names on %s: %q %v", rw, out, err)
		}
		if got := jsonpath(ctx, t, c, "pgshardgroup", clusterName+"-"+group, "{.status.primary}"); got != clusterName+"-"+group+"-0" {
			t.Errorf("group %s primary: %q", group, got)
		}
	}
	if out, err := psql(ctx, c, clusterName+"-catalog-rw", "SELECT count(*) FROM pgshard.schema_migrations"); err != nil || out == "0" || out == "" {
		t.Errorf("catalog schema migrations: %q %v", out, err)
	}
	if out, err := psql(ctx, c, clusterName+"-catalog-rw", "SELECT to_regclass('pgshard.databases') IS NOT NULL"); err != nil || out != "t" {
		t.Errorf("catalog tables: %q %v", out, err)
	}

	t.Run("ReplicaPodRecreatedWithSamePVC", func(t *testing.T) {
		victim := clusterName + "-shard-0-2"
		rw := clusterName + "-shard-0-rw"
		before, err := psql(ctx, c, rw, "SHOW synchronous_standby_names")
		if err != nil {
			t.Fatal(err)
		}
		podUID := jsonpath(ctx, t, c, "pod", victim, "{.metadata.uid}")
		pvcUID := jsonpath(ctx, t, c, "pvc", victim, "{.metadata.uid}")
		if _, err := c.Kubectl(ctx, nil, "-n", testNamespace, "delete", "pod", victim, "--wait=true"); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(3 * time.Minute)
		for {
			out, err := c.Kubectl(ctx, nil, "-n", testNamespace, "get", "pod", victim, "-o", "jsonpath={.metadata.uid}")
			if err == nil && strings.TrimSpace(out) != "" && strings.TrimSpace(out) != podUID {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("pod %s not recreated", victim)
			}
			time.Sleep(2 * time.Second)
		}
		if got := jsonpath(ctx, t, c, "pvc", victim, "{.metadata.uid}"); got != pvcUID {
			t.Fatalf("PVC uid changed %s -> %s", pvcUID, got)
		}
		if got := jsonpath(ctx, t, c, "pod", victim, "{.spec.volumes[0].persistentVolumeClaim.claimName}"); got != victim {
			t.Fatalf("recreated pod mounts %q", got)
		}
		numSync := regexp.MustCompile(`^ANY (\d+) \(`)
		during, err := psql(ctx, c, rw, "SHOW synchronous_standby_names")
		if err != nil {
			t.Fatal(err)
		}
		if numSync.FindStringSubmatch(before) == nil || numSync.FindStringSubmatch(before)[1] != numSync.FindStringSubmatch(during)[1] {
			t.Errorf("NumSync changed while a replica was down: %q -> %q", before, during)
		}
		if !strings.Contains(during, `"`+victim+`"`) {
			t.Errorf("dead replica must stay listed: %q", during)
		}
		if err := c.WaitPodsReady(ctx, testNamespace, "pgshard.io/cluster="+clusterName, 5*time.Minute); err != nil {
			t.Fatal(err)
		}
		if err := waitCondition(ctx, c, "Ready", 5*time.Minute); err != nil {
			t.Fatal(err)
		}
		if out, err := psql(ctx, c, rw, "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'"); err != nil || out != "2" {
			t.Errorf("streaming replicas after recreation: %q %v", out, err)
		}
		if _, err := strconv.Atoi(numSync.FindStringSubmatch(before)[1]); err != nil {
			t.Fatal(err)
		}
	})
}

func gatherNamespace(ctx context.Context, c *e2e.Cluster) {
	dir := filepath.Join(c.Artifacts, "operator-"+testNamespace)
	_ = os.MkdirAll(dir, 0o755)
	save := func(file string, args ...string) {
		out, err := c.Kubectl(ctx, nil, args...)
		if err != nil {
			out += "\n# error: " + err.Error() + "\n"
		}
		_ = os.WriteFile(filepath.Join(dir, file), []byte(out), 0o644)
	}
	save("cluster.yaml", "-n", testNamespace, "get", "pgshardclusters,pgshardgroups,pods,pvc,svc,pdb", "-o", "yaml")
	save("describe.txt", "-n", testNamespace, "describe", "pods")
	save("events.txt", "-n", testNamespace, "get", "events", "--sort-by=.lastTimestamp")
	pods, err := c.Kubectl(ctx, nil, "-n", testNamespace, "get", "pods", "-o", "name")
	if err == nil {
		for _, p := range strings.Fields(pods) {
			save("logs-"+strings.TrimPrefix(p, "pod/")+".txt", "-n", testNamespace, "logs", "--all-containers", p)
		}
	}
	save("operator-logs.txt", "-n", e2e.SystemNamespace, "logs", "-l", "app.kubernetes.io/name=pgshard-operator", "--tail=-1")
}
