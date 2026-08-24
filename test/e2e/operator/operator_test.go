//go:build e2e

package operator

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
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

const clientPod = "psql-client"

// clientManifest is a long-lived pod with psql from the member image; the
// tests drive SQL through it so member restarts never take the client down.
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

// psql runs sql against host from the client pod.
func psql(ctx context.Context, c *e2e.Cluster, host, sql string) (string, error) {
	out, err := c.Kubectl(ctx, nil, "-n", testNamespace, "exec", clientPod, "--",
		"psql", "-h", host, "-U", "postgres", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-tAc", sql)
	return strings.TrimSpace(out), err
}

// psqlRetry repeats psql with backoff until it succeeds or timeout elapses;
// Services briefly lose their endpoints while members restart.
func psqlRetry(ctx context.Context, c *e2e.Cluster, host, sql string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	delay := time.Second
	for {
		out, err := psql(ctx, c, host, sql)
		if err == nil || time.Now().After(deadline) {
			return out, err
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(delay):
		}
		if delay < 10*time.Second {
			delay *= 2
		}
	}
}

// writer inserts sequential ids through the shard -rw Service and keeps
// every acknowledged id plus the window in which writes failed.
type writer struct {
	c    *e2e.Cluster
	rw   string
	stop chan struct{}
	done chan struct{}

	mu         sync.Mutex
	acked      []int64
	failures   int
	firstFail  time.Time
	lastFail   time.Time
	lastAckAt  time.Time
	firstAckAf time.Time
}

func startWriter(ctx context.Context, c *e2e.Cluster, rw string, start int64) *writer {
	w := &writer{c: c, rw: rw, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		id := start
		for {
			select {
			case <-w.stop:
				return
			case <-ctx.Done():
				return
			default:
			}
			id++
			out, err := psql(ctx, c, rw, fmt.Sprintf("INSERT INTO ha_writes (id) VALUES (%d)", id))
			w.mu.Lock()
			if err == nil && strings.Contains(out, "INSERT 0 1") {
				w.acked = append(w.acked, id)
				w.lastAckAt = time.Now()
				if !w.firstFail.IsZero() && w.firstAckAf.IsZero() {
					w.firstAckAf = time.Now()
				}
			} else {
				w.failures++
				if w.firstFail.IsZero() {
					w.firstFail = time.Now()
				}
				w.lastFail = time.Now()
			}
			w.mu.Unlock()
		}
	}()
	return w
}

func (w *writer) finish() ([]int64, int, time.Duration) {
	close(w.stop)
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	var pause time.Duration
	if !w.firstFail.IsZero() {
		end := w.lastFail
		if !w.firstAckAf.IsZero() {
			end = w.firstAckAf
		}
		pause = end.Sub(w.firstFail)
	}
	return append([]int64(nil), w.acked...), w.failures, pause
}

func waitFor(ctx context.Context, t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	started := time.Now()
	deadline := started.Add(timeout)
	nextProgress := started.Add(time.Minute)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		if time.Now().After(nextProgress) {
			t.Logf("still waiting for %s (%s elapsed)", what, time.Since(started).Round(time.Second))
			nextProgress = time.Now().Add(time.Minute)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

func assertAllAcked(ctx context.Context, t *testing.T, c *e2e.Cluster, rw string, acked []int64) {
	t.Helper()
	out, err := psqlRetry(ctx, c, rw, "SELECT string_agg(id::text, ',' ORDER BY id) FROM ha_writes", 3*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	present := map[string]bool{}
	for _, id := range strings.Split(out, ",") {
		present[id] = true
	}
	var missing []int64
	for _, id := range acked {
		if !present[strconv.FormatInt(id, 10)] {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("%d acknowledged commits lost: %v", len(missing), missing)
	}
	t.Logf("%d acknowledged commits, all present after the role change", len(acked))
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
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Minute)
	defer cancel()
	root := repoRoot(t)
	major := env("PG_MAJOR", "18")

	deployOperator(ctx, t, c, root, env("OPERATOR_IMAGE", "pgshard-operator:e2e"))

	manifest := clusterManifest(major, os.Getenv("PGSHARD_POSTGRES_IMAGE"))
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

	sel := "pgshard.io/cluster=" + clusterName
	if n := count(ctx, t, c, "pods", sel+",pgshard.io/group"); n != 6 {
		t.Errorf("member pods: got %d want 6", n)
	}
	if n := count(ctx, t, c, "pvc", sel); n != 6 {
		t.Errorf("pvcs: got %d want 6", n)
	}
	if n := count(ctx, t, c, "svc", sel+",pgshard.io/group"); n != 6 {
		t.Errorf("group services: got %d want 6", n)
	}
	if n := count(ctx, t, c, "pdb", sel+",pgshard.io/group"); n != 4 {
		t.Errorf("group pdbs: got %d want 4", n)
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

	t.Run("PoolerSidecarReadyInEveryMemberPod", func(t *testing.T) {
		out, err := c.Kubectl(ctx, nil, "-n", testNamespace, "get", "pods", "-l", sel+",pgshard.io/group", "-o",
			`jsonpath={range .items[*]}{.metadata.name}{" "}{range .status.containerStatuses[?(@.name=="pooler")]}{.ready}{end}{"\n"}{end}`)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimSpace(out), "\n")
		if len(lines) != 6 {
			t.Fatalf("member pods: %q", out)
		}
		for _, l := range lines {
			if !strings.HasSuffix(l, " true") {
				t.Errorf("pooler container not ready: %q", l)
			}
		}
	})

	t.Run("RouterDeploymentAndHPA", func(t *testing.T) {
		rsel := sel + ",pgshard.io/component=router"
		for kind, want := range map[string]int{"deployment": 1, "hpa": 1, "pdb": 1, "svc": 1, "serviceaccount": 1} {
			if n := count(ctx, t, c, kind, rsel); n != want {
				t.Errorf("router %s: got %d want %d", kind, n, want)
			}
		}
		if got := jsonpath(ctx, t, c, "hpa", clusterName+"-router", "{.spec.minReplicas}/{.spec.maxReplicas}"); got != "2/10" {
			t.Errorf("router hpa bounds: %q", got)
		}
		if got := jsonpath(ctx, t, c, "deployment", clusterName+"-router", "{.spec.replicas}"); got != "2" {
			t.Errorf("router deployment replicas: %q", got)
		}
		if err := c.WaitPodsReady(ctx, testNamespace, rsel, 5*time.Minute); err != nil {
			reasons, _ := c.Kubectl(ctx, nil, "-n", testNamespace, "get", "pods", "-l", rsel, "-o", "jsonpath={.items[*].status.containerStatuses[*].state.waiting.reason}")
			t.Fatalf("router pods not ready (%s): %v", reasons, err)
		}
	})

	t.Run("AdminUIServesTopology", func(t *testing.T) {
		if err := c.WaitPodsReady(ctx, testNamespace, "pgshard.io/cluster="+clusterName+",pgshard.io/component=admin", 3*time.Minute); err != nil {
			t.Fatal(err)
		}
		if n := count(ctx, t, c, "svc", "pgshard.io/cluster="+clusterName+",pgshard.io/component=admin"); n != 1 {
			t.Errorf("admin services: got %d want 1", n)
		}
		base, stop, err := c.PortForward(ctx, testNamespace, clusterName+"-admin", 8081)
		if err != nil {
			t.Fatal(err)
		}
		defer stop()
		httpGet := func(path string) (int, string) {
			resp, err := http.Get(base + path)
			if err != nil {
				t.Fatalf("GET %s: %v", path, err)
			}
			defer func() { _ = resp.Body.Close() }()
			body, _ := io.ReadAll(resp.Body)
			return resp.StatusCode, string(body)
		}
		if code, body := httpGet("/"); code != http.StatusOK || !strings.Contains(body, clusterName) {
			t.Errorf("GET /: %d %.400s", code, body)
		}
		if code, body := httpGet("/clusters/" + testNamespace + "/" + clusterName); code != http.StatusOK || !strings.Contains(body, clusterName+"-shard-0-0") {
			t.Errorf("GET cluster page: %d %.400s", code, body)
		}
		if code, body := httpGet("/api/v1/clusters/" + testNamespace + "/" + clusterName); code != http.StatusOK ||
			!strings.Contains(body, `"name": "`+clusterName+`"`) || !strings.Contains(body, `"primary": "`+clusterName+`-shard-0-0"`) {
			t.Errorf("GET admin JSON: %d %.400s", code, body)
		}
	})

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
		// Member pods only: router pods are covered by their own subtest and may
		// lack an image in some environments.
		if err := c.WaitPodsReady(ctx, testNamespace, "pgshard.io/cluster="+clusterName+",pgshard.io/group", 5*time.Minute); err != nil {
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

	group := clusterName + "-shard-0"
	rw := group + "-rw"
	epochOf := func() int64 {
		v := jsonpath(ctx, t, c, "pgshardgroup", group, "{.status.epoch}")
		if v == "" {
			return 0
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	primaryOf := func() string { return jsonpath(ctx, t, c, "pgshardgroup", group, "{.status.primary}") }
	primaryPods := func() int {
		return count(ctx, t, c, "pods", "pgshard.io/cluster="+clusterName+",pgshard.io/group=shard-0,pgshard.io/role=primary")
	}
	assertAgentPID1 := func(pod string) {
		t.Helper()
		if got := jsonpath(ctx, t, c, "pod", pod, "{.spec.containers[0].command[0]} {.spec.containers[0].readinessProbe.httpGet.path} {.spec.containers[0].startupProbe.httpGet.path} {.spec.containers[0].livenessProbe.httpGet.path}"); got != "pgshard-agent /readyz /startz /livez" {
			t.Fatalf("pod %s must run the agent as PID 1 with HTTP probes, got %q", pod, got)
		}
	}
	assertAgentPID1(group + "-0")
	if _, err := psql(ctx, c, rw, "CREATE TABLE IF NOT EXISTS ha_writes (id bigint PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	var lastID int64

	t.Run("PrimaryPodDeletionFailsOverWithoutLosingAcknowledgedCommits", func(t *testing.T) {
		oldPrimary := primaryOf()
		oldEpoch := epochOf()
		if oldPrimary != group+"-0" || oldEpoch != 0 {
			t.Fatalf("precondition: primary=%s epoch=%d", oldPrimary, oldEpoch)
		}
		w := startWriter(ctx, c, rw, lastID)
		time.Sleep(5 * time.Second)
		if _, err := c.Kubectl(ctx, nil, "-n", testNamespace, "delete", "pod", oldPrimary, "--wait=false"); err != nil {
			t.Fatal(err)
		}
		waitFor(ctx, t, "promotion of a standby", 4*time.Minute, func() bool {
			return primaryOf() != oldPrimary && epochOf() == oldEpoch+1
		})
		newPrimary := primaryOf()
		t.Logf("failover: %s -> %s at epoch %d", oldPrimary, newPrimary, epochOf())
		waitFor(ctx, t, "writes to resume on the new primary", 3*time.Minute, func() bool {
			out, err := psql(ctx, c, rw, "SELECT pg_is_in_recovery()")
			return err == nil && out == "f"
		})
		time.Sleep(5 * time.Second)
		acked, failures, pause := w.finish()
		t.Logf("writer: %d acknowledged, %d failed, unavailability window %s", len(acked), failures, pause)
		if len(acked) > 0 {
			lastID = acked[len(acked)-1]
		}
		if len(acked) == 0 {
			t.Fatal("writer acknowledged nothing")
		}
		if got := epochOf(); got != oldEpoch+1 {
			t.Fatalf("epoch must increase by exactly one, got %d", got)
		}
		if err := waitCondition(ctx, c, "Ready", 8*time.Minute); err != nil {
			gatherNamespace(ctx, c)
			t.Fatal(err)
		}
		if n := primaryPods(); n != 1 {
			t.Fatalf("exactly one pod may carry role=primary, got %d", n)
		}
		if got := jsonpath(ctx, t, c, "pod", newPrimary, "{.metadata.labels.pgshard\\.io/role}"); got != "primary" {
			t.Fatalf("new primary pod label %q", got)
		}
		if got := jsonpath(ctx, t, c, "lease", group+"-primary", "{.spec.holderIdentity} {.metadata.annotations.pgshard\\.io/primary-epoch}"); got != newPrimary+" 1" {
			t.Fatalf("lease must be held by the new primary with the epoch published, got %q", got)
		}
		assertAllAcked(ctx, t, c, rw, acked)
		if out, err := psql(ctx, c, clusterName+"-catalog-rw", "SELECT primary_epoch, primary_endpoint FROM pgshard.shard_status WHERE shard_set = 'default' AND shard_id = 0"); err != nil || !strings.HasPrefix(out, "1|"+newPrimary+".") {
			t.Fatalf("catalog shard_status fence: %q %v", out, err)
		}
		waitFor(ctx, t, "old primary to rejoin as a streaming standby", 5*time.Minute, func() bool {
			out, err := psql(ctx, c, rw, "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming' AND application_name = '"+oldPrimary+"'")
			return err == nil && out == "1"
		})
		if got := jsonpath(ctx, t, c, "pod", oldPrimary, "{.metadata.labels.pgshard\\.io/role}"); got != "replica" {
			t.Fatalf("old primary must be relabelled replica once it streams, got %q", got)
		}
		assertAgentPID1(oldPrimary)
		if out, err := psql(ctx, c, rw, "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'"); err != nil || out != "2" {
			t.Fatalf("streaming replicas after failover: %q %v", out, err)
		}
	})

	t.Run("SwitchoverAnnotationPromotesTargetWithZeroLostCommits", func(t *testing.T) {
		oldPrimary := primaryOf()
		oldEpoch := epochOf()
		target := group + "-0"
		if oldPrimary == target {
			target = group + "-1"
		}
		w := startWriter(ctx, c, rw, lastID)
		time.Sleep(5 * time.Second)
		started := time.Now()
		if _, err := c.Kubectl(ctx, nil, "-n", testNamespace, "annotate", "pgshardcluster", clusterName, "pgshard.io/switchover="+target); err != nil {
			t.Fatal(err)
		}
		waitFor(ctx, t, "switchover to "+target, 4*time.Minute, func() bool {
			return primaryOf() == target && epochOf() == oldEpoch+1
		})
		waitFor(ctx, t, "writes to resume on the target", 3*time.Minute, func() bool {
			out, err := psql(ctx, c, rw, "SELECT pg_is_in_recovery()")
			return err == nil && out == "f"
		})
		time.Sleep(5 * time.Second)
		acked, failures, pause := w.finish()
		t.Logf("switchover %s -> %s took %s from annotation to promotion; writer: %d acknowledged, %d failed, unavailability window %s", oldPrimary, target, time.Since(started).Round(time.Second), len(acked), failures, pause)
		if len(acked) > 0 {
			lastID = acked[len(acked)-1]
		}
		if failures > 0 && pause > 90*time.Second {
			t.Fatalf("write failures must be confined to a short pause, got %s", pause)
		}
		if got := jsonpath(ctx, t, c, "pgshardcluster", clusterName, "{.metadata.annotations.pgshard\\.io/switchover}"); got != "" {
			t.Fatalf("switchover annotation must be cleared, got %q", got)
		}
		if err := waitCondition(ctx, c, "Ready", 8*time.Minute); err != nil {
			gatherNamespace(ctx, c)
			t.Fatal(err)
		}
		if n := primaryPods(); n != 1 {
			t.Fatalf("exactly one pod may carry role=primary, got %d", n)
		}
		assertAllAcked(ctx, t, c, rw, acked)
		waitFor(ctx, t, "old primary to rejoin as a streaming standby", 5*time.Minute, func() bool {
			out, err := psql(ctx, c, rw, "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming' AND application_name = '"+oldPrimary+"'")
			return err == nil && out == "1"
		})
	})

	memberSel := "pgshard.io/cluster=" + clusterName + ",pgshard.io/group"
	podUIDs := func() map[string]string {
		out, err := c.Kubectl(ctx, nil, "-n", testNamespace, "get", "pods", "-l", memberSel, "-o", `jsonpath={range .items[*]}{.metadata.name}={.metadata.uid}{"\n"}{end}`)
		if err != nil {
			t.Fatal(err)
		}
		uids := map[string]string{}
		for _, l := range strings.Fields(out) {
			name, uid, _ := strings.Cut(l, "=")
			uids[name] = uid
		}
		return uids
	}
	patchCluster := func(patch string) {
		t.Helper()
		if _, err := c.Kubectl(ctx, nil, "-n", testNamespace, "patch", "pgshardcluster", clusterName, "--type=merge", "-p", patch); err != nil {
			t.Fatal(err)
		}
	}
	rolloutIdle := func() bool {
		return jsonpath(ctx, t, c, "pgshardcluster", clusterName, `{.status.conditions[?(@.type=="RolloutInProgress")].status}`) == "False" &&
			jsonpath(ctx, t, c, "pgshardcluster", clusterName, `{.status.conditions[?(@.type=="Ready")].status}`) == "True"
	}
	ro := group + "-ro"

	t.Run("SighupParameterChangeReloadsWithoutRestart", func(t *testing.T) {
		before := podUIDs()
		if len(before) != 6 {
			t.Fatalf("member pods: %v", before)
		}
		patchCluster(`{"spec":{"postgresql":{"parameters":{"log_min_duration_statement":"250ms"}}}}`)
		waitFor(ctx, t, "log_min_duration_statement to reach the primary and a standby", 6*time.Minute, func() bool {
			p, err1 := psql(ctx, c, rw, "SHOW log_min_duration_statement")
			s, err2 := psql(ctx, c, ro, "SHOW log_min_duration_statement")
			return err1 == nil && err2 == nil && p == "250ms" && s == "250ms"
		})
		waitFor(ctx, t, "rollout to go idle", 4*time.Minute, rolloutIdle)
		after := podUIDs()
		for name, uid := range before {
			if after[name] != uid {
				t.Errorf("pod %s was restarted for a sighup setting", name)
			}
		}
		if got := jsonpath(ctx, t, c, "pgshardcluster", clusterName, "{.status.rollout.phase}"); got != "Idle" {
			t.Errorf("status.rollout.phase %q", got)
		}
	})

	t.Run("RestartRequiredParameterChangeRollsMembersWithOneSwitchover", func(t *testing.T) {
		before := podUIDs()
		oldEpoch := epochOf()
		oldPrimary := primaryOf()
		w := startWriter(ctx, c, rw, lastID)
		// Sample how many shard members are being restarted at once (pod
		// missing, or replaced and its agent not yet past the startup probe);
		// a rolling restart must never have more than one in that state. A
		// member that briefly reports not-Ready while it reconnects to a new
		// primary has long passed startup and does not count.
		maxAway := 0
		stopSampling := make(chan struct{})
		sampled := make(chan struct{})
		go func() {
			defer close(sampled)
			for {
				select {
				case <-stopSampling:
					return
				case <-time.After(2 * time.Second):
				}
				out, err := c.Kubectl(ctx, nil, "-n", testNamespace, "get", "pods", "-l", "pgshard.io/cluster="+clusterName+",pgshard.io/group=shard-0", "-o",
					`jsonpath={range .items[*]}{.metadata.name}={.metadata.uid}:{.status.containerStatuses[?(@.name=="postgres")].started}{" "}{end}`)
				if err != nil {
					continue
				}
				fields := strings.Fields(out)
				away := 3 - len(fields)
				for _, f := range fields {
					name, rest, _ := strings.Cut(f, "=")
					uid, started, _ := strings.Cut(rest, ":")
					if uid != before[name] && started != "true" {
						away++
					}
				}
				if away > maxAway {
					maxAway = away
				}
			}
		}()
		time.Sleep(5 * time.Second)
		started := time.Now()
		patchCluster(`{"spec":{"postgresql":{"parameters":{"max_connections":"150"}}}}`)
		restartedAt := map[string]time.Time{}
		waitFor(ctx, t, "every member to restart with max_connections=150", 30*time.Minute, func() bool {
			after := podUIDs()
			pending := 0
			for name, uid := range before {
				if after[name] == "" || after[name] == uid {
					pending++
					continue
				}
				if _, seen := restartedAt[name]; !seen {
					restartedAt[name] = time.Now()
					t.Logf("member %s replaced after %s", name, time.Since(started).Round(time.Second))
				}
			}
			if pending > 0 || len(after) != 6 {
				return false
			}
			p, err := psql(ctx, c, rw, "SHOW max_connections")
			if err != nil {
				t.Logf("all members replaced; %s not serving yet: %v", rw, err)
				return false
			}
			return p == "150" && rolloutIdle()
		})
		time.Sleep(5 * time.Second)
		close(stopSampling)
		<-sampled
		acked, failures, pause := w.finish()
		t.Logf("rolling restart took %s; writer: %d acknowledged, %d failed, unavailability window %s; max shard members away at once %d",
			time.Since(started).Round(time.Second), len(acked), failures, pause, maxAway)
		if len(acked) > 0 {
			lastID = acked[len(acked)-1]
		}
		if len(acked) == 0 {
			t.Fatal("writer acknowledged nothing")
		}
		if got := epochOf(); got != oldEpoch+1 {
			t.Fatalf("exactly one switchover: epoch %d -> %d", oldEpoch, got)
		}
		if primaryOf() == oldPrimary {
			t.Fatalf("primary must have moved off %s", oldPrimary)
		}
		if failures > 0 && pause > 90*time.Second {
			t.Fatalf("write failures must be confined to the switchover pause, got %s", pause)
		}
		if maxAway > 1 {
			t.Fatalf("members must restart one at a time, saw %d away", maxAway)
		}
		if n := primaryPods(); n != 1 {
			t.Fatalf("exactly one pod may carry role=primary, got %d", n)
		}
		assertAllAcked(ctx, t, c, rw, acked)
		if s, err := psqlRetry(ctx, c, ro, "SHOW max_connections", 2*time.Minute); err != nil || s != "150" {
			t.Errorf("standby max_connections: %q %v", s, err)
		}
		waitFor(ctx, t, "both replicas to stream from the new primary", 5*time.Minute, func() bool {
			out, err := psql(ctx, c, rw, "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'")
			return err == nil && out == "2"
		})
	})

	t.Run("StorageClassChangeRebuildsMembersOneByOne", func(t *testing.T) {
		alt := `
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: pgshard-e2e-alt
provisioner: rancher.io/local-path
volumeBindingMode: WaitForFirstConsumer
reclaimPolicy: Delete
`
		if err := c.Apply(ctx, alt); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Delete(context.Background(), alt) })
		rows, err := psql(ctx, c, rw, "SELECT count(*) FROM ha_writes")
		if err != nil {
			t.Fatal(err)
		}
		oldEpoch := epochOf()
		patchCluster(`{"spec":{"storage":{"storageClassName":"pgshard-e2e-alt"}}}`)
		pvcs := func() (string, error) {
			return c.Kubectl(ctx, nil, "-n", testNamespace, "get", "pvc", "-l", "pgshard.io/cluster="+clusterName+",pgshard.io/group=shard-0", "-o",
				`jsonpath={range .items[*]}{.metadata.name}:{.spec.storageClassName}{" "}{end}`)
		}
		waitFor(ctx, t, "every shard member to move onto the new class", 25*time.Minute, func() bool {
			out, err := pvcs()
			if err != nil {
				return false
			}
			fields := strings.Fields(out)
			if len(fields) != 3 {
				return false
			}
			for _, f := range fields {
				if !strings.HasSuffix(f, "-v2:pgshard-e2e-alt") {
					return false
				}
			}
			return rolloutIdle()
		})
		out, _ := pvcs()
		t.Logf("shard claims after the rebuild: %s; epoch %d -> %d", out, oldEpoch, epochOf())
		if got := epochOf(); got != oldEpoch+1 {
			t.Errorf("rebuilding the primary needs exactly one switchover: epoch %d -> %d", oldEpoch, got)
		}
		for i := 0; i < 3; i++ {
			pod := fmt.Sprintf("%s-%d", group, i)
			waitFor(ctx, t, pod+" to exist and be Ready after the rebuild", 5*time.Minute, func() bool {
				out, err := c.Kubectl(ctx, nil, "-n", testNamespace, "get", "pod", pod, "-o", `jsonpath={.status.conditions[?(@.type=="Ready")].status}`)
				return err == nil && strings.TrimSpace(out) == "True"
			})
			if got := jsonpath(ctx, t, c, "pod", pod, "{.spec.volumes[0].persistentVolumeClaim.claimName}"); got != pod+"-v2" {
				t.Errorf("pod %s mounts %q", pod, got)
			}
		}
		if got := jsonpath(ctx, t, c, "pgshardgroup", group, "{.status.members[*].pvc}"); got != group+"-0-v2 "+group+"-1-v2 "+group+"-2-v2" {
			t.Errorf("group status claims: %q", got)
		}
		if after, err := psqlRetry(ctx, c, rw, "SELECT count(*) FROM ha_writes", 2*time.Minute); err != nil || after != rows {
			t.Errorf("data after the rebuild: %q (before %q) %v", after, rows, err)
		}
		if out, err := psqlRetry(ctx, c, ro, "SELECT count(*) FROM ha_writes", 2*time.Minute); err != nil || out != rows {
			t.Errorf("standby data after the rebuild: %q (before %q) %v", out, rows, err)
		}
		if n := count(ctx, t, c, "pvc", "pgshard.io/cluster="+clusterName+",pgshard.io/group=catalog"); n != 3 {
			t.Errorf("catalog claims untouched: got %d", n)
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
