//go:build e2e

package backup

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/agent/backup"
	"github.com/andrew01234567890/pgshard/test/e2e"
)

const (
	testNamespace = "pgshard-e2e-backup"
	// pgpassEnv points psql at the pgpass file the agent keeps beside PGDATA.
	pgpassEnv = "PGPASSFILE=/var/lib/postgresql/data/.pgshard-pgpass"
	storesNS  = "objectstores"
	// azuriteKey is Azurite's well-known development account key.
	azuriteAccount = "devstoreaccount1"
	azuriteKey     = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
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
	manifest = strings.Replace(manifest, "            - run\n", "            - run\n            - --admin-image="+env("ADMIN_IMAGE", "pgshard-admin:e2e")+"\n", 1)
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

// store describes one object-store variant of the suite.
type store struct {
	name     string
	manifest string
	deploy   string
	secret   string
	policy   string
}

var stores = []store{
	{name: "s3", manifest: "minio.yaml", deploy: "minio",
		secret: `stringData: {key: minioadmin, keySecret: minioadmin}`,
		policy: `
    type: s3
    bucket: pgshard
    endpoint: http://minio.objectstores.svc:9000
    region: us-east-1
    uriStyle: path
    verifyTLS: false`},
	{name: "azure", manifest: "azurite.yaml", deploy: "azurite",
		secret: `stringData: {account: ` + azuriteAccount + `, key: "` + azuriteKey + `"}`,
		policy: `
    type: azure
    container: pgshard
    endpoint: http://azurite.objectstores.svc:10000
    uriStyle: path
    verifyTLS: false`},
	{name: "gcs", manifest: "fake-gcs.yaml", deploy: "fake-gcs",
		secret: `stringData: {token: fake-gcs-token}`,
		policy: `
    type: gcs
    bucket: pgshard
    endpoint: http://fake-gcs.objectstores.svc:4443
    credentialType: token
    verifyTLS: false`},
}

func selectedStores() []store {
	want := env("E2E_BACKUP_STORES", "s3,azure,gcs")
	var out []store
	for _, s := range stores {
		if strings.Contains(","+want+",", ","+s.name+",") {
			out = append(out, s)
		}
	}
	return out
}

func clusterManifest(name, major, image string, s store) string {
	img := ""
	if image != "" {
		img = "    image: " + image + "\n"
	}
	return fmt.Sprintf(`
apiVersion: v1
kind: Secret
metadata:
  name: %[1]s-store-credentials
  namespace: %[2]s
%[5]s
---
apiVersion: v1
kind: Secret
metadata:
  name: %[1]s-repo-key
  namespace: %[2]s
stringData: {passphrase: e2e-repository-passphrase}
---
apiVersion: pgshard.io/v1alpha1
kind: PgShardCluster
metadata:
  name: %[1]s
  namespace: %[2]s
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
  backup:
    policyRef: %[1]s-policy
---
apiVersion: pgshard.io/v1alpha1
kind: PgShardBackupPolicy
metadata:
  name: %[1]s-policy
  namespace: %[2]s
spec:
  objectStore:%[6]s
    prefix: /%[1]s
    credentials:
      secretRef: {name: %[1]s-store-credentials}
    encryption:
      secretRef: {name: %[1]s-repo-key}
  schedules:
    incremental: "* * * * *"
  retention:
    full: 2
`, name, testNamespace, major, img, s.secret, s.policy)
}

func backupManifest(cluster, name, typ string) string {
	return fmt.Sprintf(`
apiVersion: pgshard.io/v1alpha1
kind: PgShardBackup
metadata:
  name: %s
  namespace: %s
spec:
  clusterName: %s
  type: %s
`, name, testNamespace, cluster, typ)
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

func getJSON(ctx context.Context, c *e2e.Cluster, kind, name string, into any) error {
	out, err := c.Kubectl(ctx, nil, "-n", testNamespace, "get", kind, name, "-o", "json")
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(out), into)
}

// createAzuriteContainer creates the blob container from inside the cluster
// with a SharedKey-signed request; Azurite has no unauthenticated create.
func createAzuriteContainer(ctx context.Context, t *testing.T, c *e2e.Cluster, container string) {
	t.Helper()
	key, err := base64.StdEncoding.DecodeString(azuriteKey)
	if err != nil {
		t.Fatal(err)
	}
	sign := func(method, date string) string {
		canonHeaders := "x-ms-date:" + date + "\nx-ms-version:2021-08-06\n"
		canonResource := "/" + azuriteAccount + "/" + azuriteAccount + "/" + container + "\nrestype:container"
		mac := hmac.New(sha256.New, key)
		mac.Write([]byte(method + "\n\n\n\n\n\n\n\n\n\n\n\n" + canonHeaders + canonResource))
		return base64.StdEncoding.EncodeToString(mac.Sum(nil))
	}
	url := "http://azurite." + storesNS + ".svc:10000/" + azuriteAccount + "/" + container + "?restype=container"
	var lastErr error
	for i := 0; i < 10; i++ {
		put := time.Now().UTC().Format(http.TimeFormat)
		get := put
		script := fmt.Sprintf(`code=$(curl -s -o /dev/null -w '%%{http_code}' -X PUT -H 'x-ms-date: %s' -H 'x-ms-version: 2021-08-06' -H 'Authorization: SharedKey %s:%s' -H 'Content-Length: 0' '%s'); echo "put=$code"; `+
			`code=$(curl -s -o /dev/null -w '%%{http_code}' -H 'x-ms-date: %s' -H 'x-ms-version: 2021-08-06' -H 'Authorization: SharedKey %s:%s' '%s'); echo "get=$code"`,
			put, azuriteAccount, sign(http.MethodPut, put), url, get, azuriteAccount, sign(http.MethodGet, get), url)
		pod := fmt.Sprintf("azurite-mkcontainer-%d", i)
		if _, err := c.Kubectl(ctx, nil, "-n", storesNS, "run", pod, "--restart=Never", "--image=curlimages/curl:8.14.1", "--command", "--", "sh", "-c", script); err != nil {
			lastErr = err
			time.Sleep(3 * time.Second)
			continue
		}
		_, _ = c.Kubectl(ctx, nil, "-n", storesNS, "wait", "--for=jsonpath={.status.phase}=Succeeded", "pod/"+pod, "--timeout=2m")
		out, err := c.Kubectl(ctx, nil, "-n", storesNS, "logs", pod)
		_, _ = c.Kubectl(ctx, nil, "-n", storesNS, "delete", "pod", pod, "--ignore-not-found", "--wait=false")
		if err == nil && strings.Contains(out, "get=200") {
			t.Logf("azurite container %s present: %s", container, strings.TrimSpace(strings.ReplaceAll(out, "\n", " ")))
			return
		}
		lastErr = fmt.Errorf("%v: %s", err, out)
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("create azurite container: %v", lastErr)
}

func deployStores(ctx context.Context, t *testing.T, c *e2e.Cluster, root string, selected []store) {
	t.Helper()
	for _, s := range selected {
		if _, err := c.Kubectl(ctx, nil, "apply", "-f", filepath.Join(root, "hack/objectstores/k8s", s.manifest)); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		_, _ = c.Kubectl(ctx, nil, "delete", "namespace", storesNS, "--ignore-not-found", "--wait=true")
	})
	for _, s := range selected {
		if err := c.WaitPodsReady(ctx, storesNS, "app="+s.deploy, 5*time.Minute); err != nil {
			t.Fatal(err)
		}
		if s.name == "azure" {
			createAzuriteContainer(ctx, t, c, "pgshard")
			continue
		}
		if _, err := c.Kubectl(ctx, nil, "-n", storesNS, "wait", "--for=condition=complete", "job/"+s.deploy+"-create-bucket", "--timeout=5m"); err != nil {
			t.Fatal(err)
		}
	}
}

func gatherNamespace(ctx context.Context, c *e2e.Cluster) {
	dir := filepath.Join(c.Artifacts, "backup-"+testNamespace)
	_ = os.MkdirAll(dir, 0o755)
	save := func(file string, args ...string) {
		out, err := c.Kubectl(ctx, nil, args...)
		if err != nil {
			out += "\n# error: " + err.Error() + "\n"
		}
		_ = os.WriteFile(filepath.Join(dir, file), []byte(out), 0o644)
	}
	save("objects.yaml", "-n", testNamespace, "get", "pgshardclusters,pgshardgroups,pgshardbackuppolicies,pgshardbackups,pods,pvc,svc", "-o", "yaml")
	save("events.txt", "-n", testNamespace, "get", "events", "--sort-by=.lastTimestamp")
	save("stores.txt", "-n", storesNS, "get", "all,jobs", "-o", "wide")
	pods, err := c.Kubectl(ctx, nil, "-n", testNamespace, "get", "pods", "-o", "name")
	if err == nil {
		for _, p := range strings.Fields(pods) {
			save("logs-"+strings.TrimPrefix(p, "pod/")+".txt", "-n", testNamespace, "logs", "--all-containers", p)
		}
	}
	save("operator-logs.txt", "-n", e2e.SystemNamespace, "logs", "-l", "app.kubernetes.io/name=pgshard-operator", "--tail=-1")
}

func TestBackupsToObjectStores(t *testing.T) {
	c := e2e.NewCluster(t)
	c.GatherOnFailure(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()
	root := repoRoot(t)
	major := env("PG_MAJOR", "18")
	selected := selectedStores()
	if len(selected) == 0 {
		t.Fatal("E2E_BACKUP_STORES selects no store")
	}

	deployOperator(ctx, t, c, root, env("OPERATOR_IMAGE", "pgshard-operator:e2e"))
	deployStores(ctx, t, c, root, selected)
	if err := c.Apply(ctx, "apiVersion: v1\nkind: Namespace\nmetadata:\n  name: "+testNamespace+"\n"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if t.Failed() {
			gatherNamespace(ctx, c)
		}
		_, _ = c.Kubectl(ctx, nil, "delete", "pgshardclusters,pgshardbackuppolicies", "-n", testNamespace, "--all", "--wait=true", "--timeout=4m")
		_, _ = c.Kubectl(ctx, nil, "delete", "namespace", testNamespace, "--ignore-not-found", "--wait=true", "--timeout=4m")
	})
	for _, s := range selected {
		if err := c.Apply(ctx, clusterManifest(s.name, major, os.Getenv("PGSHARD_POSTGRES_IMAGE"), s)); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("stores", func(t *testing.T) {
		for _, s := range selected {
			s := s
			t.Run(s.name, func(t *testing.T) {
				t.Parallel()
				runStore(ctx, t, c, s, major)
			})
		}
	})
}

func runStore(ctx context.Context, t *testing.T, c *e2e.Cluster, s store, major string) {
	cluster := s.name
	if _, err := c.Kubectl(ctx, nil, "-n", testNamespace, "wait", "--for=condition=Ready", "pgshardcluster/"+cluster, "--timeout=15m"); err != nil {
		t.Fatal(err)
	}
	stanza := backup.StanzaName(cluster, "catalog", atoi(major))
	primary := jsonpath(ctx, t, c, "pgshardgroup", cluster+"-catalog", "{.status.primary}")
	if out, err := c.Kubectl(ctx, nil, "-n", testNamespace, "exec", primary, "-c", "postgres", "--", "env", pgpassEnv, "psql", "-h", "/tmp", "-U", "postgres", "-tAc", "SHOW archive_mode"); err != nil || strings.TrimSpace(out) != "on" {
		t.Fatalf("archive_mode on %s: %q %v", primary, out, err)
	}

	full := cluster + "-full"
	if err := c.Apply(ctx, backupManifest(cluster, full, "full")); err != nil {
		t.Fatal(err)
	}
	fullObj := waitBackup(ctx, t, c, full, 10*time.Minute)
	if len(fullObj.Status.Groups) != 2 || fullObj.Status.BackupID == "" || !strings.HasSuffix(fullObj.Status.BackupID, "F") {
		t.Fatalf("full backup status: %+v", fullObj.Status)
	}
	for _, g := range fullObj.Status.Groups {
		if g.BackupID == "" || g.StartLSN == "" || g.StopLSN == "" || g.WALStart == "" || g.WALStop == "" || g.SizeBytes == 0 || g.Duration == "" || g.Error != "" {
			t.Errorf("group %s status incomplete: %+v", g.Group, g)
		}
	}
	if got := jsonpath(ctx, t, c, "pgshardbackup", full, `{.status.conditions[?(@.type=="RetentionApplied")].status}`); got != "True" {
		t.Errorf("RetentionApplied=%q", got)
	}

	if _, err := c.Kubectl(ctx, nil, "-n", testNamespace, "exec", primary, "-c", "postgres", "--", "env", pgpassEnv, "psql", "-h", "/tmp", "-U", "postgres", "-c",
		"CREATE TABLE IF NOT EXISTS backup_e2e (id int); INSERT INTO backup_e2e SELECT generate_series(1, 1000); SELECT pg_switch_wal();"); err != nil {
		t.Fatal(err)
	}
	incr := cluster + "-incr"
	if err := c.Apply(ctx, backupManifest(cluster, incr, "incremental")); err != nil {
		t.Fatal(err)
	}
	incrObj := waitBackup(ctx, t, c, incr, 10*time.Minute)
	if len(incrObj.Status.Groups) != 2 || !strings.HasSuffix(incrObj.Status.Groups[0].BackupID, "I") || !strings.HasPrefix(incrObj.Status.Groups[0].BackupID, fullObj.Status.BackupID) {
		t.Fatalf("incremental status: %+v", incrObj.Status)
	}

	out, err := c.Kubectl(ctx, nil, "-n", testNamespace, "exec", primary, "-c", "postgres", "--",
		"pgbackrest", "--config=/etc/pgbackrest/pgbackrest.conf", "--stanza="+stanza, "--log-level-console=off", "info", "--output=json")
	if err != nil {
		t.Fatal(err)
	}
	info, err := backup.ParseInfo([]byte(out), stanza)
	if err != nil {
		t.Fatalf("parse info: %v\n%s", err, out)
	}
	if info.StatusCode != 0 || info.ArchiveMin == "" || info.ArchiveMax <= info.ArchiveMin || len(info.Backups) < 2 {
		t.Fatalf("pgbackrest info: %+v", info)
	}
	t.Logf("%s: WAL archive %s..%s, %d backups", cluster, info.ArchiveMin, info.ArchiveMax, len(info.Backups))

	waitFor(ctx, t, "BackupHealthy on policy "+cluster, 3*time.Minute, func() bool {
		return jsonpath(ctx, t, c, "pgshardbackuppolicy", cluster+"-policy", `{.status.conditions[?(@.type=="BackupHealthy")].status}`) == "True"
	})
	waitFor(ctx, t, "BackupHealthy on cluster "+cluster, 3*time.Minute, func() bool {
		return jsonpath(ctx, t, c, "pgshardcluster", cluster, `{.status.conditions[?(@.type=="BackupHealthy")].status}`) == "True"
	})
	if got := jsonpath(ctx, t, c, "pgshardbackuppolicy", cluster+"-policy", "{.status.clusters[0].lastFullTime}"); got == "" {
		t.Error("policy status.clusters[0].lastFullTime not set")
	}
	waitFor(ctx, t, "a scheduled incremental backup on "+cluster, 5*time.Minute, func() bool {
		var list pgshardv1alpha1.PgShardBackupList
		outList, err := c.Kubectl(ctx, nil, "-n", testNamespace, "get", "pgshardbackups", "-l", "pgshard.io/backup-policy="+cluster+"-policy", "-o", "json")
		if err != nil || json.Unmarshal([]byte(outList), &list) != nil {
			return false
		}
		for _, b := range list.Items {
			if b.Status.Phase == pgshardv1alpha1.BackupPhaseCompleted && b.Spec.Type == "incremental" {
				return true
			}
		}
		return false
	})
}

func waitBackup(ctx context.Context, t *testing.T, c *e2e.Cluster, name string, timeout time.Duration) *pgshardv1alpha1.PgShardBackup {
	t.Helper()
	var b pgshardv1alpha1.PgShardBackup
	waitFor(ctx, t, "backup "+name+" to finish", timeout, func() bool {
		if err := getJSON(ctx, c, "pgshardbackup", name, &b); err != nil {
			return false
		}
		return b.Status.Phase == pgshardv1alpha1.BackupPhaseCompleted || b.Status.Phase == pgshardv1alpha1.BackupPhaseFailed
	})
	if b.Status.Phase != pgshardv1alpha1.BackupPhaseCompleted {
		t.Fatalf("backup %s: phase %s: %s\n%+v", name, b.Status.Phase, b.Status.Error, b.Status.Groups)
	}
	return &b
}

func jsonpath(ctx context.Context, t *testing.T, c *e2e.Cluster, kind, name, path string) string {
	t.Helper()
	out, err := c.Kubectl(ctx, nil, "-n", testNamespace, "get", kind, name, "-o", "jsonpath="+path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(out)
}

func atoi(s string) int {
	n := 0
	for _, r := range s {
		n = n*10 + int(r-'0')
	}
	return n
}
