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
  internalTLS:
    insecure: true
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

// ledgerRole is the login the workload writes as. The workload has to go
// through the router: the router is what knows a cutover happened, buffers
// across the flip and sends the next statement to the new serving set. A
// connection straight to a group's -rw Service keeps writing to whichever
// PostgreSQL it dialled, including one the switch has already retired, and
// those writes are acknowledged by a primary nothing replicates from any
// more. The reshard suite has always written through the router; this one
// did not, and that -- not the upgrade -- is what its ledger oracle was
// reporting as lost writes.
const (
	ledgerRole     = "ledger_writer"
	ledgerPassword = "ledger-writer-password"
)

// routerSQL runs one statement against the app database through the router.
func routerSQL(ctx context.Context, c *e2e.Cluster, sql string) (string, error) {
	out, err := c.Kubectl(ctx, nil, "-n", testNamespace, "exec", clientPod, "--",
		"env", "PGPASSWORD="+ledgerPassword,
		"psql", "-h", clusterName+"-router", "-U", ledgerRole, "-d", appDatabase,
		"-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=verbose", "-tAc", sql)
	return strings.TrimSpace(out), err
}

// registerLedgerRole gives the workload a role the router will accept. The
// verifier has to come from PostgreSQL, which has no function to derive
// one, so the role is created on the catalog to have its rolpassword read
// back and registered as desired state for the applier to fan out to every
// group, including the upgrade targets provisioned later.
func registerLedgerRole(ctx context.Context, t *testing.T, c *e2e.Cluster) {
	t.Helper()
	if _, err := psql(ctx, c, clusterName+"-catalog-rw", "postgres",
		"SET password_encryption = 'scram-sha-256'; CREATE ROLE "+ledgerRole+" LOGIN PASSWORD '"+ledgerPassword+"'"); err != nil {
		t.Fatal(err)
	}
	verifier, err := psql(ctx, c, clusterName+"-catalog-rw", "postgres",
		"SELECT rolpassword FROM pg_authid WHERE rolname = '"+ledgerRole+"'")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(verifier, "SCRAM-SHA-256$") {
		t.Fatalf("verifier for %s is not SCRAM: %q", ledgerRole, verifier)
	}
	catalogSQL(ctx, t, c, "INSERT INTO pgshard.roles (rolname, verifier, login) VALUES ('"+ledgerRole+"', '"+verifier+"', true)")
	catalogSQL(ctx, t, c, "INSERT INTO pgshard.grants (rolname, database, object_kind, object_schema, object_name, privileges) VALUES "+
		"('"+ledgerRole+"', '"+appDatabase+"', 'database', '', '"+appDatabase+"', ARRAY['CONNECT']), "+
		"('"+ledgerRole+"', '"+appDatabase+"', 'schema', '', 'public', ARRAY['USAGE']), "+
		"('"+ledgerRole+"', '"+appDatabase+"', 'table', 'public', 'ledger', ARRAY['SELECT','INSERT'])")
}

func catalogSQL(ctx context.Context, t *testing.T, c *e2e.Cluster, sql string) string {
	t.Helper()
	out, err := psql(ctx, c, clusterName+"-catalog-rw", "postgres", sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return out
}

func waitFor(ctx context.Context, t *testing.T, c *e2e.Cluster, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	waitForWhy(ctx, t, c, what, timeout, nil, cond)
}

// waitForWhy is waitFor with a why the caller can use to report what it was
// waiting on. A wait that reports only that time passed cannot distinguish a
// workflow that was never created from one that is stuck from one that failed.
func waitForWhy(ctx context.Context, t *testing.T, c *e2e.Cluster, what string, timeout time.Duration, why func() string, cond func() bool) {
	t.Helper()
	started := time.Now()
	deadline := started.Add(timeout)
	nextProgress := started.Add(time.Minute)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			detail := ""
			if why != nil {
				detail = why()
			}
			t.Fatalf("timed out after %s waiting for %s%s%s", timeout, what, detail, c.Summary(ctx, testNamespace))
		}
		if time.Now().After(nextProgress) {
			t.Logf("still waiting for %s (%s elapsed)", what, time.Since(started).Round(time.Second))
			nextProgress = time.Now().Add(time.Minute)
		}
		sampleReplication(ctx, c)
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
			// The router decides which set the write lands on, so the
			// writer does not need to know and must not choose.
			hi := next + 24
			sql := fmt.Sprintf(`INSERT INTO ledger (id, tenant_id, amount) SELECT g, 4242, 1 FROM generate_series(%d, %d) g ON CONFLICT DO NOTHING`, next, hi)
			if _, err := routerSQL(lctx, c, sql); err != nil {
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
		t.Fatalf("ledger oracle on %s: %s, want %s (acked writes lost or duplicated)\n%s", group, got, want, l.explain(ctx, t, group, acked))
	}
}

// replicationSample is the last look at replication taken while the switch
// was still running. The subscriptions and slots are dropped when the
// upgrade completes, so a report gathered at the oracle -- which runs
// after -- finds nothing, and the state that matters is the state at the
// flip.
var replicationSample string

// nextSample rate-limits the sampling: three exec'd queries on every poll
// of a three-second loop would slow the wait enough to change what it is
// measuring.
var nextSample time.Time

// sampleReplication records how the target's subscriptions and the
// source's slots looked at this moment, if it can reach them.
func sampleReplication(ctx context.Context, c *e2e.Cluster) {
	if time.Now().Before(nextSample) {
		return
	}
	nextSample = time.Now().Add(15 * time.Second)
	var b strings.Builder
	for _, q := range []struct{ label, target, sql string }{
		{"subscriptions on shard-0-g2", "shard-0-g2-rw",
			`SELECT s.subname || ' enabled=' || s.subenabled || ' rels=' || coalesce(string_agg(r.srsubstate, ',' ORDER BY r.srsubstate), 'none')
			   || ' applied=' || coalesce((SELECT max(latest_end_lsn)::text FROM pg_stat_subscription st WHERE st.subid = s.oid), '-')
			   || ' last_msg=' || coalesce((SELECT max(last_msg_receipt_time)::text FROM pg_stat_subscription st WHERE st.subid = s.oid), '-')
			 FROM pg_subscription s LEFT JOIN pg_subscription_rel r ON r.srsubid = s.oid
			 GROUP BY s.oid, s.subname, s.subenabled ORDER BY 1`},
		{"slots on shard-0", "shard-0-rw",
			`SELECT slot_name || ' active=' || active || ' confirmed=' || coalesce(confirmed_flush_lsn::text, '-')
			   || ' behind=' || coalesce(pg_size_pretty(pg_current_wal_lsn() - confirmed_flush_lsn), '-')
			 FROM pg_replication_slots ORDER BY 1`},
		{"walsenders on shard-0", "shard-0-rw",
			`SELECT coalesce(application_name, '-') || ' state=' || state || ' sent=' || coalesce(sent_lsn::text, '-') || ' flush=' || coalesce(flush_lsn::text, '-') FROM pg_stat_replication ORDER BY 1`},
	} {
		out, err := psql(ctx, c, clusterName+"-"+q.target, appDatabase, q.sql)
		if err != nil {
			continue
		}
		if trimmed := strings.TrimSpace(out); trimmed != "" {
			fmt.Fprintf(&b, "  %s: %s\n", q.label, strings.ReplaceAll(trimmed, "\n", " | "))
		}
	}
	if b.Len() > 0 {
		replicationSample = b.String()
	}
}

// explain says which rows are missing and what replication looked like.
// "1675 of 6275" does not distinguish a copy that never ran from a stream
// that dropped rows, and those have different causes: a contiguous block
// missing from the front is an initial copy that was not waited for, gaps
// throughout are changes the subscription did not carry, and a block
// missing from the end is a switch that happened before the target caught
// up.
func (l *ledger) explain(ctx context.Context, t *testing.T, group string, acked int64) string {
	t.Helper()
	var b strings.Builder
	ask := func(label, target, db, sql string) {
		out, err := psql(ctx, l.c, clusterName+"-"+target, db, sql)
		if err != nil {
			fmt.Fprintf(&b, "  %s: %v\n", label, err)
			return
		}
		fmt.Fprintf(&b, "  %s: %s\n", label, strings.ReplaceAll(strings.TrimSpace(out), "\n", " | "))
	}
	shape := fmt.Sprintf(`SELECT 'rows=' || count(*) || ' min=' || coalesce(min(id)::text, '-') || ' max=' || coalesce(max(id)::text, '-')
		|| ' first_gap=' || coalesce((SELECT min(g) FROM generate_series(1, %d) g WHERE NOT EXISTS (SELECT 1 FROM ledger WHERE id = g))::text, 'none')
		FROM ledger WHERE id <= %d`, acked, acked)
	b.WriteString("ledger shape (acked ids are dense 1.." + fmt.Sprint(acked) + "):\n")
	ask("serving "+group, group+"-rw", appDatabase, shape)
	other := "shard-0"
	if group == "shard-0" {
		other = "shard-0-g2"
	}
	ask("other "+other, other+"-rw", appDatabase, shape)
	b.WriteString("replication:\n")
	ask("subscriptions on "+other, other+"-rw", appDatabase,
		`SELECT s.subname || ' enabled=' || s.subenabled || ' rels=' || coalesce(string_agg(r.srsubstate, ',' ORDER BY r.srsubstate), 'none')
		 FROM pg_subscription s LEFT JOIN pg_subscription_rel r ON r.srsubid = s.oid GROUP BY s.subname, s.subenabled ORDER BY 1`)
	ask("subscriptions on "+group, group+"-rw", appDatabase,
		`SELECT s.subname || ' enabled=' || s.subenabled || ' rels=' || coalesce(string_agg(r.srsubstate, ',' ORDER BY r.srsubstate), 'none')
		 FROM pg_subscription s LEFT JOIN pg_subscription_rel r ON r.srsubid = s.oid GROUP BY s.subname, s.subenabled ORDER BY 1`)
	ask("slots on "+other, other+"-rw", appDatabase,
		`SELECT slot_name || ' active=' || active || ' confirmed=' || coalesce(confirmed_flush_lsn::text, '-') FROM pg_replication_slots ORDER BY 1`)
	if replicationSample != "" {
		b.WriteString("replication while the switch was running:\n" + replicationSample)
	}
	b.WriteString("workflow: " + upgradeWorkflowDetail(ctx, t, l.c))
	// The cutover record says which positions the switch compared against
	// and how it got to the flip, which is the other half of any answer
	// about a target that was let through behind.
	fmt.Fprintf(&b, "\ncutover: %s\n", catalogSQL(ctx, t, l.c,
		"SELECT coalesce(jsonb_pretty(status->'cutover'), '-') FROM pgshard.workflows WHERE kind = 'upgrade' ORDER BY updated_at DESC LIMIT 1"))
	return b.String()
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
	waitFor(ctx, t, c, "serving shard set stamped major 18", 3*time.Minute, func() bool {
		return catalogSQL(ctx, t, c, "SELECT coalesce(max(pg_major), 0) FROM pgshard.shard_sets WHERE state = 'serving'") == "18"
	})
	registerLedgerRole(ctx, t, c)
	waitFor(ctx, t, c, "the workload role on every group", 3*time.Minute, func() bool {
		out, err := routerSQL(ctx, c, "SELECT 1")
		return err == nil && out == "1"
	})
}

// upgradeWorkflowDetail is what a stuck upgrade needs said about it: whether
// the workflow row exists at all, and if it does, its state, stage, message and
// error. Without this a timeout cannot tell "never created" from "stuck" from
// "failed", which are three different bugs.
func upgradeWorkflowDetail(ctx context.Context, t *testing.T, c *e2e.Cluster) string {
	t.Helper()
	// The cutover step, attempts and aborts are what distinguish "still
	// copying" from "stalled at the switch". Without them a timeout says
	// only that the switch did not happen, which is what the test already
	// said -- the reshard suite's equivalent gap cost a day of investigating
	// the wrong stage.
	return "\nworkflows: " + catalogSQL(ctx, t, c,
		"SELECT coalesce(string_agg(kind || ' ' || state || ' set=' || coalesce(spec->>'shard_set', '') || ' stage=' || coalesce(status->>'stage', '') || ' step=' || coalesce(status->'cutover'->>'step', '') || ' attempts=' || coalesce(status->'cutover'->>'attempts', '0') || ' aborts=' || coalesce((SELECT count(*)::text FROM jsonb_array_elements(status->'cutover'->'aborts')), '0') || ' msg=' || coalesce(status->>'message', '') || ' err=' || coalesce(error, ''), '; '), 'none') FROM pgshard.workflows") +
		"\nshard sets: " + catalogSQL(ctx, t, c,
		"SELECT coalesce(string_agg(shard_set || '=' || state || ' major=' || coalesce(pg_major::text, '-'), ', ' ORDER BY generation), 'none') FROM pgshard.shard_sets") +
		"\nfenced: " + catalogSQL(ctx, t, c,
		"SELECT coalesce(string_agg(shard_set || '/' || shard_id, ', '), 'none') FROM pgshard.shard_status WHERE migrating")
}

func upgradeWorkflowState(ctx context.Context, t *testing.T, c *e2e.Cluster, set string) string {
	t.Helper()
	return catalogSQL(ctx, t, c,
		"SELECT coalesce(string_agg(state || ':' || (status->>'stage'), ','), '') FROM pgshard.workflows WHERE kind = 'upgrade' AND spec->>'shard_set' = '"+set+"'")
}
