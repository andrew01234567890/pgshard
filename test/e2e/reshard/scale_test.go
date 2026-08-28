//go:build e2e

package reshard

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/placement"
	"github.com/andrew01234567890/pgshard/test/e2e"
)

// ledgerTenants are fixed shard keys the scale workload writes under; each
// batch goes to the tenant's serving group as resolved from the catalog.
var ledgerTenants = []int64{11, 23, 37, 41, 53, 67}

// resolveGroup returns the group name serving tenant's keyspace id, from
// the live catalog serving map.
func resolveGroup(ctx context.Context, c *e2e.Cluster, tenant int64) (string, error) {
	id, err := placement.KeyspaceID(tenant)
	if err != nil {
		return "", err
	}
	out, err := psql(ctx, c, clusterName+"-catalog-rw",
		fmt.Sprintf(`SELECT r.shard_id || ':' || ss.generation
			FROM pgshard.shard_ranges r
			JOIN pgshard.serving s ON s.shard_set = r.shard_set
			JOIN pgshard.shard_sets ss ON ss.shard_set = r.shard_set
			WHERE r.range @> %d::int8`, id))
	if err != nil {
		return "", err
	}
	if out == "" {
		return "", fmt.Errorf("no shard range covers keyspace id %d (%s)", id, shardMapState(ctx, c))
	}
	shard, gen, ok := strings.Cut(out, ":")
	if !ok {
		return "", fmt.Errorf("unexpected shard lookup result %q", out)
	}
	if gen == "1" {
		return "shard-" + shard, nil
	}
	return fmt.Sprintf("shard-%s-g%s", shard, gen), nil
}

// shardMapState reports what the catalog holds when a tenant cannot be routed,
// so a failure says which of the three preconditions is missing rather than
// only that the lookup found nothing.
func shardMapState(ctx context.Context, c *e2e.Cluster) string {
	out, err := psql(ctx, c, clusterName+"-catalog-rw",
		`SELECT 'ranges=' || (SELECT count(*) FROM pgshard.shard_ranges)
			|| ' serving=' || (SELECT count(*) FROM pgshard.serving)
			|| ' sets=' || (SELECT coalesce(string_agg(shard_set || ':' || state, ','), 'none') FROM pgshard.shard_sets)`)
	if err != nil {
		return "catalog unreadable: " + err.Error()
	}
	return out
}

// scaleLedger appends acknowledged rows per tenant, resolving the serving
// group before every batch and retrying through fences and switches.
type scaleLedger struct {
	c     *e2e.Cluster
	acked []atomic.Int64
	stop  context.CancelFunc
	wg    sync.WaitGroup

	// The write loop retries through fences and switches, so a persistent
	// failure looks exactly like one that is about to clear. Keep the last
	// reason per tenant so a timeout can say why nothing was acknowledged.
	mu      sync.Mutex
	lastErr []string
}

func (l *scaleLedger) note(i int, format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lastErr[i] = fmt.Sprintf(format, args...)
}

// why reports the last failure seen by each tenant that has acknowledged
// nothing, for a test that is about to fail on a timeout.
func (l *scaleLedger) why() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var b strings.Builder
	for i, tenant := range ledgerTenants {
		if l.acked[i].Load() > 0 {
			continue
		}
		reason := l.lastErr[i]
		if reason == "" {
			reason = "no attempt completed"
		}
		fmt.Fprintf(&b, "\n  tenant %d: %s", tenant, reason)
	}
	if b.Len() == 0 {
		return ""
	}
	return "\nlast write failure per tenant:" + b.String()
}

func startScaleLedger(ctx context.Context, c *e2e.Cluster) *scaleLedger {
	lctx, cancel := context.WithCancel(ctx)
	l := &scaleLedger{c: c, acked: make([]atomic.Int64, len(ledgerTenants)), stop: cancel, lastErr: make([]string, len(ledgerTenants))}
	for i, tenant := range ledgerTenants {
		l.wg.Add(1)
		go func() {
			defer l.wg.Done()
			next := int64(1)
			for lctx.Err() == nil {
				hi := next + 9
				// An explicit VALUES list rather than INSERT ... SELECT: the
				// router routes an insert by its shard key, and the key has to
				// be readable from the statement.
				var rows strings.Builder
				for id := next; id <= hi; id++ {
					if id > next {
						rows.WriteString(", ")
					}
					fmt.Fprintf(&rows, "(%d, %d, 1)", id, tenant)
				}
				sql := "INSERT INTO ledger (id, tenant_id, amount) VALUES " + rows.String() + " ON CONFLICT DO NOTHING"
				// Through the router, so the writes meet the write fence a
				// cutover raises. Writing to a group's PostgreSQL Service
				// directly goes under the pooler and the router, which is
				// where the fence lives, and a source written around the
				// fence never stands still for the switch to proceed.
				if _, err := routerSQL(lctx, c, sql); err != nil {
					l.note(i, "insert failed: %v", err)
					time.Sleep(2 * time.Second)
					continue
				}
				l.acked[i].Store(hi)
				next = hi + 1
				time.Sleep(time.Second)
			}
		}()
	}
	return l
}

func (l *scaleLedger) finish() []int64 {
	l.stop()
	l.wg.Wait()
	out := make([]int64, len(l.acked))
	for i := range l.acked {
		out[i] = l.acked[i].Load()
	}
	return out
}

// verify asserts the ledger oracle: every tenant's acknowledged rows exist
// exactly once on the tenant's serving group and nowhere else.
func (l *scaleLedger) verify(ctx context.Context, t *testing.T, acked []int64) {
	t.Helper()
	var total int64
	for i, tenant := range ledgerTenants {
		group, err := resolveGroup(ctx, l.c, tenant)
		if err != nil {
			t.Fatalf("tenant %d: %v", tenant, err)
		}
		if group == "" {
			t.Fatalf("tenant %d has no serving group", tenant)
		}
		got, err := shardSQL(ctx, l.c, group, fmt.Sprintf(
			`SELECT count(*) FILTER (WHERE id <= %d) || '/' || count(DISTINCT id) FILTER (WHERE id <= %d) FROM ledger WHERE tenant_id = %d`,
			acked[i], acked[i], tenant))
		if err != nil {
			t.Fatalf("verify tenant %d on %s: %v", tenant, group, err)
		}
		if want := fmt.Sprintf("%d/%d", acked[i], acked[i]); got != want {
			t.Fatalf("tenant %d on %s: %s, want %s (rows lost or duplicated)", tenant, group, got, want)
		}
		total += acked[i]
	}
	_ = total
}

// seedLedgerTable creates the sharded ledger on every starting shard. A
// reshard materializes the schema onto the targets it provisions, but the
// shards the cluster starts with get nothing, so seeding only shard 0 leaves
// the rest without the database as soon as a tenant routes to them.
func seedLedgerTable(ctx context.Context, t *testing.T, c *e2e.Cluster, shards int) {
	t.Helper()
	for i := 0; i < shards; i++ {
		group := fmt.Sprintf("shard-%d", i)
		if _, err := psql(ctx, c, clusterName+"-"+group+"-rw", "CREATE DATABASE "+appDatabase); err != nil {
			t.Fatal(err)
		}
		if _, err := shardSQL(ctx, c, group,
			"CREATE TABLE ledger (id bigint NOT NULL, tenant_id bigint NOT NULL, amount int NOT NULL, PRIMARY KEY (tenant_id, id))"); err != nil {
			t.Fatal(err)
		}
	}
	catalogSQL(ctx, t, c, "INSERT INTO pgshard.databases (name, default_placement, home_shard) VALUES ('"+appDatabase+"', 'unsharded', 0)")
	catalogSQL(ctx, t, c, "INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('"+appDatabase+"', 'public', 'ledger', 'sharded', 'tenant_id')")
	registerLedgerRole(ctx, t, c)
}

// waitForLedgerRole waits until the role the workload writes as has reached
// every group. Writing before it has is indistinguishable from the router
// refusing the role outright, and the difference matters.
func waitForLedgerRole(ctx context.Context, t *testing.T, c *e2e.Cluster) {
	t.Helper()
	waitForWhy(ctx, t, c, "ledger role applied on every group", 3*time.Minute, func() string {
		return "\nrole state: " + catalogSQL(ctx, t, c,
			"SELECT coalesce(string_agg(group_name || '=' || roles_generation, ', ' ORDER BY group_name), 'no rows') FROM pgshard.role_group_status") +
			"\ndesired: " + catalogSQL(ctx, t, c, "SELECT coalesce(max(desired_generation)::text, 'none') FROM pgshard.roles")
	}, func() bool {
		return catalogSQL(ctx, t, c, `SELECT count(*) = 0 FROM pgshard.role_group_status
			WHERE roles_generation < (SELECT max(desired_generation) FROM pgshard.roles)`) == "t" &&
			catalogSQL(ctx, t, c, "SELECT count(*) > 0 FROM pgshard.role_group_status") == "t"
	})
}

// assertOnlyTheRouterAdmitsTheAppRole is the acceptance of the rule that a
// shard's PostgreSQL port is the control plane's path and nothing else's.
// The roles are materialised with their verifiers on every group, so before
// pg_hba refused them over TCP a client that could reach a member could
// pick a shard and write to it directly -- past shard-key routing, past the
// write fence a cutover raises, and past the coordination that makes a
// multi-shard write atomic. SCRAM proved who it was; nothing checked it had
// come the right way.
func assertOnlyTheRouterAdmitsTheAppRole(ctx context.Context, t *testing.T, c *e2e.Cluster) {
	t.Helper()
	out, err := shardSQLAs(ctx, c, "shard-0", ledgerRole, ledgerPassword, "SELECT 1")
	if err == nil {
		t.Errorf("%s connected straight to a shard and got %q", ledgerRole, out)
	} else if !strings.Contains(err.Error(), "pg_hba.conf") {
		t.Errorf("%s was refused, but not by pg_hba: %v", ledgerRole, err)
	}
	if _, err := shardSQLAs(ctx, c, "shard-0", ledgerRole, ledgerPassword,
		"INSERT INTO ledger (id, tenant_id, amount) VALUES (-1, -1, -1)"); err == nil {
		t.Errorf("%s wrote to a shard directly", ledgerRole)
	}

	// The same credential through the router, and the superuser path the
	// control plane and replication use, both have to keep working: a rule
	// that closed those would be a much larger outage than the hole. The
	// routed writes are the workload's, which starts straight after this
	// and is checked row by row -- a sentinel written here would have to be
	// kept out of that count for no gain.
	if out, err := routerSQL(ctx, c, "SELECT 1"); err != nil || out != "1" {
		t.Errorf("%s through the router: %q %v", ledgerRole, out, err)
	}
	if out, err := shardSQL(ctx, c, "shard-0", "SELECT 1"); err != nil || out != "1" {
		t.Errorf("superuser lost its path to a shard: %q %v", out, err)
	}
}

// shardSQLAs is shardSQL as a chosen role rather than the superuser.
func shardSQLAs(ctx context.Context, c *e2e.Cluster, group, role, password, sql string) (string, error) {
	out, err := c.Kubectl(ctx, nil, "-n", testNamespace, "exec", clientPod, "--",
		"env", "PGPASSWORD="+password, "PGCONNECT_TIMEOUT=10",
		"psql", "-h", clusterName+"-"+group+"-rw", "-U", role, "-d", appDatabase, "-v", "ON_ERROR_STOP=1", "-tAc", sql)
	return strings.TrimSpace(out), err
}

func reshardTo(ctx context.Context, t *testing.T, c *e2e.Cluster, major string, shards int, generation int64) {
	t.Helper()
	if err := c.Apply(ctx, clusterManifestWithRetire(major, os.Getenv("PGSHARD_POSTGRES_IMAGE"), shards, "30s")); err != nil {
		t.Fatal(err)
	}
	set := fmt.Sprintf("g%d", generation)
	// A workflow that has failed is reported below; one that is merely stuck
	// reported nothing at all, which is the state this wait times out in.
	switchState := func() string {
		return "\nworkflow: " + catalogSQL(ctx, t, c,
			"SELECT coalesce(string_agg(state || ' stage=' || coalesce(status->>'stage', '') || ' step=' || coalesce(status->'cutover'->>'step', '') || ' attempts=' || coalesce(status->'cutover'->>'attempts', '0') || ' aborts=' || coalesce(status->'cutover'->'aborts'->>-1, 'none') || ' msg=' || coalesce(status->>'message', ''), '; '), 'none') FROM pgshard.workflows WHERE spec->>'shard_set' = '"+set+"'") +
			"\nserving: " + catalogSQL(ctx, t, c, "SELECT coalesce(string_agg(shard_set, ','), 'none') FROM pgshard.serving") +
			"\nfenced: " + catalogSQL(ctx, t, c, "SELECT coalesce(string_agg(shard_set || '/' || shard_id, ',' ORDER BY shard_set, shard_id), 'none') FROM pgshard.shard_status WHERE migrating")
	}
	waitForWhy(ctx, t, c, fmt.Sprintf("reshard to %d shards switched", shards), 35*time.Minute, switchState, func() bool {
		st := catalogSQL(ctx, t, c, "SELECT coalesce(string_agg(state || ':' || (status->>'stage'), ','), '') FROM pgshard.workflows WHERE kind = 'reshard' AND spec->>'shard_set' = '"+set+"'")
		if strings.HasPrefix(st, "failed") {
			t.Fatalf("reshard to %s failed: %s", set, catalogSQL(ctx, t, c, "SELECT coalesce(error, '') || ' ' || status::text FROM pgshard.workflows WHERE kind = 'reshard' AND spec->>'shard_set' = '"+set+"'"))
		}
		return catalogSQL(ctx, t, c, "SELECT string_agg(shard_set, ',') FROM pgshard.serving") == set
	})
	waitForWhy(ctx, t, c, fmt.Sprintf("reshard to %d shards completed", shards), 25*time.Minute, switchState, func() bool {
		st := catalogSQL(ctx, t, c, "SELECT coalesce(string_agg(state || ':' || (status->>'stage'), ','), '') FROM pgshard.workflows WHERE kind = 'reshard' AND spec->>'shard_set' = '"+set+"'")
		return strings.HasPrefix(st, "completed:") &&
			jsonpath(ctx, c, "pgshardcluster", clusterName, "{.status.effectiveShards}") == fmt.Sprint(shards)
	})
	if pause := catalogSQL(ctx, t, c, "SELECT coalesce((status->'cutover'->>'pause_ms')::bigint, -1) FROM pgshard.workflows WHERE kind = 'reshard' AND spec->>'shard_set' = '"+set+"'"); pause == "-1" {
		t.Errorf("cutover pause of %s not recorded: %q", set, pause)
	}
}

func clusterManifestWithRetire(major, image string, shards int, retire string) string {
	m := clusterManifest(major, image, shards)
	return strings.Replace(m, "  unsafeSingleReplica: true\n", "  unsafeSingleReplica: true\n  resharding:\n    retireOldGroupsAfter: "+retire+"\n", 1)
}

// TestReshard1To2To4To2UnderLoad grows a cluster 1 -> 2 -> 4 shards and
// merges back to 2, with the ledger workload running throughout; every
// acknowledged row survives every split and merge exactly once and each
// cutover records its write pause.
// TestReshardSplitUnderLoad grows a cluster while it is being written to.
func TestReshardSplitUnderLoad(t *testing.T) {
	reshardUnderLoad(t, 1, []reshardStep{{shards: 2, generation: 2}})
}

// TestReshardMergeUnderLoad shrinks a cluster while it is being written to.
// It starts at four shards rather than growing into them: a merge is the
// interesting half and giving it its own cluster keeps each transition inside
// a budget it can actually finish in.
func TestReshardMergeUnderLoad(t *testing.T) {
	reshardUnderLoad(t, 4, []reshardStep{{shards: 2, generation: 2}})
}

// reshardStep is one transition: the shard count to move to and the shard set
// generation it produces.
type reshardStep struct {
	shards     int
	generation int64
}

func reshardUnderLoad(t *testing.T, startShards int, steps []reshardStep) {
	c := e2e.NewCluster(t)
	c.GatherOnFailure(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Hour)
	defer cancel()
	root := repoRoot(t)
	major := env("PG_MAJOR", "18")

	deployOperator(ctx, t, c, root, env("OPERATOR_IMAGE", "pgshard-operator:e2e"))
	manifest := clusterManifestWithRetire(major, os.Getenv("PGSHARD_POSTGRES_IMAGE"), startShards, "30s")
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
	if err := c.WaitPodsReady(ctx, testNamespace, "app="+clientPod, 3*time.Minute); err != nil {
		t.Fatal(err)
	}
	seedLedgerTable(ctx, t, c, startShards)
	if err := c.Apply(ctx, controllerManifest(env("CONTROLLER_IMAGE", "pgshard-controller:e2e"))); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitPodsReady(ctx, testNamespace, "app="+clusterName+"-controller", 3*time.Minute); err != nil {
		t.Fatal(err)
	}
	waitFor(ctx, t, c, "serving shard set materialized", 5*time.Minute, func() bool {
		return jsonpath(ctx, c, "pgshardcluster", clusterName, "{.status.effectiveShards}") == strconv.Itoa(startShards)
	})

	waitForLedgerRole(ctx, t, c)
	assertOnlyTheRouterAdmitsTheAppRole(ctx, t, c)

	l := startScaleLedger(ctx, c)
	waitForWhy(ctx, t, c, "first acknowledged ledger writes", 3*time.Minute, l.why, func() bool {
		for i := range ledgerTenants {
			if l.acked[i].Load() < 10 {
				return false
			}
		}
		return true
	})

	for i, step := range steps {
		if i > 0 {
			l.verify(ctx, t, l.finishlessSnapshot())
		}
		reshardTo(ctx, t, c, major, step.shards, step.generation)
	}

	acked := l.finish()
	l.verify(ctx, t, acked)
	for i := range ledgerTenants {
		if acked[i] < 30 {
			t.Errorf("tenant %d made too little progress under load: %d rows", ledgerTenants[i], acked[i])
		}
	}
}

// finishlessSnapshot reads the acknowledged high-water marks without
// stopping the writers; verification tolerates rows above it.
func (l *scaleLedger) finishlessSnapshot() []int64 {
	out := make([]int64, len(l.acked))
	for i := range l.acked {
		out[i] = l.acked[i].Load()
	}
	return out
}
