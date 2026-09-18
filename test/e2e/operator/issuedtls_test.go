//go:build e2e

package operator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/test/e2e"
)

var writerError = regexp.MustCompile(`[0-9]+`)

// TestAnIssuedTLSClusterReachedFromPlaintextUnderLoad runs a cluster whose
// internal certificates the operator issues -- the mode internal TLS is to
// be required in everywhere -- through the paths that each cross an mTLS
// hop: a statement through the router (router -> pooler), and a failover
// (operator -> agent). Every other suite runs insecure, which is how an
// issuing cluster that could do neither went unnoticed (PGS-860).
//
// It gets there the way a running cluster does: created insecure and moved
// to issue: true under a write load through the router, which is what the
// staged move exists for (PGS-236). Once the move completes the cluster is
// rendered exactly as one created with issue: true.
func TestAnIssuedTLSClusterReachedFromPlaintextUnderLoad(t *testing.T) {
	c := e2e.NewCluster(t)
	c.GatherOnFailure(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Minute)
	defer cancel()
	root := repoRoot(t)
	major := env("PG_MAJOR", "18")

	deployOperator(ctx, t, c, root, env("OPERATOR_IMAGE", "pgshard-operator:e2e"))
	manifest := clusterManifestTLS(major, os.Getenv("PGSHARD_POSTGRES_IMAGE"), "    insecure: true\n")
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
	if err := waitCondition(ctx, c, "Ready", 15*time.Minute); err != nil {
		gatherNamespace(ctx, c)
		t.Fatal(err)
	}
	if err := c.WaitPodsReady(ctx, testNamespace, "app="+clientPod, 3*time.Minute); err != nil {
		t.Fatal(err)
	}
	sel := "pgshard.io/cluster=" + clusterName
	if err := c.WaitPodsReady(ctx, testNamespace, sel+",pgshard.io/component=router", 5*time.Minute); err != nil {
		t.Fatal(err)
	}

	group := clusterName + "-shard-0"
	primaryOf := func() string { return jsonpath(ctx, t, c, "pgshardgroup", group, "{.status.primary}") }
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
	router := clusterName + "-router"

	// A database the router routes to shard 0, set up on the shard and
	// registered in the catalog directly, so the statements below are the
	// router's own.
	const appDatabase = "tlsapp"
	if _, err := psql(ctx, c, clusterName+"-shard-0-rw", "CREATE DATABASE "+appDatabase); err != nil {
		t.Fatal(err)
	}
	if _, err := psqlOn(ctx, c, clusterName+"-shard-0-rw", appDatabase, "CREATE TABLE tls_probe (id int PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err := psql(ctx, c, clusterName+"-catalog-rw", "INSERT INTO pgshard.databases (name, default_placement, home_shard) VALUES ('"+appDatabase+"', 'unsharded', 0)"); err != nil {
		t.Fatal(err)
	}

	t.Run("AnInsecureClusterMovesToIssuedTLSUnderLoad", func(t *testing.T) {
		if _, err := psqlOn(ctx, c, clusterName+"-shard-0-rw", appDatabase, "CREATE TABLE tls_moves (id bigint PRIMARY KEY)"); err != nil {
			t.Fatal(err)
		}
		if out, err := psqlRetryOn(ctx, c, router, appDatabase, "SELECT 1", 5*time.Minute); err != nil {
			t.Fatalf("the insecure cluster's router does not serve %s: %q %v", appDatabase, out, err)
		}
		status := func(path string) string {
			return jsonpath(ctx, t, c, "pgshardcluster", clusterName, path)
		}

		// A writer through the router for the whole move: every hop a
		// statement crosses -- router -> pooler, and the operator's
		// switchovers through its agents -- changes transport during it.
		var (
			mu                     sync.Mutex
			acked                  []int64
			failures               int
			streakStart, lastError time.Time
			longestStreak          time.Duration
			lastErr                string
			errs                   = map[string]int{}
		)
		stop, done := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(done)
			for id := int64(1); ; id++ {
				select {
				case <-stop:
					return
				case <-ctx.Done():
					return
				default:
				}
				out, err := psqlOn(ctx, c, router, appDatabase, fmt.Sprintf("INSERT INTO tls_moves VALUES (%d)", id))
				mu.Lock()
				now := time.Now()
				if err == nil && strings.Contains(out, "INSERT 0 1") {
					acked = append(acked, id)
					if !streakStart.IsZero() {
						longestStreak = max(longestStreak, now.Sub(streakStart))
						streakStart = time.Time{}
					}
				} else {
					failures++
					lastError, lastErr = now, strings.TrimSpace(out)+" "+fmt.Sprint(err)
					errs[writerError.ReplaceAllString(lastErr, "N")]++
					if streakStart.IsZero() {
						streakStart = now
					}
				}
				mu.Unlock()
			}
		}()
		finish := func() {
			close(stop)
			<-done
			mu.Lock()
			defer mu.Unlock()
			if !streakStart.IsZero() {
				longestStreak = max(longestStreak, lastError.Sub(streakStart))
			}
		}

		// Member pods by name, every uid each name has had, and the step each
		// was rendered in: what shows the steps waited for the rolls rather
		// than just reporting them.
		type memberPod struct{ uid, phase string }
		members := func() []memberPod {
			out, err := c.Kubectl(ctx, nil, "-n", testNamespace, "get", "pods", "-l", sel+",pgshard.io/group", "-o",
				`jsonpath={range .items[*]}{.metadata.name}={.metadata.uid}={.metadata.annotations.pgshard\.io/internal-tls-phase}{" "}{end}`)
			if err != nil {
				return nil
			}
			var pods []memberPod
			for _, f := range strings.Fields(out) {
				parts := strings.SplitN(f, "=", 3)
				if len(parts) == 3 {
					pods = append(pods, memberPod{uid: parts[0] + "/" + parts[1], phase: parts[2]})
				}
			}
			return pods
		}
		uids := map[string]bool{}
		before := members()
		for _, m := range before {
			uids[m.uid] = true
		}
		var notAcceptingAtDialing []string

		phases := []string{}
		patch := `{"spec":{"internalTLS":{"insecure":null,"issue":true}}}`
		if _, err := c.Kubectl(ctx, nil, "-n", testNamespace, "patch", "pgshardcluster", clusterName, "--type=merge", "-p", patch); err != nil {
			finish()
			t.Fatal(err)
		}
		started := time.Now()
		lastLog := time.Time{}
		waitFor(ctx, t, "the move to issued TLS to complete", 60*time.Minute, func() bool {
			phase := status("{.status.internalTLS.move.phase}")
			pods := members()
			for _, m := range pods {
				uids[m.uid] = true
			}
			if phase != "" && (len(phases) == 0 || phases[len(phases)-1] != phase) {
				phases = append(phases, phase)
				t.Logf("move reached %s after %s", phase, time.Since(started).Round(time.Second))
				// A member replaced since the gate passed is rendered in
				// Dialing and serves both; one rendered before the move
				// serves plaintext only and must be gone.
				if phase == "Dialing" {
					for _, m := range pods {
						if m.phase == "" {
							notAcceptingAtDialing = append(notAcceptingAtDialing, m.uid)
						}
					}
				}
			}
			if time.Since(lastLog) > 2*time.Minute {
				lastLog = time.Now()
				mu.Lock()
				t.Logf("moving: %s; writer %d acknowledged, %d failed", status(`{.status.conditions[?(@.type=="InternalTLSMoving")].message}`), len(acked), failures)
				mu.Unlock()
			}
			return phase == "" && status("{.status.internalTLS.mode}") == "issued" &&
				status(`{.status.conditions[?(@.type=="RolloutInProgress")].status}`) == "False" &&
				status(`{.status.conditions[?(@.type=="Ready")].status}`) == "True"
		})
		time.Sleep(10 * time.Second)
		finish()
		t.Logf("move took %s through %v; writer: %d acknowledged, %d failed, longest failing stretch %s, last error %q",
			time.Since(started).Round(time.Second), phases, len(acked), failures, longestStreak.Round(time.Second), lastErr)

		// Three steps, in this order. The last one is what makes the
		// finished mode mean what it says: until every listener has been
		// rolled without --tls-accept-plaintext the cluster does still
		// accept plaintext, and the move used to report itself complete
		// before that roll (PGS-930).
		if !slices.Equal(phases, []string{"Accepting", "Dialing", "Closing"}) {
			t.Errorf("the move went through %v, want Accepting then Dialing then Closing", phases)
		}
		if len(notAcceptingAtDialing) > 0 {
			t.Errorf("routers were switched to TLS while member pods rendered before the move still served plaintext only: %v", notAcceptingAtDialing)
		}
		// Every member restarts twice: into Accepting, and out of it in
		// Closing. Dialing renders members exactly as Accepting does and
		// must roll none.
		if want := 3 * len(before); len(uids) < want {
			t.Errorf("member pods had %d incarnations over the move, want at least %d (%d members, each rolled into Accepting and out of it in Closing)", len(uids), want, len(before))
		}
		if len(acked) == 0 {
			t.Fatal("the writer acknowledged nothing through the router")
		}
		for e, n := range errs {
			t.Logf("writer error x%d: %s", n, e)
			// A switchover is retried and buffered; a caller dialling a
			// transport its server refuses fails at the handshake, however
			// briefly -- at kind's speed a roll is too quick for the length
			// of a failing stretch to tell the two apart.
			for _, sign := range []string{"handshake", "tls:", "x509", "certificate", "first record"} {
				if strings.Contains(strings.ToLower(e), sign) {
					t.Errorf("a write through the router failed on the transport during the move: %s", e)
					break
				}
			}
		}
		// Each member roll switches the primary over once, and the router
		// buffers writes across a switchover; a transport a caller cannot
		// speak fails every statement until the next step, minutes later.
		if longestStreak > 90*time.Second {
			t.Errorf("writes through the router failed for %s at a stretch; a caller dialled a transport its server refused", longestStreak.Round(time.Second))
		}
		out, err := psqlRetryOn(ctx, c, router, appDatabase, "SELECT string_agg(id::text, ',' ORDER BY id) FROM tls_moves", 3*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		have := map[string]bool{}
		for _, id := range strings.Split(out, ",") {
			have[id] = true
		}
		for _, id := range acked {
			if !have[strconv.FormatInt(id, 10)] {
				t.Fatalf("acknowledged write %d is missing after the move", id)
			}
		}

		args, err := c.Kubectl(ctx, nil, "-n", testNamespace, "get", "pods", "-l", sel, "-o",
			`jsonpath={range .items[*]}{.metadata.name}{" "}{.spec.containers[*].args}{"\n"}{end}`)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(args, "\n") {
			for _, flag := range []string{"--tls-accept-plaintext", "--tls-dial-plaintext", "--insecure-dev"} {
				if strings.Contains(line, flag) {
					t.Errorf("after the move a pod still runs %s: %s", flag, line)
				}
			}
		}
	})

	t.Run("MembersRequireMutualTLS", func(t *testing.T) {
		out, err := c.Kubectl(ctx, nil, "-n", testNamespace, "get", "pods", "-l", sel+",pgshard.io/group", "-o",
			`jsonpath={range .items[*]}{.metadata.annotations.pgshard\.io/agent-mtls}{"\n"}{end}`)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range strings.Fields(out) {
			if v != "true" {
				t.Fatalf("a member of an issuing cluster was started without requiring agent mTLS: %q", out)
			}
		}
	})

	t.Run("StatementsReachAShardThroughTheRouter", func(t *testing.T) {
		if out, err := psqlRetryOn(ctx, c, router, appDatabase, "INSERT INTO tls_probe VALUES (1) ON CONFLICT DO NOTHING", 5*time.Minute); err != nil {
			t.Fatalf("a write through the router of an issuing cluster: %q %v", out, err)
		}
		if out, err := psqlOn(ctx, c, router, appDatabase, "SELECT count(*) FROM tls_probe"); err != nil || out != "1" {
			t.Fatalf("a read through the router of an issuing cluster: %q %v", out, err)
		}
	})

	t.Run("APrimaryFailsOverAndTheRouterFollows", func(t *testing.T) {
		old, oldEpoch := primaryOf(), epochOf()
		if old == "" {
			t.Fatal("no primary recorded for the shard group")
		}
		if _, err := c.Kubectl(ctx, nil, "-n", testNamespace, "delete", "pod", old, "--wait=false"); err != nil {
			t.Fatal(err)
		}
		// Promotion is an operator -> agent call over mTLS.
		waitFor(ctx, t, "promotion of a standby", 6*time.Minute, func() bool {
			return primaryOf() != old && epochOf() == oldEpoch+1
		})
		if out, err := psqlRetryOn(ctx, c, router, appDatabase, "INSERT INTO tls_probe VALUES (2) ON CONFLICT DO NOTHING", 5*time.Minute); err != nil {
			t.Fatalf("a write through the router after failover: %q %v", out, err)
		}
		if out, err := psqlOn(ctx, c, router, appDatabase, "SELECT count(*) FROM tls_probe"); err != nil || out != "2" {
			t.Fatalf("the rows written before and after failover, read through the router: %q %v", out, err)
		}
		if err := waitCondition(ctx, c, "Ready", 10*time.Minute); err != nil {
			gatherNamespace(ctx, c)
			t.Fatal(err)
		}
	})

	// PGS-861. The two remaining operator-initiated mTLS hops that PGS-860's
	// acceptance listed and its cell never took. Both are covered by unit
	// handshake tests; neither had been made against a real cluster.
	//
	// The barrier is taken by SCHEDULE rather than by this test, and that is
	// the whole point: an in-process barrier (as the backup suite takes)
	// speaks to the catalog directly and would prove nothing about mTLS. A
	// scheduled one makes the OPERATOR call the controller, verifying
	// <cluster>-controller.<ns>.svc and the controller role.
	t.Run("ABackupAndAScheduledBarrierOnAnIssuingCluster", func(t *testing.T) {
		if _, err := c.Kubectl(ctx, nil, "apply", "-f", filepath.Join(root, "hack/objectstores/k8s/minio.yaml")); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			_, _ = c.Kubectl(ctx, nil, "delete", "namespace", "objectstores", "--ignore-not-found", "--wait=false")
		})
		if err := c.WaitPodsReady(ctx, "objectstores", "app=minio", 5*time.Minute); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Kubectl(ctx, nil, "-n", "objectstores", "wait", "--for=condition=complete",
			"job/minio-create-bucket", "--timeout=5m"); err != nil {
			t.Fatal(err)
		}

		if err := c.Apply(ctx, fmt.Sprintf(`
apiVersion: v1
kind: Secret
metadata: {name: %[1]s-store-credentials, namespace: %[2]s}
stringData: {key: minioadmin, keySecret: minioadmin}
---
apiVersion: v1
kind: Secret
metadata: {name: %[1]s-repo-key, namespace: %[2]s}
# The key must be named passphrase: the agent reads
# /etc/pgshard-backup/encryption/passphrase from this Secret's mount, and
# any other key name leaves the file absent. Naming it "key" here is what
# crash-looped demo-catalog-1 and held the rollout (docs/backup.md:32).
stringData: {passphrase: %[1]s-repo-passphrase}
---
apiVersion: pgshard.io/v1alpha1
kind: PgShardBackupPolicy
metadata: {name: %[1]s-policy, namespace: %[2]s}
spec:
  objectStore:
    type: s3
    bucket: pgshard
    endpoint: http://minio.objectstores.svc:9000
    region: us-east-1
    uriStyle: path
    verifyTLS: false
    prefix: /%[1]s
    credentials:
      secretRef: {name: %[1]s-store-credentials}
    encryption:
      secretRef: {name: %[1]s-repo-key}
  barrierSchedule: "* * * * *"
  retention:
    full: 2
`, clusterName, testNamespace)); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Kubectl(ctx, nil, "-n", testNamespace, "patch", "pgshardcluster", clusterName,
			"--type=merge", "-p", `{"spec":{"backup":{"policyRef":"`+clusterName+`-policy"}}}`); err != nil {
			t.Fatal(err)
		}

		// Wait for the policy to REACH the members before asking for a
		// backup. Patching policyRef onto a running cluster is not the same
		// as creating one with it: the operator still has to render each
		// member's pgbackrest configuration, and a backup asked for before
		// that lands fails with "no backup policy configured for this
		// member". The backup suite never sees this because it creates its
		// cluster with the policy already set.
		//
		// NOT BackupHealthy, on either object: both derive that condition
		// from COMPLETED backups, so waiting for it before taking one is
		// circular -- it cannot go true until the thing it is gating has
		// already happened.
		//
		// Nor the POLICY's acceptedGeneration, which is what this waited on
		// first and is the wrong object: it says the policy validated, not
		// that this cluster picked it up. Ready was wrong for a second
		// reason -- it was already true from the subtest before, so the
		// wait returned at once. Together the gate passed in 14 seconds and
		// every group failed the backup with "no backup policy configured
		// for this member". The cluster's own observedGeneration is what
		// says the operator has processed THIS patch.
		gen := jsonpath(ctx, t, c, "pgshardcluster", clusterName, "{.metadata.generation}")
		if gen == "" {
			t.Fatal("the cluster reports no metadata.generation after the policyRef patch")
		}
		waitFor(ctx, t, "the operator to observe the policyRef patch", 5*time.Minute, func() bool {
			return jsonpath(ctx, t, c, "pgshardcluster", clusterName, "{.status.observedGeneration}") == gen
		})

		// What actually delivers the policy is a ROLL, and this subtest is
		// expensive because of it. The policy is part of the MEMBER
		// TEMPLATE, not of the settings the agent can reload: render.go puts
		// Backup outside Settings because "it changes the pod (mounted
		// Secrets) and archive_mode, so it is part of the pod hash". So
		// attaching one replaces every member of every group, one at a time,
		// behind the usual gates -- and until that finishes, a member that
		// has not been replaced yet answers "no backup policy configured for
		// this member" (PGS-948).
		//
		// There is deliberately NO wait on the roll here. Two attempts at
		// one were both wrong in ways worth recording: the settings-hash
		// annotation cannot move for this change at all, since it covers
		// Settings alone; and PgShardGroup has no status.phase -- the phase
		// is on status.rollout, which is ABSENT once the roll is done, so a
		// wait written against the wrong path sat at the empty string for
		// twenty minutes whatever the cluster was doing. A wait that cannot
		// succeed is worse than no wait.
		//
		// The retry below is what carries it, and it is honest about what it
		// is waiting for: it asks the member, which is the only thing that
		// reports the member's own view of the policy.
		if err := waitCondition(ctx, c, "Ready", 10*time.Minute); err != nil {
			gatherNamespace(ctx, c)
			t.Fatal(err)
		}

		// The backup is an operator -> agent call carrying the per-cluster
		// <cluster>-tls-operator credential.
		backup := fmt.Sprintf(`
apiVersion: pgshard.io/v1alpha1
kind: PgShardBackup
metadata: {name: %[1]s-tls-backup, namespace: %[2]s}
spec:
  clusterName: %[1]s
  type: full
`, clusterName, testNamespace)
		phase := func() string {
			out, _ := c.Kubectl(ctx, nil, "-n", testNamespace, "get", "pgshardbackup", clusterName+"-tls-backup",
				"-o", "jsonpath={.status.phase}")
			return strings.TrimSpace(out)
		}

		// Observing the reconcile is not the same as every agent having
		// reloaded its configuration, and NOTHING reports an agent's view
		// of the policy: the member answers ErrNoBackupPolicy from its own
		// config (internal/agent/backupops.go), and no per-member status
		// names the policy it holds. So there is no condition left to wait
		// on -- the only thing that reports the agent's view is asking it.
		// This retries for that reason, and an operator meeting the same
		// error has the same recourse and no better signal.
		var last string
		succeeded := false
		deadline := time.Now().Add(15 * time.Minute)
		for time.Now().Before(deadline) && !succeeded {
			if err := c.Apply(ctx, backup); err != nil {
				t.Fatal(err)
			}
			for time.Now().Before(deadline) {
				p := phase()
				if p == "Succeeded" {
					succeeded = true
					break
				}
				if p == "Failed" {
					last, _ = c.Kubectl(ctx, nil, "-n", testNamespace, "get", "pgshardbackup",
						clusterName+"-tls-backup", "-o", "yaml")
					break
				}
				time.Sleep(2 * time.Second)
			}
			if succeeded {
				break
			}
			// Any other failure is the thing this subtest is here to catch:
			// the operator -> agent mTLS call not carrying.
			if !strings.Contains(last, "no backup policy configured") {
				gatherNamespace(ctx, c)
				t.Fatalf("the backup failed on an issuing cluster, so the operator -> agent mTLS call did not carry:\n%s", last)
			}
			if _, err := c.Kubectl(ctx, nil, "-n", testNamespace, "delete", "pgshardbackup",
				clusterName+"-tls-backup", "--ignore-not-found"); err != nil {
				t.Fatal(err)
			}
			time.Sleep(5 * time.Second)
		}
		if !succeeded {
			gatherNamespace(ctx, c)
			t.Fatalf("the backup of an issuing cluster never succeeded; last state:\n%s", last)
		}

		// And the barrier: the operator asks the CONTROLLER, over mTLS. Read
		// through the router's pgshard database, which routes to the catalog
		// set -- so this observation crosses router -> pooler mTLS too.
		waitFor(ctx, t, "a scheduled barrier to reach the catalog", 10*time.Minute, func() bool {
			out, err := psqlOn(ctx, c, router, "pgshard", "SELECT count(*) FROM pgshard.restore_points WHERE certified")
			return err == nil && out != "" && out != "0"
		})
	})
}
