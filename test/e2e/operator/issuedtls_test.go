//go:build e2e

package operator

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/test/e2e"
)

// TestAnIssuedTLSClusterRoutesAndFailsOver runs a cluster whose internal
// certificates the operator issues -- the mode internal TLS is to be
// required in everywhere -- through the paths that each cross an mTLS hop:
// a statement through the router (router -> pooler), and a failover (operator
// -> agent). Every other suite runs insecure, which is how an issuing cluster
// that could do neither went unnoticed (PGS-860).
func TestAnIssuedTLSClusterRoutesAndFailsOver(t *testing.T) {
	c := e2e.NewCluster(t)
	c.GatherOnFailure(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	defer cancel()
	root := repoRoot(t)
	major := env("PG_MAJOR", "18")

	deployOperator(ctx, t, c, root, env("OPERATOR_IMAGE", "pgshard-operator:e2e"))
	manifest := clusterManifestTLS(major, os.Getenv("PGSHARD_POSTGRES_IMAGE"), "    issue: true\n")
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
		out, err := psqlRetryOn(ctx, c, router, "postgres",
			"CREATE TABLE IF NOT EXISTS tls_probe (id int PRIMARY KEY); INSERT INTO tls_probe VALUES (1) ON CONFLICT DO NOTHING; SELECT count(*) FROM tls_probe", 5*time.Minute)
		if err != nil || out != "1" {
			t.Fatalf("a statement through the router of an issuing cluster: %q %v", out, err)
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
		out, err := psqlRetryOn(ctx, c, router, "postgres", "INSERT INTO tls_probe VALUES (2); SELECT count(*) FROM tls_probe", 5*time.Minute)
		if err != nil || out != "2" {
			t.Fatalf("a write through the router after failover: %q %v", out, err)
		}
		if err := waitCondition(ctx, c, "Ready", 10*time.Minute); err != nil {
			gatherNamespace(ctx, c)
			t.Fatal(err)
		}
	})
}
