//go:build e2e

package upgrade

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/test/e2e"
)

// TestUpgrade18To19UnderLoad provisions a one-shard cluster (plus catalog)
// on PostgreSQL 18, runs a ledger workload against the serving primary,
// bumps spec.postgresql.major to 19 and asserts, in order: the blue/green
// replacement switches with no acknowledged write lost or duplicated and
// the write pause recorded; a rollback annotation before retirement
// returns serving to the 18 groups with the ledger intact; the re-run
// upgrade completes and retires the old groups; the catalog group follows
// onto 19 behind the stable catalog Service; and the whole cluster ends on
// 19. Backup-stanza continuity is covered by the backup suite (the stanza
// is derived per group, so new-major groups archive into fresh stanzas);
// VStream continuity across the cutover is NOT asserted here. The stream
// stack does exist now (internal/router/vstream), so the original reason --
// waiting for a rebase -- no longer applies; it is simply unwritten, and
// PGS-351 tracks the gap it would cover.
func TestUpgrade18To19UnderLoad(t *testing.T) {
	c := e2e.NewCluster(t)
	c.GatherOnFailure(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()
	bringUpCluster(ctx, t, c, "2m")
	l := startLedger(ctx, t, c)
	waitForWhy(ctx, t, c, "first acknowledged ledger writes", 2*time.Minute, l.why,
		func() bool { return l.acked.Load() >= 25 })

	if err := c.Apply(ctx, clusterManifest(19, "2m")); err != nil {
		t.Fatal(err)
	}
	record := clusterName + "-reshard-g2"
	waitFor(ctx, t, c, "upgrade record in mode upgrade", 5*time.Minute, func() bool {
		return jsonpath(ctx, c, "pgshardreshard", record, "{.spec.mode}") == "upgrade"
	})
	waitForWhy(ctx, t, c, "kind=upgrade workflow running", 5*time.Minute, func() string {
		return upgradeWorkflowDetail(ctx, t, c)
	}, func() bool {
		return strings.HasPrefix(upgradeWorkflowState(ctx, t, c, "g2"), "provisioning:") ||
			strings.HasPrefix(upgradeWorkflowState(ctx, t, c, "g2"), "running:")
	})
	waitForWhy(ctx, t, c, "writes switched to the 19 set", 30*time.Minute, func() string {
		return upgradeWorkflowDetail(ctx, t, c)
	}, func() bool {
		if st := upgradeWorkflowState(ctx, t, c, "g2"); strings.HasPrefix(st, "failed") {
			t.Fatalf("upgrade workflow failed: %s", catalogSQL(ctx, t, c, "SELECT coalesce(error, '') || ' ' || status::text FROM pgshard.workflows WHERE kind = 'upgrade'"))
		}
		return servingShardGroup(ctx, c) == "shard-0-g2"
	})
	if got, err := psql(ctx, c, clusterName+"-shard-0-g2-rw", "postgres", "SELECT current_setting('server_version_num')::int / 10000"); err != nil || got != "19" {
		t.Fatalf("new serving primary major: %q %v", got, err)
	}
	if pause := catalogSQL(ctx, t, c, "SELECT coalesce((status->'cutover'->>'pause_ms')::bigint, -1) FROM pgshard.workflows WHERE kind = 'upgrade' AND spec->>'shard_set' = 'g2'"); pause == "-1" || pause == "0" {
		t.Errorf("cutover pause not recorded: %q", pause)
	}
	// Snapshot the high-water AFTER the switch is observed. acked > 0 was
	// already true before the upgrade began -- the writer runs throughout --
	// so it asserted nothing about the new major. A primary that serves
	// reads and rejects every write passed this.
	ackedAfterSwitch := l.acked.Load()
	waitFor(ctx, t, c, "writes acknowledged on the new major after the switch", 3*time.Minute, func() bool {
		return l.acked.Load() > ackedAfterSwitch && servingShardGroup(ctx, c) == "shard-0-g2"
	})
	l.verify(ctx, t, l.acked.Load())

	// VDiff-lite across the pair while both sets exist: the retired 18
	// primary must hold every row up to the switch (reverse replication
	// keeps it current until retirement).
	ackedAtSwitch := l.acked.Load()
	oldCount, err := psql(ctx, c, clusterName+"-shard-0-rw", appDatabase, "SELECT count(DISTINCT id) FROM ledger")
	if err != nil {
		t.Fatalf("retired primary unreachable before retirement: %v", err)
	}
	if oldCount == "0" {
		t.Error("reverse replication left the retired primary empty")
	}

	// Rollback before retirement: serving must return to the 18 groups.
	if _, err := c.Kubectl(ctx, nil, "-n", testNamespace, "annotate", "pgshardreshard", record, "pgshard.io/upgrade=rollback", "--overwrite"); err != nil {
		t.Fatal(err)
	}
	waitFor(ctx, t, c, "rollback to the 18 set", 15*time.Minute, func() bool {
		return servingShardGroup(ctx, c) == "shard-0"
	})
	waitFor(ctx, t, c, "rolled-back workflow closed", 10*time.Minute, func() bool {
		st := upgradeWorkflowState(ctx, t, c, "g2")
		return strings.HasPrefix(st, "cancelled:") || strings.HasPrefix(st, "completed:") || st == ""
	})
	waitFor(ctx, t, c, "writes flowing again on 18", 5*time.Minute, func() bool { return l.acked.Load() > ackedAtSwitch })
	l.verify(ctx, t, l.acked.Load())

	// The spec still asks for 19, so a fresh run (g3) starts and this time
	// runs to retirement.
	record3 := clusterName + "-reshard-g3"
	waitFor(ctx, t, c, "re-run upgrade record", 10*time.Minute, func() bool {
		return jsonpath(ctx, c, "pgshardreshard", record3, "{.spec.mode}") == "upgrade"
	})
	waitForWhy(ctx, t, c, "writes switched to the 19 set again", 30*time.Minute, func() string {
		return upgradeWorkflowDetail(ctx, t, c)
	}, func() bool {
		if st := upgradeWorkflowState(ctx, t, c, "g3"); strings.HasPrefix(st, "failed") {
			t.Fatalf("upgrade workflow failed: %s", catalogSQL(ctx, t, c, "SELECT coalesce(error, '') || ' ' || status::text FROM pgshard.workflows WHERE kind = 'upgrade' AND spec->>'shard_set' = 'g3'"))
		}
		return servingShardGroup(ctx, c) == "shard-0-g3"
	})
	waitFor(ctx, t, c, "old groups retired and deleted", 20*time.Minute, func() bool {
		st := upgradeWorkflowState(ctx, t, c, "g3")
		return strings.HasPrefix(st, "completed:") &&
			jsonpath(ctx, c, "pgshardcluster", clusterName, "{.status.servingPGMajor}") == "19"
	})

	// The catalog group goes last: a new-major catalog group comes up, the
	// pgshard database is copied over logical replication and the stable
	// catalog Service is repointed without the routers' DSN changing.
	waitFor(ctx, t, c, "catalog group on 19", 25*time.Minute, func() bool {
		return jsonpath(ctx, c, "pgshardcluster", clusterName, "{.status.catalogPGMajor}") == "19"
	})
	if got, err := psql(ctx, c, clusterName+"-catalog-rw", "postgres", "SELECT current_setting('server_version_num')::int / 10000"); err != nil || got != "19" {
		t.Fatalf("stable catalog endpoint after the flip: %q %v", got, err)
	}
	if sel := jsonpath(ctx, c, "service", clusterName+"-catalog-rw", "{.spec.selector.pgshard\\.io/group}"); sel != "catalog-g2" {
		t.Errorf("stable catalog Service selector: %q, want catalog-g2", sel)
	}
	waitFor(ctx, t, c, "catalog upgrade finished", 15*time.Minute, func() bool {
		return jsonpath(ctx, c, "pgshardcluster", clusterName, "{.status.catalogUpgrade}") == ""
	})

	acked := l.finish()
	if acked <= ackedAtSwitch {
		t.Fatalf("ledger made no progress after the switch: %d then %d", ackedAtSwitch, acked)
	}
	l.verify(ctx, t, acked)
	if majors := catalogSQL(ctx, t, c, "SELECT string_agg(DISTINCT pg_major::text, ',') FROM pgshard.shard_sets WHERE state = 'serving'"); majors != "19" {
		t.Fatalf("serving majors at the end: %q", majors)
	}
}

// TestUpgrade18To19ChaosControllerAndPrimaryKill kills the controller pod
// mid-copy and the promoted new-major primary right after the switch, and
// asserts the upgrade still converges with the ledger intact.
func TestUpgrade18To19ChaosControllerAndPrimaryKill(t *testing.T) {
	c := e2e.NewCluster(t)
	c.GatherOnFailure(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()
	bringUpCluster(ctx, t, c, "1m")
	l := startLedger(ctx, t, c)
	waitForWhy(ctx, t, c, "first acknowledged ledger writes", 2*time.Minute, l.why,
		func() bool { return l.acked.Load() >= 25 })

	if err := c.Apply(ctx, clusterManifest(19, "1m")); err != nil {
		t.Fatal(err)
	}
	waitFor(ctx, t, c, "upgrade workflow copying", 15*time.Minute, func() bool {
		return strings.HasPrefix(upgradeWorkflowState(ctx, t, c, "g2"), "running:")
	})
	// Chaos 1: the controller dies mid-copy; the Deployment restarts it
	// and the workflow resumes from its durable state.
	if _, err := c.Kubectl(ctx, nil, "-n", testNamespace, "delete", "pod", "-l", "app="+clusterName+"-controller", "--wait=false"); err != nil {
		t.Fatal(err)
	}
	waitForWhy(ctx, t, c, "writes switched to the 19 set", 30*time.Minute, func() string {
		return upgradeWorkflowDetail(ctx, t, c)
	}, func() bool {
		if st := upgradeWorkflowState(ctx, t, c, "g2"); strings.HasPrefix(st, "failed") {
			t.Fatalf("upgrade workflow failed: %s", catalogSQL(ctx, t, c, "SELECT coalesce(error, '') || ' ' || status::text FROM pgshard.workflows WHERE kind = 'upgrade'"))
		}
		return servingShardGroup(ctx, c) == "shard-0-g2"
	})
	// Chaos 2: the promoted new-major primary dies right after the switch;
	// the group fails over and the workflow still completes.
	primary := jsonpath(ctx, c, "pgshardgroup", clusterName+"-shard-0-g2", "{.status.primary}")
	if primary == "" {
		primary = clusterName + "-shard-0-g2-0"
	}
	if _, err := c.Kubectl(ctx, nil, "-n", testNamespace, "delete", "pod", primary, "--wait=false"); err != nil {
		t.Fatal(err)
	}
	waitFor(ctx, t, c, "upgrade completed after the chaos", 30*time.Minute, func() bool {
		return strings.HasPrefix(upgradeWorkflowState(ctx, t, c, "g2"), "completed:") &&
			jsonpath(ctx, c, "pgshardcluster", clusterName, "{.status.servingPGMajor}") == "19"
	})
	waitFor(ctx, t, c, "writes flowing after the chaos", 10*time.Minute, func() bool {
		before := l.acked.Load()
		time.Sleep(5 * time.Second)
		return l.acked.Load() > before
	})
	acked := l.finish()
	l.verify(ctx, t, acked)
}
