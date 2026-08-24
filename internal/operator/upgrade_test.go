package operator

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

func upgradeCluster(major int) *pgshardv1alpha1.PgShardCluster {
	c := &pgshardv1alpha1.PgShardCluster{}
	c.Name = "demo"
	c.Spec.PostgreSQL.Major = major
	return c
}

func TestUpgradeRequested(t *testing.T) {
	c := upgradeCluster(19)
	if !UpgradeRequested(c, &ShardSetInfo{Name: "default", PGMajor: 18}) {
		t.Fatal("major 18 -> 19 must trigger")
	}
	if UpgradeRequested(c, &ShardSetInfo{Name: "default", PGMajor: 19}) {
		t.Fatal("same major must not trigger")
	}
	if UpgradeRequested(c, &ShardSetInfo{Name: "default"}) {
		t.Fatal("an unstamped serving set must never trigger")
	}
	if UpgradeRequested(c, nil) {
		t.Fatal("no serving set must not trigger")
	}
	c.Spec.Upgrade.Strategy = UpgradeStrategyOffline
	if UpgradeRequested(c, &ShardSetInfo{Name: "default", PGMajor: 18}) {
		t.Fatal("the offline strategy must not start an online replacement")
	}
	if UpgradeRequested(upgradeCluster(18), &ShardSetInfo{Name: "default", PGMajor: 19}) {
		t.Fatal("downgrades must not trigger")
	}
}

func TestUpgradeBlockers(t *testing.T) {
	c := upgradeCluster(19)
	if b := UpgradeBlockers(c, nil, nil); len(b) != 0 {
		t.Fatalf("clean cluster blocked: %v", b)
	}
	c.Spec.PostgreSQL.Image = "example.invalid/postgres:18"
	if b := UpgradeBlockers(c, nil, nil); len(b) != 1 || !strings.Contains(b[0], "does not name major 19") {
		t.Fatalf("image mismatch: %v", b)
	}
	c.Spec.PostgreSQL.Image = ""
	c.Spec.Backup.PolicyRef = "nightly"
	if b := UpgradeBlockers(c, nil, nil); len(b) != 1 || !strings.Contains(b[0], "backups are not healthy") {
		t.Fatalf("backup health: %v", b)
	}
	c.Status.Conditions = []metav1.Condition{{Type: pgshardv1alpha1.ConditionBackupHealthy, Status: metav1.ConditionTrue}}
	if b := UpgradeBlockers(c, nil, nil); len(b) != 0 {
		t.Fatalf("healthy backups blocked: %v", b)
	}
	if b := UpgradeBlockers(c, &ShardSetInfo{Name: "g2"}, nil); len(b) != 1 || !strings.Contains(b[0], "already pending") {
		t.Fatalf("pending set: %v", b)
	}
	placements := []pgshardv1alpha1.ClusterPlacementWorkflowStatus{{WorkflowID: "w1", State: "running"}, {WorkflowID: "w2", State: "completed"}}
	if b := UpgradeBlockers(c, nil, placements); len(b) != 1 || !strings.Contains(b[0], "w1") {
		t.Fatalf("placements: %v", b)
	}
}

func TestImageForPinsOldMajorDuringUpgrade(t *testing.T) {
	c := upgradeCluster(19)
	if got := ImageFor(c, Group{PGMajor: 18}); got != DefaultImageRepository+":18" {
		t.Fatalf("old groups must keep their major's image: %s", got)
	}
	if got, want := ImageFor(c, Group{PGMajor: 19}), Image(c); got != want {
		t.Fatalf("new-major groups follow the spec image: %s != %s", got, want)
	}
	if got, want := ImageFor(c, Group{}), Image(c); got != want {
		t.Fatalf("unstamped groups follow the spec image: %s != %s", got, want)
	}
	c.Spec.PostgreSQL.Image = "example.invalid/postgres:19"
	if got := ImageFor(c, Group{PGMajor: 18}); got != DefaultImageRepository+":18" {
		t.Fatalf("a custom spec image must not leak to old-major groups: %s", got)
	}
}

func TestUpgradeJob(t *testing.T) {
	c := upgradeCluster(19)
	c.Namespace = "prod"
	g := Group{Cluster: "demo", Kind: "shard", ShardID: 1, Replicas: 3, Generation: 2}
	job := UpgradeJob(c, g, "demo-shard-1-g2-0", 18, 19)
	if job.Name != "demo-shard-1-g2-pg-upgrade" || job.Namespace != "prod" {
		t.Fatalf("name %s/%s", job.Namespace, job.Name)
	}
	ctr := job.Spec.Template.Spec.Containers[0]
	cmd := strings.Join(ctr.Command, " ")
	for _, want := range []string{"pgshard-agent upgrade", "--old-major 18", "--new-major 19", "--old-data " + pgdataPath} {
		if !strings.Contains(cmd, want) {
			t.Fatalf("command %q misses %q", cmd, want)
		}
	}
	if ctr.Image != DefaultImageRepository+":19" {
		t.Fatalf("image %s", ctr.Image)
	}
	if pvc := job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim; pvc == nil || pvc.ClaimName != "demo-shard-1-g2-0" {
		t.Fatalf("volume %+v", job.Spec.Template.Spec.Volumes[0])
	}
}
