package operator

import (
	"fmt"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

// UpgradeStrategyOnline and UpgradeStrategyOffline are the values of
// spec.upgrade.strategy.
const (
	UpgradeStrategyOnline  = "online"
	UpgradeStrategyOffline = "offline"
)

// UpgradeRequested reports whether spec.postgresql.major asks for an online
// blue/green replacement of the serving set: the set carries a lower
// stamped major. A set without a stamp (a catalog from before upgrades)
// never triggers; stamp it via pgshard.shard_sets.pg_major first.
func UpgradeRequested(c *pgshardv1alpha1.PgShardCluster, serving *ShardSetInfo) bool {
	if serving == nil || serving.PGMajor == 0 {
		return false
	}
	if c.Spec.Upgrade.Strategy == UpgradeStrategyOffline {
		return false
	}
	return c.Spec.PostgreSQL.Major > serving.PGMajor
}

// UpgradeBlockers are the operator-side preconditions of plan §3.11: the
// target image must be built for the new major, backups must be healthy,
// and no reshard or table placement may be in flight. The data-plane checks
// (extensions, large objects) run in the controller before the copy starts.
func UpgradeBlockers(c *pgshardv1alpha1.PgShardCluster, pending *ShardSetInfo, placements []pgshardv1alpha1.ClusterPlacementWorkflowStatus) []string {
	var blockers []string
	if img := c.Spec.PostgreSQL.Image; img != "" && !strings.Contains(img, strconv.Itoa(c.Spec.PostgreSQL.Major)) {
		blockers = append(blockers, fmt.Sprintf("spec.postgresql.image %q does not name major %d; point it at an image built for the target major or clear it for the default", img, c.Spec.PostgreSQL.Major))
	}
	if c.Spec.Backup.PolicyRef != "" {
		if cond := findCondition(c.Status.Conditions, pgshardv1alpha1.ConditionBackupHealthy); cond == nil || cond.Status != metav1.ConditionTrue {
			blockers = append(blockers, "backups are not healthy; a fresh full backup must be possible before and after the upgrade")
		}
	}
	if pending != nil {
		blockers = append(blockers, fmt.Sprintf("shard set %s is already pending; wait for the reshard to finish", pending.Name))
	}
	for _, p := range placements {
		if p.State == "pending" || p.State == "running" || p.State == "paused" {
			blockers = append(blockers, fmt.Sprintf("table placement workflow %s is %s", p.WorkflowID, p.State))
		}
	}
	return blockers
}

func findCondition(conds []metav1.Condition, t string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == t {
			return &conds[i]
		}
	}
	return nil
}

// UpgradeJobName names the offline pg_upgrade Job of one group.
func UpgradeJobName(g Group) string { return g.Prefix() + "-pg-upgrade" }

// UpgradeJob renders the offline pg_upgrade --link Job for the primary PVC
// of one group. It runs "pgshard-agent upgrade", which refuses images that
// do not carry the binaries of both majors (docs/upgrade.md); the group's
// pods must be scaled down first and its replicas re-cloned afterwards.
func UpgradeJob(c *pgshardv1alpha1.PgShardCluster, g Group, primaryPVC string, oldMajor, newMajor int) *batchv1.Job {
	labels := g.Labels()
	labels[LabelComponent] = "pg-upgrade"
	one := int32(1)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: UpgradeJobName(g), Namespace: c.Namespace, Labels: labels},
		Spec: batchv1.JobSpec{
			BackoffLimit: &one,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:  "pg-upgrade",
						Image: ImageFor(c, Group{Cluster: g.Cluster, Kind: g.Kind, ShardID: g.ShardID, PGMajor: newMajor}),
						Command: []string{"pgshard-agent", "upgrade",
							"--old-major", strconv.Itoa(oldMajor),
							"--new-major", strconv.Itoa(newMajor),
							"--old-data", pgdataPath,
							"--new-data", pgdataPath + "-" + strconv.Itoa(newMajor),
						},
						VolumeMounts: []corev1.VolumeMount{{Name: "pgdata", MountPath: dataMountPath}},
					}},
					Volumes: []corev1.Volume{{Name: "pgdata", VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: primaryPVC},
					}}},
				},
			},
		},
	}
}
