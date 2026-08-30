package admin

import (
	"context"
	"fmt"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

// UpgradeRow is one group set inside a cluster's major upgrade: the shard
// set replacement or the catalog group.
type UpgradeRow struct {
	Scope     string   `json:"scope"`
	FromMajor int      `json:"fromMajor"`
	ToMajor   int      `json:"toMajor"`
	Stage     string   `json:"stage"`
	Message   string   `json:"message,omitempty"`
	Blockers  []string `json:"blockers,omitempty"`
	Rollback  string   `json:"rollback,omitempty"`
}

// ClusterUpgrade is one cluster's major-upgrade state as the UI shows it.
type ClusterUpgrade struct {
	Namespace    string       `json:"namespace"`
	Name         string       `json:"name"`
	SpecMajor    int          `json:"specMajor"`
	ServingMajor int          `json:"servingMajor"`
	CatalogMajor int          `json:"catalogMajor"`
	State        string       `json:"state"`
	Rows         []UpgradeRow `json:"rows,omitempty"`
}

// UpgradesPage is the /upgrades document.
type UpgradesPage struct {
	Namespace string           `json:"namespace,omitempty"`
	Clusters  []ClusterUpgrade `json:"clusters"`
}

// BuildUpgradesPage assembles the upgrade progress of every cluster from
// PgShardCluster status: the shard-set replacement run (status.reshard
// with a pgMajor), the catalog group upgrade (status.catalogUpgrade) and
// the blockers the operator reports on the Resharding condition.
func BuildUpgradesPage(ctx context.Context, c client.Reader, namespace, cluster string) (UpgradesPage, error) {
	var list pgshardv1alpha1.PgShardClusterList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return UpgradesPage{}, err
	}
	list.Items = onlyCluster(list.Items, cluster)
	page := UpgradesPage{Namespace: namespace}
	for i := range list.Items {
		page.Clusters = append(page.Clusters, clusterUpgrade(ctx, c, &list.Items[i]))
	}
	sort.Slice(page.Clusters, func(i, j int) bool {
		a, b := page.Clusters[i], page.Clusters[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})
	return page, nil
}

func clusterUpgrade(ctx context.Context, c client.Reader, pc *pgshardv1alpha1.PgShardCluster) ClusterUpgrade {
	cu := ClusterUpgrade{
		Namespace: pc.Namespace, Name: pc.Name,
		SpecMajor:    pc.Spec.PostgreSQL.Major,
		ServingMajor: pc.Status.ServingPGMajor,
		CatalogMajor: pc.Status.CatalogPGMajor,
	}
	if rs := pc.Status.Reshard; rs != nil && rs.PGMajor != 0 && rs.PGMajor != pc.Status.ServingPGMajor {
		row := UpgradeRow{Scope: "shard set " + rs.ShardSet, FromMajor: pc.Status.ServingPGMajor, ToMajor: rs.PGMajor, Stage: rs.Phase}
		record := &pgshardv1alpha1.PgShardReshard{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: pc.Namespace, Name: rs.Name}, record); err == nil {
			row.Message = record.Status.Message
			if record.Annotations[pgshardv1alpha1.AnnotationUpgrade] == pgshardv1alpha1.UpgradeActionRollback {
				row.Rollback = "requested"
			}
		} else if !apierrors.IsNotFound(err) {
			row.Message = err.Error()
		}
		cu.Rows = append(cu.Rows, row)
	}
	if rs := pc.Status.Reshard; rs != nil && rs.RetiredPGMajor != 0 && rs.RetiredPGMajor != rs.PGMajor {
		cu.Rows = append(cu.Rows, UpgradeRow{Scope: "retired shard set " + rs.RetiredShardSet,
			FromMajor: rs.RetiredPGMajor, ToMajor: rs.PGMajor, Stage: "retiring",
			Message: "old-major groups kept current over reverse replication; rollback is possible until retirement"})
	}
	for _, cond := range pc.Status.Conditions {
		if cond.Type == pgshardv1alpha1.ConditionResharding && cond.Reason == "UpgradeBlocked" {
			cu.Rows = append(cu.Rows, UpgradeRow{Scope: "shard sets", FromMajor: pc.Status.ServingPGMajor, ToMajor: pc.Spec.PostgreSQL.Major,
				Stage: "blocked", Blockers: parseBlockers(cond.Message)})
		}
	}
	if up := pc.Status.CatalogUpgrade; up != nil {
		row := UpgradeRow{Scope: "catalog group", FromMajor: up.FromMajor, ToMajor: up.ToMajor, Stage: up.Stage, Message: up.Message, Blockers: up.Blockers}
		if up.RollbackRequested {
			row.Rollback = "requested"
		}
		cu.Rows = append(cu.Rows, row)
	}
	switch {
	case len(cu.Rows) > 0:
		cu.State = "upgrading"
	case cu.SpecMajor != 0 && cu.ServingMajor == cu.SpecMajor && (cu.CatalogMajor == 0 || cu.CatalogMajor == cu.SpecMajor):
		cu.State = fmt.Sprintf("on %d", cu.SpecMajor)
	case cu.ServingMajor == 0:
		cu.State = "unknown"
	default:
		cu.State = "pending"
	}
	return cu
}

// parseBlockers splits the operator's "upgrade to major N blocked: a; b"
// condition message into its blockers.
func parseBlockers(msg string) []string {
	_, rest, ok := strings.Cut(msg, "blocked: ")
	if !ok {
		return []string{msg}
	}
	parts := strings.Split(rest, "; ")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
