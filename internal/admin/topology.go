// Package admin serves the read-only pgshard admin web UI.
package admin

import (
	"context"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/controller"
	"github.com/andrew01234567890/pgshard/internal/operator"
)

// ClusterSummary is one row of the clusters list.
type ClusterSummary struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Shards    int    `json:"shards"`
	Ready     string `json:"ready"`
	Major     int    `json:"postgresMajor"`
}

// Member is one PostgreSQL instance of a group as the UI shows it.
type Member struct {
	Name           string `json:"name"`
	Role           string `json:"role"`
	Ready          bool   `json:"ready"`
	Epoch          int64  `json:"epoch"`
	ReplayLagBytes int64  `json:"replayLagBytes"`
	PodPhase       string `json:"podPhase"`
	Node           string `json:"node"`
}

// Group is one replication group with its members.
type Group struct {
	Name    string   `json:"name"`
	Kind    string   `json:"kind"`
	ShardID *int     `json:"shardId,omitempty"`
	Primary string   `json:"primary"`
	Epoch   int64    `json:"epoch"`
	Members []Member `json:"members"`
}

// Shard is one entry of the shard map.
type Shard struct {
	ID         int    `json:"id"`
	RangeStart int64  `json:"rangeStart"`
	RangeEnd   int64  `json:"rangeEnd"`
	Primary    string `json:"primary"`
	Epoch      int64  `json:"epoch"`
}

// CatalogShard is one row of the catalog's shard status, present only when
// the server was started with a catalog DSN.
type CatalogShard struct {
	ShardID        int32  `json:"shardId"`
	GroupName      string `json:"groupName"`
	ServingState   string `json:"servingState"`
	PrimaryEpoch   int64  `json:"primaryEpoch"`
	ReplayLagBytes *int64 `json:"replayLagBytes,omitempty"`
}

// Condition is a cluster status condition.
type Condition struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

// Topology is the /api/v1/clusters/{ns}/{name} document.
type Topology struct {
	Namespace          string         `json:"namespace"`
	Name               string         `json:"name"`
	PostgresMajor      int            `json:"postgresMajor"`
	ShardMapGeneration int64          `json:"shardMapGeneration"`
	ObservedGeneration int64          `json:"observedGeneration"`
	Conditions         []Condition    `json:"conditions"`
	Groups             []Group        `json:"groups"`
	Shards             []Shard        `json:"shards"`
	Catalog            []CatalogShard `json:"catalog,omitempty"`
	CatalogError       string         `json:"catalogError,omitempty"`
}

// CatalogSource reads the shard status snapshot from the catalog database.
type CatalogSource interface {
	ShardStatus(ctx context.Context) ([]catalog.ShardStatus, error)
	// RestorePoints lists the certified barriers, newest first.
	RestorePoints(ctx context.Context) ([]controller.RestorePoint, error)
}

// ListClusters summarizes every PgShardCluster in namespace (all if empty).
func ListClusters(ctx context.Context, c client.Reader, namespace string) ([]ClusterSummary, error) {
	var list pgshardv1alpha1.PgShardClusterList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	out := make([]ClusterSummary, 0, len(list.Items))
	for i := range list.Items {
		pc := &list.Items[i]
		s := ClusterSummary{Namespace: pc.Namespace, Name: pc.Name, Shards: 1, Major: pc.Spec.PostgreSQL.Major, Ready: "Unknown"}
		if pc.Spec.Shards != nil {
			s.Shards = *pc.Spec.Shards
		}
		for _, cond := range pc.Status.Conditions {
			if cond.Type == pgshardv1alpha1.ConditionReady {
				s.Ready = string(cond.Status)
			}
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// BuildTopology assembles the topology of one cluster from the API server
// and, when src is non-nil, the catalog database.
func BuildTopology(ctx context.Context, c client.Reader, src CatalogSource, namespace, name string) (*Topology, error) {
	var pc pgshardv1alpha1.PgShardCluster
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &pc); err != nil {
		return nil, err
	}
	sel := client.MatchingLabels{operator.LabelCluster: name}
	var groups pgshardv1alpha1.PgShardGroupList
	if err := c.List(ctx, &groups, client.InNamespace(namespace), sel); err != nil {
		return nil, err
	}
	var pods corev1.PodList
	if err := c.List(ctx, &pods, client.InNamespace(namespace), sel); err != nil {
		return nil, err
	}
	podByName := make(map[string]*corev1.Pod, len(pods.Items))
	for i := range pods.Items {
		podByName[pods.Items[i].Name] = &pods.Items[i]
	}

	t := &Topology{
		Namespace:          pc.Namespace,
		Name:               pc.Name,
		PostgresMajor:      pc.Spec.PostgreSQL.Major,
		ShardMapGeneration: pc.Status.ShardMapGeneration,
		ObservedGeneration: pc.Status.ObservedGeneration,
		Conditions:         []Condition{},
		Groups:             []Group{},
		Shards:             []Shard{},
	}
	for _, cond := range pc.Status.Conditions {
		t.Conditions = append(t.Conditions, Condition{Type: cond.Type, Status: string(cond.Status), Reason: cond.Reason, Message: cond.Message})
	}
	for i := range groups.Items {
		pg := &groups.Items[i]
		g := Group{Name: pg.Name, Kind: pg.Spec.Kind, ShardID: pg.Spec.ShardID, Primary: pg.Status.Primary, Epoch: pg.Status.Epoch, Members: []Member{}}
		for _, m := range pg.Status.Members {
			mem := Member{Name: m.Name, Role: m.Role, Ready: m.Ready, Epoch: pg.Status.Epoch, ReplayLagBytes: m.ReplayLagBytes}
			if pod, ok := podByName[m.Name]; ok {
				mem.PodPhase = string(pod.Status.Phase)
				mem.Node = pod.Spec.NodeName
			}
			g.Members = append(g.Members, mem)
		}
		t.Groups = append(t.Groups, g)
	}
	sort.Slice(t.Groups, func(i, j int) bool { return groupLess(t.Groups[i], t.Groups[j]) })
	for _, s := range pc.Status.Shards {
		t.Shards = append(t.Shards, Shard{ID: s.ID, RangeStart: s.RangeStart, RangeEnd: s.RangeEnd, Primary: s.Primary, Epoch: s.Epoch})
	}
	sort.Slice(t.Shards, func(i, j int) bool { return t.Shards[i].ID < t.Shards[j].ID })
	if src != nil {
		rows, err := src.ShardStatus(ctx)
		if err != nil {
			t.CatalogError = err.Error()
		}
		for _, r := range rows {
			t.Catalog = append(t.Catalog, CatalogShard{ShardID: r.ShardID, GroupName: r.GroupName, ServingState: r.ServingState, PrimaryEpoch: r.PrimaryEpoch, ReplayLagBytes: r.ReplayLagBytes})
		}
	}
	return t, nil
}

func groupLess(a, b Group) bool {
	if a.Kind != b.Kind {
		return a.Kind == "catalog"
	}
	if a.ShardID != nil && b.ShardID != nil && *a.ShardID != *b.ShardID {
		return *a.ShardID < *b.ShardID
	}
	return a.Name < b.Name
}
