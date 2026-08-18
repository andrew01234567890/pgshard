package operator

import (
	"fmt"
	"sort"
	"strings"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

// Labels applied to every object the operator manages.
const (
	LabelCluster = "pgshard.io/cluster"
	LabelGroup   = "pgshard.io/group"
	LabelKind    = "pgshard.io/group-kind"
	LabelOrdinal = "pgshard.io/ordinal"
	LabelRole    = "pgshard.io/role"

	RolePrimary = "primary"
	RoleReplica = "replica"

	// ConditionCatalogReady is set once the catalog schema migrations ran on
	// the catalog primary.
	ConditionCatalogReady = "CatalogReady"

	// DefaultImageRepository hosts the pgshard PostgreSQL images; the tag is the major.
	DefaultImageRepository = "ghcr.io/andrew01234567890/pgshard-postgres"

	postgresPort  = 5432
	superuserName = "postgres"
	secretKey     = "password"
)

// Group is one replication group derived from the cluster spec.
type Group struct {
	Cluster  string
	Kind     string
	ShardID  int
	Replicas int
	Storage  pgshardv1alpha1.StorageSpec
}

// Name is the group's short name, unique within the cluster.
func (g Group) Name() string {
	if g.Kind == "catalog" {
		return "catalog"
	}
	return fmt.Sprintf("shard-%d", g.ShardID)
}

// Prefix is <cluster>-<group>, the stem of every child object name.
func (g Group) Prefix() string { return g.Cluster + "-" + g.Name() }

// MemberName returns the pod (and PVC) name of member ordinal i.
func (g Group) MemberName(i int) string { return fmt.Sprintf("%s-%d", g.Prefix(), i) }

// ServiceRW names the Service that selects the primary.
func (g Group) ServiceRW() string { return g.Prefix() + "-rw" }

// ServiceRO names the Service that selects the replicas.
func (g Group) ServiceRO() string { return g.Prefix() + "-ro" }

// ServiceHeadless names the peer-discovery Service.
func (g Group) ServiceHeadless() string { return g.Prefix() + "-peers" }

// ConfigMapName names the group's configuration ConfigMap.
func (g Group) ConfigMapName() string { return g.Prefix() + "-config" }

// PDBPrimary names the primary's PodDisruptionBudget.
func (g Group) PDBPrimary() string { return g.Prefix() + "-primary" }

// PDBReplicas names the replicas' PodDisruptionBudget.
func (g Group) PDBReplicas() string { return g.Prefix() + "-replicas" }

// Labels returns the selector labels shared by the group's objects.
func (g Group) Labels() map[string]string {
	return map[string]string{
		LabelCluster: g.Cluster,
		LabelGroup:   g.Name(),
		LabelKind:    g.Kind,
	}
}

// Groups derives the catalog group and the shard groups from a cluster spec.
func Groups(c *pgshardv1alpha1.PgShardCluster) []Group {
	shards := 1
	if c.Spec.Shards != nil {
		shards = *c.Spec.Shards
	}
	catalogReplicas := c.Spec.Catalog.Replicas
	if catalogReplicas < 1 {
		catalogReplicas = 3
	}
	shardReplicas := c.Spec.ReplicasPerShard
	if shardReplicas < 1 {
		shardReplicas = 3
	}
	out := []Group{{Cluster: c.Name, Kind: "catalog", Replicas: catalogReplicas, Storage: c.Spec.Catalog.Storage}}
	for i := 0; i < shards; i++ {
		out = append(out, Group{Cluster: c.Name, Kind: "shard", ShardID: i, Replicas: shardReplicas, Storage: c.Spec.Storage})
	}
	return out
}

// SecretName is the Secret holding the superuser password.
func SecretName(cluster string) string { return cluster + "-superuser" }

// Image returns the PostgreSQL image for the cluster.
func Image(c *pgshardv1alpha1.PgShardCluster) string {
	if c.Spec.PostgreSQL.Image != "" {
		return c.Spec.PostgreSQL.Image
	}
	return fmt.Sprintf("%s:%d", DefaultImageRepository, c.Spec.PostgreSQL.Major)
}

// ReplicaMinAvailable is the replica PDB's minAvailable: replicas-2 when
// the group has at least three members, otherwise zero (no PDB is created).
func ReplicaMinAvailable(replicas int) int {
	if replicas >= 3 {
		return replicas - 2
	}
	return 0
}

// SyncStandbyNames renders synchronous_standby_names for a group: every
// standby, streaming ones first, each list in ordinal order. numSync is
// clamped to the number of standbys; zero means asynchronous replication.
// An empty string means "no synchronous standbys".
func SyncStandbyNames(g Group, numSync int, streaming map[string]bool) string {
	var healthy, other []string
	for i := 1; i < g.Replicas; i++ {
		name := g.MemberName(i)
		if streaming[name] {
			healthy = append(healthy, name)
		} else {
			other = append(other, name)
		}
	}
	all := make([]string, 0, len(healthy)+len(other))
	all = append(all, healthy...)
	all = append(all, other...)
	if len(all) == 0 || numSync < 1 {
		return ""
	}
	if numSync > len(all) {
		numSync = len(all)
	}
	quoted := make([]string, len(all))
	for i, n := range all {
		quoted[i] = `"` + n + `"`
	}
	return fmt.Sprintf("ANY %d (%s)", numSync, strings.Join(quoted, ", "))
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
