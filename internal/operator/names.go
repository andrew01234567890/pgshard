package operator

import (
	"fmt"
	"strings"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/agent"
	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// Labels applied to every object the operator manages.
const (
	LabelCluster = "pgshard.io/cluster"
	LabelGroup   = "pgshard.io/group"
	LabelKind    = "pgshard.io/group-kind"
	LabelOrdinal = "pgshard.io/ordinal"
	LabelRole    = "pgshard.io/role"
	// LabelMember on a PVC names the member it belongs to; the claim name
	// itself gains a -v<n> suffix after a storage rebuild.
	LabelMember = "pgshard.io/member"

	// AnnotationScrape, AnnotationScrapePort and AnnotationScrapePath mark
	// a pod for Prometheus service discovery.
	AnnotationScrape     = "prometheus.io/scrape"
	AnnotationScrapePort = "prometheus.io/port"
	AnnotationScrapePath = "prometheus.io/path"

	RolePrimary = "primary"
	RoleReplica = "replica"
	// RoleUnhealthy marks a member the operator has taken out of every
	// Service: a fenced former primary that has not rejoined yet.
	RoleUnhealthy = "unhealthy"

	// AnnotationSwitchover on a PgShardCluster names the member to promote;
	// the operator removes it once the switchover ran.
	AnnotationSwitchover = "pgshard.io/switchover"
	// AnnotationRestart on a PgShardCluster requests a rolling restart of
	// every member; the value is remembered in status.rollout.lastRestartToken.
	AnnotationRestart = "pgshard.io/restart"
	// AnnotationTemplateHash on a member pod is the MemberTemplate hash the
	// member runs with.
	AnnotationTemplateHash = "pgshard.io/template-hash"
	// AnnotationPrimaryEpoch and AnnotationPrimary on the group Lease publish
	// the fence for readers that cannot reach the catalog.
	AnnotationPrimaryEpoch = "pgshard.io/primary-epoch"
	AnnotationPrimary      = "pgshard.io/primary"

	// ConditionCatalogReady is set once the catalog schema migrations ran on
	// the catalog primary.
	ConditionCatalogReady = "CatalogReady"

	// DefaultImageRepository hosts the pgshard PostgreSQL images; the tag is the major.
	DefaultImageRepository = "ghcr.io/andrew01234567890/pgshard-postgres"

	postgresPort  = 5432
	agentHTTPPort = 8080
	// routerHTTPPort serves the router's /metrics, /readyz and /healthz.
	routerHTTPPort = 8080
	agentGRPCPort  = 9090
	superuserName  = "postgres"
	secretKey      = "password"
	// LabelShardSet on a shard group's objects names the catalog shard set
	// it belongs to.
	LabelShardSet = "pgshard.io/shard-set"
)

// Group is one replication group derived from the cluster spec.
type Group struct {
	Cluster  string
	Kind     string
	ShardID  int
	Replicas int
	Storage  pgshardv1alpha1.StorageSpec
	// Generation is the shard set generation a shard group belongs to; the
	// first generation's groups are named shard-<id>, later ones
	// shard-<id>-g<generation> so generations never collide.
	Generation int64
	// NonServing marks a reshard target that routers must not see yet.
	NonServing bool
	// Retired marks a source of a switched reshard, kept up only for
	// reverse replication until the old groups are deleted.
	Retired bool
	// PGMajor is the PostgreSQL major the group's set runs; zero means the
	// cluster spec's major.
	PGMajor int
}

// Name is the group's short name, unique within the cluster.
func (g Group) Name() string {
	if g.Kind == "catalog" {
		return "catalog"
	}
	if g.Generation > 1 {
		return fmt.Sprintf("shard-%d-g%d", g.ShardID, g.Generation)
	}
	return fmt.Sprintf("shard-%d", g.ShardID)
}

// ShardSet is the catalog shard set the group's shard belongs to.
func (g Group) ShardSet() string { return catalog.ShardSetName(g.Generation) }

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

// SlotName is the physical replication slot member streams from, as the
// agent derives it.
func SlotName(member string) string { return (&agent.Config{Member: member}).SlotName() }

// LeaseName names the Lease the group primary holds; it matches the agent's
// <cluster>-<shard>-primary.
func (g Group) LeaseName() string { return g.Prefix() + "-primary" }

// MemberHost is the stable DNS name of a member through the headless Service.
func (g Group) MemberHost(member, namespace string) string {
	return fmt.Sprintf("%s.%s.%s.svc", member, g.ServiceHeadless(), namespace)
}

// MemberNames lists every member in ordinal order.
func (g Group) MemberNames() []string {
	out := make([]string, g.Replicas)
	for i := range out {
		out[i] = g.MemberName(i)
	}
	return out
}

// HasMember reports whether name is one of the group's members.
func (g Group) HasMember(name string) bool {
	for _, m := range g.MemberNames() {
		if m == name {
			return true
		}
	}
	return false
}

// Labels returns the selector labels shared by the group's objects.
func (g Group) Labels() map[string]string {
	l := map[string]string{
		LabelCluster: g.Cluster,
		LabelGroup:   g.Name(),
		LabelKind:    g.Kind,
	}
	if g.Kind == "shard" {
		l[LabelShardSet] = g.ShardSet()
	}
	return l
}

// ServingShards is the shard count of the serving shard set: what the
// catalog materialized, or spec.shards (default 1) before the catalog exists.
func ServingShards(c *pgshardv1alpha1.PgShardCluster) int {
	if c.Status.EffectiveShards > 0 {
		return c.Status.EffectiveShards
	}
	if c.Spec.Shards != nil {
		return *c.Spec.Shards
	}
	return 1
}

// TargetGroups derives the non-serving reshard target groups from
// status.reshard; nil when no reshard is in flight.
func TargetGroups(c *pgshardv1alpha1.PgShardCluster) []Group {
	rs := c.Status.Reshard
	if rs == nil || rs.Generation == ServingGeneration(c) {
		return nil
	}
	var out []Group
	for i := 0; i < rs.Shards; i++ {
		out = append(out, Group{Cluster: c.Name, Kind: "shard", ShardID: i, Replicas: shardReplicas(c), Storage: c.Spec.Storage,
			Generation: rs.Generation, NonServing: true, PGMajor: rs.PGMajor})
	}
	return out
}

func shardReplicas(c *pgshardv1alpha1.PgShardCluster) int {
	if c.Spec.ReplicasPerShard < 1 {
		return 3
	}
	return c.Spec.ReplicasPerShard
}

// RetiredGroups derives the old shard groups kept up for reverse
// replication after a write switch; nil outside that window.
func RetiredGroups(c *pgshardv1alpha1.PgShardCluster) []Group {
	rs := c.Status.Reshard
	if rs == nil || rs.RetiredShardSet == "" {
		return nil
	}
	var out []Group
	for i := 0; i < rs.RetiredShards; i++ {
		out = append(out, Group{Cluster: c.Name, Kind: "shard", ShardID: i, Replicas: shardReplicas(c), Storage: c.Spec.Storage,
			Generation: rs.RetiredGeneration, Retired: true, PGMajor: rs.RetiredPGMajor})
	}
	return out
}

// ServingGeneration is the generation of the serving shard set (1 before
// the first reshard completed).
func ServingGeneration(c *pgshardv1alpha1.PgShardCluster) int64 {
	if c.Status.ServingGeneration > 0 {
		return c.Status.ServingGeneration
	}
	return 1
}

// Groups derives the catalog group and the serving shard groups from a
// cluster spec and status.
func Groups(c *pgshardv1alpha1.PgShardCluster) []Group {
	shards := ServingShards(c)
	gen := ServingGeneration(c)
	catalogReplicas := c.Spec.Catalog.Replicas
	if catalogReplicas < 1 {
		catalogReplicas = 3
	}
	out := []Group{{Cluster: c.Name, Kind: "catalog", Replicas: catalogReplicas, Storage: c.Spec.Catalog.Storage, PGMajor: c.Status.ServingPGMajor}}
	for i := 0; i < shards; i++ {
		out = append(out, Group{Cluster: c.Name, Kind: "shard", ShardID: i, Replicas: shardReplicas(c), Storage: c.Spec.Storage, Generation: gen, PGMajor: c.Status.ServingPGMajor})
	}
	return out
}

// SecretName is the Secret holding the superuser password.
func SecretName(cluster string) string { return cluster + "-superuser" }

// MemberServiceAccount names the ServiceAccount (and Role, RoleBinding) the
// member agents run under.
func MemberServiceAccount(cluster string) string { return cluster + "-member" }

// Image returns the PostgreSQL image for the cluster.
func Image(c *pgshardv1alpha1.PgShardCluster) string {
	if c.Spec.PostgreSQL.Image != "" {
		return c.Spec.PostgreSQL.Image
	}
	return fmt.Sprintf("%s:%d", DefaultImageRepository, c.Spec.PostgreSQL.Major)
}

// MajorFor is the PostgreSQL major a group runs: its own stamp during an
// upgrade window, the spec's otherwise.
func MajorFor(c *pgshardv1alpha1.PgShardCluster, g Group) int {
	if g.PGMajor != 0 {
		return g.PGMajor
	}
	return c.Spec.PostgreSQL.Major
}

// ImageFor returns the PostgreSQL image for one group: a group stamped
// with a major other than the spec's (the old side of a blue/green major
// upgrade, or its targets before the spec flips) runs the default image of
// its own major, so a spec.postgresql change never restarts groups of the
// other major mid-upgrade.
func ImageFor(c *pgshardv1alpha1.PgShardCluster, g Group) string {
	if g.PGMajor != 0 && g.PGMajor != c.Spec.PostgreSQL.Major {
		return fmt.Sprintf("%s:%d", DefaultImageRepository, g.PGMajor)
	}
	return Image(c)
}

// ReplicaMinAvailable is the replica PDB's minAvailable: replicas-2 when
// the group has at least three members, otherwise zero (no PDB is created).
func ReplicaMinAvailable(replicas int) int {
	if replicas >= 3 {
		return replicas - 2
	}
	return 0
}

// SyncStandbyNames renders synchronous_standby_names for a group whose
// primary is primary: every other member, streaming ones first, each list in
// ordinal order. numSync is clamped to the number of standbys; zero means
// asynchronous replication. An empty string means "no synchronous standbys".
func SyncStandbyNames(g Group, primary string, numSync int, streaming map[string]bool) string {
	var healthy, other []string
	for _, name := range g.MemberNames() {
		switch {
		case name == primary:
		case streaming[name]:
			healthy = append(healthy, name)
		default:
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

// SynchronizedStandbySlots lists the physical slots of the streaming
// standbys that synchronous_standby_names can pick from, so failover slots
// never get ahead of a synchronous standby; empty when replication is
// asynchronous. Slots of members that are not streaming are left out: a
// listed slot that is inactive stalls every failover-slot walsender.
func SynchronizedStandbySlots(g Group, primary string, numSync int, streaming map[string]bool) []string {
	if numSync < 1 {
		return nil
	}
	var slots []string
	for _, name := range g.MemberNames() {
		if name != primary && streaming[name] {
			slots = append(slots, SlotName(name))
		}
	}
	return slots
}
