package operator

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/agent"
	"github.com/andrew01234567890/pgshard/internal/pgtune"
)

const (
	overrideConfKey = "pgshard.override.conf"
	// defaultMaxBackends is the pooler's server-connection budget until the
	// pooler track exposes it on the spec.
	defaultMaxBackends = 100
)

// logicalSlotsFor bounds the subscriptions one group can hold at once: a
// reshard target subscribes to every source, so the larger of the serving
// and desired shard counts is the worst case, plus headroom for VStream.
func logicalSlotsFor(c *pgshardv1alpha1.PgShardCluster) int {
	n := ServingShards(c)
	if c.Spec.Shards != nil && *c.Spec.Shards > n {
		n = *c.Spec.Shards
	}
	if n < 1 {
		n = 1
	}
	return n + 2
}

// Tuning derives the pgtune settings for one group from the pod resources
// (limits, else requests) and spec.postgresql. It returns nil settings when
// no memory is requested: without a budget nothing can be derived and the
// agent's fixed configuration stands alone.
func Tuning(c *pgshardv1alpha1.PgShardCluster, g Group) (pgtune.Settings, error) {
	mem := resourceOf(c.Spec.Resources, corev1.ResourceMemory)
	if mem.IsZero() {
		return nil, nil
	}
	cpu := resourceOf(c.Spec.Resources, corev1.ResourceCPU)
	in := pgtune.Input{
		Major:         c.Spec.PostgreSQL.Major,
		CPUMillicores: cpu.MilliValue(),
		MemoryBytes:   mem.Value(),
		DiskBytes:     g.Storage.Size.Value(),
		Profile:       pgtune.Profile(c.Spec.PostgreSQL.Profile),
		MaxBackends:   defaultMaxBackends,
		Replicas:      g.Replicas - 1,
		Overrides:     c.Spec.PostgreSQL.Parameters,
		// A reshard target subscribes to every source it takes range from,
		// so the worst case is the larger of the current and desired shard
		// counts -- a merge from four shards to two gives each target four
		// subscriptions. This was unset, so every group was sized as though
		// resharding never happened.
		LogicalSlots: logicalSlotsFor(c),
	}
	if in.CPUMillicores <= 0 {
		in.CPUMillicores = 1000
	}
	settings, err := pgtune.Derive(in)
	if err != nil {
		return nil, err
	}
	return dropAgentOwned(settings), nil
}

func resourceOf(r corev1.ResourceRequirements, name corev1.ResourceName) *resource.Quantity {
	if q, ok := r.Limits[name]; ok && !q.IsZero() {
		return &q
	}
	q := r.Requests[name]
	return &q
}

// dropAgentOwned removes the settings the agent fixes in postgresql.conf so
// the override never contradicts them (ssl, wal_level, slot counts, ...).
func dropAgentOwned(s pgtune.Settings) pgtune.Settings {
	owned := map[string]bool{}
	for _, name := range agent.OwnedSettings() {
		owned[name] = true
	}
	out := pgtune.Settings{}
	for _, x := range s {
		if !owned[x.Name] {
			out = append(out, x)
		}
	}
	return out
}

// OverrideConf renders the override file body; empty settings render empty.
func OverrideConf(s pgtune.Settings) string {
	if len(s) == 0 {
		return ""
	}
	return s.Render()
}

// changedSettings returns the GUC names whose value differs between two
// rendered configurations (user parameters plus the override).
func changedSettings(before, after map[string]string) []string {
	var names []string
	for k, v := range after {
		if before[k] != v {
			names = append(names, k)
		}
	}
	for k := range before {
		if _, ok := after[k]; !ok {
			names = append(names, k)
		}
	}
	return names
}

// effectiveSettings merges the user parameters and the override into one
// name → value map, override winning as it does in postgresql.conf.
func effectiveSettings(params map[string]string, override pgtune.Settings) map[string]string {
	out := map[string]string{}
	for k, v := range params {
		out[strings.TrimSpace(k)] = v
	}
	for _, x := range override {
		out[x.Name] = x.Value
	}
	return out
}
