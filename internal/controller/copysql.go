package controller

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/andrew01234567890/pgshard/internal/placement"
)

// Names of the replication objects of one reshard are derived from the
// target generation and the shard ids so that two runs never collide and a
// crashed controller finds what it already created.

// PublicationName names the publication on a source shard that carries the
// sharded rows of one target shard.
func PublicationName(generation int64, target int32) string {
	return fmt.Sprintf("pgshard_reshard_g%d_t%d", generation, target)
}

// ReferencePublicationName names the publication on the home shard that
// carries the reference tables every target subscribes to.
func ReferencePublicationName(generation int64) string {
	return fmt.Sprintf("pgshard_reshard_g%d_ref", generation)
}

// HomePublicationName names the publication on the home shard that carries
// the unsharded tables only the home target subscribes to.
func HomePublicationName(generation int64) string {
	return fmt.Sprintf("pgshard_reshard_g%d_home", generation)
}

// SubscriptionName names the subscription (and its slot on the source) of
// one (target, source) pair in one database.
func SubscriptionName(generation int64, target, source int32) string {
	return fmt.Sprintf("pgshard_reshard_g%d_t%d_s%d", generation, target, source)
}

// KeyHashExpr renders the PostgreSQL expression that hashes column col of
// SQL type typ the way placement.KeyspaceID hashes the value the router
// sees: every integer type as int8, every character type as text, uuid
// over its raw bytes. Other types are refused because their PostgreSQL hash
// depends on the internal representation.
func KeyHashExpr(col, typ string) (string, error) {
	seed := fmt.Sprintf("%d::int8", int64(placement.PartitionSeed))
	base := strings.ToLower(typ)
	if i := strings.IndexByte(base, '('); i >= 0 {
		base = base[:i]
	}
	switch strings.TrimSpace(base) {
	case "bigint", "integer", "smallint", "int8", "int4", "int2":
		return fmt.Sprintf("hashint8extended(%s::int8, %s)", QuoteIdent(col), seed), nil
	case "text", "character varying", "varchar", "name":
		return fmt.Sprintf("hashtextextended(%s::text, %s)", QuoteIdent(col), seed), nil
	case "uuid":
		return fmt.Sprintf("uuid_hash_extended(%s, %s)", QuoteIdent(col), seed), nil
	case "character", "bpchar", "char":
		// Blank-padded character equality ignores trailing spaces and the
		// ::text cast strips them, so the row filter would hash a trimmed
		// value while the router hashes the client's raw bytes: two "equal"
		// keys could land on different shards. Refuse until the router can
		// normalise by column type.
		return "", fmt.Errorf("shard key %s of type %s is not supported: blank-padded character equality does not match byte-wise hashing; use text or varchar", col, typ)
	}
	return "", fmt.Errorf("shard key %s of type %s cannot be hashed by a row filter", col, typ)
}

// RangeFilter renders the row filter selecting the rows whose keyspace id
// lies in r; bounds at the ends of the key space are dropped.
func RangeFilter(hashExpr string, r placement.Range) string {
	var parts []string
	if r.Start != math.MinInt64 {
		parts = append(parts, fmt.Sprintf("%s >= %d", hashExpr, r.Start))
	}
	if r.End != math.MaxInt64 {
		parts = append(parts, fmt.Sprintf("%s <= %d", hashExpr, r.End))
	}
	if len(parts) == 0 {
		return "true"
	}
	return strings.Join(parts, " AND ")
}

// PublishedTable is one table of a publication with its optional row filter.
type PublishedTable struct {
	Schema      string
	Name        string
	Filter      string
	Partitioned bool
}

// QualifiedName renders the quoted schema.table name.
func (t PublishedTable) QualifiedName() string {
	return QuoteIdent(t.Schema) + "." + QuoteIdent(t.Name)
}

// CreatePublicationSQL renders CREATE PUBLICATION for tables; an empty list
// creates an empty publication. TRUNCATE is never published: the router
// refuses TRUNCATE while a reshard is active.
func CreatePublicationSQL(name string, tables []PublishedTable) string {
	var b strings.Builder
	b.WriteString("CREATE PUBLICATION " + QuoteIdent(name))
	if len(tables) > 0 {
		b.WriteString(" FOR TABLE ")
		for i, t := range tables {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(t.QualifiedName())
			if t.Filter != "" {
				b.WriteString(" WHERE (" + t.Filter + ")")
			}
		}
	}
	opts := "publish = 'insert, update, delete'"
	for _, t := range tables {
		if t.Partitioned {
			// A partitioned table's row filter is defined on the root, so
			// changes and the initial copy must replicate via the root for
			// the shard-key filter to apply; without this its rows are lost.
			opts += ", publish_via_partition_root = true"
			break
		}
	}
	b.WriteString(" WITH (" + opts + ")")
	return b.String()
}

// SubscriptionOptions are the CREATE SUBSCRIPTION options of a reshard copy.
type SubscriptionOptions struct {
	Slot string
	// Failover marks the slot for synchronisation to standbys (PG 17+).
	Failover bool
}

// CreateSubscriptionSQL renders CREATE SUBSCRIPTION for publications on a
// source reachable through conninfo. The initial copy streams in parallel,
// two-phase decoding stays off (the router's prepared transactions are
// decoded at commit) and rows of any origin are accepted.
func CreateSubscriptionSQL(name, conninfo string, publications []string, opts SubscriptionOptions) string {
	pubs := make([]string, 0, len(publications))
	for _, p := range publications {
		pubs = append(pubs, QuoteIdent(p))
	}
	with := []string{"copy_data = true", "create_slot = true", "enabled = true", "streaming = parallel", "two_phase = false", "origin = any",
		"slot_name = " + quoteLiteral(opts.Slot)}
	if opts.Failover {
		with = append(with, "failover = true")
	}
	return fmt.Sprintf("CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s WITH (%s)",
		QuoteIdent(name), quoteLiteral(conninfo), strings.Join(pubs, ", "), strings.Join(with, ", "))
}

// QuoteIdent quotes a SQL identifier.
func QuoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// RelState is one pg_subscription_rel state: i (init), d (data copy),
// f (finished copy), s (synchronized), r (ready).
type RelState byte

// LagUnknown marks a subscription whose apply worker reported no position
// yet (not started, or paused): it cannot be caught up.
const LagUnknown int64 = -1

// SubscriptionProgress is what one subscription reports.
type SubscriptionProgress struct {
	Rels map[RelState]int
	// LagBytes is the source WAL position minus the applied position, or
	// LagUnknown.
	LagBytes int64
	// Enabled is false while the subscription is paused by the throttle.
	Enabled bool
}

// CopyProgress aggregates every subscription of a workflow.
type CopyProgress struct {
	Subscriptions int   `json:"subscriptions"`
	TablesTotal   int   `json:"tables_total"`
	TablesReady   int   `json:"tables_ready"`
	LagBytes      int64 `json:"lag_bytes"`
	Paused        int   `json:"paused"`
}

// Aggregate folds subscription reports into one progress record; lag is
// the maximum over subscriptions and unknown as soon as one is unknown.
func Aggregate(reports []SubscriptionProgress) CopyProgress {
	var p CopyProgress
	for _, r := range reports {
		p.Subscriptions++
		for st, n := range r.Rels {
			p.TablesTotal += n
			if st == 'r' {
				p.TablesReady += n
			}
		}
		switch {
		case p.LagBytes == LagUnknown:
		case r.LagBytes == LagUnknown:
			p.LagBytes = LagUnknown
		case r.LagBytes > p.LagBytes:
			p.LagBytes = r.LagBytes
		}
		if !r.Enabled {
			p.Paused++
		}
	}
	return p
}

// CaughtUp is true once every table is ready and the lag is under threshold.
func (p CopyProgress) CaughtUp(threshold int64) bool {
	return p.Subscriptions > 0 && p.TablesTotal == p.TablesReady && p.LagBytes >= 0 && p.LagBytes < threshold
}

// Throttle decides whether the subscriptions should be paused given the
// source's standby lag: pause above high, resume below low, otherwise keep
// the current state (hysteresis).
func Throttle(paused bool, lag, high, low int64) bool {
	switch {
	case lag >= high:
		return true
	case lag <= low:
		return false
	}
	return paused
}

// HomeTarget is the index of the target range containing keyspace id 0:
// the successor of the home shard for unsharded tables.
func HomeTarget(ranges placement.RangeSet) int {
	return ranges.Locate(0)
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
