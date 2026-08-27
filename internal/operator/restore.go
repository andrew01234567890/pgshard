package operator

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/agent/backup"
)

// AnnotationRestoreSource on a PgShardCluster tells its primaries to
// bootstrap from another cluster's stanzas instead of initdb; the operator
// sets it when a PgShardRestore creates the cluster.
const AnnotationRestoreSource = "pgshard.io/restore-source"

// LabelRestoredFrom marks a cluster with the PgShardRestore that created it.
const LabelRestoredFrom = "pgshard.io/restored-from"

// RestoreSource is the annotation payload: where the groups restore from
// and the recovery target they all share.
type RestoreSource struct {
	SourceCluster string `json:"sourceCluster"`
	Major         int    `json:"major"`
	// Restore is the PgShardRestore that requested this.
	Restore string `json:"restore"`
	// BackupIDs are the pgBackRest labels per group name; a group without an
	// entry lets pgbackrest select the set for the target.
	BackupIDs map[string]string `json:"backupIds,omitempty"`
	// SourceGroups maps a new cluster's group name to the source group it
	// restores from. They differ whenever the source had resharded or been
	// upgraded: Group.Name() carries the generation, and a fresh cluster
	// has no status to carry one, so it would look for shard-0 where the
	// repository holds shard-0-g3.
	SourceGroups map[string]string `json:"sourceGroups,omitempty"`
	Type         backup.TargetType `json:"type,omitempty"`
	Target       string            `json:"target,omitempty"`
	TargetTLI    int64             `json:"targetTLI,omitempty"`
	Exclusive    bool              `json:"exclusive,omitempty"`
}

// SourceGroup is the source group a group of the new cluster restores
// from. It falls back to the group's own name, which is what an annotation
// written before source groups were recorded means, and what a source that
// never resharded says anyway.
func (s RestoreSource) SourceGroup(g Group) string {
	if name := s.SourceGroups[g.Name()]; name != "" {
		return name
	}
	return g.Name()
}

// Stanza is the source stanza a group of the new cluster restores from.
func (s RestoreSource) Stanza(g Group) string {
	return backup.StanzaName(s.SourceCluster, s.SourceGroup(g), s.Major)
}

// Options are the agent's restore options for one group. The backup labels
// are keyed by source group too: they were recorded against the source's
// own names.
func (s RestoreSource) Options(g Group) backup.RestoreOptions {
	return backup.RestoreOptions{
		Stanza: s.Stanza(g), BackupID: s.BackupIDs[s.SourceGroup(g)],
		Type: s.Type, Target: s.Target, TargetTLI: s.TargetTLI, Exclusive: s.Exclusive,
	}
}

// RestoreSourceOf decodes the restore annotation of a cluster.
func RestoreSourceOf(c *pgshardv1alpha1.PgShardCluster) (RestoreSource, bool) {
	raw := c.Annotations[AnnotationRestoreSource]
	if raw == "" {
		return RestoreSource{}, false
	}
	var src RestoreSource
	if err := json.Unmarshal([]byte(raw), &src); err != nil {
		return RestoreSource{}, false
	}
	return src, true
}

// Encode renders the annotation value.
func (s RestoreSource) Encode() string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// restoreTargetOptions translates the CRD target into pgbackrest terms;
// the stanza and backup id are filled per group later.
func restoreTargetOptions(spec *pgshardv1alpha1.PgShardRestoreSpec) (backup.RestoreOptions, error) {
	o := backup.RestoreOptions{Exclusive: spec.Exclusive}
	if spec.TargetTLI != nil {
		o.TargetTLI = *spec.TargetTLI
	}
	t := spec.Target
	set := 0
	if t.Time != nil {
		set++
		o.Type, o.Target = backup.TargetTime, t.Time.UTC().Format("2006-01-02 15:04:05+00")
	}
	if t.LSN != nil {
		set++
		o.Type, o.Target = backup.TargetLSN, *t.LSN
	}
	if t.Name != nil {
		set++
		o.Type, o.Target = backup.TargetName, *t.Name
	}
	if t.XID != nil {
		set++
		o.Type, o.Target = backup.TargetXID, *t.XID
	}
	if t.Immediate != nil && *t.Immediate {
		set++
		o.Type = backup.TargetImmediate
	}
	if t.Barrier != nil {
		set++
		if !barrierNameRE.MatchString(*t.Barrier) {
			return o, fmt.Errorf("target.barrier %q is not a barrier name", *t.Barrier)
		}
		o.Type, o.Target = backup.TargetName, BarrierRestorePoint(*t.Barrier)
	}
	if set > 1 {
		return o, fmt.Errorf("at most one restore target may be set")
	}
	if set == 0 {
		o.Type = backup.TargetDefault
	}
	if spec.BackupID != "" {
		o.BackupID = spec.BackupID
	}
	return o, o.Validate()
}

// restoreTimeout bounds a restore before it is reported failed.
const restoreTimeout = 4 * time.Hour

var barrierNameRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// BarrierRestorePoint is the WAL restore point name of a barrier, as the
// controller records it on every group.
func BarrierRestorePoint(barrier string) string { return "pgshard-" + barrier }

// isBarrierRestore reports whether the restore targets a certified barrier
// and so ends with two-phase reconciliation and unfencing.
func isBarrierRestore(rs *pgshardv1alpha1.PgShardRestore) bool { return rs.Spec.Target.Barrier != nil }
