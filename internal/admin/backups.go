package admin

import (
	"context"
	"fmt"
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/controller"
)

// PolicySummary is a backup policy as the UI shows it: store location and
// schedules only, never key material. Credential and encryption Secrets
// appear by name.
type PolicySummary struct {
	Name             string `json:"name"`
	StoreType        string `json:"storeType"`
	Bucket           string `json:"bucket,omitempty"`
	Container        string `json:"container,omitempty"`
	Endpoint         string `json:"endpoint,omitempty"`
	Prefix           string `json:"prefix,omitempty"`
	CredentialType   string `json:"credentialType,omitempty"`
	CredentialSecret string `json:"credentialSecret,omitempty"`
	EncryptionSecret string `json:"encryptionSecret,omitempty"`
	FullSchedule     string `json:"fullSchedule,omitempty"`
	DiffSchedule     string `json:"differentialSchedule,omitempty"`
	IncrSchedule     string `json:"incrementalSchedule,omitempty"`
	RetainFull       int    `json:"retainFull,omitempty"`
	RetainDiff       int    `json:"retainDifferential,omitempty"`
	BarrierSchedule  string `json:"barrierSchedule,omitempty"`
	Healthy          string `json:"healthy"`
}

// GroupBackup is one group's result inside a backup.
type GroupBackup struct {
	Group         string `json:"group"`
	Stanza        string `json:"stanza"`
	BackupID      string `json:"backupId,omitempty"`
	SizeBytes     int64  `json:"sizeBytes,omitempty"`
	RepoSizeBytes int64  `json:"repoSizeBytes,omitempty"`
	Duration      string `json:"duration,omitempty"`
	Error         string `json:"error,omitempty"`
}

// Backup is one PgShardBackup as the UI shows it.
type Backup struct {
	Namespace   string        `json:"namespace"`
	Name        string        `json:"name"`
	Cluster     string        `json:"cluster"`
	Kind        string        `json:"kind"`
	Phase       string        `json:"phase"`
	BackupID    string        `json:"backupId,omitempty"`
	StartedAt   *time.Time    `json:"startedAt,omitempty"`
	CompletedAt *time.Time    `json:"completedAt,omitempty"`
	Duration    string        `json:"duration,omitempty"`
	Error       string        `json:"error,omitempty"`
	Groups      []GroupBackup `json:"groups"`
}

// RestorePointGroup is one group's position inside a certified restore point.
type RestorePointGroup struct {
	Group      string `json:"group"`
	LSN        string `json:"lsn"`
	Timeline   int64  `json:"timeline"`
	WALSegment string `json:"walSegment,omitempty"`
}

// RestorePoint is one certified barrier from pgshard.restore_points.
type RestorePoint struct {
	Name               string              `json:"name"`
	CreatedAt          time.Time           `json:"createdAt"`
	ShardMapGeneration int64               `json:"shardMapGeneration"`
	Groups             []RestorePointGroup `json:"groups"`
}

// GroupRestore is one group's progress inside a restore.
type GroupRestore struct {
	Group         string `json:"group"`
	SourceStanza  string `json:"sourceStanza"`
	BackupID      string `json:"backupId,omitempty"`
	Timeline      int64  `json:"timeline,omitempty"`
	ReachedTarget bool   `json:"reachedTarget"`
	Message       string `json:"message,omitempty"`
}

// Reconciliation summarizes the two-phase reconciliation of a barrier restore.
type Reconciliation struct {
	Decisions      int32    `json:"decisions"`
	Committed      int32    `json:"committed"`
	RolledBack     int32    `json:"rolledBack"`
	Contradictions []string `json:"contradictions,omitempty"`
	Unfenced       bool     `json:"unfenced"`
}

// Restore is one PgShardRestore as the UI shows it.
type Restore struct {
	Namespace      string          `json:"namespace"`
	Name           string          `json:"name"`
	Cluster        string          `json:"cluster"`
	NewCluster     string          `json:"newCluster"`
	TargetKind     string          `json:"targetKind"`
	Target         string          `json:"target,omitempty"`
	BackupID       string          `json:"backupId,omitempty"`
	Phase          string          `json:"phase"`
	StartedAt      *time.Time      `json:"startedAt,omitempty"`
	CompletedAt    *time.Time      `json:"completedAt,omitempty"`
	Error          string          `json:"error,omitempty"`
	Groups         []GroupRestore  `json:"groups"`
	Reconciliation *Reconciliation `json:"reconciliation,omitempty"`
}

// ClusterBackups is the backups panel of one cluster.
type ClusterBackups struct {
	Namespace string         `json:"namespace"`
	Name      string         `json:"name"`
	PolicyRef string         `json:"policyRef,omitempty"`
	Policy    *PolicySummary `json:"policy,omitempty"`
	Backups   []Backup       `json:"backups"`
	Restores  []Restore      `json:"restores"`
}

// BackupsPage is the /backups document.
type BackupsPage struct {
	Clusters          []ClusterBackups `json:"clusters"`
	RestorePoints     []RestorePoint   `json:"restorePoints"`
	RestorePointError string           `json:"restorePointError,omitempty"`
}

// BackupCards are the overview cards of the clusters list.
type BackupCards struct {
	LastSuccess        map[string]string `json:"lastSuccessfulBackupAge"`
	FailedBackups      int               `json:"failedBackups"`
	RestoresInProgress int               `json:"restoresInProgress"`
}

// RestorePointsLimit caps the restore points list.
const RestorePointsLimit = 50

// BuildBackupsPage assembles the backups panel for every cluster in namespace.
func BuildBackupsPage(ctx context.Context, c client.Reader, src CatalogSource, namespace string) (*BackupsPage, error) {
	var clusters pgshardv1alpha1.PgShardClusterList
	if err := c.List(ctx, &clusters, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	var policies pgshardv1alpha1.PgShardBackupPolicyList
	if err := c.List(ctx, &policies, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	backups, err := ListBackups(ctx, c, namespace)
	if err != nil {
		return nil, err
	}
	restores, err := ListRestores(ctx, c, namespace)
	if err != nil {
		return nil, err
	}
	page := &BackupsPage{Clusters: []ClusterBackups{}, RestorePoints: []RestorePoint{}}
	for i := range clusters.Items {
		pc := &clusters.Items[i]
		cb := ClusterBackups{Namespace: pc.Namespace, Name: pc.Name, PolicyRef: pc.Spec.Backup.PolicyRef, Backups: []Backup{}, Restores: []Restore{}}
		for j := range policies.Items {
			p := &policies.Items[j]
			if p.Namespace == pc.Namespace && p.Name == cb.PolicyRef {
				cb.Policy = summarizePolicy(p, pc.Name)
			}
		}
		for _, b := range backups {
			if b.Namespace == pc.Namespace && b.Cluster == pc.Name {
				cb.Backups = append(cb.Backups, b)
			}
		}
		for _, r := range restores {
			if r.Namespace == pc.Namespace && r.Cluster == pc.Name {
				cb.Restores = append(cb.Restores, r)
			}
		}
		page.Clusters = append(page.Clusters, cb)
	}
	sort.Slice(page.Clusters, func(i, j int) bool {
		a, b := page.Clusters[i], page.Clusters[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})
	if src != nil {
		points, err := src.RestorePoints(ctx)
		if err != nil {
			page.RestorePointError = err.Error()
		}
		for _, rp := range points {
			if len(page.RestorePoints) == RestorePointsLimit {
				break
			}
			page.RestorePoints = append(page.RestorePoints, convertRestorePoint(rp))
		}
	}
	return page, nil
}

// BuildBackupCards derives the overview cards from the backups and restores in namespace.
func BuildBackupCards(ctx context.Context, c client.Reader, namespace string, now time.Time) (*BackupCards, error) {
	backups, err := ListBackups(ctx, c, namespace)
	if err != nil {
		return nil, err
	}
	restores, err := ListRestores(ctx, c, namespace)
	if err != nil {
		return nil, err
	}
	cards := &BackupCards{LastSuccess: map[string]string{}}
	latest := map[string]time.Time{}
	for _, b := range backups {
		key := b.Namespace + "/" + b.Cluster
		switch b.Phase {
		case pgshardv1alpha1.BackupPhaseFailed:
			cards.FailedBackups++
		case pgshardv1alpha1.BackupPhaseCompleted:
			if b.CompletedAt != nil && b.CompletedAt.After(latest[key]) {
				latest[key] = *b.CompletedAt
			}
		}
	}
	for key, t := range latest {
		cards.LastSuccess[key] = humanAge(now.Sub(t))
	}
	for _, r := range restores {
		switch r.Phase {
		case pgshardv1alpha1.RestorePhasePending, pgshardv1alpha1.RestorePhaseRestoring, pgshardv1alpha1.RestorePhaseReconciling:
			cards.RestoresInProgress++
		}
	}
	return cards, nil
}

// ListBackups returns every PgShardBackup in namespace, newest first.
func ListBackups(ctx context.Context, c client.Reader, namespace string) ([]Backup, error) {
	var list pgshardv1alpha1.PgShardBackupList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	out := make([]Backup, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, convertBackup(&list.Items[i]))
	}
	sort.Slice(out, func(i, j int) bool { return newerFirst(out[i].StartedAt, out[i].Name, out[j].StartedAt, out[j].Name) })
	return out, nil
}

// ListRestores returns every PgShardRestore in namespace, newest first.
func ListRestores(ctx context.Context, c client.Reader, namespace string) ([]Restore, error) {
	var list pgshardv1alpha1.PgShardRestoreList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	out := make([]Restore, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, convertRestore(&list.Items[i]))
	}
	sort.Slice(out, func(i, j int) bool { return newerFirst(out[i].StartedAt, out[i].Name, out[j].StartedAt, out[j].Name) })
	return out, nil
}

// GetBackup reads one PgShardBackup.
func GetBackup(ctx context.Context, c client.Reader, namespace, name string) (*Backup, error) {
	var b pgshardv1alpha1.PgShardBackup
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &b); err != nil {
		return nil, err
	}
	out := convertBackup(&b)
	return &out, nil
}

// GetRestore reads one PgShardRestore.
func GetRestore(ctx context.Context, c client.Reader, namespace, name string) (*Restore, error) {
	var r pgshardv1alpha1.PgShardRestore
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &r); err != nil {
		return nil, err
	}
	out := convertRestore(&r)
	return &out, nil
}

func summarizePolicy(p *pgshardv1alpha1.PgShardBackupPolicy, cluster string) *PolicySummary {
	st := p.Spec.ObjectStore
	s := &PolicySummary{
		Name: p.Name, StoreType: st.Type, Bucket: st.Bucket, Container: st.Container, Endpoint: st.Endpoint, Prefix: st.Prefix,
		CredentialType:  st.CredentialType,
		FullSchedule:    p.Spec.Schedules.Full,
		DiffSchedule:    p.Spec.Schedules.Differential,
		IncrSchedule:    p.Spec.Schedules.Incremental,
		RetainFull:      p.Spec.Retention.Full,
		RetainDiff:      p.Spec.Retention.Differential,
		BarrierSchedule: p.Spec.BarrierSchedule,
		Healthy:         "Unknown",
	}
	if st.Credentials.SecretRef != nil {
		s.CredentialSecret = st.Credentials.SecretRef.Name
	}
	if st.Encryption.SecretRef != nil {
		s.EncryptionSecret = st.Encryption.SecretRef.Name
	}
	for _, cs := range p.Status.Clusters {
		if cs.Name == cluster {
			if cs.Healthy {
				s.Healthy = "True"
			} else {
				s.Healthy = "False"
			}
		}
	}
	return s
}

func convertBackup(b *pgshardv1alpha1.PgShardBackup) Backup {
	out := Backup{
		Namespace: b.Namespace, Name: b.Name, Cluster: b.Spec.ClusterName, Kind: b.Spec.Type, Phase: b.Status.Phase,
		BackupID: b.Status.BackupID, StartedAt: timePtr(b.Status.StartedAt), CompletedAt: timePtr(b.Status.CompletedAt),
		Error: b.Status.Error, Groups: []GroupBackup{},
	}
	if out.Kind == "" {
		out.Kind = "full"
	}
	if out.Phase == "" {
		out.Phase = pgshardv1alpha1.BackupPhasePending
	}
	if out.StartedAt != nil && out.CompletedAt != nil {
		out.Duration = out.CompletedAt.Sub(*out.StartedAt).Round(time.Second).String()
	}
	for _, g := range b.Status.Groups {
		out.Groups = append(out.Groups, GroupBackup{Group: g.Group, Stanza: g.Stanza, BackupID: g.BackupID, SizeBytes: g.SizeBytes, RepoSizeBytes: g.RepoSizeBytes, Duration: g.Duration, Error: g.Error})
	}
	return out
}

func convertRestore(r *pgshardv1alpha1.PgShardRestore) Restore {
	out := Restore{
		Namespace: r.Namespace, Name: r.Name, Cluster: r.Spec.ClusterName, NewCluster: r.Spec.NewClusterName, BackupID: r.Spec.BackupID,
		Phase: r.Status.Phase, StartedAt: timePtr(r.Status.StartedAt), CompletedAt: timePtr(r.Status.CompletedAt), Error: r.Status.Error,
		Groups: []GroupRestore{},
	}
	out.TargetKind, out.Target = describeTarget(r.Spec.Target)
	if out.Phase == "" {
		out.Phase = pgshardv1alpha1.RestorePhasePending
	}
	for _, g := range r.Status.Groups {
		out.Groups = append(out.Groups, GroupRestore{Group: g.Group, SourceStanza: g.SourceStanza, BackupID: g.BackupID, Timeline: g.Timeline, ReachedTarget: g.ReachedTarget, Message: g.Message})
	}
	if rc := r.Status.Reconciliation; rc != nil {
		out.Reconciliation = &Reconciliation{Decisions: rc.Decisions, Committed: rc.Committed, RolledBack: rc.RolledBack, Contradictions: rc.Contradictions, Unfenced: rc.Unfenced}
	}
	return out
}

func describeTarget(t pgshardv1alpha1.RestoreTarget) (kind, value string) {
	switch {
	case t.Barrier != nil:
		return "barrier", *t.Barrier
	case t.Time != nil:
		return "time", t.Time.UTC().Format(time.RFC3339)
	case t.LSN != nil:
		return "lsn", *t.LSN
	case t.Name != nil:
		return "name", *t.Name
	case t.XID != nil:
		return "xid", *t.XID
	case t.Immediate != nil && *t.Immediate:
		return "immediate", ""
	}
	return "latest", ""
}

func convertRestorePoint(rp controller.RestorePoint) RestorePoint {
	out := RestorePoint{Name: rp.Name, CreatedAt: rp.CreatedAt, ShardMapGeneration: rp.ShardMapGeneration, Groups: []RestorePointGroup{}}
	for _, g := range rp.Groups {
		out.Groups = append(out.Groups, RestorePointGroup{Group: g.Group, LSN: formatLSN(g.LSN), Timeline: g.Timeline, WALSegment: g.WALSegment})
	}
	return out
}

func formatLSN(lsn uint64) string { return fmt.Sprintf("%X/%X", lsn>>32, uint32(lsn)) }

func timePtr(t *metav1.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.Time
	return &u
}

func newerFirst(a *time.Time, an string, b *time.Time, bn string) bool {
	switch {
	case a == nil && b == nil:
		return an < bn
	case a == nil:
		return true
	case b == nil:
		return false
	case !a.Equal(*b):
		return a.After(*b)
	}
	return an < bn
}

func humanAge(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
	}
	return fmt.Sprintf("%dd%dh", int(d.Hours())/24, int(d.Hours())%24)
}
