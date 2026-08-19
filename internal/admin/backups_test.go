package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/controller"
)

const secretKeyMaterial = "AKIAEXAMPLEKEYMATERIAL"

func backupObjects() []client.Object {
	t0 := metav1.NewTime(time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC))
	t1 := metav1.NewTime(t0.Add(90 * time.Second))
	barrier := "b-2026-08-19"
	objs := populated()
	objs[0].(*pgshardv1alpha1.PgShardCluster).Spec.Backup.PolicyRef = "nightly"
	return append(objs,
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s3-creds", Namespace: "ns1"}, StringData: map[string]string{"key": secretKeyMaterial}},
		&pgshardv1alpha1.PgShardBackupPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "ns1"},
			Spec: pgshardv1alpha1.PgShardBackupPolicySpec{
				ObjectStore: pgshardv1alpha1.ObjectStoreSpec{Type: "s3", Bucket: "pgshard-backups", Prefix: "prod",
					Credentials: pgshardv1alpha1.SecretRefSpec{SecretRef: &corev1.LocalObjectReference{Name: "s3-creds"}}},
				Schedules:       pgshardv1alpha1.BackupSchedules{Full: "0 2 * * 0", Incremental: "0 * * * *"},
				Retention:       pgshardv1alpha1.BackupRetention{Full: 4},
				BarrierSchedule: "*/30 * * * *",
			},
			Status: pgshardv1alpha1.PgShardBackupPolicyStatus{Clusters: []pgshardv1alpha1.ClusterBackupStatus{{Name: "demo", Healthy: true}}},
		},
		&pgshardv1alpha1.PgShardBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "demo-full-1", Namespace: "ns1"},
			Spec:       pgshardv1alpha1.PgShardBackupSpec{ClusterName: "demo", Type: "full"},
			Status: pgshardv1alpha1.PgShardBackupStatus{Phase: "Completed", BackupID: "20260819-100000F", StartedAt: &t0, CompletedAt: &t1,
				Groups: []pgshardv1alpha1.GroupBackupStatus{{Group: "demo-shard-0", Stanza: "demo-shard-0", BackupID: "20260819-100000F", SizeBytes: 3 * 1024 * 1024}}},
		},
		&pgshardv1alpha1.PgShardBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "demo-incr-2", Namespace: "ns1"},
			Spec:       pgshardv1alpha1.PgShardBackupSpec{ClusterName: "demo", Type: "incremental"},
			Status:     pgshardv1alpha1.PgShardBackupStatus{Phase: "Failed", StartedAt: &t1, Error: "stanza demo-shard-0: archive-push timeout"},
		},
		&pgshardv1alpha1.PgShardRestore{
			ObjectMeta: metav1.ObjectMeta{Name: "demo-restore-1", Namespace: "ns1"},
			Spec:       pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "demo", NewClusterName: "demo-2", BackupID: "demo-full-1", Target: pgshardv1alpha1.RestoreTarget{Barrier: &barrier}},
			Status: pgshardv1alpha1.PgShardRestoreStatus{Phase: "Reconciling", StartedAt: &t1,
				Groups:         []pgshardv1alpha1.GroupRestoreStatus{{Group: "demo-2-shard-0", SourceStanza: "demo-shard-0", Timeline: 2, ReachedTarget: true}},
				Reconciliation: &pgshardv1alpha1.RestoreReconciliationStatus{Decisions: 5, Committed: 4, RolledBack: 1}},
		},
		&pgshardv1alpha1.PgShardRestore{
			ObjectMeta: metav1.ObjectMeta{Name: "demo-restore-0", Namespace: "ns1"},
			Spec:       pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "demo", NewClusterName: "demo-old", Target: pgshardv1alpha1.RestoreTarget{Time: &t0}},
			Status: pgshardv1alpha1.PgShardRestoreStatus{Phase: "Failed", StartedAt: &t0, Error: "contradiction on demo-shard-0",
				Reconciliation: &pgshardv1alpha1.RestoreReconciliationStatus{Contradictions: []string{"demo-shard-0: gid-7"}}},
		},
	)
}

func restorePointSource() fakeCatalog {
	return fakeCatalog{points: []controller.RestorePoint{{
		Name: "b-2026-08-19", ShardMapGeneration: 7, Certified: true, CreatedAt: time.Date(2026, 8, 19, 9, 30, 0, 0, time.UTC),
		Groups: []controller.GroupRestorePoint{{Group: "demo-shard-0", LSN: 0x1_0000_00A8, Timeline: 1, WALSegment: "000000010000000100000000"}},
	}}}
}

func TestBackupsPageRendersPolicyBackupsRestoresAndPoints(t *testing.T) {
	s, _ := newTestServer(t, restorePointSource(), backupObjects()...)
	rec := get(t, s, "/backups")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"bucket pgshard-backups", "prefix prod", "secret s3-creds", "full 0 2 * * 0", "incremental 0 * * * *", "*/30 * * * *",
		`href="/backups/ns1/demo-full-1"`, "20260819-100000F", "3.0 MiB", "1m30s", "archive-push timeout",
		`href="/restores/ns1/demo-restore-1"`, "barrier b-2026-08-19", "Reconciling", "tli 2", "fenced · 5 decisions · 0 contradictions",
		`role="alert"`, "restore demo-restore-0 failed: contradiction on demo-shard-0", "1 contradictions",
		"1/A8", "<td>7</td>", "2026-08-19T09:30:00Z",
		`data-live hx-get="/backups/panel"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("page missing %q", want)
		}
	}
	if strings.Contains(body, secretKeyMaterial) {
		t.Fatal("secret key material leaked into the page")
	}
	if !strings.Contains(body, "time 2026-08-19T10:00:00Z") {
		t.Errorf("time target not described")
	}
	if !strings.Contains(get(t, s, "/backups/panel").Body.String(), "Certified restore points") {
		t.Error("panel fragment missing")
	}
}

func TestBackupsPageWithoutCatalogOrPolicy(t *testing.T) {
	s, _ := newTestServer(t, nil, populated()...)
	body := get(t, s, "/backups").Body.String()
	for _, want := range []string{"no backup policy bound", "No backups.", "No restores.", "No certified restore points"} {
		if !strings.Contains(body, want) {
			t.Errorf("page missing %q", want)
		}
	}
	s, _ = newTestServer(t, fakeCatalog{rpErr: context.DeadlineExceeded}, populated()...)
	if body := get(t, s, "/backups").Body.String(); !strings.Contains(body, "catalog: context deadline exceeded") {
		t.Error("catalog error not shown")
	}
}

func TestBackupsJSONNeverContainsKeyMaterial(t *testing.T) {
	s, _ := newTestServer(t, restorePointSource(), backupObjects()...)
	for _, path := range []string{"/api/v1/backups", "/api/v1/restores", "/api/v1/restore-points", "/backups"} {
		if body := get(t, s, path).Body.String(); strings.Contains(body, secretKeyMaterial) {
			t.Errorf("%s leaks key material", path)
		}
	}
	var backups []Backup
	if err := json.Unmarshal(get(t, s, "/api/v1/backups").Body.Bytes(), &backups); err != nil {
		t.Fatal(err)
	}
	if len(backups) != 2 || backups[0].Name != "demo-incr-2" || backups[1].Duration != "1m30s" || backups[1].Groups[0].SizeBytes != 3*1024*1024 {
		t.Errorf("backups: %+v", backups)
	}
	var restores []Restore
	if err := json.Unmarshal(get(t, s, "/api/v1/restores").Body.Bytes(), &restores); err != nil {
		t.Fatal(err)
	}
	if len(restores) != 2 || restores[0].TargetKind != "barrier" || restores[0].Target != "b-2026-08-19" || restores[0].Reconciliation.Decisions != 5 || restores[1].Phase != "Failed" {
		t.Errorf("restores: %+v", restores)
	}
	var points []RestorePoint
	if err := json.Unmarshal(get(t, s, "/api/v1/restore-points").Body.Bytes(), &points); err != nil {
		t.Fatal(err)
	}
	if len(points) != 1 || points[0].Groups[0].LSN != "1/A8" || points[0].ShardMapGeneration != 7 {
		t.Errorf("restore points: %+v", points)
	}
	s, _ = newTestServer(t, nil, populated()...)
	if body := strings.TrimSpace(get(t, s, "/api/v1/restore-points").Body.String()); body != "[]" {
		t.Errorf("restore points without catalog: %q", body)
	}
}

func TestBackupAndRestoreDetailPages(t *testing.T) {
	s, _ := newTestServer(t, nil, backupObjects()...)
	rec := get(t, s, "/backups/ns1/demo-full-1")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "<td>demo-shard-0</td>") || !strings.Contains(rec.Body.String(), "3.0 MiB") {
		t.Errorf("backup detail %d: %s", rec.Code, rec.Body)
	}
	rec = get(t, s, "/restores/ns1/demo-restore-0")
	body := rec.Body.String()
	if rec.Code != http.StatusOK || !strings.Contains(body, `role="alert"`) || !strings.Contains(body, "contradiction on demo-shard-0") || !strings.Contains(body, "<li>demo-shard-0: gid-7</li>") {
		t.Errorf("restore detail %d: %s", rec.Code, body)
	}
	rec = get(t, s, "/restores/ns1/demo-restore-1")
	if !strings.Contains(rec.Body.String(), "write-fenced") || strings.Contains(rec.Body.String(), `role="alert"`) {
		t.Errorf("reconciling restore: %s", rec.Body)
	}
	for _, path := range []string{"/backups/ns1/missing", "/restores/ns1/missing", "/backups/other/demo-full-1"} {
		if rec := get(t, s, path); rec.Code != http.StatusNotFound {
			t.Errorf("%s: status %d", path, rec.Code)
		}
	}
}

func TestOverviewCards(t *testing.T) {
	s, _ := newTestServer(t, nil, backupObjects()...)
	body := get(t, s, "/").Body.String()
	for _, want := range []string{"ns1/demo: ", " ago", `<div class="value bad">1</div>`, "Restores in progress</div><div class=\"value\">1</div>"} {
		if !strings.Contains(body, want) {
			t.Errorf("overview missing %q", want)
		}
	}
	cards, err := BuildBackupCards(context.Background(), s.Client, "", time.Date(2026, 8, 19, 12, 1, 30, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if cards.LastSuccess["ns1/demo"] != "2h0m" || cards.FailedBackups != 1 || cards.RestoresInProgress != 1 {
		t.Errorf("cards: %+v", cards)
	}
}

func TestHumanAge(t *testing.T) {
	for d, want := range map[time.Duration]string{30 * time.Second: "30s", 5 * time.Minute: "5m", 26 * time.Hour: "1d2h", 3*time.Hour + 4*time.Minute: "3h4m"} {
		if got := humanAge(d); got != want {
			t.Errorf("humanAge(%s) = %q, want %q", d, got, want)
		}
	}
}

func TestDescribeTarget(t *testing.T) {
	lsn, xid, name := "0/1A", "42", "rp"
	yes := true
	cases := map[string]pgshardv1alpha1.RestoreTarget{"lsn 0/1A": {LSN: &lsn}, "xid 42": {XID: &xid}, "name rp": {Name: &name}, "immediate ": {Immediate: &yes}, "latest ": {}}
	for want, tg := range cases {
		k, v := describeTarget(tg)
		if k+" "+v != want {
			t.Errorf("describeTarget = %q %q, want %q", k, v, want)
		}
	}
}
