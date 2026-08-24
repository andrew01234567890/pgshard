package admin

import (
	"net/http"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

func upgradingCluster() *pgshardv1alpha1.PgShardCluster {
	return &pgshardv1alpha1.PgShardCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "up", Namespace: "ns1"},
		Spec: pgshardv1alpha1.PgShardClusterSpec{
			PostgreSQL: pgshardv1alpha1.PostgreSQLSpec{Major: 19},
		},
		Status: pgshardv1alpha1.PgShardClusterStatus{
			ServingPGMajor: 18,
			CatalogPGMajor: 18,
			Reshard: &pgshardv1alpha1.ClusterReshardStatus{
				Name: "up-reshard-g2", ShardSet: "g2", Generation: 2, Shards: 1,
				PGMajor: 19, Phase: "Copying",
			},
			CatalogUpgrade: &pgshardv1alpha1.ClusterCatalogUpgradeStatus{
				FromMajor: 18, ToMajor: 19, Generation: 2, Stage: "catching_up",
				Message: "1024 bytes of WAL behind", RollbackRequested: true,
			},
		},
	}
}

func TestUpgradesPageShowsShardAndCatalogRows(t *testing.T) {
	record := &pgshardv1alpha1.PgShardReshard{
		ObjectMeta: metav1.ObjectMeta{Name: "up-reshard-g2", Namespace: "ns1",
			Annotations: map[string]string{pgshardv1alpha1.AnnotationUpgrade: pgshardv1alpha1.UpgradeActionRollback}},
	}
	s, _ := newTestServer(t, nil, upgradingCluster(), record)
	rec := get(t, s, "/upgrades")
	body := rec.Body.String()
	if rec.Code != http.StatusOK {
		t.Fatalf("upgrades page: %d %s", rec.Code, body)
	}
	for _, want := range []string{
		"shard set g2", "18 → 19", "Copying",
		"catalog group", "catching_up", "1024 bytes of WAL behind",
		"requested",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("upgrades page lacks %q", want)
		}
	}
	if n := strings.Count(body, "18 → 19"); n != 2 {
		t.Errorf("both the shard-set and catalog rows must show 18 → 19; saw %d", n)
	}
	if rec := get(t, s, "/upgrades/panel"); strings.Contains(rec.Body.String(), "<html") || !strings.Contains(rec.Body.String(), "catalog group") {
		t.Errorf("fragment: %d %s", rec.Code, rec.Body)
	}
	if rec := get(t, s, "/api/v1/upgrades"); !strings.Contains(rec.Body.String(), `"scope": "catalog group"`) || !strings.Contains(rec.Body.String(), `"rollback": "requested"`) {
		t.Errorf("upgrades API: %s", rec.Body)
	}
}

func TestUpgradesPageBlockedAndIdleStates(t *testing.T) {
	blocked := &pgshardv1alpha1.PgShardCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "blocked", Namespace: "ns1"},
		Spec:       pgshardv1alpha1.PgShardClusterSpec{PostgreSQL: pgshardv1alpha1.PostgreSQLSpec{Major: 19}},
		Status: pgshardv1alpha1.PgShardClusterStatus{
			ServingPGMajor: 18,
			Conditions: []metav1.Condition{{Type: pgshardv1alpha1.ConditionResharding, Status: metav1.ConditionTrue,
				Reason: "UpgradeBlocked", Message: "upgrade to major 19 blocked: backups are not healthy; 1 table placement workflow(s) in flight"}},
		},
	}
	done := &pgshardv1alpha1.PgShardCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "done", Namespace: "ns1"},
		Spec:       pgshardv1alpha1.PgShardClusterSpec{PostgreSQL: pgshardv1alpha1.PostgreSQLSpec{Major: 19}},
		Status:     pgshardv1alpha1.PgShardClusterStatus{ServingPGMajor: 19, CatalogPGMajor: 19},
	}
	s, _ := newTestServer(t, nil, blocked, done)
	body := get(t, s, "/upgrades").Body.String()
	for _, want := range []string{"blocked", "backups are not healthy", "1 table placement workflow(s) in flight", "on 19"} {
		if !strings.Contains(body, want) {
			t.Errorf("upgrades page lacks %q", want)
		}
	}
}
