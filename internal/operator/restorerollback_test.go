package operator

import (
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

// TestABarrierAfterARollbackToGenerationOneIsReadAtItsOwnAddress (PGS-973).
//
// The barrier guard reads the barrier from "the catalog group's own address"
// so that it asks the catalog the restore will recover (PGS-932). For
// generation 1 that address was the group's -rw Service -- which IS the
// stable endpoint. A rollback to generation 1 repoints the stable endpoint
// and clears the upgrade status in one pass, so the guard stops waiting at
// once, and for as long as the endpoint takes to catch up it can still reach
// generation 2, terminating, through that name. The upgrade gave generation 1
// a dedicated Service before its cutover and the rollback keeps it: that is
// the one to read.
func TestABarrierAfterARollbackToGenerationOneIsReadAtItsOwnAddress(t *testing.T) {
	barrier := "nightly-2026"
	one := 1
	run := func(t *testing.T, dedicated bool) string {
		t.Helper()
		source := boundCluster("old")
		source.Spec.Shards = &one
		source.Status.CatalogGeneration = 1
		rs := newRestore("r1", pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "old", NewClusterName: "new",
			BackupID: "b1", Target: pgshardv1alpha1.RestoreTarget{Barrier: &barrier}})
		objs := []client.Object{source, newPolicy(), completedBackup("b1", "old"), rs, superuserSecret("old")}
		if dedicated {
			objs = append(objs, Renderer{}.CatalogGenerationService(source, Groups(source)[0]))
		}
		cert := &fakeCertifier{certified: true, groups: []string{"catalog", "shard-0"}}
		r := &RestoreReconciler{Client: restoreClient(t, objs...), Agents: newFakeAgents(nil), Barriers: cert,
			Now: func() time.Time { return time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC) }}
		if _, got := reconcileRestore(t, r, "r1"); got.Status.Phase != pgshardv1alpha1.RestorePhaseRestoring {
			t.Fatalf("the restore did not start: phase %q, %s", got.Status.Phase, got.Status.Error)
		}
		return cert.dsn
	}

	if dsn := run(t, true); !strings.Contains(dsn, "host=old-catalog-g1-rw.") {
		t.Errorf("after a rollback to generation 1 the barrier was read at %q; want its dedicated old-catalog-g1-rw, not the stable endpoint a rollback has just moved", dsn)
	}
	// A catalog that was never upgraded has no dedicated Service, and its
	// stable endpoint has only ever selected generation 1.
	if dsn := run(t, false); !strings.Contains(dsn, "host=old-catalog-rw.") {
		t.Errorf("on a catalog never upgraded the barrier was read at %q; want old-catalog-rw", dsn)
	}
}

// TestAnUnreachableDedicatedCatalogAddressIsWaitedOut (PGS-973 review): the
// dedicated address can be newer than the stable one -- created at a cutover
// moments ago, endpoints not yet propagated. A failed restore cannot be
// resumed, so a failure through it is retried for a bounded time, and only
// then made final. The stable address keeps failing at once, as it did.
func TestAnUnreachableDedicatedCatalogAddressIsWaitedOut(t *testing.T) {
	barrier := "nightly-2026"
	one := 1
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	run := func(t *testing.T, dedicated bool, age time.Duration) *pgshardv1alpha1.PgShardRestore {
		t.Helper()
		source := boundCluster("old")
		source.Spec.Shards = &one
		rs := newRestore("r1", pgshardv1alpha1.PgShardRestoreSpec{ClusterName: "old", NewClusterName: "new",
			BackupID: "b1", Target: pgshardv1alpha1.RestoreTarget{Barrier: &barrier}})
		rs.CreationTimestamp = metav1.NewTime(now.Add(-age))
		objs := []client.Object{source, newPolicy(), completedBackup("b1", "old"), rs, superuserSecret("old")}
		if dedicated {
			objs = append(objs, Renderer{}.CatalogGenerationService(source, Groups(source)[0]))
		}
		r := &RestoreReconciler{Client: restoreClient(t, objs...), Agents: newFakeAgents(nil),
			Barriers: &fakeCertifier{err: errors.New("dial tcp: lookup old-catalog-g1-rw: no such host")},
			Now:      func() time.Time { return now }}
		_, got := reconcileRestore(t, r, "r1")
		return got
	}

	got := run(t, true, time.Minute)
	if got.Status.Phase == pgshardv1alpha1.RestorePhaseFailed {
		t.Fatalf("a restore a minute old failed on its first try through the dedicated address: %s", got.Status.Error)
	}
	if c := meta.FindStatusCondition(got.Status.Conditions, "Progressing"); c == nil || c.Reason != "WaitingForCatalogEndpoint" {
		t.Fatalf("the wait is not reported: %+v", got.Status.Conditions)
	}
	if got := run(t, true, 10*time.Minute); got.Status.Phase != pgshardv1alpha1.RestorePhaseFailed {
		t.Fatalf("past the grace the restore is %q, want it failed", got.Status.Phase)
	}
	if got := run(t, false, time.Minute); got.Status.Phase != pgshardv1alpha1.RestorePhaseFailed {
		t.Fatalf("through the stable address the restore is %q, want it failed at once as before", got.Status.Phase)
	}
}
