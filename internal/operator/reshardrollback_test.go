package operator

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

// TestRollbackCanBeAskedForOnAnyMode: the controller's rollback names no kind
// -- it triggers on spec.Rollback at StageSwitched and reverses whatever the
// run switched. Only the operator's mirroring was gated on upgrade mode, so an
// ordinary reshard had the machinery and no way to ask for it, leaving hand
// edits of pgshard.workflows as the only route during an incident.
func TestRollbackCanBeAskedForOnAnyMode(t *testing.T) {
	rec := func(mode string, ann map[string]string) *pgshardv1alpha1.PgShardReshard {
		return &pgshardv1alpha1.PgShardReshard{
			ObjectMeta: metav1.ObjectMeta{Annotations: ann},
			Spec:       pgshardv1alpha1.PgShardReshardSpec{Mode: mode},
		}
	}
	rollback := map[string]string{pgshardv1alpha1.AnnotationRollback: pgshardv1alpha1.UpgradeActionRollback}
	legacy := map[string]string{pgshardv1alpha1.AnnotationUpgrade: pgshardv1alpha1.UpgradeActionRollback}

	for _, tc := range []struct {
		name string
		rec  *pgshardv1alpha1.PgShardReshard
		want bool
	}{
		{"reshard asks with the generic annotation", rec(pgshardv1alpha1.ReshardModeReshard, rollback), true},
		{"upgrade asks with the generic annotation", rec(pgshardv1alpha1.ReshardModeUpgrade, rollback), true},
		{"upgrade still asks the way it always did", rec(pgshardv1alpha1.ReshardModeUpgrade, legacy), true},
		// The old annotation named upgrades, and a reshard carrying it was
		// never honoured. Keeping that false: changing what an existing
		// annotation means on a mode it never covered would roll back a run
		// somebody labelled, not one they asked to reverse.
		{"reshard carrying the upgrade annotation is not a request", rec(pgshardv1alpha1.ReshardModeReshard, legacy), false},
		{"no annotation is not a request", rec(pgshardv1alpha1.ReshardModeReshard, nil), false},
	} {
		if got := rollbackRequested(tc.rec); got != tc.want {
			t.Errorf("%s: rollbackRequested = %v, want %v", tc.name, got, tc.want)
		}
	}
}
