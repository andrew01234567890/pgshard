package controller

import (
	"context"
	"strings"
	"testing"

	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestEnsurePriorityClassesInstallsAndValidates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	base := newFakeClient(t)
	reconciler := developmentReconciler(base, base)

	if err := reconciler.ensurePriorityClasses(ctx); err != nil {
		t.Fatalf("install PriorityClasses: %v", err)
	}
	for _, desired := range owned.PgShardPriorityClasses() {
		existing := &schedulingv1.PriorityClass{}
		if err := base.Get(ctx, client.ObjectKey{Name: desired.Name}, existing); err != nil {
			t.Fatalf("PriorityClass %s not installed: %v", desired.Name, err)
		}
		if existing.Value != desired.Value {
			t.Fatalf("PriorityClass %s value = %d, want %d", desired.Name, existing.Value, desired.Value)
		}
	}
	// Idempotent.
	if err := reconciler.ensurePriorityClasses(ctx); err != nil {
		t.Fatalf("second ensure: %v", err)
	}
}

func TestEnsurePriorityClassesFailsClosedOnConflict(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	desired := owned.PgShardPriorityClasses()[0]
	conflicting := desired.DeepCopy()
	conflicting.Value = desired.Value + 1 // an immutable-value conflict
	base := newFakeClient(t, conflicting)
	reconciler := developmentReconciler(base, base)

	if err := reconciler.ensurePriorityClasses(ctx); err == nil || !strings.Contains(err.Error(), "installation constant conflict") {
		t.Fatalf("conflicting PriorityClass error = %v", err)
	}
}
