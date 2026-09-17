package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// PGS-852. CancelWorkflow is a state change, which is all a workflow that
// has not started needs. One paused while running has built replication --
// slots holding WAL on the sources, and past its switch reverse
// subscriptions into a retired set -- and cancelling it as a state change
// left all of that behind with nothing left to drive it. Pausing a switch
// while it holds the range fence or is tearing down left the range refusing
// writes for as long as nobody resumed it.
func TestOnlyAWorkflowThatHasNotStartedIsCancelledAndAHoldingSwitchIsNotPaused(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	dsn := startPostgres(t)
	if err := catalog.Migrate(ctx, connect(t, dsn)); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	srv := &Server{Pool: pool}
	workflowOf := func(kind, state, stage string) string {
		t.Helper()
		var id string
		if err := pool.QueryRow(ctx, `INSERT INTO pgshard.workflows (id, kind, state, spec, status)
			VALUES (gen_random_uuid(), $1, $2, '{}', jsonb_build_object('stage', $3::text)) RETURNING id::text`, kind, state, stage).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	workflow := func(state, stage string) string { return workflowOf(KindReshard, state, stage) }
	stateOf := func(id string) string {
		t.Helper()
		var state string
		if err := pool.QueryRow(ctx, `SELECT state FROM pgshard.workflows WHERE id::text = $1`, id).Scan(&state); err != nil {
			t.Fatal(err)
		}
		return state
	}

	pending := workflow(StatePending, StageProvisioning)
	if _, err := srv.PauseWorkflow(ctx, &pgshardv1.PauseWorkflowRequest{Id: pending}); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.CancelWorkflow(ctx, &pgshardv1.CancelWorkflowRequest{Id: pending}); err != nil {
		t.Fatalf("a workflow paused before it started is cancelled: %v", err)
	}
	if got := stateOf(pending); got != StateCancelled {
		t.Fatalf("state %s, want cancelled", got)
	}

	for _, stage := range []string{StageCopying, StageSwitched} {
		running := workflow(StateRunning, stage)
		if _, err := srv.PauseWorkflow(ctx, &pgshardv1.PauseWorkflowRequest{Id: running}); err != nil {
			t.Fatalf("pausing a running workflow at %s: %v", stage, err)
		}
		_, err := srv.CancelWorkflow(ctx, &pgshardv1.CancelWorkflowRequest{Id: running})
		if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "paused while it was running") {
			t.Fatalf("cancelling a workflow paused at %s: %v, want it refused as one that has built replication", stage, err)
		}
		if got := stateOf(running); got != StatePaused {
			t.Fatalf("a refused cancel changed the state to %s", got)
		}
	}

	for _, c := range []struct{ kind, stage string }{
		{KindReshard, StageSwitching}, {KindUpgrade, StageRollingBack}, {KindReshard, StageCompleting},
		{KindTablePlacement, StagePlacementBuffering}, {KindTablePlacement, StagePlacementSwapping},
	} {
		holding := workflowOf(c.kind, StateRunning, c.stage)
		_, err := srv.PauseWorkflow(ctx, &pgshardv1.PauseWorkflowRequest{Id: holding})
		if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), c.stage) {
			t.Fatalf("pausing a %s at %s: %v, want it refused naming the stage", c.kind, c.stage, err)
		}
		if got := stateOf(holding); got != StateRunning {
			t.Fatalf("a refused pause changed the state to %s", got)
		}
	}
	// A stage that holds nothing for one kind is not refused for another
	// kind that happens to share the name.
	copying := workflowOf(KindTablePlacement, StateRunning, StagePlacementCopying)
	if _, err := srv.PauseWorkflow(ctx, &pgshardv1.PauseWorkflowRequest{Id: copying}); err != nil {
		t.Fatalf("pausing a placement while it copies: %v", err)
	}
}
