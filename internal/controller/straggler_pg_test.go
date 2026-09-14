package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/placement"
)

// TestAWriteOpenBeforeTheFlipDoesNotOverwriteATargetWriteAfterIt (PGS-750):
// a transaction that began before the fence may still write -- the router
// exempts it. It used to be able to commit after the flip, with its row
// already writable on the target, and the forward subscription then carried
// its older write over the target's newer one; on one PostgreSQL the
// target's UPDATE would have waited for it. Quiesce now waits for it, with
// the sources refusing new writers meanwhile, before the journal.
func TestAWriteOpenBeforeTheFlipDoesNotOverwriteATargetWriteAfterIt(t *testing.T) {
	parallelPG(t)
	f := newUpgradeFixture(t)
	ctx := context.Background()
	const account = int64(750)
	kid, _ := placement.KeyspaceID(account)
	src := connect(t, f.appDSN("default", int32(f.srcRng.Locate(kid))))
	mustExec(t, src, `INSERT INTO accounts VALUES ($1, 0)`, account)

	id := f.startWorkflowKind(KindUpgrade)
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET spec = spec || '{"pause_before": "switchWrites"}' WHERE id = $1::uuid`, id)
	deadline := time.Now().Add(4 * time.Minute)
	for {
		f.pass()
		state, stage, msg := f.workflow(id)
		paused := queryOne[string](t, f.catalog, `SELECT coalesce(status->'cutover'->>'pause', '') FROM pgshard.workflows WHERE id = $1::uuid`, id)
		if paused == PauseSwitchWrites {
			break
		}
		if state == StateFailed || time.Now().After(deadline) {
			t.Fatalf("upgrade did not reach the switch: %s %s %q", state, stage, msg)
		}
		time.Sleep(500 * time.Millisecond)
	}

	wfs, err := f.copier.listCopyWorkflows(ctx)
	if err != nil {
		t.Fatal(err)
	}
	idx := slices.IndexFunc(wfs, func(w copyWorkflow) bool { return w.id == id })
	if idx < 0 {
		t.Fatalf("workflow %s not listed", id)
	}
	wf := &wfs[idx]
	var held bool
	if wf.owner, held, err = claimWorkflow(ctx, f.pool, f.copier.Replica, wf.id, f.copier.OwnerLease); err != nil || !held {
		t.Fatalf("claim: held=%v err=%v", held, err)
	}
	wf.fence = wf.state
	// A fresh set of operations for every step, as every pass builds its
	// own: nothing a step learns in memory survives to the next pass.
	step := func(name string, within time.Duration) error {
		until := time.Now().Add(within)
		for {
			ops, err := f.copier.pgCutover(ctx, wf)
			if err != nil {
				return err
			}
			waiting, err := f.copier.runStep(ctx, wf, ops, name)
			if err == nil && !waiting {
				return nil
			}
			if err != nil && !errors.Is(err, errRetry) {
				return fmt.Errorf("step %s: %w", name, err)
			}
			if time.Now().After(until) {
				return fmt.Errorf("step %s still waiting after %s: %w", name, within, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	run := func(name string, within time.Duration) {
		t.Helper()
		if err := step(name, within); err != nil {
			t.Fatal(err)
		}
	}

	tx, err := src.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var balance int64
	if err := tx.QueryRow(ctx, `SELECT balance FROM accounts WHERE id = $1`, account).Scan(&balance); err != nil || balance != 0 {
		t.Fatalf("balance %d: %v", balance, err)
	}

	run(StepFence, time.Minute)
	run(StepDrain, time.Minute)
	run(StepSweep, time.Minute)
	if _, err := tx.Exec(ctx, `UPDATE accounts SET balance = 1 WHERE id = $1`, account); err != nil {
		t.Fatalf("the transaction open before the fence could not write: %v", err)
	}
	for _, name := range []string{StepPositions, StepCatchUp, StepVerify, StepReverse} {
		run(name, time.Minute)
	}

	quiesced := make(chan error, 1)
	go func() { quiesced <- step(StepQuiesce, 3*time.Minute) }()
	time.Sleep(3 * time.Second)
	select {
	case err := <-quiesced:
		t.Fatalf("quiesce finished with a source transaction that had written still open: %v", err)
	default:
	}
	other := connect(t, f.appDSN("default", int32(f.srcRng.Locate(kid))))
	if _, err := other.Exec(ctx, `UPDATE accounts SET balance = 99 WHERE id = $1`, account+1); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("a transaction starting on a source while quiesce waits must be refused as read-only, got %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("the transaction open before the pause could not commit: %v", err)
	}
	if err := <-quiesced; err != nil {
		t.Fatal(err)
	}

	// A read begun under the pause is read-only, and nothing after quiesce
	// may wait for it: the drain's timeout is 30s, so a step that did would
	// take longer than these bounds.
	reader, err := connect(t, f.appDSN("default", int32(f.srcRng.Locate(kid)))).Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Rollback(ctx) }()
	if err := reader.QueryRow(ctx, `SELECT balance FROM accounts WHERE id = $1`, account).Scan(&balance); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{StepSequences, StepJournal, StepFlip} {
		run(name, 20*time.Second)
	}
	if queryOne[string](t, f.catalog, `SELECT state FROM pgshard.shard_sets WHERE shard_set = 'g2'`) != "serving" {
		t.Fatal("the flip finished without publishing the targets")
	}

	tgt := connect(t, f.appDSN("g2", int32(f.tgtRng.Locate(kid))))
	tag, err := tgt.Exec(ctx, `UPDATE accounts SET balance = 2 WHERE id = $1`, account)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("target write after the flip: %v (%d rows)", err, tag.RowsAffected())
	}
	run(StepSwap, 20*time.Second)
	if got := queryOne[int64](t, tgt, `SELECT balance FROM accounts WHERE id = $1`, account); got != 2 {
		t.Fatalf("the target's balance is %d after the swap, want the target's own later write", got)
	}
}
