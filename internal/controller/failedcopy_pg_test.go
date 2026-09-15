package controller

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

// TestAFailedCopyDropsItsReplicationObjects.
//
// A failed workflow is NEVER revisited: listCopyWorkflows selects pending,
// running and cancelling, and CancelWorkflow accepts only pending or paused.
// So a slot left on a serving SOURCE is left for good -- and once the targets
// are torn down it goes inactive and pins WAL on a primary until pg_wal fills
// the disk. A shard outage, caused by a failed copy nobody was watching.
//
// Placer.fail already reasons this way for the table-placement workflow. The
// copier never got it, so this asserts the copier does it too.
func TestAFailedCopyDropsItsReplicationObjects(t *testing.T) {
	parallelPG(t)
	f := newCopyFixture(t)
	ctx := context.Background()
	id := f.startWorkflow()

	// Drive until the copy has built its replication objects on the sources.
	for range 6 {
		f.pass()
		if f.sourceSlots() > 0 {
			break
		}
	}
	if f.sourceSlots() == 0 {
		t.Skip("the copy never created a slot on a source; nothing to leak")
	}

	wfs, err := f.copier.listCopyWorkflows(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var wf *copyWorkflow
	for i := range wfs {
		if wfs[i].id == id {
			wf = &wfs[i]
		}
	}
	if wf == nil {
		t.Fatalf("workflow %s is not listed; it cannot be failed from here", id)
	}
	if err := f.copier.fail(ctx, wf, errors.New("a fatal the copy cannot recover from")); err != nil {
		t.Fatal(err)
	}

	state, _, _ := f.workflow(id)
	if state != StateFailed {
		t.Fatalf("workflow state %s, want failed", state)
	}
	if n := f.sourceSlots(); n != 0 {
		t.Fatalf("%d replication slots left on the sources of a failed copy; nothing will ever revisit it and they pin WAL on a primary", n)
	}
}

// sourceSlots counts the logical slots this reshard created on every source.
func (f *copyFixture) sourceSlots() int {
	f.t.Helper()
	total := 0
	for ref, dsn := range f.dsns {
		if ref.Set != "default" {
			continue
		}
		conn := connect(f.t, dsn)
		var n int
		if err := conn.QueryRow(context.Background(),
			`SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'pgshard\_reshard\_%'`).Scan(&n); err != nil {
			f.t.Fatal(err)
		}
		_ = conn.Close(context.Background())
		total += n
	}
	return total
}

// TestAFailedSwitchDropsItsReverseReplication (PGS-846): a switch that fails
// after its reverse step has built replication in both directions. fail()
// dropped only the forward objects, and the reverse subscriptions left on
// the sources later read as another workflow's way back into the set, so the
// workflow that retires it left it writable for good.
func TestAFailedSwitchDropsItsReverseReplication(t *testing.T) {
	parallelPG(t)
	f := newUpgradeFixture(t)
	ctx := context.Background()
	id := f.startWorkflowKind(KindUpgrade)
	deadline := time.Now().Add(4 * time.Minute)
	var state, stage, msg string
	for {
		f.pass()
		state, stage, msg = f.workflow(id)
		if stage == StageSwitched || state == StateFailed || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if stage != StageSwitched {
		t.Fatalf("upgrade did not switch: %s %s %q", state, stage, msg)
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

	count := func(set, sql string) int64 {
		t.Helper()
		var n int64
		for ref := range f.dsns {
			if ref.Set == set {
				n += queryOne[int64](t, connect(t, f.appDSN(ref.Set, ref.ID)), sql)
			}
		}
		return n
	}
	reverseSubs := `SELECT count(*) FROM pg_subscription WHERE subname LIKE 'pgshard\_reshard\_g%\_rev\_%'`
	reversePubs := `SELECT count(*) FROM pg_publication WHERE pubname LIKE 'pgshard\_reshard\_g%\_rev\_%'`
	targetSlots := `SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'pgshard\_reshard\_%'`
	if count("default", reverseSubs) == 0 || count("g2", reversePubs) == 0 || count("g2", targetSlots) == 0 {
		t.Fatal("the switch left no reverse replication to drop, so this proves nothing")
	}

	// A pause standing on the sources -- a barrier's, say -- refuses the
	// drops unless they write through it.
	for ref, dsn := range f.dsns {
		if ref.Set != "default" {
			continue
		}
		admin := connect(t, dsn)
		mustExec(t, admin, `ALTER SYSTEM SET default_transaction_read_only = on`)
		mustExec(t, admin, `SELECT pg_reload_conf()`)
		waitFor(t, 10*time.Second, func() bool {
			return queryOne[string](t, connect(t, dsn), `SHOW default_transaction_read_only`) == "on"
		}, "the pause did not take effect")
		t.Cleanup(func() {
			_, _ = admin.Exec(context.Background(), `ALTER SYSTEM RESET default_transaction_read_only`)
			_, _ = admin.Exec(context.Background(), `SELECT pg_reload_conf()`)
		})
	}
	if err := f.copier.fail(ctx, wf, errors.New("a fatal the switch cannot recover from")); err != nil {
		t.Fatal(err)
	}
	if n := count("default", reverseSubs); n != 0 {
		t.Errorf("%d reverse subscriptions left on the sources", n)
	}
	if n := count("g2", reversePubs); n != 0 {
		t.Errorf("%d reverse publications left on the targets", n)
	}
	if n := count("g2", targetSlots); n != 0 {
		t.Errorf("%d reverse slots left on the targets", n)
	}
	if leaked := queryOne[string](t, f.catalog, `SELECT coalesce(status->>'leaked', '') FROM pgshard.workflows WHERE id = $1::uuid`, id); leaked != "" {
		t.Errorf("the failure recorded leaked objects: %s", leaked)
	}
}
