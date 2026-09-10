package controller

import (
	"context"
	"errors"
	"testing"
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
