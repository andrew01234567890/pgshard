package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// unreachableSource fails every dial to one shard, the way a source whose
// pod is gone does, and passes the rest through.
type unreachableSource struct {
	ShardDBDialer
	set string
	id  int32
}

func (u unreachableSource) Dial(ctx context.Context, set string, id int32) (ShardConn, error) {
	if set == u.set && id == u.id {
		return nil, errors.New("dial: connection refused")
	}
	return u.ShardDBDialer.Dial(ctx, set, id)
}

func (u unreachableSource) DialDatabase(ctx context.Context, set string, id int32, db string) (ShardConn, error) {
	if set == u.set && id == u.id {
		return nil, errors.New("dial: connection refused")
	}
	return u.ShardDBDialer.DialDatabase(ctx, set, id, db)
}

// TestACancelledReshardLeavesTheQueueWhenASourceStaysUnreachable (PGS-906):
// a cancelled reshard stays in the operation queue until its cleanup has
// dropped every slot and publication, and with a source unreachable that
// cleanup failed on every pass. Everything behind it in the queue waited,
// and there was no way out: CancelWorkflow refuses a cancelled workflow and
// pgshard_admin cannot write pgshard.workflows.
func TestACancelledReshardLeavesTheQueueWhenASourceStaysUnreachable(t *testing.T) {
	parallelPG(t)
	f := newCopyFixture(t)
	ctx := context.Background()
	id := f.startWorkflow()

	slots := func(s int32) int64 {
		return queryOne[int64](t, connect(t, f.dsns[ShardRef{Set: "default", ID: s}]), `SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'pgshard_reshard_%'`)
	}
	deadline := time.Now().Add(3 * time.Minute)
	for slots(0) == 0 || slots(1) == 0 {
		f.pass()
		if time.Now().After(deadline) {
			_, stage, msg := f.workflow(id)
			t.Fatalf("the workflow never held slots on both sources: %s %q", stage, msg)
		}
		time.Sleep(300 * time.Millisecond)
	}

	now := time.Now()
	f.copier.Now = func() time.Time { return now }
	f.copier.Shards = unreachableSource{ShardDBDialer: f.dialer, set: "default", id: 1}
	if err := f.reconcileDrop(ctx); err != nil {
		t.Fatal(err)
	}
	queued := func() bool {
		return queryOne[int64](t, f.catalog, `SELECT count(*) FROM pgshard.operation_queue WHERE id = $1::uuid`, id) > 0
	}

	// Inside the window the cleanup is retried, not abandoned: a source
	// that is only restarting must still be cleaned properly.
	if out := f.pass(); out.Cancelled != 0 {
		t.Fatalf("the cancel finished at once with a source unreachable: %+v", out)
	}
	now = now.Add(f.copier.cancelGiveUp() - time.Second)
	if out := f.pass(); out.Cancelled != 0 {
		t.Fatalf("the cancel gave up before its window: %+v", out)
	}
	state, stage, msg := f.workflow(id)
	if stage != StageCancelling || !strings.Contains(msg, "cleanup failing since") {
		t.Fatalf("a failing cleanup reads %s/%s %q; the queue view shows this message, and it must say the cleanup is failing", state, stage, msg)
	}
	if !queued() {
		t.Fatal("the cancelling run is not in the operation queue, so this test is not holding anything")
	}

	now = now.Add(2 * time.Second)
	if out := f.pass(); out.Cancelled != 1 {
		_, stage, msg := f.workflow(id)
		t.Fatalf("the cancel did not finish past its window: %+v, %s %q", out, stage, msg)
	}
	if queued() {
		t.Fatal("the cancelled run is still in the operation queue after its cleanup gave up")
	}
	if _, stage, _ := f.workflow(id); stage != StageCancelled {
		t.Fatalf("stage %s after the give-up, want %s", stage, StageCancelled)
	}
	leaked := queryOne[string](t, f.catalog, `SELECT coalesce(status->'copy'->>'leaked', '') FROM pgshard.workflows WHERE id = $1::uuid`, id)
	if !strings.Contains(leaked, "default shard 1") || !strings.Contains(leaked, "replication slots") {
		t.Fatalf("the leaked record %q does not name the slots on the unreachable source", leaked)
	}
	if strings.Contains(leaked, "shard 0") {
		t.Fatalf("the leaked record %q names the reachable source, whose objects were droppable", leaked)
	}
	// The reachable source was still cleaned: giving up applies to what
	// cannot be reached, not to the whole cleanup.
	if n := slots(0); n != 0 {
		t.Fatalf("%d reshard slots left on the reachable source", n)
	}
	if n := queryOne[int64](t, connect(t, f.appDSN("default", 0)), `SELECT count(*) FROM pg_publication WHERE pubname LIKE 'pgshard_reshard_%'`); n != 0 {
		t.Fatalf("%d reshard publications left on the reachable source", n)
	}
	if n := slots(1); n == 0 {
		t.Fatal("the unreachable source holds no slots, so the leaked record describes nothing")
	}
}
