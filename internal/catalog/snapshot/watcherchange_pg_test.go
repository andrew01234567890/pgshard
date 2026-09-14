package snapshot

import (
	"context"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/jackc/pgx/v5"
)

// TestAChangeIsPublishedForWhatTheGenerationsMiss: the buffering loops in
// the router (failover.go, fence.go) wait on a Change and then re-read the
// snapshot. What they are waiting FOR is usually a shard_status edit -- a
// new epoch, a serving state, a table-scoped migration pause -- and
// shard_status is a status table, so no desired_generation stamp covers it
// and neither generation moves. The change was not published, and the loop
// sat until its next poll tick instead of waking on the thing it exists to
// wake on.
//
// The snapshot already fingerprints everything it holds, so that is what
// decides now.
func TestAChangeIsPublishedForWhatTheGenerationsMiss(t *testing.T) {
	dsn := startPostgres(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_epoch)
		VALUES ('default', 0, 'g0', 'serving', 1)`); err != nil {
		t.Fatal(err)
	}

	w := NewWatcher(dsn, Options{Logf: func(string, ...any) {}})
	go func() { _ = w.Run(ctx) }()
	waitUntil(t, 20*time.Second, "the first snapshot", func() bool { return w.Current() != nil })

	changes, unsubscribe := w.Subscribe()
	defer unsubscribe()
	before := w.Current()

	// A promotion, as the agent records one. No desired-state row is
	// touched, so both generations stay exactly where they are.
	if _, err := conn.Exec(ctx, `UPDATE pgshard.shard_status SET primary_epoch = 2 WHERE shard_set = 'default' AND shard_id = 0`); err != nil {
		t.Fatal(err)
	}

	select {
	case c := <-changes:
		if c.ShardMapGeneration != before.ShardMapGeneration || c.DesiredGeneration != before.DesiredGeneration {
			t.Fatalf("the generations moved (%d/%d -> %d/%d), so this run did not test what it is named for",
				before.ShardMapGeneration, before.DesiredGeneration, c.ShardMapGeneration, c.DesiredGeneration)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("no change was published for a shard_status edit; every buffering loop waits for its poll tick instead")
	}

	if got := w.Current(); got.Serving[ShardKey{"default", 0}].Epoch != 2 {
		t.Fatalf("the snapshot did not pick up the new epoch: %+v", got.Serving[ShardKey{"default", 0}])
	}
}
