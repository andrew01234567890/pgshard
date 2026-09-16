package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/placement"
)

// gateFixture is a placement fixture with a pending move of one table and a
// reshard to a provisioned set g2 waiting at ready_for_copy.
type gateFixture struct {
	*placementFixture
	copier    *Copier
	placement string
	reshard   string
}

func newGateFixture(t *testing.T) *gateFixture {
	t.Helper()
	f := newPlacementFixture(t)
	mustExec(t, f.app(0), `CREATE TABLE items (id int PRIMARY KEY, v text)`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('app', 'public', 'items', 'unsharded', NULL)`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'sharded', shard_key = 'id' WHERE table_name = 'items'`)
	f.reconcile()
	g := &gateFixture{placementFixture: f, copier: &Copier{Pool: f.pool, Shards: f.placer.Shards}}
	g.placement, _, _, _ = f.workflow("items")

	ctx := context.Background()
	target, _ := placement.Split(2)
	tx, err := f.catalog.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.MaterializeShardSet(ctx, tx, "g2", 2, catalog.ShardSetProvisioning, target, 0); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return g
}

// addReshard records a reshard to the provisioning set g2 at stage, through
// q so a test can do it inside a transaction of its own.
func (g *gateFixture) addReshard(q interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}, stage string) {
	g.t.Helper()
	ctx := context.Background()
	t := g.t
	ranges, err := catalog.ListShardRanges(ctx, g.pool, "g2")
	if err != nil {
		t.Fatal(err)
	}
	g.reshard = "00000000-0000-0000-0000-00000000c0b1"
	if _, err := q.Exec(ctx, `INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES ($1, $2, $3, $4, $5)`,
		g.reshard, KindReshard, StateRunning, mustJSON(map[string]any{"shard_set": "g2", "generation": 2, "ranges": specRanges(ranges)}), mustJSON(map[string]any{"stage": stage})); err != nil {
		t.Fatal(err)
	}
}

// blockedOnTheGate runs pass until it returns or waits on the move gate's
// lock, and reports which.
func (g *gateFixture) blockedOnTheGate(pass func()) (blocked bool, finished <-chan struct{}) {
	g.t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		pass()
	}()
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		select {
		case <-done:
			return false, done
		default:
		}
		var waiting bool
		if err := g.pool.QueryRow(context.Background(), `SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted)`).Scan(&waiting); err != nil {
			g.t.Fatal(err)
		}
		if waiting {
			return true, done
		}
		time.Sleep(20 * time.Millisecond)
	}
	g.t.Fatal("the pass neither finished nor waited on the move gate")
	return false, done
}

func (g *gateFixture) state(id string) (state, stage, message string) {
	g.t.Helper()
	if err := g.catalog.QueryRow(context.Background(), `SELECT state, coalesce(status->>'stage', ''), coalesce(status->>'message', '') FROM pgshard.workflows WHERE id = $1::uuid`, id).Scan(&state, &stage, &message); err != nil {
		g.t.Fatal(err)
	}
	return state, stage, message
}

// TestAPendingPlacementDoesNotWedgeAReshardWaitingForIt (PGS-862): a
// placement's prepare waited for every active reshard, and a reshard at
// ready_for_copy waited for every active placement -- pending included. A
// placement declared while a reshard provisioned therefore waited for the
// reshard, which waited for the placement, and neither ever moved. In the
// operation queue whichever arrived first goes first, and a placement paused
// before it started keeps no place.
func TestAPendingPlacementDoesNotWedgeAReshardWaitingForIt(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()

	t.Run("the reshard arrived first", func(t *testing.T) {
		g := newGateFixture(t)
		g.addReshard(g.catalog, StageReadyForCopy)
		mustExec(t, g.catalog, `UPDATE pgshard.workflows SET arrival = 0 WHERE id = $1::uuid`, g.reshard)
		for range 3 {
			_, _ = g.copier.Pass(ctx)
			_, _ = g.placer.Pass(ctx)
			time.Sleep(50 * time.Millisecond)
		}
		_, rstage, rmsg := g.state(g.reshard)
		pstate, _, pmsg := g.state(g.placement)
		if rstage == StageReadyForCopy {
			t.Fatalf("the reshard is still at %s (%q) and the placement is %s (%q): they wait on each other", rstage, rmsg, pstate, pmsg)
		}
		if pstate == StateRunning {
			t.Fatalf("the placement started (%q) while a reshard copies", pmsg)
		}
	})

	t.Run("a placement paused before it started holds nothing", func(t *testing.T) {
		g := newGateFixture(t)
		mustExec(t, g.catalog, `UPDATE pgshard.workflows SET state = $2, status = status || '{"stage": "preparing", "paused_from": "pending"}' WHERE id = $1::uuid`, g.placement, StatePaused)
		g.addReshard(g.catalog, StageReadyForCopy)
		_, _ = g.copier.Pass(ctx)
		if _, rstage, rmsg := g.state(g.reshard); rstage == StageReadyForCopy {
			t.Fatalf("the reshard waits (%q) for a placement paused before it started", rmsg)
		}
	})
}

// TestACancellingPlacementHoldsBackAReshardsCopy (PGS-862): a cancelled
// placement is state cancelled while its cleanup still runs, and the copier
// counted only active states. A reshard could start copying, pause and flip
// the set under a cleanup that then could not load its routing and leaked
// its slots and replica identity.
func TestACancellingPlacementHoldsBackAReshardsCopy(t *testing.T) {
	parallelPG(t)
	g := newGateFixture(t)
	ctx := context.Background()
	g.driveUntil("items", time.Minute, StagePlacementCatchUp)
	mustExec(t, g.catalog, `UPDATE pgshard.workflows SET state = $2, status = status || $3::jsonb WHERE id = $1::uuid`,
		g.placement, StateCancelled, mustJSON(map[string]any{"stage": StageCancelling}))
	g.addReshard(g.catalog, StageReadyForCopy)

	_, _ = g.copier.Pass(ctx)
	if _, stage, msg := g.state(g.reshard); stage != StageReadyForCopy {
		t.Fatalf("the reshard reached %s (%q) while a cancelled placement was still cleaning up", stage, msg)
	}
	for i := 0; ; i++ {
		if _, stage, _ := g.state(g.placement); stage == StageCancelled {
			break
		}
		if i == 50 {
			t.Fatal("the cancel never finished")
		}
		if _, err := g.placer.Pass(ctx); err != nil {
			t.Fatal(err)
		}
	}
	_, _ = g.copier.Pass(ctx)
	if _, stage, msg := g.state(g.reshard); stage == StageReadyForCopy {
		t.Fatalf("the reshard still waits (%q) after the cancel finished", msg)
	}
}

// TestACopyAndAPlacementDecidingTogetherDoNotBothStart (PGS-862): each side
// counted the other and then changed its own state in separate statements,
// so a placement becoming running between a copier's count and its save let
// both proceed. The copier now decides under the lock the placement starts
// under.
func TestACopyAndAPlacementDecidingTogetherDoNotBothStart(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()

	t.Run("the copier waits for a placement starting under the gate", func(t *testing.T) {
		g := newGateFixture(t)
		g.addReshard(g.catalog, StageReadyForCopy)
		// Ahead of the placement in the queue, so that only the placement's
		// start under the gate, not its place, holds the copy back.
		mustExec(t, g.catalog, `UPDATE pgshard.workflows SET arrival = 0 WHERE id = $1::uuid`, g.reshard)
		tx, err := g.catalog.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if err := lockMoveGate(ctx, tx); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `UPDATE pgshard.workflows SET state = $2, status = status || $3::jsonb WHERE id = $1::uuid`,
			g.placement, StateRunning, mustJSON(map[string]any{"stage": StagePlacementShadow})); err != nil {
			t.Fatal(err)
		}
		blocked, done := g.blockedOnTheGate(func() { _, _ = g.copier.Pass(ctx) })
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		<-done
		if !blocked {
			_, stage, msg := g.state(g.reshard)
			t.Fatalf("the copier decided without waiting for the move gate: reshard at %s (%q)", stage, msg)
		}
		if _, stage, msg := g.state(g.reshard); stage != StageReadyForCopy {
			t.Fatalf("the reshard reached %s (%q) although a placement started while it decided", stage, msg)
		}
	})

	t.Run("a placement waits for a copy starting under the gate", func(t *testing.T) {
		g := newGateFixture(t)
		tx, err := g.catalog.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if err := lockMoveGate(ctx, tx); err != nil {
			t.Fatal(err)
		}
		g.addReshard(tx, StageCopying)
		blocked, done := g.blockedOnTheGate(func() { _, _ = g.placer.Pass(ctx) })
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		<-done
		if !blocked {
			state, _, msg := g.state(g.placement)
			t.Fatalf("the placement decided without waiting for the move gate: %s (%q)", state, msg)
		}
		if state, _, msg := g.state(g.placement); state != StatePending {
			t.Fatalf("the placement is %s (%q) although a copy started while it decided", state, msg)
		}
	})
}

// TestARangeEditNothingDrivesIsNotAnActiveCopy (PGS-866): an in-place edit
// of pgshard.shard_ranges records a reshard that nothing drives, and
// activeCopies counted it, so a placement waited at prepare until somebody
// cancelled the row.
//
// activeCopies is the gate only on a catalog that has not yet applied the
// operation queue migration, so this calls it directly: on a catalog that
// has, prepareAndStart asks the queue instead and never reaches it. What
// the queue answers for the same row is in the queue rule's own test.
//
// The row is made the way the controller makes it -- an actual range edit,
// an actual reconcile -- because what the count turns on is which keys that
// row's spec carries.
func TestARangeEditNothingDrivesIsNotAnActiveCopy(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()

	mustExec(t, f.catalog, `BEGIN`)
	mustExec(t, f.catalog, `UPDATE pgshard.shard_ranges SET range = int8range(lower(range), 1000) WHERE shard_set = 'default' AND shard_id = 0`)
	mustExec(t, f.catalog, `UPDATE pgshard.shard_ranges SET range = int8range(1000, upper(range)) WHERE shard_set = 'default' AND shard_id = 1`)
	mustExec(t, f.catalog, `COMMIT`)
	f.reconcile()
	if msg := queryOne[string](t, f.catalog, `SELECT coalesce(status->>'message', '') FROM pgshard.workflows WHERE kind = $1 AND state = $2`, KindReshard, StatePending); !strings.Contains(msg, "not driven yet") {
		t.Fatalf("the premise is the pending row an in-place range edit records: %q", msg)
	}
	if n, err := activeCopies(ctx, f.pool); err != nil || n != 0 {
		t.Fatalf("the pending range edit counts as %d active copies (%v)", n, err)
	}

	// Paused it is no less inert, and the row still carries no stage.
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET state = $2 WHERE kind = $1 AND state = $3`, KindReshard, StatePaused, StatePending)
	if n := queryOne[int64](t, f.catalog, `SELECT count(*) FROM pgshard.workflows WHERE kind = $1 AND state = $2`, KindReshard, StatePaused); n != 1 {
		t.Fatalf("%d reshards are paused; the pause did not land, so the assertion below would pass without exercising it", n)
	}
	if n, err := activeCopies(ctx, f.pool); err != nil || n != 0 {
		t.Fatalf("the paused range edit counts as %d active copies (%v)", n, err)
	}

	// The shape that must NOT be let past, and the reason the count reads the
	// stage rather than the state: a reshard whose spec names no source set
	// but which HAS started. Legacy rows resolved their source only at
	// cutover, so "no source set" does not mean "not running" for them, and a
	// copy past its first stage keeps applying through its subscriptions
	// whatever state the row is in -- paused included.
	const started = "00000000-0000-0000-0000-0000000000a2"
	mustExec(t, f.catalog, `INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES ($1, $2, $3, '{}', $4)`,
		started, KindReshard, StatePaused, mustJSON(map[string]any{"stage": "copying"}))
	if n, err := activeCopies(ctx, f.pool); err != nil || n != 1 {
		t.Fatalf("a sourceless reshard already copying counts as %d active copies, want 1 (%v)", n, err)
	}
	mustExec(t, f.catalog, `DELETE FROM pgshard.workflows WHERE id = $1::uuid`, started)

	// And the ordinary driven reshard, in the state the controller actually
	// creates it in: reshard.go inserts provisioning, never pending.
	const driven = "00000000-0000-0000-0000-0000000000a1"
	mustExec(t, f.catalog, `INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES ($1, $2, $3, $4, $5)`,
		driven, KindReshard, StateProvisioning,
		mustJSON(map[string]any{"shard_set": "g2", "generation": 2, "desired_generation": 2, "source_set": "default", "ranges": []any{}}),
		mustJSON(map[string]any{"stage": StageProvisioning}))
	if n, err := activeCopies(ctx, f.pool); err != nil || n != 1 {
		t.Fatalf("a driven reshard counts as %d active copies, want 1 (%v)", n, err)
	}
}
