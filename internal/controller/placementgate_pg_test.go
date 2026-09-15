package controller

import (
	"context"
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
// reshard, which waited for the placement, and neither ever moved.
func TestAPendingPlacementDoesNotWedgeAReshardWaitingForIt(t *testing.T) {
	parallelPG(t)
	g := newGateFixture(t)
	g.addReshard(g.catalog, StageReadyForCopy)
	ctx := context.Background()
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
	if pstate != StatePending {
		t.Fatalf("the placement started (%s, %q) while a reshard copies", pstate, pmsg)
	}
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
			t.Fatal("the copier decided without waiting for the move gate")
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
			t.Fatal("the placement decided without waiting for the move gate")
		}
		if state, _, msg := g.state(g.placement); state != StatePending {
			t.Fatalf("the placement is %s (%q) although a copy started while it decided", state, msg)
		}
	})
}
