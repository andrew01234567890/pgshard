//go:build integration

package router

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/controller"
)

// TestBarrierUnderTwoPhaseWorkload runs cross-shard transactions through
// the router while a barrier is taken: the barrier drains every prepared
// transaction, records a certified restore point for the catalog and both
// shards, and the workload only ever sees the write pause as latency.
func TestBarrierUnderTwoPhaseWorkload(t *testing.T) {
	archiving := []string{"-c archive_mode=on", "-c archive_command=/bin/true"}
	s := startShardedStackFull(t, archiving, append([]string{preparedXacts}, archiving...), append([]string{preparedXacts}, archiving...))
	ctx := context.Background()
	t0, t1 := twoTenants(t)
	conn := s.connect(t)
	s.awaitSharded(t, conn)
	_ = conn.Close(ctx)

	pool, err := pgxpool.New(ctx, s.catalogDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	res := s.resolver(t)
	res.PreparingTimeout = 5 * time.Second
	dialer := &controller.PgxShardDialer{Pool: pool, DSNs: map[controller.ShardRef]string{
		{Set: "default", ID: 0}: s.appDSN(0), {Set: "default", ID: 1}: s.appDSN(1)}}
	barrier := &controller.Barrier{Store: &controller.PGBarrierStore{Pool: pool}, Groups: &controller.SQLBarrierGroups{Pool: pool, Shards: dialer}, Resolver: res}

	var stop atomic.Bool
	var committed, paused atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			// Unnamed statements: cached named statements across two-phase
			// commits trip a router bug unrelated to barriers (42P05).
			c, err := pgx.Connect(ctx, s.dsn(appRole, appPassword, appDatabase)+"&default_query_exec_mode=exec")
			if err != nil {
				errs <- err
				return
			}
			defer func() { _ = c.Close(ctx) }()
			for i := 0; !stop.Load(); i++ {
				tx, err := c.Begin(ctx)
				if err != nil {
					errs <- err
					return
				}
				id := int64(w*1_000_000 + i)
				if _, err = tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1::int8, $2::int8)", t0, id); err == nil {
					_, err = tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1::int8, $2::int8)", t1, id)
				}
				if err == nil {
					err = tx.Commit(ctx)
				} else {
					_ = tx.Rollback(ctx)
				}
				var pgErr *pgconn.PgError
				switch {
				case err == nil:
					committed.Add(1)
				case errors.As(err, &pgErr) && pgErr.Code == "57P03":
					paused.Add(1)
				default:
					errs <- fmt.Errorf("worker %d: %w", w, err)
					return
				}
			}
		}(w)
	}
	// Let the workload settle so prepared transactions are in flight.
	deadline := time.Now().Add(20 * time.Second)
	for committed.Load() < 8 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if committed.Load() < 8 {
		stop.Store(true)
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Errorf("workload: %v", err)
		}
		t.Fatalf("workload made no progress: %d commits\n%s", committed.Load(), s.routerLog.String())
	}

	before := committed.Load()
	start := time.Now()
	rp, err := barrier.Run(ctx, "b1")
	took := time.Since(start)
	stop.Store(true)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("workload: %v", err)
	}
	if t.Failed() {
		t.Fatalf("router log:\n%s\npooler log:\n%s", s.routerLog.String(), s.poolerLog.String())
	}
	if err != nil {
		t.Fatalf("barrier: %v\n%s", err, s.routerLog.String())
	}
	if !rp.Certified || len(rp.Groups) != 3 || rp.ID == "" {
		t.Fatalf("restore point %+v", rp)
	}
	names := []string{}
	for _, g := range rp.Groups {
		names = append(names, g.Group)
		if g.LSN == 0 || g.WALSegment == "" || g.Timeline != 1 {
			t.Fatalf("group point %+v", g)
		}
	}
	if strings.Join(names, ",") != "catalog,shard0,shard1" {
		t.Fatalf("groups %v", names)
	}
	// The barrier must not take the buffering window. Its drain waits for
	// the transactions in flight, and those transactions wait for it if the
	// router holds their statements, which is a deadlock only the window
	// breaks -- and every client caught in it is refused. A healthy barrier
	// here is milliseconds.
	if took > 5*time.Second {
		t.Errorf("the barrier took %s; a drain that waits for the transactions waiting for it takes the whole buffering window", took)
	}
	// Refusals are allowed, and are not a bug: a transaction that had
	// already written and then reached a shard the barrier had just paused
	// cannot be rescued, because PostgreSQL read
	// default_transaction_read_only when that participant's transaction
	// began. What the client must never see is the shard's own 25006 --
	// the worker loop above fails the test on any code but pgshard's
	// retryable 57P03.
	if n := paused.Load(); n > 0 {
		t.Logf("%d transactions refused by the write pause, retryable", n)
	}
	after := committed.Load()
	if after < 8 {
		t.Fatalf("commits %d", after)
	}
	if after <= before {
		t.Errorf("no transaction committed after the barrier: %d then %d", before, after)
	}

	// The row and the state at the barrier: certified, all decisions
	// drained, nothing prepared on either shard.
	var name string
	var certified bool
	var perGroup string
	if err := pool.QueryRow(ctx, `SELECT name, certified, per_group::text FROM pgshard.restore_points`).Scan(&name, &certified, &perGroup); err != nil {
		t.Fatal(err)
	}
	if name != "b1" || !certified || !strings.Contains(perGroup, `"shard1"`) || !strings.Contains(perGroup, `"wal_segment"`) {
		t.Fatalf("row %s %v %s", name, certified, perGroup)
	}
	for shard := 0; shard < 2; shard++ {
		if got := s.preparedOn(t, shard); len(got) != 0 {
			t.Fatalf("shard %d prepared after the barrier: %v", shard, got)
		}
	}
	// Every restore point sits on both shards and the catalog, and nothing
	// that was in doubt at the fence remains: the decision log is empty
	// after the resolver settled and the workload stopped.
	if got := s.decisions(t); len(got) != 0 {
		t.Fatalf("decision rows: %v", got)
	}
	var fenced bool
	if err := pool.QueryRow(ctx, `SELECT write_fence FROM pgshard.shard_map_generation`).Scan(&fenced); err != nil || fenced {
		t.Fatalf("fence after the barrier: %v %v", fenced, err)
	}
	// Both shards hold the same set of ids: no transaction committed on one
	// shard only across the barrier.
	ids := func(shard int) []int64 {
		c, err := pgx.Connect(ctx, s.appDSN(shard))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close(ctx) }()
		rows, err := c.Query(ctx, "select id from orders order by id")
		if err != nil {
			t.Fatal(err)
		}
		out, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	if a, b := ids(0), ids(1); fmt.Sprint(a) != fmt.Sprint(b) || int64(len(a)) != after {
		t.Fatalf("shard 0 has %d ids, shard 1 %d, committed %d", len(a), len(b), after)
	}
	// Writes are refused, not lost, while the fence is up: raise it by
	// hand and watch a write wait, then pass once released.
	if _, err := pool.Exec(ctx, `UPDATE pgshard.shard_map_generation SET write_fence = true, write_fence_reason = 'test', write_fenced_at = now()`); err != nil {
		t.Fatal(err)
	}
	// The router learns of the fence through the catalog notification.
	time.Sleep(time.Second)
	c := s.connect(t)
	done := make(chan error, 1)
	go func() {
		_, err := c.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 999999999)", t0)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("write during the fence returned early: %v", err)
	case <-time.After(1500 * time.Millisecond):
	}
	if _, err := pool.Exec(ctx, `UPDATE pgshard.shard_map_generation SET write_fence = false, write_fence_reason = '', write_fenced_at = NULL`); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("held write: %v", err)
	}
}
