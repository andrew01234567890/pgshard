//go:build integration

package router

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestRouterPreparedStatementsSurviveBackendReuse drives the router the way
// pgx's default statement cache does: named prepared statements that stay
// cached across shard hops and multi-shard transactions. A reused backend
// must never be asked to PREPARE a name it still holds (42P05).
func TestRouterPreparedStatementsSurviveBackendReuse(t *testing.T) {
	s := startShardedStackWith(t, []string{preparedXacts}, []string{preparedXacts})
	ctx := context.Background()
	t0, t1 := twoTenants(t)
	setup := s.connect(t)
	s.awaitSharded(t, setup)
	_ = setup.Close(ctx)

	const duration = 30 * time.Second

	t.Run("concurrent multi-shard 2PC workers", func(t *testing.T) {
		var wg sync.WaitGroup
		errs := make(chan error, 64)
		stop := time.Now().Add(duration)
		for w := range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn := s.connect(t)
				defer func() { _ = conn.Close(ctx) }()
				base := 1_000_000 * (w + 1)
				for i := 0; time.Now().Before(stop); i++ {
					id := base + i
					tx, err := conn.Begin(ctx)
					if err != nil {
						errs <- fmt.Errorf("worker %d begin: %w", w, err)
						return
					}
					for _, tenant := range []int64{t0, t1} {
						if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, $2)", tenant, id); err != nil {
							errs <- fmt.Errorf("worker %d insert %d: %w", w, id, err)
							_ = tx.Rollback(ctx)
							return
						}
					}
					var n int
					if err := tx.QueryRow(ctx, "select count(*) from orders where tenant_id = $1 and id = $2", t0, id).Scan(&n); err != nil {
						errs <- fmt.Errorf("worker %d select: %w", w, err)
						_ = tx.Rollback(ctx)
						return
					}
					if err := tx.Commit(ctx); err != nil {
						errs <- fmt.Errorf("worker %d commit %d: %w", w, id, err)
						return
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Errorf("%v", err)
		}
		if t.Failed() {
			t.Logf("router log:\n%s", s.routerLog.String())
		}
	})

	t.Run("one session hops shards with the same prepared statement", func(t *testing.T) {
		conn := s.connect(t)
		defer func() { _ = conn.Close(ctx) }()
		stop := time.Now().Add(duration)
		for i := 0; time.Now().Before(stop); i++ {
			tenant := t0
			if i%2 == 1 {
				tenant = t1
			}
			var n int
			if err := conn.QueryRow(ctx, "select count(*) from orders where tenant_id = $1", tenant).Scan(&n); err != nil {
				t.Fatalf("iteration %d: %v\n%s", i, err, s.routerLog.String())
			}
			if i%7 == 0 {
				if _, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, $2)", tenant, 9_000_000+i); err != nil {
					t.Fatalf("iteration %d insert: %v\n%s", i, err, s.routerLog.String())
				}
			}
		}
	})
}
