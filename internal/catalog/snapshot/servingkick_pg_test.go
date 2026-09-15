package snapshot

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/jackc/pgx/v5"
)

// TestAFlipIsSeenWhileDesiredStateChurns drives the whole Watcher, because
// the budget alone does not decide this.
//
// Separate buckets are not enough on their own: once a desired notification
// has been charged a wait, Run sits in that wait, and under sustained churn
// that is where it is most of the time -- which is exactly when a flip
// lands. A serving kick has to be able to cut the wait short. A unit test
// that calls notifyDelay directly cannot see any of that.
//
// Asserted as "well inside the refill period" rather than a tight bound:
// the point is that the flip does not queue behind the throttle, and the
// margin is wide (measured worst case 64ms with the wake, 905ms without).
func TestAFlipIsSeenWhileDesiredStateChurns(t *testing.T) {
	flipSeenDuringChurn(t, func(ctx context.Context, c *pgx.Conn) {
		_, _ = c.Exec(ctx, `SELECT pg_notify($1, 'churn')`, catalog.DesiredChannel)
	})
}

// TestAFlipIsSeenWhileMigrationsProgress (PGS-874): every per-shard step of
// a migration updates its pgshard.migrations row. Those notifications went
// out on the serving channel and spent the budget a flip needs.
func TestAFlipIsSeenWhileMigrationsProgress(t *testing.T) {
	flipSeenDuringChurn(t, func(ctx context.Context, c *pgx.Conn) {
		_, _ = c.Exec(ctx, `UPDATE pgshard.migrations SET updated_at = now()`)
	})
}

func flipSeenDuringChurn(t *testing.T, churnOnce func(context.Context, *pgx.Conn)) {
	t.Helper()
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
	if _, err := catalog.EnqueueMigration(ctx, conn, catalog.DDLMigration{Database: "app", Statement: "ALTER TABLE orders ADD COLUMN extra int",
		Kind: "ALTER TABLE", Strategy: "direct", Scope: "all"}); err != nil {
		t.Fatal(err)
	}
	w := NewWatcher(dsn, Options{Logf: func(string, ...any) {}})
	go func() { _ = w.Run(ctx) }()
	waitUntil(t, 20*time.Second, "the first snapshot", func() bool { return w.Current() != nil })

	churn, stopChurn := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c, err := pgx.Connect(churn, dsn)
		if err != nil {
			return
		}
		defer func() { _ = c.Close(context.WithoutCancel(churn)) }()
		// Spaced, not in a tight loop: requestReload coalesces into a
		// size-1 channel, so back-to-back notifications cost one token
		// between them and drain nothing.
		for churn.Err() == nil {
			churnOnce(churn, c)
			time.Sleep(120 * time.Millisecond)
		}
	}()
	defer func() { stopChurn(); wg.Wait() }()
	time.Sleep(2 * time.Second) // drain the desired burst into the throttle

	// Several flips at irregular offsets: where one lands inside the
	// watcher's current wait is the variable, so a single sample can pass
	// on luck alone.
	for i := range 4 {
		time.Sleep(time.Duration(300+197*i) * time.Millisecond)
		gen := w.Current().ShardMapGeneration + 10
		start := time.Now()
		if _, err := conn.Exec(ctx, `UPDATE pgshard.shard_map_generation SET generation = $1`, gen); err != nil {
			t.Fatal(err)
		}
		var saw time.Duration
		for saw == 0 && time.Since(start) < 20*time.Second {
			if s := w.Current(); s != nil && s.ShardMapGeneration >= gen {
				saw = time.Since(start)
			} else {
				time.Sleep(time.Millisecond)
			}
		}
		if saw == 0 {
			t.Fatalf("flip %d never observed", i)
		}
		if saw > notifyRefill/2 {
			t.Fatalf("flip %d took %s to be seen during churn; that wait is the window a straggling write to a retiring source lives in", i, saw)
		}
	}
}
