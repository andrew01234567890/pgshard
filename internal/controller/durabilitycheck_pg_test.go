package controller

import (
	"context"
	"log/slog"
	"strings"
	"testing"
)

// TestDurabilityCheckSeesWhatTheRouterCannot: the router refuses every
// route to synchronous_commit that goes through pgshard, so drift on a
// shard arrived by connecting to it directly -- and nothing looked. A
// commit acknowledged under synchronous_commit = off is lost on the
// failover the cluster promises to survive, and today the first sign of it
// would be the missing rows.
func TestDurabilityCheckSeesWhatTheRouterCannot(t *testing.T) {
	ctx := context.Background()
	f := newPlacementFixture(t)
	check := &DurabilityCheck{Pool: f.pool, Shards: f.placer.Shards, Logger: slog.New(slog.DiscardHandler)}

	drift, err := check.Pass(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(drift) != 0 {
		t.Fatalf("a cluster at its own defaults has not drifted: %v", drift)
	}

	// The value a future session would get, which the auditing connection
	// does not see for itself.
	mustExec(t, f.app(1), `ALTER ROLE postgres SET synchronous_commit = off`)
	t.Cleanup(func() { mustExec(t, f.app(1), `ALTER ROLE postgres RESET synchronous_commit`) })
	drift, err = check.Pass(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Both halves of the query fire: the override is recorded for a future
	// session, and the auditing connection is already one of those.
	joined := strings.Join(drift[1], "; ")
	if !strings.Contains(joined, "synchronous_commit is set to off for postgres") || !strings.Contains(joined, "synchronous_commit is off") {
		t.Fatalf("a per-role override must be reported: %v", drift)
	}
	if len(drift[0]) != 0 {
		t.Fatalf("the shard that did not drift must not be reported: %v", drift)
	}

	// And the value this very session reads.
	live := f.app(0)
	mustExec(t, live, `SET synchronous_commit = off`)
	if got, err := durabilityDrift(ctx, pgxShardConn{live}); err != nil {
		t.Fatal(err)
	} else if len(got) != 1 || !strings.Contains(got[0], "synchronous_commit is off") {
		t.Fatalf("a session value below the floor must be reported: %v", got)
	}
}
