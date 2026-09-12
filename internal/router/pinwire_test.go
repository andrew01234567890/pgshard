package router

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func hintOf(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Hint
	}
	return ""
}

// admins satisfies CatalogAccessCheck: every listed role is an
// administrator, and an administrator may also open a catalog session.
type admins []string

func (a admins) MayUseCatalog(role string) bool { return a.MayAdminister(role) }
func (a admins) MayAdminister(role string) bool {
	for _, r := range a {
		if r == role {
			return true
		}
	}
	return false
}

// A tenant that can pin its own shard can read rows the shard map would
// have routed it away from, so the setting is an operator's and everyone
// else is refused it. Neki's documentation does not discuss this; it is the
// part we have to add.
func TestOnlyAnAdministratorMayPinAShard(t *testing.T) {
	h := newShardedHarness(t)
	conn := h.connect(t, h.dsn())
	_, err := conn.Exec(context.Background(), "set pgshard.shard = '1'")
	if err == nil || !strings.Contains(err.Error(), "permission denied to set pgshard.shard") {
		t.Fatalf("an ordinary session must not pin a shard, got %v", err)
	}
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || !strings.Contains(pe.Hint, "pgshard_admin") {
		t.Errorf("the refusal must name the role that would grant it; hint was %q", hintOf(err))
	}
}

// The value is checked before it is recorded, so a typo is an error rather
// than a session that silently keeps routing normally.
func TestAPinIsRefusedBeforeItIsRecorded(t *testing.T) {
	h := newShardedHarnessAdmin(t)
	conn := h.connect(t, h.dsn())
	ctx := context.Background()
	_, err := conn.Exec(ctx, "set pgshard.shard = 'shard-a'")
	if err == nil || !strings.Contains(err.Error(), "must be a shard id") {
		t.Fatalf("want the value refused, got %v", err)
	}
	// And the session still routes normally afterwards.
	var n int
	if err := conn.QueryRow(ctx, "select 1", pgx.QueryExecModeSimpleProtocol).Scan(&n); err != nil || n != 1 {
		t.Fatalf("session unusable after a refused SET: %d %v", n, err)
	}
}

// A transaction that began against one routing and continued against
// another would have statements from both in it, and the parts already sent
// cannot be taken back.
func TestAPinCannotChangeInsideATransaction(t *testing.T) {
	h := newShardedHarnessAdmin(t)
	conn := h.connect(t, h.dsn())
	ctx := context.Background()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, "set pgshard.shard = '1'")
	if err == nil || !strings.Contains(err.Error(), "cannot be changed inside a transaction") {
		t.Fatalf("want the in-transaction refusal, got %v", err)
	}
}

// An administrator pins a shard and reads from it, and the statement goes
// to that shard and no other.
func TestAPinnedSessionReachesOneShard(t *testing.T) {
	h := newShardedHarnessAdmin(t)
	conn := h.connect(t, h.dsn())
	ctx := context.Background()
	if _, err := conn.Exec(ctx, "set pgshard.shard = '2'"); err != nil {
		t.Fatalf("an administrator may pin: %v", err)
	}
	if _, err := conn.Exec(ctx, "select * from orders", pgx.QueryExecModeSimpleProtocol); err != nil {
		if !strings.Contains(err.Error(), "fake pooler does not understand") {
			t.Fatalf("pinned read: %v", err)
		}
	}
	for i := range h.poolers {
		ran := h.ranOn(i, "select * from orders")
		if i == 2 && !ran {
			t.Errorf("shard 2 was pinned but did not run the statement: %v", h.poolers[i].ran())
		}
		if i != 2 && ran {
			t.Errorf("shard %d ran a statement pinned to shard 2", i)
		}
	}
	// RESET restores ordinary routing: the scatter reaches every shard.
	if _, err := conn.Exec(ctx, "reset pgshard.shard"); err != nil {
		t.Fatal(err)
	}
	_, _ = conn.Exec(ctx, "select * from docs", pgx.QueryExecModeSimpleProtocol)
	for i := range h.poolers {
		if !h.ranOn(i, "select * from docs") {
			t.Errorf("after RESET shard %d was not reached: %v", i, h.poolers[i].ran())
		}
	}
}

// newShardedHarnessAdmin is the sharded harness whose login holds
// pgshard_admin, which is what pinning a shard needs.
func newShardedHarnessAdmin(t testing.TB) *shardedHarness {
	t.Helper()
	return newShardedHarnessWith(t, Config{CatalogAccess: admins{"app"}})
}
