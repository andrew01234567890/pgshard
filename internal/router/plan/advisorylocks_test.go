package plan

import (
	"context"
	"errors"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// TestASessionAdvisoryLockIsRefused: a session advisory lock outlives the
// statement but the backend holding it does not stay with the session, so
// the lock ends up on a backend another session then uses. Two clients
// each believe they hold it -- and advisory locks are what people use for
// leader election and migrations, so both then do the work. Saying no is
// the safe half of the answer; saying yes is the one that cannot be taken
// back.
func TestASessionAdvisoryLockIsRefused(t *testing.T) {
	for _, name := range []string{
		"pg_advisory_lock",
		"pg_advisory_lock_shared",
		"pg_try_advisory_lock",
		"pg_try_advisory_lock_shared",
		"pg_advisory_unlock",
		"pg_advisory_unlock_shared",
		"pg_advisory_unlock_all",
		// Not one of PostgreSQL's, but it is session-scoped by the same
		// rule, and a variant added later should be refused rather than
		// quietly let through.
		"pg_advisory_lock_someday",
	} {
		if !isSessionAdvisoryLock(name) {
			t.Errorf("%s() must be refused: its lock outlives the backend's stay with the session", name)
		}
	}
}

// TestATransactionAdvisoryLockIsAllowed: PostgreSQL releases these at the
// end of the transaction, and a transaction is already pinned to one
// backend, so they mean through the router what they mean on one node.
func TestATransactionAdvisoryLockIsAllowed(t *testing.T) {
	for _, name := range []string{
		"pg_advisory_xact_lock",
		"pg_advisory_xact_lock_shared",
		"pg_try_advisory_xact_lock",
		"pg_try_advisory_xact_lock_shared",
		// Neither an advisory lock nor anything to do with one. Note
		// pg_advisory_<anything> is deliberately absent: an unrecognised
		// one is refused, which is the conservative direction.
		"advisory_lock", "pg_locks", "pg_advisory", "count", "pg_sleep",
	} {
		if isSessionAdvisoryLock(name) {
			t.Errorf("%s() must not be refused", name)
		}
	}
}

// TestTheRouterRefusesASessionAdvisoryLock: the classification only helps
// if the planner acts on it, so this goes through Plan and reads what a
// client would be told.
func TestTheRouterRefusesASessionAdvisoryLock(t *testing.T) {
	snap := fixture(t)
	p := New()
	ctx := context.Background()
	for _, sql := range []string{
		"select pg_advisory_lock(1)",
		"select pg_try_advisory_lock(1, 2)",
		"select pg_advisory_unlock_all()",
		"select pg_catalog.pg_advisory_lock(1)",
		// Buried in a subquery, which is where a walk earns its keep.
		"select * from orders where id in (select 1 where pg_try_advisory_lock(7))",
	} {
		pl, err := p.Plan(ctx, session(snap), sql)
		if err == nil || pl.Kind != Refuse {
			t.Fatalf("%s: expected a refusal, got %+v %v", sql, pl, err)
		}
		var pe *pgwire.Error
		if !errors.As(err, &pe) || pe.Code != "0A000" {
			t.Fatalf("%s: %v, want a 0A000 refusal", sql, err)
		}
		if pe.Hint == "" {
			t.Fatalf("%s: a refusal has to say what to use instead", sql)
		}
	}
	// The transaction-scoped forms plan as any other statement does.
	for _, sql := range []string{
		"select pg_advisory_xact_lock(1)",
		"select pg_try_advisory_xact_lock_shared(1, 2)",
	} {
		if _, err := p.Plan(ctx, session(snap), sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
}
