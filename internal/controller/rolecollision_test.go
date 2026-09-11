package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// collidingConn fails the first n Execs with the collision PostgreSQL
// raises when another backend changed the same catalog row first.
type collidingConn struct {
	fails int
	execs int
	last  string
}

func (c *collidingConn) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("unused")
}
func (c *collidingConn) Exec(_ context.Context, sql string, _ ...any) (CommandTag, error) {
	c.execs++
	c.last = sql
	if c.execs <= c.fails {
		return nil, &pgconn.PgError{Code: "XX000", Message: "tuple concurrently updated"}
	}
	return nil, nil
}
func (c *collidingConn) Close(context.Context) error { return nil }

// TestARoleCollisionIsRetriedForThatRoleAlone.
//
// Role and grant materialization is the one place pgshard writes an object
// a HUMAN is also entitled to write, so PostgreSQL's "tuple concurrently
// updated" happens whenever an administrator runs ALTER ROLE during a repair
// pass, or two controllers overlap during a leadership handover. XX000 is
// not a serialization failure the caller is told to retry, so nothing
// retried it and the whole pass failed -- then retried EVERY role for one
// collision.
//
// It also failed CI on two unrelated PRs in one day, which is how it was
// noticed.
func TestARoleCollisionIsRetriedForThatRoleAlone(t *testing.T) {
	ctx := context.Background()

	// A collision that clears is retried and succeeds.
	c := &collidingConn{fails: 2}
	if err := execRole(ctx, c, `ALTER ROLE "analyst" LOGIN`); err != nil {
		t.Fatalf("a collision that clears must be retried, not failed: %v", err)
	}
	if c.execs != 3 {
		t.Fatalf("took %d attempts, want 3 (two collisions then success)", c.execs)
	}

	// One that never clears is a real failure, not an endless retry.
	c = &collidingConn{fails: 99}
	if err := execRole(ctx, c, `ALTER ROLE "analyst" LOGIN`); err == nil {
		t.Fatal("a role that keeps losing must fail rather than retry for ever")
	}
	if c.execs != roleCollisionRetries {
		t.Fatalf("attempted %d times, want the bound of %d", c.execs, roleCollisionRetries)
	}

	// Any other error is returned at once: retrying a syntax error or a
	// permission failure would only delay it.
	c = &collidingConn{}
	other := &pgconn.PgError{Code: "42501", Message: "permission denied"}
	c2 := &failingConn{err: other}
	if err := execRole(ctx, c2, `ALTER ROLE "analyst" LOGIN`); !errors.Is(err, other) {
		t.Fatalf("an unrelated error must be returned unchanged: %v", err)
	}
	if c2.execs != 1 {
		t.Fatalf("an unrelated error was retried %d times", c2.execs)
	}
}

type failingConn struct {
	err   error
	execs int
}

func (c *failingConn) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("unused")
}
func (c *failingConn) Exec(context.Context, string, ...any) (CommandTag, error) {
	c.execs++
	return nil, c.err
}
func (c *failingConn) Close(context.Context) error { return nil }
