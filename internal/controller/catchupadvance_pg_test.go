package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// TestAnEmptyPeekCannotAdvancePastACommitItDidNotSee.
//
// The catch-up used to advance an idle slot to pg_current_wal_lsn() read
// AFTER the peek. A transaction that committed between the two statements
// was then skipped: the peek saw nothing, and the slot moved past a commit
// nobody had decoded, so those rows never reached the shadow table.
//
// This runs the two shapes against real PostgreSQL and shows the difference
// directly: the bound read BEFORE the peek leaves the intervening commit for
// the next round, and the bound read after swallows it.
func TestAnEmptyPeekCannotAdvancePastACommitItDidNotSee(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	conn := connect(t, startPostgresWith(t, "-c wal_level=logical"))
	mustExec(t, conn, `CREATE TABLE t (id int primary key)`)
	mustExec(t, conn, `CREATE PUBLICATION p FOR ALL TABLES`)
	if _, err := conn.Exec(ctx, `SELECT pg_create_logical_replication_slot('s', 'pgoutput')`); err != nil {
		t.Fatal(err)
	}
	peek := func() int {
		t.Helper()
		var n int
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM pg_logical_slot_peek_binary_changes('s', NULL, NULL,
			'proto_version', '1', 'publication_names', 'p')`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// The bound this fix takes: read BEFORE the peek.
	var upto string
	if err := conn.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&upto); err != nil {
		t.Fatal(err)
	}
	if n := peek(); n != 0 {
		t.Fatalf("the premise is an EMPTY peek; got %d messages", n)
	}

	// A transaction commits in the window the peek has already passed.
	mustExec(t, conn, `INSERT INTO t VALUES (1)`)

	if _, err := conn.Exec(ctx, `SELECT pg_replication_slot_advance('s', $1::pg_lsn)`, upto); err != nil {
		t.Fatal(err)
	}
	if n := peek(); n == 0 {
		t.Fatal("the commit was skipped: advancing to a bound taken before the peek must leave it for the next round")
	}

	// The shape that was there before: the bound is read AFTER the peek, so
	// it is past the commit and the change is discarded. Asserting this is
	// what shows the fix is load-bearing rather than incidental.
	mustExec(t, conn, `SELECT count(*) FROM pg_logical_slot_get_binary_changes('s', NULL, NULL,
		'proto_version', '1', 'publication_names', 'p')`)
	_ = peek()
	mustExec(t, conn, `INSERT INTO t VALUES (2)`)
	if _, err := conn.Exec(ctx, `SELECT pg_replication_slot_advance('s', pg_current_wal_lsn())`); err != nil {
		t.Fatal(err)
	}
	if n := peek(); n != 0 {
		t.Fatalf("expected the old shape to swallow the commit, but %d messages survived; the two shapes no longer differ and this test proves nothing", n)
	}
}

// advanceConn serves an EMPTY peek and records what the advance was given,
// plus what the WAL end read before it. A different value each time the WAL
// end is asked for is the whole point: if the advance uses the LATER read,
// it is using a bound the peek never covered.
type advanceConn struct {
	walReads int
	handed   []string // the WAL ends handed out, in order
	advanced string   // the argument the advance was given
}

func (c *advanceConn) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	switch {
	case strings.Contains(sql, "peek_binary_changes"):
		return &peekRows{}, nil // empty peek
	case strings.Contains(sql, "SELECT pg_current_wal_lsn()::text"):
		c.walReads++
		v := "0/" + itoa(int64(c.walReads*100))
		c.handed = append(c.handed, v)
		return &textRows{v: v}, nil
	}
	return &lagRows{}, nil
}

func (c *advanceConn) Exec(_ context.Context, sql string, args ...any) (CommandTag, error) {
	if strings.Contains(sql, "pg_replication_slot_advance") && len(args) == 2 {
		c.advanced, _ = args[1].(string)
	}
	return nil, nil
}
func (c *advanceConn) Close(context.Context) error { return nil }

type textRows struct {
	v string
	i int
}

func (r *textRows) Close()                                       {}
func (r *textRows) Err() error                                   { return nil }
func (r *textRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *textRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *textRows) Next() bool                                   { r.i++; return r.i <= 1 }
func (r *textRows) Scan(dest ...any) error                       { *(dest[0].(*string)) = r.v; return nil }
func (r *textRows) Values() ([]any, error)                       { return []any{r.v}, nil }
func (r *textRows) RawValues() [][]byte                          { return nil }

// TypeMap is pgx 5.11's addition to the Rows interface. A fake that
// decodes nothing still has to answer it, and a default map is the
// honest answer: these rows carry values the caller reads directly.
func (r *textRows) TypeMap() *pgtype.Map { return pgtype.NewMap() }
func (r *textRows) Conn() *pgx.Conn      { return nil }

// TestTheAdvanceUsesTheBoundReadBeforeThePeek asserts the WIRING, not just
// the SQL shapes above: the value handed to pg_replication_slot_advance must
// be the FIRST WAL end read -- the one taken before the peek -- because a
// later read is a bound the peek never covered.
func TestTheAdvanceUsesTheBoundReadBeforeThePeek(t *testing.T) {
	wf := &placementWorkflow{
		id:   "0123456789abcdef",
		spec: placementSpec{SchemaName: "public", TableName: "orders"},
		st:   placementState{SourceSet: "a"},
	}
	c := &advanceConn{}
	if _, _, err := (&Placer{}).catchUpSource(context.Background(), wf, c, targetConns{}, 0, false); err != nil {
		t.Fatal(err)
	}
	if len(c.handed) == 0 {
		t.Fatal("the WAL end was never read; the advance cannot be using a bound taken before the peek")
	}
	if c.advanced != c.handed[0] {
		t.Fatalf("the advance was given %q; the bound read BEFORE the peek was %q (all reads: %v)", c.advanced, c.handed[0], c.handed)
	}
}
