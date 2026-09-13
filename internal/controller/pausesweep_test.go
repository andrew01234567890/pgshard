package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// fakeCatalog answers the sweep's one query from a fixed list of orphans and
// records every statement it is asked to run.
type fakeCatalog struct {
	orphans []ShardRef
	execs   []string
	queried int
	execErr error
}

func (c *fakeCatalog) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	if !strings.Contains(sql, "write_paused_by IS NOT NULL") {
		return nil, fmt.Errorf("unexpected query: %s", sql)
	}
	c.queried++
	return &shardRefRows{refs: c.orphans}, nil
}

func (c *fakeCatalog) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	c.execs = append(c.execs, fmt.Sprintf("%s %v", strings.Join(strings.Fields(sql), " "), args))
	return pgconn.CommandTag{}, c.execErr
}

type shardRefRows struct {
	refs []ShardRef
	i    int
}

func (r *shardRefRows) Close()                                       {}
func (r *shardRefRows) Err() error                                   { return nil }
func (r *shardRefRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *shardRefRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *shardRefRows) Next() bool                                   { r.i++; return r.i <= len(r.refs) }
func (r *shardRefRows) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.refs[r.i-1].Set
	*(dest[1].(*int32)) = r.refs[r.i-1].ID
	return nil
}
func (r *shardRefRows) Values() ([]any, error) { return nil, nil }
func (r *shardRefRows) RawValues() [][]byte    { return nil }
func (r *shardRefRows) TypeMap() *pgtype.Map   { return pgtype.NewMap() }
func (r *shardRefRows) Conn() *pgx.Conn        { return nil }

// pauseDialer records what each shard was asked to run, and can fail one.
type pauseDialer struct {
	ran     map[string][]string
	failOn  ShardRef
	failErr error
}

func (d *pauseDialer) Dial(_ context.Context, set string, id int32) (ShardConn, error) {
	if d.ran == nil {
		d.ran = map[string][]string{}
	}
	ref := ShardRef{Set: set, ID: id}
	if d.failErr != nil && ref == d.failOn {
		return nil, d.failErr
	}
	return &pauseConn{d: d, key: fmt.Sprintf("%s/%d", set, id)}, nil
}

type pauseConn struct {
	d   *pauseDialer
	key string
}

func (c *pauseConn) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("the sweep asks nothing")
}

func (c *pauseConn) Exec(_ context.Context, sql string, _ ...any) (CommandTag, error) {
	c.d.ran[c.key] = append(c.d.ran[c.key], sql)
	return nil, nil
}

func (c *pauseConn) Close(context.Context) error { return nil }

// PGS-789. A cutover past its swap step has set
// default_transaction_read_only = on via ALTER SYSTEM on every source
// primary. Every ordinary exit gives it back; deleting the pgshard.workflows
// row directly does not, and ALTER SYSTEM survives a restart, so the sources
// refuse every writing transaction with 25006 for good and with no hint.
func TestAPauseWhoseWorkflowIsGoneIsLifted(t *testing.T) {
	cat := &fakeCatalog{orphans: []ShardRef{{Set: "default", ID: 0}, {Set: "default", ID: 1}}}
	dialer := &pauseDialer{}
	freed, err := (&WritePauseSweep{Pool: cat, Shards: dialer}).Pass(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if freed != 2 {
		t.Fatalf("freed %d shards, want 2", freed)
	}
	for _, key := range []string{"default/0", "default/1"} {
		got := dialer.ran[key]
		if len(got) == 0 || !strings.Contains(got[0], "ALTER SYSTEM RESET default_transaction_read_only") {
			t.Errorf("%s ran %v, want the reset first", key, got)
			continue
		}
		if len(got) < 2 || !strings.Contains(got[1], "pg_reload_conf") {
			t.Errorf("%s ran %v: without the reload the reset is written and not in force", key, got)
		}
	}
	if len(cat.execs) != 2 {
		t.Fatalf("claims dropped: %v", cat.execs)
	}
	for _, e := range cat.execs {
		if !strings.Contains(e, "write_paused_by = NULL") {
			t.Errorf("unexpected catalog write %q", e)
		}
	}
}

// Nothing to sweep must touch no shard at all: the sweep runs on every
// resolve tick, and an ALTER SYSTEM per shard per tick would rewrite
// postgresql.auto.conf on a cluster that is working perfectly.
func TestASweepWithNoOrphansTouchesNothing(t *testing.T) {
	cat := &fakeCatalog{}
	dialer := &pauseDialer{}
	freed, err := (&WritePauseSweep{Pool: cat, Shards: dialer}).Pass(context.Background())
	if err != nil || freed != 0 {
		t.Fatalf("freed %d, %v", freed, err)
	}
	if len(dialer.ran) != 0 || len(cat.execs) != 0 {
		t.Fatalf("a clean cluster was written to: %v %v", dialer.ran, cat.execs)
	}
}

// The shard is reset before the claim is dropped. A crash between them
// leaves a claim on a shard that is already writable, which the next pass
// resets again for nothing; the other order would lose the record of a pause
// that is still on, which is the leak this exists to close.
func TestAShardThatCannotBeReachedKeepsItsClaim(t *testing.T) {
	cat := &fakeCatalog{orphans: []ShardRef{{Set: "default", ID: 0}, {Set: "default", ID: 1}}}
	dialer := &pauseDialer{failOn: ShardRef{Set: "default", ID: 0}, failErr: errors.New("no route to host")}
	freed, err := (&WritePauseSweep{Pool: cat, Shards: dialer}).Pass(context.Background())
	if err == nil {
		t.Fatal("a shard that could not be reached must fail the pass")
	}
	if freed != 0 {
		t.Fatalf("freed %d, want 0: the first shard is the one that failed", freed)
	}
	if len(cat.execs) != 0 {
		t.Fatalf("the claim was dropped for a shard still refusing writes: %v", cat.execs)
	}
}
