package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// slotDialer serves the pg_replication_slots flush query of one shard from
// a fixed map of slot name to confirmed flush position.
type slotDialer struct {
	slots map[string]map[string]int64
}

func (d *slotDialer) Dial(_ context.Context, set string, id int32) (ShardConn, error) {
	return &slotConn{slots: d.slots[slotShardKey(set, id)]}, nil
}

func (d *slotDialer) DialDatabase(ctx context.Context, set string, id int32, _ string) (ShardConn, error) {
	return d.Dial(ctx, set, id)
}

func slotShardKey(set string, id int32) string { return fmt.Sprintf("%s/%d", set, id) }

type slotConn struct {
	slots map[string]int64
}

func (c *slotConn) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	if !strings.Contains(sql, "pg_replication_slots") {
		return nil, nil
	}
	r := &slotRows{}
	for _, name := range sortedKeys(c.slots) {
		r.names = append(r.names, name)
		r.vals = append(r.vals, c.slots[name])
	}
	return r, nil
}

func (c *slotConn) Exec(context.Context, string, ...any) (CommandTag, error) { return nil, nil }
func (c *slotConn) Close(context.Context) error                              { return nil }

type slotRows struct {
	names []string
	vals  []int64
	i     int
}

func (r *slotRows) Close()                                       {}
func (r *slotRows) Err() error                                   { return nil }
func (r *slotRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *slotRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *slotRows) Next() bool                                   { r.i++; return r.i <= len(r.names) }
func (r *slotRows) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.names[r.i-1]
	*(dest[1].(*int64)) = r.vals[r.i-1]
	return nil
}
func (r *slotRows) Values() ([]any, error) { return []any{r.names[r.i-1], r.vals[r.i-1]}, nil }
func (r *slotRows) RawValues() [][]byte    { return nil }
func (r *slotRows) Conn() *pgx.Conn        { return nil }

func testCutover(d *slotDialer) *pgCutover {
	return &pgCutover{
		c:      &Copier{Shards: d},
		wf:     &copyWorkflow{gen: 5, set: "g2", ids: []int32{0, 1}},
		srcSet: "default",
		srcIDs: []int32{0, 1},
	}
}

// TestCaughtUpRequiresEveryForwardSlot: catch-up is declared only when the
// exact expected slot of every (target, source) pair confirmed a flush at
// or past the gathered position. A vanished slot, an empty query result or
// a NULL confirmed_flush_lsn must read as behind, never as caught-up.
func TestCaughtUpRequiresEveryForwardSlot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	positions := map[string]int64{"0": 100, "1": 100}
	full := func() map[string]map[string]int64 {
		return map[string]map[string]int64{
			slotShardKey("default", 0): {
				SubscriptionName(5, 0, 0): 150,
				SubscriptionName(5, 1, 0): 150,
			},
			slotShardKey("default", 1): {
				SubscriptionName(5, 0, 1): 150,
				SubscriptionName(5, 1, 1): 150,
			},
		}
	}

	o := testCutover(&slotDialer{slots: full()})
	if ok, why, err := o.CaughtUp(ctx, positions); err != nil || !ok {
		t.Fatalf("complete confirmed slots must be caught up: %v %q %v", ok, why, err)
	}

	vanished := full()
	delete(vanished[slotShardKey("default", 1)], SubscriptionName(5, 1, 1))
	o = testCutover(&slotDialer{slots: vanished})
	if ok, why, _ := o.CaughtUp(ctx, positions); ok || !strings.Contains(why, SubscriptionName(5, 1, 1)) || !strings.Contains(why, "missing") {
		t.Fatalf("a vanished forward slot must block the flip: %v %q", ok, why)
	}

	o = testCutover(&slotDialer{slots: map[string]map[string]int64{}})
	if ok, why, _ := o.CaughtUp(ctx, positions); ok || !strings.Contains(why, "missing") {
		t.Fatalf("an empty slot listing must block the flip: %v %q", ok, why)
	}

	unconfirmed := full()
	unconfirmed[slotShardKey("default", 0)][SubscriptionName(5, 0, 0)] = -1
	o = testCutover(&slotDialer{slots: unconfirmed})
	if ok, why, _ := o.CaughtUp(ctx, positions); ok || !strings.Contains(why, "no confirmed flush position") {
		t.Fatalf("a NULL confirmed_flush_lsn must block the flip: %v %q", ok, why)
	}
}

// TestReverseBehindRequiresEveryReverseSlot: the rollback flip waits on the
// exact expected reverse slot of every (source, target) pair; a vanished
// reverse slot or an empty listing must keep serving away from the sources.
func TestReverseBehindRequiresEveryReverseSlot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	positions := map[int32]int64{0: 100, 1: 100}
	full := func() map[string]map[string]int64 {
		return map[string]map[string]int64{
			slotShardKey("g2", 0): {
				ReverseSubscriptionName(5, 0, 0): 150,
				ReverseSubscriptionName(5, 1, 0): 150,
			},
			slotShardKey("g2", 1): {
				ReverseSubscriptionName(5, 0, 1): 150,
				ReverseSubscriptionName(5, 1, 1): 150,
			},
		}
	}

	o := testCutover(&slotDialer{slots: full()})
	if behind, err := o.reverseBehind(ctx, positions); err != nil || len(behind) != 0 {
		t.Fatalf("complete confirmed reverse slots must be caught up: %v %v", behind, err)
	}

	vanished := full()
	delete(vanished[slotShardKey("g2", 0)], ReverseSubscriptionName(5, 1, 0))
	o = testCutover(&slotDialer{slots: vanished})
	behind, err := o.reverseBehind(ctx, positions)
	if err != nil || len(behind) != 1 || !strings.Contains(behind[0], ReverseSubscriptionName(5, 1, 0)) || !strings.Contains(behind[0], "missing") {
		t.Fatalf("a vanished reverse slot must block the flip back: %v %v", behind, err)
	}

	o = testCutover(&slotDialer{slots: map[string]map[string]int64{}})
	if behind, err := o.reverseBehind(ctx, positions); err != nil || len(behind) != 4 {
		t.Fatalf("an empty reverse listing must report every expected slot behind: %v %v", behind, err)
	}
}
