//go:build integration

package router

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// copyConsumer applies a VStream with an initial copy the way a sink would:
// copy rows and inserts upsert by primary key, deletes remove, and the last
// VGtid is the resume point.
type copyConsumer struct {
	rows      map[string]bool
	last      *pgshardv1.VPosition
	completed []string
	copyRows  int
	vgtids    int
}

func newCopyConsumer() *copyConsumer { return &copyConsumer{rows: map[string]bool{}} }

func (c *copyConsumer) key(row *pgshardv1.VEvent_Row) string {
	cols := row.GetNew().GetColumns()
	return string(cols[0].GetValue()) + "/" + string(cols[1].GetValue())
}

// run consumes until stop says so after a VGtid (or the stream ends) and
// reports the event lines seen.
func (c *copyConsumer) run(tb testing.TB, st pgshardv1.VStream_StreamClient, stop func(*copyConsumer) bool) {
	tb.Helper()
	for {
		ev, err := st.Recv()
		if err != nil {
			tb.Fatalf("recv: %v", err)
		}
		switch e := ev.GetEvent().(type) {
		case *pgshardv1.VEvent_Row_:
			switch e.Row.GetKind() {
			case pgshardv1.VEvent_Row_KIND_INSERT, pgshardv1.VEvent_Row_KIND_UPDATE:
				if e.Row.GetCopy() {
					c.copyRows++
				}
				c.rows[c.key(e.Row)] = true
			case pgshardv1.VEvent_Row_KIND_DELETE:
				delete(c.rows, c.key(e.Row))
			}
		case *pgshardv1.VEvent_CopyCompleted_:
			switch {
			case e.CopyCompleted.GetShard() == nil:
				c.completed = append(c.completed, "stream")
			case e.CopyCompleted.GetTable() == "":
				c.completed = append(c.completed, fmt.Sprintf("shard%d", e.CopyCompleted.GetShard().GetShardId()))
			default:
				c.completed = append(c.completed, fmt.Sprintf("shard%d:%s", e.CopyCompleted.GetShard().GetShardId(), e.CopyCompleted.GetTable()))
			}
		case *pgshardv1.VEvent_Vgtid:
			c.last = e.Vgtid.GetPosition()
			c.vgtids++
			if stop(c) {
				return
			}
		case *pgshardv1.VEvent_Error_:
			tb.Fatalf("stream error: %v", e.Error)
		}
	}
}

func (s *vstreamStack) tableRows(tb testing.TB, shard int) []string {
	tb.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, s.appDSN(shard))
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, "select tenant_id::text || '/' || id::text from orders")
	if err != nil {
		tb.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		tb.Fatal(err)
	}
	return out
}

func TestRouterVStreamInitialCopy(t *testing.T) {
	s := startVStreamStack(t)
	ctx := context.Background()
	t0, t1 := twoTenants(t)
	conn := s.connect(t)
	s.awaitSharded(t, conn)
	if created, err := s.client.Create(ctx, &pgshardv1.CreateVStreamRequest{Stream: "orders", Database: appDatabase}); err != nil || created.GetError() != nil {
		t.Fatalf("create: %v %v", created, err)
	}

	const perShard = 10000
	for shard, tenant := range []int64{t0, t1} {
		c, err := pgx.Connect(ctx, s.appDSN(shard))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Exec(ctx, fmt.Sprintf("insert into orders select %d, g from generate_series(%d, %d) g", tenant, shard*perShard+1, (shard+1)*perShard)); err != nil {
			t.Fatal(err)
		}
		_ = c.Close(ctx)
	}

	// Concurrent writes through the router while the copy runs.
	var wg sync.WaitGroup
	stopWriter := make(chan struct{})
	written := 0
	var writerErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		wc := s.connect(t)
		id := 2*perShard + 1
		for {
			select {
			case <-stopWriter:
				return
			default:
			}
			tenant := t0
			if id%2 == 0 {
				tenant = t1
			}
			if _, err := wc.Exec(ctx, fmt.Sprintf("insert into orders (tenant_id, id) values (%d, %d)", tenant, id), pgx.QueryExecModeSimpleProtocol); err != nil {
				writerErr = err
				return
			}
			written++
			id++
			time.Sleep(2 * time.Millisecond)
		}
	}()

	open := func(pos *pgshardv1.VPosition) (pgshardv1.VStream_StreamClient, context.CancelFunc) {
		t.Helper()
		sctx, cancel := context.WithCancel(ctx)
		st, err := s.client.Stream(sctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.Send(&pgshardv1.VStreamRequest{Request: &pgshardv1.VStreamRequest_Start_{Start: &pgshardv1.VStreamRequest_Start{
			Stream: "orders", Position: pos, Options: &pgshardv1.VStreamOptions{HeartbeatIntervalMs: 500,
				StartFrom: pgshardv1.StartFrom_START_FROM_COPY, CopyBatchRows: 500}}}}); err != nil {
			t.Fatal(err)
		}
		return st, cancel
	}

	c := newCopyConsumer()
	// Killed twice in the middle of the copy, resumed from the last VGtid
	// each time.
	for attempt, rowsBeforeKill := range []int{3000, 9000} {
		st, cancel := open(c.last)
		c.run(t, st, func(c *copyConsumer) bool { return c.copyRows >= rowsBeforeKill })
		cancel()
		if c.last == nil || len(c.last.GetCopyState()) == 0 {
			t.Fatalf("attempt %d: consumer died after %d copy rows without copy state in its vector: %v", attempt, c.copyRows, c.last)
		}
		var cur []string
		for _, cs := range c.last.GetCopyState() {
			if cs.GetCurrent() != nil {
				cur = append(cur, fmt.Sprintf("shard%d:%s@%s", cs.GetShard().GetShardId(), cs.GetCurrent().GetTable(), cs.GetCurrent().GetLastpk()))
			}
		}
		t.Logf("attempt %d killed after %d copy rows; resuming from %v", attempt, c.copyRows, cur)
	}
	st, cancel := open(c.last)
	c.run(t, st, func(c *copyConsumer) bool {
		return len(c.completed) > 0 && c.completed[len(c.completed)-1] == "stream"
	})
	close(stopWriter)
	wg.Wait()
	if writerErr != nil {
		t.Fatalf("writer: %v", writerErr)
	}
	want := map[string]bool{}
	for shard := range 2 {
		for _, r := range s.tableRows(t, shard) {
			want[r] = true
		}
	}
	// Drain the stream until the consumer holds every row.
	c.run(t, st, func(c *copyConsumer) bool { return len(c.rows) >= len(want) })
	cancel()

	first := func(name string) int {
		for i, v := range c.completed {
			if v == name {
				return i
			}
		}
		return -1
	}
	for _, sh := range []string{"shard0", "shard1"} {
		if first(sh+":orders") < 0 || first(sh) < first(sh+":orders") {
			t.Fatalf("copy completion order: %v", c.completed)
		}
	}
	if first("stream") != len(c.completed)-1 || c.completed[len(c.completed)-2] == "stream" {
		t.Fatalf("copy completion order: %v", c.completed)
	}
	if c.copyRows < 2*perShard {
		t.Fatalf("copied %d rows, want at least %d (rows above the checkpoint may repeat)", c.copyRows, 2*perShard)
	}
	var missing, extra []string
	for k := range want {
		if !c.rows[k] {
			missing = append(missing, k)
		}
	}
	for k := range c.rows {
		if !want[k] {
			extra = append(extra, k)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing) > 0 || len(extra) > 0 {
		t.Fatalf("consumer state differs from the shards: missing %d %v extra %d %v (written %d concurrently)", len(missing), head(missing), len(extra), head(extra), written)
	}
	if written == 0 {
		t.Fatal("no concurrent writes happened during the copy")
	}
	t.Logf("copied %d rows in %d vgtids with %d concurrent inserts; final state %d rows", c.copyRows, c.vgtids, written, len(c.rows))
}

func head(s []string) []string {
	if len(s) > 10 {
		return s[:10]
	}
	return s
}
