package router

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// memBlocks is an in-memory BlockSource: one counter per sequence, blocks
// of fixed size, and a count of allocations.
type memBlocks struct {
	mu    sync.Mutex
	next  map[string]int64
	size  int64
	calls int
	fail  error
}

func newMemBlocks(size int64) *memBlocks { return &memBlocks{next: map[string]int64{}, size: size} }

func (m *memBlocks) AllocateBlock(_ context.Context, name string) (int64, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail != nil {
		return 0, 0, m.fail
	}
	m.calls++
	start := m.next[name]
	if start == 0 {
		start = 1
	}
	m.next[name] = start + m.size
	return start, start + m.size - 1, nil
}

func TestSequenceAllocatorHandsOutUniqueMonotonicValues(t *testing.T) {
	src := newMemBlocks(64)
	a := NewSequenceAllocator(src)
	const goroutines, per = 50, 100
	var wg sync.WaitGroup
	results := make([][]int64, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				vals, err := a.Next(context.Background(), "s", 1)
				if err != nil {
					t.Error(err)
					return
				}
				results[g] = append(results[g], vals[0])
			}
		}(g)
	}
	wg.Wait()
	seen := map[int64]bool{}
	for _, r := range results {
		for i, v := range r {
			if seen[v] {
				t.Fatalf("value %d handed out twice", v)
			}
			seen[v] = true
			if i > 0 && v <= r[i-1] {
				t.Fatalf("values of one goroutine are not increasing: %d after %d", v, r[i-1])
			}
		}
	}
	if len(seen) != goroutines*per {
		t.Fatalf("%d distinct values, want %d", len(seen), goroutines*per)
	}
	if want := goroutines * per / 64; src.calls < want || src.calls > want+1 {
		t.Fatalf("%d block fetches for %d values of block size 64", src.calls, goroutines*per)
	}
	vals, err := a.Next(context.Background(), "s", 5)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(vals); i++ {
		if vals[i] != vals[i-1]+1 || seen[vals[i]] {
			t.Fatalf("batch of 5 is not fresh and consecutive: %v", vals)
		}
	}
	src.fail = errors.New("catalog down")
	b := NewSequenceAllocator(src)
	if _, err := b.Next(context.Background(), "s", 1); err == nil || !strings.Contains(err.Error(), "catalog down") {
		t.Fatalf("source failure must surface, got %v", err)
	}
}

type refHarness struct {
	*txnHarness
	blocks *memBlocks
}

func newRefHarness(t *testing.T) *refHarness {
	t.Helper()
	log := &fakeDecisionLog{rows: map[string]string{}, fail: map[string]error{}}
	blocks := newMemBlocks(10)
	h := newShardedHarnessWith(t, Config{Decisions: log, Sequences: NewSequenceAllocator(blocks)})
	log.h = h
	return &refHarness{txnHarness: &txnHarness{shardedHarness: h, log: log}, blocks: blocks}
}

func (h *refHarness) shardsRunning(needle string) []int {
	var out []int
	for i := range h.poolers {
		if h.ranOn(i, needle) {
			out = append(out, i)
		}
	}
	return out
}

func TestReferenceWriteRunsOnEveryShardWithTwoPhaseCommit(t *testing.T) {
	h := newRefHarness(t)
	ctx := context.Background()
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeCacheStatement} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			conn := h.connect(t, h.dsn())
			for i := range h.poolers {
				h.poolers[i].script("insert into regions (id, name) values (7, 'eu') returning id", script{cols: []scriptCol{{"id", 23}}, rows: [][]string{{"7"}}})
				h.poolers[i].script("insert into regions (id, name) values ($1, $2) returning id", script{cols: []scriptCol{{"id", 23}}, rows: [][]string{{"7"}}})
			}
			h.log.mu.Lock()
			h.log.events = nil
			h.log.mu.Unlock()
			var id int
			var err error
			if mode == pgx.QueryExecModeSimpleProtocol {
				err = conn.QueryRow(ctx, "insert into regions (id, name) values (7, 'eu') returning id", mode).Scan(&id)
			} else {
				err = conn.QueryRow(ctx, "insert into regions (id, name) values ($1, $2) returning id", mode, 7, "eu").Scan(&id)
			}
			if err != nil || id != 7 {
				t.Fatalf("returning: id=%d err=%v", id, err)
			}
			if got := h.shardsRunning("insert into regions"); len(got) != 4 {
				t.Fatalf("insert ran on shards %v, want all four", got)
			}
			if got := h.allRan("prepare transaction 'pgshard-"); len(got) != 4 {
				t.Fatalf("shards that prepared: %v, want all four", got)
			}
			want := []string{"begin(prepared=0)", "commit(prepared=4)", "delete(prepared=0)"}
			if got := h.log.log(); strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("decision log %v, want %v", got, want)
			}
			if h.prepared() != 0 {
				t.Fatalf("%d prepared transactions left", h.prepared())
			}
			var n int
			if err := conn.QueryRow(ctx, "select 1", mode).Scan(&n); err != nil || n != 1 {
				t.Fatalf("session unusable afterwards: %v", err)
			}
			if mode == pgx.QueryExecModeSimpleProtocol {
				for i := range h.poolers {
					if h.poolers[i].isReserved(h.sidOf(t)) {
						t.Fatalf("shard %d still holds the session's backend after the implicit transaction", i)
					}
				}
			}
		})
	}
}

func TestReferenceWriteInsideTransactionJoinsTheShardedWrite(t *testing.T) {
	h := newRefHarness(t)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	a, _ := h.twoTenants(t)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", a); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "update regions set name = 'x' where id = 1"); err != nil {
		t.Fatalf("reference write in a transaction: %v", err)
	}
	if got := h.log.log(); len(got) != 0 {
		t.Fatalf("decision log touched before COMMIT: %v", got)
	}
	if h.prepared() != 0 {
		t.Fatalf("prepared before COMMIT: %d", h.prepared())
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if got := h.shardsRunning("update regions"); len(got) != 4 {
		t.Fatalf("update ran on shards %v", got)
	}
	if got := h.allRan("commit prepared 'pgshard-"); len(got) != 4 {
		t.Fatalf("shards that committed prepared: %v, want all four", got)
	}
	// Rolling back undoes everywhere without touching the decision log.
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "delete from regions where id = 1"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if got := h.allRan("rollback"); len(got) != 4 {
		t.Fatalf("shards that rolled back: %v", got)
	}
}

func TestReferenceWriteFailingOnOneShardAbortsEverywhere(t *testing.T) {
	h := newRefHarness(t)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	h.poolers[2].script("update regions set name = 'y'", script{err: "relation \"regions\" does not exist on shard 2"})
	_, err := conn.Exec(ctx, "update regions set name = 'y'")
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || !strings.Contains(pe.Message, "shard 2") {
		t.Fatalf("expected the shard's error, got %v", err)
	}
	if got := h.log.log(); len(got) != 0 {
		t.Fatalf("a failed statement must not reach the decision log: %v", got)
	}
	for i := range h.poolers {
		if i != 2 && !h.ranOn(i, "the statement failed on another shard") {
			t.Fatalf("shard %d was not aborted: %v", i, h.poolers[i].ran())
		}
	}
	if got := h.allRan("rollback"); len(got) != 4 {
		t.Fatalf("shards rolled back: %v, want all four", got)
	}
	var n int
	if err := conn.QueryRow(ctx, "select 1").Scan(&n); err != nil || n != 1 {
		t.Fatalf("session unusable afterwards: %v", err)
	}
	// Inside an explicit transaction the failure leaves it aborted.
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "update regions set name = 'y'"); err == nil {
		t.Fatal("expected the shard's error")
	}
	if _, err := tx.Exec(ctx, "select 1"); err == nil || !errors.As(err, &pe) || pe.Code != "25P02" {
		t.Fatalf("transaction must be aborted, got %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestReferenceWriteRefusalsAndSingleMode(t *testing.T) {
	h := newRefHarness(t)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	_ = expectRefusal(t, execFail(t, conn, "insert into regions (id, at) values (1, now())"), "a write to reference table \"regions\" cannot call now()")
	_ = expectRefusal(t, execFail(t, conn, "insert into regions select id, name from items"), "a write to reference table \"regions\" cannot read sharded or unsharded tables")
	if _, err := conn.Exec(ctx, "set pgshard.transaction_mode = single"); err != nil {
		t.Fatal(err)
	}
	pe := expectRefusal(t, execFail(t, conn, "delete from regions"), "transaction already writes to shard")
	if !strings.Contains(pe.Message, "pgshard.transaction_mode is single") {
		t.Fatalf("message %q", pe.Message)
	}
	if got := h.shardsRunning("delete from regions"); len(got) != 0 {
		t.Fatalf("delete ran on %v in single mode", got)
	}
	var n int
	if err := conn.QueryRow(ctx, "select 1").Scan(&n); err != nil || n != 1 {
		t.Fatalf("session unusable afterwards: %v", err)
	}
}

func execFail(t *testing.T, conn *pgx.Conn, sql string) error {
	t.Helper()
	_, err := conn.Exec(context.Background(), sql)
	return err
}

func TestSequenceColumnsAreFilledAndRouted(t *testing.T) {
	h := newRefHarness(t)
	ctx := context.Background()
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeCacheStatement, pgx.QueryExecModeCacheDescribe} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			conn := h.connect(t, h.dsn())
			for i := range h.poolers {
				h.poolers[i].script("insert into tickets (tenant_id, body, id) values ($1, $2, $3) returning id", script{cols: []scriptCol{{"id", 20}}, rows: [][]string{{"1"}}})
				h.poolers[i].script("insert into tickets (tenant_id, body, id) values (5, 'x', $1) returning id", script{cols: []scriptCol{{"id", 20}}, rows: [][]string{{"1"}}})
			}
			var id int64
			var err error
			if mode == pgx.QueryExecModeSimpleProtocol {
				err = conn.QueryRow(ctx, "insert into tickets (tenant_id, body) values (5, 'x') returning id", mode).Scan(&id)
			} else {
				err = conn.QueryRow(ctx, "insert into tickets (tenant_id, body) values ($1, $2) returning id", mode, int64(5), "x").Scan(&id)
			}
			if err != nil {
				t.Fatalf("insert: %v", err)
			}
			shard := h.shardOf(t, int64(5))
			var bound []string
			for i := range h.poolers {
				for _, b := range h.poolers[i].boundExecs() {
					if strings.Contains(b, "tickets") {
						if i != shard {
							t.Fatalf("insert ran on shard %d, want %d", i, shard)
						}
						bound = append(bound, b)
					}
				}
			}
			if len(bound) == 0 {
				t.Fatal("no bound execution of the rewritten insert")
			}
			last := bound[len(bound)-1]
			if !strings.Contains(last, "values ($1, $2, $3) returning id <- ") && !strings.Contains(last, "values (5, 'x', $1) returning id <- ") {
				t.Fatalf("rewritten insert not bound with an injected value: %q", last)
			}
			if v := injectedValue(last); v < 1 {
				t.Fatalf("no injected value in %q", last)
			}
			var n int
			if err := conn.QueryRow(ctx, "select 1", mode).Scan(&n); err != nil {
				t.Fatal(err)
			}
		})
	}
	// Distinct executions get distinct values, across statements and modes.
	values := map[int64]bool{}
	for i := range h.poolers {
		for _, b := range h.poolers[i].boundExecs() {
			if !strings.Contains(b, "tickets") {
				continue
			}
			v := injectedValue(b)
			if values[v] {
				t.Fatalf("sequence value %d used twice", v)
			}
			values[v] = true
		}
	}
	if len(values) != 3 {
		t.Fatalf("expected 3 distinct values, got %v", values)
	}
}

// injectedValue reads the last (router-injected, text) parameter of a
// recorded execution; -1 when there is none.
func injectedValue(bound string) int64 {
	_, params, ok := strings.Cut(bound, " <- ")
	if !ok {
		return -1
	}
	if i := strings.LastIndex(params, ","); i >= 0 {
		params = params[i+1:]
	}
	var v int64
	if _, err := fmt.Sscan(params, &v); err != nil {
		return -1
	}
	return v
}

func TestSequenceColumnAsShardKeyRoutesByInjectedValue(t *testing.T) {
	h := newRefHarness(t)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeCacheStatement} {
		var err error
		if mode == pgx.QueryExecModeSimpleProtocol {
			_, err = conn.Exec(ctx, "insert into eventlog (body) values ('x')", mode)
		} else {
			_, err = conn.Exec(ctx, "insert into eventlog (body) values ($1)", mode, "x")
		}
		if err != nil {
			t.Fatalf("%v: %v", mode, err)
		}
	}
	found := 0
	for i := range h.poolers {
		for _, b := range h.poolers[i].boundExecs() {
			if !strings.Contains(b, "eventlog") {
				continue
			}
			found++
			id := injectedValue(b)
			if id < 1 {
				t.Fatalf("no injected value in %q", b)
			}
			if h.shardOf(t, id) != i {
				t.Fatalf("event %d ran on shard %d, want %d", id, i, h.shardOf(t, id))
			}
		}
	}
	if found != 2 {
		t.Fatalf("%d event inserts found", found)
	}
}

func TestNextvalOverGlobalSequenceIsAnsweredByTheRouter(t *testing.T) {
	h := newRefHarness(t)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	var prev int64
	for i, mode := range []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeCacheStatement, pgx.QueryExecModeCacheDescribe, pgx.QueryExecModeCacheStatement} {
		var v int64
		if err := conn.QueryRow(ctx, "select nextval('tickets.id')", mode).Scan(&v); err != nil {
			t.Fatalf("%v: %v", mode, err)
		}
		if i > 0 && v <= prev {
			t.Fatalf("nextval went from %d to %d", prev, v)
		}
		prev = v
	}
	var v int64
	if err := conn.QueryRow(ctx, "select nextval('invoice_numbers')").Scan(&v); err != nil || v != 1 {
		t.Fatalf("declared sequence: %d %v", v, err)
	}
	for i := range h.poolers {
		for _, q := range h.poolers[i].ran() {
			if strings.Contains(q, "nextval") {
				t.Fatalf("nextval reached shard %d: %q", i, q)
			}
		}
	}
	// A native sequence name goes to the shard as usual.
	_, err := conn.Exec(ctx, "select nextval('items_id_seq')")
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || !strings.Contains(pe.Message, "fake pooler does not understand") {
		t.Fatalf("native nextval must reach the shard, got %v", err)
	}
	if !h.ranOn(0, "nextval('items_id_seq')") {
		t.Fatal("native nextval did not run on the home shard")
	}
}

func TestSequenceStatementsWithoutAllocatorAreRefused(t *testing.T) {
	log := &fakeDecisionLog{rows: map[string]string{}, fail: map[string]error{}}
	h := newShardedHarnessWith(t, Config{Decisions: log})
	log.h = h
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	_ = expectRefusal(t, execFail(t, conn, "insert into tickets (tenant_id, body) values (5, 'x')"), "global sequences are not available")
	_ = expectRefusal(t, execFail(t, conn, "select nextval('tickets.id')"), "global sequences are not available")
	if _, err := conn.Exec(ctx, "insert into tickets (tenant_id, id, body) values (5, 1, 'x')"); err != nil {
		t.Fatalf("an insert supplying the value needs no allocator: %v", err)
	}
}

func TestStatementPreparedWhileAnotherShardIsParkedIsReplayedOnRevival(t *testing.T) {
	h := newRefHarness(t)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	// After a reference write the session sits on the last shard; a
	// statement prepared there must reach the parked stream it then runs on.
	var tenant int64 = -1
	for i := int64(1); i < 100 && tenant < 0; i++ {
		if sh := h.shardOf(t, i); sh != 0 && sh != len(h.poolers)-1 {
			tenant = i
		}
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "update regions set name = 'x'"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 2)", tenant); err != nil {
		t.Fatalf("statement prepared on another shard must be parsed on the revived stream: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, b := range h.poolers[h.shardOf(t, tenant)].boundExecs() {
		if strings.Contains(b, "values ($1, 2)") {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("shard %d executed the statement %d times", h.shardOf(t, tenant), n)
	}
}

// TestReferenceWriteWithReturningTellsTheClientNothingUntilEveryShardRan:
// the canonical shard used to write its rows and CommandComplete straight
// to the client while the other shards were still running, so a client was
// told a write had succeeded and only then learned another shard had
// failed and the whole thing was aborted. A RETURNING large enough to
// flush had already reached the socket, past retracting.
func TestReferenceWriteWithReturningTellsTheClientNothingUntilEveryShardRan(t *testing.T) {
	h := newRefHarness(t)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	const sql = "update regions set name = 'z' returning id"
	// Shard 2 fails; every other shard, including the canonical one, would
	// have succeeded and produced rows.
	// Held back so the canonical shard certainly finishes first: that is
	// the ordering the bug needs, and leaving it to the scheduler tests
	// nothing in particular.
	h.poolers[2].script(sql, script{err: "relation \"regions\" does not exist on shard 2", delay: 250 * time.Millisecond})
	// Over the 256-row flush threshold: below it the rows sit in pgwire's
	// buffer and the error can still replace them, so a smaller result
	// hides the bug rather than testing it.
	vals := make([]string, 300)
	for i := range vals {
		vals[i] = fmt.Sprint(i)
	}
	for i := range h.poolers {
		if i != 2 {
			h.poolers[i].script(sql, int4Rows(vals...))
		}
	}
	var seen int
	rows, err := conn.Query(ctx, sql)
	if err == nil {
		for rows.Next() {
			seen++
		}
		err = rows.Err()
		rows.Close()
	}
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || !strings.Contains(pe.Message, "shard 2") {
		t.Fatalf("expected the failing shard's error, got %v", err)
	}
	// The error arriving is not enough: the statement failed, so no row of
	// its RETURNING may have reached the client first. Rows followed by an
	// error is precisely the defect -- the client was told the write had
	// produced these rows, and only afterwards that it had been aborted.
	if seen != 0 {
		t.Fatalf("the client was given %d rows before learning the write failed on shard 2", seen)
	}
}

// TestReferenceWriteSetsUpEveryShardConcurrently pins the shape of a
// reference write's setup: it costs a round trip, not one per shard.
// Reserving a backend and replaying the session state used to run in a
// sequential walk, so the setup grew with the shard count and every shard
// already opened held its transaction while the walk reached the rest.
func TestReferenceWriteSetsUpEveryShardConcurrently(t *testing.T) {
	const shards = 32
	log := &fakeDecisionLog{rows: map[string]string{}, fail: map[string]error{}}
	h := newShardedHarnessShards(t, Config{Decisions: log, Sequences: NewSequenceAllocator(newMemBlocks(10))}, shards)
	log.h = h
	// The shard the session already sits on is opened before the others,
	// so the concurrent phase is every shard but that one.
	gate := &reserveGate{width: shards - 1}
	for i := range h.poolers {
		h.poolers[i].gate = gate
		h.poolers[i].script("insert into regions (id, name) values (7, 'eu')", script{})
	}
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	// Any session setting makes the shards reserve a backend.
	if _, err := conn.Exec(ctx, "set timezone to 'UTC'"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "insert into regions (id, name) values (7, 'eu')"); err != nil {
		t.Fatal(err)
	}
	th := &txnHarness{shardedHarness: h, log: log}
	if got := th.allRan("insert into regions"); len(got) != shards {
		t.Fatalf("the write ran on %d shards, want all %d", len(got), shards)
	}
	if !gate.reached() {
		t.Errorf("no two of the %d shards were ever set up at the same time", shards)
	}
}

// TestReferenceWriteCarriesSQLPreparedStatements: a shard opened for a
// reference write must reach the state the session's statements need, and
// a SQL-level PREPARE is part of that state. Preparing the shards together
// replays it the same way the one-at-a-time walk did; without it an
// EXECUTE on a shard the write opened finds no such statement.
func TestReferenceWriteCarriesSQLPreparedStatements(t *testing.T) {
	h := newRefHarness(t)
	ctx := context.Background()
	conn := h.connect(t, h.dsn()+"&default_query_exec_mode=simple_protocol")
	for i := range h.poolers {
		h.poolers[i].script("insert into regions (id, name) values (7, 'eu')", script{})
	}
	if _, err := conn.Exec(ctx, "prepare p1 as select 1"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "insert into regions (id, name) values (7, 'eu')"); err != nil {
		t.Fatal(err)
	}
	if got := h.allRan("prepare p1"); len(got) != len(h.poolers) {
		t.Fatalf("shards that saw the PREPARE: %v, want all %d", got, len(h.poolers))
	}
}

// TestReferenceWriteReleasesAShardItCouldNotPrepare: preparing the shards
// together must leave a shard that refused exactly as it found it. Aborting
// the stream only cancels the router's side -- the pooler holds the session
// attached and refuses a second stream -- so a shard that failed after its
// stream opened has to be released, or the walk that follows reports a
// stream collision instead of the refusal the client needs to see.
func TestReferenceWriteReleasesAShardItCouldNotPrepare(t *testing.T) {
	h := newRefHarness(t)
	ctx := context.Background()
	conn := h.connect(t, h.dsn()+"&default_query_exec_mode=simple_protocol")
	for i := range h.poolers {
		h.poolers[i].script("insert into regions (id, name) values (7, 'eu')", script{})
	}
	// A session setting makes every shard reserve; shard 3 then refuses.
	if _, err := conn.Exec(ctx, "set timezone to 'UTC'"); err != nil {
		t.Fatal(err)
	}
	h.poolers[3].gen = 999

	_, err := conn.Exec(ctx, "insert into regions (id, name) values (7, 'eu')")
	if err == nil {
		t.Fatal("the write must fail: one shard refuses the session")
	}
	// The client is told the topology moved and the write did not happen,
	// not handed the shard's own 55000: a reference write that one shard
	// refuses is aborted everywhere, so running it again is the answer.
	if sqlstate(err) != "40001" {
		t.Fatalf("the client must be told to retry, got %v", err)
	}
	h.poolers[3].mu.Lock()
	releases := len(h.poolers[3].releases)
	h.poolers[3].mu.Unlock()
	if releases == 0 {
		t.Error("the shard that refused was never released, so its stream stays attached")
	}
}
