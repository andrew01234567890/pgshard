package router

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// TestPreparedHomeDDLMeetsTheGateWhenItRuns (PGS-975).
//
// SELECT ... INTO and CREATE TABLE AS create a relation on the home shard, so
// they are refused while a reshard, upgrade or placement they would overlap
// is unfinished: a reshard's copy carries rows, not new relations, and
// cutover loses one created mid-copy. The check was made where the statement
// was PLANNED. A statement prepared before the reshard started and executed
// during it was not planned again, so it was not checked again -- through
// SQL-level PREPARE/EXECUTE, which plans EXECUTE as session-local, and
// through a Parse the driver caches and Binds again later, which skips
// replanning when the snapshot has not moved.
func TestPreparedHomeDDLMeetsTheGateWhenItRuns(t *testing.T) {
	q := &fakeQueue{homeQueued: true}
	h := newDDLHarness(t, q)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	reshard := []catalog.Blocker{{Kind: catalog.OperationReshard, ID: "00000000-0000-0000-0000-00000000e975", Reason: catalog.BlockedByStarted}}
	refused := func(err error) bool {
		var pe *pgconn.PgError
		return errors.As(err, &pe) && pe.Code == "55000" && strings.Contains(pe.Message, "00000000-0000-0000-0000-00000000e975")
	}
	block := func(on bool) {
		q.mu.Lock()
		defer q.mu.Unlock()
		q.home = nil
		if on {
			q.home = reshard
		}
	}

	// SQL level: prepared with nothing in the way, executed during the
	// reshard.
	if _, err := conn.Exec(ctx, "prepare into_saved as select 1 as n into saved"); err != nil {
		t.Fatalf("PREPARE with nothing in the way: %v", err)
	}
	block(true)
	if _, err := conn.Exec(ctx, "execute into_saved"); !refused(err) {
		t.Fatalf("EXECUTE of a prepared SELECT ... INTO during a reshard: %v, want 55000 naming the reshard", err)
	}
	// PREPARE creates nothing, so it is not what is refused.
	if _, err := conn.Exec(ctx, "prepare into_later as select 1 as n into later"); refused(err) {
		t.Fatalf("PREPARE during a reshard was refused: %v; it creates nothing", err)
	}
	// An EXECUTE of an ordinary prepared statement is not caught by it.
	if _, err := conn.Exec(ctx, "prepare plain as select 1"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "execute plain"); refused(err) {
		t.Fatalf("EXECUTE of a plain SELECT during a reshard was refused: %v", err)
	}

	// Extended protocol: Parse once with nothing in the way, as a driver's
	// statement cache does, and Bind/Execute it again during the reshard.
	block(false)
	pg := conn.PgConn()
	if _, err := pg.Prepare(ctx, "ctas", "create table cached as select 1 as n", nil); err != nil {
		t.Fatalf("Parse with nothing in the way: %v", err)
	}
	block(true)
	res := pg.ExecPrepared(ctx, "ctas", nil, nil, nil).Read()
	if !refused(res.Err) {
		t.Fatalf("Bind/Execute of a cached CREATE TABLE AS during a reshard: %v, want 55000 naming the reshard", res.Err)
	}
}

// TestHomeDDLIsCheckedWhereAPortalRuns (PGS-975 review): checking where a
// statement is planned or bound left paths that execute without either.
// The check is made where a portal is executed, which every one of them
// passes through.
func TestHomeDDLIsCheckedWhereAPortalRuns(t *testing.T) {
	q := &fakeQueue{homeQueued: true}
	h := newDDLHarness(t, q)
	ctx := context.Background()
	w := discardWriter{}
	keys := &pgwire.SCRAMKeys{ClientKey: make([]byte, 32), ServerKey: make([]byte, 32)}
	var ids uint64
	session := func() *Executor {
		ids++
		return newExecutor(h.r, pgwire.SessionInfo{ID: ids, Database: "app", User: "app", Auth: &pgwire.AuthResult{SCRAM: keys}}, Shard{Set: DefaultShardSet, ID: 0})
	}
	block := func(on bool) {
		q.mu.Lock()
		defer q.mu.Unlock()
		q.home = nil
		if on {
			q.home = []catalog.Blocker{{Kind: catalog.OperationReshard, ID: "00000000-0000-0000-0000-00000000e975", Reason: catalog.BlockedByStarted}}
		}
	}
	refused := func(err error) bool {
		var pe *pgwire.Error
		return errors.As(err, &pe) && pe.Code == "55000"
	}
	step := func(e *Executor, name, sql string) error {
		t.Helper()
		if err := e.Parse(ctx, name, sql, nil, w); err != nil {
			return err
		}
		if err := e.Bind(ctx, name, name, nil, nil, nil, w); err != nil {
			return err
		}
		return e.Execute(ctx, name, 0, w)
	}

	// A PREPARE and the EXECUTE of it in one batch: the PREPARE is not
	// recorded until the batch's results are in, so the EXECUTE had
	// nothing to look up.
	block(true)
	e := session()
	if err := step(e, "a", "prepare p as select 1 as n into saved"); err != nil {
		t.Fatalf("PREPARE during a reshard: %v", err)
	}
	if err := step(e, "b", "execute p"); !refused(err) {
		t.Errorf("EXECUTE of a PREPARE earlier in the same batch: %v, want 55000", err)
	}

	// A protocol-prepared "EXECUTE p", cached from before the reshard: its
	// own plan is not home DDL, what it runs is.
	block(false)
	e = session()
	if err := e.SimpleQuery(ctx, "prepare p as select 1 as n into saved", w); err != nil {
		t.Fatal(err)
	}
	if err := e.Parse(ctx, "wrapper", "execute p", nil, w); err != nil {
		t.Fatal(err)
	}
	_ = e.Sync(ctx)
	block(true)
	if err := e.Bind(ctx, "wrapper", "wrapper", nil, nil, nil, w); err != nil && !refused(err) {
		t.Fatal(err)
	}
	if err := e.Execute(ctx, "wrapper", 0, w); !refused(err) {
		t.Errorf("a cached protocol EXECUTE of a prepared SELECT ... INTO during a reshard: %v, want 55000", err)
	}
	_ = e.Sync(ctx)

	// A portal bound inside a transaction before the reshard and executed
	// after it, across a Sync: nothing plans or binds it again.
	block(false)
	e = session()
	if err := e.SimpleQuery(ctx, "begin", w); err != nil {
		t.Fatal(err)
	}
	if err := e.Parse(ctx, "ctas", "create table x as select 1 as n", nil, w); err != nil {
		t.Fatal(err)
	}
	if err := e.Bind(ctx, "held", "ctas", nil, nil, nil, w); err != nil {
		t.Fatal(err)
	}
	_ = e.Sync(ctx)
	block(true)
	if err := e.Execute(ctx, "held", 0, w); !refused(err) {
		t.Errorf("a portal bound before the reshard and executed during it: %v, want 55000", err)
	}

	// A portal bound from the unnamed statement, which is then parsed
	// again: the portal still runs what it was bound to, so it is judged
	// by that, not by whatever the name holds now.
	block(false)
	e = session()
	if err := e.Parse(ctx, "", "create table y as select 1 as n", nil, w); err != nil {
		t.Fatal(err)
	}
	if err := e.Bind(ctx, "kept", "", nil, nil, nil, w); err != nil {
		t.Fatal(err)
	}
	if err := e.Parse(ctx, "", "select 1", nil, w); err != nil {
		t.Fatal(err)
	}
	block(true)
	if err := e.Execute(ctx, "kept", 0, w); !refused(err) {
		t.Errorf("a portal bound to a CREATE TABLE AS whose statement was parsed again: %v, want 55000", err)
	}

	// And the other way round: a portal bound to "EXECUTE p" runs whatever
	// p is when it runs, which is when PostgreSQL looks p up. Re-prepared
	// as a plain SELECT, it is not refused.
	block(false)
	e = session()
	for _, sql := range []string{"begin", "prepare p as select 1 as n into saved"} {
		if err := e.SimpleQuery(ctx, sql, w); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Parse(ctx, "wrap", "execute p", nil, w); err != nil {
		t.Fatal(err)
	}
	if err := e.Bind(ctx, "wrapped", "wrap", nil, nil, nil, w); err != nil {
		t.Fatal(err)
	}
	_ = e.Sync(ctx)
	for _, sql := range []string{"deallocate p", "prepare p as select 1"} {
		if err := e.SimpleQuery(ctx, sql, w); err != nil {
			t.Fatal(err)
		}
	}
	block(true)
	if err := e.Execute(ctx, "wrapped", 0, w); refused(err) {
		t.Errorf("a portal bound to EXECUTE p, with p re-prepared as a plain SELECT, was refused: %v", err)
	}
}

// TestExplainAnalyzeExecuteNamesWhatItRuns (PGS-975 review): EXPLAIN ANALYZE
// EXECUTE runs the prepared statement, so it has to name it as a bare
// EXECUTE does; plain EXPLAIN runs nothing and names nothing.
func TestExplainAnalyzeExecuteNamesWhatItRuns(t *testing.T) {
	q := &fakeQueue{homeQueued: true}
	h := newDDLHarness(t, q)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	if _, err := conn.Exec(ctx, "prepare into_saved as select 1 as n into saved"); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	q.home = []catalog.Blocker{{Kind: catalog.OperationReshard, ID: "00000000-0000-0000-0000-00000000e975", Reason: catalog.BlockedByStarted}}
	q.mu.Unlock()
	var pe *pgconn.PgError
	if _, err := conn.Exec(ctx, "explain analyze execute into_saved"); !errors.As(err, &pe) || pe.Code != "55000" {
		t.Fatalf("EXPLAIN ANALYZE EXECUTE of a prepared SELECT ... INTO during a reshard: %v, want 55000", err)
	}
	if _, err := conn.Exec(ctx, "explain execute into_saved"); errors.As(err, &pe) && pe.Code == "55000" {
		t.Fatalf("plain EXPLAIN EXECUTE was refused: %v; it runs nothing", err)
	}
}
