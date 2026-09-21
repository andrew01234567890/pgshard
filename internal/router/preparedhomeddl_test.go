package router

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/andrew01234567890/pgshard/internal/catalog"
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
