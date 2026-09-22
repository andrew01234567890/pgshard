package router

import (
	"context"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/jackc/pgx/v5/pgconn"
)

func atomicDDLConn(t *testing.T) *pgconn.PgConn {
	t.Helper()
	h := newDDLHarness(t, &fakeQueue{})
	app := h.snap.Databases["app"]
	app.DDLTransactions = catalog.DDLTransactionsAtomic
	h.snap.Databases["app"] = app
	return h.connect(t, h.dsn()).PgConn()
}

func mustExec(t *testing.T, pc *pgconn.PgConn, sql string) []*pgconn.Result {
	t.Helper()
	res, err := pc.Exec(context.Background(), sql).ReadAll()
	if err != nil {
		t.Fatalf("%q: %v", sql, err)
	}
	return res
}

func wantStatus(t *testing.T, pc *pgconn.PgConn, want byte, after string) {
	t.Helper()
	if got := pc.TxStatus(); got != want {
		t.Fatalf("status after %s = %c, want %c", after, got, want)
	}
}

// PostgreSQL fails a transaction on any error. A refusal the router answers
// itself has to as well, or the statements around it commit.
func TestARefusalFailsTheTransactionOnItsBackend(t *testing.T) {
	pc := atomicDDLConn(t)
	ctx := context.Background()
	mustExec(t, pc, "begin")
	mustExec(t, pc, "select 1")
	if _, err := pc.Exec(ctx, "create table zz (a int)").ReadAll(); sqlstate(err) != "0A000" {
		t.Fatalf("DDL in a transaction: %v, want the 0A000 refusal", err)
	}
	wantStatus(t, pc, 'E', "the refusal")
	if _, err := pc.Exec(ctx, "select 1").ReadAll(); sqlstate(err) != "25P02" {
		t.Fatalf("a statement after the refusal: %v, want 25P02", err)
	}
	res := mustExec(t, pc, "commit")
	if tag := res[0].CommandTag.String(); tag != "ROLLBACK" {
		t.Fatalf("COMMIT of the failed transaction answered %q, want ROLLBACK", tag)
	}
	wantStatus(t, pc, 'I', "the COMMIT")
}

func TestAnExtendedRefusalFailsTheTransaction(t *testing.T) {
	pc := atomicDDLConn(t)
	ctx := context.Background()
	mustExec(t, pc, "begin")
	mustExec(t, pc, "select 1")
	if rr := pc.ExecParams(ctx, "create table zz (a int)", nil, nil, nil, nil).Read(); sqlstate(rr.Err) != "0A000" {
		t.Fatalf("DDL in a transaction: %v, want the 0A000 refusal", rr.Err)
	}
	wantStatus(t, pc, 'E', "the refusal")
	if rr := pc.ExecParams(ctx, "select 1", nil, nil, nil, nil).Read(); sqlstate(rr.Err) != "25P02" {
		t.Fatalf("a statement after the refusal: %v, want 25P02", rr.Err)
	}
}

// With nothing on a shard yet there is no backend to fail, and the router
// answers for the failed transaction itself.
func TestARefusalBeforeAnyShardStatementFailsTheTransaction(t *testing.T) {
	pc := atomicDDLConn(t)
	ctx := context.Background()
	mustExec(t, pc, "begin")
	if _, err := pc.Exec(ctx, "create table zz (a int)").ReadAll(); sqlstate(err) != "0A000" {
		t.Fatalf("DDL in a transaction: %v", err)
	}
	wantStatus(t, pc, 'E', "the refusal")
	res := mustExec(t, pc, "commit")
	if tag := res[0].CommandTag.String(); tag != "ROLLBACK" {
		t.Fatalf("COMMIT answered %q, want ROLLBACK", tag)
	}
}

// A savepoint taken before the refusal recovers the transaction, as it does
// in PostgreSQL: that is why the backend is failed and not dropped.
func TestASavepointRecoversATransactionARefusalFailed(t *testing.T) {
	pc := atomicDDLConn(t)
	ctx := context.Background()
	mustExec(t, pc, "begin")
	mustExec(t, pc, "select 1")
	mustExec(t, pc, "savepoint s")
	if _, err := pc.Exec(ctx, "create table zz (a int)").ReadAll(); sqlstate(err) != "0A000" {
		t.Fatalf("DDL in a transaction: %v", err)
	}
	wantStatus(t, pc, 'E', "the refusal")
	mustExec(t, pc, "rollback to savepoint s")
	wantStatus(t, pc, 'T', "ROLLBACK TO")
	mustExec(t, pc, "select 1")
	res := mustExec(t, pc, "commit")
	if tag := res[0].CommandTag.String(); tag != "COMMIT" {
		t.Fatalf("COMMIT of the recovered transaction answered %q", tag)
	}
}

// Outside a transaction a refusal is only an error.
func TestARefusalOutsideATransactionLeavesTheSessionIdle(t *testing.T) {
	pc := atomicDDLConn(t)
	if _, err := pc.Exec(context.Background(), "select 1; create table zz (a int)").ReadAll(); sqlstate(err) != "0A000" {
		t.Fatalf("DDL in a batch: %v", err)
	}
	wantStatus(t, pc, 'I', "the refusal")
}

// A batch whose implicit COMMIT fails ends without a Sync or another simple
// query, and the next transaction the client opens must not inherit that
// error.
func TestAFailedImplicitCommitDoesNotFailTheNextTransaction(t *testing.T) {
	h := newHarness(t)
	pc := h.connect(t, h.dsn("app", "secret", "app")).PgConn()
	ctx := context.Background()
	h.fp.script("commit", script{err: "deferred constraint violated", code: "23505", once: true})
	if _, err := pc.Exec(ctx, "select 1; select 1").ReadAll(); sqlstate(err) != "23505" {
		t.Fatalf("batch: %v, want the commit's error", err)
	}
	wantStatus(t, pc, 'I', "the failed batch")
	if rr := pc.ExecParams(ctx, "begin", nil, nil, nil, nil).Read(); rr.Err != nil {
		t.Fatal(rr.Err)
	}
	wantStatus(t, pc, 'T', "the next BEGIN")
	if rr := pc.ExecParams(ctx, "select 1", nil, nil, nil, nil).Read(); rr.Err != nil {
		t.Fatalf("a statement of the new transaction: %v", rr.Err)
	}
}

// A batch whose BEGIN adopted its transaction leaves it the client's, so a
// refusal after that fails it rather than rolling it back.
func TestARefusalAfterAnAdoptingBeginInABatchFailsTheTransaction(t *testing.T) {
	pc := atomicDDLConn(t)
	ctx := context.Background()
	if _, err := pc.Exec(ctx, "begin; select 1; create table zz (a int)").ReadAll(); sqlstate(err) != "0A000" {
		t.Fatalf("batch: %v, want the DDL refusal", err)
	}
	wantStatus(t, pc, 'E', "the batch")
	res := mustExec(t, pc, "commit")
	if tag := res[0].CommandTag.String(); tag != "ROLLBACK" {
		t.Fatalf("COMMIT answered %q, want ROLLBACK", tag)
	}
}
