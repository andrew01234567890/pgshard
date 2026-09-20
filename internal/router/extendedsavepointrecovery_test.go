package router

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestRollbackToSavepointRecoversAFailedTransactionOverTheExtendedProtocol
// (PGS-958): TestRollbackToSavepointRecoversAFailedSequentialDDLTransaction
// over Parse/Bind/Execute. PGS-957 made the extended protocol enforce the
// failed state and refused ROLLBACK TO with everything else there, so psql
// recovered the transaction and pgx, JDBC and psycopg3 could not.
// PostgreSQL exempts it at Parse, Bind and Execute alike (postgres.c,
// IsTransactionExitStmt covers TRANS_STMT_ROLLBACK_TO).
func TestRollbackToSavepointRecoversAFailedTransactionOverTheExtendedProtocol(t *testing.T) {
	newH := func(t *testing.T) *shardedHarness {
		t.Helper()
		q := &fakeQueue{outcome: func(m catalog.DDLMigration) catalog.DDLMigration {
			m.State, m.Error = catalog.MigrationFailed, "relation already exists"
			return m
		}}
		h := newDDLHarness(t, q)
		app := h.snap.Databases["app"]
		app.DDLTransactions = catalog.DDLTransactionsSequential
		h.snap.Databases["app"] = app
		return h
	}
	failAfterSavepoint := func(t *testing.T, s *extendedSession) {
		t.Helper()
		for _, sql := range []string{"begin", "savepoint sp"} {
			if code, _, _ := s.extended(sql, sql); code != "" {
				t.Fatalf("%s: %s", sql, code)
			}
		}
		if code, _, st := s.extended("ddl", "create table t9 (id int primary key)"); code == "" || st != 'E' {
			t.Fatalf("the failing migration answered %q with status %c, so this test never reaches the state it is about", code, st)
		}
	}
	rollbackTo := func(name string) []pgproto3.FrontendMessage {
		return []pgproto3.FrontendMessage{&pgproto3.Parse{Query: "rollback to savepoint " + name}, &pgproto3.Bind{},
			&pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{}}
	}

	t.Run("TheTransactionIsUsableAgain", func(t *testing.T) {
		h := newH(t)
		s := newExtendedSession(t, h.dsn())
		failAfterSavepoint(t, s)
		code, tags, st := s.send("rollback to", rollbackTo("sp")...)
		if code != "" {
			t.Fatalf("ROLLBACK TO SAVEPOINT over the extended protocol answered %s; PostgreSQL answers it, and it is the client's only way to keep the transaction", code)
		}
		if st != 'T' {
			t.Fatalf("ReadyForQuery reported %c after the recovery, want T", st)
		}
		if want := []string{"ParseComplete", "BindComplete", "NoData", "CommandComplete", "ReadyForQuery"}; !slices.Equal(s.seen, want) {
			t.Fatalf("the recovery answered %v, want %v", s.seen, want)
		}
		if !slices.Equal(tags, []string{"ROLLBACK"}) {
			t.Fatalf("tags %v, want [ROLLBACK], PostgreSQL's tag for ROLLBACK TO", tags)
		}
		if code, _, _ := s.extended("insert", "insert into items (id) values (9581)"); code != "" {
			t.Fatalf("a write in the recovered transaction: %s", code)
		}
		if _, tags, st := s.extended("commit", "commit"); !slices.Equal(tags, []string{"COMMIT"}) || st != 'I' {
			t.Fatalf("COMMIT answered %v with status %c, want [COMMIT] and I", tags, st)
		}
		var ranRollbackTo, ranInsert bool
		for i := range h.poolers {
			for _, q := range h.poolers[i].ran() {
				q = strings.ToLower(q)
				ranRollbackTo = ranRollbackTo || strings.Contains(q, "rollback to savepoint sp")
				ranInsert = ranInsert || strings.Contains(q, "insert into items")
			}
		}
		if !ranRollbackTo || !ranInsert {
			t.Errorf("shards ran ROLLBACK TO %v and the write %v, want both: the recovered transaction must be real on a backend", ranRollbackTo, ranInsert)
		}
	})

	// The same recovery with the ROLLBACK TO carried by a NAMED statement.
	// The recovery replays the session's prepared statements onto the
	// backend it acquires; the batch then parses its own named statement
	// as it is forwarded, and PostgreSQL refuses a second Parse of a live
	// name with 42P05 -- so a replay that did not exclude the batch's own
	// names failed the very recovery the client sent.
	t.Run("ANamedStatementIsNotParsedTwice", func(t *testing.T) {
		h := newH(t)
		s := newExtendedSession(t, h.dsn())
		failAfterSavepoint(t, s)
		code, _, st := s.send("named rollback to",
			&pgproto3.Parse{Name: "rb", Query: "rollback to savepoint sp"},
			&pgproto3.Bind{DestinationPortal: "rb", PreparedStatement: "rb"},
			&pgproto3.Execute{Portal: "rb"}, &pgproto3.Sync{})
		if code != "" {
			t.Fatalf("a named ROLLBACK TO answered %s; the recovery must not parse the batch's own statement", code)
		}
		if st != 'T' {
			t.Fatalf("ReadyForQuery reported %c after the recovery, want T", st)
		}
	})

	t.Run("ARealDriverRecoversIt", func(t *testing.T) {
		h := newH(t)
		ctx := context.Background()
		conn := h.connect(t, h.dsn())
		for _, sql := range []string{"begin", "savepoint sp"} {
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
		}
		if _, err := conn.Exec(ctx, "create table t10 (id int primary key)"); err == nil {
			t.Fatal("the failing migration answered success")
		}
		// ExecParams, not Exec: pgx sends a statement without arguments
		// over the simple protocol whatever the configured mode says.
		if _, err := conn.PgConn().ExecParams(ctx, "rollback to savepoint sp", nil, nil, nil, nil).Close(); err != nil {
			t.Fatalf("ROLLBACK TO SAVEPOINT through pgconn's extended protocol: %v", err)
		}
		if st := conn.PgConn().TxStatus(); st != 'T' {
			t.Fatalf("ReadyForQuery reported %c after the recovery, want T", st)
		}
	})

	// The contrasts, each the extended twin of one in the simple-protocol
	// test: a transaction that lost work on a shard and a savepoint the
	// router has no record of both stay failed.
	t.Run("AKilledTransactionIsStillRefused", func(t *testing.T) {
		h := newH(t)
		s := newExtendedSession(t, h.dsn())
		for _, sql := range []string{"begin", "savepoint sp", "insert into items (id) values (9582)"} {
			if code, _, _ := s.extended(sql, sql); code != "" {
				t.Fatalf("%s: %s", sql, code)
			}
		}
		serving := h.fence("fenced")
		if code, _, _ := s.extended("killed", "insert into items (id) values (9583)"); code != "40001" {
			t.Fatalf("the killed statement reported %q, want 40001", code)
		}
		h.setSnap(serving)
		if code, _, st := s.send("rollback to", rollbackTo("sp")...); code != "25P02" || st != 'E' {
			t.Fatalf("ROLLBACK TO SAVEPOINT reported %q with status %c, want 25P02 and E: recovering it would tell the client the rows before the savepoint are still there", code, st)
		}
	})

	t.Run("AnUnknownSavepointIsStillRefused", func(t *testing.T) {
		h := newH(t)
		s := newExtendedSession(t, h.dsn())
		failAfterSavepoint(t, s)
		if code, _, st := s.send("rollback to", rollbackTo("other")...); code != "25P02" || st != 'E' {
			t.Fatalf("ROLLBACK TO an unset savepoint reported %q with status %c, want 25P02 and E", code, st)
		}
	})

	// A Describe of the ROLLBACK TO is admitted only because the Execute
	// after it reopens the transaction and a backend answers it. Without
	// the Execute there is neither, and the answer is the refusal.
	t.Run("ADescribeAloneDoesNotReopenIt", func(t *testing.T) {
		h := newH(t)
		s := newExtendedSession(t, h.dsn())
		failAfterSavepoint(t, s)
		code, _, st := s.send("describe only", &pgproto3.Parse{Query: "rollback to savepoint sp"}, &pgproto3.Bind{},
			&pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Sync{})
		if code != "25P02" || st != 'E' {
			t.Fatalf("a described, unexecuted ROLLBACK TO answered %q with status %c, want 25P02 and E", code, st)
		}
	})

	// PostgreSQL ends a failed transaction on the COMMIT, so a ROLLBACK TO
	// executed after it in the same batch has no transaction to recover.
	t.Run("ARollbackToAfterTheCommitIsRefused", func(t *testing.T) {
		h := newH(t)
		s := newExtendedSession(t, h.dsn())
		failAfterSavepoint(t, s)
		before := h.poolers[0].ran()
		code, _, _ := s.send("commit then rollback to",
			&pgproto3.Parse{Name: "c", Query: "commit"}, &pgproto3.Bind{DestinationPortal: "c", PreparedStatement: "c"},
			&pgproto3.Parse{Name: "r", Query: "rollback to savepoint sp"}, &pgproto3.Bind{DestinationPortal: "r", PreparedStatement: "r"},
			&pgproto3.Execute{Portal: "c"}, &pgproto3.Execute{Portal: "r"}, &pgproto3.Sync{})
		if code != "25P02" {
			t.Fatalf("a ROLLBACK TO after the COMMIT that ended the transaction answered %q, want 25P02", code)
		}
		// The refusal alone is not the claim: reopening the transaction
		// first and failing on the backend afterwards also errors. What
		// must not happen is a backend at all -- the replay would have
		// committed the prelude on the COMMIT.
		if after := h.poolers[0].ran(); !slices.Equal(after, before) {
			t.Fatalf("the shard ran %q for a transaction the COMMIT had already ended", after[len(before):])
		}
	})
}
