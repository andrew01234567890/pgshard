package router

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

// extendedSession drives raw extended-protocol messages and reports, for
// each Sync, the first error, the command tags and the ReadyForQuery status.
type extendedSession struct {
	t  *testing.T
	fe *pgproto3.Frontend
	// seen names every backend message of the last batch, in order. The
	// command tags alone cannot see a MISSING message, and a batch the
	// router answers itself owes the client the same ParseComplete and
	// BindComplete a pooler would have relayed -- leave one out and the
	// driver reads the next statement's replies one message out of step.
	seen []string
}

func newExtendedSession(t *testing.T, dsn string) *extendedSession {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	hj, err := conn.PgConn().Hijack()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hj.Conn.Close() })
	return &extendedSession{t: t, fe: hj.Frontend}
}

func (s *extendedSession) send(label string, msgs ...pgproto3.FrontendMessage) (code string, tags []string, status byte) {
	s.t.Helper()
	for _, m := range msgs {
		s.fe.Send(m)
	}
	if err := s.fe.Flush(); err != nil {
		s.t.Fatalf("%s: %v", label, err)
	}
	s.seen = nil
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		msg, err := s.fe.Receive()
		if err != nil {
			s.t.Fatalf("%s: %v", label, err)
		}
		s.seen = append(s.seen, strings.TrimPrefix(fmt.Sprintf("%T", msg), "*pgproto3."))
		switch m := msg.(type) {
		case *pgproto3.ErrorResponse:
			if code == "" {
				code = m.Code
			}
		case *pgproto3.CommandComplete:
			tags = append(tags, string(m.CommandTag))
		case *pgproto3.ReadyForQuery:
			return code, tags, m.TxStatus
		}
	}
	s.t.Fatalf("%s: no ReadyForQuery", label)
	return "", nil, 0
}

// extended runs one unnamed statement as its own batch.
func (s *extendedSession) extended(label, sql string) (string, []string, byte) {
	s.t.Helper()
	return s.send(label, &pgproto3.Parse{Query: sql}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
}

// TestAKilledTransactionIsRefusedOverTheExtendedProtocol (PGS-957): the
// failed-transaction state was enforced for the SIMPLE protocol only.
// endFailedTxn, endNoTxn and refuseInFailedTransaction were reachable from
// simpleQuery and from nowhere else, so Parse/Bind/Execute -- pgx's default,
// JDBC, psycopg3, every prepared-statement client -- walked past all three.
//
// Measured on origin/main, raw pgproto3, a mid-transaction failover:
//
//	BEGIN                     -> RFQ T
//	EXT insert (8801)         -> INSERT 0 1, RFQ T
//	<primary fails over>
//	EXT insert (8802)         -> 40001, RFQ E
//	EXT select 1              -> SELECT 1, RFQ I
//	EXT commit                -> tag COMMIT, RFQ I
//	shards ran: [insert ... (8801), commit]
//
// The COMMIT ran on a FRESH backend that had never heard of the transaction
// and answered a COMMIT tag. Row 8801 died with the old primary and the
// client was told it had committed. TestAKilledTransactionCannotBeCommitted
// ByAccident is the contrast: it drives the same sequence over the simple
// protocol, where the guard has always been.
//
// Two further things the first statement after the kill did: it acquired a
// new stream, and pump relays that backend's ReadyForQuery, so e.tx was
// overwritten with Idle -- the session stopped even reporting a failed
// transaction, which is what the 'I' above is.
func TestAKilledTransactionIsRefusedOverTheExtendedProtocol(t *testing.T) {
	t.Run("EverySubsequentStatement", func(t *testing.T) {
		h := newShardedHarness(t)
		s := newExtendedSession(t, h.dsn())
		s.send("begin", &pgproto3.Query{String: "begin"})
		if _, _, st := s.extended("the write before the failover", "insert into items (id) values (8801)"); st != 'T' {
			t.Fatalf("RFQ %c before the failover, want T", st)
		}
		serving := h.fence("fenced")
		if code, _, _ := s.extended("the killed write", "insert into items (id) values (8802)"); code != "40001" {
			t.Fatalf("the killed statement reported %q, want 40001", code)
		}
		h.setSnap(serving)

		code, _, status := s.extended("a statement after the kill", "select 1")
		if code != "25P02" {
			t.Errorf("a statement after the transaction was killed reported %q, want 25P02: over the extended protocol it RAN, and the client is being told its transaction is still usable", code)
		}
		if status != 'E' {
			t.Errorf("ReadyForQuery reported %c, want E: the session has stopped reporting the failed transaction at all", status)
		}

		// The one that loses data. A COMMIT here must not say the
		// transaction committed.
		code, tags, status := s.extended("commit", "commit")
		// The whole sequence, not only the tag: this batch is answered by
		// the router, so the completions a pooler would have relayed have
		// to be written here or the driver desynchronises.
		if got := strings.Join(s.seen, ","); got != "ParseComplete,BindComplete,CommandComplete,ReadyForQuery" {
			t.Errorf("COMMIT answered %q; a router-answered batch still owes ParseComplete and BindComplete", got)
		}
		if code != "" {
			t.Fatalf("COMMIT of a killed transaction errored with %q; it must end the transaction", code)
		}
		if len(tags) != 1 || tags[0] != "ROLLBACK" {
			t.Errorf("COMMIT answered %v, want [ROLLBACK]: PostgreSQL answers ROLLBACK for a commit in a failed transaction, and anything else tells the client its writes landed", tags)
		}
		if status != 'I' {
			t.Errorf("ReadyForQuery reported %c after the COMMIT, want I", status)
		}
		for i := range h.poolers {
			for _, q := range h.poolers[i].ran() {
				if strings.ToLower(strings.TrimSpace(q)) == "commit" {
					t.Errorf("shard %d was sent a COMMIT for the killed transaction: it runs on a fresh backend that never saw the transaction's writes and answers success", i)
				}
			}
		}
	})

	// The half a check at Parse alone would miss. A client that prepared
	// the statement BEFORE the transaction failed sends no Parse in the
	// failing batch at all -- which is exactly what a prepared-statement
	// driver does on its second and every later execution. PostgreSQL
	// checks at Parse, Bind AND Execute for this reason (postgres.c).
	t.Run("APortalOfAStatementPreparedEarlier", func(t *testing.T) {
		h := newShardedHarness(t)
		s := newExtendedSession(t, h.dsn())
		if code, _, _ := s.send("prepare", &pgproto3.Parse{Name: "ins", Query: "insert into items (id) values (8804)"}, &pgproto3.Sync{}); code != "" {
			t.Fatalf("preparing the statement reported %q", code)
		}
		s.send("begin", &pgproto3.Query{String: "begin"})
		if _, _, st := s.extended("the write before the failover", "insert into items (id) values (8803)"); st != 'T' {
			t.Fatalf("RFQ %c before the failover, want T", st)
		}
		serving := h.fence("fenced")
		if code, _, _ := s.extended("the killed write", "insert into items (id) values (8805)"); code != "40001" {
			t.Fatalf("the killed statement reported %q, want 40001", code)
		}
		h.setSnap(serving)

		code, _, status := s.send("bind and execute the earlier statement",
			&pgproto3.Bind{DestinationPortal: "p", PreparedStatement: "ins"},
			&pgproto3.Execute{Portal: "p"}, &pgproto3.Sync{})
		if code != "25P02" {
			t.Errorf("Bind/Execute of a statement prepared before the failure reported %q, want 25P02: no Parse reaches the router in this batch, so a check at Parse alone lets the write through", code)
		}
		if status != 'E' {
			t.Errorf("ReadyForQuery reported %c, want E", status)
		}
		for i := range h.poolers {
			for _, q := range h.poolers[i].ran() {
				if strings.Contains(q, "8804") {
					t.Errorf("shard %d ran the write of the killed transaction", i)
				}
			}
		}
	})

	// Neither of these carries a Parse, Bind or Execute for the checks
	// above to see, and each one on its own reopened the entire defect:
	// the batch acquired a backend, pump relayed ITS ReadyForQuery, and the
	// session went back to reporting a live transaction -- after which the
	// COMMIT answered COMMIT again. Measured, both of them, which is why
	// the rule is "nothing reaches a backend" and not a list of messages.
	t.Run("ADescribeOnlyBatch", func(t *testing.T) {
		h, s := killedExtendedSession(t)
		code, _, status := s.send("describe only", &pgproto3.Describe{ObjectType: 'S', Name: "sel"}, &pgproto3.Sync{})
		if code != "25P02" {
			t.Errorf("a Describe in a failed transaction reported %q, want 25P02", code)
		}
		if status != 'E' {
			t.Errorf("ReadyForQuery reported %c after a Describe, want E: the failed transaction has been forgotten", status)
		}
		assertCommitRollsBack(t, h, s)
	})

	// Close is answered rather than refused: it needs no backend, and a
	// driver clearing its statement cache on the way to the ROLLBACK it is
	// about to send must not be turned away.
	t.Run("ACloseOnlyBatch", func(t *testing.T) {
		h, s := killedExtendedSession(t)
		code, _, status := s.send("close only", &pgproto3.Close{ObjectType: 'S', Name: "sel"}, &pgproto3.Sync{})
		if code != "" {
			t.Errorf("Close in a failed transaction reported %q; it needs no backend and must be answered", code)
		}
		if got := strings.Join(s.seen, ","); got != "CloseComplete,ReadyForQuery" {
			t.Errorf("Close answered %q, want \"CloseComplete,ReadyForQuery\"", got)
		}
		if status != 'E' {
			t.Errorf("ReadyForQuery reported %c after a Close, want E: the failed transaction has been forgotten", status)
		}
		assertCommitRollsBack(t, h, s)
	})
}

// killedExtendedSession returns a session whose transaction a failover has
// killed, with a row-returning statement already prepared under the name
// "sel" -- prepared BEFORE the failure, as a driver's cache would be.
func killedExtendedSession(t *testing.T) (*shardedHarness, *extendedSession) {
	t.Helper()
	h := newShardedHarness(t)
	s := newExtendedSession(t, h.dsn())
	if code, _, _ := s.send("prepare", &pgproto3.Parse{Name: "sel", Query: "select id from items where id = 9901"}, &pgproto3.Sync{}); code != "" {
		t.Fatalf("preparing reported %q", code)
	}
	s.send("begin", &pgproto3.Query{String: "begin"})
	if _, _, st := s.extended("the write before the failover", "insert into items (id) values (9901)"); st != 'T' {
		t.Fatalf("RFQ %c before the failover, want T", st)
	}
	serving := h.fence("fenced")
	if code, _, _ := s.extended("the killed write", "insert into items (id) values (9902)"); code != "40001" {
		t.Fatalf("the killed statement reported %q, want 40001", code)
	}
	h.setSnap(serving)
	return h, s
}

// assertCommitRollsBack is the consequence every one of these subtests is
// really about: whatever the client sent first, the COMMIT that follows
// must not claim the transaction committed.
func assertCommitRollsBack(t *testing.T, h *shardedHarness, s *extendedSession) {
	t.Helper()
	_, tags, _ := s.extended("commit", "commit")
	if got := strings.Join(s.seen, ","); got != "ParseComplete,BindComplete,CommandComplete,ReadyForQuery" {
		t.Errorf("COMMIT answered %q; a router-answered batch still owes ParseComplete and BindComplete", got)
	}
	if len(tags) != 1 || tags[0] != "ROLLBACK" {
		t.Errorf("COMMIT answered %v, want [ROLLBACK]: the client is being told writes landed that died with the old primary", tags)
	}
	for i := range h.poolers {
		for _, q := range h.poolers[i].ran() {
			if strings.ToLower(strings.TrimSpace(q)) == "commit" {
				t.Errorf("shard %d was sent a COMMIT for the killed transaction", i)
			}
		}
	}
}

// TestARealDriverRecoversFromAKilledTransaction (PGS-957): the raw-protocol
// tests above assert what the router sends; this asserts that a driver can
// live with it. A batch the router answers itself still owes the client the
// ParseComplete and BindComplete a pooler would have relayed, and a missing
// one desynchronises the connection rather than erroring -- invisible to a
// test that reads the messages it expects and ignores the rest.
//
// pgx is driven in its describe-and-prepare mode here, which is what makes
// this the extended protocol: a query with no arguments is sent as a simple
// one whatever the mode says.
func TestARealDriverRecoversFromAKilledTransaction(t *testing.T) {
	h := newShardedHarness(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, h.dsn())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	if _, err := conn.Exec(ctx, "begin"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "insert into items (id) values ($1)", 8811); err != nil {
		t.Fatalf("the write before the failover: %v", err)
	}
	serving := h.fence("fenced")
	if _, err := conn.Exec(ctx, "insert into items (id) values ($1)", 8812); sqlstate(err) != "40001" {
		t.Fatalf("the killed statement reported %v, want 40001", err)
	}
	h.setSnap(serving)

	if _, err := conn.Exec(ctx, "insert into items (id) values ($1)", 8813); sqlstate(err) != "25P02" {
		t.Fatalf("a write after the transaction was killed reported %v, want 25P02", err)
	}
	tag, err := conn.Exec(ctx, "commit")
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if tag.String() != "ROLLBACK" {
		t.Fatalf("COMMIT answered %q, want ROLLBACK", tag.String())
	}
	// The connection is still usable, which is what a missing completion
	// would have cost: the driver reads the next statement's answers
	// against the wrong message and the session is lost.
	if _, err := conn.Exec(ctx, "insert into items (id) values ($1)", 8814); err != nil {
		t.Fatalf("the session after the recovery: %v", err)
	}
}

// TestTheFailedStateSurvivesTheWaysAroundIt (PGS-957): three routes that
// each restored the whole defect on their own, found by an adversarial
// audit of the first version of this fix. Every one ends the same way --
// the client's COMMIT answers COMMIT on a fresh backend -- so each subtest
// finishes by sending one.
func TestTheFailedStateSurvivesTheWaysAroundIt(t *testing.T) {
	// A Parse is not an Execute. Deciding "this batch ends the
	// transaction" from the statement's presence let a driver's bare
	// Parse of a COMMIT -- which PostgreSQL answers in an aborted
	// transaction, and which pgConn.Prepare sends on its own -- end the
	// transaction before the client had executed anything. The session
	// then read Idle and the client's real COMMIT was routed.
	t.Run("APreparedCommitDoesNotEndTheTransaction", func(t *testing.T) {
		h, s := killedExtendedSession(t)
		code, _, status := s.send("prepare a commit", &pgproto3.Parse{Name: "cmt", Query: "commit"}, &pgproto3.Sync{})
		if code != "" {
			t.Fatalf("preparing a COMMIT in a failed transaction reported %q; PostgreSQL admits it", code)
		}
		if status != 'E' {
			t.Errorf("ReadyForQuery reported %c after merely PREPARING a commit, want E: the transaction ended without the client executing anything", status)
		}
		_, tags, status := s.send("execute it",
			&pgproto3.Bind{DestinationPortal: "p", PreparedStatement: "cmt"},
			&pgproto3.Execute{Portal: "p"}, &pgproto3.Sync{})
		if len(tags) != 1 || tags[0] != "ROLLBACK" {
			t.Errorf("the COMMIT answered %v, want [ROLLBACK]", tags)
		}
		if status != 'I' {
			t.Errorf("ReadyForQuery reported %c after the COMMIT, want I", status)
		}
		for i := range h.poolers {
			for _, q := range h.poolers[i].ran() {
				if strings.ToLower(strings.TrimSpace(q)) == "commit" {
					t.Errorf("shard %d was sent a COMMIT for the killed transaction", i)
				}
			}
		}
	})

	// Flush is the OTHER place a staged batch reaches a pooler, and the
	// first version of this fix guarded only Sync. A Describe terminated
	// by Flush stages no Execute, so flush's own decline list passed it
	// through to acquire, and the fresh backend's relayed ReadyForQuery
	// cleared the failed state before the client's Sync was read.
	//
	// Refused rather than declined silently: a pipelined client BLOCKS on
	// its Flush, so answering nothing would hang it.
	t.Run("AFlushDoesNotReachABackend", func(t *testing.T) {
		h, s := killedExtendedSession(t)
		s.fe.Send(&pgproto3.Describe{ObjectType: 'S', Name: "sel"})
		s.fe.Send(&pgproto3.Flush{})
		if err := s.fe.Flush(); err != nil {
			t.Fatal(err)
		}
		code, _, status := s.send("sync after the flush", &pgproto3.Sync{})
		if code != "25P02" {
			t.Errorf("a Describe terminated by Flush reported %q, want 25P02: it reached a backend and cleared the failed state", code)
		}
		if status != 'E' {
			t.Errorf("ReadyForQuery reported %c, want E", status)
		}
		assertCommitRollsBack(t, h, s)
	})

	// EXPLAIN (pgshard) is answered in a failed transaction on purpose --
	// refuseSelfAnsweredInFailedTxn says so: it is how a user sees why the
	// statement that failed was routed the way it was, without first
	// losing the transaction. simpleQuery answers it before any of these
	// checks and the extended protocol must not disagree; the first
	// version of this fix refused it with 25P02.
	t.Run("ExplainIsStillAnswered", func(t *testing.T) {
		_, s := killedExtendedSession(t)
		code, tags, status := s.extended("explain", "explain (pgshard) select id from items where id = 9901")
		if code != "" {
			t.Errorf("EXPLAIN (pgshard) reported %q; it touches no shard and is deliberately available here", code)
		}
		if len(tags) != 1 || tags[0] != "EXPLAIN" {
			t.Errorf("EXPLAIN answered %v, want [EXPLAIN]", tags)
		}
		if status != 'E' {
			t.Errorf("ReadyForQuery reported %c, want E: EXPLAIN must not end the transaction", status)
		}
	})
}
