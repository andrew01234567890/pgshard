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

// TestASimpleQueryDuringABufferedBatchIsRefused (PGS-883): the extended
// batch is staged until its Sync, and a simple Query runs at once. So a
// client that sends Parse/Bind/Execute and then a Query, with no Sync in
// between, had the Query run FIRST and its own earlier statement run
// afterwards.
//
// Measured before the fix, with an INSERT buffered and "select 1" sent
// after it:
//
//	shard 0 ran: [begin select 1 ...]
//	shard 3 ran: [begin ... insert into orders ...]
//
// The select ran; the insert followed at the Sync. Replace the select with
// a CREATE TABLE and the buffered INSERT runs inside the transaction after
// the DDL, which is not the order the client wrote.
//
// Refused rather than flushed first: running a staged batch early is what
// PGS-911 could not make safe, because the router tracks transaction state
// from the relayed ReadyForQuery and a Flush does not produce one. An
// error the client can see beats silent misordering.
func TestASimpleQueryDuringABufferedBatchIsRefused(t *testing.T) {
	h := newDDLHarness(t, &fakeQueue{})
	tenant, _ := h.twoTenants(t)
	conn, err := pgx.Connect(context.Background(), h.dsn())
	if err != nil {
		t.Fatal(err)
	}
	hj, err := conn.PgConn().Hijack()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hj.Conn.Close() })
	fe := hj.Frontend

	for _, m := range []pgproto3.FrontendMessage{
		&pgproto3.Parse{Query: fmt.Sprintf("insert into orders (tenant_id, id) values (%d, 8831)", tenant)},
		&pgproto3.Bind{}, &pgproto3.Execute{},
		&pgproto3.Query{String: "select 1"},
	} {
		fe.Send(m)
	}
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}

	got := make(chan *pgproto3.ErrorResponse, 1)
	go func() {
		for {
			msg, err := fe.Receive()
			if err != nil {
				close(got)
				return
			}
			if e, ok := msg.(*pgproto3.ErrorResponse); ok {
				got <- e
				return
			}
			if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
				close(got)
				return
			}
		}
	}()
	select {
	case e := <-got:
		if e == nil {
			t.Fatal("the simple query ran while an extended batch was buffered, so it executed before statements the client sent first")
		}
		if e.Code != "0A000" {
			t.Errorf("refused with %s, want 0A000", e.Code)
		}
		for _, want := range []string{"buffered", "Sync"} {
			if !strings.Contains(e.Message+" "+e.Hint, want) {
				t.Errorf("the refusal does not mention %q, so the client is not told how to send this correctly: %s / %s", want, e.Message, e.Hint)
			}
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no answer to the simple query")
	}
}
