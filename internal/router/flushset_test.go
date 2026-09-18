package router

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

// TestAFlushAnswersASet pins what docs/router.md says about which batches
// a Flush answers. The entry used to list "a session-effect statement such
// as SET" among the shapes that hang; a SET is classified SetGUC, and the
// only session-effect kinds Executor.flush declines are an SQL-level
// PREPARE and a DISCARD ALL. So a SET was always answered, and a client
// was being told to avoid pipelining for no reason.
//
// This is a regression guard on the documented behaviour, not a fix: it
// passes today. It fails if the decline list ever widens to cover SetGUC.
func TestAFlushAnswersASet(t *testing.T) {
	h := newShardedHarness(t)
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
		&pgproto3.Parse{Query: "set application_name = 'flushed'"}, &pgproto3.Bind{},
		&pgproto3.Execute{}, &pgproto3.Flush{},
	} {
		fe.Send(m)
	}
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		for {
			msg, err := fe.Receive()
			if err != nil {
				done <- err
				return
			}
			switch m := msg.(type) {
			case *pgproto3.CommandComplete:
				done <- nil
				return
			case *pgproto3.ErrorResponse:
				done <- fmt.Errorf("%s %s", m.Code, m.Message)
				return
			case *pgproto3.ReadyForQuery:
				done <- errors.New("a Flush must not produce ReadyForQuery")
				return
			}
		}
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Execute then Flush on a SET: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the client is still waiting after flushing a SET; docs/router.md says it is answered")
	}
}
