//go:build integration

package router

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// TestFlushIsAnsweredByTheRealStack sends an extended-protocol batch and a
// Flush, with no Sync, through a real router, a real pooler and real
// PostgreSQL.
//
// The router buffers Parse, Describe and Execute until it has a reason to
// dispatch them, and a driver that sends a batch and then waits on Flush
// hangs for ever if that reason is only Sync. The unit test for this drives
// a fake pooler that answers Describe and Execute synchronously, so it
// cannot show that Flush crosses the pooler boundary -- which is where the
// buffering is.
func TestFlushIsAnsweredByTheRealStack(t *testing.T) {
	s := startShardedStack(t)
	ctx := context.Background()
	setup := s.connect(t)
	s.awaitSharded(t, setup)
	tenant, _ := twoTenants(t)
	if _, err := setup.Exec(ctx, `INSERT INTO orders (tenant_id, id) VALUES ($1, $2)`, tenant, 900001); err != nil {
		t.Fatal(err)
	}
	_ = setup.Close(ctx)

	conn := s.connect(t)
	hj, err := conn.PgConn().Hijack()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hj.Conn.Close() })
	fe := hj.Frontend

	for _, m := range []pgproto3.FrontendMessage{
		&pgproto3.Parse{Query: fmt.Sprintf("select id from orders where tenant_id = %d", tenant)},
		&pgproto3.Bind{},
		&pgproto3.Describe{ObjectType: 'P'},
		&pgproto3.Execute{},
		&pgproto3.Flush{},
	} {
		fe.Send(m)
	}
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}

	// No Sync was sent, so nothing here may wait for one. Everything the
	// batch produces must arrive on the Flush alone.
	type answer struct {
		kinds []string
		err   error
	}
	// What arrived is recorded as it arrives, so a timeout can say what the
	// client did get rather than only that it did not get everything: a
	// batch that errors answers with the error and then waits for Sync,
	// which looks identical from the outside to one that answered nothing.
	var mu sync.Mutex
	var seen []string
	done := make(chan answer, 1)
	go func() {
		for {
			msg, err := fe.Receive()
			mu.Lock()
			kinds := append([]string(nil), seen...)
			if err == nil {
				seen = append(seen, fmt.Sprintf("%T", msg))
				kinds = append(kinds, fmt.Sprintf("%T", msg))
			}
			mu.Unlock()
			if err != nil {
				done <- answer{kinds, err}
				return
			}
			if _, ok := msg.(*pgproto3.CommandComplete); ok {
				done <- answer{kinds, nil}
				return
			}
		}
	}()

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("reading the answers to a Flush: %v (got %v)", got.err, got.kinds)
		}
		want := []string{"*pgproto3.ParseComplete", "*pgproto3.BindComplete", "*pgproto3.RowDescription", "*pgproto3.DataRow", "*pgproto3.CommandComplete"}
		if fmt.Sprint(got.kinds) != fmt.Sprint(want) {
			t.Fatalf("Flush answered with %v, want %v", got.kinds, want)
		}
	case <-time.After(30 * time.Second):
		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("a batch followed by Flush never completed; the client received %v and waits for a Sync it will never send", seen)
	}

	// The connection is still usable afterwards: a Flush ends nothing.
	fe.Send(&pgproto3.Sync{})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	for {
		msg, err := fe.Receive()
		if err != nil {
			t.Fatalf("after Sync: %v", err)
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
}
