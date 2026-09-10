//go:build integration

package router

import (
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// TestBindCompleteComesFirstThroughTheRealStack drives the sequence lib/pq
// sends -- Parse and Describe on their own, then Bind and Execute with no
// Describe between them -- against a real router, pooler and PostgreSQL.
//
// The unit test for this runs against the fake executor, whose rows never
// travel the pooler path that fills the row slab. The ordering bug was
// between the slab and the backend buffer, so the rows have to be real
// ones.
func TestBindCompleteComesFirstThroughTheRealStack(t *testing.T) {
	s := startStack(t)
	conn := s.connect(t)
	hj, err := conn.PgConn().Hijack()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hj.Conn.Close() })
	fe := hj.Frontend

	drain := func() []string {
		_ = hj.Conn.SetReadDeadline(time.Now().Add(15 * time.Second))
		var got []string
		for {
			m, err := fe.Receive()
			if err != nil {
				t.Fatalf("receive: %v (got %v)", err, got)
			}
			got = append(got, fmt.Sprintf("%T", m))
			if _, done := m.(*pgproto3.ReadyForQuery); done {
				return got
			}
		}
	}

	fe.Send(&pgproto3.Parse{Query: "select 1"})
	fe.Send(&pgproto3.Describe{ObjectType: 'S'})
	fe.Send(&pgproto3.Sync{})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	drain()

	fe.Send(&pgproto3.Bind{})
	fe.Send(&pgproto3.Execute{})
	fe.Send(&pgproto3.Sync{})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	// lib/pq reads exactly one message after its Bind and kills the
	// connection on anything but BindComplete.
	if got := drain(); got[0] != "*pgproto3.BindComplete" {
		t.Fatalf("first message after Bind is %s, PostgreSQL sends BindComplete: %v", got[0], got)
	}
}
