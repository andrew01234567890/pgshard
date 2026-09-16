//go:build integration

package router

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// TestAReferenceWriteRefusesARowLimit executes a reference-table write
// with a row limit.
//
// The write runs on every shard, and the router refuses to execute a
// multi-shard portal that a later batch resumes, so a row limit cannot be
// honoured: the second Execute would be refused after the first had
// already written. Scatter reads refuse a row limit for that reason. This
// path did not: it passed the limit through to every shard and then
// dropped the PortalSuspended the shard answered with, so the client saw
// one row, no error, and a result that looked complete.
func TestAReferenceWriteRefusesARowLimit(t *testing.T) {
	s := startShardedStackWith(t, []string{preparedXacts}, []string{preparedXacts})
	s.declareReferenceAndSequences(t)
	ctx := context.Background()
	conn := s.connect(t)
	s.awaitSharded(t, conn)
	s.awaitReference(t, conn)
	if _, err := conn.Exec(ctx, "insert into regions (id, name) values (1, 'eu'), (2, 'us'), (3, 'apac')"); err != nil {
		t.Fatal(err)
	}

	hj, err := conn.PgConn().Hijack()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hj.Conn.Close() })
	fe := hj.Frontend

	read := func() ([]string, *pgproto3.ErrorResponse) {
		_ = hj.Conn.SetReadDeadline(time.Now().Add(20 * time.Second))
		var got []string
		var failed *pgproto3.ErrorResponse
		for {
			m, err := fe.Receive()
			if err != nil {
				t.Fatalf("receive: %v (got %v)", err, got)
			}
			got = append(got, fmt.Sprintf("%T", m))
			switch e := m.(type) {
			case *pgproto3.ErrorResponse:
				failed = e
			case *pgproto3.ReadyForQuery:
				return got, failed
			}
		}
	}

	fe.Send(&pgproto3.Parse{Query: "update regions set name = name returning id"})
	fe.Send(&pgproto3.Bind{})
	fe.Send(&pgproto3.Execute{MaxRows: 1})
	fe.Send(&pgproto3.Sync{})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	got, failed := read()
	if failed == nil {
		t.Fatalf("a row limit on a reference write must be refused, not half-answered:\n%s", strings.Join(got, "\n"))
	}
	if failed.Code != "0A000" || !strings.Contains(failed.Message, "row limit") {
		t.Fatalf("refusal: %s %s", failed.Code, failed.Message)
	}
	if n := countOf(got, "*pgproto3.DataRow"); n != 0 {
		t.Fatalf("the refused statement returned %d rows", n)
	}

	// Without the limit the same statement answers every row, so the
	// refusal is about the limit and not the statement.
	fe.Send(&pgproto3.Parse{Query: "update regions set name = name returning id"})
	fe.Send(&pgproto3.Bind{})
	fe.Send(&pgproto3.Execute{})
	fe.Send(&pgproto3.Sync{})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	rest, failed := read()
	if failed != nil {
		t.Fatalf("without a row limit: %s %s", failed.Code, failed.Message)
	}
	if n := countOf(rest, "*pgproto3.DataRow"); n != 3 {
		t.Fatalf("rows: %d, want 3\n%s", n, strings.Join(rest, "\n"))
	}
}

func countOf(all []string, want string) int {
	n := 0
	for _, s := range all {
		if s == want {
			n++
		}
	}
	return n
}
