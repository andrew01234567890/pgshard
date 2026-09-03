//go:build integration

package router

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/andrew01234567890/pgshard/internal/pooler"
)

// TestThePayloadLimitIsReportedAtItsBoundary walks the size boundary
// through the real stack, which is the only place it is real: the limit is
// enforced by grpc-go between the router and a pooler, so no amount of
// unit testing either side reaches it.
//
// A value just under has to work. A value just over has to come back as a
// size limit -- 54000, program_limit_exceeded -- and not as a lost
// connection, because reconnecting changes nothing about a value that is
// too big and the client would never learn why.
func TestThePayloadLimitIsReportedAtItsBoundary(t *testing.T) {
	s := startStack(t)
	ctx := context.Background()
	conn := s.connect(t)
	if _, err := conn.Exec(ctx, "create table blobs (id int primary key, body bytea)"); err != nil {
		t.Fatal(err)
	}

	// Room for the row's framing and the rest of the message around the
	// value: what is bounded is the protobuf message, not the parameter.
	const headroom = 64 << 10
	under := make([]byte, pooler.MaxMessageBytes-headroom)
	for i := range under {
		under[i] = byte('a' + i%26)
	}
	if _, err := conn.Exec(ctx, "insert into blobs values ($1, $2)", 1, under); err != nil {
		t.Fatalf("a value inside the limit must be carried: %v", err)
	}
	var got []byte
	if err := conn.QueryRow(ctx, "select body from blobs where id = $1", 1).Scan(&got); err != nil {
		t.Fatalf("reading it back: %v", err)
	}
	if len(got) != len(under) {
		t.Fatalf("read back %d bytes of %d", len(got), len(under))
	}

	over := make([]byte, pooler.MaxMessageBytes+1)
	_, err := conn.Exec(ctx, "insert into blobs values ($1, $2)", 2, over)
	if err == nil {
		t.Fatal("a value past the limit was accepted")
	}
	var pge *pgconn.PgError
	if !errors.As(err, &pge) {
		t.Fatalf("the client got %v, which is not a protocol error it can act on", err)
	}
	if pge.Code != "54000" {
		t.Fatalf("SQLSTATE %s (%s), want 54000 program_limit_exceeded", pge.Code, pge.Message)
	}
	for _, want := range []string{"pgshard", "limit"} {
		if !strings.Contains(strings.ToLower(pge.Message+pge.Detail+pge.Hint), want) {
			t.Fatalf("the error does not mention %q, so nobody can tell whose limit it is: %+v", want, pge)
		}
	}

	// And the session is still usable: a refused value is not a broken
	// connection, which is the whole distinction being drawn.
	var n int
	if err := conn.QueryRow(ctx, "select count(*) from blobs").Scan(&n); err != nil {
		t.Fatalf("the session did not survive a refused value: %v", err)
	}
	if n != 1 {
		t.Fatalf("%d rows, want the one that fit", n)
	}
}
