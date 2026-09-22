//go:build integration

package router

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

// TestAFailedBatchIsAnsweredAsPostgreSQLAnswersIt (PGS-974).
//
// PostgreSQL answers an extended-protocol batch message by message and,
// after an error, skips everything until Sync -- no ParseComplete, no
// BindComplete, no CloseComplete for what it skipped. The router answered
// every completion the backend still owed when the batch ended, so a
// client was told the statements after the failure had been parsed and
// bound when PostgreSQL had never looked at them, and a pipelining driver
// counting completions went out of step. Owner decision: relay the
// backend's completions, never synthesise them. The control is PostgreSQL
// itself: the same batch sent straight to the shard.
func TestAFailedBatchIsAnsweredAsPostgreSQLAnswersIt(t *testing.T) {
	s := startStack(t)
	ctx := context.Background()
	for _, c := range []struct {
		name  string
		batch []pgproto3.FrontendMessage
	}{
		{"error_at_bind", []pgproto3.FrontendMessage{
			// Planning folds 1/0, so PostgreSQL raises it at Bind.
			&pgproto3.Parse{Name: "bad", Query: "select 1/0"},
			&pgproto3.Bind{PreparedStatement: "bad"},
			&pgproto3.Execute{},
			&pgproto3.Parse{Name: "after", Query: "select 1"},
			&pgproto3.Bind{PreparedStatement: "after"},
			&pgproto3.Execute{},
		}},
		{"error_at_execute", []pgproto3.FrontendMessage{
			// A function scan is not folded, so this binds and then fails
			// when it runs.
			&pgproto3.Parse{Name: "bad", Query: "select 1/x from generate_series(0, 0) x"},
			&pgproto3.Bind{PreparedStatement: "bad"},
			&pgproto3.Execute{},
			&pgproto3.Parse{Name: "after", Query: "select 1"},
			&pgproto3.Bind{PreparedStatement: "after"},
			&pgproto3.Execute{},
		}},
		{"close_after_error", []pgproto3.FrontendMessage{
			&pgproto3.Parse{Name: "bad", Query: "select 1/x from generate_series(0, 0) x"},
			&pgproto3.Bind{PreparedStatement: "bad"},
			&pgproto3.Execute{},
			&pgproto3.Close{ObjectType: 'S', Name: "bad"},
		}},
		{"no_error", []pgproto3.FrontendMessage{
			&pgproto3.Parse{Name: "one", Query: "select 1"},
			&pgproto3.Bind{PreparedStatement: "one"},
			&pgproto3.Execute{},
			&pgproto3.Close{ObjectType: 'S', Name: "one"},
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			direct := answer(ctx, t, strings.Replace(s.shardDSN, "/postgres?", "/"+appDatabase+"?", 1), c.batch)
			routed := answer(ctx, t, s.dsn(appRole, appPassword, appDatabase), c.batch)
			if strings.Join(routed, " ") != strings.Join(direct, " ") {
				t.Fatalf("the router answered the batch differently from PostgreSQL:\n  postgres: %s\n  router:   %s",
					strings.Join(direct, " "), strings.Join(routed, " "))
			}
		})
	}
}

// answer sends batch and a Sync on a fresh connection to dsn and returns
// what came back up to ReadyForQuery: each message's type, and an error's
// SQLSTATE with it.
func answer(ctx context.Context, t *testing.T, dsn string, batch []pgproto3.FrontendMessage) []string {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	hj, err := conn.PgConn().Hijack()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hj.Conn.Close() }()
	for _, m := range batch {
		hj.Frontend.Send(m)
	}
	hj.Frontend.Send(&pgproto3.Sync{})
	if err := hj.Frontend.Flush(); err != nil {
		t.Fatal(err)
	}
	return receiveAll(t, hj.Conn, hj.Frontend)
}

func receiveAll(t *testing.T, conn net.Conn, fe *pgproto3.Frontend) []string {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	var got []string
	for {
		m, err := fe.Receive()
		if err != nil {
			t.Fatalf("receive: %v (got %v)", err, got)
		}
		name := strings.TrimPrefix(fmt.Sprintf("%T", m), "*pgproto3.")
		if e, ok := m.(*pgproto3.ErrorResponse); ok {
			name += "(" + e.Code + ")"
		}
		got = append(got, name)
		if _, done := m.(*pgproto3.ReadyForQuery); done {
			return got
		}
	}
}
