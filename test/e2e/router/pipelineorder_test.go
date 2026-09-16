//go:build integration

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

// TestPipelinedBatchAnswersInMessageOrder drives a pipeline of two
// statements and reads the messages back in the order they arrive.
//
// PostgreSQL answers extended-protocol messages in the order it received
// them: everything for the first statement, then everything for the
// second. A router that answers Parse and Bind as they arrive while
// deferring Describe and Execute to Sync sends the second statement's
// ParseComplete before the first statement's results, and a client reading
// one statement's results at a time -- which is what pgx does in its
// default exec mode -- fails on the message it did not expect.
func TestPipelinedBatchAnswersInMessageOrder(t *testing.T) {
	s := startStack(t)
	conn := s.connect(t)
	hj, err := conn.PgConn().Hijack()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hj.Conn.Close() })
	fe := hj.Frontend

	for _, st := range []struct {
		name string
		send func()
	}{
		{"parse_bind_describe_execute_twice", func() {
			for i, q := range []string{"select 1", "select 2"} {
				name := fmt.Sprintf("s%d", i)
				fe.Send(&pgproto3.Parse{Name: name, Query: q})
				fe.Send(&pgproto3.Bind{PreparedStatement: name})
				fe.Send(&pgproto3.Describe{ObjectType: 'P'})
				fe.Send(&pgproto3.Execute{})
			}
			fe.Send(&pgproto3.Sync{})
		}},
		{"parse_describe_bind_execute_twice", func() {
			for i, q := range []string{"select 3", "select 4"} {
				name := fmt.Sprintf("d%d", i)
				fe.Send(&pgproto3.Parse{Name: name, Query: q})
				fe.Send(&pgproto3.Describe{ObjectType: 'S', Name: name})
				fe.Send(&pgproto3.Bind{PreparedStatement: name})
				fe.Send(&pgproto3.Execute{})
			}
			fe.Send(&pgproto3.Sync{})
		}},
	} {
		t.Run(st.name, func(t *testing.T) {
			st.send()
			if err := fe.Flush(); err != nil {
				t.Fatal(err)
			}
			got := drainMessages(t, hj.Conn, fe)
			// Whatever the shape of each statement's answer, the second
			// statement's ParseComplete comes after the first statement's
			// CommandComplete: PostgreSQL finishes one before it starts
			// the next.
			firstDone := indexOf(got, "*pgproto3.CommandComplete")
			secondParsed := lastIndexOf(got, "*pgproto3.ParseComplete")
			if firstDone < 0 || secondParsed < 0 {
				t.Fatalf("unexpected answer: %v", got)
			}
			if secondParsed < firstDone {
				t.Fatalf("the second statement was answered before the first finished:\n%s", strings.Join(got, "\n"))
			}
		})
	}
}

// TestPgxBatchInEveryExecMode is the client's view of the same thing: pgx
// batches two statements, which its default exec mode sends as one
// pipeline.
func TestPgxBatchInEveryExecMode(t *testing.T) {
	s := startStack(t)
	ctx := context.Background()
	for _, mode := range []struct {
		name string
		mode pgx.QueryExecMode
	}{
		{"cache_statement", pgx.QueryExecModeCacheStatement},
		{"cache_describe", pgx.QueryExecModeCacheDescribe},
		{"describe_exec", pgx.QueryExecModeDescribeExec},
		{"exec", pgx.QueryExecModeExec},
		{"simple_protocol", pgx.QueryExecModeSimpleProtocol},
	} {
		t.Run(mode.name, func(t *testing.T) {
			cfg, err := pgx.ParseConfig(s.dsn(appRole, appPassword, appDatabase))
			if err != nil {
				t.Fatal(err)
			}
			cfg.DefaultQueryExecMode = mode.mode
			c, err := pgx.ConnectConfig(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close(ctx) }()
			var b pgx.Batch
			b.Queue("select 1")
			b.Queue("select 2")
			br := c.SendBatch(ctx, &b)
			var first, second int
			if err := br.QueryRow().Scan(&first); err != nil {
				t.Fatalf("first statement of the batch: %v", err)
			}
			if err := br.QueryRow().Scan(&second); err != nil {
				t.Fatalf("second statement of the batch: %v", err)
			}
			if err := br.Close(); err != nil {
				t.Fatalf("closing the batch: %v", err)
			}
			if first != 1 || second != 2 {
				t.Fatalf("batch answered %d and %d", first, second)
			}
		})
	}
}

func drainMessages(t *testing.T, conn interface{ SetReadDeadline(time.Time) error }, fe *pgproto3.Frontend) []string {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(20 * time.Second))
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

func indexOf(all []string, want string) int {
	for i, s := range all {
		if s == want {
			return i
		}
	}
	return -1
}

func lastIndexOf(all []string, want string) int {
	for i := len(all) - 1; i >= 0; i-- {
		if all[i] == want {
			return i
		}
	}
	return -1
}

// TestTheRoutersOwnParseIsNotAnsweredToTheClient drives the sequence that
// makes the router re-Parse a statement the client parsed in an earlier
// batch: the backend answers that Parse too, and its ParseComplete is the
// router's, not the client's. A client counting messages against what it
// sent is desynchronised by one for the rest of the session if it arrives.
func TestTheRoutersOwnParseIsNotAnsweredToTheClient(t *testing.T) {
	s := startStack(t)
	conn := s.connect(t)
	hj, err := conn.PgConn().Hijack()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hj.Conn.Close() })
	fe := hj.Frontend

	// The unnamed statement, parsed and synced on its own, so the Bind
	// below is in a batch that never parsed it. The router carries the
	// statement to whatever backend the next batch lands on by parsing it
	// again there.
	fe.Send(&pgproto3.Parse{Query: "select 1"})
	fe.Send(&pgproto3.Sync{})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := drainMessages(t, hj.Conn, fe); indexOf(got, "*pgproto3.ParseComplete") < 0 {
		t.Fatalf("the client's own Parse was not answered: %v", got)
	}

	fe.Send(&pgproto3.Bind{})
	fe.Send(&pgproto3.Execute{})
	fe.Send(&pgproto3.Sync{})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	got := drainMessages(t, hj.Conn, fe)
	if n := indexOf(got, "*pgproto3.ParseComplete"); n >= 0 {
		t.Fatalf("the router's own Parse was answered to the client:\n%s", strings.Join(got, "\n"))
	}
	if indexOf(got, "*pgproto3.BindComplete") < 0 || indexOf(got, "*pgproto3.CommandComplete") < 0 {
		t.Fatalf("the batch was not answered: %v", got)
	}
}
