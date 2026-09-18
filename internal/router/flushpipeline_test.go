package router

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// awaitFlush reads until the batch's answer arrives, reporting what it saw.
// A Flush that is answered ends in CommandComplete or PortalSuspended; one
// that is not leaves the client reading until the deadline, which is the
// defect and not a slow answer.
func awaitFlush(t *testing.T, fe *pgproto3.Frontend, within time.Duration) error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		for {
			msg, err := fe.Receive()
			if err != nil {
				done <- err
				return
			}
			switch m := msg.(type) {
			case *pgproto3.CommandComplete, *pgproto3.PortalSuspended:
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
		return err
	case <-time.After(within):
		return errors.New("the client is still waiting after its Flush")
	}
}

// TestAFlushAnswersEveryBatchTheRouterCanRunOnOneShard: PGS-911. A client
// driving the extended protocol may send Flush and wait, which is what
// Flush is for -- pgx's pgconn.Pipeline.Flush pushes bytes and then blocks
// in GetResults. The router answered a Flush only for a plain single-shard
// read and wrote nothing for a write, a transaction control statement or a
// session-effect statement, so those clients waited until their read
// deadline.
//
// Each of these is a single-target passthrough: running it at the Flush is
// what PostgreSQL does and leaves the backend's implicit transaction open
// exactly as running it at the Sync would.
func TestAFlushAnswersEveryBatchTheRouterCanRunOnOneShard(t *testing.T) {
	h := newShardedHarness(t)
	tenant, _ := h.twoTenants(t)
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"AWrite", fmt.Sprintf("insert into orders (tenant_id, id) values (%d, 9911)", tenant)},
		{"TransactionControl", "begin"},
		{"ASessionEffect", "set application_name = 'flushed'"},
		{"ASingleShardRead", fmt.Sprintf("select * from orders where tenant_id = %d", tenant)},
		// Answered by the router itself rather than by a backend. It
		// reaches no shard, so there is nothing to keep open and nothing a
		// later statement in the batch could roll back.
		{"ARouterExplain", fmt.Sprintf("explain (pgshard) select * from orders where tenant_id = %d", tenant)},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
				&pgproto3.Parse{Query: tc.sql}, &pgproto3.Bind{},
				&pgproto3.Execute{}, &pgproto3.Flush{},
			} {
				fe.Send(m)
			}
			if err := fe.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := awaitFlush(t, fe, 10*time.Second); err != nil {
				t.Fatalf("Execute then Flush on %q: %v", tc.sql, err)
			}
		})
	}
}

// TestAPipelinedClientGetsItsResultsWithoutSyncing drives the same shapes
// the way an application does, through pgx's own pipeline API. Pipeline.Flush
// sends no Sync, so GetResults blocks until the router answers; a batch the
// router declines fails here with the context deadline rather than a result.
func TestAPipelinedClientGetsItsResultsWithoutSyncing(t *testing.T) {
	h := newShardedHarness(t)
	tenant, _ := h.twoTenants(t)
	conn, err := pgx.Connect(context.Background(), h.dsn())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	p := conn.PgConn().StartPipeline(ctx)
	defer func() { _ = p.Close() }()
	for _, sql := range []string{
		fmt.Sprintf("insert into orders (tenant_id, id) values (%d, 9912)", tenant),
		fmt.Sprintf("select * from orders where tenant_id = %d", tenant),
	} {
		p.SendQueryParams(sql, nil, nil, nil, nil)
		// SendFlushRequest puts the protocol Flush on the wire; Flush only
		// empties pgx's own write buffer. No Sync is sent, so GetResults
		// below reads until the router answers.
		p.SendFlushRequest()
		if err := p.Flush(); err != nil {
			t.Fatalf("flushing %q: %v", sql, err)
		}
		res, err := p.GetResults()
		if err != nil {
			t.Fatalf("a pipelined client waiting on %q: %v", sql, err)
		}
		rr, ok := res.(*pgconn.ResultReader)
		if !ok {
			t.Fatalf("%q answered with %T, want a result", sql, res)
		}
		if _, err := rr.Close(); err != nil {
			t.Fatalf("reading the answer to %q: %v", sql, err)
		}
	}
}

// TestAFlushAnswersAGlobalSequence covers the other batch the router
// answers itself. It needs the reference harness, which is the one with a
// sequence allocator configured.
func TestAFlushAnswersAGlobalSequence(t *testing.T) {
	h := newRefHarness(t)
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
		&pgproto3.Parse{Query: "select nextval('tickets.id')"}, &pgproto3.Bind{},
		&pgproto3.Execute{}, &pgproto3.Flush{},
	} {
		fe.Send(m)
	}
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := awaitFlush(t, fe, 10*time.Second); err != nil {
		t.Fatalf("Execute then Flush on a global sequence: %v", err)
	}
}
