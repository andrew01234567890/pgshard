//go:build integration

package router

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// rawConn reads the backend's messages on ONE goroutine: a Flush has no
// ReadyForQuery to stop at, and a second reader started per batch races the
// one left waiting from the batch before for the same connection.
type rawConn struct {
	fe  *pgproto3.Frontend
	msg chan string
	// lastRow is the values of the last DataRow received.
	mu      sync.Mutex
	lastRow []string
}

func newRawConn(fe *pgproto3.Frontend) *rawConn {
	r := &rawConn{fe: fe, msg: make(chan string, 64)}
	go func() {
		for {
			m, err := fe.Receive()
			if err != nil {
				close(r.msg)
				return
			}
			name := strings.TrimPrefix(fmt.Sprintf("%T", m), "*pgproto3.")
			if e, ok := m.(*pgproto3.ErrorResponse); ok {
				name += "(" + e.Code + " " + e.Message + ")"
			}
			if d, ok := m.(*pgproto3.DataRow); ok {
				row := make([]string, len(d.Values))
				for i, v := range d.Values {
					row[i] = string(v)
				}
				r.mu.Lock()
				r.lastRow = row
				r.mu.Unlock()
			}
			r.msg <- name
		}
	}()
	return r
}

// batch sends msgs and collects answers until ReadyForQuery, or until quiet
// for a second when the batch ends in a Flush.
func (r *rawConn) batch(t *testing.T, flushOnly bool, msgs ...pgproto3.FrontendMessage) []string {
	t.Helper()
	for _, m := range msgs {
		r.fe.Send(m)
	}
	if err := r.fe.Flush(); err != nil {
		t.Fatal(err)
	}
	var out []string
	for {
		wait := 30 * time.Second
		if flushOnly {
			wait = time.Second
		}
		select {
		case m, ok := <-r.msg:
			if !ok {
				t.Fatalf("connection closed; got %v", out)
			}
			out = append(out, m)
			if m == "ReadyForQuery" {
				return out
			}
		case <-time.After(wait):
			if !flushOnly {
				t.Fatalf("no ReadyForQuery within %s; got %v", wait, out)
			}
			return out
		}
	}
}

// TestADDLStatementIsDescribedAndFlushedLikePostgreSQLDoes (PGS-905
// findings 2 and 3): the unit harness's fake pooler answers a Describe of a
// statement no backend ever parsed, so it could neither show nor rule out
// that a Describe-only batch of a DDL prepared earlier reaches a backend that
// never saw it, or that a Flush after Parse and Describe of a DDL leaves a
// backend open across the migration. Measured here against real PostgreSQL,
// both are answered exactly as PostgreSQL answers them; this pins that.
func TestADDLStatementIsDescribedAndFlushedLikePostgreSQLDoes(t *testing.T) {
	s := startDDLStack(t)
	ctx := context.Background()
	setup := s.connect(t)
	s.awaitSharded(t, setup)
	_ = setup.Close(ctx)

	conn := s.connect(t)
	hj, err := conn.PgConn().Hijack()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hj.Conn.Close() })
	r := newRawConn(hj.Frontend)
	expect := func(what string, got []string, want ...string) {
		t.Helper()
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("%s answered %v, want %v", what, got, want)
		}
	}

	expect("Parse of a DDL in its own batch",
		r.batch(t, false, &pgproto3.Parse{Name: "s", Query: "create table pgs905_t (id int)"}, &pgproto3.Sync{}),
		"ParseComplete", "ReadyForQuery")
	expect("a Describe of it in a later batch",
		r.batch(t, false, &pgproto3.Describe{ObjectType: 'S', Name: "s"}, &pgproto3.Sync{}),
		"ParameterDescription", "NoData", "ReadyForQuery")

	expect("Parse and Describe of a DDL answered at a Flush",
		r.batch(t, true, &pgproto3.Parse{Name: "d", Query: "create table pgs905_u (id int)"}, &pgproto3.Describe{ObjectType: 'S', Name: "d"}, &pgproto3.Flush{}),
		"ParseComplete", "ParameterDescription", "NoData")
	expect("its Bind and Execute after the Flush",
		r.batch(t, false, &pgproto3.Bind{PreparedStatement: "d"}, &pgproto3.Execute{}, &pgproto3.Sync{}),
		"BindComplete", "CommandComplete", "ReadyForQuery")
	if got := s.catalogValue(t, `SELECT coalesce(string_agg(kind || ':' || state, ','), '') FROM pgshard.migrations`); got != "CREATE TABLE:complete" {
		t.Fatalf("migrations %q, want the one CREATE TABLE complete and the unexecuted one never queued", got)
	}
	expect("the session afterwards",
		r.batch(t, false, &pgproto3.Query{String: "select count(*) from pgs905_u"}),
		"RowDescription", "DataRow", "CommandComplete", "ReadyForQuery")
}

// row returns the values of the last DataRow received.
func (r *rawConn) row() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastRow
}
