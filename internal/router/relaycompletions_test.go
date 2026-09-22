package router

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// TestNothingIsAnsweredForWhatAFailedBatchSkipped (PGS-974): after an error
// the backend skips the rest of the batch until Sync, and the router must
// not answer for it -- no ParseComplete, BindComplete or CloseComplete for a
// message the backend never processed. The real-stack test compares against
// PostgreSQL itself; this one keeps it in the fast suite, against a fake that
// skips after an error as PostgreSQL does.
func TestNothingIsAnsweredForWhatAFailedBatchSkipped(t *testing.T) {
	h := newHarness(t)
	h.fp.script("select boom", script{err: "division by zero", code: "22012"})
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	hj, err := conn.PgConn().Hijack()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hj.Conn.Close() })
	fe := hj.Frontend
	for _, m := range []pgproto3.FrontendMessage{
		&pgproto3.Parse{Name: "bad", Query: "select boom"},
		&pgproto3.Bind{PreparedStatement: "bad"},
		&pgproto3.Execute{},
		&pgproto3.Parse{Name: "after", Query: "select 1"},
		&pgproto3.Bind{PreparedStatement: "after"},
		&pgproto3.Execute{},
		&pgproto3.Close{ObjectType: 'S', Name: "bad"},
		&pgproto3.Sync{},
	} {
		fe.Send(m)
	}
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	_ = hj.Conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	var got []string
	for {
		m, err := fe.Receive()
		if err != nil {
			t.Fatalf("receive: %v (got %v)", err, got)
		}
		got = append(got, strings.TrimPrefix(fmt.Sprintf("%T", m), "*pgproto3."))
		if _, done := m.(*pgproto3.ReadyForQuery); done {
			break
		}
	}
	want := "ParseComplete BindComplete ErrorResponse ReadyForQuery"
	if strings.Join(got, " ") != want {
		t.Fatalf("a batch whose first statement failed at Execute was answered\n  %s\nwant, as PostgreSQL answers it,\n  %s", strings.Join(got, " "), want)
	}
}
