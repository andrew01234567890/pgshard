package router

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// A client that goes away in the middle of a COPY has not finished it.
// PostgreSQL refuses the load ("unexpected message type 0x58 during COPY
// from stdin"); Terminate used to read as CopyDone, and every shard
// committed what had arrived, a half-sent last row included.
func TestATerminateDuringACopyCommitsNothing(t *testing.T) {
	h := newCopyHarness(t)
	conn := h.connect(t, h.dsn())
	nc := conn.PgConn().Conn()
	fe := pgproto3.NewFrontend(nc, nc)
	fe.Send(&pgproto3.Query{String: "copy orders (tenant_id, id) from stdin"})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := fe.Receive(); err != nil {
		t.Fatal(err)
	}
	var data strings.Builder
	for k := 1; k <= 50; k++ {
		data.WriteString("1\t100\n")
	}
	data.WriteString("2\t12")
	fe.Send(&pgproto3.CopyData{Data: []byte(data.String())})
	fe.Send(&pgproto3.Terminate{})
	_ = fe.Flush()
	deadline := time.Now().Add(5 * time.Second)
	for {
		rolledBack := 0
		for i := range h.poolers {
			if h.ranOn(i, "commit") {
				t.Fatalf("shard %d committed a COPY the client never finished: %v", i, h.poolers[i].ran())
			}
			if h.ranOn(i, "rollback") {
				rolledBack++
			}
		}
		if rolledBack == len(h.poolers) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the COPY was not rolled back on every shard")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := h.copiedRows(t); len(got) != 0 {
		t.Fatalf("rows reached a completed COPY: %v", got)
	}
}

// Any message but CopyData, CopyDone, CopyFail, Flush or Sync during a COPY
// is a protocol violation in PostgreSQL, and the load is not committed.
func TestAQueryDuringACopyIsAProtocolViolation(t *testing.T) {
	h := newCopyHarness(t)
	conn := h.connect(t, h.dsn())
	nc := conn.PgConn().Conn()
	fe := pgproto3.NewFrontend(nc, nc)
	fe.Send(&pgproto3.Query{String: "copy orders (tenant_id, id) from stdin"})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := fe.Receive(); err != nil {
		t.Fatal(err)
	}
	// A router that dropped the Query would never answer it: fail rather
	// than hang.
	if err := nc.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	fe.Send(&pgproto3.CopyData{Data: []byte("1\t100\n")})
	fe.Send(&pgproto3.Sync{})
	fe.Send(&pgproto3.Query{String: "select 1"})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	expectProtocolLost(t, fe)
	for i := range h.poolers {
		if h.ranOn(i, "commit") {
			t.Fatalf("shard %d committed: %v", i, h.poolers[i].ran())
		}
	}
}

// expectProtocolLost reads what PostgreSQL sends after a client broke a
// COPY: one FATAL 08P01, "protocol synchronization was lost", and then the
// connection closes -- no ReadyForQuery, and no answer for the message that
// broke it.
func expectProtocolLost(t *testing.T, fe *pgproto3.Frontend) {
	t.Helper()
	var fatal *pgproto3.ErrorResponse
	for {
		msg, err := fe.Receive()
		if err != nil {
			break
		}
		switch m := msg.(type) {
		case *pgproto3.ErrorResponse:
			if m.Severity == "FATAL" {
				fatal = m
			}
		case *pgproto3.ReadyForQuery:
			t.Fatal("the session carried on after the COPY lost protocol synchronization")
		}
	}
	if fatal == nil || fatal.Code != "08P01" || !strings.Contains(fatal.Message, "protocol synchronization was lost") {
		t.Fatalf("closing error %+v, want FATAL 08P01 protocol synchronization was lost", fatal)
	}
}

// A shard that raises notices during a load -- a trigger raising one per
// row -- used to fill the stream nobody read until the COPY ended, and the
// load wedged out of reach of the client and of a cancel.
func TestACopyWhoseShardsRaiseNoticesDuringTheLoadCompletes(t *testing.T) {
	h := newCopyHarness(t)
	for _, p := range h.poolers {
		p.mu.Lock()
		p.copyNoticeBytes = 32 << 10
		p.mu.Unlock()
	}
	cfg, err := pgx.ParseConfig(h.dsn())
	if err != nil {
		t.Fatal(err)
	}
	var notices atomic.Int64
	cfg.OnNotice = func(*pgconn.PgConn, *pgconn.Notice) { notices.Add(1) }
	conn, err := pgx.ConnectConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	var in strings.Builder
	big := strings.Repeat("x", 70<<10)
	for k := 1; k <= 400; k++ {
		fmt.Fprintf(&in, "%d\t%s\n", k, big)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	type result struct {
		tag pgconn.CommandTag
		err error
	}
	done := make(chan result, 1)
	go func() {
		tag, err := conn.PgConn().CopyFrom(ctx, strings.NewReader(in.String()), "copy orders (tenant_id, id) from stdin")
		done <- result{tag, err}
	}()
	var r result
	select {
	case r = <-done:
	case <-time.After(30 * time.Second):
		// The wedge ignores the client's context too, so wait for it here.
		t.Fatal("the COPY wedged: no answer after 30s")
	}
	tag, err := r.tag, r.err
	if err != nil {
		t.Fatalf("COPY: %v", err)
	}
	if tag.RowsAffected() != 400 {
		t.Fatalf("tag %q", tag)
	}
	if notices.Load() == 0 {
		t.Fatal("the shards' notices never reached the client")
	}
}

// The unsharded relay too: the client that broke the COPY is disconnected,
// and the pooler's answer to the router's CopyFail is read on the way out.
func TestAnUnshardedCopyBrokenByAStrayMessageLeavesTheSessionInStep(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	nc := conn.PgConn().Conn()
	fe := pgproto3.NewFrontend(nc, nc)
	if err := nc.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	fe.Send(&pgproto3.Query{String: "copy t from stdin"})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := fe.Receive(); err != nil {
		t.Fatal(err)
	}
	fe.Send(&pgproto3.CopyData{Data: []byte("1\n")})
	fe.Send(&pgproto3.Query{String: "select 1"})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	expectProtocolLost(t, fe)
	// The router cleaned the COPY up on the pooler before closing: a new
	// session on the same router is answered in step.
	next := h.connect(t, h.dsn("app", "secret", "app"))
	var one int
	if err := next.QueryRow(context.Background(), "select 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("a fresh session: %d %v", one, err)
	}
}

// The unsharded relay too (PGS-983): the pooler relays the backend's
// notices while the rows arrive, and a router that read nothing until the
// load ended filled its stream and wedged.
func TestAnUnshardedCopyWhoseBackendRaisesNoticesCompletes(t *testing.T) {
	h := newHarness(t)
	h.fp.mu.Lock()
	h.fp.copyNoticeBytes = 32 << 10
	h.fp.mu.Unlock()
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	var in strings.Builder
	line := strings.Repeat("x", 70<<10) + "\n"
	for range 400 {
		in.WriteString(line)
	}
	done := make(chan error, 1)
	go func() {
		_, err := conn.PgConn().CopyFrom(context.Background(), strings.NewReader(in.String()), "copy t from stdin")
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("COPY: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the unsharded COPY wedged")
	}
}
