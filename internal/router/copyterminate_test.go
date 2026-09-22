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
	var code string
	for {
		msg, err := fe.Receive()
		if err != nil {
			t.Fatal(err)
		}
		if e, ok := msg.(*pgproto3.ErrorResponse); ok && code == "" {
			code = e.Code
		}
		if rfq, ok := msg.(*pgproto3.ReadyForQuery); ok {
			if rfq.TxStatus != 'I' {
				t.Fatalf("status %c after the failed COPY", rfq.TxStatus)
			}
			break
		}
	}
	if code != "08P01" {
		t.Fatalf("error code %q, want 08P01", code)
	}
	for i := range h.poolers {
		if h.ranOn(i, "commit") {
			t.Fatalf("shard %d committed: %v", i, h.poolers[i].ran())
		}
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
