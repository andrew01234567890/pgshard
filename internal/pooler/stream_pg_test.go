package pooler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

func (h *pgHarness) testStream(t *testing.T) {
	ctx := context.Background()
	for _, sql := range []string{
		"CREATE PUBLICATION pgshard_all FOR ALL TABLES",
		"CREATE TABLE orders (id int primary key, name text)",
		"SELECT pg_create_logical_replication_slot('pgshard_orders_shard0', 'pgoutput', false, true, true)",
		"INSERT INTO orders VALUES (1, 'one')",
		"INSERT INTO orders VALUES (2, 'two')",
		"BEGIN; INSERT INTO orders VALUES (3, 'prepared'); PREPARE TRANSACTION 'g1'",
		"COMMIT PREPARED 'g1'",
	} {
		if _, err := h.admin.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	t.Cleanup(func() {
		_, _ = h.admin.Exec(ctx, "SELECT pg_drop_replication_slot('pgshard_orders_shard0')")
	})

	if _, err := h.client.Ack(ctx, &pgshardv1.AckRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ack without slot: %v", err)
	}
	if r, err := h.client.Ack(ctx, &pgshardv1.AckRequest{Stream: "orders", Lsn: 1}); err != nil || r.GetError() == nil {
		t.Fatalf("ack without reader: %v %v", r, err)
	}
	if s, err := h.client.Stream(ctx, &pgshardv1.StreamRequest{Slot: "nope"}); err == nil {
		if _, err := s.Recv(); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("missing slot: %v", err)
		}
	}

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := h.client.Stream(sctx, &pgshardv1.StreamRequest{Stream: "orders", Options: map[string]string{"two_phase": "on"}})
	if err != nil {
		t.Fatal(err)
	}
	recv := func(s pgshardv1.Pooler_StreamClient) *pgshardv1.ChangeBatch {
		t.Helper()
		b, err := s.Recv()
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		return b
	}
	first := recv(stream)
	kinds := func(b *pgshardv1.ChangeBatch) []string {
		var out []string
		for _, e := range b.Events {
			out = append(out, fmt.Sprintf("%T", e.Event))
		}
		return out
	}
	if len(first.Events) != 4 || first.GetEvents()[0].GetBegin() == nil || first.GetEvents()[1].GetRelation() == nil ||
		first.GetEvents()[2].GetRow() == nil || first.GetEvents()[3].GetCommit() == nil || first.EndLsn == 0 {
		t.Fatalf("first batch: %v", kinds(first))
	}
	row := first.GetEvents()[2].GetRow()
	if row.Schema != "public" || row.Table != "orders" || row.Kind != pgshardv1.ChangeEvent_Row_KIND_INSERT || len(row.New) != 2 ||
		string(row.New[0].Data) != "1" || string(row.New[1].Data) != "one" || row.RelationId == 0 {
		t.Fatalf("row: %v", row)
	}
	rel := first.GetEvents()[1].GetRelation()
	if rel.Table != "orders" || len(rel.Columns) != 2 || !rel.Columns[0].Key || rel.Columns[1].Key || rel.ReplicaIdentity != "d" {
		t.Fatalf("relation: %v", rel)
	}
	if first.GetEvents()[0].GetBegin().GetXid() == 0 || first.GetEvents()[0].Xid == 0 {
		t.Fatalf("begin xid: %v", first.GetEvents()[0])
	}
	second := recv(stream)
	if len(second.Events) != 3 || string(second.GetEvents()[1].GetRow().GetNew()[0].Data) != "2" {
		t.Fatalf("second batch: %v", kinds(second))
	}
	prep := recv(stream)
	if n := len(prep.Events); n != 3 || prep.GetEvents()[0].GetBeginPrepare().GetGid() != "g1" || prep.GetEvents()[2].GetPrepare().GetGid() != "g1" {
		t.Fatalf("prepare batch: %v", kinds(prep))
	}
	cp := recv(stream)
	if len(cp.Events) != 1 || cp.GetEvents()[0].GetCommitPrepared().GetGid() != "g1" {
		t.Fatalf("commit prepared batch: %v", kinds(cp))
	}

	// A second reader on the same slot is refused.
	if dup, err := h.client.Stream(ctx, &pgshardv1.StreamRequest{Stream: "orders"}); err == nil {
		if _, err := dup.Recv(); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("concurrent reader: %v", err)
		}
	}

	// Heartbeats arrive while idle.
	hb := recv(stream)
	if len(hb.Events) != 1 || hb.GetEvents()[0].GetKeepalive() == nil {
		t.Fatalf("heartbeat: %v", kinds(hb))
	}

	ack, err := h.client.Ack(ctx, &pgshardv1.AckRequest{Stream: "orders", Lsn: cp.EndLsn})
	if err != nil || ack.GetError() != nil {
		t.Fatalf("ack: %v %v", ack, err)
	}
	var confirmed uint64
	if err := h.admin.QueryRow(ctx, "SELECT confirmed_flush_lsn - '0/0'::pg_lsn FROM pg_replication_slots WHERE slot_name = 'pgshard_orders_shard0'").Scan(&confirmed); err != nil {
		t.Fatal(err)
	}
	if confirmed < cp.EndLsn {
		t.Fatalf("confirmed_flush_lsn %d < acked %d", confirmed, cp.EndLsn)
	}
	cancel()
	deadline := time.Now().Add(5 * time.Second)
	for {
		h.srv.mu.Lock()
		n := len(h.srv.readers)
		h.srv.mu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("reader not released after cancel")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// A restarted reader resumes after the ack: only the new transaction shows.
	if _, err := h.admin.Exec(ctx, "INSERT INTO orders VALUES (4, 'after-ack')"); err != nil {
		t.Fatal(err)
	}
	stream2, err := h.client.Stream(ctx, &pgshardv1.StreamRequest{Slot: "pgshard_orders_shard0", BatchBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	b := recv(stream2)
	if len(b.Events) != 1 || b.GetEvents()[0].GetBegin() == nil {
		t.Fatalf("resumed stream with batch_bytes=1 must deliver one event per batch: %v", kinds(b))
	}
	b = recv(stream2)
	if b.GetEvents()[0].GetRelation() == nil {
		t.Fatalf("resumed stream: %v", kinds(b))
	}
	b = recv(stream2)
	if r := b.GetEvents()[0].GetRow(); r == nil || string(r.GetNew()[0].Data) != "4" {
		t.Fatalf("resumed stream must start after the acked position: %v", kinds(b))
	}

	// The per-event RPC delivers the same stream one event at a time.
	single, err := h.client.StreamChanges(ctx, &pgshardv1.StreamRequest{Slot: "pgshard_orders_shard0"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := single.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StreamChanges must be refused while Stream holds the slot: %v", err)
	}
}
